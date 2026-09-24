"""Technical "chart studies" layer: correlation-derived AND multi-timeframe-
confluence confidence signals layered on top of (never replacing) each
strategy's existing technical + model gate.

Two families of study feed this:
  - CORRELATION studies (cross-instrument): built from data each service
    already fetches every collect cycle -- no extra network calls of its
    own. See the two within-market studies below.
  - MULTI-TIMEFRAME CONFLUENCE (single-instrument): see
    multi_timeframe_bullishness -- how many of ONE coin's own timeframes
    (5m/15m/30m return, 1h/2h/3h/4h trend, MACD, RSI) currently agree on
    direction, computed straight from the SAME engineered feature row
    (perps_data.py / alpaca_crypto_data.py's own latest_feature_row) every
    strategy already fetches at decision time -- no extra data collection
    of its own either. Several independent timeframes agreeing is a real
    confluence signal; one indicator flipping alone is not.

Two independent within-market CORRELATION studies feed this:
  - Perps' own ~13-instrument correlation web, built from
    perps_data.collect_dataset_rows()'s own output on ITS data-collect
    cycle (see refresh_perps_study, called from app_kalshi.py).
  - Alpaca crypto's much broader, unlevered spot universe (currently 36
    USD pairs vs perps' 13), built from alpaca_crypto_data.collect_dataset_rows()'s
    own output on ITS OWN, independent data-collect cycle (see
    refresh_alpaca_study, called from alpaca_crypto_server.py).

Explicit user direction: because Kalshi perps can go SHORT as well as long
(Alpaca crypto is real spot, cash-settled, long-only -- see
alpaca_crypto_strategy.py's own docstring), Alpaca's broader universe is
the more useful side to SHARE across markets. A coin that has run ahead of
what its historical correlation to the market leader predicts is a
mean-reversion SHORT candidate on perps -- a trade Alpaca's own long-only
strategy structurally cannot take itself even though its own data is what
reveals the signal. That's what ALPACA_CRYPTO_CORRELATION_STUDY_HF_FILENAME
below is for: alpaca_crypto_data.py pushes its own study (a small JSON, not
the raw archive) to its HF dataset repo every collect cycle; perps_data.py
pulls that ONE small file on its own, independent, slower collect cycle and
caches it in-process (set_remote_alpaca_study/get_remote_alpaca_study) --
so the live entry-scan (~every 2min) and fast-exit-check (~every 20s) loops
on BOTH services only ever do an in-memory dict lookup for this, never a
blocking computation or network call. This mirrors the exact durability
pattern perps_strategy.py's own _push_durable_state_to_hf/
_pull_durable_state_from_hf already use for its trade state.

Every score this module returns is BULLISH-signed: positive supports a
LONG / contradicts a SHORT, negative supports a SHORT / contradicts a LONG
-- the same convention perps_strategy.decide_exit already uses for
momentum_pct/breakout_pct_b/sentiment_score, so callers on both sides flip
sign for a short position themselves rather than this module baking a side
argument into every function.

This is an ADD-ON, not a replacement: every study is an ADDITIVE confidence
adjustment layered on top of the existing technical+model gate, never a
new, independent way to enter on its own -- an entry still has to clear
every filter it always did (dip/rally, trend, volatility, volume, model
confidence) first. The one real veto path (skipping the technical-only,
no-model-yet fallback when the study actively disagrees) only tightens an
already-cautious path this codebase already treats as its riskiest --
shorts are blocked on that exact path unconditionally, with or without
this layer. Everywhere else, this can only nudge the SAME confidence bar
the base gate already enforces, capped at
+/-PERPS_CORRELATION_CONFIDENCE_MAX_ADJUSTMENT (default 0.06).

Gated behind PERPS_USE_CORRELATION_STUDY / ALPACA_CRYPTO_USE_CORRELATION_STUDY
-- both default OFF at the CODE level (any other deployment, and every
test in this repo, gets the conservative default), graduated to ON for
the actual deployed services via the live HF Space's own environment
variables (`render.yaml`'s `sync: false` env vars served this exact role
before this codebase's Render-to-HF migration -- confirmed still ON
today via the Space's own configuration, not just carried over by
assumption), the same way PERPS_ENABLE_SHORTS was graduated. Correctness
here is covered by 30+ unit tests and
leakage-free backtest wiring, but -- unlike ENABLE_SHORTS, which had two
real historical-return backtest runs behind its own graduation -- this has
not yet been validated by an actual backtest RUN showing it improves
returns; recommend_correlation_study_weight (see perps_trade_analysis.py /
alpaca_crypto_trade_analysis.py) watches real closed trades from here on
and will scale this back down, or disable it outright, the moment real
P&L shows it isn't helping -- same "ship it, then let real evidence keep
it honest" posture every other new signal in this codebase has shipped
with (see e.g. perps_strategy.py's own SKIP_QUICK_PROFIT_WHEN_PROMISING)."""
from __future__ import annotations

import datetime as dt
import logging
from typing import Any, Callable

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Shared "contract" filename both alpaca_crypto_data.py (push) and
# perps_data.py (pull) import from here, so the two independent services
# agree on where the shared study lives on HF without either hardcoding
# the other's own constant.
ALPACA_CRYPTO_CORRELATION_STUDY_HF_FILENAME = "alpaca_crypto_correlation_study.json"

# Windows in units of feature ROWS -- both services' engineer_features
# emits one row per 1-minute bar, so this is effectively minutes. Short
# enough to react to the current session, long enough that a handful of
# noisy prints can't flip a whole study.
PEER_CORR_WINDOW_ROWS = 180          # ~3h of 1-min bars
BREADTH_WINDOW_ROWS = 15             # breadth is meant to read as CURRENT, not a slow trend
DIVERGENCE_SHORT_WINDOW_ROWS = 30    # "current move" window for the leader-divergence study
DIVERGENCE_LONG_WINDOW_ROWS = 240    # window the beta/residual-vol estimate is fit over
MIN_CORR_PERIODS = 30                # below this, a correlation reading is noise, not signal
RET_COL = "ret_5m"                   # smoother than ret_1m, still responsive; same column name in both services' engineer_features


def _pivot_returns(df: pd.DataFrame, *, id_col: str, ts_col: str, ret_col: str) -> pd.DataFrame:
    """Wide [ts x id] matrix of one return column, most-recent last. Gaps
    (a coin's collect cycle failed this round, or it's simply newer/thinner
    than its peers) are left as NaN, not forward-filled -- pandas' own
    .corr() and the nanmean-based helpers below already skip NaN pairwise,
    which is the correct behavior here: silently fabricating a flat 0%
    return for a gap would understate that coin's real volatility and
    corrupt its correlation reading, not just leave it blank."""
    if df.empty or id_col not in df.columns or ret_col not in df.columns or ts_col not in df.columns:
        return pd.DataFrame()
    wide = df.pivot_table(index=ts_col, columns=id_col, values=ret_col, aggfunc="last")
    return wide.sort_index()


def _cum_return(returns: pd.Series, window_rows: int) -> float | None:
    tail = returns.tail(window_rows).dropna()
    if tail.empty:
        return None
    return float((1.0 + tail).prod() - 1.0)


def build_study(
    df: pd.DataFrame, *, id_col: str, leader_id: str | None = None, ts_col: str = "ts", ret_col: str = RET_COL,
    coin_of: Callable[[str], str] | None = None,
) -> dict[str, Any]:
    """The one entry point both services call from their own data-collect
    cycle, on the SAME DataFrame collect_dataset_rows() just fetched this
    cycle -- no extra network call here.

    `id_col` is whichever column holds per-instrument identity ("ticker"
    for perps, "symbol" for Alpaca crypto). `coin_of`, if given, maps that
    column to a plain coin code (coin_for_ticker / symbol_to_coin) BEFORE
    computing anything, so the two services' studies key off the same
    coin-code namespace ("BTC", not "KXBTCPERP" or "BTC/USD") without this
    module importing either service's own ticker-mapping helper (avoids a
    real circular-import risk -- perps_data.py needs to import FROM this
    module to pull the remote study).

    `leader_id` (already a coin code, e.g. "BTC") computes the
    leader-divergence study against that one instrument; omitted, only
    peer-correlation + breadth are returned."""
    work = df
    if coin_of is not None and id_col in df.columns:
        work = df.copy()
        work[id_col] = work[id_col].map(coin_of)

    wide = _pivot_returns(work, id_col=id_col, ts_col=ts_col, ret_col=ret_col)
    return _build_study_from_wide(wide, leader_id=leader_id)


def build_study_from_wide(wide: pd.DataFrame, *, as_of_ts: float | None = None, leader_id: str | None = None) -> dict[str, Any]:
    """Same computation as build_study, but takes an already-pivoted
    [ts x id] returns matrix (see _pivot_returns) and an explicit
    `as_of_ts` cutoff -- rows with ts > as_of_ts are dropped BEFORE
    anything is computed, so a backtest can call this at each simulated
    decision point without leaking a later timestamp's data into an
    earlier decision. Same leakage-free discipline perps_data.py's own
    engineer_features already holds itself to (see perps_backtest.py's own
    use of this for the correlation-study backtest wiring)."""
    if as_of_ts is not None and not wide.empty:
        wide = wide[wide.index <= as_of_ts]
    return _build_study_from_wide(wide, leader_id=leader_id)


def _build_study_from_wide(wide: pd.DataFrame, *, leader_id: str | None) -> dict[str, Any]:
    result: dict[str, Any] = {
        "computed_at": dt.datetime.now(dt.timezone.utc).isoformat(), "leader_id": leader_id, "ids": [],
        "corr": {}, "cum_return": {}, "breadth": None, "divergence_z": {},
    }
    if wide.empty:
        return result

    ids = list(wide.columns)
    result["ids"] = ids

    corr_matrix = wide.tail(PEER_CORR_WINDOW_ROWS).corr(min_periods=MIN_CORR_PERIODS)
    for a in ids:
        row = corr_matrix.get(a)
        if row is None:
            continue
        peers = {b: float(v) for b, v in row.items() if b != a and pd.notna(v)}
        if peers:
            result["corr"][a] = peers

    for coin in ids:
        cr = _cum_return(wide[coin], DIVERGENCE_SHORT_WINDOW_ROWS)
        if cr is not None:
            result["cum_return"][coin] = cr

    breadth_window = wide.tail(BREADTH_WINDOW_ROWS)
    if not breadth_window.empty:
        signs = [
            float(np.sign(cr)) for cr in (
                _cum_return(breadth_window[coin], BREADTH_WINDOW_ROWS) for coin in ids
            ) if cr is not None
        ]
        if signs:
            result["breadth"] = float(np.mean(signs))

    if leader_id and leader_id in wide.columns:
        leader_long = wide[leader_id].tail(DIVERGENCE_LONG_WINDOW_ROWS)
        leader_var = float(leader_long.var(skipna=True) or 0.0)
        leader_cum_short = result["cum_return"].get(leader_id)
        if leader_var > 0 and leader_cum_short is not None:
            for coin in ids:
                if coin == leader_id:
                    continue
                coin_cum_short = result["cum_return"].get(coin)
                if coin_cum_short is None:
                    continue
                coin_long = wide[coin].tail(DIVERGENCE_LONG_WINDOW_ROWS)
                paired = pd.concat([coin_long, leader_long], axis=1, keys=["coin", "leader"]).dropna()
                if len(paired) < MIN_CORR_PERIODS:
                    continue
                beta = float(paired["coin"].cov(paired["leader"]) / leader_var)
                expected = beta * leader_cum_short
                divergence = coin_cum_short - expected
                # Scale by this coin's OWN residual volatility (how much it
                # normally deviates from what beta*leader would predict),
                # not the leader's -- a coin that's naturally noisy relative
                # to the leader needs a bigger raw divergence to mean the
                # same thing as a small one on a tightly-tracking coin.
                resid_std = float((paired["coin"] - beta * paired["leader"]).std(skipna=True) or 0.0)
                scale = resid_std * (DIVERGENCE_SHORT_WINDOW_ROWS ** 0.5)
                if scale > 1e-9:
                    z = divergence / scale
                    result["divergence_z"][coin] = max(-3.0, min(3.0, float(z)))

    return result


# ── In-process caches ───────────────────────────────────────────────────
# Refreshed once per data-collect cycle (every PERPS_DATA_COLLECT_MINUTES /
# ALPACA_CRYPTO_DATA_COLLECT_MINUTES, both well ahead of the entry-scan /
# fast-exit loops) -- see refresh_perps_study/refresh_alpaca_study below,
# called from app_kalshi.py / alpaca_crypto_server.py's own data-collect
# jobs. A live decision only ever reads a plain dict already sitting in
# memory here, never blocks on a fresh computation or network call.
_PERPS_STUDY: dict[str, Any] = {}
_ALPACA_STUDY: dict[str, Any] = {}
_REMOTE_ALPACA_STUDY: dict[str, Any] = {}
_REMOTE_ALPACA_STUDY_STALE_AFTER_SEC = 3600  # a couple of missed collect cycles' worth of slack


def refresh_perps_study(
    df: pd.DataFrame, *, id_col: str = "ticker", leader_id: str = "BTC", coin_of: Callable[[str], str] | None = None,
) -> dict[str, Any]:
    global _PERPS_STUDY
    _PERPS_STUDY = build_study(df, id_col=id_col, leader_id=leader_id, coin_of=coin_of)
    return _PERPS_STUDY


def get_perps_study() -> dict[str, Any]:
    return _PERPS_STUDY


def refresh_alpaca_study(
    df: pd.DataFrame, *, id_col: str = "symbol", leader_id: str = "BTC", coin_of: Callable[[str], str] | None = None,
) -> dict[str, Any]:
    global _ALPACA_STUDY
    _ALPACA_STUDY = build_study(df, id_col=id_col, leader_id=leader_id, coin_of=coin_of)
    return _ALPACA_STUDY


def get_alpaca_study() -> dict[str, Any]:
    return _ALPACA_STUDY


# Kalshi 15-minute metals (gold/silver/copper/platinum/palladium) -- a
# THIRD, independent within-market study, added per explicit user
# direction ("use colateration studies with the commodity available").
# Genuinely simpler than perps'/Alpaca's own: no cross-service "remote"
# component at all -- kalshi_15m_metals_data.py's own data-collect job
# already runs in this SAME merged process as the strategy that consumes
# this, so there's no HF-mediated push/pull step to build (unlike
# Alpaca crypto's study, which perps pulls from a SEPARATE process via
# HF). Fed directly from kalshi_15m_metals_data.collect_dataset_rows()'s
# own output (already symbol-tagged with a real `ret_5m` column -- same
# shape build_study already expects) on ITS OWN data-collect cycle.
_METALS_STUDY: dict[str, Any] = {}

# Cross-asset input for the metals study above -- per explicit user
# direction ("use other asset for correlation and learn what other
# things correlate with GOLD/SILVER/COPPER[,] all possibilit[y]"), now
# that GOLD/SILVER/COPPER are this account's permanent, sole live-entry
# universe (see kalshi_15m_strategy.ACTIVE_ENTRY_COINS). The crypto
# coins are no longer traded directly, but they're still fully collected
# every cycle (kalshi_15m_data.py's own data-collect job) -- this just
# reuses that already-running feed as a correlation INPUT rather than
# retiring it. Cached here (set by the crypto data-collect job, read by
# the metals one) so feeding it into refresh_metals_study below costs a
# plain in-memory dict lookup, never a second collection pass -- same
# "no extra network call" discipline this module's own docstring holds
# every study to.
_LATEST_KALSHI_15M_CRYPTO_DF: pd.DataFrame = pd.DataFrame()


def set_latest_kalshi_15m_crypto_df(df: pd.DataFrame) -> None:
    """Called by app_kalshi.py's own kalshi_15m crypto data-collect job
    right after it collects -- best-effort, no return value. A missed/
    failed crypto collect cycle simply leaves the previous frame in place
    (one cycle stale, same posture as get_remote_alpaca_study), rather
    than clearing cross-asset correlation input to nothing."""
    global _LATEST_KALSHI_15M_CRYPTO_DF
    if df is not None and not df.empty:
        _LATEST_KALSHI_15M_CRYPTO_DF = df


def get_latest_kalshi_15m_crypto_df() -> pd.DataFrame:
    return _LATEST_KALSHI_15M_CRYPTO_DF


# 5-minute bucketing for the cross-asset join below -- metals collect
# every ~1 minute (kalshi_15m_metals_data.py's own gold-api.com poll),
# crypto every ~5 minutes (kalshi_15m_data.py's own data-collect job);
# their raw `ts` values essentially never coincide exactly, so a plain
# _pivot_returns join on exact ts would see almost no real overlap.
# Rounding both to the same 5-minute grid (matching RET_COL="ret_5m"'s
# own natural horizon anyway -- a trailing 5-minute return means
# basically the same thing sampled at :14 past the minute or :44 past
# it) gives real, matchable timestamps on both sides without touching
# either market's own underlying return values.
_CROSS_ASSET_TS_BUCKET_SECONDS = 300


def _bucket_ts_for_join(df: pd.DataFrame, *, bucket_seconds: int = _CROSS_ASSET_TS_BUCKET_SECONDS) -> pd.DataFrame:
    if df is None or df.empty or "ts" not in df.columns or "symbol" not in df.columns:
        return df if df is not None else pd.DataFrame()
    out = df.copy()
    out["ts"] = (out["ts"] // bucket_seconds) * bucket_seconds
    return out.sort_values("ts").drop_duplicates(subset=["symbol", "ts"], keep="last")


def refresh_metals_study(
    df: pd.DataFrame, *, id_col: str = "symbol", leader_id: str = "GOLD", coin_of: Callable[[str], str] | None = None,
    crypto_df: pd.DataFrame | None = None,
) -> dict[str, Any]:
    """crypto_df (optional) folds kalshi_15m's own crypto universe into
    THIS SAME study as extra candidate peers -- see
    _LATEST_KALSHI_15M_CRYPTO_DF's own comment. build_study/
    _peer_confirmation_bullishness/_leader_divergence_bullishness/
    _breadth_bullishness are all already fully generic over whatever ids
    a study's own `corr`/`cum_return` dicts happen to contain (no metals-
    specific logic anywhere in them) -- so a real BTC-GOLD or ETH-SILVER
    correlation, once the data shows one, gets picked up automatically
    with zero changes needed to any of those functions or to
    metals_correlation_bullishness's own callers below. Omitted/empty
    crypto_df: byte-for-byte the same metals-only study this function
    has always produced."""
    global _METALS_STUDY
    work = df
    if crypto_df is not None and not crypto_df.empty:
        work = pd.concat([_bucket_ts_for_join(df), _bucket_ts_for_join(crypto_df)], ignore_index=True)
    _METALS_STUDY = build_study(work, id_col=id_col, leader_id=leader_id, coin_of=coin_of)
    return _METALS_STUDY


def get_metals_study() -> dict[str, Any]:
    return _METALS_STUDY


def set_remote_alpaca_study(study: dict[str, Any] | None) -> None:
    """Called by perps_data.py after a successful pull from HF -- a failed
    pull (network hiccup, HF down, nothing pushed yet) simply leaves
    whatever was already cached in place rather than clearing it, so a
    single missed cycle degrades to "slightly stale" instead of "no
    signal"."""
    global _REMOTE_ALPACA_STUDY
    if study:
        _REMOTE_ALPACA_STUDY = study


def get_remote_alpaca_study() -> dict[str, Any]:
    """Empty (not stale-but-trusted) once the last successful pull is old
    enough that a whole collect cycle plus real slack has passed without a
    fresher one landing -- treated as "no signal" rather than trusting a
    potentially hours-stale reading forever if perps_data.py's own pull
    ever starts silently failing."""
    computed_at = _REMOTE_ALPACA_STUDY.get("computed_at")
    if not computed_at:
        return {}
    try:
        age_sec = (dt.datetime.now(dt.timezone.utc) - dt.datetime.fromisoformat(computed_at)).total_seconds()
    except Exception:
        return {}
    if age_sec > _REMOTE_ALPACA_STUDY_STALE_AFTER_SEC:
        return {}
    return _REMOTE_ALPACA_STUDY


def study_health(study: dict[str, Any]) -> dict[str, Any]:
    """Coverage snapshot for one study dict -- how many instruments it
    actually knows about vs. how many of those have enough peer-
    correlation / leader-divergence history to produce a real (non-"no
    data") reading. Exists so a real data-pipeline gap (most instruments
    failing to collect, or a too-thin history window) is visible on
    /api/status directly, instead of only showing up as a string of "no
    ... data" reasons buried in individual Threads posts that's otherwise
    only diagnosable by reading Render logs."""
    ids = study.get("ids") or []
    return {
        "computed_at": study.get("computed_at"),
        "num_ids": len(ids),
        "num_with_peer_data": len(study.get("corr") or {}),
        "num_with_divergence_data": len(study.get("divergence_z") or {}),
        "breadth": study.get("breadth"),
    }


# ── Individual studies (bullish-signed) ─────────────────────────────────

def _peer_confirmation_bullishness(study: dict[str, Any], target_id: str, *, min_corr: float = 0.4, max_peers: int = 5) -> tuple[float, str]:
    """Do this coin's most-correlated peers currently agree with a LONG
    (positive contribution) or a SHORT (negative)? contribution per peer =
    corr(target, peer) * sign(peer's own recent return) * how big that
    peer's move actually is (capped at 1%, so one outlier can't dominate) --
    a positively-correlated peer that's up supports a long here; a
    negatively-correlated peer that's up supports a short here; a peer
    barely moving contributes ~nothing either way."""
    peers = study.get("corr", {}).get(target_id) or {}
    cum_return = study.get("cum_return") or {}
    candidates = [(peer, corr) for peer, corr in peers.items() if abs(corr) >= min_corr and peer in cum_return]
    candidates.sort(key=lambda kv: abs(kv[1]), reverse=True)
    candidates = candidates[:max_peers]
    if not candidates:
        return 0.0, "no correlated peers with data"
    contributions = []
    for peer, corr in candidates:
        peer_return = cum_return[peer]
        magnitude = min(1.0, abs(peer_return) / 0.01)
        contributions.append(corr * (1.0 if peer_return >= 0 else -1.0) * magnitude)
    score = max(-1.0, min(1.0, float(np.mean(contributions))))
    names = ", ".join(f"{peer}{cum_return[peer]:+.2%}" for peer, _ in candidates)
    return score, f"{len(candidates)} correlated peer(s) ({names})"


def _leader_divergence_bullishness(study: dict[str, Any], target_id: str) -> tuple[float, str]:
    """Mean-reversion read against the market leader: a coin that has
    LAGGED what its own historical correlation to the leader predicts
    (negative divergence_z) is read as bullish (expected to catch up); one
    that has run AHEAD of that prediction (positive divergence_z) is read
    as bearish (expected to revert)."""
    z = (study.get("divergence_z") or {}).get(target_id)
    if z is None:
        return 0.0, "no leader-divergence data"
    score = max(-1.0, min(1.0, -z / 3.0))
    leader = study.get("leader_id") or "leader"
    if z < -0.25:
        direction = f"lagging {leader}"
    elif z > 0.25:
        direction = f"overextended vs {leader}"
    else:
        direction = f"in line with {leader}"
    return score, f"{direction} (z={z:+.2f})"


def _breadth_bullishness(study: dict[str, Any]) -> tuple[float, str]:
    """What fraction of the whole universe is currently trending up vs
    down, net -- a same-direction confirming market is a real reason to
    trust momentum-continuation a bit more; a broadly opposing market is a
    real reason to trust it a bit less, regardless of this one coin's own
    setup."""
    breadth = study.get("breadth")
    if breadth is None:
        return 0.0, "no breadth data"
    skew = "up" if breadth > 0.1 else "down" if breadth < -0.1 else "flat"
    return float(breadth), f"breadth {breadth:+.2f} ({skew}-skewed)"


# Reference scale each timeframe field is divided by before clamping to
# [-1, 1] -- roughly "what counts as a strong move at THIS horizon", scaled
# up for longer horizons the same way realistic price dispersion grows with
# time (a 4h move needs to be bigger than a 5m move to mean the same thing).
# Deliberately simple, round numbers -- NOT independently backtested/fit
# per timeframe (same disclosed-but-reasoned posture as every other
# not-yet-backtested threshold in this codebase, see e.g.
# perps_strategy.py's own PROMISING_MOMENTUM_PCT comment).
_TIMEFRAME_REFERENCE_SCALES: dict[str, float] = {
    "ret_5m": 0.003, "ret_15m": 0.005, "ret_30m": 0.008,
    "trend_1h": 0.01, "trend_2h": 0.014, "trend_3h": 0.017, "trend_4h": 0.02,
    "macd_hist_pct": 0.001,
}


def multi_timeframe_bullishness(row: dict[str, Any] | None) -> tuple[float, str]:
    """How many of ONE coin's own timeframes currently agree on direction --
    5m/15m/30m return, 1h/2h/3h/4h trend, MACD histogram, and RSI (treated
    as a momentum-regime read here, above/below the 0.5 midline, not the
    overbought/oversold contrarian read -- consistent with every OTHER
    signal in this composite being momentum/continuation-flavored, not
    mean-reversion). Each field is clamped to [-1, 1] via its own
    _TIMEFRAME_REFERENCE_SCALES entry so one large outlier can't dominate,
    then equal-weighted -- a plain "how many independent timeframes/
    indicators agree, weighted by conviction" reading, deliberately not a
    fitted/backtested weighting scheme (see that dict's own comment).
    `row` is whatever latest_feature_row already returned for this
    ticker/symbol at decision time -- no extra data collection of its own."""
    if not row:
        return 0.0, "no multi-timeframe data"
    contributions: list[float] = []
    for field, scale in _TIMEFRAME_REFERENCE_SCALES.items():
        value = row.get(field)
        if value is None or (isinstance(value, float) and value != value):  # NaN check without importing math here
            continue
        contributions.append(max(-1.0, min(1.0, float(value) / scale)))
    rsi = row.get("rsi_14")
    if rsi is not None and not (isinstance(rsi, float) and rsi != rsi):
        contributions.append(max(-1.0, min(1.0, (float(rsi) - 0.5) / 0.2)))
    if not contributions:
        return 0.0, "no multi-timeframe data"
    score = max(-1.0, min(1.0, sum(contributions) / len(contributions)))
    n_bullish = sum(1 for c in contributions if c > 0.1)
    n_bearish = sum(1 for c in contributions if c < -0.1)
    return score, f"{n_bullish}/{len(contributions)} timeframes bullish, {n_bearish}/{len(contributions)} bearish"


# ── Composite readings used directly by the strategies ──────────────────
# Weights: Alpaca's remote study gets the largest combined weight among the
# CORRELATION components in the PERPS composite specifically per explicit
# user direction -- perps can act on both the long AND short half of what
# that broader, unlevered universe reveals, so it's the primary correlation
# signal there; perps' own narrower ~13-instrument web is corroborating
# context, not the main driver. multi_timeframe is single-instrument, not
# cross-market, so it's weighted independently of that split -- per
# explicit user direction that multi-timeframe technical studies matter
# "as well", not as an afterthought. The ALPACA composite only ever draws
# on its own universe for the correlation half -- there's nothing
# perps-specific worth sharing back the other way (see this module's own
# docstring).
PERPS_PEER_WEIGHT = 0.20
PERPS_REMOTE_PEER_WEIGHT = 0.20
PERPS_REMOTE_DIVERGENCE_WEIGHT = 0.25
PERPS_REMOTE_BREADTH_WEIGHT = 0.10
PERPS_MULTI_TIMEFRAME_WEIGHT = 0.25

ALPACA_PEER_WEIGHT = 0.30
ALPACA_DIVERGENCE_WEIGHT = 0.20
ALPACA_BREADTH_WEIGHT = 0.20
ALPACA_MULTI_TIMEFRAME_WEIGHT = 0.30

# Real feedback: a composite's posted "Why:" reason used to unconditionally
# join every component's text, "no correlated peers with data"/"no
# leader-divergence data" included -- for a coin genuinely too new/thin to
# correlate against anything yet (or simply not part of Alpaca's separate
# universe at all), most of a 5-part reason could read as a wall of "no
# ... data" placeholders with barely any real signal in it, exactly the
# "a lot of data points are not coming in" clutter reported live. These
# are the exact literal strings each _*_bullishness "no data" branch
# returns (all defined a few lines above) -- filtering them out of the
# JOINED REASON TEXT ONLY changes what gets displayed; the weighted SCORE
# math above is completely unaffected (a "no data" component already
# contributes a neutral 0.0 to the score either way).
_NO_DATA_REASONS = {
    "no correlated peers with data", "no leader-divergence data",
    "no breadth data", "no multi-timeframe data",
}


def _format_reason(components: dict[str, tuple[float, str]]) -> str:
    meaningful = [f"{name}: {text}" for name, (_, text) in components.items() if text not in _NO_DATA_REASONS]
    return "; ".join(meaningful) if meaningful else "no chart-study signal available yet"


def perps_correlation_bullishness(coin: str, row: dict[str, Any] | None = None) -> dict[str, Any]:
    """Composite chart-study reading for one perps coin (already a plain
    coin code, e.g. via coin_for_ticker). Bullish-signed: positive favors a
    LONG / disfavors a SHORT on this coin, negative the reverse -- callers
    flip sign for the short side themselves (see decide_entry_technical's
    own side handling for the same convention elsewhere in this codebase).
    `row` (optional -- the same latest_feature_row dict evaluate_candidate/
    manage_open_positions already have on hand) feeds the multi-timeframe
    component; omitted, that component simply reads neutral (0.0), same as
    any other component with no data available yet."""
    perps_study = get_perps_study()
    remote_study = get_remote_alpaca_study()
    components = {
        "perps_peers": _peer_confirmation_bullishness(perps_study, coin),
        "alpaca_peers": _peer_confirmation_bullishness(remote_study, coin),
        "alpaca_divergence": _leader_divergence_bullishness(remote_study, coin),
        "alpaca_breadth": _breadth_bullishness(remote_study),
        "multi_timeframe": multi_timeframe_bullishness(row),
    }
    score = (
        PERPS_PEER_WEIGHT * components["perps_peers"][0]
        + PERPS_REMOTE_PEER_WEIGHT * components["alpaca_peers"][0]
        + PERPS_REMOTE_DIVERGENCE_WEIGHT * components["alpaca_divergence"][0]
        + PERPS_REMOTE_BREADTH_WEIGHT * components["alpaca_breadth"][0]
        + PERPS_MULTI_TIMEFRAME_WEIGHT * components["multi_timeframe"][0]
    )
    reason = _format_reason(components)
    return {"score": round(float(score), 4), "reason": reason, "components": {k: v[0] for k, v in components.items()}}


def alpaca_correlation_bullishness(coin: str, row: dict[str, Any] | None = None) -> dict[str, Any]:
    """Composite chart-study reading for one Alpaca crypto pair (already a
    plain coin code, e.g. via symbol_to_coin), from its own broader
    universe only. `row` (optional) feeds the multi-timeframe component,
    same as perps_correlation_bullishness's own."""
    study = get_alpaca_study()
    components = {
        "peers": _peer_confirmation_bullishness(study, coin),
        "divergence": _leader_divergence_bullishness(study, coin),
        "breadth": _breadth_bullishness(study),
        "multi_timeframe": multi_timeframe_bullishness(row),
    }
    score = (
        ALPACA_PEER_WEIGHT * components["peers"][0]
        + ALPACA_DIVERGENCE_WEIGHT * components["divergence"][0]
        + ALPACA_BREADTH_WEIGHT * components["breadth"][0]
        + ALPACA_MULTI_TIMEFRAME_WEIGHT * components["multi_timeframe"][0]
    )
    reason = _format_reason(components)
    return {"score": round(float(score), 4), "reason": reason, "components": {k: v[0] for k, v in components.items()}}


# Same shape as ALPACA's own weights above (a single self-contained
# study, no "remote" cross-service component) -- metals' own universe is
# even smaller (5 commodities) than Alpaca's 36-pair crypto one, so peer
# confirmation is weighted a little lighter here and multi_timeframe a
# little heavier, favoring the signal this specific coin's OWN recent
# technicals already provide over a thinner cross-commodity peer read.
METALS_PEER_WEIGHT = 0.20
METALS_DIVERGENCE_WEIGHT = 0.20
METALS_BREADTH_WEIGHT = 0.15
METALS_MULTI_TIMEFRAME_WEIGHT = 0.45


def metals_correlation_bullishness(metal: str, row: dict[str, Any] | None = None) -> dict[str, Any]:
    """Composite chart-study reading for one Kalshi 15-minute metal (GOLD/
    SILVER/COPPER/PLATINUM/PALLADIUM), from this market's own 5-commodity
    universe only -- see refresh_metals_study's own comment on why this
    needs no "remote" component at all. `row` (optional) feeds the
    multi-timeframe component, same as perps_correlation_bullishness's
    own."""
    study = get_metals_study()
    components = {
        "peers": _peer_confirmation_bullishness(study, metal),
        "divergence": _leader_divergence_bullishness(study, metal),
        "breadth": _breadth_bullishness(study),
        "multi_timeframe": multi_timeframe_bullishness(row),
    }
    score = (
        METALS_PEER_WEIGHT * components["peers"][0]
        + METALS_DIVERGENCE_WEIGHT * components["divergence"][0]
        + METALS_BREADTH_WEIGHT * components["breadth"][0]
        + METALS_MULTI_TIMEFRAME_WEIGHT * components["multi_timeframe"][0]
    )
    reason = _format_reason(components)
    return {"score": round(float(score), 4), "reason": reason, "components": {k: v[0] for k, v in components.items()}}
