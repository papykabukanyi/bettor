"""Strategy for Kalshi's 15-minute event-contract markets (KXBTC15M etc.,
see kalshi_15m.py's own module docstring for the product itself).

DRY RUN BY DEFAULT (same shared gate as perps_strategy.py: real orders
require BOTH KALSHI_15M_LIVE_TRADING_ENABLED=1 in the environment AND the
caller not passing dry_run=True), but LIVE on this account as of the
verification below.

This module's own order-placement path (kalshi_15m.create_order, POSTing
to /portfolio/events/orders) was held to a stricter bar than every other
market here until it could be confirmed against a real authenticated
call, not just cross-checked against docs -- that verification has since
happened: a real POST to this account's own /portfolio/events/orders
came back with a genuine, well-formed Kalshi API response (an
insufficient_shard_balance rejection -- a real account-side collateral-
allocation fact about exchange sharding, not a payload/mechanics
problem; see kalshi_15m.get_balance_by_shard's own docstring). The
payload itself is confirmed correct. LIVE_TRADING_ENABLED is now set on
the live Space, and this account holds real collateral on exchange shard
2 (Crypto and Commodities, the shard these markets actually settle
orders against).

Everything entry-decision-relevant was already real and verifiable even
before that: market discovery (GET /series, /markets) and settlement
checking (a closed market's own public `result` field) are both
UNAUTHENTICATED, public endpoints -- so dry-run mode here was always a
real, fully-exercisable simulation of the whole entry -> hold -> settle
lifecycle, not a stub, and stays exactly as real now that live orders can
actually fill.

Simpler than perps_strategy.py in one real, permanent way: no leverage
(these are plain $1-notional binary contracts, see kalshi_15m.py's own
docstring) and no daily-loss-cap/technical-scalper-filter machinery this
product's own economics don't need. The model's own probability_up IS
the core signal, matching what a 15-minute binary contract actually
needs: one calibrated probability, not perps' multi-signal-agreement
gate.

That said, this module is NOT "hold to settlement, no other options"
any more -- an earlier version of this docstring said exactly that, on
the reasoning that no real evidence yet justified an early-exit lever.
Per explicit, repeated user direction ("the bot need to understand when
it's not going to make it but the position is profitable so it uses
stop loss to at least win a bit", later "i need a multi time frame
study... to help also determine staying or closing the winning
position"), this market now has real, if still evidence-gated, decision
layers on TOP of the base probability_up signal:

  - A chart-study confidence layer (USE_CORRELATION_STUDY) -- crypto's
    own study reuses perps' already-running correlation web; metals get
    an independent one of their own (see crypto_correlation.py's
    refresh_metals_study). Both feed the SAME multi-timeframe-aware
    (5m/15m/30m return, 1h-4h trend, MACD, RSI) confidence nudge.
  - A meta-labeling trust gate (USE_META_MODEL, crypto only) --
    kalshi_15m_meta_model.py, a second model judging whether to trust
    the primary model's call in the CURRENT regime, not a second
    opinion on direction.
  - Conviction sizing (USE_CONVICTION_SIZING) and a win-streak size
    increase (USE_WIN_STREAK_SIZING) -- both scale a position UP on a
    stronger signal, both off by default pending real evidence (a
    ~52-54% real walk-forward accuracy means a short streak is very
    likely still noise -- see kalshi_15m_backtest.py's own results).
  - A per-symbol loss-streak throttle (always on -- see
    compute_loss_streak_size_multiplier's own comment for why this one
    doesn't need the same evidence gate: it only ever REDUCES risk).
  - Early exit (USE_EARLY_EXIT, manage_open_positions) -- re-runs the
    SAME model + chart-study reassessment against CURRENT data on every
    still-open real position; only acts when that reassessment has
    genuinely flipped away from the held side, closing to lock in a
    profit or cut a loss rather than always riding to settlement.
  - A full indicator/timeframe snapshot recorded on every position/trade
    (entry_feature_snapshot) -- what actually led to a win or a loss is
    preserved for study, not just the final probability/confidence
    numbers.
  - A regularly-scheduled walk-forward backtest AND forward test (see
    KALSHI_15M_BACKTEST_HOUR_ET's own comment in app_kalshi.py), not a
    one-off manual check -- reacts to a confirmed losing result with an
    immediate extra retrain of both models.
  - Graduated concurrency (always on -- compute_graduated_max_concurrent_positions)
    -- per explicit user direction ("the balance is not growing because
    it's opening too many entries and most of them are losing... let's
    focus on 1 after another win to grow the balance, then increase to
    2, so on and forth"): the account starts allowed only ONE concurrent
    position, earning one more slot per real win (capped at
    MAX_CONCURRENT_POSITIONS, the original flat ceiling), and dropping
    straight back to one the moment a real loss breaks that streak.
  - A per-coin trust gate (always on -- coin_is_trusted) -- per explicit
    user direction ("the bot need to know by now... what it's best on"):
    once a coin has enough real trade history to trust the read, a
    clearly poor real track record (both a low win rate AND a negative
    average P&L) pauses NEW entries on that specific coin entirely,
    genuinely different from the loss-streak throttle's short-window
    size-shrink above -- this looks at the coin's WHOLE real history, and
    re-includes it the moment its own numbers improve.

Every one of the risk-INCREASING levers above (correlation study,
meta-model, conviction sizing, win-streak sizing, early exit) stays off
by default and is evidence-gated by kalshi_15m_trade_analysis.py's own
recommend_*_trial functions, applied via apply_*_override -- exactly the
same "prove it out on real trade history first" discipline every other
market in this codebase already holds itself to. This module's own
sample-reweighting during training (kalshi_15m_model._trade_outcome_sample_weight)
is the silent, always-on complement: every real win/loss already nudges
how much the NEXT day's retrain trusts a similar-looking row, independent
of whether any of the live-tunable levers above are switched on.
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import os
import threading
import uuid
from pathlib import Path
from typing import Any

from data import crypto_correlation, kalshi_15m, kalshi_15m_meta_model, kalshi_15m_metals_model, kalshi_15m_model
from server_common import DATA_DIR

logger = logging.getLogger(__name__)

# Merged coin/metal -> series-ticker mapping across BOTH universes this
# module trades -- crypto (kalshi_15m_model, perps' own candle feed as a
# proxy) and metals (kalshi_15m_metals_model, its own from-scratch price
# history -- see kalshi_15m_metals_data.py's own module docstring for why
# these need a genuinely different data/model pair, not just a longer
# coin list on the existing one). Kept as ONE combined state file/
# dashboard/trade_log rather than a parallel strategy module: the
# ENTRY/SETTLEMENT decision logic below (market lookup, confidence
# gating, order placement, settlement checking) is already fully asset-
# agnostic -- only the MODEL CALL differs, dispatched via
# _predict_direction below.
ASSET_SERIES: dict[str, str] = {**kalshi_15m.KNOWN_15M_SERIES, **kalshi_15m.KNOWN_15M_METALS_SERIES}


def _predict_direction(coin: str) -> dict[str, Any]:
    if coin in kalshi_15m.KNOWN_15M_METALS_SERIES:
        return kalshi_15m_metals_model.predict_direction(coin)
    return kalshi_15m_model.predict_direction(coin)


def _env_flag(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name, "1" if default else "0") or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)) or str(default))
    except Exception:
        return float(default)


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)) or str(default))
    except Exception:
        return int(default)


# See this module's own docstring for why this is held to an even
# stricter bar than the shared "dry-run unless explicitly flipped"
# convention -- unverified live order-placement mechanics on a real
# account.
LIVE_TRADING_ENABLED = _env_flag("KALSHI_15M_LIVE_TRADING_ENABLED", default=False)

# Calibrated on zero real trade history yet -- an honest starting guess
# (matching this codebase's OWN established floor for a brand-new model,
# see e.g. alpaca_options_strategy.py's original MODEL_CONFIDENCE_MIN
# comment), not evidence-picked. Revisit with a real backtest/walk-forward
# once kalshi_15m_data.py has accumulated enough real history to run one
# -- same discipline as every other threshold in this codebase.
MODEL_CONFIDENCE_MIN = _env_float("KALSHI_15M_MODEL_CONFIDENCE_MIN", 0.58)

POSITION_SIZE_PCT = _env_float("KALSHI_15M_POSITION_SIZE_PCT", 0.05)
MAX_CONCURRENT_POSITIONS = _env_int("KALSHI_15M_MAX_CONCURRENT_POSITIONS", 5)

# Graduated concurrency -- per explicit user direction: "the balance is
# not growing because it['s] opening too many entr[ies] and most of them
# are losing... since the balance is low let['s] focus on 1 [position]
# after another win to grow the balance[,] [then] increase to 2 at a
# time[,] so on and forth." MAX_CONCURRENT_POSITIONS above stays the hard
# CEILING this can ever grow to; this is the actual STARTING point and
# growth rule while the account is small/recovering. Always on (a pure
# risk-REDUCER while unproven, same posture as the loss-streak throttle)
# -- can only ever narrow how many bets are open at once relative to the
# flat ceiling, never widen it.
GRADUATED_CONCURRENCY_ENABLED = _env_flag("KALSHI_15M_GRADUATED_CONCURRENCY_ENABLED", default=True)
GRADUATED_CONCURRENCY_START_SLOTS = _env_int("KALSHI_15M_GRADUATED_CONCURRENCY_START_SLOTS", 1)
GRADUATED_CONCURRENCY_WINS_PER_SLOT = _env_int("KALSHI_15M_GRADUATED_CONCURRENCY_WINS_PER_SLOT", 1)


def compute_graduated_max_concurrent_positions(trade_log: list[dict[str, Any]] | None) -> int:
    """How many concurrent positions the account is currently allowed,
    given its own REAL trade history -- starts at
    GRADUATED_CONCURRENCY_START_SLOTS, earns +1 slot per
    GRADUATED_CONCURRENCY_WINS_PER_SLOT consecutive real ACCOUNT-WIDE
    wins (account-wide, not per-coin -- this is about whether the
    account overall has earned more concurrent risk right now, a
    separate question from which SPECIFIC coin any one slot goes to),
    capped at MAX_CONCURRENT_POSITIONS -- and drops straight back to the
    start the moment a real loss breaks that streak, same "reset on the
    opposite outcome, no manual intervention" discipline
    compute_loss_streak_size_multiplier already uses. Pure function --
    reads the tail of trade_log directly rather than a separately-
    persisted counter, so it can never drift out of sync with what the
    account actually did."""
    if not GRADUATED_CONCURRENCY_ENABLED:
        return MAX_CONCURRENT_POSITIONS
    real_trades = [t for t in (trade_log or []) if not t.get("dry_run")]
    if not real_trades or float(real_trades[-1].get("realized_pnl_usd") or 0.0) <= 0:
        return GRADUATED_CONCURRENCY_START_SLOTS

    streak = 0
    for t in reversed(real_trades):
        if float(t.get("realized_pnl_usd") or 0.0) > 0:
            streak += 1
        else:
            break

    extra_slots = streak // GRADUATED_CONCURRENCY_WINS_PER_SLOT
    return min(MAX_CONCURRENT_POSITIONS, GRADUATED_CONCURRENCY_START_SLOTS + extra_slots)


# Per-coin trust gate -- per explicit user direction: "the bot need to
# know by now after analyzing[,] he need to know the patterns and what
# its best on." Genuinely different from the loss-streak throttle above:
# that reacts to a SHORT recent run (3 trades) by shrinking size; this
# looks at a coin's ENTIRE real track record (a longer, steadier sample)
# and, once there's enough of it to trust the read, PAUSES new entries
# on that coin entirely rather than merely sizing them down -- "what
# it's best on" means some coins may simply not be worth trading at all
# with this model, not just worth trading smaller. Always on (a pure
# risk-REDUCER) -- narrows the tradable universe toward what has
# actually worked, never expands it, and re-includes a coin the moment
# its own real numbers improve (no manual reset, no permanent ban).
COIN_TRUST_MIN_TRADES = _env_int("KALSHI_15M_COIN_TRUST_MIN_TRADES", 8)
COIN_TRUST_MIN_WIN_RATE = _env_float("KALSHI_15M_COIN_TRUST_MIN_WIN_RATE", 0.35)


def coin_is_trusted(coin: str, trade_log: list[dict[str, Any]] | None) -> dict[str, Any]:
    """{"trusted": True} until this coin's own real trade history is BOTH
    long enough (COIN_TRUST_MIN_TRADES -- avoids overreacting to a small,
    unlucky sample) and clearly bad (win rate below
    COIN_TRUST_MIN_WIN_RATE AND a negative average real P&L -- BOTH, not
    just one metric skewed by a single large loss, same discipline every
    recommend_*_trial comparison in kalshi_15m_trade_analysis.py already
    holds itself to)."""
    coin_trades = [t for t in (trade_log or []) if t.get("coin") == coin and not t.get("dry_run")]
    if len(coin_trades) < COIN_TRUST_MIN_TRADES:
        return {"trusted": True, "reason": "insufficient_history", "trades": len(coin_trades)}
    wins = sum(1 for t in coin_trades if float(t.get("realized_pnl_usd") or 0.0) > 0)
    win_rate = wins / len(coin_trades)
    avg_pnl = sum(float(t.get("realized_pnl_usd") or 0.0) for t in coin_trades) / len(coin_trades)
    if win_rate < COIN_TRUST_MIN_WIN_RATE and avg_pnl < 0:
        return {
            "trusted": False, "reason": "poor_real_track_record",
            "trades": len(coin_trades), "win_rate": round(win_rate, 4), "avg_pnl_usd": round(avg_pnl, 6),
        }
    return {
        "trusted": True, "reason": "track_record_ok",
        "trades": len(coin_trades), "win_rate": round(win_rate, 4), "avg_pnl_usd": round(avg_pnl, 6),
    }

# Real, deliberate guard: entering with only a few seconds left before a
# window closes is paying the spread for what's functionally a coin flip
# (no time left for the model's own predicted direction to actually play
# out) -- Kalshi's own quadratic fee structure (see kalshi_15m.py's own
# docstring) makes this worse here than perps' linear one. A third of the
# window (5 of 15 minutes) still open is the floor for a real entry.
MIN_SECONDS_TO_CLOSE_FOR_ENTRY = _env_int("KALSHI_15M_MIN_SECONDS_TO_CLOSE_FOR_ENTRY", 300)

# Chart-study confidence layer -- see crypto_correlation.py's own module
# docstring and perps_strategy.py's identical USE_CORRELATION_STUDY
# comment for the full rationale. Off by default there too (an evidence-
# gated EXPERIMENT, not a proven win, even on perps -- see
# recommend_correlation_study_weight's own docstring) -- same posture
# here: computed and attached unconditionally for observability, only
# actually nudges the entry gate once real trade history earns it via
# apply_correlation_study_override.
#
# Covers BOTH universes this market trades, each from its OWN separate
# study (one shared flag/adjustment -- not split per-universe -- since
# both feed the exact same effective_confidence_min nudge below; splitting
# would only matter if the two ever needed independently different
# on/off states or weights, which nothing here has asked for yet):
#   - Crypto (all of kalshi_15m.KNOWN_15M_SERIES -- BTC/ETH/SOL/XRP/DOGE/
#     BCH/NEAR/HYPE/ZEC): reuses perps' own already-running in-process
#     correlation study (get_perps_study(), refreshed by perps_data.py's
#     own data-collect job, already running in this same merged process)
#     rather than building a new one from scratch -- this market's crypto
#     model already proxies off perps' own data pipeline (see
#     kalshi_15m_data.latest_feature_row's own docstring), and perps' own
#     ~13-instrument watchlist already covers all 9 of these coins.
#   - Metals (all of kalshi_15m.KNOWN_15M_METALS_SERIES -- GOLD/SILVER/
#     COPPER/PLATINUM/PALLADIUM): its OWN, independent 5-commodity study
#     (see crypto_correlation.refresh_metals_study's own comment) --
#     genuinely different data/process ownership from crypto's, but the
#     SAME confidence-nudge mechanism once computed.
USE_CORRELATION_STUDY = _env_flag("KALSHI_15M_USE_CORRELATION_STUDY", default=False)
CORRELATION_CONFIDENCE_MAX_ADJUSTMENT = _env_float("KALSHI_15M_CORRELATION_CONFIDENCE_MAX_ADJUSTMENT", 0.06)

# Meta-labeling trust gate -- see kalshi_15m_meta_model.py's own module
# docstring for the full design. Off by default, same as perps'
# identical PERPS_USE_META_MODEL: a static flag pending real backtest
# validation via kalshi_15m_backtest.py, not evidence-tuned automatically
# the way MODEL_CONFIDENCE_MIN/USE_CORRELATION_STUDY are above. Crypto
# only -- no metals meta-model exists (see kalshi_15m_meta_model.py's own
# docstring on why).
USE_META_MODEL = _env_flag("KALSHI_15M_USE_META_MODEL", default=False)
META_MODEL_TRUST_MIN = _env_float("KALSHI_15M_META_MODEL_TRUST_MIN", 0.5)

# Conviction sizing -- the ONE of perps' 3 position-management-trial
# features (scale-in/partial-exit/conviction-sizing) that actually maps
# onto this market's product: a position here is entered ONCE, atomically,
# and held to settlement -- there is no open-position lifecycle to scale
# INTO (USE_SCALE_IN) or exit PART of early (USE_PARTIAL_EXIT), so neither
# of those two has a kalshi_15m equivalent. Conviction sizing needs no
# early-exit lever at all: it only resizes the ONE entry itself, bigger for
# a higher-conviction signal (how far above its own effective confidence
# floor -- MODEL_CONFIDENCE_MIN, possibly nudged by the correlation study
# -- this candidate's confidence cleared), smaller for a just-qualifying
# one. Same POSITION_SIZE_PCT that already sizes every entry, plus/minus
# this multiplier -- capital shifts toward the strongest signals instead of
# spreading identically across every candidate that merely cleared the
# bar. Default OFF, same "prove it out on real trade history first"
# posture as every other risk-shape flag here.
USE_CONVICTION_SIZING = _env_flag("KALSHI_15M_USE_CONVICTION_SIZING", default=False)
CONVICTION_SIZE_MIN_MULTIPLIER = _env_float("KALSHI_15M_CONVICTION_SIZE_MIN_MULTIPLIER", 0.7)
CONVICTION_SIZE_MAX_MULTIPLIER = _env_float("KALSHI_15M_CONVICTION_SIZE_MAX_MULTIPLIER", 1.5)


def compute_conviction_size_multiplier(entry_confidence: float | None, effective_confidence_min: float | None) -> float:
    """The size_multiplier scan_and_enter's own contracts sizing should use
    for a conviction-scaled entry -- see USE_CONVICTION_SIZING's own
    comment. Returns 1.0 (no change) if either input is missing, or
    effective_confidence_min is >= 1.0 (division-by-zero guard, not a real
    value MODEL_CONFIDENCE_MIN/the correlation nudge would ever produce,
    but defensive regardless). Identical formula to
    perps_strategy.compute_conviction_size_multiplier's own."""
    if entry_confidence is None or effective_confidence_min is None or effective_confidence_min >= 1.0:
        return 1.0
    conviction = max(0.0, min(1.0, (entry_confidence - effective_confidence_min) / (1.0 - effective_confidence_min)))
    return CONVICTION_SIZE_MIN_MULTIPLIER + (CONVICTION_SIZE_MAX_MULTIPLIER - CONVICTION_SIZE_MIN_MULTIPLIER) * conviction


# Per-symbol loss-streak throttle -- per explicit user direction: "if
# it's keep losing a certain symbol[,] retain[,] redo and reduce size
# until it start[s] getting strikes [wins]." Real, deliberate difference
# from every other sizing lever here (conviction sizing, correlation
# study, meta-model): this one ONLY EVER SHRINKS a position, never grows
# it, so it carries no new downside from being on by default -- unlike
# those, which stay off pending real trade-history evidence, there is no
# "unproven experiment" risk to gate here. Keyed on COIN specifically
# (not the whole account) -- a losing streak on one coin says nothing
# about whether another coin's own signal is trustworthy right now.
LOSS_STREAK_THROTTLE_LENGTH = _env_int("KALSHI_15M_LOSS_STREAK_THROTTLE_LENGTH", 3)
LOSS_STREAK_SIZE_MULTIPLIER = _env_float("KALSHI_15M_LOSS_STREAK_SIZE_MULTIPLIER", 0.5)


def compute_loss_streak_size_multiplier(coin: str, trade_log: list[dict[str, Any]] | None) -> float:
    """1.0 (no change) unless this coin's own most recent
    LOSS_STREAK_THROTTLE_LENGTH REAL (non-dry-run) closed trades are ALL
    losses, in which case LOSS_STREAK_SIZE_MULTIPLIER (a real, shrunk
    slice) -- resets back to 1.0 the moment this coin produces even one
    real win, not on a timer or a manual reset. Pure function -- no
    state, no side effects; scan_and_enter passes it the SAME trade_log
    slice it already has on hand."""
    trade_log = trade_log or []
    coin_trades = [t for t in trade_log if t.get("coin") == coin and not t.get("dry_run")]
    if len(coin_trades) < LOSS_STREAK_THROTTLE_LENGTH:
        return 1.0
    recent = coin_trades[-LOSS_STREAK_THROTTLE_LENGTH:]
    all_losses = all(float(t.get("realized_pnl_usd") or 0.0) <= 0 for t in recent)
    return LOSS_STREAK_SIZE_MULTIPLIER if all_losses else 1.0


# Per-symbol WIN-streak size increase -- the mirror image of the throttle
# above, per explicit user direction: "position increase only when its
# consistent win after win then we increase the position sizes." Real,
# deliberate difference from the loss-streak throttle: this one GROWS
# exposure, so unlike that pure risk-reducer, it needs the SAME "prove it
# out on real trade history first" evidence-gated posture as every other
# risk-INCREASING lever here (conviction sizing, correlation study,
# meta-model) -- a 3-trade winning streak on a model with only ~52-54%
# real walk-forward accuracy (see this module's own backtest results) is
# very likely still just noise, not a genuine change in that coin's own
# edge; chasing it with bigger size before real evidence says otherwise
# is a real, disclosed risk this default protects against. Off by
# default (USE_WIN_STREAK_SIZING).
USE_WIN_STREAK_SIZING = _env_flag("KALSHI_15M_USE_WIN_STREAK_SIZING", default=False)
WIN_STREAK_LENGTH = _env_int("KALSHI_15M_WIN_STREAK_LENGTH", 3)
WIN_STREAK_SIZE_MULTIPLIER = _env_float("KALSHI_15M_WIN_STREAK_SIZE_MULTIPLIER", 1.5)


def compute_win_streak_size_multiplier(coin: str, trade_log: list[dict[str, Any]] | None) -> float:
    """1.0 (no change) unless this coin's own most recent WIN_STREAK_LENGTH
    REAL (non-dry-run) closed trades are ALL wins, in which case
    WIN_STREAK_SIZE_MULTIPLIER (a real, grown slice) -- resets back to
    1.0 the moment this coin produces even one real loss. Mirror image of
    compute_loss_streak_size_multiplier's own logic; see
    USE_WIN_STREAK_SIZING's own comment for why this one stays off by
    default while that one doesn't."""
    trade_log = trade_log or []
    coin_trades = [t for t in trade_log if t.get("coin") == coin and not t.get("dry_run")]
    if len(coin_trades) < WIN_STREAK_LENGTH:
        return 1.0
    recent = coin_trades[-WIN_STREAK_LENGTH:]
    all_wins = all(float(t.get("realized_pnl_usd") or 0.0) > 0 for t in recent)
    return WIN_STREAK_SIZE_MULTIPLIER if all_wins else 1.0


STATE_FILE = Path(os.getenv("KALSHI_15M_STATE_FILE", str(DATA_DIR / "kalshi_15m_state.json")))
_STATE_LOCK = threading.Lock()

HF_API_KEY = os.getenv("HF_API_KEY", "")
# Real, live, confirmed bug this closes: unlike perps_strategy.py/
# alpaca_strategy.py (and every other market here), this module had NO
# HF backup for its own state at all -- purely local disk. Confirmed
# live: a routine restart (triggered to disable live trading the moment
# the order side/price bug above was found) wiped 118 real trades and a
# real day's P&L total instantly, with no way to recover any of it.
# Reuses perps' own already-private HF_MODEL_REPO rather than creating a
# dedicated repo for one more market's durable state -- same "durable
# trading state, must stay private" bucket, just a different filename.
HF_DURABLE_STATE_REPO = os.getenv("HF_MODEL_REPO", "papylove/kalshi-perps-model")
_DURABLE_STATE_HF_FILENAME = "kalshi_15m_durable_state.json"
_DURABLE_STATE_HF_TIMEOUT_SEC = int(os.getenv("KALSHI_15M_DURABLE_STATE_HF_TIMEOUT_SEC", "10") or "10")


def _durable_state_slice(state: dict[str, Any]) -> dict[str, Any]:
    """Everything worth surviving a restart. Unlike perps' own version of
    this function, `positions` IS included here (not reconstructable from
    Kalshi's own account the way perps' margin positions are -- no
    equivalent reconciliation exists for this market yet) -- losing track
    of a real open position for up to 15 minutes is worse than one extra
    small field in this payload."""
    return {
        "positions": state.get("positions") or [],
        "trade_log": state.get("trade_log") or [],
        "realized_pnl_by_date": state.get("realized_pnl_by_date") or {},
        # "tuning" (the evidence-gated confidence-threshold override -- see
        # apply_confidence_threshold_override below) MUST be included here
        # -- a real, confirmed bug found in perps_strategy.py's own
        # identical slice (fixed there, never repeated here) once left it
        # out, silently resetting any confidence threshold actually
        # LEARNED from real trade history back to the hardcoded default on
        # every single deploy.
        "tuning": state.get("tuning") or {},
    }


def _push_durable_state_to_hf(state: dict[str, Any]) -> None:
    if not HF_API_KEY:
        return

    def _upload() -> None:
        import tempfile
        from huggingface_hub import HfApi
        api = HfApi(token=HF_API_KEY)
        payload = json.dumps(_durable_state_slice(state), indent=2)
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
            tmp.write(payload)
            tmp_path = tmp.name
        try:
            api.upload_file(
                path_or_fileobj=tmp_path, path_in_repo=_DURABLE_STATE_HF_FILENAME,
                repo_id=HF_DURABLE_STATE_REPO, repo_type="model", commit_message="update kalshi 15m durable state",
            )
        finally:
            os.unlink(tmp_path)

    try:
        # Same real-incident-driven discipline as perps_strategy's own
        # identical push: an unbounded huggingface_hub call can hang
        # indefinitely on an internal lock and, while held under
        # _STATE_LOCK (every push_durable=True caller below), freeze this
        # entire shared --workers 1 process until gunicorn's timeout
        # SIGKILLs it.
        from server_common import call_with_hard_timeout
        call_with_hard_timeout(_upload, timeout_sec=_DURABLE_STATE_HF_TIMEOUT_SEC)
    except Exception as exc:
        logger.warning("[kalshi_15m_strategy] durable state push to HF failed: %s", exc)


def _pull_durable_state_from_hf() -> dict[str, Any] | None:
    if not HF_API_KEY:
        return None

    def _download() -> dict[str, Any]:
        from huggingface_hub import hf_hub_download
        path = hf_hub_download(
            repo_id=HF_DURABLE_STATE_REPO, filename=_DURABLE_STATE_HF_FILENAME, repo_type="model", token=HF_API_KEY,
        )
        return json.loads(Path(path).read_text(encoding="utf-8"))

    try:
        from server_common import call_with_hard_timeout
        return call_with_hard_timeout(_download, timeout_sec=_DURABLE_STATE_HF_TIMEOUT_SEC)
    except Exception as exc:
        logger.info("[kalshi_15m_strategy] no durable state on HF yet (or fetch failed): %s", exc)
        return None


def _load_state() -> dict[str, Any]:
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)
    except Exception:
        base = {"positions": [], "trade_log": [], "realized_pnl_by_date": {}, "tuning": {}}
        durable = _pull_durable_state_from_hf()
        if durable:
            base.update(durable)
            logger.info("[kalshi_15m_strategy] recovered durable state from HF after local state was missing")
        return base
    state.setdefault("positions", [])
    state.setdefault("trade_log", [])
    state.setdefault("realized_pnl_by_date", {})
    state.setdefault("tuning", {})
    return state


def _save_state(state: dict[str, Any], *, push_durable: bool = False) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)
    if push_durable:
        _push_durable_state_to_hf(state)


def _today_str() -> str:
    return dt.datetime.now(dt.timezone.utc).date().isoformat()


def evaluate_candidate(
    coin: str, *, confidence_min: float | None = None,
    correlation_study_enabled: bool | None = None, correlation_max_adjustment: float | None = None,
) -> dict[str, Any]:
    """Pure decision logic for one coin -- no state, no order placement,
    no side effects. Returns {"ok": False, "reason": ...} when there's
    nothing to do (no open window, too little time left, no trained model
    yet, confidence too low), or {"ok": True, "side": "yes"/"no",
    "market": {...}, "probability_up": float, "confidence": float} when a
    real entry candidate exists. `confidence` is always the probability of
    the SIDE actually chosen (i.e. probability_up for "yes",
    1-probability_up for "no"), so it's always directly comparable to
    MODEL_CONFIDENCE_MIN regardless of predicted direction.

    `confidence_min` overrides the module-level MODEL_CONFIDENCE_MIN
    default when given -- see scan_and_enter, which reads a durable-state
    override set by kalshi_15m_trade_analysis.recommend_confidence_threshold's
    own evidence-gated tuning (apply_confidence_threshold_override below),
    same pattern every other market here already uses. Kept as an
    optional parameter (not a direct read of state) so this function
    stays pure and independently testable.

    correlation_study_enabled/correlation_max_adjustment override the
    module-level USE_CORRELATION_STUDY/CORRELATION_CONFIDENCE_MAX_ADJUSTMENT
    defaults the exact same way -- see scan_and_enter, which reads a
    durable-state override set by
    kalshi_15m_trade_analysis.recommend_correlation_study_weight's own
    evidence-gated tuning (apply_correlation_study_override).

    The meta-model trust gate (USE_META_MODEL) has no equivalent override
    param -- unlike the two tunes above, it has no evidence-gated auto-
    tuner (same as perps_strategy.py's own identical PERPS_USE_META_MODEL:
    a static, manually-set flag pending real backtest validation, not
    something this codebase's trade-history-driven tuning touches)."""
    series_ticker = ASSET_SERIES.get(coin)
    if not series_ticker:
        return {"ok": False, "reason": "unknown_coin"}

    market = kalshi_15m.get_current_window_market(series_ticker)
    if market is None:
        return {"ok": False, "reason": "no_open_window"}

    remaining = kalshi_15m.seconds_to_close(market)
    if remaining is None or remaining < MIN_SECONDS_TO_CLOSE_FOR_ENTRY:
        return {"ok": False, "reason": "too_little_time_remaining", "seconds_to_close": remaining}

    prediction = _predict_direction(coin)
    if not prediction.get("model_ok"):
        return {"ok": False, "reason": "model_not_ready", "detail": prediction.get("reason")}

    probability_up = float(prediction["probability_up"])
    if probability_up >= 0.5:
        side, confidence = "yes", probability_up
    else:
        side, confidence = "no", 1.0 - probability_up

    # Chart-study confidence layer -- see USE_CORRELATION_STUDY's own
    # comment. Computed and attached unconditionally (cheap: an in-memory
    # dict lookup, see crypto_correlation.py's own caching design) so it's
    # visible for observability even while the flag is off; only actually
    # influences the confidence floor below once explicitly turned on.
    # Crypto only -- see USE_CORRELATION_STUDY's own comment on why no
    # equivalent study exists for metals.
    correlation_score, correlation_reason = 0.0, None
    if coin in kalshi_15m.KNOWN_15M_SERIES:
        correlation = crypto_correlation.perps_correlation_bullishness(coin, prediction.get("feature_row"))
        correlation_score, correlation_reason = correlation["score"], correlation["reason"]
    elif coin in kalshi_15m.KNOWN_15M_METALS_SERIES:
        # See crypto_correlation.refresh_metals_study's own comment --
        # this market's own 5-commodity study, refreshed by this same
        # process's own metals data-collect job (no cross-service
        # "remote" component needed, unlike the crypto path above).
        correlation = crypto_correlation.metals_correlation_bullishness(coin, prediction.get("feature_row"))
        correlation_score, correlation_reason = correlation["score"], correlation["reason"]
    # Bullish-signed (positive favors "yes"/up) -- flip for "no", same
    # convention crypto_correlation.py's own docstring documents for a
    # perps short.
    side_correlation_score = correlation_score if side == "yes" else -correlation_score

    effective_confidence_min = confidence_min if confidence_min is not None else MODEL_CONFIDENCE_MIN
    effective_use_correlation_study = USE_CORRELATION_STUDY if correlation_study_enabled is None else correlation_study_enabled
    effective_correlation_max_adjustment = (
        CORRELATION_CONFIDENCE_MAX_ADJUSTMENT if correlation_max_adjustment is None else correlation_max_adjustment
    )
    if effective_use_correlation_study:
        # Confirmation lowers the bar a little, disagreement raises it a
        # little -- capped at +/-effective_correlation_max_adjustment so
        # this can nudge the model's own gate, never override it outright.
        effective_confidence_min = max(
            0.5, min(0.95, effective_confidence_min - side_correlation_score * effective_correlation_max_adjustment),
        )

    if confidence < effective_confidence_min:
        return {
            "ok": False, "reason": "confidence_below_floor", "confidence": confidence,
            "correlation_score": correlation_score, "correlation_reason": correlation_reason,
        }

    # Meta-labeling trust gate -- see USE_META_MODEL's own comment. Crypto
    # only, same reasoning as the correlation study above (no metals
    # meta-model exists). None (no meta-model trained yet, or a row
    # missing context features) fails OPEN -- same "a missing signal
    # never blocks a trade" posture as model_ok=False's own absence
    # elsewhere; only an actual low trust score vetoes.
    meta_trust = None
    if USE_META_MODEL and coin in kalshi_15m.KNOWN_15M_SERIES:
        meta_trust = kalshi_15m_meta_model.trust_score(prediction.get("feature_row"), primary_probability_up=probability_up)
        if meta_trust is not None and meta_trust < META_MODEL_TRUST_MIN:
            return {
                "ok": False, "reason": "meta_model_trust_too_low", "confidence": confidence, "meta_trust_score": meta_trust,
                "correlation_score": correlation_score, "correlation_reason": correlation_reason,
            }

    result = {
        "ok": True, "coin": coin, "side": side, "market": market,
        "probability_up": probability_up, "confidence": confidence,
        "correlation_score": correlation_score, "correlation_reason": correlation_reason,
        # For USE_CONVICTION_SIZING (see scan_and_enter/compute_conviction_size_multiplier)
        # -- how far above its OWN entry bar this candidate's confidence
        # cleared.
        "effective_confidence_min": effective_confidence_min,
        # The full raw indicator row this decision was made from -- per
        # explicit user direction: "this need to remember what lead to a
        # trade and study it... with all indicator and times frames."
        # scan_and_enter turns this into entry_feature_snapshot on the
        # position/trade record itself; kept as the raw dict here (not
        # yet JSON-cleaned) so this function stays a thin, direct pass-
        # through of what _predict_direction already computed.
        "feature_row": prediction.get("feature_row"),
    }
    if meta_trust is not None:
        result["meta_trust_score"] = meta_trust
    return result


def _has_open_position(state: dict[str, Any], *, coin: str) -> bool:
    return any(p.get("coin") == coin for p in state.get("positions") or [])


def _clean_feature_snapshot(feature_row: dict[str, Any] | None) -> dict[str, float] | None:
    """The raw indicator/timeframe row a decision was made from, made
    JSON-safe for durable state/trade_log storage -- per explicit user
    direction: "this need to remember what lead to a trade and study
    it... this is trained in the bot itself with all indicator and time
    frames." Drops non-numeric fields (symbol/coin labels -- already
    recorded elsewhere on the position/trade itself) and coerces every
    remaining value to a plain float, since numpy scalar types (common in
    a pandas-derived feature row) aren't natively JSON-serializable.
    None in, None out -- never fabricates a snapshot that wasn't really
    computed."""
    if not feature_row:
        return None
    snapshot: dict[str, float] = {}
    for key, value in feature_row.items():
        if key in ("symbol", "coin"):
            continue
        try:
            snapshot[key] = float(value)
        except (TypeError, ValueError):
            continue
    return snapshot or None


def scan_and_enter(*, dry_run: bool | None = None) -> dict[str, Any]:
    """One pass over every coin: evaluate_candidate, then (if a real
    candidate exists, there's room under MAX_CONCURRENT_POSITIONS, and
    this coin doesn't already have an open position) place an entry.
    dry_run=None defers to LIVE_TRADING_ENABLED's own floor -- see this
    module's own docstring: that's now set on the live Space, so this
    places real orders unless a caller explicitly forces dry_run=True."""
    effective_dry_run = (not LIVE_TRADING_ENABLED) if dry_run is None else dry_run
    checks: list[dict[str, Any]] = []

    with _STATE_LOCK:
        state = _load_state()
        open_count = len(state.get("positions") or [])
        # A confidence floor genuinely learned from this account's own real
        # trade history (see kalshi_15m_trade_analysis.recommend_confidence_threshold
        # + apply_confidence_threshold_override below) -- falls back to the
        # module-level MODEL_CONFIDENCE_MIN default until enough real
        # trades exist to justify moving it. Same pattern every other
        # market here already uses.
        confidence_min_override = (state.get("tuning") or {}).get("model_confidence_min")
        # Same durable-state-driven override for the chart-study layer --
        # see USE_CORRELATION_STUDY's own comment and
        # apply_correlation_study_override below.
        tuning_state = state.get("tuning") or {}
        correlation_study_enabled_override = tuning_state.get("correlation_study_enabled")
        correlation_max_adjustment_override = tuning_state.get("correlation_confidence_max_adjustment")
        # Same durable-state-driven override for conviction sizing -- see
        # USE_CONVICTION_SIZING's own comment and
        # apply_conviction_sizing_override below.
        effective_use_conviction_sizing = tuning_state.get("conviction_sizing_enabled", USE_CONVICTION_SIZING)
        # Same durable-state-driven override for the win-streak size
        # increase -- see USE_WIN_STREAK_SIZING's own comment and
        # apply_win_streak_sizing_override below.
        effective_use_win_streak_sizing = tuning_state.get("win_streak_sizing_enabled", USE_WIN_STREAK_SIZING)
        # How many concurrent positions the account is allowed to hold
        # RIGHT NOW -- see compute_graduated_max_concurrent_positions'
        # own comment. Computed ONCE per scan (not re-read per coin) so
        # one cycle's own entries can't ratchet the cap up mid-loop off
        # a trade_log snapshot that's already stale by the second coin.
        effective_max_concurrent_positions = compute_graduated_max_concurrent_positions(state.get("trade_log"))

    for coin in ASSET_SERIES:
        if open_count >= effective_max_concurrent_positions:
            checks.append({
                "coin": coin, "ok": False, "reason": "max_concurrent_positions",
                "effective_max_concurrent_positions": effective_max_concurrent_positions,
            })
            continue
        with _STATE_LOCK:
            state = _load_state()
            if _has_open_position(state, coin=coin):
                checks.append({"coin": coin, "ok": False, "reason": "already_has_open_position"})
                continue
            # Per-coin trust gate -- see coin_is_trusted's own comment.
            # Reuses the SAME state snapshot already read above, no extra
            # state read needed.
            trust = coin_is_trusted(coin, state.get("trade_log"))
            if not trust["trusted"]:
                checks.append({"coin": coin, "ok": False, **trust})
                continue

        decision = evaluate_candidate(
            coin, confidence_min=confidence_min_override,
            correlation_study_enabled=correlation_study_enabled_override,
            correlation_max_adjustment=correlation_max_adjustment_override,
        )
        if not decision.get("ok"):
            checks.append({"coin": coin, **decision})
            continue

        market = decision["market"]
        # Price/side: cross the spread at the current best offer for the
        # chosen side (a marketable IOC order, same "pay the spread for a
        # real fill over a resting order that might never fill" tradeoff
        # kalshi_perps.py's own entries already accept).
        #
        # REAL, LIVE, CONFIRMED BUG this fixes (found by cross-checking
        # this account's own real Kalshi order history against this
        # module's bookkeeping): Kalshi's create-order-v2 `side` field
        # ALWAYS refers to the YES leg -- "bid" buys YES, "ask" sells YES
        # (confirmed via docs.kalshi.com's own field description AND a
        # community SDK independently, then confirmed a THIRD way against
        # this account's own real order records: every "no"-decision
        # order this code ever placed used side="bid", and Kalshi's own
        # order history shows those executing as real BUY-YES fills --
        # the exact opposite of the intended "no" position, with real
        # money). To actually hold NO, you SELL YES (side="ask") at
        # price = 1 - desired_no_price. This whole 15-minute-market
        # feature was taken offline (KALSHI_15M_LIVE_TRADING_ENABLED set
        # back to 0) the moment this was confirmed, pending this fix.
        no_ask = float(market.get("no_ask_dollars") or 0.99)
        no_bid = float(market.get("no_bid_dollars") or 0.01)
        # `price` is always what's SENT to Kalshi (its API is YES-
        # denominated regardless of which side we actually want -- see
        # the comment above). `cost_basis` is the SEPARATE, real cost per
        # contract of the side we actually end up holding, used for our
        # own settlement bookkeeping below (check_settlements' own
        # `count * (1 - entry_price)` / `-count * entry_price` formula) --
        # for "no", that's no_ask (what a NO contract really costs), NOT
        # `price` (the YES-denominated sell price Kalshi itself sees).
        # Conflating these two was part of the same real bug: recording
        # the YES-sell price as if it were the NO cost basis would have
        # silently mispriced settlement P&L even after fixing the
        # side/price sent to Kalshi.
        if decision["side"] == "no":
            side_char = "ask"
            price = round(1.0 - no_ask, 4)
            cost_basis = no_ask
        else:
            side_char = "bid"
            price = round(1.0 - no_bid, 4)
            cost_basis = price
        if price <= 0 or price >= 1:
            checks.append({"coin": coin, "ok": False, "reason": "no_valid_quote"})
            continue

        # Conviction sizing -- see USE_CONVICTION_SIZING's own comment.
        # 1.0 (no change) whenever the flag is off, so this never affects
        # sizing until real trade history earns it via
        # apply_conviction_sizing_override.
        size_multiplier = 1.0
        if effective_use_conviction_sizing:
            size_multiplier = compute_conviction_size_multiplier(decision["confidence"], decision.get("effective_confidence_min"))
        # Per-symbol loss-streak throttle -- see its own comment. Always
        # on (a pure risk-REDUCER, not an unproven experiment) -- reuses
        # the SAME state snapshot already read above for the
        # already-has-open-position check, no extra state read needed.
        loss_streak_multiplier = compute_loss_streak_size_multiplier(coin, state.get("trade_log"))
        # Per-symbol win-streak size increase -- see USE_WIN_STREAK_SIZING's
        # own comment. Off by default, so this stays 1.0 (no change) until
        # real trade history earns it via apply_win_streak_sizing_override.
        win_streak_multiplier = 1.0
        if effective_use_win_streak_sizing:
            win_streak_multiplier = compute_win_streak_size_multiplier(coin, state.get("trade_log"))
        contracts = max(1, int(
            (_account_budget_usd() * POSITION_SIZE_PCT * size_multiplier * loss_streak_multiplier * win_streak_multiplier)
            / cost_basis
        ))
        client_order_id = str(uuid.uuid4())

        order_id = None
        filled_count = float(contracts)  # dry-run: "fills" the full requested size in the simulation
        if not effective_dry_run:
            # SECOND, INDEPENDENT, ALWAYS-FRESH safety gate -- real,
            # confirmed user report: "i shut it off but it kept making
            # trades". Root cause: LIVE_TRADING_ENABLED (used above via
            # effective_dry_run) is a module-level constant, read from the
            # env var ONCE at import time -- flipping the HF Space
            # variable off restarts this process, but that restart is not
            # instantaneous, and this job runs every 2 minutes; a cycle
            # could fire mid-restart-window still holding the OLD, stale
            # "enabled" value baked in at the process's last start. This
            # re-reads the RAW env var fresh, every single time, right at
            # the last possible moment before a real order would be
            # placed -- so flipping the switch off takes effect on the
            # VERY NEXT cycle regardless of restart timing, with zero
            # dependency on this process ever actually restarting.
            # Deliberately does NOT replace LIVE_TRADING_ENABLED itself
            # (kept as-is for every existing test/observability caller);
            # this is a strictly additive, can-only-block-more safety net.
            if not _env_flag("KALSHI_15M_LIVE_TRADING_ENABLED", default=False):
                logger.warning(
                    "[kalshi_15m_strategy] live trading was just disabled -- skipping real order for %s "
                    "this cycle (fresh env re-check caught a stale cached LIVE_TRADING_ENABLED)", coin,
                )
                checks.append({"coin": coin, "ok": False, "reason": "live_trading_disabled_fresh_check"})
                continue
            try:
                order_result = kalshi_15m.create_order(
                    ticker=market["ticker"], side=side_char, count=contracts, price=price,
                    client_order_id=client_order_id,
                )
                # REAL, LIVE, CONFIRMED BUG this fixes: Kalshi's own
                # create-order response nests the order object under an
                # "order" key (confirmed against this account's own real
                # orders, and matching create_margin_order's own identical
                # response shape -- see perps_strategy.py's own
                # `order_result.get("order") or order_result` unwrap for
                # the same endpoint family) -- order_result.get("order_id")
                # directly was ALWAYS None. That None then never matched
                # any real order_id in the fresh_orders list just below,
                # so filled_count stayed 0 and every real order -- filled
                # or not -- was reported as "order_not_filled" and silently
                # dropped from local tracking. Confirmed live: two real,
                # currently-open Kalshi positions (gold, copper) exist on
                # this account with zero corresponding entry in local
                # state, discovered by cross-checking /api/kalshi15m/real-positions
                # against /api/kalshi15m/status right after this bug's own
                # introduction (the fill-verification fix earlier today).
                order = order_result.get("order") or order_result
                order_id = order.get("order_id")
            except Exception as exc:
                logger.warning("[kalshi_15m_strategy] order placement failed for %s: %s", coin, exc)
                checks.append({"coin": coin, "ok": False, "reason": "order_failed", "error": str(exc)})
                continue

            # REAL, LIVE, CONFIRMED BUG this ALSO fixes: this code used to
            # record a "position" the instant create_order returned an
            # order_id, with no check that the order actually filled.
            # Confirmed live: an IOC order that crosses no one (a stale
            # quote, a too-aggressive limit) gets Kalshi's own status
            # "canceled" with fill_count 0 -- this code was recording that
            # as an open position anyway, and later fabricating a
            # settlement outcome/P&L for a position the account never
            # actually held.
            #
            # THIRD, LIVE, CONFIRMED BUG this fixes (found by re-reading
            # Kalshi's own create-order-v2 API reference after real
            # positions kept appearing on this account -- confirmed via
            # /api/kalshi15m/real-positions -- with zero corresponding
            # local record even AFTER the order_id-unwrap fix above):
            # the ORIGINAL fill-check made a SEPARATE, immediately-
            # following GET /portfolio/orders call to look up this same
            # order's own fill_count_fp -- a real eventual-consistency
            # race against Kalshi's own backend (the just-placed order
            # isn't always visible in a LIST call microseconds after
            # CREATE returns), so a genuinely-filled real order could
            # still read back as "not found yet" and get wrongly
            # discarded. Kalshi's own docs confirm the CREATE response
            # ITSELF already carries `fill_count`/`remaining_count`
            # synchronously -- "Number of contracts filled immediately
            # upon placement" -- for an immediate_or_cancel order, no
            # follow-up read needed, and no race possible. Reads that
            # first; only falls back to the old separate GET call if the
            # create response genuinely didn't include it (defensive,
            # not the expected path for a real IOC fill).
            filled_count = float(order.get("fill_count_fp") or order.get("fill_count") or 0.0)
            if filled_count <= 0:
                try:
                    fresh_orders = kalshi_15m.get_orders(ticker=market["ticker"])
                    match = next((o for o in fresh_orders if o.get("order_id") == order_id), None)
                    if match is not None:
                        filled_count = float(match.get("fill_count_fp") or match.get("fill_count") or 0.0)
                except Exception as exc:
                    logger.warning("[kalshi_15m_strategy] could not verify fill for order %s (%s): %s", order_id, coin, exc)
            if filled_count <= 0:
                checks.append({"coin": coin, "ok": False, "reason": "order_not_filled", "order_id": order_id})
                continue

        position = {
            "coin": coin, "ticker": market["ticker"], "side": decision["side"],
            "count": filled_count, "entry_price": cost_basis, "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "close_time": market.get("close_time"), "entry_probability_up": decision["probability_up"],
            "entry_confidence": decision["confidence"], "dry_run": effective_dry_run,
            "client_order_id": client_order_id, "order_id": order_id,
            # Captured regardless of whether the correlation study was even
            # ON at entry time -- see kalshi_15m_trade_analysis.recommend_correlation_study_weight's
            # own docstring on why this makes that evidence-gated tuning
            # work from day one.
            "entry_correlation_score": decision.get("correlation_score"),
            # Captured regardless of whether conviction sizing was even ON
            # at entry time -- same "works from day one" reasoning, feeds
            # kalshi_15m_trade_analysis.recommend_conviction_sizing_trial's
            # own with-vs-without comparison.
            "entry_conviction_sizing_enabled": effective_use_conviction_sizing,
            # Observability only -- confirms a throttled entry actually
            # WAS sized down (1.0 whenever no loss streak was active).
            "entry_loss_streak_multiplier": loss_streak_multiplier,
            # Captured regardless of whether win-streak sizing was even ON
            # at entry time -- same "works from day one" reasoning, feeds
            # a future evidence-gated trial's own with-vs-without
            # comparison (see USE_WIN_STREAK_SIZING's own comment).
            "entry_win_streak_sizing_enabled": effective_use_win_streak_sizing,
            "entry_win_streak_multiplier": win_streak_multiplier,
            # The full indicator/timeframe snapshot this decision was made
            # from -- see _clean_feature_snapshot's own docstring.
            "entry_feature_snapshot": _clean_feature_snapshot(decision.get("feature_row")),
        }
        with _STATE_LOCK:
            state = _load_state()
            state["positions"].append(position)
            _save_state(state, push_durable=not effective_dry_run)
        open_count += 1
        checks.append({"coin": coin, "ok": True, "action": "entered", "side": decision["side"], "count": filled_count, "dry_run": effective_dry_run})

    return {"ok": True, "checks": checks, "live_trading_enabled": LIVE_TRADING_ENABLED}


KALSHI_15M_SHARD_INDEX = 2  # Crypto and Commodities -- see kalshi_15m.get_balance_by_shard's own docstring


def _account_budget_usd() -> float:
    """The dollar budget one full position slot sizes against. Real
    balance when live trading is actually verified and enabled; a fixed,
    clearly-labeled placeholder otherwise -- this module's own dry-run
    simulation doesn't need a real balance to exercise its own entry/
    settlement logic end to end (see this module's own docstring).

    Real, confirmed-live bug this fixes: used to call
    get_portfolio_balance() with no exchange_index -- per Kalshi's own
    docs that returns the balance POOLED ACROSS ALL SHARDS, not what's
    actually usable on shard 2 specifically (where these markets settle
    orders -- see kalshi_15m.get_balance_by_shard's own docstring on the
    exact same real gap already found once for the dashboard's own
    balance display). Sizing positions off the pooled total rather than
    the real, usable-here balance could over- or under-size every
    position depending on how much sits on other shards."""
    if not LIVE_TRADING_ENABLED:
        return 100.0
    try:
        balance = kalshi_15m.get_balance_by_shard(exchange_index=KALSHI_15M_SHARD_INDEX)
        return float(balance.get("balance_dollars") or 0.0)
    except Exception as exc:
        logger.warning("[kalshi_15m_strategy] balance fetch failed, using placeholder: %s", exc)
        return 100.0


def apply_confidence_threshold_override(new_threshold: float, *, reason: str) -> dict[str, Any]:
    """Applies an evidence-gated confidence-floor adjustment (see
    kalshi_15m_trade_analysis.recommend_confidence_threshold) durably,
    WITHOUT a redeploy -- stored in state["tuning"] (pushed to HF like the
    rest of durable state) and read by scan_and_enter on every cycle, not
    the OS env var MODEL_CONFIDENCE_MIN is seeded from at import time.
    Same pattern every other market here already uses.

    MERGES into state["tuning"] rather than replacing it wholesale -- a
    real bug already found and fixed in perps_strategy.py's own identical
    function (see its docstring): a wholesale replace here would silently
    wipe out apply_correlation_study_override's own keys
    (correlation_study_enabled/correlation_confidence_max_adjustment) the
    next time either tuning mechanism fired. Not repeated here."""
    with _STATE_LOCK:
        state = _load_state()
        tuning = dict(state.get("tuning") or {})
        previous = tuning.get("model_confidence_min", MODEL_CONFIDENCE_MIN)
        tuning.update({
            "model_confidence_min": new_threshold,
            "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "reason": reason, "previous": previous, "field": "model_confidence_min",
        })
        state["tuning"] = tuning
        _save_state(state, push_durable=True)
        return dict(state["tuning"])


def apply_correlation_study_override(*, enabled: bool | None = None, max_adjustment: float | None = None, reason: str) -> dict[str, Any]:
    """Same evidence-gated, no-redeploy-needed mechanism as
    apply_confidence_threshold_override above, for the chart-study layer
    (see kalshi_15m_trade_analysis.recommend_correlation_study_weight) --
    MERGES into state["tuning"] so this and the confidence-threshold
    override coexist. `enabled`/`max_adjustment` are each optional: pass
    only the one(s) this call is actually changing -- the other stays
    whatever it already was."""
    with _STATE_LOCK:
        state = _load_state()
        tuning = dict(state.get("tuning") or {})
        previous_enabled = tuning.get("correlation_study_enabled", USE_CORRELATION_STUDY)
        previous_max_adjustment = tuning.get("correlation_confidence_max_adjustment", CORRELATION_CONFIDENCE_MAX_ADJUSTMENT)
        if enabled is not None:
            tuning["correlation_study_enabled"] = enabled
        if max_adjustment is not None:
            tuning["correlation_confidence_max_adjustment"] = max_adjustment
        tuning.update({
            "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "reason": reason, "field": "correlation_study",
            "previous_correlation_study_enabled": previous_enabled,
            "previous_correlation_confidence_max_adjustment": previous_max_adjustment,
        })
        state["tuning"] = tuning
        _save_state(state, push_durable=True)
        return dict(state["tuning"])


def apply_conviction_sizing_override(*, enabled: bool, reason: str) -> dict[str, Any]:
    """Same evidence-gated, no-redeploy-needed mechanism as
    apply_confidence_threshold_override/apply_correlation_study_override
    above, for conviction sizing (see
    kalshi_15m_trade_analysis.recommend_conviction_sizing_trial) --
    MERGES into state["tuning"] so all 3 coexist."""
    with _STATE_LOCK:
        state = _load_state()
        tuning = dict(state.get("tuning") or {})
        previous = tuning.get("conviction_sizing_enabled", USE_CONVICTION_SIZING)
        tuning.update({
            "conviction_sizing_enabled": enabled,
            "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "reason": reason, "field": "conviction_sizing", "previous_conviction_sizing_enabled": previous,
        })
        state["tuning"] = tuning
        _save_state(state, push_durable=True)
        return dict(state["tuning"])


def apply_win_streak_sizing_override(*, enabled: bool, reason: str) -> dict[str, Any]:
    """Same evidence-gated, no-redeploy-needed mechanism as
    apply_conviction_sizing_override above, for the win-streak size
    increase (see kalshi_15m_trade_analysis.recommend_win_streak_sizing_trial)
    -- MERGES into state["tuning"] so all of these coexist."""
    with _STATE_LOCK:
        state = _load_state()
        tuning = dict(state.get("tuning") or {})
        previous = tuning.get("win_streak_sizing_enabled", USE_WIN_STREAK_SIZING)
        tuning.update({
            "win_streak_sizing_enabled": enabled,
            "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "reason": reason, "field": "win_streak_sizing", "previous_win_streak_sizing_enabled": previous,
        })
        state["tuning"] = tuning
        _save_state(state, push_durable=True)
        return dict(state["tuning"])


_LAST_BATCH_ANALYSIS_TRADE_COUNT_KEY = "last_batch_analysis_trade_count"


def _maybe_run_batch_trade_analysis() -> dict[str, Any] | None:
    """Every kalshi_15m_trade_analysis.BATCH_SIZE newly-closed REAL
    trades, studies that recent batch -- win/loss patterns, a per-trade
    "lesson" -- and, when the evidence supports it, applies THREE
    independent, small, bounded, evidence-gated tunes: the confidence
    floor (apply_confidence_threshold_override), whether the
    correlation-study layer itself is worth trusting
    (apply_correlation_study_override), and whether conviction sizing is
    worth trying/keeping (apply_conviction_sizing_override). Called right
    after
    check_settlements closes trades, inside the same job but outside
    _STATE_LOCK for the actual analysis work (same reasoning as every
    other market's identical function). Best-effort: any failure here is
    logged and swallowed, never allowed to affect trading. Returns None
    when fewer than BATCH_SIZE new real trades have landed since the last
    run (nothing to do yet), otherwise the batch summary dict."""
    from data import kalshi_15m_trade_analysis

    try:
        with _STATE_LOCK:
            state = _load_state()
            trade_log = state.get("trade_log") or []
            real_trades = [t for t in trade_log if not t.get("dry_run")]
            # Deliberately a TOP-LEVEL state key, NOT nested inside
            # state["tuning"] -- kept separate from the tuning dict (which
            # apply_confidence_threshold_override/apply_correlation_study_override
            # both merge into, not replace) purely so this counter's own
            # read/write doesn't need to reason about that merge at all. Not
            # part of _durable_state_slice either (same as every sibling
            # market's identical counter): worst case after a restart is
            # this batch re-running a little early/late, never a
            # correctness problem worth a durable push for.
            last_count = int(state.get(_LAST_BATCH_ANALYSIS_TRADE_COUNT_KEY) or 0)
            if len(real_trades) - last_count < kalshi_15m_trade_analysis.BATCH_SIZE:
                return None
            state[_LAST_BATCH_ANALYSIS_TRADE_COUNT_KEY] = len(real_trades)
            _save_state(state)
            tuning_state = state.get("tuning") or {}
            current_threshold = tuning_state.get("model_confidence_min", MODEL_CONFIDENCE_MIN)
            current_correlation_enabled = tuning_state.get("correlation_study_enabled", USE_CORRELATION_STUDY)
            current_correlation_max_adjustment = tuning_state.get(
                "correlation_confidence_max_adjustment", CORRELATION_CONFIDENCE_MAX_ADJUSTMENT,
            )
            current_conviction_sizing_enabled = tuning_state.get("conviction_sizing_enabled", USE_CONVICTION_SIZING)
            current_win_streak_sizing_enabled = tuning_state.get("win_streak_sizing_enabled", USE_WIN_STREAK_SIZING)

        batch = kalshi_15m_trade_analysis.analyze_recent_trade_batch(real_trades)
        logger.info(
            "[kalshi_15m_strategy] batch trade analysis: %s",
            kalshi_15m_trade_analysis.format_batch_snapshot_text(batch),
        )

        tuning_rec = kalshi_15m_trade_analysis.recommend_confidence_threshold(real_trades, current_threshold=current_threshold)
        if tuning_rec.get("should_apply"):
            applied = apply_confidence_threshold_override(tuning_rec["recommended_threshold"], reason="5-trade batch review")
            logger.info("[kalshi_15m_strategy] confidence threshold tuned: %s", applied)

        correlation_rec = kalshi_15m_trade_analysis.recommend_correlation_study_weight(
            real_trades, current_enabled=current_correlation_enabled, current_max_adjustment=current_correlation_max_adjustment,
        )
        if correlation_rec.get("should_apply"):
            applied = apply_correlation_study_override(
                enabled=correlation_rec.get("recommended_enabled"),
                max_adjustment=correlation_rec.get("recommended_max_adjustment"),
                reason=f"5-trade batch review ({correlation_rec['action']})",
            )
            logger.info("[kalshi_15m_strategy] correlation study tuned: %s", applied)

        conviction_rec = kalshi_15m_trade_analysis.recommend_conviction_sizing_trial(
            real_trades, current_enabled=current_conviction_sizing_enabled,
        )
        if conviction_rec.get("should_apply"):
            applied = apply_conviction_sizing_override(
                enabled=conviction_rec["recommended_enabled"], reason=f"5-trade batch review ({conviction_rec['action']})",
            )
            logger.info("[kalshi_15m_strategy] conviction sizing tuned: %s", applied)

        win_streak_rec = kalshi_15m_trade_analysis.recommend_win_streak_sizing_trial(
            real_trades, current_enabled=current_win_streak_sizing_enabled,
        )
        if win_streak_rec.get("should_apply"):
            applied = apply_win_streak_sizing_override(
                enabled=win_streak_rec["recommended_enabled"], reason=f"5-trade batch review ({win_streak_rec['action']})",
            )
            logger.info("[kalshi_15m_strategy] win-streak sizing tuned: %s", applied)
        return batch
    except Exception:
        logger.warning("[kalshi_15m_strategy] batch trade analysis failed", exc_info=True)
        return None


# ---------------------------------------------------------------------------
# Early exit -- "stay or close" a still-open real position, instead of
# always holding to settlement. Per explicit user direction: "i need a
# multi time frame study to happen on the most frames and correlated
# asset as well to enhance decision making and help also determine
# staying or closing the winning position." Off by default
# (USE_EARLY_EXIT) -- an evidence-gated EXPERIMENT, same "prove it out on
# real trade history/backtest first" posture as every other new risk-
# shape lever here (correlation study, meta-model, conviction sizing) --
# this module's own docstring's "no stop-loss/take-profit... exactly one
# exit" design was correct with ZERO real trade history to justify an
# early-exit lever; this is that lever, built but not yet trusted with
# real capital until real evidence says otherwise.
#
# Reuses the SAME model + correlation-study reassessment entry itself
# uses (see _reassess_coin below) -- the correlation study's own
# multi_timeframe_bullishness component already reads across 5m/15m/30m
# return + 1h/2h/3h/4h trend + MACD + RSI (see crypto_correlation.py's
# own _TIMEFRAME_REFERENCE_SCALES), and the peer/divergence/breadth
# components already read the correlated-asset universe (perps' own for
# crypto, this market's own 5-commodity study for metals) -- not a
# separate, second model, just the SAME real signal re-run against
# CURRENT data to ask "does the original entry thesis still hold."
# ---------------------------------------------------------------------------
USE_EARLY_EXIT = _env_flag("KALSHI_15M_USE_EARLY_EXIT", default=False)
# Don't bother managing a position that was only just opened -- a few
# seconds/minutes in, a "reassessment" is mostly noise around the same
# decision just made, not a real change of mind.
EARLY_EXIT_MIN_SECONDS_HELD = _env_int("KALSHI_15M_EARLY_EXIT_MIN_SECONDS_HELD", 120)
# How far past a coin-flip (0.5) the reassessment's own confidence in the
# OPPOSITE side must be before treating this as a real flip worth acting
# on, not noise -- same role MODEL_CONFIDENCE_MIN's own margin plays at
# entry, just measured as a margin above 0.5 instead of an absolute floor
# (a reassessment doesn't need the SAME bar as a fresh entry: reversing
# an already-taken position is a different decision than starting one).
EARLY_EXIT_CONFIDENCE_FLIP_MARGIN = _env_float("KALSHI_15M_EARLY_EXIT_CONFIDENCE_FLIP_MARGIN", 0.08)


def _reassess_coin(coin: str) -> dict[str, Any]:
    """A leaner version of evaluate_candidate's own model + correlation-
    study logic, WITHOUT the entry-only market-discovery/time-remaining
    gates (a position being MANAGED is already open regardless of how
    much time is left in its own window) -- used by decide_early_exit to
    ask "does the original thesis still hold against CURRENT data?" Pure
    function -- no state, no order placement, no side effects. Returns
    {"model_ok": False} on the same "no trained model yet" condition
    evaluate_candidate would; otherwise {"model_ok": True, "side":
    "yes"/"no", "confidence": float, "correlation_score": float} -- same
    field meanings as evaluate_candidate's own successful return."""
    prediction = _predict_direction(coin)
    if not prediction.get("model_ok"):
        return {"model_ok": False}
    probability_up = float(prediction["probability_up"])
    side, confidence = ("yes", probability_up) if probability_up >= 0.5 else ("no", 1.0 - probability_up)
    correlation_score = 0.0
    if coin in kalshi_15m.KNOWN_15M_SERIES:
        correlation_score = crypto_correlation.perps_correlation_bullishness(coin, prediction.get("feature_row"))["score"]
    elif coin in kalshi_15m.KNOWN_15M_METALS_SERIES:
        correlation_score = crypto_correlation.metals_correlation_bullishness(coin, prediction.get("feature_row"))["score"]
    return {"model_ok": True, "side": side, "confidence": confidence, "correlation_score": correlation_score}


def _exit_order_side_and_price(position_side: str, market: dict[str, Any]) -> tuple[str, float]:
    """The mirror image of scan_and_enter's own entry side/price logic --
    see this module's own top docstring on the order-side fix for the
    full mechanics this depends on. Closing a "yes" position means
    SELLING yes (side="ask") -- the exact same (side, price) pair
    scan_and_enter's own entry path already uses to OPEN a "no" position.
    Closing a "no" position means BUYING yes back (side="bid") -- the
    exact same pair used to OPEN a "yes" position. Both cross the CURRENT
    spread for a marketable IOC fill, same tradeoff entry already
    accepts."""
    no_ask = float(market.get("no_ask_dollars") or 0.99)
    no_bid = float(market.get("no_bid_dollars") or 0.01)
    if position_side == "yes":
        return "ask", round(1.0 - no_ask, 4)
    return "bid", round(1.0 - no_bid, 4)


def _current_exit_value(position_side: str, market: dict[str, Any]) -> float:
    """What one contract of the held side could be sold for RIGHT NOW, in
    the SAME entry_price-comparable terms check_settlements' own P&L
    formula already uses -- the current best bid for the held side
    (crossing it guarantees an IOC fill). For "yes", that's the implied
    yes_bid (1 - no_ask); for "no", the real no_bid field is already
    NO-denominated directly."""
    no_ask = float(market.get("no_ask_dollars") or 0.99)
    no_bid = float(market.get("no_bid_dollars") or 0.01)
    if position_side == "yes":
        return round(1.0 - no_ask, 4)
    return round(no_bid, 4)


def decide_early_exit(position: dict[str, Any], reassessment: dict[str, Any], *, current_value: float) -> dict[str, Any]:
    """Pure decision logic -- no state, no order placement. Should this
    still-open REAL position be closed NOW instead of held to
    settlement? See USE_EARLY_EXIT's own module-level comment for the
    full design.

    Only ever considers exiting when the reassessment has FLIPPED away
    from the side this position actually holds -- the entry thesis
    itself breaking down is the signal, not a fixed price target (this
    market's own quadratic-fee/short-window economics make a plain
    take-profit-percentage ladder a poor fit; see this module's own
    docstring). Two distinct outcomes once a real flip is confirmed:
      - "lock_in_profit": currently profitable -- exit now rather than
        risk giving the gain back holding to a settlement the model no
        longer expects to win.
      - "cut_loss": currently losing -- exit now rather than let a
        thesis the model itself has abandoned ride all the way to a full
        loss at settlement."""
    if not reassessment.get("model_ok"):
        return {"should_exit": False, "reason": "reassessment_not_available"}

    held_side = position["side"]
    unrealized_pnl_per_contract = round(current_value - position["entry_price"], 6)

    if reassessment["side"] == held_side:
        return {
            "should_exit": False, "reason": "thesis_still_agrees",
            "unrealized_pnl_per_contract": unrealized_pnl_per_contract,
        }

    # Confidence here is already the probability of reassessment["side"]
    # (see _reassess_coin's own docstring) -- how far past a coin-flip is
    # a measure of how real this disagreement is, not noise.
    flip_strength = round(reassessment["confidence"] - 0.5, 6)
    if flip_strength < EARLY_EXIT_CONFIDENCE_FLIP_MARGIN:
        return {
            "should_exit": False, "reason": "flip_too_weak", "flip_strength": flip_strength,
            "unrealized_pnl_per_contract": unrealized_pnl_per_contract,
        }

    reason = "lock_in_profit" if unrealized_pnl_per_contract > 0 else "cut_loss"
    return {
        "should_exit": True, "reason": reason, "flip_strength": flip_strength,
        "unrealized_pnl_per_contract": unrealized_pnl_per_contract,
    }


def manage_open_positions(*, dry_run: bool | None = None) -> dict[str, Any]:
    """For every real, currently-open position, asks decide_early_exit
    whether to close it now instead of holding it to settlement -- see
    that function's own and USE_EARLY_EXIT's own docstrings for the full
    design. Computed and checked unconditionally (for observability, same
    "visible even before this is ever turned on" posture the correlation
    study already uses) -- only actually places a real closing order once
    USE_EARLY_EXIT is on AND live trading is genuinely enabled.

    Deliberately skips dry-run positions entirely -- a dry-run position
    has no real order to close early, and its own settlement-only
    lifecycle (check_settlements) is already a fully exercisable
    simulation; this feature exists to protect REAL capital specifically.
    """
    effective_dry_run = (not LIVE_TRADING_ENABLED) if dry_run is None else dry_run
    checks: list[dict[str, Any]] = []
    with _STATE_LOCK:
        state = _load_state()
        real_positions = [p for p in (state.get("positions") or []) if not p.get("dry_run")]

    for position in real_positions:
        coin = position["coin"]
        series_ticker = ASSET_SERIES.get(coin)
        if not series_ticker:
            continue
        try:
            market = kalshi_15m.get_current_window_market(series_ticker)
        except Exception as exc:
            logger.warning("[kalshi_15m_strategy] market lookup failed while managing %s: %s", coin, exc)
            continue
        if market is None or market.get("ticker") != position.get("ticker"):
            # Either this position's own window already closed (settled
            # separately, see check_settlements) or a brief gap between
            # windows -- nothing to manage this tick.
            continue

        try:
            opened_at = dt.datetime.fromisoformat(str(position["opened_at"]).replace("Z", "+00:00"))
            held_seconds = (dt.datetime.now(dt.timezone.utc) - opened_at).total_seconds()
        except Exception:
            held_seconds = EARLY_EXIT_MIN_SECONDS_HELD  # fail open -- a parse hiccup shouldn't block management
        if held_seconds < EARLY_EXIT_MIN_SECONDS_HELD:
            checks.append({"coin": coin, "should_exit": False, "reason": "too_early_to_manage"})
            continue

        reassessment = _reassess_coin(coin)
        current_value = _current_exit_value(position["side"], market)
        decision = decide_early_exit(position, reassessment, current_value=current_value)
        checks.append({"coin": coin, **decision})

        if not USE_EARLY_EXIT or not decision.get("should_exit") or effective_dry_run:
            continue

        side_char, price = _exit_order_side_and_price(position["side"], market)
        client_order_id = str(uuid.uuid4())
        try:
            order_result = kalshi_15m.create_order(
                ticker=position["ticker"], side=side_char, count=position["count"], price=price,
                client_order_id=client_order_id,
            )
            order = order_result.get("order") or order_result
            order_id = order.get("order_id")
        except Exception as exc:
            logger.warning("[kalshi_15m_strategy] early-exit order placement failed for %s: %s", coin, exc)
            checks[-1]["exit_order_failed"] = str(exc)
            continue

        # Reads the fill count directly off the CREATE response first --
        # see scan_and_enter's own identical comment on why (Kalshi's own
        # docs confirm fill_count/remaining_count are returned
        # synchronously for an IOC order; a separate follow-up GET call
        # is a real eventual-consistency race, not a more-reliable check).
        filled_count = float(order.get("fill_count_fp") or order.get("fill_count") or 0.0)
        if filled_count <= 0:
            try:
                fresh_orders = kalshi_15m.get_orders(ticker=position["ticker"])
                match = next((o for o in fresh_orders if o.get("order_id") == order_id), None)
                if match is not None:
                    filled_count = float(match.get("fill_count_fp") or match.get("fill_count") or 0.0)
            except Exception as exc:
                logger.warning("[kalshi_15m_strategy] could not verify early-exit fill for %s (%s): %s", order_id, coin, exc)
        if filled_count <= 0:
            checks[-1]["exit_order_not_filled"] = True
            continue

        realized_pnl = round(filled_count * (current_value - position["entry_price"]), 6)
        trade = {
            "coin": coin, "ticker": position["ticker"], "side": position["side"], "count": filled_count,
            "entry_price": position["entry_price"], "result": None, "realized_pnl_usd": realized_pnl,
            "opened_at": position["opened_at"], "closed_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "entry_probability_up": position.get("entry_probability_up"), "entry_confidence": position.get("entry_confidence"),
            "dry_run": False, "entry_correlation_score": position.get("entry_correlation_score"),
            "entry_conviction_sizing_enabled": position.get("entry_conviction_sizing_enabled"),
            "entry_loss_streak_multiplier": position.get("entry_loss_streak_multiplier"),
            "entry_win_streak_sizing_enabled": position.get("entry_win_streak_sizing_enabled"),
            "entry_win_streak_multiplier": position.get("entry_win_streak_multiplier"),
            "entry_feature_snapshot": position.get("entry_feature_snapshot"),
            "exit_kind": "early", "exit_reason": decision["reason"],
        }
        with _STATE_LOCK:
            state = _load_state()
            by_date = state.setdefault("realized_pnl_by_date", {})
            today = _today_str()
            by_date[today] = round(float(by_date.get(today, 0.0)) + realized_pnl, 6)
            state["trade_log"].append(trade)
            state["positions"] = [p for p in state.get("positions") or [] if p.get("ticker") != position["ticker"]]
            _save_state(state, push_durable=True)
        checks[-1]["action"] = "closed_early"
        checks[-1]["realized_pnl_usd"] = realized_pnl

    return {"ok": True, "checks": checks}


def check_settlements() -> dict[str, Any]:
    """For every open position, checks whether its market has actually
    settled (a PUBLIC, unauthenticated read -- see this module's own
    docstring) and books the real outcome if so. dry-run and real
    positions are both checked the same way here -- the SETTLEMENT itself
    is a real, public fact regardless of whether this account actually
    held a real contract through it, which is exactly what makes dry-run
    mode here a genuine end-to-end simulation, not a stub."""
    checks: list[dict[str, Any]] = []
    with _STATE_LOCK:
        state = _load_state()
        positions = list(state.get("positions") or [])

    for position in positions:
        ticker = position["ticker"]
        try:
            fresh = kalshi_15m.get_market(ticker)
        except Exception as exc:
            logger.warning("[kalshi_15m_strategy] settlement check failed for %s: %s", ticker, exc)
            continue

        if fresh is None:
            continue
        result = (fresh.get("result") or "").strip().lower()
        if result not in ("yes", "no"):
            checks.append({"coin": position["coin"], "ok": True, "action": "still_open"})
            continue

        won = result == position["side"]
        gross = position["count"] * (1.0 - position["entry_price"]) if won else -position["count"] * position["entry_price"]
        closed_at = dt.datetime.now(dt.timezone.utc).isoformat()
        trade = {
            "coin": position["coin"], "ticker": ticker, "side": position["side"],
            "count": position["count"], "entry_price": position["entry_price"], "result": result,
            "realized_pnl_usd": round(gross, 6), "opened_at": position["opened_at"], "closed_at": closed_at,
            "entry_probability_up": position.get("entry_probability_up"),
            "entry_confidence": position.get("entry_confidence"), "dry_run": position.get("dry_run", True),
            "entry_correlation_score": position.get("entry_correlation_score"),
            "entry_conviction_sizing_enabled": position.get("entry_conviction_sizing_enabled"),
            "entry_loss_streak_multiplier": position.get("entry_loss_streak_multiplier"),
            "entry_win_streak_sizing_enabled": position.get("entry_win_streak_sizing_enabled"),
            "entry_win_streak_multiplier": position.get("entry_win_streak_multiplier"),
            "entry_feature_snapshot": position.get("entry_feature_snapshot"),
            "exit_kind": "settled",
        }
        # Removes this ONE settled position from a FRESHLY re-read state
        # (matched by ticker, which is unique per 15-minute window -- see
        # get_current_window_market's own "one open market per series at a
        # time" contract) rather than a snapshot-taken-at-function-start
        # bulk overwrite -- a real, if currently latent, correctness gap
        # closed before it could ever matter: any future caller adding a
        # position concurrently (scan_and_enter already runs sequentially
        # after this within the same locked job, so no live overlap today)
        # would otherwise have that fresh position silently erased by this
        # function's own stale end-of-loop overwrite.
        with _STATE_LOCK:
            state = _load_state()
            by_date = state.setdefault("realized_pnl_by_date", {})
            if not trade["dry_run"]:
                today = _today_str()
                by_date[today] = round(float(by_date.get(today, 0.0)) + trade["realized_pnl_usd"], 6)
            state["trade_log"].append(trade)
            state["positions"] = [p for p in state.get("positions") or [] if p.get("ticker") != ticker]
            _save_state(state, push_durable=not trade["dry_run"])
        checks.append({"coin": position["coin"], "ok": True, "action": "settled", "won": won, "realized_pnl_usd": trade["realized_pnl_usd"]})

    # Best-effort, outside any lock held above -- see
    # _maybe_run_batch_trade_analysis's own docstring. A cheap no-op call
    # on every cycle where fewer than BATCH_SIZE new real trades have
    # settled since the last run (the overwhelmingly common case).
    _maybe_run_batch_trade_analysis()
    return {"ok": True, "checks": checks}
