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

Much simpler than perps_strategy.py by design, not by omission: no
leverage (these are plain $1-notional binary contracts, see kalshi_15m.py's
own docstring), no stop-loss/take-profit/quick-profit exit percentages (a
position here has exactly one exit: the window closes and it settles --
there is no "exit early" lever worth building until real evidence shows
selling before close ever beats holding to settlement), no daily-loss-cap/
technical-scalper-filter machinery. The model's own probability_up IS the
entire signal, matching what a 15-minute binary contract actually needs:
one calibrated probability, not perps' multi-signal-agreement gate.
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
    }
    if meta_trust is not None:
        result["meta_trust_score"] = meta_trust
    return result


def _has_open_position(state: dict[str, Any], *, coin: str) -> bool:
    return any(p.get("coin") == coin for p in state.get("positions") or [])


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

    for coin in ASSET_SERIES:
        if open_count >= MAX_CONCURRENT_POSITIONS:
            checks.append({"coin": coin, "ok": False, "reason": "max_concurrent_positions"})
            continue
        with _STATE_LOCK:
            state = _load_state()
            if _has_open_position(state, coin=coin):
                checks.append({"coin": coin, "ok": False, "reason": "already_has_open_position"})
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
        contracts = max(1, int((_account_budget_usd() * POSITION_SIZE_PCT * size_multiplier) / cost_basis))
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
            # actually held. Read back this exact order's own real status
            # (an authenticated but read-only call) before trusting it.
            filled_count = 0.0
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
        return batch
    except Exception:
        logger.warning("[kalshi_15m_strategy] batch trade analysis failed", exc_info=True)
        return None


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
