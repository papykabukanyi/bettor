"""Alpaca equities/ETF trading strategy -- separate from and independent of
the Kalshi perps bot's strategy module. Long-only to start (matching the
same "start conservative, prove it out, then extend" posture the perps bot
used before enabling shorts) -- short-selling stocks needs margin approval
and carries materially different risk that hasn't been asked for here.

Alpaca charges $0 commission on equity/ETF trades (same as Schwab) -- the
real (small) costs here are the bid-ask spread and tiny regulatory fees
(SEC fee on sells, FINRA TAF), not modeled explicitly since they're a
rounding error next to Kalshi Perps' 1.6% round-trip taker fee.

Entry: a volume/volatility "something is happening right now" signal
(dollar_volume_z spike + volatility above the symbol's own recent baseline)
combined with a short-term momentum read and (once trained) the direction
model's confidence -- mirrors decide_entry_technical/evaluate_candidate's
shape in perps_strategy.py.
"""
from __future__ import annotations

import datetime as dt
import gc
import json
import logging
import os
import tempfile
import threading
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


# Real gap found in review: requiring a volume spike (dollar_volume_z) AND
# a volatility-ratio jump AND a dip, all three simultaneously, made real
# entries rare -- confirmed live, almost every cycle across the whole
# watchlist skipped on "volume not unusual enough" before the dip signal
# was even checked. perps_strategy.py's own decide_entry_technical (the
# system this one was explicitly modeled on, and the one actually taking
# opportunities) has NO volume/volatility gate at all -- just the dip/rally
# read plus a soft trend filter. Off by default now (env-overridable back
# on): MIN_VOLUME_Z=-1e9 makes "z < MIN_VOLUME_Z" unsatisfiable (a z-score
# is always a finite real number nowhere near this magnitude) -- NOT
# float("-inf"): a real, confirmed production bug found live -- Python's
# json module happily serializes float("-inf") as the bare token
# `-Infinity`, which is NOT valid JSON grammar, so every browser's
# JSON.parse() (unlike Python's own lenient json.loads()) threw a
# SyntaxError on every single /api/alpaca/status poll, silently breaking
# the dashboard's entire auto-refresh loop (caught by its own try/catch,
# so the page just sat frozen on its initial "--" placeholders forever,
# with no visible error), and MIN_VOLATILITY_RATIO=0.0 makes
# "ratio < MIN_VOLATILITY_RATIO" unsatisfiable (a volatility ratio is
# always >= 0) -- both checks below become genuine no-ops, not just
# "usually true," at these defaults.
MIN_VOLUME_Z = _env_float("ALPACA_MIN_VOLUME_Z", -1e9)
MIN_VOLATILITY_RATIO = _env_float("ALPACA_MIN_VOLATILITY_RATIO", 0.0)  # volatility_5 / volatility_30

# "Enter on a small pullback, same 0.15% perps_strategy.py itself uses"
ENTRY_DIP_PCT = _env_float("ALPACA_ENTRY_DIP_PCT", 0.0015)
SHORT_MA_MINUTES = _env_int("ALPACA_SHORT_MA_MINUTES", 15)

TAKE_PROFIT_PCT = _env_float("ALPACA_TAKE_PROFIT_PCT", 0.01)
STOP_LOSS_PCT = _env_float("ALPACA_STOP_LOSS_PCT", 0.008)
MAX_HOLD_MINUTES = _env_int("ALPACA_MAX_HOLD_MINUTES", 120)
# Same stale/flat position early exit perps_strategy.py and
# alpaca_crypto_strategy.py both carry (see either module's own comment for
# the full rationale) -- a position that hasn't captured a meaningful
# fraction of its own take-profit distance by the halfway point of
# max_hold_time is unlikely to still develop into one by the full timeout,
# so this frees the slot early instead of tying up capital for zero
# informational value. Ported here proactively for consistency, even though
# stocks' own bracket-order TP/SL already caps downside/upside natively --
# this loop's job (see manage_open_positions's own docstring) is exactly the
# max_hold_time-style forced exit that Alpaca's bracket order has no native
# concept of, and stale_position is that same class of forced exit.
STALE_POSITION_CHECK_FRACTION = _env_float("ALPACA_STALE_POSITION_CHECK_FRACTION", 0.5)
STALE_POSITION_MAX_PROGRESS_FRACTION = _env_float("ALPACA_STALE_POSITION_MAX_PROGRESS_FRACTION", 0.25)

MODEL_CONFIDENCE_MIN = _env_float("ALPACA_MODEL_CONFIDENCE_MIN", 0.55)

# At the perps-style default of 10% per slot across 5 slots, a $100 account
# gets a $10-20 budget per position -- not enough to buy even ONE share of
# most liquid, well-known stocks (a $100+ share price is completely
# ordinary). Real diversification across 5 positions needs real capital;
# at $100, spreading thin just means most "positions" silently can't afford
# a single share. Concentrating into fewer, larger slots is the only way a
# small account can actually hold real, liquid names -- "safe" here comes
# from the tight stop-loss on each position, not from spreading a tiny
# balance thinner.
POSITION_SIZE_PCT = _env_float("ALPACA_POSITION_SIZE_PCT", 0.45)
MAX_CONCURRENT_POSITIONS = max(1, _env_int("ALPACA_MAX_CONCURRENT_POSITIONS", 2))
DAILY_LOSS_CAP_PCT = _env_float("ALPACA_DAILY_LOSS_CAP_PCT", 0.10)
# Same unbounded-growth guard perps_strategy.py already needed (a real,
# confirmed OOM contributor there over weeks of live trading) -- keeps the
# most recent entries, oldest-first trimmed.
MAX_TRADE_LOG_ENTRIES = _env_int("ALPACA_MAX_TRADE_LOG_ENTRIES", 2000)

LIVE_TRADING_ENABLED = str(os.getenv("ALPACA_LIVE_TRADING_ENABLED", "")).strip().lower() in {"1", "true", "yes"}


def decide_entry_technical(row: dict[str, Any]) -> tuple[bool, str]:
    """row needs: current_price, short_ma, dollar_volume_z, volatility_5,
    volatility_30 (all already computed by engineer_features)."""
    dollar_volume_z = row.get("dollar_volume_z")
    volatility_5 = row.get("volatility_5") or 0.0
    volatility_30 = row.get("volatility_30") or 0.0

    if dollar_volume_z is None or dollar_volume_z < MIN_VOLUME_Z:
        return False, f"volume not unusual enough (z={dollar_volume_z})"
    if volatility_30 > 0 and (volatility_5 / volatility_30) < MIN_VOLATILITY_RATIO:
        return False, "not more volatile than its own recent baseline"

    current_price = row["current_price"]
    short_ma = row["short_ma"]
    if short_ma <= 0:
        return False, "no short MA yet"
    dip_pct = (short_ma - current_price) / short_ma
    if dip_pct < ENTRY_DIP_PCT:
        return False, f"no real dip ({dip_pct:+.3%})"
    return True, f"dip ({dip_pct:+.3%}, z={dollar_volume_z:.2f})"


def evaluate_candidate(row: dict[str, Any], model_prediction: dict[str, Any] | None) -> dict[str, Any]:
    technical_ok, technical_reason = decide_entry_technical(row)
    result: dict[str, Any] = {
        "symbol": row.get("symbol"), "technical_ok": technical_ok, "reason": technical_reason,
        "model_ok": False, "should_enter": False,
    }
    if not technical_ok:
        return result

    if model_prediction and model_prediction.get("model_ok"):
        proba_up = model_prediction["probability_up"]
        result["model_ok"] = True
        result["probability_up"] = proba_up
        result["model_direction"] = "up" if proba_up >= 0.5 else "down"
        if proba_up >= MODEL_CONFIDENCE_MIN:
            result["should_enter"] = True
            result["reason"] = f"{technical_reason} + model confident up ({proba_up:.2%})"
    else:
        # No trained model yet -- technical-only fallback (same posture as
        # perps_strategy.py during the first days of data collection).
        result["should_enter"] = True

    return result


def decide_exit(
    position: dict[str, Any], current_price: float, *, now: dt.datetime | None = None,
) -> tuple[bool, str]:
    """Long-only: a RISING price is favorable. Mirrors perps_strategy.py's
    decide_exit() shape (take-profit / stop-loss / max-hold), simplified
    since there's no short side or leverage-fee interaction to account for
    here."""
    entry_price = float(position["entry_price"])
    change_pct = (current_price - entry_price) / entry_price if entry_price > 0 else 0.0

    if change_pct >= TAKE_PROFIT_PCT:
        return True, f"take_profit ({change_pct:+.3%})"
    if change_pct <= -STOP_LOSS_PCT:
        return True, f"stop_loss ({change_pct:+.3%})"

    opened_at = dt.datetime.fromisoformat(position["opened_at"])
    now = now if now is not None else dt.datetime.now(dt.timezone.utc)
    held_minutes = (now - opened_at).total_seconds() / 60.0
    # Stale/flat position early exit -- see STALE_POSITION_CHECK_FRACTION's
    # own comment. Uses abs(change_pct) deliberately: this targets
    # positions that haven't moved meaningfully in EITHER direction, not
    # ones that are simply losing (a real, distinct case already owned by
    # the bracket order's own stop-loss) -- a position sitting mid-way
    # toward its real stop is a normal, developing loser that should be
    # left to either recover or hit its real stop, not cut early by a
    # second, competing mechanism.
    if held_minutes >= MAX_HOLD_MINUTES * STALE_POSITION_CHECK_FRACTION and TAKE_PROFIT_PCT > 0:
        if abs(change_pct) < TAKE_PROFIT_PCT * STALE_POSITION_MAX_PROGRESS_FRACTION:
            return True, f"stale_position ({held_minutes:.0f}min, {change_pct:+.3%}, flat)"
    if held_minutes >= MAX_HOLD_MINUTES:
        return True, f"max_hold_time ({held_minutes:.0f}min, {change_pct:+.3%})"
    return False, f"holding ({change_pct:+.3%}, {held_minutes:.0f}min)"


def position_exit_levels(position: dict[str, Any]) -> dict[str, float]:
    entry_price = float(position["entry_price"])
    return {
        "take_profit_price": round(entry_price * (1 + TAKE_PROFIT_PCT), 6),
        "stop_loss_price": round(entry_price * (1 - STOP_LOSS_PCT), 6),
    }


def compute_position_size(available_balance_usd: float, price: float) -> int:
    """Whole shares only. Alpaca supports fractional shares on plain
    market/day orders, but NOT on bracket/OCO orders (which this strategy
    relies on for the take-profit/stop-loss pair) -- so integer sizing here
    isn't just conservative, it's a real requirement."""
    if price <= 0:
        return 0
    budget = available_balance_usd * POSITION_SIZE_PCT
    return int(budget // price)


def _candles_as_dicts(df) -> list[dict[str, Any]]:
    """Converts a fetch_recent_minute_bars-style DataFrame into the plain
    list[dict] shape both chart_snapshot.generate_candlestick_chart and
    alpaca_trade_analysis expect -- keeps those two modules pandas-free."""
    if df is None or df.empty:
        return []
    cols = [c for c in ("ts", "open", "high", "low", "close") if c in df.columns]
    return df[cols].to_dict("records")


def _index_for_ts(df, iso_ts: str | None) -> int | None:
    """Which row of a fetch_recent_minute_bars-style DataFrame is closest to
    a given ISO timestamp -- used to mark ENTRY/EXIT on the candlestick
    chart. None if there's no timestamp, no data, or the closest candle is
    more than an hour away (the trade's window genuinely isn't covered by
    this data -- a wildly wrong index would mislabel the chart, so skip the
    marker instead of guessing)."""
    if df is None or df.empty or not iso_ts:
        return None
    try:
        target = dt.datetime.fromisoformat(str(iso_ts).replace("Z", "+00:00")).timestamp()
    except (ValueError, TypeError):
        return None
    ts_list = list(df["ts"])
    if not ts_list:
        return None
    best_idx, best_diff = 0, abs(ts_list[0] - target)
    for i, ts in enumerate(ts_list):
        diff = abs(ts - target)
        if diff < best_diff:
            best_idx, best_diff = i, diff
    return best_idx if best_diff <= 3600 else None


def _maybe_run_batch_trade_analysis() -> None:
    """Every alpaca_trade_analysis.BATCH_SIZE newly-closed REAL trades,
    studies that recent batch -- win/loss patterns, missed-profit/
    premature-stop diagnostics from real OHLC -- and posts a Threads
    snapshot. Called right after manage_open_positions closes trades,
    outside _STATE_LOCK (same reasoning as every other Threads/network call
    in this module). Best-effort: any failure here is logged and
    swallowed, never allowed to affect trading."""
    from data import alpaca_trade_analysis
    from data import threads_post
    from data.alpaca_data import fetch_recent_minute_bars

    try:
        with _STATE_LOCK:
            state = _load_state()
            trade_log = state.get("trade_log") or []
            real_trades = [t for t in trade_log if not t.get("dry_run")]
            last_count = int(state.get("last_batch_analysis_trade_count") or 0)
            if len(real_trades) - last_count < alpaca_trade_analysis.BATCH_SIZE:
                return
            state["last_batch_analysis_trade_count"] = len(real_trades)
            _save_state(state, push_durable=True)

        recent = real_trades[-alpaca_trade_analysis.BATCH_SIZE:]
        candles_by_symbol: dict[str, list[dict[str, Any]]] = {}
        for symbol in {t.get("symbol") for t in recent if t.get("symbol")}:
            try:
                candles_by_symbol[symbol] = _candles_as_dicts(fetch_recent_minute_bars(symbol))
            except Exception:
                logger.debug("[alpaca_strategy] candle fetch for batch analysis failed for %s", symbol, exc_info=True)

        batch = alpaca_trade_analysis.analyze_recent_trade_batch(real_trades, candles_by_symbol=candles_by_symbol)
        text = alpaca_trade_analysis.format_batch_snapshot_text(batch, market="stocks")
        threads_post.post_trade_analysis_summary(text, market="stocks")
    except Exception:
        logger.warning("[alpaca_strategy] batch trade analysis failed", exc_info=True)


# ---------------------------------------------------------------------------
# Always trades against the real Alpaca account (paper or live, whichever
# ALPACA_TRADING_BASE_URL points at) -- the custom local-balance "simulate"
# mode this used to have was removed per the user's explicit request:
# Alpaca's own paper account is now the single source of truth for balance/
# positions/fills, not a hand-rolled virtual ledger. LIVE_TRADING_ENABLED
# remains the one safety gate on whether an order is actually placed vs.
# dry-run (decide but don't call the order API).
# ---------------------------------------------------------------------------

ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT_DIR / "data"
STATE_FILE = Path(os.getenv("ALPACA_STATE_FILE", str(DATA_DIR / "alpaca_state.json")))
_STATE_LOCK = threading.RLock()

HF_API_KEY = os.getenv("HF_API_KEY", "")
HF_ALPACA_MODEL_REPO = os.getenv("HF_ALPACA_MODEL_REPO", "papylove/alpaca-model")
_DURABLE_STATE_HF_FILENAME = "alpaca_durable_state.json"
_DURABLE_PUSH_MIN_INTERVAL_SEC = 30
_last_durable_push_ts = 0.0


def _today_str() -> str:
    return dt.datetime.now(dt.timezone.utc).date().isoformat()


def _durable_state_slice(state: dict[str, Any]) -> dict[str, Any]:
    return {
        "positions": state.get("positions") or [],
        "trade_log": state.get("trade_log") or [],
        "realized_pnl_by_date": state.get("realized_pnl_by_date") or {},
        "daily_reference_balance": state.get("daily_reference_balance") or {},
    }


def _push_durable_state_to_hf(state: dict[str, Any]) -> None:
    if not HF_API_KEY:
        return
    try:
        from huggingface_hub import HfApi
        api = HfApi(token=HF_API_KEY)
        payload = json.dumps(_durable_state_slice(state), indent=2)
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
            tmp.write(payload)
            tmp_path = tmp.name
        try:
            api.upload_file(
                path_or_fileobj=tmp_path, path_in_repo=_DURABLE_STATE_HF_FILENAME,
                repo_id=HF_ALPACA_MODEL_REPO, repo_type="model", commit_message="update alpaca durable state",
            )
        finally:
            os.unlink(tmp_path)
    except Exception as exc:
        logger.warning("[alpaca_strategy] durable state push to HF failed: %s", exc)


_DURABLE_STATE_HF_TIMEOUT_SEC = int(os.getenv("ALPACA_DURABLE_STATE_HF_TIMEOUT_SEC", "10") or "10")


def _pull_durable_state_from_hf() -> dict[str, Any] | None:
    if not HF_API_KEY:
        return None

    def _download() -> dict[str, Any]:
        from huggingface_hub import hf_hub_download
        path = hf_hub_download(
            repo_id=HF_ALPACA_MODEL_REPO, filename=_DURABLE_STATE_HF_FILENAME, repo_type="model", token=HF_API_KEY,
        )
        return json.loads(Path(path).read_text(encoding="utf-8"))

    try:
        # Real, confirmed production incident (same call shape, on the
        # perps service): unbounded, this can hang for minutes on
        # huggingface_hub's own internal session lock, freezing this whole
        # --workers 1 process until gunicorn's worker timeout kills it. See
        # server_common.call_with_hard_timeout's own docstring.
        from server_common import call_with_hard_timeout
        return call_with_hard_timeout(_download, timeout_sec=_DURABLE_STATE_HF_TIMEOUT_SEC)
    except Exception as exc:
        logger.info("[alpaca_strategy] no durable state on HF yet (or fetch failed): %s", exc)
        return None


def _load_state() -> dict[str, Any]:
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)
    except Exception:
        base = {
            "positions": [], "trade_log": [], "realized_pnl_by_date": {}, "daily_reference_balance": {},
        }
        durable = _pull_durable_state_from_hf()
        if durable:
            base.update(durable)
            logger.info("[alpaca_strategy] recovered durable state from HF after local state was missing")
        return base
    state.setdefault("positions", [])
    state.setdefault("trade_log", [])
    state.setdefault("realized_pnl_by_date", {})
    state.setdefault("daily_reference_balance", {})
    return state


def _save_state(state: dict[str, Any], *, push_durable: bool = False) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)
    if not push_durable:
        return
    global _last_durable_push_ts
    now = time.time()
    if now - _last_durable_push_ts >= _DURABLE_PUSH_MIN_INTERVAL_SEC:
        _last_durable_push_ts = now
        _push_durable_state_to_hf(state)


def get_available_balance() -> float:
    """The real Alpaca cash balance."""
    from data import alpaca_client
    account = alpaca_client.get_account()
    return float(account.get("cash") or 0.0)


def get_current_price(symbol: str) -> float | None:
    """A real-time-ish bid/ask quote (cheap, no bars history needed)."""
    from data import alpaca_client
    try:
        quote = alpaca_client.get_latest_quote(symbol)
        ask, bid = quote.get("ap"), quote.get("bp")
        if ask and bid:
            return (float(ask) + float(bid)) / 2.0
        price = ask or bid
        return float(price) if price else None
    except Exception as exc:
        logger.warning("[alpaca_strategy] quote fetch failed for %s: %s", symbol, exc)
        return None


def _reference_balance_for_today(state: dict[str, Any], available_balance_usd: float | None) -> float | None:
    """The daily loss cap is a percentage of the balance as it stood at the
    START of the day, not of whatever Alpaca's account balance happens to be
    at the moment it's checked (which drifts throughout the day as trades
    close) -- captured once per day the first time a real balance read
    succeeds. Same pattern as perps_strategy.py's/alpaca_crypto_strategy.py's
    own _reference_balance_for_today."""
    today = _today_str()
    refs = state.setdefault("daily_reference_balance", {})
    if today not in refs:
        if available_balance_usd is None:
            return None
        refs[today] = available_balance_usd
        for old_date in list(refs.keys()):
            if old_date != today:
                del refs[old_date]
    return float(refs[today])


def _real_open_positions_by_symbol() -> dict[str, dict[str, Any]] | None:
    """Ground truth from Alpaca's own GET /v2/positions -- local bookkeeping
    only ever records an order having been PLACED, never confirms it
    actually FILLED at the assumed price/quantity. Returns None (never an
    empty dict) on a failed API call so callers can tell "confirmed no real
    positions" apart from "couldn't check" and avoid wiping out tracking on
    a transient error -- same discipline as alpaca_crypto_strategy.py's own
    _real_open_positions_by_symbol.

    /v2/positions returns EVERY asset class this account holds (equities,
    crypto, and options share one Alpaca account) -- filtered here to
    asset_class=="us_equity" so a crypto or options position can never be
    mistaken for one of this strategy's own."""
    from data import alpaca_client
    try:
        positions = alpaca_client.get_positions()
    except Exception as exc:
        logger.warning("[alpaca_strategy] could not fetch real positions for reconciliation: %s", exc)
        return None
    result: dict[str, dict[str, Any]] = {}
    for p in positions:
        if p.get("asset_class") != "us_equity":
            continue
        symbol = p.get("symbol") or ""
        qty = float(p.get("qty") or 0.0)
        if not symbol or qty == 0:
            continue
        result[symbol] = {"count": abs(qty), "entry_price": float(p.get("avg_entry_price") or 0.0)}
    return result


def _reconcile_positions_with_exchange(state: dict[str, Any]) -> list[dict[str, Any]]:
    """Makes local state["positions"] match what the real Alpaca account
    actually holds before any exit/entry decision is made. Handles all
    three ways local bookkeeping can have drifted from reality (same three
    modes alpaca_crypto_strategy.py's own _reconcile_positions_with_exchange
    handles):
      - a real position exists that local state never recorded (a prior
        entry order's fill was never verified) -> ADOPT it, so it starts
        being monitored for take-profit/stop-loss instead of sitting with
        no coverage at all;
      - a local position's count/entry_price doesn't match the real one
        -> CORRECT it;
      - a local position has no real counterpart at all (the entry order
        never actually filled) -> DROP it without recording a fake trade.
    Only ever called when live trading is actually active (see callers) --
    in dry-run, local positions are hypothetical (no order was ever placed)
    and deliberately have no real-exchange counterpart, so reconciling
    would just erase them.

    Complements (doesn't replace) the existing get_position() double-sell
    guard in manage_open_positions() -- that guard protects against a
    bracket order's own native TP/SL having already closed a position out
    from under this loop; this proactively fixes count/entry_price drift
    and adopts positions this process never even knew about."""
    local_positions = state.get("positions") or []
    real = _real_open_positions_by_symbol()
    if real is None:
        return local_positions

    local_by_symbol = {p["symbol"]: p for p in local_positions}
    reconciled: list[dict[str, Any]] = []
    for symbol, real_pos in real.items():
        local = local_by_symbol.get(symbol)
        if local is None:
            logger.warning(
                "[alpaca_strategy] adopting untracked real position: %s x%.4f @ %.4f",
                symbol, real_pos["count"], real_pos["entry_price"],
            )
            reconciled.append({
                "symbol": symbol, "entry_price": real_pos["entry_price"], "count": real_pos["count"],
                "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(), "order_id": None,
            })
            continue
        if (
            abs(float(local["count"]) - real_pos["count"]) > 1e-9
            or abs(float(local["entry_price"]) - real_pos["entry_price"]) > 1e-6
        ):
            logger.warning(
                "[alpaca_strategy] correcting local position for %s: count %.4f->%.4f, entry %.4f->%.4f",
                symbol, float(local["count"]), real_pos["count"], float(local["entry_price"]), real_pos["entry_price"],
            )
        local["count"] = real_pos["count"]
        local["entry_price"] = real_pos["entry_price"]
        reconciled.append(local)

    for symbol in local_by_symbol:
        if symbol not in real:
            logger.warning("[alpaca_strategy] dropping phantom local position (no matching real fill): %s", symbol)

    return reconciled


def scan_and_enter(watchlist: list[str] | None = None, *, dry_run: bool | None = None) -> dict[str, Any]:
    """Evaluates each watchlist symbol for a new entry. Places a real order
    against the Alpaca account ALPACA_TRADING_BASE_URL points at UNLESS
    dry_run resolves True, in which case it only reports what it would have
    done -- same dual-gate posture as perps_strategy.py.

    Session-aware, "stream live like perps" -- data collection already ran
    unconditionally 24/7 (no market-hours gate at all), but entries didn't
    account for WHICH kind of order Alpaca actually allows outside the
    regular 9:30-4:00 ET session: a bracket order (entry + linked TP/SL) is
    regular-hours only, and even a plain order must be type="limit" with
    extended_hours=true during pre/post-market -- a market order there is
    simply rejected. Regular session keeps the existing bracket order
    unchanged; pre/post-market places a plain extended-hours limit order
    instead (manage_open_positions then owns TP/SL/max-hold via its own
    poll loop for that position, the same pattern already proven for
    crypto/options, which never had broker-native brackets to begin with).
    Fully closed (no session at all, ~8pm-4am ET) skips entirely -- nothing
    is fillable then regardless of order type."""
    from data.alpaca_data import fetch_recent_minute_bars, get_market_session, get_stock_watchlist, latest_feature_row, load_training_dataset
    from data.alpaca_model import predict_direction
    from data import threads_post

    effective_dry_run = (not LIVE_TRADING_ENABLED) if dry_run is None else dry_run
    market_session = get_market_session()
    if market_session["session"] == "closed":
        return {"opened": [], "action": "market_closed"}
    is_extended_hours = market_session["session"] != "regular"
    if watchlist is None:
        try:
            # Real, confirmed production incident: this service kept
            # OOM-killing every 15-30 minutes even after the del+gc.collect()
            # below was added, because gc.collect() only frees references
            # AFTER this call returns -- it cannot reduce the PEAK memory
            # reached DURING load_training_dataset() itself. This call is
            # ranking-only (feeding get_stock_watchlist's volume/volatility
            # sort), not training, so it never needed training-grade depth --
            # matching perps_data.py's own _recent_volatility_by_ticker(),
            # which deliberately uses max_rows=5000 for the identical
            # "just need a ranking signal" use case.
            recent = load_training_dataset(max_rows=5_000)
        except Exception:
            recent = None
        watchlist = get_stock_watchlist(recent if recent is not None and not recent.empty else None)
        del recent
        gc.collect()

    opened: list[dict[str, Any]] = []
    with _STATE_LOCK:
        state = _load_state()
        if not effective_dry_run:
            # Ground-truth check first, before deciding anything -- see
            # _reconcile_positions_with_exchange. Dry-run positions are
            # purely hypothetical (no order was ever placed), so this only
            # ever runs when orders are actually being placed.
            state["positions"] = _reconcile_positions_with_exchange(state)
            _save_state(state)
        existing_symbols = {p["symbol"] for p in (state.get("positions") or [])}
        open_count = len(state.get("positions") or [])
        try:
            available_balance_usd = get_available_balance()
        except Exception as exc:
            available_balance_usd = None
            logger.debug("[alpaca_strategy] balance read for daily reference failed: %s", exc)
        reference_was_just_set = _today_str() not in (state.get("daily_reference_balance") or {})
        reference_balance = _reference_balance_for_today(state, available_balance_usd)
        today_pnl = float((state.get("realized_pnl_by_date") or {}).get(_today_str(), 0.0))
        loss_cap_breached = bool(
            reference_balance and reference_balance > 0
            and today_pnl <= -abs(DAILY_LOSS_CAP_PCT) * reference_balance
        )
        _save_state(state, push_durable=reference_was_just_set)
    if loss_cap_breached:
        return {"opened": [], "action": "daily_loss_cap_breached"}

    for symbol in watchlist:
        try:
            if symbol in existing_symbols:
                continue
            if open_count >= MAX_CONCURRENT_POSITIONS:
                opened.append({"symbol": symbol, "ok": True, "action": "skipped_slot_taken"})
                continue

            row = latest_feature_row(symbol)
            if row is None:
                continue
            model_prediction = predict_direction(symbol)
            candidate = evaluate_candidate(row, model_prediction)
            if not candidate["should_enter"]:
                opened.append({"symbol": symbol, "ok": True, "action": "skipped", "reason": candidate["reason"]})
                continue

            available_balance = get_available_balance()
            entry_price = row["current_price"]
            count = compute_position_size(available_balance, entry_price)
            if count < 1:
                opened.append({"symbol": symbol, "ok": True, "action": "skipped_insufficient_budget"})
                continue

            levels = position_exit_levels({"entry_price": entry_price})
            order_id = None
            if not effective_dry_run:
                from data import alpaca_client
                if is_extended_hours:
                    # Small marketable buffer above the last price -- a
                    # genuine limit (Alpaca requires one outside regular
                    # hours), sized to actually have a real chance of
                    # filling rather than sitting unfilled all session.
                    order_spec = alpaca_client.build_extended_hours_limit_order(
                        symbol=symbol, quantity=count, side="buy", limit_price=entry_price * 1.002,
                    )
                else:
                    order_spec = alpaca_client.build_bracket_order(
                        symbol=symbol, quantity=count, side="buy",
                        take_profit_price=levels["take_profit_price"], stop_loss_price=levels["stop_loss_price"],
                    )
                order_id = alpaca_client.place_order(order_spec)

            # Entry-time model/technical context -- what the model/filters
            # actually saw at decision time, so a post-trade analysis can
            # ask "what led to this win/loss" instead of only ever knowing
            # how it ended. Same fields (by name) perps_strategy.py already
            # captures, so alpaca_trade_analysis.py can mirror its logic.
            entry_context = {
                "entry_probability_up": candidate.get("probability_up"),
                "entry_model_direction": candidate.get("model_direction"),
                "entry_reason": candidate.get("reason"),
            }
            position = {
                "symbol": symbol, "entry_price": entry_price, "count": float(count),
                "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                "order_id": order_id, **levels, **entry_context,
            }
            with _STATE_LOCK:
                state = _load_state()
                positions = state.get("positions") or []
                if any(p["symbol"] == symbol for p in positions) or len(positions) >= MAX_CONCURRENT_POSITIONS:
                    opened.append({"symbol": symbol, "ok": True, "action": "skipped_slot_taken"})
                    continue
                positions.append(position)
                state["positions"] = positions
                _save_state(state)
                existing_symbols.add(symbol)
                open_count = len(positions)
            trade_dry_run = effective_dry_run
            opened.append({
                "symbol": symbol, "ok": True, "action": "opened", "entry_price": entry_price,
                "count": count, "dry_run": trade_dry_run,
            })
            # Best-effort only -- see threads_post.py's own docstring. Never
            # allow a Threads failure (or it simply not being configured
            # yet) to affect the real/simulated entry that already happened
            # above. Alpaca strategy is long-only (see decide_exit's own
            # docstring), so side is always "long" here.
            try:
                threads_post.post_trade_entry(
                    ticker=symbol, side="long", entry_price=entry_price,
                    take_profit_price=levels["take_profit_price"], stop_loss_price=levels["stop_loss_price"],
                    reason=candidate["reason"], dry_run=trade_dry_run, market="stocks",
                )
            except Exception:
                logger.warning("[alpaca_strategy] Threads post for %s entry failed", symbol, exc_info=True)
            try:
                one_min_df = fetch_recent_minute_bars(symbol)
                threads_post.post_trade_entry_chart(
                    ticker=symbol, market="stocks", candles=_candles_as_dicts(one_min_df),
                    entry_price=entry_price, take_profit_price=levels["take_profit_price"],
                    stop_loss_price=levels["stop_loss_price"],
                    entry_index=(len(one_min_df) - 1) if not one_min_df.empty else None,
                    side="long", dry_run=trade_dry_run,
                )
            except Exception:
                logger.warning("[alpaca_strategy] Threads chart post for %s entry failed", symbol, exc_info=True)
        except Exception as exc:
            opened.append({"symbol": symbol, "ok": False, "action": "entry_failed", "error": str(exc)})

    return {"opened": opened}


def manage_open_positions(*, dry_run: bool | None = None) -> dict[str, Any]:
    """Checks every open position for an exit. The take-profit/stop-loss
    are ALREADY live on the exchange as a bracket order the instant the
    entry filled (see scan_and_enter) -- this loop's main job is to catch
    max_hold_time (which Alpaca's bracket order has no native concept of)
    and force-close in that case.

    Before forcing an exit, this checks whether the position still actually
    exists on Alpaca (get_position). If Alpaca's own bracket take-profit/
    stop-loss already fired the exit moments earlier, the position is
    already gone -- forcing another close_position call there would be a
    real double-sell attempt, not just a redundant one, so this reconciles
    using the position's own stored target level instead of calling
    close_position again. _reconcile_positions_with_exchange (below) is a
    complementary, proactive check -- it fixes count/entry_price drift and
    adopts untracked positions, but doesn't replace this in-the-moment
    double-sell guard."""
    from data import threads_post
    from data.alpaca_data import fetch_recent_minute_bars

    effective_dry_run = (not LIVE_TRADING_ENABLED) if dry_run is None else dry_run
    with _STATE_LOCK:
        state = _load_state()
        if not effective_dry_run:
            state["positions"] = _reconcile_positions_with_exchange(state)
            _save_state(state)
        positions = list(state.get("positions") or [])
    if not positions:
        return {"action": "no_position", "checks": []}

    closed: list[dict[str, Any]] = []
    checks: list[dict[str, Any]] = []
    remaining_symbols = {p["symbol"] for p in positions}

    for position in positions:
        symbol = position["symbol"]
        try:
            current_price = get_current_price(symbol)
            if current_price is None:
                checks.append({"symbol": symbol, "ok": False, "error": "no_quote_available"})
                continue

            should_exit, reason = decide_exit(position, current_price)
            if not should_exit:
                checks.append({"symbol": symbol, "ok": True, "exit_check": reason})
                continue

            closed_count = float(position["count"])
            if not effective_dry_run:
                from data import alpaca_client
                from data.alpaca_data import get_market_session
                # Checked fresh at EXIT time, not entry time -- a position
                # opened during regular hours can still be sitting open
                # once the session rolls into post-market (max_hold_time,
                # a slow-moving stop), and close_position()'s at-market
                # liquidation would be rejected there just the same as for
                # a position that was opened extended-hours to begin with.
                exit_session = get_market_session()["session"]
                if exit_session == "closed":
                    checks.append({"symbol": symbol, "ok": True, "exit_deferred": "market_closed"})
                    continue
                still_open = True
                try:
                    still_open = alpaca_client.get_position(symbol) is not None
                except Exception as exc:
                    logger.warning("[alpaca_strategy] position lookup failed for %s, assuming still open: %s", symbol, exc)
                if still_open:
                    try:
                        if exit_session == "regular":
                            alpaca_client.close_position(symbol)
                        else:
                            # Extended hours: close_position() liquidates
                            # at market, which Alpaca rejects outside
                            # 9:30-4:00 ET -- needs a plain limit sell with
                            # extended_hours=true instead (no bracket to
                            # rely on either way -- see scan_and_enter).
                            order_spec = alpaca_client.build_extended_hours_limit_order(
                                symbol=symbol, quantity=position["count"], side="sell",
                                limit_price=current_price * 0.998,
                            )
                            alpaca_client.place_order(order_spec)
                    except Exception as exc:
                        logger.warning("[alpaca_strategy] close_position failed for %s (may have already closed): %s", symbol, exc)
                    # A submitted close order is not a confirmed fill -- real,
                    # confirmed production incident: booking P&L here
                    # unconditionally produced 8 duplicate "closed" trade_log
                    # entries for the SAME real META position (a stop_loss
                    # that took several 20s fast_check cycles to actually
                    # fill) -- each re-discovered as "untracked" by
                    # reconciliation next cycle and re-booked, ~$6,400 of
                    # phantom recorded loss stacked on top of the one real
                    # close. Same real-fill-verification discipline already
                    # proven in perps_strategy.py/alpaca_crypto_strategy.py's
                    # own exit paths: re-check the real remaining quantity
                    # right after, and only book what actually closed.
                    try:
                        pos_after = alpaca_client.get_position(symbol)
                        remaining_qty = float(pos_after["qty"]) if pos_after else 0.0
                        closed_count = round(max(0.0, float(position["count"]) - remaining_qty), 6)
                    except Exception as exc:
                        logger.warning(
                            "[alpaca_strategy] could not verify exit fill for %s after placing order -- assuming full close: %s",
                            symbol, exc,
                        )
                    if closed_count <= 0:
                        # Nothing actually filled yet -- keep monitoring the
                        # still-real position next cycle rather than booking
                        # a trade that never happened.
                        checks.append({"symbol": symbol, "ok": False, "error": "exit_order_did_not_fill"})
                        continue
                elif "take_profit" in reason:
                    current_price = position["take_profit_price"]
                elif "stop_loss" in reason:
                    current_price = position["stop_loss_price"]

            gross = round((current_price - float(position["entry_price"])) * closed_count, 6)
            opened_at = position.get("opened_at")
            closed_at = dt.datetime.now(dt.timezone.utc).isoformat()
            hold_minutes = None
            if opened_at:
                try:
                    opened_dt = dt.datetime.fromisoformat(opened_at)
                    hold_minutes = round((dt.datetime.now(dt.timezone.utc) - opened_dt).total_seconds() / 60, 2)
                except (ValueError, TypeError):
                    hold_minutes = None
            with _STATE_LOCK:
                state = _load_state()
                by_date = state.setdefault("realized_pnl_by_date", {})
                today = _today_str()
                by_date[today] = round(float(by_date.get(today, 0.0)) + gross, 6)
                trade = {
                    "closed_at": closed_at, "opened_at": opened_at, "hold_minutes": hold_minutes,
                    "symbol": symbol, "entry_price": position["entry_price"], "exit_price": current_price,
                    "count": closed_count, "realized_pnl_usd": gross, "reason": reason,
                    "dry_run": effective_dry_run,
                    # Entry-time context copied from the position -- see
                    # scan_and_enter's own comment on why.
                    "entry_probability_up": position.get("entry_probability_up"),
                    "entry_model_direction": position.get("entry_model_direction"),
                    "entry_reason": position.get("entry_reason"),
                }
                trade_log = state.setdefault("trade_log", [])
                trade_log.append(trade)
                if len(trade_log) > MAX_TRADE_LOG_ENTRIES:
                    del trade_log[: len(trade_log) - MAX_TRADE_LOG_ENTRIES]
                if closed_count < float(position["count"]) - 1e-6:
                    # Partial fill -- the remainder is still genuinely open,
                    # keep monitoring it rather than dropping it.
                    remaining_qty = round(float(position["count"]) - closed_count, 6)
                    state["positions"] = [
                        {**p, "count": remaining_qty} if p["symbol"] == symbol else p
                        for p in (state.get("positions") or [])
                    ]
                else:
                    state["positions"] = [p for p in (state.get("positions") or []) if p["symbol"] != symbol]
                _save_state(state, push_durable=True)
            remaining_symbols.discard(symbol)
            closed.append(trade)
            try:
                threads_post.post_trade_exit(
                    ticker=symbol, side="long", entry_price=float(position["entry_price"]), exit_price=current_price,
                    pnl_usd=gross, reason=reason, dry_run=trade["dry_run"], market="stocks",
                )
            except Exception:
                logger.warning("[alpaca_strategy] Threads post for %s exit failed", symbol, exc_info=True)
            try:
                one_min_df = fetch_recent_minute_bars(symbol)
                threads_post.post_trade_exit_chart(
                    ticker=symbol, market="stocks", candles=_candles_as_dicts(one_min_df),
                    side="long", entry_price=float(position["entry_price"]), exit_price=current_price,
                    entry_index=_index_for_ts(one_min_df, opened_at), exit_index=_index_for_ts(one_min_df, closed_at),
                    pnl_usd=gross, dry_run=trade["dry_run"],
                )
            except Exception:
                logger.warning("[alpaca_strategy] Threads exit chart post for %s failed", symbol, exc_info=True)
        except Exception as exc:
            logger.warning("[alpaca_strategy] could not process position for %s -- leaving untouched this cycle: %s", symbol, exc)
            checks.append({"symbol": symbol, "ok": False, "error": str(exc)})

    if closed:
        _maybe_run_batch_trade_analysis()

    return {"action": "closed" if closed else "no_change", "closed": closed, "checks": checks}
