"""Alpaca crypto trading strategy -- a new, separate strategy from
alpaca_strategy.py (equities), built "the same recipe as perps": real
technical indicators + real crypto news sentiment feeding a trained
direction classifier, same chronological-holdout discipline. Long-only to
start, same "start conservative" posture every strategy in this codebase
began with before any bidirectional extension was considered.

Genuinely different from alpaca_strategy.py in its order mechanics, not
just a renamed copy: Alpaca crypto orders do NOT support order_class=
"bracket" or stop orders at all (confirmed via Alpaca's own docs -- crypto
is restricted to market/limit/stop-limit, time_in_force gtc/ioc only). So
unlike the equities strategy (which lets Alpaca's own bracket order manage
the take-profit/stop-loss live on the exchange), this strategy manages
exits itself via its own poll loop -- decide_exit checks the current price
against the position's stored TP/SL/max-hold levels, and a plain market
sell order is placed when triggered. This is the SAME shape Kalshi perps
already uses (perps_strategy.py's own manage_open_positions), since Kalshi
has no broker-native bracket order either -- a genuine convergence, not
extra complexity introduced for its own sake.

Position sizing uses NOTIONAL (dollar amount) rather than a share count --
crypto supports fractional sizing down to 1e-9, so unlike equities (which
can leave a small account unable to afford even one whole share of some
names), a crypto position is never blocked by price alone.
"""
from __future__ import annotations

import datetime as dt
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


# Real gap found in review (same fix as alpaca_strategy.py's own copy of
# this): requiring a volume spike AND a volatility-ratio jump AND a dip,
# all three simultaneously, made real entries rare -- confirmed live,
# almost every cycle across the whole 36-pair universe skipped on "volume
# not unusual enough" before the dip signal was even checked.
# perps_strategy.py's own decide_entry_technical has NO volume/volatility
# gate at all -- just the dip/rally read plus a soft trend filter. Off by
# default now (env-overridable back on): MIN_VOLUME_Z=-1e9 makes
# "z < MIN_VOLUME_Z" unsatisfiable (a z-score is never remotely this large)
# -- NOT float("-inf"): a real, confirmed production bug found live --
# Python's json module happily serializes float("-inf") as the bare token
# `-Infinity`, invalid JSON grammar, so every browser's JSON.parse() threw
# a SyntaxError on every /api/alpaca/crypto/status poll, silently breaking
# this dashboard's entire auto-refresh loop (caught by its own try/catch,
# so the page just sat frozen on its initial "--" placeholders forever) --
# and MIN_VOLATILITY_RATIO=0.0 makes
# "ratio < MIN_VOLATILITY_RATIO" unsatisfiable (a ratio is always >= 0) --
# both checks below become genuine no-ops at these defaults.
MIN_VOLUME_Z = _env_float("ALPACA_CRYPTO_MIN_VOLUME_Z", -1e9)
MIN_VOLATILITY_RATIO = _env_float("ALPACA_CRYPTO_MIN_VOLATILITY_RATIO", 0.0)
ENTRY_DIP_PCT = _env_float("ALPACA_CRYPTO_ENTRY_DIP_PCT", 0.0015)
SHORT_MA_MINUTES = _env_int("ALPACA_CRYPTO_SHORT_MA_MINUTES", 15)

TAKE_PROFIT_PCT = _env_float("ALPACA_CRYPTO_TAKE_PROFIT_PCT", 0.01)
STOP_LOSS_PCT = _env_float("ALPACA_CRYPTO_STOP_LOSS_PCT", 0.008)
MAX_HOLD_MINUTES = _env_int("ALPACA_CRYPTO_MAX_HOLD_MINUTES", 120)

MODEL_CONFIDENCE_MIN = _env_float("ALPACA_CRYPTO_MODEL_CONFIDENCE_MIN", 0.55)

POSITION_SIZE_PCT = _env_float("ALPACA_CRYPTO_POSITION_SIZE_PCT", 0.45)
MAX_CONCURRENT_POSITIONS = max(1, _env_int("ALPACA_CRYPTO_MAX_CONCURRENT_POSITIONS", 2))
DAILY_LOSS_CAP_PCT = _env_float("ALPACA_CRYPTO_DAILY_LOSS_CAP_PCT", 0.10)

LIVE_TRADING_ENABLED = str(os.getenv("ALPACA_CRYPTO_LIVE_TRADING_ENABLED", "")).strip().lower() in {"1", "true", "yes"}


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
        # every other strategy here during the first days of data collection).
        result["should_enter"] = True

    return result


def decide_exit(
    position: dict[str, Any], current_price: float, *, now: dt.datetime | None = None,
) -> tuple[bool, str]:
    """Long-only: a RISING price is favorable."""
    entry_price = float(position["entry_price"])
    change_pct = (current_price - entry_price) / entry_price if entry_price > 0 else 0.0

    if change_pct >= TAKE_PROFIT_PCT:
        return True, f"take_profit ({change_pct:+.3%})"
    if change_pct <= -STOP_LOSS_PCT:
        return True, f"stop_loss ({change_pct:+.3%})"

    opened_at = dt.datetime.fromisoformat(position["opened_at"])
    now = now if now is not None else dt.datetime.now(dt.timezone.utc)
    held_minutes = (now - opened_at).total_seconds() / 60.0
    if held_minutes >= MAX_HOLD_MINUTES:
        return True, f"max_hold_time ({held_minutes:.0f}min, {change_pct:+.3%})"
    return False, f"holding ({change_pct:+.3%}, {held_minutes:.0f}min)"


def position_exit_levels(position: dict[str, Any]) -> dict[str, float]:
    entry_price = float(position["entry_price"])
    return {
        "take_profit_price": round(entry_price * (1 + TAKE_PROFIT_PCT), 6),
        "stop_loss_price": round(entry_price * (1 - STOP_LOSS_PCT), 6),
    }


def compute_position_notional(available_balance_usd: float) -> float:
    """A dollar amount, not a share/coin count -- crypto orders accept
    `notional` directly, so unlike equities there's no whole-share
    affordability problem to work around."""
    return round(max(0.0, available_balance_usd) * POSITION_SIZE_PCT, 2)


# ---------------------------------------------------------------------------
# Two modes, one codebase -- same dual-gate posture as every other strategy
# here: "simulate" (default) tracks a virtual balance and places no real
# orders; "live" places real orders, still gated by LIVE_TRADING_ENABLED.
# ---------------------------------------------------------------------------
MODE = os.getenv("ALPACA_CRYPTO_MODE", "simulate").strip().lower()
SIMULATE_STARTING_BALANCE = _env_float("ALPACA_CRYPTO_SIMULATE_STARTING_BALANCE", 500.0)

ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT_DIR / "data"
STATE_FILE = Path(os.getenv("ALPACA_CRYPTO_STATE_FILE", str(DATA_DIR / "alpaca_crypto_state.json")))
_STATE_LOCK = threading.RLock()

HF_API_KEY = os.getenv("HF_API_KEY", "")
HF_ALPACA_CRYPTO_MODEL_REPO = os.getenv("HF_ALPACA_CRYPTO_MODEL_REPO", "papylove/alpaca-crypto-model")
_DURABLE_STATE_HF_FILENAME = "alpaca_crypto_durable_state.json"
_DURABLE_PUSH_MIN_INTERVAL_SEC = 30
_last_durable_push_ts = 0.0


def _today_str() -> str:
    return dt.datetime.now(dt.timezone.utc).date().isoformat()


def _durable_state_slice(state: dict[str, Any]) -> dict[str, Any]:
    return {
        "balance": state.get("balance", SIMULATE_STARTING_BALANCE),
        "positions": state.get("positions") or [],
        "trade_log": state.get("trade_log") or [],
        "realized_pnl_by_date": state.get("realized_pnl_by_date") or {},
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
                repo_id=HF_ALPACA_CRYPTO_MODEL_REPO, repo_type="model", commit_message="update alpaca crypto durable state",
            )
        finally:
            os.unlink(tmp_path)
    except Exception as exc:
        logger.warning("[alpaca_crypto_strategy] durable state push to HF failed: %s", exc)


_DURABLE_STATE_HF_TIMEOUT_SEC = int(os.getenv("ALPACA_CRYPTO_DURABLE_STATE_HF_TIMEOUT_SEC", "10") or "10")


def _pull_durable_state_from_hf() -> dict[str, Any] | None:
    if not HF_API_KEY:
        return None

    def _download() -> dict[str, Any]:
        from huggingface_hub import hf_hub_download
        path = hf_hub_download(
            repo_id=HF_ALPACA_CRYPTO_MODEL_REPO, filename=_DURABLE_STATE_HF_FILENAME, repo_type="model", token=HF_API_KEY,
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
        logger.info("[alpaca_crypto_strategy] no durable state on HF yet (or fetch failed): %s", exc)
        return None


def _load_state() -> dict[str, Any]:
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)
    except Exception:
        base = {
            "balance": SIMULATE_STARTING_BALANCE, "positions": [], "trade_log": [], "realized_pnl_by_date": {},
        }
        durable = _pull_durable_state_from_hf()
        if durable:
            base.update(durable)
            logger.info("[alpaca_crypto_strategy] recovered durable state from HF after local state was missing")
        return base
    state.setdefault("balance", SIMULATE_STARTING_BALANCE)
    state.setdefault("positions", [])
    state.setdefault("trade_log", [])
    state.setdefault("realized_pnl_by_date", {})
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
    """simulate mode: tracked virtual balance minus whatever's already
    committed to open positions. live mode: the real Alpaca cash balance
    (shared with the equities strategy's own account -- crypto and stock
    buying power both draw from the same underlying cash)."""
    if MODE == "live":
        from data import alpaca_client
        account = alpaca_client.get_account()
        return float(account.get("cash") or 0.0)
    state = _load_state()
    committed = sum(float(p["entry_price"]) * float(p["count"]) for p in (state.get("positions") or []))
    return max(0.0, float(state.get("balance", SIMULATE_STARTING_BALANCE)) - committed)


def get_current_price(symbol: str) -> float | None:
    """live mode: a real-time-ish bid/ask quote. simulate mode ALSO uses
    the real quote -- "simulate" means no real orders are placed, not
    that market data is faked."""
    from data import alpaca_client
    try:
        quote = alpaca_client.get_crypto_latest_quote(symbol)
        ask, bid = quote.get("ap"), quote.get("bp")
        if ask and bid:
            return (float(ask) + float(bid)) / 2.0
        price = ask or bid
        return float(price) if price else None
    except Exception as exc:
        logger.warning("[alpaca_crypto_strategy] quote fetch failed for %s: %s", symbol, exc)
        return None


def scan_and_enter(symbols: list[str] | None = None, *, dry_run: bool | None = None) -> dict[str, Any]:
    """Evaluates each crypto pair for a new entry. "simulate" mode always
    paper-trades; "live" mode places a real plain market buy order
    (notional-sized) UNLESS dry_run resolves True -- same dual-gate
    posture as every other strategy here. No market-hours gating: crypto
    trades 24/7."""
    from data.alpaca_crypto_data import fetch_recent_crypto_bars, get_crypto_universe, latest_feature_row
    from data.alpaca_crypto_model import predict_direction
    from data import threads_post

    effective_dry_run = (not LIVE_TRADING_ENABLED) if dry_run is None else dry_run
    if symbols is None:
        symbols = get_crypto_universe()

    opened: list[dict[str, Any]] = []
    with _STATE_LOCK:
        state = _load_state()
        existing_symbols = {p["symbol"] for p in (state.get("positions") or [])}
        open_count = len(state.get("positions") or [])
        reference_balance = float(state.get("balance", SIMULATE_STARTING_BALANCE))
        today_pnl = float((state.get("realized_pnl_by_date") or {}).get(_today_str(), 0.0))
    if reference_balance > 0 and today_pnl <= -abs(DAILY_LOSS_CAP_PCT) * reference_balance:
        return {"opened": [], "action": "daily_loss_cap_breached"}

    for symbol in symbols:
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
            notional = compute_position_notional(available_balance)
            if notional < 1.0 or entry_price <= 0:
                opened.append({"symbol": symbol, "ok": True, "action": "skipped_insufficient_budget"})
                continue
            count = notional / entry_price

            levels = position_exit_levels({"entry_price": entry_price})
            order_id = None
            if MODE == "live" and not effective_dry_run:
                from data import alpaca_client
                order_spec = alpaca_client.build_crypto_order(symbol=symbol, side="buy", notional=notional)
                order_id = alpaca_client.place_order(order_spec)

            position = {
                "symbol": symbol, "entry_price": entry_price, "count": count,
                "opened_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                "order_id": order_id, **levels,
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
            trade_dry_run = effective_dry_run if MODE == "live" else True
            opened.append({
                "symbol": symbol, "ok": True, "action": "opened", "entry_price": entry_price,
                "notional": notional, "dry_run": trade_dry_run,
            })
            try:
                threads_post.post_trade_entry(
                    ticker=symbol, side="long", entry_price=entry_price,
                    take_profit_price=levels["take_profit_price"], stop_loss_price=levels["stop_loss_price"],
                    reason=candidate["reason"], dry_run=trade_dry_run, market="crypto",
                )
            except Exception:
                logger.warning("[alpaca_crypto_strategy] Threads post for %s entry failed", symbol, exc_info=True)
            try:
                one_min_df = fetch_recent_crypto_bars(symbol)
                threads_post.maybe_post_trade_entry_chart(
                    ticker=symbol, market="crypto", closes=list(one_min_df["close"].tail(90)),
                    entry_price=entry_price, take_profit_price=levels["take_profit_price"],
                    stop_loss_price=levels["stop_loss_price"], side="long", dry_run=trade_dry_run,
                )
            except Exception:
                logger.warning("[alpaca_crypto_strategy] Threads chart post for %s entry failed", symbol, exc_info=True)
        except Exception as exc:
            opened.append({"symbol": symbol, "ok": False, "action": "entry_failed", "error": str(exc)})

    return {"opened": opened}


def manage_open_positions(*, dry_run: bool | None = None) -> dict[str, Any]:
    """Checks every open position for an exit. Unlike the equities
    strategy, there is no broker-native bracket order to reconcile
    against here -- crypto orders don't support them at all, so this
    loop's own decide_exit check is the ONLY thing that ever closes a
    crypto position. A triggered exit places a plain market sell for the
    position's stored (approximate, since a notional buy's exact fill
    quantity isn't synchronously confirmed) coin count."""
    from data import threads_post

    effective_dry_run = (not LIVE_TRADING_ENABLED) if dry_run is None else dry_run
    with _STATE_LOCK:
        state = _load_state()
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

            if MODE == "live" and not effective_dry_run:
                from data import alpaca_client
                try:
                    order_spec = alpaca_client.build_crypto_order(symbol=symbol, side="sell", qty=float(position["count"]))
                    alpaca_client.place_order(order_spec)
                except Exception as exc:
                    logger.warning("[alpaca_crypto_strategy] close order failed for %s: %s", symbol, exc)

            gross = round((current_price - float(position["entry_price"])) * float(position["count"]), 6)
            with _STATE_LOCK:
                state = _load_state()
                state["balance"] = round(float(state.get("balance", SIMULATE_STARTING_BALANCE)) + gross, 6)
                by_date = state.setdefault("realized_pnl_by_date", {})
                today = _today_str()
                by_date[today] = round(float(by_date.get(today, 0.0)) + gross, 6)
                trade = {
                    "closed_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                    "symbol": symbol, "entry_price": position["entry_price"], "exit_price": current_price,
                    "count": position["count"], "realized_pnl_usd": gross, "reason": reason,
                    "dry_run": effective_dry_run if MODE == "live" else True,
                }
                state.setdefault("trade_log", []).append(trade)
                state["positions"] = [p for p in (state.get("positions") or []) if p["symbol"] != symbol]
                _save_state(state, push_durable=True)
            remaining_symbols.discard(symbol)
            closed.append(trade)
            try:
                threads_post.post_trade_exit(
                    ticker=symbol, side="long", entry_price=float(position["entry_price"]), exit_price=current_price,
                    pnl_usd=gross, reason=reason, dry_run=trade["dry_run"], market="crypto",
                )
            except Exception:
                logger.warning("[alpaca_crypto_strategy] Threads post for %s exit failed", symbol, exc_info=True)
        except Exception as exc:
            logger.warning("[alpaca_crypto_strategy] could not process position for %s -- leaving untouched this cycle: %s", symbol, exc)
            checks.append({"symbol": symbol, "ok": False, "error": str(exc)})

    return {"action": "closed" if closed else "no_change", "closed": closed, "checks": checks}
