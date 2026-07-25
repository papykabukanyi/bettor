"""Schwab equities/ETF trading strategy -- separate from and independent of
the Kalshi perps bot's strategy module. Long-only to start (matching the
same "start conservative, prove it out, then extend" posture the perps bot
used before enabling shorts) -- short-selling stocks needs margin approval
and carries materially different risk that hasn't been asked for here.

Unlike Kalshi Perps (0.8% taker fee per leg, ~1.6% round trip -- the single
biggest lesson from that bot this session), Schwab has charged $0 commission
on online equity/ETF trades since 2019 -- a real, structural difference that
means this strategy isn't fighting the same uphill battle against fees. The
real (small) costs here are the bid-ask spread and tiny regulatory fees
(SEC fee on sells, FINRA TAF), not modeled explicitly since they're a
rounding error next to Kalshi's 1.6%.

Entry: a volume/volatility "something is happening right now" signal
(dollar_volume_z spike + volatility above the symbol's own recent baseline)
combined with a short-term momentum read and (once trained) the direction
model's confidence -- mirrors decide_entry_technical/evaluate_candidate's
shape in perps_strategy.py.
"""
from __future__ import annotations

import datetime as dt
import os
from typing import Any


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


# "Something is happening right now": dollar_volume_z is how many standard
# deviations above this symbol's OWN recent (60-min) average dollar volume
# the current 5/15-min window is -- a spike here means real, unusual
# interest, not just noise. Combined with the volatility ratio so a
# volume spike accompanied by real price movement (not just a quiet block
# trade) is what actually qualifies.
MIN_VOLUME_Z = _env_float("SCHWAB_MIN_VOLUME_Z", 1.5)
MIN_VOLATILITY_RATIO = _env_float("SCHWAB_MIN_VOLATILITY_RATIO", 1.3)  # volatility_5 / volatility_30

# "Enter on a small pullback in an otherwise-active name, not a random tick"
ENTRY_DIP_PCT = _env_float("SCHWAB_ENTRY_DIP_PCT", 0.002)
SHORT_MA_MINUTES = _env_int("SCHWAB_SHORT_MA_MINUTES", 15)

TAKE_PROFIT_PCT = _env_float("SCHWAB_TAKE_PROFIT_PCT", 0.01)
STOP_LOSS_PCT = _env_float("SCHWAB_STOP_LOSS_PCT", 0.008)
MAX_HOLD_MINUTES = _env_int("SCHWAB_MAX_HOLD_MINUTES", 120)

MODEL_CONFIDENCE_MIN = _env_float("SCHWAB_MODEL_CONFIDENCE_MIN", 0.55)

POSITION_SIZE_PCT = _env_float("SCHWAB_POSITION_SIZE_PCT", 0.10)
MAX_CONCURRENT_POSITIONS = max(1, _env_int("SCHWAB_MAX_CONCURRENT_POSITIONS", 5))
DAILY_LOSS_CAP_PCT = _env_float("SCHWAB_DAILY_LOSS_CAP_PCT", 0.10)

LIVE_TRADING_ENABLED = str(os.getenv("SCHWAB_LIVE_TRADING_ENABLED", "")).strip().lower() in {"1", "true", "yes"}


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
    return True, f"volume spike (z={dollar_volume_z:.2f}) + dip ({dip_pct:+.3%})"


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
    """Whole shares only (no fractional-share assumption) -- floor division
    of the position's dollar budget by price."""
    if price <= 0:
        return 0
    budget = available_balance_usd * POSITION_SIZE_PCT
    return int(budget // price)
