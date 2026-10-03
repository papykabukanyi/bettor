"""Keeps the Kalshi bots' cash split the way the user chose (2026-10-03,
"auto-split 50/50"): perps trade from the margined account, the 15m bot from
the event-contract account's Crypto & Commodities shard (exchange index 2,
docs.kalshi.com/getting_started/exchange_sharding). Kalshi's app moves money
between them for app orders; API orders don't, so money deposited or left on
one side never reaches the other bot on its own.

Every REBALANCE_MINUTES, with live trading on for both bots:

  - Which pool perps' margin collateral is: the event-contract account's
    exchange 0 or a separate margined account. Kalshi's docs don't say, so
    the first run moves one cent from event-contract exchange 0 to 2 and
    watches the margin balance; the answer is kept.
  - Free cash per bot: perps' margin available balance (the same number
    perps sizes against), the 15m bot's exchange-2 balance; with a separate
    margined account, event-contract exchange 0 is idle cash too.
  - Move toward SHARE_15M of the total (POST
    /portfolio/intra_exchange_instance_transfer, amount in centicents),
    only when the split is off by more than REBALANCE_BAND of the total and
    MIN_TRANSFER_USD, and never out of a bot that has an open position --
    money backing a live trade stays where it is.

Every transfer is logged (history()) and shown on both dashboards.
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import os
import threading
import time
from typing import Any

from server_common import DATA_DIR

logger = logging.getLogger(__name__)

ENABLED = os.getenv("KALSHI_AUTO_SPLIT_ENABLED", "1").strip().lower() in {"1", "true", "yes", "on"}
SHARE_15M = float(os.getenv("KALSHI_SPLIT_15M_SHARE", "0.5") or "0.5")
REBALANCE_MINUTES = int(os.getenv("KALSHI_SPLIT_REBALANCE_MINUTES", "10") or "10")
REBALANCE_BAND = float(os.getenv("KALSHI_SPLIT_REBALANCE_BAND", "0.10") or "0.10")
MIN_TRANSFER_USD = float(os.getenv("KALSHI_SPLIT_MIN_TRANSFER_USD", "1.0") or "1.0")
FIFTEEN_SHARD = 2
STATE_PATH = DATA_DIR / "kalshi_funding.json"
_lock = threading.Lock()


def _load() -> dict[str, Any]:
    try:
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {"pool": None, "transfers": [], "last": None}


def _save(state: dict[str, Any]) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    state["transfers"] = state.get("transfers", [])[-200:]
    tmp = STATE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, default=str), encoding="utf-8")
    os.replace(tmp, STATE_PATH)


def balances() -> dict[str, Any]:
    """Event-contract balance per exchange index and perps' margin
    available balance, in dollars."""
    from data import kalshi_15m
    from data.kalshi_perps import get_margin_balance
    ec = kalshi_15m.get_balance_by_shard(exchange_index=FIFTEEN_SHARD)
    by_shard = {int(b["exchange_index"]): float(b["balance"]) for b in ec.get("balance_breakdown") or []}
    if FIFTEEN_SHARD not in by_shard:
        by_shard[FIFTEEN_SHARD] = float(ec.get("balance_dollars") or 0.0)
    margin = get_margin_balance(compute_available_balance=True)
    subs = margin.get("subaccount_balances") or []
    return {"event_contract": by_shard,
            "margined_available": max([float(s.get("available_balance") or 0.0) for s in subs] or [0.0]),
            "margined_position_value": sum(abs(float(s.get("position_value") or 0.0)) for s in subs)}


def transfer(amount_usd: float, *, source: tuple[str, int], destination: tuple[str, int]) -> dict[str, Any]:
    """One real transfer between the account's own exchange instances."""
    from data.kalshi_client import _request_json
    centicents = int(round(amount_usd * 10000))
    payload = {"source": source[0], "source_exchange_shard": int(source[1]),
               "destination": destination[0], "destination_exchange_shard": int(destination[1]), "amount": centicents}
    result = _request_json("POST", "/portfolio/intra_exchange_instance_transfer", payload=payload, auth=True)
    return {"transfer_id": result.get("transfer_id"), "amount_usd": round(amount_usd, 4), "source": list(source),
            "destination": list(destination), "at": dt.datetime.now(dt.timezone.utc).isoformat()}


def _open_positions() -> dict[str, bool]:
    from data import kalshi_15m_strategy, perps_strategy
    try:
        perps_open = bool(perps_strategy._load_state().get("positions"))  # noqa: SLF001
    except Exception:
        perps_open = True  # unknown: treat as busy, never pull its money
    try:
        k15_open = bool(kalshi_15m_strategy._load_state().get("positions"))  # noqa: SLF001
    except Exception:
        k15_open = True
    return {"perps": perps_open, "kalshi15m": k15_open}


def detect_pool(state: dict[str, Any]) -> str:
    """'event_contract' when perps' margin collateral is the event-contract
    account's exchange 0, else 'margined'. Moves one cent from exchange 0
    to exchange 2 once and watches the margin balance (the cent stays with
    the 15m bot)."""
    if state.get("pool"):
        return state["pool"]
    before = balances()
    if before["event_contract"].get(0, 0.0) < 0.01:
        state["pool"] = "margined"  # nothing on exchange 0: perps can only be the margined account
        return state["pool"]
    t = transfer(0.01, source=("event_contract", 0), destination=("event_contract", FIFTEEN_SHARD))
    time.sleep(3)
    after = balances()
    dropped = before["margined_available"] - after["margined_available"]
    state["pool"] = "event_contract" if dropped > 0.005 else "margined"
    state.setdefault("transfers", []).append({**t, "why": "pool probe", "margined_drop_usd": round(dropped, 4)})
    return state["pool"]


def plan(b: dict[str, Any], pool: str, busy: dict[str, bool]) -> dict[str, Any]:
    """The transfers (if any) that bring the 15m bot to SHARE_15M of the
    free cash, never taking from a bot with an open position."""
    ec = b["event_contract"]
    k15 = ec.get(FIFTEEN_SHARD, 0.0)
    perps_src = ("event_contract", 0) if pool == "event_contract" else ("margined", 0)
    perps = ec.get(0, 0.0) if pool == "event_contract" else b["margined_available"]
    idle = 0.0 if pool == "event_contract" else ec.get(0, 0.0)  # unused event-contract cash on exchange 0
    total = perps + k15 + idle
    target = total * SHARE_15M
    out: dict[str, Any] = {"total_usd": round(total, 4), "perps_usd": round(perps, 4), "fifteen_usd": round(k15, 4),
                           "idle_usd": round(idle, 4), "target_15m_usd": round(target, 4), "moves": []}
    need = target - k15
    if abs(need) < max(MIN_TRANSFER_USD, REBALANCE_BAND * total):
        out["action"] = "in_band"
        return out
    dest_15m = ("event_contract", FIFTEEN_SHARD)
    if need > 0:
        if idle >= 0.01:
            take = min(idle, need)
            out["moves"].append({"amount_usd": take, "source": ("event_contract", 0), "destination": dest_15m, "why": "idle cash to 15m"})
            need -= take
        if need >= MIN_TRANSFER_USD:
            if busy["perps"]:
                out["action"] = "perps_has_open_positions"
            else:
                out["moves"].append({"amount_usd": min(need, perps), "source": perps_src, "destination": dest_15m, "why": "rebalance to 15m"})
    else:
        if busy["kalshi15m"]:
            out["action"] = "15m_has_open_positions"
        else:
            out["moves"].append({"amount_usd": min(-need, k15), "source": dest_15m, "destination": perps_src, "why": "rebalance to perps"})
    out.setdefault("action", "rebalance" if out["moves"] else "nothing_to_move")
    return out


def rebalance() -> dict[str, Any]:
    """One rebalancing pass (see the module docstring)."""
    from data import kalshi_15m_strategy, perps_strategy
    if not ENABLED:
        return {"ok": True, "action": "disabled"}
    if not (perps_strategy.LIVE_TRADING_ENABLED and kalshi_15m_strategy.LIVE_TRADING_ENABLED):
        return {"ok": True, "action": "live_trading_off"}
    with _lock:
        state = _load()
        try:
            pool = detect_pool(state)
            b = balances()
            p = plan(b, pool, _open_positions())
            done = []
            for mv in p["moves"]:
                if mv["amount_usd"] < 0.01:
                    continue
                t = transfer(mv["amount_usd"], source=mv["source"], destination=mv["destination"])
                done.append({**t, "why": mv["why"]})
            state.setdefault("transfers", []).extend(done)
            result = {"ok": True, "pool": pool, **{k: v for k, v in p.items() if k != "moves"}, "transfers": done,
                      "at": dt.datetime.now(dt.timezone.utc).isoformat()}
        except Exception as exc:
            logger.warning("[kalshi_funding] rebalance failed: %s", exc)
            result = {"ok": False, "error": str(exc)[:300], "at": dt.datetime.now(dt.timezone.utc).isoformat()}
        state["last"] = result
        _save(state)
    if result.get("transfers"):
        logger.info("[kalshi_funding] %s", result)
    return result


def status() -> dict[str, Any]:
    state = _load()
    return {"enabled": ENABLED, "share_15m": SHARE_15M, "pool": state.get("pool"), "last": state.get("last"),
            "recent_transfers": state.get("transfers", [])[-10:]}
