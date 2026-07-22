"""Kalshi Perps (perpetual futures / margin) -- READ-ONLY client.

Perps are a fundamentally different product from the event-contract markets
the rest of this bot trades: they are leveraged, and positions can be
automatically liquidated if a margin-ratio threshold is breached
(GetRiskParameters.liquidation_margin_ratio_threshold). There is no
order-placement code in this file on purpose -- this module exists to answer
one question safely first: is margin/perps trading even enabled on this
account, and what does its current risk picture look like.

Per Kalshi's docs, every /margin/* endpoint lives on the SAME base host and
uses the SAME RSA-PSS request signing as the existing event-contract API
(KALSHI-ACCESS-KEY / KALSHI-ACCESS-SIGNATURE / KALSHI-ACCESS-TIMESTAMP), so
this reuses `_request_json` from kalshi_trade_api.py directly rather than
introducing a second connection/auth mechanism.

Endpoints (confirmed via https://docs.kalshi.com, 2026-07):
    GET /margin/enabled                -> {"enabled": bool}
    GET /margin/exchange/status        -> {"exchange_active", "trading_active"}
    GET /margin/balance                -> subaccount_balances[], settled_funds
    GET /margin/risk                   -> account_leverage, total_position_notional,
                                           total_maintenance_margin, positions[]
    GET /margin/risk_parameters        -> liquidation_margin_ratio_threshold,
                                           queue_entry_margin_ratio_threshold,
                                           initial_margin_multiplier{}
    GET /margin/positions              -> positions[] (entry_price, unrealized_pnl,
                                           margin_used, roe, ...)
"""
from __future__ import annotations

from typing import Any

from data.kalshi_trade_api import _request_json


def get_margin_enabled() -> dict[str, Any]:
    """Whether margin/perps trading is enabled for this account at all.
    Perps production access is a phased, member-by-member rollout -- this is
    the first thing to check before anything else."""
    return _request_json("GET", "/margin/enabled", auth=True)


def get_margin_exchange_status() -> dict[str, Any]:
    return _request_json("GET", "/margin/exchange/status", auth=False)


def get_margin_balance(*, subaccount: int | None = None, compute_available_balance: bool = False) -> dict[str, Any]:
    params: dict[str, Any] = {}
    if subaccount is not None:
        params["subaccount"] = int(subaccount)
    if compute_available_balance:
        params["compute_available_balance"] = True
    return _request_json("GET", "/margin/balance", params=params or None, auth=True)


def get_margin_risk() -> dict[str, Any]:
    """account_leverage, total_position_notional, total_maintenance_margin,
    and per-position estimated_liquidation_price -- the actual danger picture."""
    return _request_json("GET", "/margin/risk", auth=True)


def get_margin_risk_parameters() -> dict[str, Any]:
    """System-wide liquidation_margin_ratio_threshold / queue_entry_margin_ratio_threshold
    and per-market initial_margin_multiplier -- needed before sizing ANY position."""
    return _request_json("GET", "/margin/risk_parameters", auth=True)


def get_margin_positions(*, subaccount: int | None = None, ticker: str | None = None) -> dict[str, Any]:
    params: dict[str, Any] = {}
    if subaccount is not None:
        params["subaccount"] = int(subaccount)
    if ticker:
        params["ticker"] = str(ticker)
    return _request_json("GET", "/margin/positions", params=params or None, auth=True)


def run_connectivity_check() -> dict[str, Any]:
    """Read-only, zero-order connectivity + eligibility check against the real
    account. Answers: is margin enabled, is the margin exchange open, what's
    the account's current balance/risk/position state. Never places an order.
    """
    result: dict[str, Any] = {"ok": True, "checks": {}}

    def _run(name: str, fn) -> None:
        try:
            result["checks"][name] = {"ok": True, "data": fn()}
        except Exception as exc:
            result["checks"][name] = {"ok": False, "error": str(exc)}
            result["ok"] = False

    _run("margin_enabled", get_margin_enabled)
    _run("margin_exchange_status", get_margin_exchange_status)
    _run("margin_balance", lambda: get_margin_balance(compute_available_balance=True))
    _run("margin_risk", get_margin_risk)
    _run("margin_risk_parameters", get_margin_risk_parameters)
    _run("margin_positions", get_margin_positions)
    return result
