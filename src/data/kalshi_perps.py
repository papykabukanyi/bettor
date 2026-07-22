"""Kalshi Perps (perpetual futures / margin) API client.

Perps are leveraged (e.g. KXBTCPERP carries roughly 6x embedded leverage per
Kalshi's own `leverage_estimate`), and positions can be automatically
liquidated if a margin-ratio threshold is breached
(GetRiskParameters.liquidation_margin_ratio_threshold). This module is a thin,
faithful client only -- it does not decide position sizing, does not gate on
dry-run, and does not implement any strategy. All of that safety logic lives
in perps_strategy.py, on purpose, so the "am I allowed to place a real order"
decision is never buried inside a generic API wrapper.

Per Kalshi's docs, every /margin/* endpoint lives on the SAME base host and
uses the SAME RSA-PSS request signing as the rest of the Kalshi API
(KALSHI-ACCESS-KEY / KALSHI-ACCESS-SIGNATURE / KALSHI-ACCESS-TIMESTAMP), so
this reuses `_request_json` from kalshi_client.py directly rather than
introducing a second connection/auth mechanism.

Endpoints (confirmed live against the real account, 2026-07):
    GET  /margin/enabled                -> {"enabled": bool}
    GET  /margin/exchange/status        -> {"exchange_active", "trading_active"}
    GET  /margin/balance                -> subaccount_balances[], settled_funds
    GET  /margin/risk                   -> account_leverage, total_position_notional,
                                            total_maintenance_margin, positions[]
    GET  /margin/risk_parameters        -> liquidation_margin_ratio_threshold,
                                            queue_entry_margin_ratio_threshold,
                                            initial_margin_multiplier{}
    GET  /margin/positions              -> positions[] (entry_price, unrealized_pnl,
                                            margin_used, roe, ...)
    GET  /margin/markets                -> markets[] -- ALL perp instruments
                                            (16 confirmed live: BTC, ETH, SOL,
                                            XRP, DOGE, LTC, BCH, LINK, SUI,
                                            NEAR, DOT, HBAR, HYPE, kSHIB, XLM, ZEC)
    GET  /margin/markets/{ticker}       -> price, bid, ask, tick_size, contract_size,
                                            leverage_estimate
    GET  /margin/markets/{ticker}/candlesticks -> OHLC candles, period_interval in
                                            minutes: 1, 60, or 1440
    GET  /margin/orders                 -> list orders (filter by ticker/status)
    POST /margin/orders                 -> create order (ticker, side, count, price, ...)
    DELETE /margin/orders/{order_id}    -> cancel order
"""
from __future__ import annotations

from typing import Any

from data.kalshi_client import _request_json

BTC_PERP_TICKER = "KXBTCPERP"

# Every perp instrument Kalshi listed as of 2026-07 (confirmed live via
# GET /margin/markets against the real account). Used as a fallback watchlist
# if the live listing call fails; list_margin_markets() is always preferred
# when it succeeds since Kalshi can add/remove instruments over time.
KNOWN_PERP_TICKERS = [
    "KXBTCPERP", "KXETHPERP", "KXSOLPERP", "KXXRPPERP", "KXDOGEPERP",
    "KXLTCPERP", "KXBCHPERP", "KXLINKPERP", "KXSUIPERP", "KXNEARPERP",
    "KXDOTPERP", "KXHBARPERP", "KXHYPEPERP", "KXKSHIBPERP", "KXXLMPERP",
    "KXZECPERP",
]


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


def list_margin_markets(*, status: str | None = None) -> list[dict[str, Any]]:
    """All perp instruments Kalshi currently lists (ticker, price, bid/ask,
    tick_size, contract_size, leverage_estimate, status, ...). This is the
    live source of truth for "all the instruments it has" -- prefer it over
    KNOWN_PERP_TICKERS, which is only a fallback if this call fails."""
    params: dict[str, Any] = {}
    if status:
        params["status"] = str(status)
    data = _request_json("GET", "/margin/markets", params=params or None, auth=True)
    markets = data.get("markets", [])
    return markets if isinstance(markets, list) else []


def get_margin_market(ticker: str = BTC_PERP_TICKER) -> dict[str, Any]:
    return _request_json("GET", f"/margin/markets/{ticker}", auth=True)


def get_margin_candlesticks(
    ticker: str, *, start_ts: int, end_ts: int, period_interval: int, include_latest_before_start: bool = False,
) -> dict[str, Any]:
    """period_interval is in MINUTES -- Kalshi only allows 1 (1-minute), 60
    (hourly), or 1440 (daily). This is the actual "different smaller
    timeframe" data source."""
    if period_interval not in (1, 60, 1440):
        raise ValueError(f"period_interval must be 1, 60, or 1440 (minutes), got {period_interval}")
    params: dict[str, Any] = {
        "start_ts": int(start_ts), "end_ts": int(end_ts), "period_interval": int(period_interval),
    }
    if include_latest_before_start:
        params["include_latest_before_start"] = True
    return _request_json("GET", f"/margin/markets/{ticker}/candlesticks", params=params, auth=True)


def get_margin_orders(
    *, ticker: str | None = None, status: str | None = None, subaccount: int | None = None, limit: int = 200,
) -> dict[str, Any]:
    params: dict[str, Any] = {"limit": max(1, min(int(limit), 10000))}
    if ticker:
        params["ticker"] = str(ticker)
    if status:
        params["status"] = str(status)
    if subaccount is not None:
        params["subaccount"] = int(subaccount)
    return _request_json("GET", "/margin/orders", params=params, auth=True)


def create_margin_order(
    *, ticker: str, side: str, count: float, price: float,
    client_order_id: str, time_in_force: str = "immediate_or_cancel",
    reduce_only: bool = False, post_only: bool = False, subaccount: int | None = None,
) -> dict[str, Any]:
    """side: 'bid' (buy) or 'ask' (sell/short). This function places a REAL
    order with no safety checks of its own -- every caller (the strategy
    module) is responsible for dry-run gating and position-size limits
    BEFORE calling this."""
    if side not in ("bid", "ask"):
        raise ValueError("side must be 'bid' or 'ask'")
    payload: dict[str, Any] = {
        "ticker": ticker,
        "client_order_id": client_order_id,
        "side": side,
        "count": f"{float(count):.2f}",
        "price": f"{float(price):.4f}",
        "time_in_force": time_in_force,
        "self_trade_prevention_type": "taker_at_cross",
        "post_only": bool(post_only),
        "reduce_only": bool(reduce_only),
    }
    if subaccount is not None:
        payload["subaccount"] = int(subaccount)
    return _request_json("POST", "/margin/orders", payload=payload, auth=True)


def cancel_margin_order(order_id: str) -> dict[str, Any]:
    return _request_json("DELETE", f"/margin/orders/{order_id}", auth=True)


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
    _run("margin_markets", list_margin_markets)
    return result
