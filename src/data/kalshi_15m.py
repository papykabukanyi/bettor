"""Kalshi 15-minute event-contract markets API client.

A genuinely different Kalshi product from kalshi_perps.py's leveraged
margin positions: each of these is a plain BINARY (yes/no) contract that
opens a fresh window every 15 minutes and settles definitively at close --
no leverage, no liquidation risk, no holding past expiration. Confirmed
live (2026-09-19) via GET /series?category=Crypto: 15 real, currently-open
series exist, one per underlying -- KXBTC15M, KXETH15M, KXSOL15M,
KXXRP15M, KXDOGE15M (the same 5 underlyings perps_strategy.py already
trades), plus KXADA15M/KXBNB15M/KXTON15M/KXZEC15M/KXNEAR15M/KXBCH15M/
KXHYPE15M and 2 cross-asset "race" markets not currently traded by any
market here.

Confirmed live (KXBTC15M's own open market, 2026-09-19): market_type is
"binary", resolution rule compares the 60-second-averaged CF Benchmarks
Real-Time Index at window open vs. window close (a straight "did price go
up over this 15-minute window" bet), fee_type is "quadratic" (a real,
confirmed DIFFERENCE from perps' own linear round_trip_fee_usd -- do NOT
reuse perps' fee formula here without re-deriving this product's own).

This uses the STANDARD (non-margin) trade-api/v2 surface -- a genuinely
different endpoint family from kalshi_perps.py's /margin/* ones, though
both share the SAME RSA-PSS request signing (KALSHI-ACCESS-KEY/
-SIGNATURE/-TIMESTAMP) via kalshi_client._request_json, so no second
auth mechanism is introduced. Endpoint paths and the order payload shape
below are cross-checked against Kalshi's own current docs (docs.kalshi.com)
AND against kalshi_perps.py's own already-proven-live create_margin_order
(whose payload shape -- side "bid"/"ask", count/price as decimal strings,
time_in_force, self_trade_prevention_type, post_only, reduce_only,
subaccount -- is IDENTICAL to the standard order schema below, strongly
suggesting Kalshi unified the two products' order schema under one
"V2" order API even though the margin and standard ones live at
different paths). NOT yet confirmed against a live authenticated call on
this specific account -- this dev machine's own local Kalshi credentials
are separately confirmed stale (even kalshi_perps.py's own known-working
calls 401 locally right now), a dev-machine-only problem distinct from
the live Space's own separately-configured, working credential (see
kalshi_perps.py's docstring for the established pattern: this module is a
thin, faithful client only, and the live Space is always the real
verification environment before anything here trades for real).

Endpoints used here:
    GET    /series?category=Crypto        -> list all series (used to
                                              discover live 15m series --
                                              see KNOWN_15M_SERIES below
                                              for the confirmed fallback)
    GET    /series/{ticker}               -> one series' own metadata
                                              (fee_type, settlement source)
    GET    /markets?series_ticker=X&status=open
                                           -> the currently-open market(s)
                                              for one series (normally
                                              exactly one at a time, since
                                              windows are sequential)
    GET    /portfolio/balance             -> balance_dollars, portfolio_value
    GET    /portfolio/positions           -> position_fp (signed: +yes/-no),
                                              market_exposure_dollars,
                                              realized_pnl_dollars
    GET    /portfolio/orders              -> list orders (filter by ticker/status)
    POST   /portfolio/events/orders       -> create order (same payload
                                              shape as create_margin_order)
    DELETE /portfolio/events/orders/{order_id} -> cancel order
"""
from __future__ import annotations

import datetime as dt
from typing import Any

from data.kalshi_client import _request_json

# Confirmed live via GET /series?category=Crypto (2026-09-19) -- used as a
# fallback if the live listing call fails, same "prefer the live call, fall
# back to a known-good snapshot" discipline as kalshi_perps.KNOWN_PERP_TICKERS.
# Scoped to the 5 underlyings perps_strategy.py already trades -- the other
# confirmed-live 15m series (ADA/BNB/TON/ZEC/NEAR/BCH/HYPE) are real but a
# deliberately separate decision (a new underlying, not just a new product
# on an existing one) left for later, not silently bundled in here.
KNOWN_15M_SERIES = {
    "BTC": "KXBTC15M", "ETH": "KXETH15M", "SOL": "KXSOL15M",
    "XRP": "KXXRP15M", "DOGE": "KXDOGE15M",
}


def list_series(*, category: str = "Crypto") -> list[dict[str, Any]]:
    data = _request_json("GET", "/series", params={"category": category})
    return data.get("series") or []


def get_series(series_ticker: str) -> dict[str, Any]:
    data = _request_json("GET", f"/series/{series_ticker}")
    return data.get("series") or {}


def list_open_markets(series_ticker: str, *, limit: int = 10) -> list[dict[str, Any]]:
    data = _request_json(
        "GET", "/markets", params={"series_ticker": series_ticker, "status": "open", "limit": limit},
    )
    return data.get("markets") or []


def get_market(ticker: str) -> dict[str, Any] | None:
    """One market by its own specific ticker (not a series) -- used for
    settlement checking: a PUBLIC, unauthenticated read that stays
    accurate regardless of the market's current status (open/closed/
    settled), unlike list_open_markets above which only ever returns
    markets still accepting orders."""
    data = _request_json("GET", "/markets", params={"tickers": ticker})
    markets = data.get("markets") or []
    return markets[0] if markets else None


def get_current_window_market(series_ticker: str) -> dict[str, Any] | None:
    """The single currently-tradable window for one series, if any --
    normally there's exactly one open market per series at a time (windows
    are sequential, non-overlapping), but this defensively picks the one
    with the SOONEST close_time in case Kalshi ever briefly overlaps two
    (e.g. right at a window boundary). None if nothing is open right now
    (a real, expected state between the previous window's close and the
    next one's open -- callers should treat this as "nothing to trade this
    tick", not an error)."""
    markets = list_open_markets(series_ticker)
    if not markets:
        return None
    return min(markets, key=lambda m: m.get("close_time") or "9999")


def seconds_to_close(market: dict[str, Any]) -> float | None:
    """How much real time is left before this market's window closes and
    it stops accepting orders -- None if close_time is missing/unparseable.
    Callers use this to avoid entering a position with only seconds left
    (no time for the position to reflect a real directional move, just
    paying the spread for a coin flip)."""
    close_time = market.get("close_time")
    if not close_time:
        return None
    try:
        close_dt = dt.datetime.fromisoformat(str(close_time).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None
    return (close_dt - dt.datetime.now(dt.timezone.utc)).total_seconds()


def get_portfolio_balance() -> dict[str, Any]:
    return _request_json("GET", "/portfolio/balance", auth=True)


def get_portfolio_positions(*, ticker: str | None = None) -> list[dict[str, Any]]:
    params: dict[str, Any] = {}
    if ticker:
        params["ticker"] = ticker
    data = _request_json("GET", "/portfolio/positions", params=params, auth=True)
    return data.get("market_positions") or []


def get_orders(*, ticker: str | None = None, status: str | None = None) -> list[dict[str, Any]]:
    params: dict[str, Any] = {}
    if ticker:
        params["ticker"] = ticker
    if status:
        params["status"] = status
    data = _request_json("GET", "/portfolio/orders", params=params, auth=True)
    return data.get("orders") or []


def create_order(
    *, ticker: str, side: str, count: float, price: float,
    client_order_id: str, time_in_force: str = "immediate_or_cancel",
    reduce_only: bool = False, post_only: bool = False,
) -> dict[str, Any]:
    """side: 'bid' (buy) or 'ask' (sell) -- see this module's own docstring
    for why this is the SAME payload shape as kalshi_perps.create_margin_order,
    just posted to the standard order endpoint instead of the margin one.
    This function places a REAL order with no safety checks of its own --
    every caller (the strategy module) is responsible for dry-run gating
    and position-size limits BEFORE calling this, matching
    create_margin_order's own identical contract."""
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
    return _request_json("POST", "/portfolio/events/orders", payload=payload, auth=True)


def cancel_order(order_id: str) -> dict[str, Any]:
    return _request_json("DELETE", f"/portfolio/events/orders/{order_id}", auth=True)
