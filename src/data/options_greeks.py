"""A real options-pricing library -- Black-Scholes fair value, implied
volatility (solved from a real observed market price), and the Greeks
(delta/gamma/theta/vega/rho) -- so alpaca_options_model.py can finally
train on signal ABOUT THE OPTIONS MARKET ITSELF (how expensive/cheap this
contract's premium is relative to the underlying's own realized moves, how
fast it decays, how sensitive it is to a further move), not just the
underlying's own price-technical features it already shared with
alpaca_data.py.

Real, confirmed constraint that shaped this: Alpaca's own options snapshot
endpoint (/v1beta1/options/snapshots/{underlying}) returns NO greeks/
impliedVolatility fields on this account's subscription tier at all --
confirmed live via a direct curl (only dailyBar/latestQuote/latestTrade/
minuteBar come back), and the real-time OPRA feed this account would need
for Alpaca to compute those server-side is explicitly rejected
("subscription does not permit querying OPRA data"). Rather than block on
a paid subscription upgrade, this computes them locally from data already
being fetched for other reasons: the contract's own strike/expiration
(already pulled for contract SELECTION, see alpaca_options_data.select_contract),
its live bid/ask (a plain quote call, no OPRA needed), and the underlying's
own live price (already fetched for the equity-technical features).

Deliberately Black-Scholes (European), not a binomial/finite-difference
American model, despite every contract this account trades being
style="american" (see the option_contracts response) -- a real,
acknowledged simplification: American puts (and calls on a dividend-
paying underlying) carry a real early-exercise premium Black-Scholes
doesn't capture, but it's the industry-standard approximation for
short-dated, liquid, large-cap equity options (small early-exercise
premium relative to the bid/ask spread itself for the near-the-money,
near-term contracts this strategy actually selects), and building/
validating a full American pricer is a much larger undertaking than this
feature warrants. These are engineered FEATURES for a direction
classifier, not a live pricing/hedging system -- being directionally
correct and monotonic (higher IV -> higher vega, etc.) matters far more
here than matching a market-maker's own American-exercise-adjusted price
to the penny.

No new dependency: the normal CDF/PDF only need math.erf (stdlib) --
avoids pulling in py_vollib/scipy.stats just for this.
"""
from __future__ import annotations

import math
import os

# Approximate short-term risk-free rate (annualized) -- used as the
# discount rate `r` in Black-Scholes. A single constant, not a live
# Treasury-yield fetch: these Greeks are engineered features for a
# direction classifier, not a pricing system living or dying on basis-
# point precision, and every contract this strategy actually trades is
# short-dated (ALPACA_OPTIONS_MAX_DAYS_TO_EXPIRATION, 45 days by default)
# where `r`'s own effect on the price/Greeks is small. Overridable so it
# can be nudged without a code change if real-world rates move a lot.
RISK_FREE_RATE = float(os.getenv("OPTIONS_RISK_FREE_RATE", "0.045") or "0.045")

# Bisection bounds/precision for implied_volatility below -- 0.01% to
# 500% annualized vol covers every real equity option this strategy would
# ever select (a contract quoting outside this band is almost certainly a
# bad/stale quote, not a real 500%+ implied-vol regime), and 60 iterations
# of bisection is already far more precision than these features need
# (each halving the interval -- 60 iterations resolves the [0.0001, 5.0]
# band to under 1e-16, limited only by float precision long before that).
_IV_LOW, _IV_HIGH = 1e-4, 5.0
_IV_MAX_ITERATIONS = 60
_IV_TOLERANCE = 1e-6


def _norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _norm_pdf(x: float) -> float:
    return math.exp(-0.5 * x * x) / math.sqrt(2.0 * math.pi)


def _d1_d2(S: float, K: float, T: float, r: float, sigma: float) -> tuple[float, float]:
    d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    return d1, d2


def black_scholes_price(
    S: float, K: float, T: float, r: float, sigma: float, option_type: str,
) -> float | None:
    """Fair value of one option contract's PREMIUM PER SHARE (multiply by
    the contract's own multiplier, 100 for every standard equity contract,
    for the total premium). `T` in YEARS (days_to_expiration / 365.0).
    None on invalid inputs (expired/zero-DTE, non-positive price/vol) --
    callers treat that as "can't compute this one, skip it" rather than a
    hard failure, same graceful-degradation discipline as every other
    optional feature in this codebase."""
    if S <= 0 or K <= 0 or T <= 0 or sigma <= 0:
        return None
    try:
        d1, d2 = _d1_d2(S, K, T, r, sigma)
    except (ValueError, ZeroDivisionError):
        return None
    if option_type == "call":
        return S * _norm_cdf(d1) - K * math.exp(-r * T) * _norm_cdf(d2)
    return K * math.exp(-r * T) * _norm_cdf(-d2) - S * _norm_cdf(-d1)


def implied_volatility(
    market_price: float, S: float, K: float, T: float, r: float, option_type: str,
) -> float | None:
    """Solves for the sigma that makes black_scholes_price match
    `market_price` (the contract's own real observed mid-price, (bid+ask)/2)
    via bisection -- simple and robust (monotonic in sigma, so it can't
    converge to a wrong root the way Newton-Raphson occasionally can from a
    bad starting guess) over Newton-Raphson's usual speed advantage, which
    doesn't matter here (a few dozen scans/underlying every few minutes,
    nowhere near hot-path). None when the price is outside what's even
    theoretically achievable (e.g. below intrinsic value -- a real
    possibility from a stale/crossed quote) or every other input is invalid."""
    if market_price <= 0 or S <= 0 or K <= 0 or T <= 0:
        return None
    intrinsic = max(0.0, (S - K) if option_type == "call" else (K - S))
    if market_price < intrinsic:
        return None  # a real market price can't be below intrinsic value -- bad/stale quote

    low, high = _IV_LOW, _IV_HIGH
    price_low = black_scholes_price(S, K, T, r, low, option_type)
    price_high = black_scholes_price(S, K, T, r, high, option_type)
    if price_low is None or price_high is None:
        return None
    if not (price_low <= market_price <= price_high):
        return None  # observed price outside the achievable range for any vol in [_IV_LOW, _IV_HIGH]

    for _ in range(_IV_MAX_ITERATIONS):
        mid = (low + high) / 2.0
        price_mid = black_scholes_price(S, K, T, r, mid, option_type)
        if price_mid is None:
            return None
        if abs(price_mid - market_price) < _IV_TOLERANCE:
            return mid
        if price_mid < market_price:
            low = mid
        else:
            high = mid
    return (low + high) / 2.0


def greeks(S: float, K: float, T: float, r: float, sigma: float, option_type: str) -> dict[str, float] | None:
    """delta/gamma/theta/vega/rho for one contract. delta/gamma/vega are
    identical in form for calls and puts except delta's own sign; theta
    (per CALENDAR day, not per year -- divided by 365 here so it directly
    means "how much premium this contract loses to time decay each day,
    all else equal", the number that actually matters for a hold-minutes-
    bounded strategy) and rho differ by sign/term between the two. None on
    the same invalid-input conditions as black_scholes_price above."""
    if S <= 0 or K <= 0 or T <= 0 or sigma <= 0:
        return None
    try:
        d1, d2 = _d1_d2(S, K, T, r, sigma)
    except (ValueError, ZeroDivisionError):
        return None
    pdf_d1 = _norm_pdf(d1)
    sqrt_T = math.sqrt(T)
    gamma = pdf_d1 / (S * sigma * sqrt_T)
    vega = S * pdf_d1 * sqrt_T / 100.0  # per 1 VOL POINT (1%), the conventional quoting unit

    if option_type == "call":
        delta = _norm_cdf(d1)
        theta_annual = (
            -(S * pdf_d1 * sigma) / (2.0 * sqrt_T) - r * K * math.exp(-r * T) * _norm_cdf(d2)
        )
        rho = K * T * math.exp(-r * T) * _norm_cdf(d2) / 100.0
    else:
        delta = _norm_cdf(d1) - 1.0
        theta_annual = (
            -(S * pdf_d1 * sigma) / (2.0 * sqrt_T) + r * K * math.exp(-r * T) * _norm_cdf(-d2)
        )
        rho = -K * T * math.exp(-r * T) * _norm_cdf(-d2) / 100.0

    return {
        "delta": delta, "gamma": gamma, "theta": theta_annual / 365.0, "vega": vega, "rho": rho,
    }


def contract_features(
    *, underlying_price: float, strike: float, days_to_expiration: float, option_type: str,
    bid: float | None, ask: float | None, risk_free_rate: float = RISK_FREE_RATE,
) -> dict[str, float] | None:
    """One convenience entry point for the data-collection callers:
    derives IV from the contract's own real (bid+ask)/2 mid-quote, then
    every Greek from that IV -- the full set of options-specific features
    alpaca_options_data.py adds to a training/prediction row. None (not a
    partial dict) if the mid-quote is missing/non-positive or IV can't be
    solved (e.g. a stale/crossed quote) -- callers fill every one of these
    columns with NaN together rather than half-populate a row, same
    all-or-nothing discipline as the rest of this codebase's optional
    feature blocks."""
    if bid is None or ask is None or bid <= 0 or ask <= 0 or underlying_price <= 0 or strike <= 0:
        return None
    mid_price = (bid + ask) / 2.0
    T = days_to_expiration / 365.0
    if T <= 0:
        return None
    iv = implied_volatility(mid_price, underlying_price, strike, T, risk_free_rate, option_type)
    if iv is None:
        return None
    g = greeks(underlying_price, strike, T, risk_free_rate, iv, option_type)
    if g is None:
        return None
    return {
        "implied_volatility": iv,
        "moneyness": underlying_price / strike,
        "days_to_expiration": days_to_expiration,
        "bid_ask_spread_pct": (ask - bid) / mid_price if mid_price > 0 else None,
        **g,
    }
