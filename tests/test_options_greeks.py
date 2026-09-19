"""Black-Scholes pricing/IV/Greeks -- pure math, no network/state. Primary
correctness checks lean on exact mathematical identities (put-call parity,
an IV round-trip) rather than hand-computed reference numbers, so they
can't be wrong from a transcription/arithmetic slip the way copying a
textbook value into an assertion could be."""
from __future__ import annotations

import math

import pytest

from data import options_greeks as og


def test_put_call_parity_holds():
    """Call - Put == S - K*exp(-r*T) is an exact identity for European
    options regardless of sigma -- the strongest, least-gameable
    correctness check available for a from-scratch Black-Scholes
    implementation."""
    S, K, T, r, sigma = 100.0, 95.0, 0.5, 0.03, 0.25
    call = og.black_scholes_price(S, K, T, r, sigma, "call")
    put = og.black_scholes_price(S, K, T, r, sigma, "put")
    assert call - put == pytest.approx(S - K * math.exp(-r * T))


def test_at_the_money_zero_rate_call_and_put_are_equal():
    """A real, easy-to-verify-by-hand special case: S == K and r == 0 makes
    d2 == -d1, so N(d2) == N(-d1) and the call/put formulas collapse to the
    same value -- confirms the d1/d2 formula itself, not just parity."""
    price_call = og.black_scholes_price(100.0, 100.0, 1.0, 0.0, 0.2, "call")
    price_put = og.black_scholes_price(100.0, 100.0, 1.0, 0.0, 0.2, "put")
    assert price_call == pytest.approx(price_put, abs=1e-9)
    assert price_call > 0


def test_black_scholes_price_returns_none_for_an_expired_option():
    assert og.black_scholes_price(100.0, 100.0, 0.0, 0.05, 0.2, "call") is None
    assert og.black_scholes_price(100.0, 100.0, -1.0, 0.05, 0.2, "call") is None


def test_black_scholes_price_returns_none_for_non_positive_inputs():
    assert og.black_scholes_price(0.0, 100.0, 1.0, 0.05, 0.2, "call") is None
    assert og.black_scholes_price(100.0, 0.0, 1.0, 0.05, 0.2, "call") is None
    assert og.black_scholes_price(100.0, 100.0, 1.0, 0.05, 0.0, "call") is None


def test_implied_volatility_round_trips_a_known_sigma():
    """Price a contract at a known sigma, then solve IV back out of that
    same price -- should recover (near) the original sigma. The single
    strongest test of the bisection solver's own correctness."""
    S, K, T, r, true_sigma = 100.0, 105.0, 0.25, 0.04, 0.35
    price = og.black_scholes_price(S, K, T, r, true_sigma, "call")
    recovered = og.implied_volatility(price, S, K, T, r, "call")
    assert recovered == pytest.approx(true_sigma, abs=1e-4)


def test_implied_volatility_round_trips_for_a_put_too():
    S, K, T, r, true_sigma = 100.0, 95.0, 0.1, 0.02, 0.6
    price = og.black_scholes_price(S, K, T, r, true_sigma, "put")
    recovered = og.implied_volatility(price, S, K, T, r, "put")
    assert recovered == pytest.approx(true_sigma, abs=1e-4)


def test_implied_volatility_returns_none_below_intrinsic_value():
    """A real market price can never be below intrinsic value -- if it
    is, the quote is stale/crossed/bad, and this must refuse to
    hallucinate a sigma for it rather than return nonsense."""
    # Deep ITM call: intrinsic = 100 - 50 = 50, quoting it at 10 is
    # impossible for any real market.
    assert og.implied_volatility(10.0, 100.0, 50.0, 0.5, 0.05, "call") is None


def test_implied_volatility_returns_none_for_a_non_positive_price():
    assert og.implied_volatility(0.0, 100.0, 100.0, 0.5, 0.05, "call") is None
    assert og.implied_volatility(-5.0, 100.0, 100.0, 0.5, 0.05, "call") is None


def test_greeks_call_delta_is_between_zero_and_one():
    g = og.greeks(100.0, 100.0, 0.5, 0.03, 0.25, "call")
    assert 0.0 < g["delta"] < 1.0


def test_greeks_put_delta_is_between_negative_one_and_zero():
    g = og.greeks(100.0, 100.0, 0.5, 0.03, 0.25, "put")
    assert -1.0 < g["delta"] < 0.0


def test_greeks_gamma_and_vega_are_always_positive():
    """True for every plain-vanilla option, call or put -- both measure
    curvature/sensitivity magnitude, never direction."""
    call = og.greeks(100.0, 100.0, 0.5, 0.03, 0.25, "call")
    put = og.greeks(100.0, 100.0, 0.5, 0.03, 0.25, "put")
    assert call["gamma"] > 0
    assert put["gamma"] > 0
    assert call["vega"] > 0
    assert put["vega"] > 0
    # Gamma and vega don't depend on call-vs-put at all (only d1 does) --
    # a real property of the formulas, worth locking in.
    assert call["gamma"] == pytest.approx(put["gamma"], abs=1e-9)
    assert call["vega"] == pytest.approx(put["vega"], abs=1e-9)


def test_greeks_theta_is_negative_for_a_long_option_near_the_money():
    """Time decay: a long option near the money loses value as each day
    passes, all else equal -- the single most decision-relevant Greek for
    a hold-minutes-bounded strategy."""
    call = og.greeks(100.0, 100.0, 0.1, 0.03, 0.25, "call")
    put = og.greeks(100.0, 100.0, 0.1, 0.03, 0.25, "put")
    assert call["theta"] < 0
    assert put["theta"] < 0


def test_greeks_returns_none_for_invalid_inputs():
    assert og.greeks(100.0, 100.0, 0.0, 0.05, 0.2, "call") is None
    assert og.greeks(100.0, 100.0, 1.0, 0.05, 0.0, "call") is None


def test_contract_features_returns_none_without_a_real_bid_ask():
    assert og.contract_features(
        underlying_price=100.0, strike=100.0, days_to_expiration=30, option_type="call",
        bid=None, ask=5.0,
    ) is None
    assert og.contract_features(
        underlying_price=100.0, strike=100.0, days_to_expiration=30, option_type="call",
        bid=0.0, ask=5.0,
    ) is None


def test_contract_features_returns_the_full_feature_set_with_valid_inputs():
    result = og.contract_features(
        underlying_price=100.0, strike=98.0, days_to_expiration=21, option_type="call",
        bid=4.5, ask=4.9,
    )
    assert result is not None
    for key in ("implied_volatility", "moneyness", "days_to_expiration", "bid_ask_spread_pct",
                "delta", "gamma", "theta", "vega", "rho"):
        assert key in result
    assert result["moneyness"] == pytest.approx(100.0 / 98.0)
    assert result["days_to_expiration"] == 21
    assert result["bid_ask_spread_pct"] == pytest.approx((4.9 - 4.5) / 4.7)


def test_contract_features_returns_none_when_iv_cannot_be_solved():
    """A bid/ask straddling a price below intrinsic value (bad/stale quote)
    -- contract_features must propagate implied_volatility's own None
    rather than half-populate the row."""
    result = og.contract_features(
        underlying_price=100.0, strike=50.0, days_to_expiration=30, option_type="call",
        bid=1.0, ask=2.0,  # way below the ~50 of real intrinsic value
    )
    assert result is None
