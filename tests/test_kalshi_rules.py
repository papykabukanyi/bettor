"""Kalshi order-safety rules: combos must be exactly 2 legs with non-negative
EV, and a bet with no parseable date must never be matched to a market purely
on name similarity (real risk: matching the wrong day's game for a team that
plays frequently). kalshi.py imports `cryptography` lazily inside its signing
functions, so these tests -- which never sign anything -- don't need it
installed."""
from __future__ import annotations

import inspect

import data.kalshi as kalshi


def test_combo_defaults_are_exactly_two_legs_nonnegative_ev():
    sig = inspect.signature(kalshi.suggest_combo_bets)
    assert sig.parameters["max_legs"].default == 2
    assert sig.parameters["min_legs"].default == 2
    assert sig.parameters["min_ev"].default == 0.0


def test_suggest_combo_bets_never_returns_more_than_two_legs():
    bets = [
        {"uid": "a", "model_prob": 0.65, "dec_odds": 1.8, "game": "A@B", "bet_type": "moneyline"},
        {"uid": "b", "model_prob": 0.60, "dec_odds": 1.9, "game": "C@D", "bet_type": "moneyline"},
        {"uid": "c", "model_prob": 0.70, "dec_odds": 1.7, "game": "E@F", "bet_type": "moneyline"},
    ]
    resolutions = {
        uid: {"status": "matched", "market_ticker": f"TICKER-{uid}"}
        for uid in ("a", "b", "c")
    }
    combos = kalshi.suggest_combo_bets(bets, resolutions=resolutions, max_combos=50)
    for combo in combos:
        assert len(combo["legs"]) == 2, "every combo must have exactly 2 legs, never 3+"


def test_bet_with_no_parseable_date_is_refused_not_matched_by_name_alone():
    bet = {
        "sport": "mlb", "home_team": "New York Yankees", "away_team": "Boston Red Sox",
        "bet_type": "moneyline", "pick": "New York Yankees",
        # deliberately no game_date / game_time / scheduled_start
    }
    result = kalshi._resolve_single_bet(bet, markets=[], event_index={})
    assert result["status"] == "unavailable"
    assert "date" in result["message"].lower()
