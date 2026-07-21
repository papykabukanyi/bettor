"""Bankroll pacing: once available Kalshi buying power runs out, remaining
due games must be deferred (retried next cycle) rather than either crashing
or attempting an order the account can't cover. This exercises the exact
$10-bankroll / $1-per-bet scenario the bot is meant to run under."""
from __future__ import annotations

import data.kalshi_trade_api as kalshi_trade_api
import data.pregame_timing as pregame_timing


def _row(uid, confidence=0.75):
    return {
        "schedule_uid": uid,
        "predictions_json": [{"prediction_id": uid, "home_team": "A", "away_team": "B", "sport": "mlb"}],
        "confidence": confidence,
        "confidence_tier": "elite",
    }


def test_bets_deferred_not_crashed_once_capital_runs_out(monkeypatch, tmp_path):
    monkeypatch.setattr(pregame_timing, "_load_schedule_cache", lambda: {"rows": []})
    monkeypatch.setattr(pregame_timing, "_save_schedule_cache", lambda rows: None)
    monkeypatch.setattr(pregame_timing, "_push_schedule_rows", lambda rows: True)
    monkeypatch.setattr(kalshi_trade_api, "get_available_buying_power_usd", lambda: 2.50)

    call_count = {"n": 0}

    def fake_submit(*a, **k):
        call_count["n"] += 1
        return {"ok": True}

    monkeypatch.setattr(pregame_timing, "submit_prediction_orders", fake_submit)

    # $2.50 available, 5 games each wanting $1 single + $1 combo = $2/game.
    # Only the first game should fit; the rest must be deferred, not crashed.
    rows = [_row(f"g{i}") for i in range(5)]
    result = pregame_timing._place_due_games(
        rows, dry_run=False, stake_usd=1.0, max_single_orders=1, max_combo_orders=1, include_combos=True,
    )

    assert result["placed"] == 1
    assert result["deferred"] == 4
    assert call_count["n"] == 1, "submit_prediction_orders must not be called once capital is exhausted"
    assert result["available_capital_usd"] == 2.50


def test_dry_run_is_never_capital_gated(monkeypatch):
    monkeypatch.setattr(pregame_timing, "_load_schedule_cache", lambda: {"rows": []})
    monkeypatch.setattr(pregame_timing, "_save_schedule_cache", lambda rows: None)
    monkeypatch.setattr(pregame_timing, "_push_schedule_rows", lambda rows: True)

    def fail_if_called():
        raise AssertionError("buying power should never be checked in dry-run")
    monkeypatch.setattr(kalshi_trade_api, "get_available_buying_power_usd", fail_if_called)
    monkeypatch.setattr(pregame_timing, "submit_prediction_orders", lambda *a, **k: {"ok": True, "dry_run": True})

    rows = [_row(f"g{i}") for i in range(3)]
    result = pregame_timing._place_due_games(
        rows, dry_run=True, stake_usd=1.0, max_single_orders=1, max_combo_orders=1, include_combos=True,
    )
    assert result["placed"] == 3
    assert result["deferred"] == 0
