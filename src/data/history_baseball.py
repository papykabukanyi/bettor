from __future__ import annotations

from data.history_generic_sport import collect_sport_history_from_espn


def collect_baseball_history(days_back: int = 180) -> dict:
    """Baseball collector (MLB focus) with per-game ESPN boxscore player outcomes."""
    return collect_sport_history_from_espn(
        sport_tag="baseball",
        espn_paths=["baseball/mlb"],
        league_fallback="MLB",
        days_back=days_back,
    )
