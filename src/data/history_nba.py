from __future__ import annotations

from data.history_generic_sport import collect_sport_history_from_espn


def collect_nba_history(days_back: int = 120) -> dict:
    """NBA collector with per-game player outcomes from ESPN boxscores."""
    return collect_sport_history_from_espn(
        sport_tag="basketball",
        espn_paths=["basketball/nba", "basketball/wnba"],
        league_fallback="NBA",
        days_back=days_back,
    )
