from __future__ import annotations

from data.history_generic_sport import collect_sport_history_from_espn


def collect_nfl_history(days_back: int = 120) -> dict:
    """NFL collector with per-game player outcomes from ESPN boxscores."""
    return collect_sport_history_from_espn(
        sport_tag="football",
        espn_paths=["football/nfl", "football/college-football"],
        league_fallback="NFL",
        days_back=days_back,
    )
