from __future__ import annotations

from data.history_generic_sport import collect_sport_history_from_espn


def collect_tennis_history(days_back: int = 180) -> dict:
    """Tennis collector (ATP/WTA) with per-game ESPN boxscore outcomes when available."""
    return collect_sport_history_from_espn(
        sport_tag="tennis",
        espn_paths=["tennis/atp", "tennis/wta"],
        league_fallback="Tennis",
        days_back=days_back,
    )
