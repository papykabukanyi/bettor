from __future__ import annotations

from data.history_generic_sport import collect_sport_history_from_espn


def collect_nhl_history(days_back: int = 120) -> dict:
    """NHL collector with per-game player outcomes from ESPN boxscores."""
    return collect_sport_history_from_espn(
        sport_tag="hockey",
        espn_paths=["hockey/nhl"],
        league_fallback="NHL",
        days_back=days_back,
    )
