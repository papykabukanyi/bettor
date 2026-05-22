from __future__ import annotations

from data.history_generic_sport import collect_sport_history_from_espn


def collect_golf_history(days_back: int = 180) -> dict:
    """Golf collector with PGA summary stats where available."""
    return collect_sport_history_from_espn(
        sport_tag="golf",
        espn_paths=["golf/pga"],
        league_fallback="Golf",
        days_back=days_back,
    )
