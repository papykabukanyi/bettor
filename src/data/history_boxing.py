from __future__ import annotations

from data.history_generic_sport import collect_sport_history_from_espn


def collect_boxing_history(days_back: int = 180) -> dict:
    """Boxing collector with ESPN fight summaries where available."""
    return collect_sport_history_from_espn(
        sport_tag="boxing",
        espn_paths=["boxing/boxing"],
        league_fallback="Boxing",
        days_back=days_back,
    )
