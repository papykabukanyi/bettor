from __future__ import annotations

from data.history_generic_sport import collect_sport_history_from_espn


def collect_cricket_history(days_back: int = 180) -> dict:
    """Cricket collector (IPL + global) with ESPN summaries where available."""
    return collect_sport_history_from_espn(
        sport_tag="cricket",
        espn_paths=["cricket/ipl", "cricket/icc"],
        league_fallback="Cricket",
        days_back=days_back,
    )
