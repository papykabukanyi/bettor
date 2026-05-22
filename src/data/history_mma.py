from __future__ import annotations

from data.history_generic_sport import collect_sport_history_from_espn


def collect_mma_history(days_back: int = 180) -> dict:
    """MMA collector with UFC/ESPN summaries where available."""
    return collect_sport_history_from_espn(
        sport_tag="mma",
        espn_paths=["mma/ufc", "mma/pfl"],
        league_fallback="MMA",
        days_back=days_back,
    )
