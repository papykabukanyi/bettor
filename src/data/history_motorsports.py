from __future__ import annotations

from data.history_generic_sport import collect_sport_history_from_espn


def collect_motorsports_history(days_back: int = 180) -> dict:
    """Motorsports collector (F1/Nascar) with ESPN event summaries where available."""
    return collect_sport_history_from_espn(
        sport_tag="motorsports",
        espn_paths=["racing/f1", "racing/nascar"],
        league_fallback="Motorsports",
        days_back=days_back,
    )
