from __future__ import annotations

from data.history_generic_sport import collect_sport_history_from_espn
from data.golf_data_sources import build_golf_history_rows, load_golf_reference_rows


def collect_golf_history(days_back: int = 180) -> dict:
    """Golf collector with PGA summary stats where available."""
    reference_rows = load_golf_reference_rows()
    built_rows = build_golf_history_rows(reference_rows) if reference_rows else {"game_rows": [], "player_rows": []}

    espn_rows = collect_sport_history_from_espn(
        sport_tag="golf",
        espn_paths=["golf/pga"],
        league_fallback="Golf",
        days_back=days_back,
    )

    def _merge_rows(left, right, *, key_fields):
        merged = []
        seen = set()
        for row in (left or []) + (right or []):
            if not isinstance(row, dict):
                continue
            key = tuple(str(row.get(field) or "") for field in key_fields)
            if key in seen:
                continue
            seen.add(key)
            merged.append(row)
        return merged

    return {
        "sport": "golf",
        "game_rows": _merge_rows(
            built_rows.get("game_rows"),
            espn_rows.get("game_rows"),
            key_fields=("game_key", "game_date", "home_team", "away_team"),
        ),
        "player_rows": _merge_rows(
            built_rows.get("player_rows"),
            espn_rows.get("player_rows"),
            key_fields=("game_key", "game_date", "player_name", "stat_type", "source"),
        ),
        "injury_rows": espn_rows.get("injury_rows") or [],
    }
