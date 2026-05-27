from __future__ import annotations

from data.history_generic_sport import collect_sport_history_from_espn
from data.tennis_data_sources import (
    build_tennis_history_rows,
    fetch_espn_tennis_live_bundle,
    load_tennis_reference_rows,
)


def collect_tennis_history(days_back: int = 180) -> dict:
    """Tennis collector (ATP/WTA) with per-game ESPN boxscore outcomes when available."""
    reference_rows = load_tennis_reference_rows()
    built_rows = build_tennis_history_rows(reference_rows) if reference_rows else {"game_rows": [], "player_rows": []}
    espn_rows = collect_sport_history_from_espn(
        sport_tag="tennis",
        espn_paths=["tennis/atp", "tennis/wta"],
        league_fallback="Tennis",
        days_back=days_back,
    )

    # Keep the ESPN fallback live bundle available to callers that want scoreboard context.
    try:
        espn_live = fetch_espn_tennis_live_bundle()
    except Exception:
        espn_live = {"games": [], "player_rows": []}

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

    game_rows = _merge_rows(
        built_rows.get("game_rows"),
        espn_rows.get("game_rows"),
        key_fields=("game_key", "game_date", "home_team", "away_team"),
    )
    player_rows = _merge_rows(
        built_rows.get("player_rows"),
        espn_rows.get("player_rows"),
        key_fields=("game_key", "game_date", "player_name", "stat_type", "source"),
    )
    injury_rows = _merge_rows(
        espn_rows.get("injury_rows"),
        espn_live.get("player_rows"),
        key_fields=("player_name", "team", "status", "source"),
    )

    return {
        "game_rows": game_rows,
        "player_rows": player_rows,
        "injury_rows": injury_rows,
        "live_rows": espn_live.get("games") or [],
        "live_player_rows": espn_live.get("player_rows") or [],
    }
