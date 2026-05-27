from __future__ import annotations

from typing import Any


def _dedupe_player_rows_for_sport(player_rows: list[dict[str, Any]], sport_tag: str) -> list[dict[str, Any]]:
    """Drop duplicate player stat rows within a single sport ingest batch."""
    out: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str, str]] = set()
    sport_norm = str(sport_tag or "").strip().lower()

    for row in player_rows:
        if not isinstance(row, dict):
            continue
        key = (
            sport_norm,
            str(row.get("game_key") or "").strip().lower(),
            str(row.get("player_name") or "").strip().lower(),
            str(row.get("stat_type") or "").strip().lower(),
            str(row.get("source") or "").strip().lower(),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


def ingest_multi_sport_history(
    *,
    days_back: int = 180,
    sports: list[str] | tuple[str, ...] | None = None,
) -> dict[str, Any]:
    """Ingest NFL/NBA/NHL/soccer historical depth into unified training tables.

    This is a scaffolded orchestrator that calls dedicated sport modules and
    stores everything in:
      - training_game_history
      - training_player_history
      - training_injury_history
    """
    from data.db import (
        init_schema,
        save_training_game_history,
        save_training_injury_history,
        save_training_player_history,
    )

    selected = [
        str(s or "").strip().lower()
        for s in (sports or [
            "nfl", "nba", "nhl", "soccer",
            "baseball", "tennis", "boxing", "mma", "golf", "motorsports", "cricket",
        ])
    ]
    selected = [s for s in selected if s]

    # Ensure new unified tables exist.
    try:
        init_schema()
    except Exception:
        pass

    summary: dict[str, Any] = {
        "ok": True,
        "days_back": int(days_back),
        "sports": selected,
        "by_sport": {},
        "totals": {"games": 0, "players": 0, "injuries": 0},
    }

    collectors = {
        "nfl": ("football", "data.history_nfl", "collect_nfl_history"),
        "nba": ("basketball", "data.history_nba", "collect_nba_history"),
        "wnba": ("wnba", "data.history_wnba", "collect_wnba_history"),
        "nhl": ("hockey", "data.history_nhl", "collect_nhl_history"),
        "soccer": ("soccer", "data.history_soccer_deep", "collect_soccer_history"),
        "baseball": ("baseball", "data.history_baseball", "collect_baseball_history"),
        "tennis": ("tennis", "data.history_tennis", "collect_tennis_history"),
        "boxing": ("boxing", "data.history_boxing", "collect_boxing_history"),
        "mma": ("mma", "data.history_mma", "collect_mma_history"),
        "golf": ("golf", "data.history_golf", "collect_golf_history"),
        "motorsports": ("motorsports", "data.history_motorsports", "collect_motorsports_history"),
        "cricket": ("cricket", "data.history_cricket", "collect_cricket_history"),
        # aliases
        "football": ("football", "data.history_nfl", "collect_nfl_history"),
        "basketball": ("basketball", "data.history_nba", "collect_nba_history"),
        "hockey": ("hockey", "data.history_nhl", "collect_nhl_history"),
        "women_basketball": ("wnba", "data.history_wnba", "collect_wnba_history"),
        "mlb": ("baseball", "data.history_baseball", "collect_baseball_history"),
    }

    for key in selected:
        if key not in collectors:
            continue
        sport_tag, module_name, fn_name = collectors[key]
        try:
            mod = __import__(module_name, fromlist=[fn_name])
            fn = getattr(mod, fn_name)
            payload = fn(days_back=days_back) or {}

            game_rows = payload.get("game_rows") or []
            player_rows = _dedupe_player_rows_for_sport(payload.get("player_rows") or [], sport_tag)
            injury_rows = payload.get("injury_rows") or []

            saved_games = save_training_game_history(game_rows)
            saved_players = save_training_player_history(player_rows)
            saved_injuries = save_training_injury_history(injury_rows)

            summary["by_sport"][sport_tag] = {
                "input": {
                    "games": len(game_rows),
                    "players": len(player_rows),
                    "injuries": len(injury_rows),
                },
                "saved": {
                    "games": int(saved_games),
                    "players": int(saved_players),
                    "injuries": int(saved_injuries),
                },
            }
            summary["totals"]["games"] += int(saved_games)
            summary["totals"]["players"] += int(saved_players)
            summary["totals"]["injuries"] += int(saved_injuries)
        except Exception as exc:
            summary["ok"] = False
            summary["by_sport"][sport_tag] = {"error": str(exc)}

    return summary
