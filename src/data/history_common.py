from __future__ import annotations

import datetime
from typing import Any


def iter_lookback_days(days_back: int) -> list[datetime.date]:
    days = max(1, int(days_back or 1))
    today = datetime.date.today()
    return [today - datetime.timedelta(days=i) for i in range(days)]


def build_player_rows_from_season_stats(
    *,
    sport: str,
    stats: list[dict[str, Any]],
    source: str,
    fallback_season: int | None = None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for s in stats or []:
        if not isinstance(s, dict):
            continue
        player_name = str(s.get("player_name") or "").strip()
        if not player_name:
            continue
        season = s.get("season") or fallback_season
        stat_blob = s.get("stats_json") if isinstance(s.get("stats_json"), dict) else {}

        numeric_keys = [
            k for k, v in (stat_blob or {}).items()
            if isinstance(v, (int, float))
        ][:8]
        if not numeric_keys:
            # Keep at least one row per player so they exist in unified table.
            rows.append({
                "sport": sport,
                "season": season,
                "game_date": None,
                "game_key": f"season:{season}:{player_name}",
                "player_name": player_name,
                "team": s.get("team"),
                "stat_type": str(s.get("stat_group") or "season_profile"),
                "stat_value": None,
                "source": source,
                "raw_json": stat_blob,
            })
            continue

        for k in numeric_keys:
            rows.append({
                "sport": sport,
                "season": season,
                "game_date": None,
                "game_key": f"season:{season}:{player_name}",
                "player_name": player_name,
                "team": s.get("team"),
                "stat_type": str(k),
                "stat_value": float(stat_blob.get(k)),
                "source": source,
                "raw_json": stat_blob,
            })
    return rows


def build_injury_rows_from_history(sport: str, injury_rows: list[dict[str, Any]], source_fallback: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for r in injury_rows or []:
        if not isinstance(r, dict):
            continue
        player = str(r.get("player_name") or "").strip()
        if not player:
            continue
        fetched = str(r.get("fetched_at") or "")
        injury_date = fetched[:10] if fetched else datetime.date.today().isoformat()
        out.append({
            "sport": sport,
            "injury_date": injury_date,
            "team": str(r.get("team") or "").strip(),
            "player_name": player,
            "status": str(r.get("status") or "").strip(),
            "injury_type": str(r.get("injury_type") or "").strip(),
            "detail": str(r.get("description") or "").strip(),
            "source": str(r.get("source") or source_fallback),
            "raw_json": r,
        })
    return out
