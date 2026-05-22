from __future__ import annotations

import datetime
from typing import Any


def iter_lookback_days(days_back: int) -> list[datetime.date]:
    days = max(1, int(days_back or 1))
    today = datetime.date.today()
    return [today - datetime.timedelta(days=i) for i in range(days)]


def season_from_date(d: datetime.date | str | None) -> int | None:
    if d is None:
        return None
    if isinstance(d, datetime.date):
        return int(d.year)
    raw = str(d or "")[:4]
    try:
        return int(raw)
    except Exception:
        return None


def normalize_tsdb_event(event: dict[str, Any], *, sport: str, league_fallback: str, source: str) -> dict[str, Any] | None:
    if not isinstance(event, dict):
        return None
    home = str(event.get("strHomeTeam") or "").strip()
    away = str(event.get("strAwayTeam") or "").strip()
    game_date = str(event.get("dateEvent") or "").strip()
    if not home or not away or not game_date:
        return None

    hs_raw = event.get("intHomeScore")
    as_raw = event.get("intAwayScore")
    try:
        home_score = int(hs_raw) if hs_raw not in (None, "") else None
    except Exception:
        home_score = None
    try:
        away_score = int(as_raw) if as_raw not in (None, "") else None
    except Exception:
        away_score = None

    game_key = str(event.get("idEvent") or f"{away}@{home}#{game_date}")
    return {
        "sport": sport,
        "league": str(event.get("strLeague") or league_fallback),
        "season": season_from_date(game_date),
        "game_date": game_date[:10],
        "game_key": game_key,
        "home_team": home,
        "away_team": away,
        "home_score": home_score,
        "away_score": away_score,
        "status": str(event.get("strStatus") or "Scheduled"),
        "source": source,
        "raw_json": event,
    }


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
