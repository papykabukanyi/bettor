from __future__ import annotations

import datetime
from typing import Any

from data.history_generic_sport import collect_sport_history_from_espn
from data.wnba_data_sources import (
    fetch_basketball_reference_wnba_history,
    fetch_wnba_live_game_bundle,
    load_kaggle_wnba_datasets,
)


def _merge_unique_game_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for r in rows:
        if not isinstance(r, dict):
            continue
        key = str(r.get("game_key") or "").strip().lower()
        if not key:
            away = str(r.get("away_team") or "").strip().lower()
            home = str(r.get("home_team") or "").strip().lower()
            gdate = str(r.get("game_date") or "").strip().lower()
            key = f"{gdate}:{away}@{home}"
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


def collect_wnba_history(days_back: int = 365) -> dict:
    """WNBA deep collector using ESPN + wnba_stats_api + Basketball Reference + Kaggle CSVs."""
    base = collect_sport_history_from_espn(
        sport_tag="wnba",
        espn_paths=["basketball/wnba"],
        league_fallback="WNBA",
        days_back=days_back,
    )

    game_rows = list(base.get("game_rows") or [])
    player_rows = list(base.get("player_rows") or [])
    injury_rows = list(base.get("injury_rows") or [])

    # 1) Live same-day context from wnba_stats_api (scores, box, play-by-play)
    today = datetime.date.today().strftime("%m/%d/%Y")
    live_bundle = fetch_wnba_live_game_bundle(today)
    for g in live_bundle.get("games") or []:
        if not isinstance(g, dict):
            continue
        gid = str(g.get("game_id") or "").strip()
        home = str(g.get("home_team") or "").strip()
        away = str(g.get("away_team") or "").strip()
        gdate = str(g.get("game_date") or datetime.date.today().isoformat())[:10]
        if not gid or not home or not away:
            continue
        game_rows.append(
            {
                "sport": "wnba",
                "league": "WNBA",
                "season": int(gdate[:4]) if gdate[:4].isdigit() else None,
                "game_date": gdate,
                "game_key": f"wnba_stats:{gid}",
                "home_team": home,
                "away_team": away,
                "home_score": g.get("home_score"),
                "away_score": g.get("away_score"),
                "status": str(g.get("status") or "In Progress"),
                "source": "wnba_stats_api",
                "raw_json": g,
            }
        )

        for p in (live_bundle.get("box_scores") or {}).get(gid, []) or []:
            if not isinstance(p, dict):
                continue
            player = str(p.get("player_name") or "").strip()
            if not player:
                continue
            for stat_type in ("points", "rebounds", "assists", "steals", "blocks", "turnovers"):
                val = p.get(stat_type)
                if val is None:
                    continue
                player_rows.append(
                    {
                        "sport": "wnba",
                        "season": int(gdate[:4]) if gdate[:4].isdigit() else None,
                        "game_date": gdate,
                        "game_key": f"wnba_stats:{gid}",
                        "player_name": player,
                        "team": str(p.get("team") or ""),
                        "stat_type": stat_type,
                        "stat_value": float(val),
                        "source": "wnba_stats_api",
                        "raw_json": p,
                    }
                )

    # 2) Basketball Reference historical games (1997 -> today)
    for r in fetch_basketball_reference_wnba_history() or []:
        if isinstance(r, dict):
            game_rows.append(r)

    # 3) Kaggle local pre-cleaned CSVs for ML training
    kaggle = load_kaggle_wnba_datasets()
    game_rows.extend(kaggle.get("game_rows") or [])
    player_rows.extend(kaggle.get("player_rows") or [])

    return {
        "sport": "wnba",
        "game_rows": _merge_unique_game_rows(game_rows),
        "player_rows": player_rows,
        "injury_rows": injury_rows,
    }
