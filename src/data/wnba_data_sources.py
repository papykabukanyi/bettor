from __future__ import annotations

import csv
import datetime
import glob
import os
import time
from typing import Any

import requests

from config import (
    WNBA_BREF_START_YEAR,
    WNBA_DATA_CACHE_TTL_SEC,
    WNBA_KAGGLE_DATA_DIR,
    WNBA_STATS_API_BASE,
    WNBA_STATS_API_TIMEOUT_SEC,
)

try:
    import pandas as pd
except Exception:  # pragma: no cover - optional fallback
    pd = None

_cache: dict[str, tuple[Any, float]] = {}


def _cached(key: str, ttl: int, fn, *args, **kwargs):
    now = time.time()
    found = _cache.get(key)
    if found and (now - found[1]) < ttl:
        return found[0]
    value = fn(*args, **kwargs)
    _cache[key] = (value, now)
    return value


def _as_int(value: Any) -> int | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        return int(float(value))
    except Exception:
        return None


def _as_float(value: Any) -> float | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        return float(value)
    except Exception:
        return None


def _wnba_headers() -> dict[str, str]:
    # stats.wnba.com blocks generic clients; these headers mirror browser usage.
    return {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://www.wnba.com/",
        "Origin": "https://www.wnba.com",
    }


def _wnba_get(path: str, params: dict[str, Any]) -> dict[str, Any] | None:
    base = str(WNBA_STATS_API_BASE or "https://stats.wnba.com/stats").rstrip("/")
    url = f"{base}/{path.lstrip('/')}"
    try:
        r = requests.get(url, params=params, headers=_wnba_headers(), timeout=max(4, int(WNBA_STATS_API_TIMEOUT_SEC or 10)))
        if r.status_code != 200:
            return None
        payload = r.json()
        return payload if isinstance(payload, dict) else None
    except Exception:
        return None


def _extract_result_set(payload: dict[str, Any], key: str = "resultSets") -> list[dict[str, Any]]:
    sets = payload.get(key)
    if isinstance(sets, dict):
        sets = [sets]
    out: list[dict[str, Any]] = []
    if not isinstance(sets, list):
        return out
    for rs in sets:
        if not isinstance(rs, dict):
            continue
        headers = rs.get("headers") or []
        rows = rs.get("rowSet") or []
        if not isinstance(headers, list) or not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, list):
                continue
            out.append({str(headers[i]): row[i] for i in range(min(len(headers), len(row)))})
    return out


def fetch_wnba_live_scoreboard(game_date: str | None = None) -> list[dict[str, Any]]:
    """Live scoreboard from wnba stats API."""
    date_str = game_date or datetime.date.today().strftime("%m/%d/%Y")

    def _pull() -> list[dict[str, Any]]:
        payload = _wnba_get(
            "scoreboardv2",
            {
                "GameDate": date_str,
                "LeagueID": "10",
                "DayOffset": 0,
            },
        )
        if not payload:
            return []
        rows = _extract_result_set(payload)
        out: list[dict[str, Any]] = []
        for r in rows:
            game_id = str(r.get("GAME_ID") or "").strip()
            if not game_id:
                continue
            out.append(
                {
                    "game_id": game_id,
                    "game_date": str(r.get("GAME_DATE_EST") or "")[:10],
                    "home_team": str(r.get("HOME_TEAM_NAME") or r.get("HOME_TEAM_ABBREVIATION") or "").strip(),
                    "away_team": str(r.get("VISITOR_TEAM_NAME") or r.get("VISITOR_TEAM_ABBREVIATION") or "").strip(),
                    "home_score": _as_int(r.get("HOME_TEAM_SCORE")),
                    "away_score": _as_int(r.get("VISITOR_TEAM_SCORE")),
                    "status": str(r.get("GAME_STATUS_TEXT") or "").strip(),
                    "source": "wnba_stats_api",
                    "raw_json": r,
                }
            )
        return out

    key = f"wnba_scoreboard::{date_str}"
    return _cached(key, max(30, int(WNBA_DATA_CACHE_TTL_SEC or 300)), _pull) or []


def fetch_wnba_box_score(game_id: str) -> list[dict[str, Any]]:
    """Traditional box score rows for a game id."""
    game_id_s = str(game_id or "").strip()
    if not game_id_s:
        return []

    def _pull() -> list[dict[str, Any]]:
        payload = _wnba_get(
            "boxscoretraditionalv2",
            {
                "GameID": game_id_s,
                "StartPeriod": 0,
                "EndPeriod": 0,
                "StartRange": 0,
                "EndRange": 0,
                "RangeType": 0,
            },
        )
        if not payload:
            return []
        rows = _extract_result_set(payload)
        out: list[dict[str, Any]] = []
        for r in rows:
            player_name = str(r.get("PLAYER_NAME") or "").strip()
            if not player_name:
                continue
            out.append(
                {
                    "game_id": game_id_s,
                    "team": str(r.get("TEAM_ABBREVIATION") or "").strip(),
                    "player_name": player_name,
                    "minutes": str(r.get("MIN") or "").strip(),
                    "points": _as_float(r.get("PTS")),
                    "rebounds": _as_float(r.get("REB")),
                    "assists": _as_float(r.get("AST")),
                    "steals": _as_float(r.get("STL")),
                    "blocks": _as_float(r.get("BLK")),
                    "turnovers": _as_float(r.get("TO")),
                    "source": "wnba_stats_api",
                    "raw_json": r,
                }
            )
        return out

    key = f"wnba_box::{game_id_s}"
    return _cached(key, max(30, int(WNBA_DATA_CACHE_TTL_SEC or 300)), _pull) or []


def fetch_wnba_play_by_play(game_id: str) -> list[dict[str, Any]]:
    """Play-by-play event rows for a game id."""
    game_id_s = str(game_id or "").strip()
    if not game_id_s:
        return []

    def _pull() -> list[dict[str, Any]]:
        payload = _wnba_get(
            "playbyplayv2",
            {
                "GameID": game_id_s,
                "StartPeriod": 0,
                "EndPeriod": 10,
            },
        )
        if not payload:
            return []
        rows = _extract_result_set(payload)
        out: list[dict[str, Any]] = []
        for r in rows:
            out.append(
                {
                    "game_id": game_id_s,
                    "event_num": _as_int(r.get("EVENTNUM")),
                    "period": _as_int(r.get("PERIOD")),
                    "pctimestring": str(r.get("PCTIMESTRING") or "").strip(),
                    "homedesc": str(r.get("HOMEDESCRIPTION") or "").strip(),
                    "visitordesc": str(r.get("VISITORDESCRIPTION") or "").strip(),
                    "neutraldesc": str(r.get("NEUTRALDESCRIPTION") or "").strip(),
                    "score": str(r.get("SCORE") or "").strip(),
                    "source": "wnba_stats_api",
                    "raw_json": r,
                }
            )
        return out

    key = f"wnba_pbp::{game_id_s}"
    return _cached(key, max(30, int(WNBA_DATA_CACHE_TTL_SEC or 300)), _pull) or []


def fetch_wnba_live_game_bundle(game_date: str | None = None) -> dict[str, Any]:
    games = fetch_wnba_live_scoreboard(game_date)
    box_scores: dict[str, list[dict[str, Any]]] = {}
    pbp_rows: dict[str, list[dict[str, Any]]] = {}
    for g in games:
        gid = str(g.get("game_id") or "").strip()
        if not gid:
            continue
        box_scores[gid] = fetch_wnba_box_score(gid)
        pbp_rows[gid] = fetch_wnba_play_by_play(gid)
    return {
        "games": games,
        "box_scores": box_scores,
        "play_by_play": pbp_rows,
    }


def fetch_basketball_reference_wnba_history(
    start_year: int | None = None,
    end_year: int | None = None,
) -> list[dict[str, Any]]:
    """Historical WNBA game logs from basketball-reference yearly games tables."""
    if pd is None:
        return []
    start = int(start_year or WNBA_BREF_START_YEAR or 1997)
    end = int(end_year or datetime.date.today().year)
    rows: list[dict[str, Any]] = []

    for year in range(start, end + 1):
        url = f"https://www.basketball-reference.com/wnba/years/{year}_games.html"
        try:
            dfs = pd.read_html(url)
        except Exception:
            continue
        if not dfs:
            continue
        df = dfs[0].copy()
        cols = {str(c): str(c).strip() for c in df.columns}
        df = df.rename(columns=cols)

        date_col = "Date" if "Date" in df.columns else None
        away_col = "Visitor/Neutral" if "Visitor/Neutral" in df.columns else None
        home_col = "Home/Neutral" if "Home/Neutral" in df.columns else None
        away_pts_col = "PTS" if "PTS" in df.columns else None
        home_pts_col = "PTS.1" if "PTS.1" in df.columns else None
        if not all([date_col, away_col, home_col]):
            continue

        for _, r in df.iterrows():
            away = str(r.get(away_col) or "").strip()
            home = str(r.get(home_col) or "").strip()
            date_raw = str(r.get(date_col) or "").strip()
            if not away or not home or not date_raw or away.lower() == "visitor/neutral":
                continue
            game_date = date_raw
            try:
                game_date = datetime.datetime.strptime(date_raw, "%a, %b %d, %Y").date().isoformat()
            except Exception:
                pass

            hs = _as_int(r.get(home_pts_col)) if home_pts_col else None
            aw = _as_int(r.get(away_pts_col)) if away_pts_col else None

            rows.append(
                {
                    "sport": "wnba",
                    "league": "WNBA",
                    "season": year,
                    "game_date": game_date[:10],
                    "game_key": f"bref:{game_date}:{away}@{home}",
                    "home_team": home,
                    "away_team": away,
                    "home_score": hs,
                    "away_score": aw,
                    "status": "Final" if hs is not None and aw is not None else "Scheduled",
                    "source": "basketball_reference",
                    "raw_json": {
                        "year": year,
                        "date": date_raw,
                        "home": home,
                        "away": away,
                        "home_pts": hs,
                        "away_pts": aw,
                    },
                }
            )
    return rows


def load_kaggle_wnba_datasets(data_dir: str | None = None) -> dict[str, Any]:
    """
    Load local Kaggle-exported CSVs for WNBA training.
    Expects CSV files to be present in WNBA_KAGGLE_DATA_DIR.
    """
    base_dir = str(data_dir or WNBA_KAGGLE_DATA_DIR or "").strip()
    if not base_dir or not os.path.isdir(base_dir):
        return {"game_rows": [], "player_rows": [], "files": []}

    game_rows: list[dict[str, Any]] = []
    player_rows: list[dict[str, Any]] = []
    files = sorted(glob.glob(os.path.join(base_dir, "*.csv")))

    for fp in files:
        name = os.path.basename(fp).lower()
        try:
            with open(fp, "r", encoding="utf-8", newline="") as fh:
                reader = csv.DictReader(fh)
                for row in reader:
                    home = str(row.get("home_team") or row.get("home") or row.get("HomeTeam") or "").strip()
                    away = str(row.get("away_team") or row.get("away") or row.get("AwayTeam") or "").strip()
                    game_date = str(row.get("game_date") or row.get("date") or row.get("Date") or "").strip()
                    if home and away and game_date:
                        game_rows.append(
                            {
                                "sport": "wnba",
                                "league": "WNBA",
                                "season": _as_int(game_date[:4]),
                                "game_date": game_date[:10],
                                "game_key": f"kaggle:{game_date}:{away}@{home}",
                                "home_team": home,
                                "away_team": away,
                                "home_score": _as_int(row.get("home_score") or row.get("HomeScore") or row.get("PTS_H")),
                                "away_score": _as_int(row.get("away_score") or row.get("AwayScore") or row.get("PTS_A")),
                                "status": "Final",
                                "source": "kaggle_datasets",
                                "raw_json": row,
                            }
                        )

                    player_name = str(row.get("player_name") or row.get("player") or row.get("Player") or "").strip()
                    if player_name:
                        stat_candidates = [
                            "pts", "points", "reb", "rebounds", "ast", "assists", "stl", "blk", "to", "turnovers"
                        ]
                        for stat in stat_candidates:
                            raw_val = row.get(stat) or row.get(stat.upper())
                            val = _as_float(raw_val)
                            if val is None:
                                continue
                            player_rows.append(
                                {
                                    "sport": "wnba",
                                    "season": _as_int((row.get("season") or game_date[:4] or "")[:4]),
                                    "game_date": game_date[:10] if game_date else None,
                                    "game_key": str(row.get("game_key") or f"kaggle:{game_date}:{away}@{home}"),
                                    "player_name": player_name,
                                    "team": str(row.get("team") or row.get("Team") or "").strip(),
                                    "stat_type": stat.lower(),
                                    "stat_value": val,
                                    "source": "kaggle_datasets",
                                    "raw_json": row,
                                }
                            )
        except Exception:
            continue

    return {
        "game_rows": game_rows,
        "player_rows": player_rows,
        "files": files,
    }
