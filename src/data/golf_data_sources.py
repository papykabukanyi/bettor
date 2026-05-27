from __future__ import annotations

import csv
import datetime
import glob
import io
import os
import re
import time
from collections import defaultdict, deque
from typing import Any

import requests

from config import (
    GOLF_DATAGOLF_API_BASE,
    GOLF_DATAGOLF_API_KEY,
    GOLF_DATA_CACHE_TTL_SEC,
    GOLF_GOLFAPI_BASE,
    GOLF_GOLFAPI_KEY,
    GOLF_KAGGLE_DATA_DIR,
    GOLF_PGA_STATDATA_BASE,
    GOLF_REFERENCE_YEARS,
)

_DEFAULT_TIMEOUT = 12
_cache: dict[str, tuple[Any, float]] = {}


def _cached(key: str, ttl: int, fn, *args, **kwargs):
    now = time.time()
    found = _cache.get(key)
    if found and (now - found[1]) < ttl:
        return found[0]
    value = fn(*args, **kwargs)
    _cache[key] = (value, now)
    return value


def _safe_get(
    url: str,
    *,
    headers: dict[str, str] | None = None,
    params: dict[str, Any] | None = None,
    timeout: int = _DEFAULT_TIMEOUT,
) -> Any:
    try:
        r = requests.get(url, headers=headers or {}, params=params or {}, timeout=timeout)
        if r.status_code != 200:
            return None
        ctype = str(r.headers.get("Content-Type") or "").lower()
        if "application/json" in ctype:
            return r.json()
        return r.text
    except Exception:
        return None


def _as_float(value: Any) -> float | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        return float(value)
    except Exception:
        return None


def _as_int(value: Any) -> int | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        return int(float(value))
    except Exception:
        return None


def _norm(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip().lower()


def _parse_date(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    if raw.isdigit() and len(raw) == 8:
        try:
            return datetime.datetime.strptime(raw, "%Y%m%d").date().isoformat()
        except Exception:
            pass
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%a, %b %d, %Y"):
        try:
            return datetime.datetime.strptime(raw, fmt).date().isoformat()
        except Exception:
            continue
    try:
        dt = datetime.datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return dt.date().isoformat()
    except Exception:
        return raw[:10]


def _read_local_csvs(base_dir: str, patterns: list[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not base_dir or not os.path.isdir(base_dir):
        return rows
    for pattern in patterns:
        for path in sorted(glob.glob(os.path.join(base_dir, pattern))):
            try:
                with open(path, encoding="utf-8-sig", newline="") as fh:
                    reader = csv.DictReader(fh)
                    rows.extend([dict(row) for row in reader])
            except Exception:
                continue
    return rows


def fetch_datagolf_bundle(player_name: str = "", event_date: str = "") -> dict[str, Any]:
    """Fetch DataGolf strokes-gained and ranking context when configured."""
    if not GOLF_DATAGOLF_API_BASE:
        return {"players": [], "events": [], "stats": []}
    headers: dict[str, str] = {}
    if GOLF_DATAGOLF_API_KEY:
        headers["Authorization"] = f"Bearer {GOLF_DATAGOLF_API_KEY}"
        headers["X-API-Key"] = GOLF_DATAGOLF_API_KEY
    payload = _safe_get(
        f"{GOLF_DATAGOLF_API_BASE.rstrip('/')}/bundle",
        headers=headers,
        params={"player": player_name, "date": event_date},
    )
    if not isinstance(payload, dict):
        return {"players": [], "events": [], "stats": []}
    return {
        "players": payload.get("players") or payload.get("player_stats") or [],
        "events": payload.get("events") or payload.get("tournaments") or [],
        "stats": payload.get("stats") or payload.get("strokes_gained") or [],
    }


def fetch_pga_statdata_rounds(event_id: str = "", event_date: str = "") -> list[dict[str, Any]]:
    """Fetch PGA Tour statdata round-level rows when endpoint is configured."""
    if not GOLF_PGA_STATDATA_BASE:
        return []
    payload = _safe_get(
        f"{GOLF_PGA_STATDATA_BASE.rstrip('/')}/rounds",
        params={"event_id": event_id, "date": event_date},
    )
    if isinstance(payload, dict):
        return payload.get("rounds") or payload.get("results") or []
    return []


def fetch_espn_golf_live_bundle(game_date: datetime.date | str | None = None) -> dict[str, Any]:
    """Fetch ESPN golf leaderboard-style events for daily card context."""
    from data.history_boxscore_parsers import fetch_espn_scoreboard_events

    if isinstance(game_date, datetime.date):
        date_obj = game_date
    else:
        try:
            date_obj = datetime.date.fromisoformat(str(game_date or ""))
        except Exception:
            date_obj = datetime.date.today()

    events = fetch_espn_scoreboard_events("golf/pga", date_obj) or []
    return {"events": events, "date": date_obj.isoformat()}


def fetch_golfapi_course_details(course_name: str = "") -> dict[str, Any]:
    """Fetch course metadata (par/yardage/layout) when GolfAPI endpoint is configured."""
    if not GOLF_GOLFAPI_BASE:
        return {}
    headers: dict[str, str] = {}
    if GOLF_GOLFAPI_KEY:
        headers["Authorization"] = f"Bearer {GOLF_GOLFAPI_KEY}"
        headers["X-API-Key"] = GOLF_GOLFAPI_KEY
    payload = _safe_get(
        f"{GOLF_GOLFAPI_BASE.rstrip('/')}/course",
        headers=headers,
        params={"name": course_name},
    )
    if isinstance(payload, dict):
        return payload.get("course") or payload
    return {}


def fetch_kaggle_golf_history(data_dir: str | None = None) -> list[dict[str, Any]]:
    """Load historical golf rows from local Kaggle exports."""
    base_dir = data_dir or GOLF_KAGGLE_DATA_DIR
    ttl = max(60, int(GOLF_DATA_CACHE_TTL_SEC or 300))

    def _pull() -> list[dict[str, Any]]:
        return _read_local_csvs(base_dir, ["*.csv"]) if base_dir and os.path.isdir(base_dir) else []

    return _cached(f"golf_kaggle::{base_dir or 'none'}", ttl, _pull) or []


def _canonical_golf_row(raw: dict[str, Any], *, source: str) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None

    player = str(raw.get("player_name") or raw.get("player") or raw.get("name") or "").strip()
    if not player:
        return None

    game_date = _parse_date(raw.get("date") or raw.get("event_date") or raw.get("tournament_date"))
    season = _as_int(raw.get("season") or raw.get("year") or (game_date[:4] if game_date[:4].isdigit() else None))
    event_name = str(raw.get("event_name") or raw.get("tournament") or raw.get("event") or "Golf Event").strip()
    course_name = str(raw.get("course") or raw.get("course_name") or "").strip()
    round_score = _as_float(raw.get("round_score") or raw.get("score") or raw.get("strokes"))
    finish_position = _as_int(raw.get("finish_position") or raw.get("finish") or raw.get("position"))

    sg_total = _as_float(raw.get("sg_total") or raw.get("strokes_gained_total"))
    sg_approach = _as_float(raw.get("sg_approach") or raw.get("strokes_gained_approach"))
    sg_putting = _as_float(raw.get("sg_putting") or raw.get("strokes_gained_putting"))
    driving_distance = _as_float(raw.get("driving_distance") or raw.get("avg_driving_distance"))
    cut_made = raw.get("cut_made")
    if isinstance(cut_made, str):
        cut_made = cut_made.strip().lower() in {"1", "true", "yes", "made"}
    elif not isinstance(cut_made, bool):
        cut_made = bool(finish_position is not None and finish_position > 0)

    game_key = str(
        raw.get("game_key")
        or raw.get("event_id")
        or f"golf:{game_date}:{event_name}:{player}"
    )

    return {
        "sport": "golf",
        "league": "Golf",
        "season": season,
        "game_date": game_date[:10],
        "game_key": game_key,
        "home_team": player,
        "away_team": event_name,
        "home_score": finish_position,
        "away_score": round_score,
        "status": str(raw.get("status") or "Final"),
        "source": source,
        "player_name": player,
        "event_name": event_name,
        "course_name": course_name,
        "course_par": _as_int(raw.get("course_par") or raw.get("par")),
        "course_yardage": _as_int(raw.get("course_yardage") or raw.get("yardage")),
        "course_type": str(raw.get("course_type") or raw.get("grass_type") or "").strip().lower(),
        "sg_total": sg_total,
        "sg_approach": sg_approach,
        "sg_putting": sg_putting,
        "driving_distance": driving_distance,
        "cut_made": cut_made,
        "owgr_rank": _as_int(raw.get("owgr_rank") or raw.get("world_rank")),
        "weather": str(raw.get("weather") or raw.get("wind") or "").strip(),
        "finish_position": finish_position,
        "raw_json": raw,
    }


def load_golf_reference_rows(*, limit_years: int | None = None) -> list[dict[str, Any]]:
    years = int(limit_years or GOLF_REFERENCE_YEARS or 8)
    cache_key = f"golf_reference::{years}"

    def _pull() -> list[dict[str, Any]]:
        raw_rows = fetch_kaggle_golf_history()
        out: list[dict[str, Any]] = []
        for raw in raw_rows:
            row = _canonical_golf_row(raw, source="kaggle_golf")
            if row:
                out.append(row)
        min_year = datetime.date.today().year - years
        out = [r for r in out if (_as_int((r.get("season") or 0)) or 0) >= min_year]
        out.sort(key=lambda r: (str(r.get("game_date") or ""), str(r.get("player_name") or ""), str(r.get("event_name") or "")))
        return out

    return _cached(cache_key, max(120, int(GOLF_DATA_CACHE_TTL_SEC or 300)), _pull) or []


def build_golf_prediction_context(
    *,
    player_name: str,
    event_name: str = "",
    course_name: str = "",
    game_date: str = "",
    weather: str = "",
    reference_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    refs = reference_rows or load_golf_reference_rows()
    pn = _norm(player_name)
    en = _norm(event_name)
    cn = _norm(course_name)

    player_rows = [r for r in refs if _norm(r.get("player_name")) == pn]
    course_rows = [r for r in player_rows if cn and _norm(r.get("course_name")) == cn]
    event_rows = [r for r in player_rows if en and _norm(r.get("event_name")) == en]

    recent = player_rows[-5:]
    recent_form = 0.5
    if recent:
        vals = []
        for r in recent:
            fp = _as_int(r.get("finish_position"))
            if fp and fp > 0:
                vals.append(max(0.0, min(1.0, 1.0 - ((fp - 1) / 100.0))))
        if vals:
            recent_form = sum(vals) / len(vals)

    putt_rows = player_rows[-4:]
    sg_putting = [
        _as_float(r.get("sg_putting")) for r in putt_rows if _as_float(r.get("sg_putting")) is not None
    ]
    sg_putting_recent = (sum(sg_putting) / len(sg_putting)) if sg_putting else 0.0

    sg_total_vals = [
        _as_float(r.get("sg_total")) for r in player_rows[-12:] if _as_float(r.get("sg_total")) is not None
    ]
    sg_approach_vals = [
        _as_float(r.get("sg_approach")) for r in player_rows[-12:] if _as_float(r.get("sg_approach")) is not None
    ]
    driving_vals = [
        _as_float(r.get("driving_distance")) for r in player_rows[-12:] if _as_float(r.get("driving_distance")) is not None
    ]

    made_cut_flags = [1 if bool(r.get("cut_made")) else 0 for r in player_rows[-8:]]
    cut_streak = 0
    for flag in reversed(made_cut_flags):
        if flag:
            cut_streak += 1
        else:
            break

    last_played = str(player_rows[-1].get("game_date") or "") if player_rows else ""
    fatigue_days = None
    if last_played:
        try:
            ref_day = datetime.date.fromisoformat((game_date or datetime.date.today().isoformat())[:10])
            prev_day = datetime.date.fromisoformat(last_played[:10])
            fatigue_days = max(0, (ref_day - prev_day).days)
        except Exception:
            fatigue_days = None

    course_finish_vals = [
        _as_int(r.get("finish_position")) for r in course_rows if _as_int(r.get("finish_position")) is not None
    ]
    course_fit = None
    if course_finish_vals:
        avg_finish = sum(course_finish_vals) / len(course_finish_vals)
        course_fit = max(0.0, min(1.0, 1.0 - ((avg_finish - 1.0) / 100.0)))

    owgr_rank = None
    for r in reversed(player_rows):
        owgr_rank = _as_int(r.get("owgr_rank"))
        if owgr_rank is not None:
            break

    return {
        "player_name": player_name,
        "event_name": event_name,
        "course_name": course_name,
        "sg_total": round(sum(sg_total_vals) / len(sg_total_vals), 4) if sg_total_vals else 0.0,
        "sg_approach": round(sum(sg_approach_vals) / len(sg_approach_vals), 4) if sg_approach_vals else 0.0,
        "sg_putting": round(sg_putting_recent, 4),
        "course_fit": round(float(course_fit), 4) if course_fit is not None else None,
        "course_type": str((course_rows[-1].get("course_type") if course_rows else "") or "").strip().lower(),
        "recent_form": round(recent_form, 4),
        "driving_distance": round(sum(driving_vals) / len(driving_vals), 2) if driving_vals else 0.0,
        "cut_streak": int(cut_streak),
        "owgr_rank": owgr_rank,
        "weather": weather,
        "fatigue_days": fatigue_days,
        "event_history_count": len(event_rows),
        "course_history_count": len(course_rows),
    }


def build_golf_history_rows(raw_rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    refs: list[dict[str, Any]] = []
    for raw in raw_rows or []:
        row = _canonical_golf_row(raw, source="golf_history")
        if row:
            refs.append(row)

    refs.sort(key=lambda r: (str(r.get("game_date") or ""), str(r.get("player_name") or ""), str(r.get("event_name") or "")))

    game_rows: list[dict[str, Any]] = []
    player_rows: list[dict[str, Any]] = []
    by_player_recent: dict[str, deque[dict[str, Any]]] = defaultdict(lambda: deque(maxlen=12))

    for row in refs:
        player = str(row.get("player_name") or "").strip()
        if not player:
            continue
        recent_ref = list(by_player_recent[_norm(player)])
        ctx = build_golf_prediction_context(
            player_name=player,
            event_name=str(row.get("event_name") or ""),
            course_name=str(row.get("course_name") or ""),
            game_date=str(row.get("game_date") or ""),
            weather=str(row.get("weather") or ""),
            reference_rows=recent_ref if recent_ref else refs,
        )

        game_rows.append(
            {
                **row,
                "sport": "golf",
                "feature_snapshot": ctx,
            }
        )

        stat_map = {
            "sg_total": ctx.get("sg_total"),
            "sg_approach": ctx.get("sg_approach"),
            "sg_putting": ctx.get("sg_putting"),
            "course_fit": ctx.get("course_fit"),
            "recent_form": ctx.get("recent_form"),
            "driving_distance": ctx.get("driving_distance"),
            "cut_streak": ctx.get("cut_streak"),
            "owgr_rank": ctx.get("owgr_rank"),
            "fatigue_days": ctx.get("fatigue_days"),
        }
        for stat_type, stat_value in stat_map.items():
            if stat_value is None:
                continue
            player_rows.append(
                {
                    "sport": "golf",
                    "season": row.get("season"),
                    "game_date": row.get("game_date"),
                    "game_key": row.get("game_key"),
                    "player_name": player,
                    "team": str(row.get("event_name") or ""),
                    "stat_type": stat_type,
                    "stat_value": float(stat_value) if isinstance(stat_value, (int, float)) else stat_value,
                    "source": str(row.get("source") or "golf_history"),
                    "raw_json": {
                        "feature_snapshot": ctx,
                        "match_row": row,
                    },
                }
            )

        by_player_recent[_norm(player)].append(row)

    return {"game_rows": game_rows, "player_rows": player_rows}