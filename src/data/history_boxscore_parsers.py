from __future__ import annotations

import datetime
from typing import Any

import requests


_ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports"


def _safe_get(url: str, params: dict[str, Any] | None = None, timeout: int = 10) -> dict[str, Any] | None:
    try:
        r = requests.get(url, params=params or {}, timeout=timeout)
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, dict):
                return data
    except Exception:
        pass
    return None


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("%"):
        text = text[:-1]
    # Keep ratio strings like 5/12 out of numeric pipeline.
    if "/" in text:
        return None
    try:
        return float(text)
    except Exception:
        return None


def _split_sport_path(sport_path: str) -> tuple[str, str]:
    parts = [p for p in str(sport_path or "").split("/") if p]
    if len(parts) < 2:
        return "", ""
    return parts[0], parts[1]


def fetch_espn_scoreboard_events(sport_path: str, day: datetime.date) -> list[dict[str, Any]]:
    sport, league = _split_sport_path(sport_path)
    if not sport or not league:
        return []
    url = f"{_ESPN_BASE}/{sport}/{league}/scoreboard"
    data = _safe_get(url, params={"dates": day.strftime("%Y%m%d"), "limit": 300})
    return list((data or {}).get("events") or [])


def normalize_espn_event_to_game_row(
    event: dict[str, Any],
    *,
    sport_tag: str,
    league_fallback: str,
    source: str,
) -> dict[str, Any] | None:
    if not isinstance(event, dict):
        return None

    competitions = event.get("competitions") if isinstance(event.get("competitions"), list) else []
    comp = competitions[0] if competitions else {}
    competitors = comp.get("competitors") if isinstance(comp.get("competitors"), list) else []

    home_team = ""
    away_team = ""
    home_score = None
    away_score = None
    for c in competitors:
        if not isinstance(c, dict):
            continue
        team_blob = c.get("team") if isinstance(c.get("team"), dict) else {}
        tname = str(team_blob.get("displayName") or team_blob.get("shortDisplayName") or "").strip()
        side = str(c.get("homeAway") or "").lower()
        score_val = _to_float(c.get("score"))
        score_int = int(score_val) if score_val is not None else None
        if side == "home":
            home_team = tname
            home_score = score_int
        elif side == "away":
            away_team = tname
            away_score = score_int

    game_date_iso = str(event.get("date") or "")[:10]
    if not home_team or not away_team or not game_date_iso:
        return None

    league_name = league_fallback
    if isinstance(comp.get("league"), dict):
        league_name = str(comp.get("league", {}).get("name") or league_name)
    elif isinstance(event.get("league"), dict):
        league_name = str(event.get("league", {}).get("name") or league_name)

    status_blob = event.get("status") if isinstance(event.get("status"), dict) else {}
    status_type = status_blob.get("type") if isinstance(status_blob.get("type"), dict) else {}
    status_name = str(status_type.get("description") or status_type.get("name") or status_blob.get("displayClock") or "Scheduled")

    return {
        "sport": sport_tag,
        "league": league_name,
        "season": int(game_date_iso[:4]) if game_date_iso[:4].isdigit() else None,
        "game_date": game_date_iso,
        "game_key": str(event.get("id") or f"{away_team}@{home_team}#{game_date_iso}"),
        "home_team": home_team,
        "away_team": away_team,
        "home_score": home_score,
        "away_score": away_score,
        "status": status_name,
        "source": source,
        "raw_json": event,
    }


def fetch_espn_summary_player_rows(
    *,
    sport_path: str,
    event_id: str,
    sport_tag: str,
    game_key: str,
    game_date: str,
    source: str = "espn_summary",
) -> list[dict[str, Any]]:
    """Extract per-game player stat rows from ESPN summary boxscore payload."""
    sport, league = _split_sport_path(sport_path)
    if not sport or not league or not event_id:
        return []

    url = f"{_ESPN_BASE}/{sport}/{league}/summary"
    payload = _safe_get(url, params={"event": event_id})
    if not isinstance(payload, dict):
        return []

    boxscore = payload.get("boxscore") if isinstance(payload.get("boxscore"), dict) else {}
    teams = boxscore.get("players") if isinstance(boxscore.get("players"), list) else []
    out: list[dict[str, Any]] = []

    season = int(game_date[:4]) if str(game_date or "")[:4].isdigit() else None

    for team_block in teams:
        if not isinstance(team_block, dict):
            continue
        team_name = ""
        if isinstance(team_block.get("team"), dict):
            team_name = str(team_block.get("team", {}).get("displayName") or "").strip()

        stat_groups = team_block.get("statistics") if isinstance(team_block.get("statistics"), list) else []
        for group in stat_groups:
            if not isinstance(group, dict):
                continue
            group_name = str(group.get("name") or group.get("displayName") or "general").strip().lower().replace(" ", "_")

            labels = []
            if isinstance(group.get("labels"), list):
                labels = [str(x).strip().lower().replace(" ", "_") for x in group.get("labels")]
            elif isinstance(group.get("names"), list):
                labels = [str(x).strip().lower().replace(" ", "_") for x in group.get("names")]

            athletes = group.get("athletes") if isinstance(group.get("athletes"), list) else []
            for a in athletes:
                if not isinstance(a, dict):
                    continue
                athlete_blob = a.get("athlete") if isinstance(a.get("athlete"), dict) else {}
                player_name = str(
                    athlete_blob.get("displayName")
                    or athlete_blob.get("shortName")
                    or a.get("displayName")
                    or ""
                ).strip()
                if not player_name:
                    continue

                stats = a.get("stats") if isinstance(a.get("stats"), list) else []
                for idx, stat_val in enumerate(stats):
                    num = _to_float(stat_val)
                    if num is None:
                        continue
                    stat_label = labels[idx] if idx < len(labels) and labels[idx] else f"stat_{idx+1}"
                    out.append(
                        {
                            "sport": sport_tag,
                            "season": season,
                            "game_date": game_date,
                            "game_key": game_key,
                            "player_name": player_name,
                            "team": team_name,
                            "stat_type": f"{group_name}:{stat_label}",
                            "stat_value": num,
                            "source": source,
                            "raw_json": {
                                "sport_path": sport_path,
                                "event_id": event_id,
                                "group": group_name,
                                "label": stat_label,
                                "value": stat_val,
                            },
                        }
                    )

    return out
