from __future__ import annotations

import csv
import datetime
import io
import json
import os
import re
import time
from typing import Any

import requests

from config import (
    API_FOOTBALL_BASE,
    API_FOOTBALL_HOST,
    API_FOOTBALL_KEY,
    BSD_API_BASE,
    BSD_API_KEY,
    SOCCER_DS_CACHE_TTL_SEC,
    TRANSFERMARKT_PROXY_URL,
)

_DEFAULT_TIMEOUT = 10
_cache: dict[str, tuple[Any, float]] = {}


def _cached(key: str, ttl: int, fn, *args, **kwargs):
    now = time.time()
    found = _cache.get(key)
    if found and (now - found[1]) < ttl:
        return found[0]
    value = fn(*args, **kwargs)
    _cache[key] = (value, now)
    return value


def _norm_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip().lower()


def _as_float(value: Any) -> float | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        return float(value)
    except Exception:
        return None


def _safe_get(url: str, *, headers: dict[str, str] | None = None, params: dict[str, Any] | None = None, timeout: int = _DEFAULT_TIMEOUT) -> Any:
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


def fetch_football_data_uk_history(
    league_codes: list[str] | None = None,
    season_codes: list[str] | None = None,
) -> list[dict[str, Any]]:
    """
    Pull historical odds rows from football-data.co.uk CSV archives.
    season code format: 2526, 2425, etc.
    """
    leagues = league_codes or ["E0", "SP1", "D1", "I1", "F1", "N1", "P1"]
    seasons = season_codes or ["2526", "2425", "2324", "2223", "2122"]

    rows: list[dict[str, Any]] = []
    for season in seasons:
        for code in leagues:
            url = f"https://www.football-data.co.uk/mmz4281/{season}/{code}.csv"
            text = _safe_get(url)
            if not text or not isinstance(text, str):
                continue
            try:
                reader = csv.DictReader(io.StringIO(text))
                for row in reader:
                    home = str(row.get("HomeTeam") or "").strip()
                    away = str(row.get("AwayTeam") or "").strip()
                    if not home or not away:
                        continue
                    rows.append(
                        {
                            "source": "football-data.co.uk",
                            "league": code,
                            "season": season,
                            "date": str(row.get("Date") or "").strip(),
                            "home_team": home,
                            "away_team": away,
                            "fthg": _as_float(row.get("FTHG")),
                            "ftag": _as_float(row.get("FTAG")),
                            "ftr": str(row.get("FTR") or "").strip(),
                            "odds_home": _as_float(row.get("B365H") or row.get("PSH") or row.get("BWH")),
                            "odds_draw": _as_float(row.get("B365D") or row.get("PSD") or row.get("BWD")),
                            "odds_away": _as_float(row.get("B365A") or row.get("PSA") or row.get("BWA")),
                            "odds_over25": _as_float(row.get("B365>2.5") or row.get("P>2.5")),
                            "odds_under25": _as_float(row.get("B365<2.5") or row.get("P<2.5")),
                        }
                    )
            except Exception:
                continue
    return rows


def fetch_statsbomb_open_data(
    competition_id: int | None = None,
    season_id: int | None = None,
    match_id: int | None = None,
) -> dict[str, Any]:
    """Fetch StatsBomb open-data slices from GitHub raw JSON."""
    base = "https://raw.githubusercontent.com/statsbomb/open-data/master/data"
    out: dict[str, Any] = {"competitions": [], "matches": [], "events": []}

    competitions = _safe_get(f"{base}/competitions.json")
    if isinstance(competitions, list):
        out["competitions"] = competitions

    if competition_id is not None and season_id is not None:
        matches = _safe_get(f"{base}/matches/{competition_id}/{season_id}.json")
        if isinstance(matches, list):
            out["matches"] = matches

    if match_id is not None:
        events = _safe_get(f"{base}/events/{match_id}.json")
        if isinstance(events, list):
            out["events"] = events
    return out


def fetch_bsd_live_data(sport: str = "soccer") -> dict[str, Any]:
    """Fetch live scores and multi-book odds from BSD API (provider-specific endpoint via env)."""
    if not BSD_API_BASE:
        return {"events": [], "odds": []}

    headers: dict[str, str] = {}
    if BSD_API_KEY:
        headers["Authorization"] = f"Bearer {BSD_API_KEY}"
        headers["X-API-Key"] = BSD_API_KEY

    payload = _safe_get(
        f"{BSD_API_BASE.rstrip('/')}/live",
        headers=headers,
        params={"sport": sport},
    )
    if not isinstance(payload, dict):
        return {"events": [], "odds": []}
    return {
        "events": payload.get("events") or payload.get("matches") or [],
        "odds": payload.get("odds") or payload.get("book_odds") or [],
    }


def fetch_understat_league_table(league: str, season: int) -> list[dict[str, Any]]:
    """Parse Understat team-level xG table from embedded JS payload."""
    url = f"https://understat.com/league/{league}/{season}"
    html = _safe_get(url)
    if not isinstance(html, str) or not html:
        return []

    # Understat embeds escaped JSON in scripts as ('...').
    match = re.search(r"teamsData\s*=\s*JSON\.parse\('(.*?)'\)", html, flags=re.DOTALL)
    if not match:
        return []
    raw = match.group(1)
    try:
        decoded = bytes(raw, "utf-8").decode("unicode_escape")
        payload = json.loads(decoded)
    except Exception:
        return []

    rows: list[dict[str, Any]] = []
    if not isinstance(payload, dict):
        return rows
    for _, team in payload.items():
        title = str(team.get("title") or "").strip()
        hist = team.get("history") or []
        if not title or not isinstance(hist, list) or not hist:
            continue
        xg_for = 0.0
        xg_against = 0.0
        matches = 0
        for h in hist:
            if not isinstance(h, dict):
                continue
            xg_for += float(h.get("xG") or 0.0)
            xg_against += float(h.get("xGA") or 0.0)
            matches += 1
        if matches == 0:
            continue
        rows.append(
            {
                "team": title,
                "league": league,
                "season": season,
                "xg_for_per_match": round(xg_for / matches, 4),
                "xga_per_match": round(xg_against / matches, 4),
                "sample_size": matches,
                "source": "understat",
            }
        )
    return rows


def fetch_transfermarkt_injuries(team: str = "") -> list[dict[str, Any]]:
    """Fetch injury history via a proxy endpoint for Transfermarkt data."""
    if not TRANSFERMARKT_PROXY_URL:
        return []
    params: dict[str, Any] = {}
    if team:
        params["team"] = team
    payload = _safe_get(TRANSFERMARKT_PROXY_URL, params=params)
    if not isinstance(payload, (dict, list)):
        return []
    if isinstance(payload, list):
        rows = payload
    else:
        rows = payload.get("injuries") or payload.get("data") or []
    out: list[dict[str, Any]] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        out.append(
            {
                "team": str(r.get("team") or "").strip(),
                "player_name": str(r.get("player") or r.get("player_name") or "").strip(),
                "injury_type": str(r.get("injury") or r.get("injury_type") or "").strip(),
                "status": str(r.get("status") or "").strip(),
                "from": str(r.get("from") or r.get("start_date") or "").strip(),
                "until": str(r.get("until") or r.get("end_date") or "").strip(),
                "source": "transfermarkt",
            }
        )
    return out


def fetch_api_football_bundle(
    *,
    home_team: str,
    away_team: str,
    match_date: str = "",
    season: int | None = None,
) -> dict[str, Any]:
    """Pull fixtures, H2H, lineups, odds, and predictions from API-Football."""
    if not API_FOOTBALL_KEY:
        return {"fixture": {}, "h2h": [], "lineups": [], "odds": [], "prediction": {}}

    headers = {
        "x-rapidapi-key": API_FOOTBALL_KEY,
        "x-rapidapi-host": API_FOOTBALL_HOST,
    }
    base = API_FOOTBALL_BASE.rstrip("/")

    fixture_rows: list[dict[str, Any]] = []
    fixtures_payload = _safe_get(
        f"{base}/fixtures",
        headers=headers,
        params={
            "date": match_date or datetime.date.today().isoformat(),
            "season": season or datetime.date.today().year,
        },
    )
    if isinstance(fixtures_payload, dict):
        fixture_rows = fixtures_payload.get("response") or []

    best_fixture: dict[str, Any] = {}
    target_home = _norm_text(home_team)
    target_away = _norm_text(away_team)
    for row in fixture_rows:
        if not isinstance(row, dict):
            continue
        teams = row.get("teams") or {}
        h_name = _norm_text((teams.get("home") or {}).get("name"))
        a_name = _norm_text((teams.get("away") or {}).get("name"))
        if (target_home in h_name or h_name in target_home) and (target_away in a_name or a_name in target_away):
            best_fixture = row
            break

    fixture_id = ((best_fixture.get("fixture") or {}).get("id") if isinstance(best_fixture, dict) else None)
    h2h_rows: list[dict[str, Any]] = []
    lineups_rows: list[dict[str, Any]] = []
    odds_rows: list[dict[str, Any]] = []
    prediction: dict[str, Any] = {}

    if fixture_id:
        h2h_payload = _safe_get(f"{base}/fixtures/headtohead", headers=headers, params={"h2h": f"{home_team}-{away_team}"})
        if isinstance(h2h_payload, dict):
            h2h_rows = h2h_payload.get("response") or []

        lineups_payload = _safe_get(f"{base}/fixtures/lineups", headers=headers, params={"fixture": fixture_id})
        if isinstance(lineups_payload, dict):
            lineups_rows = lineups_payload.get("response") or []

        odds_payload = _safe_get(f"{base}/odds", headers=headers, params={"fixture": fixture_id})
        if isinstance(odds_payload, dict):
            odds_rows = odds_payload.get("response") or []

        pred_payload = _safe_get(f"{base}/predictions", headers=headers, params={"fixture": fixture_id})
        if isinstance(pred_payload, dict):
            pred_rows = pred_payload.get("response") or []
            if pred_rows and isinstance(pred_rows[0], dict):
                prediction = pred_rows[0]

    return {
        "fixture": best_fixture,
        "h2h": h2h_rows,
        "lineups": lineups_rows,
        "odds": odds_rows,
        "prediction": prediction,
    }


def _find_team_understat_xg(rows: list[dict[str, Any]], team_name: str) -> tuple[float, float, int]:
    target = _norm_text(team_name)
    if not target:
        return 0.0, 0.0, 0
    for r in rows:
        team = _norm_text(r.get("team"))
        if not team:
            continue
        if target in team or team in target:
            return (
                float(r.get("xg_for_per_match") or 0.0),
                float(r.get("xga_per_match") or 0.0),
                int(r.get("sample_size") or 0),
            )
    return 0.0, 0.0, 0


def build_soccer_prediction_context(
    home_team: str,
    away_team: str,
    *,
    league_hint: str = "EPL",
    match_date: str = "",
) -> dict[str, Any]:
    """Unified context object consumed by soccer predictor to enrich probabilities."""
    cache_key = f"ctx::{_norm_text(home_team)}::{_norm_text(away_team)}::{league_hint}::{match_date}"

    def _compute() -> dict[str, Any]:
        today_year = datetime.date.today().year
        understat = fetch_understat_league_table(league=league_hint or "EPL", season=today_year)
        hxg, hxga, hs = _find_team_understat_xg(understat, home_team)
        axg, axga, as_ = _find_team_understat_xg(understat, away_team)

        transfer_home = fetch_transfermarkt_injuries(home_team)
        transfer_away = fetch_transfermarkt_injuries(away_team)
        home_inj = len([r for r in transfer_home if _norm_text(r.get("status")) not in {"", "fit", "available"}])
        away_inj = len([r for r in transfer_away if _norm_text(r.get("status")) not in {"", "fit", "available"}])

        api_bundle = fetch_api_football_bundle(
            home_team=home_team,
            away_team=away_team,
            match_date=match_date,
            season=today_year,
        )
        lineups_ready = len(api_bundle.get("lineups") or []) >= 2

        pred = api_bundle.get("prediction") or {}
        pred_percent = (pred.get("predictions") or {}).get("percent") if isinstance(pred, dict) else {}
        api_home = _as_float((pred_percent or {}).get("home"))
        api_draw = _as_float((pred_percent or {}).get("draw"))
        api_away = _as_float((pred_percent or {}).get("away"))
        if api_home is not None:
            api_home /= 100.0
        if api_draw is not None:
            api_draw /= 100.0
        if api_away is not None:
            api_away /= 100.0

        return {
            "home_understat_xg": hxg,
            "home_understat_xga": hxga,
            "away_understat_xg": axg,
            "away_understat_xga": axga,
            "home_understat_sample": hs,
            "away_understat_sample": as_,
            "home_transfermarkt_injuries": home_inj,
            "away_transfermarkt_injuries": away_inj,
            "lineups_confirmed": lineups_ready,
            "api_home_prob": api_home,
            "api_draw_prob": api_draw,
            "api_away_prob": api_away,
            "api_fixture": api_bundle.get("fixture") or {},
            "api_h2h": api_bundle.get("h2h") or [],
            "api_odds": api_bundle.get("odds") or [],
            "api_prediction": pred,
            "sources": {
                "understat": bool(understat),
                "transfermarkt": bool(transfer_home or transfer_away),
                "api_football": bool(api_bundle.get("fixture") or api_bundle.get("prediction")),
            },
        }

    return _cached(cache_key, max(120, SOCCER_DS_CACHE_TTL_SEC), _compute) or {}
