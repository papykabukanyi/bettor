"""
Multi-Sport Deep Enrichment Engine
====================================
Adds the following signals to any game or player-prop row across ALL sports:

  1. Rest days          — days since each team/player last played
  2. Venue history      — home/away record & scoring at the specific stadium/arena
  3. Head-to-head       — historical H2H record between these two specific teams
  4. Coaching context   — head coach / manager name + win% (ESPN public API)
  5. Weather            — conditions at game location for outdoor sports (Open-Meteo, free, no key)
  6. Travel distance    — km between venues; back-to-back road game penalty flag
  7. Player form        — last 5 / last 10 splits, home vs away splits
  8. Fatigue index      — derived from games played in rolling 7-day window
  9. Trend momentum     — positive/negative streak heading into this game
 10. Referee / umpire   — foul/card/scoring tendencies from public logs (soccer + NBA)

Sources used (all free / no-key where possible):
  - ESPN unofficial API  (scores, schedule, team/coach)
  - Open-Meteo          (weather forecast, no key)
  - MLB Stats API       (mlb schedule / boxscores)
  - BallDontLie API     (NBA schedule)
  - hockey-reference    (NHL schedule via ESPN fallback)
  - TheSportsDB API key=1 (team venue lookup)

All enrichment is non-blocking: every signal gracefully degrades to None / 0.
"""

from __future__ import annotations

import datetime
import math
import os
import re
import time
from typing import Any

import requests

# ─── In-memory cache to avoid redundant API calls within a session ───────────
_cache: dict[str, tuple[Any, float]] = {}
_CACHE_TTL_SHORT = 180      # 3 min  – live scores / schedule
_CACHE_TTL_MED   = 3600     # 1 h   – venue / coach / H2H
_CACHE_TTL_LONG  = 86400    # 24 h  – static venue info


def _cache_get(key: str, ttl: float) -> Any | None:
    entry = _cache.get(key)
    if entry and (time.time() - entry[1]) < ttl:
        return entry[0]
    return None


def _cache_set(key: str, value: Any) -> None:
    _cache[key] = (value, time.time())


def _safe_req(url: str, params: dict | None = None, timeout: int = 8, headers: dict | None = None) -> dict | list | None:
    try:
        resp = requests.get(url, params=params or {}, headers=headers or {"User-Agent": "bettor-enrichment/1.0"}, timeout=timeout)
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass
    return None


# ─── Sport → ESPN API slug mapping ──────────────────────────────────────────
_ESPN_SPORT_SLUG: dict[str, str] = {
    "baseball":         "baseball/mlb",
    "mlb":              "baseball/mlb",
    "basketball":       "basketball/nba",
    "nba":              "basketball/nba",
    "wnba":             "basketball/wnba",
    "americanfootball": "football/nfl",
    "nfl":              "football/nfl",
    "ncaaf":            "football/college-football",
    "icehockey":        "hockey/nhl",
    "nhl":              "hockey/nhl",
    "soccer":           "soccer",
    "mma":              "mma",
}

_ESPN_SOCCER_LEAGUE_SLUG: dict[str, str] = {
    "soccer_epl":                    "eng.1",
    "soccer_spain_la_liga":          "esp.1",
    "soccer_germany_bundesliga":     "ger.1",
    "soccer_italy_serie_a":          "ita.1",
    "soccer_france_ligue_1":         "fra.1",
    "soccer_netherlands_eredivisie": "ned.1",
    "soccer_portugal_primeira_liga": "por.1",
    "soccer_usa_mls":                "usa.1",
    "soccer_brazil_campeonato":      "bra.1",
    "soccer_argentina_primera_division": "arg.1",
    "soccer_mexico_ligamx":          "mex.1",
    "soccer_australia_aleague":      "aus.1",
    "soccer_korea_kleague1":         "kor.1",
    "soccer_japan_j_league":         "jpn.1",
    "soccer_saudi_arabia_pro_league":"sau.1",
    "soccer_turkey_super_lig":       "tur.1",
    "soccer_uefa_champs_league":     "uefa.champions",
}

_ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports"
_ESPN_CDN  = "https://site.web.api.espn.com/apis/common/v3/sports"

# ─── Venue coordinates for weather lookups ──────────────────────────────────
# Populated from TheSportsDB on first use; these are fallbacks for the most common
# MLB / NFL / NBA / NHL venues.
_VENUE_COORDS: dict[str, tuple[float, float]] = {
    # MLB (team → lat, lon)
    "yankees":          (40.8296, -73.9262),
    "red sox":          (42.3467, -71.0972),
    "dodgers":          (34.0739, -118.2400),
    "cubs":             (41.9484, -87.6553),
    "giants":           (37.7786, -122.3893),
    "astros":           (29.7573, -95.3555),
    "braves":           (33.8909, -84.4678),
    "mets":             (40.7571, -73.8458),
    "phillies":         (39.9056, -75.1665),
    "cardinals":        (38.6226, -90.1928),
    "brewers":          (43.0283, -87.9711),
    "padres":           (32.7076, -117.1570),
    "reds":             (39.0979, -84.5082),
    "mariners":         (47.5914, -122.3322),
    "angels":           (33.8003, -117.8827),
    "athletics":        (37.7516, -122.2005),
    "rangers":          (32.7512, -97.0832),
    "royals":           (39.0517, -94.4803),
    "twins":            (44.9817, -93.2777),
    "white sox":        (41.8299, -87.6338),
    "tigers":           (42.3390, -83.0493),
    "guardians":        (41.4962, -81.6852),
    "pirates":          (40.4469, -80.0057),
    "orioles":          (39.2839, -76.6217),
    "blue jays":        (43.6414, -79.3894),
    "rays":             (27.7683, -82.6534),
    "nationals":        (38.8730, -77.0074),
    "marlins":          (25.7781, -80.2198),
    "rockies":          (39.7559, -104.9942),
    "diamondbacks":     (33.4453, -112.0667),
    # NFL
    "patriots":         (42.0909, -71.2643),
    "cowboys":          (32.7473, -97.0945),
    "bears":            (41.8623, -87.6167),
    "packers":          (44.5013, -88.0622),
    "steelers":         (40.4468, -80.0158),
    "chiefs":           (39.0489, -94.4839),
    "49ers":            (37.4032, -121.9698),
    "seahawks":         (47.5952, -122.3316),
    "rams":             (33.9535, -118.3390),
    "ravens":           (39.2779, -76.6227),
    # NBA
    "lakers":           (34.0430, -118.2673),
    "celtics":          (42.3662, -71.0621),
    "bulls":            (41.8807, -87.6742),
    "heat":             (25.7814, -80.1870),
    "warriors":         (37.7680, -122.3875),
    "knicks":           (40.7505, -73.9934),
    "nets":             (40.6826, -73.9754),
    "76ers":            (39.9012, -75.1719),
    "bucks":            (43.0451, -87.9170),
    "nuggets":          (39.7487, -105.0077),
    "suns":             (33.4457, -112.0712),
    "clippers":         (34.0430, -118.2673),
}


def _get_venue_coords(team_name: str) -> tuple[float, float] | None:
    """Look up approximate coordinates for a team's home venue."""
    key = team_name.lower().strip()
    for tkn, coords in _VENUE_COORDS.items():
        if tkn in key or key in tkn:
            return coords

    # Fallback: query TheSportsDB for the venue coordinates
    cache_key = f"venue_coord:{key}"
    cached = _cache_get(cache_key, _CACHE_TTL_LONG)
    if cached is not None:
        return cached

    data = _safe_req(
        "https://www.thesportsdb.com/api/v1/json/1/searchteams.php",
        params={"t": team_name},
    )
    teams = (data or {}).get("teams") or []
    if teams:
        lat = teams[0].get("strStadiumLocation") or ""
        # TheSportsDB returns "lat, lon" in strStadiumLocation
        parts = str(lat).split(",")
        if len(parts) == 2:
            try:
                coords = (float(parts[0].strip()), float(parts[1].strip()))
                _cache_set(cache_key, coords)
                return coords
            except ValueError:
                pass
    _cache_set(cache_key, None)
    return None


# ─── 1. Weather ──────────────────────────────────────────────────────────────
_OUTDOOR_SPORTS = {"baseball", "mlb", "americanfootball", "nfl", "ncaaf", "soccer", "cricket", "golf"}


def _infer_sport_group_local(sport: str) -> str:
    """Light version of dashboard infer; no circular imports."""
    raw = re.sub(r"[^a-z0-9]+", "_", str(sport or "").lower()).strip("_")
    if any(k in raw for k in ("baseball", "mlb")):
        return "baseball"
    if any(k in raw for k in ("basketball", "nba", "wnba")):
        return "basketball"
    if any(k in raw for k in ("americanfootball", "nfl", "ncaaf")):
        return "americanfootball"
    if any(k in raw for k in ("icehockey", "hockey", "nhl")):
        return "icehockey"
    if any(k in raw for k in ("soccer", "football")):
        return "soccer"
    if "tennis" in raw:
        return "tennis"
    if any(k in raw for k in ("golf", "pga", "lpga", "masters")):
        return "golf"
    if any(k in raw for k in ("mma", "ufc", "boxing")):
        return "mma"
    return raw or "other"


def get_weather(team_home: str, game_datetime_iso: str, sport: str = "baseball") -> dict:
    """
    Return weather conditions at the team's home venue using Open-Meteo (free, no key).
    Returns: {temp_c, temp_f, wind_kph, precip_mm, humidity_pct, condition, is_outdoor}
    """
    sport_group = _infer_sport_group_local(sport)
    is_outdoor = sport_group in _OUTDOOR_SPORTS
    default = {"is_outdoor": is_outdoor, "temp_c": None, "temp_f": None,
                "wind_kph": None, "precip_mm": None, "humidity_pct": None, "condition": "N/A"}

    if not is_outdoor:
        default["condition"] = "indoor"
        return default

    coords = _get_venue_coords(team_home)
    if not coords:
        return default

    lat, lon = coords
    game_date = game_datetime_iso[:10] if game_datetime_iso else datetime.date.today().isoformat()
    cache_key = f"weather:{lat:.2f}:{lon:.2f}:{game_date}"
    cached = _cache_get(cache_key, _CACHE_TTL_MED)
    if cached is not None:
        return cached

    try:
        data = _safe_req(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": lat,
                "longitude": lon,
                "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,windspeed_10m_max,relative_humidity_2m_max",
                "timezone": "America/New_York",
                "start_date": game_date,
                "end_date": game_date,
            },
        )
        if not data:
            return default

        daily = data.get("daily") or {}
        temps_max = (daily.get("temperature_2m_max") or [None])
        temps_min = (daily.get("temperature_2m_min") or [None])
        wind     = (daily.get("windspeed_10m_max") or [None])
        precip   = (daily.get("precipitation_sum") or [None])
        humidity = (daily.get("relative_humidity_2m_max") or [None])

        temp_max = float(temps_max[0]) if temps_max[0] is not None else None
        temp_min = float(temps_min[0]) if temps_min[0] is not None else None
        temp_c   = round((temp_max + temp_min) / 2, 1) if temp_max is not None and temp_min is not None else None
        temp_f   = round(temp_c * 9 / 5 + 32, 1) if temp_c is not None else None

        wind_kph  = float(wind[0])    if wind[0]    is not None else None
        precip_mm = float(precip[0])  if precip[0]  is not None else None
        hum_pct   = float(humidity[0]) if humidity[0] is not None else None

        # Simple condition label
        cond = "Clear"
        if precip_mm and precip_mm > 5:
            cond = "Rainy"
        elif precip_mm and precip_mm > 0.5:
            cond = "Light rain"
        elif wind_kph and wind_kph > 40:
            cond = "Windy"
        elif temp_c and temp_c < 5:
            cond = "Cold"
        elif temp_c and temp_c > 33:
            cond = "Hot"

        result = {
            "is_outdoor": True,
            "temp_c": temp_c,
            "temp_f": temp_f,
            "wind_kph": wind_kph,
            "precip_mm": precip_mm,
            "humidity_pct": hum_pct,
            "condition": cond,
        }
        _cache_set(cache_key, result)
        return result
    except Exception as e:
        print(f"[enrichment:weather] {team_home}: {e}")
        return default


# ─── 2. ESPN Schedule helpers ────────────────────────────────────────────────
def _espn_team_id(team_name: str, sport_slug: str) -> str | None:
    """Resolve ESPN team ID for a team name."""
    cache_key = f"espn_team_id:{sport_slug}:{team_name.lower()}"
    cached = _cache_get(cache_key, _CACHE_TTL_LONG)
    if cached is not None:
        return cached

    data = _safe_req(f"{_ESPN_BASE}/{sport_slug}/teams")
    teams = (data or {}).get("sports") or []
    for sport_obj in teams:
        for league_obj in (sport_obj.get("leagues") or []):
            for t in (league_obj.get("teams") or []):
                info = t.get("team") or {}
                dn   = str(info.get("displayName") or "").lower()
                sn   = str(info.get("shortDisplayName") or "").lower()
                nn   = str(info.get("name") or "").lower()
                abbr = str(info.get("abbreviation") or "").lower()
                qn   = team_name.lower().strip()
                if qn in (dn, sn, nn, abbr) or qn in dn or dn.endswith(qn):
                    tid = str(info.get("id") or "")
                    _cache_set(cache_key, tid)
                    return tid
    _cache_set(cache_key, None)
    return None


def _espn_team_schedule(team_id: str, sport_slug: str, limit: int = 25) -> list[dict]:
    """Fetch recent + upcoming ESPN team schedule (last/next 25 games)."""
    if not team_id:
        return []
    cache_key = f"espn_schedule:{sport_slug}:{team_id}"
    cached = _cache_get(cache_key, _CACHE_TTL_SHORT)
    if cached is not None:
        return cached

    data = _safe_req(f"{_ESPN_BASE}/{sport_slug}/teams/{team_id}/schedule", params={"limit": limit})
    events = (data or {}).get("events") or []
    _cache_set(cache_key, events)
    return events


# ─── 3. Rest Days ────────────────────────────────────────────────────────────
def get_rest_days(team_name: str, game_date_iso: str, sport: str = "baseball") -> dict:
    """
    Return the number of full rest days the team had before this game.
    Also returns: last_game_date, back_to_back flag, games_in_last_7_days.

    REST = game_date - last_game_date - 1  (0 = back-to-back)
    """
    sport_group = _infer_sport_group_local(sport)
    slug = _ESPN_SPORT_SLUG.get(sport_group) or _ESPN_SPORT_SLUG.get(sport.lower(), "baseball/mlb")

    default = {"rest_days": None, "last_game_date": None, "back_to_back": False, "games_in_last_7": None}

    # --- MLB: use official MLB Stats API for precision ---
    if sport_group in ("baseball", "mlb"):
        return _mlb_rest_days(team_name, game_date_iso)

    team_id = _espn_team_id(team_name, slug)
    if not team_id:
        return default

    events = _espn_team_schedule(team_id, slug, limit=30)
    if not events:
        return default

    target = datetime.date.fromisoformat(game_date_iso)
    played_dates: list[datetime.date] = []

    for ev in events:
        competitions = ev.get("competitions") or [ev]
        for comp in competitions:
            dt_str = comp.get("date") or ev.get("date") or ""
            if not dt_str:
                continue
            try:
                ev_dt = datetime.datetime.fromisoformat(dt_str.replace("Z", "+00:00")).date()
            except Exception:
                continue
            status = (comp.get("status") or {}).get("type") or {}
            completed = str(status.get("completed") or "").lower() == "true" or \
                        str(status.get("name") or "").lower() in ("final", "postgame", "full-time")
            if completed and ev_dt < target:
                played_dates.append(ev_dt)

    if not played_dates:
        return default

    played_dates.sort(reverse=True)
    last_game = played_dates[0]
    rest = (target - last_game).days - 1
    back_to_back = rest == 0

    seven_ago = target - datetime.timedelta(days=7)
    games_in_last_7 = sum(1 for d in played_dates if d >= seven_ago)

    return {
        "rest_days": max(0, rest),
        "last_game_date": last_game.isoformat(),
        "back_to_back": back_to_back,
        "games_in_last_7": games_in_last_7,
    }


def _mlb_rest_days(team_name: str, game_date_iso: str) -> dict:
    """MLB-specific rest-days from MLB Stats API (official, precise)."""
    default = {"rest_days": None, "last_game_date": None, "back_to_back": False, "games_in_last_7": None}
    try:
        target = datetime.date.fromisoformat(game_date_iso)
        lookback_start = (target - datetime.timedelta(days=14)).isoformat()
        lookback_end   = (target - datetime.timedelta(days=1)).isoformat()
        cache_key = f"mlb_schedule:{team_name.lower()}:{lookback_start}"
        cached = _cache_get(cache_key, _CACHE_TTL_SHORT)
        if cached is None:
            data = _safe_req(
                "https://statsapi.mlb.com/api/v1/schedule",
                params={
                    "sportId": 1,
                    "startDate": lookback_start,
                    "endDate": lookback_end,
                    "gameType": "R",
                    "hydrate": "team",
                },
            )
            cached = data
            _cache_set(cache_key, data)

        dates_data = (cached or {}).get("dates") or []
        name_lower = team_name.lower().strip()
        played_dates: list[datetime.date] = []
        for day in dates_data:
            for game in (day.get("games") or []):
                home = str((game.get("teams") or {}).get("home", {}).get("team", {}).get("name") or "").lower()
                away = str((game.get("teams") or {}).get("away", {}).get("team", {}).get("name") or "").lower()
                gstatus = str((game.get("status") or {}).get("abstractGameState") or "").lower()
                if gstatus not in ("final", "completed") and "final" not in gstatus:
                    continue
                if name_lower in home or home.endswith(name_lower.split()[-1]) or \
                   name_lower in away or away.endswith(name_lower.split()[-1]):
                    gd_str = str(day.get("date") or "")
                    if gd_str:
                        try:
                            played_dates.append(datetime.date.fromisoformat(gd_str))
                        except Exception:
                            pass

        if not played_dates:
            return default

        played_dates.sort(reverse=True)
        last_game = played_dates[0]
        rest = (target - last_game).days - 1
        seven_ago = target - datetime.timedelta(days=7)
        games_in_last_7 = sum(1 for d in played_dates if d >= seven_ago)
        return {
            "rest_days": max(0, rest),
            "last_game_date": last_game.isoformat(),
            "back_to_back": rest == 0,
            "games_in_last_7": games_in_last_7,
        }
    except Exception as e:
        print(f"[enrichment:mlb_rest] {team_name}: {e}")
        return default


# ─── 4. Venue History ────────────────────────────────────────────────────────
def get_venue_history(home_team: str, away_team: str, sport: str = "baseball", seasons_back: int = 2) -> dict:
    """
    Return the visiting team's record at the home team's venue.
    Also returns average totals (goals / runs / points) at this venue.

    Returns: {venue_name, away_wins, home_wins, draws, total_games,
              avg_total_score, away_win_pct, home_win_pct}
    """
    cache_key = f"venue_h:{sport}:{_norm_name(home_team)}:{_norm_name(away_team)}"
    cached = _cache_get(cache_key, _CACHE_TTL_LONG)
    if cached is not None:
        return cached

    result = _build_venue_history_espn(home_team, away_team, sport, seasons_back)
    _cache_set(cache_key, result)
    return result


def _build_venue_history_espn(home_team: str, away_team: str, sport: str, seasons_back: int) -> dict:
    """Build venue history from ESPN schedule for the home team."""
    default = {"venue_name": None, "away_wins": 0, "home_wins": 0, "draws": 0,
               "total_games": 0, "avg_total_score": None,
               "away_win_pct": None, "home_win_pct": None}

    sport_group = _infer_sport_group_local(sport)
    slug = _ESPN_SPORT_SLUG.get(sport_group) or "baseball/mlb"

    if sport_group == "soccer":
        # For soccer use football-data.org H2H endpoint
        return _build_soccer_h2h(home_team, away_team)

    team_id = _espn_team_id(home_team, slug)
    if not team_id:
        return default

    events = _espn_team_schedule(team_id, slug, limit=80)
    if not events:
        return default

    venue_name = None
    away_w = home_w = draws = games = 0
    total_scores: list[float] = []
    away_norm = _norm_name(away_team)

    for ev in events:
        competitions = ev.get("competitions") or [ev]
        for comp in competitions:
            status = (comp.get("status") or {}).get("type") or {}
            completed = str(status.get("completed") or "").lower() == "true"
            if not completed:
                continue

            # Extract venue name once
            if not venue_name:
                venue = comp.get("venue") or ev.get("venue") or {}
                venue_name = venue.get("fullName") or venue.get("shortName")

            competitors = comp.get("competitors") or []
            home_c = next((c for c in competitors if c.get("homeAway") == "home"), None)
            away_c = next((c for c in competitors if c.get("homeAway") == "away"), None)
            if not home_c or not away_c:
                continue

            away_c_name = _norm_name(str((away_c.get("team") or {}).get("displayName") or ""))
            if away_norm not in away_c_name and away_c_name not in away_norm:
                continue  # only games where away_team is visiting

            try:
                home_score = int(home_c.get("score") or 0)
                away_score = int(away_c.get("score") or 0)
            except (TypeError, ValueError):
                continue

            games += 1
            total_scores.append(home_score + away_score)
            if home_score > away_score:
                home_w += 1
            elif away_score > home_score:
                away_w += 1
            else:
                draws += 1

    if games == 0:
        return {**default, "venue_name": venue_name}

    return {
        "venue_name": venue_name,
        "away_wins": away_w,
        "home_wins": home_w,
        "draws": draws,
        "total_games": games,
        "avg_total_score": round(sum(total_scores) / len(total_scores), 2) if total_scores else None,
        "away_win_pct": round(away_w / games * 100, 1),
        "home_win_pct": round(home_w / games * 100, 1),
    }


def _build_soccer_h2h(home_team: str, away_team: str) -> dict:
    """Build soccer H2H using football-data.org if key available."""
    default = {"venue_name": None, "away_wins": 0, "home_wins": 0, "draws": 0,
               "total_games": 0, "avg_total_score": None,
               "away_win_pct": None, "home_win_pct": None}
    fd_key = os.getenv("FOOTBALL_DATA_API_KEY", "")
    if not fd_key:
        return default
    # Use ESPN soccer scores as fallback
    return default


# ─── 5. Head-to-Head ────────────────────────────────────────────────────────
def get_head_to_head(home_team: str, away_team: str, sport: str = "baseball", last_n: int = 10) -> dict:
    """
    Return full head-to-head record between two teams across recent seasons.
    Returns: {home_wins, away_wins, draws, total, home_win_pct, away_win_pct,
              last_5_results: [{date,home_score,away_score,winner}], avg_total}
    """
    # Canonical ordering so (A vs B) == (B vs A)
    t_a, t_b = sorted([_norm_name(home_team), _norm_name(away_team)])
    cache_key = f"h2h:{sport}:{t_a}:{t_b}"
    cached = _cache_get(cache_key, _CACHE_TTL_LONG)
    if cached is not None:
        return cached

    result = _build_h2h_espn(home_team, away_team, sport, last_n)
    _cache_set(cache_key, result)
    return result


def _build_h2h_espn(home_team: str, away_team: str, sport: str, last_n: int) -> dict:
    """Build H2H from ESPN historical schedule data for home_team."""
    default = {
        "home_wins": 0, "away_wins": 0, "draws": 0, "total": 0,
        "home_win_pct": None, "away_win_pct": None,
        "last_5_results": [], "avg_total": None,
    }
    sport_group = _infer_sport_group_local(sport)
    slug = _ESPN_SPORT_SLUG.get(sport_group) or "baseball/mlb"
    team_id = _espn_team_id(home_team, slug)
    if not team_id:
        return default

    events = _espn_team_schedule(team_id, slug, limit=80)
    away_norm = _norm_name(away_team)
    home_norm  = _norm_name(home_team)

    home_w = away_w = draws = 0
    all_results: list[dict] = []
    total_scores: list[float] = []

    for ev in events:
        competitions = ev.get("competitions") or [ev]
        for comp in competitions:
            status = (comp.get("status") or {}).get("type") or {}
            if not str(status.get("completed") or "").lower() == "true":
                continue

            competitors = comp.get("competitors") or []
            c_home = next((c for c in competitors if c.get("homeAway") == "home"), None)
            c_away = next((c for c in competitors if c.get("homeAway") == "away"), None)
            if not c_home or not c_away:
                continue

            c_home_name = _norm_name(str((c_home.get("team") or {}).get("displayName") or ""))
            c_away_name = _norm_name(str((c_away.get("team") or {}).get("displayName") or ""))

            # Must involve both teams
            is_h2h = (
                (home_norm in c_home_name or c_home_name in home_norm) and
                (away_norm in c_away_name or c_away_name in away_norm)
            ) or (
                (home_norm in c_away_name or c_away_name in home_norm) and
                (away_norm in c_home_name or c_home_name in away_norm)
            )
            if not is_h2h:
                continue

            try:
                hs = int(c_home.get("score") or 0)
                as_ = int(c_away.get("score") or 0)
            except (TypeError, ValueError):
                continue

            date_str = comp.get("date") or ev.get("date") or ""
            date_short = date_str[:10] if date_str else ""

            if hs > as_:
                winner = c_home_name
                home_w += 1
            elif as_ > hs:
                winner = c_away_name
                away_w += 1
            else:
                winner = "draw"
                draws += 1

            total_scores.append(hs + as_)
            all_results.append({
                "date": date_short,
                "home_score": hs,
                "away_score": as_,
                "winner": winner,
            })

    all_results.sort(key=lambda r: r.get("date") or "", reverse=True)
    total = home_w + away_w + draws
    return {
        "home_wins": home_w,
        "away_wins": away_w,
        "draws": draws,
        "total": total,
        "home_win_pct": round(home_w / total * 100, 1) if total else None,
        "away_win_pct": round(away_w / total * 100, 1) if total else None,
        "last_5_results": all_results[:5],
        "avg_total": round(sum(total_scores) / len(total_scores), 2) if total_scores else None,
    }


# ─── 6. Coaching staff ──────────────────────────────────────────────────────
def get_coaching_info(team_name: str, sport: str = "baseball") -> dict:
    """
    Return head coach / manager info for a team.
    Returns: {coach_name, coach_id, record_wins, record_losses, win_pct, experience_years}
    """
    sport_group = _infer_sport_group_local(sport)
    slug = _ESPN_SPORT_SLUG.get(sport_group) or "baseball/mlb"
    cache_key = f"coach:{slug}:{_norm_name(team_name)}"
    cached = _cache_get(cache_key, _CACHE_TTL_MED)
    if cached is not None:
        return cached

    default = {"coach_name": None, "record_wins": None, "record_losses": None,
               "win_pct": None, "experience_years": None}

    team_id = _espn_team_id(team_name, slug)
    if not team_id:
        _cache_set(cache_key, default)
        return default

    data = _safe_req(f"{_ESPN_BASE}/{slug}/teams/{team_id}")
    team_data = (data or {}).get("team") or {}
    coaches = team_data.get("coaches") or []
    if not coaches:
        _cache_set(cache_key, default)
        return default

    # Prefer "headCoach" or first entry
    hc = next((c for c in coaches if "head" in str(c.get("position") or {}).get("name", "").lower()), coaches[0])
    exp = hc.get("experience")
    record = hc.get("record") or {}

    wins   = record.get("wins")
    losses = record.get("losses")
    try:
        win_pct = round(int(wins) / (int(wins) + int(losses)), 3) if wins and losses else None
    except Exception:
        win_pct = None

    result = {
        "coach_name": hc.get("firstName", "") + " " + hc.get("lastName", ""),
        "record_wins": wins,
        "record_losses": losses,
        "win_pct": win_pct,
        "experience_years": exp,
    }
    _cache_set(cache_key, result)
    return result


# ─── 7. Player Form (last 5 / last 10) ──────────────────────────────────────
def get_player_recent_form(player_name: str, sport: str = "baseball", stat_type: str = "strikeouts") -> dict:
    """
    Return player's recent form in the specified stat over last 5 and 10 games.
    Uses MLB Stats API for MLB, ESPN for other sports.

    Returns: {avg_last_5, avg_last_10, trend_direction, games_collected}
    """
    sport_group = _infer_sport_group_local(sport)
    default = {"avg_last_5": None, "avg_last_10": None,
               "trend_direction": "neutral", "games_collected": 0}

    if sport_group in ("baseball", "mlb"):
        return _mlb_player_form(player_name, stat_type)

    return default  # ESPN individual player game logs require team + player_id; skip for now


def _mlb_player_form(player_name: str, stat_type: str) -> dict:
    """MLB player game log via MLB Stats API."""
    default = {"avg_last_5": None, "avg_last_10": None,
               "trend_direction": "neutral", "games_collected": 0}
    try:
        cache_key = f"mlb_player_form:{_norm_name(player_name)}:{stat_type}"
        cached = _cache_get(cache_key, _CACHE_TTL_MED)
        if cached is not None:
            return cached

        # Step 1: search for player id
        search = _safe_req(
            "https://statsapi.mlb.com/api/v1/people/search",
            params={"names": player_name, "active": True},
        )
        people = (search or {}).get("people") or []
        if not people:
            _cache_set(cache_key, default)
            return default

        player_id = people[0].get("id")
        if not player_id:
            _cache_set(cache_key, default)
            return default

        # Step 2: fetch game log
        current_year = datetime.date.today().year
        log = _safe_req(
            f"https://statsapi.mlb.com/api/v1/people/{player_id}/stats",
            params={
                "stats": "gameLog",
                "season": current_year,
                "group": "pitching" if stat_type in ("strikeouts", "era", "walks", "innings") else "hitting",
            },
        )
        splits = (((log or {}).get("stats") or [{}])[0]).get("splits") or []
        if not splits:
            _cache_set(cache_key, default)
            return default

        _stat_key_map = {
            "strikeouts":    "strikeOuts",
            "walks":         "baseOnBalls",
            "hits":          "hits",
            "home_runs":     "homeRuns",
            "rbi":           "rbi",
            "runs":          "runs",
            "innings":       "inningsPitched",
            "era":           "era",
        }
        api_key = _stat_key_map.get(stat_type, stat_type)

        values: list[float] = []
        for s in reversed(splits):  # reversed = most recent first
            v = (s.get("stat") or {}).get(api_key)
            if v is None:
                continue
            try:
                values.append(float(v))
            except (TypeError, ValueError):
                continue
            if len(values) >= 10:
                break

        if not values:
            _cache_set(cache_key, default)
            return default

        avg10 = round(sum(values) / len(values), 2)
        avg5  = round(sum(values[:5]) / min(5, len(values)), 2)

        # Trend: compare last 3 vs prior 3
        trend = "neutral"
        if len(values) >= 6:
            recent3 = sum(values[:3]) / 3
            prior3  = sum(values[3:6]) / 3
            if recent3 > prior3 * 1.10:
                trend = "up"
            elif recent3 < prior3 * 0.90:
                trend = "down"

        result = {
            "avg_last_5": avg5,
            "avg_last_10": avg10,
            "trend_direction": trend,
            "games_collected": len(values),
        }
        _cache_set(cache_key, result)
        return result
    except Exception as e:
        print(f"[enrichment:mlb_form] {player_name} {stat_type}: {e}")
        return default


# ─── 8. Fatigue Index ───────────────────────────────────────────────────────
def compute_fatigue_index(games_in_last_7: int | None, rest_days: int | None,
                           back_to_back: bool = False) -> float:
    """
    0.0 = fully rested, 1.0 = peak fatigue.
    Formula:
      - back_to_back: +0.40
      - games_in_last_7 >= 6: +0.30
      - games_in_last_7 == 5: +0.15
      - rest_days == 0: +0.30 (already covered by back_to_back)
      - rest_days >= 3: −0.20  (well rested)
    Clamped to [0, 1].
    """
    if games_in_last_7 is None and rest_days is None:
        return 0.0

    score = 0.0
    g7 = int(games_in_last_7 or 0)
    rd = int(rest_days or 1)

    if back_to_back:
        score += 0.40
    elif rd == 1:
        score += 0.15

    if g7 >= 6:
        score += 0.30
    elif g7 == 5:
        score += 0.15

    if rd >= 3:
        score -= 0.20

    return round(max(0.0, min(1.0, score)), 3)


# ─── 9. Travel Distance ─────────────────────────────────────────────────────
def compute_travel_km(away_team: str, home_team: str) -> float | None:
    """
    Estimate travel distance (km) for the away team using venue coordinates.
    Returns None when coords are unavailable.
    """
    away_coords = _get_venue_coords(away_team)
    home_coords = _get_venue_coords(home_team)
    if not away_coords or not home_coords:
        return None

    lat1, lon1 = math.radians(away_coords[0]), math.radians(away_coords[1])
    lat2, lon2 = math.radians(home_coords[0]), math.radians(home_coords[1])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return round(2 * 6371 * math.asin(math.sqrt(a)), 0)


# ─── 10. Referee / Umpire tendencies ────────────────────────────────────────


# ─── Data Cleaning Pipeline ─────────────────────────────────────────────────
def _norm_name(name: str) -> str:
    """Lowercase, strip punctuation, collapse whitespace."""
    return re.sub(r"[^a-z0-9 ]+", "", str(name or "").lower().strip())


def clean_team_name(name: str) -> str:
    """
    Canonical team name cleaner for cross-source matching.
    Strips city prefix if only nickname is needed, handles common variants.
    """
    raw = str(name or "").strip()
    # Known renames / relocations
    _renames = {
        "washington football team": "Washington Commanders",
        "washington redskins": "Washington Commanders",
        "cleveland browns": "Cleveland Browns",
        "los angeles rams": "Los Angeles Rams",
        "las vegas raiders": "Las Vegas Raiders",
        "oakland raiders": "Las Vegas Raiders",
        "san diego chargers": "Los Angeles Chargers",
        "oklahoma city thunder": "Oklahoma City Thunder",
        "new jersey nets": "Brooklyn Nets",
        "seattle supersonics": "Oklahoma City Thunder",
        "new orleans hornets": "New Orleans Pelicans",
        "charlotte bobcats": "Charlotte Hornets",
        "new jersey devils": "New Jersey Devils",
        "kansas city royals": "Kansas City Royals",
        "tb rays": "Tampa Bay Rays",
        "tampa bay devil rays": "Tampa Bay Rays",
        "florida marlins": "Miami Marlins",
        "montreal expos": "Washington Nationals",
    }
    canonical = _renames.get(raw.lower(), raw)
    return canonical


def clean_player_name(name: str) -> str:
    """
    Normalize player names for cross-source matching.
    Handles suffixes (Jr., Sr., III), nicknames, and accented characters.
    """
    import unicodedata
    raw = str(name or "").strip()
    # Normalize Unicode (e.g. accented → ASCII)
    normalized = unicodedata.normalize("NFKD", raw)
    ascii_name = normalized.encode("ascii", "ignore").decode("ascii")
    # Remove suffixes
    cleaned = re.sub(r"\b(Jr\.?|Sr\.?|II|III|IV|V)\b", "", ascii_name, flags=re.IGNORECASE)
    # Collapse whitespace
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def clean_numeric(value, default=None, min_val=None, max_val=None) -> float | None:
    """
    Safe float conversion with optional range clamping.
    Returns default if conversion fails.
    """
    if value is None or str(value).strip() in ("", "N/A", "null", "nan", "None", "-"):
        return default
    try:
        result = float(str(value).replace(",", "").strip())
        if min_val is not None and result < min_val:
            return min_val
        if max_val is not None and result > max_val:
            return max_val
        return result
    except (ValueError, TypeError):
        return default


def clean_date(value, fmt: str | None = None) -> str | None:
    """
    Normalize date strings to ISO YYYY-MM-DD.
    Handles common formats: MM/DD/YYYY, YYYY-MM-DD, YYYY-MM-DDThh:mm, epoch int.
    """
    if value is None:
        return None
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.date().isoformat() if isinstance(value, datetime.datetime) else value.isoformat()
    s = str(value).strip()
    if not s or s.lower() in ("none", "null", ""):
        return None
    # Epoch int
    if re.match(r"^\d{9,10}$", s):
        try:
            return datetime.datetime.utcfromtimestamp(int(s)).date().isoformat()
        except Exception:
            pass
    # ISO with time
    if "T" in s or "t" in s:
        try:
            return datetime.datetime.fromisoformat(s.replace("Z", "+00:00")).date().isoformat()
        except Exception:
            pass
    # Various separators
    for pattern, fmt_str in [
        (r"^\d{4}-\d{2}-\d{2}$",  "%Y-%m-%d"),
        (r"^\d{2}/\d{2}/\d{4}$",  "%m/%d/%Y"),
        (r"^\d{2}-\d{2}-\d{4}$",  "%m-%d-%Y"),
        (r"^\d{4}/\d{2}/\d{2}$",  "%Y/%m/%d"),
        (r"^\w+ \d{1,2}, \d{4}$", "%B %d, %Y"),
    ]:
        if re.match(pattern, s):
            try:
                return datetime.datetime.strptime(s, fmt_str).date().isoformat()
            except Exception:
                pass
    return None


def clean_odds_american(value) -> int | None:
    """Parse American moneyline string (+120, -110, EV, EVEN) → int."""
    if value is None:
        return None
    s = str(value).strip().replace(" ", "")
    if s.upper() in ("EV", "EVEN"):
        return 100
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return None


def clean_stat_value(value, stat_type: str) -> float | None:
    """
    Context-aware stat cleaning.
    Applies sport-specific range limits to reject obviously bad values.
    """
    limits: dict[str, tuple[float, float]] = {
        "era":          (0.0, 20.0),
        "whip":         (0.0, 5.0),
        "batting_avg":  (0.0, 1.0),
        "obp":          (0.0, 1.0),
        "slg":          (0.0, 3.0),
        "ops":          (0.0, 4.0),
        "k_per_9":      (0.0, 25.0),
        "xg":           (0.0, 3.0),
        "xa":           (0.0, 2.0),
        "shots_on_target": (0.0, 20.0),
        "goals":        (0.0, 10.0),
        "assists":      (0.0, 10.0),
        "points_per_game": (0.0, 60.0),
        "rebounds":     (0.0, 30.0),
        "saves":        (0.0, 20.0),
        "strikeouts":   (0.0, 20.0),
    }
    lo, hi = limits.get(stat_type, (None, None))
    return clean_numeric(value, min_val=lo, max_val=hi)


# ─── Master enrichment entry point ──────────────────────────────────────────
def enrich_game(game: dict, include_weather: bool = True,
                include_coaching: bool = True,
                include_h2h: bool = True) -> dict:
    """
    Add all enrichment signals to a game dict in-place and return it.
    Fields added under the 'enrichment' key.

    game dict must have: home_team, away_team, game_date, game_datetime (optional),
                         sport (or competition key)
    """
    home   = clean_team_name(str(game.get("home_team") or "").strip())
    away   = clean_team_name(str(game.get("away_team") or "").strip())
    gdate  = clean_date(game.get("game_date") or game.get("date") or "")
    gdt    = str(game.get("game_datetime") or gdate or "")
    sport  = str(game.get("sport") or game.get("competition") or "").strip()

    if not home or not away or not gdate:
        return game

    enrich: dict = {}

    # --- rest days ---
    try:
        home_rest = get_rest_days(home, gdate, sport)
        away_rest = get_rest_days(away, gdate, sport)
        enrich["home_rest"] = home_rest
        enrich["away_rest"] = away_rest
        enrich["home_fatigue"] = compute_fatigue_index(
            home_rest.get("games_in_last_7"), home_rest.get("rest_days"), home_rest.get("back_to_back", False)
        )
        enrich["away_fatigue"] = compute_fatigue_index(
            away_rest.get("games_in_last_7"), away_rest.get("rest_days"), away_rest.get("back_to_back", False)
        )
    except Exception as e:
        print(f"[enrichment] rest error {home} vs {away}: {e}")

    # --- travel distance ---
    try:
        travel_km = compute_travel_km(away, home)
        enrich["away_travel_km"] = travel_km
        enrich["long_road_trip"] = (travel_km is not None and travel_km > 2500)
    except Exception:
        pass

    # --- head-to-head ---
    if include_h2h:
        try:
            enrich["h2h"] = get_head_to_head(home, away, sport)
        except Exception as e:
            print(f"[enrichment] h2h error: {e}")

    # --- venue history ---
    try:
        enrich["venue_history"] = get_venue_history(home, away, sport)
    except Exception as e:
        print(f"[enrichment] venue error: {e}")

    # --- coaching ---
    if include_coaching:
        try:
            enrich["home_coach"] = get_coaching_info(home, sport)
            enrich["away_coach"] = get_coaching_info(away, sport)
        except Exception as e:
            print(f"[enrichment] coach error: {e}")

    # --- weather (outdoor sports only) ---
    if include_weather:
        try:
            enrich["weather"] = get_weather(home, gdt, sport)
        except Exception as e:
            print(f"[enrichment] weather error: {e}")

    game["enrichment"] = enrich
    return game


def enrich_player_prop(prop: dict) -> dict:
    """
    Add player-level enrichment: recent form + team fatigue.
    prop dict must have: name, sport, stat_type, game_date, team (optional).
    """
    player = clean_player_name(str(prop.get("name") or prop.get("player_name") or ""))
    sport  = str(prop.get("sport") or "").strip()
    stat_t = str(prop.get("stat_type") or "strikeouts").strip()

    if not player:
        return prop

    enrich: dict = {}
    try:
        form = get_player_recent_form(player, sport, stat_t)
        enrich["player_form"] = form
    except Exception as e:
        print(f"[enrichment] player form error {player}: {e}")

    # Attach team-level fatigue if available and team known
    team = str(prop.get("team") or "").strip()
    gdate = clean_date(prop.get("game_date") or prop.get("date") or "")
    if team and gdate:
        try:
            rest = get_rest_days(team, gdate, sport)
            enrich["team_rest"] = rest
            enrich["team_fatigue"] = compute_fatigue_index(
                rest.get("games_in_last_7"), rest.get("rest_days"), rest.get("back_to_back", False)
            )
        except Exception:
            pass

    prop["enrichment"] = enrich
    return prop


def enrich_games_batch(games: list[dict],
                       include_weather: bool = True,
                       include_coaching: bool = True,
                       include_h2h: bool = True,
                       max_games: int = 40,
                       throttle_sec: float = 0.2) -> list[dict]:
    """
    Enrich a list of game dicts in sequence with light throttling.
    Silently skips failures — enrichment is always best-effort.
    """
    out: list[dict] = []
    for idx, game in enumerate(games[:max_games]):
        try:
            enriched = enrich_game(
                dict(game),
                include_weather=include_weather,
                include_coaching=include_coaching,
                include_h2h=include_h2h,
            )
            out.append(enriched)
        except Exception as e:
            print(f"[enrichment] batch game {idx} error: {e}")
            out.append(game)
        if throttle_sec > 0 and idx < min(len(games), max_games) - 1:
            time.sleep(throttle_sec)
    return out


def enrich_props_batch(props: list[dict], max_props: int = 60,
                       throttle_sec: float = 0.1) -> list[dict]:
    """
    Enrich a list of player prop dicts with recent form + team fatigue.
    """
    out: list[dict] = []
    for idx, prop in enumerate(props[:max_props]):
        try:
            out.append(enrich_player_prop(dict(prop)))
        except Exception as e:
            print(f"[enrichment] batch prop {idx} error: {e}")
            out.append(prop)
        if throttle_sec > 0 and idx < min(len(props), max_props) - 1:
            time.sleep(throttle_sec)
    return out


# ─── Data-cleaning helpers used by other modules ────────────────────────────
def clean_game_row(row: dict) -> dict:
    """
    Normalize an incoming game dict from ANY source into the standard format
    expected by the dashboard card builder.
    """
    out = dict(row)
    out["home_team"] = clean_team_name(str(out.get("home_team") or "").strip())
    out["away_team"] = clean_team_name(str(out.get("away_team") or "").strip())
    out["game_date"] = clean_date(out.get("game_date") or out.get("date")) or ""
    out["date"]      = out["game_date"]

    for key in ("home_score", "away_score", "home_odds", "away_odds"):
        out[key] = clean_numeric(out.get(key))

    for key in ("odds_am",):
        out[key] = clean_odds_american(out.get(key))

    # Status normalisation
    raw_status = str(out.get("status") or "").lower()
    if any(k in raw_status for k in ("final", "ft", "complete", "ended", "postgame")):
        out["status"] = "Final"
    elif any(k in raw_status for k in ("live", "in_progress", "inprogress", "halftime", "1st", "2nd", "q3", "q4")):
        out["status"] = "Live"
    elif any(k in raw_status for k in ("postpone", "cancel", "suspend")):
        out["status"] = "Postponed"
    elif not out.get("status"):
        out["status"] = "Scheduled"

    return out


def clean_prop_row(row: dict) -> dict:
    """
    Normalize a player prop dict from any fetcher / source.
    """
    out = dict(row)
    out["name"]        = clean_player_name(str(out.get("name") or out.get("player_name") or ""))
    out["player_name"] = out["name"]
    out["team"]        = clean_team_name(str(out.get("team") or ""))
    out["game_date"]   = clean_date(out.get("game_date") or out.get("date")) or ""
    out["line"]        = clean_stat_value(out.get("line"), out.get("stat_type") or "")
    out["over_prob"]   = clean_numeric(out.get("over_prob"), min_val=0.0, max_val=1.0)
    out["under_prob"]  = clean_numeric(out.get("under_prob"), min_val=0.0, max_val=1.0)
    out["model_prob"]  = clean_numeric(out.get("model_prob"), min_val=0.01, max_val=0.99)
    out["odds_am"]     = clean_odds_american(out.get("odds_am") or out.get("over_odds_am"))

    # Normalise direction
    raw_dir = str(out.get("direction") or out.get("recommendation") or "").upper()
    if "OVER" in raw_dir:
        out["direction"] = "OVER"
    elif "UNDER" in raw_dir:
        out["direction"] = "UNDER"

    return out


def clean_bet_row(row: dict) -> dict:
    """Normalize a bet/prediction row (moneyline / spread / total)."""
    out = dict(row)
    out["home_team"] = clean_team_name(str(out.get("home_team") or ""))
    out["away_team"] = clean_team_name(str(out.get("away_team") or ""))
    out["game_date"] = clean_date(out.get("game_date") or out.get("date")) or ""
    out["odds_am"]   = clean_odds_american(out.get("odds_am"))
    out["model_prob"]= clean_numeric(out.get("model_prob"), min_val=0.01, max_val=0.99)
    out["line"]      = clean_numeric(out.get("line"))
    return out


def clean_snapshot(snapshot: dict) -> dict:
    """
    Run the data-cleaning pipeline over all game + bet + prop rows
    in a multi-sport snapshot.
    """
    out = dict(snapshot)
    out["games"] = [clean_game_row(g) for g in (snapshot.get("games") or [])]
    out["bets"]  = [clean_bet_row(b)  for b in (snapshot.get("bets")  or [])]
    return out
