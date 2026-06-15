"""
soccer_fetcher.py — Multi-tournament soccer data hub
=====================================================
Live-first free soccer data with automatic source failover.

Supported competitions (free tier):
  PL   Premier League (England)
  BL1  Bundesliga (Germany)
  SA   Serie A (Italy)
  PD   La Liga (Spain)
  FL1  Ligue 1 (France)
  DED  Eredivisie (Netherlands)
  PPL  Primeira Liga (Portugal)
  CL   UEFA Champions League
  EC   UEFA European Championship
  WC   FIFA World Cup 2026
  BSA  Série A (Brazil)
  ELC  Championship (England)
  CLI  Copa Libertadores (South America)

Data sources:
    1. ESPN unofficial API (primary, no key)
    2. TheSportsDB free API (backup, no key)
    3. football-data.org (optional, disabled by default)
"""

from __future__ import annotations

import datetime
import os
import time
from typing import Any

import requests


def _et_calendar_today() -> datetime.date:
    """Return calendar date in America/New_York to match dashboard filtering."""
    try:
        import zoneinfo

        eastern = zoneinfo.ZoneInfo("America/New_York")
        return datetime.datetime.now(tz=eastern).date()
    except Exception:
        try:
            import pytz

            eastern = pytz.timezone("America/New_York")
            return datetime.datetime.now(tz=eastern).date()
        except Exception:
            return datetime.date.today()

# ── Config ────────────────────────────────────────────────────────────────────
_FD_KEY  = os.getenv("FOOTBALL_DATA_API_KEY", "")
_FD_BASE = "https://api.football-data.org/v4"
_ODDS_API_KEY = os.getenv("ODDS_API_KEY", "")

_CACHE_MATCH  = 180    # 3 min for live matches
_CACHE_SQUAD  = 3600   # 1 h for squads
_CACHE_STATIC = 86400  # 24 h for standings/scorers
_cache: dict[str, tuple[Any, float]] = {}
_PRIORITY_COMPETITIONS = ("WC", "CL", "PL", "BL1", "SA", "PD", "FL1")
_FD_COOLDOWN_SEC = max(120, int(os.getenv("SOCCER_FD_COOLDOWN_SEC", "900") or "900"))
_FD_COOLDOWN_UNTIL = 0.0
_FD_LAST_COOLDOWN_LOG_TS = 0.0
_SOCCER_USE_FD = str(os.getenv("SOCCER_USE_FD", "0") or "0").strip().lower() in {"1", "true", "yes", "on"}


# ── All supported tournaments ─────────────────────────────────────────────────
TOURNAMENTS: dict[str, dict] = {
    "WC": {
        "name":    "FIFA World Cup 2026",
        "emoji":   "🌍",
        "flag":    "🌐",
        "country": "USA/CAN/MEX",
        "season":  "2026",
        "type":    "international",
    },
    "CL": {
        "name":    "UEFA Champions League",
        "emoji":   "⭐",
        "flag":    "🇪🇺",
        "country": "Europe",
        "season":  "2025-26",
        "type":    "club",
    },
    "PL": {
        "name":    "Premier League",
        "emoji":   "🦁",
        "flag":    "🏴󠁧󠁢󠁥󠁮󠁧󠁿",
        "country": "England",
        "season":  "2025-26",
        "type":    "club",
    },
    "BL1": {
        "name":    "Bundesliga",
        "emoji":   "🦅",
        "flag":    "🇩🇪",
        "country": "Germany",
        "season":  "2025-26",
        "type":    "club",
    },
    "SA": {
        "name":    "Serie A",
        "emoji":   "🏛️",
        "flag":    "🇮🇹",
        "country": "Italy",
        "season":  "2025-26",
        "type":    "club",
    },
    "PD": {
        "name":    "La Liga",
        "emoji":   "🌞",
        "flag":    "🇪🇸",
        "country": "Spain",
        "season":  "2025-26",
        "type":    "club",
    },
    "FL1": {
        "name":    "Ligue 1",
        "emoji":   "⚜️",
        "flag":    "🇫🇷",
        "country": "France",
        "season":  "2025-26",
        "type":    "club",
    },
    "DED": {
        "name":    "Eredivisie",
        "emoji":   "🌷",
        "flag":    "🇳🇱",
        "country": "Netherlands",
        "season":  "2025-26",
        "type":    "club",
    },
    "PPL": {
        "name":    "Primeira Liga",
        "emoji":   "🐓",
        "flag":    "🇵🇹",
        "country": "Portugal",
        "season":  "2025-26",
        "type":    "club",
    },
    "BSA": {
        "name":    "Série A (Brazil)",
        "emoji":   "🇧🇷",
        "flag":    "🇧🇷",
        "country": "Brazil",
        "season":  "2025",
        "type":    "club",
    },
    "ELC": {
        "name":    "Championship",
        "emoji":   "🏆",
        "flag":    "🏴󠁧󠁢󠁥󠁮󠁧󠁿",
        "country": "England",
        "season":  "2025-26",
        "type":    "club",
    },
    "EC": {
        "name":    "UEFA Euro 2024",
        "emoji":   "🇪🇺",
        "flag":    "🇪🇺",
        "country": "Europe",
        "season":  "2024",
        "type":    "international",
    },
    "MLS": {
        "name":    "MLS (Major League Soccer)",
        "emoji":   "🇺🇸",
        "flag":    "🇺🇸",
        "country": "USA",
        "season":  "2026",
        "type":    "club",
    },
    "CLI": {
        "name":    "Copa Libertadores",
        "emoji":   "🏆",
        "flag":    "🌎",
        "country": "South America",
        "season":  "2026",
        "type":    "club",
    },
}


# ── HTTP helpers ──────────────────────────────────────────────────────────────
def _fd_get(path: str, params: dict | None = None) -> Any:
    """GET football-data.org, return parsed JSON or None."""
    global _FD_COOLDOWN_UNTIL, _FD_LAST_COOLDOWN_LOG_TS
    if not _SOCCER_USE_FD:
        return None
    if not _FD_KEY:
        return None
    now = time.time()
    if now < _FD_COOLDOWN_UNTIL:
        # Throttle repeated calls after 429 so the fallback path is used immediately.
        if (now - _FD_LAST_COOLDOWN_LOG_TS) > 60:
            left = int(max(0, _FD_COOLDOWN_UNTIL - now))
            print(f"[soccer_fetcher] football-data cooldown active ({left}s left), using ESPN/static fallback")
            _FD_LAST_COOLDOWN_LOG_TS = now
        return None
    url = f"{_FD_BASE}{path}"
    headers = {"X-Auth-Token": _FD_KEY}
    try:
        r = requests.get(url, headers=headers, params=params or {}, timeout=10)
        if r.status_code == 429:
            retry_after = r.headers.get("Retry-After", "12")
            try:
                retry_delay = max(1, int(float(retry_after)))
            except (TypeError, ValueError):
                retry_delay = 12
            cooldown_for = max(_FD_COOLDOWN_SEC, retry_delay)
            _FD_COOLDOWN_UNTIL = time.time() + cooldown_for
            _FD_LAST_COOLDOWN_LOG_TS = time.time()
            print(
                f"[soccer_fetcher] API 429 for {path}; pausing football-data requests for {cooldown_for}s"
            )
            return None
        if r.status_code == 200:
            return r.json()
        print(f"[soccer_fetcher] API {r.status_code} for {path}")
    except Exception as e:
        print(f"[soccer_fetcher] Request error: {e}")
    return None


def _espn_get(path: str, params: dict | None = None) -> Any:
    """ESPN unofficial API (no auth required)."""
    base = "https://site.api.espn.com/apis/site/v2/sports/soccer"
    try:
        r = requests.get(f"{base}/{path}", params=params or {}, timeout=8)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return None


def _sportsdb_get(path: str, params: dict | None = None) -> Any:
    """TheSportsDB free API (key=1) fallback."""
    base = "https://www.thesportsdb.com/api/v1/json/1"
    try:
        r = requests.get(f"{base}/{path}", params=params or {}, timeout=8)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return None


def _cached(key: str, ttl: int, fn, *args, **kwargs) -> Any:
    now = time.time()
    if key in _cache:
        val, ts = _cache[key]
        if now - ts < ttl:
            return val
    val = fn(*args, **kwargs)
    if val is not None:
        _cache[key] = (val, now)
    return val


def _safe_team_name(value: Any, default: str = "TBD") -> str:
    name = str(value or "").strip()
    return name or default


# ── Competition metadata ──────────────────────────────────────────────────────
def get_tournaments() -> list[dict]:
    """Return list of all supported tournaments with metadata."""
    result = []
    for code, info in TOURNAMENTS.items():
        result.append({"code": code, **info})
    return result


def get_competition_info(code: str) -> dict:
    return TOURNAMENTS.get(code, {"name": code, "emoji": "⚽", "flag": "🌐",
                                  "country": "International", "type": "club"})


# ── Matches ───────────────────────────────────────────────────────────────────


def get_matches_in_range(competition_code: str, date_from: str, date_to: str) -> list[dict]:
    """Matches for a competition over a date range."""
    key = f"matches_{competition_code}_{date_from}_{date_to}"
    return _cached(key, _CACHE_MATCH, _fetch_matches_window, competition_code, date_from, date_to) or []


def get_live_matches(competition_code: str | None = None) -> list[dict]:
    """All currently live matches, optionally filtered by competition."""
    # ESPN covers live scores for all leagues at once
    data = _espn_get("scoreboard", {"limit": 100})
    results = []
    if data:
        for event in data.get("events", []):
            m = _normalize_espn_event(event)
            if competition_code and m.get("competition") != competition_code:
                continue
            if m.get("status") in ("In Progress", "Halftime"):
                results.append(m)
    if results:
        return results

    # Secondary free fallback when ESPN feed is temporarily unavailable.
    data2 = _sportsdb_get("livescore.php", {"s": "Soccer"})
    events = data2.get("events") if isinstance(data2, dict) else None
    if isinstance(events, list):
        for ev in events:
            m = _normalize_sportsdb_event(ev, competition_code or "")
            if not m:
                continue
            if m.get("status") in ("In Progress", "Halftime"):
                results.append(m)
    return results


def get_matches_range_all(date_from: str, date_to: str,
                          competition_codes: list[str] | tuple[str, ...] | None = None) -> list[dict]:
    """Fetch one cached window per competition and flatten the results."""
    codes = competition_codes or _PRIORITY_COMPETITIONS
    all_matches: list[dict] = []
    for code in codes:
        all_matches.extend(get_matches_in_range(code, date_from, date_to))
    return all_matches


def _fetch_matches_window(code: str, date_from: str, date_to: str) -> list[dict]:
    """Fetch matches from football-data.org for a date range."""
    espn_rows = _fetch_matches_espn_range(code, date_from, date_to)
    if espn_rows:
        return espn_rows

    data = _fd_get(
        f"/competitions/{code}/matches",
        {
            "dateFrom": date_from,
            "dateTo": date_to,
            "status": "SCHEDULED,IN_PLAY,PAUSED,FINISHED,LIVE",
        },
    )
    if data and "matches" in data:
        rows = [_normalize_fd_match(m, code) for m in data.get("matches", [])]
        if rows:
            return rows
    return _fetch_matches_sportsdb_range(code, date_from, date_to)


def _fetch_matches_espn_range(code: str, date_from: str, date_to: str) -> list[dict]:
    """ESPN fallback for one or more calendar days."""
    try:
        start = datetime.date.fromisoformat(date_from)
        end = datetime.date.fromisoformat(date_to)
    except ValueError:
        return _fetch_matches_espn(code, date_from) or []

    seen: set[str] = set()
    matches: list[dict] = []
    day = start
    while day <= end:
        for match in _fetch_matches_espn(code, day.isoformat()) or []:
            dedupe_key = str(match.get("match_id") or match.get("game_key") or "")
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            matches.append(match)
        day += datetime.timedelta(days=1)
    return matches


def _fetch_matches_espn(code: str, date_str: str) -> list[dict]:
    """ESPN fallback for match schedule."""
    # ESPN league slug mapping
    espn_league = _fd_to_espn_league(code)
    if not espn_league:
        return []
    dates_formatted = date_str.replace("-", "")
    data = _espn_get(f"{espn_league}/scoreboard", {"dates": dates_formatted})
    if not data:
        return _fetch_matches_sportsdb(code, date_str)
    rows = [_normalize_espn_event(e) for e in data.get("events", [])]
    if rows:
        return rows
    return _fetch_matches_sportsdb(code, date_str)


def _fetch_matches_sportsdb_range(code: str, date_from: str, date_to: str) -> list[dict]:
    try:
        start = datetime.date.fromisoformat(date_from)
        end = datetime.date.fromisoformat(date_to)
    except ValueError:
        return _fetch_matches_sportsdb(code, date_from) or []

    seen: set[str] = set()
    matches: list[dict] = []
    day = start
    while day <= end:
        for match in _fetch_matches_sportsdb(code, day.isoformat()) or []:
            dedupe_key = str(match.get("match_id") or match.get("game_key") or "")
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            matches.append(match)
        day += datetime.timedelta(days=1)
    return matches


def _fetch_matches_sportsdb(code: str, date_str: str) -> list[dict]:
    data = _sportsdb_get("eventsday.php", {"d": date_str, "s": "Soccer"})
    events = data.get("events") if isinstance(data, dict) else None
    if not isinstance(events, list):
        return []
    wanted = _sportsdb_comp_keywords(code)
    rows = []
    for ev in events:
        row = _normalize_sportsdb_event(ev, code)
        if not row:
            continue
        if wanted:
            league_text = str(ev.get("strLeague") or ev.get("strLeagueAlternate") or "").lower()
            if not any(tok in league_text for tok in wanted):
                continue
        rows.append(row)
    return rows


def _sportsdb_comp_keywords(code: str) -> tuple[str, ...]:
    mapping = {
        "PL": ("premier league",),
        "BL1": ("bundesliga",),
        "SA": ("serie a",),
        "PD": ("la liga",),
        "FL1": ("ligue 1",),
        "DED": ("eredivisie",),
        "PPL": ("primeira liga",),
        "CL": ("champions league",),
        "ELC": ("championship",),
        "BSA": ("brasileirao", "serie a brazil"),
        "MLS": ("major league soccer", "mls"),
        "EC": ("euro", "uefa european championship"),
        "WC": ("world cup",),
        "CLI": ("copa libertadores", "libertadores"),
    }
    return mapping.get(code, ())


def _fd_to_espn_league(code: str) -> str:
    """Map football-data.org competition code to ESPN league slug."""
    mapping = {
        "PL":  "eng.1",
        "BL1": "ger.1",
        "SA":  "ita.1",
        "PD":  "esp.1",
        "FL1": "fra.1",
        "DED": "ned.1",
        "PPL": "por.1",
        "CL":  "uefa.champions",
        "EC":  "uefa.euro",
        "WC":  "fifa.world",
        "ELC": "eng.2",
        "BSA": "bra.1",
        "MLS": "usa.1",
        "CLI": "conmebol.libertadores",
    }
    return mapping.get(code, "")


# ── Standings ─────────────────────────────────────────────────────────────────
def get_standings(competition_code: str) -> list[dict]:
    """League table / group standings for a competition."""
    key = f"standings_{competition_code}"
    return _cached(key, _CACHE_STATIC, _fetch_standings, competition_code) or []


def _fetch_standings(code: str) -> list[dict]:
    data = _fd_get(f"/competitions/{code}/standings")
    if not data:
        return _fetch_standings_espn(code)
    standings = []
    for s in data.get("standings", []):
        group  = s.get("group") or s.get("stage", "")
        table  = []
        for row in s.get("table", []):
            table.append({
                "position": row.get("position"),
                "team":     row["team"]["name"],
                "short":    row["team"].get("shortName", row["team"]["name"][:10]),
                "crest":    row["team"].get("crest", ""),
                "played":   row.get("playedGames", 0),
                "won":      row.get("won", 0),
                "drawn":    row.get("draw", 0),
                "lost":     row.get("lost", 0),
                "gf":       row.get("goalsFor", 0),
                "ga":       row.get("goalsAgainst", 0),
                "gd":       row.get("goalDifference", 0),
                "pts":      row.get("points", 0),
                "form":     row.get("form", ""),
            })
        standings.append({
            "group": group,
            "stage": s.get("stage", ""),
            "table": table,
        })
    return standings


def _fetch_standings_espn(code: str) -> list[dict]:
    espn_league = _fd_to_espn_league(code)
    if not espn_league:
        return []
    data = _espn_get(f"{espn_league}/standings")
    if not data:
        return []
    table = []
    for entry in data.get("standings", {}).get("entries", []):
        stats = {s["abbreviation"]: s.get("value", 0) for s in entry.get("stats", [])}
        table.append({
            "position": entry.get("note", {}).get("rank", 0),
            "team":     entry.get("team", {}).get("displayName", ""),
            "short":    entry.get("team", {}).get("abbreviation", ""),
            "crest":    entry.get("team", {}).get("logos", [{}])[0].get("href", "") if entry.get("team", {}).get("logos") else "",
            "played":   int(stats.get("GP", stats.get("gamesPlayed", 0))),
            "won":      int(stats.get("W", 0)),
            "drawn":    int(stats.get("D", 0)),
            "lost":     int(stats.get("L", 0)),
            "gf":       int(stats.get("F", stats.get("pointsFor", 0))),
            "ga":       int(stats.get("A", stats.get("pointsAgainst", 0))),
            "gd":       int(stats.get("D", 0)),
            "pts":      int(stats.get("Pts", stats.get("points", 0))),
            "form":     "",
        })
    return [{"group": espn_league, "stage": "TABLE", "table": table}]


# ── Top Scorers / Players ─────────────────────────────────────────────────────
def get_top_scorers(competition_code: str, limit: int = 20) -> list[dict]:
    """Top scorers for a competition (football-data.org)."""
    key = f"scorers_{competition_code}"
    return _cached(key, _CACHE_STATIC, _fetch_scorers, competition_code, limit) or []


def _fetch_scorers(code: str, limit: int) -> list[dict]:
    data = _fd_get(f"/competitions/{code}/scorers", {"limit": limit})
    if not data:
        return []
    scorers = []
    for s in data.get("scorers", []):
        p = s.get("player", {})
        t = s.get("team", {})
        scorers.append({
            "name":       p.get("name", ""),
            "nationality":p.get("nationality", ""),
            "position":   p.get("position", ""),
            "dob":        p.get("dateOfBirth", ""),
            "team":       t.get("name", ""),
            "team_crest": t.get("crest", ""),
            "goals":      s.get("goals", 0),
            "assists":    s.get("assists", 0) or 0,
            "played":     s.get("playedMatches", 0),
        })
    return scorers


# ── Team squad / roster ───────────────────────────────────────────────────────


# ── Odds ─────────────────────────────────────────────────────────────────────
def get_odds_for_sport(sport_key: str) -> list[dict]:
    """The Odds API for any soccer sport key."""
    if not _ODDS_API_KEY:
        return []
    try:
        r = requests.get(
            f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds/",
            params={"apiKey": _ODDS_API_KEY, "regions": "us,eu",
                    "markets": "h2h,totals", "oddsFormat": "american"},
            timeout=10,
        )
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        print(f"[soccer_fetcher] Odds API error: {e}")
    return []


# Sport key map: football-data competition code → The Odds API sport key
FD_TO_ODDS_SPORT: dict[str, str] = {
    "PL":  "soccer_england_premier_league",
    "BL1": "soccer_germany_bundesliga",
    "SA":  "soccer_italy_serie_a",
    "PD":  "soccer_spain_la_liga",
    "FL1": "soccer_france_ligue_one",
    "DED": "soccer_netherlands_eredivisie",
    "CL":  "soccer_uefa_champs_league",
    "WC":  "soccer_fifa_world_cup",
    "ELC": "soccer_england_championship",
    "PPL": "soccer_portugal_primeira_liga",
    "BSA": "soccer_brazil_campeonato",
    "MLS": "soccer_usa_mls",
}


def get_competition_odds(competition_code: str) -> list[dict]:
    sport_key = FD_TO_ODDS_SPORT.get(competition_code)
    if not sport_key:
        return []
    key = f"odds_{competition_code}"
    return _cached(key, 300, get_odds_for_sport, sport_key) or []


# ── Normalizers ───────────────────────────────────────────────────────────────
def _normalize_fd_match(m: dict, competition_code: str = "") -> dict:
    """Normalize football-data.org match to internal format."""
    utc_str = m.get("utcDate", "")
    try:
        import zoneinfo
        dt = datetime.datetime.fromisoformat(utc_str.replace("Z", "+00:00"))
        dt_et = dt.astimezone(zoneinfo.ZoneInfo("America/New_York"))
        date_str = dt_et.strftime("%Y-%m-%d")
        time_str = dt_et.strftime("%H:%M")
    except Exception:
        date_str = utc_str[:10]
        time_str = utc_str[11:16]

    status_map = {
        "SCHEDULED":  "Scheduled",
        "IN_PLAY":    "In Progress",
        "PAUSED":     "Halftime",
        "FINISHED":   "Final",
        "CANCELLED":  "Cancelled",
        "POSTPONED":  "Postponed",
    }

    home = _safe_team_name(m.get("homeTeam", {}).get("name"))
    away = _safe_team_name(m.get("awayTeam", {}).get("name"))
    home_crest = m.get("homeTeam", {}).get("crest", "")
    away_crest = m.get("awayTeam", {}).get("crest", "")
    score  = m.get("score", {})
    ft     = score.get("fullTime", {})
    ht     = score.get("halfTime", {})
    comp   = m.get("competition", {})
    comp_code = comp.get("code", competition_code)
    t_info = TOURNAMENTS.get(comp_code, {"emoji": "⚽", "name": comp.get("name", comp_code)})

    return {
        "match_id":    str(m.get("id", "")),
        "competition": comp_code,
        "comp_name":   t_info.get("name", comp.get("name", "")),
        "comp_emoji":  t_info.get("emoji", "⚽"),
        "group":       m.get("group", ""),
        "stage":       m.get("stage", "REGULAR_SEASON"),
        "date":        date_str,
        "game_date":   date_str,
        "game_time":   time_str,
        "home_team":   home,
        "away_team":   away,
        "home_crest":  home_crest,
        "away_crest":  away_crest,
        "home_id":     m.get("homeTeam", {}).get("id"),
        "away_id":     m.get("awayTeam", {}).get("id"),
        "venue":       m.get("venue") or m.get("area", {}).get("name", ""),
        "status":      status_map.get(m.get("status", "SCHEDULED"), "Scheduled"),
        "home_score":  ft.get("home"),
        "away_score":  ft.get("away"),
        "home_ht":     ht.get("home"),
        "away_ht":     ht.get("away"),
        "game_key":    f"{date_str}#{away}@{home}",
        "match_key":   f"{away.replace(' ','')[:3]}@{home.replace(' ','')[:3]}".upper(),
        "sport":       "soccer",
    }


def _normalize_espn_event(e: dict) -> dict:
    """Normalize ESPN event to internal format."""
    competitions = e.get("competitions", [{}])
    comp = competitions[0] if competitions else {}
    competitors = comp.get("competitors", [])
    home_c = next((c for c in competitors if c.get("homeAway") == "home"), {})
    away_c = next((c for c in competitors if c.get("homeAway") == "away"), {})

    home = _safe_team_name(home_c.get("team", {}).get("displayName"))
    away = _safe_team_name(away_c.get("team", {}).get("displayName"))

    date_str = e.get("date", "")[:10]
    time_str = e.get("date", "")[11:16]

    status_detail = e.get("status", {}).get("type", {}).get("description", "Scheduled")
    status_map = {
        "Scheduled": "Scheduled", "In Progress": "In Progress",
        "Halftime": "Halftime", "Final": "Final", "Full Time": "Final",
    }

    return {
        "match_id":    str(e.get("id", "")),
        "competition": "ESPN",
        "comp_name":   e.get("name", ""),
        "comp_emoji":  "⚽",
        "group":       "",
        "stage":       "REGULAR_SEASON",
        "date":        date_str,
        "game_date":   date_str,
        "game_time":   time_str,
        "home_team":   home,
        "away_team":   away,
        "home_crest":  home_c.get("team", {}).get("logo", ""),
        "away_crest":  away_c.get("team", {}).get("logo", ""),
        "venue":       comp.get("venue", {}).get("fullName", ""),
        "status":      status_map.get(status_detail, status_detail),
        "home_score":  int(home_c.get("score", 0) or 0) if status_detail not in ("Scheduled",) else None,
        "away_score":  int(away_c.get("score", 0) or 0) if status_detail not in ("Scheduled",) else None,
        "game_key":    f"{date_str}#{away}@{home}",
        "match_key":   f"{away.replace(' ','')[:3]}@{home.replace(' ','')[:3]}".upper(),
        "sport":       "soccer",
    }


def _normalize_sportsdb_event(event: dict, competition_code: str) -> dict:
    home = _safe_team_name(event.get("strHomeTeam"))
    away = _safe_team_name(event.get("strAwayTeam"))
    if not home or not away:
        return {}

    date_str = str(event.get("dateEvent") or event.get("strTimestamp") or "")[:10]
    time_str = str(event.get("strTime") or "")[:5]

    status = str(event.get("strStatus") or "Scheduled").strip()
    status_low = status.lower()
    if any(tok in status_low for tok in ("ft", "aet", "pen", "final", "finished", "full time")):
        norm_status = "Final"
    elif any(tok in status_low for tok in ("live", "ht", "in progress", "1h", "2h")):
        norm_status = "In Progress"
    else:
        norm_status = "Scheduled"

    hs = event.get("intHomeScore")
    aw = event.get("intAwayScore")
    try:
        home_score = int(hs) if hs is not None and str(hs).strip() != "" else None
    except Exception:
        home_score = None
    try:
        away_score = int(aw) if aw is not None and str(aw).strip() != "" else None
    except Exception:
        away_score = None

    comp_name = TOURNAMENTS.get(competition_code, {}).get("name") or event.get("strLeague") or competition_code
    comp_emoji = TOURNAMENTS.get(competition_code, {}).get("emoji", "⚽")

    return {
        "match_id": str(event.get("idEvent") or ""),
        "competition": competition_code or "SPORTSDB",
        "comp_name": comp_name,
        "comp_emoji": comp_emoji,
        "group": "",
        "stage": "REGULAR_SEASON",
        "date": date_str,
        "game_date": date_str,
        "game_time": time_str,
        "home_team": home,
        "away_team": away,
        "home_crest": str(event.get("strHomeTeamBadge") or ""),
        "away_crest": str(event.get("strAwayTeamBadge") or ""),
        "venue": str(event.get("strVenue") or ""),
        "status": norm_status,
        "home_score": home_score,
        "away_score": away_score,
        "game_key": f"{date_str}#{away}@{home}",
        "match_key": f"{away.replace(' ','')[:3]}@{home.replace(' ','')[:3]}".upper(),
        "sport": "soccer",
    }


# ── Daily refresh (call from scheduler) ──────────────────────────────────────


def get_matches_today_all() -> list[dict]:
    """All matches today across all supported competitions."""
    today = _et_calendar_today().isoformat()
    tomorrow = (_et_calendar_today() + datetime.timedelta(days=1)).isoformat()
    window_matches = get_matches_range_all(today, tomorrow, list(TOURNAMENTS.keys()))
    return [m for m in window_matches if (m.get("game_date") or m.get("date")) == today]


def get_matches_tomorrow_all() -> list[dict]:
    """All matches tomorrow across all supported competitions."""
    tomorrow = (_et_calendar_today() + datetime.timedelta(days=1)).isoformat()
    today = _et_calendar_today().isoformat()
    window_matches = get_matches_range_all(today, tomorrow, list(TOURNAMENTS.keys()))
    return [m for m in window_matches if (m.get("game_date") or m.get("date")) == tomorrow]


def get_team_recent_form(
    team_name: str,
    days_back: int = 140,
    max_matches: int = 12,
    competition_codes: list[str] | None = None,
) -> dict:
    """
    Compute recent historical form for a team from finished matches.
    Used by the soccer predictor to blend model priors with season form.
    """
    team = _safe_team_name(team_name, default="").strip()
    if not team:
        return {
            "sample_size": 0,
            "goals_for_per_match": 0.0,
            "goals_against_per_match": 0.0,
            "points_per_match": 0.0,
            "win_rate": 0.0,
        }

    codes = competition_codes or list(TOURNAMENTS.keys())
    end_date = _et_calendar_today()
    start_date = end_date - datetime.timedelta(days=max(7, int(days_back)))
    cache_key = f"recent_form::{team.lower()}::{start_date.isoformat()}::{end_date.isoformat()}::{','.join(codes)}::{max_matches}"

    def _compute() -> dict:
        matches: list[dict] = []
        for code in codes:
            try:
                rows = get_matches_in_range(code, start_date.isoformat(), end_date.isoformat())
                matches.extend(rows or [])
            except Exception:
                continue

        team_low = team.lower()
        relevant: list[dict] = []
        for m in matches:
            status = str(m.get("status") or "").lower()
            if "final" not in status and "completed" not in status:
                continue
            home = str(m.get("home_team") or "").strip()
            away = str(m.get("away_team") or "").strip()
            if team_low not in home.lower() and team_low not in away.lower():
                continue
            relevant.append(m)

        relevant.sort(key=lambda x: (x.get("game_date") or x.get("date") or ""), reverse=True)
        relevant = relevant[: max(1, max_matches)]

        gf = 0.0
        ga = 0.0
        wins = 0
        points = 0
        used = 0
        for m in relevant:
            home = str(m.get("home_team") or "")
            away = str(m.get("away_team") or "")
            hs = m.get("home_score")
            as_ = m.get("away_score")
            if hs is None or as_ is None:
                continue
            try:
                hs_f = float(hs)
                as_f = float(as_)
            except (TypeError, ValueError):
                continue

            is_home = team_low in home.lower()
            scored = hs_f if is_home else as_f
            conceded = as_f if is_home else hs_f
            gf += scored
            ga += conceded
            used += 1
            if scored > conceded:
                wins += 1
                points += 3
            elif scored == conceded:
                points += 1

        if used == 0:
            return {
                "sample_size": 0,
                "goals_for_per_match": 0.0,
                "goals_against_per_match": 0.0,
                "points_per_match": 0.0,
                "win_rate": 0.0,
            }

        return {
            "sample_size": used,
            "goals_for_per_match": round(gf / used, 3),
            "goals_against_per_match": round(ga / used, 3),
            "points_per_match": round(points / used, 3),
            "win_rate": round(wins / used, 3),
        }

    return _cached(cache_key, _CACHE_MATCH, _compute) or {
        "sample_size": 0,
        "goals_for_per_match": 0.0,
        "goals_against_per_match": 0.0,
        "points_per_match": 0.0,
        "win_rate": 0.0,
    }
