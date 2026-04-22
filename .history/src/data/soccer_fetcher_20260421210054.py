"""
Soccer Data Fetcher
===================
Data sources (all free / free-tier):

1. football-data.co.uk
   - Free CSV downloads with historical match results AND closing odds
   - URL: https://www.football-data.co.uk/mmz4281/{season}/{league_code}.csv
   - Columns used: HomeTeam, AwayTeam, FTHG, FTAG, FTR,
                   B365H, B365D, B365A (Bet365 closing odds)

2. football-data.org API  (free tier: 10 leagues, 10 req/min)
   - Requires free registration at https://www.football-data.org/client/register
   - Returns today's fixtures with teams
   - Set FOOTBALL_DATA_API_KEY in .env

Provides:
  - Historical match results with odds (for model training + back-testing)
  - Team attack/defence strength ratings (Poisson model inputs)
  - Today's fixtures for selected leagues
  - Player stats scraped from fbref via requests (no key needed)
"""

import io
import sys
import os
import time
import warnings
import requests
import pandas as pd
import numpy as np

warnings.filterwarnings("ignore")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config import FOOTBALL_DATA_API_KEY, FOOTBALL_DATA_BASE, FOOTBALL_DATA_UK_LEAGUES

# ---------------------------------------------------------------------------
# football-data.co.uk  (free historical CSVs)
# ---------------------------------------------------------------------------

_FDUK_BASE = "https://www.football-data.co.uk/mmz4281/{season}/{code}.csv"

# MLS uses a different URL pattern
_FDUK_MLS_BASE = "https://www.football-data.co.uk/new/{code}{season}.csv"

def _fetch_fduk_csv(season_code: str, league_code: str) -> pd.DataFrame:
    """Download one season CSV from football-data.co.uk."""
    if league_code == "USA":
        url = f"https://www.football-data.co.uk/new/MLS{season_code}.csv"
    else:
        url = _FDUK_BASE.format(season=season_code, code=league_code)
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        df = pd.read_csv(io.StringIO(resp.text), low_memory=False, on_bad_lines="skip")
        df["season_code"] = season_code
        df["league_code"] = league_code
        return df
    except Exception as e:
        print(f"[soccer_fetcher] CSV fetch failed {url}: {e}")
        return pd.DataFrame()


def get_historical_matches(league_key: str = "EPL", seasons: list[str] | None = None) -> pd.DataFrame:
    """
    Download and clean historical match results + Bet365 odds for a league.

    league_key: one of EPL, ELC, ESP, GER, ITA, FRA, MLS
    Returns DataFrame with columns:
        home_team, away_team, fthg, ftag, ftr,
        b365h, b365d, b365a, season_code, league
    """
    info = FOOTBALL_DATA_UK_LEAGUES.get(league_key)
    if info is None:
        print(f"[soccer_fetcher] Unknown league key: {league_key}")
        return pd.DataFrame()

    league_label, default_seasons, code = info
    use_seasons = seasons if seasons else default_seasons

    frames = []
    for sc in use_seasons:
        df = _fetch_fduk_csv(sc, code)
        if df.empty:
            continue
        # Standardise column names (some seasons use different casing)
        df.columns = [c.strip() for c in df.columns]
        rename = {
            "HomeTeam": "home_team", "AwayTeam": "away_team",
            "FTHG": "fthg", "FTAG": "ftag", "FTR": "ftr",
            "B365H": "b365h", "B365D": "b365d", "B365A": "b365a",
            "Date": "match_date",
        }
        df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})
        df["league"] = league_label

        keep = ["home_team", "away_team", "fthg", "ftag", "ftr",
                "b365h", "b365d", "b365a", "match_date", "season_code", "league"]
        df = df[[c for c in keep if c in df.columns]].dropna(subset=["home_team", "away_team", "fthg", "ftag"])
        frames.append(df)

    if not frames:
        return pd.DataFrame()

    result = pd.concat(frames, ignore_index=True)
    result["fthg"] = pd.to_numeric(result["fthg"], errors="coerce")
    result["ftag"] = pd.to_numeric(result["ftag"], errors="coerce")
    result["b365h"] = pd.to_numeric(result.get("b365h"), errors="coerce")
    result["b365d"] = pd.to_numeric(result.get("b365d"), errors="coerce")
    result["b365a"] = pd.to_numeric(result.get("b365a"), errors="coerce")
    return result


def compute_team_strength(matches: pd.DataFrame) -> pd.DataFrame:
    """
    Compute attack and defence strength ratings per team using the
    Dixon-Coles / Poisson strength method:

        attack_strength  = avg goals scored / league avg goals per game
        defence_strength = avg goals conceded / league avg goals per game

    Returns DataFrame: team, attack_strength, defence_strength, home_adv
    """
    if matches.empty:
        return pd.DataFrame()

    avg_goals = (matches["fthg"].mean() + matches["ftag"].mean()) / 2

    records = []
    teams = set(matches["home_team"].tolist() + matches["away_team"].tolist())
    for team in teams:
        home_g = matches[matches["home_team"] == team]["fthg"]
        away_g = matches[matches["away_team"] == team]["ftag"]
        home_c = matches[matches["home_team"] == team]["ftag"]
        away_c = matches[matches["away_team"] == team]["fthg"]

        scored = pd.concat([home_g, away_g]).mean()
        conceded = pd.concat([home_c, away_c]).mean()

        # Home advantage: ratio of goals scored at home vs away
        home_adv = home_g.mean() / away_g.mean() if away_g.mean() > 0 else 1.0

        records.append({
            "team": team,
            "attack_strength": round(scored / avg_goals, 4) if avg_goals > 0 else 1.0,
            "defence_strength": round(conceded / avg_goals, 4) if avg_goals > 0 else 1.0,
            "home_adv": round(home_adv, 4),
            "games_played": len(home_g) + len(away_g),
        })

    return pd.DataFrame(records).sort_values("team")


# ---------------------------------------------------------------------------
# football-data.org API  (today's fixtures)
# ---------------------------------------------------------------------------

_FDORG_LEAGUE_MAP = {
    "EPL": "PL",     # Premier League
    "ESP": "PD",     # La Liga
    "GER": "BL1",    # Bundesliga
    "ITA": "SA",     # Serie A
    "FRA": "FL1",    # Ligue 1
    "MLS": "MLS",    # MLS
}


def get_todays_fixtures(league_keys: list[str] | None = None) -> list[dict]:
    """
    Fetch today's soccer fixtures from football-data.org API.
    Requires FOOTBALL_DATA_API_KEY in .env (free registration).
    Falls back to empty list if key not set.
    """
    if not FOOTBALL_DATA_API_KEY or FOOTBALL_DATA_API_KEY == "your_football_data_key_here":
        print("[soccer_fetcher] FOOTBALL_DATA_API_KEY not set – skipping live fixtures.")
        return []

    import datetime as _dt
    import pytz

    headers  = {"X-Auth-Token": FOOTBALL_DATA_API_KEY}
    use_keys = league_keys if league_keys else list(_FDORG_LEAGUE_MAP.keys())
    fixtures = []
    today    = _dt.date.today().isoformat()
    tomorrow = (_dt.date.today() + _dt.timedelta(days=1)).isoformat()

    for lk in use_keys:
        comp_id = _FDORG_LEAGUE_MAP.get(lk)
        if not comp_id:
            continue
        url = f"{FOOTBALL_DATA_BASE}/competitions/{comp_id}/matches?status=SCHEDULED"
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            if resp.status_code == 429:
                print("[soccer_fetcher] Rate limit hit – sleeping 12s")
                time.sleep(12)
                resp = requests.get(url, headers=headers, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            for m in data.get("matches", []):
                raw_utc   = m.get("utcDate", "")
                utc_date  = raw_utc[:10]
                if utc_date not in (today, tomorrow):
                    continue
                # Parse game time to ET
                game_time = None
                try:
                    utc_dt  = _dt.datetime.fromisoformat(raw_utc.replace("Z", "+00:00"))
                    eastern = pytz.timezone("America/New_York")
                    local   = utc_dt.astimezone(eastern)
                    game_time = local.strftime("%H:%M")
                except Exception:
                    pass
                fixtures.append({
                    "league":    lk,
                    "sport":     "soccer",
                    "date":      utc_date,
                    "game_time": game_time,
                    "home_team": m["homeTeam"]["name"],
                    "away_team": m["awayTeam"]["name"],
                    "match_id":  m["id"],
                    "status":    m.get("status", "SCHEDULED"),
                })
            time.sleep(1)  # stay under 10 req/min
        except Exception as e:
            print(f"[soccer_fetcher] fixture fetch error for {lk}: {e}")

    _save_soccer_fixtures_to_db(fixtures)
    return [f for f in fixtures if f["date"] == today]   # only today for analysis


def get_fixtures_range(league_keys=None) -> list[dict]:
    """
    Return today + tomorrow fixtures (for the upcoming schedule view).
    Uses same API call as get_todays_fixtures but returns all dates.
    """
    if not FOOTBALL_DATA_API_KEY or FOOTBALL_DATA_API_KEY == "your_football_data_key_here":
        return []

    import datetime as _dt
    import pytz

    headers  = {"X-Auth-Token": FOOTBALL_DATA_API_KEY}
    use_keys = league_keys if league_keys else list(_FDORG_LEAGUE_MAP.keys())
    fixtures = []
    today    = _dt.date.today().isoformat()
    tomorrow = (_dt.date.today() + _dt.timedelta(days=1)).isoformat()

    for lk in use_keys:
        comp_id = _FDORG_LEAGUE_MAP.get(lk)
        if not comp_id:
            continue
        url = f"{FOOTBALL_DATA_BASE}/competitions/{comp_id}/matches?status=SCHEDULED"
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            if resp.status_code == 429:
                time.sleep(12)
                resp = requests.get(url, headers=headers, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            for m in data.get("matches", []):
                raw_utc  = m.get("utcDate", "")
                utc_date = raw_utc[:10]
                if utc_date not in (today, tomorrow):
                    continue
                game_time = None
                try:
                    utc_dt  = _dt.datetime.fromisoformat(raw_utc.replace("Z", "+00:00"))
                    eastern = pytz.timezone("America/New_York")
                    local   = utc_dt.astimezone(eastern)
                    game_time = local.strftime("%H:%M")
                except Exception:
                    pass
                fixtures.append({
                    "league":    lk,
                    "sport":     "soccer",
                    "date":      utc_date,
                    "game_time": game_time,
                    "home_team": m["homeTeam"]["name"],
                    "away_team": m["awayTeam"]["name"],
                    "match_id":  m["id"],
                    "status":    m.get("status", "SCHEDULED"),
                })
            time.sleep(1)
        except Exception as e:
            print(f"[soccer_fetcher] fixture_range error for {lk}: {e}")

    return fixtures


def _save_soccer_fixtures_to_db(fixtures: list[dict]):
    """Persist soccer fixtures to PostgreSQL (best-effort)."""
    try:
        from data.db import upsert_game
        for f in fixtures:
            upsert_game(
                sport="soccer", league=f.get("league", ""),
                home_team=f["home_team"], away_team=f["away_team"],
                game_date=f["date"],
                game_time=f.get("game_time"),
                status=f.get("status", "Scheduled"),
            )
    except Exception as e:
        print(f"[soccer_fetcher] db save error: {e}")


# ---------------------------------------------------------------------------
# FBRef player stats scraper (no API key required)
# ---------------------------------------------------------------------------

# FBRef league IDs
_FBREF_LEAGUE_IDS = {
    "EPL": "9",   # Premier League
    "ESP": "12",  # La Liga
    "GER": "20",  # Bundesliga
    "ITA": "11",  # Serie A
    "FRA": "13",  # Ligue 1
    "MLS": "22",  # MLS
}

# FBRef uses full league names in URL slugs — NOT abbreviations
_FBREF_LEAGUE_URL_NAMES = {
    "EPL": "Premier-League",
    "ESP": "La-Liga",
    "GER": "Bundesliga",
    "ITA": "Serie-A",
    "FRA": "Ligue-1",
    "MLS": "Major-League-Soccer",
}

# Circuit-breaker: avoid hammering FBRef if blocked
_FBREF_BLOCKED: bool = False


def _fbref_standard_stats(league_key: str, season: str = "2024-2025") -> pd.DataFrame:
    """
    Scrape FBRef standard player stats for a league/season.
    Returns normalised DataFrame with columns including:
        player, squad, nation, pos, age, mp, starts, min,
        goals, assists, xg, xa, npxg, prgc, prgp
    """
    global _FBREF_BLOCKED
    if _FBREF_BLOCKED:
        return pd.DataFrame()

    league_id   = _FBREF_LEAGUE_IDS.get(league_key)
    league_name = _FBREF_LEAGUE_URL_NAMES.get(league_key, league_key)
    if not league_id:
        return pd.DataFrame()

    # MLS uses a single year (2024), all others use 2024-2025 format
    url_season = season.split("-")[0] if league_key == "MLS" else season
    url = (
        f"https://fbref.com/en/comps/{league_id}/{url_season}/stats/"
        f"{url_season}-{league_name}-Stats"
    )
    print(f"[soccer_fetcher] FBRef URL: {url}")
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"}
    try:
        resp = requests.get(url, headers=headers, timeout=25)
        if resp.status_code in (403, 429):
            _FBREF_BLOCKED = True
            print(f"[soccer_fetcher] FBRef blocked ({resp.status_code}) — skipping player stats")
            return pd.DataFrame()
        resp.raise_for_status()
        tables = pd.read_html(io.StringIO(resp.text), attrs={"id": "stats_standard"})
        df = tables[0] if tables else pd.DataFrame()
        if df.empty:
            # fallback: grab first large table
            all_tables = pd.read_html(io.StringIO(resp.text))
            df = max(all_tables, key=len) if all_tables else pd.DataFrame()
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [" ".join([c for c in col if "Unnamed" not in c]).strip()
                          for col in df.columns]
        # normalise column names
        df.columns = (
            df.columns.str.lower()
            .str.replace(r"[^a-z0-9]", "_", regex=True)
            .str.strip("_")
        )
        # remove header-repeat rows
        if "player" in df.columns:
            df = df[df["player"] != "Player"].copy()
        df["league_key"] = league_key
        df["season"]     = season
        return df
    except Exception as e:
        if "403" in str(e) or "429" in str(e):
            _FBREF_BLOCKED = True
            print("[soccer_fetcher] FBRef blocked — skipping player stats")
        else:
            print(f"[soccer_fetcher] FBRef scrape error ({league_key}): {e}")
        return pd.DataFrame()


def _fbref_shooting_stats(league_key: str, season: str = "2024-2025") -> pd.DataFrame:
    """
    Scrape FBRef shooting stats — shots, shots on target, npxg.
    Returns normalised DataFrame with: player, squad, sh, sot, sot_pct, npxg_per_sh.
    """
    global _FBREF_BLOCKED
    if _FBREF_BLOCKED:
        return pd.DataFrame()

    league_id   = _FBREF_LEAGUE_IDS.get(league_key)
    league_name = _FBREF_LEAGUE_URL_NAMES.get(league_key, league_key)
    if not league_id:
        return pd.DataFrame()

    url_season = season.split("-")[0] if league_key == "MLS" else season
    url = (
        f"https://fbref.com/en/comps/{league_id}/{url_season}/shooting/"
        f"{url_season}-{league_name}-Stats"
    )
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"}
    try:
        resp = requests.get(url, headers=headers, timeout=25)
        if resp.status_code in (403, 429):
            _FBREF_BLOCKED = True
            print(f"[soccer_fetcher] FBRef blocked ({resp.status_code})")
            return pd.DataFrame()
        resp.raise_for_status()
        tables = pd.read_html(io.StringIO(resp.text), attrs={"id": "stats_shooting"})
        df = tables[0] if tables else pd.DataFrame()
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [" ".join([c for c in col if "Unnamed" not in c]).strip()
                          for col in df.columns]
        df.columns = (
            df.columns.str.lower()
            .str.replace(r"[^a-z0-9]", "_", regex=True)
            .str.strip("_")
        )
        if "player" in df.columns:
            df = df[df["player"] != "Player"].copy()
        return df
    except Exception as e:
        print(f"[soccer_fetcher] FBRef shooting error ({league_key}): {e}")
        return pd.DataFrame()


def get_soccer_player_props_batch(
    fixtures: list[dict],
    league_keys: list[str] | None = None,
    season: str = "2024-2025",
) -> list[dict]:
    """
    Generate soccer player prop bets for today's/tomorrow's fixtures.

    Props generated per player:
      - goals_scored       (line 0.5 — anytime scorer)
      - assists            (line 0.5)
      - shots_on_target    (line 1.5)
      - shots_total        (line 1.5)
      - cards              (line 0.5 — yellow/red card)
      - goal_or_assist     (line 0.5 — composite)

    Data:  FBRef standard stats + shooting stats (free, no key).
    Prob:  Poisson (discrete counts) for goals/assists/shots.
           P(X >= 1) = 1 - e^{-lambda} for goals/assists/cards
           Normal cdf for shots above line.

    Returns list of dicts compatible with _build_prop_pick() in dashboard.
    """
    import math

    if not fixtures:
        return []

    use_leagues = league_keys if league_keys else list(_FBREF_LEAGUE_IDS.keys())
    # Build a map fixture label → fixture dict
    fixture_map: dict[str, dict] = {}
    for f in fixtures:
        home = f.get("home_team", "")
        away = f.get("away_team", "")
        if home and away:
            fixture_map[f"{away} @ {home}"] = f
            fixture_map[f"{home} vs {away}"] = f

    # Derive integer season year for DB cache key  (e.g. "2024-2025" → 2025)
    try:
        _season_int = int(season.split("-")[-1])
    except Exception:
        _season_int = 0
    _stat_group = "fbref_std"  # ≤ 20 chars for team_stats.stat_group

    # Load stats per league — check DB cache first, then scrape FBRef
    std_frames: list[pd.DataFrame] = []
    for lk in use_leagues:
        # ── DB cache check ──────────────────────────────────────────────────
        cached_df = pd.DataFrame()
        try:
            from data.db import get_stats_cache, save_stats_cache as _save_fbref
            cached_json = get_stats_cache('soccer', lk, _season_int, _stat_group,
                                          max_age_hours=6)
            if cached_json:
                cached_df = pd.read_json(cached_json, orient='records')
                if not cached_df.empty:
                    print(f"[soccer_fetcher] FBRef {lk} stats loaded from DB cache "
                          f"({len(cached_df)} players)")
        except Exception as _dce:
            print(f"[soccer_fetcher] DB cache read error ({lk}): {_dce}")

        if not cached_df.empty:
            std_frames.append(cached_df)
            continue  # skip scraping this league

        # ── FBRef scrape (only if not cached) ──────────────────────────────
        df = _fbref_standard_stats(lk, season)
        # Merge shooting stats to get real shot counts
        if not df.empty and not _FBREF_BLOCKED:
            try:
                time.sleep(1)
                sh_df = _fbref_shooting_stats(lk, season)
                if not sh_df.empty:
                    sh_key = next((c for c in ("player", "Player") if c in sh_df.columns), None)
                    df_key = next((c for c in ("player", "Player") if c in df.columns), None)
                    if sh_key and df_key:
                        sh_df = sh_df.rename(columns={sh_key: df_key})
                        # bring in sh (shots), sot (shots on target) columns
                        sh_cols = [df_key] + [c for c in sh_df.columns if c in ("sh", "sot", "sh_90", "sot_90", "npxg_per_sh")]
                        df = df.merge(sh_df[sh_cols], on=df_key, how="left")
                        print(f"[soccer_fetcher] Merged shooting stats for {lk}")
            except Exception as _sher:
                print(f"[soccer_fetcher] Shooting merge skipped ({lk}): {_sher}")
        if not df.empty:
            std_frames.append(df)
            print(f"[soccer_fetcher] FBRef standard stats: {len(df)} players ({lk} {season})")
            # Save to DB cache for future runs
            try:
                from data.db import save_stats_cache as _save_fbref
                _save_fbref('soccer', lk, _season_int, _stat_group,
                            df.to_json(orient='records'))
            except Exception as _dse:
                print(f"[soccer_fetcher] DB cache save error ({lk}): {_dse}")
        if _FBREF_BLOCKED:
            break
        time.sleep(2)  # stay under FBRef rate limit

    if not std_frames:
        print("[soccer_fetcher] No FBRef player data — soccer props unavailable this run")
        return []

    combined = pd.concat(std_frames, ignore_index=True)

    # Normalise key columns
    def _col(df, *candidates):
        for c in candidates:
            if c in df.columns:
                return c
        return None

    # Find column names (FBRef occasionally renames them)
    col_player   = _col(combined, "player")
    col_squad    = _col(combined, "squad", "team")
    col_mp       = _col(combined, "mp", "matches_played", "playing_time_mp")
    col_min      = _col(combined, "min", "playing_time_min", "90s")
    col_goals    = _col(combined, "gls", "goals", "performance_gls")
    col_assists  = _col(combined, "ast", "assists", "performance_ast")
    col_xg       = _col(combined, "xg", "expected_xg")
    col_xa       = _col(combined, "xa", "xag", "expected_xa", "expected_xag")
    col_yc       = _col(combined, "crdy", "yellow_cards", "performance_crdy")
    col_rc       = _col(combined, "crdr", "red_cards",    "performance_crdr")

    required = [col_player, col_squad, col_mp, col_goals]
    if any(c is None for c in required):
        print(f"[soccer_fetcher] FBRef columns not recognised: {combined.columns.tolist()[:20]}")
        return []

    def _flt(row, col, default=0.0):
        if col is None:
            return default
        v = row.get(col, default)
        try:
            return float(v)
        except (TypeError, ValueError):
            return default

    props: list[dict] = []
    seen:  set[str]   = set()

    for _, row in combined.iterrows():
        pname = str(row.get(col_player, "")).strip()
        squad = str(row.get(col_squad, "")).strip()
        if not pname or pname in ("", "Player", "nan"):
            continue

        mp     = max(_flt(row, col_mp),  1)
        goals  = _flt(row, col_goals)
        ast    = _flt(row, col_assists)
        xg     = _flt(row, col_xg)
        xa     = _flt(row, col_xa)
        yc     = _flt(row, col_yc)
        rc     = _flt(row, col_rc)

        # Per-game rates (use xG as better predictor when available)
        goal_rate  = (xg if xg > 0 else goals) / mp
        ast_rate   = (xa if xa > 0 else ast)   / mp
        card_rate  = (yc + rc * 2)              / mp  # weighted
        goa_rate   = goal_rate + ast_rate        # goal or assist

        # Find which fixture this player's team is in
        game_label = ""
        fixture    = {}
        for fl, fx in fixture_map.items():
            ht_lower = fx.get("home_team", "").lower()
            at_lower = fx.get("away_team", "").lower()
            sq_lower = squad.lower()
            if sq_lower in ht_lower or ht_lower in sq_lower or \
               sq_lower in at_lower or at_lower in sq_lower or \
               squad.split()[-1].lower() in ht_lower or \
               squad.split()[-1].lower() in at_lower:
                game_label = fl
                fixture    = fx
                break

        if not game_label:
            continue

        key = (pname, game_label)
        if key in seen:
            continue
        seen.add(key)

        # Minimum games played filter
        if mp < 5:
            continue

        # ── Poisson probabilities ─────────────────────────────────────────
        # Goals (anytime scorer) — P(≥1 goal this match)
        # We scale season rate by average minutes / 90 to get per-match lambda
        min_ratio = min(_flt(row, col_min, 90) / 90, 1.0) if col_min else 0.7

        def poisson_over(lam: float, line: float) -> float:
            """P(X > line) using Poisson distribution."""
            if lam <= 0:
                return 0.0
            if line < 1:
                # line=0.5: P(X>=1) = 1 - P(X=0) = 1 - e^-λ
                return 1.0 - math.exp(-lam)
            # P(X > line) = 1 - P(X <= floor(line))
            k = int(line)
            cdf = sum(
                math.exp(-lam) * (lam ** i) / math.factorial(i)
                for i in range(k + 1)
            )
            return max(0.0, min(1.0, 1.0 - cdf))

        def normal_over(mean: float, std: float, line: float) -> float:
            from scipy import stats as _st
            if mean <= 0 or std <= 0:
                return 0.0
            return float(_st.norm.sf(line, loc=mean, scale=std))

        base_props = [
            # (stat_type,       per_match_lambda/mean, line, is_poisson, std_if_normal)
            ("goals_scored",    goal_rate * min_ratio,  0.5,  True,  0.0),
            ("assists",         ast_rate  * min_ratio,  0.5,  True,  0.0),
            ("goal_or_assist",  goa_rate  * min_ratio,  0.5,  True,  0.0),
            ("cards",           card_rate,              0.5,  True,  0.0),
        ]
        # Add shots if FBRef shooting data is merged later (placeholder via xG proxy)
        sh_rate  = xg / 0.096 if xg > 0 else 0.0  # ~9.6% shot conversion
        sot_rate = sh_rate * 0.40  # ~40% of shots on target
        if sh_rate > 0.5:
            base_props += [
                ("shots_total",      sh_rate  * min_ratio, 1.5, False, 1.2),
                ("shots_on_target",  sot_rate * min_ratio, 0.5, False, 0.7),
            ]

        league_key_val = str(row.get("league_key", ""))
        for stat_type, mean_val, line, is_poisson, std in base_props:
            if mean_val <= 0:
                continue
            if is_poisson:
                over_p  = poisson_over(mean_val, line)
            else:
                over_p  = normal_over(mean_val, std, line)
            under_p = 1.0 - over_p
            if max(over_p, under_p) < 0.52:
                continue

            props.append({
                "name":         pname,
                "team":         squad,
                "game":         game_label,
                "league":       fixture.get("league", league_key_val),
                "sport":        "soccer",
                "date":         fixture.get("date", ""),
                "game_time":    fixture.get("game_time"),
                "stat_type":    stat_type,
                "line":         line,
                "avg_per_game": round(mean_val, 3),
                "over_prob":    round(over_p, 4),
                "under_prob":   round(under_p, 4),
                # display fields
                "goals":        round(goals),
                "assists":      round(ast),
                "xg":           round(xg, 2),
                "xa":           round(xa, 2),
                "goals_pg":     round(goal_rate, 3),
                "assists_pg":   round(ast_rate, 3),
                "card_pg":      round(card_rate, 3),
                "mp":           round(mp),
                # Pitcher/hitter fields (0 — not applicable to soccer)
                "era": 0.0, "xfip": 0.0, "k9": 0.0, "k_pct": 0.0,
                "whip": 0.0, "avg_ks": 0.0, "avg": 0.0, "ops": 0.0,
                "wrc_plus": 0, "over_pct": round(over_p * 100), "under_pct": round(under_p * 100),
            })

    print(f"[soccer_fetcher] Soccer player props: {len(props)} qualifying picks")
    return props


def get_player_stats_fbref(league_key: str = "EPL", season: str = "2024-2025") -> pd.DataFrame:
    """
    Legacy wrapper — use _fbref_standard_stats() directly for new code.
    """
    return _fbref_standard_stats(league_key, season)
