"""
MLB Data Fetcher
================
Data sources (all free):
  - MLB Stats API (statsapi) : official API for team stats + schedule (no key, no blocking)
  - pybaseball (bref)        : Baseball Reference individual player stats

Provides:
  - Team season batting/pitching stats
  - Starting pitcher stats
  - Recent team form (last N games)
  - Today's scheduled games with probable starters
  - Player prop relevant stats (HR rate, K rate, hit rate)
"""

import pandas as pd
import numpy as np
import datetime
import sys
import os

# Suppress pybaseball progress bars in non-interactive mode
import warnings
warnings.filterwarnings("ignore")

# Eastern-time date helper (10 PM cutover)
try:
    _SRC = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if _SRC not in sys.path:
        sys.path.insert(0, _SRC)
    from config import et_today as _et_today
except Exception:
    def _et_today():
        return datetime.date.today()

try:
    import pybaseball as pb
    pb.cache.enable()
    PYBASEBALL_OK = True
except ImportError:
    PYBASEBALL_OK = False
    print("[mlb_fetcher] pybaseball not installed – pip install pybaseball")

try:
    import statsapi as mlbstatsapi  # pip install MLB-StatsAPI
    MLB_API_OK = True
except ImportError:
    MLB_API_OK = False
    print("[mlb_fetcher] MLB-StatsAPI not installed – pip install MLB-StatsAPI")


def _calendar_today_et() -> datetime.date:
    """Return calendar date in America/New_York (no 10 PM cutover)."""
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


def get_team_batting_stats(season: int) -> pd.DataFrame:
    """Return team-level batting stats for a season via official MLB Stats API."""
    if not MLB_API_OK:
        return pd.DataFrame()
    try:
        data = mlbstatsapi.get("teams_stats", {
            "stats": "season", "group": "hitting",
            "season": season, "sportIds": 1, "gameType": "R",
        })
        splits = data.get("stats", [{}])[0].get("splits", [])
        rows = []
        for t in splits:
            stat = t.get("stat", {})
            rows.append({
                "team":        t.get("team", {}).get("name", ""),
                "season":      season,
                "runs_scored": int(stat.get("runs", 0) or 0),
                "bat_avg":     float(stat.get("avg", 0) or 0),
                "obp":         float(stat.get("obp", 0) or 0),
                "slg":         float(stat.get("slg", 0) or 0),
                "hr_scored":   int(stat.get("homeRuns", 0) or 0),
                "wrc_plus":    100,  # not in official API; placeholder
            })
        return pd.DataFrame(rows)
    except Exception as e:
        print(f"[mlb_fetcher] team_batting error: {e}")
        return pd.DataFrame()


def get_team_pitching_stats(season: int) -> pd.DataFrame:
    """Return team-level pitching stats for a season via official MLB Stats API."""
    if not MLB_API_OK:
        return pd.DataFrame()
    try:
        data = mlbstatsapi.get("teams_stats", {
            "stats": "season", "group": "pitching",
            "season": season, "sportIds": 1, "gameType": "R",
        })
        splits = data.get("stats", [{}])[0].get("splits", [])
        rows = []
        for t in splits:
            stat = t.get("stat", {})
            ip   = float(stat.get("inningsPitched", 1) or 1)
            so   = float(stat.get("strikeOuts",    0) or 0)
            bb   = float(stat.get("baseOnBalls",   0) or 0)
            hr   = float(stat.get("homeRuns",      0) or 0)
            rows.append({
                "team":             t.get("team", {}).get("name", ""),
                "season":           season,
                "era":              float(stat.get("era",  0) or 0),
                "whip":             float(stat.get("whip", 0) or 0),
                "k_per_9":          round(so / ip * 9, 2) if ip > 0 else 0.0,
                "bb_per_9":         round(bb / ip * 9, 2) if ip > 0 else 0.0,
                "fip":              float(stat.get("era",  0) or 0),  # approx
                "hr_allowed_per_9": round(hr / ip * 9, 2) if ip > 0 else 0.0,
            })
        return pd.DataFrame(rows)
    except Exception as e:
        print(f"[mlb_fetcher] team_pitching error: {e}")
        return pd.DataFrame()


def get_pitcher_stats(season: int, min_ip: float = 20.0) -> pd.DataFrame:
    """Return individual pitcher stats from Baseball Reference (no FanGraphs key needed)."""
    if not PYBASEBALL_OK:
        return pd.DataFrame()
    try:
        df = pb.pitching_stats_bref(season)
        df["season"] = season
        df["IP_num"] = pd.to_numeric(df.get("IP", pd.Series(dtype=float)), errors="coerce").fillna(0)
        df = df[df["IP_num"] >= min_ip].copy()
        # Compute per-9 rates from raw counting stats
        for raw, per9 in (("SO", "K/9"), ("BB", "BB/9"), ("HR", "HR/9")):
            if raw in df.columns:
                df[per9] = (pd.to_numeric(df[raw], errors="coerce").fillna(0)
                            / df["IP_num"].replace(0, 1) * 9).round(2)
        keep = ["Name", "Team", "W", "L", "ERA", "WHIP", "K/9", "BB/9", "HR/9", "IP", "season"]
        return df[[c for c in keep if c in df.columns]]
    except Exception as e:
        print(f"[mlb_fetcher] pitching_stats_bref error: {e}")
        return pd.DataFrame()


def get_batter_stats(season: int, min_pa: int = 100) -> pd.DataFrame:
    """Return individual batter stats from Baseball Reference (no FanGraphs key needed)."""
    if not PYBASEBALL_OK:
        return pd.DataFrame()
    try:
        df = pb.batting_stats_bref(season)
        df["season"] = season
        if "PA" in df.columns:
            df = df[pd.to_numeric(df["PA"], errors="coerce").fillna(0) >= min_pa].copy()
        # Bref uses 'BA' not 'AVG'
        if "BA" in df.columns and "AVG" not in df.columns:
            df = df.rename(columns={"BA": "AVG"})
        keep = ["Name", "Team", "PA", "AB", "H", "HR", "RBI", "BB", "SO",
                "AVG", "OBP", "SLG", "OPS", "season"]
        return df[[c for c in keep if c in df.columns]]
    except Exception as e:
        print(f"[mlb_fetcher] batting_stats_bref error: {e}")
        return pd.DataFrame()


def _parse_mlb_game(game, fallback_date: str) -> dict:
    """Convert a raw mlbstatsapi schedule entry to a standardised game dict."""
    raw_dt = game.get("game_datetime", "") or ""
    game_time = None
    game_datetime_iso = None
    game_date = game.get("game_date", fallback_date)
    if raw_dt:
        try:
            import pytz
            utc_dt = datetime.datetime.fromisoformat(raw_dt.replace("Z", "+00:00"))
            eastern = pytz.timezone("America/New_York")
            local_dt = utc_dt.astimezone(eastern)
            game_time = local_dt.strftime("%H:%M")
            game_datetime_iso = local_dt.isoformat()
            game_date = local_dt.date().isoformat()
        except Exception:
            pass
    return {
        "game_pk":           game.get("game_id"),
        "date":              game_date,
        "game_time":         game_time,
        "game_datetime":     game_datetime_iso,
        "home_team":         game.get("home_name", ""),
        "away_team":         game.get("away_name", ""),
        "home_starter":      game.get("home_probable_pitcher", "TBD") or "TBD",
        "away_starter":      game.get("away_probable_pitcher", "TBD") or "TBD",
        "status":            game.get("status", ""),
        "sport":             "mlb",
    }


def get_schedule_today() -> list[dict]:
    """
    Return today's MLB schedule with probable starters via official MLB Stats API.
    Each item: {game_pk, date, game_time, home_team, away_team, home_starter, away_starter, status}
    Also saves games to PostgreSQL DB.
    """
    if not MLB_API_OK:
        return []
    try:
        today = _calendar_today_et().strftime("%Y-%m-%d")
        schedule = mlbstatsapi.schedule(start_date=today, end_date=today)
        games = [_parse_mlb_game(g, today) for g in schedule]
        _save_mlb_games_to_db(games)
        return games
    except Exception as e:
        print(f"[mlb_fetcher] schedule error: {e}")
        return []


def get_schedule_range(days_ahead: int = 1) -> list[dict]:
    """
    Return today + N days ahead MLB games (today + tomorrow by default).
    Saves all fetched games to DB.
    """
    if not MLB_API_OK:
        return []
    try:
        today = _calendar_today_et()
        end   = today + datetime.timedelta(days=days_ahead)
        schedule = mlbstatsapi.schedule(
            start_date=today.strftime("%Y-%m-%d"),
            end_date=end.strftime("%Y-%m-%d"),
        )
        games = [_parse_mlb_game(g, today.strftime("%Y-%m-%d")) for g in schedule]
        _save_mlb_games_to_db(games)
        return games
    except Exception as e:
        print(f"[mlb_fetcher] schedule_range error: {e}")
        return []


def _save_mlb_games_to_db(games: list[dict]):
    """Persist MLB games to PostgreSQL (best-effort, silent on error)."""
    try:
        from data.db import upsert_game
        for g in games:
            upsert_game(
                sport="mlb", league="MLB",
                home_team=g["home_team"], away_team=g["away_team"],
                game_date=g["date"],
                game_time=g.get("game_time"),
                game_datetime=g.get("game_datetime"),
                status=g.get("status", "Scheduled"),
                home_starter=g.get("home_starter"),
                away_starter=g.get("away_starter"),
            )
    except Exception as e:
        print(f"[mlb_fetcher] db save error: {e}")


def get_team_recent_form(team_abbr: str, season: int, last_n: int = 10) -> dict:
    """
    Return win/loss record and run differential for the last N games.
    Uses pybaseball schedule_and_record.
    team_abbr: e.g. 'NYY', 'LAD', 'BOS'
    """
    if not PYBASEBALL_OK:
        return {}
    try:
        df = pb.schedule_and_record(season, team_abbr)
        df = df[df["R"].notna()].tail(last_n)  # completed games only
        wins = (df["W/L"].str.startswith("W")).sum()
        losses = (df["W/L"].str.startswith("L")).sum()
        run_diff = (df["R"].astype(float) - df["RA"].astype(float)).sum()
        return {
            "team": team_abbr,
            "last_n": last_n,
            "wins": int(wins),
            "losses": int(losses),
            "run_diff": float(run_diff),
            "win_pct": round(wins / last_n, 3) if last_n > 0 else 0.0,
        }
    except Exception as e:
        print(f"[mlb_fetcher] recent_form error for {team_abbr}: {e}")
        return {}


def build_game_dataset(seasons: list[int]) -> pd.DataFrame:
    """
    Build a merged team-vs-team feature dataset for model training.
    Merges batting + pitching stats per team per season.
    Returns one row per team per season with offensive and pitching features.
    """
    batting_frames, pitching_frames = [], []
    for s in seasons:
        b = get_team_batting_stats(s)
        p = get_team_pitching_stats(s)
        if not b.empty:
            batting_frames.append(b)
        if not p.empty:
            pitching_frames.append(p)

    if not batting_frames or not pitching_frames:
        print("[mlb_fetcher] No data retrieved – check pybaseball installation.")
        return pd.DataFrame()

    batting = pd.concat(batting_frames, ignore_index=True)
    pitching = pd.concat(pitching_frames, ignore_index=True)

    # Normalise team name column (FanGraphs uses 'Team')
    bat_cols = {"Team": "team", "season": "season",
                "R": "runs_scored", "AVG": "bat_avg",
                "OBP": "obp", "SLG": "slg", "wRC+": "wrc_plus",
                "HR": "hr_scored"}
    pit_cols = {"Team": "team", "season": "season",
                "ERA": "era", "WHIP": "whip",
                "K/9": "k_per_9", "BB/9": "bb_per_9",
                "FIP": "fip", "HR/9": "hr_allowed_per_9"}

    batting = batting.rename(columns={k: v for k, v in bat_cols.items() if k in batting.columns})
    pitching = pitching.rename(columns={k: v for k, v in pit_cols.items() if k in pitching.columns})

    bat_keep = [v for v in bat_cols.values() if v in batting.columns]
    pit_keep = [v for v in pit_cols.values() if v in pitching.columns]

    merged = pd.merge(
        batting[bat_keep],
        pitching[pit_keep],
        on=["team", "season"],
        how="inner",
    )
    return merged


# --------------------------------------------------------------------------- #
# Player Props helpers
# --------------------------------------------------------------------------- #

def _load_bref_season(fetch_fn, seasons: list[int]):
    """
    Try loading Baseball Reference data for the first season that succeeds.
    Returns (DataFrame, season_used) or (empty DataFrame, None).
    Suppresses errors silently — bref scraping fails when seasons aren't fully posted.
    """
    for s in seasons:
        try:
            df = fetch_fn(s)
            if df is not None and not df.empty:
                # Flatten multi-level columns if pybaseball returns them
                if hasattr(df.columns, "levels"):
                    df.columns = [" ".join(str(c) for c in col).strip()
                                  for col in df.columns.values]
                return df, s
        except Exception:
            pass
    return pd.DataFrame(), None


def get_player_prop_stats(player_name: str, season: int) -> dict:
    """
    Return per-game averages useful for player prop bets.
    Tries the requested season first, then falls back to season-1 and season-2.
      - H/game, HR/game, RBI/game (hitters)
      - K/game, IP/game, ER/game (pitchers)
    """
    if not PYBASEBALL_OK:
        return {}

    import datetime
    current_year = _et_today().year
    fallback_seasons = sorted({season, current_year - 1, current_year - 2}, reverse=True)

    # ── Hitter lookup ────────────────────────────────────────────────────
    batters, _ = _load_bref_season(pb.batting_stats_bref, fallback_seasons)
    if not batters.empty and "Name" in batters.columns:
        row = batters[batters["Name"].str.contains(player_name, case=False, na=False)]
        if not row.empty:
            row = row.iloc[0]
            games = float(row.get("G", 1) or 1)
            avg_col = "BA" if "BA" in row.index else "AVG"
            return {
                "type":        "hitter",
                "name":        row["Name"],
                "team":        row.get("Team", ""),
                "season":      season,
                "H_per_game":  round(float(row.get("H",   0) or 0) / games, 3),
                "HR_per_game": round(float(row.get("HR",  0) or 0) / games, 3),
                "RBI_per_game":round(float(row.get("RBI", 0) or 0) / games, 3),
                "AVG": float(row.get(avg_col, 0) or 0),
                "OBP": float(row.get("OBP", 0) or 0),
                "SLG": float(row.get("SLG", 0) or 0),
            }

    # ── Pitcher lookup ───────────────────────────────────────────────────
    pitchers, _ = _load_bref_season(pb.pitching_stats_bref, fallback_seasons)
    if not pitchers.empty and "Name" in pitchers.columns:
        row = pitchers[pitchers["Name"].str.contains(player_name, case=False, na=False)]
        if not row.empty:
            row = row.iloc[0]
            games = float(row.get("G", 1) or 1)
            gs    = float(row.get("GS", 1) or 1)
            ip    = float(row.get("IP", 1) or 1)
            so    = float(row.get("SO", 0) or 0)
            return {
                "type":         "pitcher",
                "name":         row["Name"],
                "team":         row.get("Team", ""),
                "season":       season,
                "K_per_game":   round(so / games, 3),
                "IP_per_start": round(ip / max(gs, 1), 3),
                "ERA":          float(row.get("ERA",  0) or 0),
                "WHIP":         float(row.get("WHIP", 0) or 0),
                "K9":           round(so / ip * 9, 2) if ip > 0 else 0.0,
            }

    return {}


# ---------------------------------------------------------------------------
# MLB Stats API individual player lookups  (primary — always current data)
# ---------------------------------------------------------------------------

def _pitcher_stats_mlb_api(player_name: str) -> dict:
    """Fetch current-season pitcher stats from official MLB Stats API."""
    if not MLB_API_OK:
        return {}
    try:
        lookup = mlbstatsapi.lookup_player(player_name, sportId=1)
        if not lookup:
            return {}
        pid = lookup[0].get("id")
        if not pid:
            return {}
        data = mlbstatsapi.player_stat_data(pid, group="pitching", type="season", sportId=1)
        splits = data.get("stats") or []
        stat = {}
        for sp in splits:
            if sp.get("stats"):
                stat = sp["stats"]
                break
        if not stat:
            return {}
        ip_raw = str(stat.get("inningsPitched", "1") or "1")
        try:
            parts = ip_raw.split(".")
            ip = float(parts[0]) + (float(parts[1]) / 3.0 if len(parts) == 2 else 0)
        except Exception:
            ip = float(ip_raw or 1)
        era_s  = str(stat.get("era",  "0") or "0")
        whip_s = str(stat.get("whip", "0") or "0")
        return {
            "Name":  data.get("fullName", player_name),
            "Team":  "",
            "G":     float(stat.get("gamesPlayed", stat.get("games", 1)) or 1),
            "GS":    float(stat.get("gamesStarted", 1) or 1),
            "IP":    ip,
            "SO":    float(stat.get("strikeOuts", 0) or 0),
            "BB":    float(stat.get("baseOnBalls", 0) or 0),
            "ERA":   float(era_s)  if era_s  not in ("-.--", "--", "") else 0.0,
            "WHIP":  float(whip_s) if whip_s not in ("-.--", "--", "") else 0.0,
            "_source": "mlb_api",
        }
    except Exception:
        return {}


def _batter_stats_mlb_api(player_name: str) -> dict:
    """Fetch current-season batting stats from official MLB Stats API."""
    if not MLB_API_OK:
        return {}
    try:
        lookup = mlbstatsapi.lookup_player(player_name, sportId=1)
        if not lookup:
            return {}
        pid = lookup[0].get("id")
        if not pid:
            return {}
        data = mlbstatsapi.player_stat_data(pid, group="hitting", type="season", sportId=1)
        splits = data.get("stats") or []
        stat = {}
        for sp in splits:
            if sp.get("stats"):
                stat = sp["stats"]
                break
        if not stat:
            return {}
        g   = float(stat.get("gamesPlayed", 1) or 1)
        h   = float(stat.get("hits", 0) or 0)
        hr  = float(stat.get("homeRuns", 0) or 0)
        rbi = float(stat.get("rbi", 0) or 0)
        d2  = float(stat.get("doubles", 0) or 0)
        d3  = float(stat.get("triples", 0) or 0)
        tb  = h + d2 + 2 * d3 + 3 * hr
        return {
            "Name":   data.get("fullName", player_name),
            "G":      g,
            "H":      h,  "HR": hr, "RBI": rbi,
            "2B":     d2, "3B": d3, "TB":  tb,
            "H_pg":   round(h  / g, 3) if g > 0 else 0,
            "HR_pg":  round(hr / g, 3) if g > 0 else 0,
            "RBI_pg": round(rbi/ g, 3) if g > 0 else 0,
            "TB_pg":  round(tb / g, 3) if g > 0 else 0,
            "AVG":    float(stat.get("avg", 0) or 0),
            "OBP":    float(stat.get("obp", 0) or 0),
            "SLG":    float(stat.get("slg", 0) or 0),
            "OPS":    float(stat.get("ops", 0) or 0),
            "_source": "mlb_api",
        }
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# FanGraphs enriched stats  (K%, BB%, xFIP, wRC+, wOBA, WAR)
# ---------------------------------------------------------------------------

# Circuit-breaker: if FanGraphs returns 403 once, skip all subsequent calls
# this session — fall through to Baseball Reference / MLB Stats API instead.
_FANGRAPHS_BLOCKED: bool = False


def get_fangraphs_pitcher_stats(season: int, min_ip: float = 10.0) -> pd.DataFrame:
    """FanGraphs pitcher leaderboard — xFIP, K%, BB%, SIERA, WAR."""
    global _FANGRAPHS_BLOCKED
    if not PYBASEBALL_OK or _FANGRAPHS_BLOCKED:
        return pd.DataFrame()
    try:
        df = pb.pitching_stats(season, qual=0)
        if df is None or df.empty:
            return pd.DataFrame()
        df["season"] = season
        if "IP" in df.columns:
            df = df[pd.to_numeric(df["IP"], errors="coerce").fillna(0) >= min_ip].copy()
        keep = ["Name", "Team", "G", "GS", "IP", "ERA", "WHIP", "FIP",
                "xFIP", "K%", "BB%", "K/9", "BB/9", "BABIP", "WAR", "season"]
        return df[[c for c in keep if c in df.columns]]
    except Exception as e:
        msg = str(e)
        if "403" in msg:
            _FANGRAPHS_BLOCKED = True
            print("[mlb_fetcher] FanGraphs blocked (403) — using Baseball Reference + MLB API instead")
        else:
            print(f"[mlb_fetcher] fangraphs pitching error: {e}")
        return pd.DataFrame()


def get_fangraphs_batting_stats(season: int, min_pa: int = 30) -> pd.DataFrame:
    """FanGraphs batting leaderboard — wRC+, wOBA, ISO, WAR."""
    global _FANGRAPHS_BLOCKED
    if not PYBASEBALL_OK or _FANGRAPHS_BLOCKED:
        return pd.DataFrame()
    try:
        df = pb.batting_stats(season, qual=0)
        if df is None or df.empty:
            return pd.DataFrame()
        df["season"] = season
        if "PA" in df.columns:
            df = df[pd.to_numeric(df["PA"], errors="coerce").fillna(0) >= min_pa].copy()
        keep = ["Name", "Team", "G", "PA", "H", "2B", "3B", "HR", "R", "RBI",
                "BB", "SO", "AVG", "OBP", "SLG", "OPS", "wRC+", "wOBA",
                "ISO", "BABIP", "SB", "WAR", "season"]
        return df[[c for c in keep if c in df.columns]]
    except Exception as e:
        msg = str(e)
        if "403" in msg:
            _FANGRAPHS_BLOCKED = True
            print("[mlb_fetcher] FanGraphs blocked (403) — using Baseball Reference + MLB API instead")
        else:
            print(f"[mlb_fetcher] fangraphs batting error: {e}")
        return pd.DataFrame()


# Team full name → FanGraphs 2-3 char abbreviation
_TEAM_TO_ABBR = {
    "yankees": "NYY",  "red sox": "BOS",   "blue jays": "TOR",  "rays": "TBR",
    "orioles": "BAL",  "white sox": "CHW", "guardians": "CLE",  "tigers": "DET",
    "royals":  "KCR",  "twins": "MIN",     "astros": "HOU",     "athletics": "OAK",
    "mariners":"SEA",  "angels": "LAA",    "rangers": "TEX",    "braves": "ATL",
    "phillies":"PHI",  "mets": "NYM",      "marlins": "MIA",    "nationals": "WSN",
    "cubs":    "CHC",  "cardinals": "STL", "brewers": "MIL",    "reds": "CIN",
    "pirates": "PIT",  "dodgers": "LAD",   "giants": "SFG",     "padres": "SDP",
    "rockies": "COL",  "diamondbacks": "ARI",
}


def _team_abbr(full_name: str) -> str:
    """Return FanGraphs team abbreviation for a full MLB team name."""
    lower = full_name.lower()
    for keyword, abbr in _TEAM_TO_ABBR.items():
        if keyword in lower:
            return abbr
    return full_name.split()[-1][:3].upper() if full_name else ""


# ---------------------------------------------------------------------------
# Hitter props  (H, HR, Total Bases per game)
# ---------------------------------------------------------------------------

def _build_hitter_df_from_mlb_api(games: list[dict], season: int) -> pd.DataFrame:
    """
    Build a batting stats DataFrame using the official MLB Stats API.
    Pulls the active roster for each team in today's games, then fetches
    season hitting stats for every position player.
    Always available — no FanGraphs / bref dependency.
    """
    if not MLB_API_OK:
        return pd.DataFrame()

    import time as _time
    rows = []
    seen_players: set = set()
    teams_needed = set()
    for g in games:
        teams_needed.add(g.get("home_team", ""))
        teams_needed.add(g.get("away_team", ""))
    teams_needed.discard("")

    # Get all MLB team IDs once
    try:
        teams_data = mlbstatsapi.get("teams", {"sportId": 1, "season": season})
        team_list  = teams_data.get("teams", [])
        name_to_id = {t["name"]: t["id"] for t in team_list}
    except Exception as e:
        print(f"[mlb_fetcher] MLB API team list error: {e}")
        return pd.DataFrame()

    for team_name in teams_needed:
        # Fuzzy match team name → ID
        team_id = name_to_id.get(team_name)
        if not team_id:
            # partial match on last word (e.g. "Yankees")
            kw = team_name.split()[-1] if team_name else ""
            team_id = next((v for k, v in name_to_id.items()
                            if kw and kw.lower() in k.lower()), None)
        if not team_id:
            print(f"[mlb_fetcher] MLB API: team not found: {team_name}")
            continue

        # Get active 40-man roster
        try:
            roster_data = mlbstatsapi.get("team_roster",
                                           {"teamId": team_id, "season": season,
                                            "rosterType": "active"})
            roster = roster_data.get("roster", [])
        except Exception as e:
            print(f"[mlb_fetcher] MLB API roster error ({team_name}): {e}")
            continue

        for player in roster:
            pos_type = player.get("position", {}).get("type", "")
            # Skip pitchers
            if pos_type in ("Pitcher",):
                continue
            pid   = player.get("person", {}).get("id")
            pname = player.get("person", {}).get("fullName", "")
            if not pid or pname in seen_players:
                continue
            seen_players.add(pname)

            try:
                data = mlbstatsapi.player_stat_data(pid, group="hitting",
                                                     type="season", sportId=1)
                splits = data.get("stats") or []
                stat = {}
                for sp in splits:
                    if sp.get("stats"):
                        stat = sp["stats"]
                        break
                if not stat:
                    continue
                g_played = float(stat.get("gamesPlayed", 0) or 0)
                if g_played < 3:
                    continue
                h   = float(stat.get("hits",       0) or 0)
                hr  = float(stat.get("homeRuns",   0) or 0)
                rbi = float(stat.get("rbi",        0) or 0)
                d2  = float(stat.get("doubles",    0) or 0)
                d3  = float(stat.get("triples",    0) or 0)
                bb  = float(stat.get("baseOnBalls",0) or 0)
                so  = float(stat.get("strikeOuts", 0) or 0)
                sb  = float(stat.get("stolenBases",0) or 0)
                r   = float(stat.get("runs",       0) or 0)
                tb  = h + d2 + 2 * d3 + 3 * hr
                avg_s = str(stat.get("avg",  ".000") or ".000")
                ops_s = str(stat.get("ops",  ".000") or ".000")
                def _safe_stat(s):
                    try:
                        return float(s)
                    except Exception:
                        return 0.0
                rows.append({
                    "Name":  pname,
                    "Team":  team_name,
                    "G":     g_played,
                    "H":     h,  "HR": hr,  "RBI": rbi,
                    "2B":    d2, "3B": d3,  "BB":  bb,
                    "SO":    so, "SB": sb,  "R":   r,
                    "TB":    tb,
                    "AVG":   _safe_stat(avg_s),
                    "OPS":   _safe_stat(ops_s),
                    "wRC+":  100,  # not in official API
                    "_source": "mlb_api",
                })
                _time.sleep(0.05)  # gentle rate-limit
            except Exception:
                continue

    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    print(f"[mlb_fetcher] MLB API batting fallback: {len(df)} batters from {len(teams_needed)} teams")
    return df


def get_hitter_props_batch(games: list[dict], season: int) -> list[dict]:
    """
    Generate hitter prop bets for today's/tomorrow's games.

    Data priority:
      1. FanGraphs batting stats (best: wRC+, wOBA, ISO)
      2. Baseball Reference bref as fallback
    Returns props for Hits, Home Runs, Total Bases.
    """
    from scipy import stats as scipy_stats
    import datetime
    if not games:
        return []

    current_year = _et_today().year
    fallback_seasons = sorted({season, current_year - 1}, reverse=True)

    # ── Check DB cache before hitting external APIs ───────────────────────
    fg_df = pd.DataFrame()
    try:
        from data.db import get_stats_cache, save_stats_cache as _save_bat
        for s in fallback_seasons:
            cached = get_stats_cache('mlb', '__batting_all__', s, 'batting', max_age_hours=6)
            if cached:
                fg_df = pd.read_json(cached, orient='records')
                if not fg_df.empty:
                    print(f"[mlb_fetcher] Batting stats loaded from DB cache (season {s}, {len(fg_df)} batters)")
                    break
    except Exception as _ce:
        print(f"[mlb_fetcher] DB batting cache read error: {_ce}")

    if fg_df.empty:
        # Load FanGraphs batting stats
        for s in fallback_seasons:
            fg_df = get_fangraphs_batting_stats(s, min_pa=30)
            if not fg_df.empty:
                print(f"[mlb_fetcher] FanGraphs batting: {len(fg_df)} batters (season {s})")
                try:
                    from data.db import save_stats_cache as _save_bat
                    _save_bat('mlb', '__batting_all__', s, 'batting', fg_df.to_json(orient='records'))
                except Exception:
                    pass
                break
            if _FANGRAPHS_BLOCKED:
                break  # 403 hit — skip remaining seasons immediately

    if fg_df.empty and PYBASEBALL_OK:
        fg_df, used_s = _load_bref_season(pb.batting_stats_bref, fallback_seasons)
        if not fg_df.empty:
            # Bref uses BA not AVG
            if "BA" in fg_df.columns and "AVG" not in fg_df.columns:
                fg_df = fg_df.rename(columns={"BA": "AVG"})
            print(f"[mlb_fetcher] Bref batting fallback: {len(fg_df)} batters")
            try:
                from data.db import save_stats_cache as _save_bat
                _save_bat('mlb', '__batting_all__', used_s or fallback_seasons[0], 'batting',
                          fg_df.to_json(orient='records'))
            except Exception:
                pass

    if fg_df.empty:
        # Final fallback: use MLB Stats API to get roster + per-player hitting stats
        # This is always available (official API, no blocking), just slower
        print("[mlb_fetcher] Using MLB Stats API per-player batting fallback")
        fg_df = _build_hitter_df_from_mlb_api(games, season)

    if fg_df.empty:
        return []

    props = []
    seen:  set[str] = set()

    for g in games:
        home = g.get("home_team", "")
        away = g.get("away_team", "")
        game_label = f"{away} @ {home}"
        for team_name in (home, away):
            abbr = _team_abbr(team_name)
            team_rows = fg_df[fg_df["Team"].astype(str).apply(
                lambda t: abbr.lower() in t.lower() or t.lower() in abbr.lower()
            )]
            if team_rows.empty:
                # looser fuzzy on last word
                kw = team_name.split()[-1].lower()
                team_rows = fg_df[fg_df["Team"].astype(str).str.lower().str.contains(kw, na=False)]
            if team_rows.empty:
                continue

            # Sort by best offensive metric available
            sort_col = next((c for c in ("wRC+", "OPS", "SLG", "AVG") if c in team_rows.columns), None)
            if sort_col:
                team_rows = team_rows.sort_values(sort_col, ascending=False)
            top_hitters = team_rows.head(9)  # top 9 starters

            for _, row in top_hitters.iterrows():
                pname = str(row.get("Name", ""))
                key   = (pname, game_label)
                if not pname or key in seen:
                    continue
                seen.add(key)
                _g  = float(row.get("G", 1) or 1)
                if _g < 3:  # lower threshold for early season
                    continue
                h   = float(row.get("H",  0) or 0)
                hr  = float(row.get("HR", 0) or 0)
                d2  = float(row.get("2B", 0) or 0)
                d3  = float(row.get("3B", 0) or 0)
                tb  = h + d2 + 2 * d3 + 3 * hr
                h_pg  = h  / _g
                hr_pg = hr / _g
                tb_pg = tb / _g
                avg   = float(row.get("AVG",  0) or 0)
                ops   = float(row.get("OPS",  0) or 0)
                wrc   = float(row.get("wRC+", 0) or 0)

                rbi  = float(row.get("RBI", 0) or 0)
                bb   = float(row.get("BB",  0) or 0)
                r    = float(row.get("R",   0) or 0)
                sb   = float(row.get("SB",  0) or 0)
                so   = float(row.get("SO",  0) or 0)  # batter strikeouts
                rbi_pg = rbi / _g
                bb_pg  = bb  / _g
                r_pg   = r   / _g
                sb_pg  = sb  / _g
                so_pg  = so  / _g
                # 2B/game for doubles prop
                d2b_pg = d2  / _g

                # (stat_type, per-game mean, sportsbook line, std_factor)
                # std = max(mean * std_factor, 0.12) — proportional so high-avg
                # players show meaningfully higher probability than average ones
                # Lines match typical DraftKings / FanDuel offerings
                for prop_type, mean_val, line, sf in [
                    ("hits",             h_pg,   0.5,  0.55),
                    ("home_runs",        hr_pg,  0.5,  0.80),
                    ("total_bases",      tb_pg,  1.5,  0.50),
                    ("rbi",              rbi_pg, 0.5,  0.70),
                    ("runs",             r_pg,   0.5,  0.65),
                    ("walks",            bb_pg,  0.5,  0.48),
                    ("stolen_bases",     sb_pg,  0.5,  0.75),
                    ("batter_strikeouts",so_pg,  0.5,  0.50),
                    ("doubles",          d2b_pg, 0.5,  0.55),
                ]:
                    std     = max(mean_val * sf, 0.12)
                    over_p  = float(scipy_stats.norm.sf(line, loc=mean_val, scale=std))
                    under_p = 1.0 - over_p
                    if max(over_p, under_p) < 0.51:  # lower threshold = more props
                        continue
                    props.append({
                        "name":         pname,
                        "team":         team_name,
                        "game":         game_label,
                        "stat_type":    prop_type,
                        "line":         line,
                        "avg_per_game": round(mean_val, 3),
                        "over_prob":    round(over_p, 4),
                        "under_prob":   round(under_p, 4),
                        # pitcher fields (0 for hitters — template uses stat_type to decide display)
                        "era":          0.0,
                        "whip":         0.0,
                        "k9":           0.0,
                        "ip_per_start": 0.0,
                        # hitter-specific
                        "avg":          avg,
                        "ops":          ops,
                        "wrc_plus":     wrc,
                        "season_used":  int(row.get("season", season)),
                    })
    return props


def get_starters_props_batch(games: list[dict], season: int) -> list[dict]:
    """
    Fetch pitcher strikeout props for all probable starters.

    Data priority:
      1. FanGraphs pitching (xFIP, K%, best stats)
      2. Baseball Reference bref (fallback bulk)
      3. Official MLB Stats API per-player (always has current season)
    """
    from scipy import stats as scipy_stats
    import datetime

    if not games:
        return []

    current_year = _et_today().year
    fallback_seasons = sorted({season, current_year - 1, current_year - 2}, reverse=True)

    # ── Check DB cache before hitting external APIs ───────────────────────
    combined_df = pd.DataFrame()
    try:
        from data.db import get_stats_cache
        for s in fallback_seasons:
            cached = get_stats_cache('mlb', '__pitching_all__', s, 'pitching', max_age_hours=6)
            if cached:
                combined_df = pd.read_json(cached, orient='records')
                if not combined_df.empty:
                    print(f"[mlb_fetcher] Pitching stats loaded from DB cache (season {s}, {len(combined_df)} pitchers)")
                    break
    except Exception as _ce:
        print(f"[mlb_fetcher] DB pitching cache read error: {_ce}")

    if combined_df.empty:
        # ── Layer 1: FanGraphs bulk load ─────────────────────────────────────
        for s in fallback_seasons:
            df = get_fangraphs_pitcher_stats(s, min_ip=5.0)
            if not df.empty:
                combined_df = df
                print(f"[mlb_fetcher] FanGraphs pitching: {len(df)} pitchers (season {s})")
                try:
                    from data.db import save_stats_cache as _save_pit
                    _save_pit('mlb', '__pitching_all__', s, 'pitching', df.to_json(orient='records'))
                except Exception:
                    pass
                break
            if _FANGRAPHS_BLOCKED:
                break  # 403 hit — skip remaining seasons immediately

    # ── Layer 2: Baseball Reference fallback ──────────────────────────────
    if combined_df.empty and PYBASEBALL_OK:
        bref_df, used_s = _load_bref_season(pb.pitching_stats_bref, fallback_seasons)
        if not bref_df.empty:
            combined_df = bref_df
            print(f"[mlb_fetcher] Bref pitching fallback: {len(combined_df)} pitchers")
            try:
                from data.db import save_stats_cache as _save_pit
                _save_pit('mlb', '__pitching_all__', used_s or fallback_seasons[0], 'pitching',
                          bref_df.to_json(orient='records'))
            except Exception:
                pass

    props = []
    seen: set[str] = set()

    for g in games:
        for role_key in ("home_starter", "away_starter"):
            name = g.get(role_key, "TBD")
            if not name or name in ("TBD", "") or name in seen:
                continue
            seen.add(name)

            team_name = g.get("home_team", "") if role_key == "home_starter" else g.get("away_team", "")

            stat: dict = {}
            source = "none"

            # Try bulk dataframe first
            if not combined_df.empty and "Name" in combined_df.columns:
                rows = combined_df[combined_df["Name"].str.contains(name, case=False, na=False)]
                if not rows.empty:
                    stat   = rows.iloc[0].to_dict()
                    source = "fg" if "xFIP" in stat else "bref"

            # ── Layer 3: MLB Stats API per-player (guaranteed current data) ──
            if not stat:
                stat = _pitcher_stats_mlb_api(name)
                if stat:
                    source = "mlb_api"
                    print(f"[mlb_fetcher] MLB API pitcher: {name} ERA={stat.get('ERA')} SO={stat.get('SO')}")

            if not stat:
                print(f"[mlb_fetcher] No pitcher stats found for: {name}")
                continue

            _g   = float(stat.get("G",  stat.get("GS", 1)) or 1)
            _gs  = float(stat.get("GS", 1) or 1)
            _ip  = float(stat.get("IP", 1) or 1)
            _so  = float(stat.get("SO", stat.get("K", 0)) or 0)
            _era = float(stat.get("ERA", 4.5) or 4.5)
            _whip= float(stat.get("WHIP", 1.3) or 1.3)
            k9   = float(stat.get("K/9", 0) or 0) or (round(_so / _ip * 9, 2) if _ip > 0 else 0.0)
            xfip = float(stat.get("xFIP", _era) or _era)
            k_pct= float(stat.get("K%", 0) or 0)

            if _g < 1 or _so < 1:
                continue

            k_per_start  = round(_so / max(_g, 1), 3)
            ip_per_start = round(_ip / max(_gs, 1), 3)
            # Per-pitcher std: proportional to average K output (≈28% CV)
            k_std = max(k_per_start * 0.28, 1.0)
            # Tiered line: elite pitchers get a line further below their avg
            # so their genuine edge is reflected in a higher over probability
            if k_per_start >= 8.0:
                line_offset = 1.5   # ace tier
            elif k_per_start >= 6.5:
                line_offset = 1.0   # good tier
            else:
                line_offset = 0.5   # average tier
            prop_line    = max(3.5, round((k_per_start - line_offset) * 2) / 2)
            over_prob    = float(scipy_stats.norm.sf(prop_line, loc=k_per_start, scale=k_std))
            under_prob   = 1.0 - over_prob

            props.append({
                "name":         str(stat.get("Name", name)),
                "team":         team_name or str(stat.get("Team", "")),
                "game":         f"{g['away_team']} @ {g['home_team']}",
                "stat_type":    "strikeouts",
                "line":         prop_line,
                "avg_per_game": round(k_per_start, 2),
                "over_prob":    round(over_prob, 4),
                "under_prob":   round(under_prob, 4),
                "era":          _era,
                "xfip":         xfip,
                "k_pct":        k_pct,
                "whip":         _whip,
                "k9":           k9,
                "ip_per_start": ip_per_start,
                "season_used":  int(stat.get("season", season)),
                "_source":      source,
            })

    return props


def estimate_game_total(home_team: str, away_team: str, team_stats: pd.DataFrame) -> float:
    """
    Estimate expected total runs for a game.

    Blends home offense (runs/game) with away pitching (ERA) and vice versa.
    Returns expected total as a float (e.g. 9.4 runs).
    """
    MLB_GAMES = 162
    MLB_AVG_TOTAL = 9.0

    def _row(team: str):
        mask = team_stats["team"].str.contains(team, case=False, na=False)
        rows = team_stats[mask]
        if rows.empty:
            return None
        return rows.sort_values("season", ascending=False).iloc[0]

    home = _row(home_team)
    away = _row(away_team)
    if home is None or away is None:
        return MLB_AVG_TOTAL

    home_rpg  = float(home.get("runs_scored", 700) or 700) / MLB_GAMES
    away_rpg  = float(away.get("runs_scored", 700) or 700) / MLB_GAMES
    home_era  = float(home.get("era", 4.5) or 4.5)
    away_era  = float(away.get("era", 4.5) or 4.5)

    # Expected runs = blend of own offense and opponent pitching
    home_exp = (home_rpg + away_era) / 2.0
    away_exp  = (away_rpg  + home_era)  / 2.0
    return round(home_exp + away_exp, 2)

