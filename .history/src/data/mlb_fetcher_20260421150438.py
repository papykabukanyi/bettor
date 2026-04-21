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


def get_schedule_today() -> list[dict]:
    """
    Return today's MLB schedule with probable starters via official MLB Stats API.
    Each item: {game_pk, date, home_team, away_team, home_starter, away_starter}
    """
    if not MLB_API_OK:
        return []
    try:
        today = datetime.date.today().strftime("%Y-%m-%d")
        schedule = mlbstatsapi.schedule(start_date=today, end_date=today)
        games = []
        for game in schedule:
            games.append({
                "game_pk":      game.get("game_id"),
                "date":         game.get("game_date", today),
                "home_team":    game.get("home_name", ""),
                "away_team":    game.get("away_name", ""),
                "home_starter": game.get("home_probable_pitcher", "TBD") or "TBD",
                "away_starter": game.get("away_probable_pitcher", "TBD") or "TBD",
                "status":       game.get("status", ""),
            })
        return games
    except Exception as e:
        print(f"[mlb_fetcher] schedule error: {e}")
        return []


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
    current_year = datetime.date.today().year
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


def get_starters_props_batch(games: list[dict], season: int) -> list[dict]:
    """
    Auto-fetch pitcher prop stats for all of today's probable starters.

    Loads Baseball Reference once for the entire batch (not once per player),
    then looks up each starter in the cached DataFrame.

    Returns list of prop dicts ready for display in the daily report.
    """
    from scipy import stats as scipy_stats
    import datetime

    if not PYBASEBALL_OK:
        return []

    current_year = datetime.date.today().year
    fallback_seasons = sorted({season, current_year - 1, current_year - 2}, reverse=True)

    # Load pitcher table once for all lookups
    pitchers_df, used_season = _load_bref_season(pb.pitching_stats_bref, fallback_seasons)
    if pitchers_df.empty or "Name" not in pitchers_df.columns:
        print("[mlb_fetcher] Pitcher stats unavailable — Baseball Reference may not have current data yet.")
        return []

    print(f"[mlb_fetcher] Pitcher stats loaded from {used_season} season.")

    props  = []
    seen:  set[str] = set()
    K_STD = 2.5  # typical standard deviation of Ks per start

    for g in games:
        for role_key in ("home_starter", "away_starter"):
            name = g.get(role_key, "TBD")
            if not name or name in ("TBD", "") or name in seen:
                continue
            seen.add(name)

            # Fuzzy name match
            mask = pitchers_df["Name"].str.contains(name, case=False, na=False)
            row_df = pitchers_df[mask]
            if row_df.empty:
                continue
            row = row_df.iloc[0]

            games_p = float(row.get("G", 1) or 1)
            gs      = float(row.get("GS", 1) or 1)
            ip      = float(row.get("IP", 1) or 1)
            so      = float(row.get("SO", 0) or 0)

            if games_p == 0 or so == 0:
                continue

            k_per_start = round(so / max(games_p, 1), 3)
            ip_per_start = round(ip / max(gs, 1), 3)
            k9 = round(so / ip * 9, 2) if ip > 0 else 0.0

            # Prop line = nearest 0.5 below the average (slight under-set)
            prop_line  = max(3.5, round(k_per_start * 2) / 2 - 0.5)
            over_prob  = float(scipy_stats.norm.sf(prop_line, loc=k_per_start, scale=K_STD))
            under_prob = 1.0 - over_prob

            props.append({
                "name":         str(row["Name"]),
                "team":         str(row.get("Team", "")),
                "game":         f"{g['away_team']} @ {g['home_team']}",
                "stat_type":    "strikeouts",
                "line":         prop_line,
                "avg_per_game": round(k_per_start, 2),
                "over_prob":    round(over_prob, 4),
                "under_prob":   round(under_prob, 4),
                "era":          float(row.get("ERA",  0) or 0),
                "whip":         float(row.get("WHIP", 0) or 0),
                "k9":           k9,
                "ip_per_start": ip_per_start,
                "season_used":  used_season,
            })

    return props
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


def get_starters_props_batch(games: list[dict], season: int) -> list[dict]:
    """
    Auto-fetch pitcher prop stats for all of today's probable starters.

    For each starter found in Baseball Reference, computes K/start average
    and picks a sensible prop line (rounded to nearest 0.5).

    Returns list of prop dicts ready for display in the daily report.
    """
    from scipy import stats as scipy_stats

    props = []
    seen: set[str] = set()
    K_STD = 2.5  # typical standard deviation of Ks per start

    for g in games:
        for role_key in ("home_starter", "away_starter"):
            name = g.get(role_key, "TBD")
            if not name or name in ("TBD", "") or name in seen:
                continue
            seen.add(name)

            stat = get_player_prop_stats(name, season)
            if not stat or stat.get("type") != "pitcher":
                continue

            k_per_start = stat.get("K_per_game", 0)
            if not k_per_start:
                continue

            # Pick the standard prop line closest to the pitcher's average
            # (rounded down to nearest 0.5 to favour "over" scenarios)
            prop_line = max(3.5, round(k_per_start * 2) / 2 - 0.5)

            over_prob  = float(scipy_stats.norm.sf(prop_line, loc=k_per_start, scale=K_STD))
            under_prob = 1.0 - over_prob

            props.append({
                "name":         stat["name"],
                "team":         stat.get("team", ""),
                "game":         f"{g['away_team']} @ {g['home_team']}",
                "stat_type":    "strikeouts",
                "line":         prop_line,
                "avg_per_game": round(k_per_start, 2),
                "over_prob":    round(over_prob, 4),
                "under_prob":   round(under_prob, 4),
                "era":          stat.get("ERA", 0),
                "whip":         stat.get("WHIP", 0),
                "k9":           stat.get("K9", 0),
                "ip_per_start": stat.get("IP_per_start", 0),
            })

    return props
