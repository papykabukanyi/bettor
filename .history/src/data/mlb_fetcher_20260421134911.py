"""
MLB Data Fetcher
================
Data sources (all free):
  - pybaseball   : scrapes FanGraphs, Baseball Reference (game logs, batting, pitching)
  - mlbstatsapi  : official MLB Stats API (no key required)

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
    """Return team-level batting stats for a season from FanGraphs."""
    if not PYBASEBALL_OK:
        return pd.DataFrame()
    try:
        df = pb.team_batting(season)
        df["season"] = season
        return df
    except Exception as e:
        print(f"[mlb_fetcher] team_batting error: {e}")
        return pd.DataFrame()


def get_team_pitching_stats(season: int) -> pd.DataFrame:
    """Return team-level pitching stats for a season from FanGraphs."""
    if not PYBASEBALL_OK:
        return pd.DataFrame()
    try:
        df = pb.team_pitching(season)
        df["season"] = season
        return df
    except Exception as e:
        print(f"[mlb_fetcher] team_pitching error: {e}")
        return pd.DataFrame()


def get_pitcher_stats(season: int, min_ip: float = 20.0) -> pd.DataFrame:
    """Return individual pitcher stats for a season (min_ip innings pitched filter)."""
    if not PYBASEBALL_OK:
        return pd.DataFrame()
    try:
        df = pb.pitching_stats(season, season, qual=min_ip)
        df["season"] = season
        keep = ["Name", "Team", "W", "L", "ERA", "WHIP", "K/9", "BB/9",
                "HR/9", "FIP", "xFIP", "BABIP", "LOB%", "GB%", "IP", "season"]
        return df[[c for c in keep if c in df.columns]]
    except Exception as e:
        print(f"[mlb_fetcher] pitching_stats error: {e}")
        return pd.DataFrame()


def get_batter_stats(season: int, min_pa: int = 100) -> pd.DataFrame:
    """Return individual batter stats for a season."""
    if not PYBASEBALL_OK:
        return pd.DataFrame()
    try:
        df = pb.batting_stats(season, season, qual=min_pa)
        df["season"] = season
        keep = ["Name", "Team", "PA", "AB", "H", "HR", "RBI", "BB", "SO",
                "AVG", "OBP", "SLG", "OPS", "wRC+", "wOBA", "BABIP",
                "ISO", "K%", "BB%", "Hard%", "season"]
        return df[[c for c in keep if c in df.columns]]
    except Exception as e:
        print(f"[mlb_fetcher] batting_stats error: {e}")
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
            home = game.get("teams", {}).get("home", {}).get("team", {}).get("name", "")
            away = game.get("teams", {}).get("away", {}).get("team", {}).get("name", "")
            home_sp_obj = game.get("teams", {}).get("home", {}).get("probablePitcher", {})
            away_sp_obj = game.get("teams", {}).get("away", {}).get("probablePitcher", {})
            home_sp = home_sp_obj.get("fullName", "TBD") if home_sp_obj else "TBD"
            away_sp = away_sp_obj.get("fullName", "TBD") if away_sp_obj else "TBD"
            games.append({
                "game_pk":      game.get("gamePk"),
                "date":         today,
                "home_team":    home,
                "away_team":    away,
                "home_starter": home_sp,
                "away_starter": away_sp,
                "status":       game.get("status", {}).get("detailedState", ""),
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

def get_player_prop_stats(player_name: str, season: int) -> dict:
    """
    Return per-game averages useful for player prop bets:
      - H/game, HR/game, RBI/game (hitters)
      - K/game, IP/game, ER/game (pitchers)
    """
    result = {}

    # Hitter lookup
    if PYBASEBALL_OK:
        try:
            batters = pb.batting_stats(season, season, qual=10)
            row = batters[batters["Name"].str.contains(player_name, case=False, na=False)]
            if not row.empty:
                row = row.iloc[0]
                games = row.get("G", 1) or 1
                result["type"] = "hitter"
                result["name"] = row["Name"]
                result["team"] = row.get("Team", "")
                result["season"] = season
                result["H_per_game"] = round(row.get("H", 0) / games, 3)
                result["HR_per_game"] = round(row.get("HR", 0) / games, 3)
                result["RBI_per_game"] = round(row.get("RBI", 0) / games, 3)
                result["AVG"] = row.get("AVG", 0)
                result["OBP"] = row.get("OBP", 0)
                result["SLG"] = row.get("SLG", 0)
                result["K_pct"] = row.get("K%", 0)
                return result
        except Exception as e:
            print(f"[mlb_fetcher] prop lookup batting error: {e}")

    # Pitcher lookup
    if PYBASEBALL_OK:
        try:
            pitchers = pb.pitching_stats(season, season, qual=5)
            row = pitchers[pitchers["Name"].str.contains(player_name, case=False, na=False)]
            if not row.empty:
                row = row.iloc[0]
                games = row.get("G", 1) or 1
                result["type"] = "pitcher"
                result["name"] = row["Name"]
                result["team"] = row.get("Team", "")
                result["season"] = season
                result["K_per_game"] = round(row.get("SO", row.get("K", 0)) / games, 3)
                result["IP_per_start"] = round(row.get("IP", 0) / max(row.get("GS", 1), 1), 3)
                result["ERA"] = row.get("ERA", 0)
                result["WHIP"] = row.get("WHIP", 0)
                result["K9"] = row.get("K/9", 0)
                return result
        except Exception as e:
            print(f"[mlb_fetcher] prop lookup pitching error: {e}")

    return result
