"""
Retrosheet / Historical Game Outcomes Fetcher
=============================================
Provides game-by-game win/loss records for MLB teams.

Strategy (two tiers):
  1. MLB Stats API (statsapi) — free, official, covers 2008-present.
     Returns schedule + scores for any date range.
  2. Retrosheet game log CSV (pre-2008 or when offline).
     URL pattern: https://www.retrosheet.org/gamelogs/gl{year}.zip
     Contains fixed-width fields; we parse the columns we need.

For the training pipeline the ML Stats API tier is sufficient and
requires no external downloads.  The Retrosheet tier is a fallback
for deeper historical data (pre-2008 or when building large datasets).
"""

import os
import sys
import csv
import io
import zipfile
import datetime
import time

import requests
import pandas as pd

SRC = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, SRC)

_RETROSHEET_BASE = "https://www.retrosheet.org/gamelogs"
_CACHE_DIR       = os.path.join(os.path.dirname(__file__), "..", "..", "data", "retrosheet")
_TIMEOUT         = 60

# Retrosheet game log column indices (0-based)
# Source: https://www.retrosheet.org/gamelogs/glfields.txt
_COL_DATE     = 0   # YYYYMMDD
_COL_HOME     = 6   # home team code
_COL_VISITOR  = 3   # visiting team code
_COL_HOME_SC  = 9   # home score
_COL_VIS_SC   = 8   # visitor score
_COL_ATTEND   = 17  # attendance

# Map Retrosheet 3-char codes to names (most common franchises)
_RS_TEAM_MAP = {
    "NYA": "New York Yankees",   "BOS": "Boston Red Sox",
    "TOR": "Toronto Blue Jays",  "BAL": "Baltimore Orioles",
    "TBA": "Tampa Bay Rays",     "CHA": "Chicago White Sox",
    "CLE": "Cleveland Guardians","DET": "Detroit Tigers",
    "KCA": "Kansas City Royals", "MIN": "Minnesota Twins",
    "OAK": "Oakland Athletics",  "HOU": "Houston Astros",
    "CAL": "Los Angeles Angels", "ANA": "Los Angeles Angels",
    "LAA": "Los Angeles Angels", "SEA": "Seattle Mariners",
    "TEX": "Texas Rangers",      "NYN": "New York Mets",
    "PHI": "Philadelphia Phillies","FLO":"Miami Marlins",
    "MIA": "Miami Marlins",      "ATL": "Atlanta Braves",
    "WAS": "Washington Nationals","MON":"Montreal Expos",
    "CHN": "Chicago Cubs",       "MIL": "Milwaukee Brewers",
    "SLN": "St. Louis Cardinals","PIT": "Pittsburgh Pirates",
    "CIN": "Cincinnati Reds",    "LAN": "Los Angeles Dodgers",
    "SFN": "San Francisco Giants","SDN": "San Diego Padres",
    "ARI": "Arizona Diamondbacks","COL": "Colorado Rockies",
}


# ─── Tier 1: MLB Stats API (2008-present) ────────────────────────────────────

def get_game_results_statsapi(
    seasons: list[int],
    verbose: bool = True,
) -> pd.DataFrame:
    """
    Fetch final scores for every regular-season game via statsapi.
    Returns DataFrame with: date, home_team, away_team, home_score,
                            away_score, winner, game_pk.
    """
    try:
        import statsapi as mlbapi
    except ImportError:
        print("[retrosheet] statsapi not available")
        return pd.DataFrame()

    rows = []
    for season in seasons:
        if verbose:
            print(f"[retrosheet] Fetching {season} schedule via MLB Stats API…")
        try:
            # Get full regular-season schedule by month to avoid large calls
            for month in range(3, 11):
                start = f"{season}-{month:02d}-01"
                end   = f"{season}-{month:02d}-30"
                sched = mlbapi.schedule(start_date=start, end_date=end,
                                        sportId=1) or []
                for g in sched:
                    status = (g.get("status") or "").lower()
                    if "final" not in status and "completed" not in status:
                        continue
                    hs = g.get("home_score")
                    vs = g.get("away_score")
                    if hs is None or vs is None:
                        continue
                    ht = g.get("home_name", "")
                    at = g.get("away_name", "")
                    rows.append({
                        "date":       g.get("game_date", start),
                        "season":     season,
                        "home_team":  ht,
                        "away_team":  at,
                        "home_score": int(hs),
                        "away_score": int(vs),
                        "winner":     ht if int(hs) > int(vs) else at,
                        "total_runs": int(hs) + int(vs),
                        "game_pk":    g.get("game_id"),
                    })
                time.sleep(0.15)  # polite rate limit
        except Exception as e:
            print(f"[retrosheet] statsapi error for {season}: {e}")

    df = pd.DataFrame(rows)
    if verbose:
        print(f"[retrosheet] Loaded {len(df)} games via MLB Stats API")
    return df


# ─── Tier 2: Retrosheet game log CSVs (historical fallback) ──────────────────

def _download_retrosheet_year(year: int) -> pd.DataFrame:
    """
    Download and parse the Retrosheet game log ZIP for one year.
    Returns DataFrame with same schema as get_game_results_statsapi.
    """
    os.makedirs(_CACHE_DIR, exist_ok=True)
    cache_csv = os.path.join(_CACHE_DIR, f"gl{year}.csv")

    # Use cached CSV if present
    if os.path.exists(cache_csv):
        try:
            return pd.read_csv(cache_csv, low_memory=False)
        except Exception:
            pass

    url = f"{_RETROSHEET_BASE}/gl{year}.zip"
    print(f"[retrosheet] Downloading {url}")
    try:
        r = requests.get(url, timeout=_TIMEOUT)
        r.raise_for_status()
    except Exception as e:
        print(f"[retrosheet] download error for {year}: {e}")
        return pd.DataFrame()

    rows = []
    try:
        with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
            gl_name = next((n for n in zf.namelist() if n.startswith("GL")), None)
            if not gl_name:
                return pd.DataFrame()
            with zf.open(gl_name) as f:
                reader = csv.reader(io.TextIOWrapper(f, encoding="latin-1"))
                for line in reader:
                    if len(line) < 10:
                        continue
                    try:
                        date_raw  = line[_COL_DATE]
                        home_code = line[_COL_HOME].strip()
                        vis_code  = line[_COL_VISITOR].strip()
                        home_sc   = int(line[_COL_HOME_SC])
                        vis_sc    = int(line[_COL_VIS_SC])
                        date_fmt  = f"{date_raw[:4]}-{date_raw[4:6]}-{date_raw[6:8]}"
                        home_name = _RS_TEAM_MAP.get(home_code, home_code)
                        vis_name  = _RS_TEAM_MAP.get(vis_code, vis_code)
                        rows.append({
                            "date":       date_fmt,
                            "season":     int(date_raw[:4]),
                            "home_team":  home_name,
                            "away_team":  vis_name,
                            "home_score": home_sc,
                            "away_score": vis_sc,
                            "winner":     home_name if home_sc > vis_sc else vis_name,
                            "total_runs": home_sc + vis_sc,
                            "game_pk":    None,
                        })
                    except (ValueError, IndexError):
                        pass
    except Exception as e:
        print(f"[retrosheet] parse error for {year}: {e}")
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    if not df.empty:
        df.to_csv(cache_csv, index=False)
        print(f"[retrosheet] {year}: {len(df)} games parsed and cached")
    return df


def get_game_results(
    seasons: list[int],
    use_statsapi: bool = True,
    verbose: bool = True,
) -> pd.DataFrame:
    """
    Return game-by-game outcomes for the requested seasons.
    Uses MLB Stats API for recent seasons (≥ 2008), Retrosheet for older.
    """
    recent_seasons  = [s for s in seasons if s >= 2008]
    historic_seasons = [s for s in seasons if s < 2008]

    dfs = []

    if recent_seasons and use_statsapi:
        dfs.append(get_game_results_statsapi(recent_seasons, verbose=verbose))

    for yr in historic_seasons:
        dfs.append(_download_retrosheet_year(yr))

    if dfs:
        combined = pd.concat([d for d in dfs if not d.empty], ignore_index=True)
        if verbose:
            print(f"[retrosheet] Total games loaded: {len(combined)}")
        return combined
    return pd.DataFrame()


def build_team_win_pct_by_season(seasons: list[int]) -> pd.DataFrame:
    """
    Convenience: return per-team per-season win percentage from game results.
    Used as a calibration ground-truth for the model.
    """
    df = get_game_results(seasons)
    if df.empty:
        return pd.DataFrame()

    records = []
    for (season, team), grp in df.groupby(["season", "home_team"]):
        home_w = (grp["home_score"] > grp["away_score"]).sum()
        home_g = len(grp)
        records.append((season, team, home_w, home_g))
    for (season, team), grp in df.groupby(["season", "away_team"]):
        away_w = (grp["away_score"] > grp["home_score"]).sum()
        away_g = len(grp)
        records.append((season, team, away_w, away_g))

    agg = (pd.DataFrame(records, columns=["season","team","wins","games"])
             .groupby(["season","team"])
             .sum()
             .reset_index())
    agg["win_pct"] = (agg["wins"] / agg["games"].replace(0, float("nan"))).round(3)
    return agg
