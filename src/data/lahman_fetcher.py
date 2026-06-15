"""
Lahman Database — Player Career History Fetcher
================================================
Downloads Lahman Baseball Database CSVs from the official Chadwick Bureau
GitHub mirror (no API key required, MIT-licensed data).

Files used:
  Teams.csv   — franchise-season records (W, L, R, RA, ERA, ...)
  Batting.csv — player-season batting lines
  Pitching.csv — player-season pitching lines
  People.csv  — player bio + name lookup

These provide rich multi-decade historical context that improves the
model's team-level feature vectors.

Download location (git raw):
  https://github.com/chadwickbureau/baseballdatabank/raw/master/core/<file>
"""

import os
import sys
import io
import datetime

import requests
import pandas as pd

SRC = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, SRC)

_LAHMAN_BASE = (
    "https://github.com/chadwickbureau/baseballdatabank/raw/master/core"
)
_CACHE_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "lahman")
_TIMEOUT   = 30

# Maps Lahman team abbreviations to common full names
_LAHMAN_TEAM_MAP = {
    "NYA": "New York Yankees",  "BOS": "Boston Red Sox",
    "TOR": "Toronto Blue Jays", "BAL": "Baltimore Orioles",
    "TBA": "Tampa Bay Rays",    "CHA": "Chicago White Sox",
    "CLE": "Cleveland Guardians","DET": "Detroit Tigers",
    "KCA": "Kansas City Royals","MIN": "Minnesota Twins",
    "OAK": "Oakland Athletics", "HOU": "Houston Astros",
    "ANA": "Los Angeles Angels","LAA": "Los Angeles Angels",
    "SEA": "Seattle Mariners",  "TEX": "Texas Rangers",
    "NYN": "New York Mets",     "PHI": "Philadelphia Phillies",
    "MIA": "Miami Marlins",     "FLO": "Miami Marlins",
    "ATL": "Atlanta Braves",    "WAS": "Washington Nationals",
    "MON": "Montreal Expos",    "CHN": "Chicago Cubs",
    "MIL": "Milwaukee Brewers", "SLN": "St. Louis Cardinals",
    "PIT": "Pittsburgh Pirates","CIN": "Cincinnati Reds",
    "LAN": "Los Angeles Dodgers","SFN": "San Francisco Giants",
    "SDN": "San Diego Padres",  "ARI": "Arizona Diamondbacks",
    "COL": "Colorado Rockies",
}


def _download_csv(filename: str) -> pd.DataFrame:
    """
    Download a Lahman CSV from GitHub or return cached copy.
    Caches to data/lahman/<filename>.
    """
    os.makedirs(_CACHE_DIR, exist_ok=True)
    cache_path = os.path.join(_CACHE_DIR, filename)

    # Use cached file if < 7 days old
    if os.path.exists(cache_path):
        age = datetime.datetime.now().timestamp() - os.path.getmtime(cache_path)
        if age < 7 * 86400:
            try:
                return pd.read_csv(cache_path, low_memory=False)
            except Exception:
                pass

    url = f"{_LAHMAN_BASE}/{filename}"
    print(f"[lahman] Downloading {url}")
    try:
        r = requests.get(url, timeout=_TIMEOUT)
        r.raise_for_status()
        df = pd.read_csv(io.StringIO(r.text), low_memory=False)
        df.to_csv(cache_path, index=False)
        return df
    except Exception as e:
        print(f"[lahman] Download error for {filename}: {e}")
        if os.path.exists(cache_path):
            return pd.read_csv(cache_path, low_memory=False)
        return pd.DataFrame()


def get_team_records(
    seasons: list[int] | None = None,
    min_year: int = 2000,
) -> pd.DataFrame:
    """
    Return a DataFrame with franchise-season records + run totals.

    Columns: team_name, season, wins, losses, runs_scored, runs_allowed,
             ba (batting avg proxy), era_approx, win_pct
    """
    df = _download_csv("Teams.csv")
    if df.empty:
        return pd.DataFrame()

    if seasons:
        df = df[df["yearID"].isin(seasons)]
    else:
        df = df[df["yearID"] >= min_year]

    df = df.rename(columns={
        "yearID": "season",
        "teamID": "team_id",
        "W":      "wins",
        "L":      "losses",
        "R":      "runs_scored",
        "RA":     "runs_allowed",
        "H":      "hits",
        "AB":     "at_bats",
        "HR":     "home_runs",
        "ERA":    "era",
    })

    df["team_name"] = df["team_id"].map(_LAHMAN_TEAM_MAP).fillna(df.get("name", df["team_id"]))
    df["win_pct"]   = (df["wins"] / (df["wins"] + df["losses"])).round(3)
    df["bat_avg"]   = (df["hits"] / df["at_bats"].replace(0, float("nan"))).round(3)

    keep = ["season", "team_id", "team_name", "wins", "losses", "win_pct",
            "runs_scored", "runs_allowed", "home_runs", "bat_avg", "era"]
    return df[[c for c in keep if c in df.columns]].reset_index(drop=True)


