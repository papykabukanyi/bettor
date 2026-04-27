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


def get_batting_stats(
    seasons: list[int] | None = None,
    min_pa: int = 100,
    min_year: int = 2010,
) -> pd.DataFrame:
    """
    Return per-player per-season batting aggregates.

    Columns: player_id, first_name, last_name, team_name, season,
             g, ab, r, h, hr, rbi, sb, bb, so, avg, obp_approx, slg_approx
    """
    bat = _download_csv("Batting.csv")
    ppl = _download_csv("People.csv")
    if bat.empty:
        return pd.DataFrame()

    if seasons:
        bat = bat[bat["yearID"].isin(seasons)]
    else:
        bat = bat[bat["yearID"] >= min_year]

    # Aggregate multiple stints per season (player traded mid-season)
    num_cols = ["AB","R","H","2B","3B","HR","RBI","SB","CS","BB","SO","IBB","HBP","SH","SF","GIDP"]
    bat_agg  = bat.groupby(["playerID","yearID","teamID"])[
        [c for c in num_cols if c in bat.columns]
    ].sum().reset_index()

    bat_agg = bat_agg.rename(columns={
        "yearID": "season", "teamID": "team_id",
        "AB": "ab", "R": "runs", "H": "hits",
        "HR": "hr", "RBI": "rbi", "SB": "sb",
        "BB": "bb", "SO": "so",
    })

    bat_agg["avg"] = (bat_agg["hits"] / bat_agg["ab"].replace(0, float("nan"))).round(3)
    bat_agg["obp_approx"] = (
        (bat_agg["hits"] + bat_agg.get("bb", 0) + bat_agg.get("HBP", 0)) /
        (bat_agg["ab"] + bat_agg.get("bb", 0) + bat_agg.get("HBP", 0) + bat_agg.get("SF", 0))
        .replace(0, float("nan"))
    ).round(3)

    if not ppl.empty:
        bat_agg = bat_agg.merge(
            ppl[["playerID","nameFirst","nameLast"]].rename(
                columns={"nameFirst":"first_name","nameLast":"last_name"}
            ),
            on="playerID", how="left",
        )
    bat_agg["team_name"] = bat_agg["team_id"].map(_LAHMAN_TEAM_MAP).fillna(bat_agg["team_id"])

    bat_agg = bat_agg[bat_agg["ab"] >= min_pa]
    return bat_agg.reset_index(drop=True)


def get_pitching_stats(
    seasons: list[int] | None = None,
    min_ip: int = 20,
    min_year: int = 2010,
) -> pd.DataFrame:
    """
    Return per-player per-season pitching aggregates.

    Columns: player_id, first_name, last_name, team_name, season,
             w, l, g, gs, cg, sho, sv, ipouts, h, er, hr, bb, so, era, whip_approx
    """
    pit = _download_csv("Pitching.csv")
    ppl = _download_csv("People.csv")
    if pit.empty:
        return pd.DataFrame()

    if seasons:
        pit = pit[pit["yearID"].isin(seasons)]
    else:
        pit = pit[pit["yearID"] >= min_year]

    num_cols = ["W","L","G","GS","CG","SHO","SV","IPouts","H","ER","HR","BB","SO","IBB","WP","HBP","BK","BFP","R"]
    pit_agg  = pit.groupby(["playerID","yearID","teamID"])[
        [c for c in num_cols if c in pit.columns]
    ].sum().reset_index()

    pit_agg = pit_agg.rename(columns={
        "yearID": "season", "teamID": "team_id",
        "W": "wins", "L": "losses", "G": "g", "GS": "gs",
        "SV": "sv", "IPouts": "ip_outs", "H": "h_allowed",
        "ER": "er", "HR": "hr_allowed", "BB": "bb", "SO": "so",
    })

    pit_agg["ip"]  = (pit_agg["ip_outs"] / 3).round(1)
    pit_agg["era"] = (pit_agg["er"] * 9 / pit_agg["ip"].replace(0, float("nan"))).round(2)
    pit_agg["whip"] = (
        (pit_agg["h_allowed"] + pit_agg["bb"]) /
        pit_agg["ip"].replace(0, float("nan"))
    ).round(3)
    pit_agg["k_per_9"] = (pit_agg["so"] * 9 / pit_agg["ip"].replace(0, float("nan"))).round(2)

    if not ppl.empty:
        pit_agg = pit_agg.merge(
            ppl[["playerID","nameFirst","nameLast"]].rename(
                columns={"nameFirst":"first_name","nameLast":"last_name"}
            ),
            on="playerID", how="left",
        )
    pit_agg["team_name"] = pit_agg["team_id"].map(_LAHMAN_TEAM_MAP).fillna(pit_agg["team_id"])

    pit_agg = pit_agg[pit_agg["ip"] >= min_ip]
    return pit_agg.reset_index(drop=True)
