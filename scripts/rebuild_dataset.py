#!/usr/bin/env python3
"""
rebuild_dataset.py — Full 2-season multi-sport dataset rebuild for HF Hub.

This script fetches 2 full seasons of game data (Baseball/Soccer/Cricket) from
all major leagues, deduplicates locally before any upload, then pushes clean
parquet shards to papylove/sportprediction on HF Hub.

Run this locally (not on Render) to avoid memory/timeout limits.

Usage:
    cd bettor
    pip install -r requirements.txt
    python scripts/rebuild_dataset.py               # full rebuild (all sports)
    python scripts/rebuild_dataset.py --dry-run     # validate data, no HF writes
    python scripts/rebuild_dataset.py --sport mlb   # MLB only
    python scripts/rebuild_dataset.py --sport soccer
    python scripts/rebuild_dataset.py --sport cricket
    python scripts/rebuild_dataset.py --year 2025   # single year

Required env vars:
    HF_API_KEY            - Hugging Face write token
    HF_DATASET_REPO       - e.g. papylove/sportprediction
    FOOTBALL_DATA_API_KEY - football-data.org free key (optional, improves soccer coverage)
    CRICKET_RAPIDAPI_KEY  - RapidAPI cricket key (optional)
"""

from __future__ import annotations

import argparse
import datetime
import json
import logging
import os
import sys
import time
import zipfile
from pathlib import Path
from typing import Iterator
from uuid import uuid4

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("rebuild_dataset")

# ─── Project path setup ────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
SRC_DIR = REPO_ROOT / "src"
DATA_DIR = REPO_ROOT / "data"
sys.path.insert(0, str(SRC_DIR))

DEDUP_INDEX_PATH = DATA_DIR / "rebuild_pushed_keys.json"
BATCH_SIZE = 500
SLEEP_BETWEEN_BATCHES = 1.5  # seconds — avoids rate limiting

# ─── Seasons to fetch ──────────────────────────────────────────────────────────
MLB_SEASONS = [2024, 2025]
SOCCER_SEASONS = ["2023-24", "2024-25", "2025-26"]
CRICKET_SEASONS = [2024, 2025]

# ─── Soccer leagues via ESPN (free, no key) ────────────────────────────────────
ESPN_SOCCER_LEAGUES = [
    ("eng.1",   "Premier League",      "soccer"),
    ("esp.1",   "La Liga",             "soccer"),
    ("ger.1",   "Bundesliga",          "soccer"),
    ("ita.1",   "Serie A",             "soccer"),
    ("fra.1",   "Ligue 1",             "soccer"),
    ("usa.1",   "MLS",                 "soccer"),
    ("uefa.champions", "Champions League", "soccer"),
    ("concacaf.leagues.cup", "Leagues Cup", "soccer"),
    ("fifa.worldq.concacaf", "WC Qualifiers CONCACAF", "soccer"),
]

# ─── Football-data.org league codes (free tier) ────────────────────────────────
FOOTBALL_DATA_LEAGUES = {
    "PL":  ("Premier League",      "soccer"),
    "PD":  ("La Liga",             "soccer"),
    "BL1": ("Bundesliga",          "soccer"),
    "SA":  ("Serie A",             "soccer"),
    "FL1": ("Ligue 1",             "soccer"),
    "MLS": ("MLS",                 "soccer"),
    "CL":  ("Champions League",    "soccer"),
}

# ─── Cricket leagues via Cricsheet (free archive) ──────────────────────────────
CRICSHEET_FORMATS = [
    ("t20s",  "T20", "cricket"),
    ("odis",  "ODI", "cricket"),
    ("tests", "Test", "cricket"),
]

SESSION = requests.Session()
SESSION.headers["User-Agent"] = "bettor-rebuild/1.0"


# ──────────────────────────────────────────────────────────────────────────────
# Local deduplication index
# ──────────────────────────────────────────────────────────────────────────────

def load_dedup_index() -> set:
    try:
        if DEDUP_INDEX_PATH.exists():
            with open(DEDUP_INDEX_PATH, "r") as f:
                return set(json.load(f))
    except Exception as exc:
        logger.warning("dedup: failed to load index: %s", exc)
    return set()


def save_dedup_index(keys: set) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    try:
        with open(DEDUP_INDEX_PATH, "w") as f:
            json.dump(sorted(keys), f)
    except Exception as exc:
        logger.warning("dedup: failed to save index: %s", exc)


def record_dedupe_key(r: dict) -> str:
    gid = str(r.get("game_id") or "").strip().lower()
    if gid:
        return gid
    sport = str(r.get("sport") or "").lower().strip()
    date = str(r.get("game_date") or "")[:10]
    home = str(r.get("home_team") or "").lower().strip()
    away = str(r.get("away_team") or "").lower().strip()
    return f"{sport}|{date}|{home}|{away}"


def dedup_records(records: list[dict], seen_keys: set) -> tuple[list[dict], int]:
    """Return (unique_records, duplicate_count)."""
    out, dups = [], 0
    for r in records:
        k = record_dedupe_key(r)
        if k in seen_keys:
            dups += 1
        else:
            seen_keys.add(k)
            out.append(r)
    return out, dups


# ──────────────────────────────────────────────────────────────────────────────
# Generic game record builder
# ──────────────────────────────────────────────────────────────────────────────

def make_record(
    game_id: str,
    sport: str,
    league: str,
    game_date: str,
    home_team: str,
    away_team: str,
    home_score: float | None,
    away_score: float | None,
    season: int,
    status: str = "Final",
    metadata: dict | None = None,
) -> dict:
    return {
        "game_id":       game_id or str(uuid4()),
        "record_id":     str(uuid4()),
        "sport":         sport,
        "league":        league,
        "game_date":     game_date,
        "game_datetime": game_date,
        "status":        status,
        "home_team":     home_team,
        "away_team":     away_team,
        "home_score":    float(home_score) if home_score is not None else None,
        "away_score":    float(away_score) if away_score is not None else None,
        "home_starter":  "",
        "away_starter":  "",
        "season":        int(season),
        "metadata":      json.dumps(metadata or {}),
        "created_at":    datetime.datetime.utcnow().isoformat() + "Z",
    }


# ──────────────────────────────────────────────────────────────────────────────
# MLB via statsapi.mlb.com (free, no key)
# ──────────────────────────────────────────────────────────────────────────────

def _iter_mlb_days(season: int) -> Iterator[str]:
    start = datetime.date(season, 4, 1)
    end = datetime.date(season, 11, 1)
    if end > datetime.date.today():
        end = datetime.date.today()
    cur = start
    while cur <= end:
        yield cur.isoformat()
        cur += datetime.timedelta(days=1)


def fetch_mlb_season(season: int) -> list[dict]:
    logger.info("[MLB] Fetching season %d", season)
    records = []
    days = list(_iter_mlb_days(season))
    for i, day in enumerate(days):
        if i % 30 == 0:
            logger.info("[MLB] %s progress: day %d/%d", season, i + 1, len(days))
        try:
            resp = SESSION.get(
                "https://statsapi.mlb.com/api/v1/schedule",
                params={"sportId": 1, "date": day, "hydrate": "linescore", "gameType": "R"},
                timeout=20,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            logger.debug("[MLB] %s fetch error: %s", day, exc)
            continue
        for date_entry in data.get("dates", []):
            for g in date_entry.get("games", []):
                try:
                    status = str(g.get("status", {}).get("abstractGameState", "")).lower()
                    if status not in ("final", "completed early"):
                        continue
                    teams = g.get("teams", {})
                    home = teams.get("home", {})
                    away = teams.get("away", {})
                    home_name = str(home.get("team", {}).get("name", "")).strip()
                    away_name = str(away.get("team", {}).get("name", "")).strip()
                    if not home_name or not away_name:
                        continue
                    ls = g.get("linescore", {}) or {}
                    home_score = ls.get("teams", {}).get("home", {}).get("runs")
                    away_score = ls.get("teams", {}).get("away", {}).get("runs")
                    if home_score is None:
                        home_score = home.get("score")
                    if away_score is None:
                        away_score = away.get("score")
                    if home_score is None or away_score is None:
                        continue
                    game_id = f"mlb_{g.get('gamePk', '')}_{day}"
                    records.append(make_record(
                        game_id=game_id,
                        sport="mlb",
                        league="MLB",
                        game_date=day,
                        home_team=home_name,
                        away_team=away_name,
                        home_score=float(home_score),
                        away_score=float(away_score),
                        season=season,
                        status="Final",
                        metadata={"gamePk": g.get("gamePk"), "venue": g.get("venue", {}).get("name")},
                    ))
                except Exception as exc:
                    logger.debug("[MLB] record parse error: %s", exc)
        time.sleep(0.05)
    logger.info("[MLB] Season %d: %d records", season, len(records))
    return records


# ──────────────────────────────────────────────────────────────────────────────
# Soccer via ESPN API (free, no key)
# ──────────────────────────────────────────────────────────────────────────────

def fetch_espn_soccer_league_season(league_id: str, league_name: str, year: int) -> list[dict]:
    """Fetch completed games for one league/year from ESPN scoreboard API."""
    logger.info("[Soccer/ESPN] Fetching %s %d", league_name, year)
    records = []

    # ESPN season year is typically the END year (e.g., 2025 = 2024-25 season)
    base_url = f"https://site.api.espn.com/apis/site/v2/sports/soccer/{league_id}/scoreboard"
    for month in range(1, 13):
        for week_offset in range(0, 5):
            day = datetime.date(year, month, 1) + datetime.timedelta(weeks=week_offset)
            if day.year != year and month == 12:
                break
            try:
                resp = SESSION.get(
                    base_url,
                    params={"dates": day.strftime("%Y%m%d"), "limit": 50},
                    timeout=20,
                )
                resp.raise_for_status()
                data = resp.json()
            except Exception:
                continue
            for event in data.get("events", []):
                try:
                    comps = event.get("competitions", [{}])
                    if not comps:
                        continue
                    comp = comps[0]
                    if str(comp.get("status", {}).get("type", {}).get("completed", False)).lower() != "true":
                        continue
                    competitors = {c["homeAway"]: c for c in comp.get("competitors", [])}
                    home = competitors.get("home", {})
                    away = competitors.get("away", {})
                    home_name = home.get("team", {}).get("displayName", "").strip()
                    away_name = away.get("team", {}).get("displayName", "").strip()
                    if not home_name or not away_name:
                        continue
                    game_date = str(event.get("date", ""))[:10]
                    game_id = f"soccer_espn_{league_id}_{event.get('id', '')}_{game_date}"
                    records.append(make_record(
                        game_id=game_id,
                        sport="soccer",
                        league=league_name,
                        game_date=game_date,
                        home_team=home_name,
                        away_team=away_name,
                        home_score=float(home.get("score", 0)),
                        away_score=float(away.get("score", 0)),
                        season=year,
                        status="Final",
                        metadata={"espn_id": event.get("id"), "league_id": league_id},
                    ))
                except Exception as exc:
                    logger.debug("[Soccer/ESPN] parse error: %s", exc)
            time.sleep(0.1)

    logger.info("[Soccer/ESPN] %s %d: %d records", league_name, year, len(records))
    return records


def fetch_football_data_league_season(league_code: str, league_name: str, season: int, api_key: str) -> list[dict]:
    """Fetch via football-data.org free tier (10 req/min, seasonal)."""
    logger.info("[Soccer/FD] Fetching %s %d", league_name, season)
    records = []
    url = f"https://api.football-data.org/v4/competitions/{league_code}/matches"
    headers = {"X-Auth-Token": api_key}
    params = {"season": str(season), "status": "FINISHED"}
    try:
        resp = SESSION.get(url, headers=headers, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        logger.warning("[Soccer/FD] %s %d fetch error: %s", league_name, season, exc)
        return records

    for m in data.get("matches", []):
        try:
            score = m.get("score", {})
            full = score.get("fullTime", {})
            home_score = full.get("home")
            away_score = full.get("away")
            if home_score is None or away_score is None:
                continue
            home_name = str(m.get("homeTeam", {}).get("name", "")).strip()
            away_name = str(m.get("awayTeam", {}).get("name", "")).strip()
            if not home_name or not away_name:
                continue
            game_date = str(m.get("utcDate", ""))[:10]
            game_id = f"soccer_fd_{league_code}_{m.get('id', '')}_{game_date}"
            records.append(make_record(
                game_id=game_id,
                sport="soccer",
                league=league_name,
                game_date=game_date,
                home_team=home_name,
                away_team=away_name,
                home_score=float(home_score),
                away_score=float(away_score),
                season=season,
                status="Final",
                metadata={"fd_match_id": m.get("id"), "stage": m.get("stage")},
            ))
        except Exception as exc:
            logger.debug("[Soccer/FD] parse error: %s", exc)

    logger.info("[Soccer/FD] %s %d: %d records", league_name, season, len(records))
    time.sleep(7)  # respect 10 req/min free tier limit
    return records


# ──────────────────────────────────────────────────────────────────────────────
# Cricket via Cricsheet.org (free ZIP archive — no API key needed)
# ──────────────────────────────────────────────────────────────────────────────

def fetch_cricsheet_format(fmt: str, league_label: str, min_year: int = 2023) -> list[dict]:
    """Download and parse a Cricsheet ZIP archive (JSON format)."""
    url = f"https://cricsheet.org/downloads/{fmt}_json.zip"
    logger.info("[Cricket/Cricsheet] Downloading %s archive from %s", fmt, url)

    cache_path = DATA_DIR / f"cricsheet_{fmt}.zip"
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    try:
        if not cache_path.exists():
            resp = SESSION.get(url, timeout=120, stream=True)
            resp.raise_for_status()
            with open(cache_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=65536):
                    f.write(chunk)
            logger.info("[Cricket/Cricsheet] Downloaded %s (%.1f MB)", fmt, cache_path.stat().st_size / 1e6)
        else:
            logger.info("[Cricket/Cricsheet] Using cached %s (%.1f MB)", fmt, cache_path.stat().st_size / 1e6)
    except Exception as exc:
        logger.error("[Cricket/Cricsheet] Download failed for %s: %s", fmt, exc)
        return []

    records = []
    try:
        with zipfile.ZipFile(cache_path, "r") as zf:
            json_files = [n for n in zf.namelist() if n.endswith(".json")]
            logger.info("[Cricket/Cricsheet] %s: %d match files in archive", fmt, len(json_files))
            for i, name in enumerate(json_files):
                if i % 500 == 0:
                    logger.info("[Cricket/Cricsheet] %s: processing %d/%d", fmt, i, len(json_files))
                try:
                    with zf.open(name) as mf:
                        match = json.load(mf)
                    info = match.get("info", {})
                    dates = info.get("dates", [])
                    if not dates:
                        continue
                    game_date = str(dates[0])
                    if int(game_date[:4]) < min_year:
                        continue
                    season = int(game_date[:4])
                    teams = info.get("teams", [])
                    if len(teams) < 2:
                        continue
                    outcome = info.get("outcome", {})
                    winner = outcome.get("winner", "")
                    method = outcome.get("method", "")

                    # Determine scores from innings
                    innings_data = match.get("innings", [])
                    scores: dict[str, int] = {}
                    for inn in innings_data:
                        team_name = inn.get("team", "")
                        overs = inn.get("overs", [])
                        total_runs = sum(
                            b.get("runs", {}).get("total", 0)
                            for ov in overs for b in ov.get("deliveries", [])
                        )
                        scores[team_name] = scores.get(team_name, 0) + total_runs

                    home_team = teams[0]
                    away_team = teams[1]
                    home_score = float(scores.get(home_team, 0))
                    away_score = float(scores.get(away_team, 0))

                    # Resolve outcome to score differential if winner known
                    if winner and method not in ("D/L",):
                        if home_score == 0 and away_score == 0:
                            if winner == home_team:
                                home_score, away_score = 1.0, 0.0
                            elif winner == away_team:
                                home_score, away_score = 0.0, 1.0

                    competition = str(info.get("competition") or info.get("event", {}).get("name") or fmt.upper())
                    gid = str(info.get("match_type_number") or "")
                    game_id = f"cricket_{fmt}_{game_date}_{home_team[:10]}_{away_team[:10]}_{gid}"[:64]
                    records.append(make_record(
                        game_id=game_id,
                        sport="cricket",
                        league=f"{competition} ({league_label})",
                        game_date=game_date,
                        home_team=home_team,
                        away_team=away_team,
                        home_score=home_score,
                        away_score=away_score,
                        season=season,
                        status="Final",
                        metadata={
                            "format": fmt,
                            "winner": winner,
                            "method": method,
                            "competition": competition,
                            "gender": info.get("gender"),
                            "match_type": info.get("match_type"),
                        },
                    ))
                except Exception as exc:
                    logger.debug("[Cricket/Cricsheet] parse error %s: %s", name, exc)
    except Exception as exc:
        logger.error("[Cricket/Cricsheet] ZIP read failed for %s: %s", fmt, exc)

    logger.info("[Cricket/Cricsheet] %s: %d records parsed", fmt, len(records))
    return records


# ──────────────────────────────────────────────────────────────────────────────
# Sentiment / News signals (off-season & live)
# ──────────────────────────────────────────────────────────────────────────────

def _fetch_newsdata_signals(api_key: str, sport: str, query: str, days_back: int = 90) -> list[dict]:
    """Fetch news headlines from newsdata.io for sentiment records."""
    if not api_key:
        return []
    since = (datetime.date.today() - datetime.timedelta(days=days_back)).isoformat()
    records = []
    try:
        resp = SESSION.get(
            "https://newsdata.io/api/1/news",
            params={"apikey": api_key, "q": query, "language": "en", "from_date": since, "size": 50},
            timeout=20,
        )
        resp.raise_for_status()
        data = resp.json()
        for art in data.get("results", []):
            headline = str(art.get("title") or "").strip()
            if not headline:
                continue
            records.append({
                "news_id": str(uuid4()),
                "sport": sport,
                "league": "",
                "game_id": "",
                "game_date": str(art.get("pubDate", ""))[:10],
                "game_time": "",
                "home_team": "",
                "away_team": "",
                "entity_type": "team",
                "entity_name": "",
                "entity_team": "",
                "impact_type": "general_update",
                "impact_scope": "team",
                "headline": headline,
                "description": str(art.get("description") or "")[:500],
                "article_url": str(art.get("link") or ""),
                "source_name": str(art.get("source_id") or ""),
                "published_at": str(art.get("pubDate") or ""),
                "sentiment_score": 0.0,
                "impact_score": 0.5,
                "metadata": "{}",
                "created_at": datetime.datetime.utcnow().isoformat() + "Z",
            })
    except Exception as exc:
        logger.debug("[News] %s query failed: %s", sport, exc)
    return records


def fetch_sentiment_records(newsdata_key: str) -> list[dict]:
    """Collect off-season sentiment news for all sports."""
    if not newsdata_key:
        logger.info("[News] No NEWSDATA_API_KEY — skipping sentiment records")
        return []
    queries = [
        ("mlb", "MLB baseball transfer trade injury signing"),
        ("soccer", "soccer football transfer injury Premier League La Liga"),
        ("cricket", "cricket IPL T20 ODI injury squad"),
    ]
    records = []
    for sport, query in queries:
        records.extend(_fetch_newsdata_signals(newsdata_key, sport, query, days_back=365))
        time.sleep(1.5)
    logger.info("[News] Collected %d sentiment records", len(records))
    return records


# ──────────────────────────────────────────────────────────────────────────────
# HF push helpers
# ──────────────────────────────────────────────────────────────────────────────

def push_batch_to_hf(records: list[dict], subset: str, hf_token: str, repo_id: str, dry_run: bool) -> bool:
    """Push a single batch of records as one parquet shard to HF Hub."""
    if not records:
        return True
    if dry_run:
        logger.info("[HF/DRY] Would push %d %s records to %s", len(records), subset, repo_id)
        return True

    try:
        import io
        import pyarrow as pa
        import pyarrow.parquet as pq
        from huggingface_hub import HfApi
    except ImportError as exc:
        logger.error("[HF] Missing dependencies: %s — run: pip install pyarrow huggingface_hub", exc)
        return False

    # Build Arrow table from records, allowing schema evolution
    df_records = []
    for r in records:
        clean = {}
        for k, v in r.items():
            if v is None:
                clean[k] = ""
            elif isinstance(v, float) and not (v == v):  # NaN check
                clean[k] = 0.0
            else:
                clean[k] = v
        df_records.append(clean)

    try:
        import pandas as pd
        df = pd.DataFrame(df_records)
        buf = io.BytesIO()
        df.to_parquet(buf, index=False, engine="pyarrow")
        buf.seek(0)

        ts = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        shard_name = f"shard_{ts}_{subset[:4]}.parquet"
        path_in_repo = f"data/{subset}/{shard_name}"

        api = HfApi(token=hf_token)
        api.upload_file(
            path_or_fileobj=buf,
            path_in_repo=path_in_repo,
            repo_id=repo_id,
            repo_type="dataset",
            commit_message=f"rebuild: {len(records)} {subset} records",
        )
        logger.info("[HF] Pushed %d %s records → %s", len(records), subset, path_in_repo)
        return True
    except Exception as exc:
        logger.error("[HF] Push failed for %s/%s: %s", subset, subset, exc)
        return False


def push_all_in_batches(
    records: list[dict],
    subset: str,
    hf_token: str,
    repo_id: str,
    dry_run: bool,
    seen_keys: set,
) -> int:
    """Dedup, batch and push records. Returns count of records actually pushed."""
    unique, dups = dedup_records(records, seen_keys)
    logger.info("[Push] %s: %d records, %d duplicates dropped, %d unique to push", subset, len(records), dups, len(unique))
    pushed = 0
    for i in range(0, len(unique), BATCH_SIZE):
        batch = unique[i:i + BATCH_SIZE]
        ok = push_batch_to_hf(batch, subset, hf_token, repo_id, dry_run)
        if ok:
            pushed += len(batch)
        time.sleep(SLEEP_BETWEEN_BATCHES)
    return pushed


# ──────────────────────────────────────────────────────────────────────────────
# Retrain model after rebuild
# ──────────────────────────────────────────────────────────────────────────────

def retrain_model_after_rebuild(hf_token: str, repo_id: str, dry_run: bool) -> None:
    """Trigger model retraining via the HFDirectPipeline (uses the new dataset)."""
    if dry_run:
        logger.info("[Retrain/DRY] Would trigger model retrain on %s", repo_id)
        return
    logger.info("[Retrain] Triggering model retrain on fresh dataset...")
    try:
        from data.hf_pipeline import HFDirectPipeline
        p = HFDirectPipeline(token=hf_token, dataset_repo=repo_id, model_repo=repo_id)
        if not p.ok:
            logger.warning("[Retrain] Pipeline not ready — skipping retrain")
            return
        summary = p.train_and_publish_best_model(min_rows=100, forced_model="auto")
        logger.info("[Retrain] Model published: %s (AUC=%.4f, rows=%d)", summary.best_model, summary.cv_roc_auc, summary.rows)
    except Exception as exc:
        logger.error("[Retrain] Retrain failed: %s", exc)


# ──────────────────────────────────────────────────────────────────────────────
# Main entry point
# ──────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Rebuild HF sports dataset — 2 seasons all leagues")
    parser.add_argument("--dry-run", action="store_true", help="Validate and count records, no HF writes")
    parser.add_argument("--sport", choices=["mlb", "soccer", "cricket", "all"], default="all")
    parser.add_argument("--year", type=int, default=0, help="Single year only (0 = fetch both seasons)")
    parser.add_argument("--skip-retrain", action="store_true", help="Skip model retrain after rebuild")
    parser.add_argument("--reset-dedup", action="store_true", help="Clear local dedup index before running")
    args = parser.parse_args()

    hf_token = os.getenv("HF_API_KEY", "")
    repo_id = os.getenv("HF_DATASET_REPO", "papylove/sportprediction")
    fd_key = os.getenv("FOOTBALL_DATA_API_KEY", "")
    cricket_key = os.getenv("CRICKET_RAPIDAPI_KEY", "")
    newsdata_key = os.getenv("NEWSDATA_API_KEY", "")

    if not hf_token and not args.dry_run:
        logger.error("HF_API_KEY not set. Export it or use --dry-run")
        sys.exit(1)

    logger.info("=" * 70)
    logger.info("Rebuild: sport=%s year=%s dry_run=%s repo=%s", args.sport, args.year or "all", args.dry_run, repo_id)
    logger.info("=" * 70)

    # Load / optionally reset dedup index
    if args.reset_dedup and DEDUP_INDEX_PATH.exists():
        DEDUP_INDEX_PATH.unlink()
        logger.info("Dedup index cleared")
    seen_keys = load_dedup_index()
    logger.info("Loaded %d existing dedup keys", len(seen_keys))

    total_pushed = 0
    sport_filter = args.sport.lower()

    # ── MLB ────────────────────────────────────────────────────────────────
    if sport_filter in ("mlb", "all"):
        seasons = [args.year] if args.year else MLB_SEASONS
        for season in seasons:
            logger.info("── MLB season %d ──", season)
            records = fetch_mlb_season(season)
            n = push_all_in_batches(records, "games", hf_token, repo_id, args.dry_run, seen_keys)
            total_pushed += n
            logger.info("MLB %d: %d records pushed", season, n)

    # ── Soccer ─────────────────────────────────────────────────────────────
    if sport_filter in ("soccer", "all"):
        years = [args.year] if args.year else [2024, 2025]
        for year in years:
            if fd_key:
                logger.info("── Soccer via football-data.org year %d ──", year)
                for code, (name, _) in FOOTBALL_DATA_LEAGUES.items():
                    if code == "MLS" and year < 2025:
                        continue  # MLS season ends in November
                    records = fetch_football_data_league_season(code, name, year, fd_key)
                    n = push_all_in_batches(records, "games", hf_token, repo_id, args.dry_run, seen_keys)
                    total_pushed += n
                    logger.info("Soccer/FD %s %d: %d pushed", code, year, n)
            else:
                logger.info("── Soccer via ESPN API year %d (FOOTBALL_DATA_API_KEY not set) ──", year)
                for league_id, league_name, sport in ESPN_SOCCER_LEAGUES:
                    records = fetch_espn_soccer_league_season(league_id, league_name, year)
                    n = push_all_in_batches(records, "games", hf_token, repo_id, args.dry_run, seen_keys)
                    total_pushed += n
                    logger.info("Soccer/ESPN %s %d: %d pushed", league_name, year, n)

    # ── Cricket ────────────────────────────────────────────────────────────
    if sport_filter in ("cricket", "all"):
        min_year = args.year if args.year else 2023
        for fmt, label, sport in CRICSHEET_FORMATS:
            logger.info("── Cricket %s ──", label)
            records = fetch_cricsheet_format(fmt, label, min_year=min_year)
            if args.year:
                records = [r for r in records if r.get("season") == args.year]
            n = push_all_in_batches(records, "games", hf_token, repo_id, args.dry_run, seen_keys)
            total_pushed += n
            logger.info("Cricket %s: %d pushed", label, n)

    # ── Sentiment / News ───────────────────────────────────────────────────
    if sport_filter == "all":
        logger.info("── Sentiment / News signals ──")
        records = fetch_sentiment_records(newsdata_key)
        if records:
            n = push_all_in_batches(records, "news_signals", hf_token, repo_id, args.dry_run, seen_keys)
            total_pushed += n
            logger.info("News signals: %d pushed", n)

    # Save updated dedup index
    save_dedup_index(seen_keys)
    logger.info("Dedup index saved: %d total keys", len(seen_keys))

    # ── Retrain model ──────────────────────────────────────────────────────
    if not args.skip_retrain and total_pushed > 0:
        retrain_model_after_rebuild(hf_token, repo_id, args.dry_run)
    elif args.skip_retrain:
        logger.info("Model retrain skipped (--skip-retrain)")
    else:
        logger.info("No records pushed — skipping retrain")

    logger.info("=" * 70)
    logger.info("Rebuild complete. Total records pushed: %d", total_pushed)
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
