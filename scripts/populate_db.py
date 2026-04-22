"""
populate_db.py
==============
Standalone data collection script.  Run manually or on a cron/Railway Job
to pre-populate the database from all configured APIs.

Usage:
    python scripts/populate_db.py [--sport mlb|soccer|all] [--force]

What it does (in order):
  1.  Init schema (create new tables if missing)
  2.  SportsData.io  → MLB standings + player stats + injuries
  3.  SportsData.io  → Soccer standings + injuries (5 major leagues)
  4.  TheSportsDB    → Soccer standings (6 leagues) + today's events
  5.  RapidAPI Live  → Current live soccer scores
  6.  NewsAPI        → Sports headlines for all active teams
  7.  Report: row counts for all tables
"""

import sys
import os
import argparse
import datetime

# ── Path setup ────────────────────────────────────────────────────────────────
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "src"))

from dotenv import load_dotenv
load_dotenv(os.path.join(ROOT, ".env"))

# ── Args ──────────────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser(description="Populate betting bot database")
parser.add_argument("--sport",  default="all",
                    choices=["all", "mlb", "soccer"],
                    help="Which sport(s) to collect data for")
parser.add_argument("--force",  action="store_true",
                    help="Force refresh even if data is recent")
args = parser.parse_args()
TARGET = args.sport

# ── Helpers ───────────────────────────────────────────────────────────────────
_start = datetime.datetime.now(datetime.timezone.utc)

def _section(title: str):
    print(f"\n{'═'*60}")
    print(f"  {title}")
    print(f"{'═'*60}")

def _ok(msg):   print(f"  ✓  {msg}")
def _warn(msg): print(f"  ⚠  {msg}")
def _err(msg):  print(f"  ✗  {msg}")

# ── Step 1: Schema ────────────────────────────────────────────────────────────
_section("Step 1 · Init schema")
try:
    from src.data.db import init_schema
    init_schema()
    _ok("Schema up to date")
except Exception as e:
    _err(f"Schema init failed: {e}")

# ── Step 2–3: SportsData.io ───────────────────────────────────────────────────
if TARGET in ("all", "mlb"):
    _section("Step 2 · SportsData.io — MLB")
    try:
        from src.data.sportsdata_fetcher import populate_mlb
        populate_mlb()
        _ok("MLB complete")
    except Exception as e:
        _err(f"MLB: {e}")

if TARGET in ("all", "soccer"):
    _section("Step 3 · SportsData.io — Soccer (5 leagues)")
    COMP_IDS = {
        "EPL (5)":         5,
        "Ligue 1 (12)":   12,
        "Bundesliga (10)":10,
        "Serie A (11)":   11,
        "La Liga (8)":     8,
    }
    try:
        from src.data.sportsdata_fetcher import populate_soccer
        for name, comp_id in COMP_IDS.items():
            try:
                populate_soccer(competition=comp_id)
                _ok(f"{name}")
            except Exception as e:
                _warn(f"{name}: {e}")
    except Exception as e:
        _err(f"Soccer import failed: {e}")

# ── Step 4: TheSportsDB ───────────────────────────────────────────────────────
if TARGET in ("all", "soccer"):
    _section("Step 4 · TheSportsDB — Soccer standings + today events")
    try:
        from src.data.thesportsdb_fetcher import (populate_soccer_standings,
                                                    populate_today_events)
        populate_soccer_standings()
        _ok("Soccer standings saved")
        for sport_name in ["Soccer", "Baseball"]:
            try:
                populate_today_events(sport_name.lower())
                _ok(f"Today events saved: {sport_name}")
            except Exception as e:
                _warn(f"Today events {sport_name}: {e}")
    except Exception as e:
        _err(f"TheSportsDB: {e}")

# ── Step 5: RapidAPI live scores ──────────────────────────────────────────────
if TARGET in ("all", "soccer"):
    _section("Step 5 · RapidAPI — Live soccer scores")
    try:
        from src.data.rapidapi_football_fetcher import populate_live_scores
        populate_live_scores()
        _ok("Live scores saved")
    except Exception as e:
        _warn(f"RapidAPI live scores: {e}")

# ── Step 6: NewsAPI headlines ─────────────────────────────────────────────────
_section("Step 6 · NewsAPI — Sports headlines")
MLB_TEAMS = [
    "Yankees", "Dodgers", "Red Sox", "Cubs", "Giants", "Mets",
    "Cardinals", "Braves", "Astros", "Phillies",
]
SOCCER_TEAMS = [
    "Manchester City", "Arsenal", "Chelsea", "Liverpool",
    "Real Madrid", "Barcelona", "Bayern Munich", "PSG",
    "Juventus", "Inter Milan",
]

TEAM_MAP = {}
if TARGET in ("all", "mlb"):
    TEAM_MAP.update({t: "mlb" for t in MLB_TEAMS})
if TARGET in ("all", "soccer"):
    TEAM_MAP.update({t: "soccer" for t in SOCCER_TEAMS})

try:
    from src.models.news_model import _newsapi_articles, _sentiment_score
    from src.data.db import save_news_articles
    total_saved = 0
    for team, sport in TEAM_MAP.items():
        try:
            arts = _newsapi_articles(f"{team} {sport}", page_size=5,
                                      sport=sport, team=team)
            if arts:
                # Back-fill sentiment scores before saving
                rows = []
                for a in arts:
                    rows.append({
                        "sport":       sport,
                        "team":        team,
                        "headline":    (a.get("title") or "")[:500],
                        "description": (a.get("description") or "")[:1000],
                        "url":         (a.get("url") or "")[:500],
                        "source_name": ((a.get("source") or {}).get("name") or ""),
                        "sentiment":   round(_sentiment_score([a]), 3),
                        "published_at": a.get("publishedAt"),
                    })
                save_news_articles(rows)
                total_saved += len(rows)
        except Exception as e:
            _warn(f"  news {team}: {e}")
    _ok(f"{total_saved} news articles saved")
except Exception as e:
    _err(f"NewsAPI: {e}")

# ── Step 7: Report ─────────────────────────────────────────────────────────────────
_section("Step 7 · Database row counts")
TABLES = [
    "games", "odds_history", "value_bets", "injury_reports",
    "team_stats", "prop_history", "player_profiles",
    "player_season_stats", "standings", "news_articles",
    "match_events", "head_to_head",
]
try:
    from src.data.db import get_conn
    conn = get_conn()
    if conn:
        cur = conn.cursor()
        for tbl in TABLES:
            try:
                cur.execute(f"SELECT COUNT(*) FROM {tbl}")
                n = cur.fetchone()[0]
                _ok(f"{tbl:<25} {n:>7,} rows")
            except Exception:
                _warn(f"{tbl:<25} (missing or error)")
        conn.close()
    else:
        _warn("No DB connection")
except Exception as e:
    _err(f"Report failed: {e}")

elapsed = (datetime.datetime.now(datetime.timezone.utc) - _start).total_seconds()
print(f"\n  Done in {elapsed:.1f}s\n")
