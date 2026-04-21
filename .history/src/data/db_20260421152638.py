"""
PostgreSQL Database Layer
=========================
Persists: games, odds history, value bets, team stats, injury reports.
All operations silently no-op when DB is unreachable — the bot runs fine without it.
"""

import os
import sys
import json
import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

DATABASE_URL = os.getenv("DATABASE_URL", "")

try:
    import psycopg2
    import psycopg2.extras
    PSYCOPG2_OK = True
except ImportError:
    PSYCOPG2_OK = False

# ──────────────────────────────────────────────────────────────────────────────
# Connection
# ──────────────────────────────────────────────────────────────────────────────

def get_conn():
    """Return a new psycopg2 connection, or None if unavailable."""
    if not PSYCOPG2_OK:
        return None
    url = DATABASE_URL or os.getenv("DATABASE_URL", "")
    if not url:
        return None
    try:
        return psycopg2.connect(url, connect_timeout=10)
    except Exception as e:
        print(f"[db] connection error: {e}")
        return None

# ──────────────────────────────────────────────────────────────────────────────
# Schema
# ──────────────────────────────────────────────────────────────────────────────

_SCHEMA = """
CREATE TABLE IF NOT EXISTS games (
    id              SERIAL PRIMARY KEY,
    sport           VARCHAR(20)  NOT NULL,
    league          VARCHAR(50),
    home_team       VARCHAR(100) NOT NULL,
    away_team       VARCHAR(100) NOT NULL,
    game_date       DATE         NOT NULL,
    game_time       TIME,
    game_datetime   TIMESTAMPTZ,
    status          VARCHAR(30)  DEFAULT 'Scheduled',
    home_score      INTEGER,
    away_score      INTEGER,
    home_starter    VARCHAR(100),
    away_starter    VARCHAR(100),
    created_at      TIMESTAMPTZ  DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  DEFAULT NOW(),
    UNIQUE(sport, home_team, away_team, game_date)
);

CREATE TABLE IF NOT EXISTS odds_history (
    id          SERIAL PRIMARY KEY,
    sport       VARCHAR(20),
    home_team   VARCHAR(100),
    away_team   VARCHAR(100),
    game_date   DATE,
    market      VARCHAR(20),
    outcome     VARCHAR(50),
    odds_am     INTEGER,
    dec_odds    NUMERIC(8,4),
    total_line  NUMERIC(5,1),
    bookmaker   VARCHAR(50),
    fetched_at  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS value_bets (
    id              SERIAL PRIMARY KEY,
    sport           VARCHAR(20),
    matchup         VARCHAR(200),
    game_date       DATE,
    bet             VARCHAR(20),
    model_prob      NUMERIC(5,4),
    book_prob       NUMERIC(5,4),
    edge            NUMERIC(5,4),
    odds_am         INTEGER,
    dec_odds        NUMERIC(8,4),
    stake_usd       NUMERIC(8,2),
    ev              NUMERIC(8,4),
    total_line      NUMERIC(5,1),
    predicted_total NUMERIC(5,1),
    bet_type        VARCHAR(20),
    detected_at     TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS injury_reports (
    id              SERIAL PRIMARY KEY,
    sport           VARCHAR(20),
    team            VARCHAR(100),
    player_name     VARCHAR(100),
    status          VARCHAR(50),
    description     VARCHAR(500),
    injury_type     VARCHAR(100),
    fetched_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS team_stats (
    id          SERIAL PRIMARY KEY,
    sport       VARCHAR(20),
    team        VARCHAR(100),
    season      INTEGER,
    stat_group  VARCHAR(20),
    stats_json  JSONB,
    updated_at  TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(sport, team, season, stat_group)
);

CREATE TABLE IF NOT EXISTS prop_history (
    id              SERIAL PRIMARY KEY,
    sport           VARCHAR(20),
    player_name     VARCHAR(100),
    team            VARCHAR(100),
    game_date       DATE,
    prop_type       VARCHAR(50),
    line            NUMERIC(5,1),
    over_prob       NUMERIC(5,4),
    under_prob      NUMERIC(5,4),
    recommendation  VARCHAR(30),
    stats_json      JSONB,
    detected_at     TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_games_date     ON games(game_date);
CREATE INDEX IF NOT EXISTS idx_games_sport    ON games(sport, game_date);
CREATE INDEX IF NOT EXISTS idx_injuries_sport ON injury_reports(sport, fetched_at);
CREATE INDEX IF NOT EXISTS idx_vbets_date     ON value_bets(detected_at);
"""


def init_schema():
    """Create all tables. Called once at dashboard startup."""
    conn = get_conn()
    if conn is None:
        print("[db] schema init skipped — no DB connection")
        return False
    try:
        cur = conn.cursor()
        cur.execute(_SCHEMA)
        conn.commit()
        print("[db] schema ready")
        return True
    except Exception as e:
        conn.rollback()
        print(f"[db] schema init error: {e}")
        return False
    finally:
        conn.close()

# ──────────────────────────────────────────────────────────────────────────────
# Games
# ──────────────────────────────────────────────────────────────────────────────

def upsert_game(sport, league, home_team, away_team, game_date,
                game_time=None, game_datetime=None, status="Scheduled",
                home_starter=None, away_starter=None):
    conn = get_conn()
    if conn is None:
        return None
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO games
                (sport, league, home_team, away_team, game_date, game_time,
                 game_datetime, status, home_starter, away_starter)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (sport, home_team, away_team, game_date) DO UPDATE SET
                status        = EXCLUDED.status,
                league        = COALESCE(EXCLUDED.league,        games.league),
                game_time     = COALESCE(EXCLUDED.game_time,     games.game_time),
                game_datetime = COALESCE(EXCLUDED.game_datetime, games.game_datetime),
                home_starter  = COALESCE(EXCLUDED.home_starter,  games.home_starter),
                away_starter  = COALESCE(EXCLUDED.away_starter,  games.away_starter),
                updated_at    = NOW()
            RETURNING id
        """, (sport, league, home_team, away_team, game_date, game_time,
              game_datetime, status, home_starter, away_starter))
        row = cur.fetchone()
        conn.commit()
        return row[0] if row else None
    except Exception as e:
        conn.rollback()
        print(f"[db] upsert_game error: {e}")
        return None
    finally:
        conn.close()


def get_upcoming_games(days_ahead=1):
    """Return today + N days ahead games from DB, sorted by datetime."""
    conn = get_conn()
    if conn is None:
        return []
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        end_date = datetime.date.today() + datetime.timedelta(days=days_ahead)
        cur.execute("""
            SELECT * FROM games
            WHERE game_date BETWEEN CURRENT_DATE AND %s
            ORDER BY game_date, game_time NULLS LAST
        """, (end_date,))
        rows = [dict(r) for r in cur.fetchall()]
        # Convert date/time objects to strings for JSON
        for r in rows:
            if isinstance(r.get("game_date"), datetime.date):
                r["game_date"] = r["game_date"].isoformat()
            if isinstance(r.get("game_time"), datetime.time):
                r["game_time"] = r["game_time"].strftime("%H:%M")
            if r.get("game_datetime"):
                r["game_datetime"] = r["game_datetime"].isoformat()
        return rows
    except Exception as e:
        print(f"[db] get_upcoming_games error: {e}")
        return []
    finally:
        conn.close()


# ──────────────────────────────────────────────────────────────────────────────
# Value bets
# ──────────────────────────────────────────────────────────────────────────────

def save_value_bets(bets, bet_type, game_date=None):
    """Bulk-insert detected value bets for today."""
    if not bets:
        return
    conn = get_conn()
    if conn is None:
        return
    gdate = game_date or datetime.date.today()
    try:
        cur = conn.cursor()
        for b in bets:
            cur.execute("""
                INSERT INTO value_bets
                    (sport, matchup, game_date, bet, model_prob, book_prob, edge,
                     odds_am, dec_odds, stake_usd, ev, total_line, predicted_total, bet_type)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                b.get("sport"), b.get("matchup"), gdate, b.get("bet"),
                b.get("model_prob"), b.get("book_prob"), b.get("edge"),
                b.get("odds_am"), b.get("dec_odds"), b.get("stake_usd"),
                b.get("ev"), b.get("total_line"), b.get("predicted_total"),
                bet_type,
            ))
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[db] save_value_bets error: {e}")
    finally:
        conn.close()


def get_value_bets_history(days=30):
    """Fetch value bets from last N days for historical context."""
    conn = get_conn()
    if conn is None:
        return []
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT sport, matchup, game_date, bet, model_prob, edge, ev, bet_type, detected_at
            FROM value_bets
            WHERE detected_at > NOW() - INTERVAL '%s days'
            ORDER BY detected_at DESC
            LIMIT 200
        """, (days,))
        rows = [dict(r) for r in cur.fetchall()]
        for r in rows:
            if isinstance(r.get("game_date"), datetime.date):
                r["game_date"] = r["game_date"].isoformat()
            if r.get("detected_at"):
                r["detected_at"] = r["detected_at"].isoformat()
        return rows
    except Exception as e:
        print(f"[db] get_value_bets_history error: {e}")
        return []
    finally:
        conn.close()


# ──────────────────────────────────────────────────────────────────────────────
# Injuries
# ──────────────────────────────────────────────────────────────────────────────

def save_injuries(sport, injuries):
    """Replace injury records for a sport with fresh data."""
    conn = get_conn()
    if conn is None:
        return
    try:
        cur = conn.cursor()
        # Delete stale records for this sport (older than 12h)
        cur.execute("""
            DELETE FROM injury_reports
            WHERE sport = %s AND fetched_at < NOW() - INTERVAL '12 hours'
        """, (sport,))
        for inj in injuries:
            cur.execute("""
                INSERT INTO injury_reports
                    (sport, team, player_name, status, description, injury_type)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                sport, inj.get("team"), inj.get("player_name"),
                inj.get("status"), inj.get("description"), inj.get("injury_type"),
            ))
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[db] save_injuries error: {e}")
    finally:
        conn.close()


def get_injuries(sport=None):
    """Get current injury reports (last 24h)."""
    conn = get_conn()
    if conn is None:
        return []
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if sport:
            cur.execute("""
                SELECT * FROM injury_reports
                WHERE sport = %s AND fetched_at > NOW() - INTERVAL '24 hours'
                ORDER BY team, player_name
            """, (sport,))
        else:
            cur.execute("""
                SELECT * FROM injury_reports
                WHERE fetched_at > NOW() - INTERVAL '24 hours'
                ORDER BY sport, team, player_name
            """)
        return [dict(r) for r in cur.fetchall()]
    except Exception as e:
        print(f"[db] get_injuries error: {e}")
        return []
    finally:
        conn.close()


# ──────────────────────────────────────────────────────────────────────────────
# Odds history
# ──────────────────────────────────────────────────────────────────────────────

def save_odds_snapshot(sport, home_team, away_team, game_date, market,
                       outcome, odds_am, dec_odds, total_line=None, bookmaker=None):
    conn = get_conn()
    if conn is None:
        return
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO odds_history
                (sport, home_team, away_team, game_date, market, outcome,
                 odds_am, dec_odds, total_line, bookmaker)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (sport, home_team, away_team, game_date, market, outcome,
              odds_am, dec_odds, total_line, bookmaker))
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[db] save_odds_snapshot error: {e}")
    finally:
        conn.close()
