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
    external_id     VARCHAR(100),
    season          INTEGER,
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
    signal_boost    NUMERIC(5,4),
    signal_sources  TEXT,
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
    source          VARCHAR(50),
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
    actual_value    NUMERIC(6,2),
    outcome         VARCHAR(20)  DEFAULT 'PENDING',
    resolved_at     TIMESTAMPTZ,
    detected_at     TIMESTAMPTZ DEFAULT NOW()
);
-- Add outcome columns to existing prop_history tables (idempotent)
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='prop_history' AND column_name='outcome') THEN
        ALTER TABLE prop_history ADD COLUMN actual_value NUMERIC(6,2);
        ALTER TABLE prop_history ADD COLUMN outcome VARCHAR(20) DEFAULT 'PENDING';
        ALTER TABLE prop_history ADD COLUMN resolved_at TIMESTAMPTZ;
    END IF;
END $$;

-- ── NEW: player profiles (BallDontLie, SportsData.io, TheSportsDB) ──
CREATE TABLE IF NOT EXISTS player_profiles (
    id              SERIAL PRIMARY KEY,
    sport           VARCHAR(20)  NOT NULL,
    external_id     VARCHAR(100),
    player_name     VARCHAR(150) NOT NULL,
    team            VARCHAR(100),
    position        VARCHAR(30),
    jersey_number   VARCHAR(10),
    height          VARCHAR(20),
    weight          VARCHAR(20),
    birthdate       DATE,
    nationality     VARCHAR(80),
    status          VARCHAR(30),
    source          VARCHAR(50),
    profile_json    JSONB,
    updated_at      TIMESTAMPTZ  DEFAULT NOW(),
    UNIQUE(sport, source, external_id)
);

-- ── NEW: player season stats ──
CREATE TABLE IF NOT EXISTS player_season_stats (
    id              SERIAL PRIMARY KEY,
    sport           VARCHAR(20)  NOT NULL,
    player_name     VARCHAR(150) NOT NULL,
    team            VARCHAR(100),
    season          INTEGER,
    stat_group      VARCHAR(30),   -- 'batting','pitching','nba_base','soccer_std', etc.
    stats_json      JSONB,
    source          VARCHAR(50),
    updated_at      TIMESTAMPTZ    DEFAULT NOW(),
    UNIQUE(sport, player_name, season, stat_group, source)
);

-- ── NEW: standings ──
CREATE TABLE IF NOT EXISTS standings (
    id          SERIAL PRIMARY KEY,
    sport       VARCHAR(20)  NOT NULL,
    league      VARCHAR(50)  NOT NULL,
    season      INTEGER,
    team        VARCHAR(100) NOT NULL,
    rank        INTEGER,
    wins        INTEGER,
    losses      INTEGER,
    draws       INTEGER,
    points      INTEGER,
    gf          INTEGER,
    ga          INTEGER,
    gd          INTEGER,
    form        VARCHAR(20),
    stats_json  JSONB,
    source      VARCHAR(50),
    updated_at  TIMESTAMPTZ  DEFAULT NOW(),
    UNIQUE(sport, league, season, team)
);

-- ── NEW: news articles cache ──
CREATE TABLE IF NOT EXISTS news_articles (
    id              SERIAL PRIMARY KEY,
    sport           VARCHAR(20),
    team            VARCHAR(100),
    headline        TEXT,
    description     TEXT,
    url             TEXT,
    source_name     VARCHAR(100),
    sentiment       NUMERIC(4,3),   -- -1.0 to +1.0
    published_at    TIMESTAMPTZ,
    fetched_at      TIMESTAMPTZ DEFAULT NOW()
);

-- ── NEW: match events / live data ──
CREATE TABLE IF NOT EXISTS match_events (
    id              SERIAL PRIMARY KEY,
    sport           VARCHAR(20),
    league          VARCHAR(50),
    home_team       VARCHAR(100),
    away_team       VARCHAR(100),
    game_date       DATE,
    event_type      VARCHAR(50),   -- 'goal','card','substitution','injury_time', etc.
    minute          INTEGER,
    player_name     VARCHAR(150),
    team            VARCHAR(100),
    detail          TEXT,
    source          VARCHAR(50),
    fetched_at      TIMESTAMPTZ DEFAULT NOW()
);

-- ── NEW: head-to-head records ──
CREATE TABLE IF NOT EXISTS head_to_head (
    id              SERIAL PRIMARY KEY,
    sport           VARCHAR(20),
    league          VARCHAR(50),
    team_a          VARCHAR(100),
    team_b          VARCHAR(100),
    season          INTEGER,
    h2h_json        JSONB,          -- {wins_a, wins_b, draws, last5, avg_goals, ...}
    source          VARCHAR(50),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(sport, team_a, team_b, season, source)
);

CREATE INDEX IF NOT EXISTS idx_games_date         ON games(game_date);
CREATE INDEX IF NOT EXISTS idx_games_sport        ON games(sport, game_date);
CREATE INDEX IF NOT EXISTS idx_injuries_sport     ON injury_reports(sport, fetched_at);
CREATE INDEX IF NOT EXISTS idx_vbets_date         ON value_bets(detected_at);
CREATE INDEX IF NOT EXISTS idx_player_stats_name  ON player_season_stats(player_name, sport);
CREATE INDEX IF NOT EXISTS idx_standings_league   ON standings(sport, league, season);
CREATE INDEX IF NOT EXISTS idx_news_team          ON news_articles(sport, team, fetched_at);
CREATE INDEX IF NOT EXISTS idx_h2h_teams          ON head_to_head(sport, team_a, team_b);
CREATE INDEX IF NOT EXISTS idx_player_prof_name   ON player_profiles(sport, player_name);

-- ── Analysis cache: stores full game-card + parlay results per day ──
CREATE TABLE IF NOT EXISTS analysis_cache (
    id         SERIAL PRIMARY KEY,
    cache_date DATE        NOT NULL UNIQUE,
    data_json  JSONB       NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- ── SMS recipients: phone numbers to receive daily pick texts ──
CREATE TABLE IF NOT EXISTS phone_numbers (
    id       SERIAL PRIMARY KEY,
    phone    VARCHAR(30) NOT NULL UNIQUE,
    label    VARCHAR(100) DEFAULT '',
    active   BOOLEAN      DEFAULT TRUE,
    added_at TIMESTAMPTZ  DEFAULT NOW()
);

-- ── MLB Predictions: every prediction the bot makes ──
CREATE TABLE IF NOT EXISTS predictions (
    id              SERIAL PRIMARY KEY,
    game_key        VARCHAR(200)  NOT NULL,
    sport           VARCHAR(20)   DEFAULT 'mlb',
    bet_type        VARCHAR(50),  -- 'moneyline','spread','total','f5','player_prop','parlay'
    pick            VARCHAR(200),
    line            NUMERIC(6,2),
    odds_am         INTEGER,
    dec_odds        NUMERIC(8,4),
    model_prob      NUMERIC(5,4),
    confidence      INTEGER,
    safety_label    VARCHAR(20),
    game_date       DATE,
    game_time       VARCHAR(20),
    home_team       VARCHAR(100),
    away_team       VARCHAR(100),
    home_starter    VARCHAR(100),
    away_starter    VARCHAR(100),
    predicted_at    TIMESTAMPTZ   DEFAULT NOW(),
    outcome         VARCHAR(20)   DEFAULT 'PENDING',  -- 'WIN','LOSS','PUSH','PENDING'
    actual_result   TEXT,
    resolved_at     TIMESTAMPTZ,
    sentiment_score NUMERIC(5,4),
    news_snippet    TEXT
);
CREATE INDEX IF NOT EXISTS idx_predictions_date    ON predictions(game_date);
CREATE INDEX IF NOT EXISTS idx_predictions_outcome ON predictions(outcome);

-- ── Player trends: historical per-player stats ──
CREATE TABLE IF NOT EXISTS player_trends (
    id          SERIAL PRIMARY KEY,
    player_name VARCHAR(150),
    player_id   INTEGER,
    team        VARCHAR(100),
    season      INTEGER,
    stat_type   VARCHAR(50),   -- 'batting','pitching'
    last_5      JSONB,         -- {avg, hr, rbi, k, etc.}
    last_10     JSONB,
    season_avg  NUMERIC(8,4),
    vs_lefty    NUMERIC(8,4),
    vs_righty   NUMERIC(8,4),
    home_avg    NUMERIC(8,4),
    away_avg    NUMERIC(8,4),
    updated_at  TIMESTAMPTZ    DEFAULT NOW(),
    UNIQUE(player_name, season, stat_type)
);
CREATE INDEX IF NOT EXISTS idx_player_trends_name ON player_trends(player_name);

-- ── Sentiment scores: Reddit/news/combined per team or player ──
CREATE TABLE IF NOT EXISTS sentiment_scores (
    id           SERIAL PRIMARY KEY,
    entity       VARCHAR(150),   -- team or player name
    entity_type  VARCHAR(20),    -- 'team' or 'player'
    source       VARCHAR(50),    -- 'reddit','news','combined'
    score        NUMERIC(5,4),   -- -1.0 to +1.0
    volume       INTEGER,        -- number of posts/articles
    keywords     TEXT,
    computed_at  TIMESTAMPTZ     DEFAULT NOW(),
    UNIQUE(entity, source, DATE(computed_at))
);
CREATE INDEX IF NOT EXISTS idx_sentiment_entity ON sentiment_scores(entity, computed_at);

-- ── Tracked parlays: user-built and bot-generated parlays ──
CREATE TABLE IF NOT EXISTS tracked_parlays (
    id             SERIAL PRIMARY KEY,
    name           VARCHAR(200),
    legs_json      JSONB,          -- [{pick, game, odds, type}, ...]
    combined_odds  NUMERIC(8,2),
    stake_usd      NUMERIC(8,2)    DEFAULT 0,
    created_at     TIMESTAMPTZ     DEFAULT NOW(),
    resolved_at    TIMESTAMPTZ,
    outcome        VARCHAR(20)     DEFAULT 'PENDING',
    payout_usd     NUMERIC(8,2)
);
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
                home_starter=None, away_starter=None,
                home_score=None, away_score=None,
                season=None, external_id=None):
    conn = get_conn()
    if conn is None:
        return None
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO games
                (sport, league, home_team, away_team, game_date, game_time,
                 game_datetime, status, home_starter, away_starter,
                 home_score, away_score, season, external_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (sport, home_team, away_team, game_date) DO UPDATE SET
                status        = EXCLUDED.status,
                league        = COALESCE(EXCLUDED.league,        games.league),
                game_time     = COALESCE(EXCLUDED.game_time,     games.game_time),
                game_datetime = COALESCE(EXCLUDED.game_datetime, games.game_datetime),
                home_starter  = COALESCE(EXCLUDED.home_starter,  games.home_starter),
                away_starter  = COALESCE(EXCLUDED.away_starter,  games.away_starter),
                home_score    = COALESCE(EXCLUDED.home_score,    games.home_score),
                away_score    = COALESCE(EXCLUDED.away_score,    games.away_score),
                season        = COALESCE(EXCLUDED.season,        games.season),
                external_id   = COALESCE(EXCLUDED.external_id,   games.external_id),
                updated_at    = NOW()
            RETURNING id
        """, (sport, league, home_team, away_team, game_date, game_time,
              game_datetime, status, home_starter, away_starter,
              home_score, away_score, season, str(external_id) if external_id else None))
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
            sig_sources = b.get("signal_sources")
            if isinstance(sig_sources, list):
                sig_sources = ",".join(sig_sources)
            cur.execute("""
                INSERT INTO value_bets
                    (sport, matchup, game_date, bet, model_prob, book_prob, edge,
                     odds_am, dec_odds, stake_usd, ev, total_line, predicted_total,
                     bet_type, signal_boost, signal_sources)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                b.get("sport"), b.get("matchup"), gdate, b.get("bet"),
                b.get("model_prob"), b.get("book_prob"), b.get("edge"),
                b.get("odds_am"), b.get("dec_odds"), b.get("stake_usd"),
                b.get("ev"), b.get("total_line"), b.get("predicted_total"),
                bet_type,
                b.get("signal_boost"), sig_sources,
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

def save_injuries(sport, injuries, keep_history: bool = False):
    """Save injury records. When keep_history is True, do not delete old rows."""
    conn = get_conn()
    if conn is None:
        return
    try:
        cur = conn.cursor()
        if not keep_history:
            # Delete stale records for this sport (older than 12h)
            cur.execute("""
                DELETE FROM injury_reports
                WHERE sport = %s AND fetched_at < NOW() - INTERVAL '12 hours'
            """, (sport,))
        for inj in injuries:
            fetched_at = inj.get("fetched_at")
            cur.execute("""
                INSERT INTO injury_reports
                    (sport, team, player_name, status, description, injury_type, fetched_at)
                VALUES (%s, %s, %s, %s, %s, %s, COALESCE(%s, NOW()))
            """, (
                sport, inj.get("team"), inj.get("player_name"),
                inj.get("status"), inj.get("description"), inj.get("injury_type"),
                fetched_at,
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


# ──────────────────────────────────────────────────────────────────────────────
# Stats cache  (reuses team_stats table to store serialised DataFrames)
# ──────────────────────────────────────────────────────────────────────────────

def save_stats_cache(sport: str, cache_key: str, season: int,
                     stat_group: str, json_str: str):
    """
    Upsert a JSON-serialised DataFrame into team_stats for caching.
    cache_key: arbitrary string identifier (e.g. '__batting_all__', 'EPL').
    stat_group: max 20 chars (e.g. 'batting', 'pitching', 'fbref_std').
    """
    conn = get_conn()
    if conn is None:
        return
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO team_stats (sport, team, season, stat_group, stats_json, updated_at)
            VALUES (%s, %s, %s, %s, %s::jsonb, NOW())
            ON CONFLICT (sport, team, season, stat_group) DO UPDATE SET
                stats_json = EXCLUDED.stats_json,
                updated_at = NOW()
        """, (sport, cache_key, int(season), stat_group, json_str))
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[db] save_stats_cache error: {e}")
    finally:
        conn.close()


def get_stats_cache(sport: str, cache_key: str, season: int,
                    stat_group: str, max_age_hours: int = 6) -> "str | None":
    """
    Return cached stats JSON string if it exists and is younger than max_age_hours.
    Returns None if not found or stale — caller should then fetch from external API.
    """
    conn = get_conn()
    if conn is None:
        return None
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT stats_json::text
            FROM   team_stats
            WHERE  sport      = %s
              AND  team       = %s
              AND  season     = %s
              AND  stat_group = %s
              AND  updated_at > NOW() - (INTERVAL '1 hour' * %s)
        """, (sport, cache_key, int(season), stat_group, max_age_hours))
        row = cur.fetchone()
        return row[0] if row else None
    except Exception as e:
        print(f"[db] get_stats_cache error: {e}")
        return None
    finally:
        conn.close()


# ──────────────────────────────────────────────────────────────────────────────
# Prop history  (save daily qualified prop picks)
# ──────────────────────────────────────────────────────────────────────────────

def save_prop_picks(picks: list, game_date=None):
    """
    Bulk-insert qualified player prop picks into prop_history.
    picks: list of dicts produced by _build_prop_pick() in dashboard.py.
    Silently skips duplicates (same player+date+prop_type combination).
    """
    if not picks:
        return
    conn = get_conn()
    if conn is None:
        return
    gdate = game_date or datetime.date.today()
    saved = 0
    try:
        cur = conn.cursor()
        for p in picks:
            stats_snap = json.dumps({
                "era":       p.get("era"),     "k9":        p.get("k9"),
                "avg":       p.get("avg"),     "ops":       p.get("ops"),
                "xg":        p.get("xg"),      "xa":        p.get("xa"),
                "over_pct":  p.get("over_pct"),"under_pct": p.get("under_pct"),
                "league":    p.get("league"),  "game":      p.get("game"),
            })
            try:
                cur.execute("""
                    INSERT INTO prop_history
                        (sport, player_name, team, game_date, prop_type,
                         line, over_prob, under_prob, recommendation, stats_json)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    p.get("sport", "mlb"),
                    p.get("name"),
                    p.get("team"),
                    gdate,
                    p.get("stat_type"),
                    p.get("line"),
                    (p.get("over_pct") or 50) / 100.0,
                    (p.get("under_pct") or 50) / 100.0,
                    p.get("direction"),
                    stats_snap,
                ))
                saved += 1
            except Exception:
                pass  # ignore duplicate key violations per pick
        conn.commit()
        print(f"[db] Saved {saved}/{len(picks)} prop picks to history")
    except Exception as e:
        conn.rollback()
        print(f"[db] save_prop_picks error: {e}")
    finally:
        conn.close()


def get_todays_prop_picks(sport: str = None, max_age_hours: int = 2) -> list:
    """
    Return today's prop picks from prop_history if they were saved within
    max_age_hours.  Returns [] when no fresh data exists (caller fetches fresh).
    """
    conn = get_conn()
    if conn is None:
        return []
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        base = """
            SELECT sport, player_name, team, game_date, prop_type,
                   line, over_prob, under_prob, recommendation, stats_json, detected_at
            FROM   prop_history
            WHERE  game_date  = CURRENT_DATE
              AND  detected_at > NOW() - (INTERVAL '1 hour' * %s)
        """
        if sport:
            cur.execute(base + " AND sport = %s ORDER BY detected_at DESC",
                        (max_age_hours, sport))
        else:
            cur.execute(base + " ORDER BY detected_at DESC", (max_age_hours,))
        rows = []
        for r in cur.fetchall():
            d = dict(r)
            if isinstance(d.get("game_date"), datetime.date):
                d["game_date"] = d["game_date"].isoformat()
            if d.get("detected_at"):
                d["detected_at"] = d["detected_at"].isoformat()
            rows.append(d)
        return rows
    except Exception as e:
        print(f"[db] get_todays_prop_picks error: {e}")
        return []
    finally:
        conn.close()


# ──────────────────────────────────────────────────────────────────────────────
# Player profiles
# ──────────────────────────────────────────────────────────────────────────────

def upsert_player_profile(sport: str, source: str, external_id: str,
                          player_name: str, team: str = None, position: str = None,
                          profile_json: dict = None, **kwargs):
    conn = get_conn()
    if conn is None:
        return
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO player_profiles
                (sport, source, external_id, player_name, team, position,
                 jersey_number, height, weight, birthdate, nationality, status, profile_json, updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,NOW())
            ON CONFLICT (sport, source, external_id) DO UPDATE SET
                player_name   = EXCLUDED.player_name,
                team          = COALESCE(EXCLUDED.team,         player_profiles.team),
                position      = COALESCE(EXCLUDED.position,     player_profiles.position),
                jersey_number = COALESCE(EXCLUDED.jersey_number,player_profiles.jersey_number),
                height        = COALESCE(EXCLUDED.height,       player_profiles.height),
                weight        = COALESCE(EXCLUDED.weight,       player_profiles.weight),
                birthdate     = COALESCE(EXCLUDED.birthdate,    player_profiles.birthdate),
                nationality   = COALESCE(EXCLUDED.nationality,  player_profiles.nationality),
                status        = COALESCE(EXCLUDED.status,       player_profiles.status),
                profile_json  = COALESCE(EXCLUDED.profile_json, player_profiles.profile_json),
                updated_at    = NOW()
        """, (
            sport, source, str(external_id), player_name, team, position,
            kwargs.get("jersey_number"), kwargs.get("height"), kwargs.get("weight"),
            kwargs.get("birthdate"), kwargs.get("nationality"), kwargs.get("status"),
            json.dumps(profile_json or {}),
        ))
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[db] upsert_player_profile error: {e}")
    finally:
        conn.close()


def bulk_upsert_player_profiles(profiles: list):
    """profiles: list of dicts matching upsert_player_profile kwargs."""
    if not profiles:
        return
    conn = get_conn()
    if conn is None:
        return
    saved = 0
    try:
        cur = conn.cursor()
        for p in profiles:
            try:
                cur.execute("""
                    INSERT INTO player_profiles
                        (sport, source, external_id, player_name, team, position,
                         jersey_number, height, weight, birthdate, nationality,
                         status, profile_json, updated_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,NOW())
                    ON CONFLICT (sport, source, external_id) DO UPDATE SET
                        player_name   = EXCLUDED.player_name,
                        team          = COALESCE(EXCLUDED.team,         player_profiles.team),
                        position      = COALESCE(EXCLUDED.position,     player_profiles.position),
                        profile_json  = COALESCE(EXCLUDED.profile_json, player_profiles.profile_json),
                        updated_at    = NOW()
                """, (
                    p.get("sport"), p.get("source"), str(p.get("external_id","")),
                    p.get("player_name",""), p.get("team"), p.get("position"),
                    p.get("jersey_number"), p.get("height"), p.get("weight"),
                    p.get("birthdate"), p.get("nationality"), p.get("status"),
                    json.dumps(p.get("profile_json") or {}),
                ))
                saved += 1
            except Exception:
                pass
        conn.commit()
        print(f"[db] upserted {saved}/{len(profiles)} player profiles")
    except Exception as e:
        conn.rollback()
        print(f"[db] bulk_upsert_player_profiles error: {e}")
    finally:
        conn.close()


# ──────────────────────────────────────────────────────────────────────────────
# Player season stats
# ──────────────────────────────────────────────────────────────────────────────

def save_player_season_stats(stats: list):
    """
    stats: list of dicts with keys:
        sport, player_name, team, season, stat_group, stats_json (dict), source
    """
    if not stats:
        return
    conn = get_conn()
    if conn is None:
        return
    saved = 0
    try:
        cur = conn.cursor()
        for s in stats:
            try:
                cur.execute("""
                    INSERT INTO player_season_stats
                        (sport, player_name, team, season, stat_group, stats_json, source, updated_at)
                    VALUES (%s,%s,%s,%s,%s,%s::jsonb,%s,NOW())
                    ON CONFLICT (sport, player_name, season, stat_group, source) DO UPDATE SET
                        team        = COALESCE(EXCLUDED.team, player_season_stats.team),
                        stats_json  = EXCLUDED.stats_json,
                        updated_at  = NOW()
                """, (
                    s.get("sport"), s.get("player_name"), s.get("team"),
                    int(s.get("season", 0)), s.get("stat_group","general"),
                    json.dumps(s.get("stats_json") or {}), s.get("source",""),
                ))
                saved += 1
            except Exception:
                pass
        conn.commit()
        print(f"[db] saved {saved}/{len(stats)} player season stats rows")
    except Exception as e:
        conn.rollback()
        print(f"[db] save_player_season_stats error: {e}")
    finally:
        conn.close()


def get_player_season_stats(sport: str, player_name: str = None,
                             season: int = None, source: str = None) -> list:
    conn = get_conn()
    if conn is None:
        return []
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        wheres = ["sport = %s"]
        vals   = [sport]
        if player_name:
            wheres.append("player_name ILIKE %s"); vals.append(f"%{player_name}%")
        if season:
            wheres.append("season = %s"); vals.append(season)
        if source:
            wheres.append("source = %s"); vals.append(source)
        cur.execute(f"SELECT * FROM player_season_stats WHERE {' AND '.join(wheres)} "
                    "ORDER BY season DESC, player_name LIMIT 2000", vals)
        return [dict(r) for r in cur.fetchall()]
    except Exception as e:
        print(f"[db] get_player_season_stats error: {e}")
        return []
    finally:
        conn.close()


# ──────────────────────────────────────────────────────────────────────────────
# Standings
# ──────────────────────────────────────────────────────────────────────────────

def save_standings(standings: list):
    """
    standings: list of dicts with keys:
        sport, league, season, team, rank, wins, losses, draws, points,
        gf, ga, gd, form, stats_json (dict), source
    """
    if not standings:
        return
    conn = get_conn()
    if conn is None:
        return
    saved = 0
    try:
        cur = conn.cursor()
        for s in standings:
            try:
                cur.execute("""
                    INSERT INTO standings
                        (sport, league, season, team, rank, wins, losses, draws,
                         points, gf, ga, gd, form, stats_json, source, updated_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,NOW())
                    ON CONFLICT (sport, league, season, team) DO UPDATE SET
                        rank       = EXCLUDED.rank,
                        wins       = EXCLUDED.wins,
                        losses     = EXCLUDED.losses,
                        draws      = EXCLUDED.draws,
                        points     = EXCLUDED.points,
                        gf         = EXCLUDED.gf,
                        ga         = EXCLUDED.ga,
                        gd         = EXCLUDED.gd,
                        form       = EXCLUDED.form,
                        stats_json = EXCLUDED.stats_json,
                        updated_at = NOW()
                """, (
                    s.get("sport"), s.get("league"), int(s.get("season",0)),
                    s.get("team"), s.get("rank"), s.get("wins"), s.get("losses"),
                    s.get("draws"), s.get("points"), s.get("gf"), s.get("ga"),
                    s.get("gd"), s.get("form"), json.dumps(s.get("stats_json") or {}),
                    s.get("source",""),
                ))
                saved += 1
            except Exception:
                pass
        conn.commit()
        print(f"[db] saved {saved}/{len(standings)} standings rows")
    except Exception as e:
        conn.rollback()
        print(f"[db] save_standings error: {e}")
    finally:
        conn.close()


def get_standings(sport: str, league: str = None, season: int = None) -> list:
    conn = get_conn()
    if conn is None:
        return []
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        wheres = ["sport = %s"]
        vals   = [sport]
        if league:
            wheres.append("league = %s"); vals.append(league)
        if season:
            wheres.append("season = %s"); vals.append(season)
        cur.execute(f"SELECT * FROM standings WHERE {' AND '.join(wheres)} "
                    "ORDER BY rank ASC NULLS LAST LIMIT 100", vals)
        return [dict(r) for r in cur.fetchall()]
    except Exception as e:
        print(f"[db] get_standings error: {e}")
        return []
    finally:
        conn.close()


# ──────────────────────────────────────────────────────────────────────────────
# News articles
# ──────────────────────────────────────────────────────────────────────────────

def save_news_articles(articles: list):
    """
    articles: list of dicts with keys:
        sport, team, headline, description, url, source_name, sentiment, published_at
    """
    if not articles:
        return
    conn = get_conn()
    if conn is None:
        return
    saved = 0
    try:
        cur = conn.cursor()
        for a in articles:
            try:
                pub = a.get("published_at")
                cur.execute("""
                    INSERT INTO news_articles
                        (sport, team, headline, description, url,
                         source_name, sentiment, published_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT DO NOTHING
                """, (
                    a.get("sport"), a.get("team"),
                    a.get("headline","")[:500], a.get("description","")[:1000],
                    a.get("url","")[:500], a.get("source_name","")[:100],
                    a.get("sentiment"), pub,
                ))
                saved += 1
            except Exception:
                pass
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[db] save_news_articles error: {e}")
    finally:
        conn.close()


def get_news_articles(sport: str = None, team: str = None,
                      hours: int = 12) -> list:
    conn = get_conn()
    if conn is None:
        return []
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        wheres = ["fetched_at > NOW() - (INTERVAL '1 hour' * %s)"]
        vals   = [hours]
        if sport:
            wheres.append("sport = %s"); vals.append(sport)
        if team:
            wheres.append("team ILIKE %s"); vals.append(f"%{team}%")
        cur.execute(f"SELECT * FROM news_articles WHERE {' AND '.join(wheres)} "
                    "ORDER BY published_at DESC NULLS LAST LIMIT 100", vals)
        rows = [dict(r) for r in cur.fetchall()]
        for r in rows:
            if r.get("published_at"):
                r["published_at"] = r["published_at"].isoformat()
            if r.get("fetched_at"):
                r["fetched_at"] = r["fetched_at"].isoformat()
        return rows
    except Exception as e:
        print(f"[db] get_news_articles error: {e}")
        return []
    finally:
        conn.close()


# ──────────────────────────────────────────────────────────────────────────────
# Head-to-Head
# ──────────────────────────────────────────────────────────────────────────────

def save_h2h(sport: str, league: str, team_a: str, team_b: str,
             season: int, h2h_data: dict, source: str = ""):
    conn = get_conn()
    if conn is None:
        return
    # Normalise team order for consistent lookups
    ta, tb = sorted([team_a, team_b])
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO head_to_head
                (sport, league, team_a, team_b, season, h2h_json, source, updated_at)
            VALUES (%s,%s,%s,%s,%s,%s::jsonb,%s,NOW())
            ON CONFLICT (sport, team_a, team_b, season, source) DO UPDATE SET
                h2h_json   = EXCLUDED.h2h_json,
                updated_at = NOW()
        """, (sport, league, ta, tb, season, json.dumps(h2h_data), source))
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[db] save_h2h error: {e}")
    finally:
        conn.close()


def get_h2h(sport: str, team_a: str, team_b: str,
            season: int = None) -> dict | None:
    ta, tb = sorted([team_a, team_b])
    conn = get_conn()
    if conn is None:
        return None
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if season:
            cur.execute("""
                SELECT h2h_json FROM head_to_head
                WHERE sport=%s AND team_a=%s AND team_b=%s AND season=%s
                ORDER BY updated_at DESC LIMIT 1
            """, (sport, ta, tb, season))
        else:
            cur.execute("""
                SELECT h2h_json FROM head_to_head
                WHERE sport=%s AND team_a=%s AND team_b=%s
                ORDER BY season DESC, updated_at DESC LIMIT 1
            """, (sport, ta, tb))
        row = cur.fetchone()
        return dict(row["h2h_json"]) if row else None
    except Exception as e:
        print(f"[db] get_h2h error: {e}")
        return None
    finally:
        conn.close()


# ──────────────────────────────────────────────────────────────────────────────
# Match events
# ──────────────────────────────────────────────────────────────────────────────

def save_match_events(events: list):
    if not events:
        return
    conn = get_conn()
    if conn is None:
        return
    try:
        cur = conn.cursor()
        for ev in events:
            try:
                cur.execute("""
                    INSERT INTO match_events
                        (sport, league, home_team, away_team, game_date,
                         event_type, minute, player_name, team, detail, source)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    ev.get("sport"), ev.get("league"),
                    ev.get("home_team"), ev.get("away_team"), ev.get("game_date"),
                    ev.get("event_type"), ev.get("minute"), ev.get("player_name"),
                    ev.get("team"), ev.get("detail","")[:500], ev.get("source",""),
                ))
            except Exception:
                pass
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[db] save_match_events error: {e}")
    finally:
        conn.close()


# ──────────────────────────────────────────────────────────────────────────────
# Analysis cache  (stores full game-card + parlay results per day)
# ──────────────────────────────────────────────────────────────────────────────

def save_analysis_cache(data: dict, cache_date=None):
    """
    Save the full analysis result (game cards, parlays, picks) for today.
    On a second run within the same day the row is updated in-place.
    data: serialisable dict (all values must be JSON-safe).
    """
    if not data:
        return
    cdate = cache_date or datetime.date.today()
    conn = get_conn()
    if conn is None:
        return
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO analysis_cache (cache_date, data_json, updated_at)
            VALUES (%s, %s::jsonb, NOW())
            ON CONFLICT (cache_date) DO UPDATE SET
                data_json  = EXCLUDED.data_json,
                updated_at = NOW()
        """, (cdate, json.dumps(data)))
        conn.commit()
        print(f"[db] analysis_cache saved for {cdate}")
    except Exception as e:
        conn.rollback()
        print(f"[db] save_analysis_cache error: {e}")
    finally:
        conn.close()


def get_analysis_cache(max_age_hours: int = 22, cache_date=None) -> "dict | None":
    """
    Return today's cached analysis data if it was saved within max_age_hours.
    Returns None when no fresh cache exists — caller should run full analysis.
    The returned dict also contains '_updated_at' (ISO string) for display.
    """
    cdate = cache_date or datetime.date.today()
    conn = get_conn()
    if conn is None:
        return None
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT data_json, updated_at
            FROM   analysis_cache
            WHERE  cache_date = %s
              AND  updated_at > NOW() - (INTERVAL '1 hour' * %s)
        """, (cdate, max_age_hours))
        row = cur.fetchone()
        if not row:
            return None
        data = dict(row["data_json"])
        ts = row["updated_at"]
        data["_updated_at"] = ts.strftime("%b %d %I:%M %p ET") if hasattr(ts, "strftime") else str(ts)[:16]
        return data
    except Exception as e:
        print(f"[db] get_analysis_cache error: {e}")
        return None
    finally:
        conn.close()


# ──────────────────────────────────────────────────────────────────────────────
# Phone numbers  (SMS recipient list)
# ──────────────────────────────────────────────────────────────────────────────

def add_phone_number(phone: str, label: str = "") -> tuple[bool, str]:
    """Add or reactivate a phone number in the SMS recipient list."""
    conn = get_conn()
    if conn is None:
        return False, "Database unavailable"
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO phone_numbers (phone, label, active)
            VALUES (%s, %s, TRUE)
            ON CONFLICT (phone) DO UPDATE SET
                label  = COALESCE(NULLIF(EXCLUDED.label, ''), phone_numbers.label),
                active = TRUE
        """, (phone.strip(), label.strip()))
        conn.commit()
        return True, "ok"
    except Exception as e:
        conn.rollback()
        print(f"[db] add_phone_number error: {e}")
        return False, str(e)
    finally:
        conn.close()


def remove_phone_number(phone: str) -> bool:
    """Permanently delete a phone number from the recipient list."""
    conn = get_conn()
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM phone_numbers WHERE phone = %s", (phone.strip(),))
        conn.commit()
        return cur.rowcount > 0
    except Exception as e:
        conn.rollback()
        print(f"[db] remove_phone_number error: {e}")
        return False
    finally:
        conn.close()


def get_phone_numbers(active_only: bool = True) -> list:
    """Return all registered phone numbers, optionally only active ones."""
    conn = get_conn()
    if conn is None:
        return []
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if active_only:
            cur.execute("SELECT * FROM phone_numbers WHERE active = TRUE ORDER BY added_at")
        else:
            cur.execute("SELECT * FROM phone_numbers ORDER BY added_at")
        rows = [dict(r) for r in cur.fetchall()]
        for r in rows:
            if r.get("added_at"):
                r["added_at"] = r["added_at"].isoformat()
        return rows
    except Exception as e:
        print(f"[db] get_phone_numbers error: {e}")
        return []
    finally:
        conn.close()


# ──────────────────────────────────────────────────────────────────────────────
# Predictions  (save every bet the bot recommends + track outcomes)
# ──────────────────────────────────────────────────────────────────────────────

def save_predictions(predictions: list) -> int:
    """
    Bulk-insert new predictions.
    Returns count of rows inserted.
    """
    if not predictions:
        return 0
    conn = get_conn()
    if conn is None:
        return 0
    saved = 0
    try:
        cur = conn.cursor()
        for p in predictions:
            try:
                cur.execute("""
                    INSERT INTO predictions
                        (game_key, sport, bet_type, pick, line, odds_am, dec_odds,
                         model_prob, confidence, safety_label, game_date, game_time,
                         home_team, away_team, home_starter, away_starter,
                         outcome, sentiment_score, news_snippet)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'PENDING',%s,%s)
                    ON CONFLICT DO NOTHING
                """, (
                    p.get("game_key"), p.get("sport","mlb"), p.get("bet_type"),
                    p.get("pick"), p.get("line"), p.get("odds_am"), p.get("dec_odds"),
                    p.get("model_prob"), p.get("confidence"), p.get("safety_label"),
                    p.get("game_date"), p.get("game_time"),
                    p.get("home_team"), p.get("away_team"),
                    p.get("home_starter"), p.get("away_starter"),
                    p.get("sentiment_score"), p.get("news_snippet","")[:500],
                ))
                saved += 1
            except Exception:
                pass
        conn.commit()
        print(f"[db] saved {saved} predictions")
        return saved
    except Exception as e:
        conn.rollback()
        print(f"[db] save_predictions error: {e}")
        return 0
    finally:
        conn.close()


def update_prediction_outcome(game_key: str, game_date: str, outcome: str,
                               actual_result: str = ""):
    """Mark pending predictions for a game as WIN/LOSS/PUSH."""
    conn = get_conn()
    if conn is None:
        return
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE predictions
            SET outcome = %s, actual_result = %s, resolved_at = NOW()
            WHERE game_key = %s AND game_date = %s AND outcome = 'PENDING'
        """, (outcome.upper(), actual_result, game_key, game_date))
        conn.commit()
        print(f"[db] resolved {cur.rowcount} predictions → {outcome}")
    except Exception as e:
        conn.rollback()
        print(f"[db] update_prediction_outcome error: {e}")
    finally:
        conn.close()


def get_predictions(days: int = 30, outcome: str = None,
                    bet_type: str = None) -> list:
    """Fetch prediction history."""
    conn = get_conn()
    if conn is None:
        return []
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        wheres = ["predicted_at > NOW() - (INTERVAL '1 day' * %s)"]
        vals   = [days]
        if outcome:
            wheres.append("outcome = %s"); vals.append(outcome.upper())
        if bet_type:
            wheres.append("bet_type = %s"); vals.append(bet_type)
        cur.execute(
            f"SELECT * FROM predictions WHERE {' AND '.join(wheres)} "
            "ORDER BY predicted_at DESC LIMIT 500", vals
        )
        rows = [dict(r) for r in cur.fetchall()]
        for r in rows:
            for k in ("game_date","predicted_at","resolved_at"):
                if r.get(k):
                    r[k] = r[k].isoformat() if hasattr(r[k],"isoformat") else str(r[k])
        return rows
    except Exception as e:
        print(f"[db] get_predictions error: {e}")
        return []
    finally:
        conn.close()


def get_performance_stats() -> dict:
    """Return win/loss/push counts and ROI for prediction tracking."""
    conn = get_conn()
    if conn is None:
        return {}
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT
                COUNT(*) FILTER (WHERE outcome = 'WIN')     AS wins,
                COUNT(*) FILTER (WHERE outcome = 'LOSS')    AS losses,
                COUNT(*) FILTER (WHERE outcome = 'PUSH')    AS pushes,
                COUNT(*) FILTER (WHERE outcome = 'PENDING') AS pending,
                COUNT(*) AS total,
                ROUND(AVG(CASE WHEN outcome='WIN' THEN 1.0
                               WHEN outcome='LOSS' THEN 0.0 END) * 100, 1) AS hit_rate,
                ROUND(AVG(model_prob)*100, 1) AS avg_confidence,
                COUNT(DISTINCT bet_type) AS bet_types_used
            FROM predictions
            WHERE predicted_at > NOW() - INTERVAL '90 days'
        """)
        row = cur.fetchone()
        stats = dict(row) if row else {}
        # By bet type
        cur.execute("""
            SELECT bet_type,
                   COUNT(*) FILTER (WHERE outcome='WIN')  AS wins,
                   COUNT(*) FILTER (WHERE outcome='LOSS') AS losses,
                   COUNT(*) AS total
            FROM predictions
            WHERE predicted_at > NOW() - INTERVAL '90 days'
              AND outcome IN ('WIN','LOSS')
            GROUP BY bet_type ORDER BY total DESC
        """)
        stats["by_bet_type"] = [dict(r) for r in cur.fetchall()]
        # Last 30 days trend
        cur.execute("""
            SELECT game_date::text AS date,
                   COUNT(*) FILTER (WHERE outcome='WIN')  AS wins,
                   COUNT(*) FILTER (WHERE outcome='LOSS') AS losses
            FROM predictions
            WHERE game_date >= CURRENT_DATE - INTERVAL '30 days'
              AND outcome IN ('WIN','LOSS')
            GROUP BY game_date ORDER BY game_date
        """)
        stats["daily_trend"] = [dict(r) for r in cur.fetchall()]
        return stats
    except Exception as e:
        print(f"[db] get_performance_stats error: {e}")
        return {}
    finally:
        conn.close()


def get_prop_performance_stats() -> dict:
    """Return prop hit-rate breakdown by prop_type for last 90 days."""
    conn = get_conn()
    if conn is None:
        return {}
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        # Overall prop stats
        cur.execute("""
            SELECT
                COUNT(*) FILTER (WHERE outcome='WIN')     AS wins,
                COUNT(*) FILTER (WHERE outcome='LOSS')    AS losses,
                COUNT(*) FILTER (WHERE outcome='PUSH')    AS pushes,
                COUNT(*) FILTER (WHERE outcome='PENDING') AS pending,
                COUNT(*) AS total,
                ROUND(AVG(CASE WHEN outcome='WIN' THEN 1.0
                               WHEN outcome='LOSS' THEN 0.0 END)*100, 1) AS hit_rate
            FROM prop_history
            WHERE game_date >= CURRENT_DATE - INTERVAL '90 days'
        """)
        row = cur.fetchone()
        stats = dict(row) if row else {}
        # By prop type
        cur.execute("""
            SELECT prop_type,
                   recommendation,
                   COUNT(*) FILTER (WHERE outcome='WIN')  AS wins,
                   COUNT(*) FILTER (WHERE outcome='LOSS') AS losses,
                   COUNT(*) AS total,
                   ROUND(AVG(CASE WHEN outcome='WIN' THEN 1.0
                                  WHEN outcome='LOSS' THEN 0.0 END)*100, 1) AS hit_rate
            FROM prop_history
            WHERE game_date >= CURRENT_DATE - INTERVAL '90 days'
              AND outcome IN ('WIN','LOSS')
            GROUP BY prop_type, recommendation
            ORDER BY total DESC
        """)
        stats["by_prop_type"] = [dict(r) for r in cur.fetchall()]
        # Last 30 days trend
        cur.execute("""
            SELECT game_date::text AS date,
                   COUNT(*) FILTER (WHERE outcome='WIN')  AS wins,
                   COUNT(*) FILTER (WHERE outcome='LOSS') AS losses
            FROM prop_history
            WHERE game_date >= CURRENT_DATE - INTERVAL '30 days'
              AND outcome IN ('WIN','LOSS')
            GROUP BY game_date ORDER BY game_date
        """)
        stats["daily_trend"] = [dict(r) for r in cur.fetchall()]
        return stats
    except Exception as e:
        print(f"[db] get_prop_performance_stats error: {e}")
        return {}
    finally:
        conn.close()


def get_pending_props(days_back: int = 3) -> list:
    """Return PENDING prop picks from the last N days for resolution."""
    conn = get_conn()
    if conn is None:
        return []
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT id, player_name, team, game_date::text, prop_type, line,
                   over_prob, under_prob, recommendation, stats_json
            FROM prop_history
            WHERE outcome = 'PENDING'
              AND game_date >= CURRENT_DATE - (%s * INTERVAL '1 day')
              AND game_date < CURRENT_DATE
            ORDER BY game_date DESC
        """, (days_back,))
        return [dict(r) for r in cur.fetchall()]
    except Exception as e:
        print(f"[db] get_pending_props error: {e}")
        return []
    finally:
        conn.close()


def update_prop_outcome(prop_id: int, actual_value: float, outcome: str):
    """Set actual_value + WIN/LOSS/PUSH/PENDING on a prop_history row."""
    conn = get_conn()
    if conn is None:
        return
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE prop_history
            SET actual_value = %s, outcome = %s, resolved_at = NOW()
            WHERE id = %s
        """, (actual_value, outcome, prop_id))
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[db] update_prop_outcome error: {e}")
    finally:
        conn.close()


# ──────────────────────────────────────────────────────────────────────────────
# Sentiment scores
# ──────────────────────────────────────────────────────────────────────────────

def save_sentiment(entity: str, entity_type: str, source: str,
                   score: float, volume: int = 0, keywords: str = ""):
    """Save or update a sentiment score for today."""
    conn = get_conn()
    if conn is None:
        return
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO sentiment_scores
                (entity, entity_type, source, score, volume, keywords, computed_at)
            VALUES (%s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (entity, source, DATE(computed_at)) DO UPDATE SET
                score       = EXCLUDED.score,
                volume      = EXCLUDED.volume,
                keywords    = EXCLUDED.keywords,
                computed_at = NOW()
        """, (entity, entity_type, source, score, volume, keywords[:500]))
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[db] save_sentiment error: {e}")
    finally:
        conn.close()


def get_sentiment(entity: str, hours: int = 24) -> dict:
    """Get latest sentiment scores for an entity (team or player)."""
    conn = get_conn()
    if conn is None:
        return {}
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT source, score, volume, keywords, computed_at
            FROM sentiment_scores
            WHERE entity ILIKE %s
              AND computed_at > NOW() - (INTERVAL '1 hour' * %s)
            ORDER BY source, computed_at DESC
        """, (entity, hours))
        rows = cur.fetchall()
        result = {}
        for r in rows:
            result[r["source"]] = {
                "score": float(r["score"] or 0),
                "volume": r["volume"],
                "keywords": r["keywords"],
            }
        return result
    except Exception as e:
        print(f"[db] get_sentiment error: {e}")
        return {}
    finally:
        conn.close()


# ──────────────────────────────────────────────────────────────────────────────
# Player trends
# ──────────────────────────────────────────────────────────────────────────────

def save_player_trends(player_name: str, team: str, season: int,
                       stat_type: str, trends: dict, player_id: int = None):
    """Save historical trend data for a player."""
    conn = get_conn()
    if conn is None:
        return
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO player_trends
                (player_name, player_id, team, season, stat_type,
                 last_5, last_10, season_avg, vs_lefty, vs_righty,
                 home_avg, away_avg, updated_at)
            VALUES (%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,%s,%s,%s,%s,%s,NOW())
            ON CONFLICT (player_name, season, stat_type) DO UPDATE SET
                team       = EXCLUDED.team,
                last_5     = EXCLUDED.last_5,
                last_10    = EXCLUDED.last_10,
                season_avg = EXCLUDED.season_avg,
                vs_lefty   = EXCLUDED.vs_lefty,
                vs_righty  = EXCLUDED.vs_righty,
                home_avg   = EXCLUDED.home_avg,
                away_avg   = EXCLUDED.away_avg,
                updated_at = NOW()
        """, (
            player_name, player_id, team, season, stat_type,
            json.dumps(trends.get("last_5", {})),
            json.dumps(trends.get("last_10", {})),
            trends.get("season_avg"), trends.get("vs_lefty"),
            trends.get("vs_righty"), trends.get("home_avg"), trends.get("away_avg"),
        ))
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[db] save_player_trends error: {e}")
    finally:
        conn.close()


def get_player_trends(player_name: str, season: int = None) -> list:
    conn = get_conn()
    if conn is None:
        return []
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if season:
            cur.execute("""
                SELECT * FROM player_trends
                WHERE player_name ILIKE %s AND season = %s
                ORDER BY stat_type
            """, (f"%{player_name}%", season))
        else:
            cur.execute("""
                SELECT * FROM player_trends
                WHERE player_name ILIKE %s
                ORDER BY season DESC, stat_type
                LIMIT 20
            """, (f"%{player_name}%",))
        return [dict(r) for r in cur.fetchall()]
    except Exception as e:
        print(f"[db] get_player_trends error: {e}")
        return []
    finally:
        conn.close()


# ──────────────────────────────────────────────────────────────────────────────
# Tracked Parlays (user-built + bot-generated)
# ──────────────────────────────────────────────────────────────────────────────

def save_tracked_parlay(name: str, legs: list, combined_odds: float,
                        stake_usd: float = 0) -> int:
    """Save a tracked parlay. Returns the new parlay ID."""
    conn = get_conn()
    if conn is None:
        return 0
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO tracked_parlays
                (name, legs_json, combined_odds, stake_usd, outcome)
            VALUES (%s, %s::jsonb, %s, %s, 'PENDING')
            RETURNING id
        """, (name, json.dumps(legs), combined_odds, stake_usd))
        row = cur.fetchone()
        conn.commit()
        return row[0] if row else 0
    except Exception as e:
        conn.rollback()
        print(f"[db] save_tracked_parlay error: {e}")
        return 0
    finally:
        conn.close()


def get_tracked_parlays(include_resolved: bool = False) -> list:
    conn = get_conn()
    if conn is None:
        return []
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if include_resolved:
            cur.execute("SELECT * FROM tracked_parlays ORDER BY created_at DESC LIMIT 100")
        else:
            cur.execute("""
                SELECT * FROM tracked_parlays
                WHERE outcome = 'PENDING'
                ORDER BY created_at DESC LIMIT 50
            """)
        rows = [dict(r) for r in cur.fetchall()]
        for r in rows:
            for k in ("created_at", "resolved_at"):
                if r.get(k):
                    r[k] = r[k].isoformat()
        return rows
    except Exception as e:
        print(f"[db] get_tracked_parlays error: {e}")
        return []
    finally:
        conn.close()


def resolve_tracked_parlay(parlay_id: int, outcome: str, payout: float = 0):
    conn = get_conn()
    if conn is None:
        return
    try:
        cur = conn.cursor()
        cur.execute("""
            UPDATE tracked_parlays
            SET outcome = %s, payout_usd = %s, resolved_at = NOW()
            WHERE id = %s
        """, (outcome.upper(), payout, parlay_id))
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[db] resolve_tracked_parlay error: {e}")
    finally:
        conn.close()

