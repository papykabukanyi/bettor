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
    detected_at     TIMESTAMPTZ DEFAULT NOW()
);

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

