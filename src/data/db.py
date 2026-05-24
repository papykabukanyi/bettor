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
import hashlib

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
        print("[db] psycopg2 not available")
        return None
    url = DATABASE_URL or os.getenv("DATABASE_URL", "")
    if not url:
        print("[db] DATABASE_URL not set")
        return None
    try:
        return psycopg2.connect(url, connect_timeout=10)
    except Exception as e:
        print(f"[db] connection error: {e}")
        return None


# Cache table columns to handle schema drift across deployments.
_TABLE_COLS_CACHE: dict[str, set[str]] = {}


def _get_table_columns(conn, table_name: str) -> set[str]:
    cached = _TABLE_COLS_CACHE.get(table_name)
    if cached is not None:
        return cached
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name = %s",
            (table_name,)
        )
        cols = {r[0] for r in cur.fetchall()}
        _TABLE_COLS_CACHE[table_name] = cols
        return cols
    except Exception as e:
        print(f"[db] column lookup error ({table_name}): {e}")
        _TABLE_COLS_CACHE[table_name] = set()
        return set()


def _uid_part(value) -> str:
    """Normalize values before hashing to build deterministic tracking IDs."""
    if value is None:
        return ""
    if isinstance(value, datetime.datetime):
        return value.date().isoformat()
    if isinstance(value, datetime.date):
        return value.isoformat()
    if isinstance(value, float):
        txt = f"{value:.4f}".rstrip("0").rstrip(".")
        return txt
    return str(value).strip().lower()


def _make_tracking_uid(prefix: str, *parts) -> str:
    raw = "|".join(_uid_part(p) for p in parts)
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:20]
    return f"{prefix}_{digest}"


def _prediction_uid(payload: dict) -> str:
    """Deterministic unique ID for a game/prediction row."""
    date_scope = (
        payload.get("game_date")
        or payload.get("date")
        or payload.get("run_date")
        or _cache_date_default().isoformat()
    )
    return _make_tracking_uid(
        "pred",
        payload.get("sport", ""),
        date_scope,
        payload.get("game_key", ""),
        payload.get("bet_type", ""),
        payload.get("pick", ""),
        payload.get("line", ""),
    )


def _prop_uid(payload: dict, game_date=None) -> str:
    """Deterministic unique ID for a player prop row."""
    date_scope = (
        game_date
        or payload.get("date")
        or payload.get("game_date")
        or payload.get("run_date")
        or _cache_date_default().isoformat()
    )
    return _make_tracking_uid(
        "prop",
        payload.get("sport", ""),
        date_scope,
        payload.get("game_key") or payload.get("game") or "",
        payload.get("name") or payload.get("player_name") or "",
        payload.get("team", ""),
        payload.get("stat_type") or payload.get("prop_type") or "",
        payload.get("line", ""),
        payload.get("direction") or payload.get("recommendation") or "",
    )


def _normalize_parlay_legs_for_uid(legs: list) -> list:
    normalized = []
    for leg in (legs or []):
        if not isinstance(leg, dict):
            continue
        normalized.append({
            "prediction_uid": _uid_part(leg.get("prediction_uid") or leg.get("bet_uid") or ""),
            "source": _uid_part(leg.get("source") or ""),
            "sport": _uid_part(leg.get("sport") or ""),
            "game": _uid_part(leg.get("game") or leg.get("game_key") or ""),
            "bet_type": _uid_part(leg.get("bet_type") or ""),
            "label": _uid_part(leg.get("label") or leg.get("pick") or ""),
            "line": _uid_part(leg.get("line") or ""),
            "direction": _uid_part(leg.get("direction") or leg.get("recommendation") or ""),
            "player": _uid_part(leg.get("player_name") or leg.get("name") or ""),
            "prop_type": _uid_part(leg.get("prop_type") or leg.get("stat_type") or ""),
        })
    normalized.sort(key=lambda row: json.dumps(row, sort_keys=True))
    return normalized


def _parlay_uid(name: str, legs: list, created_date=None) -> str:
    """Deterministic unique ID for tracked parlays (date-scoped)."""
    day = created_date or _cache_date_default()
    legs_norm = _normalize_parlay_legs_for_uid(legs)
    return _make_tracking_uid(
        "par",
        day,
        json.dumps(legs_norm, sort_keys=True),
    )

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
    bet_uid         VARCHAR(80),
    game_key        VARCHAR(200),
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
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='prop_history' AND column_name='game_key') THEN
        ALTER TABLE prop_history ADD COLUMN game_key VARCHAR(200);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='prop_history' AND column_name='bet_uid') THEN
        ALTER TABLE prop_history ADD COLUMN bet_uid VARCHAR(80);
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

-- ── Daily run log: one row per morning analysis run ──
CREATE TABLE IF NOT EXISTS daily_runs (
    id           SERIAL PRIMARY KEY,
    run_id       VARCHAR(50) NOT NULL UNIQUE,  -- e.g. 'MLB-2026-05-03'
    run_date     DATE        NOT NULL,
    status       VARCHAR(20) DEFAULT 'RUNNING',  -- RUNNING, DONE, ARCHIVED
    games_today  INTEGER     DEFAULT 0,
    games_tmrw   INTEGER     DEFAULT 0,
    props_count  INTEGER     DEFAULT 0,
    parlays_count INTEGER    DEFAULT 0,
    started_at   TIMESTAMPTZ DEFAULT NOW(),
    finished_at  TIMESTAMPTZ,
    archived_at  TIMESTAMPTZ
);

-- Idempotent migrations for run_date tracking
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='predictions' AND column_name='run_date') THEN
        ALTER TABLE predictions ADD COLUMN run_date DATE;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='prop_history' AND column_name='run_date') THEN
        ALTER TABLE prop_history ADD COLUMN run_date DATE;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='predictions' AND column_name='run_id') THEN
        ALTER TABLE predictions ADD COLUMN run_id VARCHAR(50);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='prop_history' AND column_name='run_id') THEN
        ALTER TABLE prop_history ADD COLUMN run_id VARCHAR(50);
    END IF;
END $$;

-- ── Analysis cache: stores full game-card + parlay results per day ──
CREATE TABLE IF NOT EXISTS analysis_cache (
    id         SERIAL PRIMARY KEY,
    cache_date DATE        NOT NULL UNIQUE,
    data_json  JSONB       NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- ── MLB Predictions: every prediction the bot makes ──
CREATE TABLE IF NOT EXISTS predictions (
    id              SERIAL PRIMARY KEY,
    bet_uid         VARCHAR(80),
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
    computed_date DATE           NOT NULL DEFAULT CURRENT_DATE,
    UNIQUE(entity, source, computed_date)
);
CREATE INDEX IF NOT EXISTS idx_sentiment_entity ON sentiment_scores(entity, computed_at);

-- ── Tracked parlays: user-built and bot-generated parlays ──
CREATE TABLE IF NOT EXISTS tracked_parlays (
    id             SERIAL PRIMARY KEY,
    parlay_uid     VARCHAR(80),
    name           VARCHAR(200),
    legs_json      JSONB,          -- [{pick, game, odds, type}, ...]
    combined_odds  NUMERIC(8,2),
    stake_usd      NUMERIC(8,2)    DEFAULT 0,
    created_at     TIMESTAMPTZ     DEFAULT NOW(),
    resolved_at    TIMESTAMPTZ,
    outcome        VARCHAR(20)     DEFAULT 'PENDING',
    payout_usd     NUMERIC(8,2)
);

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='predictions' AND column_name='bet_uid') THEN
        ALTER TABLE predictions ADD COLUMN bet_uid VARCHAR(80);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='prop_history' AND column_name='bet_uid') THEN
        ALTER TABLE prop_history ADD COLUMN bet_uid VARCHAR(80);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='tracked_parlays' AND column_name='parlay_uid') THEN
        ALTER TABLE tracked_parlays ADD COLUMN parlay_uid VARCHAR(80);
    END IF;
END $$;

-- Backfill stable IDs for legacy rows that predate tracking UID support.
UPDATE predictions
SET bet_uid = CONCAT('pred_legacy_', id::text)
WHERE bet_uid IS NULL;

UPDATE prop_history
SET bet_uid = CONCAT('prop_legacy_', id::text)
WHERE bet_uid IS NULL;

UPDATE tracked_parlays
SET parlay_uid = CONCAT('par_legacy_', id::text)
WHERE parlay_uid IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_prop_history_bet_uid ON prop_history(bet_uid);
CREATE UNIQUE INDEX IF NOT EXISTS uq_predictions_bet_uid ON predictions(bet_uid);
CREATE UNIQUE INDEX IF NOT EXISTS uq_tracked_parlays_uid ON tracked_parlays(parlay_uid);

-- ── Deep enrichment: rest / venue / coaching / weather per game ──────────────
CREATE TABLE IF NOT EXISTS game_enrichment (
    id              SERIAL PRIMARY KEY,
    sport           VARCHAR(20)   NOT NULL,
    home_team       VARCHAR(100)  NOT NULL,
    away_team       VARCHAR(100)  NOT NULL,
    game_date       DATE          NOT NULL,
    -- Rest & fatigue
    home_rest_days      INTEGER,
    away_rest_days      INTEGER,
    home_back_to_back   BOOLEAN   DEFAULT FALSE,
    away_back_to_back   BOOLEAN   DEFAULT FALSE,
    home_games_last_7   INTEGER,
    away_games_last_7   INTEGER,
    home_fatigue        NUMERIC(4,3),
    away_fatigue        NUMERIC(4,3),
    away_travel_km      NUMERIC(8,1),
    long_road_trip      BOOLEAN   DEFAULT FALSE,
    -- Head-to-head summary
    h2h_home_wins       INTEGER,
    h2h_away_wins       INTEGER,
    h2h_draws           INTEGER,
    h2h_total           INTEGER,
    h2h_avg_total       NUMERIC(5,2),
    h2h_last_5          JSONB,
    -- Venue history
    venue_name          VARCHAR(200),
    venue_home_wins     INTEGER,
    venue_away_wins     INTEGER,
    venue_draws         INTEGER,
    venue_total_games   INTEGER,
    venue_avg_total     NUMERIC(5,2),
    -- Coaching
    home_coach_name     VARCHAR(150),
    home_coach_win_pct  NUMERIC(4,3),
    away_coach_name     VARCHAR(150),
    away_coach_win_pct  NUMERIC(4,3),
    -- Weather (outdoor sports)
    weather_condition   VARCHAR(50),
    weather_temp_f      NUMERIC(5,1),
    weather_wind_kph    NUMERIC(5,1),
    weather_precip_mm   NUMERIC(5,2),
    weather_humidity    NUMERIC(5,1),
    -- Full enrichment JSON (all signals)
    enrichment_json     JSONB,
    fetched_at          TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(sport, home_team, away_team, game_date)
);
CREATE INDEX IF NOT EXISTS idx_game_enrich_date ON game_enrichment(sport, game_date);

-- ── Player form cache (last-5 / last-10 rolling averages) ────────────────────
CREATE TABLE IF NOT EXISTS player_form_cache (
    id              SERIAL PRIMARY KEY,
    sport           VARCHAR(20)   NOT NULL,
    player_name     VARCHAR(150)  NOT NULL,
    stat_type       VARCHAR(50)   NOT NULL,
    avg_last_5      NUMERIC(6,3),
    avg_last_10     NUMERIC(6,3),
    trend_direction VARCHAR(10),  -- 'up', 'down', 'neutral'
    games_collected INTEGER,
    form_json       JSONB,
    computed_date   DATE          NOT NULL DEFAULT CURRENT_DATE,
    fetched_at      TIMESTAMPTZ   DEFAULT NOW(),
    UNIQUE(sport, player_name, stat_type, computed_date)
);
CREATE INDEX IF NOT EXISTS idx_player_form_name ON player_form_cache(sport, player_name);

-- ── Venue coordinates table (lat/lon for weather + travel) ───────────────────
CREATE TABLE IF NOT EXISTS venue_coords (
    id          SERIAL PRIMARY KEY,
    team_name   VARCHAR(150) NOT NULL UNIQUE,
    venue_name  VARCHAR(200),
    sport       VARCHAR(20),
    latitude    NUMERIC(9,6),
    longitude   NUMERIC(9,6),
    city        VARCHAR(100),
    country     VARCHAR(60),
    source      VARCHAR(50),
    updated_at  TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_venue_coords_team ON venue_coords(team_name);

-- ── Idempotent migrations: sentiment signal columns ───────────────────────────
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='predictions' AND column_name='signal_type') THEN
        ALTER TABLE predictions ADD COLUMN signal_type    VARCHAR(30);
        ALTER TABLE predictions ADD COLUMN active_sources TEXT;
        ALTER TABLE predictions ADD COLUMN injury_flag    BOOLEAN DEFAULT FALSE;
        ALTER TABLE predictions ADD COLUMN momentum_flag  BOOLEAN DEFAULT FALSE;
        ALTER TABLE predictions ADD COLUMN lineup_flag    BOOLEAN DEFAULT FALSE;
    END IF;
END $$;

-- ── Idempotent migrations: Kalshi market link + investor grade ──────────────
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='predictions' AND column_name='kalshi_ticker') THEN
        ALTER TABLE predictions ADD COLUMN kalshi_ticker        VARCHAR(120);
        ALTER TABLE predictions ADD COLUMN kalshi_event_ticker  VARCHAR(120);
        ALTER TABLE predictions ADD COLUMN kalshi_series_ticker VARCHAR(120);
        ALTER TABLE predictions ADD COLUMN kalshi_side          VARCHAR(10);
        ALTER TABLE predictions ADD COLUMN kalshi_price_cents   INTEGER;
        ALTER TABLE predictions ADD COLUMN kalshi_status        VARCHAR(20);
        ALTER TABLE predictions ADD COLUMN grade                VARCHAR(2);
        ALTER TABLE predictions ADD COLUMN investor_score       NUMERIC(5,2);
    END IF;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='predictions' AND column_name='kalshi_series_ticker') THEN
        ALTER TABLE predictions ADD COLUMN kalshi_series_ticker VARCHAR(120);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='predictions' AND column_name='polymarket_ticker') THEN
        ALTER TABLE predictions ADD COLUMN polymarket_ticker VARCHAR(160);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='predictions' AND column_name='polymarket_market_slug') THEN
        ALTER TABLE predictions ADD COLUMN polymarket_market_slug VARCHAR(200);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='predictions' AND column_name='polymarket_event_slug') THEN
        ALTER TABLE predictions ADD COLUMN polymarket_event_slug VARCHAR(200);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='predictions' AND column_name='polymarket_series_ticker') THEN
        ALTER TABLE predictions ADD COLUMN polymarket_series_ticker VARCHAR(120);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='predictions' AND column_name='polymarket_side') THEN
        ALTER TABLE predictions ADD COLUMN polymarket_side VARCHAR(10);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='predictions' AND column_name='polymarket_price') THEN
        ALTER TABLE predictions ADD COLUMN polymarket_price NUMERIC(8,4);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='predictions' AND column_name='polymarket_status') THEN
        ALTER TABLE predictions ADD COLUMN polymarket_status VARCHAR(20);
    END IF;
END $$;

-- ── Unified multi-sport training history tables ───────────────────────────
CREATE TABLE IF NOT EXISTS training_game_history (
    id                 SERIAL PRIMARY KEY,
    sport              VARCHAR(20)  NOT NULL,
    league             VARCHAR(80),
    season             INTEGER,
    game_date          DATE         NOT NULL,
    game_key           VARCHAR(220) NOT NULL,
    home_team          VARCHAR(120) NOT NULL,
    away_team          VARCHAR(120) NOT NULL,
    home_score         INTEGER,
    away_score         INTEGER,
    status             VARCHAR(40),
    source             VARCHAR(50),
    raw_json           JSONB,
    ingested_at        TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(sport, game_key, source)
);

CREATE TABLE IF NOT EXISTS training_player_history (
    id                 SERIAL PRIMARY KEY,
    sport              VARCHAR(20)  NOT NULL,
    season             INTEGER,
    game_date          DATE,
    game_key           VARCHAR(220),
    player_name        VARCHAR(160) NOT NULL,
    team               VARCHAR(120),
    stat_type          VARCHAR(60)  NOT NULL,
    stat_value         NUMERIC(10,3),
    source             VARCHAR(50),
    raw_json           JSONB,
    ingested_at        TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(sport, season, game_key, player_name, stat_type, source)
);

CREATE TABLE IF NOT EXISTS training_injury_history (
    id                 SERIAL PRIMARY KEY,
    sport              VARCHAR(20)  NOT NULL,
    injury_date        DATE         NOT NULL,
    team               VARCHAR(120),
    player_name        VARCHAR(160) NOT NULL,
    status             VARCHAR(80),
    injury_type        VARCHAR(120),
    detail             TEXT,
    source             VARCHAR(50),
    raw_json           JSONB,
    ingested_at        TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(sport, injury_date, player_name, team, source)
);

CREATE INDEX IF NOT EXISTS idx_train_game_sport_date
    ON training_game_history(sport, game_date);
CREATE INDEX IF NOT EXISTS idx_train_player_sport_date
    ON training_player_history(sport, game_date);
CREATE INDEX IF NOT EXISTS idx_train_injury_sport_date
    ON training_injury_history(sport, injury_date);
"""


def init_schema():
    """Create all tables. Called once at dashboard startup."""
    conn = get_conn()
    if conn is None:
        raise Exception("No DB connection - DATABASE_URL not set or connection failed")
    try:
        cur = conn.cursor()
        cur.execute(_SCHEMA)
        conn.commit()
        _TABLE_COLS_CACHE.clear()
        print("[db] schema ready")
        return True
    except Exception as e:
        conn.rollback()
        print(f"[db] schema init error: {e}")
        raise
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
        cols = _get_table_columns(conn, "games")
        has_season = "season" in cols
        has_external_id = "external_id" in cols

        insert_cols = [
            "sport", "league", "home_team", "away_team", "game_date", "game_time",
            "game_datetime", "status", "home_starter", "away_starter",
            "home_score", "away_score",
        ]
        values = [
            sport, league, home_team, away_team, game_date, game_time,
            game_datetime, status, home_starter, away_starter,
            home_score, away_score,
        ]
        if has_season:
            insert_cols.append("season")
            values.append(season)
        if has_external_id:
            insert_cols.append("external_id")
            values.append(str(external_id) if external_id else None)

        update_parts = [
            "status = EXCLUDED.status",
            "league = COALESCE(EXCLUDED.league, games.league)",
            "game_time = COALESCE(EXCLUDED.game_time, games.game_time)",
            "game_datetime = COALESCE(EXCLUDED.game_datetime, games.game_datetime)",
            "home_starter = COALESCE(EXCLUDED.home_starter, games.home_starter)",
            "away_starter = COALESCE(EXCLUDED.away_starter, games.away_starter)",
            "home_score = COALESCE(EXCLUDED.home_score, games.home_score)",
            "away_score = COALESCE(EXCLUDED.away_score, games.away_score)",
        ]
        if has_season:
            update_parts.append("season = COALESCE(EXCLUDED.season, games.season)")
        if has_external_id:
            update_parts.append("external_id = COALESCE(EXCLUDED.external_id, games.external_id)")
        update_parts.append("updated_at = NOW()")

        sql = f"""
            INSERT INTO games ({', '.join(insert_cols)})
            VALUES ({', '.join(['%s'] * len(insert_cols))})
            ON CONFLICT (sport, home_team, away_team, game_date) DO UPDATE SET
                {', '.join(update_parts)}
            RETURNING id
        """
        cur.execute(sql, values)
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


def get_injury_history(sport: str | None = None, days_back: int = 120) -> list[dict]:
    """Return injury rows over a longer lookback window for training timelines."""
    conn = get_conn()
    if conn is None:
        return []
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        days_back = max(1, int(days_back or 120))
        if sport:
            cur.execute(
                """
                SELECT sport, team, player_name, status, description, injury_type,
                       source, fetched_at
                FROM injury_reports
                WHERE sport = %s
                  AND fetched_at > NOW() - (INTERVAL '1 day' * %s)
                ORDER BY fetched_at DESC
                """,
                (sport, days_back),
            )
        else:
            cur.execute(
                """
                SELECT sport, team, player_name, status, description, injury_type,
                       source, fetched_at
                FROM injury_reports
                WHERE fetched_at > NOW() - (INTERVAL '1 day' * %s)
                ORDER BY fetched_at DESC
                """,
                (days_back,),
            )
        rows = [dict(r) for r in cur.fetchall()]
        for r in rows:
            if r.get("fetched_at"):
                r["fetched_at"] = r["fetched_at"].isoformat()
        return rows
    except Exception as e:
        print(f"[db] get_injury_history error: {e}")
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
    saved = 0
    try:
        cur = conn.cursor()
        cols = _get_table_columns(conn, "prop_history")
        has_bet_uid  = "bet_uid"  in cols
        has_game_key = "game_key" in cols
        has_run_id   = "run_id"   in cols
        has_run_date = "run_date" in cols
        for p in picks:
            pick_game_key = (p.get("game_key") or p.get("game") or "").strip()
            pick_game_date = p.get("date") or game_date or datetime.date.today()
            prop_type = p.get("stat_type")
            recommendation = p.get("direction")
            line_value = p.get("line")
            pick_bet_uid = _prop_uid(p, game_date=pick_game_date)
            stats_snap = json.dumps({
                "game_key":  pick_game_key,
                "era":       p.get("era"),     "k9":        p.get("k9"),
                "avg":       p.get("avg"),     "ops":       p.get("ops"),
                "xg":        p.get("xg"),      "xa":        p.get("xa"),
                "over_pct":  p.get("over_pct"),"under_pct": p.get("under_pct"),
                "league":    p.get("league"),  "game":      p.get("game"),
            })
            pick_run_id   = p.get("run_id")
            pick_run_date = p.get("run_date") or str(pick_game_date)
            try:
                if has_bet_uid and pick_bet_uid:
                    cur.execute(
                        """
                        SELECT 1 FROM prop_history
                        WHERE bet_uid = %s
                          AND outcome != 'ARCHIVED'
                        LIMIT 1
                        """,
                        (pick_bet_uid,),
                    )
                elif has_game_key and pick_game_key:
                    cur.execute(
                        """
                        SELECT 1 FROM prop_history
                        WHERE game_key = %s
                          AND player_name = %s
                          AND game_date = %s
                          AND prop_type = %s
                          AND recommendation = %s
                          AND COALESCE(line::text, '') = COALESCE(%s::text, '')
                          AND outcome != 'ARCHIVED'
                        LIMIT 1
                        """,
                        (
                            pick_game_key,
                            p.get("name"),
                            pick_game_date,
                            prop_type,
                            recommendation,
                            line_value,
                        ),
                    )
                else:
                    cur.execute(
                        """
                        SELECT 1 FROM prop_history
                        WHERE player_name = %s
                          AND team = %s
                          AND game_date = %s
                          AND prop_type = %s
                          AND recommendation = %s
                          AND COALESCE(line::text, '') = COALESCE(%s::text, '')
                          AND outcome != 'ARCHIVED'
                        LIMIT 1
                        """,
                        (
                            p.get("name"),
                            p.get("team"),
                            pick_game_date,
                            prop_type,
                            recommendation,
                            line_value,
                        ),
                    )
                if cur.fetchone():
                    continue

                # Build dynamic INSERT to support optional run_id / run_date columns
                base_cols = ["sport", "player_name", "team", "game_date", "prop_type",
                             "line", "over_prob", "under_prob", "recommendation", "stats_json"]
                base_vals = [
                    p.get("sport", "mlb"), p.get("name"), p.get("team"),
                    pick_game_date, prop_type, line_value,
                    (p.get("over_pct") or 50) / 100.0,
                    (p.get("under_pct") or 50) / 100.0,
                    recommendation, stats_snap,
                ]
                if has_bet_uid:
                    base_cols.insert(0, "bet_uid")
                    base_vals.insert(0, pick_bet_uid)
                if has_game_key:
                    base_cols.insert(0, "game_key")
                    base_vals.insert(0, pick_game_key or None)
                if has_run_id and pick_run_id:
                    base_cols.append("run_id");   base_vals.append(pick_run_id)
                if has_run_date:
                    base_cols.append("run_date"); base_vals.append(pick_run_date)

                ph = ", ".join(["%s"] * len(base_vals))
                cur.execute(
                    f"INSERT INTO prop_history ({', '.join(base_cols)}) VALUES ({ph})",
                    base_vals
                )
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
        cols = _get_table_columns(conn, "prop_history")
        bet_uid_col = "bet_uid" if "bet_uid" in cols else "NULL::text AS bet_uid"
        base = """
            SELECT {bet_uid_col}, sport, player_name, team, game_date, prop_type,
                   line, over_prob, under_prob, recommendation, stats_json, detected_at
            FROM   prop_history
            WHERE  game_date  = CURRENT_DATE
              AND  detected_at > NOW() - (INTERVAL '1 hour' * %s)
        """.format(bet_uid_col=bet_uid_col)
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


def has_prop_picks_for_date(game_date: "datetime.date | str", sport: str = "mlb") -> bool:
    """Return True if any non-archived prop_history rows exist for the given game_date."""
    conn = get_conn()
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        common_filter = " AND outcome != 'ARCHIVED'"
        if sport:
            cur.execute(
                f"""
                SELECT 1 FROM prop_history
                WHERE game_date = %s AND sport = %s
                  {common_filter}
                LIMIT 1
                """,
                (game_date, sport),
            )
        else:
            cur.execute(
                f"""
                SELECT 1 FROM prop_history
                WHERE game_date = %s
                  {common_filter}
                LIMIT 1
                """,
                (game_date,),
            )
        return cur.fetchone() is not None
    except Exception as e:
        print(f"[db] has_prop_picks_for_date error: {e}")
        return False
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
# Unified multi-sport training history
# ──────────────────────────────────────────────────────────────────────────────

def save_training_game_history(rows: list[dict]) -> int:
    """Upsert normalized historical game outcomes for model training."""
    if not rows:
        return 0
    conn = get_conn()
    if conn is None:
        return 0
    saved = 0
    try:
        cur = conn.cursor()
        for r in rows:
            try:
                cur.execute(
                    """
                    INSERT INTO training_game_history
                        (sport, league, season, game_date, game_key,
                         home_team, away_team, home_score, away_score,
                         status, source, raw_json, ingested_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,NOW())
                    ON CONFLICT (sport, game_key, source) DO UPDATE SET
                        league      = COALESCE(EXCLUDED.league, training_game_history.league),
                        season      = COALESCE(EXCLUDED.season, training_game_history.season),
                        game_date   = COALESCE(EXCLUDED.game_date, training_game_history.game_date),
                        home_team   = COALESCE(EXCLUDED.home_team, training_game_history.home_team),
                        away_team   = COALESCE(EXCLUDED.away_team, training_game_history.away_team),
                        home_score  = COALESCE(EXCLUDED.home_score, training_game_history.home_score),
                        away_score  = COALESCE(EXCLUDED.away_score, training_game_history.away_score),
                        status      = COALESCE(EXCLUDED.status, training_game_history.status),
                        raw_json    = COALESCE(EXCLUDED.raw_json, training_game_history.raw_json),
                        ingested_at = NOW()
                    """,
                    (
                        r.get("sport"),
                        r.get("league"),
                        r.get("season"),
                        r.get("game_date"),
                        r.get("game_key"),
                        r.get("home_team"),
                        r.get("away_team"),
                        r.get("home_score"),
                        r.get("away_score"),
                        r.get("status"),
                        r.get("source"),
                        json.dumps(r.get("raw_json") or {}),
                    ),
                )
                saved += 1
            except Exception:
                pass
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[db] save_training_game_history error: {e}")
        return 0
    finally:
        conn.close()
    return saved


def save_training_player_history(rows: list[dict]) -> int:
    """Upsert normalized historical player-level rows for model training."""
    if not rows:
        return 0
    conn = get_conn()
    if conn is None:
        return 0
    saved = 0
    try:
        cur = conn.cursor()
        for r in rows:
            try:
                cur.execute(
                    """
                    INSERT INTO training_player_history
                        (sport, season, game_date, game_key, player_name,
                         team, stat_type, stat_value, source, raw_json, ingested_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,NOW())
                    ON CONFLICT (sport, season, game_key, player_name, stat_type, source)
                    DO UPDATE SET
                        game_date   = COALESCE(EXCLUDED.game_date, training_player_history.game_date),
                        team        = COALESCE(EXCLUDED.team, training_player_history.team),
                        stat_value  = COALESCE(EXCLUDED.stat_value, training_player_history.stat_value),
                        raw_json    = COALESCE(EXCLUDED.raw_json, training_player_history.raw_json),
                        ingested_at = NOW()
                    """,
                    (
                        r.get("sport"),
                        r.get("season"),
                        r.get("game_date"),
                        r.get("game_key"),
                        r.get("player_name"),
                        r.get("team"),
                        r.get("stat_type"),
                        r.get("stat_value"),
                        r.get("source"),
                        json.dumps(r.get("raw_json") or {}),
                    ),
                )
                saved += 1
            except Exception:
                pass
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[db] save_training_player_history error: {e}")
        return 0
    finally:
        conn.close()
    return saved


def save_training_injury_history(rows: list[dict]) -> int:
    """Upsert normalized historical injury timeline rows for model training."""
    if not rows:
        return 0
    conn = get_conn()
    if conn is None:
        return 0
    saved = 0
    try:
        cur = conn.cursor()
        for r in rows:
            try:
                cur.execute(
                    """
                    INSERT INTO training_injury_history
                        (sport, injury_date, team, player_name, status,
                         injury_type, detail, source, raw_json, ingested_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,NOW())
                    ON CONFLICT (sport, injury_date, player_name, team, source)
                    DO UPDATE SET
                        status      = COALESCE(EXCLUDED.status, training_injury_history.status),
                        injury_type = COALESCE(EXCLUDED.injury_type, training_injury_history.injury_type),
                        detail      = COALESCE(EXCLUDED.detail, training_injury_history.detail),
                        raw_json    = COALESCE(EXCLUDED.raw_json, training_injury_history.raw_json),
                        ingested_at = NOW()
                    """,
                    (
                        r.get("sport"),
                        r.get("injury_date"),
                        r.get("team"),
                        r.get("player_name"),
                        r.get("status"),
                        r.get("injury_type"),
                        r.get("detail"),
                        r.get("source"),
                        json.dumps(r.get("raw_json") or {}),
                    ),
                )
                saved += 1
            except Exception:
                pass
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"[db] save_training_injury_history error: {e}")
        return 0
    finally:
        conn.close()
    return saved


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

def _cache_date_default() -> datetime.date:
    try:
        import zoneinfo

        eastern = zoneinfo.ZoneInfo("America/New_York")
        return datetime.datetime.now(tz=eastern).date()
    except Exception:
        try:
            import pytz

            eastern = pytz.timezone("America/New_York")
            return datetime.datetime.now(tz=eastern).date()
        except Exception:
            return datetime.date.today()

def save_analysis_cache(data: dict, cache_date=None):
    """
    Save the full analysis result (game cards, parlays, picks) for today.
    On a second run within the same day the row is updated in-place.
    data: serialisable dict (all values must be JSON-safe).
    """
    if not data:
        return
    cdate = cache_date or _cache_date_default()
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
def get_analysis_cache(max_age_hours: int = 22, cache_date=None,
                       allow_latest_fallback: bool = False) -> "dict | None":
    """
    Return the requested day's cached analysis data if it was saved within
    max_age_hours.
    When allow_latest_fallback is True, the most recently updated fresh row may
    be returned if the requested day is missing.
    Returns None when no fresh cache exists — caller should run full analysis.
    The returned dict also contains '_updated_at' (ISO string) for display.
    """
    cdate = cache_date or _cache_date_default()
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
        if not row and allow_latest_fallback:
            cur.execute("""
                SELECT data_json, updated_at
                FROM   analysis_cache
                WHERE  updated_at > NOW() - (INTERVAL '1 hour' * %s)
                ORDER BY updated_at DESC
                LIMIT 1
            """, (max_age_hours,))
            row = cur.fetchone()
        if not row:
            return None
        data = dict(row["data_json"])
        ts = row["updated_at"]
        data["_updated_at"] = ts.strftime("%b %d %I:%M %p ET") if hasattr(ts, "strftime") else str(ts)[:16]
        if ts:
            data["cache_updated_at_iso"] = ts.isoformat() if hasattr(ts, "isoformat") else str(ts)
            try:
                if ts.tzinfo:
                    now = datetime.datetime.now(datetime.timezone.utc)
                else:
                    now = datetime.datetime.utcnow()
                data["cache_age_min"] = max(0, int((now - ts).total_seconds() / 60))
            except Exception:
                data["cache_age_min"] = None
        return data
    except Exception as e:
        print(f"[db] get_analysis_cache error: {e}")
        return None
    finally:
        conn.close()



# ──────────────────────────────────────────────────────────────────────────────
# Enrichment persistence
# ──────────────────────────────────────────────────────────────────────────────

def save_game_enrichment(enriched_games: list) -> int:
    """
    Upsert game_enrichment rows from a list of enriched game dicts.
    Each game dict must include 'enrichment' sub-dict produced by enrichment.enrich_game().
    Returns the number of rows saved.
    """
    import json as _json
    conn = get_conn()
    if conn is None:
        return 0
    saved = 0
    cur = conn.cursor()
    for g in enriched_games:
        enrich = g.get("enrichment") or {}
        if not enrich:
            continue
        sport     = str(g.get("sport") or "").strip() or "unknown"
        home_team = str(g.get("home_team") or "").strip()
        away_team = str(g.get("away_team") or "").strip()
        game_date = str(g.get("game_date") or g.get("date") or "").strip()
        if not (home_team and away_team and game_date):
            continue

        hr = enrich.get("home_rest") or {}
        ar = enrich.get("away_rest") or {}
        h2h = enrich.get("h2h") or {}
        vh  = enrich.get("venue_history") or {}
        hc  = enrich.get("home_coach") or {}
        ac  = enrich.get("away_coach") or {}
        wt  = enrich.get("weather") or {}

        try:
            cur.execute("""
                INSERT INTO game_enrichment (
                    sport, home_team, away_team, game_date,
                    home_rest_days, away_rest_days,
                    home_back_to_back, away_back_to_back,
                    home_games_last_7, away_games_last_7,
                    home_fatigue, away_fatigue,
                    away_travel_km, long_road_trip,
                    h2h_home_wins, h2h_away_wins, h2h_draws, h2h_total,
                    h2h_avg_total, h2h_last_5,
                    venue_name, venue_home_wins, venue_away_wins, venue_draws,
                    venue_total_games, venue_avg_total,
                    home_coach_name, home_coach_win_pct,
                    away_coach_name, away_coach_win_pct,
                    weather_condition, weather_temp_f,
                    weather_wind_kph, weather_precip_mm, weather_humidity,
                    enrichment_json, fetched_at
                ) VALUES (
                    %s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,
                    %s,%s,%s,%s,%s,
                    %s, NOW()
                )
                ON CONFLICT (sport, home_team, away_team, game_date)
                DO UPDATE SET
                    home_rest_days    = EXCLUDED.home_rest_days,
                    away_rest_days    = EXCLUDED.away_rest_days,
                    home_back_to_back = EXCLUDED.home_back_to_back,
                    away_back_to_back = EXCLUDED.away_back_to_back,
                    home_fatigue      = EXCLUDED.home_fatigue,
                    away_fatigue      = EXCLUDED.away_fatigue,
                    away_travel_km    = EXCLUDED.away_travel_km,
                    h2h_home_wins     = EXCLUDED.h2h_home_wins,
                    h2h_last_5        = EXCLUDED.h2h_last_5,
                    weather_condition = EXCLUDED.weather_condition,
                    weather_temp_f    = EXCLUDED.weather_temp_f,
                    enrichment_json   = EXCLUDED.enrichment_json,
                    fetched_at        = NOW()
            """, (
                sport, home_team, away_team, game_date,
                hr.get("rest_days"), ar.get("rest_days"),
                bool(hr.get("back_to_back")), bool(ar.get("back_to_back")),
                hr.get("games_in_last_7"), ar.get("games_in_last_7"),
                enrich.get("home_fatigue"), enrich.get("away_fatigue"),
                enrich.get("away_travel_km"), bool(enrich.get("long_road_trip")),
                h2h.get("home_wins"), h2h.get("away_wins"), h2h.get("draws"), h2h.get("total"),
                h2h.get("avg_total"), _json.dumps(h2h.get("last_5_results") or []),
                vh.get("venue_name"), vh.get("home_wins"), vh.get("away_wins"), vh.get("draws"),
                vh.get("total_games"), vh.get("avg_total_score"),
                (hc.get("coach_name") or "").strip(), hc.get("win_pct"),
                (ac.get("coach_name") or "").strip(), ac.get("win_pct"),
                wt.get("condition"), wt.get("temp_f"),
                wt.get("wind_kph"), wt.get("precip_mm"), wt.get("humidity_pct"),
                _json.dumps(enrich),
            ))
            saved += 1
        except Exception as _e:
            conn.rollback()
            print(f"[db:save_game_enrichment] {home_team} vs {away_team}: {_e}")
            continue
    try:
        conn.commit()
    except Exception:
        pass
    return saved


def save_player_form(forms: list) -> int:
    """
    Upsert player_form_cache rows.
    Each dict must have: sport, player_name, stat_type, avg_last_5, avg_last_10,
    trend_direction, games_collected.
    """
    import json as _json
    conn = get_conn()
    if conn is None:
        return 0
    saved = 0
    cur = conn.cursor()
    today = __import__("datetime").date.today().isoformat()
    for f in forms:
        try:
            cur.execute("""
                INSERT INTO player_form_cache
                    (sport, player_name, stat_type, avg_last_5, avg_last_10,
                     trend_direction, games_collected, form_json, computed_date)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (sport, player_name, stat_type, computed_date)
                DO UPDATE SET
                    avg_last_5      = EXCLUDED.avg_last_5,
                    avg_last_10     = EXCLUDED.avg_last_10,
                    trend_direction = EXCLUDED.trend_direction,
                    games_collected = EXCLUDED.games_collected,
                    form_json       = EXCLUDED.form_json,
                    fetched_at      = NOW()
            """, (
                str(f.get("sport") or "").strip(),
                str(f.get("player_name") or f.get("name") or "").strip(),
                str(f.get("stat_type") or "").strip(),
                f.get("avg_last_5"), f.get("avg_last_10"),
                str(f.get("trend_direction") or "neutral"),
                f.get("games_collected"),
                _json.dumps(f),
                today,
            ))
            saved += 1
        except Exception as _e:
            conn.rollback()
            continue
    try:
        conn.commit()
    except Exception:
        pass
    return saved


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
        cols = _get_table_columns(conn, "predictions")
        has_bet_uid      = "bet_uid"      in cols
        has_run_id       = "run_id"       in cols
        has_run_date     = "run_date"     in cols
        has_signal_type  = "signal_type"  in cols
        has_kalshi       = "kalshi_ticker" in cols
        has_kalshi_series = "kalshi_series_ticker" in cols
        has_polymarket   = "polymarket_ticker" in cols
        has_grade        = "grade"         in cols
        for p in predictions:
            try:
                pred_uid = _prediction_uid(p)
                if has_bet_uid:
                    cur.execute(
                        """SELECT 1 FROM predictions
                           WHERE bet_uid = %s
                             AND outcome != 'ARCHIVED'
                           LIMIT 1""",
                        (pred_uid,),
                    )
                else:
                    cur.execute(
                        """SELECT 1 FROM predictions
                           WHERE game_key = %s AND bet_type = %s AND pick = %s AND game_date = %s
                             AND outcome != 'ARCHIVED'
                           LIMIT 1""",
                        (p.get("game_key"), p.get("bet_type"), p.get("pick"), p.get("game_date")),
                    )
                if cur.fetchone():
                    continue
                extra_cols = ""
                extra_ph   = ""
                extra_vals = []
                if has_bet_uid:
                    extra_cols += ", bet_uid"
                    extra_ph   += ", %s"
                    extra_vals.append(pred_uid)
                if has_run_id:
                    extra_cols += ", run_id"
                    extra_ph   += ", %s"
                    extra_vals.append(p.get("run_id"))
                if has_run_date:
                    extra_cols += ", run_date"
                    extra_ph   += ", %s"
                    extra_vals.append(p.get("run_date") or p.get("game_date"))
                if has_signal_type:
                    extra_cols += ", signal_type, active_sources, injury_flag, momentum_flag, lineup_flag"
                    extra_ph   += ", %s, %s, %s, %s, %s"
                    srcs = p.get("active_sources") or []
                    if isinstance(srcs, list):
                        srcs = ",".join(str(s) for s in srcs)
                    extra_vals.extend([
                        (p.get("signal_type") or "neutral")[:30],
                        str(srcs)[:200],
                        bool(p.get("injury_flag")),
                        bool(p.get("momentum_flag")),
                        bool(p.get("lineup_flag")),
                    ])
                if has_kalshi:
                    extra_cols += ", kalshi_ticker, kalshi_event_ticker"
                    extra_ph   += ", %s, %s"
                    extra_vals.extend([
                        str(p.get("kalshi_ticker")       or "")[:120],
                        str(p.get("kalshi_event_ticker") or "")[:120],
                    ])
                    if has_kalshi_series:
                        extra_cols += ", kalshi_series_ticker"
                        extra_ph   += ", %s"
                        extra_vals.append(str(p.get("kalshi_series_ticker") or "")[:120])
                    extra_cols += ", kalshi_side, kalshi_price_cents, kalshi_status"
                    extra_ph   += ", %s, %s, %s"
                    extra_vals.extend([
                        str(p.get("kalshi_side")         or "")[:10],
                        p.get("kalshi_price_cents"),
                        str(p.get("kalshi_status")       or "unavailable")[:20],
                    ])
                if has_polymarket:
                    extra_cols += ", polymarket_ticker, polymarket_market_slug, polymarket_event_slug, polymarket_series_ticker, polymarket_side, polymarket_price, polymarket_status"
                    extra_ph   += ", %s, %s, %s, %s, %s, %s, %s"
                    extra_vals.extend([
                        str(p.get("polymarket_ticker") or "")[:160],
                        str(p.get("polymarket_market_slug") or "")[:200],
                        str(p.get("polymarket_event_slug") or "")[:200],
                        str(p.get("polymarket_series_ticker") or "")[:120],
                        str(p.get("polymarket_side") or "")[:10],
                        p.get("polymarket_price"),
                        str(p.get("polymarket_status") or "unavailable")[:20],
                    ])
                if has_grade:
                    extra_cols += ", grade, investor_score"
                    extra_ph   += ", %s, %s"
                    extra_vals.extend([
                        str(p.get("grade") or "")[:2] or None,
                        p.get("investor_score"),
                    ])
                conflict_sql = "ON CONFLICT (bet_uid) DO NOTHING" if has_bet_uid else "ON CONFLICT DO NOTHING"
                cur.execute(f"""
                    INSERT INTO predictions
                        (game_key, sport, bet_type, pick, line, odds_am, dec_odds,
                         model_prob, confidence, safety_label, game_date, game_time,
                         home_team, away_team, home_starter, away_starter,
                         outcome, sentiment_score, news_snippet{extra_cols})
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'PENDING',%s,%s{extra_ph})
                    {conflict_sql}
                """, (
                    p.get("game_key"), p.get("sport","mlb"), p.get("bet_type"),
                    p.get("pick"), p.get("line"), p.get("odds_am"), p.get("dec_odds"),
                    p.get("model_prob"), p.get("confidence"), p.get("safety_label"),
                    p.get("game_date"), p.get("game_time"),
                    p.get("home_team"), p.get("away_team"),
                    p.get("home_starter"), p.get("away_starter"),
                    p.get("sentiment_score"), p.get("news_snippet","")[:500],
                    *extra_vals,
                ))
                saved += cur.rowcount
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


def has_predictions_for_date(game_date: "datetime.date | str", sport: str | None = None) -> bool:
    """Return True if any non-archived predictions exist for the given game_date."""
    conn = get_conn()
    if conn is None:
        return False
    try:
        cur = conn.cursor()
        if sport:
            cur.execute(
                "SELECT 1 FROM predictions WHERE game_date = %s AND sport = %s AND outcome != 'ARCHIVED' LIMIT 1",
                (game_date, sport),
            )
        else:
            cur.execute(
                "SELECT 1 FROM predictions WHERE game_date = %s AND outcome != 'ARCHIVED' LIMIT 1",
                (game_date,),
            )
        return cur.fetchone() is not None
    except Exception as e:
        print(f"[db] has_predictions_for_date error: {e}")
        return False
    finally:
        conn.close()


def archive_previous_day_data(today_date: "datetime.date | str") -> dict:
    """
    Archive all PENDING predictions and prop_history rows from before today.
    This lets the daily lock check always pass for a genuinely new day.
    Returns dict with counts of archived rows.
    """
    conn = get_conn()
    if conn is None:
        return {}
    results = {}
    try:
        cur = conn.cursor()
        # Archive old PENDING predictions
        cur.execute(
            """
            UPDATE predictions
            SET outcome = 'ARCHIVED', resolved_at = NOW()
            WHERE game_date < %s
              AND outcome = 'PENDING'
            """,
            (today_date,)
        )
        results["predictions_archived"] = cur.rowcount
        # Archive old PENDING prop picks
        cur.execute(
            """
            UPDATE prop_history
            SET outcome = 'ARCHIVED', resolved_at = NOW()
            WHERE game_date < %s
              AND outcome = 'PENDING'
            """,
            (today_date,)
        )
        results["props_archived"] = cur.rowcount
        # Delete stale analysis_cache rows older than 2 days (keep yesterday for resolution)
        cur.execute(
            """
            DELETE FROM analysis_cache
            WHERE cache_date < %s - INTERVAL '2 days'
            """,
            (today_date,)
        )
        results["cache_rows_deleted"] = cur.rowcount
        conn.commit()
        print(f"[db] archived {results.get('predictions_archived',0)} predictions, "
              f"{results.get('props_archived',0)} props, "
              f"deleted {results.get('cache_rows_deleted',0)} stale cache rows")
        return results
    except Exception as e:
        conn.rollback()
        print(f"[db] archive_previous_day_data error: {e}")
        return {}
    finally:
        conn.close()


def upsert_daily_run(run_id: str, run_date, status: str = 'RUNNING',
                     games_today: int = 0, games_tmrw: int = 0,
                     props_count: int = 0, parlays_count: int = 0,
                     finished: bool = False) -> int:
    """Insert or update a daily run log entry. Returns the run's DB id."""
    conn = get_conn()
    if conn is None:
        return 0
    try:
        cur = conn.cursor()
        if finished:
            cur.execute(
                """
                INSERT INTO daily_runs
                    (run_id, run_date, status, games_today, games_tmrw,
                     props_count, parlays_count, finished_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (run_id) DO UPDATE SET
                    status        = EXCLUDED.status,
                    games_today   = EXCLUDED.games_today,
                    games_tmrw    = EXCLUDED.games_tmrw,
                    props_count   = EXCLUDED.props_count,
                    parlays_count = EXCLUDED.parlays_count,
                    finished_at   = NOW()
                RETURNING id
                """,
                (run_id, run_date, status, games_today, games_tmrw,
                 props_count, parlays_count)
            )
        else:
            cur.execute(
                """
                INSERT INTO daily_runs
                    (run_id, run_date, status, games_today, games_tmrw,
                     props_count, parlays_count)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (run_id) DO UPDATE SET
                    status        = EXCLUDED.status,
                    games_today   = EXCLUDED.games_today,
                    games_tmrw    = EXCLUDED.games_tmrw,
                    props_count   = EXCLUDED.props_count,
                    parlays_count = EXCLUDED.parlays_count
                RETURNING id
                """,
                (run_id, run_date, status, games_today, games_tmrw,
                 props_count, parlays_count)
            )
        row = cur.fetchone()
        conn.commit()
        return row[0] if row else 0
    except Exception as e:
        conn.rollback()
        print(f"[db] upsert_daily_run error: {e}")
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


def get_predictions_for_date(game_date: "str | datetime.date", sport: str | None = None) -> list:
    """
    Return saved prediction rows for a specific game_date, formatted as bet dicts
    compatible with _build_card in dashboard.py (includes a numeric 'safety' score).
    """
    _SAFETY_SCORES = {"ELITE": 0.80, "SAFE": 0.65, "MODERATE": 0.52, "RISKY": 0.45}
    conn = get_conn()
    if conn is None:
        return []
    try:
        date_str = game_date.isoformat() if hasattr(game_date, "isoformat") else str(game_date)
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cols = _get_table_columns(conn, "predictions")
        bet_uid_col = "bet_uid" if "bet_uid" in cols else "NULL::text AS bet_uid"
        wheres = ["game_date = %s", "outcome != 'ARCHIVED'"]
        vals = [date_str]
        if sport:
            wheres.append("sport = %s")
            vals.append(sport)
        cur.execute(
            f"""SELECT {bet_uid_col}, sport, game_key, bet_type, pick, line, odds_am, dec_odds, model_prob,
                      confidence, safety_label, game_date, game_time,
                      home_team, away_team, home_starter, away_starter, outcome
               FROM predictions
               WHERE {' AND '.join(wheres)}
               ORDER BY model_prob DESC""",
            vals,
        )
        rows = []
        for r in cur.fetchall():
            d = dict(r)
            d["safety"] = _SAFETY_SCORES.get(d.get("safety_label", "MODERATE"), 0.52)
            d["match_key"] = d.get("game_key", "")
            d["edge"] = max(0.0, float(d.get("model_prob") or 0.5) - 0.476)
            for k in ("game_date",):
                if d.get(k) and hasattr(d[k], "isoformat"):
                    d[k] = d[k].isoformat()
            for k in ("line", "dec_odds", "model_prob"):
                if d.get(k) is not None:
                    try:
                        d[k] = float(d[k])
                    except (TypeError, ValueError):
                        pass
            rows.append(d)
        return rows
    except Exception as e:
        print(f"[db] get_predictions_for_date error: {e}")
        return []
    finally:
        conn.close()


def get_predictions(days: int = 30, outcome: str = None,
                    bet_type: str = None, sport: str | None = None) -> list:
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
        if sport:
            wheres.append("sport = %s"); vals.append(sport)
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


def get_prop_history(days: int = 30, outcome: str = None,
                     sport: str | None = None) -> list:
    """Fetch prop history rows for tracking UI and status summaries."""
    conn = get_conn()
    if conn is None:
        return []
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        wheres = ["detected_at > NOW() - (INTERVAL '1 day' * %s)"]
        vals = [days]
        if outcome:
            wheres.append("outcome = %s")
            vals.append(outcome.upper())
        if sport:
            wheres.append("sport = %s")
            vals.append(sport)

        cur.execute(
            f"SELECT * FROM prop_history WHERE {' AND '.join(wheres)} "
            "ORDER BY detected_at DESC LIMIT 500",
            vals,
        )
        rows = [dict(r) for r in cur.fetchall()]
        for row in rows:
            for key in ("game_date", "detected_at", "resolved_at"):
                if row.get(key):
                    row[key] = row[key].isoformat() if hasattr(row[key], "isoformat") else str(row[key])
            if isinstance(row.get("stats_json"), str):
                try:
                    row["stats_json"] = json.loads(row["stats_json"])
                except Exception:
                    row["stats_json"] = {}
        return rows
    except Exception as e:
        print(f"[db] get_prop_history error: {e}")
        return []
    finally:
        conn.close()


def get_performance_stats(sport: str | None = None, target_date=None) -> dict:
    """Return win/loss/push counts and ROI for prediction tracking."""
    conn = get_conn()
    if conn is None:
        return {}
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        vals = []
        where_parts = ["outcome != 'ARCHIVED'"]
        if sport:
            where_parts.append("sport = %s")
            vals.append(sport)
        if target_date is not None:
            where_parts.append("game_date = %s")
            vals.append(target_date.isoformat() if hasattr(target_date, "isoformat") else str(target_date))
        where_sql = " AND ".join(where_parts)

        cur.execute(f"""
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
            WHERE {where_sql}
        """, vals)
        row = cur.fetchone()
        stats = dict(row) if row else {}
        # By bet type
        cur.execute(f"""
            SELECT bet_type,
                   COUNT(*) FILTER (WHERE outcome='WIN')  AS wins,
                   COUNT(*) FILTER (WHERE outcome='LOSS') AS losses,
                   COUNT(*) AS total
            FROM predictions
                        WHERE {where_sql}
              AND outcome IN ('WIN','LOSS')
            GROUP BY bet_type ORDER BY total DESC
        """, vals)
        stats["by_bet_type"] = [dict(r) for r in cur.fetchall()]
        # Last 30 days trend
        cur.execute(f"""
            SELECT game_date::text AS date,
                   COUNT(*) FILTER (WHERE outcome='WIN')  AS wins,
                   COUNT(*) FILTER (WHERE outcome='LOSS') AS losses
            FROM predictions
                        WHERE {where_sql}
                            AND game_date >= CURRENT_DATE - INTERVAL '30 days'
              AND outcome IN ('WIN','LOSS')
            GROUP BY game_date ORDER BY game_date
        """, vals)
        stats["daily_trend"] = [dict(r) for r in cur.fetchall()]
        return stats
    except Exception as e:
        print(f"[db] get_performance_stats error: {e}")
        return {}
    finally:
        conn.close()


def update_prediction_exchange_statuses(rows: list[dict]) -> int:
    """Persist Kalshi/Polymarket resolution metadata for prediction rows by bet_uid."""
    if not rows:
        return 0
    conn = get_conn()
    if conn is None:
        return 0
    updated = 0
    try:
        cols = _get_table_columns(conn, "predictions")
        if "bet_uid" not in cols:
            return 0

        has_kalshi_ticker = "kalshi_ticker" in cols
        has_kalshi_event = "kalshi_event_ticker" in cols
        has_kalshi_series = "kalshi_series_ticker" in cols
        has_kalshi_side = "kalshi_side" in cols
        has_kalshi_price = "kalshi_price_cents" in cols
        has_kalshi_status = "kalshi_status" in cols

        has_poly_ticker = "polymarket_ticker" in cols
        has_poly_slug = "polymarket_market_slug" in cols
        has_poly_event = "polymarket_event_slug" in cols
        has_poly_series = "polymarket_series_ticker" in cols
        has_poly_side = "polymarket_side" in cols
        has_poly_price = "polymarket_price" in cols
        has_poly_status = "polymarket_status" in cols

        set_cols = []
        if has_kalshi_ticker:
            set_cols.append("kalshi_ticker = COALESCE(NULLIF(%s, ''), kalshi_ticker)")
        if has_kalshi_event:
            set_cols.append("kalshi_event_ticker = COALESCE(NULLIF(%s, ''), kalshi_event_ticker)")
        if has_kalshi_series:
            set_cols.append("kalshi_series_ticker = COALESCE(NULLIF(%s, ''), kalshi_series_ticker)")
        if has_kalshi_side:
            set_cols.append("kalshi_side = COALESCE(NULLIF(%s, ''), kalshi_side)")
        if has_kalshi_price:
            set_cols.append("kalshi_price_cents = COALESCE(%s, kalshi_price_cents)")
        if has_kalshi_status:
            set_cols.append("kalshi_status = COALESCE(NULLIF(%s, ''), kalshi_status)")

        if has_poly_ticker:
            set_cols.append("polymarket_ticker = COALESCE(NULLIF(%s, ''), polymarket_ticker)")
        if has_poly_slug:
            set_cols.append("polymarket_market_slug = COALESCE(NULLIF(%s, ''), polymarket_market_slug)")
        if has_poly_event:
            set_cols.append("polymarket_event_slug = COALESCE(NULLIF(%s, ''), polymarket_event_slug)")
        if has_poly_series:
            set_cols.append("polymarket_series_ticker = COALESCE(NULLIF(%s, ''), polymarket_series_ticker)")
        if has_poly_side:
            set_cols.append("polymarket_side = COALESCE(NULLIF(%s, ''), polymarket_side)")
        if has_poly_price:
            set_cols.append("polymarket_price = COALESCE(%s, polymarket_price)")
        if has_poly_status:
            set_cols.append("polymarket_status = COALESCE(NULLIF(%s, ''), polymarket_status)")

        if not set_cols:
            return 0

        sql = (
            "UPDATE predictions "
            f"SET {', '.join(set_cols)} "
            "WHERE bet_uid = %s"
        )

        cur = conn.cursor()
        for row in rows:
            if not isinstance(row, dict):
                continue
            bet_uid = str(row.get("bet_uid") or "").strip()
            if not bet_uid:
                continue

            vals = []
            if has_kalshi_ticker:
                vals.append(str(row.get("kalshi_ticker") or "")[:120])
            if has_kalshi_event:
                vals.append(str(row.get("kalshi_event_ticker") or "")[:120])
            if has_kalshi_series:
                vals.append(str(row.get("kalshi_series_ticker") or "")[:120])
            if has_kalshi_side:
                vals.append(str(row.get("kalshi_side") or "")[:10])
            if has_kalshi_price:
                vals.append(row.get("kalshi_price_cents"))
            if has_kalshi_status:
                vals.append(str(row.get("kalshi_status") or "")[:20])

            if has_poly_ticker:
                vals.append(str(row.get("polymarket_ticker") or "")[:160])
            if has_poly_slug:
                vals.append(str(row.get("polymarket_market_slug") or "")[:200])
            if has_poly_event:
                vals.append(str(row.get("polymarket_event_slug") or "")[:200])
            if has_poly_series:
                vals.append(str(row.get("polymarket_series_ticker") or "")[:120])
            if has_poly_side:
                vals.append(str(row.get("polymarket_side") or "")[:10])
            if has_poly_price:
                vals.append(row.get("polymarket_price"))
            if has_poly_status:
                vals.append(str(row.get("polymarket_status") or "")[:20])

            vals.append(bet_uid)
            try:
                cur.execute(sql, vals)
                updated += int(cur.rowcount or 0)
            except Exception:
                continue

        conn.commit()
        return updated
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        print(f"[db] update_prediction_exchange_statuses error: {e}")
        return 0
    finally:
        conn.close()


def get_settlement_summary(sport: str | None = None, target_date=None, stale_hours: int = 6) -> dict:
    """Return settlement-health summary used by Tracking Summary UI/automation."""
    conn = get_conn()
    if conn is None:
        return {}
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cols = _get_table_columns(conn, "predictions")

        stale_hours = max(1, int(stale_hours or 6))
        vals = []
        where_parts = ["outcome != 'ARCHIVED'"]
        if sport:
            where_parts.append("sport = %s")
            vals.append(sport)
        if target_date is not None:
            where_parts.append("game_date = %s")
            vals.append(target_date.isoformat() if hasattr(target_date, "isoformat") else str(target_date))
        where_sql = " AND ".join(where_parts)

        has_kalshi_status = "kalshi_status" in cols
        has_poly_status = "polymarket_status" in cols

        kalshi_live_expr = "0::int"
        if has_kalshi_status:
            kalshi_live_expr = (
                "COUNT(*) FILTER (WHERE outcome='PENDING' "
                "AND LOWER(COALESCE(kalshi_status,'')) IN ('started','done'))"
            )
        poly_live_expr = "0::int"
        if has_poly_status:
            poly_live_expr = (
                "COUNT(*) FILTER (WHERE outcome='PENDING' "
                "AND LOWER(COALESCE(polymarket_status,'')) IN ('started','done','resolved','closed'))"
            )

        cur.execute(
            f"""
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE outcome='PENDING') AS pending,
                COUNT(*) FILTER (WHERE outcome IN ('WIN','LOSS','PUSH')) AS settled,
                COUNT(*) FILTER (
                    WHERE outcome='PENDING'
                      AND COALESCE(game_date::timestamp, predicted_at) < NOW() - (%s * INTERVAL '1 hour')
                ) AS stale_pending,
                {kalshi_live_expr} AS pending_with_kalshi_market_state,
                {poly_live_expr} AS pending_with_polymarket_market_state
            FROM predictions
            WHERE {where_sql}
            """,
            [stale_hours, *vals],
        )
        pred = dict(cur.fetchone() or {})

        prop_where_parts = ["outcome != 'ARCHIVED'"]
        prop_vals = []
        if sport:
            prop_where_parts.append("sport = %s")
            prop_vals.append(sport)
        if target_date is not None:
            prop_where_parts.append("game_date = %s")
            prop_vals.append(target_date.isoformat() if hasattr(target_date, "isoformat") else str(target_date))
        prop_where_sql = " AND ".join(prop_where_parts)

        cur.execute(
            f"""
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE outcome='PENDING') AS pending,
                COUNT(*) FILTER (WHERE outcome IN ('WIN','LOSS','PUSH')) AS settled,
                COUNT(*) FILTER (
                    WHERE outcome='PENDING'
                      AND COALESCE(game_date::timestamp, detected_at) < NOW() - (%s * INTERVAL '1 hour')
                ) AS stale_pending
            FROM prop_history
            WHERE {prop_where_sql}
            """,
            [stale_hours, *prop_vals],
        )
        props = dict(cur.fetchone() or {})

        return {
            "stale_hours": stale_hours,
            "predictions": pred,
            "props": props,
            "needs_attention": int(pred.get("stale_pending") or 0) + int(props.get("stale_pending") or 0),
        }
    except Exception as e:
        print(f"[db] get_settlement_summary error: {e}")
        return {}
    finally:
        conn.close()


def get_prop_performance_stats(sport: str | None = None, days_back: int | None = None,
                               target_date=None) -> dict:
    """Return prop stats for dashboard tracking."""
    conn = get_conn()
    if conn is None:
        return {}
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        vals: list = []
        where_parts = []
        if sport:
            where_parts.append("sport = %s")
            vals.append(sport)
        cols = _get_table_columns(conn, "prop_history")
        has_bet_uid = "bet_uid" in cols
        dedupe_key = (
            "COALESCE(NULLIF(bet_uid, ''), "
            "concat_ws('|', lower(COALESCE(sport,'')), COALESCE(game_date::text,''), "
            "lower(COALESCE(player_name,'')), lower(COALESCE(team,'')), "
            "lower(COALESCE(prop_type,'')), lower(COALESCE(recommendation,'')), COALESCE(line::text,'')))"
            if has_bet_uid else
            "concat_ws('|', lower(COALESCE(sport,'')), COALESCE(game_date::text,''), "
            "lower(COALESCE(player_name,'')), lower(COALESCE(team,'')), "
            "lower(COALESCE(prop_type,'')), lower(COALESCE(recommendation,'')), COALESCE(line::text,''))"
        )
        # No recommendation filter — track all bet types across all sports.
        # Prefer a current-day slice for the dashboard, falling back to a trailing window.
        date_filter = ""
        if target_date is not None:
            where_parts.append("game_date = %s")
            vals.append(target_date.isoformat() if hasattr(target_date, "isoformat") else str(target_date))
        elif days_back is not None and int(days_back) > 0:
            date_filter = f"game_date >= CURRENT_DATE - ({int(days_back)} * INTERVAL '1 day')"

        if date_filter:
            where_parts.append(date_filter)
        where_sql = " AND ".join(where_parts) if where_parts else "TRUE"
        cte = f"""
            WITH filtered AS (
                SELECT *, {dedupe_key} AS dedupe_key
                FROM prop_history
                WHERE {where_sql}
            ),
            dedup AS (
                SELECT DISTINCT ON (dedupe_key) *
                FROM filtered
                ORDER BY dedupe_key, detected_at DESC, id DESC
            )
        """

        # Overall prop stats
        cur.execute(cte + f"""
            SELECT
                COUNT(*) FILTER (WHERE outcome='WIN')      AS wins,
                COUNT(*) FILTER (WHERE outcome='LOSS')     AS losses,
                COUNT(*) FILTER (WHERE outcome='PUSH')     AS pushes,
                COUNT(*) FILTER (WHERE outcome='PENDING')  AS pending,
                COUNT(*) FILTER (WHERE outcome='ARCHIVED') AS archived,
                COUNT(*) FILTER (WHERE outcome NOT IN ('PENDING','ARCHIVED')) AS total_resolved,
                COUNT(*) AS total,
                ROUND(AVG(CASE WHEN outcome='WIN' THEN 1.0
                               WHEN outcome='LOSS' THEN 0.0 END)*100, 1) AS hit_rate
                        FROM dedup
            WHERE outcome != 'ARCHIVED'
        """, vals)
        row = cur.fetchone()
        stats = dict(row) if row else {}
        # Detailed hit-rate rows by prop_type + direction so dashboard can show all markets.
        cur.execute(cte + f"""
            SELECT
                COALESCE(NULLIF(lower(COALESCE(prop_type,'')), ''), 'unknown') AS prop_type,
                CASE
                    WHEN lower(COALESCE(recommendation,'')) IN ('over','under') THEN upper(lower(COALESCE(recommendation,'')))
                    WHEN NULLIF(trim(COALESCE(recommendation,'')), '') IS NOT NULL THEN upper(trim(COALESCE(recommendation,'')))
                    ELSE '—'
                END AS recommendation,
                COUNT(*) FILTER (WHERE outcome='WIN')      AS wins,
                COUNT(*) FILTER (WHERE outcome='LOSS')     AS losses,
                COUNT(*) FILTER (WHERE outcome='PUSH')     AS pushes,
                COUNT(*) FILTER (WHERE outcome='PENDING')  AS pending,
                COUNT(*) FILTER (WHERE outcome='ARCHIVED') AS unresolvable,
                COUNT(*) AS total,
                ROUND(AVG(CASE WHEN outcome='WIN' THEN 1.0
                               WHEN outcome='LOSS' THEN 0.0 END)*100, 1) AS hit_rate
            FROM dedup
            GROUP BY 1, 2
            ORDER BY total DESC, prop_type ASC, recommendation ASC
        """, vals)
        stats["by_prop_type"] = [dict(r) for r in cur.fetchall()]
        # Daily trend
        cur.execute(cte + f"""
            SELECT game_date::text AS date,
                   COUNT(*) FILTER (WHERE outcome='WIN')  AS wins,
                   COUNT(*) FILTER (WHERE outcome='LOSS') AS losses,
                   COUNT(*) FILTER (WHERE outcome='PENDING') AS pending
                        FROM dedup
            WHERE outcome != 'ARCHIVED'
            GROUP BY game_date
            ORDER BY game_date DESC
            LIMIT 30
        """, vals)
        stats["daily_trend"] = [dict(r) for r in cur.fetchall()]
        return stats
    except Exception as e:
        print(f"[db] get_prop_performance_stats error: {e}")
        return {}
    finally:
        conn.close()


def get_pending_props(days_back: int = 3, sport: str | None = None) -> list:
    """Return PENDING prop picks from the last N days for resolution."""
    conn = get_conn()
    if conn is None:
        return []
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cols = _get_table_columns(conn, "prop_history")
        bet_uid_col = "bet_uid" if "bet_uid" in cols else "NULL::text AS bet_uid"
        game_key_col = "game_key" if "game_key" in cols else "NULL::text AS game_key"
        vals: list = [days_back]
        sport_where = ""
        if sport:
            sport_where = " AND sport = %s"
            vals.append(sport)
        # No recommendation filter — resolve all bet types for all sports
        cur.execute(f"""
            SELECT id, {bet_uid_col}, {game_key_col}, sport, player_name, team, game_date::text,
                   prop_type, line, over_prob, under_prob, recommendation, stats_json
            FROM prop_history
            WHERE outcome = 'PENDING'
              AND game_date >= CURRENT_DATE - (%s * INTERVAL '1 day')
              AND game_date <= CURRENT_DATE
              {sport_where}
            ORDER BY game_date DESC
        """, vals)
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
                (entity, entity_type, source, score, volume, keywords, computed_at, computed_date)
            VALUES (%s, %s, %s, %s, %s, %s, NOW(), CURRENT_DATE)
            ON CONFLICT (entity, source, computed_date) DO UPDATE SET
                score       = EXCLUDED.score,
                volume      = EXCLUDED.volume,
                keywords    = EXCLUDED.keywords,
                computed_at = NOW(),
                computed_date = EXCLUDED.computed_date
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
                        stake_usd: float = 0, dedupe_pending: bool = False) -> int:
    """Save a tracked parlay. Returns existing/new parlay ID when saved."""
    conn = get_conn()
    if conn is None:
        return 0
    try:
        cur = conn.cursor()
        cols = _get_table_columns(conn, "tracked_parlays")
        has_parlay_uid = "parlay_uid" in cols
        legs_json = json.dumps(legs or [])
        current_date = _cache_date_default()

        # Always compute a deterministic UID so the same parlay is not re-inserted.
        par_uid = _parlay_uid(name=name, legs=legs or [], created_date=current_date)

        if has_parlay_uid and par_uid:
            if dedupe_pending:
                cur.execute(
                    """
                    SELECT id
                    FROM tracked_parlays
                    WHERE parlay_uid = %s
                      AND outcome = 'PENDING'
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (par_uid,),
                )
            else:
                cur.execute(
                    """
                    SELECT id
                    FROM tracked_parlays
                    WHERE parlay_uid = %s
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (par_uid,),
                )
            existing = cur.fetchone()
            if existing:
                return int(existing[0])

        if dedupe_pending:
            cur.execute("""
                SELECT id
                FROM tracked_parlays
                WHERE outcome = 'PENDING'
                  AND DATE(created_at AT TIME ZONE 'America/New_York') = %s
                  AND legs_json = %s::jsonb
                  AND ABS(COALESCE(combined_odds, 0) - COALESCE(%s, 0)) < 0.01
                ORDER BY created_at DESC
                LIMIT 1
            """, (current_date, legs_json, combined_odds))
            existing = cur.fetchone()
            if existing:
                return int(existing[0])

        if has_parlay_uid:
            cur.execute(
                """
                INSERT INTO tracked_parlays
                    (parlay_uid, name, legs_json, combined_odds, stake_usd, outcome)
                VALUES (%s, %s, %s::jsonb, %s, %s, 'PENDING')
                RETURNING id
                """,
                (par_uid, name, legs_json, combined_odds, stake_usd),
            )
        else:
            cur.execute(
                """
                INSERT INTO tracked_parlays
                    (name, legs_json, combined_odds, stake_usd, outcome)
                VALUES (%s, %s::jsonb, %s, %s, 'PENDING')
                RETURNING id
                """,
                (name, legs_json, combined_odds, stake_usd),
            )
        row = cur.fetchone()
        conn.commit()
        return row[0] if row else 0
    except Exception as e:
        conn.rollback()
        print(f"[db] save_tracked_parlay error: {e}")
        return 0
    finally:
        conn.close()
def get_tracked_parlays(include_resolved: bool = False, target_date=None,
                       sport: str | None = None) -> list:
    conn = get_conn()
    if conn is None:
        return []
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        clauses = []
        params = []
        if not include_resolved:
            clauses.append("outcome = 'PENDING'")
        if target_date is not None:
            target_iso = target_date.isoformat() if hasattr(target_date, "isoformat") else str(target_date)
            clauses.append("""
                (
                    DATE(tp.created_at AT TIME ZONE 'America/New_York') = %s
                    OR EXISTS (
                        SELECT 1
                        FROM jsonb_array_elements(
                            CASE
                                WHEN jsonb_typeof(tp.legs_json) = 'array' THEN tp.legs_json
                                ELSE '[]'::jsonb
                            END
                        ) AS leg
                        WHERE COALESCE(leg->>'game_date', '') ~ '^\\d{4}-\\d{2}-\\d{2}'
                          AND substring(leg->>'game_date', 1, 10) = %s
                    )
                    OR EXISTS (
                        SELECT 1
                        FROM jsonb_array_elements(
                            CASE
                                WHEN jsonb_typeof(tp.legs_json) = 'array' THEN tp.legs_json
                                ELSE '[]'::jsonb
                            END
                        ) AS leg
                        WHERE COALESCE(leg->>'scheduled_start', '') ~ '^\\d{4}-\\d{2}-\\d{2}'
                          AND substring(leg->>'scheduled_start', 1, 10) = %s
                    )
                )
            """)
            params.extend([target_date, target_iso, target_iso])
        if sport:
            clauses.append("""
                EXISTS (
                    SELECT 1
                    FROM jsonb_array_elements(
                        CASE
                            WHEN jsonb_typeof(tp.legs_json) = 'array' THEN tp.legs_json
                            ELSE '[]'::jsonb
                        END
                    ) AS leg
                    WHERE lower(COALESCE(leg->>'sport', '')) = %s
                )
            """)
            params.append(str(sport).lower())
        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        cur.execute(
            f"SELECT * FROM tracked_parlays tp {where_sql} ORDER BY created_at DESC",
            tuple(params),
        )
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


def prune_tracked_parlays_to_date(target_date=None) -> int:
    """Delete tracked parlays that are not for the target ET calendar date."""
    conn = get_conn()
    if conn is None:
        return 0
    try:
        cur = conn.cursor()
        keep_date = target_date if target_date is not None else _cache_date_default()
        keep_iso = keep_date.isoformat() if hasattr(keep_date, "isoformat") else str(keep_date)
        cur.execute(
            """
            DELETE FROM tracked_parlays tp
            WHERE NOT (
                DATE(tp.created_at AT TIME ZONE 'America/New_York') = %s
                OR EXISTS (
                    SELECT 1
                    FROM jsonb_array_elements(
                        CASE
                            WHEN jsonb_typeof(tp.legs_json) = 'array' THEN tp.legs_json
                            ELSE '[]'::jsonb
                        END
                    ) AS leg
                    WHERE COALESCE(leg->>'game_date', '') ~ '^\\d{4}-\\d{2}-\\d{2}'
                      AND substring(leg->>'game_date', 1, 10) = %s
                )
                OR EXISTS (
                    SELECT 1
                    FROM jsonb_array_elements(
                        CASE
                            WHEN jsonb_typeof(tp.legs_json) = 'array' THEN tp.legs_json
                            ELSE '[]'::jsonb
                        END
                    ) AS leg
                    WHERE COALESCE(leg->>'scheduled_start', '') ~ '^\\d{4}-\\d{2}-\\d{2}'
                      AND substring(leg->>'scheduled_start', 1, 10) = %s
                )
            )
            """,
            (keep_date, keep_iso, keep_iso),
        )
        removed = int(cur.rowcount or 0)
        conn.commit()
        return removed
    except Exception as e:
        conn.rollback()
        print(f"[db] prune_tracked_parlays_to_date error: {e}")
        return 0
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


def get_parlay_performance_stats(sport: str | None = None, target_date=None) -> dict:
    """Win/loss/ROI stats for tracked parlays."""
    conn = get_conn()
    if conn is None:
        return {}
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        clauses = []
        vals = []
        if target_date is not None:
            target_iso = target_date.isoformat() if hasattr(target_date, "isoformat") else str(target_date)
            clauses.append("""
                (
                    DATE(tp.created_at AT TIME ZONE 'America/New_York') = %s
                    OR EXISTS (
                        SELECT 1
                        FROM jsonb_array_elements(
                            CASE
                                WHEN jsonb_typeof(tp.legs_json) = 'array' THEN tp.legs_json
                                ELSE '[]'::jsonb
                            END
                        ) AS leg
                        WHERE COALESCE(leg->>'game_date', '') ~ '^\\d{4}-\\d{2}-\\d{2}'
                          AND substring(leg->>'game_date', 1, 10) = %s
                    )
                    OR EXISTS (
                        SELECT 1
                        FROM jsonb_array_elements(
                            CASE
                                WHEN jsonb_typeof(tp.legs_json) = 'array' THEN tp.legs_json
                                ELSE '[]'::jsonb
                            END
                        ) AS leg
                        WHERE COALESCE(leg->>'scheduled_start', '') ~ '^\\d{4}-\\d{2}-\\d{2}'
                          AND substring(leg->>'scheduled_start', 1, 10) = %s
                    )
                )
            """)
            vals.extend([target_date, target_iso, target_iso])
        if sport:
            clauses.append("""
            EXISTS (
                SELECT 1
                FROM jsonb_array_elements(
                    CASE
                        WHEN jsonb_typeof(tp.legs_json) = 'array' THEN tp.legs_json
                        ELSE '[]'::jsonb
                    END
                ) AS leg
                WHERE lower(COALESCE(leg->>'sport', '')) = %s
            )
            """
            )
            vals.append(str(sport).lower())
        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        cur.execute(f"""
            SELECT
                COUNT(*)                                           AS total,
                COUNT(*) FILTER (WHERE outcome='WIN')             AS wins,
                COUNT(*) FILTER (WHERE outcome='LOSS')            AS losses,
                COUNT(*) FILTER (WHERE outcome='PUSH')            AS pushes,
                COUNT(*) FILTER (WHERE outcome='PENDING')         AS pending,
                COALESCE(SUM(payout_usd),0)                       AS total_payout,
                COALESCE(SUM(stake_usd),0)                        AS total_staked
            FROM tracked_parlays tp
            {where_sql}
        """, vals)
        row = dict(cur.fetchone() or {})
        wins   = int(row.get("wins",   0))
        losses = int(row.get("losses", 0))
        staked = float(row.get("total_staked",  0))
        payout = float(row.get("total_payout",  0))
        row["hit_rate"] = round(wins / (wins + losses) * 100, 1) if (wins + losses) > 0 else None
        row["roi"]      = round((payout - staked) / staked * 100, 1) if staked > 0 else None
        return {k: (int(v) if isinstance(v, float) and v == int(v) else v)
                for k, v in row.items()}
    except Exception as e:
        print(f"[db] get_parlay_performance_stats error: {e}")
        return {}
    finally:
        conn.close()


def get_calibration_data(days_back: int = 90) -> dict:
    """
    Return raw model_prob + outcome data so the calibration curve can be computed.
    Also returns pre-computed ECE and bin stats.
    """
    conn = get_conn()
    if conn is None:
        return {}
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT model_prob, outcome, bet_type, game_date
            FROM   predictions
            WHERE  outcome IN ('WIN','LOSS')
              AND  model_prob IS NOT NULL
              AND  game_date >= CURRENT_DATE - INTERVAL '%s days'
            ORDER  BY model_prob
        """ % int(days_back))
        rows = cur.fetchall()
        if not rows:
            return {"total_resolved": 0, "bins": [], "ece": None}

        import numpy as np
        probs   = [float(r["model_prob"]) for r in rows]
        actuals = [1.0 if r["outcome"] == "WIN" else 0.0 for r in rows]
        probs_a   = np.array(probs)
        actuals_a = np.array(actuals)
        n = len(probs_a)

        bins = np.linspace(0, 1, 11)
        ece  = 0.0
        bin_stats = []
        for lo, hi in zip(bins[:-1], bins[1:]):
            mask = (probs_a >= lo) & (probs_a < hi)
            if not mask.any():
                continue
            bn       = int(mask.sum())
            avg_pred = float(probs_a[mask].mean())
            avg_act  = float(actuals_a[mask].mean())
            ece     += (bn / n) * abs(avg_pred - avg_act)
            bin_stats.append({
                "bin":        f"{lo:.1f}–{hi:.1f}",
                "n":          bn,
                "avg_pred":   round(avg_pred, 3),
                "avg_actual": round(avg_act, 3),
                "gap":        round(abs(avg_pred - avg_act), 3),
            })

        return {
            "total_resolved": n,
            "ece":            round(float(ece), 4),
            "bins":           bin_stats,
        }
    except Exception as e:
        print(f"[db] get_calibration_data error: {e}")
        return {}
    finally:
        conn.close()


def get_completed_games_for_training(sport: str = "mlb",
                                     seasons: list = None) -> list[dict]:
    """
    Return completed games with actual scores from the games table.
    Used to build real W/L training labels instead of synthetic season-run comparisons.
    Each returned dict: {home_team, away_team, home_score, away_score, game_date, season}
    """
    conn = get_conn()
    if conn is None:
        return []
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        seasons_list = None
        if seasons:
            seasons_list = [int(s) for s in seasons if s is not None]
            if not seasons_list:
                seasons_list = None

        cols = _get_table_columns(conn, "games")
        has_season = "season" in cols

        base_where = """
            sport = %s
            AND home_score IS NOT NULL
            AND away_score IS NOT NULL
            AND status IN ('Final','Game Over','Completed Early','Completed')
        """

        if has_season:
            if seasons_list:
                placeholders = ",".join(["%s"] * len(seasons_list))
                cur.execute(f"""
                    SELECT home_team, away_team, home_score, away_score,
                           game_date, season
                    FROM   games
                    WHERE  {base_where}
                      AND  season IN ({placeholders})
                    ORDER  BY game_date
                """, [sport] + seasons_list)
            else:
                cur.execute(f"""
                    SELECT home_team, away_team, home_score, away_score,
                           game_date, season
                    FROM   games
                    WHERE  {base_where}
                    ORDER  BY game_date
                """, (sport,))
        else:
            if seasons_list:
                placeholders = ",".join(["%s"] * len(seasons_list))
                cur.execute(f"""
                    SELECT home_team, away_team, home_score, away_score,
                           game_date, EXTRACT(YEAR FROM game_date)::int AS season
                    FROM   games
                    WHERE  {base_where}
                      AND  EXTRACT(YEAR FROM game_date)::int IN ({placeholders})
                    ORDER  BY game_date
                """, [sport] + seasons_list)
            else:
                cur.execute(f"""
                    SELECT home_team, away_team, home_score, away_score,
                           game_date, EXTRACT(YEAR FROM game_date)::int AS season
                    FROM   games
                    WHERE  {base_where}
                    ORDER  BY game_date
                """, (sport,))
        rows = [dict(r) for r in cur.fetchall()]
        for r in rows:
            if isinstance(r.get("game_date"), datetime.date):
                r["game_date"] = r["game_date"].isoformat()
        return rows
    except Exception as e:
        print(f"[db] get_completed_games_for_training error: {e}")
        return []
    finally:
        conn.close()

