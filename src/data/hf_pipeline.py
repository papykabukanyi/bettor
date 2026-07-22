"""
HF-first pipeline: automatic multi-sport predictions via HuggingFace.

Pipeline stages (all automatic from deployment):
  1. bootstrap_one_year_history() — One-time: load 1yr MLB/NBA/NHL/Soccer -> HF Dataset
  2. append_daily_results()       — Daily: append completed games -> HF Dataset
  3. train_and_publish_best_model() — Daily: retrain on full dataset -> HF Model Hub
  4. predict_daily_schedule()     — Daily: generate today+tomorrow predictions
  5. run_daily_pipeline()         — Orchestrates steps 2-4

Record IDs: every game record gets a UUID4 `record_id`.
Prediction IDs: every prediction gets a UUID4 `prediction_id`.
Sports: MLB (statsapi.mlb.com free), NBA (balldontlie.io free),
        NHL (api-web.nhle.com free), Soccer (thesportsdb free key "1").
Features: home_team, away_team, sport, season, month, day_of_week.
"""

from __future__ import annotations

import datetime
import csv
import io
import json
import logging
import os
import re
import tempfile
import time
import zipfile
from dataclasses import dataclass, field
from uuid import uuid4

import requests

from data.hf_uploader import HFUploader

try:
    from config import et_today
except Exception:
    def et_today() -> datetime.date:
        return datetime.date.today()

logger = logging.getLogger(__name__)


def _now_utc() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


@dataclass
class TrainSummary:
    repo_id: str
    rows: int
    best_model: str
    cv_roc_auc: float
    trained_at: str
    version: str = ""
    features: list = field(default_factory=list)
    sports_covered: list = field(default_factory=list)
    per_sport: dict = field(default_factory=dict)


class HFDirectPipeline:
    FINAL_STATES = frozenset(
        {"Final", "Game Over", "Completed Early", "Completed", "F", "STATUS_FINAL", "OFF", "FINAL", "OVER"}
    )
    UPCOMING_STATES = frozenset(
        {"Preview", "Pre-Game", "Scheduled", "Warmup", "Pre-game", "Sched", "FUT", "NS", "Pre-Preview"}
    )
    _CONFIDENCE_TIERS = [(0.70, "elite"), (0.60, "solid"), (0.55, "lean"), (0.0, "uncertain")]
    _CAT_FEATURES = ["home_team", "away_team", "sport"]
    _FORM_FEATURES = [
        "home_recent_win_rate", "away_recent_win_rate",
        "home_rest_days", "away_rest_days",
        "h2h_home_win_rate",
    ]
    _NEWS_FEATURES = [
        "home_news_sentiment", "away_news_sentiment",
        "home_negative_news_flag", "away_negative_news_flag",
    ]
    _NEWS_LOOKBACK_DAYS = 3
    _NUM_FEATURES = ["season", "month", "day_of_week"] + _FORM_FEATURES + _NEWS_FEATURES
    _TRAIN_FEATURES = _CAT_FEATURES + _NUM_FEATURES
    _MIN_ROWS_PER_SPORT = 60
    _MARKET_SPORT_ALIASES = {
        "baseball": "mlb",
        "mlb": "mlb",
        "wnba": "wnba",
        "women's basketball": "wnba",
        "womens basketball": "wnba",
        "basketball": "nba",
        "nba": "nba",
        "american football": "nfl",
        "nfl": "nfl",
        "hockey": "nhl",
        "nhl": "nhl",
        "soccer": "soccer",
        "football": "soccer",
        "tennis": "tennis",
        "lpga": "golf",
        "golf": "golf",
        "cricket": "cricket",
    }
    _SPORT_MARKET_PROFILES = {
        "mlb": [
            ("moneyline", "Game Winner", 1.0),
            ("first_3_innings", "First 3 Innings Winner", 0.67),
            ("first_5_innings", "First 5 Innings Winner", 0.72),
            ("first_7_innings", "First 7 Innings Winner", 0.80),
        ],
        "nba": [
            ("moneyline", "Game Winner", 1.0),
            ("first_quarter", "First Quarter Winner", 0.63),
            ("first_half", "First Half Winner", 0.74),
            ("third_quarter", "Third Quarter Winner", 0.66),
        ],
        "wnba": [
            ("moneyline", "Game Winner", 1.0),
            ("first_quarter", "First Quarter Winner", 0.63),
            ("first_half", "First Half Winner", 0.74),
            ("third_quarter", "Third Quarter Winner", 0.66),
        ],
        "nhl": [
            ("moneyline", "Game Winner", 1.0),
            ("first_period", "First Period Winner", 0.66),
            ("second_period", "Second Period Winner", 0.70),
        ],
        "soccer": [
            # Full-time result (1X2) — draw is real soccer outcome
            ("full_time_result",  "Full Time Result (1X2)",        1.00),
            ("moneyline",         "Match Winner (excl. draw)",     0.90),
            # Half-time markets
            ("first_half_result", "First Half Result (1X2)",       0.72),
            ("first_half_winner", "First Half Winner",             0.70),
            ("second_half_result","Second Half Result (1X2)",      0.74),
            # Total goals
            ("total_goals_over_2_5",   "Total Goals Over 2.5",    0.68),
            ("total_goals_under_2_5",  "Total Goals Under 2.5",   0.68),
            ("total_goals_over_1_5",   "Total Goals Over 1.5",    0.72),
            ("both_teams_to_score",    "Both Teams To Score",     0.70),
            # Team-specific totals
            ("home_team_over_0_5_goals","Home Team Over 0.5 Goals", 0.74),
            ("away_team_over_0_5_goals","Away Team Over 0.5 Goals", 0.72),
            # Double chance
            ("double_chance_home_draw","Double Chance: Home or Draw", 0.85),
            ("double_chance_away_draw","Double Chance: Away or Draw", 0.82),
            # Clean sheet
            ("home_clean_sheet", "Home Team Clean Sheet",          0.65),
            ("away_clean_sheet", "Away Team Clean Sheet",          0.62),
            # Asian handicap proxies
            ("asian_handicap_home", "Asian Handicap -0.5 Home",   0.88),
            ("asian_handicap_away", "Asian Handicap -0.5 Away",   0.85),
            # Corners / cards (volume stats)
            ("over_9_5_corners",  "Over 9.5 Match Corners",       0.60),
            ("over_3_5_cards",    "Over 3.5 Match Cards",         0.60),
        ],
        "tennis": [
            ("match_winner", "Match Winner", 1.0),
            ("set_1_winner", "Set 1 Winner", 0.76),
            ("set_2_winner", "Set 2 Winner", 0.74),
        ],
        "golf": [("winner", "Tournament Leader", 1.0)],
        "boxing": [("winner", "Fight Winner", 1.0)],
        "mma": [("winner", "Fight Winner", 1.0)],
        "cricket": [("moneyline", "Match Winner", 1.0), ("first_innings", "First Innings Winner", 0.74)],
        "nfl": [("moneyline", "Game Winner", 1.0), ("first_quarter", "First Quarter Winner", 0.64), ("first_half", "First Half Winner", 0.73)],
    }
    _PLAYER_PROP_PROFILES = {
        "mlb": [("hit_recorded", "Player To Record A Hit", 0.66), ("rbi_recorded", "Player To Record RBI", 0.58)],
        "nba": [("points_20_plus", "Player 20+ Points", 0.64), ("assists_5_plus", "Player 5+ Assists", 0.60)],
        "wnba": [("points_15_plus", "Player 15+ Points", 0.64), ("assists_4_plus", "Player 4+ Assists", 0.60)],
        "nhl": [("anytime_goal", "Anytime Goal Scorer", 0.58), ("point_recorded", "Player To Record A Point", 0.63)],
        "soccer": [
            # Goals
            ("anytime_goal",         "Anytime Goal Scorer",              0.57),
            ("first_goal_scorer",    "First Goal Scorer",                0.42),
            ("last_goal_scorer",     "Last Goal Scorer",                 0.42),
            ("brace_or_more",        "To Score 2+ Goals",                0.35),
            # Assists
            ("assist_recorded",      "Player To Record An Assist",       0.54),
            ("key_pass_recorded",    "Player Key Pass Recorded",         0.60),
            # Shots
            ("shot_on_target",       "Player Shot On Target",            0.65),
            ("shot_over_1_5",        "Player Over 1.5 Shots",            0.58),
            # Discipline
            ("yellow_card",          "Player To Receive Yellow Card",    0.52),
            ("to_be_carded",         "Player To Be Carded",              0.54),
            # Defensive / GK
            ("save_recorded",        "Goalkeeper Save Recorded",         0.72),
            ("saves_over_2_5",       "Goalkeeper Over 2.5 Saves",        0.60),
            ("clean_sheet_player",   "Player Keeps Clean Sheet",         0.55),
            # Involvement
            ("man_of_the_match",     "Man Of The Match",                 0.38),
            ("dribble_completed",    "Player Dribble Completed",         0.60),
            ("foul_committed",       "Player Foul Committed",            0.62),
            ("tackle_won",           "Player Tackle Won",                0.61),
        ],
        "nfl": [("anytime_td", "Anytime Touchdown", 0.60), ("rushing_yards_over", "Player Rushing Yards Over", 0.56)],
        "tennis": [("wins_set_1", "Player To Win Set 1", 0.62)],
        "cricket": [
            ("batter_runs_over_24_5", "Batter Runs Over 24.5", 0.64),
            ("batter_sixes_over_1_5", "Batter Sixes Over 1.5", 0.57),
            ("bowler_wickets_over_1_5", "Bowler Wickets Over 1.5", 0.60),
            ("player_to_score_50", "Player To Score 50+", 0.48),
        ],
    }
    _PLAYER_PROP_BASELINES = {
        # MLB
        "hit_recorded":        {"line": 0.5,  "unit": "hits",        "scale": 2.1},
        "rbi_recorded":        {"line": 0.5,  "unit": "rbi",         "scale": 1.6},
        # NBA
        "points_20_plus":      {"line": 19.5, "unit": "points",      "scale": 38.0},
        "assists_5_plus":      {"line": 4.5,  "unit": "assists",     "scale": 10.0},
        # WNBA
        "points_15_plus":      {"line": 14.5, "unit": "points",      "scale": 30.0},
        "assists_4_plus":      {"line": 3.5,  "unit": "assists",     "scale": 8.0},
        # NHL
        "anytime_goal":        {"line": 0.5,  "unit": "goals",       "scale": 1.3},
        "point_recorded":      {"line": 0.5,  "unit": "points",      "scale": 1.9},
        # Soccer — goals
        "first_goal_scorer":   {"line": 0.5,  "unit": "goals",       "scale": 1.0},
        "last_goal_scorer":    {"line": 0.5,  "unit": "goals",       "scale": 1.0},
        "brace_or_more":       {"line": 1.5,  "unit": "goals",       "scale": 3.0},
        # Soccer — assists / passes
        "assist_recorded":     {"line": 0.5,  "unit": "assists",     "scale": 1.5},
        "key_pass_recorded":   {"line": 0.5,  "unit": "key passes",  "scale": 2.5},
        # Soccer — shots
        "shot_on_target":      {"line": 0.5,  "unit": "shots on tgt","scale": 2.5},
        "shot_over_1_5":       {"line": 1.5,  "unit": "shots",       "scale": 4.0},
        # Soccer — discipline
        "yellow_card":         {"line": 0.5,  "unit": "cards",       "scale": 1.0},
        "to_be_carded":        {"line": 0.5,  "unit": "cards",       "scale": 1.0},
        # Soccer — goalkeeper
        "save_recorded":       {"line": 0.5,  "unit": "saves",       "scale": 5.0},
        "saves_over_2_5":      {"line": 2.5,  "unit": "saves",       "scale": 6.0},
        "clean_sheet_player":  {"line": 0.5,  "unit": "clean sheets","scale": 1.0},
        # Soccer — involvement
        "man_of_the_match":    {"line": 0.5,  "unit": "awards",      "scale": 1.0},
        "dribble_completed":   {"line": 0.5,  "unit": "dribbles",    "scale": 3.0},
        "foul_committed":      {"line": 0.5,  "unit": "fouls",       "scale": 2.5},
        "tackle_won":          {"line": 0.5,  "unit": "tackles",     "scale": 3.0},
        # NFL
        "anytime_td":          {"line": 0.5,  "unit": "touchdowns",  "scale": 1.4},
        "rushing_yards_over":  {"line": 59.5, "unit": "yards",       "scale": 120.0},
        # Tennis
        "wins_set_1":          {"line": 0.5,  "unit": "sets",        "scale": 1.0},
        # Cricket
        "batter_runs_over_24_5": {"line": 24.5, "unit": "runs",      "scale": 80.0},
        "batter_sixes_over_1_5": {"line": 1.5,  "unit": "sixes",     "scale": 6.0},
        "bowler_wickets_over_1_5": {"line": 1.5, "unit": "wickets",  "scale": 5.0},
        "player_to_score_50": {"line": 49.5, "unit": "runs",         "scale": 100.0},
    }
    _SPORT_ALIASES = {
        "baseball": "mlb",
        "mlb": "mlb",
        "wnba": "wnba",
        "women's basketball": "wnba",
        "womens basketball": "wnba",
        "basketball": "nba",
        "nba": "nba",
        "hockey": "nhl",
        "nhl": "nhl",
        "soccer": "soccer",
        "football": "soccer",
        "epl": "soccer",
        "la liga": "soccer",
        "serie a": "soccer",
        "bundesliga": "soccer",
        "ligue 1": "soccer",
        "tennis": "tennis",
        "lpga": "golf",
        "golf": "golf",
        "mma": "mma",
        "boxing": "boxing",
        "cricket": "cricket",
    }
    _NEWS_IMPACT_KEYWORDS = {
        "injury_concern": ("injury", "injured", "out", "doubtful", "questionable", "suspended", "ruled out", "hamstring", "ankle", "knee"),
        "lineup_change": ("starting", "starter", "lineup", "bench", "rotation", "activated", "returns", "called up", "debut"),
        "transfer_update": ("trade", "traded", "signed", "waived", "released", "loan", "extension", "contract"),
        "positive_momentum": ("dominant", "hot streak", "in form", "wins", "winning", "breakout", "career high"),
        "negative_momentum": ("slump", "cold streak", "struggling", "losing", "setback", "poor form"),
    }
    _NEWS_SENTIMENT_POS = ("win", "winning", "dominant", "returns", "healthy", "breakout", "strong", "surge")
    _NEWS_SENTIMENT_NEG = ("injury", "injured", "out", "suspended", "slump", "struggle", "doubtful", "setback")
    _NEWS_NEGATIVE_IMPACT_TYPES = frozenset({"injury_concern", "suspension", "lineup_change", "negative_momentum"})
    _NEWS_ADJUSTMENT = 0.04
    # ESPN's public scoreboard API (no key, no auth) supports these slugs directly.
    # Shared between the completed-results fetcher and the upcoming-schedule fetcher
    # so both see the same league breadth. Includes women's competitions.
    _SOCCER_ESPN_LEAGUES = [
        ("fifa.world", "FIFA World Cup 2026"),
        ("usa.1", "MLS"),
        ("eng.1", "Premier League"),
        ("eng.2", "EFL Championship"),
        ("esp.1", "La Liga"),
        ("ger.1", "Bundesliga"),
        ("ita.1", "Serie A"),
        ("fra.1", "Ligue 1"),
        ("ned.1", "Eredivisie"),
        ("por.1", "Primeira Liga"),
        ("sco.1", "Scottish Premiership"),
        ("tur.1", "Super Lig"),
        ("mex.1", "Liga MX"),
        ("arg.1", "Argentine Primera Division"),
        ("bra.1", "Brasileirao"),
        ("uefa.champions", "Champions League"),
        ("uefa.europa", "Europa League"),
        ("uefa.euro", "European Championship"),
        ("conmebol.copa", "Copa Libertadores"),
        # Women's soccer
        ("usa.nwsl", "NWSL (Women)"),
        ("eng.w.1", "FA Women's Super League"),
        ("uefa.wchampions", "UEFA Women's Champions League"),
    ]

    def __init__(
        self,
        token: str | None = None,
        dataset_repo: str | None = None,
        model_repo: str | None = None,
    ):
        try:
            from config import (
                HF_API_KEY,
                HF_DATASET_REPO,
                HF_MODEL_REPO,
                FOOTBALL_DATA_API_KEY,
                TENNIS_JEFF_SACKMANN_DIR,
                NEWSDATA_API_KEY,
                CRICKET_CRICSHEET_DIR,
                CRICKET_KAGGLE_DATA_DIR,
                CRICAPI_BASE_URL,
                CRICAPI_KEY,
                CRICKET_RAPIDAPI_BASE_URL,
                CRICKET_RAPIDAPI_HOST,
                CRICKET_RAPIDAPI_KEY,
                THESPORTSDB_API_KEY,
                BALLDONTLIE_API_KEY,
                BALLDONTLIE_BASE_URL,
            )
        except Exception:
            HF_API_KEY = os.getenv("HF_API_KEY", "")
            HF_DATASET_REPO = os.getenv("HF_DATASET_REPO", "papylove/sportprediction")
            HF_MODEL_REPO = os.getenv("HF_MODEL_REPO", "papylove/sportprediction")
            FOOTBALL_DATA_API_KEY = os.getenv("FOOTBALL_DATA_API_KEY", "")
            TENNIS_JEFF_SACKMANN_DIR = os.getenv("TENNIS_JEFF_SACKMANN_DIR", "")
            NEWSDATA_API_KEY = os.getenv("NEWSDATA_API_KEY", "")
            CRICKET_CRICSHEET_DIR = os.getenv("CRICKET_CRICSHEET_DIR", "")
            CRICKET_KAGGLE_DATA_DIR = os.getenv("CRICKET_KAGGLE_DATA_DIR", "")
            CRICAPI_BASE_URL = os.getenv("CRICAPI_BASE_URL", "https://api.cricapi.com/v1")
            CRICAPI_KEY = os.getenv("CRICAPI_KEY", "")
            CRICKET_RAPIDAPI_BASE_URL = os.getenv("CRICKET_RAPIDAPI_BASE_URL", "https://cricket-live-data.p.rapidapi.com")
            CRICKET_RAPIDAPI_HOST = os.getenv("CRICKET_RAPIDAPI_HOST", "cricket-live-data.p.rapidapi.com")
            CRICKET_RAPIDAPI_KEY = os.getenv("CRICKET_RAPIDAPI_KEY", "")
            THESPORTSDB_API_KEY = os.getenv("THESPORTSDB_API_KEY", "3")
            BALLDONTLIE_API_KEY = os.getenv("BALLDONTLIE_API_KEY", "")
            BALLDONTLIE_BASE_URL = os.getenv("BALLDONTLIE_BASE_URL", "https://api.balldontlie.io/v1")

        self.token = str(token or HF_API_KEY or "").strip()
        self.thesportsdb_api_key = str(THESPORTSDB_API_KEY or "3").strip() or "3"
        self.balldontlie_api_key = str(BALLDONTLIE_API_KEY or "").strip()
        self.balldontlie_base_url = str(BALLDONTLIE_BASE_URL or "https://api.balldontlie.io/v1").strip().rstrip("/")
        self.football_data_api_key = str(FOOTBALL_DATA_API_KEY or "").strip()
        self.tennis_sackmann_dir = str(TENNIS_JEFF_SACKMANN_DIR or "").strip()
        self.newsdata_api_key = str(NEWSDATA_API_KEY or "").strip()
        self.cricsheet_dir = str(CRICKET_CRICSHEET_DIR or "").strip()
        self.cricket_kaggle_dir = str(CRICKET_KAGGLE_DATA_DIR or "").strip()
        self.cricapi_base_url = str(CRICAPI_BASE_URL or "https://api.cricapi.com/v1").strip().rstrip("/")
        self.cricapi_key = str(CRICAPI_KEY or "").strip()
        self.cricket_rapidapi_base_url = str(CRICKET_RAPIDAPI_BASE_URL or "https://cricket-live-data.p.rapidapi.com").strip().rstrip("/")
        self.cricket_rapidapi_host = str(CRICKET_RAPIDAPI_HOST or "cricket-live-data.p.rapidapi.com").strip()
        self.cricket_rapidapi_key = str(CRICKET_RAPIDAPI_KEY or "").strip()
        self.uploader = HFUploader(token=self.token, repo_name=dataset_repo or HF_DATASET_REPO)
        self.dataset_repo_id = getattr(self.uploader, "_repo_id", "")
        self.model_repo_name = str(model_repo or HF_MODEL_REPO or "sports-win-model").strip()
        self.model_repo_id = self.model_repo_name
        self._data_dir = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data"
        )
        self._status_file = os.getenv(
            "HF_PIPELINE_STATUS_FILE",
            os.path.join(self._data_dir, "hf_pipeline_status.json"),
        )
        self._predictions_file = os.getenv(
            "HF_DAILY_PREDICTIONS_FILE",
            os.path.join(self._data_dir, "hf_daily_predictions.json"),
        )
        self._training_history_file = os.path.join(self._data_dir, "training_history.json")
        self._api = None
        self._model_cache: dict[str, Any] = {}
        # A single active-cycle run (append -> train -> predict) independently
        # downloaded the FULL games and news_signals datasets from HF Hub up to
        # ~4x and ~3x respectively (append dedupe check, training, feature
        # engineering, form/news snapshots each re-fetched from scratch). Every
        # shard listing + download is its own HF API request, and this was a
        # major contributor to exhausting HF's rate limit within one cycle.
        # Cache both for the span of one cycle so repeated calls reuse the same
        # in-memory copy instead of re-downloading everything each time.
        self._games_df_cache: tuple[float, Any] | None = None
        self._news_df_cache: tuple[float, Any] | None = None
        self._df_cache_ttl_sec = float(os.getenv("HF_PIPELINE_DF_CACHE_TTL_SEC", "600") or "600")
        self._ok = bool(self.token and self.uploader and getattr(self.uploader, "_ok", False))

        if self._ok:
            try:
                from huggingface_hub import HfApi
                self._api = HfApi(token=self.token)
            except Exception as exc:
                logger.warning("[hf_pipeline] HF API init failed: %s", exc)
                self._ok = False
            else:
                # Only resolve the username when model_repo_id doesn't already have
                # an explicit owner. This is the common case (HF_MODEL_REPO=
                # "owner/repo") and skips whoami() entirely, avoiding HF's strict
                # rate limit on that endpoint. A whoami failure is not fatal to the
                # pipeline -- it just means the repo id stays unprefixed.
                if "/" not in self.model_repo_id:
                    try:
                        from data.hf_uploader import _cached_hf_username

                        user = _cached_hf_username(self._api, self.token)
                        if user:
                            self.model_repo_id = f"{user}/{self.model_repo_id}"
                    except Exception as exc:
                        logger.warning("[hf_pipeline] HF username resolution skipped: %s", exc)

    @property
    def ok(self) -> bool:
        return self._ok

    # ──────────────────────────────────────────────────────────
    # Public pipeline methods
    # ──────────────────────────────────────────────────────────

    def bootstrap_one_year_history(self, days_back: int = 365) -> dict:
        """One-time: fetch and upload ~1yr of multi-sport game data to HF Dataset."""
        end = et_today()
        start = end - datetime.timedelta(days=max(1, int(days_back)))
        logger.info("[hf_pipeline] Bootstrapping %d days (%s to %s)", days_back, start, end)
        records = self._clean_game_records(self._fetch_completed_games(start, end))
        if not records:
            self._write_status({"last_step": "bootstrap", "ok": False, "message": "No records found"})
            return {"ok": False, "msg": "No historical records found", "records": 0}
        self.uploader.push_records("games", records)
        self.uploader.flush_all()
        sports = sorted({r.get("sport", "") for r in records})
        self._write_status({
            "last_step": "bootstrap",
            "ok": True,
            "bootstrap_records": len(records),
            "bootstrap_sports": sports,
            "bootstrap_date_range": f"{start} to {end}",
            "bootstrap_completed_at": _now_utc(),
        })
        logger.info("[hf_pipeline] Bootstrap done: %d records, sports=%s", len(records), sports)
        return {"ok": True, "records": len(records), "sports": sports, "date_range": f"{start} to {end}"}

    def append_daily_results(self, day: datetime.date | None = None) -> dict:
        """Daily: append completed games for `day` to HF Dataset."""
        target = day or et_today()
        logger.info("[hf_pipeline] Appending daily results for %s", target)
        records = self._clean_game_records(self._fetch_completed_games(target, target))
        records = self._filter_records_not_in_hub(records)
        if not records:
            self._write_status({
                "last_step": "append_daily", "ok": True,
                "append_records": 0, "append_date": target.isoformat(), "append_sports": [],
            })
            return {"ok": True, "records": 0, "date": target.isoformat()}
        self.uploader.push_records("games", records)
        self.uploader.flush_all()
        sports = sorted({r.get("sport", "") for r in records})
        self._write_status({
            "last_step": "append_daily", "ok": True,
            "append_records": len(records), "append_date": target.isoformat(),
            "append_sports": sports, "append_completed_at": _now_utc(),
        })
        return {"ok": True, "records": len(records), "date": target.isoformat(), "sports": sports}

    def _fit_best_model(self, X, y, forced_model: str = "auto") -> tuple[str, float, object]:
        """Cross-validate the candidate classifiers on (X, y) and fit the best one
        on the full data. Shared by the global model and every per-sport model so
        selection logic can't drift between them.
        """
        from sklearn.compose import ColumnTransformer
        from sklearn.ensemble import ExtraTreesClassifier, GradientBoostingClassifier, HistGradientBoostingClassifier, RandomForestClassifier
        from sklearn.linear_model import LogisticRegression
        from sklearn.model_selection import StratifiedKFold, cross_val_score
        from sklearn.pipeline import Pipeline
        from sklearn.preprocessing import OneHotEncoder

        pre = ColumnTransformer(
            transformers=[
                ("cats", OneHotEncoder(handle_unknown="ignore"), self._CAT_FEATURES),
                ("nums", "passthrough", self._NUM_FEATURES),
            ]
        )
        candidates = {
            "logistic_regression": LogisticRegression(max_iter=2000, random_state=42),
            "random_forest": RandomForestClassifier(n_estimators=300, random_state=42, n_jobs=-1),
            "gradient_boosting": GradientBoostingClassifier(n_estimators=200, random_state=42),
            "extra_trees": ExtraTreesClassifier(n_estimators=400, random_state=42, n_jobs=-1),
            # HistGradientBoosting needs dense input — use a separate pre with sparse=False
            "hist_gradient_boosting": HistGradientBoostingClassifier(random_state=42),
        }
        forced_key = str(forced_model or "auto").strip().lower()
        if forced_key and forced_key != "auto" and forced_key in candidates:
            candidates = {forced_key: candidates[forced_key]}

        n_splits = min(5, int(y.value_counts().min())) if hasattr(y, "value_counts") else 5
        if n_splits < 2:
            raise RuntimeError("Not enough class diversity to cross-validate (need both winners and losers)")
        cv = StratifiedKFold(n_splits=n_splits, shuffle=True, random_state=42)
        best_name, best_score, best_pipeline = "", -1.0, None
        for name, model in candidates.items():
            # HistGradientBoosting needs dense arrays — use separate pre with sparse_output=False
            if name == "hist_gradient_boosting":
                from sklearn.preprocessing import OneHotEncoder as _OHE
                _pre_dense = ColumnTransformer(
                    transformers=[
                        ("cats", _OHE(handle_unknown="ignore", sparse_output=False), self._CAT_FEATURES),
                        ("nums", "passthrough", self._NUM_FEATURES),
                    ]
                )
                pipe = Pipeline([("pre", _pre_dense), ("model", model)])
            else:
                pipe = Pipeline([("pre", pre), ("model", model)])
            try:
                scores = cross_val_score(pipe, X, y, cv=cv, scoring="roc_auc")
                score = float(scores.mean())
                logger.info("[hf_pipeline] %s CV AUC=%.4f", name, score)
                if score > best_score:
                    best_score, best_name, best_pipeline = score, name, pipe
            except Exception as exc:
                logger.warning("[hf_pipeline] %s failed: %s", name, exc)

        if best_pipeline is None:
            raise RuntimeError("No model candidate succeeded")
        best_pipeline.fit(X, y)
        return best_name, best_score, best_pipeline

    def train_and_publish_best_model(self, min_rows: int = 200, forced_model: str = "auto") -> TrainSummary:
        """Daily: train an INDIVIDUAL classifier per sport (model_{sport}.joblib)
        plus one combined model.joblib as a fallback for sports without enough
        rows yet, and publish all of them to HF Model Hub.
        """
        import joblib

        if not self._ok or not self._api:
            raise RuntimeError("HF pipeline not configured. Set HF_API_KEY.")

        logger.info("[hf_pipeline] Loading games parquet shards from %s", self.dataset_repo_id)
        df = self._load_games_dataframe_from_hub()
        if df.empty:
            raise RuntimeError("HF dataset has no rows in games/train split")

        df = self._build_training_df(df)
        if len(df) < min_rows:
            raise RuntimeError(f"Not enough rows: {len(df)} < {min_rows}")

        y_all = (df["home_score"] > df["away_score"]).astype(int)
        X_all = df[self._TRAIN_FEATURES].copy()
        sports_covered = sorted(df["sport"].dropna().astype(str).unique().tolist())

        # Global model: trained across every sport combined. Always published as
        # model.joblib -- the fallback for any sport that doesn't (yet) have
        # enough rows of its own, and for callers on an older deployment that
        # only knows about a single model file.
        best_name, best_score, best_pipeline = self._fit_best_model(X_all, y_all, forced_model)

        # Individual per-sport models: each sport learns its own scoring
        # dynamics instead of being blended into one global average.
        per_sport_models: dict[str, tuple[str, float, object, int]] = {}
        for sport in sports_covered:
            sport_df = df[df["sport"] == sport]
            if len(sport_df) < self._MIN_ROWS_PER_SPORT:
                logger.info(
                    "[hf_pipeline] Skipping individual model for %s: %d rows < min %d (falls back to global model)",
                    sport, len(sport_df), self._MIN_ROWS_PER_SPORT,
                )
                continue
            y_sport = (sport_df["home_score"] > sport_df["away_score"]).astype(int)
            if y_sport.nunique() < 2:
                logger.info("[hf_pipeline] Skipping individual model for %s: only one outcome class present", sport)
                continue
            X_sport = sport_df[self._TRAIN_FEATURES].copy()
            try:
                s_name, s_score, s_pipeline = self._fit_best_model(X_sport, y_sport, forced_model)
                per_sport_models[sport] = (s_name, s_score, s_pipeline, len(sport_df))
                logger.info("[hf_pipeline] %s individual model: %s CV AUC=%.4f (%d rows)", sport, s_name, s_score, len(sport_df))
            except Exception as exc:
                logger.warning("[hf_pipeline] Individual model for %s failed, will use global fallback: %s", sport, exc)

        trained_at = _now_utc()
        version = trained_at[:19].replace(":", "-").replace("T", "_")
        per_sport_meta = {
            sport: {
                "best_model": s_name, "cv_roc_auc": round(s_score, 6), "rows": s_rows,
                "model_file": f"model_{sport}.joblib",
            }
            for sport, (s_name, s_score, s_pipeline, s_rows) in per_sport_models.items()
        }
        metadata = {
            "version": version, "trained_at": trained_at,
            "rows": int(len(df)), "best_model": best_name,
            "cv_roc_auc": round(best_score, 6),
            "dataset_repo": self.dataset_repo_id,
            "features": self._TRAIN_FEATURES,
            "categorical_features": self._CAT_FEATURES,
            "numerical_features": self._NUM_FEATURES,
            "target": "home_win", "sports_covered": sports_covered,
            "per_sport": per_sport_meta,
            "min_rows_per_sport": self._MIN_ROWS_PER_SPORT,
        }

        news_train_summary: dict[str, object] = {"ok": False, "reason": "not_run"}
        with tempfile.TemporaryDirectory(prefix="hf_model_") as td:
            model_path = os.path.join(td, "model.joblib")
            meta_path = os.path.join(td, "metadata.json")
            readme_path = os.path.join(td, "README.md")
            joblib.dump(best_pipeline, model_path)
            upload_pairs = [("model.joblib", model_path), ("metadata.json", meta_path), ("README.md", readme_path)]
            for sport, (s_name, s_score, s_pipeline, s_rows) in per_sport_models.items():
                sport_model_path = os.path.join(td, f"model_{sport}.joblib")
                joblib.dump(s_pipeline, sport_model_path)
                upload_pairs.append((f"model_{sport}.joblib", sport_model_path))

            news_train_summary = self._train_news_impact_model(output_dir=td)
            metadata["news_impact_model"] = {
                "enabled": bool(news_train_summary.get("ok")),
                "rows": int(news_train_summary.get("rows") or 0),
                "classes": list(news_train_summary.get("classes") or []),
                "cv_f1_macro": float(news_train_summary.get("cv_f1_macro") or 0.0),
            }
            with open(meta_path, "w", encoding="utf-8") as f:
                json.dump(metadata, f, indent=2)
            readme_content = self._build_model_card_readme(metadata)
            with open(readme_path, "w", encoding="utf-8") as f:
                f.write(readme_content)
            self._api.create_repo(repo_id=self.model_repo_id, repo_type="model", exist_ok=True, private=False)
            news_model_path = os.path.join(td, "news_impact_model.joblib")
            news_meta_path = os.path.join(td, "news_impact_metadata.json")
            if os.path.exists(news_model_path):
                upload_pairs.append(("news_impact_model.joblib", news_model_path))
            if os.path.exists(news_meta_path):
                upload_pairs.append(("news_impact_metadata.json", news_meta_path))
            for fname, fpath in upload_pairs:
                self._api.upload_file(
                    path_or_fileobj=fpath, path_in_repo=fname,
                    repo_id=self.model_repo_id, repo_type="model",
                    commit_message=f"v{version}: update {fname}",
                )
        logger.info(
            "[hf_pipeline] Published model v%s to %s (global AUC=%.4f, %d individual sport models)",
            version, self.model_repo_id, best_score, len(per_sport_models),
        )

        # Individual models fall back to the combined-dataset model's AUC in
        # sports_covered summaries below; self._model_cache is invalidated so the
        # next prediction run downloads today's freshly published files instead
        # of whatever was cached from a previous version.
        self._model_cache = {}

        summary = TrainSummary(
            repo_id=self.model_repo_id, rows=int(len(df)),
            best_model=best_name, cv_roc_auc=float(best_score),
            trained_at=trained_at, version=version,
            features=self._TRAIN_FEATURES, sports_covered=sports_covered,
            per_sport=per_sport_meta,
        )
        self._append_training_history(summary)
        self._write_status({
            "last_step": "train_publish", "ok": True,
            "trained_rows": int(len(df)), "best_model": best_name,
            "cv_roc_auc": round(float(best_score), 6),
            "model_repo": self.model_repo_id,
            "trained_at": trained_at, "model_version": version,
            "sports_covered": sports_covered,
            "per_sport": per_sport_meta,
            "news_impact_model": {
                "enabled": bool(news_train_summary.get("ok")),
                "rows": int(news_train_summary.get("rows") or 0),
                "cv_f1_macro": float(news_train_summary.get("cv_f1_macro") or 0.0),
            },
            "train_completed_at": _now_utc(),
        })
        return summary

    def ensure_model_card_metadata(self) -> dict:
        """Ensure model repo README has valid YAML metadata frontmatter."""
        from huggingface_hub import hf_hub_download

        if not self._ok or not self._api:
            return {"ok": False, "updated": False, "reason": "hf_not_configured"}
        try:
            metadata_path = hf_hub_download(
                repo_id=self.model_repo_id,
                repo_type="model",
                filename="metadata.json",
                token=self.token,
            )
            with open(metadata_path, "r", encoding="utf-8") as f:
                metadata = json.load(f) or {}
        except Exception as exc:
            return {"ok": False, "updated": False, "reason": f"missing_metadata:{exc}"}

        needs_update = True
        try:
            readme_path = hf_hub_download(
                repo_id=self.model_repo_id,
                repo_type="model",
                filename="README.md",
                token=self.token,
            )
            with open(readme_path, "r", encoding="utf-8") as f:
                existing = f.read()
            if existing.lstrip().startswith("---"):
                needs_update = False
        except Exception:
            needs_update = True

        if not needs_update:
            return {"ok": True, "updated": False}

        content = self._build_model_card_readme(metadata)
        self._api.upload_file(
            path_or_fileobj=io.BytesIO(content.encode("utf-8")),
            path_in_repo="README.md",
            repo_id=self.model_repo_id,
            repo_type="model",
            commit_message="Add model card metadata",
        )
        return {"ok": True, "updated": True}

    def predict_daily_schedule(
        self,
        day: datetime.date | None = None,
        output_path: str | None = None,
        via_api: bool = False,
        model_id: str | None = None,
        endpoint_url: str | None = None,
    ) -> dict:
        """Daily: generate predictions for today and tomorrow, save to JSON.

        Today gets the in-depth pass (freshest same-day injury/lineup news folds
        into the probability); tomorrow gets the standard pass and is snapshotted
        so tomorrow's run can compare its own in-depth today-prediction against
        what was foreseen for it a day earlier.
        """
        today = day or et_today()
        tomorrow = today + datetime.timedelta(days=1)
        meta = self._get_model_metadata()
        model_version = meta.get("version", "unknown")
        model_type = meta.get("best_model", "unknown")
        model_auc = float(meta.get("cv_roc_auc") or 0.0)

        team_stats, h2h_stats = self._build_form_snapshot()
        news_stats = self._build_news_snapshot()

        today_negative_teams: set[str] = set()
        if not via_api:
            try:
                news_result = self.collect_news_signals(days=[today], return_rows=True)
                for sig in news_result.get("signals") or []:
                    impact = str(sig.get("impact_type") or "")
                    team = str(sig.get("entity_team") or "").strip().lower()
                    if team and impact in self._NEWS_NEGATIVE_IMPACT_TYPES:
                        today_negative_teams.add(team)
            except Exception as exc:
                logger.debug("[hf_pipeline] today news adjustment skipped: %s", exc)

        all_predictions: list[dict] = []
        player_cache: dict[str, list[str]] = {}
        sports_seen: set[str] = set()
        for target_date in [today, tomorrow]:
            is_today = target_date == today
            depth_tag = "deep" if is_today else "standard"
            games = self._fetch_upcoming_games(target_date)
            for g in games:
                home_team = str(g.get("home_team") or "").strip()
                away_team = str(g.get("away_team") or "").strip()
                if not home_team or not away_team:
                    continue
                sport = str(g.get("sport") or "mlb")
                sports_seen.add(self._normalize_sport(sport))
                league = str(g.get("league") or sport.upper())
                game_date = str(g.get("game_date") or target_date.isoformat())
                game_time = str(g.get("game_time") or "")
                game_id = str(g.get("game_id") or "")
                home_starter = str(g.get("home_starter") or "").strip()
                away_starter = str(g.get("away_starter") or "").strip()
                home_team_id = str(g.get("home_team_id") or "").strip()
                away_team_id = str(g.get("away_team_id") or "").strip()
                try:
                    if via_api:
                        resp = self.predict_via_hf_api(home_team, away_team, target_date.year, model_id, endpoint_url)
                        api_resp = resp.get("response") or {}
                        home_prob = float(api_resp[0].get("score", 0.5) if isinstance(api_resp, list) else 0.5)
                        away_prob = 1.0 - home_prob
                    else:
                        pred = self.predict_from_model_repo(
                            home_team=home_team, away_team=away_team,
                            sport=sport, season=target_date.year,
                            team_stats=team_stats, h2h_stats=h2h_stats,
                            news_stats=news_stats,
                        )
                        home_prob = float(pred.get("home_win_prob", 0.5))
                        away_prob = float(pred.get("away_win_prob", 0.5))
                        if is_today and today_negative_teams:
                            home_prob, away_prob = self._apply_news_adjustment(
                                home_prob, away_prob,
                                home_team.strip().lower() in today_negative_teams,
                                away_team.strip().lower() in today_negative_teams,
                            )
                    market_rows = self._expand_market_predictions(
                        game_id=game_id,
                        sport=sport,
                        league=league,
                        home_team=home_team,
                        away_team=away_team,
                        game_date=game_date,
                        game_time=game_time,
                        home_prob=home_prob,
                        away_prob=away_prob,
                        model_version=model_version,
                        model_type=model_type,
                        model_auc=model_auc,
                        via_api=via_api,
                    )
                    prop_rows = self._expand_player_prop_predictions(
                        sport=sport,
                        league=league,
                        game_id=game_id,
                        game_date=game_date,
                        game_time=game_time,
                        home_team=home_team,
                        away_team=away_team,
                        home_prob=home_prob,
                        away_prob=away_prob,
                        model_version=model_version,
                        model_type=model_type,
                        model_auc=model_auc,
                        via_api=via_api,
                        player_cache=player_cache,
                        home_starter=home_starter,
                        away_starter=away_starter,
                        home_team_id=home_team_id,
                        away_team_id=away_team_id,
                        team_stats=team_stats,
                    )
                    for r in market_rows:
                        r["analysis_depth"] = depth_tag
                    for r in prop_rows:
                        r["analysis_depth"] = depth_tag
                    all_predictions.extend(market_rows)
                    all_predictions.extend(prop_rows)
                except Exception as exc:
                    logger.warning("[hf_pipeline] predict error %s vs %s: %s", home_team, away_team, exc)
                    all_predictions.append({
                        "prediction_id": str(uuid4()),
                        "game_id": game_id, "sport": sport, "league": league,
                        "home_team": home_team, "away_team": away_team,
                        "game_date": game_date, "game_time": game_time,
                        "error": str(exc), "predicted_at": _now_utc(),
                    })

        good = [p for p in all_predictions if not p.get("error")]
        today_rows = [p for p in good if str(p.get("game_date") or "")[:10] == today.isoformat()]
        tomorrow_rows = [p for p in good if str(p.get("game_date") or "")[:10] == tomorrow.isoformat()]

        # The day-over-day comparison is a nice-to-have on top of the core
        # deliverable below (the fresh predictions file) -- any failure here
        # (malformed snapshot data, disk issue, etc.) must never prevent today's
        # predictions from being written, so it's fully isolated in its own
        # try/except rather than left to bubble up and abort the whole run.
        drift_summary = None
        try:
            drift_summary = self._compare_with_snapshot(today, today_rows)
            if drift_summary is not None:
                drift_path = os.path.join(self._drift_dir(), f"{today.isoformat()}.json")
                try:
                    with open(drift_path, "w", encoding="utf-8") as f:
                        json.dump(drift_summary, f, indent=2)
                except Exception as exc:
                    logger.debug("[hf_pipeline] drift write skipped: %s", exc)
                logger.info(
                    "[hf_pipeline] Prediction drift vs yesterday's forecast for %s: %d matched, %d flips, avg |delta|=%.4f",
                    today, drift_summary["matched_count"], drift_summary["pick_flips"], drift_summary["avg_abs_prob_delta"],
                )
            # Yesterday's snapshot for this date is consumed (compared or unavailable) --
            # discard it regardless, and store today's fresh preliminary look at
            # tomorrow for the next run.
            self._delete_snapshot(today)
            self._save_snapshot(tomorrow, tomorrow_rows)
            self._cleanup_stale_snapshots(today)
        except Exception as exc:
            logger.warning("[hf_pipeline] prediction drift/snapshot step failed, continuing without it: %s", exc)

        out_path = output_path or self._predictions_file
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        payload = {
            "generated_at": _now_utc(),
            "today": today.isoformat(),
            "tomorrow": tomorrow.isoformat(),
            "prediction_count": len(good),
            "error_count": len(all_predictions) - len(good),
            "model_version": model_version,
            "model_type": model_type,
            "model_auc": model_auc,
            "prediction_drift": drift_summary,
            "predictions": all_predictions,
        }
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        try:
            from data.db import save_predictions

            save_predictions(good)
        except Exception as exc:
            logger.debug("[hf_pipeline] prediction DB save skipped: %s", exc)
        self._write_status({
            "last_step": "predict_daily", "ok": True,
            "prediction_date": today.isoformat(),
            "prediction_count": len(good),
            "today_count": len(today_rows),
            "tomorrow_count": len(tomorrow_rows),
            "prediction_sports": sorted(sports_seen),
            "prediction_file": out_path,
            "model_version": model_version,
            "prediction_drift_summary": (
                {
                    "matched_count": drift_summary["matched_count"],
                    "pick_flips": drift_summary["pick_flips"],
                    "avg_abs_prob_delta": drift_summary["avg_abs_prob_delta"],
                }
                if drift_summary
                else None
            ),
            "predict_completed_at": _now_utc(),
        })
        logger.info("[hf_pipeline] %d predictions generated (%s + %s)", len(good), today, tomorrow)
        return {"ok": True, "prediction_count": len(good), "output_file": out_path, "date": today.isoformat()}

    def _expand_player_prop_predictions(
        self,
        *,
        sport: str,
        league: str,
        game_id: str,
        game_date: str,
        game_time: str,
        home_team: str,
        away_team: str,
        home_prob: float,
        away_prob: float,
        model_version: str,
        model_type: str,
        model_auc: float,
        via_api: bool,
        player_cache: dict[str, list[str]],
        home_starter: str = "",
        away_starter: str = "",
        home_team_id: str = "",
        away_team_id: str = "",
        team_stats: dict | None = None,
    ) -> list[dict]:
        normalized_sport = self._normalize_sport(sport)
        profile = self._PLAYER_PROP_PROFILES.get(normalized_sport) or []
        if not profile:
            return []

        home_players = ([home_starter] if home_starter else []) + self._fetch_team_players_thesportsdb(home_team, normalized_sport, player_cache)
        away_players = ([away_starter] if away_starter else []) + self._fetch_team_players_thesportsdb(away_team, normalized_sport, player_cache)
        if normalized_sport == "mlb":
            if not home_players and home_team_id:
                home_players = self._fetch_mlb_team_players(home_team_id, player_cache)
            if not away_players and away_team_id:
                away_players = self._fetch_mlb_team_players(away_team_id, player_cache)
        if normalized_sport == "soccer":
            if not home_players:
                home_players = self._fetch_soccer_squad_espn(home_team, player_cache)
            if not away_players:
                away_players = self._fetch_soccer_squad_espn(away_team, player_cache)
        if not home_players and normalized_sport == "cricket":
            home_players = self._fetch_cricket_players_cricapi(home_team, player_cache)
        if not away_players and normalized_sport == "cricket":
            away_players = self._fetch_cricket_players_cricapi(away_team, player_cache)
        if not home_players and not away_players:
            return []

        # Soccer/cricket: use more players per team for richer prop coverage.
        max_players = 2
        if normalized_sport == "soccer":
            max_players = 4
        elif normalized_sport == "cricket":
            max_players = 3
        rows: list[dict] = []
        candidate_players = (
            [(home_team, p, home_prob) for p in home_players[:max_players]] +
            [(away_team, p, away_prob) for p in away_players[:max_players]]
        )
        for team_name, player_name, team_prob in candidate_players:
            team_form = (team_stats or {}).get((normalized_sport, str(team_name).strip().lower())) or {}
            recent_win_rate = float(team_form.get("recent_win_rate", 0.5))
            # Blend the matchup win probability with the player's own team's recent
            # form so a player on a hot/cold streak shifts prop confidence, not just
            # the head-to-head odds.
            blended_prob = 0.7 * float(team_prob) + 0.3 * recent_win_rate
            for prop_type, prop_name, weight in profile:
                p_yes = 0.5 + (blended_prob - 0.5) * float(weight)
                p_yes = max(0.05, min(0.95, p_yes))
                p_no = 1.0 - p_yes
                predicted_outcome = "OVER" if p_yes >= p_no else "UNDER"
                confidence = max(p_yes, p_no)
                details = self._player_prop_details(prop_type, p_yes)
                line_value = float(details.get("line", 0.5))
                projection = float(details.get("projection", 0.5))
                unit = str(details.get("unit", "stat"))
                predicted_label = (
                    f"{predicted_outcome} {line_value:.1f} {unit}"
                    if line_value < 10
                    else f"{predicted_outcome} {line_value:.1f}"
                )
                rows.append(
                    {
                        "prediction_id": str(uuid4()),
                        "game_id": game_id,
                        "sport": normalized_sport,
                        "league": league,
                        "home_team": home_team,
                        "away_team": away_team,
                        "game_date": game_date,
                        "game_time": game_time,
                        "prediction_scope": "player_prop",
                        "player_name": player_name,
                        "player_team": team_name,
                        "market_type": prop_type,
                        "market_name": prop_name,
                        "prop_line": round(line_value, 2),
                        "prop_unit": unit,
                        "projected_value": round(projection, 2),
                        "over_prob": round(p_yes, 4),
                        "under_prob": round(p_no, 4),
                        "predicted_side": predicted_outcome.lower(),
                        "predicted_label": predicted_label,
                        "predicted_outcome": predicted_outcome,
                        "predicted_team": team_name,
                        "home_win_prob": round(p_yes, 4),
                        "away_win_prob": round(p_no, 4),
                        "confidence": round(confidence, 4),
                        "confidence_tier": self._confidence_tier(confidence),
                        "model_version": model_version,
                        "model_type": model_type,
                        "model_name": model_type,
                        "model_auc": model_auc,
                        "predicted_at": _now_utc(),
                        "predict_mode": "api" if via_api else "artifact",
                    }
                )
        return rows

    def _player_prop_details(self, prop_type: str, p_yes: float) -> dict:
        base = self._PLAYER_PROP_BASELINES.get(str(prop_type or "").strip().lower(), {"line": 0.5, "unit": "stat", "scale": 1.0})
        line = float(base.get("line", 0.5))
        unit = str(base.get("unit", "stat"))
        scale = float(base.get("scale", max(1.0, line * 2.0)))
        projection = max(0.0, min(scale, p_yes * scale))
        return {"line": line, "unit": unit, "projection": projection}

    def _expand_market_predictions(
        self,
        *,
        game_id: str,
        sport: str,
        league: str,
        home_team: str,
        away_team: str,
        game_date: str,
        game_time: str,
        home_prob: float,
        away_prob: float,
        model_version: str,
        model_type: str,
        model_auc: float,
        via_api: bool,
    ) -> list[dict]:
        rows: list[dict] = []
        normalized_sport = self._normalize_sport(sport)
        profile = self._SPORT_MARKET_PROFILES.get(normalized_sport) or [("moneyline", "Game Winner", 1.0)]

        # For soccer, derive a 3-way distribution (home/draw/away).
        # Draw probability estimated from strength differential: closer = more draw-likely.
        draw_prob = 0.0
        if normalized_sport == "soccer":
            strength_diff = abs(float(home_prob) - float(away_prob))
            # draw ranges from ~35% (even) to ~8% (dominant favourite)
            draw_prob = max(0.08, 0.35 - strength_diff * 0.60)
            draw_prob = min(0.35, draw_prob)
            scale = 1.0 - draw_prob
            home_prob_3way = float(home_prob) * scale
            away_prob_3way = float(away_prob) * scale
        else:
            home_prob_3way = float(home_prob)
            away_prob_3way = float(away_prob)

        for market_type, market_name, compression in profile:
            # ── Soccer: compute market-specific probabilities ──
            if normalized_sport == "soccer":
                row = self._build_soccer_market_row(
                    game_id=game_id, league=league, home_team=home_team, away_team=away_team,
                    game_date=game_date, game_time=game_time,
                    home_prob=home_prob_3way, away_prob=away_prob_3way, draw_prob=draw_prob,
                    market_type=market_type, market_name=market_name, compression=compression,
                    model_version=model_version, model_type=model_type, model_auc=model_auc,
                    via_api=via_api,
                )
                if row:
                    rows.append(row)
                continue

            # ── Non-soccer: binary home/away ──
            adjusted_home = 0.5 + (float(home_prob) - 0.5) * float(compression)
            adjusted_home = max(0.01, min(0.99, adjusted_home))
            adjusted_away = max(0.01, min(0.99, 1.0 - adjusted_home))
            predicted_team = home_team if adjusted_home >= adjusted_away else away_team
            confidence = max(adjusted_home, adjusted_away)
            rows.append({
                "prediction_id": str(uuid4()),
                "game_id": game_id,
                "sport": normalized_sport,
                "league": league,
                "home_team": home_team,
                "away_team": away_team,
                "game_date": game_date,
                "game_time": game_time,
                "market_type": market_type,
                "market_name": market_name,
                "predicted_team": predicted_team,
                "home_win_prob": round(adjusted_home, 4),
                "away_win_prob": round(adjusted_away, 4),
                "confidence": round(confidence, 4),
                "confidence_tier": self._confidence_tier(confidence),
                "model_version": model_version,
                "model_type": model_type,
                "model_name": model_type,
                "model_auc": model_auc,
                "predicted_at": _now_utc(),
                "predict_mode": "api" if via_api else "artifact",
            })
        return rows

    def _build_soccer_market_row(
        self,
        *,
        game_id: str,
        league: str,
        home_team: str,
        away_team: str,
        game_date: str,
        game_time: str,
        home_prob: float,
        away_prob: float,
        draw_prob: float,
        market_type: str,
        market_name: str,
        compression: float,
        model_version: str,
        model_type: str,
        model_auc: float,
        via_api: bool,
    ) -> dict | None:
        """Build a single soccer prediction row with draw-aware logic."""
        c = float(compression)
        h = float(home_prob)
        a = float(away_prob)
        d = float(draw_prob)

        # --- Full-time 1X2 ---
        if market_type == "full_time_result":
            # Re-normalise to 3-way
            total = h + d + a
            hp = round(h / total, 4) if total else round(h, 4)
            dp = round(d / total, 4) if total else round(d, 4)
            ap = round(a / total, 4) if total else round(a, 4)
            if hp >= dp and hp >= ap:
                pick, conf = home_team, hp
            elif ap >= hp and ap >= dp:
                pick, conf = away_team, ap
            else:
                pick, conf = "Draw", dp
            return {
                "prediction_id": str(uuid4()), "game_id": game_id,
                "sport": "soccer", "league": league,
                "home_team": home_team, "away_team": away_team,
                "game_date": game_date, "game_time": game_time,
                "market_type": market_type, "market_name": market_name,
                "predicted_team": pick,
                "home_win_prob": hp, "away_win_prob": ap, "draw_prob": dp,
                "confidence": round(conf, 4),
                "confidence_tier": self._confidence_tier(conf),
                "model_version": model_version, "model_type": model_type,
                "model_name": model_type, "model_auc": model_auc,
                "predicted_at": _now_utc(),
                "predict_mode": "api" if via_api else "artifact",
            }

        # --- Moneyline (exclude draw — 2-way) ---
        if market_type == "moneyline":
            total_no_draw = h + a or 1.0
            hp2 = round(h / total_no_draw * c + (1 - c) * 0.5, 4)
            ap2 = round(1.0 - hp2, 4)
            pick = home_team if hp2 >= ap2 else away_team
            conf = max(hp2, ap2)
            return {
                "prediction_id": str(uuid4()), "game_id": game_id,
                "sport": "soccer", "league": league,
                "home_team": home_team, "away_team": away_team,
                "game_date": game_date, "game_time": game_time,
                "market_type": market_type, "market_name": market_name,
                "predicted_team": pick,
                "home_win_prob": hp2, "away_win_prob": ap2,
                "confidence": round(conf, 4),
                "confidence_tier": self._confidence_tier(conf),
                "model_version": model_version, "model_type": model_type,
                "model_name": model_type, "model_auc": model_auc,
                "predicted_at": _now_utc(),
                "predict_mode": "api" if via_api else "artifact",
            }

        # --- First / second half result (1X2 compressed) ---
        if market_type in ("first_half_result", "second_half_result"):
            half_draw = d * 1.3  # draws more common in halves
            half_h = h * c
            half_a = a * c
            t = half_h + half_draw + half_a or 1.0
            hp3 = round(half_h / t, 4)
            dp3 = round(half_draw / t, 4)
            ap3 = round(half_a / t, 4)
            if hp3 >= dp3 and hp3 >= ap3:
                pick, conf = home_team, hp3
            elif ap3 >= hp3 and ap3 >= dp3:
                pick, conf = away_team, ap3
            else:
                pick, conf = "Draw", dp3
            return {
                "prediction_id": str(uuid4()), "game_id": game_id,
                "sport": "soccer", "league": league,
                "home_team": home_team, "away_team": away_team,
                "game_date": game_date, "game_time": game_time,
                "market_type": market_type, "market_name": market_name,
                "predicted_team": pick,
                "home_win_prob": hp3, "away_win_prob": ap3, "draw_prob": dp3,
                "confidence": round(conf, 4),
                "confidence_tier": self._confidence_tier(conf),
                "model_version": model_version, "model_type": model_type,
                "model_name": model_type, "model_auc": model_auc,
                "predicted_at": _now_utc(),
                "predict_mode": "api" if via_api else "artifact",
            }

        # --- Binary totals / BTTS / corners / cards ---
        # These are independent of home/away; derive from match strength
        if market_type in (
            "total_goals_over_2_5", "total_goals_under_2_5",
            "total_goals_over_1_5", "both_teams_to_score",
            "over_9_5_corners", "over_3_5_cards",
        ):
            dominance = max(h, a)  # how one-sided the game is
            if market_type == "total_goals_over_2_5":
                # more goals in balanced games + strong favourites scoring freely
                p_yes = 0.52 + (1.0 - abs(h - a)) * 0.12
            elif market_type == "total_goals_under_2_5":
                p_yes = 1.0 - (0.52 + (1.0 - abs(h - a)) * 0.12)
            elif market_type == "total_goals_over_1_5":
                p_yes = 0.72 + (1.0 - abs(h - a)) * 0.10
            elif market_type == "both_teams_to_score":
                p_yes = 0.55 - (dominance - 0.5) * 0.25  # dominant team = lower BTTS
            elif market_type == "over_9_5_corners":
                p_yes = 0.50 + (1.0 - abs(h - a)) * 0.10
            else:  # over_3_5_cards
                p_yes = 0.48 + (1.0 - abs(h - a)) * 0.08
            p_yes = round(max(0.15, min(0.85, p_yes)), 4)
            p_no = round(1.0 - p_yes, 4)
            pred = "Yes" if p_yes >= p_no else "No"
            conf = max(p_yes, p_no)
            return {
                "prediction_id": str(uuid4()), "game_id": game_id,
                "sport": "soccer", "league": league,
                "home_team": home_team, "away_team": away_team,
                "game_date": game_date, "game_time": game_time,
                "market_type": market_type, "market_name": market_name,
                "predicted_team": pred,
                "home_win_prob": p_yes, "away_win_prob": p_no,
                "over_prob": p_yes, "under_prob": p_no,
                "confidence": round(conf, 4),
                "confidence_tier": self._confidence_tier(conf),
                "model_version": model_version, "model_type": model_type,
                "model_name": model_type, "model_auc": model_auc,
                "predicted_at": _now_utc(),
                "predict_mode": "api" if via_api else "artifact",
            }

        # --- Team goal totals (home / away team to score) ---
        if market_type in ("home_team_over_0_5_goals", "away_team_over_0_5_goals"):
            base_prob = h if "home" in market_type else a
            p_yes = round(max(0.30, min(0.90, 0.60 + (base_prob - 0.40) * 0.80)), 4)
            p_no = round(1.0 - p_yes, 4)
            pred = "Yes" if p_yes >= p_no else "No"
            conf = max(p_yes, p_no)
            return {
                "prediction_id": str(uuid4()), "game_id": game_id,
                "sport": "soccer", "league": league,
                "home_team": home_team, "away_team": away_team,
                "game_date": game_date, "game_time": game_time,
                "market_type": market_type, "market_name": market_name,
                "predicted_team": pred,
                "home_win_prob": p_yes, "away_win_prob": p_no,
                "over_prob": p_yes, "under_prob": p_no,
                "confidence": round(conf, 4),
                "confidence_tier": self._confidence_tier(conf),
                "model_version": model_version, "model_type": model_type,
                "model_name": model_type, "model_auc": model_auc,
                "predicted_at": _now_utc(),
                "predict_mode": "api" if via_api else "artifact",
            }

        # --- Double chance ---
        if market_type in ("double_chance_home_draw", "double_chance_away_draw"):
            p_yes = round(min(0.95, (h + d) if "home" in market_type else (a + d)), 4)
            p_no = round(1.0 - p_yes, 4)
            conf = max(p_yes, p_no)
            return {
                "prediction_id": str(uuid4()), "game_id": game_id,
                "sport": "soccer", "league": league,
                "home_team": home_team, "away_team": away_team,
                "game_date": game_date, "game_time": game_time,
                "market_type": market_type, "market_name": market_name,
                "predicted_team": "Yes" if p_yes >= p_no else "No",
                "home_win_prob": p_yes, "away_win_prob": p_no,
                "confidence": round(conf, 4),
                "confidence_tier": self._confidence_tier(conf),
                "model_version": model_version, "model_type": model_type,
                "model_name": model_type, "model_auc": model_auc,
                "predicted_at": _now_utc(),
                "predict_mode": "api" if via_api else "artifact",
            }

        # --- Clean sheet ---
        if market_type in ("home_clean_sheet", "away_clean_sheet"):
            defender_strength = h if "home" in market_type else a
            attacker_strength = a if "home" in market_type else h
            p_yes = round(max(0.10, min(0.70, 0.30 + (defender_strength - attacker_strength) * 0.60)), 4)
            p_no = round(1.0 - p_yes, 4)
            conf = max(p_yes, p_no)
            return {
                "prediction_id": str(uuid4()), "game_id": game_id,
                "sport": "soccer", "league": league,
                "home_team": home_team, "away_team": away_team,
                "game_date": game_date, "game_time": game_time,
                "market_type": market_type, "market_name": market_name,
                "predicted_team": "Yes" if p_yes >= p_no else "No",
                "home_win_prob": p_yes, "away_win_prob": p_no,
                "confidence": round(conf, 4),
                "confidence_tier": self._confidence_tier(conf),
                "model_version": model_version, "model_type": model_type,
                "model_name": model_type, "model_auc": model_auc,
                "predicted_at": _now_utc(),
                "predict_mode": "api" if via_api else "artifact",
            }

        # --- Asian handicap ---
        if market_type in ("asian_handicap_home", "asian_handicap_away"):
            fav_prob = h if "home" in market_type else a
            p_cover = round(max(0.20, min(0.80, fav_prob * 1.1)), 4)
            p_no = round(1.0 - p_cover, 4)
            conf = max(p_cover, p_no)
            team = home_team if "home" in market_type else away_team
            return {
                "prediction_id": str(uuid4()), "game_id": game_id,
                "sport": "soccer", "league": league,
                "home_team": home_team, "away_team": away_team,
                "game_date": game_date, "game_time": game_time,
                "market_type": market_type, "market_name": market_name,
                "predicted_team": team,
                "home_win_prob": p_cover, "away_win_prob": p_no,
                "confidence": round(conf, 4),
                "confidence_tier": self._confidence_tier(conf),
                "model_version": model_version, "model_type": model_type,
                "model_name": model_type, "model_auc": model_auc,
                "predicted_at": _now_utc(),
                "predict_mode": "api" if via_api else "artifact",
            }

        # --- First / second half winner (2-way, no draw) ---
        if market_type in ("first_half_winner", "second_half_winner"):
            adj_h = round(max(0.15, min(0.85, 0.5 + (h - a) * c)), 4)
            adj_a = round(1.0 - adj_h, 4)
            pick = home_team if adj_h >= adj_a else away_team
            conf = max(adj_h, adj_a)
            return {
                "prediction_id": str(uuid4()), "game_id": game_id,
                "sport": "soccer", "league": league,
                "home_team": home_team, "away_team": away_team,
                "game_date": game_date, "game_time": game_time,
                "market_type": market_type, "market_name": market_name,
                "predicted_team": pick,
                "home_win_prob": adj_h, "away_win_prob": adj_a,
                "confidence": round(conf, 4),
                "confidence_tier": self._confidence_tier(conf),
                "model_version": model_version, "model_type": model_type,
                "model_name": model_type, "model_auc": model_auc,
                "predicted_at": _now_utc(),
                "predict_mode": "api" if via_api else "artifact",
            }

        # --- Default fallback (treat as binary) ---
        adj_h = round(max(0.01, min(0.99, 0.5 + (h - a) * c)), 4)
        adj_a = round(1.0 - adj_h, 4)
        pick = home_team if adj_h >= adj_a else away_team
        conf = max(adj_h, adj_a)
        return {
            "prediction_id": str(uuid4()), "game_id": game_id,
            "sport": "soccer", "league": league,
            "home_team": home_team, "away_team": away_team,
            "game_date": game_date, "game_time": game_time,
            "market_type": market_type, "market_name": market_name,
            "predicted_team": pick,
            "home_win_prob": adj_h, "away_win_prob": adj_a,
            "confidence": round(conf, 4),
            "confidence_tier": self._confidence_tier(conf),
            "model_version": model_version, "model_type": model_type,
            "model_name": model_type, "model_auc": model_auc,
            "predicted_at": _now_utc(),
            "predict_mode": "api" if via_api else "artifact",
        }

    def run_daily_pipeline(
        self,
        custom_model: str = "auto",
        min_rows: int = 200,
        predictions_output_path: str | None = None,
        via_api: bool = False,
        model_id: str | None = None,
        endpoint_url: str | None = None,
    ) -> dict:
        """Orchestrate full daily pipeline: append results -> train -> predict."""
        logger.info("[hf_pipeline] Starting daily pipeline")
        self._write_status({"last_step": "daily_pipeline_started", "ok": True, "started_at": _now_utc()})
        yesterday = et_today() - datetime.timedelta(days=1)
        today = et_today()
        try:
            from data.db import archive_previous_day_data

            archived = archive_previous_day_data(today)
            logger.info("[hf_pipeline] Archived stale prior-day rows: %s", archived)
        except Exception as exc:
            logger.debug("[hf_pipeline] archive_previous_day_data skipped: %s", exc)
        self._cleanup_stale_snapshots(today)
        append_y = self.append_daily_results(yesterday)
        append_t = self.append_daily_results(today)
        news_signals = self.collect_news_signals(days=[today, today + datetime.timedelta(days=1)])
        try:
            summary = self.train_and_publish_best_model(min_rows=min_rows, forced_model=custom_model)
            train_result = {
                "ok": True, "repo_id": summary.repo_id, "rows": summary.rows,
                "best_model": summary.best_model, "cv_roc_auc": summary.cv_roc_auc,
                "trained_at": summary.trained_at, "version": summary.version,
                "sports_covered": summary.sports_covered,
            }
        except Exception as exc:
            logger.warning("[hf_pipeline] Training failed: %s", exc)
            train_result = {"ok": False, "error": str(exc)}
        preds = self.predict_daily_schedule(
            output_path=predictions_output_path,
            via_api=via_api, model_id=model_id, endpoint_url=endpoint_url,
        )
        self.ensure_model_card_metadata()
        result = {
            "ok": True,
            "append_yesterday": append_y, "append_today": append_t,
            "news_signals": news_signals,
            "train": train_result, "predictions": preds,
            "completed_at": _now_utc(),
        }
        self.publish_runtime_artifacts()
        self._write_status({"last_step": "daily_pipeline", "ok": True, "daily_completed_at": _now_utc()})
        logger.info("[hf_pipeline] Daily pipeline complete")
        return result

    def _load_global_model(self) -> tuple[object, str, bool]:
        import joblib
        from huggingface_hub import hf_hub_download

        cached = self._model_cache.get("global")
        if cached is not None:
            return cached
        model_path = hf_hub_download(
            repo_id=self.model_repo_id, filename="model.joblib",
            repo_type="model", token=self.token,
        )
        result = (joblib.load(model_path), "model.joblib", False)
        self._model_cache["global"] = result
        return result

    def _load_sport_model(self, sport: str, meta: dict) -> tuple[object, str, bool]:
        """Load (and cache in-process) the individual model for `sport` if the
        published metadata says one exists; otherwise fall back to the combined
        global model.joblib. Returns (model, model_file, is_individual).
        """
        import joblib
        from huggingface_hub import hf_hub_download

        sport_key = self._normalize_sport(str(sport or "mlb"))
        per_sport = meta.get("per_sport") or {}
        has_individual = isinstance(per_sport, dict) and sport_key in per_sport
        if not has_individual:
            return self._load_global_model()

        cache_key = f"sport:{sport_key}"
        cached = self._model_cache.get(cache_key)
        if cached is not None:
            return cached
        filename = f"model_{sport_key}.joblib"
        try:
            model_path = hf_hub_download(
                repo_id=self.model_repo_id, filename=filename,
                repo_type="model", token=self.token,
            )
            result = (joblib.load(model_path), filename, True)
        except Exception as exc:
            logger.warning("[hf_pipeline] Individual model %s unavailable (%s), falling back to global", filename, exc)
            return self._load_global_model()
        self._model_cache[cache_key] = result
        return result

    def predict_from_model_repo(
        self,
        home_team: str,
        away_team: str,
        sport: str = "mlb",
        season: int | None = None,
        team_stats: dict | None = None,
        h2h_stats: dict | None = None,
        news_stats: dict | None = None,
    ) -> dict:
        """Return win probabilities using the sport's own individual model when
        one has been published (model_{sport}.joblib), falling back to the
        combined global model.joblib for sports without enough training rows yet.

        `team_stats`/`h2h_stats`/`news_stats` are the pre-computed snapshot
        lookups built once per prediction run (`_build_form_snapshot()` /
        `_build_news_snapshot()`), not per matchup. When omitted, neutral
        defaults are used, so this stays usable for standalone single-matchup
        calls (e.g. the CLI).
        """
        import pandas as pd

        today = et_today()
        meta = self._get_model_metadata()
        model, model_file, is_individual = self._load_sport_model(sport, meta)
        form = self._form_features_for_matchup(home_team, away_team, sport, team_stats, h2h_stats)
        news = self._news_features_for_matchup(home_team, away_team, sport, news_stats)
        row = pd.DataFrame([{
            "home_team": home_team,
            "away_team": away_team,
            "sport": str(sport).lower(),
            "season": int(season or today.year),
            "month": today.month,
            "day_of_week": today.weekday(),
            **form,
            **news,
        }])
        probs = model.predict_proba(row)[0]
        home_prob = float(probs[1])
        sport_key = self._normalize_sport(str(sport or "mlb"))
        sport_meta = (meta.get("per_sport") or {}).get(sport_key) or {}
        return {
            "home_team": home_team, "away_team": away_team,
            "sport": sport, "season": int(season or today.year),
            "home_win_prob": round(home_prob, 4),
            "away_win_prob": round(1.0 - home_prob, 4),
            "model_repo": self.model_repo_id,
            "model_version": meta.get("version", ""),
            "model_file": model_file,
            "model_scope": "individual" if is_individual else "global",
            "model_type": sport_meta.get("best_model") if is_individual else meta.get("best_model"),
            "model_auc": sport_meta.get("cv_roc_auc") if is_individual else meta.get("cv_roc_auc"),
        }

    def predict_via_hf_api(
        self,
        home_team: str,
        away_team: str,
        season: int | None = None,
        model_id: str | None = None,
        endpoint_url: str | None = None,
    ) -> dict:
        """Call HF Inference API or custom endpoint."""
        url = str(endpoint_url or "").strip()
        if not url:
            url = f"https://api-inference.huggingface.co/models/{str(model_id or self.model_repo_id).strip()}"
        headers = {"Content-Type": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        payload = {"inputs": {"home_team": home_team, "away_team": away_team,
                               "season": int(season or datetime.date.today().year)}}
        resp = requests.post(url, headers=headers, json=payload, timeout=60)
        resp.raise_for_status()
        return {"url": url, "response": resp.json()}

    # ──────────────────────────────────────────────────────────
    # Private sport-specific completed game fetchers
    # ──────────────────────────────────────────────────────────

    def _fetch_completed_games(self, start: datetime.date, end: datetime.date) -> list[dict]:
        rows: list[dict] = []
        rows += self._fetch_mlb_games(start, end)
        rows += self._fetch_nba_games(start, end)
        rows += self._fetch_nhl_games(start, end)
        rows += self._fetch_soccer_games(start, end)
        rows += self._fetch_cricket_games(start, end)
        rows += self._fetch_tennis_games_jeff_sackmann(start, end)
        if (end - start).days <= 7:
            rows += self._fetch_additional_sportsdb_completed(start, end)
        return rows

    def _fetch_mlb_games(self, start: datetime.date, end: datetime.date) -> list[dict]:
        rows: list[dict] = []
        current = start
        while current <= end:
            day = current.isoformat()
            try:
                resp = requests.get(
                    "https://statsapi.mlb.com/api/v1/schedule",
                    params={"sportId": 1, "date": day, "hydrate": "linescore", "gameType": "R"},
                    timeout=20,
                )
                resp.raise_for_status()
                payload = resp.json() or {}
                for de in payload.get("dates", []):
                    for game in de.get("games", []):
                        status = str(
                            (game.get("status") or {}).get("detailedState")
                            or (game.get("status") or {}).get("abstractGameState") or ""
                        )
                        if status not in self.FINAL_STATES:
                            continue
                        teams = game.get("teams") or {}
                        home = teams.get("home") or {}
                        away = teams.get("away") or {}
                        home_team = str(((home.get("team") or {}).get("name")) or "").strip()
                        away_team = str(((away.get("team") or {}).get("name")) or "").strip()
                        home_score = home.get("score")
                        away_score = away.get("score")
                        if not home_team or not away_team or home_score is None or away_score is None:
                            continue
                        rows.append(self._make_game_record(
                            game_id=str(game.get("gamePk") or ""),
                            sport="mlb", league="MLB", game_date=day,
                            game_datetime=str(game.get("gameDate") or ""),
                            status=status, home_team=home_team, away_team=away_team,
                            home_score=float(home_score), away_score=float(away_score),
                            season=int(day[:4]),
                        ))
            except Exception as exc:
                logger.debug("[hf_pipeline] MLB %s: %s", day, exc)
            current += datetime.timedelta(days=1)
        return rows

    def _fetch_nba_games(self, start: datetime.date, end: datetime.date) -> list[dict]:
        rows: list[dict] = []
        if not self.balldontlie_api_key:
            logger.debug("[hf_pipeline] NBA fetch skipped: BALLDONTLIE_API_KEY not set")
            return rows
        current = start
        while current <= end:
            day = current.isoformat()
            try:
                resp = requests.get(
                    f"{self.balldontlie_base_url}/games",
                    params={"start_date": day, "end_date": day, "per_page": 100},
                    headers={"Authorization": self.balldontlie_api_key},
                    timeout=20,
                )
                resp.raise_for_status()
                for g in (resp.json() or {}).get("data", []):
                    if str(g.get("status") or "").strip() != "Final":
                        continue
                    home_team = str((g.get("home_team") or {}).get("full_name") or "").strip()
                    away_team = str((g.get("visitor_team") or {}).get("full_name") or "").strip()
                    hs = g.get("home_team_score")
                    as_ = g.get("visitor_team_score")
                    if not home_team or not away_team or hs is None or as_ is None:
                        continue
                    gd = str(g.get("date") or day)[:10]
                    rows.append(self._make_game_record(
                        game_id=str(g.get("id") or ""), sport="nba", league="NBA",
                        game_date=gd, game_datetime=str(g.get("date") or ""),
                        status="Final", home_team=home_team, away_team=away_team,
                        home_score=float(hs), away_score=float(as_), season=int(gd[:4]),
                    ))
                time.sleep(0.25)
            except Exception as exc:
                logger.debug("[hf_pipeline] NBA %s: %s", day, exc)
            current += datetime.timedelta(days=1)
        return rows

    def _fetch_nhl_games(self, start: datetime.date, end: datetime.date) -> list[dict]:
        rows: list[dict] = []
        current = start
        while current <= end:
            day = current.isoformat()
            try:
                resp = requests.get(f"https://api-web.nhle.com/v1/schedule/{day}", timeout=20)
                resp.raise_for_status()
                for week in (resp.json() or {}).get("gameWeek", []):
                    for game in week.get("games", []):
                        state = str(game.get("gameState") or "")
                        if state not in ("OFF", "FINAL", "OVER"):
                            continue
                        hd = game.get("homeTeam") or {}
                        ad = game.get("awayTeam") or {}
                        ht = str((hd.get("commonName") or {}).get("default") or hd.get("abbrev") or "").strip()
                        at = str((ad.get("commonName") or {}).get("default") or ad.get("abbrev") or "").strip()
                        hs = hd.get("score")
                        as_ = ad.get("score")
                        if not ht or not at or hs is None or as_ is None:
                            continue
                        gd = str(game.get("gameDate") or day)[:10]
                        rows.append(self._make_game_record(
                            game_id=str(game.get("id") or ""), sport="nhl", league="NHL",
                            game_date=gd, game_datetime=str(game.get("startTimeUTC") or ""),
                            status="Final", home_team=ht, away_team=at,
                            home_score=float(hs), away_score=float(as_), season=int(gd[:4]),
                        ))
            except Exception as exc:
                logger.debug("[hf_pipeline] NHL %s: %s", day, exc)
            current += datetime.timedelta(days=1)
        return rows

    def _fetch_soccer_games(self, start: datetime.date, end: datetime.date) -> list[dict]:
        """Aggregate completed soccer games from all free sources."""
        rows: list[dict] = []
        rows += self._fetch_soccer_games_football_data(start, end)
        rows += self._fetch_soccer_games_thesportsdb(start, end)
        rows += self._fetch_soccer_games_espn(start, end)
        # dedupe by (home, away, date)
        seen: set[tuple] = set()
        deduped: list[dict] = []
        for r in rows:
            k = (
                str(r.get("home_team") or "").lower().strip(),
                str(r.get("away_team") or "").lower().strip(),
                str(r.get("game_date") or ""),
            )
            if k[0] and k[1] and k not in seen:
                seen.add(k)
                deduped.append(r)
        return deduped

    def _fetch_soccer_games_espn(self, start: datetime.date, end: datetime.date) -> list[dict]:
        """Fetch completed soccer results from ESPN public API (no key, no auth)."""
        rows: list[dict] = []
        leagues = self._SOCCER_ESPN_LEAGUES
        current = start
        while current <= end:
            date_str = current.strftime("%Y%m%d")
            for espn_slug, league_name in leagues:
                try:
                    r = requests.get(
                        f"https://site.api.espn.com/apis/site/v2/sports/soccer/{espn_slug}/scoreboard",
                        params={"dates": date_str},
                        timeout=15,
                        headers={"User-Agent": "Mozilla/5.0"},
                    )
                    if r.status_code != 200:
                        continue
                    for event in (r.json() or {}).get("events") or []:
                        for comp in (event.get("competitions") or []):
                            status_name = str(((event.get("status") or {}).get("type") or {}).get("name") or "")
                            if status_name not in ("STATUS_FINAL", "STATUS_FULL_TIME", "STATUS_END_PERIOD"):
                                continue
                            competitors = comp.get("competitors") or []
                            home = next((c for c in competitors if c.get("homeAway") == "home"), None)
                            away = next((c for c in competitors if c.get("homeAway") == "away"), None)
                            if not home or not away:
                                if len(competitors) == 2:
                                    home, away = competitors[0], competitors[1]
                                else:
                                    continue
                            ht = str((home.get("team") or {}).get("displayName") or "").strip()
                            at = str((away.get("team") or {}).get("displayName") or "").strip()
                            hs = home.get("score")
                            as_ = away.get("score")
                            if not ht or not at or hs is None or as_ is None:
                                continue
                            try:
                                hs_f = float(hs)
                                as_f = float(as_)
                            except Exception:
                                continue
                            gdt = str(event.get("date") or comp.get("date") or current.isoformat())
                            rows.append(self._make_game_record(
                                game_id=f"espn_{event.get('id', '')}",
                                sport="soccer",
                                league=league_name,
                                game_date=gdt[:10],
                                game_datetime=gdt,
                                status="Final",
                                home_team=ht,
                                away_team=at,
                                home_score=hs_f,
                                away_score=as_f,
                                season=current.year,
                            ))
                    time.sleep(0.15)
                except Exception as exc:
                    logger.debug("[hf_pipeline] ESPN soccer %s %s: %s", espn_slug, current, exc)
            current += datetime.timedelta(days=1)
        return rows

    def _fetch_soccer_upcoming_espn(self, day: datetime.date) -> list[dict]:
        """Upcoming-schedule counterpart to _fetch_soccer_games_espn -- same
        league breadth (including women's competitions) so today/tomorrow
        predictions can actually be generated for every league we collect
        historical results for, not just the football-data.org subset.
        """
        rows: list[dict] = []
        date_str = day.strftime("%Y%m%d")
        for espn_slug, league_name in self._SOCCER_ESPN_LEAGUES:
            try:
                r = requests.get(
                    f"https://site.api.espn.com/apis/site/v2/sports/soccer/{espn_slug}/scoreboard",
                    params={"dates": date_str},
                    timeout=15,
                    headers={"User-Agent": "Mozilla/5.0"},
                )
                if r.status_code != 200:
                    continue
                for event in (r.json() or {}).get("events") or []:
                    for comp in (event.get("competitions") or []):
                        status_name = str(((event.get("status") or {}).get("type") or {}).get("name") or "")
                        if status_name in ("STATUS_FINAL", "STATUS_FULL_TIME", "STATUS_END_PERIOD"):
                            continue
                        competitors = comp.get("competitors") or []
                        home = next((c for c in competitors if c.get("homeAway") == "home"), None)
                        away = next((c for c in competitors if c.get("homeAway") == "away"), None)
                        if not home or not away:
                            if len(competitors) == 2:
                                home, away = competitors[0], competitors[1]
                            else:
                                continue
                        ht = str((home.get("team") or {}).get("displayName") or "").strip()
                        at = str((away.get("team") or {}).get("displayName") or "").strip()
                        if not ht or not at:
                            continue
                        gdt = str(event.get("date") or comp.get("date") or day.isoformat())
                        rows.append({
                            "sport": "soccer",
                            "league": league_name,
                            "home_team": ht,
                            "away_team": at,
                            "game_date": gdt[:10],
                            "game_time": gdt,
                            "game_id": f"espn_{event.get('id', '')}",
                        })
                time.sleep(0.15)
            except Exception as exc:
                logger.debug("[hf_pipeline] ESPN soccer upcoming %s %s: %s", espn_slug, day, exc)
        return rows

    def _fetch_soccer_games_football_data(self, start: datetime.date, end: datetime.date) -> list[dict]:
        rows: list[dict] = []
        if not self.football_data_api_key:
            return rows
        headers = {"X-Auth-Token": self.football_data_api_key}
        # Expanded competition codes for maximum soccer coverage
        competitions = (
            # Europe top 5 leagues
            "PL", "PD", "SA", "BL1", "FL1", "PPL",
            # Europe second tier & cups
            "ELC", "DED", "BSA", "CL", "EL", "EC",
            # Americas
            "MLS", "CLI", "BSA",
            # Other major regions when available
            "WC", "ASC", "AFR", "OC",
        )
        for comp in competitions:
            try:
                resp = requests.get(
                    f"https://api.football-data.org/v4/competitions/{comp}/matches",
                    params={"dateFrom": start.isoformat(), "dateTo": end.isoformat(), "status": "FINISHED"},
                    headers=headers,
                    timeout=25,
                )
                resp.raise_for_status()
                for m in (resp.json() or {}).get("matches", []):
                    score = m.get("score") or {}
                    full = score.get("fullTime") or {}
                    hs = full.get("home")
                    as_ = full.get("away")
                    ht = str(((m.get("homeTeam") or {}).get("name")) or "").strip()
                    at = str(((m.get("awayTeam") or {}).get("name")) or "").strip()
                    gd = str(m.get("utcDate") or "")[:10]
                    if hs is None or as_ is None or not ht or not at or not gd:
                        continue
                    # Extract additional match details for enrichment
                    match_stats = {}
                    if m.get("odds"):
                        odds = m.get("odds") or {}
                        if odds.get("homeWin"):
                            match_stats["odds_home_win"] = float(odds.get("homeWin") or 0)
                        if odds.get("awayWin"):
                            match_stats["odds_away_win"] = float(odds.get("awayWin") or 0)
                        if odds.get("draw"):
                            match_stats["odds_draw"] = float(odds.get("draw") or 0)
                    rows.append(
                        self._make_game_record(
                            game_id=str(m.get("id") or ""),
                            sport="soccer",
                            league=str((m.get("competition") or {}).get("name") or comp),
                            game_date=gd,
                            game_datetime=str(m.get("utcDate") or ""),
                            status="Final",
                            home_team=ht,
                            away_team=at,
                            home_score=float(hs),
                            away_score=float(as_),
                            season=int(gd[:4]),
                            extra_data=match_stats if match_stats else None,
                        )
                    )
                time.sleep(0.2)
            except Exception as exc:
                logger.debug("[hf_pipeline] football-data %s: %s", comp, exc)
        return rows

    def _fetch_soccer_games_thesportsdb(self, start: datetime.date, end: datetime.date) -> list[dict]:
        rows: list[dict] = []
        current = start
        while current <= end:
            day = current.isoformat()
            try:
                resp = requests.get(
                    f"https://www.thesportsdb.com/api/v1/json/{self.thesportsdb_api_key}/eventsday.php",
                    params={"d": day, "s": "Soccer"}, timeout=20,
                )
                resp.raise_for_status()
                for ev in ((resp.json() or {}).get("events") or []):
                    hs = ev.get("intHomeScore")
                    as_ = ev.get("intAwayScore")
                    if hs is None or as_ is None:
                        continue
                    ht = str(ev.get("strHomeTeam") or "").strip()
                    at = str(ev.get("strAwayTeam") or "").strip()
                    if not ht or not at:
                        continue
                    rows.append(self._make_game_record(
                        game_id=str(ev.get("idEvent") or ""), sport="soccer",
                        league=str(ev.get("strLeague") or "Soccer"),
                        game_date=day, game_datetime=day, status="Final",
                        home_team=ht, away_team=at,
                        home_score=float(hs), away_score=float(as_), season=current.year,
                    ))
            except Exception as exc:
                logger.debug("[hf_pipeline] Soccer %s: %s", day, exc)
            current += datetime.timedelta(days=1)
        return rows

    def _fetch_cricket_games(self, start: datetime.date, end: datetime.date) -> list[dict]:
        rows: list[dict] = []
        rows += self._fetch_cricket_games_cricsheet(start, end)
        rows += self._fetch_cricket_games_kaggle(start, end)
        rows += self._fetch_cricket_games_cricapi(start, end)
        rows += self._fetch_cricket_games_rapidapi(start, end)
        rows += self._fetch_cricket_games_thesportsdb(start, end)
        return rows

    def _fetch_cricket_games_thesportsdb(self, start: datetime.date, end: datetime.date) -> list[dict]:
        rows: list[dict] = []
        current = start
        while current <= end:
            day = current.isoformat()
            try:
                resp = requests.get(
                    f"https://www.thesportsdb.com/api/v1/json/{self.thesportsdb_api_key}/eventsday.php",
                    params={"d": day, "s": "Cricket"},
                    timeout=20,
                )
                resp.raise_for_status()
                for ev in ((resp.json() or {}).get("events") or []):
                    hs = ev.get("intHomeScore")
                    as_ = ev.get("intAwayScore")
                    if hs is None or as_ is None:
                        continue
                    ht = str(ev.get("strHomeTeam") or "").strip()
                    at = str(ev.get("strAwayTeam") or "").strip()
                    if not ht or not at:
                        continue
                    rows.append(
                        self._make_game_record(
                            game_id=str(ev.get("idEvent") or ""),
                            sport="cricket",
                            league=str(ev.get("strLeague") or "Cricket"),
                            game_date=day,
                            game_datetime=str(ev.get("strTimestamp") or day),
                            status="Final",
                            home_team=ht,
                            away_team=at,
                            home_score=float(hs),
                            away_score=float(as_),
                            season=current.year,
                        )
                    )
            except Exception as exc:
                logger.debug("[hf_pipeline] Cricket SportsDB %s: %s", day, exc)
            current += datetime.timedelta(days=1)
        return rows

    def _fetch_cricket_games_cricsheet(self, start: datetime.date, end: datetime.date) -> list[dict]:
        rows: list[dict] = []
        base_dir = str(self.cricsheet_dir or "").strip()
        if not base_dir:
            return rows
        path = os.path.abspath(base_dir)
        if not os.path.exists(path):
            return rows

        def _append_payload(payload: dict, source_name: str) -> None:
            info = payload.get("info") if isinstance(payload.get("info"), dict) else {}
            dates = info.get("dates") if isinstance(info.get("dates"), list) else []
            game_date = self._normalize_game_date(str(dates[0] if dates else ""), "")
            if not game_date:
                game_date = self._normalize_game_date(str(info.get("date") or ""), "")
            if not game_date:
                return
            try:
                game_dt = datetime.date.fromisoformat(game_date)
            except Exception:
                return
            if game_dt < start or game_dt > end:
                return
            teams = info.get("teams") if isinstance(info.get("teams"), list) else []
            if len(teams) < 2:
                return
            home_team = str(teams[0] or "").strip()
            away_team = str(teams[1] or "").strip()
            if not home_team or not away_team:
                return
            outcome = info.get("outcome") if isinstance(info.get("outcome"), dict) else {}
            winner = str(outcome.get("winner") or "").strip().lower()
            hs = 1.0 if winner and winner == home_team.lower() else (0.0 if winner and winner == away_team.lower() else 0.5)
            as_ = 1.0 - hs if hs in {0.0, 1.0} else 0.5
            league = str(((info.get("event") or {}).get("name")) or info.get("match_type") or "Cricket").strip()
            metadata = {
                "source": "cricsheet",
                "winner": str(outcome.get("winner") or ""),
                "match_type": str(info.get("match_type") or ""),
                "venue": str(info.get("venue") or ""),
            }
            rows.append(
                self._make_game_record(
                    game_id=f"cricsheet_{source_name}",
                    sport="cricket",
                    league=league[:80] or "Cricket",
                    game_date=game_date,
                    game_datetime=f"{game_date}T00:00:00Z",
                    status="Final",
                    home_team=home_team,
                    away_team=away_team,
                    home_score=hs,
                    away_score=as_,
                    season=int(game_date[:4]),
                    metadata=json.dumps(metadata, ensure_ascii=True),
                )
            )

        if os.path.isdir(path):
            json_files = [os.path.join(path, f) for f in os.listdir(path) if f.lower().endswith(".json")]
            zip_files = [os.path.join(path, f) for f in os.listdir(path) if f.lower().endswith(".zip")]
        else:
            json_files = [path] if path.lower().endswith(".json") else []
            zip_files = [path] if path.lower().endswith(".zip") else []

        for json_path in json_files:
            try:
                with open(json_path, "r", encoding="utf-8") as handle:
                    payload = json.load(handle)
                if isinstance(payload, dict):
                    _append_payload(payload, os.path.basename(json_path))
            except Exception:
                continue

        for zip_path in zip_files:
            try:
                with zipfile.ZipFile(zip_path, "r") as archive:
                    for member in archive.namelist():
                        if not member.lower().endswith(".json"):
                            continue
                        try:
                            payload = json.loads(archive.read(member).decode("utf-8"))
                        except Exception:
                            continue
                        if isinstance(payload, dict):
                            _append_payload(payload, member)
            except Exception:
                continue
        return rows

    def _fetch_cricket_games_kaggle(self, start: datetime.date, end: datetime.date) -> list[dict]:
        rows: list[dict] = []
        base_dir = str(self.cricket_kaggle_dir or "").strip()
        if not base_dir or not os.path.isdir(base_dir):
            return rows
        for root, _, files in os.walk(base_dir):
            for file_name in files:
                if not file_name.lower().endswith(".csv"):
                    continue
                csv_path = os.path.join(root, file_name)
                try:
                    with open(csv_path, "r", encoding="utf-8", newline="") as handle:
                        reader = csv.DictReader(handle)
                        field_map = {str(f or "").strip().lower(): f for f in (reader.fieldnames or [])}
                        date_key = field_map.get("date") or field_map.get("match_date")
                        home_key = field_map.get("team1") or field_map.get("home_team")
                        away_key = field_map.get("team2") or field_map.get("away_team")
                        winner_key = field_map.get("winner")
                        league_key = field_map.get("tournament") or field_map.get("series") or field_map.get("league")
                        if not date_key or not home_key or not away_key:
                            continue
                        for row in reader:
                            gd = self._normalize_game_date(str(row.get(date_key) or ""), "")
                            if not gd:
                                continue
                            try:
                                game_dt = datetime.date.fromisoformat(gd)
                            except Exception:
                                continue
                            if game_dt < start or game_dt > end:
                                continue
                            ht = str(row.get(home_key) or "").strip()
                            at = str(row.get(away_key) or "").strip()
                            if not ht or not at:
                                continue
                            winner = str(row.get(winner_key) or "").strip().lower() if winner_key else ""
                            hs = 1.0 if winner and winner == ht.lower() else (0.0 if winner and winner == at.lower() else 0.5)
                            as_ = 1.0 - hs if hs in {0.0, 1.0} else 0.5
                            league = str(row.get(league_key) or "Cricket").strip() if league_key else "Cricket"
                            rows.append(
                                self._make_game_record(
                                    game_id=f"kaggle_{file_name}_{gd}_{ht}_{at}",
                                    sport="cricket",
                                    league=league[:80] or "Cricket",
                                    game_date=gd,
                                    game_datetime=f"{gd}T00:00:00Z",
                                    status="Final",
                                    home_team=ht,
                                    away_team=at,
                                    home_score=hs,
                                    away_score=as_,
                                    season=int(gd[:4]),
                                    metadata=json.dumps({"source": "kaggle", "winner": winner}, ensure_ascii=True),
                                )
                            )
                except Exception as exc:
                    logger.debug("[hf_pipeline] Cricket Kaggle %s: %s", csv_path, exc)
        return rows

    def _fetch_cricket_games_cricapi(self, start: datetime.date, end: datetime.date) -> list[dict]:
        rows: list[dict] = []
        if not self.cricapi_key:
            return rows
        current = start
        while current <= end:
            day = current.isoformat()
            try:
                resp = requests.get(
                    f"{self.cricapi_base_url}/matches",
                    params={"apikey": self.cricapi_key, "offset": 0, "date": day},
                    timeout=20,
                )
                if resp.status_code != 200:
                    current += datetime.timedelta(days=1)
                    continue
                for m in (resp.json() or {}).get("data") or []:
                    status = str(m.get("status") or "").lower()
                    if not any(token in status for token in ("result", "won", "draw", "tie", "completed", "final")):
                        continue
                    ht = str(m.get("teamInfo", [{}])[0].get("name") if isinstance(m.get("teamInfo"), list) and len(m.get("teamInfo")) > 0 else m.get("t1") or "").strip()
                    at = str(m.get("teamInfo", [{}, {}])[1].get("name") if isinstance(m.get("teamInfo"), list) and len(m.get("teamInfo")) > 1 else m.get("t2") or "").strip()
                    if not ht or not at:
                        continue
                    winner = str(m.get("matchWinner") or "").strip().lower()
                    hs = 1.0 if winner and winner == ht.lower() else (0.0 if winner and winner == at.lower() else 0.5)
                    as_ = 1.0 - hs if hs in {0.0, 1.0} else 0.5
                    game_time = str(m.get("dateTimeGMT") or f"{day}T00:00:00Z")
                    rows.append(
                        self._make_game_record(
                            game_id=str(m.get("id") or f"cricapi_{day}_{ht}_{at}"),
                            sport="cricket",
                            league=str(m.get("series") or m.get("matchType") or "Cricket"),
                            game_date=day,
                            game_datetime=game_time,
                            status="Final",
                            home_team=ht,
                            away_team=at,
                            home_score=hs,
                            away_score=as_,
                            season=int(day[:4]),
                            metadata=json.dumps({"source": "cricapi", "status": m.get("status"), "winner": m.get("matchWinner")}, ensure_ascii=True),
                        )
                    )
            except Exception as exc:
                logger.debug("[hf_pipeline] CricAPI completed %s: %s", day, exc)
            current += datetime.timedelta(days=1)
        return rows

    def _fetch_cricket_games_rapidapi(self, start: datetime.date, end: datetime.date) -> list[dict]:
        rows: list[dict] = []
        if not self.cricket_rapidapi_key:
            return rows
        headers = {
            "X-RapidAPI-Key": self.cricket_rapidapi_key,
            "X-RapidAPI-Host": self.cricket_rapidapi_host,
        }
        current = start
        while current <= end:
            day = current.isoformat()
            try:
                resp = requests.get(
                    f"{self.cricket_rapidapi_base_url}/fixtures-by-date/{day}",
                    headers=headers,
                    timeout=20,
                )
                if resp.status_code != 200:
                    current += datetime.timedelta(days=1)
                    continue
                payload = resp.json() or {}
                fixture_rows = payload.get("results") if isinstance(payload.get("results"), list) else payload.get("data")
                if not isinstance(fixture_rows, list):
                    current += datetime.timedelta(days=1)
                    continue
                for m in fixture_rows:
                    if not isinstance(m, dict):
                        continue
                    status = str(m.get("status") or m.get("match_status") or "").lower()
                    if not any(token in status for token in ("result", "won", "draw", "tie", "complete", "final")):
                        continue
                    ht = str(m.get("home") or m.get("home_team") or m.get("team1") or "").strip()
                    at = str(m.get("away") or m.get("away_team") or m.get("team2") or "").strip()
                    if not ht or not at:
                        continue
                    winner = str(m.get("winner") or "").strip().lower()
                    hs = 1.0 if winner and winner == ht.lower() else (0.0 if winner and winner == at.lower() else 0.5)
                    as_ = 1.0 - hs if hs in {0.0, 1.0} else 0.5
                    rows.append(
                        self._make_game_record(
                            game_id=str(m.get("id") or f"rapidapi_{day}_{ht}_{at}"),
                            sport="cricket",
                            league=str(m.get("series_name") or m.get("league") or "Cricket"),
                            game_date=day,
                            game_datetime=str(m.get("date") or m.get("start_time") or f"{day}T00:00:00Z"),
                            status="Final",
                            home_team=ht,
                            away_team=at,
                            home_score=hs,
                            away_score=as_,
                            season=int(day[:4]),
                            metadata=json.dumps({"source": "rapidapi", "winner": m.get("winner"), "status": m.get("status")}, ensure_ascii=True),
                        )
                    )
            except Exception as exc:
                logger.debug("[hf_pipeline] RapidAPI cricket completed %s: %s", day, exc)
            current += datetime.timedelta(days=1)
        return rows

    # ──────────────────────────────────────────────────────────
    # Private sport-specific upcoming fetchers
    # ──────────────────────────────────────────────────────────

    def _fetch_upcoming_games(self, day: datetime.date) -> list[dict]:
        rows: list[dict] = []
        rows += self._fetch_mlb_upcoming(day)
        rows += self._fetch_nba_upcoming(day)
        rows += self._fetch_nhl_upcoming(day)
        rows += self._fetch_soccer_upcoming(day)
        rows += self._fetch_cricket_upcoming(day)
        rows += self._fetch_additional_sportsdb_upcoming(day)
        rows += self._fetch_kalshi_upcoming(day)

        deduped = []
        seen = set()
        for row in rows:
            key = (
                str(row.get("sport") or "").lower(),
                str(row.get("game_date") or ""),
                str(row.get("away_team") or "").lower(),
                str(row.get("home_team") or "").lower(),
            )
            if key in seen:
                continue
            seen.add(key)
            deduped.append(row)
        return deduped

    def _disambiguate_sportsdb_sport(self, dst_sport: str, league_name: str) -> str:
        """TheSportsDB's broad category query (e.g. s=Basketball) mixes multiple
        leagues together. Route by the event's actual league name so WNBA games
        don't get silently folded into the NBA bucket."""
        if dst_sport != "nba":
            return dst_sport
        league_l = str(league_name or "").strip().lower()
        if "wnba" in league_l or "women" in league_l:
            return "wnba"
        return dst_sport

    def _fetch_additional_sportsdb_completed(self, start: datetime.date, end: datetime.date) -> list[dict]:
        rows: list[dict] = []
        sport_map = {
            "Basketball": "nba",
            "Ice Hockey": "nhl",
            "American Football": "nfl",
            "Tennis": "tennis",
            "Boxing": "boxing",
            "Mixed Martial Arts": "mma",
            "Cricket": "cricket",
        }
        current = start
        while current <= end:
            day = current.isoformat()
            for src_sport, dst_sport in sport_map.items():
                try:
                    resp = requests.get(
                        f"https://www.thesportsdb.com/api/v1/json/{self.thesportsdb_api_key}/eventsday.php",
                        params={"d": day, "s": src_sport},
                        timeout=20,
                    )
                    resp.raise_for_status()
                    for ev in ((resp.json() or {}).get("events") or []):
                        hs = ev.get("intHomeScore")
                        as_ = ev.get("intAwayScore")
                        if hs is None or as_ is None:
                            continue
                        ht = str(ev.get("strHomeTeam") or "").strip()
                        at = str(ev.get("strAwayTeam") or "").strip()
                        if not ht or not at:
                            continue
                        game_iso = str(ev.get("strTimestamp") or f"{day}T00:00:00Z")
                        league_name = str(ev.get("strLeague") or src_sport)
                        rows.append(
                            self._make_game_record(
                                game_id=str(ev.get("idEvent") or ""),
                                sport=self._disambiguate_sportsdb_sport(dst_sport, league_name),
                                league=league_name,
                                game_date=day,
                                game_datetime=game_iso,
                                status="Final",
                                home_team=ht,
                                away_team=at,
                                home_score=float(hs),
                                away_score=float(as_),
                                season=int(day[:4]),
                            )
                        )
                except Exception as exc:
                    logger.debug("[hf_pipeline] SportsDB %s %s: %s", src_sport, day, exc)
            current += datetime.timedelta(days=1)
        return rows

    def _fetch_additional_sportsdb_upcoming(self, day: datetime.date) -> list[dict]:
        rows: list[dict] = []
        sport_map = {
            "Basketball": "nba",
            "Ice Hockey": "nhl",
            "American Football": "nfl",
            "Soccer": "soccer",
            "Tennis": "tennis",
            "Golf": "golf",
            "Boxing": "boxing",
            "Mixed Martial Arts": "mma",
            "Cricket": "cricket",
        }
        for src_sport, dst_sport in sport_map.items():
            try:
                resp = requests.get(
                    f"https://www.thesportsdb.com/api/v1/json/{self.thesportsdb_api_key}/eventsday.php",
                    params={"d": day.isoformat(), "s": src_sport},
                    timeout=20,
                )
                resp.raise_for_status()
                for ev in ((resp.json() or {}).get("events") or []):
                    ht = str(ev.get("strHomeTeam") or "").strip()
                    at = str(ev.get("strAwayTeam") or "").strip()
                    if not ht or not at:
                        continue
                    league_name = str(ev.get("strLeague") or src_sport)
                    rows.append(
                        {
                            "sport": self._disambiguate_sportsdb_sport(dst_sport, league_name),
                            "league": league_name,
                            "home_team": ht,
                            "away_team": at,
                            "game_date": day.isoformat(),
                            "game_time": str(ev.get("strTimestamp") or ""),
                            "game_id": str(ev.get("idEvent") or ""),
                        }
                    )
            except Exception as exc:
                logger.debug("[hf_pipeline] SportsDB upcoming %s %s: %s", src_sport, day, exc)
        return rows

    def _fetch_cricket_upcoming(self, day: datetime.date) -> list[dict]:
        rows: list[dict] = []
        rows += self._fetch_cricket_upcoming_cricapi(day)
        rows += self._fetch_cricket_upcoming_rapidapi(day)
        rows += self._fetch_cricket_games_thesportsdb(day, day)
        # dedupe by home/away/date
        deduped: list[dict] = []
        seen: set[tuple[str, str, str]] = set()
        for row in rows:
            key = (
                str(row.get("home_team") or "").strip().lower(),
                str(row.get("away_team") or "").strip().lower(),
                str(row.get("game_date") or "").strip(),
            )
            if not key[0] or not key[1] or key in seen:
                continue
            seen.add(key)
            deduped.append(row)
        return deduped

    def _fetch_cricket_upcoming_cricapi(self, day: datetime.date) -> list[dict]:
        rows: list[dict] = []
        if not self.cricapi_key:
            return rows
        try:
            resp = requests.get(
                f"{self.cricapi_base_url}/matches",
                params={"apikey": self.cricapi_key, "offset": 0, "date": day.isoformat()},
                timeout=20,
            )
            if resp.status_code != 200:
                return rows
            for m in (resp.json() or {}).get("data") or []:
                if not isinstance(m, dict):
                    continue
                status = str(m.get("status") or "").lower()
                if any(token in status for token in ("result", "won", "draw", "tie", "complete", "final")):
                    continue
                team_info = m.get("teamInfo") if isinstance(m.get("teamInfo"), list) else []
                ht = str((team_info[0].get("name") if len(team_info) > 0 and isinstance(team_info[0], dict) else m.get("t1")) or "").strip()
                at = str((team_info[1].get("name") if len(team_info) > 1 and isinstance(team_info[1], dict) else m.get("t2")) or "").strip()
                if not ht or not at:
                    continue
                rows.append(
                    {
                        "sport": "cricket",
                        "league": str(m.get("series") or m.get("matchType") or "Cricket"),
                        "home_team": ht,
                        "away_team": at,
                        "game_date": day.isoformat(),
                        "game_time": str(m.get("dateTimeGMT") or f"{day.isoformat()}T00:00:00Z"),
                        "game_id": str(m.get("id") or f"cricapi_{day.isoformat()}_{ht}_{at}"),
                    }
                )
        except Exception as exc:
            logger.debug("[hf_pipeline] CricAPI upcoming %s: %s", day, exc)
        return rows

    def _fetch_cricket_upcoming_rapidapi(self, day: datetime.date) -> list[dict]:
        rows: list[dict] = []
        if not self.cricket_rapidapi_key:
            return rows
        try:
            headers = {
                "X-RapidAPI-Key": self.cricket_rapidapi_key,
                "X-RapidAPI-Host": self.cricket_rapidapi_host,
            }
            resp = requests.get(
                f"{self.cricket_rapidapi_base_url}/fixtures-by-date/{day.isoformat()}",
                headers=headers,
                timeout=20,
            )
            if resp.status_code != 200:
                return rows
            payload = resp.json() or {}
            fixture_rows = payload.get("results") if isinstance(payload.get("results"), list) else payload.get("data")
            if not isinstance(fixture_rows, list):
                return rows
            for m in fixture_rows:
                if not isinstance(m, dict):
                    continue
                status = str(m.get("status") or m.get("match_status") or "").lower()
                if any(token in status for token in ("result", "won", "draw", "tie", "complete", "final")):
                    continue
                ht = str(m.get("home") or m.get("home_team") or m.get("team1") or "").strip()
                at = str(m.get("away") or m.get("away_team") or m.get("team2") or "").strip()
                if not ht or not at:
                    continue
                rows.append(
                    {
                        "sport": "cricket",
                        "league": str(m.get("series_name") or m.get("league") or "Cricket"),
                        "home_team": ht,
                        "away_team": at,
                        "game_date": day.isoformat(),
                        "game_time": str(m.get("date") or m.get("start_time") or f"{day.isoformat()}T00:00:00Z"),
                        "game_id": str(m.get("id") or f"rapidapi_{day.isoformat()}_{ht}_{at}"),
                    }
                )
        except Exception as exc:
            logger.debug("[hf_pipeline] RapidAPI cricket upcoming %s: %s", day, exc)
        return rows

    def _fetch_mlb_upcoming(self, day: datetime.date) -> list[dict]:
        rows: list[dict] = []
        try:
            resp = requests.get(
                "https://statsapi.mlb.com/api/v1/schedule",
                params={"sportId": 1, "date": day.isoformat(), "gameType": "R"}, timeout=20,
            )
            resp.raise_for_status()
            for de in (resp.json() or {}).get("dates", []):
                for game in de.get("games", []):
                    teams = game.get("teams") or {}
                    ht = str((((teams.get("home") or {}).get("team") or {}).get("name") or "")).strip()
                    at = str((((teams.get("away") or {}).get("team") or {}).get("name") or "")).strip()
                    home_team_id = str((((teams.get("home") or {}).get("team") or {}).get("id") or "")).strip()
                    away_team_id = str((((teams.get("away") or {}).get("team") or {}).get("id") or "")).strip()
                    if not ht or not at:
                        continue
                    home_starter = str((((teams.get("home") or {}).get("probablePitcher") or {}).get("fullName") or "")).strip()
                    away_starter = str((((teams.get("away") or {}).get("probablePitcher") or {}).get("fullName") or "")).strip()
                    rows.append({"sport": "mlb", "league": "MLB", "home_team": ht, "away_team": at,
                                 "game_date": day.isoformat(), "game_time": str(game.get("gameDate") or ""),
                                 "game_id": str(game.get("gamePk") or ""),
                                 "home_starter": home_starter, "away_starter": away_starter,
                                 "home_team_id": home_team_id, "away_team_id": away_team_id})
        except Exception as exc:
            logger.warning("[hf_pipeline] MLB upcoming fetch failed for %s: %s", day, exc)
        return rows

    def _fetch_mlb_team_players(self, team_id: str, cache: dict[str, list[str]]) -> list[str]:
        key = f"mlb_team|{str(team_id or '').strip()}"
        if key in cache:
            return cache[key]
        rows: list[str] = []
        if not str(team_id or "").strip():
            cache[key] = rows
            return rows
        try:
            resp = requests.get(
                f"https://statsapi.mlb.com/api/v1/teams/{team_id}/roster",
                params={"rosterType": "active"},
                timeout=20,
            )
            resp.raise_for_status()
            for entry in ((resp.json() or {}).get("roster") or []):
                name = str(((entry.get("person") or {}).get("fullName")) or "").strip()
                if name:
                    rows.append(name)
        except Exception:
            rows = []
        deduped = []
        seen = set()
        for name in rows:
            k = name.lower()
            if k in seen:
                continue
            seen.add(k)
            deduped.append(name)
        cache[key] = deduped
        return deduped

    def _fetch_soccer_squad_espn(self, team_name: str, cache: dict[str, list[str]]) -> list[str]:
        """
        Fetch soccer squad from ESPN team roster API (free, no auth).
        Falls back to a curated WC 2026 starter list if ESPN fails.
        Returns a list of player full names for the given team.
        """
        key = f"soccer_squad|{str(team_name or '').strip().lower()}"
        if key in cache:
            return cache[key]
        players: list[str] = []

        # Attempt ESPN team search → roster
        try:
            # Step 1: search for team ID
            r = requests.get(
                "https://site.api.espn.com/apis/site/v2/sports/soccer/all/teams",
                timeout=15,
                headers={"User-Agent": "Mozilla/5.0"},
            )
            if r.status_code == 200:
                for team in (r.json() or {}).get("sports", [{}])[0].get("leagues", [{}])[0].get("teams", []):
                    t = team.get("team") or {}
                    dn = str(t.get("displayName") or t.get("name") or "").lower()
                    if dn and (dn in team_name.lower() or team_name.lower() in dn):
                        team_id = str(t.get("id") or "")
                        if team_id:
                            # Step 2: fetch roster
                            r2 = requests.get(
                                f"https://site.api.espn.com/apis/site/v2/sports/soccer/all/teams/{team_id}/roster",
                                timeout=15,
                                headers={"User-Agent": "Mozilla/5.0"},
                            )
                            if r2.status_code == 200:
                                for athlete in (r2.json() or {}).get("athletes", []):
                                    name = str(athlete.get("fullName") or athlete.get("displayName") or "").strip()
                                    if name:
                                        players.append(name)
                        break
        except Exception as exc:
            logger.debug("[hf_pipeline] ESPN squad %s: %s", team_name, exc)

        # Fallback: curated WC 2026 key players per nation
        if not players:
            players = self._wc2026_squad_fallback(team_name)

        # Dedupe and cache
        seen: set[str] = set()
        deduped: list[str] = []
        for name in players:
            k = name.lower()
            if k not in seen:
                seen.add(k)
                deduped.append(name)
        cache[key] = deduped
        return deduped

    def _wc2026_squad_fallback(self, team_name: str) -> list[str]:
        """Curated key players for WC 2026 nations. Used when ESPN roster fails."""
        name_lower = str(team_name or "").lower()
        # mapping: (keywords) -> key players
        squads: dict[tuple, list[str]] = {
            ("brazil",): ["Vinicius Jr", "Rodrygo", "Raphinha", "Endrick", "Alisson Becker"],
            ("argentina",): ["Lionel Messi", "Julian Alvarez", "Lautaro Martinez", "Enzo Fernandez", "Emiliano Martinez"],
            ("france",): ["Kylian Mbappe", "Antoine Griezmann", "Olivier Giroud", "Ousmane Dembele", "Mike Maignan"],
            ("england",): ["Harry Kane", "Jude Bellingham", "Phil Foden", "Bukayo Saka", "Jordan Pickford"],
            ("germany",): ["Jamal Musiala", "Florian Wirtz", "Kai Havertz", "Thomas Muller", "Manuel Neuer"],
            ("spain",): ["Pedri", "Gavi", "Alvaro Morata", "Rodri", "Unai Simon"],
            ("portugal",): ["Cristiano Ronaldo", "Bruno Fernandes", "Rafael Leao", "Joao Felix", "Diogo Costa"],
            ("netherlands", "holland",): ["Virgil van Dijk", "Memphis Depay", "Cody Gakpo", "Xavi Simons", "Bart Verbruggen"],
            ("italy",): ["Federico Chiesa", "Gianluigi Donnarumma", "Nicolo Barella", "Ciro Immobile", "Sandro Tonali"],
            ("belgium",): ["Kevin De Bruyne", "Romelu Lukaku", "Eden Hazard", "Alexis Saelemaekers", "Thibaut Courtois"],
            ("morocco",): ["Achraf Hakimi", "Hakim Ziyech", "Youssef En-Nesyri", "Sofyan Amrabat", "Bono"],
            ("usa", "united states",): ["Christian Pulisic", "Weston McKennie", "Gio Reyna", "Matt Turner", "Tim Weah"],
            ("mexico",): ["Hirving Lozano", "Raul Jimenez", "Edson Alvarez", "Guillermo Ochoa", "Henry Martin"],
            ("canada",): ["Alphonso Davies", "Jonathan David", "Cyle Larin", "Milan Borjan", "Tajon Buchanan"],
            ("norway",): ["Erling Haaland", "Martin Odegaard", "Alexander Sorloth", "Sander Berge", "Jorgen Strand Larsen"],
            ("japan",): ["Takumi Minamino", "Hiroki Sakai", "Daichi Kamada", "Shuichi Gonda", "Takefusa Kubo"],
            ("australia",): ["Mathew Leckie", "Martin Boyle", "Ajdin Hrustic", "Mat Ryan", "Aaron Mooy"],
            ("senegal",): ["Sadio Mane", "Edouard Mendy", "Kalidou Koulibaly", "Idrissa Gueye", "Ismaila Sarr"],
            ("croatia",): ["Luka Modric", "Mateo Kovacic", "Ivan Perisic", "Ante Budimir", "Dominik Livakovic"],
            ("sweden",): ["Zlatan Ibrahimovic", "Victor Lindelof", "Dejan Kulusevski", "Alexander Isak", "Robin Olsen"],
            ("denmark",): ["Christian Eriksen", "Pierre-Emile Hojbjerg", "Jonas Wind", "Kasper Schmeichel", "Mikkel Damsgaard"],
            ("switzerland",): ["Granit Xhaka", "Xherdan Shaqiri", "Breel Embolo", "Yann Sommer", "Denis Zakaria"],
            ("colombia",): ["James Rodriguez", "Radamel Falcao", "Luis Diaz", "David Ospina", "Juan Cuadrado"],
            ("uruguay",): ["Luis Suarez", "Edinson Cavani", "Federico Valverde", "Darwin Nunez", "Fernando Muslera"],
        }
        for keywords, players in squads.items():
            if any(kw in name_lower for kw in keywords):
                return players
        return []

    def _fetch_nba_upcoming(self, day: datetime.date) -> list[dict]:
        rows: list[dict] = []
        if not self.balldontlie_api_key:
            logger.debug("[hf_pipeline] NBA upcoming skipped: BALLDONTLIE_API_KEY not set")
            return rows
        try:
            resp = requests.get(
                f"{self.balldontlie_base_url}/games",
                params={"start_date": day.isoformat(), "end_date": day.isoformat(), "per_page": 100},
                headers={"Authorization": self.balldontlie_api_key},
                timeout=20,
            )
            resp.raise_for_status()
            for g in (resp.json() or {}).get("data", []):
                ht = str((g.get("home_team") or {}).get("full_name") or "").strip()
                at = str((g.get("visitor_team") or {}).get("full_name") or "").strip()
                if not ht or not at:
                    continue
                rows.append({"sport": "nba", "league": "NBA", "home_team": ht, "away_team": at,
                             "game_date": day.isoformat(), "game_time": str(g.get("date") or ""),
                             "game_id": str(g.get("id") or "")})
        except Exception as exc:
            logger.warning("[hf_pipeline] NBA upcoming fetch failed for %s: %s", day, exc)
        return rows

    def _fetch_nhl_upcoming(self, day: datetime.date) -> list[dict]:
        rows: list[dict] = []
        try:
            resp = requests.get(f"https://api-web.nhle.com/v1/schedule/{day.isoformat()}", timeout=20)
            resp.raise_for_status()
            for week in (resp.json() or {}).get("gameWeek", []):
                for game in week.get("games", []):
                    hd = game.get("homeTeam") or {}
                    ad = game.get("awayTeam") or {}
                    ht = str((hd.get("commonName") or {}).get("default") or hd.get("abbrev") or "").strip()
                    at = str((ad.get("commonName") or {}).get("default") or ad.get("abbrev") or "").strip()
                    if not ht or not at:
                        continue
                    rows.append({"sport": "nhl", "league": "NHL", "home_team": ht, "away_team": at,
                                 "game_date": str(game.get("gameDate") or day.isoformat()),
                                 "game_time": str(game.get("startTimeUTC") or ""),
                                 "game_id": str(game.get("id") or "")})
        except Exception as exc:
            logger.warning("[hf_pipeline] NHL upcoming fetch failed for %s: %s", day, exc)
        return rows

    def _fetch_soccer_upcoming(self, day: datetime.date) -> list[dict]:
        """Aggregate upcoming soccer from all free sources; never short-circuit."""
        rows: list[dict] = []
        rows += self._fetch_soccer_upcoming_football_data(day)
        rows += self._fetch_soccer_upcoming_thesportsdb(day)
        rows += self._fetch_soccer_upcoming_openligadb(day)
        rows += self._fetch_soccer_upcoming_wc2026(day)
        rows += self._fetch_soccer_upcoming_espn(day)
        # dedupe by (home, away, date)
        seen: set[tuple] = set()
        deduped: list[dict] = []
        for r in rows:
            k = (
                str(r.get("home_team") or "").lower().strip(),
                str(r.get("away_team") or "").lower().strip(),
                str(r.get("game_date") or ""),
            )
            if k[0] and k[1] and k not in seen:
                seen.add(k)
                deduped.append(r)
        return deduped

    def _fetch_soccer_upcoming_openligadb(self, day: datetime.date) -> list[dict]:
        """Fetch from open-ligadb.de (free, no key, German leagues + international)."""
        rows: list[dict] = []
        # openligadb league short-names available without auth
        league_slugs = [
            ("bl1", 2026, "Bundesliga"),
            ("bl2", 2026, "2. Bundesliga"),
            ("ucl_24_25", 2025, "Champions League"),
        ]
        for slug, season, league_name in league_slugs:
            try:
                resp = requests.get(
                    f"https://api.openligadb.de/getmatchdata/{slug}/{season}",
                    timeout=15,
                )
                if resp.status_code != 200:
                    continue
                for m in (resp.json() or []):
                    dt_raw = str(m.get("matchDateTime") or m.get("matchDateTimeUTC") or "")
                    if not dt_raw:
                        continue
                    try:
                        match_date = datetime.date.fromisoformat(dt_raw[:10])
                    except Exception:
                        continue
                    if match_date != day:
                        continue
                    t1 = (m.get("team1") or {})
                    t2 = (m.get("team2") or {})
                    ht = str(t1.get("teamName") or t1.get("shortName") or "").strip()
                    at = str(t2.get("teamName") or t2.get("shortName") or "").strip()
                    if not ht or not at:
                        continue
                    match_id = str(m.get("matchID") or "")
                    rows.append({
                        "sport": "soccer",
                        "league": league_name,
                        "home_team": ht,
                        "away_team": at,
                        "game_date": day.isoformat(),
                        "game_time": dt_raw if "T" in dt_raw else f"{dt_raw}T00:00:00Z",
                        "game_id": f"openliga_{match_id}",
                    })
            except Exception as exc:
                logger.debug("[hf_pipeline] openligadb %s: %s", slug, exc)
        return rows

    def _fetch_soccer_upcoming_wc2026(self, day: datetime.date) -> list[dict]:
        """Fetch FIFA World Cup 2026 schedule from public JSON feeds (no API key)."""
        rows: list[dict] = []
        wc_start = datetime.date(2026, 6, 11)
        wc_end = datetime.date(2026, 7, 19)
        if not (wc_start <= day <= wc_end):
            return rows
        # Primary: try scoreboard/schedule APIs that serve WC 2026 data freely
        sources = [
            # ESPN public API (no auth, CORS-open)
            "https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/scoreboard",
            # SofaScore public API proxy
            "https://api.sofascore.com/api/v1/sport/football/scheduled-events/" + day.isoformat(),
        ]
        # Try ESPN
        try:
            r = requests.get(
                "https://site.api.espn.com/apis/site/v2/sports/soccer/fifa.world/scoreboard",
                params={"dates": day.strftime("%Y%m%d")},
                timeout=15,
                headers={"User-Agent": "Mozilla/5.0"},
            )
            if r.status_code == 200:
                data = r.json()
                for event in (data.get("events") or []):
                    comps = event.get("competitions") or []
                    for comp in comps:
                        competitors = comp.get("competitors") or []
                        home = next((c for c in competitors if c.get("homeAway") == "home"), None)
                        away = next((c for c in competitors if c.get("homeAway") == "away"), None)
                        if not home or not away:
                            if len(competitors) == 2:
                                home, away = competitors[0], competitors[1]
                            else:
                                continue
                        ht = str((home.get("team") or {}).get("displayName") or home.get("team", {}).get("name") or "").strip()
                        at = str((away.get("team") or {}).get("displayName") or away.get("team", {}).get("name") or "").strip()
                        if not ht or not at:
                            continue
                        game_time = str(event.get("date") or comp.get("date") or "")
                        eid = str(event.get("id") or comp.get("id") or "")
                        status_type = str(((event.get("status") or {}).get("type") or {}).get("name") or "")
                        # only upcoming / scheduled
                        if status_type in ("STATUS_FINAL", "STATUS_FULL_TIME"):
                            continue
                        rows.append({
                            "sport": "soccer",
                            "league": "FIFA World Cup 2026",
                            "home_team": ht,
                            "away_team": at,
                            "game_date": day.isoformat(),
                            "game_time": game_time,
                            "game_id": f"espn_wc_{eid}",
                        })
        except Exception as exc:
            logger.debug("[hf_pipeline] ESPN WC 2026: %s", exc)
        # Try SofaScore
        if not rows:
            try:
                r2 = requests.get(
                    f"https://api.sofascore.com/api/v1/sport/football/scheduled-events/{day.isoformat()}",
                    timeout=15,
                    headers={"User-Agent": "Mozilla/5.0"},
                )
                if r2.status_code == 200:
                    for ev in (r2.json() or {}).get("events") or []:
                        tournament = (ev.get("tournament") or {})
                        cat = str((tournament.get("category") or {}).get("name") or "").lower()
                        tname = str(tournament.get("name") or "").lower()
                        # filter to soccer internationals / major leagues
                        is_relevant = any(kw in tname or kw in cat for kw in (
                            "world cup", "copa", "euro", "nations league",
                            "premier league", "la liga", "serie a", "bundesliga",
                            "ligue 1", "mls", "champions league", "europa",
                        ))
                        if not is_relevant:
                            continue
                        ht = str((ev.get("homeTeam") or {}).get("name") or "").strip()
                        at = str((ev.get("awayTeam") or {}).get("name") or "").strip()
                        if not ht or not at:
                            continue
                        start_ts = ev.get("startTimestamp")
                        game_time = ""
                        if start_ts:
                            try:
                                game_time = datetime.datetime.utcfromtimestamp(start_ts).strftime("%Y-%m-%dT%H:%M:%SZ")
                            except Exception:
                                pass
                        rows.append({
                            "sport": "soccer",
                            "league": str(tournament.get("name") or "Soccer"),
                            "home_team": ht,
                            "away_team": at,
                            "game_date": day.isoformat(),
                            "game_time": game_time,
                            "game_id": f"sofa_{ev.get('id','')}",
                        })
            except Exception as exc:
                logger.debug("[hf_pipeline] SofaScore WC 2026: %s", exc)
        # Hard-coded WC 2026 quarter/semi/final schedule as final fallback
        if not rows:
            rows += self._wc2026_static_schedule(day)
        return rows

    def _wc2026_static_schedule(self, day: datetime.date) -> list[dict]:
        """
        Static FIFA World Cup 2026 schedule (knockout stage fixtures).
        Used as last-resort fallback when all APIs fail.
        Dates and times in UTC. Teams listed by slot (filled as tournament progresses).
        """
        # Round of 16, QF, SF, Final schedule (UTC)
        fixtures = [
            # Round of 16 (June 29 - July 3)
            ("2026-06-29", "22:00", "WC R16 Match 1", "WC Group A Winner", "WC Group B Runner-up"),
            ("2026-06-30", "18:00", "WC R16 Match 2", "WC Group C Winner", "WC Group D Runner-up"),
            ("2026-06-30", "22:00", "WC R16 Match 3", "WC Group E Winner", "WC Group F Runner-up"),
            ("2026-07-01", "18:00", "WC R16 Match 4", "WC Group B Winner", "WC Group A Runner-up"),
            ("2026-07-01", "22:00", "WC R16 Match 5", "WC Group D Winner", "WC Group C Runner-up"),
            ("2026-07-02", "18:00", "WC R16 Match 6", "WC Group F Winner", "WC Group E Runner-up"),
            ("2026-07-02", "22:00", "WC R16 Match 7", "WC Group G Winner", "WC Group H Runner-up"),
            ("2026-07-03", "18:00", "WC R16 Match 8", "WC Group H Winner", "WC Group G Runner-up"),
            # Quarter-Finals (July 5-6)
            ("2026-07-04", "22:00", "WC QF 1", "WC R16 M1 Winner", "WC R16 M2 Winner"),
            ("2026-07-05", "18:00", "WC QF 2", "WC R16 M3 Winner", "WC R16 M4 Winner"),
            ("2026-07-05", "22:00", "WC QF 3", "WC R16 M5 Winner", "WC R16 M6 Winner"),
            ("2026-07-06", "22:00", "WC QF 4", "WC R16 M7 Winner", "WC R16 M8 Winner"),
            # Semi-Finals (July 9-10)
            ("2026-07-09", "22:00", "WC SF 1", "WC QF1 Winner", "WC QF2 Winner"),
            ("2026-07-10", "22:00", "WC SF 2", "WC QF3 Winner", "WC QF4 Winner"),
            # Third-place (July 14)
            ("2026-07-14", "22:00", "WC 3rd Place", "WC SF1 Loser", "WC SF2 Loser"),
            # Final (July 19)
            ("2026-07-19", "22:00", "FIFA World Cup 2026 Final", "WC SF1 Winner", "WC SF2 Winner"),
        ]
        rows = []
        for date_str, time_str, match_name, home, away in fixtures:
            if date_str != day.isoformat():
                continue
            rows.append({
                "sport": "soccer",
                "league": "FIFA World Cup 2026",
                "home_team": home,
                "away_team": away,
                "game_date": day.isoformat(),
                "game_time": f"{date_str}T{time_str}:00Z",
                "game_id": f"wc2026_{date_str}_{home[:8]}",
            })
        return rows

    def _fetch_soccer_upcoming_football_data(self, day: datetime.date) -> list[dict]:
        rows: list[dict] = []
        if not self.football_data_api_key:
            return rows
        headers = {"X-Auth-Token": self.football_data_api_key}
        # Expanded competition codes for maximum soccer coverage
        competitions = (
            # Europe top 5 leagues
            "PL", "PD", "SA", "BL1", "FL1", "PPL",
            # Europe second tier & cups
            "ELC", "DED", "BSA", "CL", "EL", "EC",
            # Americas
            "MLS", "CLI", "BSA",
            # Other major regions when available
            "WC", "ASC", "AFR", "OC",
        )
        for comp in competitions:
            try:
                resp = requests.get(
                    f"https://api.football-data.org/v4/competitions/{comp}/matches",
                    params={"dateFrom": day.isoformat(), "dateTo": day.isoformat(), "status": "SCHEDULED"},
                    headers=headers,
                    timeout=25,
                )
                resp.raise_for_status()
                for m in (resp.json() or {}).get("matches", []):
                    ht = str(((m.get("homeTeam") or {}).get("name")) or "").strip()
                    at = str(((m.get("awayTeam") or {}).get("name")) or "").strip()
                    if not ht or not at:
                        continue
                    # Extract odds if available
                    odds_data = {}
                    if m.get("odds"):
                        odds = m.get("odds") or {}
                        if odds.get("homeWin"):
                            odds_data["odds_home_win"] = float(odds.get("homeWin") or 0)
                        if odds.get("awayWin"):
                            odds_data["odds_away_win"] = float(odds.get("awayWin") or 0)
                        if odds.get("draw"):
                            odds_data["odds_draw"] = float(odds.get("draw") or 0)
                    rows.append(
                        {
                            "sport": "soccer",
                            "league": str((m.get("competition") or {}).get("name") or comp),
                            "home_team": ht,
                            "away_team": at,
                            "game_date": day.isoformat(),
                            "game_time": str(m.get("utcDate") or ""),
                            "game_id": str(m.get("id") or ""),
                            "odds": odds_data if odds_data else None,
                        }
                    )
                time.sleep(0.2)
            except Exception as exc:
                logger.debug("[hf_pipeline] football-data upcoming %s: %s", comp, exc)
        return rows

    def _fetch_team_players_thesportsdb(self, team_name: str, sport: str, cache: dict[str, list[str]]) -> list[str]:
        key = f"{self._normalize_sport(sport)}|{str(team_name or '').strip().lower()}"
        if not key.strip("|"):
            return []
        if key in cache:
            return cache[key]
        players: list[str] = []
        try:
            # searchplayers.php is premium-gated even under the free test key; go via
            # searchteams.php -> lookup_all_players.php by team ID instead, which
            # still works on the free tier.
            team_resp = requests.get(
                f"https://www.thesportsdb.com/api/v1/json/{self.thesportsdb_api_key}/searchteams.php",
                params={"t": str(team_name or "").strip()},
                timeout=20,
            )
            team_resp.raise_for_status()
            team_id = ""
            wanted_sport = self._normalize_sport(sport)
            for t in ((team_resp.json() or {}).get("teams") or []):
                t_sport = self._normalize_sport(str(t.get("strSport") or ""))
                # TheSportsDB's own strSport field is a broad category (e.g. "Basketball"
                # covers both NBA and WNBA teams) so it can't distinguish nba vs wnba --
                # only reject on a genuine cross-sport mismatch, not this ambiguity.
                same_broad_basketball = {t_sport, wanted_sport} <= {"nba", "wnba"}
                if t_sport and t_sport != wanted_sport and not same_broad_basketball:
                    continue
                team_id = str(t.get("idTeam") or "").strip()
                if team_id:
                    break
            if team_id:
                roster_resp = requests.get(
                    f"https://www.thesportsdb.com/api/v1/json/{self.thesportsdb_api_key}/lookup_all_players.php",
                    params={"id": team_id},
                    timeout=20,
                )
                roster_resp.raise_for_status()
                for p in ((roster_resp.json() or {}).get("player") or []):
                    name = str(p.get("strPlayer") or "").strip()
                    if name:
                        players.append(name)
        except Exception:
            players = []
        if not players and self._normalize_sport(sport) == "cricket":
            players = self._fetch_cricket_players_cricapi(team_name, cache)
        deduped = []
        seen = set()
        for name in players:
            k = name.lower()
            if k in seen:
                continue
            seen.add(k)
            deduped.append(name)
        cache[key] = deduped
        return deduped

    def _fetch_cricket_players_cricapi(self, team_name: str, cache: dict[str, list[str]]) -> list[str]:
        key = f"cricket_players|{str(team_name or '').strip().lower()}"
        if key in cache:
            return cache[key]
        rows: list[str] = []
        if self.cricapi_key:
            try:
                resp = requests.get(
                    f"{self.cricapi_base_url}/players",
                    params={"apikey": self.cricapi_key, "offset": 0, "search": str(team_name or "").strip()},
                    timeout=20,
                )
                if resp.status_code == 200:
                    for p in (resp.json() or {}).get("data") or []:
                        if not isinstance(p, dict):
                            continue
                        name = str(p.get("name") or "").strip()
                        country = str(p.get("country") or p.get("teamName") or "").strip().lower()
                        if not name:
                            continue
                        if country and str(team_name or "").strip().lower() not in country:
                            continue
                        rows.append(name)
                        if len(rows) >= 8:
                            break
            except Exception as exc:
                logger.debug("[hf_pipeline] CricAPI players %s: %s", team_name, exc)
        if not rows:
            rows = [
                f"{team_name} Top Batter".strip(),
                f"{team_name} Top Bowler".strip(),
                f"{team_name} Strike All-Rounder".strip(),
            ]
        cache[key] = rows
        return rows

    def _fetch_soccer_upcoming_thesportsdb(self, day: datetime.date) -> list[dict]:
        rows: list[dict] = []
        try:
            resp = requests.get(
                f"https://www.thesportsdb.com/api/v1/json/{self.thesportsdb_api_key}/eventsday.php",
                params={"d": day.isoformat(), "s": "Soccer"}, timeout=20,
            )
            resp.raise_for_status()
            for ev in ((resp.json() or {}).get("events") or []):
                if ev.get("intHomeScore") is not None:
                    continue
                ht = str(ev.get("strHomeTeam") or "").strip()
                at = str(ev.get("strAwayTeam") or "").strip()
                if not ht or not at:
                    continue
                rows.append({"sport": "soccer", "league": str(ev.get("strLeague") or "Soccer"),
                             "home_team": ht, "away_team": at,
                             "game_date": day.isoformat(), "game_time": str(ev.get("strTime") or ""),
                             "game_id": str(ev.get("idEvent") or "")})
        except Exception as exc:
            logger.warning("[hf_pipeline] soccer TheSportsDB upcoming fetch failed for %s: %s", day, exc)
        return rows

    def _fetch_tennis_games_jeff_sackmann(self, start: datetime.date, end: datetime.date) -> list[dict]:
        rows: list[dict] = []
        base_dir = str(self.tennis_sackmann_dir or "").strip()
        if not base_dir or not os.path.isdir(base_dir):
            return rows
        try:
            import csv
        except Exception:
            return rows

        for year in range(start.year, end.year + 1):
            csv_path = os.path.join(base_dir, f"atp_matches_{year}.csv")
            if not os.path.exists(csv_path):
                continue
            try:
                with open(csv_path, "r", encoding="utf-8", newline="") as handle:
                    reader = csv.DictReader(handle)
                    for row in reader:
                        date_raw = str(row.get("tourney_date") or "").strip()
                        if len(date_raw) != 8 or not date_raw.isdigit():
                            continue
                        game_date = f"{date_raw[:4]}-{date_raw[4:6]}-{date_raw[6:8]}"
                        try:
                            parsed = datetime.date.fromisoformat(game_date)
                        except Exception:
                            continue
                        if parsed < start or parsed > end:
                            continue
                        winner = str(row.get("winner_name") or "").strip()
                        loser = str(row.get("loser_name") or "").strip()
                        if not winner or not loser:
                            continue
                        rows.append(
                            self._make_game_record(
                                game_id=str(row.get("match_num") or f"{year}-{winner}-{loser}-{game_date}"),
                                sport="tennis",
                                league=str(row.get("tourney_name") or "ATP"),
                                game_date=game_date,
                                game_datetime=game_date,
                                status="Final",
                                home_team=winner,
                                away_team=loser,
                                home_score=1.0,
                                away_score=0.0,
                                season=parsed.year,
                            )
                        )
            except Exception as exc:
                logger.debug("[hf_pipeline] Jeff Sackmann %s: %s", year, exc)
        return rows

    def _fetch_kalshi_upcoming(self, day: datetime.date) -> list[dict]:
        rows: list[dict] = []
        try:
            from data.kalshi import get_today_kalshi_tickers

            kalshi_payload = get_today_kalshi_tickers()
            markets = []
            sports = kalshi_payload.get("sports") or {}
            for sport_name, sport_rows in sports.items():
                for market in sport_rows or []:
                    if not isinstance(market, dict):
                        continue
                    market["sport"] = sport_name
                    markets.append(market)
        except Exception as exc:
            logger.debug("[hf_pipeline] kalshi upcoming fetch failed: %s", exc)
            return rows

        for m in markets:
            if not isinstance(m, dict):
                continue
            event_iso = str(m.get("close_time") or "").strip()
            if not event_iso:
                continue
            event_day = event_iso[:10]
            if event_day != day.isoformat():
                continue
            home, away = self._parse_market_matchup(str(m.get("title") or ""))
            if not home or not away:
                continue
            text_blob = " ".join(
                [
                    str(m.get("sport") or ""),
                    str(m.get("title") or ""),
                ]
            ).strip().lower()
            sport_raw = str(m.get("sport") or "").strip().lower()
            sport = self._MARKET_SPORT_ALIASES.get(sport_raw, "")
            if not sport:
                for k, v in self._MARKET_SPORT_ALIASES.items():
                    if k in sport_raw:
                        sport = v
                        break
            if not sport:
                sport = self._infer_sport_from_text(text_blob)
            if not sport:
                continue
            rows.append(
                {
                    "sport": sport,
                    "league": str(m.get("event_ticker") or "Kalshi"),
                    "home_team": home,
                    "away_team": away,
                    "game_date": event_day,
                    "game_time": event_iso,
                    "game_id": str(m.get("ticker") or ""),
                }
            )
        return rows

    def _parse_market_matchup(self, text: str) -> tuple[str, str]:
        raw = " ".join(str(text or "").split())
        if not raw:
            return "", ""
        patterns = [
            r"will\s+(?P<a>.+?)\s+beat\s+(?P<b>.+)",
            r"(?P<a>.+?)\s+to\s+beat\s+(?P<b>.+)",
            r"(?P<a>.+?)\s+vs\.?\s+(?P<b>.+)",
            r"(?P<a>.+?)\s+v\.?\s+(?P<b>.+)",
            r"(?P<a>.+?)\s+@\s+(?P<b>.+)",
            r"(?P<a>.+?)\s+at\s+(?P<b>.+)",
        ]
        for pat in patterns:
            m = re.search(pat, raw, flags=re.IGNORECASE)
            if not m:
                continue
            a = str(m.group("a") or "").strip(" ?!,:;")
            b = str(m.group("b") or "").strip(" ?!,:;")
            if a and b and a.lower() != b.lower():
                return b, a
        return "", ""

    def _infer_sport_from_text(self, text: str) -> str:
        raw = str(text or "").strip().lower()
        if not raw:
            return ""
        keyword_map = {
            "world series": "mlb",
            "mlb": "mlb",
            "wnba": "wnba",
            "nba": "nba",
            "nfl": "nfl",
            "super bowl": "nfl",
            "nhl": "nhl",
            "premier league": "soccer",
            "la liga": "soccer",
            "uefa": "soccer",
            "soccer": "soccer",
            "tennis": "tennis",
            "atp": "tennis",
            "wta": "tennis",
            "lpga": "golf",
            "pga": "golf",
            "golf": "golf",
            "boxing": "boxing",
            "ufc": "mma",
            "mma": "mma",
            "cricket": "cricket",
        }
        for key, normalized in keyword_map.items():
            if key in raw:
                return normalized
        return ""

    def collect_news_signals(
        self,
        *,
        days: list[datetime.date] | None = None,
        max_games: int = 120,
        max_players_per_team: int = 6,
        return_rows: bool = False,
    ) -> dict:
        """Fetch and push structured news-impact rows to HF dataset.

        When `return_rows` is set, the in-memory rows are also included under
        the "signals" key so a caller (e.g. the same-day in-depth prediction
        pass) can react to them without a second push/fetch cycle.
        """
        target_days = list(days or [])
        if not target_days:
            today = et_today()
            target_days = [today, today + datetime.timedelta(days=1)]

        games: list[dict] = []
        for day in target_days:
            games.extend(self._fetch_upcoming_games(day))

        deduped_games: list[dict] = []
        seen_games: set[tuple[str, str, str, str]] = set()
        for g in games:
            key = (
                self._normalize_sport(str(g.get("sport") or "")),
                str(g.get("game_date") or ""),
                str(g.get("away_team") or "").strip().lower(),
                str(g.get("home_team") or "").strip().lower(),
            )
            if key in seen_games:
                continue
            seen_games.add(key)
            deduped_games.append(g)
            if len(deduped_games) >= max_games:
                break

        player_cache: dict[str, list[str]] = {}
        rows: list[dict] = []
        for game in deduped_games:
            sport = self._normalize_sport(str(game.get("sport") or ""))
            home_team = str(game.get("home_team") or "").strip()
            away_team = str(game.get("away_team") or "").strip()
            if not home_team or not away_team:
                continue
            home_players = self._fetch_team_players_thesportsdb(home_team, sport, player_cache)[:max_players_per_team]
            away_players = self._fetch_team_players_thesportsdb(away_team, sport, player_cache)[:max_players_per_team]
            players = home_players + away_players
            articles = self._fetch_news_articles_for_game(home_team, away_team, sport=sport)
            if not articles:
                continue
            for article in articles:
                rows.extend(
                    self._expand_article_to_news_rows(
                        article=article,
                        game=game,
                        sport=sport,
                        players=players,
                    )
                )

        rows = self._dedupe_news_rows(rows)
        if rows:
            self.uploader.push_records("news_signals", rows)
            self.uploader.flush_all()
        self._write_status(
            {
                "last_step": "news_signals",
                "ok": True,
                "news_signal_rows": len(rows),
                "news_signal_games": len(deduped_games),
                "news_signal_updated_at": _now_utc(),
            }
        )
        result = {"ok": True, "rows": len(rows), "games": len(deduped_games)}
        if return_rows:
            result["signals"] = rows
        return result

    def _fetch_news_articles_for_game(self, home_team: str, away_team: str, *, sport: str) -> list[dict]:
        query = f"{home_team} OR {away_team}"
        rows: list[dict] = []

        # newsdata.io (if configured)
        if self.newsdata_api_key:
            try:
                resp = requests.get(
                    "https://newsdata.io/api/1/news",
                    params={
                        "apikey": self.newsdata_api_key,
                        "q": query,
                        "language": "en",
                        "category": "sports",
                        "size": 30,
                    },
                    timeout=15,
                )
                if resp.status_code == 200:
                    for item in (resp.json() or {}).get("results") or []:
                        rows.append(
                            {
                                "title": str(item.get("title") or "").strip(),
                                "description": str(item.get("description") or "").strip(),
                                "url": str(item.get("link") or "").strip(),
                                "source": str(item.get("source_id") or "newsdata").strip(),
                                "published": str(item.get("pubDate") or "").strip(),
                            }
                        )
            except Exception as exc:
                logger.debug("[hf_pipeline] newsdata fetch failed: %s", exc)

        # GDELT free fallback
        try:
            resp = requests.get(
                "https://api.gdeltproject.org/api/v2/doc/doc",
                params={
                    "query": f"({query}) {sport}",
                    "mode": "artlist",
                    "maxrecords": 20,
                    "format": "json",
                    "sourcelang": "english",
                    "sort": "datedesc",
                },
                timeout=15,
            )
            if resp.status_code == 200:
                for item in (resp.json() or {}).get("articles") or []:
                    rows.append(
                        {
                            "title": str(item.get("title") or "").strip(),
                            "description": "",
                            "url": str(item.get("url") or "").strip(),
                            "source": str(item.get("domain") or "gdelt").strip(),
                            "published": str(item.get("seendate") or "").strip(),
                        }
                    )
        except Exception as exc:
            logger.debug("[hf_pipeline] gdelt fetch failed: %s", exc)

        # Google News RSS fallback
        try:
            from urllib.parse import quote
            import xml.etree.ElementTree as ET

            rss_url = f"https://news.google.com/rss/search?q={quote(query + ' ' + sport)}&hl=en-US&gl=US&ceid=US:en"
            resp = requests.get(rss_url, timeout=15)
            if resp.status_code == 200:
                root = ET.fromstring(resp.content)
                for item in root.findall(".//item"):
                    rows.append(
                        {
                            "title": str(item.findtext("title") or "").strip(),
                            "description": str(item.findtext("description") or "").strip(),
                            "url": str(item.findtext("link") or "").strip(),
                            "source": "google_news_rss",
                            "published": str(item.findtext("pubDate") or "").strip(),
                        }
                    )
        except Exception as exc:
            logger.debug("[hf_pipeline] google rss fetch failed: %s", exc)

        deduped: list[dict] = []
        seen: set[str] = set()
        for row in rows:
            title = str(row.get("title") or "").strip()
            url = str(row.get("url") or "").strip().lower()
            if not title:
                continue
            key = f"{title.lower()[:180]}|{url[:220]}"
            if key in seen:
                continue
            seen.add(key)
            deduped.append(row)
            if len(deduped) >= 45:
                break
        return deduped

    def _expand_article_to_news_rows(self, *, article: dict, game: dict, sport: str, players: list[str]) -> list[dict]:
        now = _now_utc()
        headline = str(article.get("title") or "").strip()
        description = str(article.get("description") or "").strip()
        if not headline:
            return []
        home_team = str(game.get("home_team") or "").strip()
        away_team = str(game.get("away_team") or "").strip()
        text_norm = self._news_norm_text(f"{headline} {description}")
        impact_type = self._classify_news_impact(text_norm)
        sentiment_score = self._news_sentiment_score(text_norm)
        impact_score = min(1.0, max(0.0, abs(sentiment_score) + (0.25 if impact_type in {"injury_concern", "lineup_change"} else 0.1)))

        team_hits: list[str] = []
        for team in (home_team, away_team):
            if team and self._news_contains_name(text_norm, team):
                team_hits.append(team)

        player_hits: list[str] = []
        for player in players or []:
            if player and self._news_contains_name(text_norm, player):
                player_hits.append(player)
            if len(player_hits) >= 2:
                break

        rows: list[dict] = []
        base = {
            "news_id": str(uuid4()),
            "sport": sport,
            "league": str(game.get("league") or sport.upper())[:80],
            "game_id": str(game.get("game_id") or ""),
            "game_date": str(game.get("game_date") or "")[:10],
            "game_time": str(game.get("game_time") or "")[:40],
            "home_team": home_team[:120],
            "away_team": away_team[:120],
            "impact_type": impact_type,
            "impact_scope": "game",
            "headline": headline[:400],
            "description": description[:1000],
            "article_url": str(article.get("url") or "")[:500],
            "source_name": str(article.get("source") or "unknown")[:100],
            "published_at": str(article.get("published") or "")[:40],
            "sentiment_score": round(sentiment_score, 4),
            "impact_score": round(impact_score, 4),
            "metadata": json.dumps({"team_hits": team_hits, "player_hits": player_hits}, ensure_ascii=True),
            "created_at": now,
        }
        for player in player_hits:
            rows.append(
                {
                    **base,
                    "news_id": str(uuid4()),
                    "entity_type": "player",
                    "entity_name": player[:120],
                    "entity_team": self._infer_player_team(player, home_team, away_team)[:120],
                    "impact_scope": "player",
                }
            )
        for team in team_hits:
            rows.append(
                {
                    **base,
                    "news_id": str(uuid4()),
                    "entity_type": "team",
                    "entity_name": team[:120],
                    "entity_team": team[:120],
                    "impact_scope": "team",
                }
            )
        if not rows:
            rows.append(
                {
                    **base,
                    "entity_type": "game",
                    "entity_name": f"{away_team} @ {home_team}"[:120],
                    "entity_team": "",
                }
            )
        return rows

    def _infer_player_team(self, player_name: str, home_team: str, away_team: str) -> str:
        player_tokens = set(self._news_norm_text(player_name).split())
        home_tokens = set(self._news_norm_text(home_team).split())
        away_tokens = set(self._news_norm_text(away_team).split())
        if player_tokens & home_tokens:
            return home_team
        if player_tokens & away_tokens:
            return away_team
        return home_team

    def _news_norm_text(self, value: str) -> str:
        return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", str(value or "").lower())).strip()

    def _news_contains_name(self, text_norm: str, name: str) -> bool:
        tokenized = self._news_norm_text(name)
        if not tokenized:
            return False
        needle = f" {tokenized} "
        haystack = f" {text_norm} "
        if needle in haystack:
            return True
        compact = tokenized.replace(" ", "")
        return bool(compact and compact in haystack.replace(" ", ""))

    def _classify_news_impact(self, text_norm: str) -> str:
        for impact_type, keywords in self._NEWS_IMPACT_KEYWORDS.items():
            if any(keyword in text_norm for keyword in keywords):
                return impact_type
        return "general_update"

    def _news_sentiment_score(self, text_norm: str) -> float:
        pos = sum(1 for term in self._NEWS_SENTIMENT_POS if term in text_norm)
        neg = sum(1 for term in self._NEWS_SENTIMENT_NEG if term in text_norm)
        total = pos + neg
        if total <= 0:
            return 0.0
        return max(-1.0, min(1.0, (pos - neg) / float(total)))

    def _dedupe_news_rows(self, rows: list[dict]) -> list[dict]:
        deduped: list[dict] = []
        seen: set[str] = set()
        for row in rows or []:
            if not isinstance(row, dict):
                continue
            key = "|".join(
                [
                    str(row.get("sport") or ""),
                    str(row.get("game_date") or ""),
                    str(row.get("entity_type") or ""),
                    str(row.get("entity_name") or "").lower(),
                    str(row.get("headline") or "").lower()[:160],
                    str(row.get("source_name") or "").lower(),
                ]
            )
            if key in seen:
                continue
            seen.add(key)
            deduped.append(row)
        return deduped

    def _load_news_signals_dataframe_from_hub(self):
        import pandas as pd
        from huggingface_hub import hf_hub_download

        if self._news_df_cache is not None:
            cached_ts, cached_df = self._news_df_cache
            if (time.time() - cached_ts) < self._df_cache_ttl_sec:
                return cached_df.copy()

        if not self._ok or not self._api:
            return pd.DataFrame()
        try:
            files = self._api.list_repo_files(repo_id=self.dataset_repo_id, repo_type="dataset")
        except Exception:
            return pd.DataFrame()
        shard_paths = [f for f in files if str(f).startswith("data/news_signals/") and str(f).endswith(".parquet")]
        if not shard_paths:
            return pd.DataFrame()
        frames: list[pd.DataFrame] = []
        for path_in_repo in shard_paths:
            try:
                local_path = hf_hub_download(
                    repo_id=self.dataset_repo_id,
                    repo_type="dataset",
                    filename=path_in_repo,
                    token=self.token,
                )
                frames.append(pd.read_parquet(local_path))
            except Exception:
                continue
        if not frames:
            return pd.DataFrame()
        result = pd.concat(frames, ignore_index=True, sort=False)
        self._news_df_cache = (time.time(), result)
        return result.copy()

    def _train_news_impact_model(self, *, output_dir: str) -> dict[str, object]:
        import joblib
        import pandas as pd
        from sklearn.feature_extraction.text import TfidfVectorizer
        from sklearn.linear_model import LogisticRegression
        from sklearn.model_selection import StratifiedKFold, cross_val_score
        from sklearn.pipeline import Pipeline

        news_df = self._load_news_signals_dataframe_from_hub()
        if news_df.empty:
            return {"ok": False, "reason": "no_news_rows", "rows": 0}

        cols_needed = {"headline", "description", "impact_type"}
        if not cols_needed.issubset(set(news_df.columns)):
            return {"ok": False, "reason": "missing_columns", "rows": len(news_df)}

        work = news_df.copy()
        work["impact_type"] = work["impact_type"].fillna("").astype(str).str.strip().replace("", "general_update")
        work["headline"] = work["headline"].fillna("").astype(str)
        work["description"] = work["description"].fillna("").astype(str)
        work["text"] = (work["headline"] + " " + work["description"]).str.strip()
        work = work[work["text"] != ""].copy()
        if len(work) < 80:
            return {"ok": False, "reason": "insufficient_news_rows", "rows": len(work)}

        class_counts = work["impact_type"].value_counts()
        valid_classes = class_counts[class_counts >= 8].index.tolist()
        work = work[work["impact_type"].isin(valid_classes)].copy()
        if len(work) < 80 or work["impact_type"].nunique() < 2:
            return {"ok": False, "reason": "insufficient_class_balance", "rows": len(work)}

        model = Pipeline(
            [
                ("tfidf", TfidfVectorizer(ngram_range=(1, 2), min_df=2, max_features=12000)),
                ("clf", LogisticRegression(max_iter=2000, multi_class="auto")),
            ]
        )
        cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
        scores = cross_val_score(model, work["text"], work["impact_type"], cv=cv, scoring="f1_macro")
        model.fit(work["text"], work["impact_type"])

        model_path = os.path.join(output_dir, "news_impact_model.joblib")
        metadata_path = os.path.join(output_dir, "news_impact_metadata.json")
        joblib.dump(model, model_path)
        metadata = {
            "trained_at": _now_utc(),
            "rows": int(len(work)),
            "classes": sorted(work["impact_type"].astype(str).unique().tolist()),
            "cv_f1_macro": round(float(scores.mean()), 6),
            "dataset_repo": self.dataset_repo_id,
        }
        with open(metadata_path, "w", encoding="utf-8") as handle:
            json.dump(metadata, handle, indent=2)
        return {"ok": True, **metadata}

    # ──────────────────────────────────────────────────────────
    # Private helpers
    # ──────────────────────────────────────────────────────────

    def _normalize_sport(self, value: str) -> str:
        raw = str(value or "").strip().lower()
        if not raw:
            return "mlb"
        if raw in self._SPORT_ALIASES:
            return self._SPORT_ALIASES[raw]
        for key, normalized in self._SPORT_ALIASES.items():
            if key in raw:
                return normalized
        return raw[:32]

    def _record_dedupe_key(self, row: dict) -> str:
        gid = str((row or {}).get("game_id") or "").strip().lower()
        sport = self._normalize_sport(str((row or {}).get("sport") or ""))
        day = str((row or {}).get("game_date") or "").strip()[:10]
        home = " ".join(str((row or {}).get("home_team") or "").strip().lower().split())
        away = " ".join(str((row or {}).get("away_team") or "").strip().lower().split())
        if gid:
            return f"id:{gid}"
        return f"{sport}|{day}|{away}|{home}"

    def _normalize_game_date(self, raw_date: str, raw_datetime: str) -> str:
        for candidate in (str(raw_date or "").strip(), str(raw_datetime or "").strip()):
            if not candidate:
                continue
            try:
                return datetime.datetime.fromisoformat(candidate.replace("Z", "+00:00")).date().isoformat()
            except Exception:
                pass
            if len(candidate) >= 10:
                return candidate[:10]
        return ""

    def _build_model_card_readme(self, metadata: dict) -> str:
        best_model = str(metadata.get("best_model") or "unknown")
        score = float(metadata.get("cv_roc_auc") or 0.0)
        rows = int(metadata.get("rows") or 0)
        version = str(metadata.get("version") or "")
        trained_at = str(metadata.get("trained_at") or _now_utc())
        sports = [str(s).strip() for s in (metadata.get("sports_covered") or []) if str(s).strip()]
        sports_text = ", ".join(sports) if sports else "unknown"
        return (
            "---\n"
            "license: mit\n"
            "library_name: scikit-learn\n"
            "pipeline_tag: tabular-classification\n"
            "tags:\n"
            "- sports\n"
            "- betting\n"
            "- scikit-learn\n"
            "- auto-training\n"
            "metrics:\n"
            "- roc_auc\n"
            "datasets:\n"
            f"- {self.dataset_repo_id}\n"
            "---\n\n"
            "# Sports Win Prediction Model\n\n"
            f"Auto-trained {trained_at[:10]} by bettor HF pipeline.\n\n"
            "| Field | Value |\n|---|---|\n"
            f"| best_model | {best_model} |\n"
            f"| cv_roc_auc | {score:.4f} |\n"
            f"| rows | {rows:,} |\n"
            f"| sports | {sports_text} |\n"
            f"| version | {version} |\n"
        )

    def _make_game_record(self, **kwargs) -> dict:
        now = _now_utc()
        record = {
            "record_id": str(uuid4()),
            "game_id": "", "sport": "mlb", "league": "",
            "game_date": "", "game_datetime": "", "status": "",
            "home_team": "", "away_team": "",
            "home_score": 0.0, "away_score": 0.0,
            "home_starter": "", "away_starter": "",
            "season": datetime.date.today().year,
            "metadata": "{}", "created_at": now,
        }
        record.update(kwargs)
        return record

    def _confidence_tier(self, prob: float) -> str:
        for threshold, tier in self._CONFIDENCE_TIERS:
            if prob >= threshold:
                return tier
        return "uncertain"

    def _clean_game_records(self, records: list[dict]) -> list[dict]:
        cleaned: list[dict] = []
        seen: set[str] = set()
        for raw in (records or []):
            if not isinstance(raw, dict):
                continue
            game_id = str(raw.get("game_id") or "").strip()
            ht = " ".join(str(raw.get("home_team") or "").strip().split())
            at = " ".join(str(raw.get("away_team") or "").strip().split())
            gd = str(raw.get("game_date") or "").strip()
            if not ht or not at or not gd:
                continue
            if ht.lower() == at.lower():
                continue
            key = self._record_dedupe_key(raw)
            if key in seen:
                continue
            seen.add(key)
            row = dict(raw)
            row["record_id"] = str(row.get("record_id") or uuid4())
            row["home_team"] = ht[:120]
            row["away_team"] = at[:120]
            row["league"] = str(row.get("league") or "").strip()[:80]
            row["sport"] = self._normalize_sport(str(row.get("sport") or "mlb"))
            row["status"] = str(row.get("status") or "").strip()[:80]
            normalized_date = self._normalize_game_date(gd, str(row.get("game_datetime") or ""))
            if not normalized_date:
                continue
            row["game_date"] = normalized_date
            row["game_datetime"] = str(row.get("game_datetime") or "").strip()[:40]
            row["season"] = int(str(row.get("season") or normalized_date[:4] or datetime.date.today().year))
            row["metadata"] = str(row.get("metadata") or "{}")
            row["created_at"] = str(row.get("created_at") or _now_utc())
            try:
                row["home_score"] = float(row.get("home_score"))
                row["away_score"] = float(row.get("away_score"))
                if row["home_score"] < 0 or row["away_score"] < 0:
                    continue
            except Exception:
                continue
            cleaned.append(row)
        return cleaned

    def _build_training_df(self, df):
        import pandas as pd
        for col in ("home_score", "away_score"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna(subset=["home_team", "away_team", "home_score", "away_score"])
        df = df[df["home_score"] != df["away_score"]].copy()
        if "sport" not in df.columns:
            df["sport"] = "mlb"
        df["sport"] = df["sport"].fillna("mlb").astype(str).map(self._normalize_sport)
        if "game_id" not in df.columns:
            df["game_id"] = ""
        if "game_date" not in df.columns:
            df["game_date"] = ""
        dedupe_key = (
            df["game_id"].fillna("").astype(str).str.lower().str.strip().replace("", pd.NA)
            .fillna(
                df["sport"].fillna("").astype(str).str.lower().str.strip()
                + "|"
                + df["game_date"].fillna("").astype(str).str[:10]
                + "|"
                + df["away_team"].fillna("").astype(str).str.lower().str.strip()
                + "|"
                + df["home_team"].fillna("").astype(str).str.lower().str.strip()
            )
        )
        df = df.assign(_dedupe_key=dedupe_key).drop_duplicates(subset=["_dedupe_key"], keep="last").drop(columns=["_dedupe_key"])
        df["season"] = pd.to_numeric(df.get("season"), errors="coerce").fillna(0).astype(int)
        if "game_date" in df.columns:
            gd = pd.to_datetime(df["game_date"], errors="coerce")
            df["month"] = gd.dt.month.fillna(6).astype(int)
            df["day_of_week"] = gd.dt.dayofweek.fillna(0).astype(int)
        else:
            df["month"] = 6
            df["day_of_week"] = 0
        df = self._add_form_features(df)
        df = self._add_news_features(df)
        return df

    def _add_form_features(self, df):
        """Add leakage-free team-form features: recent win rate, rest days, head-to-head.

        Each row only uses information available strictly BEFORE that game (games
        are processed in chronological order and every stat is shifted by one), so
        training never sees the future.
        """
        import numpy as np
        import pandas as pd

        df = df.copy()
        df["_game_date_dt"] = pd.to_datetime(df["game_date"], errors="coerce")
        df["_orig_order"] = range(len(df))
        df = df.sort_values("_game_date_dt", kind="mergesort").reset_index(drop=True)

        wins = (df["home_score"] > df["away_score"]).astype(int)

        home_long = pd.DataFrame({
            "orig_index": df.index, "sport": df["sport"], "team": df["home_team"],
            "game_date_dt": df["_game_date_dt"], "is_home": True, "win": wins,
        })
        away_long = pd.DataFrame({
            "orig_index": df.index, "sport": df["sport"], "team": df["away_team"],
            "game_date_dt": df["_game_date_dt"], "is_home": False, "win": 1 - wins,
        })
        long_df = pd.concat([home_long, away_long], ignore_index=True)
        long_df = long_df.sort_values(["sport", "team", "game_date_dt"], kind="mergesort")

        grp = long_df.groupby(["sport", "team"], sort=False)
        long_df["recent_win_rate"] = grp["win"].transform(
            lambda s: s.shift(1).rolling(window=10, min_periods=1).mean()
        )
        long_df["rest_days"] = grp["game_date_dt"].transform(lambda s: s.diff().dt.days)

        long_df["recent_win_rate"] = long_df["recent_win_rate"].fillna(0.5)
        long_df["rest_days"] = long_df["rest_days"].fillna(3.0).clip(lower=0, upper=30)

        home_feats = long_df[long_df["is_home"]].set_index("orig_index")
        away_feats = long_df[~long_df["is_home"]].set_index("orig_index")
        df["home_recent_win_rate"] = home_feats["recent_win_rate"].reindex(df.index).fillna(0.5)
        df["away_recent_win_rate"] = away_feats["recent_win_rate"].reindex(df.index).fillna(0.5)
        df["home_rest_days"] = home_feats["rest_days"].reindex(df.index).fillna(3.0)
        df["away_rest_days"] = away_feats["rest_days"].reindex(df.index).fillna(3.0)

        # Head-to-head: home team's historical win rate in this matchup, prior meetings only.
        h2h_win: list[float] = []
        pair_history: dict[str, list[str]] = {}
        for _, row in df.iterrows():
            home_l = str(row["home_team"]).strip().lower()
            away_l = str(row["away_team"]).strip().lower()
            key = f"{row['sport']}|" + "|".join(sorted([home_l, away_l]))
            history = pair_history.get(key, [])
            if history:
                h2h_win.append(sum(1 for w in history if w == home_l) / len(history))
            else:
                h2h_win.append(0.5)
            winner = home_l if row["home_score"] > row["away_score"] else away_l
            history.append(winner)
            pair_history[key] = history
        df["h2h_home_win_rate"] = h2h_win

        df = df.sort_values("_orig_order", kind="mergesort").reset_index(drop=True)
        df = df.drop(columns=["_game_date_dt", "_orig_order"], errors="ignore")
        return df

    def _add_news_features(self, df):
        """Add leakage-free news-sentiment features per team: mean sentiment and a
        negative-signal flag over the _NEWS_LOOKBACK_DAYS strictly before each
        game's date. Missing/unavailable news data defaults to neutral (0.0),
        never blocks training.
        """
        import pandas as pd

        for col in self._NEWS_FEATURES:
            df[col] = 0.0
        try:
            news_df = self._load_news_signals_dataframe_from_hub()
        except Exception as exc:
            logger.debug("[hf_pipeline] news feature join skipped: %s", exc)
            return df
        if news_df is None or news_df.empty:
            return df

        news_df = news_df.copy()
        news_df["_news_date"] = pd.to_datetime(news_df.get("game_date"), errors="coerce")
        news_df["_entity"] = news_df.get("entity_team").fillna("").astype(str).str.strip().str.lower()
        news_df["_sport"] = news_df.get("sport").fillna("").astype(str).str.strip().str.lower().map(self._normalize_sport)
        news_df["_sentiment"] = pd.to_numeric(news_df.get("sentiment_score"), errors="coerce").fillna(0.0)
        news_df["_negative"] = news_df.get("impact_type").isin(self._NEWS_NEGATIVE_IMPACT_TYPES).astype(float)
        news_df = news_df.dropna(subset=["_news_date"])
        news_df = news_df[news_df["_entity"] != ""]
        if news_df.empty:
            return df

        news_by_key: dict[tuple[str, str], "pd.DataFrame"] = {
            key: sub.sort_values("_news_date")
            for key, sub in news_df.groupby(["_sport", "_entity"])
        }

        def _lookup(team: str, sport: str, game_date) -> tuple[float, float]:
            if pd.isna(game_date):
                return 0.0, 0.0
            key = (str(sport).strip().lower(), str(team).strip().lower())
            sub = news_by_key.get(key)
            if sub is None or sub.empty:
                return 0.0, 0.0
            window_start = game_date - pd.Timedelta(days=self._NEWS_LOOKBACK_DAYS)
            windowed = sub[(sub["_news_date"] >= window_start) & (sub["_news_date"] < game_date)]
            if windowed.empty:
                return 0.0, 0.0
            return float(windowed["_sentiment"].mean()), float(windowed["_negative"].max())

        game_dates = pd.to_datetime(df["game_date"], errors="coerce")
        home_sent: list[float] = []
        away_sent: list[float] = []
        home_neg: list[float] = []
        away_neg: list[float] = []
        for i, row in df.iterrows():
            gdate = game_dates.loc[i]
            hs, hn = _lookup(row["home_team"], row["sport"], gdate)
            as_, an = _lookup(row["away_team"], row["sport"], gdate)
            home_sent.append(hs)
            away_sent.append(as_)
            home_neg.append(hn)
            away_neg.append(an)

        df["home_news_sentiment"] = home_sent
        df["away_news_sentiment"] = away_sent
        df["home_negative_news_flag"] = home_neg
        df["away_negative_news_flag"] = away_neg
        return df

    def _build_form_snapshot(self) -> tuple[dict, dict]:
        """Compute current (as-of-today) team form + head-to-head lookups for prediction time.

        Returns (team_stats, h2h_stats):
          team_stats[(sport, team_lower)] = {"recent_win_rate": float, "rest_days": float}
          h2h_stats["sport|team_a|team_b" (sorted)] = home-team-name win rate in that pairing
        """
        team_stats: dict[tuple[str, str], dict[str, float]] = {}
        h2h_stats: dict[str, float] = {}
        try:
            df = self._load_games_dataframe_from_hub()
            if df is None or df.empty:
                return team_stats, h2h_stats
            import pandas as pd

            for col in ("home_score", "away_score"):
                df[col] = pd.to_numeric(df[col], errors="coerce")
            df = df.dropna(subset=["home_team", "away_team", "home_score", "away_score"])
            df = df[df["home_score"] != df["away_score"]].copy()
            if "sport" not in df.columns:
                df["sport"] = "mlb"
            df["sport"] = df["sport"].fillna("mlb").astype(str).map(self._normalize_sport)
            df["_game_date_dt"] = pd.to_datetime(df.get("game_date"), errors="coerce")
            df = df.dropna(subset=["_game_date_dt"]).sort_values("_game_date_dt", kind="mergesort")
            if df.empty:
                return team_stats, h2h_stats

            today_ts = pd.Timestamp(et_today())
            wins = (df["home_score"] > df["away_score"]).astype(int)
            home_long = pd.DataFrame({
                "sport": df["sport"], "team": df["home_team"],
                "game_date_dt": df["_game_date_dt"], "win": wins,
            })
            away_long = pd.DataFrame({
                "sport": df["sport"], "team": df["away_team"],
                "game_date_dt": df["_game_date_dt"], "win": 1 - wins,
            })
            long_df = pd.concat([home_long, away_long], ignore_index=True)
            long_df["team_key"] = long_df["team"].astype(str).str.strip().str.lower()
            long_df = long_df.sort_values(["sport", "team_key", "game_date_dt"], kind="mergesort")
            for (sport, team_key), g in long_df.groupby(["sport", "team_key"], sort=False):
                recent = g["win"].tail(10).mean()
                last_date = g["game_date_dt"].iloc[-1]
                rest = float((today_ts - last_date).days)
                rest = max(0.0, min(30.0, rest))
                team_stats[(sport, team_key)] = {
                    "recent_win_rate": float(recent) if pd.notna(recent) else 0.5,
                    "rest_days": rest,
                }

            for sport, g in df.groupby("sport", sort=False):
                pair_win: dict[str, list[str]] = {}
                for _, row in g.iterrows():
                    home_l = str(row["home_team"]).strip().lower()
                    away_l = str(row["away_team"]).strip().lower()
                    key = f"{sport}|" + "|".join(sorted([home_l, away_l]))
                    winner = home_l if row["home_score"] > row["away_score"] else away_l
                    pair_win.setdefault(key, []).append(winner)
                for key, winners in pair_win.items():
                    parts = key.split("|")
                    team_a = parts[1]
                    h2h_stats[key] = sum(1 for w in winners if w == team_a) / len(winners)
        except Exception as exc:
            logger.debug("[hf_pipeline] form snapshot build skipped: %s", exc)
        return team_stats, h2h_stats

    def _form_features_for_matchup(
        self,
        home_team: str,
        away_team: str,
        sport: str,
        team_stats: dict | None,
        h2h_stats: dict | None,
    ) -> dict:
        sport_key = self._normalize_sport(str(sport or "mlb"))
        home_key = str(home_team or "").strip().lower()
        away_key = str(away_team or "").strip().lower()
        home_s = (team_stats or {}).get((sport_key, home_key)) or {}
        away_s = (team_stats or {}).get((sport_key, away_key)) or {}
        pair_key = f"{sport_key}|" + "|".join(sorted([home_key, away_key]))
        h2h_raw = (h2h_stats or {}).get(pair_key)
        if h2h_raw is None:
            h2h_home = 0.5
        else:
            # h2h_stats is keyed by the sorted pair; re-orient to home_team's perspective.
            sorted_first = sorted([home_key, away_key])[0]
            h2h_home = h2h_raw if sorted_first == home_key else (1.0 - h2h_raw)
        return {
            "home_recent_win_rate": float(home_s.get("recent_win_rate", 0.5)),
            "away_recent_win_rate": float(away_s.get("recent_win_rate", 0.5)),
            "home_rest_days": float(home_s.get("rest_days", 3.0)),
            "away_rest_days": float(away_s.get("rest_days", 3.0)),
            "h2h_home_win_rate": float(h2h_home),
        }

    def _build_news_snapshot(self) -> dict:
        """As-of-now news lookup for prediction time, mirroring _add_news_features'
        lookback window so training and inference see the same feature semantics.
        Returns {(sport, team_lower): {"sentiment": float, "negative_flag": float}}.
        """
        news_stats: dict[tuple[str, str], dict[str, float]] = {}
        try:
            news_df = self._load_news_signals_dataframe_from_hub()
            if news_df is None or news_df.empty:
                return news_stats
            import pandas as pd

            news_df = news_df.copy()
            news_df["_news_date"] = pd.to_datetime(news_df.get("game_date"), errors="coerce")
            news_df["_entity"] = news_df.get("entity_team").fillna("").astype(str).str.strip().str.lower()
            news_df["_sport"] = news_df.get("sport").fillna("").astype(str).str.strip().str.lower().map(self._normalize_sport)
            news_df["_sentiment"] = pd.to_numeric(news_df.get("sentiment_score"), errors="coerce").fillna(0.0)
            news_df["_negative"] = news_df.get("impact_type").isin(self._NEWS_NEGATIVE_IMPACT_TYPES).astype(float)
            news_df = news_df.dropna(subset=["_news_date"])
            news_df = news_df[news_df["_entity"] != ""]
            if news_df.empty:
                return news_stats

            today_ts = pd.Timestamp(et_today())
            window_start = today_ts - pd.Timedelta(days=self._NEWS_LOOKBACK_DAYS)
            recent = news_df[(news_df["_news_date"] >= window_start) & (news_df["_news_date"] <= today_ts)]
            for (sport, entity), sub in recent.groupby(["_sport", "_entity"]):
                news_stats[(sport, entity)] = {
                    "sentiment": float(sub["_sentiment"].mean()),
                    "negative_flag": float(sub["_negative"].max()),
                }
        except Exception as exc:
            logger.debug("[hf_pipeline] news snapshot build skipped: %s", exc)
        return news_stats

    def _news_features_for_matchup(
        self, home_team: str, away_team: str, sport: str, news_stats: dict | None
    ) -> dict:
        sport_key = self._normalize_sport(str(sport or "mlb"))
        home_key = str(home_team or "").strip().lower()
        away_key = str(away_team or "").strip().lower()
        home_s = (news_stats or {}).get((sport_key, home_key)) or {}
        away_s = (news_stats or {}).get((sport_key, away_key)) or {}
        return {
            "home_news_sentiment": float(home_s.get("sentiment", 0.0)),
            "away_news_sentiment": float(away_s.get("sentiment", 0.0)),
            "home_negative_news_flag": float(home_s.get("negative_flag", 0.0)),
            "away_negative_news_flag": float(away_s.get("negative_flag", 0.0)),
        }

    def _apply_news_adjustment(
        self, home_prob: float, away_prob: float, home_flagged: bool, away_flagged: bool
    ) -> tuple[float, float]:
        """Nudge win probability toward the team WITHOUT a same-day negative signal.

        Bounded (+/-0.04) and cancels out when both sides are flagged, so it can
        never flip a strong pick on its own -- it only sharpens close games using
        same-day injury/lineup/suspension news that the base model can't see.
        """
        if home_flagged == away_flagged:
            return home_prob, away_prob
        delta = self._NEWS_ADJUSTMENT if away_flagged else -self._NEWS_ADJUSTMENT
        adjusted_home = min(0.95, max(0.05, home_prob + delta))
        return adjusted_home, round(1.0 - adjusted_home, 4)

    # ──────────────────────────────────────────────────────────
    # Today/tomorrow snapshot persistence + day-over-day comparison
    # ──────────────────────────────────────────────────────────

    def _snapshot_dir(self) -> str:
        d = os.path.join(self._data_dir, "prediction_snapshots")
        os.makedirs(d, exist_ok=True)
        return d

    def _drift_dir(self) -> str:
        d = os.path.join(self._data_dir, "prediction_drift")
        os.makedirs(d, exist_ok=True)
        return d

    def _save_snapshot(self, target_date: datetime.date, rows: list[dict]) -> None:
        path = os.path.join(self._snapshot_dir(), f"{target_date.isoformat()}.json")
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump({"date": target_date.isoformat(), "saved_at": _now_utc(), "predictions": rows}, f)
        except Exception as exc:
            logger.debug("[hf_pipeline] snapshot save skipped for %s: %s", target_date, exc)

    def _load_snapshot(self, target_date: datetime.date) -> dict | None:
        path = os.path.join(self._snapshot_dir(), f"{target_date.isoformat()}.json")
        if not os.path.exists(path):
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None

    def _delete_snapshot(self, target_date: datetime.date) -> None:
        path = os.path.join(self._snapshot_dir(), f"{target_date.isoformat()}.json")
        try:
            if os.path.exists(path):
                os.remove(path)
        except Exception:
            pass

    @staticmethod
    def _row_identity(row: dict) -> str:
        return "|".join([
            str(row.get("sport") or "").strip().lower(),
            str(row.get("home_team") or "").strip().lower(),
            str(row.get("away_team") or "").strip().lower(),
            str(row.get("market_type") or "").strip().lower(),
            str(row.get("player_name") or "").strip().lower(),
            str(row.get("prop_line") or ""),
        ])

    def _compare_with_snapshot(self, target_date: datetime.date, fresh_rows: list[dict]) -> dict | None:
        """Compare today's freshly (in-depth) generated rows against the
        preliminary 'tomorrow' snapshot saved for this same date yesterday."""
        snapshot = self._load_snapshot(target_date)
        if not snapshot:
            return None
        prev_rows = {
            self._row_identity(r): r for r in (snapshot.get("predictions") or []) if isinstance(r, dict)
        }
        if not prev_rows:
            return None
        diffs: list[dict] = []
        flips = 0
        total_abs_delta = 0.0
        matched = 0
        for row in fresh_rows:
            prev = prev_rows.get(self._row_identity(row))
            if not prev:
                continue
            matched += 1
            prev_prob = float(prev.get("home_win_prob") if prev.get("home_win_prob") is not None else prev.get("confidence") or 0.0)
            new_prob = float(row.get("home_win_prob") if row.get("home_win_prob") is not None else row.get("confidence") or 0.0)
            delta = round(new_prob - prev_prob, 4)
            total_abs_delta += abs(delta)
            prev_pick = str(prev.get("predicted_team") or "")
            new_pick = str(row.get("predicted_team") or "")
            flipped = bool(prev_pick and new_pick and prev_pick != new_pick)
            if flipped:
                flips += 1
            diffs.append({
                "game": f"{row.get('away_team')} @ {row.get('home_team')}",
                "market_type": row.get("market_type"),
                "player_name": row.get("player_name") or None,
                "prior_prob": round(prev_prob, 4),
                "fresh_prob": round(new_prob, 4),
                "prob_delta": delta,
                "pick_flipped": flipped,
            })
        return {
            "date": target_date.isoformat(),
            "compared_at": _now_utc(),
            "prior_snapshot_saved_at": snapshot.get("saved_at"),
            "matched_count": matched,
            "pick_flips": flips,
            "avg_abs_prob_delta": round(total_abs_delta / matched, 4) if matched else 0.0,
            "details": diffs[:200],
        }

    def _cleanup_stale_snapshots(self, today: datetime.date) -> None:
        """Delete any snapshot/drift artifact dated before today -- the day
        after a date passes, its snapshot has either already been consumed by
        `_compare_with_snapshot` + `_delete_snapshot`, or was never matched
        (e.g. a skipped run) and is now stale and safe to discard."""
        for d in (self._snapshot_dir(), self._drift_dir()):
            try:
                for fname in os.listdir(d):
                    if not fname.endswith(".json"):
                        continue
                    try:
                        file_date = datetime.date.fromisoformat(fname[:-5])
                    except ValueError:
                        continue
                    if file_date < today:
                        try:
                            os.remove(os.path.join(d, fname))
                        except OSError:
                            pass
            except FileNotFoundError:
                continue

    def _filter_records_not_in_hub(self, records: list[dict]) -> list[dict]:
        if not records:
            return []
        if not self._ok or not self._api:
            return records
        try:
            existing_df = self._load_games_dataframe_from_hub()
        except Exception:
            return records
        if existing_df is None or getattr(existing_df, "empty", True):
            return records

        existing_records = existing_df.to_dict("records")
        existing_keys = {self._record_dedupe_key(r) for r in existing_records}
        filtered = [r for r in records if self._record_dedupe_key(r) not in existing_keys]
        dropped = len(records) - len(filtered)
        if dropped > 0:
            logger.info("[hf_pipeline] Filtered %d duplicate records before HF upload", dropped)
        return filtered

    def _load_games_dataframe_from_hub(self):
        import pandas as pd
        from huggingface_hub import hf_hub_download

        if self._games_df_cache is not None:
            cached_ts, cached_df = self._games_df_cache
            if (time.time() - cached_ts) < self._df_cache_ttl_sec:
                return cached_df.copy()

        if not self._ok or not self._api:
            return pd.DataFrame()

        try:
            files = self._api.list_repo_files(repo_id=self.dataset_repo_id, repo_type="dataset")
        except Exception as exc:
            logger.warning("[hf_pipeline] list_repo_files failed: %s", exc)
            return pd.DataFrame()

        game_files = [f for f in files if str(f).startswith("data/games/") and str(f).endswith(".parquet")]
        if not game_files:
            return pd.DataFrame()

        frames = []
        for path_in_repo in game_files:
            try:
                local_path = hf_hub_download(
                    repo_id=self.dataset_repo_id,
                    repo_type="dataset",
                    filename=path_in_repo,
                    token=self.token,
                )
                frames.append(pd.read_parquet(local_path))
            except Exception as exc:
                logger.debug("[hf_pipeline] parquet read failed for %s: %s", path_in_repo, exc)
                continue
        if not frames:
            return pd.DataFrame()
        df = pd.concat(frames, ignore_index=True, sort=False)
        self._games_df_cache = (time.time(), df)
        return df.copy()

    def _write_status(self, patch: dict) -> None:
        os.makedirs(os.path.dirname(self._status_file), exist_ok=True)
        base: dict = {}
        try:
            if os.path.exists(self._status_file):
                with open(self._status_file, "r", encoding="utf-8") as f:
                    base = json.load(f) or {}
        except Exception:
            base = {}
        base.update(patch or {})
        base["updated_at"] = _now_utc()
        base["dataset_repo"] = self.dataset_repo_id
        base["model_repo"] = self.model_repo_id
        with open(self._status_file, "w", encoding="utf-8") as f:
            json.dump(base, f, indent=2)

    def _append_training_history(self, summary: TrainSummary) -> None:
        os.makedirs(os.path.dirname(self._training_history_file), exist_ok=True)
        history: list[dict] = []
        try:
            if os.path.exists(self._training_history_file):
                with open(self._training_history_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        history = data
        except Exception:
            history = []
        history.append({
            "version": summary.version,
            "trained_at": summary.trained_at,
            "rows": summary.rows,
            "best_model": summary.best_model,
            "cv_roc_auc": summary.cv_roc_auc,
            "sports_covered": summary.sports_covered,
            "repo_id": summary.repo_id,
        })
        history = history[-100:]
        with open(self._training_history_file, "w", encoding="utf-8") as f:
            json.dump(history, f, indent=2)
        if self._ok and self._api:
            try:
                buf = io.BytesIO(json.dumps(history, indent=2).encode("utf-8"))
                self._api.upload_file(
                    path_or_fileobj=buf, path_in_repo="training_history.json",
                    repo_id=self.model_repo_id, repo_type="model",
                    commit_message=f"v{summary.version}: update training history",
                )
            except Exception as exc:
                logger.debug("[hf_pipeline] training_history push: %s", exc)

    def publish_runtime_artifacts(self) -> dict:
        """Publish latest runtime JSON artifacts to model repo for dashboard fallback."""
        if not self._ok or not self._api:
            return {"ok": False, "uploaded": 0, "reason": "hf_not_configured"}

        artifacts = {
            "artifacts/hf_pipeline_status.json": self._status_file,
            "artifacts/hf_daily_predictions.json": self._predictions_file,
            "artifacts/training_history.json": self._training_history_file,
            "artifacts/hf_daily_prediction_markets.json": os.path.join(self._data_dir, "hf_daily_prediction_markets.json"),
            "artifacts/hf_daily_prediction_combos.json": os.path.join(self._data_dir, "hf_daily_prediction_combos.json"),
        }
        uploaded = 0
        for repo_path, local_path in artifacts.items():
            if not os.path.exists(local_path):
                continue
            try:
                with open(local_path, "rb") as f:
                    self._api.upload_file(
                        path_or_fileobj=f.read(),
                        path_in_repo=repo_path,
                        repo_id=self.model_repo_id,
                        repo_type="model",
                        commit_message=f"Update {repo_path}",
                    )
                uploaded += 1
            except Exception as exc:
                logger.debug("[hf_pipeline] publish artifact failed %s: %s", repo_path, exc)
        return {"ok": True, "uploaded": uploaded}

    def _get_model_metadata(self) -> dict:
        if not self._ok or not self._api:
            return {}
        try:
            from huggingface_hub import hf_hub_download
            path = hf_hub_download(
                repo_id=self.model_repo_id, filename="metadata.json",
                repo_type="model", token=self.token,
            )
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f) or {}
        except Exception:
            return {}
