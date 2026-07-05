"""
Hugging Face Dataset Uploader
==============================
Pushes all sports data (games, odds, injuries, predictions) to a HF dataset
repo as streamable Parquet shards.  Resolves the HF username from the token
automatically so no hard-coded org is needed.

Usage
-----
    from src.data.hf_uploader import HFUploader
    up = HFUploader()

    # Push a batch of game records
    up.push_records("games", [{"sport": "mlb", "home_team": "NYY", ...}])

    # Backfill everything from PostgreSQL
    up.sync_from_db()

Dataset layout on HF Hub
-------------------------
    {username}/sports-dataset
    ├── README.md  (dataset card with config definitions)
    └── data/
        ├── games/       shard_YYYYMMDD_HHMMSS.parquet  ...
        ├── odds/        shard_...parquet
        ├── injuries/    shard_...parquet
        └── predictions/ shard_...parquet

Stream any subset from Python:
    from datasets import load_dataset
    ds = load_dataset("{username}/sports-dataset", "games", streaming=True)
"""

import io
import os
import sys
import datetime
import logging

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Lazy imports so the rest of the bot still works when HF libs are absent
# ---------------------------------------------------------------------------
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    _PA_OK = True
except ImportError:
    _PA_OK = False

try:
    from huggingface_hub import HfApi
    _HF_OK = True
except ImportError:
    _HF_OK = False


# ---------------------------------------------------------------------------
# Schema definitions — each subset has a fixed Arrow schema.
# Sport-specific extras go into the `metadata` JSON string column.
# ---------------------------------------------------------------------------

SCHEMAS: dict[str, "pa.Schema"] = {}

def _build_schemas():
    if not _PA_OK:
        return
    SCHEMAS["games"] = pa.schema([
        pa.field("game_id",          pa.string()),
        pa.field("record_id",        pa.string()),
        pa.field("sport",            pa.string()),
        pa.field("league",           pa.string()),
        pa.field("game_date",        pa.string()),
        pa.field("game_datetime",    pa.string()),
        pa.field("status",           pa.string()),
        pa.field("home_team",        pa.string()),
        pa.field("away_team",        pa.string()),
        pa.field("home_score",       pa.float32()),
        pa.field("away_score",       pa.float32()),
        pa.field("home_starter",     pa.string()),
        pa.field("away_starter",     pa.string()),
        pa.field("season",           pa.int32()),
        pa.field("metadata",         pa.string()),   # JSON
        pa.field("created_at",       pa.string()),
    ])
    SCHEMAS["odds"] = pa.schema([
        pa.field("sport",       pa.string()),
        pa.field("home_team",   pa.string()),
        pa.field("away_team",   pa.string()),
        pa.field("game_date",   pa.string()),
        pa.field("market",      pa.string()),
        pa.field("outcome",     pa.string()),
        pa.field("odds_am",     pa.int32()),
        pa.field("dec_odds",    pa.float64()),
        pa.field("total_line",  pa.float32()),
        pa.field("bookmaker",   pa.string()),
        pa.field("fetched_at",  pa.string()),
    ])
    SCHEMAS["injuries"] = pa.schema([
        pa.field("sport",        pa.string()),
        pa.field("team",         pa.string()),
        pa.field("player_name",  pa.string()),
        pa.field("status",       pa.string()),
        pa.field("description",  pa.string()),
        pa.field("injury_type",  pa.string()),
        pa.field("source",       pa.string()),
        pa.field("fetched_at",   pa.string()),
    ])
    SCHEMAS["predictions"] = pa.schema([
        pa.field("prediction_uid",    pa.string()),
        pa.field("sport",             pa.string()),
        pa.field("matchup",           pa.string()),
        pa.field("game_date",         pa.string()),
        pa.field("bet_type",          pa.string()),
        pa.field("bet",               pa.string()),
        pa.field("model_prob",        pa.float64()),
        pa.field("book_prob",         pa.float64()),
        pa.field("edge",              pa.float64()),
        pa.field("odds_am",           pa.int32()),
        pa.field("dec_odds",          pa.float64()),
        pa.field("stake_usd",         pa.float64()),
        pa.field("ev",                pa.float64()),
        pa.field("signal_boost",      pa.float64()),
        pa.field("signal_sources",    pa.string()),
        pa.field("detected_at",       pa.string()),
    ])
    SCHEMAS["news_signals"] = pa.schema([
        pa.field("news_id",           pa.string()),
        pa.field("sport",             pa.string()),
        pa.field("league",            pa.string()),
        pa.field("game_id",           pa.string()),
        pa.field("game_date",         pa.string()),
        pa.field("game_time",         pa.string()),
        pa.field("home_team",         pa.string()),
        pa.field("away_team",         pa.string()),
        pa.field("entity_type",       pa.string()),
        pa.field("entity_name",       pa.string()),
        pa.field("entity_team",       pa.string()),
        pa.field("impact_type",       pa.string()),
        pa.field("impact_scope",      pa.string()),
        pa.field("headline",          pa.string()),
        pa.field("description",       pa.string()),
        pa.field("article_url",       pa.string()),
        pa.field("source_name",       pa.string()),
        pa.field("published_at",      pa.string()),
        pa.field("sentiment_score",   pa.float32()),
        pa.field("impact_score",      pa.float32()),
        pa.field("metadata",          pa.string()),
        pa.field("created_at",        pa.string()),
    ])
    SCHEMAS["pregame_schedule"] = pa.schema([
        pa.field("schedule_uid",      pa.string()),
        pa.field("game_key",          pa.string()),
        pa.field("sport",             pa.string()),
        pa.field("league",            pa.string()),
        pa.field("home_team",         pa.string()),
        pa.field("away_team",         pa.string()),
        pa.field("game_date",         pa.string()),
        pa.field("game_time",         pa.string()),
        pa.field("scheduled_start",   pa.string()),
        pa.field("analysis_at",       pa.string()),
        pa.field("bet_at",            pa.string()),
        pa.field("confidence",        pa.float64()),
        pa.field("confidence_tier",   pa.string()),
        pa.field("model_version",     pa.string()),
        pa.field("model_type",        pa.string()),
        pa.field("prediction_count",  pa.int32()),
        pa.field("predictions_json",  pa.string()),
        pa.field("analysis_state",    pa.string()),
        pa.field("bet_state",         pa.string()),
        pa.field("analysis_payload",  pa.string()),
        pa.field("bet_payload",       pa.string()),
        pa.field("last_analysis_at",  pa.string()),
        pa.field("last_bet_at",       pa.string()),
        pa.field("source",            pa.string()),
        pa.field("created_at",        pa.string()),
    ])

_build_schemas()


# ---------------------------------------------------------------------------
# Dataset card README template
# ---------------------------------------------------------------------------

_DATASET_CARD = """\
---
license: other
task_categories:
- tabular-classification
tags:
- sports
- betting
- mlb
- soccer
- nba
- nfl
- nhl
- tennis
- golf
configs:
- config_name: games
  data_files: data/games/*.parquet
- config_name: odds
  data_files: data/odds/*.parquet
- config_name: injuries
  data_files: data/injuries/*.parquet
- config_name: predictions
  data_files: data/predictions/*.parquet
- config_name: news_signals
  data_files: data/news_signals/*.parquet
- config_name: pregame_schedule
  data_files: data/pregame_schedule/*.parquet
---

# Sports Dataset

Multi-sport game, odds, injury, prediction, and news-impact data generated by the bettor bot.
Covers: MLB, Soccer (PL/MLS/CL/WC2026), NBA, NFL, NHL, WNBA, Tennis, Golf, Boxing, MMA, Cricket.

## Subsets

| Config | Description |
|---|---|
| `games` | Scheduled & completed games across all sports |
| `odds` | Historical odds snapshots per bookmaker |
| `injuries` | Player injury reports |
| `predictions` | Model predictions and value-bet signals |
| `news_signals` | Structured player/team/game news impact rows |

## Streaming

```python
from datasets import load_dataset
ds = load_dataset("{repo_id}", "games", streaming=True)
for row in ds["train"]:
    print(row)
```
"""


# ---------------------------------------------------------------------------
# Main uploader class
# ---------------------------------------------------------------------------

class HFUploader:
    """Push sports data records to a HuggingFace dataset repo as Parquet."""

    VALID_SUBSETS = {"games", "odds", "injuries", "predictions", "news_signals", "pregame_schedule"}
    # Flush buffer to HF when it reaches this many records
    FLUSH_THRESHOLD = 500

    def __init__(self, token: str | None = None, repo_name: str | None = None):
        if not _HF_OK or not _PA_OK:
            logger.warning("[hf_uploader] huggingface_hub or pyarrow not installed — skipping")
            self._ok = False
            return

        try:
            from src.config import HF_API_KEY, HF_DATASET_REPO
        except ImportError:
            HF_API_KEY = os.getenv("HF_API_KEY", "")
            HF_DATASET_REPO = os.getenv("HF_DATASET_REPO", "papylove/sportprediction")

        self._token = token or HF_API_KEY
        _repo_name = repo_name or HF_DATASET_REPO

        if not self._token:
            logger.warning("[hf_uploader] HF_API_KEY not set — skipping")
            self._ok = False
            return

        self._api = HfApi(token=self._token)
        self._username = self._resolve_username()
        if not self._username:
            self._ok = False
            return

        repo_raw = str(_repo_name or "").strip().strip("/")
        if "/" in repo_raw:
            owner, name = (repo_raw.split("/", 1) + [""])[:2]
            owner = owner.strip()
            name = name.strip()
            if not owner or not name:
                self._repo_id = f"{self._username}/{name or owner}"
            else:
                self._repo_id = f"{owner}/{name}"
        else:
            self._repo_id = f"{self._username}/{repo_raw}"
        self._buffers: dict[str, list[dict]] = {s: [] for s in self.VALID_SUBSETS}
        self._ok = True

        self._ensure_repo()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def push_records(self, subset: str, records: list[dict]) -> bool:
        """Buffer records and flush to HF when threshold is reached."""
        if not self._ok or not records:
            return False
        if subset not in self.VALID_SUBSETS:
            logger.warning("[hf_uploader] unknown subset: %s", subset)
            return False

        self._buffers[subset].extend(records)
        if len(self._buffers[subset]) >= self.FLUSH_THRESHOLD:
            return self._flush(subset)
        return True

    def flush_all(self) -> bool:
        """Force-flush all pending buffers to HF."""
        if not self._ok:
            return False
        ok = True
        for subset in self.VALID_SUBSETS:
            if self._buffers[subset]:
                ok = self._flush(subset) and ok
        return ok

    def sync_from_db(self) -> bool:
        """Pull all data from PostgreSQL and push to HF Hub."""
        if not self._ok:
            return False
        try:
            from src.data.db import get_conn
        except ImportError:
            logger.error("[hf_uploader] cannot import db module")
            return False

        conn = get_conn()
        if not conn:
            logger.warning("[hf_uploader] no DB connection — skipping sync")
            return False

        ok = True
        try:
            ok = self._sync_games(conn) and ok
            ok = self._sync_odds(conn) and ok
            ok = self._sync_injuries(conn) and ok
            ok = self._sync_predictions(conn) and ok
            self.flush_all()
        finally:
            conn.close()
        return ok

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _resolve_username(self) -> str:
        try:
            info = self._api.whoami()
            return info.get("name", "")
        except Exception as e:
            logger.error("[hf_uploader] cannot resolve HF username: %s", e)
            return ""

    def _ensure_repo(self):
        try:
            self._api.repo_info(repo_id=self._repo_id, repo_type="dataset")
            logger.info("[hf_uploader] repo exists: %s", self._repo_id)
        except Exception:
            logger.info("[hf_uploader] creating dataset repo: %s", self._repo_id)
            try:
                self._api.create_repo(
                    repo_id=self._repo_id,
                    repo_type="dataset",
                    private=False,
                    exist_ok=True,
                )
                self._push_dataset_card()
            except Exception as e:
                logger.error("[hf_uploader] failed to create repo: %s", e)
                self._ok = False

    def _push_dataset_card(self):
        card_content = _DATASET_CARD.replace("{repo_id}", self._repo_id)
        buf = io.BytesIO(card_content.encode("utf-8"))
        self._api.upload_file(
            path_or_fileobj=buf,
            path_in_repo="README.md",
            repo_id=self._repo_id,
            repo_type="dataset",
            commit_message="Add dataset card",
        )

    def _flush(self, subset: str) -> bool:
        records = self._buffers[subset]
        if not records:
            return True
        try:
            parquet_bytes = self._records_to_parquet(subset, records)
            shard_name = self._shard_filename()
            path_in_repo = f"data/{subset}/{shard_name}"
            buf = io.BytesIO(parquet_bytes)
            self._api.upload_file(
                path_or_fileobj=buf,
                path_in_repo=path_in_repo,
                repo_id=self._repo_id,
                repo_type="dataset",
                commit_message=f"Add {subset} shard ({len(records)} records)",
            )
            logger.info("[hf_uploader] flushed %d %s records → %s", len(records), subset, path_in_repo)
            self._buffers[subset] = []
            return True
        except Exception as e:
            logger.error("[hf_uploader] flush failed for %s: %s", subset, e)
            return False

    def _records_to_parquet(self, subset: str, records: list[dict]) -> bytes:
        schema = SCHEMAS.get(subset)
        if schema is None:
            raise ValueError(f"No schema defined for subset: {subset}")

        # Build column arrays, coercing types and filling missing fields
        col_data: dict[str, list] = {field.name: [] for field in schema}
        for rec in records:
            for field in schema:
                val = rec.get(field.name)
                col_data[field.name].append(_coerce(val, field.type))

        arrays = [pa.array(col_data[f.name], type=f.type) for f in schema]
        table = pa.table(
            {f.name: arr for f, arr in zip(schema, arrays)},
            schema=schema,
        )
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        return buf.getvalue()

    @staticmethod
    def _shard_filename() -> str:
        ts = datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        return f"shard_{ts}.parquet"

    # ------------------------------------------------------------------
    # DB sync helpers
    # ------------------------------------------------------------------

    def _sync_games(self, conn) -> bool:
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT sport, league, home_team, away_team, game_date, game_datetime,
                       status, home_score, away_score, home_starter, away_starter,
                       season, external_id, created_at
                FROM games ORDER BY game_date
            """)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
            records = []
            for row in rows:
                r = dict(zip(cols, row))
                records.append({
                    "game_id":       str(r.get("external_id") or ""),
                    "sport":         str(r.get("sport") or ""),
                    "league":        str(r.get("league") or ""),
                    "game_date":     _date_str(r.get("game_date")),
                    "game_datetime": _date_str(r.get("game_datetime")),
                    "status":        str(r.get("status") or ""),
                    "home_team":     str(r.get("home_team") or ""),
                    "away_team":     str(r.get("away_team") or ""),
                    "home_score":    _float_or_none(r.get("home_score")),
                    "away_score":    _float_or_none(r.get("away_score")),
                    "home_starter":  str(r.get("home_starter") or ""),
                    "away_starter":  str(r.get("away_starter") or ""),
                    "season":        _int_or_none(r.get("season")),
                    "metadata":      "{}",
                    "created_at":    _date_str(r.get("created_at")),
                })
            self.push_records("games", records)
            logger.info("[hf_uploader] queued %d game records", len(records))
            return True
        except Exception as e:
            logger.error("[hf_uploader] sync_games error: %s", e)
            return False

    def _sync_odds(self, conn) -> bool:
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT sport, home_team, away_team, game_date, market, outcome,
                       odds_am, dec_odds, total_line, bookmaker, fetched_at
                FROM odds_history ORDER BY fetched_at
            """)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
            records = [dict(zip(cols, r)) for r in rows]
            for r in records:
                r["game_date"] = _date_str(r.get("game_date"))
                r["fetched_at"] = _date_str(r.get("fetched_at"))
                r["odds_am"] = _int_or_none(r.get("odds_am"))
                r["dec_odds"] = _float_or_none(r.get("dec_odds"))
                r["total_line"] = _float_or_none(r.get("total_line"))
            self.push_records("odds", records)
            logger.info("[hf_uploader] queued %d odds records", len(records))
            return True
        except Exception as e:
            logger.error("[hf_uploader] sync_odds error: %s", e)
            return False

    def _sync_injuries(self, conn) -> bool:
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT sport, team, player_name, status, description,
                       injury_type, source, fetched_at
                FROM injury_reports ORDER BY fetched_at
            """)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
            records = [dict(zip(cols, r)) for r in rows]
            for r in records:
                r["fetched_at"] = _date_str(r.get("fetched_at"))
            self.push_records("injuries", records)
            logger.info("[hf_uploader] queued %d injury records", len(records))
            return True
        except Exception as e:
            logger.error("[hf_uploader] sync_injuries error: %s", e)
            return False

    def _sync_predictions(self, conn) -> bool:
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT sport, matchup, game_date, bet, model_prob, book_prob,
                       edge, odds_am, dec_odds, stake_usd, ev, bet_type,
                       signal_boost, signal_sources, detected_at
                FROM value_bets ORDER BY detected_at
            """)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
            records = []
            for row in rows:
                r = dict(zip(cols, row))
                records.append({
                    "prediction_uid": "",
                    "sport":          str(r.get("sport") or ""),
                    "matchup":        str(r.get("matchup") or ""),
                    "game_date":      _date_str(r.get("game_date")),
                    "bet_type":       str(r.get("bet_type") or ""),
                    "bet":            str(r.get("bet") or ""),
                    "model_prob":     _float_or_none(r.get("model_prob")),
                    "book_prob":      _float_or_none(r.get("book_prob")),
                    "edge":           _float_or_none(r.get("edge")),
                    "odds_am":        _int_or_none(r.get("odds_am")),
                    "dec_odds":       _float_or_none(r.get("dec_odds")),
                    "stake_usd":      _float_or_none(r.get("stake_usd")),
                    "ev":             _float_or_none(r.get("ev")),
                    "signal_boost":   _float_or_none(r.get("signal_boost")),
                    "signal_sources": str(r.get("signal_sources") or ""),
                    "detected_at":    _date_str(r.get("detected_at")),
                })
            self.push_records("predictions", records)
            logger.info("[hf_uploader] queued %d prediction records", len(records))
            return True
        except Exception as e:
            logger.error("[hf_uploader] sync_predictions error: %s", e)
            return False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _date_str(val) -> str:
    if val is None:
        return ""
    if isinstance(val, (datetime.datetime, datetime.date)):
        return val.isoformat()
    return str(val)


def _float_or_none(val) -> float | None:
    try:
        return float(val) if val is not None else None
    except (TypeError, ValueError):
        return None


def _int_or_none(val) -> int | None:
    try:
        return int(val) if val is not None else None
    except (TypeError, ValueError):
        return None


def _coerce(val, arrow_type: "pa.DataType"):
    """Coerce a Python value to match an Arrow type, returning None on failure."""
    if val is None:
        return None
    try:
        if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
            return str(val)
        if pa.types.is_float32(arrow_type) or pa.types.is_float64(arrow_type):
            return float(val)
        if pa.types.is_int32(arrow_type) or pa.types.is_int64(arrow_type):
            return int(val)
        return val
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# CLI entry-point: python -m src.data.hf_uploader [--sync]
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="HuggingFace sports dataset uploader")
    parser.add_argument("--sync", action="store_true", help="Sync all data from DB to HF Hub")
    parser.add_argument("--flush", action="store_true", help="Flush pending buffers")
    args = parser.parse_args()

    up = HFUploader()
    if not up._ok:
        print("Uploader not ready — check HF_API_KEY and installed packages.")
        sys.exit(1)

    print(f"Connected to HF dataset: {up._repo_id}")

    if args.sync:
        print("Syncing from DB...")
        ok = up.sync_from_db()
        up.flush_all()
        print("Done." if ok else "Sync completed with some errors — check logs.")
    elif args.flush:
        up.flush_all()
        print("Buffers flushed.")
    else:
        parser.print_help()
