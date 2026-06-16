"""
Betting Bot — Web Dashboard (MLB)
==================================
Routes:
  GET  /                      → main dashboard (SSR empty arrays + phases)
  POST /api/run               → kick off analysis in background thread
  GET  /api/status            → {status, phase, phase_idx, phase_total, last_updated, error}
  GET  /api/cached-state      → {ok, game_cards_today, game_cards_tomorrow,
                                   best_parlays, player_props, last_updated, status}
  GET  /api/logs              → {logs: [...]}
  GET  /api/performance       → {ok, stats}
  GET  /api/predictions       → {ok, predictions}
  POST /api/resolve-outcomes  → {ok, resolved}
  POST /api/parlay/save       → {ok}
  GET  /api/parlay/list       → {ok, parlays}
  POST /api/parlay/resolve    → {ok}
  GET  /api/email/recipients  → {recipients}
  POST /api/email/send        → {ok} / {error}
  POST /api/email/send-parlay → {ok}
"""

import os
import sys
import json
import queue
import datetime
import threading
import traceback
import warnings
import atexit
import tempfile
import re
import math
import hashlib
import time
from typing import Any

from flask import Flask, render_template, jsonify, request, Response

SRC_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SRC_DIR)
from config import BANKROLL, MLB_SEASONS, SPORT as CONFIG_SPORT

# Dashboard uses a lower edge threshold to show more picks
# (bot tracks accuracy; high-edge filter is for real-money staking only)
_DASH_MIN_EDGE = 0.02
_DAILY_LOCK_HOUR_ET = int(os.getenv("DAILY_LOCK_HOUR_ET", "5"))
_DAILY_LOCK_MINUTE_ET = int(os.getenv("DAILY_LOCK_MINUTE_ET", "0"))
_AUTO_ANALYSIS_INTERVAL_MIN = int(os.getenv("AUTO_ANALYSIS_INTERVAL_MIN", "60"))
_BOOT_FORCE_ANALYSIS = str(os.getenv("BOOT_FORCE_ANALYSIS", "0")).strip().lower() in {"1", "true", "yes", "on"}
_AUTO_BACKFILL_ENABLED = str(os.getenv("AUTO_BACKFILL_ENABLED", "1")).strip().lower() in {"1", "true", "yes", "on"}
_AUTO_BACKFILL_HOUR_ET = int(os.getenv("AUTO_BACKFILL_HOUR_ET", "3"))
_AUTO_BACKFILL_MINUTE_ET = int(os.getenv("AUTO_BACKFILL_MINUTE_ET", "35"))
_AUTO_BACKFILL_DAYS = max(7, int(os.getenv("AUTO_BACKFILL_DAYS", "14") or "14"))
_AUTO_BACKFILL_SPORTS = str(
    os.getenv(
        "AUTO_BACKFILL_SPORTS",
        "nfl,nba,nhl,soccer,baseball,tennis,boxing,mma,golf,motorsports,cricket",
    )
).strip()
_ACTIVE_SPORT = str(CONFIG_SPORT or os.getenv("SPORT", "all") or "all").strip().lower()
if _ACTIVE_SPORT not in {"mlb", "soccer", "all"}:
    _ACTIVE_SPORT = "all"

_PREDICTION_QUALITY_MIN = max(0.35, min(float(os.getenv("PREDICTION_QUALITY_MIN", "0.62") or "0.62"), 0.98))
_PREDICTION_EV_MIN = max(-0.10, min(float(os.getenv("PREDICTION_EV_MIN", "-0.01") or "-0.01"), 0.50))
_PREDICTION_PROB_MIN = max(0.50, min(float(os.getenv("PREDICTION_PROB_MIN", "0.56") or "0.56"), 0.98))
_PRE_PREDICTION_REQUIRE_FULL_SETTLEMENT = str(
    os.getenv("PRE_PREDICTION_REQUIRE_FULL_SETTLEMENT", "1")
).strip().lower() in {"1", "true", "yes", "on"}
_PRE_PREDICTION_SETTLEMENT_LOOKBACK_DAYS = max(
    1,
    int(os.getenv("PRE_PREDICTION_SETTLEMENT_LOOKBACK_DAYS", "3") or "3"),
)
_PRE_PREDICTION_BLOCK_IF_RESOLVER_BUSY = str(
    os.getenv("PRE_PREDICTION_BLOCK_IF_RESOLVER_BUSY", "1")
).strip().lower() in {"1", "true", "yes", "on"}
# Strategy defaults are intentionally hardcoded to avoid env drift.
_TODAY_TOMORROW_MAX_UNDER_SHARE = 0.20
_OVER_ONLY_PLAYER_PROPS = True
_OVER_ONLY_PROPS = True
_SCHED_MISFIRE_GRACE_SEC = 45

app = Flask(__name__, template_folder="templates")

_READY_RESOLVE_CACHE_LOCK = threading.Lock()
_READY_RESOLVE_CACHE = {"ts": 0.0, "sig": "", "payload": None}
_READY_RESOLVE_MIN_INTERVAL_SEC = max(
    15, int(os.getenv("READY_RESOLVE_MIN_INTERVAL_SEC", "40") or "40")
)


def _ready_resolve_signature(bets: list[dict]) -> str:
    """Stable signature for ready-bet resolve requests (order-insensitive)."""
    rows = []
    for idx, bet in enumerate(bets or []):
        if not isinstance(bet, dict):
            continue
        rows.append(
            {
                "uid": str(
                    bet.get("uid")
                    or bet.get("bet_uid")
                    or bet.get("prediction_uid")
                    or f"ready_{idx}"
                ).strip(),
                "kind": str(bet.get("kind") or "").strip().lower(),
                "sport": str(bet.get("sport") or "").strip().lower(),
                "bet_type": str(bet.get("bet_type") or "").strip().lower(),
                "pick": str(bet.get("pick") or bet.get("label") or "").strip().lower(),
                "line": str(bet.get("line") if bet.get("line") is not None else "").strip(),
                "game": str(bet.get("game") or bet.get("game_key") or "").strip().lower(),
                "game_date": str(bet.get("game_date") or "").strip()[:10],
                "start": str(
                    bet.get("scheduled_start")
                    or bet.get("game_datetime")
                    or bet.get("start_time")
                    or ""
                ).strip(),
            }
        )
    rows.sort(key=lambda r: (r.get("uid") or "", r.get("kind") or "", r.get("pick") or ""))
    return json.dumps(rows, sort_keys=True, separators=(",", ":"))


def _clean_ready_bets_payload(bets: list[dict]) -> list[dict]:
    """Normalize and dedupe incoming ready-bet payloads before Kalshi matching."""
    def _normalize_leg(raw_leg: dict[str, Any]) -> dict[str, Any]:
        leg = dict(raw_leg)
        leg["kind"] = str(leg.get("kind") or "single").strip().lower()
        leg["sport"] = str(leg.get("sport") or "").strip().lower()
        leg["bet_type"] = str(leg.get("bet_type") or leg.get("prop_type") or "").strip().lower()
        leg["prop_type"] = str(leg.get("prop_type") or leg.get("stat_type") or leg.get("bet_type") or "").strip().lower()
        leg["pick"] = str(leg.get("pick") or leg.get("label") or "").strip()
        leg["label"] = str(leg.get("label") or leg.get("pick") or "").strip()
        leg["player_name"] = str(
            leg.get("player_name") or leg.get("name") or leg.get("player") or leg.get("athlete_name") or ""
        ).strip()
        leg["name"] = str(leg.get("name") or leg.get("player_name") or "").strip()
        leg["direction"] = str(leg.get("direction") or leg.get("recommendation") or "").strip().upper()
        leg["game"] = str(leg.get("game") or leg.get("game_key") or "").strip()
        leg["game_key"] = str(leg.get("game_key") or leg.get("game") or "").strip()
        leg["game_date"] = str(leg.get("game_date") or "").strip()[:10]
        leg["scheduled_start"] = str(
            leg.get("scheduled_start")
            or leg.get("game_datetime")
            or leg.get("start_time")
            or leg.get("game_time")
            or ""
        ).strip()
        for trusted_key in (
            "kalshi_ticker",
            "kalshi_event_ticker",
            "kalshi_series_ticker",
            "kalshi_side",
            "kalshi_status",
            "kalshi_price_cents",
            "polymarket_ticker",
            "polymarket_market_slug",
            "polymarket_event_slug",
            "polymarket_series_ticker",
            "polymarket_series_ticker_raw",
            "polymarket_series_match",
            "polymarket_side",
            "polymarket_price",
            "polymarket_status",
        ):
            leg.pop(trusted_key, None)
        return leg

    clean_rows: list[dict] = []
    seen: set[str] = set()
    for idx, raw in enumerate(bets or []):
        if not isinstance(raw, dict):
            continue
        row = dict(raw)
        uid = str(
            row.get("uid")
            or row.get("bet_uid")
            or row.get("prediction_uid")
            or f"ready_{idx}"
        ).strip()
        row["uid"] = uid
        row["bet_uid"] = str(row.get("bet_uid") or uid).strip()
        row["prediction_uid"] = str(row.get("prediction_uid") or row["bet_uid"]).strip()
        row["kind"] = str(row.get("kind") or "single").strip().lower()
        row["sport"] = str(row.get("sport") or "").strip().lower()
        row["bet_type"] = str(row.get("bet_type") or row.get("prop_type") or "").strip().lower()
        row["prop_type"] = str(row.get("prop_type") or row.get("stat_type") or row.get("bet_type") or "").strip().lower()
        row["pick"] = str(row.get("pick") or row.get("label") or "").strip()
        row["label"] = str(row.get("label") or row.get("pick") or "").strip()
        row["player_name"] = str(
            row.get("player_name") or row.get("name") or row.get("player") or row.get("athlete_name") or ""
        ).strip()
        row["name"] = str(row.get("name") or row.get("player_name") or "").strip()
        row["direction"] = str(row.get("direction") or row.get("recommendation") or "").strip().upper()
        row["game"] = str(row.get("game") or row.get("game_key") or "").strip()
        row["game_key"] = str(row.get("game_key") or row.get("game") or "").strip()
        row["game_date"] = str(row.get("game_date") or "").strip()[:10]
        row["scheduled_start"] = str(
            row.get("scheduled_start")
            or row.get("game_datetime")
            or row.get("start_time")
            or row.get("game_time")
            or ""
        ).strip()

        # Backend is source-of-truth: ignore client-provided Kalshi matching fields.
        for trusted_key in (
            "kalshi_ticker",
            "kalshi_event_ticker",
            "kalshi_series_ticker",
            "kalshi_side",
            "kalshi_status",
            "kalshi_price_cents",
            "polymarket_ticker",
            "polymarket_market_slug",
            "polymarket_event_slug",
            "polymarket_series_ticker",
            "polymarket_series_ticker_raw",
            "polymarket_series_match",
            "polymarket_side",
            "polymarket_price",
            "polymarket_status",
        ):
            row.pop(trusted_key, None)

        if row.get("kind") == "combo":
            legs = row.get("legs") if isinstance(row.get("legs"), list) else []
            row["legs"] = [
                _normalize_leg(leg)
                for leg in legs
                if isinstance(leg, dict)
            ]

        for num_key in ("line", "model_prob", "probability", "dec_odds", "decimal_odds", "ev", "quality"):
            val = row.get(num_key)
            try:
                if val is not None and str(val).strip() != "":
                    row[num_key] = float(val)
            except Exception:
                pass

        sig = "|".join([
            str(row.get("kind") or ""),
            str(row.get("sport") or ""),
            str(row.get("bet_type") or ""),
            str(row.get("label") or row.get("pick") or ""),
            str(row.get("player_name") or ""),
            str(row.get("line") if row.get("line") is not None else ""),
            str(row.get("direction") or ""),
            str(row.get("game") or ""),
            str(row.get("game_date") or ""),
        ])
        if sig in seen:
            continue
        seen.add(sig)
        clean_rows.append(row)
    return clean_rows


def _is_player_prediction_row(row: dict[str, Any]) -> bool:
    if not isinstance(row, dict):
        return False
    kind = str(row.get("kind") or "").strip().lower()
    if kind == "player_prop":
        return True
    player_name = str(
        row.get("player_name")
        or row.get("name")
        or row.get("player")
        or row.get("athlete_name")
        or ""
    ).strip()
    return bool(player_name)


def _team_only_ready_bets(bets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep ready-bet payload focused on team/game markets.

    - Drops singles that are player predictions.
    - For combo rows, removes player legs and keeps combos with >=2 team legs.
    """
    out: list[dict[str, Any]] = []
    for raw in bets or []:
        if not isinstance(raw, dict):
            continue
        row = dict(raw)
        kind = str(row.get("kind") or "").strip().lower()
        if kind == "combo":
            legs = [
                dict(leg)
                for leg in (row.get("legs") or [])
                if isinstance(leg, dict) and not _is_player_prediction_row(leg)
            ]
            if len(legs) < 2:
                continue
            row["legs"] = legs
            out.append(row)
            continue
        if _is_player_prediction_row(row):
            continue
        out.append(row)
    return out


def _prediction_to_ready_bet(pred: dict, today_str: str) -> dict:
    """Convert DB prediction row to canonical ready-bet payload for exchange resolvers."""
    def _norm_exchange_sport(raw: str) -> str:
        s = str(raw or "").strip().lower().replace("_", "")
        if not s:
            return ""
        if s in {"icehockey", "nhl", "hockey"}:
            return "hockey"
        if s in {"americanfootball", "nfl", "football"}:
            return "football"
        if s in {"soccer", "mls", "epl"}:
            return "soccer"
        if s in {"basketball", "nba", "wnba"}:
            return "basketball"
        if s in {"baseball", "mlb"}:
            return "baseball"
        if s in {"boxing"}:
            return "boxing"
        if s in {"mma", "ufc", "pfl", "bellator"}:
            return "mma"
        if s in {"tennis", "atp", "wta"}:
            return "tennis"
        if s in {"golf", "pga", "lpga"}:
            return "golf"
        if s in {"motorsports", "f1", "nascar"}:
            return "motorsports"
        if s in {"cricket"}:
            return "cricket"
        return str(raw or "").strip().lower()

    def _split_matchup_text(raw: str) -> tuple[str, str]:
        text = str(raw or "").strip()
        if not text:
            return "", ""
        normalized = re.sub(r"\s+", " ", text)
        for sep in (" @ ", " vs ", " v ", " vs. "):
            if sep in normalized.lower():
                parts = re.split(re.escape(sep), normalized, flags=re.IGNORECASE)
                if len(parts) == 2:
                    away = str(parts[0] or "").strip()
                    home = str(parts[1] or "").strip()
                    return away, home
        if "@" in normalized:
            parts = normalized.split("@", 1)
            if len(parts) == 2:
                return str(parts[0] or "").strip(), str(parts[1] or "").strip()
        return "", ""

    pick = str(pred.get("pick") or "")
    game_text = str(pred.get("game") or pred.get("game_key") or "")
    home_team = str(pred.get("home_team") or "").strip()
    away_team = str(pred.get("away_team") or "").strip()
    if not home_team or not away_team:
        away_guess, home_guess = _split_matchup_text(game_text)
        if not away_team and away_guess:
            away_team = away_guess
        if not home_team and home_guess:
            home_team = home_guess

    bet_type = str(pred.get("bet_type") or "moneyline").strip().lower()
    kind = "player_prop" if ("player_prop" in bet_type or str(pred.get("prop_type") or "").strip()) else "single"
    sport = _norm_exchange_sport(str(pred.get("sport") or ""))

    return {
        "uid": str(pred.get("bet_uid") or pred.get("id") or "").strip(),
        "bet_uid": str(pred.get("bet_uid") or pred.get("id") or "").strip(),
        "kind": kind,
        "sport": sport,
        "bet_type": bet_type,
        "pick": pick,
        "label": pick,
        "line": pred.get("line"),
        "game_date": str(pred.get("game_date") or today_str),
        "game_time": str(pred.get("game_time") or ""),
        "game": game_text,
        "game_key": str(pred.get("game_key") or game_text),
        "home_team": home_team,
        "away_team": away_team,
        "player_name": _extract_player_name(pick),
        "team": _extract_pick_team(pick, home_team, away_team),
        "direction": _extract_direction(pick),
        "prop_type": _extract_prop_type(bet_type, pick),
        "league": str(pred.get("league") or pred.get("competition") or pred.get("competition_name") or ""),
        "series_ticker": str(pred.get("kalshi_series_ticker") or pred.get("polymarket_series_ticker") or ""),
        "kalshi_series_ticker": str(pred.get("kalshi_series_ticker") or ""),
        "polymarket_series_ticker": str(pred.get("polymarket_series_ticker") or ""),
        "side_default": "no" if "under" in pick.lower() else "yes",
    }


def _sync_exchange_resolution_statuses(days_back: int = 5, max_rows: int = 300) -> dict:
    """Refresh pending prediction exchange metadata so tracking/settlement stays in sync."""
    try:
        from data.db import get_predictions, update_prediction_exchange_statuses
        from data.kalshi import resolve_ready_bets as resolve_kalshi_ready_bets
        from data.polymarket import resolve_ready_bets as resolve_polymarket_ready_bets
    except Exception:
        return {"scanned": 0, "db_updated": 0, "kalshi_hits": 0, "polymarket_hits": 0}

    try:
        db_sport = None if _ACTIVE_SPORT == "all" else _ACTIVE_SPORT
        pending_preds = get_predictions(
            days=max(2, int(days_back) + 2),
            outcome="PENDING",
            sport=db_sport,
        ) or []
        if not pending_preds:
            return {"scanned": 0, "db_updated": 0, "kalshi_hits": 0, "polymarket_hits": 0}

        today = _et_calendar_today()
        min_date = today - datetime.timedelta(days=max(1, int(days_back) + 1))

        picked: list[dict] = []
        for pred in pending_preds:
            if not isinstance(pred, dict):
                continue
            game_date_s = str(pred.get("game_date") or "")[:10]
            try:
                game_date = datetime.date.fromisoformat(game_date_s)
            except Exception:
                game_date = today
            if game_date < min_date:
                continue
            if not str(pred.get("bet_uid") or pred.get("id") or "").strip():
                continue
            picked.append(pred)
            if len(picked) >= max(50, int(max_rows)):
                break

        if not picked:
            return {"scanned": 0, "db_updated": 0, "kalshi_hits": 0, "polymarket_hits": 0}

        today_str = today.isoformat()
        ready_bets = _clean_ready_bets_payload([_prediction_to_ready_bet(p, today_str) for p in picked])
        if not ready_bets:
            return {"scanned": 0, "db_updated": 0, "kalshi_hits": 0, "polymarket_hits": 0}

        kalshi_payload = {}
        polymarket_payload = {}
        try:
            kalshi_payload = resolve_kalshi_ready_bets(ready_bets) or {}
        except Exception:
            kalshi_payload = {}
        try:
            polymarket_payload = resolve_polymarket_ready_bets(ready_bets) or {}
        except Exception:
            polymarket_payload = {}

        def _match_count(payload: dict) -> int:
            if not isinstance(payload, dict):
                return 0
            try:
                direct = int(payload.get("matched") or 0)
            except Exception:
                direct = 0
            if direct > 0:
                return direct
            summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
            try:
                summary_count = int(summary.get("matched") or 0)
            except Exception:
                summary_count = 0
            if summary_count > 0:
                return summary_count
            resolutions = payload.get("resolutions") if isinstance(payload.get("resolutions"), dict) else {}
            return sum(
                1
                for v in resolutions.values()
                if isinstance(v, dict) and str(v.get("status") or "").lower() == "matched"
            )

        # Retry once with forced market-catalog refresh when first pass is weak.
        k_initial = _match_count(kalshi_payload)
        p_initial = _match_count(polymarket_payload)
        if ready_bets and (k_initial == 0 or p_initial == 0):
            if k_initial == 0:
                try:
                    kalshi_payload = resolve_kalshi_ready_bets(ready_bets, force_refresh=True) or kalshi_payload
                except Exception:
                    pass
            if p_initial == 0:
                try:
                    polymarket_payload = resolve_polymarket_ready_bets(ready_bets, force_refresh=True) or polymarket_payload
                except Exception:
                    pass

        kalshi_res = kalshi_payload.get("resolutions") if isinstance(kalshi_payload, dict) else {}
        poly_res = polymarket_payload.get("resolutions") if isinstance(polymarket_payload, dict) else {}
        if not isinstance(kalshi_res, dict):
            kalshi_res = {}
        if not isinstance(poly_res, dict):
            poly_res = {}

        updates: list[dict] = []
        kalshi_hits = 0
        poly_hits = 0
        for bet in ready_bets:
            uid = str(bet.get("bet_uid") or bet.get("uid") or "").strip()
            if not uid:
                continue
            k_row = kalshi_res.get(uid) if isinstance(kalshi_res.get(uid), dict) else {}
            p_row = poly_res.get(uid) if isinstance(poly_res.get(uid), dict) else {}
            row = {"bet_uid": uid}

            if k_row:
                row["kalshi_ticker"] = str(k_row.get("market_ticker") or "")
                row["kalshi_event_ticker"] = str(k_row.get("event_ticker") or "")
                row["kalshi_series_ticker"] = str(k_row.get("series_ticker") or "")
                row["kalshi_side"] = str(k_row.get("side") or "")
                row["kalshi_price_cents"] = k_row.get("price_cents")
                row["kalshi_status"] = str(k_row.get("status") or "")
                if row["kalshi_ticker"] or row["kalshi_status"]:
                    kalshi_hits += 1

            if p_row:
                row["polymarket_ticker"] = str(p_row.get("market_ticker") or "")
                row["polymarket_market_slug"] = str(p_row.get("market_slug") or "")
                row["polymarket_event_slug"] = str(p_row.get("event_slug") or "")
                row["polymarket_series_ticker"] = str(p_row.get("series_ticker") or "")
                row["polymarket_side"] = str(p_row.get("side") or "")
                row["polymarket_price"] = p_row.get("price")
                row["polymarket_status"] = str(p_row.get("status") or "")
                if row["polymarket_ticker"] or row["polymarket_status"]:
                    poly_hits += 1

            if len(row) > 1:
                updates.append(row)

        db_updated = update_prediction_exchange_statuses(updates)
        return {
            "scanned": len(ready_bets),
            "db_updated": int(db_updated or 0),
            "kalshi_hits": kalshi_hits,
            "polymarket_hits": poly_hits,
        }
    except Exception:
        return {"scanned": 0, "db_updated": 0, "kalshi_hits": 0, "polymarket_hits": 0}

# ─── Gunicorn / production: init once per worker ─────────────────────────────
_worker_initialized = False
_worker_init_lock   = threading.Lock()
_scheduler          = None
_worker_boot_thread_started = False
_worker_boot_lock = threading.Lock()
_last_analysis_started_ts = 0.0
_last_analysis_finished_ts = 0.0
_last_analysis_ok: bool | None = None
_last_analysis_error = ""
_last_analysis_mode = ""

_BG_LOCK_PATH = os.path.join(tempfile.gettempdir(), "bettor_bg.lock")
_BG_LOCK_FD = None
_BG_IS_LEADER = False


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except Exception:
        return False


def _release_bg_lock():
    global _BG_LOCK_FD
    try:
        if _BG_LOCK_FD is not None:
            os.close(_BG_LOCK_FD)
            _BG_LOCK_FD = None
        if os.path.exists(_BG_LOCK_PATH):
            os.remove(_BG_LOCK_PATH)
    except Exception:
        pass


def _acquire_bg_lock() -> bool:
    """Return True if this process becomes the background-job leader."""
    global _BG_LOCK_FD
    try:
        fd = os.open(_BG_LOCK_PATH, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(fd, str(os.getpid()).encode("utf-8"))
        _BG_LOCK_FD = fd
        atexit.register(_release_bg_lock)
        return True
    except FileExistsError:
        try:
            with open(_BG_LOCK_PATH, "r", encoding="utf-8") as f:
                pid = int((f.read() or "0").strip() or "0")
            if _pid_alive(pid):
                return False
        except Exception:
            return False
        try:
            os.remove(_BG_LOCK_PATH)
        except Exception:
            return False
        return _acquire_bg_lock()


def _init_worker():
    global _worker_initialized, _scheduler, _BG_IS_LEADER
    with _worker_init_lock:
        if _worker_initialized:
            return
        _worker_initialized = True
    try:
        from data.db import init_schema
        init_schema()
    except Exception as e:
        print(f"[worker-init] DB init: {e}")
    _BG_IS_LEADER = _acquire_bg_lock()
    print(f"[worker-init] pid={os.getpid()} leader={_BG_IS_LEADER}")
    if _BG_IS_LEADER:
        try:
            _scheduler = _start_scheduler()
        except Exception as e:
            _scheduler = None
            print(f"[worker-init] scheduler start error: {e}")

        for svc_name, svc_fn in (
            ("live-scores", _start_live_scores),
            ("outcome-resolver", _start_outcome_resolver),
            ("kalshi-monitor", _start_kalshi_monitor),
            ("kalshi-ws", _start_kalshi_ws_client),
            ("auto-boot-analysis", _auto_boot_analysis),
        ):
            try:
                if svc_name == "auto-boot-analysis":
                    # Never block request handling on boot analysis.
                    threading.Thread(target=svc_fn, name="bettor-auto-boot", daemon=True).start()
                else:
                    svc_fn()
            except Exception as e:
                print(f"[worker-init] {svc_name} start error: {e}")
    else:
        _scheduler = None
        try:
            _start_cache_poller()
        except Exception as e:
            print(f"[worker-init] cache-poller start error: {e}")


def _schedule_worker_boot(reason: str = "import") -> None:
    """Schedule one asynchronous worker boot attempt for this process."""
    global _worker_boot_thread_started
    with _worker_boot_lock:
        if _worker_initialized or _worker_boot_thread_started:
            return
        _worker_boot_thread_started = True

    def _runner():
        global _worker_boot_thread_started
        try:
            _init_worker()
        finally:
            with _worker_boot_lock:
                _worker_boot_thread_started = False

    try:
        threading.Thread(
            target=_runner,
            name=f"bettor-worker-init-{reason}",
            daemon=True,
        ).start()
    except Exception as e:
        with _worker_boot_lock:
            _worker_boot_thread_started = False
        print(f"[worker-init] bootstrap scheduling error ({reason}): {e}")


_EAGER_WORKER_INIT = str(os.getenv("EAGER_WORKER_INIT", "1")).strip().lower() in {"1", "true", "yes", "on"}
if _EAGER_WORKER_INIT and not _worker_initialized:
    # Start autonomous background services even if no request ever hits the app.
    _schedule_worker_boot("import")

_MLB_PHASES = [
    "Fetching MLB schedule",
    "Loading team stats & model",
    "Fetching injuries",
    "Fetching live odds",
    "Running game predictions",
    "Building player props",
    "Building parlays",
    "Fetching sentiment",
    "Saving to database",
]

_SOCCER_PHASES = [
    "Fetching tournament fixtures",
    "Running soccer model + sentiment",
    "Building player props",
    "Building parlays",
    "Saving to database",
]

_ALL_SPORTS_PHASES = [
    "Discovering available sports",
    "Fetching live odds feed",
    "Ranking best available bets",
    "Building cards",
]

if _ACTIVE_SPORT == "soccer":
    _PHASES = _SOCCER_PHASES
elif _ACTIVE_SPORT == "mlb":
    _PHASES = _MLB_PHASES
else:
    _PHASES = _ALL_SPORTS_PHASES


_MULTI_SPORT_CACHE = {
    "snapshot": None,
    "ts": 0.0,
}
_MULTI_SPORT_CACHE_TTL_SEC = int(os.getenv("MULTI_SPORT_CACHE_TTL_SEC", "180"))
_MAX_ODDS_SPORTS = int(os.getenv("MAX_ODDS_SPORTS", "24"))
_MIN_ODDS_SPORTS = max(1, int(os.getenv("MIN_ODDS_SPORTS", "10")))
_TENNIS_TOP_OUTRIGHT_PICKS = max(4, min(int(os.getenv("TENNIS_TOP_OUTRIGHT_PICKS", "14") or "14"), 30))
_TENNIS_TOP_MARKET_PICKS = max(2, min(int(os.getenv("TENNIS_TOP_MARKET_PICKS", "6") or "6"), 20))
_SPORTS_HUB_FORECAST_DAYS = max(2, min(21, int(os.getenv("SPORTS_HUB_FORECAST_DAYS", "14") or "14")))
_ON_RAILWAY = any(
    str(os.getenv(k, "")).strip()
    for k in ("RAILWAY_ENVIRONMENT", "RAILWAY_PROJECT_ID", "RAILWAY_PUBLIC_DOMAIN")
)
_BILL_SAVER_MODE = str(
    os.getenv("BILL_SAVER_MODE", "1" if _ON_RAILWAY else "0")
).strip().lower() in {"1", "true", "yes", "on"}


def _env_flag(name: str, default: str = "0") -> bool:
    return str(os.getenv(name, default)).strip().lower() in {"1", "true", "yes", "on"}


_ALL_SPORTS_SENTIMENT_MAX_GAMES = max(1, int(os.getenv("ALL_SPORTS_SENTIMENT_MAX_GAMES", "40")))
_ALL_SPORTS_SENTIMENT_PLAYERS_PER_GAME = max(1, int(os.getenv("ALL_SPORTS_SENTIMENT_PLAYERS_PER_GAME", "16")))
_ALL_SPORTS_SENTIMENT_INCLUDE_NEWS = _env_flag("ALL_SPORTS_SENTIMENT_INCLUDE_NEWS", "0")
_ALL_SPORTS_STRICT_SENTIMENT_ONLY = _env_flag("ALL_SPORTS_STRICT_SENTIMENT_ONLY", "0")

_state = {
    "status":           "idle",
    "phase":            "",
    "phase_idx":        0,
    "phase_total":      len(_PHASES),
    "last_updated":     None,
    "last_updated_ts":  None,
    "error":            None,
    "game_cards_today":    [],
    "game_cards_tomorrow": [],
    "best_parlays":        [],
    "player_props":        [],
    "elite_parlay":        None,
    "live_scores":         {},
    "logs":                [],
}
_lock = threading.Lock()

# ─── Server-Sent Events broadcast ────────────────────────────────────────────
_sse_clients: list[queue.Queue] = []
_sse_lock = threading.Lock()
_backfill_lock = threading.Lock()
_backfill_running = False
_last_auto_backfill_date: str | None = None
_last_auto_backfill_info: dict[str, Any] = {
    "running": False,
    "last_run_date": None,
    "started_at": None,
    "finished_at": None,
    "ok": None,
    "error": None,
    "elapsed_sec": None,
    "days_back": _AUTO_BACKFILL_DAYS,
    "sports": [s.strip().lower() for s in _AUTO_BACKFILL_SPORTS.split(",") if s.strip()],
    "totals": {"games": 0, "players": 0, "injuries": 0},
}

_POLY_TP_ENABLED = str(os.getenv("POLYMARKET_AUTO_TP_ENABLED", "1")).strip().lower() in {"1", "true", "yes", "on"}
_POLY_TP_TARGET_PCT = max(1.0, float(os.getenv("POLYMARKET_AUTO_TP_TARGET_PCT", "90") or "90"))
_POLY_TP_CHECK_SEC = max(20, int(os.getenv("POLYMARKET_AUTO_TP_CHECK_SEC", "45") or "45"))
_POLY_TP_SLIPPAGE_BIPS = max(1, int(os.getenv("POLYMARKET_AUTO_TP_SLIPPAGE_BIPS", "50") or "50"))
_POLY_TP_STATE_PATH = os.path.join(os.path.dirname(SRC_DIR), "data", "polymarket_order_manager.json")
_poly_tp_lock = threading.Lock()
_poly_tp_runtime = {
    "enabled": _POLY_TP_ENABLED,
    "target_pct": _POLY_TP_TARGET_PCT,
}
_poly_tp_state: dict[str, Any] = {
    "updated_at": None,
    "orders": {},
}


def _utc_now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def _poly_tp_load_state() -> None:
    os.makedirs(os.path.dirname(_POLY_TP_STATE_PATH), exist_ok=True)
    payload: dict[str, Any] = {"updated_at": None, "orders": {}}
    if os.path.exists(_POLY_TP_STATE_PATH):
        try:
            with open(_POLY_TP_STATE_PATH, "r", encoding="utf-8") as fh:
                raw = json.load(fh)
            if isinstance(raw, dict):
                payload["updated_at"] = raw.get("updated_at")
                orders = raw.get("orders")
                if isinstance(orders, dict):
                    payload["orders"] = orders
        except Exception as exc:
            _log(f"[polymarket-tp] failed to load state: {exc}")
    with _poly_tp_lock:
        _poly_tp_state.clear()
        _poly_tp_state.update(payload)


def _poly_tp_save_state() -> None:
    with _poly_tp_lock:
        _poly_tp_state["updated_at"] = _utc_now_iso()
        payload = {
            "updated_at": _poly_tp_state.get("updated_at"),
            "orders": dict(_poly_tp_state.get("orders") or {}),
        }
    os.makedirs(os.path.dirname(_POLY_TP_STATE_PATH), exist_ok=True)
    tmp_path = f"{_POLY_TP_STATE_PATH}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, sort_keys=True)
    os.replace(tmp_path, _POLY_TP_STATE_PATH)


def _poly_tp_extract_order_id(order_response: Any) -> str:
    if not isinstance(order_response, dict):
        return ""
    for key in ("id", "orderId", "order_id"):
        value = str(order_response.get(key) or "").strip()
        if value:
            return value
    nested = order_response.get("order")
    if isinstance(nested, dict):
        for key in ("id", "orderId", "order_id"):
            value = str(nested.get(key) or "").strip()
            if value:
                return value
    return ""


def _poly_tp_extract_order_price(order_response: Any, fallback_price: Any = None) -> float | None:
    candidates: list[Any] = []
    if isinstance(order_response, dict):
        candidates.append(order_response.get("price"))
        candidates.append(order_response.get("avgPx"))
        nested = order_response.get("order")
        if isinstance(nested, dict):
            candidates.append(nested.get("price"))
            candidates.append(nested.get("avgPx"))
    candidates.append(fallback_price)

    for value in candidates:
        if isinstance(value, dict):
            value = value.get("value")
        try:
            px = float(value)
            if px > 1.0 and px <= 100.0:
                px = px / 100.0
            if px > 0:
                return px
        except Exception:
            continue
    return None


def _poly_tp_track_order(placed: dict[str, Any], resolved_bet: dict[str, Any] | None = None) -> dict[str, Any] | None:
    response = placed.get("response") if isinstance(placed, dict) else None
    order_id = _poly_tp_extract_order_id(response)
    if not order_id:
        return None

    resolved = resolved_bet if isinstance(resolved_bet, dict) else {}
    market_slug = str(placed.get("market_slug") or resolved.get("market_slug") or "").strip()
    side = str(placed.get("side") or resolved.get("side") or "yes").strip().lower()
    side = "no" if side == "no" else "yes"
    amount_usd = float(placed.get("amount_usd") or 0.0)
    entry_price = _poly_tp_extract_order_price(response, fallback_price=placed.get("limit_price") or resolved.get("price"))

    record = {
        "order_id": order_id,
        "market_slug": market_slug,
        "side": side,
        "amount_usd": amount_usd,
        "entry_price": entry_price,
        "status": "tracking",
        "tracked_at": _utc_now_iso(),
        "last_checked_at": None,
        "last_order_state": None,
        "last_mark_price": None,
        "last_green_pct": None,
        "last_error": None,
        "target_pct": float(_poly_tp_runtime.get("target_pct") or _POLY_TP_TARGET_PCT),
        "close_order_id": None,
        "close_requested_at": None,
    }
    with _poly_tp_lock:
        orders = _poly_tp_state.setdefault("orders", {})
        orders[order_id] = record
    _poly_tp_save_state()
    return record


def _poly_tp_mark_price(side: str, bbo: dict[str, Any]) -> float | None:
    if not isinstance(bbo, dict):
        return None
    side_norm = "no" if str(side or "").strip().lower() == "no" else "yes"
    current_px = bbo.get("current_px")
    long_px = bbo.get("long_px")
    short_px = bbo.get("short_px")
    try:
        current_px = float(current_px) if current_px is not None else None
    except Exception:
        current_px = None
    try:
        long_px = float(long_px) if long_px is not None else None
    except Exception:
        long_px = None
    try:
        short_px = float(short_px) if short_px is not None else None
    except Exception:
        short_px = None

    if side_norm == "no":
        if short_px is not None:
            return short_px
        if current_px is not None and 0 <= current_px <= 1:
            return max(0.0, min(1.0, 1.0 - current_px))
        return None

    if long_px is not None:
        return long_px
    return current_px


def _poly_tp_is_terminal_order_state(state: str) -> bool:
    state_norm = str(state or "").strip().upper()
    return state_norm in {
        "ORDER_STATE_CANCELLED",
        "ORDER_STATE_REJECTED",
        "ORDER_STATE_EXPIRED",
    }


def run_polymarket_take_profit_cycle() -> dict[str, Any]:
    """Check tracked Polymarket orders and auto-close positions at configured profit target."""
    enabled = bool(_poly_tp_runtime.get("enabled", _POLY_TP_ENABLED))
    target_pct = float(_poly_tp_runtime.get("target_pct") or _POLY_TP_TARGET_PCT)
    if not enabled:
        return {"ok": True, "enabled": False, "checked": 0, "triggered": 0}

    try:
        from data.polymarket import close_position_order, get_market_bbo, get_order
    except Exception as exc:
        return {"ok": False, "enabled": True, "checked": 0, "triggered": 0, "error": str(exc)}

    with _poly_tp_lock:
        tracked = [
            dict(row)
            for row in (_poly_tp_state.get("orders") or {}).values()
            if isinstance(row, dict) and str(row.get("status") or "").strip().lower() == "tracking"
        ]

    checked = 0
    triggered = 0
    for row in tracked:
        order_id = str(row.get("order_id") or "").strip()
        if not order_id:
            continue
        checked += 1
        now_iso = _utc_now_iso()

        try:
            order_payload = get_order(order_id)
            order = order_payload.get("order") if isinstance(order_payload, dict) else {}
            order = order if isinstance(order, dict) else {}
            order_state = str(order.get("state") or "").strip()
            market_slug = str(order.get("marketSlug") or row.get("market_slug") or "").strip()
            entry_px = row.get("entry_price")
            if entry_px is None:
                entry_px = _poly_tp_extract_order_price(order_payload, fallback_price=row.get("entry_price"))

            with _poly_tp_lock:
                live = (_poly_tp_state.get("orders") or {}).get(order_id)
                if isinstance(live, dict):
                    live["last_checked_at"] = now_iso
                    live["last_order_state"] = order_state
                    live["market_slug"] = market_slug or live.get("market_slug")
                    if entry_px is not None:
                        live["entry_price"] = entry_px

            if _poly_tp_is_terminal_order_state(order_state):
                with _poly_tp_lock:
                    live = (_poly_tp_state.get("orders") or {}).get(order_id)
                    if isinstance(live, dict):
                        live["status"] = "inactive"
                        live["last_error"] = f"Terminal order state: {order_state}"
                continue

            if not market_slug:
                with _poly_tp_lock:
                    live = (_poly_tp_state.get("orders") or {}).get(order_id)
                    if isinstance(live, dict):
                        live["last_error"] = "Missing market slug; cannot check take-profit."
                continue

            bbo = get_market_bbo(market_slug)
            side = str(row.get("side") or "yes").strip().lower()
            mark_px = _poly_tp_mark_price(side, bbo)
            try:
                entry_num = float(entry_px) if entry_px is not None else None
            except Exception:
                entry_num = None

            green_pct = None
            if entry_num and entry_num > 0 and mark_px is not None:
                green_pct = ((float(mark_px) - float(entry_num)) / float(entry_num)) * 100.0

            with _poly_tp_lock:
                live = (_poly_tp_state.get("orders") or {}).get(order_id)
                if isinstance(live, dict):
                    live["last_mark_price"] = mark_px
                    live["last_green_pct"] = green_pct
                    live["last_error"] = None

            if green_pct is None or green_pct < target_pct:
                continue

            close_resp = close_position_order(
                market_slug=market_slug,
                synchronous_execution=False,
                slippage_bips=_POLY_TP_SLIPPAGE_BIPS,
            )
            close_order_id = str(close_resp.get("close_order_id") or "").strip() or None
            with _poly_tp_lock:
                live = (_poly_tp_state.get("orders") or {}).get(order_id)
                if isinstance(live, dict):
                    live["status"] = "close_requested"
                    live["close_order_id"] = close_order_id
                    live["close_requested_at"] = now_iso
                    live["close_response"] = close_resp.get("response") if isinstance(close_resp, dict) else close_resp
            triggered += 1
            _log(
                "[polymarket-tp] close-position requested "
                f"order_id={order_id} market={market_slug} green={green_pct:.2f}% target={target_pct:.2f}%"
            )
        except Exception as exc:
            with _poly_tp_lock:
                live = (_poly_tp_state.get("orders") or {}).get(order_id)
                if isinstance(live, dict):
                    live["last_checked_at"] = now_iso
                    live["last_error"] = str(exc)

    try:
        _poly_tp_save_state()
    except Exception as exc:
        _log(f"[polymarket-tp] failed to save state: {exc}")

    return {
        "ok": True,
        "enabled": True,
        "checked": checked,
        "triggered": triggered,
        "target_pct": target_pct,
    }


def _json_safe_default(obj):
    """Fallback serializer for json.dumps so DB-derived values never crash SSE.

    psycopg2 returns SQL NUMERIC/ROUND() columns as ``decimal.Decimal`` and date
    columns as ``datetime`` objects, neither of which is JSON serializable by
    default. Convert them to plain JSON-friendly primitives instead of raising.
    """
    import decimal

    if isinstance(obj, decimal.Decimal):
        # Preserve integer-ness where possible, else fall back to float.
        return int(obj) if obj == obj.to_integral_value() else float(obj)
    if isinstance(obj, (datetime.datetime, datetime.date, datetime.time)):
        return obj.isoformat()
    if isinstance(obj, (set, frozenset)):
        return list(obj)
    if isinstance(obj, bytes):
        return obj.decode("utf-8", "replace")
    return str(obj)


def _sse_broadcast(event: str, data: dict):
    """Push an SSE message to every connected browser tab.

    For 'state_update' events, strip the large game-card / player-prop arrays
    from the payload and replace them with a ``needs_refresh: true`` flag.
    The client will call /api/cached-state to get the full data.  This keeps
    SSE messages tiny (< 1 KB) and avoids the memory spike that was causing
    the Gunicorn worker to be OOM-killed after every analysis run.
    """
    if event == "state_update":
        light = {
            "status":         data.get("status"),
            "phase":          data.get("phase"),
            "last_updated":   data.get("last_updated"),
            "needs_refresh":  True,
            # Pass along live_scores as they're small
            "live_scores":    data.get("live_scores"),
        }
        # Strip Nones to keep the payload minimal
        msg = f"event: {event}\ndata: {json.dumps({k: v for k, v in light.items() if v is not None}, default=_json_safe_default)}\n\n"
    else:
        msg = f"event: {event}\ndata: {json.dumps(data, default=_json_safe_default)}\n\n"
    with _sse_lock:
        dead = []
        for q in _sse_clients:
            try:
                q.put_nowait(msg)
            except queue.Full:
                dead.append(q)
        for q in dead:
            _sse_clients.remove(q)


def _log(msg):
    ts = datetime.datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    with _lock:
        _state["logs"].append(line)
        if len(_state["logs"]) > 200:
            _state["logs"] = _state["logs"][-200:]


def _phase(idx, name=""):
    with _lock:
        _state["phase"]     = name or (_PHASES[idx] if idx < len(_PHASES) else name)
        _state["phase_idx"] = idx


_last_mandatory_calibration_date = None


def _run_mandatory_daily_calibration(run_date: datetime.date, team_stats=None, min_resolved: int = 50) -> dict:
    """Run one calibration check per ET day before archival/cleanup steps."""
    global _last_mandatory_calibration_date

    if _last_mandatory_calibration_date == run_date:
        return {"ok": True, "skipped": True, "msg": "already-calibrated"}

    try:
        if team_stats is None:
            try:
                from data.mlb_fetcher import fetch_team_stats
                team_stats = fetch_team_stats(MLB_SEASONS)
            except Exception:
                # Backward/partial-deploy safe fallback when fetch_team_stats is absent.
                from data.mlb_fetcher import get_team_batting_stats, get_team_pitching_stats
                import pandas as _pd

                seasons = MLB_SEASONS if isinstance(MLB_SEASONS, (list, tuple)) else [int(MLB_SEASONS)]
                frames = []
                for season in seasons:
                    bat = get_team_batting_stats(int(season))
                    pit = get_team_pitching_stats(int(season))
                    if bat is None or getattr(bat, "empty", True):
                        continue
                    if pit is None or getattr(pit, "empty", True):
                        frames.append(bat)
                    else:
                        frames.append(_pd.merge(bat, pit, on=["team", "season"], how="inner"))
                team_stats = _pd.concat(frames, ignore_index=True) if frames else _pd.DataFrame()
        from models.mlb_model import auto_improve

        result = auto_improve(team_stats, min_resolved=min_resolved, verbose=False) or {}
        _last_mandatory_calibration_date = run_date
        _log(
            "[calibration] "
            f"{result.get('msg', 'daily calibration complete')} "
            f"(ECE={result.get('ece')}, resolved={result.get('total_resolved')})"
        )
        return {"ok": True, **result}
    except Exception as exc:
        _log(f"[calibration] mandatory daily calibration failed: {exc}")
        return {"ok": False, "msg": str(exc)}


def _backfill_thesportsdb_history(days_back: int = 30) -> int:
    """Backfill multi-sport completed events from TheSportsDB into games table."""
    from data.db import upsert_game
    from data.thesportsdb_fetcher import get_events_by_date

    sport_map = [
        ("Soccer", "soccer"),
        ("Baseball", "mlb"),
        ("Basketball", "basketball"),
        ("Ice Hockey", "hockey"),
    ]
    saved = 0
    today = _et_calendar_today()

    for offset in range(max(0, int(days_back)) + 1):
        target_day = today - datetime.timedelta(days=offset)
        for tsdb_sport, internal_sport in sport_map:
            events = get_events_by_date(target_day, tsdb_sport) or []
            for ev in events:
                try:
                    game_date = str(ev.get("dateEvent") or target_day.isoformat())[:10]
                    hs_raw = ev.get("intHomeScore")
                    as_raw = ev.get("intAwayScore")
                    home_score = int(hs_raw) if hs_raw not in (None, "") else None
                    away_score = int(as_raw) if as_raw not in (None, "") else None
                    status = str(ev.get("strStatus") or "Scheduled")
                    upsert_game(
                        sport=internal_sport,
                        league=str(ev.get("strLeague") or ""),
                        home_team=str(ev.get("strHomeTeam") or ""),
                        away_team=str(ev.get("strAwayTeam") or ""),
                        game_date=game_date,
                        status=status,
                        home_score=home_score,
                        away_score=away_score,
                        external_id=str(ev.get("idEvent") or ""),
                    )
                    saved += 1
                except Exception:
                    continue
    return saved


def _run_prediction_preflight(mode: str) -> dict[str, Any]:
    """Pre-prediction backend hygiene pipeline.

    Runs before every prediction cycle:
      1) ingest recent historical data,
      2) refresh deep sentiment caches,
      3) run a quick backtest/calibration check.
    """
    enabled = str(os.getenv("PRE_PREDICTION_PREFLIGHT_ENABLED", "1")).strip().lower() in {"1", "true", "yes", "on"}
    if not enabled:
        return {"ok": True, "skipped": True, "msg": "disabled"}

    mode_norm = str(mode or "mlb").strip().lower()
    hist_days = max(7, int(os.getenv("PRE_PREDICTION_HISTORY_DAYS", "45") or "45"))
    gdelt_days = max(3, int(os.getenv("PRE_PREDICTION_GDELT_DAYS", "14") or "14"))
    backtest_days = max(30, int(os.getenv("PRE_PREDICTION_BACKTEST_DAYS", "180") or "180"))
    strict_backtest = str(os.getenv("PRE_PREDICTION_STRICT_BACKTEST", "0")).strip().lower() in {"1", "true", "yes", "on"}
    max_ece = float(os.getenv("PRE_PREDICTION_MAX_ECE", "0.18") or "0.18")
    run_multi_history = str(os.getenv("PRE_PREDICTION_MULTI_SPORT_HISTORY", "1")).strip().lower() in {"1", "true", "yes", "on"}

    summary: dict[str, Any] = {
        "ok": True,
        "mode": mode_norm,
        "history_days": hist_days,
        "gdelt_days": gdelt_days,
        "backtest_days": backtest_days,
    }

    _log(
        f"[preflight] mode={mode_norm} hist={hist_days}d gdelt={gdelt_days}d "
        f"backtest={backtest_days}d"
    )

    # 0) Hard gate: prior-day prediction/parlay tracking must be fully settled.
    try:
        lookback_days = max(1, _PRE_PREDICTION_SETTLEMENT_LOOKBACK_DAYS)
        if _PRE_PREDICTION_REQUIRE_FULL_SETTLEMENT:
            from data.db import (
                get_settlement_summary,
                get_parlay_performance_stats,
                archive_previous_day_data,
                prune_tracked_parlays_to_date,
                force_archive_pending_for_dates,
                get_tracked_parlays,
                resolve_tracked_parlay,
            )

            db_sport = None if mode_norm == "all" else mode_norm
            today = _et_calendar_today()

            def _collect_gate_snapshot() -> dict[str, Any]:
                day_rows: list[dict[str, Any]] = []
                total_items_local = 0
                total_pending_local = 0
                for delta in range(1, lookback_days + 1):
                    d = today - datetime.timedelta(days=delta)
                    settlement = get_settlement_summary(sport=db_sport, target_date=d, stale_hours=6) or {}
                    pred = settlement.get("predictions") or {}
                    props = settlement.get("props") or {}
                    parlay = get_parlay_performance_stats(sport=db_sport, target_date=d) or {}

                    pred_total = int(pred.get("total") or 0)
                    pred_pending = int(pred.get("pending") or 0)
                    prop_total = int(props.get("total") or 0)
                    prop_pending = int(props.get("pending") or 0)
                    parlay_total = int(parlay.get("total") or 0)
                    parlay_pending = int(parlay.get("pending") or 0)

                    day_total = pred_total + prop_total + parlay_total
                    day_pending = pred_pending + prop_pending + parlay_pending
                    if day_total <= 0:
                        continue

                    total_items_local += day_total
                    total_pending_local += day_pending
                    day_rows.append(
                        {
                            "date": d.isoformat(),
                            "total": day_total,
                            "pending": day_pending,
                            "predictions_pending": pred_pending,
                            "props_pending": prop_pending,
                            "parlays_pending": parlay_pending,
                        }
                    )

                settled_local = max(0, total_items_local - total_pending_local)
                completion_local = (
                    100.0 if total_items_local <= 0 else round((settled_local / total_items_local) * 100.0, 2)
                )
                return {
                    "completion_pct": completion_local,
                    "total_items": total_items_local,
                    "pending_items": total_pending_local,
                    "by_day": day_rows,
                }

            resolver_attempt = _run_resolver_locked(days_back=max(3, lookback_days + 1)) or {}
            # If the resolver lock was already held (e.g. by the EOD settle job),
            # wait up to 90 s for it to finish then retry once before blocking.
            if bool(resolver_attempt.get("skipped")):
                _log("[preflight] Resolver busy — waiting up to 90s before retry")
                _waited = 0
                while _waited < 90 and not _resolve_run_lock.acquire(blocking=False):
                    time.sleep(5)
                    _waited += 5
                if _waited < 90:
                    # We acquired the lock ourselves — release it so the retry can use it
                    _resolve_run_lock.release()
                resolver_attempt = _run_resolver_locked(days_back=max(3, lookback_days + 1)) or {}
                _log(f"[preflight] Resolver retry after wait: skipped={resolver_attempt.get('skipped')} games={resolver_attempt.get('games')} props={resolver_attempt.get('props')}")
            summary["settlement_resolver"] = {
                "skipped": bool(resolver_attempt.get("skipped")),
                "games": int(resolver_attempt.get("games") or 0),
                "props": int(resolver_attempt.get("props") or 0),
                "parlays": int(resolver_attempt.get("parlays") or 0),
            }
            if bool(resolver_attempt.get("skipped")) and _PRE_PREDICTION_BLOCK_IF_RESOLVER_BUSY:
                raise RuntimeError("Settlement gate blocked: resolver is busy; waiting for pending outcomes to settle")

            gate_snapshot = _collect_gate_snapshot()
            completion_pct = float(gate_snapshot.get("completion_pct") or 0.0)
            total_items = int(gate_snapshot.get("total_items") or 0)
            total_pending = int(gate_snapshot.get("pending_items") or 0)
            day_rows = gate_snapshot.get("by_day") or []

            if total_pending > 0:
                remediation = {
                    "archive": archive_previous_day_data(today) or {},
                    "parlays_pruned": int(prune_tracked_parlays_to_date(target_date=today) or 0),
                }
                summary["settlement_remediation"] = remediation
                _log(
                    "[preflight] settlement remediation "
                    f"archived_preds={int(remediation['archive'].get('predictions_archived') or 0)} "
                    f"archived_props={int(remediation['archive'].get('props_archived') or 0)} "
                    f"parlays_pruned={int(remediation.get('parlays_pruned') or 0)}"
                )

                resolver_attempt_2 = _run_resolver_locked(days_back=max(3, lookback_days + 1)) or {}
                summary["settlement_resolver_after_remediation"] = {
                    "skipped": bool(resolver_attempt_2.get("skipped")),
                    "games": int(resolver_attempt_2.get("games") or 0),
                    "props": int(resolver_attempt_2.get("props") or 0),
                    "parlays": int(resolver_attempt_2.get("parlays") or 0),
                }

                gate_snapshot = _collect_gate_snapshot()
                completion_pct = float(gate_snapshot.get("completion_pct") or 0.0)
                total_items = int(gate_snapshot.get("total_items") or 0)
                total_pending = int(gate_snapshot.get("pending_items") or 0)
                day_rows = gate_snapshot.get("by_day") or []

            # Final deadlock breaker: stale prior-day pendings are force-closed,
            # then settlement is evaluated one last time.
            if total_pending > 0:
                pending_dates = [
                    str(r.get("date") or "")[:10]
                    for r in day_rows
                    if int(r.get("pending") or 0) > 0 and str(r.get("date") or "")
                ]
                if pending_dates:
                    forced_archive = force_archive_pending_for_dates(pending_dates, sport=db_sport)
                    forced_parlays = 0
                    for _d in pending_dates:
                        for _p in (get_tracked_parlays(include_resolved=False, target_date=_d, sport=db_sport) or []):
                            _pid = int(_p.get("id") or 0)
                            if _pid <= 0:
                                continue
                            try:
                                # Force-close stale tracked parlays so a single dead leg cannot block new cycle forever.
                                resolve_tracked_parlay(_pid, outcome="PUSH", payout=0.0)
                                forced_parlays += 1
                            except Exception:
                                continue

                    summary["settlement_force_close"] = {
                        "dates": pending_dates,
                        "archive": forced_archive,
                        "parlays_pushed": forced_parlays,
                    }
                    _log(
                        "[preflight] settlement force-close "
                        f"preds={int((forced_archive or {}).get('predictions_archived') or 0)} "
                        f"props={int((forced_archive or {}).get('props_archived') or 0)} "
                        f"parlays={forced_parlays}"
                    )

                    resolver_attempt_3 = _run_resolver_locked(days_back=max(3, lookback_days + 1)) or {}
                    summary["settlement_resolver_after_force_close"] = {
                        "skipped": bool(resolver_attempt_3.get("skipped")),
                        "games": int(resolver_attempt_3.get("games") or 0),
                        "props": int(resolver_attempt_3.get("props") or 0),
                        "parlays": int(resolver_attempt_3.get("parlays") or 0),
                    }

                    gate_snapshot = _collect_gate_snapshot()
                    completion_pct = float(gate_snapshot.get("completion_pct") or 0.0)
                    total_items = int(gate_snapshot.get("total_items") or 0)
                    total_pending = int(gate_snapshot.get("pending_items") or 0)
                    day_rows = gate_snapshot.get("by_day") or []

            summary["settlement_gate"] = {
                "enabled": True,
                "lookback_days": lookback_days,
                "sport": db_sport or "all",
                "completion_pct": completion_pct,
                "total_items": total_items,
                "pending_items": total_pending,
                "by_day": day_rows,
            }
            _log(
                "[preflight] settlement gate "
                f"completion={completion_pct:.2f}% pending={total_pending}"
            )
            if total_pending > 0:
                raise RuntimeError(
                    "Settlement gate failed: unresolved prior-day items remain in tracking/parlays tab "
                    f"(pending={total_pending}, completion={completion_pct:.2f}%)"
                )
        else:
            summary["settlement_gate"] = {
                "enabled": False,
                "lookback_days": _PRE_PREDICTION_SETTLEMENT_LOOKBACK_DAYS,
            }
    except Exception as settle_exc:
        summary["settlement_gate_error"] = str(settle_exc)
        _log(f"[preflight] settlement gate error: {settle_exc}")
        if _PRE_PREDICTION_REQUIRE_FULL_SETTLEMENT:
            raise

    # 1) Historical ingestion
    try:
        if mode_norm in {"mlb", "all"}:
            from data.history_ingest import backfill_news, backfill_injuries, backfill_game_results
            summary["mlb_news_rows"] = backfill_news(days_back=hist_days)
            summary["mlb_injury_rows"] = backfill_injuries(days_back=hist_days)
            summary["mlb_game_rows"] = backfill_game_results(days_back=hist_days)

        # TheSportsDB coverage for non-MLB leagues (soccer/basketball/hockey + baseball)
        if mode_norm in {"soccer", "all", "mlb"}:
            summary["thesportsdb_game_rows"] = _backfill_thesportsdb_history(days_back=min(hist_days, 60))

        if run_multi_history:
            from data.multi_sport_history import ingest_multi_sport_history

            full_kalshi_sport_set = [
                "nfl", "nba", "nhl", "soccer", "baseball",
                "tennis", "boxing", "mma", "golf", "motorsports", "cricket",
            ]

            if mode_norm == "all":
                history_sports = full_kalshi_sport_set
            elif mode_norm == "soccer":
                history_sports = ["soccer"]
            elif mode_norm == "mlb":
                # Keep broad background training tables warm even on MLB runs.
                history_sports = full_kalshi_sport_set
            else:
                history_sports = full_kalshi_sport_set

            history_result = ingest_multi_sport_history(
                days_back=hist_days,
                sports=history_sports,
            )
            summary["multi_sport_history"] = history_result
            totals = history_result.get("totals") or {}
            _log(
                "[preflight] unified history saved "
                f"games={totals.get('games', 0)} players={totals.get('players', 0)} injuries={totals.get('injuries', 0)}"
            )
    except Exception as hist_exc:
        _log(f"[preflight] historical ingest warning: {hist_exc}")
        summary["history_error"] = str(hist_exc)

    # 2) Deep sentiment refresh/warm caches
    try:
        from data.gdelt_fetcher import fetch_gdelt_sentiment
        summary["gdelt_articles"] = int(fetch_gdelt_sentiment(days_back=gdelt_days, verbose=False) or 0)
    except Exception as gdelt_exc:
        _log(f"[preflight] GDELT refresh warning: {gdelt_exc}")
        summary["gdelt_error"] = str(gdelt_exc)

    try:
        if mode_norm in {"soccer", "all"}:
            from data.soccer_news import get_soccer_news
            soccer_news = get_soccer_news(query="soccer", max_results=120, max_age_hours=96)
            summary["soccer_news_rows"] = len(soccer_news or [])
    except Exception as soccer_news_exc:
        _log(f"[preflight] soccer sentiment warmup warning: {soccer_news_exc}")
        summary["soccer_news_error"] = str(soccer_news_exc)

    # 3) Backtest gate (calibration snapshot)
    try:
        from data.db import get_calibration_data

        cal = get_calibration_data(days_back=backtest_days) or {}
        total_resolved = int(cal.get("total_resolved") or 0)
        ece = cal.get("ece")
        summary["backtest_total_resolved"] = total_resolved
        summary["backtest_ece"] = ece

        if total_resolved > 0:
            _log(f"[preflight] backtest resolved={total_resolved}, ece={ece}")
        else:
            _log("[preflight] backtest skipped: no resolved outcomes yet")

        if strict_backtest and total_resolved >= 80 and isinstance(ece, (int, float)) and float(ece) > max_ece:
            raise RuntimeError(
                f"Backtest gate failed: ECE {float(ece):.4f} > {max_ece:.4f}"
            )
    except Exception as backtest_exc:
        summary["backtest_error"] = str(backtest_exc)
        _log(f"[preflight] backtest warning: {backtest_exc}")
        if strict_backtest:
            raise

    _log(
        "[preflight] complete "
        f"(mlb_news={summary.get('mlb_news_rows', 0)}, "
        f"mlb_games={summary.get('mlb_game_rows', 0)}, "
        f"thesportsdb={summary.get('thesportsdb_game_rows', 0)}, "
        f"gdelt={summary.get('gdelt_articles', 0)})"
    )
    return summary


def _clean(obj):
    if isinstance(obj, list):
        return [_clean(x) for x in obj]
    if isinstance(obj, dict):
        return {k: _clean(v) for k, v in obj.items()}
    try:
        import numpy as np
        if isinstance(obj, np.integer):  return int(obj)
        if isinstance(obj, np.floating): return float(obj)
        if isinstance(obj, np.bool_):    return bool(obj)
        if isinstance(obj, np.ndarray):  return obj.tolist()
    except ImportError:
        pass
    if isinstance(obj, (datetime.date, datetime.datetime)):
        return obj.isoformat()
    return obj


def _norm_gk(s: str) -> str:
    """Normalize game key so 'Away @ Home' == 'Away@Home'."""
    return s.replace(" @ ", "@").replace(" @", "@").replace("@ ", "@").strip()


def _compose_game_key(away_team: str, home_team: str,
                      game_datetime=None, game_date=None, game_time=None) -> str:
    """Build a stable unique key for a scheduled game instance."""
    match_key = _norm_gk(f"{away_team}@{home_team}")
    suffix = ""
    raw_dt = str(game_datetime or "").strip()
    if raw_dt:
        et_date, et_time = _datetime_to_et_parts(raw_dt)
        if et_date or et_time:
            suffix = f"{et_date}T{et_time}".strip("T")
        else:
            suffix = raw_dt
    if not suffix:
        gd = str(game_date or "").strip()
        gt = _time_hhmm(game_time)
        suffix = f"{gd}T{gt}".strip("T")
    return f"{match_key}#{suffix}" if suffix else match_key


def _card_date_from_iso(game_datetime) -> str:
    try:
        raw = str(game_datetime or "").strip()
        if not raw:
            return ""
        et_date, _ = _datetime_to_et_parts(raw)
        return et_date or ""
    except Exception:
        return ""


def _card_status_phase(status: str) -> str:
    s = str(status or "").lower()
    if not s:
        return "upcoming"
    if any(token in s for token in ("pre-game", "pregame", "warmup", "scheduled")):
        return "upcoming"
    if any(token in s for token in ("postpon", "cancel", "suspend", "final", "game over", "completed", "finished")):
        return "final"
    if re.search(r"\b(top|bottom|mid|end)\s*\d", s):
        return "live"
    if "inning" in s and "scheduled" not in s:
        return "live"
    if any(token in s for token in ("in progress", "in_play", "live", "halftime", "paused", "challenge", "progress")):
        return "live"
    return "upcoming"


def _normalize_card_list(cards, expected_date: str | None = None) -> list:
    out = []
    seen = set()
    for raw in cards or []:
        if not isinstance(raw, dict):
            continue
        card = dict(raw)
        away = card.get("away_team", "")
        home = card.get("home_team", "")
        match_key = card.get("match_key") or _norm_gk(f"{away}@{home}")
        raw_dt = str(card.get("game_datetime") or "").strip()
        has_explicit_tz = bool(re.search(r"(Z|[+-]\d{2}:\d{2})$", raw_dt, re.IGNORECASE))
        derived_date, derived_time = _datetime_to_et_parts(raw_dt) if raw_dt else ("", "")
        normalized_time = _time_hhmm(card.get("game_time"))

        if derived_time and (has_explicit_tz or not normalized_time):
            card["game_time"] = derived_time
        elif normalized_time:
            card["game_time"] = normalized_time

        game_date = str(card.get("game_date") or "").strip()
        if has_explicit_tz and derived_date:
            game_date = derived_date
        elif not game_date:
            game_date = derived_date or _card_date_from_iso(card.get("game_datetime"))
        if expected_date and game_date and game_date != expected_date:
            continue
        if expected_date and not game_date:
            # If feed omitted a date entirely, pin card to the target bucket date
            # so it cannot leak into both Today and Tomorrow.
            game_date = expected_date
        card["match_key"] = match_key
        if game_date and (has_explicit_tz or not card.get("game_date")):
            card["game_date"] = game_date
        card["game_key"] = _compose_game_key(
            away,
            home,
            card.get("game_datetime"),
            card.get("game_date"),
            card.get("game_time"),
        )
        dedupe_key = card.get("game_key") or match_key
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        out.append(card)
    return out


def _normalize_dashboard_card_buckets(today_cards, tomorrow_cards) -> tuple[list, list]:
    today_str = _et_calendar_today().isoformat()
    tomorrow_str = (_et_calendar_today() + datetime.timedelta(days=1)).isoformat()
    today_norm = _normalize_card_list(today_cards, expected_date=today_str)
    tomorrow_norm = _normalize_card_list(tomorrow_cards, expected_date=tomorrow_str)

    def _card_base_key(card: dict) -> str:
        if not isinstance(card, dict):
            return ""
        return _norm_gk(card.get("match_key") or card.get("game_key") or "")

    today_index: set[str] = {_card_base_key(c) for c in today_norm if _card_base_key(c)}
    promoted_live: list[dict] = []
    tomorrow_clean: list[dict] = []

    for card in tomorrow_norm:
        phase = _card_status_phase(card.get("status") or "")
        key = _card_base_key(card)
        if phase in {"live", "final"}:
            # Any in-progress or settled game belongs on Today's board only.
            if key and key in today_index:
                continue
            today_index.add(key)
            promoted_live.append(card)
            continue
        if key and key in today_index:
            continue
        tomorrow_clean.append(card)

    if promoted_live:
        today_norm.extend(promoted_live)

    return (today_norm, tomorrow_clean)


def _et_calendar_today() -> datetime.date:
    """Return calendar date in America/New_York (no 10 PM cutover)."""
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


def _line_value(val) -> float | None:
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    try:
        s = str(val).strip()
    except Exception:
        return None
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        pass
    m = re.search(r"-?\d+(?:\.\d+)?", s)
    if not m:
        return None
    try:
        return float(m.group(0))
    except Exception:
        return None


def _is_public_prop(p: dict) -> bool:
    if _ACTIVE_SPORT in {"soccer", "all"}:
        return True
    if (p.get("direction") or "").upper() != "OVER":
        return False
    lv = _line_value(p.get("line"))
    if lv is not None and lv <= 0.5:
        return False
    return True


def _attach_tracking_uids(game_bets: list[dict], prop_rows: list[dict]):
    """Attach deterministic IDs to every displayed/saved bet row."""
    try:
        from data.db import _prediction_uid, _prop_uid
    except Exception:
        return

    for bet in game_bets or []:
        if not isinstance(bet, dict):
            continue
        game_date = (
            bet.get("game_date")
            or bet.get("date")
            or _et_calendar_today().isoformat()
        )
        payload = {
            "sport": bet.get("sport") or _ACTIVE_SPORT,
            "game_date": game_date,
            "game_key": bet.get("game_key") or bet.get("game") or bet.get("match_key") or "",
            "bet_type": bet.get("bet_type") or "",
            "pick": bet.get("pick") or "",
            "line": bet.get("line"),
        }
        uid = bet.get("bet_uid") or bet.get("prediction_uid") or _prediction_uid(payload)
        if uid:
            bet["bet_uid"] = uid
            bet.setdefault("prediction_uid", uid)

    for prop in prop_rows or []:
        if not isinstance(prop, dict):
            continue
        game_date = (
            prop.get("date")
            or prop.get("game_date")
            or _et_calendar_today().isoformat()
        )
        payload = {
            "sport": prop.get("sport") or _ACTIVE_SPORT,
            "game_date": game_date,
            "date": game_date,
            "game_key": prop.get("game_key") or prop.get("game") or prop.get("match_key") or "",
            "name": prop.get("name") or prop.get("player_name") or "",
            "player_name": prop.get("player_name") or prop.get("name") or "",
            "team": prop.get("team") or "",
            "stat_type": prop.get("stat_type") or prop.get("prop_type") or "",
            "prop_type": prop.get("prop_type") or prop.get("stat_type") or "",
            "line": prop.get("line"),
            "direction": prop.get("direction") or prop.get("recommendation") or "",
            "recommendation": prop.get("recommendation") or prop.get("direction") or "",
        }
        uid = prop.get("bet_uid") or prop.get("prediction_uid") or _prop_uid(payload, game_date=game_date)
        if uid:
            prop["bet_uid"] = uid
            prop.setdefault("prediction_uid", uid)


def _build_card(game, bets, props, when):
    ht  = game.get("home_team", "")
    at  = game.get("away_team", "")
    sport_group = _infer_sport_group(
        game.get("sport") or game.get("competition") or game.get("league") or _ACTIVE_SPORT
    )
    competition_name = str(game.get("competition_name") or game.get("league") or "").strip()
    league_name = str(game.get("league") or competition_name or sport_group.upper() or "SPORT").strip()
    match_key = _norm_gk(game.get("match_key") or f"{at}@{ht}")
    unique_gk = _compose_game_key(
        at,
        ht,
        game.get("game_datetime"),
        game.get("date") or game.get("game_date"),
        game.get("game_time"),
    )
    gk_norm = _norm_gk(match_key)
    unique_norm = _norm_gk(unique_gk)
    alt_gk  = game.get("match_key", match_key)
    alt_norm = _norm_gk(alt_gk)

    # Also store a reversed form for reverse-key matches
    rev_gk  = _norm_gk(f"{ht}@{at}")

    card = {
        "game_key":     unique_gk,
        "match_key":    match_key,
        "sport":        sport_group,
        "league":       league_name,
        "competition":  str(game.get("competition") or "").strip(),
        "competition_name": competition_name or league_name,
        "game_pk":      game.get("game_pk") or game.get("game_id") or game.get("external_id"),
        "game_date":    game.get("date") or game.get("game_date"),
        "game_datetime": game.get("game_datetime"),
        "when":         when,
        "when_label":   when,
        "home_team":    ht,
        "away_team":    at,
        "home_starter": game.get("home_starter", "TBD"),
        "away_starter": game.get("away_starter", "TBD"),
        "game_time":    game.get("game_time", ""),
        "status":       game.get("status", ""),
        "home_score":   game.get("home_score"),
        "away_score":   game.get("away_score"),
        "inning":       game.get("inning", ""),
        "moneyline":       None,
        "run_line":        None,
        "total":           None,
        "f5_moneyline":    None,
        "f5_total":        None,
        "home_team_total": None,
        "away_team_total": None,
        "suggested_bets": [],
        "home_props":  [],
        "away_props":  [],
    }

    _GAME_BET_TYPES = ("moneyline", "run_line", "total", "f5_moneyline",
                       "f5_total", "home_team_total", "away_team_total")

    def _slot_for_bet(bet: dict) -> str | None:
        bt = str(bet.get("bet_type", ""))
        if bt in _GAME_BET_TYPES:
            return bt
        if _ACTIVE_SPORT in {"soccer", "all"}:
            if bt in {"1X2", "Draw No Bet"}:
                return "moneyline"
            if bt == "Goals O/U":
                return "total"
            if bt == "BTTS":
                return "run_line"
        return None

    def _key_matches(k: str) -> bool:
        kn = _norm_gk(str(k or ""))
        if not kn:
            return False

        # Exact key match first (supports fully-qualified unique keys).
        if kn in (gk_norm, alt_norm, rev_gk, unique_norm):
            return True

        # If both keys include a unique suffix, require exact match to avoid
        # leaking props/bets across different scheduled instances.
        if "#" in kn and "#" in unique_norm:
            return False

        base = kn.split("#", 1)[0]
        return base in (gk_norm, alt_norm, rev_gk)

    def _team_token(name: str) -> str:
        return re.sub(r"[^a-z0-9]+", "", str(name or "").strip().lower())

    def _team_aliases(name: str) -> set[str]:
        return _participant_aliases(name)

    home_token = _team_token(ht)
    away_token = _team_token(at)
    home_aliases = _team_aliases(ht)
    away_aliases = _team_aliases(at)
    participant_aliases = home_aliases | away_aliases

    def _matches_participant(value: str) -> bool:
        token = _team_token(value)
        if not token:
            return False
        if token in participant_aliases:
            return True
        for alias in participant_aliases:
            if len(alias) >= 4 and (token in alias or alias in token):
                return True
        return False

    def _same_matchup(row: dict) -> bool:
        rh = _team_token(row.get("home_team") or "")
        ra = _team_token(row.get("away_team") or "")
        if not rh or not ra:
            return False
        return (rh == home_token and ra == away_token) or (rh == away_token and ra == home_token)

    card_date = str(card.get("game_date") or "").strip()

    matched_bets = {}

    for bet in bets:
        bet_sport = _infer_sport_group(
            bet.get("sport") or bet.get("competition") or bet.get("league") or ""
        )
        if bet_sport not in {"other", ""} and sport_group not in {"other", ""} and bet_sport != sport_group:
            continue

        bet_date = str(bet.get("game_date") or bet.get("date") or "").strip()
        if card_date and bet_date and bet_date != card_date:
            continue

        bk = bet.get("game_key", bet.get("game", ""))
        bm = bet.get("match_key", "")
        if not (_key_matches(bk) or _key_matches(bm)):
            continue

        # Keep every matched market for modal/details rendering.
        sig = "|".join([
            str(bet.get("bet_type") or ""),
            str(bet.get("pick") or ""),
            str(bet.get("line") if bet.get("line") is not None else ""),
            str(bet.get("odds_am") if bet.get("odds_am") is not None else ""),
        ])
        prev = matched_bets.get(sig)
        if prev is None or float(bet.get("model_prob") or 0.0) > float(prev.get("model_prob") or 0.0):
            matched_bets[sig] = bet

        slot = _slot_for_bet(bet)
        if slot:
            current = card[slot]
            if current is None or bet.get("safety", 0) > current.get("safety", 0):
                card[slot] = bet

    card["suggested_bets"] = sorted(
        matched_bets.values(),
        key=lambda b: float(b.get("model_prob") or 0.0),
        reverse=True,
    )

    individual_sport_groups = {"tennis", "golf", "mma", "boxing", "motorsports", "cricket"}

    for p in props:
        prop_sport = _infer_sport_group(
            p.get("sport") or p.get("competition") or p.get("league") or ""
        )
        if prop_sport not in {"other", ""} and sport_group not in {"other", ""} and prop_sport != sport_group:
            continue

        prop_date = str(p.get("game_date") or p.get("date") or "").strip()
        if card_date and prop_date and prop_date != card_date:
            continue

        pk = p.get("game_key", p.get("game", ""))
        pm = p.get("match_key", "")
        key_match = _key_matches(pk) or _key_matches(pm)
        if not key_match and not _same_matchup(p):
            continue

        prop_name = str(p.get("name") or p.get("player_name") or "").strip()
        if not prop_name:
            continue
        if not str(p.get("stat_type") or p.get("prop_type") or p.get("prop_label") or "").strip():
            continue
        if sport_group in {"tennis", "golf"}:
            if _is_placeholder_participant(prop_name, game.get("event_title") or competition_name):
                continue
            if not (_matches_participant(prop_name) or _matches_participant(str(p.get("team") or ""))):
                continue

        team_token = _team_token(p.get("team") or "")
        is_home = team_token in home_aliases if team_token else False
        is_away = team_token in away_aliases if team_token else False

        # For team sports we skip ambiguous props to avoid wrong assignment.
        # For individual sports, keep props by attaching them to a visible side.
        if is_home == is_away:
            if sport_group not in individual_sport_groups:
                continue

            pname_token = _team_token(p.get("name") or p.get("team") or "")
            if pname_token:
                if pname_token in home_aliases or (home_token and pname_token in home_token):
                    is_home, is_away = True, False
                elif pname_token in away_aliases or (away_token and pname_token in away_token):
                    is_home, is_away = False, True

            if is_home == is_away:
                # Deterministic fallback for outrights/tournament style markets.
                # This guarantees player predictions are present on the card.
                if sport_group == "tennis":
                    is_home = len(card["home_props"]) <= len(card["away_props"])
                    is_away = not is_home
                else:
                    is_home, is_away = True, False

        if is_home:
            card["home_props"].append(p)
        else:
            card["away_props"].append(p)

    safety_scores = [b["safety"] for b in
                     [card["moneyline"], card["run_line"], card["total"]] if b]
    avg = sum(safety_scores) / len(safety_scores) if safety_scores else 0.45
    if avg >= 0.72:   card["overall_safety_label"] = "ELITE"
    elif avg >= 0.60: card["overall_safety_label"] = "SAFE"
    elif avg >= 0.50: card["overall_safety_label"] = "MODERATE"
    else:             card["overall_safety_label"] = "RISKY"

    # Expose tennis-specific match context (players, surface, ranks, form, H2H,
    # serve stats, fatigue) so the dashboard can render a detailed insight panel.
    if sport_group == "tennis":
        tctx = game.get("tennis_context")
        if isinstance(tctx, dict) and tctx:
            card["tennis_context"] = tctx
        for fld in ("surface", "rank_diff", "recent_form_gap",
                    "fatigue_home_days", "fatigue_away_days"):
            if game.get(fld) is not None:
                card[fld] = game.get(fld)

    return card


def _safety_label_from_prob(prob: float) -> str:
    p = float(prob or 0.5)
    if p >= 0.72:
        return "ELITE"
    if p >= 0.60:
        return "SAFE"
    if p >= 0.50:
        return "MODERATE"
    return "RISKY"


def _safety_score_from_label(label: str | None) -> float:
    v = str(label or "MODERATE").upper()
    if v == "ELITE":
        return 0.80
    if v == "SAFE":
        return 0.65
    if v == "MODERATE":
        return 0.52
    return 0.45


def _normalize_soccer_bet(game: dict, bet: dict, default_date: str) -> dict:
    row = dict(bet or {})
    home = game.get("home_team", "")
    away = game.get("away_team", "")
    match_key = _norm_gk(game.get("match_key") or row.get("match_key") or f"{away}@{home}")
    game_key = row.get("game_key") or _compose_game_key(
        away,
        home,
        game.get("game_datetime"),
        game.get("date") or game.get("game_date"),
        game.get("game_time"),
    )

    try:
        odds_am = int(float(row.get("odds_am", row.get("odds", -110)) or -110))
    except (TypeError, ValueError):
        odds_am = -110
    try:
        dec_odds = float(row.get("dec_odds") or (1 + (odds_am / 100.0) if odds_am > 0 else 1 + (100.0 / abs(odds_am))))
    except Exception:
        dec_odds = 1.91

    model_prob = float(row.get("model_prob", row.get("probability", 0.5)) or 0.5)
    safety_label = row.get("safety_label") or _safety_label_from_prob(model_prob)
    confidence = int(row.get("confidence") or round(model_prob * 100))

    row.update({
        "sport": "soccer",
        "pick": row.get("pick") or row.get("pick_label") or row.get("bet_type") or "Soccer Market",
        "match_key": match_key,
        "game_key": game_key,
        "game_date": row.get("game_date") or game.get("date") or game.get("game_date") or default_date,
        "game_time": row.get("game_time") or game.get("game_time") or "",
        "home_team": row.get("home_team") or home,
        "away_team": row.get("away_team") or away,
        "odds_am": odds_am,
        "dec_odds": round(dec_odds, 4),
        "model_prob": max(0.01, min(0.99, model_prob)),
        "confidence": confidence,
        "safety_label": safety_label,
        "safety": float(row.get("safety", _safety_score_from_label(safety_label))),
    })
    row.setdefault("worth_score", 0.0)
    row.setdefault("worth_it", False)
    row.setdefault("worth_reason", "")
    row.setdefault("market_popularity", 0.0)
    row.setdefault("market_mentions", 0)
    return row


def _normalize_soccer_prop(game: dict, prop: dict, default_date: str) -> dict:
    row = dict(prop or {})
    home = game.get("home_team", "")
    away = game.get("away_team", "")
    match_key = _norm_gk(game.get("match_key") or row.get("match_key") or f"{away}@{home}")
    game_key = row.get("game_key") or _compose_game_key(
        away,
        home,
        game.get("game_datetime"),
        game.get("date") or game.get("game_date"),
        game.get("game_time"),
    )
    try:
        odds_am = int(float(row.get("odds_am", -110) or -110))
    except (TypeError, ValueError):
        odds_am = -110
    try:
        dec_odds = float(row.get("dec_odds") or (1 + (odds_am / 100.0) if odds_am > 0 else 1 + (100.0 / abs(odds_am))))
    except Exception:
        dec_odds = 1.91

    model_prob = float(row.get("model_prob", 0.5) or 0.5)
    safety_label = row.get("safety_label") or _safety_label_from_prob(model_prob)

    row.update({
        "sport": "soccer",
        "game": row.get("game") or f"{away} @ {home}",
        "match_key": match_key,
        "game_key": game_key,
        "date": row.get("date") or game.get("date") or game.get("game_date") or default_date,
        "game_date": row.get("game_date") or game.get("date") or game.get("game_date") or default_date,
        "game_time": row.get("game_time") or game.get("game_time") or "",
        "home_team": row.get("home_team") or home,
        "away_team": row.get("away_team") or away,
        "direction": str(row.get("direction") or "OVER").upper(),
        "odds_am": odds_am,
        "dec_odds": round(dec_odds, 4),
        "model_prob": max(0.01, min(0.99, model_prob)),
        "confidence": int(row.get("confidence") or round(model_prob * 100)),
        "safety_label": safety_label,
        "safety": float(row.get("safety", _safety_score_from_label(safety_label))),
    })
    row.setdefault("worth_score", 0.0)
    row.setdefault("worth_it", False)
    row.setdefault("worth_reason", "")
    row.setdefault("market_popularity", 0.0)
    row.setdefault("market_mentions", 0)
    return row


def _infer_sport_group(sport_key: str) -> str:
    raw = str(sport_key or "").strip().lower()
    if not raw:
        return "other"

    token = re.sub(r"[^a-z0-9]+", "_", raw).strip("_")
    compact = token.replace("_", "")
    exact = {
        "mlb": "baseball",
        "baseball": "baseball",
        "baseball_mlb": "baseball",
        "soccer": "soccer",
        "football": "soccer",
        "basketball": "basketball",
        "nba": "basketball",
        "wnba": "basketball",
        "americanfootball": "americanfootball",
        "american_football": "americanfootball",
        "nfl": "americanfootball",
        "ncaaf": "americanfootball",
        "icehockey": "icehockey",
        "ice_hockey": "icehockey",
        "hockey": "icehockey",
        "nhl": "icehockey",
        "tennis": "tennis",
        "mma": "mma",
        "boxing": "boxing",
        "combat": "mma",
        "golf": "golf",
        "motorsports": "motorsports",
        "cricket": "cricket",
    }
    if token in exact:
        return exact[token]
    if compact in exact:
        return exact[compact]

    if any(k in token for k in ("american_football", "americanfootball", "nfl", "ncaaf", "college_football", "xfl", "ufl", "cfl")):
        return "americanfootball"
    if any(k in token for k in ("ice_hockey", "icehockey", "nhl", "hockey")):
        return "icehockey"
    if any(k in token for k in ("basketball", "nba", "wnba", "ncaab", "euroleague")):
        return "basketball"
    if any(k in token for k in ("baseball", "mlb", "npb", "kbo")):
        return "baseball"
    if any(k in token for k in ("soccer", "mls", "epl", "bundesliga", "la_liga", "ligue", "serie_a", "uefa", "fifa", "eng_1", "ger_1", "ita_1", "esp_1", "fra_1", "ned_1", "por_1", "champions")):
        return "soccer"
    if any(k in token for k in ("tennis", "atp", "wta")):
        return "tennis"
    if any(k in token for k in ("boxing", "box", "heavyweight", "welterweight", "middleweight")):
        return "boxing"
    if any(k in token for k in ("mma", "ufc", "bellator", "pfl")):
        return "mma"
    if any(k in token for k in ("golf", "pga", "lpga", "masters")):
        return "golf"
    if any(k in token for k in ("f1", "formula", "nascar", "indycar", "motogp", "motorsport")):
        return "motorsports"
    if any(k in token for k in ("cricket", "ipl", "t20", "odi", "test")):
        return "cricket"

    return token or "other"


def _prob_from_american(odds) -> float | None:
    try:
        o = float(odds)
    except (TypeError, ValueError):
        return None
    if o == 0:
        return None
    if o > 0:
        return 100.0 / (o + 100.0)
    return abs(o) / (abs(o) + 100.0)


def _datetime_to_et_parts(iso_value: str) -> tuple[str, str]:
    raw = str(iso_value or "").strip()
    if not raw:
        return "", ""
    try:
        dt = datetime.datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        try:
            import zoneinfo

            eastern = zoneinfo.ZoneInfo("America/New_York")
            et = dt.astimezone(eastern)
        except Exception:
            et = dt
        return et.date().isoformat(), et.strftime("%H:%M")
    except Exception:
        return "", ""


def _row_game_date(row: dict[str, Any]) -> str:
    if not isinstance(row, dict):
        return ""
    gd = str(row.get("game_date") or row.get("date") or "").strip()[:10]
    if re.match(r"^\d{4}-\d{2}-\d{2}$", gd):
        return gd
    iso_dt = str(row.get("game_datetime") or row.get("scheduled_start") or "").strip()
    if iso_dt:
        et_date, _ = _datetime_to_et_parts(iso_dt)
        return et_date or ""
    return ""


def _event_datetime_value(event_row: dict[str, Any]) -> str:
    if not isinstance(event_row, dict):
        return ""
    for key in (
        "commence_time",
        "start_time",
        "start_date",
        "startDate",
        "startDateIso",
        "scheduled_start",
        "event_start",
        "close_time",
    ):
        raw = str(event_row.get(key) or "").strip()
        if raw:
            return raw
    return ""


def _derive_individual_matchup(label: str) -> tuple[str, str]:
    """Parse event labels like 'Player A vs Player B' for individual sports."""
    title = str(label or "").strip()
    if not title:
        return "", ""
    parts = re.split(r"\s+vs\.?\s+|\s+v\.?\s+|\s+@\s+|\s+-\s+", title, maxsplit=1, flags=re.IGNORECASE)
    if len(parts) >= 2:
        left = str(parts[0] or "").strip()
        right = str(parts[1] or "").strip()
        if left and right:
            return left[:120], right[:120]
    return title[:120], ""


def _participant_aliases(name: str) -> set[str]:
    words = [w for w in re.findall(r"[a-z0-9]+", str(name or "").lower()) if w]
    aliases: set[str] = set()
    full = "".join(words)
    if full:
        aliases.add(full)
    if words:
        aliases.add(words[-1])
        aliases.add("".join(w[0] for w in words if w))
    if len(words) >= 2:
        aliases.add("".join(words[-2:]))
    return {a for a in aliases if len(a) >= 2}


def _is_placeholder_participant(name: str, event_title: str = "") -> bool:
    token = re.sub(r"[^a-z0-9]+", "", str(name or "").strip().lower())
    if not token:
        return True
    if token in {
        "field",
        "thefield",
        "tbd",
        "unknown",
        "opponent",
        "player",
        "contestant",
        "bye",
    }:
        return True
    event_token = re.sub(r"[^a-z0-9]+", "", str(event_title or "").strip().lower())
    if event_token and len(event_token) >= 8 and token == event_token:
        return True
    return False


def _is_actionable_individual_matchup(home: str, away: str, event_title: str = "") -> bool:
    home_token = re.sub(r"[^a-z0-9]+", "", str(home or "").strip().lower())
    away_token = re.sub(r"[^a-z0-9]+", "", str(away or "").strip().lower())
    if not home_token or not away_token or home_token == away_token:
        return False
    if _is_placeholder_participant(home, event_title) or _is_placeholder_participant(away, event_title):
        return False
    return True


def _is_actionable_prop_row(row: dict) -> bool:
    if not isinstance(row, dict):
        return False
    name = str(row.get("name") or row.get("player_name") or "").strip()
    if not name:
        return False
    stat_type = str(row.get("stat_type") or row.get("prop_type") or "").strip()
    prop_label = str(row.get("prop_label") or "").strip()
    if not stat_type and not prop_label:
        return False
    sport_group = _infer_sport_group(
        row.get("sport") or row.get("competition") or row.get("league") or ""
    )
    event_title = str(row.get("game") or row.get("event_title") or "").strip()
    if sport_group in {"tennis", "golf"} and _is_placeholder_participant(name, event_title):
        return False
    return True


def _rank_label(prob: float) -> str:
    if prob >= 0.72:
        return "ELITE"
    if prob >= 0.60:
        return "SAFE"
    if prob >= 0.50:
        return "MODERATE"
    return "RISKY"


def _slug_token(text: str) -> str:
    raw = re.sub(r"[^a-z0-9]+", "_", str(text or "").strip().lower())
    return raw.strip("_") or "unknown"


def _time_hhmm(raw: str) -> str:
    s = str(raw or "").strip()
    if not s:
        return ""
    if any(token in s for token in ("T", "Z", "+", "-")) and re.search(r"\d{4}-\d{2}-\d{2}", s):
        _, et_time = _datetime_to_et_parts(s)
        if et_time:
            return et_time
    for fmt in ("%I:%M %p", "%I:%M%p", "%H:%M", "%H:%M:%S"):
        try:
            return datetime.datetime.strptime(s.upper().replace(".", ""), fmt).strftime("%H:%M")
        except Exception:
            pass
    m = re.search(r"(\d{1,2}):(\d{2})", s)
    if m:
        return f"{int(m.group(1)):02d}:{m.group(2)}"
    return ""


def _collect_fallback_games_for_all_sports(
    today: datetime.date,
    tomorrow: datetime.date,
    forecast_days: int | None = None,
) -> list[dict]:
    rows: list[dict] = []
    days_span = int(forecast_days or _SPORTS_HUB_FORECAST_DAYS or 2)
    days_span = max(1, min(days_span, 14))
    quick_mode = days_span <= 2
    horizon_end = today + datetime.timedelta(days=max(1, days_span - 1))
    horizon_dates = [
        today + datetime.timedelta(days=offset)
        for offset in range((horizon_end - today).days + 1)
    ]
    allowed_dates = {
        (today + datetime.timedelta(days=offset)).isoformat()
        for offset in range((horizon_end - today).days + 1)
    }

    def _push_game(
        *,
        sport_group: str,
        league: str,
        competition: str,
        competition_name: str,
        home: str,
        away: str,
        game_date: str,
        game_time: str = "",
        game_datetime: str = "",
        status: str = "Scheduled",
        source: str = "",
        home_score=None,
        away_score=None,
    ):
        if not home or not away:
            return
        gd = str(game_date or "").strip()
        if gd and gd not in allowed_dates:
            return
        gt = _time_hhmm(game_time)
        match_key = _norm_gk(f"{away}@{home}")
        game_key = _compose_game_key(away, home, game_datetime, gd, gt)
        rows.append({
            "sport": _infer_sport_group(sport_group),
            "league": league or competition_name or competition,
            "competition": competition,
            "competition_name": competition_name or league or competition,
            "home_team": home,
            "away_team": away,
            "date": gd,
            "game_date": gd,
            "game_time": gt,
            "game_datetime": game_datetime or "",
            "status": status or "Scheduled",
            "source": str(source or "").strip().lower(),
            "home_score": home_score,
            "away_score": away_score,
            "match_key": match_key,
            "game_key": game_key,
        })

    def _as_score_int(value):
        try:
            if value is None:
                return None
            if isinstance(value, str) and not value.strip():
                return None
            return int(float(value))
        except Exception:
            return None

    def _record_summary(comp: dict) -> str:
        recs = comp.get("records") or []
        if not recs:
            return ""
        for rec in recs:
            rtype = str(rec.get("type") or "").strip().lower()
            rname = str(rec.get("name") or "").strip().lower()
            if rtype in {"total", "overall"} or rname in {"overall", "total"}:
                return str(rec.get("summary") or "")
        return str(recs[0].get("summary") or "")

    def _record_win_pct(summary: str) -> float | None:
        s = str(summary or "").strip()
        if not s:
            return None
        nums = [int(x) for x in re.findall(r"\d+", s)]
        if len(nums) < 2:
            return None
        wins = float(nums[0])
        losses = float(nums[1])
        draws = float(nums[2]) if len(nums) >= 3 else 0.0
        total = wins + losses + draws
        if total <= 0:
            return None
        return round((wins + 0.5 * draws) / total, 4)

    def _derive_event_matchup(event_payload: dict, default_label: str = "") -> tuple[str, str]:
        """Derive home/away placeholders for event-based feeds without explicit team buckets."""
        title = str(
            event_payload.get("event_title")
            or event_payload.get("strEvent")
            or event_payload.get("name")
            or default_label
            or ""
        ).strip()
        if not title:
            return "", ""
        parts = re.split(r"\s+vs\.?\s+|\s+v\.?\s+|\s+@\s+|\s+-\s+", title, maxsplit=1, flags=re.IGNORECASE)
        if len(parts) >= 2:
            left = str(parts[0] or "").strip()
            right = str(parts[1] or "").strip()
            if left and right:
                return left[:120], right[:120]
        return title[:120], "Field"

    # 0) Prefer DB-cached schedule first for speed.
    has_mlb = False
    has_soccer = False
    try:
        from data.db import get_upcoming_games

        for g in (get_upcoming_games(days_ahead=_SPORTS_HUB_FORECAST_DAYS) or []):
            sport_group = _infer_sport_group(g.get("sport") or "")
            league = str(g.get("league") or sport_group.upper() or "SPORT")
            comp_code = f"db_{_slug_token(sport_group)}_{_slug_token(league)}".upper()[:64]
            if sport_group in {"baseball", "mlb"} and "mlb" in league.lower():
                comp_code = "baseball_mlb"
                has_mlb = True
            if sport_group == "soccer":
                has_soccer = True
            _push_game(
                sport_group=sport_group,
                league=league,
                competition=comp_code,
                competition_name=league,
                home=str(g.get("home_team") or "").strip(),
                away=str(g.get("away_team") or "").strip(),
                game_date=str(g.get("game_date") or "").strip(),
                game_time=str(g.get("game_time") or ""),
                game_datetime=str(g.get("game_datetime") or ""),
                status=str(g.get("status") or "Scheduled"),
                source="db_cache",
                home_score=g.get("home_score"),
                away_score=g.get("away_score"),
            )
    except Exception as e:
        _log(f"[all-sports] DB schedule fallback unavailable: {e}")

    # 1) MLB official schedule (free)
    if not has_mlb:
        try:
            from data.mlb_fetcher import get_schedule_range

            for g in (get_schedule_range(days_ahead=_SPORTS_HUB_FORECAST_DAYS) or []):
                _push_game(
                    sport_group="baseball",
                    league="MLB",
                    competition="baseball_mlb",
                    competition_name="MLB",
                    home=str(g.get("home_team") or "").strip(),
                    away=str(g.get("away_team") or "").strip(),
                    game_date=str(g.get("date") or g.get("game_date") or "").strip(),
                    game_time=str(g.get("game_time") or ""),
                    game_datetime=str(g.get("game_datetime") or ""),
                    status=str(g.get("status") or "Scheduled"),
                    source="mlb_statsapi",
                    home_score=g.get("home_score"),
                    away_score=g.get("away_score"),
                )
        except Exception as e:
            _log(f"[all-sports] MLB fallback fetch failed: {e}")

    # 2) Soccer tournaments via fast ESPN path to avoid football-data 429 backoff delays.
    if not has_soccer:
        try:
            from data.soccer_fetcher import _fetch_matches_espn_range

            start = today.isoformat()
            end = horizon_end.isoformat()
            raw_codes = os.getenv("SOCCER_FALLBACK_COMPETITIONS", "PL,MLS,CL,EC,CLI,BL1,PD,SA,FL1,ELC")
            codes = [c.strip().upper() for c in str(raw_codes).split(",") if c.strip()]
            if not codes:
                codes = ["PL", "MLS", "CL"]

            for code in codes:
                for g in (_fetch_matches_espn_range(code, start, end) or []):
                    comp = str(g.get("competition") or code).strip().upper()
                    comp_name = str(g.get("competition_name") or g.get("comp_name") or g.get("league") or comp)
                    _push_game(
                        sport_group="soccer",
                        league=comp_name,
                        competition=comp,
                        competition_name=comp_name,
                        home=str(g.get("home_team") or "").strip(),
                        away=str(g.get("away_team") or "").strip(),
                        game_date=str(g.get("date") or g.get("game_date") or "").strip(),
                        game_time=str(g.get("game_time") or ""),
                        game_datetime=str(g.get("game_datetime") or ""),
                        status=str(g.get("status") or "Scheduled"),
                        source="soccer_espn_range",
                        home_score=g.get("home_score"),
                        away_score=g.get("away_score"),
                    )

            # Add tournament fixtures from the regular soccer fetcher for richer same-day depth.
            try:
                from data.soccer_fetcher import get_matches_today_all, get_matches_tomorrow_all

                soccer_feed_games = (get_matches_today_all() or []) + (get_matches_tomorrow_all() or [])
                for g in soccer_feed_games:
                    home = str(g.get("home_team") or "").strip()
                    away = str(g.get("away_team") or "").strip()
                    if not home or not away:
                        continue
                    comp = str(g.get("competition") or "SOCCER").strip().upper()
                    comp_name = str(g.get("competition_name") or g.get("comp_name") or g.get("league") or comp)
                    _push_game(
                        sport_group="soccer",
                        league=comp_name,
                        competition=comp,
                        competition_name=comp_name,
                        home=home,
                        away=away,
                        game_date=str(g.get("date") or g.get("game_date") or "").strip(),
                        game_time=str(g.get("game_time") or ""),
                        game_datetime=str(g.get("game_datetime") or ""),
                        status=str(g.get("status") or "Scheduled"),
                        source="soccer_fetcher",
                        home_score=g.get("home_score"),
                        away_score=g.get("away_score"),
                    )
            except Exception as soccer_feed_exc:
                _log(f"[all-sports] Soccer fetcher fallback feed skipped: {soccer_feed_exc}")
        except Exception as e:
            _log(f"[all-sports] Soccer fallback fetch failed: {e}")

    # 3) ESPN multi-sport scoreboards (free) for non-MLB/soccer coverage.
    espn_enabled = str(os.getenv("ENABLE_ESPN_MULTI_SPORT_FALLBACK", "1")).strip().lower() in {"1", "true", "yes", "on"}
    if espn_enabled:
        try:
            import requests

            espn_sources = [
                ("basketball", "nba", "basketball", "NBA"),
                ("basketball", "wnba", "basketball", "WNBA"),
                ("hockey", "nhl", "icehockey", "NHL"),
                ("football", "nfl", "americanfootball", "NFL"),
                ("football", "college-football", "americanfootball", "NCAAF"),
                ("basketball", "mens-college-basketball", "basketball", "NCAAB"),
                ("basketball", "womens-college-basketball", "basketball", "WNCAAB"),
                ("tennis", "atp", "tennis", "ATP"),
                ("tennis", "wta", "tennis", "WTA"),
                ("mma", "ufc", "mma", "UFC"),
                ("boxing", "boxing", "boxing", "Boxing"),
                ("golf", "pga", "golf", "PGA"),
                ("racing", "f1", "motorsports", "F1"),
                ("racing", "nascar", "motorsports", "NASCAR"),
                ("cricket", "icc", "cricket", "ICC"),
            ]
            if quick_mode:
                # Keep request-time fallback responsive while still covering key sports.
                espn_sources = [
                    ("basketball", "nba", "basketball", "NBA"),
                    ("basketball", "wnba", "basketball", "WNBA"),
                    ("hockey", "nhl", "icehockey", "NHL"),
                    ("football", "nfl", "americanfootball", "NFL"),
                    ("tennis", "atp", "tennis", "ATP"),
                    ("tennis", "wta", "tennis", "WTA"),
                    ("golf", "pga", "golf", "PGA"),
                    ("racing", "f1", "motorsports", "F1"),
                    ("racing", "nascar", "motorsports", "NASCAR"),
                    ("cricket", "icc", "cricket", "ICC"),
                    ("mma", "ufc", "mma", "UFC"),
                    ("boxing", "boxing", "boxing", "Boxing"),
                ]
            for d in horizon_dates:
                dates_token = d.strftime("%Y%m%d")
                for sport_path, league_path, sport_group, league_label in espn_sources:
                    url = f"https://site.api.espn.com/apis/site/v2/sports/{sport_path}/{league_path}/scoreboard"
                    try:
                        resp = requests.get(url, params={"dates": dates_token, "limit": 200}, timeout=3 if quick_mode else 8)
                        if resp.status_code != 200:
                            continue
                        data = resp.json() or {}
                    except Exception:
                        continue

                    for ev in (data.get("events") or []):
                        comp = (ev.get("competitions") or [{}])[0]
                        competitors = comp.get("competitors") or []
                        if len(competitors) < 2:
                            continue

                        home_c = next((c for c in competitors if str(c.get("homeAway") or "").lower() == "home"), competitors[0])
                        away_c = next((c for c in competitors if str(c.get("homeAway") or "").lower() == "away"), competitors[1] if len(competitors) > 1 else competitors[0])
                        home = str(((home_c.get("team") or {}).get("displayName") or "")).strip()
                        away = str(((away_c.get("team") or {}).get("displayName") or "")).strip()
                        if not home or not away:
                            continue

                        iso_dt = str(ev.get("date") or comp.get("date") or "").strip()
                        game_date, game_time = _datetime_to_et_parts(iso_dt)
                        if not game_date:
                            game_date = d.isoformat()

                        status_desc = str((((ev.get("status") or {}).get("type") or {}).get("description")) or "").strip()
                        status_state = str((((ev.get("status") or {}).get("type") or {}).get("state")) or "").strip().lower()
                        status_low = status_desc.lower()
                        if status_state in {"post", "final", "finished"} or "final" in status_low:
                            status = "Final"
                        elif status_state in {"in", "in_progress", "live"} or "progress" in status_low or "halftime" in status_low:
                            status = "In Progress"
                        else:
                            status = "Scheduled"

                        competition_name = str((comp.get("league") or {}).get("name") or league_label).strip()
                        comp_code = f"espn_{_slug_token(sport_group)}_{_slug_token(league_path)}".upper()[:64]
                        before = len(rows)
                        _push_game(
                            sport_group=sport_group,
                            league=competition_name or league_label,
                            competition=comp_code,
                            competition_name=competition_name or league_label,
                            home=home,
                            away=away,
                            game_date=game_date,
                            game_time=game_time,
                            game_datetime=iso_dt,
                            status=status,
                            source="espn",
                            home_score=_as_score_int(home_c.get("score")),
                            away_score=_as_score_int(away_c.get("score")),
                        )
                        if len(rows) > before:
                            home_rec = _record_summary(home_c)
                            away_rec = _record_summary(away_c)
                            rows[-1].update({
                                "source": "espn",
                                "espn_event_id": str(ev.get("id") or "").strip(),
                                "espn_sport_path": sport_path,
                                "espn_league_path": league_path,
                                "home_record": home_rec,
                                "away_record": away_rec,
                                "home_record_pct": _record_win_pct(home_rec),
                                "away_record_pct": _record_win_pct(away_rec),
                                "home_rank": home_c.get("curatedRank") or (home_c.get("team") or {}).get("rank"),
                                "away_rank": away_c.get("curatedRank") or (away_c.get("team") or {}).get("rank"),
                            })
        except Exception as e:
            _log(f"[all-sports] ESPN multi-sport fallback fetch failed: {e}")

    # 3b) Direct tennis/golf live bundles as an additional free source.
    # These endpoints return event payloads that may not always surface in generic scoreboard loops.
    try:
        from data.tennis_data_sources import fetch_espn_tennis_live_bundle

        tennis_bundle = fetch_espn_tennis_live_bundle(today)
        for item in (tennis_bundle.get("games") or []):
            ev = item.get("event") or {}
            comp = (ev.get("competitions") or [{}])[0]
            competitors = comp.get("competitors") or []
            home_c = {}
            away_c = {}
            if len(competitors) >= 2:
                home_c = next((c for c in competitors if str(c.get("homeAway") or "").lower() == "home"), competitors[0])
                away_c = next((c for c in competitors if str(c.get("homeAway") or "").lower() == "away"), competitors[1])
                home = str(((home_c.get("athlete") or {}).get("displayName") or (home_c.get("team") or {}).get("displayName") or home_c.get("displayName") or "")).strip()
                away = str(((away_c.get("athlete") or {}).get("displayName") or (away_c.get("team") or {}).get("displayName") or away_c.get("displayName") or "")).strip()
            else:
                # Tournament-level events without competitors are not actionable for
                # player predictions and lead to empty tennis popups.
                continue
            if not home or not away:
                continue
            iso_dt = str(ev.get("date") or comp.get("date") or "").strip()
            game_date, game_time = _datetime_to_et_parts(iso_dt)
            status_desc = str((((ev.get("status") or {}).get("type") or {}).get("description")) or "").strip()
            status_state = str((((ev.get("status") or {}).get("type") or {}).get("state")) or "").strip().lower()
            status_low = status_desc.lower()
            if status_state in {"post", "final", "finished"} or "final" in status_low:
                status = "Final"
            elif status_state in {"in", "in_progress", "live"} or "progress" in status_low:
                status = "In Progress"
            else:
                status = "Scheduled"
            if game_date not in allowed_dates:
                game_date = today.isoformat()
            _push_game(
                sport_group="tennis",
                league=str(item.get("league") or "Tennis"),
                competition="espn_tennis_live",
                competition_name=str(item.get("league") or "Tennis"),
                home=home,
                away=away,
                game_date=game_date or today.isoformat(),
                game_time=game_time,
                game_datetime=iso_dt,
                status=status,
                source="espn_tennis_live",
                home_score=_as_score_int(home_c.get("score")),
                away_score=_as_score_int(away_c.get("score")),
            )
    except Exception as e:
        _log(f"[all-sports] tennis live bundle fallback failed: {e}")

    try:
        from data.golf_data_sources import fetch_espn_golf_live_bundle

        golf_bundle = fetch_espn_golf_live_bundle(today)
        for ev in (golf_bundle.get("events") or []):
            comp = (ev.get("competitions") or [{}])[0]
            competitors = comp.get("competitors") or []
            # Golf leaderboard rows are player-vs-field style; map top two into a card.
            if len(competitors) < 2:
                continue
            c1 = competitors[0]
            c2 = competitors[1]
            home = str(((c1.get("athlete") or {}).get("displayName") or c1.get("displayName") or "")).strip()
            away = str(((c2.get("athlete") or {}).get("displayName") or c2.get("displayName") or "")).strip()
            if not home or not away:
                continue
            iso_dt = str(ev.get("date") or comp.get("date") or "").strip()
            game_date, game_time = _datetime_to_et_parts(iso_dt)
            status_desc = str((((ev.get("status") or {}).get("type") or {}).get("description")) or "").strip()
            status_state = str((((ev.get("status") or {}).get("type") or {}).get("state")) or "").strip().lower()
            status_low = status_desc.lower()
            if status_state in {"post", "final", "finished"} or "final" in status_low:
                status = "Final"
            elif status_state in {"in", "in_progress", "live"} or "progress" in status_low:
                status = "In Progress"
            else:
                status = "Scheduled"
            if game_date not in allowed_dates:
                game_date = today.isoformat()
            _push_game(
                sport_group="golf",
                league="PGA",
                competition="espn_golf_live",
                competition_name="PGA",
                home=home,
                away=away,
                game_date=game_date or today.isoformat(),
                game_time=game_time,
                game_datetime=iso_dt,
                status=status,
                source="espn_golf_live",
                home_score=_as_score_int(c1.get("score")),
                away_score=_as_score_int(c2.get("score")),
            )
    except Exception as e:
        _log(f"[all-sports] golf live bundle fallback failed: {e}")

    # 4) TheSportsDB (multi-sport free fixture feed) - opt-in due endpoint variability.
    tsdb_enabled = (not quick_mode) and str(os.getenv("ENABLE_TSDB_FALLBACK", "1")).strip().lower() in {"1", "true", "yes", "on"}
    if tsdb_enabled:
        try:
            from data.thesportsdb_fetcher import get_events_by_date

            tsdb_sports = [
                ("Soccer", "soccer"),
                ("Baseball", "baseball"),
                ("Basketball", "basketball"),
                ("Ice Hockey", "icehockey"),
                ("American Football", "americanfootball"),
                ("Tennis", "tennis"),
                ("MMA", "mma"),
                ("Boxing", "boxing"),
                ("Golf", "golf"),
                ("Motorsport", "motorsports"),
                ("Cricket", "cricket"),
                ("Rugby", "rugby"),
                ("Darts", "darts"),
                ("Snooker", "snooker"),
                ("Cycling", "cycling"),
            ]
            for d in horizon_dates:
                for tsdb_name, sport_group in tsdb_sports:
                    events = get_events_by_date(d, sport=tsdb_name) or []
                    for ev in events:
                        home = str(ev.get("strHomeTeam") or "").strip()
                        away = str(ev.get("strAwayTeam") or "").strip()
                        if not home or not away:
                            home, away = _derive_event_matchup(ev, default_label=tsdb_name)
                        if not home or not away:
                            continue
                        league = str(ev.get("strLeague") or tsdb_name)
                        sport_name = str(ev.get("strSport") or tsdb_name)
                        group = _infer_sport_group(sport_name)
                        comp_code = f"tsdb_{_slug_token(group)}_{_slug_token(league)}".upper()[:64]
                        status = str(ev.get("strStatus") or "").strip()
                        hs = ev.get("intHomeScore")
                        aw = ev.get("intAwayScore")
                        if not status:
                            status = "Final" if hs is not None and aw is not None else "Scheduled"
                        _push_game(
                            sport_group=group,
                            league=league,
                            competition=comp_code,
                            competition_name=league,
                            home=home,
                            away=away,
                            game_date=str(ev.get("dateEvent") or d.isoformat()),
                            game_time=str(ev.get("strTime") or ""),
                            game_datetime=str(ev.get("strTimestamp") or ""),
                            status=status,
                            source="tsdb",
                            home_score=hs,
                            away_score=aw,
                        )
        except Exception as e:
            _log(f"[all-sports] TheSportsDB fallback fetch failed: {e}")

    # Dedupe by competition + matchup + schedule slot.
    deduped: list[dict] = []
    seen = set()
    for g in rows:
        key = (
            str(g.get("competition") or ""),
            str(g.get("match_key") or ""),
            str(g.get("game_date") or ""),
            str(g.get("game_time") or ""),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(g)

    deduped.sort(key=lambda x: (
        str(x.get("game_date") or ""),
        str(x.get("game_time") or ""),
        str(x.get("competition_name") or x.get("league") or ""),
    ))
    return deduped


def _build_model_fallback_bets(games: list[dict]) -> list[dict]:
    bets: list[dict] = []
    today_str = _et_calendar_today().isoformat()
    tomorrow_str = (_et_calendar_today() + datetime.timedelta(days=1)).isoformat()

    def _prob_to_american(prob: float) -> int:
        p = max(0.01, min(0.99, float(prob or 0.5)))
        if p >= 0.5:
            return int(round(-p / (1.0 - p) * 100))
        return int(round((1.0 - p) / p * 100))

    def _record_win_pct(summary: str) -> float | None:
        s = str(summary or "").strip()
        if not s:
            return None
        nums = [int(x) for x in re.findall(r"\d+", s)]
        if len(nums) < 2:
            return None
        wins = float(nums[0])
        losses = float(nums[1])
        draws = float(nums[2]) if len(nums) >= 3 else 0.0
        total = wins + losses + draws
        if total <= 0:
            return None
        return (wins + (0.5 * draws)) / total

    def _to_float(value) -> float | None:
        try:
            if value is None:
                return None
            if isinstance(value, str) and not value.strip():
                return None
            return float(value)
        except Exception:
            return None

    def _to_int(value) -> int | None:
        try:
            if value is None:
                return None
            if isinstance(value, str) and not value.strip():
                return None
            return int(float(value))
        except Exception:
            return None

    def _expected_total_and_over_prob(game: dict, sport: str) -> tuple[float, float]:
        # Baseline totals by sport for fallback O/U generation.
        sport_baseline = {
            "soccer": 2.6,
            "baseball": 8.6,
            "mlb": 8.6,
            "basketball": 222.5,
            "icehockey": 6.1,
            "americanfootball": 45.5,
            "football": 45.5,
            "tennis": 22.5,
            "mma": 2.5,
            "boxing": 8.5,
            "golf": 141.5,
            "motorsports": 199.5,
            "cricket": 156.5,
        }
        baseline = float(sport_baseline.get(sport, 3.5))

        home_rec_pct = _to_float(game.get("home_record_pct"))
        away_rec_pct = _to_float(game.get("away_record_pct"))
        if home_rec_pct is None:
            home_rec_pct = _record_win_pct(game.get("home_record") or "")
        if away_rec_pct is None:
            away_rec_pct = _record_win_pct(game.get("away_record") or "")

        pace_boost = 0.0
        if home_rec_pct is not None and away_rec_pct is not None:
            pace_boost += ((home_rec_pct + away_rec_pct) - 1.0) * 0.12

        hs = _to_int(game.get("home_score"))
        aw = _to_int(game.get("away_score"))
        status = str(game.get("status") or "").strip().lower()
        if hs is not None and aw is not None and ("progress" in status or "live" in status):
            live_total = max(0.0, float(hs + aw))
            baseline = max(baseline, live_total + max(0.5, baseline * 0.08))

        expected_total = baseline * (1.0 + pace_boost)
        expected_total = max(1.5, expected_total)

        # Line near market key numbers with slight deterministic jitter per game.
        seed = "|".join([
            str(game.get("home_team") or "").strip().lower(),
            str(game.get("away_team") or "").strip().lower(),
            str(game.get("game_date") or game.get("date") or "").strip(),
            f"totals:{sport}",
        ])
        digest = hashlib.md5(seed.encode("utf-8", errors="ignore")).digest()[0]
        jitter = ((digest / 255.0) - 0.5) * 0.8
        line = round((expected_total + jitter) * 2.0) / 2.0

        # Logistic-ish mapping from expected edge to OVER probability.
        edge = expected_total - line
        over_prob = 0.5 + max(-0.18, min(0.18, edge * 0.11))
        over_prob = max(0.52, min(0.72, over_prob))
        return (line, over_prob)

    def _expected_spread_and_cover_prob(
        game: dict,
        sport: str,
        home_prob: float,
    ) -> tuple[str, float, float]:
        # Derive a reasonable spread from home-win edge with sport-specific bounds.
        # Returns (pick_text, line_abs, cover_prob) where line_abs is positive.
        spread_bounds = {
            "soccer": (0.5, 1.5),
            "baseball": (1.5, 2.5),
            "mlb": (1.5, 2.5),
            "basketball": (2.5, 10.5),
            "icehockey": (0.5, 1.5),
            "football": (1.5, 10.5),
            "americanfootball": (1.5, 10.5),
            "cricket": (1.5, 12.5),
        }
        lo, hi = spread_bounds.get(sport, (1.5, 6.5))
        edge = abs(float(home_prob or 0.5) - 0.5)
        line_abs = lo + (hi - lo) * min(1.0, edge / 0.22)
        line_abs = round(line_abs * 2.0) / 2.0

        home = str(game.get("home_team") or "").strip()
        away = str(game.get("away_team") or "").strip()
        favored_home = float(home_prob or 0.5) >= 0.5
        side_team = home if favored_home else away
        pick = f"{side_team} -{line_abs:g}"

        # Keep spreads from being too overconfident in fallback mode.
        cover_prob = 0.52 + min(0.18, edge * 0.9)
        cover_prob = max(0.52, min(0.70, cover_prob))
        return pick, line_abs, cover_prob

    def _estimate_home_prob(game: dict, sport: str) -> float:
        if sport == "soccer":
            base = 0.48
        elif sport in {"baseball", "mlb"}:
            base = 0.55
        else:
            base = 0.53

        had_structured_signal = False

        hp = _to_float(game.get("home_record_pct"))
        ap = _to_float(game.get("away_record_pct"))
        if hp is None:
            hp = _record_win_pct(game.get("home_record") or "")
        if ap is None:
            ap = _record_win_pct(game.get("away_record") or "")
        if hp is not None and ap is not None:
            base = 0.5 + ((hp - ap) * 0.75)
            if sport != "soccer":
                base += 0.02  # modest home-edge prior
            had_structured_signal = True

        hr = _to_int(game.get("home_rank"))
        ar = _to_int(game.get("away_rank"))
        if hr and ar and hr > 0 and ar > 0:
            # Lower rank number is stronger.
            rank_edge = (ar - hr) / max(10.0, float(ar + hr))
            base += max(-0.08, min(0.08, rank_edge * 0.6))
            had_structured_signal = True

        hs = _to_int(game.get("home_score"))
        aw = _to_int(game.get("away_score"))
        status = str(game.get("status") or "").strip().lower()
        if hs is not None and aw is not None:
            diff = hs - aw
            if "final" in status:
                if diff > 0:
                    return 0.99
                if diff < 0:
                    return 0.01
                return 0.50
            if "progress" in status or "live" in status:
                base += max(-0.22, min(0.22, diff * 0.035))
                had_structured_signal = True

        # If we have no standings/rank/live context, add a tiny deterministic spread
        # so fallback confidence is not uniformly identical across every game.
        if not had_structured_signal:
            seed = "|".join([
                str(game.get("home_team") or "").strip().lower(),
                str(game.get("away_team") or "").strip().lower(),
                str(game.get("game_date") or game.get("date") or "").strip(),
                sport,
            ])
            digest = hashlib.md5(seed.encode("utf-8", errors="ignore")).digest()[0]
            jitter = (digest / 255.0) - 0.5  # [-0.5, 0.5]
            jitter_span = 0.06 if sport == "soccer" else 0.04
            base += jitter * jitter_span

        return max(0.05, min(0.95, float(base)))

    # Deterministic, zero-network fallback pick generation.
    # Used only when sportsbook/model feeds are unavailable.
    max_games = max(120, min(int(os.getenv("ALL_SPORTS_FALLBACK_MAX_GAMES", "220") or "220"), 400))
    for g in (games or [])[:max_games]:
        home = str(g.get("home_team") or "").strip()
        away = str(g.get("away_team") or "").strip()
        if not home or not away:
            continue

        sport = _infer_sport_group(g.get("sport") or g.get("competition") or "")
        home_prob = _estimate_home_prob(g, sport)
        pick_home = home_prob >= 0.5
        pick_prob = home_prob if pick_home else (1.0 - home_prob)
        pick_team = home if pick_home else away
        if sport == "soccer":
            bet_type = "1X2"
            pick = f"{pick_team} to Win"
        else:
            bet_type = "moneyline"
            pick = f"{pick_team} ML"

        odds_am = _prob_to_american(pick_prob)
        label = _rank_label(pick_prob)
        game_date = g.get("game_date") or g.get("date") or today_str
        game_key = g.get("game_key") or _compose_game_key(
            away,
            home,
            g.get("game_datetime"),
            game_date,
            g.get("game_time"),
        )

        reason = "Fallback baseline pick while live odds are unavailable"
        home_rec = str(g.get("home_record") or "").strip()
        away_rec = str(g.get("away_record") or "").strip()
        if home_rec or away_rec:
            reason = f"Record-based fallback ({away} {away_rec or '?'} at {home} {home_rec or '?'})"

        bets.append({
            "sport": sport,
            "league": g.get("league") or g.get("competition_name") or sport.upper(),
            "competition": g.get("competition") or sport.upper(),
            "competition_name": g.get("competition_name") or g.get("league") or sport.upper(),
            "bet_type": bet_type,
            "pick": pick,
            "line": None,
            "odds_am": odds_am,
            "dec_odds": round((1 + (odds_am / 100.0)) if odds_am > 0 else (1 + (100.0 / abs(odds_am))), 4),
            "model_prob": round(pick_prob, 4),
            "confidence": int(round(pick_prob * 100)),
            "safety_label": label,
            "safety": _safety_score_from_label(label),
            "game_date": game_date,
            "game_time": g.get("game_time") or "",
            "home_team": home,
            "away_team": away,
            "match_key": g.get("match_key") or _norm_gk(f"{away}@{home}"),
            "game_key": game_key,
            "worth_it": pick_prob >= 0.53,
            "worth_score": round(pick_prob * 100.0, 2),
            "worth_reason": reason,
        })

        # Add totals-focused fallback bet (OVER-biased) for today/tomorrow so the
        # board remains active when paid odds credits are exhausted.
        if str(game_date) in {today_str, tomorrow_str}:
            total_line, over_prob = _expected_total_and_over_prob(g, sport)
            over_odds_am = _prob_to_american(over_prob)
            over_label = _rank_label(over_prob)
            bets.append({
                "sport": sport,
                "league": g.get("league") or g.get("competition_name") or sport.upper(),
                "competition": g.get("competition") or sport.upper(),
                "competition_name": g.get("competition_name") or g.get("league") or sport.upper(),
                "bet_type": "total",
                "pick": f"OVER {total_line}",
                "line": total_line,
                "odds_am": over_odds_am,
                "dec_odds": round((1 + (over_odds_am / 100.0)) if over_odds_am > 0 else (1 + (100.0 / abs(over_odds_am))), 4),
                "model_prob": round(over_prob, 4),
                "confidence": int(round(over_prob * 100)),
                "safety_label": over_label,
                "safety": _safety_score_from_label(over_label),
                "game_date": game_date,
                "game_time": g.get("game_time") or "",
                "home_team": home,
                "away_team": away,
                "match_key": g.get("match_key") or _norm_gk(f"{away}@{home}"),
                "game_key": game_key,
                "worth_it": over_prob >= 0.54,
                "worth_score": round(over_prob * 100.0, 2),
                "worth_reason": "Fallback totals model (OVER-priority today/tomorrow)",
                "direction": "OVER",
            })

            # Add spread fallback for team-vs-team sports to improve market-type coverage.
            if sport in {"soccer", "baseball", "mlb", "basketball", "icehockey", "football", "americanfootball", "cricket"}:
                spread_pick, spread_line, spread_prob = _expected_spread_and_cover_prob(g, sport, home_prob)
                spread_odds_am = _prob_to_american(spread_prob)
                spread_label = _rank_label(spread_prob)
                bets.append({
                    "sport": sport,
                    "league": g.get("league") or g.get("competition_name") or sport.upper(),
                    "competition": g.get("competition") or sport.upper(),
                    "competition_name": g.get("competition_name") or g.get("league") or sport.upper(),
                    "bet_type": "spread",
                    "pick": spread_pick,
                    "line": spread_line,
                    "odds_am": spread_odds_am,
                    "dec_odds": round((1 + (spread_odds_am / 100.0)) if spread_odds_am > 0 else (1 + (100.0 / abs(spread_odds_am))), 4),
                    "model_prob": round(spread_prob, 4),
                    "confidence": int(round(spread_prob * 100)),
                    "safety_label": spread_label,
                    "safety": _safety_score_from_label(spread_label),
                    "game_date": game_date,
                    "game_time": g.get("game_time") or "",
                    "home_team": home,
                    "away_team": away,
                    "match_key": g.get("match_key") or _norm_gk(f"{away}@{home}"),
                    "game_key": game_key,
                    "worth_it": spread_prob >= 0.54,
                    "worth_score": round(spread_prob * 100.0, 2),
                    "worth_reason": "Fallback spread model (today/tomorrow)",
                })

    # Dedupe similar bets.
    deduped: list[dict] = []
    seen = set()
    for b in bets:
        key = (
            str(b.get("game_key") or ""),
            str(b.get("bet_type") or ""),
            str(b.get("pick") or b.get("pick_label") or ""),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(b)
    deduped.sort(key=lambda x: float(x.get("model_prob") or 0.0), reverse=True)
    return deduped


def _multi_sport_best_bets_rows(bets: list[dict]) -> list[dict]:
    """Convert ranked game bets into rows consumed by the Best Bets table."""
    rows: list[dict] = []
    seen = set()

    for b in (bets or []):
        if not isinstance(b, dict):
            continue

        pick = str(b.get("pick") or b.get("pick_label") or "").strip()
        if not pick:
            continue

        market = str(b.get("bet_type") or "best_bet").strip() or "best_bet"
        sport = _infer_sport_group(b.get("sport") or b.get("competition") or b.get("league") or "")
        home = str(b.get("home_team") or "").strip()
        away = str(b.get("away_team") or "").strip()
        game_date = str(b.get("game_date") or b.get("date") or "").strip()
        game_time = str(b.get("game_time") or "")
        game_key = str(b.get("game_key") or "").strip()
        if not game_key and (home and away):
            game_key = _compose_game_key(away, home, b.get("game_datetime"), game_date, game_time)

        # Keep a stable, compact stat token for table filters and sorting.
        stat_type = re.sub(r"[^a-z0-9_]+", "_", market.lower()).strip("_") or "best_bet"
        prop_label = market.replace("_", " ").strip().title() or "Best Bet"

        prob_raw = b.get("model_prob", b.get("probability", 0.5))
        try:
            prob = float(prob_raw)
        except (TypeError, ValueError):
            prob = 0.5
        prob = max(0.01, min(0.99, prob))

        direction = ""
        pick_up = pick.upper()
        if "OVER" in pick_up:
            direction = "OVER"
        elif "UNDER" in pick_up:
            direction = "UNDER"

        team = str(b.get("team") or "").strip()
        if not team:
            if home and home.upper() in pick_up:
                team = home
            elif away and away.upper() in pick_up:
                team = away
            elif sport in {"golf", "tennis", "mma", "boxing", "motorsports"} and pick:
                team = pick
            else:
                team = str(b.get("competition_name") or b.get("league") or sport.upper() or "SPORT")

        dec_odds = b.get("dec_odds")
        try:
            dec_odds_f = float(dec_odds)
        except (TypeError, ValueError):
            dec_odds_f = None

        ev_val = b.get("ev")
        try:
            ev = float(ev_val)
        except (TypeError, ValueError):
            if dec_odds_f and dec_odds_f > 1:
                ev = (dec_odds_f - 1.0) * prob - (1.0 - prob)
            else:
                ev = 0.0

        row = {
            "sport": sport,
            "name": pick,
            "team": team,
            "prop_label": prop_label,
            "stat_type": stat_type,
            "line": b.get("line"),
            "direction": direction,
            "model_prob": prob,
            "safety_label": str(b.get("safety_label") or _safety_label_from_prob(prob)).upper(),
            "ev": ev,
            "odds_am": b.get("odds_am"),
            "dec_odds": dec_odds_f,
            "confidence": int(b.get("confidence") or round(prob * 100)),
            "pick": pick,
            "game": b.get("game") or (f"{away} @ {home}" if (away and home) else ""),
            "game_key": game_key,
            "match_key": b.get("match_key") or _norm_gk(f"{away}@{home}") if (away and home) else "",
            "game_date": game_date,
            "game_time": game_time,
            "league": b.get("league"),
            "competition": b.get("competition"),
            "competition_name": b.get("competition_name") or b.get("league"),
        }

        dedupe_key = (
            str(row.get("game_key") or ""),
            str(row.get("stat_type") or ""),
            str(row.get("pick") or ""),
        )
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        rows.append(row)

    today = _et_calendar_today().isoformat()
    tomorrow = (_et_calendar_today() + datetime.timedelta(days=1)).isoformat()

    def _row_sort_key(row: dict) -> tuple[int, int, float, float]:
        gdate = str(row.get("game_date") or "")[:10]
        is_near = 1 if gdate in {today, tomorrow} else 0
        direction = str(row.get("direction") or "").strip().upper()
        over_bias = 1 if direction == "OVER" else 0
        prob = float(row.get("model_prob") or 0.0)
        ev = float(row.get("ev") or 0.0)
        return (is_near, over_bias, prob, ev)

    rows.sort(key=_row_sort_key, reverse=True)

    # Hard-limit UNDER rows for today/tomorrow so near-date board stays OVER-heavy.
    # Algebraic cap keeps UNDER share <= r: U <= (r/(1-r)) * O for r in [0, 1).
    if _TODAY_TOMORROW_MAX_UNDER_SHARE < 1.0:
        near_rows: list[dict] = []
        far_rows: list[dict] = []
        for row in rows:
            gdate = str(row.get("game_date") or "")[:10]
            if gdate in {today, tomorrow}:
                near_rows.append(row)
            else:
                far_rows.append(row)

        near_over = [
            r for r in near_rows
            if str(r.get("direction") or "").strip().upper() == "OVER"
        ]
        near_under = [
            r for r in near_rows
            if str(r.get("direction") or "").strip().upper() == "UNDER"
        ]
        near_other = [
            r for r in near_rows
            if str(r.get("direction") or "").strip().upper() not in {"OVER", "UNDER"}
        ]

        if near_under:
            if not near_over:
                allowed_under = 0
            else:
                ratio = _TODAY_TOMORROW_MAX_UNDER_SHARE / max(1e-9, (1.0 - _TODAY_TOMORROW_MAX_UNDER_SHARE))
                allowed_under = int(math.floor(len(near_over) * ratio))
            allowed_under = max(0, min(len(near_under), allowed_under))
            near_rows = near_over + near_under[:allowed_under] + near_other
            near_rows.sort(key=_row_sort_key, reverse=True)

        rows = near_rows + far_rows

    return rows[:300]


def _merge_all_sports_table_rows(sentiment_rows: list[dict], best_bet_rows: list[dict]) -> list[dict]:
    """Combine sentiment/player rows with best-bet rows for sports not covered by props."""
    s_rows = list(sentiment_rows or [])
    b_rows = list(best_bet_rows or [])

    if _ALL_SPORTS_STRICT_SENTIMENT_ONLY:
        return s_rows[:400]
    if not s_rows:
        return b_rows[:400]

    covered_sports = {
        _infer_sport_group(r.get("sport") or r.get("competition") or r.get("league") or "")
        for r in s_rows
    }
    merged = list(s_rows)
    seen = {
        (
            str(r.get("game_key") or ""),
            str(r.get("name") or r.get("pick") or "").strip().lower(),
            str(r.get("stat_type") or "").strip().lower(),
            str(r.get("line") if r.get("line") is not None else ""),
            str(r.get("direction") or "").strip().upper(),
        )
        for r in merged
    }

    for row in b_rows:
        sport = _infer_sport_group(row.get("sport") or row.get("competition") or row.get("league") or "")
        if sport in covered_sports:
            continue

        key = (
            str(row.get("game_key") or ""),
            str(row.get("name") or row.get("pick") or "").strip().lower(),
            str(row.get("stat_type") or "").strip().lower(),
            str(row.get("line") if row.get("line") is not None else ""),
            str(row.get("direction") or "").strip().upper(),
        )
        if key in seen:
            continue
        seen.add(key)
        merged.append(row)

    merged.sort(
        key=lambda x: (
            float(x.get("model_prob") or 0.0),
            int(x.get("sentiment_mentions") or 0),
        ),
        reverse=True,
    )
    return merged[:400]


def _is_player_prop_style_row(row: dict) -> bool:
    if not isinstance(row, dict):
        return False
    stat_type = str(row.get("stat_type") or row.get("prop_type") or row.get("bet_type") or "").strip().lower()
    if not stat_type:
        return False
    if stat_type in {"moneyline", "spread", "total", "team_total", "best_bet", "1x2"}:
        return False
    if row.get("line") is None:
        return False
    return True


def _is_prop_style_row(row: dict) -> bool:
    if not isinstance(row, dict):
        return False
    bet_type = str(row.get("bet_type") or "").strip().lower()
    stat_type = str(row.get("stat_type") or row.get("prop_type") or "").strip().lower()
    if bet_type in {"moneyline", "spread", "run_line", "1x2"}:
        return False
    if stat_type in {"moneyline", "spread", "run_line", "1x2", "best_bet"}:
        return False
    if row.get("line") is None:
        return False
    return _is_player_prop_style_row(row) or bet_type in {"total", "team_total", "f5_total"} or stat_type in {"total", "team_total"}


def _enforce_over_only_player_props(rows: list[dict]) -> list[dict]:
    if not (_OVER_ONLY_PLAYER_PROPS or _OVER_ONLY_PROPS):
        return list(rows or [])
    out: list[dict] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        direction = str(row.get("direction") or "").strip().upper()
        if _OVER_ONLY_PROPS and _is_prop_style_row(row) and direction == "UNDER":
            continue
        if _OVER_ONLY_PLAYER_PROPS and _is_player_prop_style_row(row) and direction == "UNDER":
            continue
        out.append(row)
    return out


def _build_all_sport_sentiment_props(games: list[dict], bets: list[dict]) -> list[dict]:
    """Build all-sports player rows strictly from sentiment-mentioned players."""
    try:
        from data.sentiment import get_game_player_sentiment_props
    except Exception as e:
        _log(f"[all-sports] sentiment player extractor unavailable: {e}")
        return []
    
    if not games:
        return []

    today_str = _et_calendar_today().isoformat()
    tomorrow_str = (_et_calendar_today() + datetime.timedelta(days=1)).isoformat()
    allowed = {today_str, tomorrow_str}

    target_games = [g for g in (games or []) if _row_game_date(g) in allowed]
    target_games = target_games[:_ALL_SPORTS_SENTIMENT_MAX_GAMES]
    if not target_games:
        return []

    # Index ranked bets by game/match key to extract an odds anchor per game.
    idx: dict[str, list[dict]] = {}

    def _index_bet_key(k: str, row: dict):
        nk = _norm_gk(k or "")
        if not nk:
            return
        idx.setdefault(nk, []).append(row)

    for b in (bets or []):
        if not isinstance(b, dict):
            continue
        _index_bet_key(str(b.get("game_key") or ""), b)
        _index_bet_key(str(b.get("match_key") or ""), b)
        home = str(b.get("home_team") or "").strip()
        away = str(b.get("away_team") or "").strip()
        if home and away:
            _index_bet_key(f"{away}@{home}", b)

    rows: list[dict] = []
    seen = set()
    include_news = _ALL_SPORTS_SENTIMENT_INCLUDE_NEWS and len(target_games) <= 12

    def _is_real_prop_row(row: dict) -> bool:
        """Keep only actionable prop/game markets, not generic sentiment placeholders."""
        st = str(row.get("stat_type") or "").strip().lower()
        label = str(row.get("prop_label") or "").strip().lower()
        if not st:
            return False
        # Drop synthetic markets like "baseball_sentiment" + "Sentiment Edge".
        if st.endswith("_sentiment") or "sentiment edge" in label:
            return False
        return True

    for g in target_games:
        home = str(g.get("home_team") or "").strip()
        away = str(g.get("away_team") or "").strip()
        if not home or not away:
            continue

        game_date = str(g.get("game_date") or g.get("date") or "")
        game_time = str(g.get("game_time") or "")
        match_key = _norm_gk(g.get("match_key") or f"{away}@{home}")
        game_key = str(g.get("game_key") or _compose_game_key(away, home, g.get("game_datetime"), game_date, game_time))
        sport = _infer_sport_group(g.get("sport") or g.get("competition") or g.get("league") or "other")

        keys = {
            _norm_gk(game_key),
            match_key,
            _norm_gk(f"{home}@{away}"),
        }
        candidate_bets: list[dict] = []
        for k in keys:
            if not k:
                continue
            candidate_bets.extend(idx.get(k, []))
        candidate_bets.sort(key=lambda x: float(x.get("model_prob") or 0.0), reverse=True)

        odds_hint = None
        for cb in candidate_bets:
            if cb.get("odds_am") is not None:
                odds_hint = cb.get("odds_am")
                break

        try:
            per_game = get_game_player_sentiment_props(
                home_team=home,
                away_team=away,
                sport=sport,
                game_key=game_key,
                game_date=game_date,
                game_time=game_time,
                max_players=_ALL_SPORTS_SENTIMENT_PLAYERS_PER_GAME,
                odds_hint=odds_hint,
                include_news=include_news,
            ) or []
        except Exception as e:
            _log(f"[all-sports] sentiment extraction failed for {away}@{home}: {e}")
            continue

        for r in per_game:
            row = dict(r)
            if not _is_real_prop_row(row):
                continue
            row["sport"] = _infer_sport_group(row.get("sport") or sport)
            row["league"] = row.get("league") or g.get("league") or g.get("competition_name")
            row["competition"] = row.get("competition") or g.get("competition")
            row["competition_name"] = row.get("competition_name") or g.get("competition_name") or g.get("league")
            row["game_key"] = row.get("game_key") or game_key
            row["match_key"] = row.get("match_key") or match_key
            row["game_date"] = row.get("game_date") or game_date
            row["game_time"] = row.get("game_time") or game_time
            row["home_team"] = row.get("home_team") or home
            row["away_team"] = row.get("away_team") or away

            dedupe_key = (
                str(row.get("game_key") or ""),
                str(row.get("name") or "").strip().lower(),
                str(row.get("stat_type") or "").strip().lower(),
            )
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            rows.append(row)

    rows.sort(
        key=lambda x: (
            float(x.get("model_prob") or 0.0),
            int(x.get("sentiment_mentions") or 0),
        ),
        reverse=True,
    )

    # If social mention volume is low, backfill with model-generated player props.
    fallback_threshold = max(6, min(60, len(target_games)))
    game_sports = {
        _infer_sport_group(g.get("sport") or g.get("competition") or g.get("league") or "")
        for g in target_games
    }
    row_sports = {
        _infer_sport_group(r.get("sport") or r.get("competition") or r.get("league") or "")
        for r in rows
    }
    missing_sports = sorted(s for s in game_sports if s not in {"", "other"} and s not in row_sports)
    should_backfill = len(rows) < fallback_threshold or bool(missing_sports)

    if should_backfill:
        model_rows = _build_model_player_props_fallback(target_games, max_per_game=_ALL_SPORTS_SENTIMENT_PLAYERS_PER_GAME)
        if model_rows and missing_sports:
            model_rows = [
                r for r in model_rows
                if _infer_sport_group(r.get("sport") or r.get("competition") or r.get("league") or "") in set(missing_sports)
            ]
        if model_rows:
            seen = {
                (
                    str(r.get("game_key") or ""),
                    str(r.get("name") or "").strip().lower(),
                    str(r.get("stat_type") or "").strip().lower(),
                )
                for r in rows
            }
            for r in model_rows:
                key = (
                    str(r.get("game_key") or ""),
                    str(r.get("name") or "").strip().lower(),
                    str(r.get("stat_type") or "").strip().lower(),
                )
                if key in seen:
                    continue
                seen.add(key)
                rows.append(r)
            rows.sort(
                key=lambda x: (
                    float(x.get("model_prob") or 0.0),
                    int(x.get("sentiment_mentions") or 0),
                ),
                reverse=True,
            )
    return rows[:400]


def _build_model_player_props_fallback(games: list[dict], max_per_game: int = 6) -> list[dict]:
    """Fallback player props from model/history sources when social mentions are sparse."""
    rows: list[dict] = []
    max_per_game = max(1, min(int(max_per_game or 6), 18))
    today_str = _et_calendar_today().isoformat()
    season = _et_calendar_today().year

    def _prob_to_american(prob: float) -> int:
        p = max(0.01, min(0.99, float(prob or 0.5)))
        if p >= 0.5:
            return int(round(-p / (1.0 - p) * 100))
        return int(round((1.0 - p) / p * 100))

    # Soccer model props (uses squad/market context).
    try:
        from models.soccer_predictor import get_player_props as get_soccer_player_props

        soccer_games = [
            g for g in (games or [])
            if _infer_sport_group(g.get("sport") or g.get("competition") or g.get("league") or "") == "soccer"
        ]
        for g in soccer_games[:36]:
            props = get_soccer_player_props(g) or []
            per_game = []
            for prop in props:
                norm = _normalize_soccer_prop(g, prop, today_str)
                norm["prop_label"] = norm.get("prop_label") or str(norm.get("stat_type") or "soccer_prop").replace("_", " ").title()
                norm.setdefault("sentiment_mentions", int(norm.get("market_mentions") or 0))
                norm.setdefault("sentiment_sources", "soccer_model")
                norm.setdefault("worth_reason", "Soccer model + market popularity")
                per_game.append(norm)
            per_game.sort(key=lambda x: float(x.get("model_prob") or 0.0), reverse=True)
            rows.extend(per_game[:max_per_game])
    except Exception as e:
        _log(f"[all-sports] soccer player-prop fallback skipped: {e}")

    # Basketball player props from ESPN summary leaders/boxscore (free endpoint).
    try:
        import requests

        basketball_games = [
            g for g in (games or [])
            if _infer_sport_group(g.get("sport") or g.get("competition") or g.get("league") or "") == "basketball"
        ]

        if basketball_games:
            stat_meta = {
                "points": ("points", "Points", 8.5),
                "rebounds": ("rebounds", "Rebounds", 3.5),
                "assists": ("assists", "Assists", 2.5),
                "steals": ("steals", "Steals", 0.5),
                "blocks": ("blocks", "Blocks", 0.5),
                "three_pointers_made": ("three_pointers_made", "3PT Made", 0.5),
            }
            stat_aliases = {
                "points": "points",
                "point": "points",
                "rebounds": "rebounds",
                "rebound": "rebounds",
                "totalrebounds": "rebounds",
                "assists": "assists",
                "assist": "assists",
                "steals": "steals",
                "steal": "steals",
                "blocks": "blocks",
                "block": "blocks",
                "threepointfieldgoalsmade": "three_pointers_made",
                "3ptfieldgoalsmade": "three_pointers_made",
                "threepointersmade": "three_pointers_made",
                "3ptmade": "three_pointers_made",
                "threes": "three_pointers_made",
                "3pm": "three_pointers_made",
            }
            scoreboard_cache: dict[tuple[str, str], list[dict]] = {}
            _wnba_history_profiles: list[dict] | None = None

            def _b_num(value):
                if value is None:
                    return None
                if isinstance(value, (int, float)):
                    return float(value)
                s = str(value).strip()
                if not s:
                    return None
                m = re.search(r"-?\d+(?:\.\d+)?", s)
                if not m:
                    return None
                try:
                    return float(m.group(0))
                except Exception:
                    return None

            def _team_token(name: str) -> str:
                return re.sub(r"[^a-z0-9]+", "", str(name or "").strip().lower())

            def _team_parts(name: str) -> list[str]:
                return [p for p in re.findall(r"[a-z0-9]+", str(name or "").lower()) if len(p) >= 2]

            def _team_match(a: str, b: str) -> bool:
                ta = _team_token(a)
                tb = _team_token(b)
                if not ta or not tb:
                    return False
                if ta == tb or ta in tb or tb in ta:
                    return True
                pa = set(_team_parts(a))
                pb = set(_team_parts(b))
                if not pa or not pb:
                    return False
                overlap = pa & pb
                if len(overlap) >= 2:
                    return True
                # Nickname-only matches (e.g. "Aces" vs "Las Vegas Aces").
                if len(overlap) == 1 and (len(pa) == 1 or len(pb) == 1):
                    return True
                return False

            def _b_poisson_over(rate: float, line: float) -> float:
                lam = max(0.01, float(rate or 0.01))
                target = int(math.floor(float(line or 0.5)) + 1)
                cdf = 0.0
                for k in range(max(0, target)):
                    try:
                        cdf += math.exp(-lam) * (lam ** k) / math.factorial(k)
                    except Exception:
                        pass
                return max(0.05, min(0.95, 1.0 - cdf))

            def _league_slug(game: dict) -> str:
                src_slug = str(game.get("espn_league_path") or "").strip().lower()
                if src_slug:
                    return src_slug
                comp = str(game.get("competition") or "").lower()
                league = str(game.get("league") or game.get("competition_name") or "").lower()
                if "wnba" in comp or "wnba" in league:
                    return "wnba"
                if "womens-college-basketball" in comp or "womens_college_basketball" in comp:
                    return "womens-college-basketball"
                if "mens-college-basketball" in comp or "mens_college_basketball" in comp or "ncaab" in comp:
                    return "mens-college-basketball"
                if "nba" in comp or "nba" in league:
                    return "nba"
                return "nba"

            def _norm_stat_key(value: Any) -> str:
                return re.sub(r"[^a-z0-9]+", "", str(value or "").strip().lower())

            def _load_wnba_history_profiles() -> list[dict]:
                nonlocal _wnba_history_profiles
                if _wnba_history_profiles is not None:
                    return _wnba_history_profiles
                try:
                    from data.history_wnba import collect_wnba_history

                    hist = collect_wnba_history(days_back=365) or {}
                    player_rows = hist.get("player_rows") or []
                    buckets: dict[tuple[str, str, str], list[float]] = {}
                    for row in player_rows:
                        if not isinstance(row, dict):
                            continue
                        stat = str(row.get("stat_type") or "").strip().lower()
                        if stat not in {"points", "rebounds", "assists", "steals", "blocks"}:
                            continue
                        team = str(row.get("team") or row.get("home_team") or "").strip()
                        player = str(row.get("player_name") or row.get("name") or "").strip()
                        val = _b_num(row.get("stat_value"))
                        if not team or not player or val is None or val < 0:
                            continue
                        key = (team, player, stat)
                        buckets.setdefault(key, []).append(float(val))

                    profiles: list[dict] = []
                    for (team, player, stat), values in buckets.items():
                        if len(values) < 4:
                            continue
                        avg_val = sum(values) / max(1, len(values))
                        profiles.append(
                            {
                                "team": team,
                                "player": player,
                                "stat": stat,
                                "avg": avg_val,
                                "samples": len(values),
                            }
                        )

                    _wnba_history_profiles = profiles
                    return profiles
                except Exception:
                    _wnba_history_profiles = []
                    return []

            def _wnba_recent_rows_for_game(game: dict) -> list[dict]:
                profiles = _load_wnba_history_profiles()
                if not profiles:
                    return []

                team_names = [
                    str(game.get("home_team") or "").strip(),
                    str(game.get("away_team") or "").strip(),
                ]
                rows_for_game: list[dict] = []

                for team_name in team_names:
                    if not team_name:
                        continue
                    team_profiles = [p for p in profiles if _team_match(str(p.get("team") or ""), team_name)]
                    if not team_profiles:
                        continue

                    points_rank: dict[str, float] = {}
                    for p in team_profiles:
                        if str(p.get("stat") or "") == "points":
                            points_rank[str(p.get("player") or "")] = float(p.get("avg") or 0.0)
                    top_players = [
                        name for name, _ in sorted(points_rank.items(), key=lambda x: x[1], reverse=True)[:6]
                    ]
                    if not top_players:
                        continue

                    for player in top_players:
                        for p in team_profiles:
                            if str(p.get("player") or "") != player:
                                continue
                            stat_name = str(p.get("stat") or "").strip().lower()
                            avg_val = float(p.get("avg") or 0.0)
                            if stat_name in {"steals", "blocks"} and avg_val < 0.6:
                                continue
                            if stat_name == "assists" and avg_val < 1.8:
                                continue
                            rows_for_game.append(
                                _mk_bball_row(
                                    game,
                                    team_name,
                                    player,
                                    stat_name,
                                    avg_val,
                                    "wnba_recent_player_profile",
                                )
                            )

                deduped_rows: list[dict] = []
                seen_local = set()
                for row in rows_for_game:
                    key = (
                        str(row.get("game_key") or ""),
                        str(row.get("name") or "").strip().lower(),
                        str(row.get("stat_type") or "").strip().lower(),
                    )
                    if key in seen_local:
                        continue
                    seen_local.add(key)
                    deduped_rows.append(row)
                deduped_rows.sort(key=lambda x: float(x.get("model_prob") or 0.0), reverse=True)
                return deduped_rows

            def _scoreboard_events(slug: str, date_token: str) -> list[dict]:
                key = (slug, date_token)
                if key in scoreboard_cache:
                    return scoreboard_cache[key]
                url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/{slug}/scoreboard"
                try:
                    resp = requests.get(url, params={"dates": date_token, "limit": 200}, timeout=8)
                    if resp.status_code != 200:
                        scoreboard_cache[key] = []
                        return []
                    events = (resp.json() or {}).get("events") or []
                    scoreboard_cache[key] = events
                    return events
                except Exception:
                    scoreboard_cache[key] = []
                    return []

            def _resolve_event_id(game: dict, slug: str) -> str:
                eid = str(game.get("espn_event_id") or game.get("event_id") or "").strip()
                if eid:
                    return eid

                gd = str(game.get("game_date") or game.get("date") or "").strip()
                date_token = gd.replace("-", "") if re.match(r"^\d{4}-\d{2}-\d{2}$", gd) else _et_calendar_today().strftime("%Y%m%d")

                away_tok = _team_token(game.get("away_team") or "")
                home_tok = _team_token(game.get("home_team") or "")
                if not away_tok or not home_tok:
                    return ""

                for ev in _scoreboard_events(slug, date_token):
                    comp = (ev.get("competitions") or [{}])[0]
                    competitors = comp.get("competitors") or []
                    if len(competitors) < 2:
                        continue
                    home_c = next((c for c in competitors if str(c.get("homeAway") or "").lower() == "home"), competitors[0])
                    away_c = next((c for c in competitors if str(c.get("homeAway") or "").lower() == "away"), competitors[1] if len(competitors) > 1 else competitors[0])
                    eh_name = (home_c.get("team") or {}).get("displayName") or ""
                    ea_name = (away_c.get("team") or {}).get("displayName") or ""
                    eh = _team_token(eh_name)
                    ea = _team_token(ea_name)
                    if eh == home_tok and ea == away_tok:
                        return str(ev.get("id") or "").strip()
                    if eh == away_tok and ea == home_tok:
                        return str(ev.get("id") or "").strip()
                    game_home = str(game.get("home_team") or "")
                    game_away = str(game.get("away_team") or "")
                    if _team_match(eh_name, game_home) and _team_match(ea_name, game_away):
                        return str(ev.get("id") or "").strip()
                    if _team_match(eh_name, game_away) and _team_match(ea_name, game_home):
                        return str(ev.get("id") or "").strip()
                return ""

            def _mk_bball_team_total_row(game: dict, team_name: str, opp_name: str, slug: str, source: str) -> dict:
                league_baseline = 111.5
                if slug == "wnba":
                    league_baseline = 82.5
                elif "college" in slug:
                    league_baseline = 73.5

                team_score = _b_num(game.get("home_score") if str(game.get("home_team") or "") == team_name else game.get("away_score"))
                opp_score = _b_num(game.get("away_score") if str(game.get("home_team") or "") == team_name else game.get("home_score"))
                base_rate = league_baseline
                if team_score is not None and opp_score is not None:
                    base_rate = max(55.0, min(140.0, team_score + ((team_score - opp_score) * 0.25)))
                line_val = max(58.5, round(base_rate * 0.95 * 2.0) / 2.0)
                over_prob = _b_poisson_over(base_rate, line_val)
                model_prob = max(0.53, min(0.82, over_prob))
                odds_am = _prob_to_american(model_prob)
                dec_odds = round((1 + (odds_am / 100.0)) if odds_am > 0 else (1 + (100.0 / abs(odds_am))), 4)
                ev = (dec_odds - 1.0) * model_prob - (1.0 - model_prob)

                home = str(game.get("home_team") or "").strip()
                away = str(game.get("away_team") or "").strip()
                game_date = str(game.get("game_date") or game.get("date") or today_str)
                game_time = str(game.get("game_time") or "")
                game_key = str(game.get("game_key") or _compose_game_key(away, home, game.get("game_datetime"), game_date, game_time))
                match_key = _norm_gk(game.get("match_key") or f"{away}@{home}")

                return {
                    "sport": "basketball",
                    "name": team_name,
                    "team": team_name,
                    "prop_label": "Projected Team Points",
                    "stat_type": "team_points",
                    "line": line_val,
                    "direction": "OVER",
                    "model_prob": round(model_prob, 4),
                    "confidence": int(round(model_prob * 100)),
                    "safety_label": _safety_label_from_prob(model_prob),
                    "ev": round(ev, 4),
                    "odds_am": odds_am,
                    "dec_odds": dec_odds,
                    "game": f"{away} @ {home}",
                    "game_key": game_key,
                    "match_key": match_key,
                    "game_date": game_date,
                    "game_time": game_time,
                    "home_team": home,
                    "away_team": away,
                    "sentiment_score": 0.0,
                    "sentiment_mentions": 0,
                    "sentiment_sources": source,
                    "worth_it": model_prob >= 0.56,
                    "worth_score": round(model_prob * 100.0, 2),
                    "worth_reason": f"{team_name} team-total fallback vs {opp_name}",
                }

            def _mk_bball_row(game: dict, team_name: str, player_name: str, stat_name: str, raw_value: float, source: str) -> dict:
                stat_type, stat_label, min_line = stat_meta.get(stat_name, ("points", "Points", 8.5))
                base_rate = max(0.1, float(raw_value or min_line))
                line_val = max(min_line, round(max(min_line, base_rate * 0.88) * 2.0) / 2.0)
                over_prob = _b_poisson_over(base_rate, line_val)
                model_prob = max(0.52, min(0.88, over_prob))
                odds_am = _prob_to_american(model_prob)
                dec_odds = round((1 + (odds_am / 100.0)) if odds_am > 0 else (1 + (100.0 / abs(odds_am))), 4)
                ev = (dec_odds - 1.0) * model_prob - (1.0 - model_prob)

                home = str(game.get("home_team") or "").strip()
                away = str(game.get("away_team") or "").strip()
                game_date = str(game.get("game_date") or game.get("date") or today_str)
                game_time = str(game.get("game_time") or "")
                game_key = str(game.get("game_key") or _compose_game_key(away, home, game.get("game_datetime"), game_date, game_time))
                match_key = _norm_gk(game.get("match_key") or f"{away}@{home}")

                return {
                    "sport": "basketball",
                    "name": player_name,
                    "team": team_name or home,
                    "prop_label": f"Projected {stat_label}",
                    "stat_type": stat_type,
                    "line": line_val,
                    "direction": "OVER",
                    "model_prob": round(model_prob, 4),
                    "confidence": int(round(model_prob * 100)),
                    "safety_label": _safety_label_from_prob(model_prob),
                    "ev": round(ev, 4),
                    "odds_am": odds_am,
                    "dec_odds": dec_odds,
                    "game": f"{away} @ {home}",
                    "game_key": game_key,
                    "match_key": match_key,
                    "game_date": game_date,
                    "game_time": game_time,
                    "home_team": home,
                    "away_team": away,
                    "sentiment_score": 0.0,
                    "sentiment_mentions": 0,
                    "sentiment_sources": source,
                    "worth_it": model_prob >= 0.57,
                    "worth_score": round(model_prob * 100.0, 2),
                    "worth_reason": "ESPN team leaders + boxscore trend",
                }

            for g in basketball_games[:60]:
                home = str(g.get("home_team") or "").strip()
                away = str(g.get("away_team") or "").strip()
                if not home or not away:
                    continue

                league_slug = _league_slug(g)
                per_game_cap = max_per_game
                if league_slug == "wnba":
                    per_game_cap = max(max_per_game, min(18, max_per_game + 6))
                event_id = _resolve_event_id(g, league_slug)
                if not event_id:
                    # Keep WNBA/NBA coverage alive even when summary event IDs cannot be resolved.
                    game_rows = _wnba_recent_rows_for_game(g) if league_slug == "wnba" else []
                    if not game_rows:
                        game_rows = [
                            _mk_bball_team_total_row(g, home, away, league_slug, "espn_basketball_team_total_fallback"),
                            _mk_bball_team_total_row(g, away, home, league_slug, "espn_basketball_team_total_fallback"),
                        ]
                    rows.extend(game_rows[:per_game_cap])
                    continue

                url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/{league_slug}/summary"
                try:
                    resp = requests.get(url, params={"event": event_id}, timeout=8)
                    if resp.status_code != 200:
                        continue
                    summary = resp.json() or {}
                except Exception:
                    continue

                game_rows: list[dict] = []

                for team_bucket in (summary.get("leaders") or []):
                    team_name = str(((team_bucket.get("team") or {}).get("displayName") or "")).strip()
                    for cat in (team_bucket.get("leaders") or []):
                        stat_name = stat_aliases.get(_norm_stat_key(cat.get("name")), "")
                        if stat_name not in stat_meta:
                            continue
                        leader_list = cat.get("leaders") or []
                        if not leader_list:
                            continue
                        lead = leader_list[0] or {}
                        athlete = lead.get("athlete") or {}
                        player_name = str(athlete.get("displayName") or athlete.get("fullName") or "").strip()
                        if not player_name:
                            continue
                        raw_val = _b_num(lead.get("value"))
                        if raw_val is None:
                            raw_val = _b_num(lead.get("displayValue"))
                        if raw_val is None:
                            stats_arr = lead.get("statistics") or []
                            raw_val = _b_num((stats_arr[0] or {}).get("value")) if stats_arr else None
                        if raw_val is None or raw_val <= 0:
                            continue
                        game_rows.append(_mk_bball_row(g, team_name, player_name, stat_name, raw_val, "espn_basketball_leaders"))

                if not game_rows:
                    for team_box in ((summary.get("boxscore") or {}).get("players") or []):
                        team_name = str(((team_box.get("team") or {}).get("displayName") or "")).strip()
                        top_by_stat: dict[str, tuple[str, float]] = {}
                        for stat_block in (team_box.get("statistics") or []):
                            keys = [str(k or "").strip().lower() for k in (stat_block.get("keys") or [])]
                            key_idx = {k: i for i, k in enumerate(keys)}
                            for arow in (stat_block.get("athletes") or []):
                                athlete = arow.get("athlete") or {}
                                player_name = str(athlete.get("displayName") or athlete.get("fullName") or "").strip()
                                if not player_name:
                                    continue
                                vals = arow.get("stats") or []
                                for stat_name, aliases_to_try in {
                                    "points": ["points"],
                                    "rebounds": ["rebounds", "totalrebounds"],
                                    "assists": ["assists"],
                                    "steals": ["steals"],
                                    "blocks": ["blocks"],
                                    "three_pointers_made": ["threepointfieldgoalsmade", "threepointersmade", "3ptfieldgoalsmade", "3ptmade", "threes"],
                                }.items():
                                    idx = None
                                    for alias in aliases_to_try:
                                        idx = key_idx.get(alias)
                                        if idx is not None:
                                            break
                                    if idx is None or idx >= len(vals):
                                        continue
                                    raw_val = _b_num(vals[idx])
                                    if raw_val is None or raw_val <= 0:
                                        continue
                                    prev = top_by_stat.get(stat_name)
                                    if (not prev) or raw_val > prev[1]:
                                        top_by_stat[stat_name] = (player_name, raw_val)

                        for stat_name, payload in top_by_stat.items():
                            game_rows.append(_mk_bball_row(g, team_name, payload[0], stat_name, payload[1], "espn_basketball_boxscore"))

                if not game_rows and league_slug == "wnba":
                    game_rows = _wnba_recent_rows_for_game(g)

                if not game_rows:
                    game_rows = [
                        _mk_bball_team_total_row(g, home, away, league_slug, "espn_basketball_team_total_fallback"),
                        _mk_bball_team_total_row(g, away, home, league_slug, "espn_basketball_team_total_fallback"),
                    ]

                deduped_game_rows: list[dict] = []
                seen_game = set()
                for row in game_rows:
                    key = (
                        str(row.get("game_key") or ""),
                        str(row.get("name") or "").strip().lower(),
                        str(row.get("stat_type") or "").strip().lower(),
                    )
                    if key in seen_game:
                        continue
                    seen_game.add(key)
                    deduped_game_rows.append(row)

                deduped_game_rows.sort(key=lambda x: float(x.get("model_prob") or 0.0), reverse=True)
                rows.extend(deduped_game_rows[:per_game_cap])
    except Exception as e:
        _log(f"[all-sports] basketball player-prop fallback skipped: {e}")

    # Hockey player props from ESPN summary leaders (free endpoint).
    try:
        import requests

        hockey_games = [
            g for g in (games or [])
            if _infer_sport_group(g.get("sport") or g.get("competition") or g.get("league") or "") == "icehockey"
        ]

        if hockey_games:
            stat_meta = {
                "goals": ("goals", "Goals", 0.5),
                "assists": ("assists", "Assists", 0.5),
                "shots": ("shots_on_goal", "Shots on Goal", 1.5),
                "saves": ("saves", "Saves", 20.5),
            }
            scoreboard_cache: dict[tuple[str, str], list[dict]] = {}

            def _h_num(value):
                if value is None:
                    return None
                if isinstance(value, (int, float)):
                    return float(value)
                s = str(value).strip()
                if not s:
                    return None
                m = re.search(r"-?\d+(?:\.\d+)?", s)
                if not m:
                    return None
                try:
                    return float(m.group(0))
                except Exception:
                    return None

            def _team_token(name: str) -> str:
                return re.sub(r"[^a-z0-9]+", "", str(name or "").strip().lower())

            def _h_poisson_over(rate: float, line: float) -> float:
                lam = max(0.01, float(rate or 0.01))
                target = int(math.floor(float(line or 0.5)) + 1)
                cdf = 0.0
                for k in range(max(0, target)):
                    try:
                        cdf += math.exp(-lam) * (lam ** k) / math.factorial(k)
                    except Exception:
                        pass
                return max(0.05, min(0.95, 1.0 - cdf))

            def _scoreboard_events(slug: str, date_token: str) -> list[dict]:
                key = (slug, date_token)
                if key in scoreboard_cache:
                    return scoreboard_cache[key]
                url = f"https://site.api.espn.com/apis/site/v2/sports/hockey/{slug}/scoreboard"
                try:
                    resp = requests.get(url, params={"dates": date_token, "limit": 200}, timeout=8)
                    if resp.status_code != 200:
                        scoreboard_cache[key] = []
                        return []
                    events = (resp.json() or {}).get("events") or []
                    scoreboard_cache[key] = events
                    return events
                except Exception:
                    scoreboard_cache[key] = []
                    return []

            def _resolve_event_id(game: dict, slug: str) -> str:
                eid = str(game.get("espn_event_id") or game.get("event_id") or "").strip()
                if eid:
                    return eid
                gd = str(game.get("game_date") or game.get("date") or "").strip()
                date_token = gd.replace("-", "") if re.match(r"^\d{4}-\d{2}-\d{2}$", gd) else _et_calendar_today().strftime("%Y%m%d")

                away_tok = _team_token(game.get("away_team") or "")
                home_tok = _team_token(game.get("home_team") or "")
                if not away_tok or not home_tok:
                    return ""

                for ev in _scoreboard_events(slug, date_token):
                    comp = (ev.get("competitions") or [{}])[0]
                    competitors = comp.get("competitors") or []
                    if len(competitors) < 2:
                        continue
                    home_c = next((c for c in competitors if str(c.get("homeAway") or "").lower() == "home"), competitors[0])
                    away_c = next((c for c in competitors if str(c.get("homeAway") or "").lower() == "away"), competitors[1] if len(competitors) > 1 else competitors[0])
                    eh = _team_token((home_c.get("team") or {}).get("displayName") or "")
                    ea = _team_token((away_c.get("team") or {}).get("displayName") or "")
                    if eh == home_tok and ea == away_tok:
                        return str(ev.get("id") or "").strip()
                    if eh == away_tok and ea == home_tok:
                        return str(ev.get("id") or "").strip()
                return ""

            def _mk_hockey_row(game: dict, team_name: str, player_name: str, stat_name: str, raw_value: float, source: str) -> dict:
                stat_type, stat_label, min_line = stat_meta.get(stat_name, ("goals", "Goals", 0.5))

                # ESPN leader values can be season totals for pregame cards.
                if stat_name in {"goals", "assists", "shots"}:
                    base_rate = float(raw_value) / 82.0 if raw_value > 8 else float(raw_value)
                elif stat_name == "saves":
                    base_rate = float(raw_value) / 60.0 if raw_value > 70 else float(raw_value)
                else:
                    base_rate = float(raw_value)

                base_rate = max(0.05, base_rate)
                line_val = max(min_line, round(max(min_line, base_rate * 0.9) * 2.0) / 2.0)
                over_prob = _h_poisson_over(base_rate, line_val)
                model_prob = max(0.52, min(0.86, over_prob))
                odds_am = _prob_to_american(model_prob)
                dec_odds = round((1 + (odds_am / 100.0)) if odds_am > 0 else (1 + (100.0 / abs(odds_am))), 4)
                ev = (dec_odds - 1.0) * model_prob - (1.0 - model_prob)

                home = str(game.get("home_team") or "").strip()
                away = str(game.get("away_team") or "").strip()
                game_date = str(game.get("game_date") or game.get("date") or today_str)
                game_time = str(game.get("game_time") or "")
                game_key = str(game.get("game_key") or _compose_game_key(away, home, game.get("game_datetime"), game_date, game_time))
                match_key = _norm_gk(game.get("match_key") or f"{away}@{home}")

                return {
                    "sport": "icehockey",
                    "name": player_name,
                    "team": team_name or home,
                    "prop_label": f"Projected {stat_label}",
                    "stat_type": stat_type,
                    "line": line_val,
                    "direction": "OVER",
                    "model_prob": round(model_prob, 4),
                    "confidence": int(round(model_prob * 100)),
                    "safety_label": _safety_label_from_prob(model_prob),
                    "ev": round(ev, 4),
                    "odds_am": odds_am,
                    "dec_odds": dec_odds,
                    "game": f"{away} @ {home}",
                    "game_key": game_key,
                    "match_key": match_key,
                    "game_date": game_date,
                    "game_time": game_time,
                    "home_team": home,
                    "away_team": away,
                    "sentiment_score": 0.0,
                    "sentiment_mentions": 0,
                    "sentiment_sources": source,
                    "worth_it": model_prob >= 0.57,
                    "worth_score": round(model_prob * 100.0, 2),
                    "worth_reason": "ESPN hockey leaders trend",
                }

            for g in hockey_games[:50]:
                home = str(g.get("home_team") or "").strip()
                away = str(g.get("away_team") or "").strip()
                if not home or not away:
                    continue

                league_slug = str(g.get("espn_league_path") or "nhl").strip().lower() or "nhl"
                event_id = _resolve_event_id(g, league_slug)
                if not event_id:
                    continue

                url = f"https://site.api.espn.com/apis/site/v2/sports/hockey/{league_slug}/summary"
                try:
                    resp = requests.get(url, params={"event": event_id}, timeout=8)
                    if resp.status_code != 200:
                        continue
                    summary = resp.json() or {}
                except Exception:
                    continue

                game_rows: list[dict] = []
                for team_bucket in (summary.get("leaders") or []):
                    team_name = str(((team_bucket.get("team") or {}).get("displayName") or "")).strip()
                    for cat in (team_bucket.get("leaders") or []):
                        stat_name = str(cat.get("name") or "").strip().lower()
                        if stat_name not in stat_meta:
                            continue
                        leader_list = cat.get("leaders") or []
                        if not leader_list:
                            continue
                        lead = leader_list[0] or {}
                        athlete = lead.get("athlete") or {}
                        player_name = str(athlete.get("displayName") or athlete.get("fullName") or "").strip()
                        if not player_name:
                            continue
                        raw_val = _h_num(lead.get("value"))
                        if raw_val is None:
                            raw_val = _h_num(lead.get("displayValue"))
                        if raw_val is None or raw_val <= 0:
                            continue
                        game_rows.append(_mk_hockey_row(g, team_name, player_name, stat_name, raw_val, "espn_hockey_leaders"))

                deduped_game_rows: list[dict] = []
                seen_game = set()
                for row in game_rows:
                    key = (
                        str(row.get("game_key") or ""),
                        str(row.get("name") or "").strip().lower(),
                        str(row.get("stat_type") or "").strip().lower(),
                    )
                    if key in seen_game:
                        continue
                    seen_game.add(key)
                    deduped_game_rows.append(row)

                deduped_game_rows.sort(key=lambda x: float(x.get("model_prob") or 0.0), reverse=True)
                rows.extend(deduped_game_rows[:max_per_game])
    except Exception as e:
        _log(f"[all-sports] hockey player-prop fallback skipped: {e}")

    # NFL / American Football player props via ESPN free scoreboard leaders.
    try:
        import requests as _nfl_req

        nfl_games = [
            g for g in (games or [])
            if _infer_sport_group(g.get("sport") or g.get("competition") or g.get("league") or "") == "americanfootball"
        ]

        if nfl_games:
            nfl_stat_meta = {
                "passingYards":   ("passing_yards",  "Passing Yards",    199.5),
                "rushingYards":   ("rushing_yards",  "Rushing Yards",     49.5),
                "receivingYards": ("receiving_yards","Receiving Yards",   39.5),
                "receptions":     ("receptions",     "Receptions",         3.5),
                "passingTouchdowns": ("passing_touchdowns", "Passing TDs", 0.5),
                "rushingTouchdowns": ("rushing_touchdowns", "Rushing TDs", 0.5),
            }
            nfl_stat_aliases = {
                "passingyards":   "passingYards",
                "passingyard":    "passingYards",
                "rushingyards":   "rushingYards",
                "rushingyard":    "rushingYards",
                "receivingyards": "receivingYards",
                "receivingyard":  "receivingYards",
                "receptions":     "receptions",
                "reception":      "receptions",
                "passingtouchdowns": "passingTouchdowns",
                "passingtd":      "passingTouchdowns",
                "rushingtouchdowns": "rushingTouchdowns",
                "rushingtd":      "rushingTouchdowns",
            }

            def _nfl_league_slug(game: dict) -> str:
                src = str(game.get("espn_league_path") or "").strip().lower()
                if src:
                    return src
                comp = str(game.get("competition") or "").lower()
                if "ncaaf" in comp or "college" in comp:
                    return "college-football"
                return "nfl"

            def _nfl_norm_stat(k: str) -> str:
                return re.sub(r"[^a-z0-9]+", "", str(k or "").strip().lower())

            def _mk_nfl_row(game: dict, team_name: str, player_name: str,
                            stat_key: str, raw_value: float, source: str) -> dict:
                stat_type, prop_lbl, default_line = nfl_stat_meta.get(stat_key, (stat_key, stat_key, raw_value * 0.8))
                line = max(0.5, round(raw_value * 0.85 / 0.5) * 0.5)
                import math as _m
                lam = max(0.01, float(raw_value or 0.01))
                target = int(_m.floor(float(line)) + 1)
                cdf = 0.0
                for k in range(max(0, target)):
                    try:
                        cdf += _m.exp(-lam) * (lam ** k) / _m.factorial(k)
                    except Exception:
                        pass
                over_prob = max(0.05, min(0.95, 1.0 - cdf))
                model_prob = max(0.01, min(0.99, over_prob))
                odds_am_v = _prob_to_american(model_prob)
                dec_odds_v = round((1 + (odds_am_v / 100.0)) if odds_am_v > 0 else (1 + (100.0 / abs(odds_am_v))), 4)
                ev_v = (dec_odds_v - 1.0) * model_prob - (1.0 - model_prob)
                home = str(game.get("home_team") or "").strip()
                away = str(game.get("away_team") or "").strip()
                gd = str(game.get("game_date") or game.get("date") or today_str)
                gt = str(game.get("game_time") or "")
                gk = str(game.get("game_key") or _compose_game_key(away, home, game.get("game_datetime"), gd, gt))
                mk = _norm_gk(game.get("match_key") or f"{away}@{home}")
                return {
                    "sport": "americanfootball",
                    "name": player_name,
                    "team": team_name,
                    "prop_label": prop_lbl,
                    "stat_type": stat_type,
                    "line": line,
                    "direction": "OVER",
                    "model_prob": round(model_prob, 4),
                    "confidence": int(round(model_prob * 100)),
                    "safety_label": _safety_label_from_prob(model_prob),
                    "ev": round(ev_v, 4),
                    "odds_am": odds_am_v,
                    "dec_odds": dec_odds_v,
                    "game": f"{away} @ {home}",
                    "game_key": gk,
                    "match_key": mk,
                    "game_date": gd,
                    "game_time": gt,
                    "home_team": home,
                    "away_team": away,
                    "sentiment_score": 0.0,
                    "sentiment_mentions": 0,
                    "sentiment_sources": source,
                    "worth_it": model_prob >= 0.54,
                    "worth_score": round(model_prob * 100.0, 2),
                    "worth_reason": f"NFL/NCAAF season avg ({stat_type.replace('_', ' ')})",
                }

            nfl_scoreboard_cache: dict[tuple[str, str], list[dict]] = {}

            def _nfl_scoreboard_events(slug: str, date_token: str) -> list[dict]:
                key = (slug, date_token)
                if key in nfl_scoreboard_cache:
                    return nfl_scoreboard_cache[key]
                url = f"https://site.api.espn.com/apis/site/v2/sports/football/{slug}/scoreboard"
                try:
                    resp = _nfl_req.get(url, params={"dates": date_token, "limit": 200}, timeout=8)
                    evs = (resp.json() or {}).get("events") or [] if resp.status_code == 200 else []
                    nfl_scoreboard_cache[key] = evs
                    return evs
                except Exception:
                    nfl_scoreboard_cache[key] = []
                    return []

            def _nfl_team_leaders(event_id: str, slug: str) -> list[tuple[str, str, str, float]]:
                """Fetch per-team stat leaders from ESPN event summary."""
                url = f"https://site.api.espn.com/apis/site/v2/sports/football/{slug}/summary"
                try:
                    resp = _nfl_req.get(url, params={"event": event_id}, timeout=10)
                    if resp.status_code != 200:
                        return []
                    data = resp.json() or {}
                    leaders_data = data.get("leaders") or []
                    results: list[tuple[str, str, str, float]] = []
                    for team_leaders in leaders_data:
                        team_name = str((team_leaders.get("team") or {}).get("displayName") or "").strip()
                        for category in (team_leaders.get("leaders") or []):
                            stat_key_raw = str(category.get("name") or "").strip()
                            norm_key = _nfl_norm_stat(stat_key_raw)
                            mapped_key = nfl_stat_aliases.get(norm_key, "")
                            if not mapped_key or mapped_key not in nfl_stat_meta:
                                continue
                            for leader in ((category.get("leaders") or [])[:3]):
                                pname = str((leader.get("athlete") or {}).get("displayName") or "").strip()
                                val_raw = leader.get("value")
                                try:
                                    val = float(val_raw or 0)
                                except Exception:
                                    val = 0.0
                                if pname and val > 0:
                                    results.append((team_name, pname, mapped_key, val))
                    return results
                except Exception:
                    return []

            for g in nfl_games[:30]:
                home = str(g.get("home_team") or "").strip()
                away = str(g.get("away_team") or "").strip()
                if not home or not away:
                    continue
                slug = _nfl_league_slug(g)
                gd = str(g.get("game_date") or g.get("date") or "").strip()
                date_token = gd.replace("-", "") if re.match(r"^\d{4}-\d{2}-\d{2}$", gd) else _et_calendar_today().strftime("%Y%m%d")

                # Try to resolve ESPN event ID
                eid = str(g.get("espn_event_id") or g.get("event_id") or "").strip()
                if not eid:
                    for ev in _nfl_scoreboard_events(slug, date_token):
                        comp = (ev.get("competitions") or [{}])[0]
                        competitors = comp.get("competitors") or []
                        if len(competitors) < 2:
                            continue
                        h_c = next((c for c in competitors if str(c.get("homeAway") or "").lower() == "home"), competitors[0])
                        eh = str((h_c.get("team") or {}).get("displayName") or "").strip()
                        if _team_token(home) and (_team_token(eh) == _team_token(home) or _team_token(home) in _team_token(eh) or _team_token(eh) in _team_token(home)):
                            eid = str(ev.get("id") or "").strip()
                            break

                game_rows: list[dict] = []
                seen_game: set = set()
                if eid:
                    for team_name, player_name, stat_key, raw_val in _nfl_team_leaders(eid, slug):
                        row = _mk_nfl_row(g, team_name, player_name, stat_key, raw_val, "espn_nfl_leaders")
                        dk = (str(row.get("game_key") or ""), player_name.lower(), stat_key)
                        if dk not in seen_game:
                            seen_game.add(dk)
                            game_rows.append(row)
                game_rows.sort(key=lambda x: float(x.get("model_prob") or 0.0), reverse=True)
                rows.extend(game_rows[:max_per_game])
    except Exception as e:
        _log(f"[all-sports] NFL player-prop fallback skipped: {e}")

    # MLB historical hitter props across recent seasons.
    try:
        from data import mlb_fetcher as _mlb_fetcher

        mlb_games = [
            g for g in (games or [])
            if _infer_sport_group(g.get("sport") or g.get("competition") or g.get("league") or "") in {"baseball", "mlb"}
        ]
        if mlb_games and getattr(_mlb_fetcher, "PYBASEBALL_OK", False):
            by_match = {}
            batch_games = []
            for g in mlb_games[:40]:
                home = str(g.get("home_team") or "").strip()
                away = str(g.get("away_team") or "").strip()
                if not home or not away:
                    continue
                match_key = _norm_gk(g.get("match_key") or f"{away}@{home}")
                by_match[match_key] = g
                batch_games.append({
                    "home_team": home,
                    "away_team": away,
                    "game_time": g.get("game_time") or "",
                    "date": g.get("game_date") or g.get("date") or today_str,
                    "game_date": g.get("game_date") or g.get("date") or today_str,
                    "game_datetime": g.get("game_datetime") or "",
                    "match_key": match_key,
                })

            raw = _mlb_fetcher.get_hitter_props_batch(batch_games, season=season) or []
            per_game: dict[str, list[dict]] = {}
            for p in raw:
                game_txt = str(p.get("game") or "").strip()
                mk = _norm_gk(game_txt.replace(" @ ", "@")) if game_txt else ""
                g = by_match.get(mk)
                if not g:
                    continue

                over_p = float(p.get("over_prob") or 0.5)
                direction = "OVER"
                model_prob = over_p
                model_prob = max(0.01, min(0.99, model_prob))
                odds_am = _prob_to_american(model_prob)
                dec_odds = round((1 + (odds_am / 100.0)) if odds_am > 0 else (1 + (100.0 / abs(odds_am))), 4)
                ev = (dec_odds - 1.0) * model_prob - (1.0 - model_prob)

                home = str(g.get("home_team") or "").strip()
                away = str(g.get("away_team") or "").strip()
                game_date = str(g.get("game_date") or g.get("date") or today_str)
                game_time = str(g.get("game_time") or "")
                game_key = str(g.get("game_key") or _compose_game_key(away, home, g.get("game_datetime"), game_date, game_time))
                stat_type = str(p.get("stat_type") or "hits")

                row = {
                    "sport": "baseball",
                    "name": p.get("name"),
                    "team": p.get("team") or home,
                    "prop_label": f"Historical {stat_type.replace('_', ' ').title()}",
                    "stat_type": stat_type,
                    "line": p.get("line"),
                    "direction": direction,
                    "model_prob": round(model_prob, 4),
                    "confidence": int(round(model_prob * 100)),
                    "safety_label": _safety_label_from_prob(model_prob),
                    "ev": round(ev, 4),
                    "odds_am": odds_am,
                    "dec_odds": dec_odds,
                    "game": f"{away} @ {home}",
                    "game_key": game_key,
                    "match_key": mk,
                    "game_date": game_date,
                    "game_time": game_time,
                    "home_team": home,
                    "away_team": away,
                    "sentiment_score": 0.0,
                    "sentiment_mentions": 0,
                    "sentiment_sources": "mlb_historical_model",
                    "worth_it": model_prob >= 0.57,
                    "worth_score": round(model_prob * 100.0, 2),
                    "worth_reason": "Historical multi-season batter profile",
                }
                per_game.setdefault(game_key, []).append(row)

            for gk, arr in per_game.items():
                arr.sort(key=lambda x: float(x.get("model_prob") or 0.0), reverse=True)
                rows.extend(arr[:max_per_game])

        existing_mlb_rows = [r for r in rows if _infer_sport_group(r.get("sport") or "") in {"baseball", "mlb"}]
        if mlb_games and len(existing_mlb_rows) < max(8, len(mlb_games)):
            try:
                from data.sportsdata_fetcher import get_mlb_player_season_stats, get_mlb_teams

                def _norm_team_name(name: str) -> str:
                    return re.sub(r"[^a-z0-9]+", "", str(name or "").strip().lower())

                def _poisson_over_prob(rate: float, line: float) -> float:
                    lam = max(0.01, float(rate or 0.01))
                    target = int(math.floor(float(line or 0.5)) + 1)
                    cdf = 0.0
                    for k in range(max(0, target)):
                        try:
                            cdf += math.exp(-lam) * (lam ** k) / math.factorial(k)
                        except Exception:
                            pass
                    return max(0.05, min(0.95, 1.0 - cdf))

                team_rows = get_mlb_teams() or []
                alias_to_key: dict[str, str] = {}
                for t in team_rows:
                    key = str(t.get("Key") or "").strip().upper()
                    city = str(t.get("City") or "").strip()
                    name = str(t.get("Name") or "").strip()
                    full = f"{city} {name}".strip()
                    for alias in (full, name):
                        a = _norm_team_name(alias)
                        if a and key:
                            alias_to_key[a] = key

                def _resolve_team_key(team_name: str) -> str:
                    n = _norm_team_name(team_name)
                    if not n:
                        return ""
                    if n in alias_to_key:
                        return alias_to_key[n]
                    for alias, key in alias_to_key.items():
                        if n and (n in alias or alias in n):
                            return key
                    return ""

                player_stats = get_mlb_player_season_stats(season=season) or []
                if not player_stats:
                    player_stats = get_mlb_player_season_stats() or []

                by_team: dict[str, list[dict]] = {}
                for p in player_stats:
                    tkey = str(p.get("Team") or "").strip().upper()
                    if not tkey:
                        continue
                    by_team.setdefault(tkey, []).append(p)

                def _mk_prop_row(game: dict, team_name: str, player_name: str, stat_type: str, prop_label: str,
                                 line_val: float, over_prob: float, direction: str, source: str) -> dict:
                    model_prob = over_prob if direction == "OVER" else (1.0 - over_prob)
                    model_prob = max(0.01, min(0.99, model_prob))
                    odds_am = _prob_to_american(model_prob)
                    dec_odds = round((1 + (odds_am / 100.0)) if odds_am > 0 else (1 + (100.0 / abs(odds_am))), 4)
                    ev = (dec_odds - 1.0) * model_prob - (1.0 - model_prob)
                    home = str(game.get("home_team") or "").strip()
                    away = str(game.get("away_team") or "").strip()
                    game_date = str(game.get("game_date") or game.get("date") or today_str)
                    game_time = str(game.get("game_time") or "")
                    game_key = str(game.get("game_key") or _compose_game_key(away, home, game.get("game_datetime"), game_date, game_time))
                    match_key = _norm_gk(game.get("match_key") or f"{away}@{home}")
                    return {
                        "sport": "baseball",
                        "name": player_name,
                        "team": team_name,
                        "prop_label": prop_label,
                        "stat_type": stat_type,
                        "line": line_val,
                        "direction": direction,
                        "model_prob": round(model_prob, 4),
                        "confidence": int(round(model_prob * 100)),
                        "safety_label": _safety_label_from_prob(model_prob),
                        "ev": round(ev, 4),
                        "odds_am": odds_am,
                        "dec_odds": dec_odds,
                        "game": f"{away} @ {home}",
                        "game_key": game_key,
                        "match_key": match_key,
                        "game_date": game_date,
                        "game_time": game_time,
                        "home_team": home,
                        "away_team": away,
                        "sentiment_score": 0.0,
                        "sentiment_mentions": 0,
                        "sentiment_sources": source,
                        "worth_it": model_prob >= 0.57,
                        "worth_score": round(model_prob * 100.0, 2),
                        "worth_reason": "Historical MLB season profile",
                    }

                for g in mlb_games[:40]:
                    home = str(g.get("home_team") or "").strip()
                    away = str(g.get("away_team") or "").strip()
                    if not home or not away:
                        continue
                    hk = _resolve_team_key(home)
                    ak = _resolve_team_key(away)
                    game_rows: list[dict] = []

                    for team_name, tkey in ((home, hk), (away, ak)):
                        if not tkey:
                            continue
                        candidates = by_team.get(tkey, [])
                        if not candidates:
                            continue

                        hitters = [p for p in candidates if str(p.get("PositionCategory") or "").upper() != "P"]
                        pitchers = [p for p in candidates if str(p.get("PositionCategory") or "").upper() == "P"]

                        hitters.sort(key=lambda x: float(x.get("FantasyPoints") or 0.0), reverse=True)
                        for p in hitters[:3]:
                            pname = str(p.get("Name") or "").strip()
                            games_played = float(p.get("Games") or 0.0)
                            if not pname or games_played < 5:
                                continue
                            stats = [
                                ("hits", "Historical Hits", float(p.get("Hits") or 0.0), 0.5),
                                ("home_runs", "Historical Home Runs", float(p.get("HomeRuns") or 0.0), 0.5),
                                ("rbi", "Historical RBI", float(p.get("RunsBattedIn") or 0.0), 0.5),
                                ("runs", "Historical Runs", float(p.get("Runs") or 0.0), 0.5),
                                ("stolen_bases", "Historical Stolen Bases", float(p.get("StolenBases") or 0.0), 0.5),
                                ("total_bases", "Historical Total Bases", float(p.get("TotalBases") or 0.0), 1.5),
                            ]
                            best_prop = None
                            best_prob = 0.0
                            for stat_type, label, total_val, line_val in stats:
                                if total_val <= 0:
                                    continue
                                rate = total_val / max(games_played, 1.0)
                                over_prob = _poisson_over_prob(rate, line_val)
                                if over_prob >= 0.52 and over_prob > best_prob:
                                    best_prob = over_prob
                                    best_prop = (stat_type, label, line_val, over_prob)
                            if best_prop:
                                game_rows.append(
                                    _mk_prop_row(
                                        g,
                                        team_name,
                                        pname,
                                        best_prop[0],
                                        best_prop[1],
                                        best_prop[2],
                                        best_prop[3],
                                        "OVER",
                                        "mlb_sportsdata_historical",
                                    )
                                )

                        pitchers.sort(key=lambda x: float(x.get("PitchingStrikeouts") or 0.0), reverse=True)
                        for p in pitchers[:1]:
                            pname = str(p.get("Name") or "").strip()
                            games_played = float(p.get("Games") or p.get("Started") or 0.0)
                            strikeouts = float(p.get("PitchingStrikeouts") or 0.0)
                            if not pname or games_played < 3 or strikeouts <= 0:
                                continue
                            k_rate = strikeouts / max(games_played, 1.0)
                            line_val = max(3.5, round(k_rate * 0.85 * 2.0) / 2.0)
                            over_prob = _poisson_over_prob(k_rate, line_val)
                            if over_prob >= 0.52:
                                game_rows.append(
                                    _mk_prop_row(
                                        g,
                                        team_name,
                                        pname,
                                        "strikeouts",
                                        "Historical Pitcher Strikeouts",
                                        line_val,
                                        over_prob,
                                        "OVER",
                                        "mlb_sportsdata_historical",
                                    )
                                )

                    game_rows.sort(key=lambda x: float(x.get("model_prob") or 0.0), reverse=True)
                    rows.extend(game_rows[:max_per_game])
            except Exception as se:
                _log(f"[all-sports] sportsdata MLB fallback skipped: {se}")
        elif mlb_games:
            _log("[all-sports] pybaseball unavailable — skipping bulk hitter fallback")
    except Exception as e:
        _log(f"[all-sports] mlb player-prop fallback skipped: {e}")

    # Golf tournament/player markets from outright + matchup odds.
    try:
        golf_games = [
            g for g in (games or [])
            if _infer_sport_group(g.get("sport") or g.get("competition") or g.get("league") or "") == "golf"
        ]
        if golf_games:
            golf_market_types = {
                "outrights": ("outright_winner", "Tournament Winner"),
                "h2h": ("head_to_head", "Head-to-Head"),
                "winner": ("winner", "Winner"),
                "top_5": ("top_5_finish", "Top 5 Finish"),
                "top_10": ("top_10_finish", "Top 10 Finish"),
                "top_20": ("top_20_finish", "Top 20 Finish"),
            }
            comp_codes = []
            for g in golf_games:
                code = str(g.get("competition") or "").strip()
                if code and code not in comp_codes:
                    comp_codes.append(code)

            max_events = max(2, min(16, len(golf_games) * 2))
            for code in comp_codes[:8]:
                try:
                    events = get_live_odds(code, markets="outrights,h2h") or []
                except Exception:
                    continue
                for ev in events[:max_events]:
                    game_datetime = _event_datetime_value(ev)
                    game_date, game_time = _datetime_to_et_parts(game_datetime)
                    if game_date and game_date < today_str:
                        continue

                    event_title = str(
                        ev.get("event_name")
                        or ev.get("title")
                        or ev.get("description")
                        or code
                    ).strip() or code

                    books = ev.get("bookmakers") or []
                    if not books:
                        continue
                    markets = books[0].get("markets") or []
                    if not isinstance(markets, list):
                        continue

                    home = str(ev.get("home_team") or "").strip()
                    away = str(ev.get("away_team") or "").strip()
                    if not _is_actionable_individual_matchup(home, away, event_title):
                        candidate_names: list[str] = []
                        for mk_probe in markets:
                            mk_key_probe = str((mk_probe or {}).get("key") or "").strip().lower()
                            if mk_key_probe not in {"h2h", "winner", "outrights", "top_5", "top_10", "top_20"}:
                                continue
                            for out_probe in (mk_probe.get("outcomes") or []):
                                nm = str((out_probe or {}).get("name") or (out_probe or {}).get("description") or "").strip()
                                if not nm or _is_placeholder_participant(nm, event_title):
                                    continue
                                if nm not in candidate_names:
                                    candidate_names.append(nm)
                            if len(candidate_names) >= 2:
                                break
                        if len(candidate_names) >= 2:
                            home, away = candidate_names[0], candidate_names[1]
                    if not _is_actionable_individual_matchup(home, away, event_title):
                        continue

                    game_key = _compose_game_key(away, home, game_datetime, game_date, game_time)
                    match_key = _norm_gk(f"{away}@{home}")

                    game_rows = []
                    for market in markets:
                        mk = str(market.get("key") or "").strip().lower()
                        meta = golf_market_types.get(mk)
                        if not meta:
                            continue
                        stat_type, prop_label = meta

                        priced = []
                        for out in (market.get("outcomes") or []):
                            name = str(out.get("name") or out.get("description") or "").strip()
                            implied = _prob_from_american(out.get("price"))
                            if not name or implied is None:
                                continue
                            try:
                                priced.append((name, int(float(out.get("price"))), float(implied), out))
                            except Exception:
                                continue
                        if len(priced) < 2:
                            continue

                        vig = sum(x[2] for x in priced)
                        normalized = []
                        for name, odds_am, imp, out in priced:
                            p = imp / vig if vig > 0 else imp
                            normalized.append((name, odds_am, max(0.01, min(0.99, p)), out))

                        top_n = 8 if mk in {"outrights", "winner", "top_5", "top_10", "top_20"} else 3
                        for name, odds_am, model_prob, out in sorted(normalized, key=lambda x: x[2], reverse=True)[:top_n]:
                            dec_odds = round((1 + (odds_am / 100.0)) if odds_am > 0 else (1 + (100.0 / abs(odds_am))), 4)
                            ev = (dec_odds - 1.0) * model_prob - (1.0 - model_prob)
                            game_rows.append({
                                "sport": "golf",
                                "name": name,
                                "team": name,
                                "prop_label": prop_label,
                                "stat_type": stat_type,
                                "line": (out.get("point") if isinstance(out, dict) else None),
                                "direction": "OVER",
                                "model_prob": round(model_prob, 4),
                                "confidence": int(round(model_prob * 100)),
                                "safety_label": _safety_label_from_prob(model_prob),
                                "ev": round(ev, 4),
                                "odds_am": odds_am,
                                "dec_odds": dec_odds,
                                "game": event_title,
                                "game_key": game_key,
                                "match_key": match_key,
                                "game_date": game_date,
                                "game_time": game_time,
                                "home_team": home,
                                "away_team": away,
                                "league": code,
                                "competition": code,
                                "competition_name": code,
                                "sentiment_score": 0.0,
                                "sentiment_mentions": 0,
                                "sentiment_sources": "golf_market_model",
                                "worth_it": model_prob >= 0.08,
                                "worth_score": round(model_prob * 100.0, 2),
                                "worth_reason": f"No-vig golf {prop_label.lower()} market edge",
                            })

                    game_rows.sort(key=lambda x: float(x.get("model_prob") or 0.0), reverse=True)
                    golf_per_game_cap = max(max_per_game, min(18, max_per_game * 2))
                    rows.extend(game_rows[:golf_per_game_cap])
    except Exception as e:
        _log(f"[all-sports] golf player-prop fallback skipped: {e}")

    # Tennis/combat/single-person sports player-card style markets.
    try:
        single_games = [
            g for g in (games or [])
            if _infer_sport_group(g.get("sport") or g.get("competition") or g.get("league") or "")
            in {"tennis", "mma", "boxing", "motorsports", "cricket", "golf"}
        ]
        if single_games:
            market_profiles = {
                "tennis": "h2h,spreads,totals,outrights",
                "mma": "h2h,totals,outrights",
                "boxing": "h2h,totals,outrights",
                "motorsports": "h2h,outrights,winner,top_3,top_5,top_10",
                "cricket": "h2h,spreads,totals,outrights",
                "golf": "outrights,h2h,winner,top_5,top_10,top_20",
            }
            market_type_map = {
                "h2h": "match_winner",
                "spreads": "handicap",
                "totals": "total",
                "outrights": "outright_winner",
                "winner": "winner",
                "top_3": "top_3_finish",
                "top_5": "top_5_finish",
                "top_10": "top_10_finish",
                "top_20": "top_20_finish",
            }
            by_comp: dict[str, list[dict]] = {}
            for g in single_games:
                comp = str(g.get("competition") or "").strip()
                if not comp:
                    continue
                by_comp.setdefault(comp, []).append(g)

            for comp, comp_games in list(by_comp.items())[:12]:
                sport_group = _infer_sport_group(comp_games[0].get("sport") or comp)
                profile = market_profiles.get(sport_group, "h2h,outrights")
                try:
                    events = get_live_odds(comp, markets=profile) or []
                except Exception:
                    continue

                max_events = max(2, min(20, len(comp_games) * 3))
                for ev in events[:max_events]:
                    game_datetime = _event_datetime_value(ev)
                    game_date, game_time = _datetime_to_et_parts(game_datetime)
                    if game_date and game_date < today_str:
                        continue

                    event_title = str(
                        ev.get("event_name")
                        or ev.get("title")
                        or ev.get("description")
                        or comp
                    ).strip() or comp
                    home = str(ev.get("home_team") or "").strip()
                    away = str(ev.get("away_team") or "").strip()

                    books = ev.get("bookmakers") or []
                    if not books:
                        continue
                    markets = books[0].get("markets") or []
                    if not isinstance(markets, list):
                        continue
                    if sport_group in {"tennis", "golf", "mma", "boxing", "motorsports", "cricket"} and not _is_actionable_individual_matchup(home, away, event_title):
                        left, right = _derive_individual_matchup(event_title)
                        if left and right:
                            home, away = left, right
                    if sport_group in {"tennis", "golf", "mma", "boxing", "motorsports", "cricket"} and not _is_actionable_individual_matchup(home, away, event_title):
                        outcome_names: list[str] = []
                        for mk_probe in markets:
                            mk_key_probe = str((mk_probe or {}).get("key") or "").strip().lower()
                            if mk_key_probe not in {"h2h", "winner", "outrights", "top_3", "top_5", "top_10", "top_20"}:
                                continue
                            for out_probe in (mk_probe.get("outcomes") or []):
                                nm = str((out_probe or {}).get("name") or (out_probe or {}).get("description") or "").strip()
                                if not nm or _is_placeholder_participant(nm, event_title):
                                    continue
                                if nm not in outcome_names:
                                    outcome_names.append(nm)
                            if len(outcome_names) >= 2:
                                break
                        if len(outcome_names) >= 2:
                            home, away = outcome_names[0], outcome_names[1]
                    if sport_group in {"tennis", "golf", "mma", "boxing", "motorsports", "cricket"} and not _is_actionable_individual_matchup(home, away, event_title):
                        continue
                    home = home or event_title
                    away = away or "Field"
                    game_key = _compose_game_key(away, home, game_datetime, game_date, game_time)
                    match_key = _norm_gk(f"{away}@{home}")

                    game_rows = []
                    for market in markets:
                        mk = str(market.get("key") or "").strip().lower()
                        outcomes = market.get("outcomes") or []
                        priced = []
                        for out in outcomes:
                            name = str(out.get("name") or out.get("description") or "").strip()
                            implied = _prob_from_american(out.get("price"))
                            if not name or implied is None:
                                continue
                            try:
                                priced.append((name, int(float(out.get("price"))), float(implied), out))
                            except Exception:
                                continue
                        if len(priced) < 2:
                            continue

                        vig = sum(x[2] for x in priced)
                        norm = []
                        for name, odds_am, imp, out in priced:
                            p = imp / vig if vig > 0 else imp
                            norm.append((name, odds_am, max(0.01, min(0.99, p)), out))

                        top_n = 8 if mk in {"outrights", "winner", "top_3", "top_5", "top_10", "top_20"} else 3
                        # Golf outrights often have 50+ players — raise cap so favourites don't crowd out value picks.
                        if sport_group == "golf" and mk in {"outrights", "winner", "top_5", "top_10", "top_20"}:
                            top_n = 16
                        for name, odds_am, model_prob, out in sorted(norm, key=lambda x: x[2], reverse=True)[:top_n]:
                            dec_odds = round((1 + (odds_am / 100.0)) if odds_am > 0 else (1 + (100.0 / abs(odds_am))), 4)
                            ev = (dec_odds - 1.0) * model_prob - (1.0 - model_prob)
                            prop_type = market_type_map.get(mk, mk or "match_winner")
                            game_rows.append({
                                "sport": sport_group,
                                "name": name,
                                "team": name,
                                "prop_label": prop_type.replace("_", " ").title(),
                                "stat_type": prop_type,
                                "line": (out.get("point") if isinstance(out, dict) else None),
                                "direction": "OVER",
                                "model_prob": round(model_prob, 4),
                                "confidence": int(round(model_prob * 100)),
                                "safety_label": _safety_label_from_prob(model_prob),
                                "ev": round(ev, 4),
                                "odds_am": odds_am,
                                "dec_odds": dec_odds,
                                "game": event_title,
                                "game_key": game_key,
                                "match_key": match_key,
                                "game_date": game_date,
                                "game_time": game_time,
                                "home_team": home,
                                "away_team": away,
                                "league": comp,
                                "competition": comp,
                                "competition_name": comp,
                                "sentiment_score": 0.0,
                                "sentiment_mentions": 0,
                                "sentiment_sources": "single_sport_market_model",
                                "worth_it": model_prob >= 0.08,
                                "worth_score": round(model_prob * 100.0, 2),
                                "worth_reason": f"No-vig {sport_group} {prop_type.replace('_', ' ')} market edge",
                            })

                    game_rows.sort(key=lambda x: float(x.get("model_prob") or 0.0), reverse=True)
                    # Golf and outright-heavy sports need a higher per-event cap.
                    per_event_cap = max(max_per_game, min(18, max_per_game * 2)) if sport_group == "golf" else max_per_game
                    rows.extend(game_rows[:per_event_cap])
    except Exception as e:
        _log(f"[all-sports] single-sport market fallback skipped: {e}")

    # Last-resort MLB starter props (does not require pybaseball dependencies).
    if not rows:
        try:
            from data.sentiment import get_player_prop_signal

            starter_rows: list[dict] = []
            for g in (games or []):
                sport = _infer_sport_group(g.get("sport") or g.get("competition") or g.get("league") or "")
                if sport not in {"baseball", "mlb"}:
                    continue
                home = str(g.get("home_team") or "").strip()
                away = str(g.get("away_team") or "").strip()
                game_date = str(g.get("game_date") or g.get("date") or today_str)
                game_time = str(g.get("game_time") or "")
                game_key = str(g.get("game_key") or _compose_game_key(away, home, g.get("game_datetime"), game_date, game_time))
                match_key = _norm_gk(g.get("match_key") or f"{away}@{home}")

                starters = [
                    (str(g.get("home_starter") or "").strip(), home),
                    (str(g.get("away_starter") or "").strip(), away),
                ]
                for starter_name, team in starters:
                    if not starter_name or starter_name.upper() == "TBD":
                        continue

                    line = 4.5
                    direction = "OVER"
                    chosen_prob = 0.55
                    sent_score = 0.0
                    reason = "Starter historical + sentiment strikeout profile"

                    try:
                        signal = get_player_prop_signal(starter_name, "strikeouts", line)
                        over_prob = float(signal.get("probability") or 0.5)
                        direction = str(signal.get("direction") or "OVER").upper()
                        chosen_prob = over_prob if direction == "OVER" else (1.0 - over_prob)
                        sent_score = float(signal.get("sentiment_score") or 0.0)
                        if signal.get("rationale"):
                            reason = str(signal.get("rationale"))[:220]
                    except Exception:
                        pass

                    chosen_prob = max(0.51, min(0.88, float(chosen_prob)))
                    odds_am = _prob_to_american(chosen_prob)
                    dec_odds = round((1 + (odds_am / 100.0)) if odds_am > 0 else (1 + (100.0 / abs(odds_am))), 4)
                    ev = (dec_odds - 1.0) * chosen_prob - (1.0 - chosen_prob)

                    starter_rows.append({
                        "sport": "baseball",
                        "name": starter_name,
                        "team": team,
                        "prop_label": "Pitcher Strikeouts",
                        "stat_type": "strikeouts",
                        "line": line,
                        "direction": direction,
                        "model_prob": round(chosen_prob, 4),
                        "confidence": int(round(chosen_prob * 100)),
                        "safety_label": _safety_label_from_prob(chosen_prob),
                        "ev": round(ev, 4),
                        "odds_am": odds_am,
                        "dec_odds": dec_odds,
                        "game": f"{away} @ {home}",
                        "game_key": game_key,
                        "match_key": match_key,
                        "game_date": game_date,
                        "game_time": game_time,
                        "home_team": home,
                        "away_team": away,
                        "sentiment_score": round(sent_score, 4),
                        "sentiment_mentions": 0,
                        "sentiment_sources": "historical_trends,sentiment",
                        "worth_it": chosen_prob >= 0.56,
                        "worth_score": round(chosen_prob * 100.0, 2),
                        "worth_reason": reason,
                    })

            starter_rows.sort(key=lambda x: float(x.get("model_prob") or 0.0), reverse=True)
            rows.extend(starter_rows)
        except Exception as e:
            _log(f"[all-sports] starter-prop fallback skipped: {e}")

    # Tennis match props — games won, aces, sets — from ESPN tournament leaders.
    try:
        import math as _tnmath

        tennis_games = [
            g for g in (games or [])
            if _infer_sport_group(g.get("sport") or g.get("competition") or g.get("league") or "") == "tennis"
        ]
        if tennis_games:
            # Stat profile: (stat_type, prop_label, typical_line, typical_rate)
            _tn_stat_profiles = [
                ("aces",         "Aces",          3.5,  5.0),
                ("games_won",    "Games Won",      9.5, 12.0),
                ("first_serves", "1st Serve %",   54.5, 60.0),
            ]

            def _mk_tennis_row(game: dict, player_name: str, side_team: str,
                               stat_type: str, prop_label: str, line: float,
                               model_prob: float, source: str) -> dict:
                model_prob = max(0.01, min(0.99, float(model_prob)))
                ods = _prob_to_american(model_prob)
                dec = round((1 + (ods / 100.0)) if ods > 0 else (1 + (100.0 / abs(ods))), 4)
                ev_r = (dec - 1.0) * model_prob - (1.0 - model_prob)
                home = str(game.get("home_team") or "").strip()
                away = str(game.get("away_team") or "").strip()
                gd = str(game.get("game_date") or game.get("date") or today_str)
                gt = str(game.get("game_time") or "")
                gk = str(game.get("game_key") or _compose_game_key(away, home, game.get("game_datetime"), gd, gt))
                mk = _norm_gk(game.get("match_key") or f"{away}@{home}")
                return {
                    "sport": "tennis",
                    "name": player_name,
                    "team": side_team,
                    "prop_label": prop_label,
                    "stat_type": stat_type,
                    "line": line,
                    "direction": "OVER",
                    "model_prob": round(model_prob, 4),
                    "confidence": int(round(model_prob * 100)),
                    "safety_label": _safety_label_from_prob(model_prob),
                    "ev": round(ev_r, 4),
                    "odds_am": ods,
                    "dec_odds": dec,
                    "game": f"{away} @ {home}",
                    "game_key": gk,
                    "match_key": mk,
                    "game_date": gd,
                    "game_time": gt,
                    "home_team": home,
                    "away_team": away,
                    "sentiment_score": 0.0,
                    "sentiment_mentions": 0,
                    "sentiment_sources": source,
                    "worth_it": model_prob >= 0.52,
                    "worth_score": round(model_prob * 100.0, 2),
                    "worth_reason": f"Tennis match projection ({stat_type.replace('_', ' ')})",
                }

            for g in tennis_games[:20]:
                home = str(g.get("home_team") or "").strip()
                away = str(g.get("away_team") or "").strip()
                if not home or not away:
                    continue
                tn_game_rows: list[dict] = []
                # Generate model props for both players using historical avg rates
                for player_name, side_team in ((home, home), (away, away)):
                    for stat_type, prop_label, line, avg_rate in _tn_stat_profiles:
                        # Poisson over-line probability using typical rate
                        lam = avg_rate
                        target = int(_tnmath.floor(line) + 1)
                        cdf = 0.0
                        for k in range(max(0, target)):
                            try:
                                cdf += _tnmath.exp(-lam) * (lam ** k) / _tnmath.factorial(k)
                            except Exception:
                                pass
                        model_prob = max(0.38, min(0.92, 1.0 - cdf))
                        tn_game_rows.append(
                            _mk_tennis_row(g, player_name, side_team, stat_type, prop_label, line, model_prob, "tennis_baseline_model")
                        )
                tn_game_rows.sort(key=lambda x: float(x.get("model_prob") or 0.0), reverse=True)
                rows.extend(tn_game_rows[:max_per_game])
    except Exception as e:
        _log(f"[all-sports] tennis player-prop fallback skipped: {e}")

    # MMA / Boxing match props — total rounds, fight outcome.
    try:

        combat_games = [
            g for g in (games or [])
            if _infer_sport_group(g.get("sport") or g.get("competition") or g.get("league") or "") in {"mma", "boxing"}
        ]
        if combat_games:
            for g in combat_games[:16]:
                home = str(g.get("home_team") or "").strip()
                away = str(g.get("away_team") or "").strip()
                if not home or not away:
                    continue
                sport_tok = _infer_sport_group(g.get("sport") or g.get("competition") or "mma")
                # Total rounds over — fights typically go past 2.5 rounds
                for player_name, side_team in ((home, home), (away, away)):
                    model_prob = 0.58
                    ods = _prob_to_american(model_prob)
                    dec = round((1 + (ods / 100.0)) if ods > 0 else (1 + (100.0 / abs(ods))), 4)
                    ev_r = (dec - 1.0) * model_prob - (1.0 - model_prob)
                    gd = str(g.get("game_date") or g.get("date") or today_str)
                    gt = str(g.get("game_time") or "")
                    gk = str(g.get("game_key") or _compose_game_key(away, home, g.get("game_datetime"), gd, gt))
                    mk = _norm_gk(g.get("match_key") or f"{away}@{home}")
                    rows.append({
                        "sport": sport_tok,
                        "name": player_name,
                        "team": side_team,
                        "prop_label": "Fight Winner",
                        "stat_type": "fight_winner",
                        "line": None,
                        "direction": "OVER",
                        "model_prob": round(model_prob, 4),
                        "confidence": int(round(model_prob * 100)),
                        "safety_label": _safety_label_from_prob(model_prob),
                        "ev": round(ev_r, 4),
                        "odds_am": ods,
                        "dec_odds": dec,
                        "game": f"{away} @ {home}",
                        "game_key": gk,
                        "match_key": mk,
                        "game_date": gd,
                        "game_time": gt,
                        "home_team": home,
                        "away_team": away,
                        "sentiment_score": 0.0,
                        "sentiment_mentions": 0,
                        "sentiment_sources": "combat_baseline_model",
                        "worth_it": True,
                        "worth_score": round(model_prob * 100.0, 2),
                        "worth_reason": f"{sport_tok.upper()} baseline pick model",
                    })
    except Exception as e:
        _log(f"[all-sports] combat-sport prop fallback skipped: {e}")

    deduped: list[dict] = []
    seen = set()
    for r in rows:
        key = (
            str(r.get("game_key") or ""),
            str(r.get("name") or "").strip().lower(),
            str(r.get("stat_type") or "").strip().lower(),
            str(r.get("line") if r.get("line") is not None else ""),
            str(r.get("direction") or "").strip().upper(),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(r)
    deduped.sort(key=lambda x: float(x.get("model_prob") or 0.0), reverse=True)
    return deduped[:350]


def _build_multi_sport_snapshot(force_refresh: bool = False) -> dict:
    now = datetime.datetime.now(datetime.timezone.utc).timestamp()
    if not force_refresh:
        cached = _MULTI_SPORT_CACHE.get("snapshot")
        cache_ts = float(_MULTI_SPORT_CACHE.get("ts") or 0.0)
        if cached and (now - cache_ts) < _MULTI_SPORT_CACHE_TTL_SEC:
            return cached

    snapshot = {
        "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "tournaments": [],
        "games": [],
        "bets": [],
    }

    try:
        from data.odds_fetcher import get_available_sports, get_live_odds, get_odds_budget_state
    except Exception as e:
        _log(f"[all-sports] odds fetcher unavailable: {e}")
        return snapshot

    budget = get_odds_budget_state() or {}
    est_remaining = budget.get("estimated_remaining")
    reserve = int(budget.get("reserve") or 0)
    hard_stop = int(budget.get("hard_stop") or 0)
    if isinstance(est_remaining, int):
        _log(
            "[all-sports] odds credits "
            f"remaining~{est_remaining}/{int(budget.get('monthly_limit') or 0)} "
            f"(reserve={reserve}, hard_stop={hard_stop})"
        )

    sports = get_available_sports() or []
    if not sports:
        _log("[all-sports] No sports returned by odds API (missing key or quota exhausted) — trying free-source fallbacks")

    active = []
    for s in sports:
        key = str(s.get("key") or "").strip()
        if not key:
            continue
        sport_group = _infer_sport_group(key)
        # Keep outright markets for golf so player/tournament markets are ingested.
        if s.get("has_outrights") and sport_group != "golf":
            continue
        active.append(s)

    # League-aware ordering so top markets are covered first when credits are limited.
    league_priority = {
        "americanfootball_nfl": 400,
        "basketball_nba": 390,
        "baseball_mlb": 380,
        "icehockey_nhl": 370,
        "soccer_epl": 360,
        "soccer_uefa_champs_league": 350,
        "soccer_spain_la_liga": 340,
        "soccer_germany_bundesliga": 335,
        "soccer_italy_serie_a": 330,
        "soccer_france_ligue_1": 325,
        "soccer_usa_mls": 320,
        "basketball_wnba": 315,
        "americanfootball_ncaaf": 310,
        "basketball_ncaab": 305,
        "tennis_atp": 290,
        "tennis_wta": 285,
        "mma_mixed_martial_arts": 280,
        "boxing_boxing": 275,
        "golf_pga_championship_winner": 270,
        "golf_us_open_winner": 268,
        "motorsports_nascar_cup": 266,
        "motorsports_formula_1": 264,
        "cricket_ipl": 262,
    }
    active.sort(
        key=lambda s: (
            league_priority.get(str(s.get("key") or "").strip().lower(), 0),
            str(s.get("title") or s.get("key") or "").strip().lower(),
        ),
        reverse=True,
    )

    dynamic_limit = max(1, _MAX_ODDS_SPORTS)
    if isinstance(est_remaining, int):
        usable = max(0, est_remaining - reserve)
        if usable <= 0:
            dynamic_limit = 0
        else:
            dynamic_limit = max(min(_MIN_ODDS_SPORTS, _MAX_ODDS_SPORTS), min(dynamic_limit, usable))
    if active:
        active = active[:dynamic_limit]
    if isinstance(est_remaining, int):
        _log(f"[all-sports] querying odds for {len(active)} sports this cycle (dynamic cap={dynamic_limit})")
    today = _et_calendar_today()
    tomorrow = today + datetime.timedelta(days=1)
    horizon_end = today + datetime.timedelta(days=max(1, _SPORTS_HUB_FORECAST_DAYS - 1))
    allowed_dates = {
        (today + datetime.timedelta(days=offset)).isoformat()
        for offset in range((horizon_end - today).days + 1)
    }

    games: list[dict] = []
    bets: list[dict] = []

    # ── Pre-fetch ESPN tennis h2h matches (free, always run) ─────────────────
    # These give us real player names for matches the Odds API doesn't have h2h for.
    _espn_tennis_h2h: dict[str, dict] = {}  # match_key -> game dict
    try:
        from data.tennis_data_sources import fetch_espn_tennis_live_bundle
        _espn_bundle = fetch_espn_tennis_live_bundle(today)
        for item in (_espn_bundle.get("games") or []):
            ev = item.get("event") or {}
            comp = (ev.get("competitions") or [{}])[0]
            competitors = comp.get("competitors") or []
            if len(competitors) < 2:
                continue
            home_c = next((c for c in competitors if str(c.get("homeAway") or "").lower() == "home"), competitors[0])
            away_c = next((c for c in competitors if str(c.get("homeAway") or "").lower() == "away"), competitors[1])
            espn_home = str(((home_c.get("athlete") or {}).get("displayName") or (home_c.get("team") or {}).get("displayName") or home_c.get("displayName") or "")).strip()
            espn_away = str(((away_c.get("athlete") or {}).get("displayName") or (away_c.get("team") or {}).get("displayName") or away_c.get("displayName") or "")).strip()
            if not espn_home or not espn_away:
                continue
            espn_league = str(item.get("league") or "Tennis")
            iso_dt = str(ev.get("date") or comp.get("date") or "").strip()
            espn_date, espn_time = _datetime_to_et_parts(iso_dt)
            mk = _norm_gk(f"{espn_away}@{espn_home}")
            _espn_tennis_h2h[mk] = {
                "home": espn_home, "away": espn_away,
                "league": espn_league, "game_datetime": iso_dt,
                "game_date": espn_date or today.isoformat(),
                "game_time": espn_time,
                "home_score": _as_score_int(home_c.get("score")),
                "away_score": _as_score_int(away_c.get("score")),
            }
        _log(f"[all-sports] ESPN tennis pre-fetch: {len(_espn_tennis_h2h)} h2h matches")
    except Exception as _espn_pre_err:
        _log(f"[all-sports] ESPN tennis pre-fetch failed: {_espn_pre_err}")

    golf_market_label = {
        "outrights": "outright_winner",
        "h2h": "head_to_head",
        "winner": "winner",
        "top_5": "top_5_finish",
        "top_10": "top_10_finish",
        "top_20": "top_20_finish",
    }
    single_person_sports = {"golf", "tennis", "boxing", "mma", "motorsports"}
    sport_market_label_map: dict[str, dict[str, str]] = {
        "golf": golf_market_label,
        "tennis": {
            "h2h": "match_winner",
            "spreads": "match_handicap",
            "totals": "match_total_games",
            "outrights": "tournament_winner",
        },
        "boxing": {
            "h2h": "fight_winner",
            "outrights": "event_winner",
            "totals": "round_total",
        },
        "mma": {
            "h2h": "fight_winner",
            "outrights": "event_winner",
            "totals": "round_total",
        },
        "motorsports": {
            "h2h": "race_head_to_head",
            "outrights": "race_winner",
            "winner": "race_winner",
            "top_3": "top_3_finish",
            "top_5": "top_5_finish",
            "top_10": "top_10_finish",
        },
        "cricket": {
            "h2h": "match_winner",
            "spreads": "run_line",
            "totals": "match_total_runs",
            "outrights": "tournament_winner",
        },
    }

    for sport in active:
        sport_key = str(sport.get("key") or "").strip()
        title = str(sport.get("title") or sport_key)
        if not sport_key:
            continue
        sport_group = _infer_sport_group(sport_key)
        market_query = "outrights,h2h"
        if sport_group in {"tennis", "boxing", "mma", "cricket"}:
            # These sports frequently reject mixed market bundles (422).
            # Start with the most broadly supported market to reduce wasted calls.
            market_query = "h2h"
        elif sport_group in {"motorsports", "golf"}:
            market_query = "outrights,h2h"
        elif sport_group not in single_person_sports:
            market_query = "h2h"

        try:
            events = get_live_odds(sport_key, markets=market_query) or []
        except Exception as e:
            _log(f"[all-sports] {sport_key} odds error: {e}")
            continue

        if not events:
            continue

        for ev in events:
            home = str(ev.get("home_team") or "").strip()
            away = str(ev.get("away_team") or "").strip()
            books = ev.get("bookmakers") or []

            game_datetime = _event_datetime_value(ev)
            game_date, game_time = _datetime_to_et_parts(game_datetime)
            if game_date and game_date not in allowed_dates:
                continue

            event_title = str(
                ev.get("event_name")
                or ev.get("title")
                or ev.get("description")
                or title
            ).strip() or title

            is_single_person_event = sport_group in single_person_sports
            _is_field_val = lambda s: str(s or "").strip().lower() in {"field", "the field", ""}

            # Detect when home_team IS the tournament name (outright market masquerading
            # as a matchup). For WTA/ATP Challenger events the Odds API has no h2h data
            # so returns tournament-winner outrights with home_team = tournament title.
            _title_norm = re.sub(r"\s+", " ", title.strip().lower())
            _home_norm = re.sub(r"\s+", " ", str(home or "").strip().lower())
            _is_tournament_outright = (
                sport_group == "tennis"
                and (
                    _is_field_val(away)
                    or _home_norm == _title_norm
                    or (len(_home_norm) > 6 and _home_norm in _title_norm)
                    or (len(_title_norm) > 6 and _title_norm in _home_norm)
                )
            )

            if _is_tournament_outright and books:
                # Extract real player names ranked by highest implied probability.
                _outright_players: list[tuple[str, int, float]] = []  # (name, odds_am, raw_prob)
                for _mk in (books[0].get("markets") or []):
                    _mk_key = str((_mk or {}).get("key") or "").strip().lower()
                    if _mk_key not in {"outrights", "winner", "h2h"}:
                        continue
                    for _out in (_mk.get("outcomes") or []):
                        _nm = str((_out or {}).get("name") or "").strip()
                        _pr = _out.get("price")
                        if not _nm or _pr is None:
                            continue
                        if _nm.lower() in {"field", "the field"} or re.sub(r"\s+", " ", _nm.lower()) == _title_norm:
                            continue
                        try:
                            _ip = _prob_from_american(float(_pr))
                            if _ip is not None:
                                _outright_players.append((_nm, int(float(_pr)), float(_ip)))
                        except Exception:
                            continue
                    if _outright_players:
                        break
                # Sort by probability (highest first) and pick top 2 for the "matchup" card.
                _outright_players.sort(key=lambda x: x[2], reverse=True)
                if len(_outright_players) >= 1:
                    home = _outright_players[0][0]
                if len(_outright_players) >= 2:
                    away = _outright_players[1][0]
                elif not _is_field_val(away) and not (_is_tournament_outright and _home_norm == _title_norm):
                    pass  # away is already a real value
                else:
                    away = _outright_players[0][0] if _outright_players else away

            elif is_single_person_event and (not home or not away or _is_field_val(home) or _is_field_val(away)):
                left, right = _derive_individual_matchup(event_title)
                if (not home or _is_field_val(home)) and left:
                    home = left
                if (not away or _is_field_val(away)) and right:
                    away = right

            if is_single_person_event and (not home or not away or _is_field_val(home) or _is_field_val(away)) and books:
                try:
                    markets_for_names = books[0].get("markets") or []
                    outcome_names: list[str] = []
                    for mk_n in markets_for_names:
                        mk_key_n = str((mk_n or {}).get("key") or "").strip().lower()
                        if mk_key_n not in {"h2h", "winner", "outrights"}:
                            continue
                        for out in (mk_n.get("outcomes") or []):
                            nm = str((out or {}).get("name") or (out or {}).get("description") or "").strip()
                            if nm and nm.lower() not in {"field", "the field"} and re.sub(r"\s+", " ", nm.lower()) != _title_norm and nm not in outcome_names:
                                outcome_names.append(nm)
                        if len(outcome_names) >= 2:
                            break
                    if (not home or _is_field_val(home)) and outcome_names:
                        home = outcome_names[0]
                    if (not away or _is_field_val(away)) and len(outcome_names) >= 2:
                        away = outcome_names[1]
                except Exception:
                    pass

            if is_single_person_event and not _is_actionable_individual_matchup(home, away, event_title):
                continue

            if not home or not away:
                continue

            match_key = _norm_gk(f"{away}@{home}")
            game_key = _compose_game_key(away, home, game_datetime, game_date, game_time)
            status = str(ev.get("status") or "Scheduled")

            games.append({
                "sport": sport_group,
                "league": title,
                "competition": sport_key,
                "competition_name": title,
                "event_title": event_title,
                "home_team": home,
                "away_team": away,
                "date": game_date,
                "game_date": game_date,
                "game_time": game_time,
                "game_datetime": game_datetime,
                "status": status,
                "source": "odds",
                "match_key": match_key,
                "game_key": game_key,
            })

            books = ev.get("bookmakers") or []
            if not books:
                continue

            markets = books[0].get("markets") or []
            if not isinstance(markets, list):
                continue

            for market in markets:
                market_key = str(market.get("key") or "").strip().lower()
                outcomes = market.get("outcomes") or []
                priced = []
                for out in outcomes:
                    name = str(out.get("name") or out.get("description") or "").strip()
                    odds_am = out.get("price")
                    implied = _prob_from_american(odds_am)
                    if not name or implied is None:
                        continue
                    try:
                        priced.append((name, int(float(odds_am)), float(implied), out))
                    except Exception:
                        continue
                if len(priced) < 2:
                    continue

                total = sum(x[2] for x in priced)
                norm = []
                for name, odds_am, implied, out in priced:
                    true_prob = implied / total if total > 0 else implied
                    norm.append((name, odds_am, max(0.01, min(0.99, true_prob)), out))

                sport_market_map = sport_market_label_map.get(sport_group, {})
                if market_key in {"outrights", "winner", "top_3", "top_5", "top_10", "top_20"}:
                    if sport_group == "tennis":
                        top_n = _TENNIS_TOP_OUTRIGHT_PICKS
                    else:
                        top_n = 8 if is_single_person_event else 1
                else:
                    if sport_group == "tennis":
                        top_n = _TENNIS_TOP_MARKET_PICKS
                    else:
                        top_n = 3 if is_single_person_event else 1
                top_rows = sorted(norm, key=lambda x: x[2], reverse=True)[:top_n]
                for pick_name, pick_odds, model_prob, out in top_rows:
                    label = _rank_label(model_prob)
                    market_label = sport_market_map.get(market_key, market_key or "moneyline")
                    bet_type = market_label if is_single_person_event else ("moneyline" if market_key == "h2h" else market_label)
                    min_prob_gate = 0.08 if is_single_person_event else 0.56
                    worth_it = model_prob >= min_prob_gate
                    worth_reason = (
                        f"Best no-vig side from live {market_key or 'h2h'} market ({title})"
                        if not is_single_person_event
                        else f"No-vig {sport_group} {market_label.replace('_', ' ')} edge ({title})"
                    )
                    line_val = out.get("point") if isinstance(out, dict) else None

                    bet = {
                        "sport": sport_group,
                        "league": title,
                        "competition": sport_key,
                        "competition_name": title,
                        "event_title": event_title,
                        "bet_type": bet_type,
                        "pick": pick_name,
                        "team": pick_name if is_single_person_event else "",
                        "line": line_val,
                        "odds_am": int(float(pick_odds)),
                        "dec_odds": round((1 + (pick_odds / 100.0)) if float(pick_odds) > 0 else (1 + 100.0 / abs(float(pick_odds))), 4),
                        "model_prob": float(model_prob),
                        "confidence": int(round(model_prob * 100)),
                        "safety_label": label,
                        "safety": _safety_score_from_label(label),
                        "game_date": game_date,
                        "game_time": game_time,
                        "home_team": home,
                        "away_team": away,
                        "match_key": match_key,
                        "game_key": game_key,
                        "worth_it": worth_it,
                        "worth_score": round(model_prob * 100.0, 2),
                        "worth_reason": worth_reason,
                    }
                    bets.append(bet)

    # ── Merge ESPN tennis h2h matches that have no Odds API h2h equivalent ──
    # This adds real player-vs-player matchups that the Odds API only covers as outrights.
    _games_match_keys = {_norm_gk(str(g.get("match_key") or "")) for g in games}
    _espn_added = 0
    for _mk, _eg in _espn_tennis_h2h.items():
        if _mk in _games_match_keys:
            continue
        if _eg.get("game_date") not in allowed_dates:
            continue
        _espn_gk = _compose_game_key(_eg["away"], _eg["home"], _eg.get("game_datetime"), _eg.get("game_date"), _eg.get("game_time"))
        games.append({
            "sport": "tennis",
            "league": _eg.get("league") or "Tennis",
            "competition": "espn_tennis_live",
            "competition_name": _eg.get("league") or "Tennis",
            "event_title": f"{_eg['away']} vs {_eg['home']}",
            "home_team": _eg["home"],
            "away_team": _eg["away"],
            "date": _eg.get("game_date") or today.isoformat(),
            "game_date": _eg.get("game_date") or today.isoformat(),
            "game_time": _eg.get("game_time"),
            "game_datetime": _eg.get("game_datetime"),
            "status": "Scheduled",
            "source": "espn_tennis_live",
            "match_key": _mk,
            "game_key": _espn_gk,
            "home_score": _eg.get("home_score"),
            "away_score": _eg.get("away_score"),
        })
        _espn_added += 1
    if _espn_added:
        _log(f"[all-sports] ESPN tennis added {_espn_added} h2h matches not in Odds feed")

    # If odds feed failed, backfill games from free sources.
    if not games:
        fallback_games = _collect_fallback_games_for_all_sports(today, tomorrow)
        if fallback_games:
            _log(f"[all-sports] Fallback feeds supplied {len(fallback_games)} games")
            games = fallback_games

    # If we still have no ranked bets, derive baseline bets from MLB/soccer models.
    if games and not bets:
        fallback_bets = _build_model_fallback_bets(games)
        if fallback_bets:
            _log(f"[all-sports] Fallback models supplied {len(fallback_bets)} ranked bets")
            bets = fallback_bets

    # Build tournaments from final game pool (odds + fallback merged).
    tournament_counts: dict[str, int] = {}
    tournament_meta: dict[str, dict] = {}
    for g in games:
        code = str(g.get("competition") or "UNKNOWN").strip()
        if not code:
            continue
        tournament_counts[code] = tournament_counts.get(code, 0) + 1
        if code not in tournament_meta:
            tournament_meta[code] = {
                "code": code,
                "name": g.get("competition_name") or g.get("league") or code,
                "country": "Global",
                "type": _infer_sport_group(g.get("sport") or code),
            }

    tournaments = []
    for code, meta in tournament_meta.items():
        row = dict(meta)
        row["match_count"] = int(tournament_counts.get(code, 0))
        tournaments.append(row)
    tournaments.sort(key=lambda x: (x.get("match_count", 0), x.get("name", "")), reverse=True)

    games.sort(key=lambda x: (
        str(x.get("game_date") or x.get("date") or ""),
        str(x.get("game_time") or ""),
        str(x.get("competition_name") or x.get("league") or ""),
    ))

    # ── Data cleaning + deep enrichment ──────────────────────────────────────
    try:
        from data.enrichment import clean_snapshot, enrich_games_batch
        snapshot_clean = clean_snapshot({"games": games, "bets": bets})
        games = snapshot_clean["games"]
        bets  = snapshot_clean["bets"]
        _log(f"[all-sports] Data-clean pass: {len(games)} games, {len(bets)} bets")
        # Enrich a wider batch while preserving full horizon rows.
        _enrich_limit = max(1, min(300, int(os.getenv("ENRICH_MAX_GAMES", "120"))))
        _include_weather  = str(os.getenv("ENRICH_WEATHER",  "1")).strip().lower() in {"1","true","yes"}
        _include_coaching = str(os.getenv("ENRICH_COACHING", "1")).strip().lower() in {"1","true","yes"}
        _include_h2h      = str(os.getenv("ENRICH_H2H",     "1")).strip().lower() in {"1","true","yes"}
        _games_before_enrich = list(games)
        enriched_games = enrich_games_batch(
            _games_before_enrich,
            include_weather=_include_weather,
            include_coaching=_include_coaching,
            include_h2h=_include_h2h,
            max_games=_enrich_limit,
            throttle_sec=0.12,
        )
        # Keep full horizon rows; merge enrichment for only the subset that was processed.
        if isinstance(enriched_games, list) and enriched_games:
            by_key: dict[str, dict] = {}
            for eg in enriched_games:
                if not isinstance(eg, dict):
                    continue
                k = str(eg.get("game_key") or "").strip() or str(eg.get("match_key") or "").strip()
                if k:
                    by_key[k] = eg

            merged_games: list[dict] = []
            for g in _games_before_enrich:
                if not isinstance(g, dict):
                    continue
                k = str(g.get("game_key") or "").strip() or str(g.get("match_key") or "").strip()
                if k and k in by_key:
                    merged_games.append({**g, **by_key[k]})
                else:
                    merged_games.append(g)
            games = merged_games
        else:
            games = _games_before_enrich

        _log(
            f"[all-sports] Enrichment complete for {len(enriched_games or [])} games "
            f"(retained horizon rows={len(games)})"
        )
        # ── Persist enrichment signals to DB ────────────────────────────
        try:
            from data.db import save_game_enrichment
            n_enrich = save_game_enrichment(games)
            _log(f"[all-sports] Enrichment saved to DB: {n_enrich} rows")
        except Exception as _db_enrich_err:
            _log(f"[all-sports] DB enrichment persist skipped: {_db_enrich_err}")

        try:
            tennis_games = [
                g for g in games
                if _infer_sport_group(g.get("sport") or g.get("competition") or g.get("league") or "") == "tennis"
            ]
            if tennis_games:
                from data.tennis_data_sources import (
                    build_tennis_prediction_context,
                    load_tennis_reference_rows,
                    compute_player_elos,
                    elo_win_probability,
                    _DEFAULT_ELO,
                )

                tennis_reference_rows = load_tennis_reference_rows()

                # Pre-compute global Elo table from all historical reference data.
                _all_elos = compute_player_elos(tennis_reference_rows)

                def _infer_surface_from_tournament(name: str) -> str:
                    n = str(name or "").lower()
                    if any(k in n for k in ("clay", "terre", "roland", "monte", "madrid", "rome", "barcelona", "hamburg", "brescia", "figueira", "itf", "open de")):
                        return "clay"
                    if any(k in n for k in ("wimbledon", "grass", "queens", "halle", "s-hertogenbosch", "eastbourne", "nottingham")):
                        return "grass"
                    if any(k in n for k in ("hard", "australian open", "us open", "indian wells", "miami", "canada", "cincinnati", "dubai", "doha")):
                        return "hard"
                    return ""

                enriched_tennis = 0
                for g in tennis_games:
                    home_player = str(g.get("home_team") or "")
                    away_player = str(g.get("away_team") or "")
                    # Skip cards that still have no real player name
                    if not home_player or not away_player:
                        continue

                    # Infer surface if not already known
                    surface = str(g.get("surface") or g.get("court_surface") or g.get("venue_surface") or "").strip()
                    if not surface:
                        surface = _infer_surface_from_tournament(
                            g.get("competition_name") or g.get("league") or g.get("event_title") or ""
                        )

                    ctx = build_tennis_prediction_context(
                        home_player=home_player,
                        away_player=away_player,
                        surface=surface,
                        match_date=str(g.get("game_date") or g.get("date") or ""),
                        reference_rows=tennis_reference_rows,
                    )
                    if ctx:
                        g["tennis_context"] = ctx
                        g.update({
                            "surface": ctx.get("surface") or surface or g.get("surface"),
                            "rank_diff": ctx.get("rank_diff") if ctx.get("rank_diff") is not None else g.get("rank_diff"),
                            "recent_form_gap": ctx.get("recent_form_gap"),
                            "fatigue_home_days": ctx.get("fatigue_home_days"),
                            "fatigue_away_days": ctx.get("fatigue_away_days"),
                        })

                        # Re-score bets for this match using Elo-blended win probability.
                        gk = str(g.get("game_key") or "").strip()
                        home_p = _norm_gk(home_player.lower())
                        away_p = _norm_gk(away_player.lower())
                        win_prob_home = float(ctx.get("win_prob_home") or 0.5)
                        win_prob_away = float(ctx.get("win_prob_away") or 0.5)
                        for bet in bets:
                            if _norm_gk(str(bet.get("game_key") or "").strip()) != gk:
                                continue
                            pick_n = _norm_gk(str(bet.get("pick") or "").lower())
                            if pick_n and home_p and (pick_n == home_p or pick_n in home_p or home_p in pick_n):
                                new_prob = round(max(0.50, min(0.98, win_prob_home)), 4)
                            elif pick_n and away_p and (pick_n == away_p or pick_n in away_p or away_p in pick_n):
                                new_prob = round(max(0.50, min(0.98, win_prob_away)), 4)
                            else:
                                continue
                            bet["model_prob"] = new_prob
                            bet["confidence"] = int(round(new_prob * 100))
                            bet["safety_label"] = _safety_label_from_prob(new_prob)
                            bet["safety"] = _safety_score_from_label(bet["safety_label"])

                        enriched_tennis += 1
                    else:
                        # No historical data — use raw Elo if available, else raw odds as-is.
                        surf_key = (surface or "hard").strip().lower()
                        if surf_key not in {"hard", "clay", "grass", "carpet"}:
                            surf_key = "hard"
                        home_norm_key = next((k for k in _all_elos if re.sub(r"\s+", "", k.lower()) == re.sub(r"\s+", "", home_player.lower())), None)
                        away_norm_key = next((k for k in _all_elos if re.sub(r"\s+", "", k.lower()) == re.sub(r"\s+", "", away_player.lower())), None)
                        if home_norm_key or away_norm_key:
                            he = (_all_elos.get(home_norm_key or "") or {}).get(surf_key, _DEFAULT_ELO)
                            ae = (_all_elos.get(away_norm_key or "") or {}).get(surf_key, _DEFAULT_ELO)
                            elo_p_home = elo_win_probability(he, ae)
                            minimal_ctx = {
                                "surface": surface,
                                "home_player": home_player,
                                "away_player": away_player,
                                "elo_home": he,
                                "elo_away": ae,
                                "elo_prob_home": elo_p_home,
                                "elo_prob_away": round(1.0 - elo_p_home, 4),
                                "win_prob_home": elo_p_home,
                                "win_prob_away": round(1.0 - elo_p_home, 4),
                                "surface_win_rate_home": 0.5,
                                "surface_win_rate_away": 0.5,
                                "recent_form_home": 0.5,
                                "recent_form_away": 0.5,
                                "h2h_record_surface_home": 0,
                                "h2h_record_surface_away": 0,
                            }
                            g["tennis_context"] = minimal_ctx
                            g.update({"surface": surface or g.get("surface")})

                _log(f"[all-sports] Tennis context enriched: {enriched_tennis} games")
        except Exception as _tennis_enrich_err:
            _log(f"[all-sports] Tennis enrichment skipped: {_tennis_enrich_err}")

        try:
            golf_games = [
                g for g in games
                if _infer_sport_group(g.get("sport") or g.get("competition") or g.get("league") or "") == "golf"
            ]
            if golf_games:
                from data.golf_data_sources import build_golf_prediction_context, load_golf_reference_rows

                golf_reference_rows = load_golf_reference_rows()
                enriched_golf = 0
                for g in golf_games:
                    player_name = str(g.get("home_team") or "").strip()
                    if player_name.lower() in {"field", ""}:
                        continue
                    ctx = build_golf_prediction_context(
                        player_name=player_name,
                        event_name=str(g.get("competition_name") or g.get("league") or g.get("event_title") or ""),
                        course_name=str(g.get("course_name") or g.get("venue") or ""),
                        game_date=str(g.get("game_date") or g.get("date") or ""),
                        weather=str(g.get("weather") or ""),
                        reference_rows=golf_reference_rows,
                    )
                    if ctx:
                        g["golf_context"] = ctx
                        g.update({
                            "sg_total": ctx.get("sg_total"),
                            "sg_approach": ctx.get("sg_approach"),
                            "sg_putting": ctx.get("sg_putting"),
                            "course_fit": ctx.get("course_fit"),
                            "course_type": ctx.get("course_type") or g.get("course_type"),
                            "recent_form": ctx.get("recent_form"),
                            "driving_distance": ctx.get("driving_distance"),
                            "cut_streak": ctx.get("cut_streak"),
                            "owgr_rank": ctx.get("owgr_rank"),
                            "weather": ctx.get("weather") or g.get("weather"),
                        })
                        enriched_golf += 1
                _log(f"[all-sports] Golf context enriched: {enriched_golf} games")
        except Exception as _golf_enrich_err:
            _log(f"[all-sports] Golf enrichment skipped: {_golf_enrich_err}")
    except Exception as _enrich_exc:
        _log(f"[all-sports] Enrichment step skipped: {_enrich_exc}")

    snapshot["tournaments"] = tournaments
    snapshot["games"] = games
    snapshot["bets"] = sorted(bets, key=lambda x: (x.get("model_prob") or 0), reverse=True)
    snapshot["forecast_horizon_days"] = _SPORTS_HUB_FORECAST_DAYS
    snapshot["forecast_window"] = {
        "start": today.isoformat(),
        "end": horizon_end.isoformat(),
    }
    snapshot["odds_budget"] = budget
    _MULTI_SPORT_CACHE["snapshot"] = snapshot
    _MULTI_SPORT_CACHE["ts"] = now
    return snapshot


def _sports_coverage_snapshot(force_refresh: bool = False) -> dict[str, Any]:
    """Summarize sports coverage for today/tomorrow and overall snapshot support."""
    today_str = _et_calendar_today().isoformat()
    tomorrow_str = (_et_calendar_today() + datetime.timedelta(days=1)).isoformat()

    supported_general = [
        "baseball", "basketball", "football", "hockey", "soccer",
        "tennis", "boxing", "mma", "golf", "motorsports", "cricket",
    ]

    snapshot = _build_multi_sport_snapshot(force_refresh=force_refresh)
    games = snapshot.get("games") or []
    tournaments = snapshot.get("tournaments") or []

    today_sports: set[str] = set()
    tomorrow_sports: set[str] = set()
    all_seen: set[str] = set()
    leagues_by_sport: dict[str, set[str]] = {}

    for game in games:
        if not isinstance(game, dict):
            continue
        sport = _infer_sport_group(game.get("sport") or game.get("competition") or game.get("league") or "")
        if not sport or sport == "other":
            continue
        all_seen.add(sport)
        gd = str(game.get("game_date") or game.get("date") or "").strip()
        if gd == today_str:
            today_sports.add(sport)
        if gd == tomorrow_str:
            tomorrow_sports.add(sport)
        leagues_by_sport.setdefault(sport, set()).add(
            str(game.get("competition_name") or game.get("league") or game.get("competition") or "")
        )

    for trn in tournaments:
        if not isinstance(trn, dict):
            continue
        sport = _infer_sport_group(trn.get("type") or trn.get("code") or trn.get("name") or "")
        if sport and sport != "other":
            all_seen.add(sport)

    return {
        "today_date": today_str,
        "tomorrow_date": tomorrow_str,
        "today_sports": sorted(today_sports),
        "tomorrow_sports": sorted(tomorrow_sports),
        "all_seen_sports": sorted(all_seen),
        "supported_general_sports": supported_general,
        "league_count_by_sport": {k: len([v for v in vals if v]) for k, vals in leagues_by_sport.items()},
        "game_count": len(games),
    }


def _infer_game_source(game: dict) -> str:
    if not isinstance(game, dict):
        return "unknown"
    raw = str(game.get("source") or "").strip().lower()
    if raw:
        return raw
    comp = str(game.get("competition") or "").strip().lower()
    if comp.startswith("tsdb_"):
        return "tsdb"
    if comp.startswith("espn_"):
        return "espn"
    if comp.startswith("db_"):
        return "db_cache"
    if game.get("espn_event_id"):
        return "espn"
    return "unknown"


def _sports_source_coverage_snapshot(force_refresh: bool = False) -> dict[str, Any]:
    """Return per-sport source fill counts for current multi-sport snapshot."""
    snapshot = _build_multi_sport_snapshot(force_refresh=force_refresh)
    games = [g for g in (snapshot.get("games") or []) if isinstance(g, dict)]

    by_sport: dict[str, dict[str, Any]] = {}
    total_by_source: dict[str, int] = {}
    per_day: dict[str, dict[str, int]] = {}
    today_str = _et_calendar_today().isoformat()

    for g in games:
        sport = _infer_sport_group(g.get("sport") or g.get("competition") or g.get("league") or "")
        if not sport:
            sport = "other"
        source = _infer_game_source(g)
        gd = str(g.get("game_date") or g.get("date") or "").strip()[:10]

        bucket = by_sport.setdefault(sport, {"total_games": 0, "by_source": {}, "by_date": {}})
        bucket["total_games"] = int(bucket.get("total_games") or 0) + 1
        src_map = bucket.setdefault("by_source", {})
        src_map[source] = int(src_map.get(source) or 0) + 1
        if gd:
            day_map = bucket.setdefault("by_date", {}).setdefault(gd, {})
            day_map[source] = int(day_map.get(source) or 0) + 1

        total_by_source[source] = int(total_by_source.get(source) or 0) + 1
        if gd:
            day_total = per_day.setdefault(gd, {})
            day_total[source] = int(day_total.get(source) or 0) + 1

    sports_sorted = sorted(by_sport.keys())
    for s in sports_sorted:
        src_rows = by_sport[s].get("by_source") or {}
        by_sport[s]["primary_source"] = max(src_rows, key=src_rows.get) if src_rows else "unknown"

    return {
        "generated_at": str(snapshot.get("generated_at") or ""),
        "forecast_window": snapshot.get("forecast_window") or {},
        "forecast_horizon_days": int(snapshot.get("forecast_horizon_days") or _SPORTS_HUB_FORECAST_DAYS),
        "snapshot_game_count": len(games),
        "today_date": today_str,
        "total_by_source": dict(sorted(total_by_source.items(), key=lambda kv: kv[0])),
        "by_date": dict(sorted(per_day.items(), key=lambda kv: kv[0])),
        "sports": sports_sorted,
        "by_sport": by_sport,
    }


def _run_all_sports_analysis():
    with _lock:
        _state["status"] = "running"
        _state["error"] = None
        _state["logs"] = []
        _state["phase"] = _PHASES[0]
        _state["phase_idx"] = 0

    try:
        today_date = _et_calendar_today()
        _run_mandatory_daily_calibration(today_date)
        _run_prediction_preflight("all")
        try:
            from models.all_sports_predictor import daily_self_train

            learn_result = daily_self_train(today_date)
            _log(
                "[all-sports] self-train: "
                f"ok={bool(learn_result.get('ok', True))} "
                f"skipped={bool(learn_result.get('skipped', False))}"
            )
        except Exception as learn_exc:
            _log(f"[all-sports] self-train skipped: {learn_exc}")

        _phase(0)
        _log("[all-sports] Discovering online sportsbooks and events...")
        snapshot = _build_multi_sport_snapshot(force_refresh=True)

        _phase(1)
        games = snapshot.get("games") or []
        bets = snapshot.get("bets") or []
        best_bet_rows = _multi_sport_best_bets_rows(bets)
        fallback_only = bool(bets) and all(
            "fallback" in str(b.get("worth_reason") or "").lower()
            for b in bets
            if isinstance(b, dict)
        )
        if fallback_only:
            _log("[all-sports] Snapshot is fallback-only; using lightweight model player props")
            sentiment_prop_rows = _build_model_player_props_fallback((games or [])[:30], max_per_game=14)
        else:
            sentiment_prop_rows = _build_all_sport_sentiment_props(games, bets)

        has_player_predictions = any(
            isinstance(r, dict)
            and str(r.get("name") or r.get("player") or r.get("player_name") or "").strip()
            for r in (sentiment_prop_rows or [])
        )
        if not has_player_predictions:
            _log("[all-sports] No sentiment player predictions found; backfilling model player props")
            fallback_prop_rows = _build_model_player_props_fallback((games or [])[:50], max_per_game=16)
            sentiment_prop_rows = _merge_all_sports_table_rows(sentiment_prop_rows, fallback_prop_rows)

        sentiment_prop_rows = _enforce_over_only_player_props(sentiment_prop_rows)
        table_rows = _merge_all_sports_table_rows(sentiment_prop_rows, best_bet_rows)
        table_rows = _enforce_over_only_player_props(table_rows)
        try:
            from models.all_sports_predictor import rank_best_bets

            def _meets_sport_prediction_standard(row: dict) -> bool:
                if not isinstance(row, dict):
                    return False
                sport_token = str(row.get("sport") or row.get("sport_group") or "").strip().lower()
                min_q = _PREDICTION_QUALITY_MIN
                min_ev = _PREDICTION_EV_MIN
                min_p = _PREDICTION_PROB_MIN
                if sport_token in {"golf", "tennis", "mma", "boxing", "motorsports", "cricket"}:
                    min_q = min(min_q, 0.18)
                    min_ev = min(min_ev, -0.06)
                    min_p = min(min_p, 0.05)
                elif sport_token in {"soccer", "basketball", "icehockey", "americanfootball"}:
                    min_q = min(min_q, 0.48)
                    min_ev = min(min_ev, -0.04)
                    min_p = min(min_p, 0.50)
                elif sport_token in {"baseball"}:
                    min_q = min(min_q, 0.52)
                    min_ev = min(min_ev, -0.03)
                    min_p = min(min_p, 0.52)

                q_val = float(row.get("quality_score") or row.get("quality") or 0.0)
                ev_val = float(row.get("ev") or 0.0)
                p_val = float(row.get("model_prob") or 0.0)
                return (q_val >= min_q) and (ev_val >= min_ev) and (p_val >= min_p)

            table_rows = rank_best_bets(table_rows, raw_bets=bets)
            before_gate = len(table_rows)
            gated_rows = []
            for _row in table_rows:
                if _meets_sport_prediction_standard(_row):
                    gated_rows.append(_row)
            if gated_rows:
                table_rows = gated_rows
            removed = max(0, before_gate - len(table_rows))
            _log(f"[all-sports] quality ranking applied: {len(table_rows)} rows")
            if removed:
                _log(
                    "[all-sports] usefulness gate removed "
                    f"{removed} low-quality rows "
                    f"(min q={_PREDICTION_QUALITY_MIN:.2f}, p={_PREDICTION_PROB_MIN:.2f}, ev={_PREDICTION_EV_MIN:.2f})"
                )
        except Exception as rank_exc:
            _log(f"[all-sports] quality ranking skipped: {rank_exc}")

        # Always sanitize row payloads before they flow to scoring/DB/UI.
        try:
            from data.enrichment import clean_bet_row, clean_prop_row
            bets = [clean_bet_row(b) for b in (bets or []) if isinstance(b, dict)]
            sentiment_prop_rows = [
                clean_prop_row(p) for p in (sentiment_prop_rows or []) if isinstance(p, dict)
            ]
            table_rows = [clean_prop_row(r) for r in (table_rows or []) if isinstance(r, dict)]
            sentiment_prop_rows = [p for p in sentiment_prop_rows if _is_actionable_prop_row(p)]
            table_rows = [r for r in table_rows if _is_actionable_prop_row(r)]
            _log(
                f"[all-sports] row-clean pass: bets={len(bets)} props={len(sentiment_prop_rows)}"
            )
        except Exception as _row_clean_exc:
            _log(f"[all-sports] row-clean skipped: {_row_clean_exc}")

        _attach_tracking_uids(bets, table_rows)
        card_prop_rows = sentiment_prop_rows if sentiment_prop_rows else []
        _log(f"[all-sports] Pulled {len(games)} games and {len(bets)} ranked bets")
        if best_bet_rows:
            _log(f"[all-sports] Best-bets table rows prepared: {len(best_bet_rows)}")
        if sentiment_prop_rows:
            _log(f"[all-sports] Sentiment player rows prepared: {len(sentiment_prop_rows)}")

        today_str = today_date.isoformat()
        tomorrow_str = (today_date + datetime.timedelta(days=1)).isoformat()

        _phase(2)
        today_games = [g for g in games if _row_game_date(g) == today_str]
        tomorrow_games = [g for g in games if _row_game_date(g) == tomorrow_str]

        # ── Enrich player props with recent form + fatigue ───────────────────
        if card_prop_rows:
            try:
                from data.enrichment import enrich_props_batch
                card_prop_rows = enrich_props_batch(card_prop_rows, max_props=60, throttle_sec=0.08)
                _log(f"[all-sports] Props enrichment done: {len(card_prop_rows)} rows")
            except Exception as _pe:
                _log(f"[all-sports] Props enrichment skipped: {_pe}")

        _phase(3)
        today_cards = [_build_card(g, bets, card_prop_rows, "TODAY") for g in today_games]
        tomorrow_cards = [_build_card(g, bets, card_prop_rows, "TOMORROW") for g in tomorrow_games]

        def _card_score(card: dict) -> float:
            s = [b["safety"] for b in [card.get("moneyline"), card.get("run_line"), card.get("total")] if b]
            return sum(s) / len(s) if s else 0.45

        today_cards.sort(key=_card_score, reverse=True)
        tomorrow_cards.sort(key=_card_score, reverse=True)
        today_cards, tomorrow_cards = _normalize_dashboard_card_buckets(today_cards, tomorrow_cards)
        best_parlays = []
        try:
            from models.all_sports_predictor import build_best_parlays

            best_parlays = build_best_parlays(table_rows, max_legs=5, top_n=8)
            _log(f"[all-sports] expert parlays built: {len(best_parlays)} combos")
        except Exception as parlay_exc:
            _log(f"[all-sports] parlay builder skipped: {parlay_exc}")

        if not best_parlays:
            try:
                # Keep combo coverage alive in degraded mode when the expert
                # parlay scorer has too little structured signal.
                from models.mlb_predictor import build_parlays as _build_generic_parlays

                best_parlays = _build_generic_parlays(table_rows, max_legs=4, top_n=6)
                if best_parlays:
                    _log(f"[all-sports] fallback parlays built: {len(best_parlays)} combos")
            except Exception as fallback_parlay_exc:
                _log(f"[all-sports] fallback parlay builder skipped: {fallback_parlay_exc}")

        # Persist generated parlays in backend so tracking keeps moving even when no UI tab is open.
        if best_parlays:
            try:
                from data.db import save_tracked_parlay

                saved_count = 0
                for idx, combo in enumerate(best_parlays[:8], start=1):
                    if not isinstance(combo, dict):
                        continue
                    legs = combo.get("legs") if isinstance(combo.get("legs"), list) else []
                    if len(legs) < 2:
                        continue
                    save_tracked_parlay(
                        name=f"Auto Expert {idx}-#{int(combo.get('n_legs') or len(legs))}",
                        legs=legs,
                        combined_odds=float(combo.get("combined_dec") or 0.0),
                        stake_usd=10.0,
                        dedupe_pending=True,
                    )
                    saved_count += 1
                if saved_count:
                    _log(f"[all-sports] tracked-parlays auto-saved: {saved_count}")
            except Exception as parlay_save_exc:
                _log(f"[all-sports] tracked-parlays auto-save skipped: {parlay_save_exc}")

        last_updated = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M")

        # ── Persist analysis cache so state survives worker crashes ───────────
        try:
            from data.db import save_analysis_cache as _sac
            _sac({
                "game_cards_today":    _clean(today_cards),
                "game_cards_tomorrow": _clean(tomorrow_cards),
                "best_parlays":        _clean(best_parlays),
                "player_props":        _clean(table_rows),
                "last_updated":        last_updated,
            }, cache_date=_et_calendar_today())
            _log("[all-sports] Analysis cache saved to DB")
        except Exception as _cache_exc:
            _log(f"[all-sports] Cache save error: {_cache_exc}")

        # ── Persist all-sports bets to DB so they survive server restarts ─────
        try:
            from data.db import save_predictions, save_prop_picks
            today_str_allsports = _et_calendar_today().isoformat()
            run_id_allsports = f"ALL-{today_str_allsports}"

            # ── Kalshi ticker + investor grade enrichment ──────────────────────
            try:
                from data.kalshi import attach_kalshi_to_bets
                bets = attach_kalshi_to_bets(bets)

                # Backend-side guarantee: if enrichment produced fresh Kalshi matches,
                # emit alert emails even when /api/kalshi/resolve-ready was not called.
                try:
                    enriched_resolutions: dict[str, dict] = {}
                    for _i, _b in enumerate(bets or []):
                        if not isinstance(_b, dict):
                            continue
                        _status = str(_b.get("kalshi_status") or "").strip().lower()
                        _ticker = str(_b.get("kalshi_ticker") or "").strip()
                        if _status != "matched" or not _ticker:
                            continue
                        _uid = str(
                            _b.get("uid")
                            or _b.get("bet_uid")
                            or _b.get("prediction_uid")
                            or f"ready_{_i}"
                        ).strip()
                        enriched_resolutions[_uid] = {
                            "status": "matched",
                            "market_ticker": _ticker,
                            "event_ticker": str(_b.get("kalshi_event_ticker") or ""),
                            "series_ticker": str(_b.get("kalshi_series_ticker") or ""),
                            "side": str(_b.get("kalshi_side") or ""),
                            "price_cents": int(_b.get("kalshi_price_cents") or 0),
                            "message": "Matched from backend enrichment.",
                        }
                    if enriched_resolutions:
                        _maybe_email_kalshi_matches(bets, enriched_resolutions, source_tag="analysis-attach")
                except Exception as _email_attach_exc:
                    _log(f"[kalshi-email] analysis attach alert step skipped: {_email_attach_exc}")
            except Exception as _ke:
                _log(f"[all-sports] Kalshi enrichment skipped: {_ke}")
            try:
                from data.polymarket import attach_polymarket_to_bets
                bets = attach_polymarket_to_bets(bets)
            except Exception as _pe:
                _log(f"[all-sports] Polymarket enrichment skipped: {_pe}")
            try:
                from analysis.investor import investor_grade as _ig
                for _b in bets:
                    _b.update(_ig(_b))
            except Exception:
                pass

            pred_rows_allsports = []
            for b in bets:
                pred_rows_allsports.append({
                    "game_key":     b.get("game_key") or b.get("match_key") or "",
                    "run_id":       run_id_allsports,
                    "run_date":     today_str_allsports,
                    "sport":        b.get("sport") or "unknown",
                    "bet_type":     b.get("bet_type") or "moneyline",
                    "pick":         b.get("pick") or "",
                    "line":         b.get("line"),
                    "odds_am":      b.get("odds_am"),
                    "dec_odds":     b.get("dec_odds") or 2.0,
                    "model_prob":   float(b.get("model_prob") or 0.5),
                    "confidence":   int(b.get("confidence") or 50),
                    "safety_label": b.get("safety_label") or "MODERATE",
                    "game_date":    b.get("game_date") or today_str_allsports,
                    "game_time":    b.get("game_time") or "",
                    "home_team":    b.get("home_team") or "",
                    "away_team":    b.get("away_team") or "",
                    "bet_uid":      b.get("bet_uid") or "",
                    "signal_type":    b.get("signal_type") or "neutral",
                    "injury_flag":    bool(b.get("injury_flag")),
                    "momentum_flag":  bool(b.get("momentum_flag")),
                    "lineup_flag":    bool(b.get("lineup_flag")),
                    "active_sources": ",".join(b.get("active_sources") or []),
                    "kalshi_ticker":        b.get("kalshi_ticker") or "",
                    "kalshi_event_ticker":  b.get("kalshi_event_ticker") or "",
                    "kalshi_series_ticker": b.get("kalshi_series_ticker") or "",
                    "kalshi_side":          b.get("kalshi_side") or "",
                    "kalshi_price_cents":   int(b.get("kalshi_price_cents") or 0),
                    "kalshi_status":        b.get("kalshi_status") or "unavailable",
                    "polymarket_ticker":        b.get("polymarket_ticker") or "",
                    "polymarket_market_slug":   b.get("polymarket_market_slug") or "",
                    "polymarket_event_slug":    b.get("polymarket_event_slug") or "",
                    "polymarket_series_ticker": b.get("polymarket_series_ticker") or "",
                    "polymarket_side":          b.get("polymarket_side") or "",
                    "polymarket_price":         b.get("polymarket_price"),
                    "polymarket_status":        b.get("polymarket_status") or "unavailable",
                    "grade":                b.get("grade") or "X",
                    "investor_score":       float(b.get("investor_score") or 0),
                })
            horizon_end_str_allsports = (
                _et_calendar_today() + datetime.timedelta(days=max(1, _SPORTS_HUB_FORECAST_DAYS - 1))
            ).isoformat()
            pred_rows_window = [
                row
                for row in pred_rows_allsports
                if today_str_allsports <= str(row.get("game_date") or "")[:10] <= horizon_end_str_allsports
            ]
            if pred_rows_window:
                save_predictions(pred_rows_window)
                _log(
                    f"[all-sports] Saved {len(pred_rows_window)} forecast-window bets to DB"
                    f" ({today_str_allsports}..{horizon_end_str_allsports})"
                )

            props_rows_allsports = []
            for p in table_rows or []:
                player_name = str(p.get("name") or p.get("player") or "").strip()
                if not player_name:
                    continue
                stat_type = str(p.get("stat_type") or p.get("prop_type") or p.get("bet_type") or "").strip()
                if not stat_type:
                    continue
                rec = str(p.get("direction") or p.get("recommendation") or "OVER").strip().upper()
                if rec not in {"OVER", "UNDER"}:
                    rec = "OVER"
                if (_OVER_ONLY_PROPS and _is_prop_style_row(p) and rec == "UNDER") or (
                    _OVER_ONLY_PLAYER_PROPS and _is_player_prop_style_row(p) and rec == "UNDER"
                ):
                    continue
                model_prob = float(p.get("model_prob") or 0.5)
                over_prob = p.get("over_prob")
                under_prob = p.get("under_prob")
                if (_OVER_ONLY_PROPS and _is_prop_style_row(p)) or (_OVER_ONLY_PLAYER_PROPS and _is_player_prop_style_row(p)):
                    over_prob = model_prob
                    under_prob = 0.0
                elif over_prob is None or under_prob is None:
                    over_prob = model_prob if rec == "OVER" else 1.0 - model_prob
                    under_prob = model_prob if rec == "UNDER" else 1.0 - model_prob

                props_rows_allsports.append({
                    "sport": p.get("sport") or "unknown",
                    "name": player_name,
                    "team": p.get("team") or "",
                    "game": p.get("game") or p.get("game_key") or "",
                    "game_key": p.get("game_key") or p.get("game") or "",
                    "date": p.get("date") or p.get("game_date") or today_str_allsports,
                    "stat_type": stat_type,
                    "line": p.get("line"),
                    "direction": rec,
                    "over_pct": max(0.0, min(100.0, float(over_prob) * 100.0)),
                    "under_pct": max(0.0, min(100.0, float(under_prob) * 100.0)),
                    "run_id": run_id_allsports,
                    "run_date": today_str_allsports,
                })
            props_rows_window = [
                row
                for row in props_rows_allsports
                if today_str_allsports <= str(row.get("date") or "")[:10] <= horizon_end_str_allsports
            ]
            if props_rows_window:
                save_prop_picks(props_rows_window, game_date=today_str_allsports)
                _log(
                    f"[all-sports] Saved {len(props_rows_window)} forecast-window prop picks to DB"
                    f" ({today_str_allsports}..{horizon_end_str_allsports})"
                )
        except Exception as _db_exc:
            _log(f"[all-sports] DB save skipped: {_db_exc}")

        with _lock:
            _state.update({
                "status": "done",
                "phase": "Complete",
                "last_updated": last_updated,
                "last_updated_ts": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "game_cards_today": _clean(today_cards),
                "game_cards_tomorrow": _clean(tomorrow_cards),
                "best_parlays": _clean(best_parlays),
                "player_props": _clean(table_rows),
                "elite_parlay": None,
            })

        _sse_broadcast("state_update", {
            "status": "done",
            "last_updated": last_updated,
            "game_cards_today": _clean(today_cards),
            "game_cards_tomorrow": _clean(tomorrow_cards),
            "best_parlays": _clean(best_parlays),
            "player_props": _clean(table_rows),
            "elite_parlay": None,
        })
        _log(f"[all-sports] Complete — {len(today_cards)} today, {len(tomorrow_cards)} tomorrow")
        # Free intermediate analysis objects so memory is released back to the OS
        del snapshot, games, bets, best_bet_rows, sentiment_prop_rows, table_rows
        del card_prop_rows, today_cards, tomorrow_cards, today_games, tomorrow_games
        try:
            import gc as _gc
            _gc.collect()
        except Exception:
            pass
    except Exception:
        err = traceback.format_exc()
        _log(f"[all-sports] FAILED:\n{err}")
        with _lock:
            _state["status"] = "error"
            _state["phase"] = "Error"
            _state["error"] = err
        _sse_broadcast("status", {"status": "error", "error": err[:300]})


def _run_soccer_analysis(lock_date: datetime.date | None = None):
    warnings.filterwarnings("ignore")

    with _lock:
        _state["status"] = "running"
        _state["error"] = None
        _state["logs"] = []
        _state["phase"] = _PHASES[0]
        _state["phase_idx"] = 0

    try:
        today_date = _et_calendar_today()
        today_str = today_date.isoformat()
        run_id = f"SOCCER-{today_str}"
        _run_mandatory_daily_calibration(today_date)
        _run_prediction_preflight("soccer")

        need_preds = False
        need_props = False
        try:
            from data.db import has_predictions_for_date, has_prop_picks_for_date, upsert_daily_run
            upsert_daily_run(run_id, today_date, status="RUNNING")
            if lock_date is None:
                need_preds = not has_predictions_for_date(today_date, sport="soccer")
                need_props = not has_prop_picks_for_date(today_date, sport="soccer")
                if need_preds or need_props:
                    lock_date = today_date
                    _log(f"[lock] No soccer picks for {today_date} yet — this run will lock picks")
                else:
                    _log(f"[lock] Soccer picks already saved for {today_date} — updating cards only")
            else:
                need_preds = not has_predictions_for_date(lock_date, sport="soccer")
                need_props = not has_prop_picks_for_date(lock_date, sport="soccer")
        except Exception as lock_exc:
            _log(f"[lock] Soccer lock check failed: {lock_exc}")
            if lock_date is None:
                lock_date = today_date
            need_preds = True
            need_props = True

        _phase(0)
        _log("Fetching soccer fixtures across tournaments...")
        from data.soccer_fetcher import get_matches_today_all, get_matches_tomorrow_all

        today_games = get_matches_today_all() or []
        tomorrow_games = get_matches_tomorrow_all() or []
        display_today = today_games
        display_tomorrow = tomorrow_games
        all_games = display_today + display_tomorrow
        _log(f"Fixtures: {len(display_today)} today, {len(display_tomorrow)} tomorrow")

        _phase(1)
        _log("Running soccer model + sentiment analysis...")
        from models.soccer_predictor import analyze_matches

        analyzed = analyze_matches(all_games, use_sentiment=True) or []
        by_match: dict[str, dict] = {}
        by_game: dict[str, dict] = {}
        for card in analyzed:
            mk = _norm_gk(card.get("match_key") or "")
            gk = _norm_gk(card.get("game_key") or "")
            if mk:
                by_match[mk] = card
            if gk:
                by_game[gk] = card

        all_bets: list[dict] = []
        all_props: list[dict] = []

        for g in all_games:
            home = g.get("home_team", "")
            away = g.get("away_team", "")
            match_key = _norm_gk(f"{away}@{home}")
            game_key = _norm_gk(_compose_game_key(
                away,
                home,
                g.get("game_datetime"),
                g.get("date") or g.get("game_date"),
                g.get("game_time"),
            ))
            card = by_game.get(game_key) or by_match.get(match_key)
            if not card:
                continue
            for bet in card.get("suggested_bets", []) or []:
                all_bets.append(_normalize_soccer_bet(g, bet, today_str))
            for prop in card.get("suggested_props", []) or []:
                all_props.append(_normalize_soccer_prop(g, prop, today_str))

        # Sanitize normalized rows before scoring/parlay/DB persistence.
        try:
            from data.enrichment import clean_bet_row, clean_prop_row
            all_bets = [clean_bet_row(b) for b in all_bets if isinstance(b, dict)]
            all_props = [clean_prop_row(p) for p in all_props if isinstance(p, dict)]
            _log(f"[soccer] row-clean pass: bets={len(all_bets)} props={len(all_props)}")
        except Exception as _row_clean_exc:
            _log(f"[soccer] row-clean skipped: {_row_clean_exc}")

        _attach_tracking_uids(all_bets, all_props)

        _log(f"Soccer bets generated: {len(all_bets)}")
        _phase(2)
        _log(f"Soccer player props generated: {len(all_props)}")

        _phase(3)
        _log("Building soccer parlays...")
        try:
            from models.mlb_predictor import build_parlays
            best_parlays = build_parlays(all_bets + all_props, max_legs=5, top_n=5)
        except Exception as parlay_exc:
            _log(f"Parlay builder fallback: {parlay_exc}")
            best_parlays = []

        if best_parlays:
            try:
                from data.db import save_tracked_parlay

                saved_count = 0
                for idx, combo in enumerate(best_parlays[:8], start=1):
                    if not isinstance(combo, dict):
                        continue
                    legs = combo.get("legs") if isinstance(combo.get("legs"), list) else []
                    if len(legs) < 2:
                        continue
                    save_tracked_parlay(
                        name=f"Auto Soccer {idx}-#{int(combo.get('n_legs') or len(legs))}",
                        legs=legs,
                        combined_odds=float(combo.get("combined_dec") or 0.0),
                        stake_usd=10.0,
                        dedupe_pending=True,
                    )
                    saved_count += 1
                if saved_count:
                    _log(f"[soccer] tracked-parlays auto-saved: {saved_count}")
            except Exception as parlay_save_exc:
                _log(f"[soccer] tracked-parlays auto-save skipped: {parlay_save_exc}")

        _phase(4)
        _log("Saving soccer analysis and building cards...")
        from data.db import save_predictions, save_prop_picks, save_analysis_cache

        def _date_str(value) -> str:
            if isinstance(value, datetime.datetime):
                return value.date().isoformat()
            if isinstance(value, datetime.date):
                return value.isoformat()
            return str(value or "")

        pred_rows = []
        for b in all_bets:
            pred_rows.append({
                "game_key": b.get("game_key", ""),
                "run_id": run_id,
                "run_date": today_str,
                "sport": "soccer",
                "bet_type": b.get("bet_type", "soccer_market"),
                "pick": b.get("pick") or b.get("pick_label") or "Soccer Market",
                "line": b.get("line"),
                "odds_am": b.get("odds_am"),
                "dec_odds": b.get("dec_odds", 1.91),
                "model_prob": b.get("model_prob", 0.5),
                "confidence": b.get("confidence", 50),
                "safety_label": b.get("safety_label", "MODERATE"),
                "game_date": b.get("game_date", today_str),
                "game_time": b.get("game_time", ""),
                "home_team": b.get("home_team", ""),
                "away_team": b.get("away_team", ""),
                "home_starter": "",
                "away_starter": "",
                "sentiment_score": b.get("market_popularity"),
                "news_snippet": (b.get("worth_reason") or "")[:500],
            })

        if lock_date:
            lock_str = _date_str(lock_date)
            if need_preds:
                pred_rows_locked = [p for p in pred_rows if _date_str(p.get("game_date")) == lock_str]
                save_predictions(pred_rows_locked)
            else:
                _log(f"[lock] Soccer predictions already saved for {lock_str} — cards updating")

            if need_props:
                props_locked = [p for p in all_props if _date_str(p.get("date") or p.get("game_date")) == lock_str]
                for pp in props_locked:
                    pp["run_id"] = run_id
                save_prop_picks(props_locked, game_date=lock_date)
            else:
                _log(f"[lock] Soccer props already saved for {lock_str} — tracking only")
        else:
            _log("[lock] No lock_date — updating analysis cards without re-saving soccer picks")

        today_cards = [_build_card(g, all_bets, all_props, "TODAY") for g in display_today]
        tomorrow_cards = [_build_card(g, all_bets, all_props, "TOMORROW") for g in display_tomorrow]

        def _card_score(card: dict) -> float:
            s = [b["safety"] for b in [card.get("moneyline"), card.get("run_line"), card.get("total")] if b]
            return sum(s) / len(s) if s else 0.45

        today_cards.sort(key=_card_score, reverse=True)
        tomorrow_cards.sort(key=_card_score, reverse=True)
        today_cards, tomorrow_cards = _normalize_dashboard_card_buckets(today_cards, tomorrow_cards)
        all_props_flat = sorted(all_props, key=lambda x: x.get("safety", 0), reverse=True)

        now_ts = datetime.datetime.now(datetime.timezone.utc)
        last_updated = now_ts.strftime("%Y-%m-%d %H:%M")
        try:
            save_analysis_cache({
                "game_cards_today": today_cards,
                "game_cards_tomorrow": tomorrow_cards,
                "best_parlays": best_parlays,
                "player_props": all_props_flat,
                "last_updated": last_updated,
            }, cache_date=today_date)
        except Exception as cache_exc:
            _log(f"Soccer cache save error: {cache_exc}")

        with _lock:
            _state.update({
                "status": "done",
                "phase": "Complete",
                "last_updated": last_updated,
                "last_updated_ts": now_ts.isoformat(),
                "game_cards_today": _clean(today_cards),
                "game_cards_tomorrow": _clean(tomorrow_cards),
                "best_parlays": _clean(best_parlays),
                "player_props": _clean(all_props_flat),
                "elite_parlay": None,
            })

        try:
            from data.db import upsert_daily_run
            upsert_daily_run(
                run_id,
                today_date,
                status="DONE",
                games_today=len(today_cards),
                games_tmrw=len(tomorrow_cards),
                props_count=len(all_props_flat),
                parlays_count=len(best_parlays),
                finished=True,
            )
        except Exception as run_exc:
            _log(f"[run-log] {run_exc}")

        if need_preds or need_props:
            try:
                from email_notify import send_daily_picks
                mail_state = {
                    "best_parlays": _clean(best_parlays),
                    "game_cards_today": _clean(today_cards),
                    "player_props": _clean(all_props_flat),
                }
                mail_result = send_daily_picks(mail_state)
                _log(f"[email] Sent daily picks — {mail_result.get('sent',0)} delivered, {mail_result.get('failed',0)} failed")
            except Exception as mail_exc:
                _log(f"[email] Soccer send failed: {mail_exc}")

        _sse_broadcast("state_update", {
            "status": "done",
            "last_updated": last_updated,
            "game_cards_today": _clean(today_cards),
            "game_cards_tomorrow": _clean(tomorrow_cards),
            "best_parlays": _clean(best_parlays),
            "player_props": _clean(all_props_flat),
            "elite_parlay": None,
        })

        _log(
            f"Soccer analysis complete — {len(today_cards)} today, "
            f"{len(tomorrow_cards)} tomorrow, {len(all_props_flat)} props"
        )
    except Exception:
        err = traceback.format_exc()
        _log(f"Soccer analysis FAILED:\n{err}")
        with _lock:
            _state["status"] = "error"
            _state["phase"] = "Error"
            _state["error"] = err
        _sse_broadcast("status", {"status": "error", "error": err[:300]})


def _run_analysis(lock_date: datetime.date | None = None):
    global _last_analysis_started_ts, _last_analysis_finished_ts
    global _last_analysis_ok, _last_analysis_error, _last_analysis_mode

    _last_analysis_started_ts = time.time()
    _last_analysis_mode = str(_ACTIVE_SPORT or "").strip().lower()
    _last_analysis_ok = None
    _last_analysis_error = ""

    if _ACTIVE_SPORT == "all":
        try:
            return _run_all_sports_analysis()
        except Exception as exc:
            _last_analysis_ok = False
            _last_analysis_error = str(exc)
            raise
        finally:
            _last_analysis_finished_ts = time.time()
            if _last_analysis_ok is None:
                with _lock:
                    _last_analysis_ok = (_state.get("status") == "done")
                    if _last_analysis_ok is False:
                        _last_analysis_error = str(_state.get("error") or "")
    if _ACTIVE_SPORT == "soccer":
        try:
            return _run_soccer_analysis(lock_date)
        except Exception as exc:
            _last_analysis_ok = False
            _last_analysis_error = str(exc)
            raise
        finally:
            _last_analysis_finished_ts = time.time()
            if _last_analysis_ok is None:
                with _lock:
                    _last_analysis_ok = (_state.get("status") == "done")
                    if _last_analysis_ok is False:
                        _last_analysis_error = str(_state.get("error") or "")

    warnings.filterwarnings("ignore")

    with _lock:
        _state["status"]    = "running"
        _state["error"]     = None
        _state["logs"]      = []
        _state["phase"]     = _PHASES[0]
        _state["phase_idx"] = 0

    try:
        today_date = _et_calendar_today()
        today_str  = today_date.isoformat()
        tomorrow_str = (today_date + datetime.timedelta(days=1)).isoformat()
        run_id = f"MLB-{today_str}"

        # ── Step 0: Mandatory calibration before any archival/cleanup ─────────
        try:
            from data.db import archive_previous_day_data, upsert_daily_run
            upsert_daily_run(run_id, today_date, status="RUNNING")

            _run_mandatory_daily_calibration(today_date)

            arch = archive_previous_day_data(today_date)
            if arch.get("predictions_archived") or arch.get("props_archived"):
                _log(f"[archive] Archived {arch.get('predictions_archived',0)} preds, "
                     f"{arch.get('props_archived',0)} props from prior days for training")
        except Exception as _ae:
            _log(f"[archive] Archive step skipped: {_ae}")

        _run_prediction_preflight("mlb")

        # Decide whether this run should lock/save today's picks
        need_preds = False
        need_props = False
        try:
            from data.db import has_predictions_for_date, has_prop_picks_for_date
            if lock_date is None:
                today = today_date
                need_preds = not has_predictions_for_date(today)
                need_props = not has_prop_picks_for_date(today)
                if need_preds or need_props:
                    lock_date = today
                    _log(f"[lock] No picks for {today} yet — this run will lock picks")
                else:
                    _log(f"[lock] Today's picks already saved for {today} — updating cards only")
            else:
                need_preds = not has_predictions_for_date(lock_date)
                need_props = not has_prop_picks_for_date(lock_date)
        except Exception as _le:
            _log(f"[lock] Daily lock check failed: {_le}")
            if lock_date is None:
                lock_date = today_date
            need_preds = True
            need_props = True

        # Retrain deferred to after team_stats is loaded (later in pipeline)

        _phase(0)
        _log("Fetching MLB schedule...")
        from data.mlb_fetcher import get_schedule_range
        all_games = get_schedule_range(days_ahead=2)
        today_games    = [g for g in all_games if g.get("date", "") == today_str]
        tomorrow_games = [g for g in all_games if g.get("date", "") == tomorrow_str]

        # ── Clean + enrich MLB schedule games ──────────────────────────────
        try:
            from data.enrichment import clean_game_row, enrich_games_batch
            all_games = [clean_game_row(g) for g in all_games]
            today_games    = [g for g in all_games if g.get("date", "") == today_str]
            tomorrow_games = [g for g in all_games if g.get("date", "") == tomorrow_str]
            _enrich_mlb_limit = max(1, min(30, int(os.getenv("ENRICH_MAX_GAMES", "30"))))
            all_games_enriched = enrich_games_batch(
                today_games + tomorrow_games,
                include_weather=True,
                include_coaching=True,
                include_h2h=True,
                max_games=_enrich_mlb_limit,
                throttle_sec=0.12,
            )
            # Re-split after enrichment
            today_games    = [g for g in all_games_enriched if g.get("date", "") == today_str]
            tomorrow_games = [g for g in all_games_enriched if g.get("date", "") == tomorrow_str]
            _log(f"[mlb] Schedule enriched: {len(today_games)} today, {len(tomorrow_games)} tomorrow")
        except Exception as _enrich_err:
            _log(f"[mlb] Enrichment skipped: {_enrich_err}")

        # Keep all calendar-today games visible on the Today tab, including finals,
        # until the next refresh/day boundary removes them naturally.
        display_today = today_games
        display_tomorrow = tomorrow_games
        _log(f"Schedule: {len(display_today)} today, {len(display_tomorrow)} tomorrow")

        _phase(1)
        _log("Loading team stats and model...")
        from data.mlb_fetcher import build_game_dataset
        from models.mlb_model import load_model, train as train_model
        # Use 3 seasons for robust team differentiation (early-season 2026 data is sparse)
        team_stats = build_game_dataset(MLB_SEASONS[:3])
        model      = load_model()
        _log(f"Team stats rows: {len(team_stats)} (seasons: {sorted(team_stats['season'].unique().tolist(), reverse=True) if not team_stats.empty else 'none'})")
        # Auto-train model if not found or team_stats updated
        if model is None and not team_stats.empty:
            _log("No saved model — training now...")
            try:
                model = train_model(team_stats, verbose=False)
                _log("Model trained and saved.")
            except Exception as e:
                _log(f"Model training failed: {e}")

        # Retrain enhanced model with backfilled game results
        if lock_date:
            try:
                from models.mlb_model import retrain_with_history
                retrain_with_history(team_stats)
                model = load_model()  # reload after retrain
                _log("[backfill] Enhanced model retrained and reloaded.")
            except Exception as _rt_e:
                _log(f"[backfill] Retrain skipped: {_rt_e}")

        _phase(2)
        _log("Fetching injury reports...")
        injuries = []
        injured_names = set()
        def _is_out(status: str) -> bool:
            s = (status or "").lower()
            return any(k in s for k in (
                "out", "il", "dl", "inj", "dtd", "day-to-day",
                "suspended", "inactive", "placed", "covid",
            ))
        try:
            from data.injury_fetcher import fetch_all_injuries
            from data.db import save_injuries
            raw_inj = fetch_all_injuries()
            mlb_inj = raw_inj.get("mlb", [])
            save_injuries("mlb", mlb_inj)
            injuries = mlb_inj
        except Exception as e:
            _log(f"Injuries skipped: {e}")

        if not injuries:
            try:
                from data.db import get_injuries
                injuries = get_injuries(sport="mlb")
                _log(f"Injuries loaded from DB: {len(injuries)}")
            except Exception as e:
                _log(f"Injuries DB fallback skipped: {e}")

        injured_names = {i.get("player_name", "") for i in injuries if _is_out(i.get("status", ""))}

        _phase(3)
        _log("Fetching live odds...")
        odds_by_game = {}
        try:
            from data.odds_fetcher import get_live_odds, odds_to_dataframe
            raw_odds = get_live_odds("baseball_mlb", markets="h2h,totals")
            if raw_odds:
                odds_df = odds_to_dataframe(raw_odds)
                for _, row in odds_df.iterrows():
                    key = f"{row.get('away_team','')}@{row.get('home_team','')}"
                    odds_by_game[key] = row.to_dict()
                _log(f"Odds loaded for {len(odds_by_game)} games")
        except Exception as e:
            _log(f"Odds skipped: {e}")

        _phase(4)
        _log("Running game predictions...")
        import models.mlb_predictor as _mp
        from data.sentiment import get_game_sentiments
        from models.mlb_predictor import predict_game, build_game_bets
        # Lower edge threshold so dashboard shows all value picks (accuracy tracking)
        _orig_edge = _mp.MIN_VALUE_EDGE
        _mp.MIN_VALUE_EDGE = _DASH_MIN_EDGE

        all_bets = []
        sentiment_cache = {}
        def _is_terminal_status(s: str) -> bool:
            sl = (s or "").lower()
            return any(k in sl for k in (
                "final", "game over", "completed", "cancelled", "suspended", "postponed"
            ))

        # Identify which games are still upcoming (not yet final)
        _today_upcoming   = [g for g in today_games    if not _is_terminal_status(g.get("status", ""))]
        _tomorrow_upcoming = [g for g in tomorrow_games if not _is_terminal_status(g.get("status", ""))]
        _all_today_final   = len(today_games) > 0 and len(_today_upcoming) == 0

        for g in today_games + tomorrow_games:
            ht = g.get("home_team", "")
            at = g.get("away_team", "")
            if not ht or not at:
                continue
            st = g.get("status", "")
            if _is_terminal_status(st):
                _log(f"Skip {at}@{ht} status={st!r}")
                continue
            try:
                match_key = _norm_gk(f"{at}@{ht}")
                matchup_sentiment = sentiment_cache.get(match_key)
                if matchup_sentiment is None:
                    try:
                        matchup_sentiment = get_game_sentiments(ht, at)
                    except Exception as sentiment_exc:
                        _log(f"Sentiment skipped for {at}@{ht}: {sentiment_exc}")
                        matchup_sentiment = {}
                    sentiment_cache[match_key] = matchup_sentiment
                pred = predict_game(ht, at, team_stats, model, sentiment=matchup_sentiment, injuries=injuries)
                pred["game_key"] = _compose_game_key(
                    at,
                    ht,
                    g.get("game_datetime"),
                    g.get("date"),
                    g.get("game_time"),
                )
                pred["match_key"] = match_key
                hw   = pred.get("home_win_prob", 0.5)
                _log(f"  {at}@{ht}: home win prob={hw:.1%}")
                gk   = pred["match_key"]
                # Try exact key then reversed
                orow = (odds_by_game.get(gk)
                        or odds_by_game.get(f"{at}@{ht}")
                        or odds_by_game.get(f"{ht}@{at}"))
                gb   = build_game_bets(g, pred, orow)
                all_bets.extend(gb)
                _log(f"  bets for {gk}: {len(gb)}")
            except Exception as e:
                _log(f"Prediction error {ht} vs {at}: {e}")
        _mp.MIN_VALUE_EDGE = _orig_edge
        _log(f"Game bets generated: {len(all_bets)}")

        # ── DB fallback: restore today's saved predictions when all today games are Final ──
        if _all_today_final:
            _log(f"[fallback] All {len(today_games)} today games are Final — loading saved predictions from DB for today's cards...")
            try:
                from data.db import get_predictions_for_date
                saved_today = get_predictions_for_date(today_str)
                if saved_today:
                    all_bets.extend(saved_today)
                    _log(f"[fallback] Restored {len(saved_today)} saved picks for today's cards")
                else:
                    _log("[fallback] No saved predictions found in DB for today")
            except Exception as _fb_e:
                _log(f"[fallback] DB prediction restore failed: {_fb_e}")

        _phase(5)
        _log("Building player prop bets...")
        all_props = []
        try:
            from data.mlb_fetcher import get_starters_props_batch, get_hitter_props_batch
            from models.mlb_predictor import build_player_prop_bets

            prop_odds = {}
            try:
                from data.odds_fetcher import get_player_props_odds
                prop_odds = get_player_props_odds("baseball_mlb") or {}
            except Exception as e:
                _log(f"Prop odds skipped: {e}")

            starter_props = get_starters_props_batch(today_games + tomorrow_games, MLB_SEASONS[0])
            try:
                hitter_props = get_hitter_props_batch(today_games + tomorrow_games, MLB_SEASONS[0])
            except Exception:
                hitter_props = []

            raw_props = starter_props + hitter_props
            scheduled_keys_by_slot = {}
            scheduled_keys_by_match_day = {}
            for sg in today_games + tomorrow_games:
                match_key = _norm_gk(f"{sg.get('away_team','')}@{sg.get('home_team','')}")
                slot = (match_key, str(sg.get("date") or ""), _time_hhmm(sg.get("game_time")))
                unique_key = _compose_game_key(
                    sg.get("away_team", ""),
                    sg.get("home_team", ""),
                    sg.get("game_datetime"),
                    sg.get("date"),
                    sg.get("game_time"),
                )
                scheduled_keys_by_slot[slot] = unique_key
                scheduled_keys_by_match_day.setdefault((match_key, str(sg.get("date") or "")), []).append(unique_key)

            for raw_prop in raw_props:
                game_str = _norm_gk(str(raw_prop.get("game") or ""))
                raw_date = str(raw_prop.get("date") or "")
                raw_time = _time_hhmm(raw_prop.get("game_time"))
                unique_prop_key = scheduled_keys_by_slot.get((game_str, raw_date, raw_time))
                if not unique_prop_key:
                    day_matches = scheduled_keys_by_match_day.get((game_str, raw_date), [])
                    if len(day_matches) == 1:
                        unique_prop_key = day_matches[0]
                raw_prop["match_key"] = game_str
                raw_prop["game_key"] = unique_prop_key or game_str

            _log(f"Raw props fetched: {len(raw_props)}")
            all_props = build_player_prop_bets(
                raw_props,
                injured_players=injured_names,
                odds_lines=prop_odds,
                min_prob=0.60,
                only_over=True,
            )
            if not all_props:
                _log("No qualifying props at 0.60 - relaxing to 0.55")
                all_props = build_player_prop_bets(
                    raw_props,
                    injured_players=injured_names,
                    odds_lines=prop_odds,
                    min_prob=0.55,
                    only_over=True,
                )
            _log(f"Prop bets built: {len(all_props)}")
            raw_props_count = len(all_props)
            all_props = [p for p in all_props if _is_public_prop(p)]
            if len(all_props) != raw_props_count:
                _log(f"Public props tracked: {len(all_props)}/{raw_props_count}")

            # ── Enrich props: recent form, team fatigue ──────────────────
            try:
                from data.enrichment import enrich_props_batch, clean_prop_row
                all_props = [clean_prop_row(p) for p in all_props]
                all_props = enrich_props_batch(all_props, max_props=60, throttle_sec=0.08)
                _log(f"[mlb] Props enriched: {len(all_props)}")
            except Exception as _ep:
                _log(f"[mlb] Props enrichment skipped: {_ep}")
        except Exception as e:
            _log(f"Props error: {e}")

        try:
            from data.enrichment import clean_bet_row, clean_prop_row
            all_bets = [clean_bet_row(b) for b in (all_bets or []) if isinstance(b, dict)]
            all_props = [clean_prop_row(p) for p in (all_props or []) if isinstance(p, dict)]
            _log(f"[mlb] row-clean pass: bets={len(all_bets)} props={len(all_props)}")
        except Exception as _row_clean_exc:
            _log(f"[mlb] row-clean skipped: {_row_clean_exc}")

        _attach_tracking_uids(all_bets, all_props)

        _phase(6)
        _log("Building parlays...")
        from models.mlb_predictor import build_parlays
        best_parlays = build_parlays(all_bets + all_props, max_legs=5, top_n=5)
        _log(f"Parlays built: {len(best_parlays)}")

        if best_parlays:
            try:
                from data.db import save_tracked_parlay

                saved_count = 0
                for idx, combo in enumerate(best_parlays[:8], start=1):
                    if not isinstance(combo, dict):
                        continue
                    legs = combo.get("legs") if isinstance(combo.get("legs"), list) else []
                    if len(legs) < 2:
                        continue
                    save_tracked_parlay(
                        name=f"Auto MLB {idx}-#{int(combo.get('n_legs') or len(legs))}",
                        legs=legs,
                        combined_odds=float(combo.get("combined_dec") or 0.0),
                        stake_usd=10.0,
                        dedupe_pending=True,
                    )
                    saved_count += 1
                if saved_count:
                    _log(f"[mlb] tracked-parlays auto-saved: {saved_count}")
            except Exception as parlay_save_exc:
                _log(f"[mlb] tracked-parlays auto-save skipped: {parlay_save_exc}")

        _phase(7)
        _log(f"Sentiment snapshot ready for {len(sentiment_cache)} matchups")

        _phase(8)
        _log("Saving to database and building cards...")
        from data.db import save_predictions, save_prop_picks, save_analysis_cache

        def _date_str(val) -> str:
            if isinstance(val, datetime.datetime):
                return val.date().isoformat()
            if isinstance(val, datetime.date):
                return val.isoformat()
            return str(val) if val is not None else ""

        try:
            # ── Kalshi ticker + investor grade enrichment ────────────────────
            try:
                from data.kalshi import attach_kalshi_to_bets
                all_bets = attach_kalshi_to_bets(all_bets)
            except Exception as _ke:
                _log(f"[mlb] Kalshi enrichment skipped: {_ke}")
            try:
                from data.polymarket import attach_polymarket_to_bets
                all_bets = attach_polymarket_to_bets(all_bets)
            except Exception as _pe:
                _log(f"[mlb] Polymarket enrichment skipped: {_pe}")
            try:
                from analysis.investor import investor_grade as _ig
                for _b in all_bets:
                    _b.update(_ig(_b))
            except Exception:
                pass

            pred_rows = []
            for b in all_bets:
                pred_rows.append({
                    "game_key":     b.get("game_key", ""),
                    "run_id":       run_id,
                    "run_date":     today_str,
                    "sport":        "mlb",
                    "bet_type":     b.get("bet_type", ""),
                    "pick":         b.get("pick", ""),
                    "line":         b.get("line"),
                    "odds_am":      b.get("odds_am"),
                    "dec_odds":     b.get("dec_odds", 2.0),
                    "model_prob":   b.get("model_prob", 0.0),
                    "confidence":   b.get("confidence", 50),
                    "safety_label": b.get("safety_label", "MODERATE"),
                    "game_date":    b.get("game_date", today_str),
                    "game_time":    b.get("game_time", ""),
                    "home_team":    b.get("home_team", ""),
                    "away_team":    b.get("away_team", ""),
                    "home_starter": b.get("home_starter", ""),
                    "away_starter": b.get("away_starter", ""),
                    "sentiment_score": (sentiment_cache.get(b.get("match_key", ""), {}).get("home", {}) or {}).get("combined"),
                    "news_snippet": "",
                    "signal_type":    b.get("signal_type", "neutral"),
                    "injury_flag":    bool(b.get("injury_flag")),
                    "momentum_flag":  bool(b.get("momentum_flag")),
                    "lineup_flag":    bool(b.get("lineup_flag")),
                    "active_sources": ",".join(b.get("active_sources") or []),
                    "kalshi_ticker":        b.get("kalshi_ticker") or "",
                    "kalshi_event_ticker":  b.get("kalshi_event_ticker") or "",
                    "kalshi_series_ticker": b.get("kalshi_series_ticker") or "",
                    "kalshi_side":          b.get("kalshi_side") or "",
                    "kalshi_price_cents":   int(b.get("kalshi_price_cents") or 0),
                    "kalshi_status":        b.get("kalshi_status") or "unavailable",
                    "polymarket_ticker":        b.get("polymarket_ticker") or "",
                    "polymarket_market_slug":   b.get("polymarket_market_slug") or "",
                    "polymarket_event_slug":    b.get("polymarket_event_slug") or "",
                    "polymarket_series_ticker": b.get("polymarket_series_ticker") or "",
                    "polymarket_side":          b.get("polymarket_side") or "",
                    "polymarket_price":         b.get("polymarket_price"),
                    "polymarket_status":        b.get("polymarket_status") or "unavailable",
                    "grade":                b.get("grade") or "X",
                    "investor_score":       float(b.get("investor_score") or 0),
                })
            for p in all_props:
                game_str = p.get("game") or p.get("game_key") or ""
                away_team = ""
                home_team = ""
                if "@" in game_str:
                    parts = [s.strip() for s in game_str.split("@")]
                    if len(parts) == 2:
                        away_team, home_team = parts[0], parts[1]
                pick_label = f"{p.get('name','')} {p.get('direction','')} {p.get('line','')} {p.get('prop_label','')}".strip()
                pred_rows.append({
                    "game_key":     p.get("game_key", p.get("game", "")),
                    "run_id":       run_id,
                    "run_date":     today_str,
                    "sport":        "mlb",
                    "bet_type":     "player_prop",
                    "pick":         pick_label,
                    "line":         p.get("line"),
                    "odds_am":      p.get("odds_am"),
                    "dec_odds":     p.get("dec_odds", 2.0),
                    "confidence":   p.get("confidence", p.get("conf", 50)),
                    "model_prob":   p.get("model_prob", 0.0),
                    "safety_label": p.get("safety_label", "MODERATE"),
                    "edge":         p.get("edge", 0.0),
                    "stake_usd":    0.0,
                    "ev":           p.get("ev", 0.0),
                    "game_date":    p.get("date", today_str),
                    "game_time":    p.get("game_time", ""),
                    "home_team":    home_team,
                    "away_team":    away_team,
                    "matchup":      game_str,
                    "sentiment_score": p.get("signal_sentiment"),
                })
            if lock_date:
                lock_str = _date_str(lock_date)
                if need_preds:
                    pred_rows_locked = [p for p in pred_rows if _date_str(p.get("game_date")) == lock_str]
                    save_predictions(pred_rows_locked)
                else:
                    _log(f"[lock] Predictions already saved for {lock_str} — cards updating")

                if need_props:
                    props_locked = [p for p in all_props if _date_str(p.get("date")) == lock_str]
                    # Stamp run_id on each prop pick
                    for _pp in props_locked:
                        _pp["run_id"] = run_id
                    save_prop_picks(props_locked, game_date=lock_date)
                else:
                    _log(f"[lock] Props already saved for {lock_str} — tracking only")
            else:
                _log("[lock] No lock_date — updating analysis cards without re-saving picks")
        except Exception as e:
            _log(f"DB save error: {e}")

        # Display only upcoming games; past/live ones were predicted & saved for accuracy tracking
        today_cards    = [_build_card(g, all_bets, all_props, "TODAY")    for g in display_today]
        tomorrow_cards = [_build_card(g, all_bets, all_props, "TOMORROW") for g in display_tomorrow]

        def _card_score(c):
            s = [b["safety"] for b in [c.get("moneyline"), c.get("run_line"), c.get("total")] if b]
            return sum(s) / len(s) if s else 0

        today_cards.sort(key=_card_score, reverse=True)
        tomorrow_cards.sort(key=_card_score, reverse=True)
        today_cards, tomorrow_cards = _normalize_dashboard_card_buckets(today_cards, tomorrow_cards)
        all_props_flat = sorted(all_props, key=lambda x: x.get("safety", 0), reverse=True)

        now_ts = datetime.datetime.now(datetime.timezone.utc)
        last_updated = now_ts.strftime("%Y-%m-%d %H:%M")

        try:
            save_analysis_cache({
                "game_cards_today":    today_cards,
                "game_cards_tomorrow": tomorrow_cards,
                "best_parlays":        best_parlays,
                "player_props":        all_props_flat,
                "last_updated":        last_updated,
            }, cache_date=today_date)
        except Exception as e:
            _log(f"Cache save error: {e}")

        with _lock:
            _state.update({
                "status":              "done",
                "phase":               "Complete",
                "last_updated":        last_updated,
                "last_updated_ts":     now_ts.isoformat(),
                "game_cards_today":    _clean(today_cards),
                "game_cards_tomorrow": _clean(tomorrow_cards),
                "best_parlays":        _clean(best_parlays),
                "player_props":        _clean(all_props_flat),
            })

        # Auto-resolve outcomes for recent past predictions + props + tracked parlays
        try:
            resolved = _run_resolver_locked(days_back=3)
            n_games = int(resolved.get("games", 0) or 0)
            n_props = int(resolved.get("props", 0) or 0)
            n_parlay = int(resolved.get("parlays", 0) or 0)
            _log(f"Auto-resolved: {n_games} game preds, {n_props} props, {n_parlay} parlays")
        except Exception as e:
            _log(f"Auto-resolve skipped: {e}")

        # Build elite parlay and store in state
        try:
            from models.mlb_predictor import build_elite_parlay
            elite = build_elite_parlay(all_bets + all_props)
            with _lock:
                _state["elite_parlay"] = _clean(elite)
            if elite:
                _log(f"Elite parlay built: {elite['n_legs']} legs, "
                     f"combined prob={elite['combined_prob']}%, "
                     f"EV={elite['combined_ev']:.3f}")
            else:
                _log("Elite parlay: no qualifying legs (need 80%+ prob + positive EV + ELITE)")
        except Exception as e:
            _log(f"Elite parlay skipped: {e}")

        _log(f"Analysis complete — {len(today_cards)} today (upcoming), "
             f"{len(tomorrow_cards)} tomorrow, {len(all_props_flat)} props")

        # Mark the daily run as finished in DB
        try:
            from data.db import upsert_daily_run
            upsert_daily_run(run_id, today_date, status="DONE",
                             games_today=len(today_cards),
                             games_tmrw=len(tomorrow_cards),
                             props_count=len(all_props_flat),
                             parlays_count=len(best_parlays),
                             finished=True)
        except Exception as _re:
            _log(f"[run-log] {_re}")

        # ── Send email notification when new picks are saved ──────────────────
        if need_preds or need_props:
            try:
                from email_notify import send_daily_picks
                _mail_state = {
                    "best_parlays":     _clean(best_parlays),
                    "game_cards_today": _clean(today_cards),
                    "player_props":     _clean(all_props_flat),
                }
                _mail_result = send_daily_picks(_mail_state)
                _log(f"[email] Sent daily picks — {_mail_result.get('sent',0)} delivered, "
                     f"{_mail_result.get('failed',0)} failed")
            except Exception as _me:
                _log(f"[email] Send failed: {_me}")

        # Broadcast full state update to all SSE clients
        _sse_broadcast("state_update", {
            "status":              "done",
            "last_updated":        last_updated,
            "game_cards_today":    _clean(today_cards),
            "game_cards_tomorrow": _clean(tomorrow_cards),
            "best_parlays":        _clean(best_parlays),
            "player_props":        _clean(all_props_flat),
            "elite_parlay":        _clean(_state.get("elite_parlay")),
        })

    except Exception:
        err = traceback.format_exc()
        _log(f"Analysis FAILED:\n{err}")
        with _lock:
            _state["status"] = "error"
            _state["phase"]  = "Error"
            _state["error"]  = err
        _sse_broadcast("status", {"status": "error", "error": err[:300]})
        _last_analysis_ok = False
        _last_analysis_error = err[:4000]
    finally:
        _last_analysis_finished_ts = time.time()
        if _last_analysis_ok is None:
            with _lock:
                _last_analysis_ok = (_state.get("status") == "done")
                if _last_analysis_ok is False:
                    _last_analysis_error = str(_state.get("error") or "")[:4000]


@app.route("/")
def index():
    with _lock:
        state = dict(_state)
    today_cards, tomorrow_cards = _normalize_dashboard_card_buckets(
        state.get("game_cards_today", []),
        state.get("game_cards_tomorrow", []),
    )
    return render_template(
        "dashboard.html",
        state=state,
        bankroll=BANKROLL,
        active_sport=_ACTIVE_SPORT,
        phases=_PHASES,
        today_cards=today_cards,
        tomorrow_cards=tomorrow_cards,
        best_parlays=state.get("best_parlays", []),
        all_props=state.get("player_props", []),
    )


@app.route("/api/run", methods=["POST"])
def api_run():
    with _lock:
        if _state["status"] == "running":
            return jsonify({"ok": False, "msg": "Analysis already running"}), 409
        _state["status"]    = "running"
        _state["phase"]     = _PHASES[0]
        _state["phase_idx"] = 0
    threading.Thread(target=_run_analysis, daemon=True).start()
    return jsonify({"ok": True, "msg": "Analysis started"})


@app.route("/api/status")
def api_status():
    with _lock:
        status_payload = {k: _state[k] for k in
            ("status", "phase", "phase_idx", "phase_total", "last_updated", "error")}
    with _backfill_lock:
        status_payload["auto_backfill"] = dict(_last_auto_backfill_info)
        status_payload["auto_backfill"]["running"] = bool(_backfill_running)
    return jsonify(status_payload)


@app.route("/api/backfill/status")
def api_backfill_status():
    with _backfill_lock:
        payload = dict(_last_auto_backfill_info)
        payload["running"] = bool(_backfill_running)
        payload["scheduler"] = {
            "enabled": bool(_AUTO_BACKFILL_ENABLED),
            "hour_et": int(_AUTO_BACKFILL_HOUR_ET),
            "minute_et": int(_AUTO_BACKFILL_MINUTE_ET),
        }
    return jsonify({"ok": True, "backfill": payload})


@app.route("/api/sports/coverage")
def api_sports_coverage():
    force = str(request.args.get("refresh", "0")).strip().lower() in {"1", "true", "yes", "on"}
    coverage = _sports_coverage_snapshot(force_refresh=force)
    return jsonify({"ok": True, "coverage": _clean(coverage)})


@app.route("/api/sports/coverage/sources")
def api_sports_coverage_sources():
    force = str(request.args.get("refresh", "0")).strip().lower() in {"1", "true", "yes", "on"}
    coverage = _sports_source_coverage_snapshot(force_refresh=force)
    return jsonify({"ok": True, "coverage": _clean(coverage)})


@app.route("/api/cached-state")
def api_cached_state():
    state_payload = None
    with _lock:
        if (
            _state.get("game_cards_today")
            or _state.get("game_cards_tomorrow")
            or _state.get("player_props")
            or _state.get("best_parlays")
        ):
            today_str = _et_calendar_today().isoformat()
            tomorrow_str = (_et_calendar_today() + datetime.timedelta(days=1)).isoformat()
            today_cards, tomorrow_cards = _normalize_dashboard_card_buckets(
                _state.get("game_cards_today", []),
                _state.get("game_cards_tomorrow", []),
            )
            cache_updated_at_iso = _state.get("last_updated_ts")
            cache_age_min = None
            if cache_updated_at_iso:
                try:
                    dt = datetime.datetime.fromisoformat(cache_updated_at_iso)
                    now = datetime.datetime.now(datetime.timezone.utc) if dt.tzinfo else datetime.datetime.utcnow()
                    cache_age_min = max(0, int((now - dt).total_seconds() / 60))
                except Exception:
                    cache_age_min = None
            state_payload = {
                "ok":                  True,
                "sport":               _ACTIVE_SPORT,
                "status":              _state["status"],
                "last_updated":        _state["last_updated"],
                "cache_updated_at_iso": cache_updated_at_iso,
                "cache_age_min":        cache_age_min,
                "game_cards_today":    today_cards,
                "game_cards_tomorrow": tomorrow_cards,
                "best_parlays":        _state["best_parlays"],
                "player_props":        _state["player_props"],
                "elite_parlay":        _state.get("elite_parlay"),
            }
            if today_cards or tomorrow_cards or _ACTIVE_SPORT != "all":
                return jsonify(state_payload)

    if _ACTIVE_SPORT != "all":
        try:
            from data.db import get_analysis_cache
            cached = get_analysis_cache(max_age_hours=22)
            if cached:
                today_str = _et_calendar_today().isoformat()
                tomorrow_str = (_et_calendar_today() + datetime.timedelta(days=1)).isoformat()
                cached["game_cards_today"] = _normalize_card_list(cached.get("game_cards_today", []), expected_date=today_str)
                cached["game_cards_tomorrow"] = _normalize_card_list(cached.get("game_cards_tomorrow", []), expected_date=tomorrow_str)
                if not (
                    cached.get("game_cards_today")
                    or cached.get("game_cards_tomorrow")
                    or cached.get("player_props")
                    or cached.get("best_parlays")
                ):
                    cached = None
            if cached:
                cached["ok"] = True
                cached["sport"] = _ACTIVE_SPORT
                return jsonify(cached)
        except Exception:
            pass

    # Fallback: build schedule-only cards so tabs are never blank while analysis/cache is unavailable.
    try:
        today_date = _et_calendar_today()
        today_str = today_date.isoformat()
        tomorrow_str = (today_date + datetime.timedelta(days=1)).isoformat()
        if _ACTIVE_SPORT == "all":
            from data.db import get_upcoming_games

            all_games = get_upcoming_games(days_ahead=2) or []
            sport_count = len({
                _infer_sport_group(g.get("sport") or g.get("competition") or g.get("league") or "")
                for g in (all_games or [])
                if isinstance(g, dict)
            })
            if sport_count < 3 or len(all_games) < 10:
                try:
                    extra_games = _collect_fallback_games_for_all_sports(
                        today_date,
                        today_date + datetime.timedelta(days=1),
                        forecast_days=2,
                    )
                except Exception:
                    extra_games = []
                if extra_games:
                    merged: list[dict] = []
                    seen = set()
                    for g in list(all_games) + list(extra_games):
                        if not isinstance(g, dict):
                            continue
                        key = str(g.get("game_key") or "").strip() or "|".join([
                            str(g.get("sport") or g.get("competition") or ""),
                            str(g.get("match_key") or _norm_gk(f"{g.get('away_team','')}@{g.get('home_team','')}")),
                            str(g.get("game_date") or g.get("date") or ""),
                            str(g.get("game_time") or ""),
                        ])
                        if key in seen:
                            continue
                        seen.add(key)
                        merged.append(g)
                    all_games = merged

            all_bets = _build_model_fallback_bets(all_games)
            today_games = [g for g in all_games if _row_game_date(g) == today_str]
            tomorrow_games = [g for g in all_games if _row_game_date(g) == tomorrow_str]
            fallback_player_props = _build_model_player_props_fallback(today_games + tomorrow_games, max_per_game=10)
            fallback_best_bets = _multi_sport_best_bets_rows(all_bets)
            fallback_props = _merge_all_sports_table_rows(fallback_player_props, fallback_best_bets)
            fallback_today = [_build_card(g, all_bets, fallback_player_props, "TODAY") for g in today_games]
            fallback_tomorrow = [_build_card(g, all_bets, fallback_player_props, "TOMORROW") for g in tomorrow_games]
            fallback_today, fallback_tomorrow = _normalize_dashboard_card_buckets(fallback_today, fallback_tomorrow)
        elif _ACTIVE_SPORT == "soccer":
            from data.soccer_fetcher import get_matches_today_all, get_matches_tomorrow_all

            today_games = get_matches_today_all() or []
            tomorrow_games = get_matches_tomorrow_all() or []
            fallback_today = [_build_card(g, [], [], "TODAY") for g in today_games]
            fallback_tomorrow = [_build_card(g, [], [], "TOMORROW") for g in tomorrow_games]
            fallback_today, fallback_tomorrow = _normalize_dashboard_card_buckets(fallback_today, fallback_tomorrow)
        else:
            from data.mlb_fetcher import get_schedule_range

            all_games = get_schedule_range(days_ahead=2) or []
            today_games = [g for g in all_games if g.get("date", "") == today_str]
            tomorrow_games = [g for g in all_games if g.get("date", "") == tomorrow_str]
            fallback_today = [_build_card(g, [], [], "TODAY") for g in today_games]
            fallback_tomorrow = [_build_card(g, [], [], "TOMORROW") for g in tomorrow_games]
            fallback_today, fallback_tomorrow = _normalize_dashboard_card_buckets(fallback_today, fallback_tomorrow)

        if fallback_today or fallback_tomorrow:
            if state_payload and _ACTIVE_SPORT == "all":
                state_payload["game_cards_today"] = _clean(fallback_today)
                state_payload["game_cards_tomorrow"] = _clean(fallback_tomorrow)
                return jsonify(state_payload)
            return jsonify({
                "ok": True,
                "sport": _ACTIVE_SPORT,
                "status": "idle",
                "last_updated": datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
                "game_cards_today": _clean(fallback_today),
                "game_cards_tomorrow": _clean(fallback_tomorrow),
                "best_parlays": [],
                "player_props": _clean(fallback_props) if _ACTIVE_SPORT == "all" else [],
                "elite_parlay": None,
            })
    except Exception:
        pass

    return jsonify({
        "ok": True if _ACTIVE_SPORT == "all" else False,
        "sport": _ACTIVE_SPORT,
        "status": "idle",
        "game_cards_today": [],
        "game_cards_tomorrow": [],
        "best_parlays": [],
        "player_props": [],
        "elite_parlay": None,
    })


@app.route("/api/logs")
def api_logs():
    with _lock:
        return jsonify({"logs": list(_state.get("logs", []))})


@app.route("/api/parlay/build-elite", methods=["POST"])
def api_parlay_build_elite():
    """Build and save one elite parlay from the current in-memory state."""
    with _lock:
        all_props = list(_state.get("player_props", []))
        # Reconstruct from game cards for bet-level picks
        raw_picks = []
        for card in (_state.get("game_cards_today", []) +
                     _state.get("game_cards_tomorrow", [])):
            for slot in ("moneyline", "run_line", "total", "f5_moneyline",
                         "f5_total", "home_team_total", "away_team_total"):
                b = card.get(slot)
                if b:
                    raw_picks.append(b)
        raw_picks += all_props

    if not raw_picks:
        return jsonify({"ok": False, "msg": "No picks available — run analysis first"})

    try:
        from models.mlb_predictor import build_elite_parlay
        parlay = build_elite_parlay(raw_picks)
        if parlay:
            with _lock:
                _state["elite_parlay"] = _clean(parlay)
            return jsonify({"ok": True, "parlay": _clean(parlay)})
        else:
            return jsonify({
                "ok":  False,
                "msg": "No qualifying legs found. Need model_prob ≥ 80%, positive EV, and ELITE safety.",
            })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/calibration")
def api_calibration():
    """Return model calibration stats (ECE + per-bin breakdown)."""
    days = int(request.args.get("days", 90))
    try:
        from data.db import get_calibration_data
        return jsonify({"ok": True, "calibration": get_calibration_data(days_back=days)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/parlay/performance")
def api_parlay_performance():
    """Win/loss/ROI stats for all tracked parlays."""
    try:
        _maybe_trigger_tracking_sync_on_read()
        from data.db import get_parlay_performance_stats, prune_tracked_parlays_to_date
        current_only_raw = str(request.args.get("current_only", "1")).strip().lower()
        current_only = current_only_raw in {"1", "true", "yes", "on"}
        target_date = _et_calendar_today() if current_only else None
        if current_only:
            # Keep tracked_parlays table aligned with the "today-only" parlay tab policy.
            prune_tracked_parlays_to_date(target_date=target_date)
        db_sport = None if _ACTIVE_SPORT == "all" else _ACTIVE_SPORT
        return jsonify({"ok": True, "stats": get_parlay_performance_stats(sport=db_sport, target_date=target_date)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/parlay/auto-resolve", methods=["POST"])
def api_parlay_auto_resolve():
    """Auto-resolve pending tracked parlays based on leg prediction outcomes."""
    try:
        # First run the universal resolver so game/prop outcomes are up-to-date
        result   = _run_resolver_locked(days_back=21)
        n_parlay = result.get("parlays", 0)
        n_other  = result.get("games", 0) + result.get("props", 0)
        return jsonify({
            "ok":      True,
            "resolved": n_parlay,
            "msg":     f"Resolved {n_parlay} parlay(s) + {n_other} game/prop bet(s)",
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/auto-improve", methods=["POST"])
def api_auto_improve():
    """Manual trigger is deprecated; calibration now runs automatically each ET day."""
    return jsonify({
        "ok": False,
        "disabled": True,
        "msg": "Manual auto-improve is disabled. Daily calibration now runs automatically before archival.",
    }), 410


@app.route("/api/backfill", methods=["POST"])
def api_backfill():
    """
    Run the full backfill pipeline (news → injuries → game scores → retrain).
    Accepts optional JSON body: {"days_back": 3}
    Runs in a background thread; returns immediately.
    """
    with _lock:
        if _state["status"] == "running":
            return jsonify({"ok": False, "msg": "Analysis already running"}), 409

    days_back = int((request.get_json(silent=True) or {}).get("days_back", 3))

    def _run_backfill():
        import sys, os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
        try:
            with _lock:
                _state["status"] = "running"
                _state["phase"]  = "Backfilling data"
            _log(f"[backfill] Starting backfill (days_back={days_back})")

            # News
            try:
                from data.history_ingest import backfill_news
                n = backfill_news(days_back=days_back)
                _log(f"[backfill] News rows ingested: {n}")
            except Exception as e:
                _log(f"[backfill] News error: {e}")

            # Injuries
            try:
                from data.history_ingest import backfill_injuries
                inj = backfill_injuries(days_back=days_back)
                _log(f"[backfill] Injury rows: {inj}")
            except Exception as e:
                _log(f"[backfill] Injury error: {e}")

            # Game results
            try:
                from data.history_ingest import backfill_game_results
                n_games = backfill_game_results(days_back=days_back)
                _log(f"[backfill] Completed games saved: {n_games}")
            except Exception as e:
                _log(f"[backfill] Game results error: {e}")

            # Retrain
            try:
                from data.mlb_fetcher import build_game_dataset
                from models.mlb_model import retrain_with_history
                team_stats = build_game_dataset(MLB_SEASONS[:3])
                retrain_with_history(team_stats)
                _log("[backfill] Model retrained and saved.")
            except Exception as e:
                _log(f"[backfill] Retrain error: {e}")

            with _lock:
                _state["status"] = "idle"
                _state["phase"]  = "Backfill complete"
        except Exception as e:
            _log(f"[backfill] Fatal error: {e}")
            with _lock:
                _state["status"] = "idle"
                _state["error"]  = str(e)

    threading.Thread(target=_run_backfill, daemon=True).start()
    return jsonify({"ok": True, "msg": f"Backfill started (days_back={days_back})"})


@app.route("/api/performance")
def api_performance():
    try:
        _maybe_trigger_tracking_sync_on_read()
        from data.db import get_performance_stats, get_settlement_summary
        current_only_raw = str(request.args.get("current_only", "0")).strip().lower()
        current_only = current_only_raw in {"1", "true", "yes", "on"}
        target_date = _et_calendar_today() if current_only else None
        db_sport = None if _ACTIVE_SPORT == "all" else _ACTIVE_SPORT
        return jsonify(
            {
                "ok": True,
                "stats": get_performance_stats(sport=db_sport, target_date=target_date),
                "settlement_summary": get_settlement_summary(
                    sport=db_sport,
                    target_date=target_date,
                    stale_hours=6,
                ),
            }
        )
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/prop-performance")
def api_prop_performance():
    try:
        _maybe_trigger_tracking_sync_on_read()
        from data.db import get_prop_performance_stats
        current_only_raw = str(request.args.get("current_only", "1")).strip().lower()
        current_only = current_only_raw in {"1", "true", "yes", "on"}
        target_date = _et_calendar_today() if current_only else None
        db_sport = None if _ACTIVE_SPORT == "all" else _ACTIVE_SPORT
        stats = get_prop_performance_stats(sport=db_sport, target_date=target_date)
        rows = [r for r in (stats.get("by_prop_type") or []) if isinstance(r, dict)]
        has_resolved = any((int(r.get("wins") or 0) + int(r.get("losses") or 0)) > 0 for r in rows)
        if current_only and not has_resolved:
            # Avoid empty hit-rate table on days where today's props are still pending.
            stats = get_prop_performance_stats(sport=db_sport, days_back=21)
            stats["fallback_window_days"] = 21
        return jsonify({"ok": True, "stats": stats})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/predictions")
def api_predictions():
    days    = int(request.args.get("days", 30))
    outcome = request.args.get("outcome")
    try:
        _maybe_trigger_tracking_sync_on_read()
        from data.db import get_predictions
        db_sport = None if _ACTIVE_SPORT == "all" else _ACTIVE_SPORT
        preds = get_predictions(days=days, outcome=outcome or None, sport=db_sport)
        current_only_raw = str(request.args.get("current_only", "0")).strip().lower()
        if current_only_raw in {"1", "true", "yes", "on"}:
            today_iso = _et_calendar_today().isoformat()
            preds = [p for p in preds if str(p.get("game_date", ""))[:10] == today_iso]
        preds = _annotate_tracking_phase(preds)
        return jsonify({"ok": True, "predictions": _clean(preds)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "predictions": []})


@app.route("/api/prop-history")
def api_prop_history():
    days = int(request.args.get("days", 30))
    outcome = request.args.get("outcome")
    try:
        _maybe_trigger_tracking_sync_on_read()
        from data.db import get_prop_history

        db_sport = None if _ACTIVE_SPORT == "all" else _ACTIVE_SPORT
        rows = get_prop_history(days=days, outcome=outcome or None, sport=db_sport)
        current_only_raw = str(request.args.get("current_only", "0")).strip().lower()
        if current_only_raw in {"1", "true", "yes", "on"}:
            today_iso = _et_calendar_today().isoformat()
            rows = [r for r in rows if str(r.get("game_date", ""))[:10] == today_iso]
        rows = _annotate_tracking_phase(rows)
        return jsonify({"ok": True, "props": _clean(rows)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "props": []})


def _match_status_bucket(status: str) -> str:
    s = str(status or "").lower()
    if any(k in s for k in ("in progress", "in_play", "live", "halftime", "paused")):
        return "live"
    if any(k in s for k in ("final", "finished", "completed")):
        return "finished"
    return "scheduled"


_TRACKING_SYNC_MIN_INTERVAL_SEC = max(
    10,
    int(
        os.getenv(
            "TRACKING_SYNC_MIN_INTERVAL_SEC",
            "180" if _BILL_SAVER_MODE else "20",
        )
        or ("180" if _BILL_SAVER_MODE else "20")
    ),
)
_SYNC_ON_READ_ENABLED = str(
    os.getenv("SYNC_ON_READ_ENABLED", "0" if _BILL_SAVER_MODE else "1")
).strip().lower() in {"1", "true", "yes", "on"}
_last_tracking_sync_ts = 0.0
_tracking_sync_lock = threading.Lock()


def _tracking_sync_due() -> bool:
    return (time.time() - float(_last_tracking_sync_ts or 0.0)) >= _TRACKING_SYNC_MIN_INTERVAL_SEC


def _maybe_trigger_tracking_sync_sync(force: bool = False):
    """Run resolver inline (when due) so tracked API responses are fresh."""
    global _last_tracking_sync_ts
    if not force and not _tracking_sync_due():
        return
    if not _tracking_sync_lock.acquire(blocking=False):
        return
    try:
        _run_resolver_locked(days_back=5)
    except Exception:
        pass
    finally:
        _last_tracking_sync_ts = time.time()
        try:
            _tracking_sync_lock.release()
        except Exception:
            pass


def _maybe_trigger_tracking_sync_on_read(force: bool = False):
    """DB cost guard for read APIs; disable frequent resolver writes unless explicitly enabled."""
    if not force and not _SYNC_ON_READ_ENABLED:
        return
    _maybe_trigger_tracking_sync_sync(force=force)


def _live_status_for_game_key(game_key: str) -> str:
    key = str(game_key or "").strip().lower()
    if not key:
        return ""
    prefix = _norm_gk(key.split("#", 1)[0])
    with _lock:
        live_scores = dict(_state.get("live_scores") or {})
    for k, payload in live_scores.items():
        kk = str(k or "").strip().lower()
        if not kk:
            continue
        if kk == key:
            return str((payload or {}).get("status") or "")
        kk_prefix = _norm_gk(kk.split("#", 1)[0])
        if prefix and kk_prefix == prefix:
            return str((payload or {}).get("status") or "")
    return ""


def _tracking_phase_for_row(row: dict) -> str:
    if not isinstance(row, dict):
        return "upcoming"
    outcome = str(row.get("outcome") or "PENDING").upper()
    if outcome != "PENDING":
        return "settled"
    game_key = str(row.get("game_key") or row.get("game") or "")
    bucket = _match_status_bucket(_live_status_for_game_key(game_key))
    if bucket == "live":
        return "live"
    if bucket == "finished":
        # Keep finished-but-unresolved rows out of upcoming while resolver catches up.
        return "live"

    # Exchange-side readiness hints (Kalshi/Polymarket) help phase rows faster
    # when public score feeds lag behind market start/final transitions.
    kalshi_status = str(row.get("kalshi_status") or "").strip().lower()
    poly_status = str(row.get("polymarket_status") or "").strip().lower()
    if kalshi_status in {"started", "done"}:
        return "live"
    if poly_status in {"started", "done", "resolved", "closed"}:
        return "live"
    return "upcoming"


def _annotate_tracking_phase(rows: list[dict]) -> list[dict]:
    out = []
    for row in (rows or []):
        if not isinstance(row, dict):
            continue
        item = dict(row)
        item["tracking_phase"] = _tracking_phase_for_row(item)
        out.append(item)
    return out


def _annotate_parlay_tracking(rows: list[dict]) -> list[dict]:
    out = []
    for row in (rows or []):
        if not isinstance(row, dict):
            continue
        item = dict(row)
        outcome = str(item.get("outcome") or "PENDING").upper()
        if outcome != "PENDING":
            item["tracking_phase"] = "settled"
        else:
            legs = [leg for leg in (item.get("legs_json") or []) if isinstance(leg, dict)]
            item["tracking_phase"] = "live" if any(_tracking_phase_for_row(leg) == "live" for leg in legs) else "upcoming"
        out.append(item)
    return out


def _normalize_prop_history_row_for_tracking(row: dict) -> dict:
    """Shape a prop_history row into the single-bet schema the tracking UI renders."""
    stats = row.get("stats_json") if isinstance(row.get("stats_json"), dict) else {}
    direction = str(row.get("recommendation") or "").upper()
    prop_type = str(row.get("prop_type") or "player_prop").replace("_", " ")
    line = row.get("line")
    line_txt = f" {line}" if (line is not None and line != "") else ""
    pick = " ".join(
        f"{row.get('player_name') or 'Player'} {direction}{line_txt} {prop_type}".split()
    )
    out = dict(row)
    out["pick"] = pick
    out["bet_type"] = "player_prop"
    out["game"] = row.get("game") or stats.get("game") or ""
    out["game_key"] = row.get("game_key") or stats.get("game_key") or stats.get("game") or ""
    out["predicted_at"] = row.get("detected_at") or out.get("predicted_at") or ""
    return out


def _single_tracking_uid(row: dict) -> str:
    """Deterministic dedupe key matching the prior client-side logic."""
    uid = row.get("bet_uid")
    if uid:
        return str(uid).strip().lower()
    parts = [
        str(row.get("game_key") or ""),
        str(row.get("bet_type") or ""),
        str(row.get("pick") or ""),
        ("" if row.get("line") is None else str(row.get("line"))),
        str(row.get("game_date") or ""),
    ]
    return "|".join(parts).strip().lower()


def _merge_tracking_row(base: dict, incoming: dict) -> dict:
    merged = dict(base or {})
    for key, value in (incoming or {}).items():
        if value is None or value == "":
            continue
        if merged.get(key) in (None, ""):
            merged[key] = value
    return merged


def _bucket_tracking_rows(rows: list[dict]) -> dict:
    """Split annotated rows into upcoming/live/won/lost/other using outcome + tracking_phase."""
    buckets = {"upcoming": [], "live": [], "won": [], "lost": [], "other": []}
    for row in (rows or []):
        if not isinstance(row, dict):
            continue
        outcome = str(row.get("outcome") or "PENDING").upper()
        if outcome == "WIN":
            buckets["won"].append(row)
        elif outcome == "LOSS":
            buckets["lost"].append(row)
        elif outcome == "PENDING":
            if str(row.get("tracking_phase") or "") == "live":
                buckets["live"].append(row)
            else:
                buckets["upcoming"].append(row)
        else:
            buckets["other"].append(row)
    return buckets


def _tracking_bucket_counts(buckets: dict) -> dict:
    won = len(buckets.get("won") or [])
    lost = len(buckets.get("lost") or [])
    live = len(buckets.get("live") or [])
    upcoming = len(buckets.get("upcoming") or [])
    other = len(buckets.get("other") or [])
    pending = live + upcoming
    settled = won + lost
    return {
        "won": won,
        "lost": lost,
        "live": live,
        "upcoming": upcoming,
        "other": other,
        "pending": pending,
        "settled": settled,
        "total": won + lost + live + upcoming + other,
        "win_rate": round(won / settled * 100) if settled else None,
    }


@app.route("/api/tournaments")
def api_tournaments():
    if _ACTIVE_SPORT == "all":
        try:
            snap = _build_multi_sport_snapshot(force_refresh=False)
            return jsonify({"ok": True, "tournaments": snap.get("tournaments", [])})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e), "tournaments": []})
    if _ACTIVE_SPORT != "soccer":
        return jsonify({"ok": True, "tournaments": []})
    try:
        from data.soccer_fetcher import get_tournaments, get_matches_today_all, get_matches_tomorrow_all

        tournaments = get_tournaments() or []
        counts: dict[str, int] = {}
        for m in (get_matches_today_all() or []) + (get_matches_tomorrow_all() or []):
            code = str(m.get("competition") or "").strip().upper()
            if not code:
                continue
            counts[code] = counts.get(code, 0) + 1

        payload = []
        for t in tournaments:
            code = str(t.get("code") or "").upper()
            row = dict(t)
            row["match_count"] = int(counts.get(code, 0))
            payload.append(row)

        payload.sort(key=lambda x: (x.get("match_count", 0), x.get("name", "")), reverse=True)
        return jsonify({"ok": True, "tournaments": payload})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "tournaments": []})


@app.route("/api/tournament/data")
def api_tournament_data():
    if _ACTIVE_SPORT == "all":
        code = str(request.args.get("code", "") or "").strip()
        sport_filter = _infer_sport_group(str(request.args.get("sport", "all") or "all").strip())
        if sport_filter in {"", "other", "all"}:
            sport_filter = "all"
        query = str(request.args.get("q", "") or "").strip().lower()
        status_filter = str(request.args.get("status", "all") or "all").strip().lower()
        try:
            snap = _build_multi_sport_snapshot(force_refresh=bool(request.args.get("refresh")))
            tournaments = {str(t.get("code") or ""): t for t in (snap.get("tournaments") or [])}
            if code and code not in tournaments:
                return jsonify({"ok": False, "error": f"Unsupported sport code: {code}", "matches": [], "standings": [], "top_scorers": []}), 404

            matches = []
            for m in (snap.get("games") or []):
                if code and str(m.get("competition") or "") != code:
                    continue
                ms = _infer_sport_group(m.get("sport") or m.get("competition") or m.get("league") or "")
                if sport_filter != "all" and ms != sport_filter:
                    continue
                bucket = _match_status_bucket(m.get("status", ""))
                if status_filter in {"scheduled", "live", "finished"} and bucket != status_filter:
                    continue
                if query:
                    home = str(m.get("home_team") or "").lower()
                    away = str(m.get("away_team") or "").lower()
                    if query not in home and query not in away:
                        continue
                row = dict(m)
                row["status_bucket"] = bucket
                matches.append(row)

            matches.sort(key=lambda x: (x.get("game_date") or x.get("date") or "", x.get("game_time") or ""))

            match_keys = {
                _norm_gk(str(m.get("match_key") or f"{m.get('away_team','')}@{m.get('home_team','')}"))
                for m in matches
            }
            team_map = {}
            for m in matches:
                for t in (m.get("home_team"), m.get("away_team")):
                    token = re.sub(r"[^a-z0-9]+", "", str(t or "").strip().lower())
                    if token:
                        team_map[token] = str(t or "").strip()

            def _resolve_team_name(name: str) -> str:
                token = re.sub(r"[^a-z0-9]+", "", str(name or "").strip().lower())
                if not token:
                    return ""
                if token in team_map:
                    return team_map[token]
                for tk, label in team_map.items():
                    if token in tk or tk in token:
                        return label
                return ""

            selected_bets = []
            if match_keys:
                for b in (snap.get("bets") or []):
                    bm = _norm_gk(str(b.get("match_key") or f"{b.get('away_team','')}@{b.get('home_team','')}"))
                    if bm not in match_keys:
                        continue
                    selected_bets.append(b)

            with _lock:
                state_props = list(_state.get("player_props") or [])
            selected_props = []
            if match_keys:
                for p in state_props:
                    pm = _norm_gk(str(p.get("match_key") or ""))
                    if not pm:
                        pm = _norm_gk(str(p.get("game") or "").replace(" @ ", "@"))
                    if pm not in match_keys:
                        continue
                    ps = _infer_sport_group(p.get("sport") or p.get("competition") or p.get("league") or "")
                    if sport_filter != "all" and ps != sport_filter:
                        continue
                    selected_props.append(p)

            team_stats: dict[str, dict] = {}
            for m in matches:
                for label in (str(m.get("home_team") or "").strip(), str(m.get("away_team") or "").strip()):
                    if not label:
                        continue
                    team_stats.setdefault(label, {
                        "team": label,
                        "games": 0,
                        "team_picks": 0,
                        "against_picks": 0,
                        "prop_count": 0,
                        "prob_sum": 0.0,
                        "prob_n": 0,
                        "markets": {},
                    })
                    team_stats[label]["games"] += 1

            for b in selected_bets:
                home = str(b.get("home_team") or "").strip()
                away = str(b.get("away_team") or "").strip()
                pick = str(b.get("pick") or "").strip().lower()
                bet_type = str(b.get("bet_type") or "moneyline").strip().lower()
                prob = float(b.get("model_prob") or 0.0)

                pick_team = ""
                if home and home.lower() in pick:
                    pick_team = home
                elif away and away.lower() in pick:
                    pick_team = away
                elif "home" in pick and home:
                    pick_team = home
                elif "away" in pick and away:
                    pick_team = away

                if pick_team and pick_team in team_stats:
                    row = team_stats[pick_team]
                    row["team_picks"] += 1
                    row["prob_sum"] += prob
                    row["prob_n"] += 1
                    row["markets"][bet_type] = row["markets"].get(bet_type, 0) + 1
                    opp = away if pick_team == home else home
                    if opp in team_stats:
                        team_stats[opp]["against_picks"] += 1

            for p in selected_props:
                team_label = _resolve_team_name(p.get("team") or "")
                if team_label and team_label in team_stats:
                    team_stats[team_label]["prop_count"] += 1

            team_rows = []
            for row in team_stats.values():
                avg_prob = (row["prob_sum"] / row["prob_n"]) if row["prob_n"] else 0.0
                markets = row.get("markets") or {}
                top_market = max(markets.items(), key=lambda kv: kv[1])[0] if markets else "—"
                team_rows.append({
                    "team": row["team"],
                    "games": int(row["games"]),
                    "team_picks": int(row["team_picks"]),
                    "against_picks": int(row["against_picks"]),
                    "prop_count": int(row["prop_count"]),
                    "avg_model": round(avg_prob * 100.0, 1),
                    "top_market": top_market.replace("_", " "),
                })

            team_rows.sort(key=lambda x: (x.get("team_picks", 0), x.get("prop_count", 0), x.get("avg_model", 0.0)), reverse=True)

            market_stats: dict[str, dict] = {}
            for b in selected_bets:
                market = str(b.get("bet_type") or "moneyline").strip().lower() or "moneyline"
                entry = market_stats.setdefault(market, {"count": 0, "prob_sum": 0.0, "best_prob": 0.0, "best_pick": ""})
                prob = float(b.get("model_prob") or 0.0)
                entry["count"] += 1
                entry["prob_sum"] += prob
                if prob > entry["best_prob"]:
                    entry["best_prob"] = prob
                    entry["best_pick"] = str(b.get("pick") or "")

            for p in selected_props:
                market = str(p.get("stat_type") or "player_prop").strip().lower() or "player_prop"
                entry = market_stats.setdefault(market, {"count": 0, "prob_sum": 0.0, "best_prob": 0.0, "best_pick": ""})
                prob = float(p.get("model_prob") or 0.0)
                entry["count"] += 1
                entry["prob_sum"] += prob
                if prob > entry["best_prob"]:
                    entry["best_prob"] = prob
                    entry["best_pick"] = f"{p.get('name','')} {str(p.get('direction') or '').upper()} {p.get('line')}"

            market_rows = []
            for name, stat in market_stats.items():
                cnt = int(stat.get("count") or 0)
                avg_prob = (float(stat.get("prob_sum") or 0.0) / cnt) if cnt else 0.0
                market_rows.append({
                    "name": name.replace("_", " "),
                    "count": cnt,
                    "avg_model": round(avg_prob * 100.0, 1),
                    "best_pick": str(stat.get("best_pick") or ""),
                    "best_model": round(float(stat.get("best_prob") or 0.0) * 100.0, 1),
                })
            market_rows.sort(key=lambda x: (x.get("count", 0), x.get("avg_model", 0.0)), reverse=True)

            return jsonify({
                "ok": True,
                "code": code,
                "sport": sport_filter,
                "competition": tournaments.get(code, {"code": code, "name": code or "All Sports"}),
                "matches": matches,
                "standings": [{"group": "Team Bet Analysis", "mode": "team_bets", "table": team_rows}],
                "top_scorers": market_rows,
            })
        except Exception as e:
            return jsonify({"ok": False, "error": str(e), "matches": [], "standings": [], "top_scorers": []})

    if _ACTIVE_SPORT != "soccer":
        return jsonify({"ok": False, "error": "Tournament data is available only in soccer mode"}), 400

    code = str(request.args.get("code", "PL") or "PL").strip().upper()
    try:
        days = int(request.args.get("days", 7) or 7)
    except Exception:
        days = 7
    days = max(1, min(days, 14))
    query = str(request.args.get("q", "") or "").strip().lower()
    status_filter = str(request.args.get("status", "all") or "all").strip().lower()

    try:
        from data.soccer_fetcher import (
            TOURNAMENTS,
            get_competition_info,
            get_matches_in_range,
            get_standings,
            get_top_scorers,
        )

        if code not in TOURNAMENTS:
            return jsonify({"ok": False, "error": f"Unsupported tournament code: {code}"}), 404

        start_date = _et_calendar_today()
        end_date = start_date + datetime.timedelta(days=days)
        matches = get_matches_in_range(code, start_date.isoformat(), end_date.isoformat()) or []

        filtered = []
        for m in matches:
            bucket = _match_status_bucket(m.get("status", ""))
            if status_filter in {"scheduled", "live", "finished"} and bucket != status_filter:
                continue
            if query:
                home = str(m.get("home_team") or "").lower()
                away = str(m.get("away_team") or "").lower()
                if query not in home and query not in away:
                    continue
            row = dict(m)
            row["status_bucket"] = bucket
            filtered.append(row)

        filtered.sort(key=lambda x: (x.get("game_date") or x.get("date") or "", x.get("game_time") or ""))
        standings = get_standings(code) or []
        top_scorers = get_top_scorers(code, limit=20) or []
        return jsonify({
            "ok": True,
            "code": code,
            "competition": get_competition_info(code),
            "matches": filtered,
            "standings": standings,
            "top_scorers": top_scorers,
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "matches": [], "standings": [], "top_scorers": []})


# ─── Universal all-sport outcome resolver ────────────────────────────────────
_SOCCER_STAT_MAP = {
    # Standard ESPN soccer stat block keys (after _stat_key_norm lowercases & strips)
    "goals": "goals",
    "g": "goals",
    "assists": "assists",
    "a": "assists",
    "shotsontarget": "shots_on_target",
    "shotsongoal": "shots_on_target",
    "sot": "shots_on_target",   # ESPN abbreviation
    "sh": "shots",
    "shots": "shots",
    "keypasses": "key_passes",
    "kp": "key_passes",         # ESPN abbreviation (if present)
    "tackles": "tackles",
    "tkl": "tackles",           # ESPN abbreviation (if present)
    "tackleswon": "tackles",
    "saves": "saves",
    "sv": "saves",              # ESPN abbreviation (goalkeeper)
}

# Soccer prop types that ESPN boxscore API cannot resolve (Opta-only data).
# Props of these types are archived after 3 days if still PENDING.
_SOCCER_ESPN_UNRESOLVABLE = frozenset({
    "key_passes", "keypasses",
    "tackles", "tackles_won", "tackleswon",
})

_ESPN_RESOLVE_CONFIGS = [
    # (espn_sport_path, espn_league_path, sport_label, boxscore_stat_map)
    ("basketball", "nba",                       "basketball",      {"points": "points", "rebounds": "totalRebounds", "assists": "assists", "steals": "steals", "blocks": "blocks", "threePointFieldGoalsMade": "threes"}),
    ("basketball", "wnba",                      "basketball",      {"points": "points", "rebounds": "totalRebounds", "assists": "assists"}),
    ("hockey",     "nhl",                       "icehockey",       {"goals": "goals", "assists": "assists", "shots": "shots_on_goal", "saves": "saves"}),
    ("football",   "nfl",                       "americanfootball", {"passingYards": "passing_yards", "rushingYards": "rushing_yards", "receivingYards": "receiving_yards", "touchdowns": "touchdowns", "receptions": "receptions"}),
    ("baseball",   "mlb",                       "baseball",        {"hits": "hits", "homeRuns": "home_runs", "rbi": "rbi", "strikeouts": "strikeouts"}),
    ("soccer",     "usa.1",                     "soccer",          _SOCCER_STAT_MAP),
    ("soccer",     "eng.1",                     "soccer",          _SOCCER_STAT_MAP),
    ("soccer",     "esp.1",                     "soccer",          _SOCCER_STAT_MAP),
    ("soccer",     "ger.1",                     "soccer",          _SOCCER_STAT_MAP),
]


def _run_resolver_locked(days_back: int = 3) -> dict:
    """Run resolver with a non-blocking global lock to avoid overlap across threads."""
    global _last_resolve_started_ts, _last_resolve_finished_ts
    global _last_resolve_ok, _last_resolve_error
    if not _resolve_run_lock.acquire(blocking=False):
        return {"games": 0, "props": 0, "parlays": 0, "skipped": True}
    _last_resolve_started_ts = time.time()
    _last_resolve_ok = None
    _last_resolve_error = ""
    try:
        result = _resolve_all_sports_outcomes(days_back=days_back)
        _last_resolve_ok = True
        return result
    except Exception as exc:
        _last_resolve_ok = False
        _last_resolve_error = str(exc)
        raise
    finally:
        _last_resolve_finished_ts = time.time()
        try:
            _resolve_run_lock.release()
        except Exception:
            pass


def _ts_to_iso(ts: float) -> str | None:
    if not ts or ts <= 0:
        return None
    try:
        return datetime.datetime.fromtimestamp(float(ts), tz=datetime.timezone.utc).isoformat()
    except Exception:
        return None


def _build_live_feed_health_report(state_snapshot: dict[str, Any]) -> dict[str, Any]:
    live_scores = state_snapshot.get("live_scores") if isinstance(state_snapshot, dict) else {}
    today_cards = state_snapshot.get("game_cards_today") if isinstance(state_snapshot, dict) else []
    tomorrow_cards = state_snapshot.get("game_cards_tomorrow") if isinstance(state_snapshot, dict) else []

    live_by_game: dict[str, dict[str, Any]] = {}
    live_by_match: dict[str, dict[str, Any]] = {}
    for key, row in (live_scores or {}).items():
        if not isinstance(row, dict):
            continue
        game_key = str(row.get("game_key") or key or "").strip()
        if game_key:
            live_by_game[game_key] = row
        match_key = _norm_gk(str(row.get("match_key") or "").strip())
        if not match_key and game_key:
            match_key = _norm_gk(game_key.split("#", 1)[0])
        if match_key:
            live_by_match[match_key] = row

    def _bucket(cards: list[dict[str, Any]]) -> dict[str, int]:
        out = {
            "total": 0,
            "live_feed_updated": 0,
            "scheduled_only": 0,
            "upcoming": 0,
            "live": 0,
            "final": 0,
        }
        for card in cards or []:
            if not isinstance(card, dict):
                continue
            out["total"] += 1
            game_key = str(card.get("game_key") or "").strip()
            match_key = _norm_gk(str(card.get("match_key") or "").strip())
            live_row = None
            if game_key:
                live_row = live_by_game.get(game_key)
            if live_row is None and match_key:
                live_row = live_by_match.get(match_key)
            if live_row is not None:
                out["live_feed_updated"] += 1
                phase = _card_status_phase(str(live_row.get("status") or card.get("status") or ""))
            else:
                out["scheduled_only"] += 1
                phase = _card_status_phase(str(card.get("status") or ""))
            if phase == "live":
                out["live"] += 1
            elif phase == "final":
                out["final"] += 1
            else:
                out["upcoming"] += 1
        return out

    today_stats = _bucket(today_cards if isinstance(today_cards, list) else [])
    tomorrow_stats = _bucket(tomorrow_cards if isinstance(tomorrow_cards, list) else [])

    return {
        "today": today_stats,
        "tomorrow": tomorrow_stats,
        "summary": {
            "cards_total": int(today_stats["total"] + tomorrow_stats["total"]),
            "cards_live_feed_updated": int(today_stats["live_feed_updated"] + tomorrow_stats["live_feed_updated"]),
            "cards_scheduled_only": int(today_stats["scheduled_only"] + tomorrow_stats["scheduled_only"]),
            "live": int(today_stats["live"] + tomorrow_stats["live"]),
            "final": int(today_stats["final"] + tomorrow_stats["final"]),
            "upcoming": int(today_stats["upcoming"] + tomorrow_stats["upcoming"]),
        },
    }


@app.route("/api/backend/status")
def api_backend_status():
    scheduler_running = False
    scheduler_detail = "disabled"
    try:
        sched = _scheduler
        if sched is not None:
            state = int(getattr(sched, "state", 0) or 0)
            scheduler_running = state == 1
            scheduler_detail = "running" if scheduler_running else f"state:{state}"
    except Exception as exc:
        scheduler_detail = f"error:{exc}"

    with _lock:
        state_status = str(_state.get("status") or "")
        state_phase = str(_state.get("phase") or "")

    payload = {
        "ok": True,
        "pid": int(os.getpid()),
        "is_leader": bool(_BG_IS_LEADER),
        "worker_initialized": bool(_worker_initialized),
        "worker_boot_thread_active": bool(_worker_boot_thread_started),
        "scheduler": {
            "running": bool(scheduler_running),
            "detail": scheduler_detail,
            "auto_analysis_interval_min": int(_AUTO_ANALYSIS_INTERVAL_MIN),
        },
        "analysis": {
            "mode": str(_last_analysis_mode or ""),
            "last_started_at": _ts_to_iso(_last_analysis_started_ts),
            "last_finished_at": _ts_to_iso(_last_analysis_finished_ts),
            "last_ok": _last_analysis_ok,
            "last_error": str(_last_analysis_error or "")[:600],
            "current_state_status": state_status,
            "current_phase": state_phase,
        },
        "resolver": {
            "poll_interval_sec": int(_RESOLVE_INTERVAL),
            "last_started_at": _ts_to_iso(_last_resolve_started_ts),
            "last_finished_at": _ts_to_iso(_last_resolve_finished_ts),
            "last_ok": _last_resolve_ok,
            "last_error": str(_last_resolve_error or "")[:600],
            "lock_held": bool(_resolve_run_lock.locked()),
        },
        "exchange_tracking": {
            "kalshi_monitor_interval_sec": int(_KALSHI_MONITOR_INTERVAL_SEC),
            "ready_resolve_min_interval_sec": int(_READY_RESOLVE_MIN_INTERVAL_SEC),
        },
        "server_time_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }
    return jsonify(payload)


@app.route("/api/backend/live-feed-health")
def api_backend_live_feed_health():
    with _lock:
        snapshot = {
            "game_cards_today": list(_state.get("game_cards_today") or []),
            "game_cards_tomorrow": list(_state.get("game_cards_tomorrow") or []),
            "live_scores": dict(_state.get("live_scores") or {}),
        }
    report = _build_live_feed_health_report(snapshot)
    report["poll_interval_sec"] = int(_LIVE_SCORE_INTERVAL)
    report["last_polled_at"] = _ts_to_iso(_last_live_poll_ts)
    report["generated_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    return jsonify({"ok": True, "report": report})


def _resolve_all_sports_outcomes(days_back: int = 3) -> dict:
    """
    Universal outcome resolver.  For each sport/day with PENDING bets:
      1. Fetches completed ESPN scoreboards.
      2. Resolves pending game predictions (moneyline / spread / total).
      3. Resolves pending player props using ESPN boxscore stats.
      4. Resolves pending tracked parlays once all their legs are settled.
    Works in 'all' mode and every single-sport mode.
    Returns {"games": N, "props": N, "parlays": N}.
    """
    import requests as _req
    try:
        from data.db import (
            get_conn, get_pending_props, update_prop_outcome,
        )
        import psycopg2.extras as _dba
    except Exception:
        return {"games": 0, "props": 0, "parlays": 0}

    today        = _et_calendar_today()
    n_games      = 0
    n_props      = 0
    n_parlays    = 0
    exchange_sync = _sync_exchange_resolution_statuses(days_back=max(3, days_back), max_rows=350)

    def _num(v):
        if v is None:
            return None
        try:
            return float(str(v).strip().replace(",", ""))
        except Exception:
            return None

    def _teams_match(espn_name: str, pick_fragment: str) -> bool:
        en = espn_name.lower()
        pf = pick_fragment.lower()
        # direct substring or any two-char+ word overlap
        if en in pf or pf in en:
            return True
        for word in en.split():
            if len(word) > 2 and word in pf:
                return True
        return False

    def _stat_key_norm(v: str) -> str:
        return re.sub(r"[^a-z0-9]+", "", str(v or "").lower())

    def _prop_type_aliases(v: str):
        compact = _stat_key_norm(v)
        base = re.sub(r"[^a-z0-9]+", "_", str(v or "").lower()).strip("_")
        aliases = {base, compact}
        alias_map = {
            "keypasses": {"key_passes", "keypasses"},
            "shotsontarget": {"shots_on_target", "shotsontarget", "shots_on_goal", "shotsongoal"},
            "shotsongoal": {"shots_on_target", "shots_on_goal", "shotsontarget", "shotsongoal"},
            "tackles": {"tackles", "tackles_won", "tackleswon"},
            "saves": {"saves"},
            "goals": {"goals"},
            "assists": {"assists"},
            "strikeouts": {"strikeouts"},
        }
        aliases.update(alias_map.get(compact, set()))
        return [a for a in aliases if a]

    # ── 1. Collect all ESPN completed games grouped by (sport_path, league_path) ──
    # Prefer only dates that still have pending records to keep resolver fast while
    # still allowing wider backfill windows.
    completed_games_by_config = {}
    pending_dates = set()
    conn_dates = get_conn()
    if conn_dates:
        try:
            cdates = conn_dates.cursor()
            cdates.execute("""
                SELECT DISTINCT game_date::date
                FROM predictions
                WHERE outcome = 'PENDING'
                  AND game_date >= CURRENT_DATE - INTERVAL '%s days'
                  AND game_date <= CURRENT_DATE
            """ % int(days_back + 1))
            for row in (cdates.fetchall() or []):
                d = row[0] if row else None
                if d:
                    pending_dates.add(d)

            cdates.execute("""
                SELECT DISTINCT game_date::date
                FROM prop_history
                WHERE outcome = 'PENDING'
                  AND game_date >= CURRENT_DATE - INTERVAL '%s days'
                  AND game_date <= CURRENT_DATE
            """ % int(days_back + 1))
            for row in (cdates.fetchall() or []):
                d = row[0] if row else None
                if d:
                    pending_dates.add(d)
        except Exception:
            pass
        finally:
            try:
                conn_dates.close()
            except Exception:
                pass

    pending_dates.add(today)

    if pending_dates:
        check_dates = sorted(pending_dates, reverse=True)
    else:
        # Include today (days_ago=0) so same-day finals resolve immediately.
        check_dates = [today - datetime.timedelta(days=days_ago) for days_ago in range(0, days_back + 1)]

    for check_date in check_dates:
        dates_token = check_date.strftime("%Y%m%d")
        for cfg in _ESPN_RESOLVE_CONFIGS:
            sport_path, league_path, sport_group, _stat_map = cfg
            url = f"https://site.api.espn.com/apis/site/v2/sports/{sport_path}/{league_path}/scoreboard"
            try:
                resp = _req.get(url, params={"dates": dates_token, "limit": 200}, timeout=8)
                if resp.status_code != 200:
                    continue
                data = resp.json() or {}
            except Exception:
                continue
            for ev in (data.get("events") or []):
                comp   = (ev.get("competitions") or [{}])[0]
                status = str(((comp.get("status") or {}).get("type") or {}).get("name") or "").lower()
                if not any(k in status for k in ("final", "complete", "finished", "ended", "postgame")):
                    continue
                competitors = comp.get("competitors") or []
                if len(competitors) < 2:
                    continue
                home_c = next((c for c in competitors if str(c.get("homeAway") or "").lower() == "home"), competitors[0])
                away_c = next((c for c in competitors if str(c.get("homeAway") or "").lower() == "away"), competitors[1] if len(competitors) > 1 else competitors[0])
                home   = str(((home_c.get("team") or {}).get("displayName") or "")).strip()
                away   = str(((away_c.get("team") or {}).get("displayName") or "")).strip()
                h_sc   = _num(home_c.get("score"))
                a_sc   = _num(away_c.get("score"))
                if not home or not away or h_sc is None or a_sc is None:
                    continue
                key = (sport_path, league_path)
                completed_games_by_config.setdefault(key, []).append({
                    "event_id":   str(ev.get("id") or ""),
                    "sport_path": sport_path,
                    "league_path": league_path,
                    "sport_group": sport_group,
                    "home":       home,
                    "away":       away,
                    "home_score": h_sc,
                    "away_score": a_sc,
                    "total":      h_sc + a_sc,
                    "game_date":  check_date.isoformat(),
                    "stat_map":   _stat_map,
                })

    all_completed = [g for games in completed_games_by_config.values() for g in games]

    # ── 2. Resolve pending game predictions ───────────────────────────────────
    conn = get_conn()
    if conn:
        try:
            cur = conn.cursor(cursor_factory=_dba.RealDictCursor)
            cur.execute("""
                SELECT id, sport, bet_type, pick, line, game_key, game_date::text
                FROM predictions
                WHERE outcome = 'PENDING'
                  AND game_date >= CURRENT_DATE - INTERVAL '%s days'
                                    AND game_date <= CURRENT_DATE
            """ % int(days_back + 1))
            pending_preds = cur.fetchall()

            for pred in pending_preds:
                pick     = str(pred.get("pick") or "")
                line_val = _num(pred.get("line")) or 0.0
                bet_type = str(pred.get("bet_type") or "").lower()
                game_key = str(pred.get("game_key") or "")
                pred_sport = str(pred.get("sport") or "").lower()

                for g in all_completed:
                    sport_group = g["sport_group"]
                    # sport filter
                    if pred_sport and pred_sport not in (sport_group, "all", ""):
                        if pred_sport == "mlb" and sport_group != "baseball":
                            continue
                        if pred_sport not in ("all", ""):
                            if pred_sport == "basketball" and sport_group != "basketball":
                                continue
                            if pred_sport == "icehockey" and sport_group != "icehockey":
                                continue
                            if pred_sport == "soccer" and sport_group != "soccer":
                                continue

                    home, away = g["home"], g["away"]
                    h_sc, a_sc = g["home_score"], g["away_score"]
                    total = g["total"]

                    # Verify this game matches the prediction's game_key
                    if game_key and home not in game_key and away not in game_key:
                        if not (any(w in game_key.lower() for w in home.lower().split() if len(w) > 3) or
                                any(w in game_key.lower() for w in away.lower().split() if len(w) > 3)):
                            continue

                    outcome = None
                    result_str = f"{away} {int(a_sc)} @ {home} {int(h_sc)}"

                    if "moneyline" in bet_type or bet_type == "money_line":
                        winner = home if h_sc > a_sc else away
                        if h_sc == a_sc:
                            outcome = "PUSH"
                        elif _teams_match(winner, pick):
                            outcome = "WIN"
                        else:
                            outcome = "LOSS"

                    elif bet_type in ("run_line", "puck_line", "spread", "point_spread"):
                        margin = h_sc - a_sc
                        # '+' / '-' convention in pick
                        if f"-1.5" in pick or f"-2.5" in pick:
                            fav_margin = float(next((p for p in pick.split() if p.startswith("-")), "-1.5"))
                            fav_team   = pick.split()[0] if pick else ""
                            if _teams_match(home, fav_team):
                                outcome = "WIN" if margin > abs(fav_margin) else "LOSS"
                            else:
                                outcome = "WIN" if -margin > abs(fav_margin) else "LOSS"
                        elif "+1.5" in pick or "+2.5" in pick:
                            dog_margin = float(next((p for p in pick.split() if p.startswith("+")), "+1.5"))
                            dog_team   = pick.split()[0] if pick else ""
                            if _teams_match(home, dog_team):
                                outcome = "WIN" if margin > -abs(dog_margin) else "LOSS"
                            else:
                                outcome = "WIN" if -margin > -abs(dog_margin) else "LOSS"

                    elif bet_type in ("total", "f5_total", "game_total"):
                        if "OVER" in pick.upper():
                            if total > line_val:
                                outcome = "WIN"
                            elif total == line_val:
                                outcome = "PUSH"
                            else:
                                outcome = "LOSS"
                        elif "UNDER" in pick.upper():
                            if total < line_val:
                                outcome = "WIN"
                            elif total == line_val:
                                outcome = "PUSH"
                            else:
                                outcome = "LOSS"

                    if outcome:
                        try:
                            cur.execute("""
                                UPDATE predictions
                                SET outcome = %s, actual_result = %s, resolved_at = NOW()
                                WHERE id = %s AND outcome = 'PENDING'
                            """, (outcome, result_str, pred["id"]))
                            n_games += cur.rowcount
                        except Exception:
                            pass
                        break  # matched — stop searching completed games for this pred

            conn.commit()
        except Exception as exc:
            try:
                conn.rollback()
            except Exception:
                pass
            print(f"[resolve] game predictions error: {exc}")
        finally:
            conn.close()

    # ── 3. Resolve pending player props via ESPN boxscore ─────────────────────
    pending_props = get_pending_props(days_back=days_back)
    if pending_props:
        # Cache boxscore player stats per event_id so we don't re-fetch
        _boxscore_cache: dict[str, dict[str, float]] = {}

        for prop in pending_props:
            prop_sport  = str(prop.get("sport") or "").lower()
            player_name = str(prop.get("player_name") or "").strip().lower()
            team_name   = str(prop.get("team") or "").strip().lower()
            prop_type_raw = str(prop.get("prop_type") or "").strip().lower()
            prop_type = re.sub(r"[^a-z0-9]+", "_", prop_type_raw).strip("_")
            line_val    = _num(prop.get("line")) or 0.0
            rec         = str(prop.get("recommendation") or "OVER").upper()
            game_key    = str(prop.get("game_key") or "")

            for g in all_completed:
                sport_group = g["sport_group"]
                stat_map    = g["stat_map"]

                # Sport matching
                def _sport_matches(ps: str, sg: str) -> bool:
                    if not ps or ps in ("all", ""):
                        return True
                    if ps == sg:
                        return True
                    if ps == "mlb" and sg == "baseball":
                        return True
                    if ps == "basketball" and sg == "basketball":
                        return True
                    if ps == "icehockey" and sg == "icehockey":
                        return True
                    if ps == "soccer" and sg == "soccer":
                        return True
                    return False

                if not _sport_matches(prop_sport, sport_group):
                    continue

                # Game matching
                home, away = g["home"], g["away"]
                if game_key:
                    if not (any(w in game_key.lower() for w in home.lower().split() if len(w) > 3) or
                            any(w in game_key.lower() for w in away.lower().split() if len(w) > 3)):
                        if not any(w in game_key.lower() for w in team_name.split() if len(w) > 3):
                            continue
                else:
                    if team_name and not (_teams_match(home, team_name) or _teams_match(away, team_name)):
                        continue

                event_id    = g["event_id"]
                sport_path  = g["sport_path"]
                league_path = g["league_path"]
                cache_key   = f"{sport_path}/{league_path}/{event_id}"

                # Fetch + cache boxscore
                if cache_key not in _boxscore_cache:
                    player_stats = {}
                    try:
                        url = f"https://site.api.espn.com/apis/site/v2/sports/{sport_path}/{league_path}/summary"
                        resp = _req.get(url, params={"event": event_id}, timeout=8)
                        if resp.status_code == 200:
                            summary = resp.json() or {}
                            # Boxscore players block
                            for team_box in ((summary.get("boxscore") or {}).get("players") or []):
                                for stat_block in (team_box.get("statistics") or []):
                                    keys = [str(k or "").strip() for k in (stat_block.get("keys") or [])]
                                    key_idx = {_stat_key_norm(k): i for i, k in enumerate(keys)}
                                    for arow in (stat_block.get("athletes") or []):
                                        athlete  = arow.get("athlete") or {}
                                        pname    = str(athlete.get("displayName") or athlete.get("fullName") or "").strip().lower()
                                        if not pname:
                                            continue
                                        vals = arow.get("stats") or []
                                        for raw_k, mapped_k in stat_map.items():
                                            idx = key_idx.get(_stat_key_norm(raw_k))
                                            if idx is not None and idx < len(vals):
                                                v = _num(vals[idx])
                                                if v is not None:
                                                    player_stats.setdefault(pname, {})[mapped_k] = v
                            # Leaders block as fallback
                            for team_bucket in (summary.get("leaders") or []):
                                for cat in (team_bucket.get("leaders") or []):
                                    cat_name = str(cat.get("name") or "").strip()
                                    for raw_k, mapped_k in stat_map.items():
                                        if (_stat_key_norm(cat_name) == _stat_key_norm(raw_k)
                                                or _stat_key_norm(mapped_k) == _stat_key_norm(cat_name)):
                                            for lead in (cat.get("leaders") or []):
                                                athlete = lead.get("athlete") or {}
                                                pname   = str(athlete.get("displayName") or athlete.get("fullName") or "").strip().lower()
                                                v       = _num(lead.get("value"))
                                                if pname and v is not None:
                                                    player_stats.setdefault(pname, {})[mapped_k] = v
                    except Exception:
                        pass
                    _boxscore_cache[cache_key] = player_stats

                player_stats = _boxscore_cache[cache_key]
                if not player_stats:
                    continue

                # Find player in boxscore (exact or partial)
                matched_stats = player_stats.get(player_name)
                if matched_stats is None:
                    parts = [w for w in player_name.split() if len(w) > 2]
                    matched_stats = next(
                        (v for k, v in player_stats.items() if all(p in k for p in parts)),
                        None,
                    )
                if matched_stats is None:
                    continue

                # Find the actual stat value
                # prop_type in db can be "points", "rebounds", "assists", "goals", "shots_on_goal" etc.
                prop_aliases = _prop_type_aliases(prop_type)
                prop_alias_norms = {_stat_key_norm(a) for a in prop_aliases}
                actual_val = None
                for alias in prop_aliases:
                    actual_val = matched_stats.get(alias)
                    if actual_val is not None:
                        break
                if actual_val is None:
                    # Try mapped alias
                    for raw_k, mapped_k in stat_map.items():
                        if (_stat_key_norm(mapped_k) in prop_alias_norms
                                or _stat_key_norm(raw_k) in prop_alias_norms):
                            actual_val = matched_stats.get(mapped_k)
                            if actual_val is not None:
                                break
                if actual_val is None:
                    continue

                # Resolve
                if actual_val > line_val:
                    outcome = "WIN" if rec == "OVER" else "LOSS"
                elif actual_val < line_val:
                    outcome = "LOSS" if rec == "OVER" else "WIN"
                else:
                    outcome = "PUSH"

                update_prop_outcome(prop["id"], actual_val, outcome)
                n_props += 1
                break  # matched this prop — move to next

    # ── 4. ARCHIVE soccer props that ESPN cannot resolve (key_passes, tackles) ─
    # These prop types rely on Opta/WhoScored data — ESPN boxscore API never
    # returns per-player key passes or tackle counts.  After 3 days with no
    # resolution they are permanently stuck; archive them to keep the hit-rate
    # table accurate.
    conn_archive = get_conn()
    if conn_archive:
        try:
            ca = conn_archive.cursor()
            ca.execute("""
                UPDATE prop_history
                SET outcome = 'ARCHIVED', resolved_at = NOW()
                WHERE outcome = 'PENDING'
                  AND sport = 'soccer'
                  AND game_date < CURRENT_DATE - INTERVAL '3 days'
                  AND LOWER(REGEXP_REPLACE(prop_type, '[^a-z0-9]', '_', 'g'))
                      = ANY(%s)
            """, (list(_SOCCER_ESPN_UNRESOLVABLE),))
            n_archived_soccer = ca.rowcount
            conn_archive.commit()
            if n_archived_soccer:
                _log(f"[resolve] Archived {n_archived_soccer} unresolvable soccer props (ESPN data unavailable)")
        except Exception as _arc_err:
            try:
                conn_archive.rollback()
            except Exception:
                pass
            _log(f"[resolve] soccer archive step error: {_arc_err}")
        finally:
            try:
                conn_archive.close()
            except Exception:
                pass

    # ── 5. MLB-specific prop + game resolution (statsapi) ─────────────────────
    try:
        from models.mlb_predictor import (
            resolve_game_outcomes as _mlb_game_res,
            resolve_prop_outcomes as _mlb_prop_res,
        )
        ng = _mlb_game_res(days_back=days_back)
        np_ = _mlb_prop_res(days_back=days_back)
        n_games += ng
        n_props  += np_
    except Exception:
        pass

    # ── 6. Resolve parlays ─────────────────────────────────────────────────────
    try:
        from models.mlb_predictor import resolve_tracked_parlays as _rtp
        n_parlays = _rtp(days_back=days_back + 4)
    except Exception:
        n_parlays = 0

    total = n_games + n_props + n_parlays
    if total:
        print(f"[resolve_all] Resolved {n_games} game preds + {n_props} props + {n_parlays} parlays")
        try:
            from data.db import get_performance_stats, get_parlay_performance_stats
            db_sport = None if _ACTIVE_SPORT == "all" else _ACTIVE_SPORT
            _sse_broadcast("performance_update", {
                "stats":        get_performance_stats(sport=db_sport),
                "parlay_stats": get_parlay_performance_stats(sport=db_sport),
            })
        except Exception:
            pass

    return {
        "games": n_games,
        "props": n_props,
        "parlays": n_parlays,
        "exchange_sync": exchange_sync,
    }


@app.route("/api/resolve-outcomes", methods=["POST"])
def api_resolve_outcomes():
    try:
        result = _run_resolver_locked(days_back=21)
        n_games  = result["games"]
        n_props  = result["props"]
        n_parlay = result["parlays"]
        return jsonify({
            "ok": True,
            "resolved_games":   n_games,
            "resolved_props":   n_props,
            "resolved_parlays": n_parlay,
            "msg": f"Resolved {n_games} game preds + {n_props} props + {n_parlay} parlays",
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/parlay/save", methods=["POST"])
def api_parlay_save():
    data = request.get_json(force=True) or {}
    try:
        from data.db import save_tracked_parlay, _prediction_uid, _prop_uid
        # Default to duplicate prevention for all saves unless explicitly disabled.
        dedupe_raw = str(data.get("dedupe_pending", "1")).strip().lower()
        dedupe_pending = dedupe_raw in {"1", "true", "yes", "on"}
        raw_legs = data.get("legs", [])
        norm_legs = []
        if isinstance(raw_legs, list):
            for leg in raw_legs:
                if isinstance(leg, dict):
                    leg_payload = dict(leg)
                    leg_payload.setdefault("sport", _ACTIVE_SPORT)
                    leg_payload.setdefault("game_key", leg_payload.get("game", ""))
                    leg_payload.setdefault("game", leg_payload.get("game_key", ""))
                    game_date = leg_payload.get("game_date") or _et_calendar_today().isoformat()
                    source = str(leg_payload.get("source") or "").strip().lower()
                    bet_type = str(leg_payload.get("bet_type") or "").strip().lower()

                    is_prop_leg = (
                        source == "prop"
                        or bet_type == "player_prop"
                        or bool(leg_payload.get("prop_type") or leg_payload.get("stat_type"))
                    )

                    scheduled_start = str(leg_payload.get("scheduled_start") or "").strip()
                    if not scheduled_start:
                        game_ref = str(leg_payload.get("game_key") or leg_payload.get("game") or "")
                        if "#" in game_ref:
                            scheduled_start = game_ref.rsplit("#", 1)[-1].strip()

                    direction = str(
                        leg_payload.get("direction")
                        or leg_payload.get("recommendation")
                        or ""
                    ).strip().upper()
                    direction_text = " ".join(
                        str(leg_payload.get(key) or "")
                        for key in ("direction", "recommendation", "pick", "label", "bet_type")
                    ).upper()
                    side_default = str(leg_payload.get("side_default") or "").strip().lower()
                    if side_default not in {"yes", "no"}:
                        side_default = "no" if "UNDER" in direction_text else "yes"

                    leg_payload["game_date"] = game_date
                    leg_payload["scheduled_start"] = scheduled_start
                    leg_payload["direction"] = direction
                    leg_payload["recommendation"] = direction
                    leg_payload["player_name"] = leg_payload.get("player_name") or leg_payload.get("name") or ""
                    leg_payload["name"] = leg_payload.get("name") or leg_payload.get("player_name") or ""
                    leg_payload["prop_type"] = leg_payload.get("prop_type") or leg_payload.get("stat_type") or ""
                    leg_payload["stat_type"] = leg_payload.get("stat_type") or leg_payload.get("prop_type") or ""
                    leg_payload["kind"] = leg_payload.get("kind") or ("player_prop" if is_prop_leg else "single")
                    leg_payload["bet_type"] = leg_payload.get("bet_type") or ("player_prop" if is_prop_leg else "single")
                    leg_payload["label"] = leg_payload.get("label") or leg_payload.get("pick") or ""
                    leg_payload["pick"] = leg_payload.get("pick") or leg_payload.get("label") or ""
                    leg_payload["home_team"] = leg_payload.get("home_team") or ""
                    leg_payload["away_team"] = leg_payload.get("away_team") or ""
                    leg_payload["team"] = leg_payload.get("team") or ""
                    leg_payload["side_default"] = side_default

                    # Attach deterministic prediction UID per leg for exact outcome lookups.
                    if not leg_payload.get("prediction_uid") and not leg_payload.get("bet_uid"):
                        if is_prop_leg:
                            leg_uid = _prop_uid({
                                "sport": leg_payload.get("sport", _ACTIVE_SPORT),
                                "game_date": game_date,
                                "game_key": leg_payload.get("game_key") or leg_payload.get("game") or "",
                                "name": leg_payload.get("name") or leg_payload.get("player_name") or "",
                                "player_name": leg_payload.get("player_name") or leg_payload.get("name") or "",
                                "team": leg_payload.get("team") or "",
                                "stat_type": leg_payload.get("stat_type") or leg_payload.get("prop_type") or "",
                                "prop_type": leg_payload.get("prop_type") or leg_payload.get("stat_type") or "",
                                "line": leg_payload.get("line"),
                                "direction": leg_payload.get("direction") or leg_payload.get("recommendation") or "",
                                "recommendation": leg_payload.get("recommendation") or leg_payload.get("direction") or "",
                            }, game_date=game_date)
                        else:
                            pick_label = (
                                leg_payload.get("pick")
                                or leg_payload.get("label")
                                or ""
                            )
                            leg_uid = _prediction_uid({
                                "sport": leg_payload.get("sport", _ACTIVE_SPORT),
                                "game_date": game_date,
                                "game_key": leg_payload.get("game_key") or leg_payload.get("game") or "",
                                "bet_type": leg_payload.get("bet_type") or "",
                                "pick": pick_label,
                                "line": leg_payload.get("line"),
                            })

                        leg_payload["prediction_uid"] = leg_uid
                        leg_payload["bet_uid"] = leg_uid
                    elif leg_payload.get("prediction_uid") and not leg_payload.get("bet_uid"):
                        leg_payload["bet_uid"] = leg_payload.get("prediction_uid")
                    elif leg_payload.get("bet_uid") and not leg_payload.get("prediction_uid"):
                        leg_payload["prediction_uid"] = leg_payload.get("bet_uid")
                    leg_payload["uid"] = leg_payload.get("uid") or leg_payload.get("bet_uid") or leg_payload.get("prediction_uid") or ""
                    norm_legs.append(leg_payload)
        pid = save_tracked_parlay(
            name=data.get("name", "My Parlay"),
            legs=norm_legs,
            combined_odds=float(data.get("combined_odds", 0)),
            stake_usd=float(data.get("stake_usd", 0)),
            dedupe_pending=dedupe_pending,
        )
        return jsonify({"ok": True, "id": pid})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/parlay/list")
def api_parlay_list():
    try:
        _maybe_trigger_tracking_sync_on_read()
        from data.db import get_tracked_parlays, prune_tracked_parlays_to_date
        inc = str(request.args.get("include_resolved", "1")).strip().lower()
        include_resolved = inc in {"1", "true", "yes", "on"}
        current_only_raw = str(request.args.get("current_only", "1")).strip().lower()
        current_only = current_only_raw in {"1", "true", "yes", "on"}
        target_date = _et_calendar_today() if current_only else None
        if current_only:
            # Remove stale rows so old parlays cannot show up again in this tab.
            prune_tracked_parlays_to_date(target_date=target_date)
        db_sport = None if _ACTIVE_SPORT == "all" else _ACTIVE_SPORT
        rows = get_tracked_parlays(
            include_resolved=include_resolved,
            target_date=target_date,
            sport=db_sport,
        )
        rows = _annotate_parlay_tracking(rows)
        return jsonify({"ok": True, "parlays": _clean(rows)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "parlays": []})


@app.route("/api/parlay/resolve", methods=["POST"])
def api_parlay_resolve():
    data = request.get_json(force=True) or {}
    try:
        from data.db import resolve_tracked_parlay
        resolve_tracked_parlay(
            parlay_id=int(data.get("id", 0)),
            outcome=data.get("outcome", "WIN"),
            payout=float(data.get("payout", 0)),
        )
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/parlay/tracking-overview")
def api_parlay_tracking_overview():
    """Single consolidated payload for the Parlays/Tracking tab.

    Does ALL heavy lifting server-side (fetch, today-filter, prop normalization,
    dedupe/merge, live/settled bucketing, full accounting) so the mobile frontend
    only renders. Every prediction the bot produces — singles, props and parlays —
    is counted here so nothing is silently dropped.
    """
    try:
        _maybe_trigger_tracking_sync_on_read()
        from data.db import (
            get_predictions,
            get_prop_history,
            get_tracked_parlays,
            prune_tracked_parlays_to_date,
        )

        db_sport = None if _ACTIVE_SPORT == "all" else _ACTIVE_SPORT
        today = _et_calendar_today()
        today_iso = today.isoformat()

        # ── Parlays (prefer today, fall back to all tracked) ──────────────────
        try:
            prune_tracked_parlays_to_date(target_date=today)
        except Exception:
            pass
        parlay_rows = get_tracked_parlays(
            include_resolved=True, target_date=today, sport=db_sport
        )
        scope = "today"
        if not parlay_rows:
            parlay_rows = get_tracked_parlays(
                include_resolved=True, target_date=None, sport=db_sport
            )
            scope = "all"
        parlay_rows = _annotate_parlay_tracking(parlay_rows)
        parlay_buckets = _bucket_tracking_rows(parlay_rows)
        parlay_counts = _tracking_bucket_counts(parlay_buckets)

        # ── Singles (game predictions) + player/team props for today ──────────
        preds = [
            p
            for p in get_predictions(days=21, sport=db_sport)
            if str(p.get("game_date", ""))[:10] == today_iso
        ]
        props = [
            _normalize_prop_history_row_for_tracking(r)
            for r in get_prop_history(days=21, sport=db_sport)
            if str(r.get("game_date", ""))[:10] == today_iso
        ]

        dedupe: dict[str, dict] = {}
        for row in [*preds, *props]:
            if not isinstance(row, dict):
                continue
            uid = _single_tracking_uid(row)
            if not uid:
                continue
            if uid not in dedupe:
                dedupe[uid] = row
            else:
                dedupe[uid] = _merge_tracking_row(dedupe[uid], row)

        singles = [
            row
            for row in dedupe.values()
            if str(row.get("bet_type") or "").lower() != "parlay"
            and str(row.get("game_date", ""))[:10] == today_iso
        ]
        singles = _annotate_tracking_phase(singles)
        single_buckets = _bucket_tracking_rows(singles)
        single_counts = _tracking_bucket_counts(single_buckets)

        # ── Combined accounting (every prediction counted) ────────────────────
        settled_wins = single_counts["won"] + parlay_counts["won"]
        settled_losses = single_counts["lost"] + parlay_counts["lost"]
        settled_total = settled_wins + settled_losses
        live_tracked = single_counts["live"] + parlay_counts["live"]
        pending_total = single_counts["pending"] + parlay_counts["pending"]
        total_tracked = single_counts["total"] + parlay_counts["total"]

        # Compact, always-available summary — the only thing the tab renders.
        summary = {
            "settled_wins": settled_wins,
            "settled_losses": settled_losses,
            "settled_total": settled_total,
            "win_rate": round(settled_wins / settled_total * 100) if settled_total else None,
            "live_tracked": live_tracked,
            "pending": pending_total,
            "total_tracked": total_tracked,
        }

        return jsonify({
            "ok": True,
            "scope": scope,
            "today": today_iso,
            "generated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "summary": summary,
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/kalshi/markets")
def api_kalshi_markets():
    """Public Kalshi market data (no auth required on Kalshi side)."""
    try:
        from data.kalshi import list_markets

        limit = int(request.args.get("limit", 200))
        cursor = (request.args.get("cursor") or "").strip() or None
        status = (request.args.get("status") or "open").strip() or None
        event_ticker = (request.args.get("event_ticker") or "").strip() or None
        series_ticker = (request.args.get("series_ticker") or "").strip() or None

        data = list_markets(
            limit=limit,
            cursor=cursor,
            status=status,
            event_ticker=event_ticker,
            series_ticker=series_ticker,
        )
        markets = data.get("markets") or []
        return jsonify({
            "ok": True,
            "markets": _clean(markets),
            "cursor": data.get("cursor"),
            "count": len(markets),
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "markets": [], "count": 0})


@app.route("/api/kalshi/events")
def api_kalshi_events():
    """Public Kalshi event data (for market timing context)."""
    try:
        from data.kalshi import list_events

        limit = int(request.args.get("limit", 200))
        cursor = (request.args.get("cursor") or "").strip() or None
        status = (request.args.get("status") or "").strip() or None
        series_ticker = (request.args.get("series_ticker") or "").strip() or None

        data = list_events(
            limit=limit,
            cursor=cursor,
            status=status,
            series_ticker=series_ticker,
        )
        events = data.get("events") or []
        return jsonify({
            "ok": True,
            "events": _clean(events),
            "cursor": data.get("cursor"),
            "count": len(events),
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "events": [], "count": 0})


@app.route("/api/kalshi/resolve-ready", methods=["POST"])
def api_kalshi_resolve_ready():
    """Resolve dashboard Ready Bets to exact Kalshi markets on the server."""
    data = request.get_json(force=True) or {}
    try:
        from data.kalshi import resolve_ready_bets, suggest_combo_bets

        bets = data.get("bets") or []
        if not isinstance(bets, list):
            bets = []
        clean_bets = _clean_ready_bets_payload([bet for bet in bets if isinstance(bet, dict)])
        clean_bets = _team_only_ready_bets(clean_bets)
        force_refresh = bool(data.get("force_refresh"))
        include_combo_suggestions = bool(data.get("include_combo_suggestions", True))
        combo_max_input = max(30, int(os.getenv("KALSHI_COMBO_INPUT_LIMIT", "70") or "70"))
        combo_source_bets = [
            bet
            for bet in clean_bets
            if (
                str(bet.get("kind") or "").strip().lower() != "combo"
                and not _is_player_prediction_row(bet)
            )
        ]
        combo_input_bets = combo_source_bets
        if include_combo_suggestions and len(combo_source_bets) > combo_max_input:
            def _combo_score(row: dict) -> tuple[float, float, float]:
                return (
                    float(row.get("quality") or 0.0),
                    float(row.get("model_prob") or row.get("probability") or 0.0),
                    float(row.get("ev") or 0.0),
                )

            combo_input_bets = sorted(combo_source_bets, key=_combo_score, reverse=True)[:combo_max_input]

            if len(combo_input_bets) < combo_max_input:
                chosen_ids = {
                    str(b.get("uid") or b.get("bet_uid") or b.get("prediction_uid") or "")
                    for b in combo_input_bets
                }
                fillers = [
                    b
                    for b in sorted(combo_source_bets, key=_combo_score, reverse=True)
                    if str(b.get("uid") or b.get("bet_uid") or b.get("prediction_uid") or "") not in chosen_ids
                ]
                combo_input_bets.extend(fillers[: combo_max_input - len(combo_input_bets)])

            _log(
                "[kalshi] combo suggestions bounded "
                f"({len(combo_source_bets)} team bets -> {len(combo_input_bets)} candidates, limit {combo_max_input})"
            )

        request_sig = _ready_resolve_signature(clean_bets) + f"|combo={1 if include_combo_suggestions else 0}"
        now = time.time()
        if not force_refresh:
            with _READY_RESOLVE_CACHE_LOCK:
                cache_sig = str(_READY_RESOLVE_CACHE.get("sig") or "")
                cache_ts = float(_READY_RESOLVE_CACHE.get("ts") or 0.0)
                cache_payload = _READY_RESOLVE_CACHE.get("payload")
            cache_age = now - cache_ts
            if (
                cache_payload
                and cache_sig == request_sig
                and cache_age < _READY_RESOLVE_MIN_INTERVAL_SEC
            ):
                payload = dict(cache_payload)
                payload["cache_age_sec"] = max(float(payload.get("cache_age_sec") or 0.0), float(cache_age))
                payload["server_cached"] = True
                return jsonify({"ok": True, **_clean(payload)})

        resolved = resolve_ready_bets(
            clean_bets,
            force_refresh=force_refresh,
        )
        combos = []
        if include_combo_suggestions:
            # Combo suggestion search is CPU-heavier; run only when explicitly requested.
            # Never fail the whole resolve response if combo analysis crashes.
            try:
                combos = suggest_combo_bets(
                    combo_input_bets,
                    resolutions=resolved.get("resolutions") or {},
                    max_legs=5,
                    min_legs=2,
                    min_combined_prob=0.12,
                    min_ev=-0.20,
                    max_combos=80,
                )
                if combos:
                    combo_resolved = resolve_ready_bets(combos, force_refresh=False)
                    combo_resolution_map = combo_resolved.get("resolutions") or {}
                    if combo_resolution_map:
                        resolved_map = resolved.get("resolutions") or {}
                        resolved_map.update(combo_resolution_map)
                        resolved["resolutions"] = resolved_map
                        hydrated_combos = []
                        for combo in combos:
                            combo_uid = str(combo.get("uid") or "")
                            combo_resolution = combo_resolution_map.get(combo_uid)
                            if isinstance(combo_resolution, dict):
                                hydrated_combos.append(
                                    {
                                        **combo,
                                        **{k: v for k, v in combo_resolution.items() if k != "legs"},
                                        "all_matched": str(combo_resolution.get("status") or "").lower() == "matched",
                                    }
                                )
                            else:
                                hydrated_combos.append(combo)
                        combos = hydrated_combos
            except Exception as combo_exc:
                _log(f"[kalshi] combo suggestion skipped: {combo_exc}")
        response_payload = {
            **resolved,
            "combo_suggestions": combos,
            "server_cached": False,
        }
        try:
            _maybe_email_kalshi_matches(clean_bets, resolved.get("resolutions") or {}, source_tag="resolve-ready")
        except Exception as _email_exc:
            _log(f"[kalshi-email] resolve-ready alert step skipped: {_email_exc}")
        with _READY_RESOLVE_CACHE_LOCK:
            _READY_RESOLVE_CACHE["ts"] = time.time()
            _READY_RESOLVE_CACHE["sig"] = request_sig
            _READY_RESOLVE_CACHE["payload"] = response_payload
        return jsonify({"ok": True, **_clean(response_payload)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "resolutions": {}, "summary": {}, "combo_suggestions": []})


@app.route("/api/polymarket/resolve-ready", methods=["POST"])
def api_polymarket_resolve_ready():
    """Resolve dashboard Ready Bets to exact Polymarket markets on the server."""
    data = request.get_json(force=True) or {}
    try:
        from data.polymarket import resolve_ready_bets

        bets = data.get("bets") or []
        if not isinstance(bets, list):
            bets = []
        clean_bets = _clean_ready_bets_payload([bet for bet in bets if isinstance(bet, dict)])
        clean_bets = _team_only_ready_bets(clean_bets)
        force_refresh = bool(data.get("force_refresh"))

        resolved = resolve_ready_bets(clean_bets, force_refresh=force_refresh)
        return jsonify({"ok": True, **_clean(resolved)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "resolutions": {}, "summary": {}, "market_count": 0})


@app.route("/api/kalshi/combo-study", methods=["POST"])
def api_kalshi_combo_study():
    """Analyze a set of bets and return combo parlay suggestions with EV scores.

    Accepts the same bets payload as /api/kalshi/resolve-ready but focuses on
    multi-leg parlay combinations.  Pre-runs Kalshi resolution so each leg shows
    its matched ticker.
    """
    data = request.get_json(force=True) or {}
    try:
        from data.kalshi import resolve_ready_bets, suggest_combo_bets

        bets = data.get("bets") or []
        if not isinstance(bets, list):
            bets = []
        clean_bets = _clean_ready_bets_payload([bet for bet in bets if isinstance(bet, dict)])
        clean_bets = _team_only_ready_bets(clean_bets)

        # Resolve individual bets first so combo legs show Kalshi tickers
        force = bool(data.get("force_refresh"))
        resolved = resolve_ready_bets(clean_bets, force_refresh=force)

        max_legs = min(int(data.get("max_legs") or 5), 6)
        min_legs = max(2, int(data.get("min_legs") or 2))
        min_prob = float(data.get("min_combined_prob") or 0.12)
        min_ev = float(data.get("min_ev") or -0.20)

        combos = suggest_combo_bets(
            clean_bets,
            resolutions=resolved.get("resolutions") or {},
            max_legs=max_legs,
            min_legs=min_legs,
            min_combined_prob=min_prob,
            min_ev=min_ev,
            max_combos=80,
        )
        if combos:
            combo_resolved = resolve_ready_bets(combos, force_refresh=False)
            combo_resolution_map = combo_resolved.get("resolutions") or {}
            if combo_resolution_map:
                resolved_map = resolved.get("resolutions") or {}
                resolved_map.update(combo_resolution_map)
                resolved["resolutions"] = resolved_map
                hydrated_combos = []
                for combo in combos:
                    combo_uid = str(combo.get("uid") or "")
                    combo_resolution = combo_resolution_map.get(combo_uid)
                    if isinstance(combo_resolution, dict):
                        hydrated_combos.append(
                            {
                                **combo,
                                **{k: v for k, v in combo_resolution.items() if k != "legs"},
                                "all_matched": str(combo_resolution.get("status") or "").lower() == "matched",
                            }
                        )
                    else:
                        hydrated_combos.append(combo)
                combos = hydrated_combos
        return jsonify({
            "ok": True,
            "combos": _clean(combos),
            "count": len(combos),
            "bet_count": len(clean_bets),
            "market_count": int(resolved.get("market_count") or 0),
            "resolutions": _clean(resolved.get("resolutions") or {}),
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "combos": [], "count": 0})


@app.route("/api/kalshi/series-registry", methods=["GET"])
def api_kalshi_series_registry():
    """Return the current persisted Kalshi series registry."""
    try:
        from data.kalshi import _load_series_registry, _SPORTS_SERIES_TO_FETCH, _SERIES_SPORT_MAP
        registry = _load_series_registry()
        # Merge with hardcoded series so the response is comprehensive
        merged = {**{s: _SERIES_SPORT_MAP.get(s, "") for s in _SPORTS_SERIES_TO_FETCH}, **registry}
        return jsonify({"ok": True, "series": merged, "count": len(merged)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "series": {}})


@app.route("/api/kalshi/today-tickers", methods=["GET"])
def api_kalshi_today_tickers():
    """Reverse-match: return all open Kalshi sports markets for today, grouped by sport.

    Serves from the live WebSocket ticker cache when available (instant, no
    extra REST call).  Falls back to the REST implementation when the cache is
    still cold (e.g. app just restarted).
    """
    try:
        from data.kalshi import get_today_kalshi_tickers, get_live_tickers

        # Check if the WS cache has data yet
        live = get_live_tickers()
        force_rest = request.args.get("force_rest", "").lower() in ("1", "true", "yes")

        if live and not force_rest:
            # The WS cache holds individual ticker dicts keyed by market_ticker.
            # Pass them into get_today_kalshi_tickers as a pre-fetched supplement
            # so it can skip the heavy REST pagination and just filter/group.
            try:
                result = get_today_kalshi_tickers(live_tickers=live)
                return jsonify({"ok": True, "source": "websocket", **_clean(result)})
            except TypeError:
                # Older signature — fall through to normal REST call
                pass

        result = get_today_kalshi_tickers()
        return jsonify({"ok": True, "source": "rest", **_clean(result)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "sports": {}, "total": 0})


@app.route("/api/kalshi/ws-status", methods=["GET"])
def api_kalshi_ws_status():
    """Debug endpoint: returns WebSocket connection status and cache size."""
    try:
        from data.kalshi import get_kalshi_ws_manager
        mgr = get_kalshi_ws_manager()
        if mgr is None:
            return jsonify({"ok": True, "running": False, "connected": False, "cached_tickers": 0})
        return jsonify({
            "ok": True,
            "running": mgr._running,
            "connected": mgr.is_connected(),
            "cached_tickers": len(mgr.get_tickers()),
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/kalshi/balance")
def api_kalshi_balance():
    """Fetch current Kalshi account balance (authenticated)."""
    try:
        from data.kalshi import get_balance
        data = get_balance()
        balance_cents = int(data.get("balance", 0) or 0)
        portfolio_cents = int(data.get("portfolio_value", 0) or 0)
        return jsonify({
            "ok": True,
            "balance_cents": balance_cents,
            "balance_usd": round(balance_cents / 100, 2),
            "portfolio_cents": portfolio_cents,
            "portfolio_usd": round(portfolio_cents / 100, 2),
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/polymarket/balance")
def api_polymarket_balance():
    """Fetch current Polymarket collateral balance (authenticated)."""
    try:
        from data.polymarket import get_balance

        data = get_balance()
        if not data.get("ok"):
            return jsonify({"ok": False, "error": "Polymarket balance unavailable.", "raw": _clean(data.get("raw") or {})})
        balance_usd = float(data.get("balance_usd") or 0.0)
        portfolio_usd = float(data.get("portfolio_usd") or balance_usd)
        return jsonify({
            "ok": True,
            "balance_usd": round(balance_usd, 2),
            "portfolio_usd": round(portfolio_usd, 2),
            "raw": _clean(data.get("raw") or {}),
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/kalshi/order", methods=["POST"])
def api_kalshi_order():
    """Execute a Kalshi order using API credentials from environment variables."""
    data = request.get_json(force=True) or {}
    try:
        from data.kalshi import place_order, resolve_ready_bets

        resolved_bet = None
        bet_payload = data.get("bet_payload")
        explicit_ticker = str(data.get("market_ticker") or data.get("ticker") or "").strip()

        # Use caller-provided matched ticker when present to avoid order-time re-resolve drift.
        # If no ticker is provided, resolve once from bet payload using cached catalog.
        if isinstance(bet_payload, dict) and not explicit_ticker:
            resolved = resolve_ready_bets([bet_payload], force_refresh=False)
            resolved_map = resolved.get("resolutions") or {}
            bet_uid = str(
                bet_payload.get("uid")
                or bet_payload.get("bet_uid")
                or bet_payload.get("prediction_uid")
                or "ready_0"
            )
            resolved_bet = resolved_map.get(bet_uid) or resolved_map.get("ready_0")
            if isinstance(resolved_bet, dict):
                status = str(resolved_bet.get("status") or "")
                if status and status != "matched":
                    return jsonify({
                        "ok": False,
                        "error": resolved_bet.get("message") or "Kalshi market is not available for this bet.",
                        "resolution": _clean(resolved_bet),
                    }), 400

        ticker = str(
            (resolved_bet or {}).get("market_ticker")
            or explicit_ticker
            or ""
        ).strip()
        if not ticker:
            return jsonify({"ok": False, "error": "market_ticker is required"}), 400

        side = str((resolved_bet or {}).get("side") or data.get("side") or "yes").strip().lower()
        if side not in {"yes", "no"}:
            side = "yes"

        action = str(data.get("action") or "buy").strip().lower()
        if action not in {"buy", "sell"}:
            action = "buy"

        try:
            amount_usd = float(data.get("amount_usd", 0) or 0)
        except Exception:
            amount_usd = 0.0
        try:
            price_cents = int(
                float(
                    (resolved_bet or {}).get("price_cents")
                    or data.get("limit_price_cents", data.get("price_cents", 50))
                    or 50
                )
            )
        except Exception:
            price_cents = 50
        price_cents = max(1, min(price_cents, 99))

        count = data.get("count")
        if count is None:
            if amount_usd > 0:
                count = max(1, int((amount_usd * 100.0) // max(price_cents, 1)))
            else:
                count = 1
        else:
            count = max(1, int(count))

        client_order_id = (
            str(data.get("client_order_id") or "").strip()
            or f"bettor_{_et_calendar_today().strftime('%Y%m%d')}_{abs(hash((ticker, side, count, price_cents))) % 1000000:06d}"
        )

        payload = {
            "ticker": ticker,
            "side": side,
            "action": action,
            "type": str(data.get("type") or "limit"),
            "count": count,
            "client_order_id": client_order_id,
        }
        if side == "yes":
            payload["yes_price"] = price_cents
        else:
            payload["no_price"] = price_cents

        # Allow advanced callers to override/append raw order fields.
        raw_order = data.get("order")
        if isinstance(raw_order, dict):
            payload.update(raw_order)

        response = place_order(payload)
        order = response.get("order") if isinstance(response, dict) else None
        return jsonify({
            "ok": True,
            "client_order_id": client_order_id,
            "order": _clean(order) if isinstance(order, dict) else None,
            "resolution": _clean(resolved_bet) if isinstance(resolved_bet, dict) else None,
            "request": _clean(payload),
            "response": _clean(response),
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/polymarket/order", methods=["POST"])
def api_polymarket_order():
    """Execute a Polymarket order using configured exchange credentials."""
    data = request.get_json(force=True) or {}
    try:
        from data.polymarket import place_order, resolve_ready_bets

        resolved_bet = None
        bet_payload = data.get("bet_payload")
        explicit_token_id = str(data.get("token_id") or "").strip()
        explicit_market_id = str(data.get("market_id") or data.get("market_ticker") or "").strip()
        explicit_market_slug = str(data.get("market_slug") or "").strip()

        if isinstance(bet_payload, dict) and not explicit_market_slug:
            resolved = resolve_ready_bets([bet_payload], force_refresh=False)
            resolved_map = resolved.get("resolutions") or {}
            bet_uid = str(
                bet_payload.get("uid")
                or bet_payload.get("bet_uid")
                or bet_payload.get("prediction_uid")
                or "ready_0"
            )
            resolved_bet = resolved_map.get(bet_uid) or resolved_map.get("ready_0")
            if isinstance(resolved_bet, dict):
                status = str(resolved_bet.get("status") or "")
                if status and status != "matched":
                    return jsonify({
                        "ok": False,
                        "error": resolved_bet.get("message") or "Polymarket market is not available for this bet.",
                        "resolution": _clean(resolved_bet),
                    }), 400

        token_id = str(
            explicit_token_id
            or (resolved_bet or {}).get("token_id")
            or ""
        ).strip()
        if not token_id:
            side_from_payload = str(data.get("side") or (resolved_bet or {}).get("side") or "yes").strip().lower()
            if side_from_payload == "no":
                token_id = str((resolved_bet or {}).get("no_token_id") or "").strip()
            else:
                token_id = str((resolved_bet or {}).get("yes_token_id") or "").strip()

        market_slug = str(
            explicit_market_slug
            or (resolved_bet or {}).get("market_slug")
            or ""
        ).strip()
        if not market_slug:
            return jsonify({"ok": False, "error": "market_slug is required (no Polymarket market slug found for matched bet)."}), 400

        side = str(data.get("side") or (resolved_bet or {}).get("side") or "yes").strip().lower()
        side = "yes" if side not in {"yes", "no"} else side

        try:
            amount_usd = float(data.get("amount_usd", 0) or 0)
        except Exception:
            amount_usd = 0.0
        if amount_usd <= 0:
            return jsonify({"ok": False, "error": "amount_usd must be greater than 0."}), 400

        try:
            px = data.get("price")
            if px is None:
                px = (resolved_bet or {}).get("price")
            price = float(px) if px is not None else None
        except Exception:
            price = None

        placed = place_order(
            market_slug=market_slug,
            amount_usd=amount_usd,
            side=side,
            price=price,
            order_type=str(data.get("order_type") or "ORDER_TYPE_MARKET").upper(),
        )
        tracking = _poly_tp_track_order(placed if isinstance(placed, dict) else {}, resolved_bet)

        return jsonify({
            "ok": bool(placed.get("ok", True)),
            "exchange": "polymarket",
            "market_id": explicit_market_id or str((resolved_bet or {}).get("market_id") or ""),
            "market_slug": market_slug,
            "market_ticker": str((resolved_bet or {}).get("market_ticker") or explicit_market_id or ""),
            "token_id": token_id,
            "side": side,
            "amount_usd": amount_usd,
            "resolution": _clean(resolved_bet) if isinstance(resolved_bet, dict) else None,
            "manager_tracking": _clean(tracking) if isinstance(tracking, dict) else None,
            "response": _clean(placed.get("response") if isinstance(placed, dict) else placed),
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/polymarket/order-manager/status")
def api_polymarket_order_manager_status():
    try:
        with _poly_tp_lock:
            rows = [row for row in (_poly_tp_state.get("orders") or {}).values() if isinstance(row, dict)]
            rows.sort(key=lambda r: str(r.get("tracked_at") or ""), reverse=True)
        return jsonify(
            {
                "ok": True,
                "enabled": bool(_poly_tp_runtime.get("enabled", _POLY_TP_ENABLED)),
                "target_pct": float(_poly_tp_runtime.get("target_pct") or _POLY_TP_TARGET_PCT),
                "check_interval_sec": _POLY_TP_CHECK_SEC,
                "count": len(rows),
                "orders": _clean(rows[:300]),
            }
        )
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "orders": []})


@app.route("/api/polymarket/order-manager/run", methods=["POST"])
def api_polymarket_order_manager_run():
    try:
        result = run_polymarket_take_profit_cycle()
        return jsonify(_clean(result))
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/polymarket/order-manager/config", methods=["POST"])
def api_polymarket_order_manager_config():
    data = request.get_json(force=True) or {}
    try:
        with _poly_tp_lock:
            if "enabled" in data:
                _poly_tp_runtime["enabled"] = bool(data.get("enabled"))
            if "target_pct" in data:
                _poly_tp_runtime["target_pct"] = max(1.0, float(data.get("target_pct") or _POLY_TP_TARGET_PCT))
            target = float(_poly_tp_runtime.get("target_pct") or _POLY_TP_TARGET_PCT)
            enabled = bool(_poly_tp_runtime.get("enabled", _POLY_TP_ENABLED))
            # Keep persisted rows aligned with runtime target.
            for row in (_poly_tp_state.get("orders") or {}).values():
                if isinstance(row, dict) and str(row.get("status") or "").strip().lower() == "tracking":
                    row["target_pct"] = target
        _poly_tp_save_state()
        return jsonify({"ok": True, "enabled": enabled, "target_pct": target})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


_AUTOBET_STATE_PATH = os.path.join(os.path.dirname(SRC_DIR), "data", "polymarket_autobet_state.json")


@app.route("/api/polymarket/autobet-status")
@app.route("/api/autobet-status")
def api_polymarket_autobet_status():
    """Return the current state of the unified auto-betting engine (Polymarket + Kalshi)."""
    try:
        state: dict[str, Any] = {}
        if os.path.exists(_AUTOBET_STATE_PATH):
            with open(_AUTOBET_STATE_PATH, "r", encoding="utf-8") as fh:
                state = json.load(fh)
        placed = state.get("placed_bets") or {}
        bets_list = []
        for key, b in placed.items():
            if isinstance(b, dict):
                exchange = b.get("exchange") or ("kalshi" if "kalshi::" in key else "polymarket")
                bets_list.append({
                    "key": key,
                    "exchange": exchange,
                    "market_title": b.get("market_title") or "",
                    "market_slug": b.get("market_slug") or b.get("market_id") or "",
                    "market_ticker": b.get("market_ticker") or "",
                    "side": b.get("side") or "",
                    "amount_usd": b.get("amount_usd") or 0,
                    "confidence": b.get("confidence") or 0,
                    "pick": b.get("pick") or "",
                    "sport": b.get("sport") or "",
                    "game_date": b.get("game_date") or "",
                    "placed_at": b.get("placed_at") or "",
                    "dry_run": bool(b.get("dry_run")),
                })
        bets_list.sort(key=lambda x: str(x.get("placed_at") or ""), reverse=True)
        by_exchange: dict[str, dict] = {}
        for b in bets_list:
            ex = b.get("exchange") or "unknown"
            if ex not in by_exchange:
                by_exchange[ex] = {"count": 0, "spent": 0.0}
            by_exchange[ex]["count"] += 1
            by_exchange[ex]["spent"] = round(by_exchange[ex]["spent"] + float(b.get("amount_usd") or 0), 4)
        return jsonify({
            "ok": True,
            "cycles": int(state.get("cycles") or 0),
            "total_bets": len(bets_list),
            "total_spent_usd": float(state.get("total_spent_usd") or 0.0),
            "total_spent_polymarket": float(state.get("total_spent_polymarket") or 0.0),
            "total_spent_kalshi": float(state.get("total_spent_kalshi") or 0.0),
            "last_balance_usd": state.get("last_balance_usd"),
            "last_balance_polymarket": state.get("last_balance_polymarket"),
            "last_balance_kalshi": state.get("last_balance_kalshi"),
            "last_balance_check": state.get("last_balance_check"),
            "last_cycle_at": state.get("last_cycle_at"),
            "started_at": state.get("started_at"),
            "by_exchange": by_exchange,
            "bets": _clean(bets_list[:100]),
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "bets": [], "total_bets": 0})


@app.route("/api/kalshi/order-statuses", methods=["POST"])
def api_kalshi_order_statuses():
    """Fetch current Kalshi order status for one or more submitted orders."""
    data = request.get_json(force=True) or {}
    raw_ids = data.get("order_ids") or []
    order_ids = []
    for value in raw_ids if isinstance(raw_ids, list) else []:
        clean_value = str(value or "").strip()
        if clean_value and clean_value not in order_ids:
            order_ids.append(clean_value)
        if len(order_ids) >= 20:
            break
    if not order_ids:
        return jsonify({"ok": False, "error": "order_ids are required", "orders": {}}), 400

    try:
        from data.kalshi import get_order

        orders = {}
        errors = {}
        for order_id in order_ids:
            try:
                payload = get_order(order_id)
                order = payload.get("order") if isinstance(payload, dict) else None
                if isinstance(order, dict):
                    orders[order_id] = _clean(order)
                else:
                    errors[order_id] = "Order payload missing order object"
            except Exception as exc:
                errors[order_id] = str(exc)
        return jsonify({
            "ok": bool(orders),
            "orders": orders,
            "errors": errors,
            "count": len(orders),
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "orders": {}}), 500


@app.route("/api/email/recipients")
def api_email_recipients():
    """Return the configured email recipients from ENV."""
    try:
        raw = os.getenv("EMAIL_TO", "")
        recipients = [e.strip() for e in raw.split(",") if e.strip()]
        return jsonify({"ok": True, "recipients": recipients})
    except Exception as e:
        return jsonify({"ok": False, "recipients": [], "error": str(e)})


@app.route("/api/email/send", methods=["POST"])
def api_email_send():
    """Manually trigger a daily picks email to all configured recipients."""
    try:
        from email_notify import send_daily_picks
        with _lock:
            state = {
                "best_parlays":     list(_state.get("best_parlays", [])),
                "game_cards_today": list(_state.get("game_cards_today", [])),
                "player_props":     list(_state.get("player_props", [])),
            }
        result = send_daily_picks(state)
        ok = bool(result.get("ok")) or (int(result.get("sent", 0) or 0) > 0 and int(result.get("failed", 0) or 0) == 0)
        return jsonify({"ok": ok, **result}), (200 if ok else 500)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/email/send-parlay", methods=["POST"])
def api_email_send_parlay():
    """Send a parlay alert email."""
    data = request.get_json(force=True) or {}
    try:
        from email_notify import send_parlay_alert
        result = send_parlay_alert(data)
        ok = bool(result.get("ok")) or (int(result.get("sent", 0) or 0) > 0 and int(result.get("failed", 0) or 0) == 0)
        return jsonify({"ok": ok, **result}), (200 if ok else 500)
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/live-scores")
def api_live_scores():
    """Poll MLB Stats API for today's game statuses/scores."""
    if _ACTIVE_SPORT == "soccer":
        try:
            from data.soccer_fetcher import get_live_matches
            live = get_live_matches() or []
            games = []
            for g in live:
                away = g.get("away_team", "")
                home = g.get("home_team", "")
                game_key = _compose_game_key(
                    away,
                    home,
                    g.get("game_datetime"),
                    g.get("game_date") or g.get("date"),
                    g.get("game_time"),
                )
                games.append({
                    "home_team": home,
                    "away_team": away,
                    "home_score": g.get("home_score"),
                    "away_score": g.get("away_score"),
                    "status": g.get("status"),
                    "inning": g.get("minute", ""),
                    "inning_half": "",
                    "match_key": _norm_gk(g.get("match_key") or f"{away}@{home}"),
                    "game_key": game_key,
                })
            return jsonify({"ok": True, "games": games})
        except Exception as e:
            return jsonify({"ok": False, "error": str(e), "games": []})
    try:
        import statsapi as mlbstatsapi
        from data.mlb_fetcher import _parse_mlb_game
        today = _et_calendar_today().strftime("%m/%d/%Y")
        raw = mlbstatsapi.schedule(start_date=today, end_date=today) or []
        games = []
        for g in raw:
            status = g.get("status", "")
            parsed = _parse_mlb_game(g, _et_calendar_today().isoformat())
            match_key = _norm_gk(f"{g.get('away_name','')}@{g.get('home_name','')}")
            games.append({
                "game_pk":     g.get("game_id"),
                "home_team":   g.get("home_name", ""),
                "away_team":   g.get("away_name", ""),
                "home_score":  g.get("home_score"),
                "away_score":  g.get("away_score"),
                "status":      status,
                "inning":      g.get("current_inning", ""),
                "inning_half": g.get("inning_state", ""),
                "match_key":   match_key,
                "game_key":    _compose_game_key(
                    g.get("away_name", ""),
                    g.get("home_name", ""),
                    parsed.get("game_datetime"),
                    parsed.get("date"),
                    parsed.get("game_time"),
                ),
            })
        return jsonify({"ok": True, "games": games})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e), "games": []})


# ─── Live-score background watcher ───────────────────────────────────────────
_live_score_timer = None
_LIVE_SCORE_INTERVAL = max(30, int(os.getenv("LIVE_SCORE_INTERVAL_SEC", "60") or "60"))
_last_live_poll_ts = 0.0
_eod_email_sent_dates: set = set()  # track which dates have had EOD results email sent
_cache_poll_timer = None
_CACHE_POLL_INTERVAL = int(os.getenv("CACHE_POLL_INTERVAL_SEC", "120"))
_last_prediction_restudy_ts = 0.0


def _maybe_schedule_day_roll_reanalysis(cache_iso: str | None, today_cards: list[dict], tomorrow_cards: list[dict]) -> None:
    """Kick off a fresh all-sports analysis after midnight so tomorrow's slate is re-evaluated as today."""
    global _last_prediction_restudy_ts
    if _ACTIVE_SPORT != "all":
        return
    if not (today_cards or tomorrow_cards):
        return
    if _state.get("status") == "running":
        return

    try:
        cache_dt = datetime.datetime.fromisoformat(str(cache_iso or "")) if cache_iso else None
    except Exception:
        cache_dt = None

    now_utc = datetime.datetime.now(datetime.timezone.utc)
    today_date = _et_calendar_today()
    should_refresh = False
    if cache_dt is not None:
        cache_day = cache_dt.date() if cache_dt.tzinfo else cache_dt.date()
        if cache_day != today_date:
            should_refresh = True

    if not should_refresh:
        return

    if (time.time() - float(_last_prediction_restudy_ts or 0.0)) < 6 * 60 * 60:
        return

    _last_prediction_restudy_ts = time.time()
    print(f"[cache-sync] Day rollover detected at {now_utc.isoformat()} — re-running analysis for today/tomorrow cards")
    threading.Thread(target=_run_analysis, daemon=True).start()


def _sync_state_from_cache(broadcast: bool = False) -> bool:
    """Refresh in-memory state from DB cache when available."""
    try:
        from data.db import get_analysis_cache
        cached = get_analysis_cache(max_age_hours=22)
        if not cached:
            return False
        today_cards, tomorrow_cards = _normalize_dashboard_card_buckets(
            cached.get("game_cards_today", []),
            cached.get("game_cards_tomorrow", []),
        )
        cache_iso = cached.get("cache_updated_at_iso")
        with _lock:
            if cache_iso and cache_iso == _state.get("last_updated_ts"):
                return False
            if _state.get("status") == "running":
                return False
            _state.update({
                "game_cards_today":    today_cards,
                "game_cards_tomorrow": tomorrow_cards,
                "best_parlays":        cached.get("best_parlays", []),
                "player_props":        cached.get("player_props", []),
                "elite_parlay":        cached.get("elite_parlay"),
                "last_updated":        cached.get("last_updated"),
            })
            if cache_iso:
                _state["last_updated_ts"] = cache_iso
        _maybe_schedule_day_roll_reanalysis(cache_iso, today_cards, tomorrow_cards)
        if broadcast:
            _sse_broadcast("state_update", {
                "status":              "done",
                "last_updated":        cached.get("last_updated"),
                "game_cards_today":    today_cards,
                "game_cards_tomorrow": tomorrow_cards,
                "best_parlays":        cached.get("best_parlays", []),
                "player_props":        cached.get("player_props", []),
                "elite_parlay":        cached.get("elite_parlay"),
            })
        return True
    except Exception:
        return False


def _start_cache_poller():
    """Non-leader workers poll DB cache and broadcast updates to their SSE clients."""
    global _cache_poll_timer
    if _cache_poll_timer is not None:
        return

    def _tick():
        global _cache_poll_timer
        _sync_state_from_cache(broadcast=True)
        _cache_poll_timer = threading.Timer(_CACHE_POLL_INTERVAL, _tick)
        _cache_poll_timer.daemon = True
        _cache_poll_timer.start()

    _tick()


# Interval for the periodic background outcome resolver.
_RESOLVE_INTERVAL = max(
    30,
    int(os.getenv("RESOLVE_INTERVAL_SEC", "300" if _BILL_SAVER_MODE else "60") or ("300" if _BILL_SAVER_MODE else "60")),
)
_PERIODIC_RESOLVE_DAYS_BACK = max(1, int(os.getenv("PERIODIC_RESOLVE_DAYS_BACK", "5") or "5"))
_RESOLVE_START_DELAY_SEC = max(5, int(os.getenv("RESOLVE_START_DELAY_SEC", "15") or "15"))
_resolve_poller_timer: threading.Timer | None = None
_resolve_run_lock = threading.Lock()
_last_resolve_started_ts = 0.0
_last_resolve_finished_ts = 0.0
_last_resolve_ok: bool | None = None
_last_resolve_error = ""

# ─── Kalshi availability monitor ─────────────────────────────────────────────
_KALSHI_MONITOR_INTERVAL_SEC = max(
    120,
    int(
        os.getenv(
            "KALSHI_MONITOR_INTERVAL_SEC",
            "900" if _BILL_SAVER_MODE else "300",
        )
        or ("900" if _BILL_SAVER_MODE else "300")
    ),
)
_kalshi_monitor_timer: threading.Timer | None = None
_kalshi_monitor_last_statuses: dict[str, str] = {}   # bet_uid → last known status
_kalshi_match_alert_lock = threading.Lock()
_kalshi_match_alerted: dict[str, float] = {}
_KALSHI_MATCH_ALERT_COOLDOWN_SEC = max(
    300, int(os.getenv("KALSHI_MATCH_ALERT_COOLDOWN_SEC", "21600") or "21600")
)


def _prune_kalshi_match_alerts(now_ts: float | None = None) -> None:
    now = float(now_ts or time.time())
    cutoff = now - _KALSHI_MATCH_ALERT_COOLDOWN_SEC
    stale = [k for k, ts in _kalshi_match_alerted.items() if float(ts or 0.0) < cutoff]
    for key in stale:
        _kalshi_match_alerted.pop(key, None)


def _maybe_email_kalshi_matches(source_bets: list[dict], resolutions: dict, *, source_tag: str = "resolve-ready") -> int:
    """Send a single email for newly matched bets, deduped by bet+ticker cooldown."""
    if not isinstance(resolutions, dict) or not resolutions:
        return 0
    if not isinstance(source_bets, list) or not source_bets:
        return 0

    lookup: dict[str, dict] = {}
    for idx, bet in enumerate(source_bets):
        if not isinstance(bet, dict):
            continue
        uid = str(
            bet.get("uid")
            or bet.get("bet_uid")
            or bet.get("prediction_uid")
            or f"ready_{idx}"
        ).strip()
        if uid:
            lookup[uid] = bet

    newly_matched = []
    now_ts = time.time()
    with _kalshi_match_alert_lock:
        _prune_kalshi_match_alerts(now_ts)
        for uid, res in resolutions.items():
            if not isinstance(res, dict):
                continue
            status = str(res.get("status") or "").strip().lower()
            if status != "matched":
                continue
            ticker = str(res.get("market_ticker") or "").strip()
            if not ticker:
                continue
            alert_key = f"{uid}|{ticker}"
            if alert_key in _kalshi_match_alerted:
                continue
            _kalshi_match_alerted[alert_key] = now_ts
            bet = lookup.get(str(uid).strip()) or {}
            newly_matched.append({
                "bet_uid": uid,
                "bet": bet,
                "pred": bet,
                "resolution": res,
            })

    if not newly_matched:
        return 0

    try:
        _send_kalshi_alert(newly_matched)
        _log(f"[kalshi-email] Sent {len(newly_matched)} new match alert(s) from {source_tag}")
        return len(newly_matched)
    except Exception as exc:
        _log(f"[kalshi-email] Failed to send match alerts from {source_tag}: {exc}")
        return 0


def _run_kalshi_monitor():
    """Check all DB predictions against live Kalshi markets.
    When a bet transitions to 'matched', send an email alert.
    Runs every 20 minutes without hammering the Kalshi API.
    """
    try:
        today_str = _et_calendar_today().isoformat()
        from data.db import get_predictions_for_date, get_prop_history
        from data.kalshi import resolve_ready_bets

        preds = []
        try:
            preds = get_predictions_for_date(today_str) or []
        except Exception:
            pass

        props = []
        try:
            props = get_prop_history(
                days=2,
                outcome="PENDING",
                sport=None if _ACTIVE_SPORT == "all" else _ACTIVE_SPORT,
            ) or []
        except Exception:
            props = []

        if not preds and not props:
            return

        # Build bet dicts compatible with resolve_ready_bets
        ready_bets = []
        for p in preds:
            pick = str(p.get("pick") or "")
            home_team = str(p.get("home_team") or "")
            away_team = str(p.get("away_team") or "")
            ready_bets.append({
                "bet_uid":     p.get("bet_uid") or str(p.get("id") or ""),
                "sport":       p.get("sport") or "",
                "bet_type":    p.get("bet_type") or "moneyline",
                "pick":        pick,
                "label":       pick,
                "line":        p.get("line"),
                "game_date":   str(p.get("game_date") or today_str),
                "game_time":   str(p.get("game_time") or ""),
                "home_team":   home_team,
                "away_team":   away_team,
                "player_name": _extract_player_name(pick),
                "team":        _extract_pick_team(pick, home_team, away_team),
                "direction":   _extract_direction(pick),
                "prop_type":   _extract_prop_type(p.get("bet_type") or "", pick),
                "side_default": "no" if "under" in pick.lower() else "yes",
            })

        for p in props:
            stats_json = p.get("stats_json") if isinstance(p.get("stats_json"), dict) else {}
            pick_label = " ".join(
                [
                    str(p.get("player_name") or p.get("name") or "").strip(),
                    str(p.get("recommendation") or p.get("direction") or "").strip().upper(),
                    str(p.get("line") if p.get("line") is not None else "").strip(),
                    str(p.get("prop_type") or p.get("stat_type") or "").strip(),
                ]
            ).strip()
            game_key = str(p.get("game_key") or stats_json.get("game_key") or stats_json.get("game") or "").strip()
            ready_bets.append({
                "bet_uid": p.get("bet_uid") or str(p.get("id") or ""),
                "sport": p.get("sport") or "",
                "kind": "player_prop",
                "bet_type": "player_prop",
                "pick": pick_label,
                "label": pick_label,
                "line": p.get("line"),
                "game_date": str(p.get("game_date") or today_str),
                "game_time": "",
                "game": game_key,
                "game_key": game_key,
                "home_team": str(stats_json.get("home_team") or ""),
                "away_team": str(stats_json.get("away_team") or ""),
                "player_name": str(p.get("player_name") or ""),
                "team": str(p.get("team") or ""),
                "direction": _extract_direction(pick_label),
                "prop_type": p.get("prop_type") or p.get("stat_type") or "",
                "side_default": "no" if "under" in pick_label.lower() else "yes",
            })

        if not ready_bets:
            return

        resolved_payload = resolve_ready_bets(ready_bets)
        resolutions = resolved_payload.get("resolutions") if isinstance(resolved_payload, dict) else {}
        if not isinstance(resolutions, dict):
            resolutions = {}
        _maybe_email_kalshi_matches(ready_bets, resolutions, source_tag="monitor")
    except Exception as exc:
        print(f"[kalshi-monitor] Error: {exc}")


def _extract_player_name(pick_text: str) -> str:
    """Try to extract a player name from a pick label like 'Victor Wembanyama Over 32.5 Points'."""
    import re
    m = re.match(r'^([A-Z][a-z]+ [A-Z][a-z\']+)', pick_text.strip())
    return m.group(1) if m else ""


def _extract_pick_team(pick_text: str, home_team: str, away_team: str) -> str:
    """Best-effort team extraction from a pick label for resolver context."""
    import re

    pick = str(pick_text or "").lower()
    home = str(home_team or "").strip()
    away = str(away_team or "").strip()
    if not pick:
        return ""

    if home and home.lower() in pick:
        return home
    if away and away.lower() in pick:
        return away

    stop = {"the", "fc", "cf", "sc", "club", "city", "united"}

    def _tokens(team_name: str) -> list[str]:
        return [t for t in re.findall(r"[a-z0-9']+", team_name.lower()) if len(t) >= 4 and t not in stop]

    home_hits = sum(1 for tok in _tokens(home) if tok in pick)
    away_hits = sum(1 for tok in _tokens(away) if tok in pick)

    if home_hits == 0 and away_hits == 0:
        return ""
    return home if home_hits >= away_hits else away


def _extract_direction(pick_text: str) -> str:
    t = pick_text.lower()
    if "over" in t:
        return "over"
    if "under" in t:
        return "under"
    return ""


def _extract_prop_type(bet_type: str, pick_text: str) -> str:
    bt = str(bet_type or "").strip().lower()
    t = str(pick_text or "").lower()
    if "point" in t or "pts" in t:
        return "points"
    if "rebound" in t or "reb" in t:
        return "rebounds"
    if "assist" in t or "ast" in t:
        return "assists"
    if "strikeout" in t or " k " in f" {t} ":
        return "strikeouts"
    if "home run" in t or " hr" in t:
        return "home_runs"
    if "rbi" in t:
        return "rbis"
    if "total base" in t or "tb" in t:
        return "total_bases"
    if "hit" in t:
        return "hits"
    if "goal" in t:
        return "goals"
    if "shot" in t:
        return "shots"
    if "touchdown" in t or " td" in t:
        return "touchdowns"
    if "passing" in t:
        return "passing"
    if "rushing" in t:
        return "rushing"
    if "receiving" in t:
        return "receiving"
    if bt == "player_prop":
        return "player_prop"
    return bt


def _send_kalshi_alert(matched_bets: list[dict]):
    """Send email alert for newly matched Kalshi bets."""
    try:
        from email_notify import send_email
        rows_html = ""
        rows_plain = ""
        for item in matched_bets:
            pred = item.get("pred") or {}
            res = item.get("resolution") or {}
            sport = str(pred.get("sport") or "").upper()
            pick = str(pred.get("pick") or item.get("bet", {}).get("pick") or "")
            game = f"{pred.get('away_team') or ''} @ {pred.get('home_team') or ''}".strip(" @")
            game_date = str(pred.get("game_date") or "")
            market_ticker = res.get("market_ticker") or ""
            market_title = res.get("market_title") or ""
            price_cents = res.get("price_cents")
            close_time = res.get("close_time") or ""
            price_str = f"{price_cents}¢" if price_cents else "—"
            line_note = str(res.get("line_note") or "").strip()
            line_note_html = (
                f"<br><small style='color:#e67e22'><b>⚠ {line_note}</b></small>"
                if line_note else ""
            )
            rows_html += (
                f"<tr>"
                f"<td style='padding:6px 10px'><b>{sport}</b></td>"
                f"<td style='padding:6px 10px'>{game}<br><small>{game_date}</small></td>"
                f"<td style='padding:6px 10px'>{pick}{line_note_html}</td>"
                f"<td style='padding:6px 10px'>{market_ticker}<br><small>{market_title}</small></td>"
                f"<td style='padding:6px 10px; text-align:center'>{price_str}</td>"
                f"<td style='padding:6px 10px'>{close_time[:16]}</td>"
                f"</tr>"
            )
            note_suffix = f" [{line_note}]" if line_note else ""
            rows_plain += f"  [{sport}] {pick}{note_suffix} | {game} | Ticker: {market_ticker} | Price: {price_str}\n"

        subject = f"🎯 {len(matched_bets)} Kalshi bet(s) now available — place now!"
        html_body = f"""
        <html><body style='font-family:Arial,sans-serif'>
        <h2 style='color:#1a7a4a'>🎯 Kalshi Bets Available Now</h2>
        <p>{len(matched_bets)} prediction(s) have been matched to open Kalshi markets.
        Log in to <a href='https://kalshi.com'>kalshi.com</a> to place your bets before markets close.</p>
        <table border='1' cellspacing='0' style='border-collapse:collapse;width:100%'>
        <thead><tr style='background:#1a7a4a;color:white'>
          <th style='padding:8px'>Sport</th>
          <th style='padding:8px'>Game</th>
          <th style='padding:8px'>Pick</th>
          <th style='padding:8px'>Kalshi Market</th>
          <th style='padding:8px'>Price</th>
          <th style='padding:8px'>Closes</th>
        </tr></thead>
        <tbody>{rows_html}</tbody>
        </table>
        <p style='color:#666;font-size:12px'>This alert was generated automatically by your Bettor bot.</p>
        </body></html>
        """
        plain_body = f"Kalshi Bets Available:\n{rows_plain}\nLog in to kalshi.com to place your bets."
        result = send_email(subject, html_body, plain_body)
        if result.get("ok"):
            print(f"[kalshi-monitor] Email alert sent: {result}")
        else:
            note = result.get("note") or result.get("error") or "email skipped"
            print(f"[kalshi-monitor] Email alert skipped: {note}")
    except Exception as exc:
        print(f"[kalshi-monitor] Email send failed: {exc}")


def _start_kalshi_monitor():
    """Start the background Kalshi availability monitor."""
    global _kalshi_monitor_timer
    if _kalshi_monitor_timer is not None:
        return

    def _tick():
        global _kalshi_monitor_timer
        try:
            _run_kalshi_monitor()
        except Exception as exc:
            print(f"[kalshi-monitor] tick error: {exc}")
        _kalshi_monitor_timer = threading.Timer(_KALSHI_MONITOR_INTERVAL_SEC, _tick)
        _kalshi_monitor_timer.daemon = True
        _kalshi_monitor_timer.start()

    # First check after 90 seconds (let startup complete but alert quickly).
    _kalshi_monitor_timer = threading.Timer(90, _tick)
    _kalshi_monitor_timer.daemon = True
    _kalshi_monitor_timer.start()
    print(f"[kalshi-monitor] Started (every {max(1, _KALSHI_MONITOR_INTERVAL_SEC // 60)} min)")


def _start_kalshi_ws_client():
    """Start the persistent Kalshi WebSocket connection (ticker feed).

    Runs in a daemon thread via the KalshiWebSocketManager.  If Kalshi
    credentials are missing the manager stops itself silently on first connect,
    so this is always safe to call.
    """
    try:
        from data.kalshi import start_kalshi_ws, get_live_tickers  # noqa: F401

        mgr = start_kalshi_ws()

        # Push a kalshi_ticker SSE event whenever new live prices arrive so the
        # frontend can refresh without polling.
        _ticker_sse_last: dict = {}

        def _on_ticker(market_ticker: str, data: dict) -> None:  # noqa: WPS430
            # Batch: only broadcast every 5 s to avoid SSE flood.
            pass  # batching handled by a timer — see below

        mgr.add_update_callback(_on_ticker)

        # Periodic SSE broadcast of the full ticker snapshot (every 5 s)
        def _broadcast_tickers() -> None:
            global _ticker_broadcast_timer
            try:
                tickers = get_live_tickers()
                if tickers:
                    _sse_broadcast("kalshi_ticker", {"tickers": tickers})
            except Exception as exc:
                print(f"[kalshi-ws-sse] broadcast error: {exc}")
            _ticker_broadcast_timer = threading.Timer(5, _broadcast_tickers)
            _ticker_broadcast_timer.daemon = True
            _ticker_broadcast_timer.start()

        # First broadcast after 30 s (give WS time to accumulate data)
        _ticker_broadcast_timer = threading.Timer(30, _broadcast_tickers)
        _ticker_broadcast_timer.daemon = True
        _ticker_broadcast_timer.start()

        print("[kalshi-ws] WebSocket manager started (ticker feed)")
    except Exception as exc:
        print(f"[kalshi-ws] Failed to start WebSocket manager: {exc}")


_ticker_broadcast_timer: "threading.Timer | None" = None

# ─── End-of-Day Bulk Settlement ───────────────────────────────────────────────
# Runs at 10pm, 11pm, 1am, and 2am ET to aggressively settle every prediction
# made during the day so the 3am settlement gate always passes clean.
_EOD_SETTLE_ENABLED = str(os.getenv("EOD_SETTLE_ENABLED", "1")).strip().lower() in {"1", "true", "yes", "on"}
_EOD_SETTLE_DAYS_BACK = max(2, int(os.getenv("EOD_SETTLE_DAYS_BACK", "3") or "3"))


def _run_eod_bulk_settle(label: str = "eod") -> None:
    """Aggressively settle all pending predictions/props/parlays.

    Strategy:
      1. Run the full resolver with a 3-day look-back.
      2. If any PENDING items remain, run archive_previous_day_data so
         they are force-closed as ARCHIVED rather than blocking the gate.
      3. Broadcast a performance_update SSE so the dashboard refreshes.
    """
    print(f"[eod-settle/{label}] Starting EOD bulk settlement sweep …")
    try:
        result = _run_resolver_locked(days_back=_EOD_SETTLE_DAYS_BACK) or {}
        n_g   = result.get("games", 0)
        n_p   = result.get("props", 0)
        n_par = result.get("parlays", 0)
        skip  = result.get("skipped", False)
        if skip:
            print(f"[eod-settle/{label}] Resolver busy — will retry next scheduled window")
            return
        print(f"[eod-settle/{label}] Resolver resolved {n_g} games, {n_p} props, {n_par} parlays")
    except Exception as exc:
        print(f"[eod-settle/{label}] Resolver error: {exc}")

    # Safety archival: force-close anything still PENDING from previous days
    try:
        from data.db import archive_previous_day_data
        today = _et_calendar_today()
        archive_previous_day_data(today.isoformat())
        print(f"[eod-settle/{label}] Safety archive completed for dates before {today.isoformat()}")
    except Exception as exc:
        print(f"[eod-settle/{label}] Archive error: {exc}")

    # Broadcast refresh so dashboard cards update immediately
    try:
        from data.db import get_performance_stats, get_parlay_performance_stats
        db_sport = None if _ACTIVE_SPORT == "all" else _ACTIVE_SPORT
        _sse_broadcast("performance_update", {
            "stats": get_performance_stats(sport=db_sport),
            "parlay_stats": get_parlay_performance_stats(sport=db_sport),
        })
        print(f"[eod-settle/{label}] Performance update SSE broadcast sent")
    except Exception as exc:
        print(f"[eod-settle/{label}] SSE broadcast error: {exc}")


def _start_outcome_resolver():
    """Periodically resolve pending bets for ALL sports.
    Uses threading.Timer — no APScheduler needed."""
    global _resolve_poller_timer
    if _resolve_poller_timer is not None:
        return

    def _tick():
        global _resolve_poller_timer
        try:
            global _last_resolve_started_ts
            now_ts = time.time()
            if (now_ts - _last_resolve_started_ts) < max(30.0, _RESOLVE_INTERVAL * 0.5):
                print("[auto-resolve] Skipped periodic run (recent run already executed)")
            elif _resolve_run_lock.acquire(blocking=False):
                _last_resolve_started_ts = now_ts
                try:
                    print("[auto-resolve] Running periodic all-sport resolver…")
                    _resolve_all_sports_outcomes(days_back=_PERIODIC_RESOLVE_DAYS_BACK)
                finally:
                    _resolve_run_lock.release()
            else:
                print("[auto-resolve] Skipped periodic run (resolver already in progress)")
        except Exception as exc:
            print(f"[auto-resolve] error: {exc}")
        _resolve_poller_timer = threading.Timer(_RESOLVE_INTERVAL, _tick)
        _resolve_poller_timer.daemon = True
        _resolve_poller_timer.start()

    # First run shortly after startup.
    _resolve_poller_timer = threading.Timer(_RESOLVE_START_DELAY_SEC, _tick)
    _resolve_poller_timer.daemon = True
    _resolve_poller_timer.start()
    print(
        "[auto-resolve] Periodic resolver started "
        f"(every {_RESOLVE_INTERVAL}s, first run in {_RESOLVE_START_DELAY_SEC}s)"
    )


def _start_live_scores():
    # Start live-score + auto-resolve polling for all sports
    if _live_score_timer is None:
        _poll_live_scores()


def _poll_live_scores():
    """Runs in background: updates live scores and auto-resolves completed games."""
    global _live_score_timer, _last_live_poll_ts
    try:
        import requests as _req

        def _status_from_espn_event(ev: dict[str, Any]) -> tuple[str, str]:
            st = (ev.get("status") or {}).get("type") or {}
            desc = str(st.get("description") or "").strip()
            state = str(st.get("state") or "").strip().lower()
            short = str(st.get("shortDetail") or "").strip()
            low = desc.lower()
            if state in {"post", "final", "finished"} or "final" in low:
                return "Final", short
            if state in {"in", "in_progress", "live"} or "progress" in low or "halftime" in low:
                return "In Progress", short
            return "Scheduled", short

        def _to_num(v):
            try:
                if v is None or str(v).strip() == "":
                    return None
                return int(float(v))
            except Exception:
                return None

        today = _et_calendar_today()
        tomorrow = today + datetime.timedelta(days=1)
        state_map: dict[str, dict[str, Any]] = {}
        raw = []

        try:
            import statsapi as mlbstatsapi
            from data.mlb_fetcher import _parse_mlb_game

            today_str = today.strftime("%m/%d/%Y")
            raw = mlbstatsapi.schedule(start_date=today_str, end_date=today_str) or []
            for g in raw:
                parsed = _parse_mlb_game(g, today.isoformat())
                match_key = _norm_gk(f"{g.get('away_name','')}@{g.get('home_name','')}")
                key = _compose_game_key(
                    g.get("away_name", ""),
                    g.get("home_name", ""),
                    parsed.get("game_datetime"),
                    parsed.get("date"),
                    parsed.get("game_time"),
                )
                state_map[key] = {
                    "game_pk":     g.get("game_id"),
                    "match_key":   match_key,
                    "game_key":    key,
                    "home_score":  g.get("home_score"),
                    "away_score":  g.get("away_score"),
                    "status":      g.get("status"),
                    "inning":      g.get("current_inning", ""),
                    "inning_half": g.get("inning_state", ""),
                }
        except Exception:
            raw = []

        # Multi-sport fallback keeps status/score tracking alive even when MLB statsapi
        # is unavailable in the runtime environment.
        try:
            pairs = sorted({(sport_path, league_path) for sport_path, league_path, _sport_group, _map in _ESPN_RESOLVE_CONFIGS})
            for day in (today, tomorrow):
                token = day.strftime("%Y%m%d")
                for sport_path, league_path in pairs:
                    url = f"https://site.api.espn.com/apis/site/v2/sports/{sport_path}/{league_path}/scoreboard"
                    try:
                        resp = _req.get(url, params={"dates": token, "limit": 200}, timeout=8)
                        if resp.status_code != 200:
                            continue
                        payload = resp.json() or {}
                    except Exception:
                        continue

                    for ev in (payload.get("events") or []):
                        comp = (ev.get("competitions") or [{}])[0]
                        competitors = comp.get("competitors") or []
                        if len(competitors) < 2:
                            continue
                        home_c = next((c for c in competitors if str(c.get("homeAway") or "").lower() == "home"), competitors[0])
                        away_c = next((c for c in competitors if str(c.get("homeAway") or "").lower() == "away"), competitors[1] if len(competitors) > 1 else competitors[0])
                        home = str(((home_c.get("team") or {}).get("displayName") or "")).strip()
                        away = str(((away_c.get("team") or {}).get("displayName") or "")).strip()
                        if not home or not away:
                            continue

                        game_dt = str(ev.get("date") or "").strip()
                        game_date = game_dt[:10] if len(game_dt) >= 10 else day.isoformat()
                        game_time = game_dt[11:16] if "T" in game_dt and len(game_dt) >= 16 else ""
                        match_key = _norm_gk(f"{away}@{home}")
                        key = _compose_game_key(away, home, game_dt, game_date, game_time)
                        status, short = _status_from_espn_event(ev)
                        score_home = _to_num(home_c.get("score"))
                        score_away = _to_num(away_c.get("score"))

                        existing = state_map.get(key)
                        # Prefer live/final payloads over scheduled-only duplicates.
                        if existing and status == "Scheduled" and str(existing.get("status") or "").lower() not in {"scheduled", ""}:
                            continue

                        state_map[key] = {
                            "game_pk": str(ev.get("id") or ""),
                            "match_key": match_key,
                            "game_key": key,
                            "home_score": score_home,
                            "away_score": score_away,
                            "status": status,
                            "inning": short,
                            "inning_half": "",
                        }
        except Exception:
            pass

        with _lock:
            _state["live_scores"] = state_map
            _last_live_poll_ts = time.time()

        # Broadcast full status map every poll (including empty) so clients can clear stale entries.
        _sse_broadcast("live_scores", {"scores": state_map})

        # Auto-resolve finished games (non-blocking, errors suppressed)
        def _is_final_status(status):
            s = (status or "").lower()
            return any(k in s for k in ("final", "game over", "completed"))

        # After 8pm ET the resolver uses a 2-day look-back so games that
        # started "yesterday" (e.g. late West Coast games) are also swept.
        _now_et_hour = (
            datetime.datetime.now(datetime.timezone.utc)
            .astimezone(datetime.timezone(datetime.timedelta(hours=-5)))
            .hour
        )
        # Evening hours: 20–23 and 0–3 ET (8pm – 3am window)
        _is_evening = _now_et_hour >= 20 or _now_et_hour <= 3
        _resolve_days = 2 if _is_evening else 1

        has_final = any(_is_final_status(v.get("status", "")) for v in state_map.values())
        # Also trigger if state_map has games but none are explicitly "final" yet —
        # the resolver can still pick up results via MLB StatsAPI / TheSportsDB.
        has_games = bool(state_map)
        if has_final or (has_games and _is_evening):
            try:
                # Universal resolver handles all sports (MLB statsapi path included)
                res = _run_resolver_locked(days_back=_resolve_days)
                n_g = res.get("games", 0)
                n_p = res.get("props", 0)
                n_par = res.get("parlays", 0)
                if n_g or n_p or n_par:
                    print(f"[live-scores] Auto-resolved {n_g} predictions, {n_p} props, {n_par} parlays")
                    try:
                        from data.db import get_performance_stats, get_parlay_performance_stats

                        db_sport = None if _ACTIVE_SPORT == "all" else _ACTIVE_SPORT
                        _sse_broadcast("performance_update", {
                            "stats": get_performance_stats(sport=db_sport),
                            "parlay_stats": get_parlay_performance_stats(sport=db_sport),
                        })
                    except Exception:
                        pass
            except Exception as exc:
                print(f"[live-scores] resolve error: {exc}")

        # ── EOD results email — fire once when all today's games are Final ──
        today_key = today.isoformat()
        all_today_final = bool(raw) and all(_is_final_status(g.get("status","")) for g in raw)
        if all_today_final and today_key not in _eod_email_sent_dates:
            # Mark attempted immediately — never retry the same date even on failure
            _eod_email_sent_dates.add(today_key)
            print(f"[live-scores] All today's games final — building EOD results email for {today_key}")
            try:
                from data.db import get_conn
                import psycopg2.extras
                conn = get_conn()
                rows = []
                if conn:
                    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                    cur.execute("""
                        SELECT bet_type, pick, odds_am, model_prob, confidence,
                               home_team, away_team, outcome
                        FROM predictions
                        WHERE game_date = %s AND sport = 'mlb' AND outcome IN ('WIN','LOSS','PUSH')
                        ORDER BY outcome, model_prob DESC
                    """, (today_key,))
                    rows = [dict(r) for r in cur.fetchall()]
                    conn.close()

                prop_rows = []
                try:
                    conn2 = get_conn()
                    if conn2:
                        cur2 = conn2.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                        cur2.execute("""
                            SELECT name, team, stat_type, prop_label, line, direction, outcome, actual
                            FROM prop_history
                            WHERE game_date = %s AND sport = 'mlb' AND outcome IN ('WIN','LOSS','PUSH')
                            ORDER BY outcome
                        """, (today_key,))
                        prop_rows = [dict(r) for r in cur2.fetchall()]
                        conn2.close()
                except Exception:
                    pass

                wins   = sum(1 for r in rows if r.get("outcome") == "WIN")
                losses = sum(1 for r in rows if r.get("outcome") == "LOSS")
                pushes = sum(1 for r in rows if r.get("outcome") == "PUSH")
                total  = wins + losses + pushes
                hit_rate = round(wins / total * 100, 1) if total > 0 else 0.0

                picks_formatted = [
                    {
                        "pick":      r.get("pick",""),
                        "bet_type":  r.get("bet_type",""),
                        "outcome":   r.get("outcome",""),
                        "game":      f"{r.get('away_team','')} @ {r.get('home_team','')}",
                        "odds_am":   r.get("odds_am"),
                        "model_prob":r.get("model_prob",0),
                    }
                    for r in rows
                ]

                import datetime as _dt
                results_payload = {
                    "date_str": _dt.date.today().strftime("%A, %B %d, %Y"),
                    "total":    total,
                    "wins":     wins,
                    "losses":   losses,
                    "pushes":   pushes,
                    "hit_rate": hit_rate,
                    "picks":    picks_formatted,
                    "props":    prop_rows,
                    "parlays":  [],
                }

                from email_notify import send_daily_results
                result = send_daily_results(results_payload)
                if result.get("sent", 0) > 0:
                    print(f"[live-scores] EOD results email sent ({wins}W/{losses}L/{pushes}P)")
                else:
                    print(f"[live-scores] EOD email failed: {result.get('errors')}")
                    # Remove from sent set so a manual server restart can retry
                    # (already added above to prevent per-poll retries)
            except Exception as _eod_e:
                print(f"[live-scores] EOD email error: {_eod_e}")
    except Exception as exc:
        print(f"[live-scores] poll error: {exc}")
    finally:
        _live_score_timer = threading.Timer(_LIVE_SCORE_INTERVAL, _poll_live_scores)
        _live_score_timer.daemon = True
        _live_score_timer.start()


# Live-score polling is started by the leader worker.


# ─── SSE stream endpoint ─────────────────────────────────────────────────────
@app.route("/api/stream")
def api_stream():
    """Long-lived SSE connection. Each browser tab connects once."""
    # Enforce a hard cap so SSE connections can't starve all Gunicorn threads.
    _MAX_SSE_CLIENTS = 8
    with _sse_lock:
        if len(_sse_clients) >= _MAX_SSE_CLIENTS:
            # Drop oldest client to make room
            try:
                evicted = _sse_clients.pop(0)
                evicted.put_nowait("event: close\ndata: {}\n\n")
            except Exception:
                pass
    q: queue.Queue = queue.Queue(maxsize=50)
    with _sse_lock:
        _sse_clients.append(q)

    # Immediately send current state so fresh page loads fill in fast
    with _lock:
        hello = {
            "status":              _state.get("status", "idle"),
            "last_updated":        _state.get("last_updated"),
            "game_cards_today":    _state.get("game_cards_today", []),
            "game_cards_tomorrow": _state.get("game_cards_tomorrow", []),
            "best_parlays":        _state.get("best_parlays", []),
            "player_props":        _state.get("player_props", []),
            "elite_parlay":        _state.get("elite_parlay"),
            "live_scores":         _state.get("live_scores", {}),
        }
    try:
        q.put_nowait(f"event: state_update\ndata: {json.dumps(hello, default=_json_safe_default)}\n\n")
    except queue.Full:
        pass

    def _generate():
        yield ": connected\n\n"
        while True:
            try:
                msg = q.get(timeout=25)
                yield msg
            except queue.Empty:
                yield ": ping\n\n"   # keep-alive

    def _cleanup(resp):
        with _sse_lock:
            if q in _sse_clients:
                _sse_clients.remove(q)
        return resp

    response = Response(
        _generate(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )
    response.call_on_close(lambda: _cleanup(None))
    return response


# ─── APScheduler: auto-run analysis every 5 hours ───────────────────────────
def _scheduled_analysis(force: bool = False, lock_today: bool = False):
    """Called by APScheduler. Skips if already running or cache is very fresh."""
    with _lock:
        if _state["status"] == "running":
            return
        last_ts = _state.get("last_updated_ts")

    # Skip if ran within the last ~5 hours
    if not force and last_ts:
        try:
            dt = datetime.datetime.fromisoformat(last_ts)
            now = datetime.datetime.now(datetime.timezone.utc) if dt.tzinfo else datetime.datetime.utcnow()
            age_min = (now - dt).total_seconds() / 60
            if age_min < 295:
                return
        except Exception:
            pass

    print(f"[scheduler] Auto-running analysis at {datetime.datetime.now().strftime('%H:%M')}")
    with _lock:
        _state["status"]    = "running"
        _state["phase"]     = _PHASES[0]
        _state["phase_idx"] = 0
    _sse_broadcast("status", {"status": "running", "phase": _PHASES[0]})
    lock_date = _et_calendar_today() if lock_today else None
    threading.Thread(target=_run_analysis, args=(lock_date,), daemon=True).start()


def _scheduled_hf_push():
    """Daily job: sync all sport data from PostgreSQL → HuggingFace dataset repo."""
    def _run():
        print(f"[hf-push] Starting daily HuggingFace data sync at {datetime.datetime.now().strftime('%H:%M')}")
        try:
            from data.hf_uploader import HFUploader
            up = HFUploader()
            if not up._ok:
                print("[hf-push] Uploader not ready (missing key or libs) — skipping")
                return
            up.sync_from_db()
            up.flush_all()
            print("[hf-push] Daily sync complete → https://huggingface.co/datasets/papylove/sportprediction")
        except Exception as exc:
            print(f"[hf-push] Error during daily sync: {exc}")
    threading.Thread(target=_run, daemon=True, name="hf-daily-push").start()


def _start_scheduler():
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        from apscheduler.triggers.interval import IntervalTrigger
        from apscheduler.triggers.cron import CronTrigger
        _poly_tp_load_state()
        sched = BackgroundScheduler(daemon=True)
        if _AUTO_ANALYSIS_INTERVAL_MIN > 0:
            sched.add_job(
                _scheduled_analysis,
                IntervalTrigger(minutes=_AUTO_ANALYSIS_INTERVAL_MIN),
                id="auto_analysis",
                max_instances=1,
                coalesce=True,
                misfire_grace_time=_SCHED_MISFIRE_GRACE_SEC,
            )
        # Daily lock run (ET morning)
        sched.add_job(
            lambda: _scheduled_analysis(force=True, lock_today=True),
            CronTrigger(hour=_DAILY_LOCK_HOUR_ET, minute=_DAILY_LOCK_MINUTE_ET,
                        timezone="America/New_York"),
            id="daily_lock",
            max_instances=1,
            coalesce=True,
            misfire_grace_time=_SCHED_MISFIRE_GRACE_SEC,
        )
        if _AUTO_BACKFILL_ENABLED:
            sched.add_job(
                _scheduled_multi_sport_backfill,
                CronTrigger(
                    hour=_AUTO_BACKFILL_HOUR_ET,
                    minute=_AUTO_BACKFILL_MINUTE_ET,
                    timezone="America/New_York",
                ),
                id="daily_multi_sport_backfill",
                max_instances=1,
                coalesce=True,
                misfire_grace_time=_SCHED_MISFIRE_GRACE_SEC,
            )
        if _POLY_TP_CHECK_SEC > 0:
            sched.add_job(
                run_polymarket_take_profit_cycle,
                IntervalTrigger(seconds=_POLY_TP_CHECK_SEC),
                id="polymarket_auto_take_profit",
                max_instances=1,
                coalesce=True,
                misfire_grace_time=_SCHED_MISFIRE_GRACE_SEC,
            )

        # ── Nightly EOD settlement sweeps (ET) ─────────────────────────────
        # Run at 10pm, 11pm, 1am, and 2am to settle every prediction before
        # the 3am settlement gate.  A final safety-archive job at 2:50am
        # force-closes any remaining PENDING rows before the backfill window.
        if _EOD_SETTLE_ENABLED:
            for _eod_hour, _eod_minute, _eod_label in (
                (22, 0,  "2200"),
                (23, 0,  "2300"),
                ( 1, 0,  "0100"),
                ( 2, 0,  "0200"),
            ):
                sched.add_job(
                    lambda lbl=_eod_label: _run_eod_bulk_settle(lbl),
                    CronTrigger(hour=_eod_hour, minute=_eod_minute,
                                timezone="America/New_York"),
                    id=f"eod_settle_{_eod_label}",
                    max_instances=1,
                    coalesce=True,
                    misfire_grace_time=120,
                )
            # 2:50am safety archive — ensures nothing is PENDING when the gate
            # runs at 3:00am or whenever the first prediction of the new day fires.
            sched.add_job(
                lambda: _run_eod_bulk_settle("0250_safety"),
                CronTrigger(hour=2, minute=50, timezone="America/New_York"),
                id="eod_settle_0250_safety",
                max_instances=1,
                coalesce=True,
                misfire_grace_time=120,
            )

        # ── Daily HuggingFace data push (4:15 AM ET) ───────────────────────
        # Runs after all settlement and backfill jobs are done so the dataset
        # always contains a complete, settled snapshot of the day's data.
        sched.add_job(
            _scheduled_hf_push,
            CronTrigger(hour=4, minute=15, timezone="America/New_York"),
            id="daily_hf_push",
            max_instances=1,
            coalesce=True,
            misfire_grace_time=_SCHED_MISFIRE_GRACE_SEC,
        )

        sched.start()
        if _AUTO_ANALYSIS_INTERVAL_MIN > 0:
            print(f"[scheduler] APScheduler started — analysis every {_AUTO_ANALYSIS_INTERVAL_MIN} minutes")
        else:
            print("[scheduler] APScheduler started — daily morning snapshot only")
        if _EOD_SETTLE_ENABLED:
            print("[scheduler] EOD bulk-settle cron jobs registered at 22:00, 23:00, 01:00, 02:00, 02:50 ET")
        if _AUTO_BACKFILL_ENABLED:
            print(
                "[scheduler] Daily multi-sport backfill scheduled "
                f"at {_AUTO_BACKFILL_HOUR_ET:02d}:{_AUTO_BACKFILL_MINUTE_ET:02d} ET "
                f"(days={_AUTO_BACKFILL_DAYS})"
            )
        print("[scheduler] Daily HuggingFace push scheduled at 04:15 ET → papylove/sportprediction")
        print(
            "[scheduler] Polymarket auto TP "
            f"enabled={bool(_poly_tp_runtime.get('enabled', _POLY_TP_ENABLED))} "
            f"target={float(_poly_tp_runtime.get('target_pct') or _POLY_TP_TARGET_PCT):.2f}% "
            f"interval={_POLY_TP_CHECK_SEC}s"
        )
        return sched
    except Exception as e:
        print(f"[scheduler] Could not start APScheduler: {e}")
        return None


def _parse_backfill_sports_csv(csv_text: str) -> list[str]:
    parts = [p.strip().lower() for p in str(csv_text or "").split(",") if p.strip()]
    return parts or [
        "nfl", "nba", "nhl", "soccer", "baseball",
        "tennis", "boxing", "mma", "golf", "motorsports", "cricket",
    ]


def _scheduled_multi_sport_backfill(force: bool = False):
    """Daily off-peak backfill for unified multi-sport history tables."""
    global _backfill_running, _last_auto_backfill_date

    run_date = _et_calendar_today().isoformat()
    with _backfill_lock:
        if _backfill_running:
            _log("[backfill-auto] skipped: previous run still active")
            return
        if not force and _last_auto_backfill_date == run_date:
            _log(f"[backfill-auto] skipped: already ran today ({run_date})")
            return
        _backfill_running = True
        _last_auto_backfill_info.update(
            {
                "running": True,
                "started_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "finished_at": None,
                "ok": None,
                "error": None,
                "days_back": _AUTO_BACKFILL_DAYS,
                "sports": _parse_backfill_sports_csv(_AUTO_BACKFILL_SPORTS),
            }
        )

    started = datetime.datetime.now()
    sports = _parse_backfill_sports_csv(_AUTO_BACKFILL_SPORTS)
    try:
        _log(
            "[backfill-auto] started "
            f"days={_AUTO_BACKFILL_DAYS} sports={','.join(sports)}"
        )
        from data.multi_sport_history import ingest_multi_sport_history

        result = ingest_multi_sport_history(days_back=_AUTO_BACKFILL_DAYS, sports=sports) or {}
        totals = result.get("totals") or {}
        elapsed = (datetime.datetime.now() - started).total_seconds()
        _last_auto_backfill_date = run_date
        with _backfill_lock:
            _last_auto_backfill_info.update(
                {
                    "running": False,
                    "last_run_date": run_date,
                    "finished_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    "ok": bool(result.get("ok", True)),
                    "error": None,
                    "elapsed_sec": round(float(elapsed), 3),
                    "totals": {
                        "games": int(totals.get("games", 0) or 0),
                        "players": int(totals.get("players", 0) or 0),
                        "injuries": int(totals.get("injuries", 0) or 0),
                    },
                }
            )
        _log(
            "[backfill-auto] complete "
            f"ok={result.get('ok', True)} "
            f"games={totals.get('games', 0)} players={totals.get('players', 0)} "
            f"injuries={totals.get('injuries', 0)} elapsed={elapsed:.1f}s"
        )
    except Exception as exc:
        elapsed = (datetime.datetime.now() - started).total_seconds()
        with _backfill_lock:
            _last_auto_backfill_info.update(
                {
                    "running": False,
                    "last_run_date": run_date,
                    "finished_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    "ok": False,
                    "error": str(exc),
                    "elapsed_sec": round(float(elapsed), 3),
                }
            )
        _log(f"[backfill-auto] failed after {elapsed:.1f}s: {exc}")
    finally:
        with _backfill_lock:
            _backfill_running = False


def _load_boot_schedule_fallback() -> bool:
    """Populate state with schedule-only cards so UI isn't blank when cache is absent."""
    try:
        today_date = _et_calendar_today()
        today_str = today_date.isoformat()
        tomorrow_str = (today_date + datetime.timedelta(days=1)).isoformat()
        if _ACTIVE_SPORT == "all":
            from data.db import get_upcoming_games

            all_games = get_upcoming_games(days_ahead=2) or []
            sport_count = len({
                _infer_sport_group(g.get("sport") or g.get("competition") or g.get("league") or "")
                for g in (all_games or [])
                if isinstance(g, dict)
            })
            if sport_count < 3 or len(all_games) < 10:
                try:
                    extra_games = _collect_fallback_games_for_all_sports(
                        today_date,
                        today_date + datetime.timedelta(days=1),
                        forecast_days=2,
                    )
                except Exception:
                    extra_games = []
                if extra_games:
                    merged: list[dict] = []
                    seen = set()
                    for g in list(all_games) + list(extra_games):
                        if not isinstance(g, dict):
                            continue
                        key = str(g.get("game_key") or "").strip() or "|".join([
                            str(g.get("sport") or g.get("competition") or ""),
                            str(g.get("match_key") or _norm_gk(f"{g.get('away_team','')}@{g.get('home_team','')}")),
                            str(g.get("game_date") or g.get("date") or ""),
                            str(g.get("game_time") or ""),
                        ])
                        if key in seen:
                            continue
                        seen.add(key)
                        merged.append(g)
                    all_games = merged

            all_bets = _build_model_fallback_bets(all_games)
            boot_player_props = _build_model_player_props_fallback(all_games, max_per_game=10)
            boot_best_bets = _multi_sport_best_bets_rows(all_bets)
            boot_props = _merge_all_sports_table_rows(boot_player_props, boot_best_bets)
            today_games = [g for g in all_games if _row_game_date(g) == today_str]
            tomorrow_games = [g for g in all_games if _row_game_date(g) == tomorrow_str]
            today_cards = [_build_card(g, all_bets, boot_player_props, "TODAY") for g in today_games]
            tomorrow_cards = [_build_card(g, all_bets, boot_player_props, "TOMORROW") for g in tomorrow_games]
        elif _ACTIVE_SPORT == "soccer":
            from data.soccer_fetcher import get_matches_today_all, get_matches_tomorrow_all

            today_games = get_matches_today_all() or []
            tomorrow_games = get_matches_tomorrow_all() or []
            today_cards = [_build_card(g, [], [], "TODAY") for g in today_games]
            tomorrow_cards = [_build_card(g, [], [], "TOMORROW") for g in tomorrow_games]
        else:
            from data.mlb_fetcher import get_schedule_range

            all_games = get_schedule_range(days_ahead=2) or []
            today_games = [g for g in all_games if g.get("date", "") == today_str]
            tomorrow_games = [g for g in all_games if g.get("date", "") == tomorrow_str]
            today_cards = [_build_card(g, [], [], "TODAY") for g in today_games]
            tomorrow_cards = [_build_card(g, [], [], "TOMORROW") for g in tomorrow_games]

        today_cards, tomorrow_cards = _normalize_dashboard_card_buckets(today_cards, tomorrow_cards)

        with _lock:
            _state.update({
                "status": "idle",
                "game_cards_today": today_cards,
                "game_cards_tomorrow": tomorrow_cards,
                "best_parlays": [],
                "player_props": _clean(boot_props) if _ACTIVE_SPORT == "all" else [],
                "last_updated": datetime.datetime.now().strftime("%Y-%m-%d %H:%M"),
            })

        print(f"[boot] Loaded schedule fallback ({len(today_cards)} today, {len(tomorrow_cards)} tomorrow)")
        return True
    except Exception as exc:
        print(f"[boot] Schedule fallback load failed: {exc}")
        return False


def _auto_boot_analysis():
    """On startup: load today's DB snapshot, or generate one if today's snapshot is missing/stale."""
    if _ACTIVE_SPORT == "all":
        # Try to restore from DB cache first so dashboard isn't blank after a crash
        restored_cache = False
        try:
            from data.db import get_analysis_cache
            today_str    = _et_calendar_today().isoformat()
            tomorrow_str = (_et_calendar_today() + datetime.timedelta(days=1)).isoformat()
            cached = get_analysis_cache(max_age_hours=22)
            if cached:
                raw_today    = cached.get("game_cards_today", [])
                raw_tomorrow = cached.get("game_cards_tomorrow", [])
                today_dates    = {c.get("game_date") for c in raw_today    if isinstance(c, dict)}
                tomorrow_dates = {c.get("game_date") for c in raw_tomorrow if isinstance(c, dict)}
                cache_is_fresh = (
                    (not raw_today    or today_str    in today_dates)
                    and
                    (not raw_tomorrow or tomorrow_str in tomorrow_dates)
                )
                if cache_is_fresh and (raw_today or raw_tomorrow):
                    with _lock:
                        _state.update({
                            "game_cards_today":    _normalize_card_list(raw_today,    expected_date=today_str),
                            "game_cards_tomorrow": _normalize_card_list(raw_tomorrow, expected_date=tomorrow_str),
                            "best_parlays":        cached.get("best_parlays", []),
                            "player_props":        cached.get("player_props", []),
                            "last_updated":        cached.get("last_updated"),
                        })
                    restored_cache = True
                    n_today = len(_state["game_cards_today"])
                    n_tmrw  = len(_state["game_cards_tomorrow"])
                    print(f"[boot] Loaded all-sports cache — {n_today} today, {n_tmrw} tomorrow "
                          f"(last updated: {cached.get('last_updated')})")
        except Exception as _boot_cache_exc:
            print(f"[boot] All-sports cache restore error: {_boot_cache_exc}")
        # Run fresh analysis only when cache is missing/stale unless explicitly forced.
        if not restored_cache:
            _load_boot_schedule_fallback()
            threading.Thread(target=_run_analysis, daemon=True).start()
        elif _BOOT_FORCE_ANALYSIS:
            threading.Thread(target=_run_analysis, daemon=True).start()
        return
    try:
        from data.db import get_analysis_cache
        today_str    = _et_calendar_today().isoformat()
        tomorrow_str = (_et_calendar_today() + datetime.timedelta(days=1)).isoformat()
        cached = get_analysis_cache(max_age_hours=22)

        if cached:
            raw_today    = cached.get("game_cards_today", [])
            raw_tomorrow = cached.get("game_cards_tomorrow", [])

            # Validate that cached cards actually match today's calendar date
            today_dates    = {c.get("game_date") for c in raw_today    if isinstance(c, dict)}
            tomorrow_dates = {c.get("game_date") for c in raw_tomorrow if isinstance(c, dict)}
            cache_is_fresh = (
                (not raw_today    or today_str    in today_dates)
                and
                (not raw_tomorrow or tomorrow_str in tomorrow_dates)
            )

            if cache_is_fresh:
                with _lock:
                    _state.update({
                        "game_cards_today":    _normalize_card_list(raw_today,    expected_date=today_str),
                        "game_cards_tomorrow": _normalize_card_list(raw_tomorrow, expected_date=tomorrow_str),
                        "best_parlays":        cached.get("best_parlays", []),
                        "player_props":        cached.get("player_props", []),
                        "last_updated":        cached.get("last_updated"),
                    })
                n_today = len(_state["game_cards_today"])
                n_tmrw  = len(_state["game_cards_tomorrow"])
                print(f"[boot] Loaded valid today cache — {n_today} today, {n_tmrw} tomorrow "
                      f"(last updated: {cached.get('last_updated')})")
                # If cached cards are empty (no games at all), still trigger a refresh
                if n_today == 0 and n_tmrw == 0:
                    print("[boot] Cache has 0 games — triggering fresh analysis...")
                    threading.Thread(target=_run_analysis, daemon=True).start()
                elif _BOOT_FORCE_ANALYSIS:
                    threading.Thread(target=_run_analysis, daemon=True).start()
                return
            else:
                stale_dates = today_dates | tomorrow_dates
                print(f"[boot] Cache has stale game dates {stale_dates} (expected {today_str}) "
                      f"— triggering fresh analysis to replace stale data...")
        else:
            _load_boot_schedule_fallback()
            print(f"[boot] No cache for {today_str} — triggering fresh analysis...")

        # Always run fresh analysis when cache is missing or stale
        threading.Thread(target=_run_analysis, daemon=True).start()

    except Exception as e:
        print(f"[boot] Auto-boot error: {e}")
        if not _load_boot_schedule_fallback():
            threading.Thread(target=_run_analysis, daemon=True).start()


if __name__ == "__main__":
    _init_worker()

    # ── Auto-start the unified betting engine in the background ──────────────
    def _start_autobet_engine():
        import time as _time
        _time.sleep(8)  # Let Flask finish binding before engine starts importing
        try:
            import importlib.util
            autobet_path = os.path.join(SRC_DIR, "polymarket_autobet.py")
            spec = importlib.util.spec_from_file_location("polymarket_autobet", autobet_path)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            print("[autobet] Unified auto-bet engine started (Polymarket + Kalshi)")
            mod.main()
        except Exception as _e:
            print(f"[autobet] Engine failed to start: {_e}")

    threading.Thread(target=_start_autobet_engine, daemon=True, name="autobet-engine").start()
    print("[boot] Auto-bet engine thread launched — will scan Polymarket + Kalshi every 5 min")

    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
