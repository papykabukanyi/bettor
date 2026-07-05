from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
from collections import defaultdict
from pathlib import Path
from typing import Any

from data.hf_pipeline import HFDirectPipeline
from data.hf_uploader import HFUploader
from data.kalshi_trade_api import submit_prediction_orders

ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT_DIR / "data"
PREDICTIONS_FILE = DATA_DIR / "hf_daily_predictions.json"
SCHEDULE_CACHE_FILE = DATA_DIR / "pregame_schedule.json"
ANALYSIS_LEAD_MINUTES = max(15, int(os.getenv("PREGAME_ANALYSIS_LEAD_MINUTES", "90") or "90"))
BET_LEAD_MINUTES = max(5, int(os.getenv("PREGAME_BET_LEAD_MINUTES", "60") or "60"))
TIMING_MINUTES = max(1, int(os.getenv("PREGAME_TIMING_MINUTES", "5") or "5"))
MIN_CONFIDENCE = max(0, int(os.getenv("AUTOBET_MIN_CONFIDENCE", "52") or "52"))
_UPLOADER: HFUploader | None = None
_UPLOADER_FAILED = False


def _now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def _env_flag(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name, "1" if default else "0") or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _parse_dt(game_date: str, game_time: str) -> dt.datetime | None:
    raw = str(game_time or "").strip()
    date_raw = str(game_date or "").strip()
    if raw:
        candidate = raw.replace("Z", "+00:00")
        try:
            parsed = dt.datetime.fromisoformat(candidate)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=dt.timezone.utc)
            return parsed
        except Exception:
            pass
    if date_raw:
        try:
            parsed_date = dt.date.fromisoformat(date_raw[:10])
            return dt.datetime(parsed_date.year, parsed_date.month, parsed_date.day, tzinfo=dt.timezone.utc)
        except Exception:
            return None
    return None


def _confidence_tier(value: float) -> str:
    if value >= 0.70:
        return "elite"
    if value >= 0.60:
        return "solid"
    if value >= 0.55:
        return "lean"
    return "watch"


def _schedule_uid(row: dict[str, Any]) -> str:
    raw = "|".join(
        [
            str(row.get("game_key") or row.get("game_id") or ""),
            str(row.get("game_date") or ""),
            str(row.get("sport") or ""),
            str(row.get("home_team") or ""),
            str(row.get("away_team") or ""),
        ]
    )
    return "sched_" + hashlib.sha1(raw.encode("utf-8")).hexdigest()[:20]


def _load_json(path: Path, default: Any) -> Any:
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:
        return default


def _save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


def _load_predictions_payload() -> dict[str, Any]:
    if PREDICTIONS_FILE.exists():
        payload = _load_json(PREDICTIONS_FILE, {})
        if isinstance(payload, dict) and isinstance(payload.get("predictions"), list):
            return payload
    return {"predictions": [], "source": "cache"}


def _load_schedule_cache() -> dict[str, Any]:
    payload = _load_json(SCHEDULE_CACHE_FILE, {"updated_at": "", "rows": []})
    if not isinstance(payload, dict):
        return {"updated_at": "", "rows": []}
    rows = payload.get("rows")
    if not isinstance(rows, list):
        payload["rows"] = []
    return payload


def _save_schedule_cache(rows: list[dict[str, Any]]) -> None:
    _save_json(
        SCHEDULE_CACHE_FILE,
        {
            "updated_at": _now_utc().isoformat(),
            "rows": rows,
        },
    )


def _get_uploader() -> HFUploader | None:
    global _UPLOADER, _UPLOADER_FAILED
    if _UPLOADER is None and not _UPLOADER_FAILED:
        uploader = HFUploader()
        if getattr(uploader, "_ok", False):
            _UPLOADER = uploader
        else:
            _UPLOADER_FAILED = True
    return _UPLOADER


def _push_schedule_rows(rows: list[dict[str, Any]]) -> bool:
    if not rows:
        return True
    uploader = _get_uploader()
    if uploader is None:
        return False
    uploader.push_records("pregame_schedule", rows)
    return uploader.flush_all()


def _group_predictions(predictions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    now = _now_utc()
    for row in predictions:
        if not isinstance(row, dict) or row.get("error"):
            continue
        game_key = str(row.get("game_key") or row.get("game_id") or "").strip()
        if not game_key:
            game_key = "|".join(
                [
                    str(row.get("sport") or ""),
                    str(row.get("game_date") or ""),
                    str(row.get("home_team") or ""),
                    str(row.get("away_team") or ""),
                ]
            )
        grouped[game_key].append(row)

    jobs: list[dict[str, Any]] = []
    for game_key, rows in grouped.items():
        rows = [r for r in rows if isinstance(r, dict)]
        if not rows:
            continue
        rows.sort(
            key=lambda r: max(
                float(r.get("confidence") or 0.0),
                float(r.get("model_prob") or 0.0),
            ),
            reverse=True,
        )
        best = rows[0]
        scheduled_start = _parse_dt(str(best.get("game_date") or ""), str(best.get("scheduled_start") or best.get("game_time") or ""))
        if scheduled_start is None:
            continue
        if scheduled_start < now - dt.timedelta(minutes=10):
            continue
        confidence = max(
            float(best.get("confidence") or 0.0),
            float(best.get("model_prob") or 0.0),
        )
        jobs.append(
            {
                "schedule_uid": _schedule_uid(best),
                "game_key": game_key,
                "sport": str(best.get("sport") or "").strip().lower() or "mlb",
                "league": str(best.get("league") or "").strip(),
                "home_team": str(best.get("home_team") or "").strip(),
                "away_team": str(best.get("away_team") or "").strip(),
                "game_date": str(best.get("game_date") or "")[:10],
                "game_time": str(best.get("game_time") or best.get("scheduled_start") or "").strip(),
                "scheduled_start": scheduled_start.isoformat(),
                "analysis_at": (scheduled_start - dt.timedelta(minutes=ANALYSIS_LEAD_MINUTES)).isoformat(),
                "bet_at": (scheduled_start - dt.timedelta(minutes=BET_LEAD_MINUTES)).isoformat(),
                "confidence": round(confidence, 4),
                "confidence_tier": str(best.get("confidence_tier") or _confidence_tier(confidence)),
                "model_version": str(best.get("model_version") or "").strip(),
                "model_type": str(best.get("model_type") or best.get("model_name") or "").strip(),
                "prediction_count": len(rows),
                "predictions_json": rows,
                "analysis_state": "queued",
                "bet_state": "queued",
                "analysis_payload": {},
                "bet_payload": {},
                "last_analysis_at": "",
                "last_bet_at": "",
                "source": "hf_predictions",
                "created_at": now.isoformat(),
            }
        )
    return jobs


def _merge_schedule_rows(existing_rows: list[dict[str, Any]], new_rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    by_uid = {str(row.get("schedule_uid") or ""): dict(row) for row in existing_rows if isinstance(row, dict)}
    changed: list[dict[str, Any]] = []
    for row in new_rows:
        uid = str(row.get("schedule_uid") or "").strip()
        if not uid:
            continue
        prev = by_uid.get(uid, {})
        merged = dict(prev)
        merged.update(row)
        merged.setdefault("created_at", prev.get("created_at") or row.get("created_at") or _now_utc().isoformat())
        comparable_prev = {k: v for k, v in prev.items() if k != "updated_at"}
        comparable_merged = {k: v for k, v in merged.items() if k != "updated_at"}
        by_uid[uid] = merged
        if comparable_merged != comparable_prev:
            merged["updated_at"] = _now_utc().isoformat()
            changed.append(merged)
    rows = sorted(by_uid.values(), key=lambda r: str(r.get("scheduled_start") or ""))
    return rows, changed


def sync_pregame_schedule(predictions_payload: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = predictions_payload or _load_predictions_payload()
    predictions = payload.get("predictions") or []
    jobs = _group_predictions([row for row in predictions if isinstance(row, dict)])
    cache = _load_schedule_cache()
    existing_rows = cache.get("rows") or []
    merged_rows, changed_rows = _merge_schedule_rows(existing_rows, jobs)
    _save_schedule_cache(merged_rows)
    pushed = _push_schedule_rows(changed_rows)
    return {
        "ok": True,
        "jobs": len(jobs),
        "saved": len(changed_rows),
        "source": payload.get("source") or "cache",
        "pushed": pushed,
    }


def _refresh_analysis_for_row(pipeline: HFDirectPipeline, row: dict[str, Any]) -> dict[str, Any]:
    game_date = str(row.get("game_date") or "")[:10]
    try:
        pipeline.collect_news_signals(days=[dt.date.fromisoformat(game_date)])
    except Exception as exc:
        return {"ok": False, "error": str(exc), "news_collected": False}

    try:
        model_snapshot = pipeline.predict_from_model_repo(
            home_team=str(row.get("home_team") or ""),
            away_team=str(row.get("away_team") or ""),
            sport=str(row.get("sport") or "mlb"),
            season=int(game_date[:4]),
        )
    except Exception as exc:
        model_snapshot = {"ok": False, "error": str(exc)}

    return {
        "ok": True,
        "news_collected": True,
        "analysis_window_minutes": ANALYSIS_LEAD_MINUTES,
        "refreshed_at": _now_utc().isoformat(),
        "model": model_snapshot,
    }


def _analysis_due(rows: list[dict[str, Any]], now: dt.datetime) -> list[dict[str, Any]]:
    due: list[dict[str, Any]] = []
    for row in rows:
        if str(row.get("analysis_state") or "").lower() in {"done", "skipped"}:
            continue
        analysis_at = _parse_dt(str(row.get("game_date") or ""), str(row.get("analysis_at") or ""))
        if analysis_at and analysis_at <= now:
            due.append(row)
    return due


def _bet_due(rows: list[dict[str, Any]], now: dt.datetime) -> list[dict[str, Any]]:
    due: list[dict[str, Any]] = []
    for row in rows:
        if str(row.get("bet_state") or "").lower() in {"done", "skipped"}:
            continue
        bet_at = _parse_dt(str(row.get("game_date") or ""), str(row.get("bet_at") or ""))
        if bet_at and bet_at <= now:
            due.append(row)
    return due


def _updated_row(row: dict[str, Any], **fields: Any) -> dict[str, Any]:
    updated = dict(row)
    for key, value in fields.items():
        updated[key] = value
    updated["updated_at"] = _now_utc().isoformat()
    return updated


def _place_due_games(rows: list[dict[str, Any]], *, dry_run: bool, stake_usd: float, max_single_orders: int, max_combo_orders: int, include_combos: bool) -> dict[str, Any]:
    placed = 0
    skipped = 0
    failed = 0
    submitted: list[dict[str, Any]] = []
    cache = _load_schedule_cache()
    current_rows = cache.get("rows") or []
    by_uid = {str(row.get("schedule_uid") or ""): dict(row) for row in current_rows if isinstance(row, dict)}
    changed_rows: list[dict[str, Any]] = []

    for row in rows:
        schedule_uid = str(row.get("schedule_uid") or "").strip()
        predictions = row.get("predictions_json") or []
        if not isinstance(predictions, list):
            predictions = []
        confidence = float(row.get("confidence") or 0.0)
        tier = str(row.get("confidence_tier") or _confidence_tier(confidence))
        if confidence * 100 < MIN_CONFIDENCE or tier not in {"elite", "solid"}:
            updated = _updated_row(
                row,
                bet_state="skipped",
                last_bet_at=_now_utc().isoformat(),
                bet_payload={"ok": True, "skipped": True, "reason": "below_threshold", "confidence": confidence, "confidence_tier": tier},
            )
            by_uid[schedule_uid] = updated
            changed_rows.append(updated)
            skipped += 1
            continue
        try:
            result = submit_prediction_orders(
                {"predictions": predictions},
                stake_usd=max(0.01, float(stake_usd or 1.0)),
                max_orders=max(1, int(max_single_orders or 1)),
                dry_run=dry_run,
                include_combos=include_combos,
                max_combos=max(0, int(max_combo_orders or 0)),
            )
            ok = bool(result.get("ok", True))
            updated = _updated_row(
                row,
                bet_state="done" if ok else "failed",
                last_bet_at=_now_utc().isoformat(),
                bet_payload=result,
            )
            by_uid[schedule_uid] = updated
            changed_rows.append(updated)
            if ok:
                placed += 1
                submitted.append(result)
            else:
                failed += 1
        except Exception as exc:
            updated = _updated_row(
                row,
                bet_state="failed",
                last_bet_at=_now_utc().isoformat(),
                bet_payload={"ok": False, "error": str(exc)},
            )
            by_uid[schedule_uid] = updated
            changed_rows.append(updated)
            failed += 1

    merged_rows = sorted(by_uid.values(), key=lambda r: str(r.get("scheduled_start") or ""))
    _save_schedule_cache(merged_rows)
    _push_schedule_rows(changed_rows)
    return {"ok": True, "placed": placed, "skipped": skipped, "failed": failed, "submitted": submitted}


def run_pregame_timing_cycle(*, dry_run: bool | None = None, stake_usd: float | None = None, max_single_orders: int | None = None, max_combo_orders: int | None = None, include_combos: bool | None = None) -> dict[str, Any]:
    sync = sync_pregame_schedule()
    cache = _load_schedule_cache()
    rows = cache.get("rows") or []
    now = _now_utc()
    if not rows:
        return {"ok": True, "synced": sync, "due_rows": 0, "analysis": {"ok": True, "analysis_rows": 0, "refreshed": 0}, "bets": {"ok": True, "placed": 0, "skipped": 0, "failed": 0, "submitted": []}}

    pipeline = HFDirectPipeline()
    live_enabled = _env_flag("KALSHI_LIVE_TRADING_ENABLED", default=False)
    env_dry_run = _env_flag("AUTOBET_DRY_RUN", default=True)
    effective_dry_run = env_dry_run if dry_run is None else bool(dry_run)
    if not live_enabled:
        effective_dry_run = True

    analysis_rows = _analysis_due(rows, now)
    bet_rows = _bet_due(rows, now)

    by_uid = {str(row.get("schedule_uid") or ""): dict(row) for row in rows if isinstance(row, dict)}
    analysis_changed_rows: list[dict[str, Any]] = []

    analysis_count = 0
    if analysis_rows and pipeline.ok:
        for row in analysis_rows:
            schedule_uid = str(row.get("schedule_uid") or "")
            analysis_payload = _refresh_analysis_for_row(pipeline, row)
            updated = _updated_row(
                row,
                analysis_state="done" if analysis_payload.get("ok") else "failed",
                last_analysis_at=_now_utc().isoformat(),
                analysis_payload=analysis_payload,
            )
            if analysis_payload.get("model", {}).get("home_win_prob") is not None:
                model = analysis_payload["model"]
                conf = max(float(model.get("home_win_prob") or 0.0), float(model.get("away_win_prob") or 0.0))
                updated["confidence"] = round(conf, 4)
                updated["confidence_tier"] = _confidence_tier(conf)
                updated["model_version"] = str(model.get("model_version") or updated.get("model_version") or "")
                updated["model_type"] = str(model.get("model_type") or updated.get("model_type") or "")
            by_uid[schedule_uid] = updated
            analysis_changed_rows.append(updated)
            analysis_count += 1
    elif analysis_rows:
        for row in analysis_rows:
            schedule_uid = str(row.get("schedule_uid") or "")
            updated = _updated_row(
                row,
                analysis_state="skipped",
                last_analysis_at=_now_utc().isoformat(),
                analysis_payload={"ok": False, "reason": "hf_pipeline_unavailable"},
            )
            by_uid[schedule_uid] = updated
            analysis_changed_rows.append(updated)
            analysis_count += 1

    if analysis_changed_rows:
        merged_rows = sorted(by_uid.values(), key=lambda r: str(r.get("scheduled_start") or ""))
        _save_schedule_cache(merged_rows)
        _push_schedule_rows(analysis_changed_rows)

    bet_result = _place_due_games(
        [by_uid[str(row.get("schedule_uid") or "")] for row in bet_rows if str(row.get("schedule_uid") or "") in by_uid],
        dry_run=effective_dry_run,
        stake_usd=float(stake_usd if stake_usd is not None else os.getenv("KALSHI_AUTOBET_STAKE_USD", "1.0") or 1.0),
        max_single_orders=int(max_single_orders if max_single_orders is not None else os.getenv("KALSHI_AUTOBET_MAX_SINGLE_ORDERS", "1") or 1),
        max_combo_orders=int(max_combo_orders if max_combo_orders is not None else os.getenv("KALSHI_AUTOBET_MAX_COMBO_ORDERS", "1") or 1),
        include_combos=bool(include_combos if include_combos is not None else _env_flag("KALSHI_AUTO_CREATE_COMBOS", default=True)),
    )

    return {
        "ok": True,
        "synced": sync,
        "due_rows": len(analysis_rows) + len(bet_rows),
        "analysis": {"ok": True, "analysis_rows": len(analysis_rows), "refreshed": analysis_count},
        "bets": bet_result,
        "timing": {
            "analysis_lead_minutes": ANALYSIS_LEAD_MINUTES,
            "bet_lead_minutes": BET_LEAD_MINUTES,
            "interval_minutes": TIMING_MINUTES,
        },
    }
