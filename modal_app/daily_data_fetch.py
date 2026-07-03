from __future__ import annotations

import datetime as dt
import json
import os

from modal_app import common

app = common.make_app("bettor-daily-data-fetch")


def run_daily_data_fetch(*, bootstrap_days: int | None = None, backfill_days: int | None = None) -> dict:
    common.ensure_directories()
    today = common.today_et()
    existing_records = common.load_history_records()
    bootstrap_window = max(30, int(bootstrap_days or os.getenv("MODAL_BOOTSTRAP_DAYS", "365") or "365"))
    daily_backfill = max(1, int(backfill_days or os.getenv("MODAL_DAILY_BACKFILL_DAYS", "3") or "3"))

    if existing_records:
        existing_dates = sorted({str(row.get("game_date") or "") for row in existing_records if row.get("game_date")})
        latest_date = existing_dates[-1] if existing_dates else today.isoformat()
        latest_dt = dt.date.fromisoformat(latest_date)
        start_date = min(latest_dt, today) - dt.timedelta(days=daily_backfill)
    else:
        start_date = today - dt.timedelta(days=bootstrap_window)
    end_date = today

    completed_games = common.fetch_completed_games(start_date, end_date)
    merged_history = common.dedupe_records([*existing_records, *completed_games])
    common.save_history_records(merged_history)

    tomorrow = today + dt.timedelta(days=1)
    schedule_today = common.fetch_upcoming_games(today)
    schedule_tomorrow = common.fetch_upcoming_games(tomorrow)
    schedule_payload = {
        "updated_at": common.now_utc_iso(),
        "today": today.isoformat(),
        "tomorrow": tomorrow.isoformat(),
        "today_games": schedule_today,
        "tomorrow_games": schedule_tomorrow,
        "counts": {
            "today": common.sport_counts(schedule_today),
            "tomorrow": common.sport_counts(schedule_tomorrow),
        },
    }
    common.save_schedule_payload(schedule_payload)

    summary = {
        "ok": True,
        "started_at": common.now_utc_iso(),
        "fetched_window": {"start": start_date.isoformat(), "end": end_date.isoformat()},
        "completed_games_added": max(0, len(merged_history) - len(existing_records)),
        "completed_games_total": len(merged_history),
        "completed_sport_counts": common.sport_counts(completed_games),
        "today_schedule_count": len(schedule_today),
        "tomorrow_schedule_count": len(schedule_tomorrow),
        "schedule_counts": schedule_payload["counts"],
        "updated_at": common.now_utc_iso(),
    }
    common.save_json(common.path_for("pipeline", "fetch_summary.json"), summary)
    common.update_pipeline_status("fetch", summary)
    return summary


@app.function(
    image=common.image,
    volumes={common.REMOTE_VOLUME_MOUNT: common.volume},
    secrets=common.modal_secrets(),
    schedule=common.modal.Cron("0 2 * * *", timezone="America/New_York"),
    timeout=60 * 30,
)
def daily_data_fetch() -> dict:
    result = run_daily_data_fetch()
    common.commit_volume()
    return result


@app.local_entrypoint()
def main(bootstrap_days: int = 365, backfill_days: int = 3):
    print(json.dumps(run_daily_data_fetch(bootstrap_days=bootstrap_days, backfill_days=backfill_days), indent=2))
