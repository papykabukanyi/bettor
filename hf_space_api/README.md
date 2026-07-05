---
title: Bettor HF Auto Pipeline API
emoji: "🚀"
colorFrom: blue
colorTo: purple
sdk: docker
pinned: false
license: mit
---

# Bettor HF Control Hub

One Space. Full cycle automation:

- ingest fresh sports data
- keep HF dataset updated
- retrain and publish best model
- generate daily predictions
- expose API for dashboard + Kalshi execution status

## What runs automatically

1. **Bootstrap (first run)** -> historical load to HF Dataset
2. **Active cycle (interval)** -> append recent outcomes + refresh predictions
3. **Daily cycle (cron)** -> full retrain + publish + predict

## Required secrets

- `HF_API_KEY`
- `HF_DATASET_REPO`
- `HF_MODEL_REPO`
- `FOOTBALL_DATA_API_KEY`
- `KALSHI_API_KEY`
- `KALSHI_PRIVATE_KEY` (or `KALSHI_PRIVATE_KEY_FILE`)

## Recommended runtime config

- `HF_AUTORUN_ON_STARTUP=1`
- `HF_BOOTSTRAP_ON_EMPTY=1`
- `HF_BOOTSTRAP_DAYS=365`
- `HF_ACTIVE_SCAN_MINUTES=30`
- `HF_RETRAIN_INTERVAL_MINUTES=180`
- `HF_DAILY_RUN_HOUR_ET=4`
- `HF_DAILY_RUN_MINUTE_ET=15`
- `HF_DAILY_CUSTOM_MODEL=auto`
- `HF_DAILY_MIN_TRAIN_ROWS=200`
- `HF_ATTACH_KALSHI=1`

## API surface

- `GET /health`
- `GET /status`
- `GET /predictions/today`
- `GET /predictions/tomorrow`
- `GET /model/stats`
- `GET /kalshi/submissions`
- `GET /kalshi/positions`
- `POST /run/bootstrap?days_back=365`
- `POST /run/daily`
- `POST /run/active`

## Deploy notes

- `app.py` is the FastAPI entrypoint.
- Scheduler is APScheduler in-process.
- Point dashboard to this Space using `HF_SPACE_API_URL`.
