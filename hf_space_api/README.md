---
title: Bettor HF Auto Pipeline API
emoji: ⚽
colorFrom: blue
colorTo: indigo
sdk: docker
pinned: false
license: mit
---

# Bettor HF Auto Pipeline API (Space Entrypoint)

This Space runs the bettor Hugging Face pipeline automatically:

1. Bootstrap historical data to **HF Dataset Hub** (first run only)
2. Append daily results
3. Retrain and publish the latest model to **HF Model Hub**
4. Generate daily predictions
5. Expose API endpoints for dashboard + Kalshi status

## Required Space files (exact)

- `README.md` (this file with YAML frontmatter)
- `Dockerfile`
- `app.py` (FastAPI entrypoint, must expose `app`)
- `requirements.txt`

## Required Space Secrets / Variables

- `HF_API_KEY` (write token)
- `HF_DATASET_REPO` (ex: `yourname/sportprediction`)
- `HF_MODEL_REPO` (ex: `yourname/sports-win-model`)
- `FOOTBALL_DATA_API_KEY`
- `KALSHI_API_KEY`
- `KALSHI_PRIVATE_KEY` (or `KALSHI_PRIVATE_KEY_FILE`)

Recommended:

- `HF_AUTORUN_ON_STARTUP=1`
- `HF_BOOTSTRAP_ON_EMPTY=1`
- `HF_BOOTSTRAP_DAYS=365`
- `HF_DAILY_RUN_HOUR_ET=4`
- `HF_DAILY_RUN_MINUTE_ET=15`
- `HF_ACTIVE_SCAN_MINUTES=30`
- `HF_DAILY_CUSTOM_MODEL=auto`
- `HF_DAILY_MIN_TRAIN_ROWS=200`
- `KALSHI_LIVE_TRADING_ENABLED=0`

## API endpoints

- `GET /health`
- `GET /status`
- `GET /predictions/today`
- `GET /predictions/tomorrow`
- `GET /model/stats`
- `GET /kalshi/submissions`
- `GET /kalshi/positions`
- `GET /kalshi/live`
- `POST /kalshi/place-from-predictions`
- `POST /run/bootstrap?days_back=365`
- `POST /run/daily`
- `POST /run/active`

## Notes

- Active scheduling is done inside `app.py` via APScheduler interval job.
- Full retrain scheduling is done daily via cron job.
- Startup autorun is enabled by default and will create/push dataset + model artifacts when empty.
- Use this Space URL as `HF_SPACE_API_URL` in Vercel so dashboard reads live predictions.
