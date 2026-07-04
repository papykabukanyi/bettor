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
5. Expose API endpoints for dashboard + Polymarket status

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
- `POLYMARKET_KEY_ID`
- `POLYMARKET_SECRET_KEY`

Recommended:

- `HF_AUTORUN_ON_STARTUP=1`
- `HF_BOOTSTRAP_ON_EMPTY=1`
- `HF_BOOTSTRAP_DAYS=365`
- `HF_DAILY_RUN_HOUR_ET=4`
- `HF_DAILY_RUN_MINUTE_ET=15`
- `HF_DAILY_CUSTOM_MODEL=auto`
- `HF_DAILY_MIN_TRAIN_ROWS=200`
- `POLYMARKET_DRY_RUN=true`

## API endpoints

- `GET /health`
- `GET /status`
- `GET /predictions/today`
- `GET /predictions/tomorrow`
- `GET /model/stats`
- `GET /polymarket/submissions`
- `GET /polymarket/positions`
- `POST /run/bootstrap?days_back=365`
- `POST /run/daily`

## Notes

- Daily scheduling is done inside `app.py` via APScheduler.
- Startup autorun is enabled by default and will create/push dataset + model artifacts when empty.
- Use this Space URL as `HF_SPACE_API_URL` in Vercel so dashboard reads live predictions.
