# Bettor HF-First Zero-Cost Pipeline

Production-ready sports prediction stack using Hugging Face for dataset/model storage, HF Space API inference, and a Vercel dashboard.

## Cost Plan ($0 target)

| Component | Tool | Cost |
|---|---|---|
| Dataset storage | HF Dataset Hub | Free (up to 5GB) |
| Model storage | HF Model Hub | Free |
| Training GPU | HF Spaces T4 | Free tier |
| Inference API | HF Spaces FastAPI | Free tier minutes |
| Data sources | MLB Stats API, NHL API, football-data.org, TheSportsDB, balldontlie, Jeff Sackmann, Polymarket Gamma, NewsData.io/GDELT/Google News RSS | Free |

## Architecture

```text
One-time: bootstrap 1 year history -> HF Dataset Hub
Daily: append new results -> same HF Dataset
Daily: collect multi-sport news signals (team/player/game impact) -> same HF Dataset
Daily: retrain best model -> HF Model Hub
Daily: retrain news-impact text classifier -> HF Model Hub
Anytime: call HF Space FastAPI inference endpoint
Dashboard: Vercel Flask UI -> proxies HF Space API or local HF artifacts
Execution: Polymarket (Kalshi removed from active flow)
```

## Quick start

1. Install dependencies:
   ```powershell
   pip install -r requirements.txt
   ```
2. Copy `.env.example` to `.env` and set at least:
   - `HF_API_KEY`
   - `HF_DATASET_REPO`
   - `HF_MODEL_REPO`
   - `FOOTBALL_DATA_API_KEY`
   - `NEWSDATA_API_KEY` (recommended for richer player/team news coverage)
   - Polymarket credentials
3. Run pipeline:
   ```powershell
   python src\betting_bot.py --hf-bootstrap --hf-days-back 365
   python src\betting_bot.py --hf-daily-run --hf-attach-markets
   ```
4. Start dashboard:
   ```powershell
   python src\dashboard.py
   ```

## Automation (without GitHub Actions dependency)

- Use HF Space FastAPI app (`hf_space_api/app.py`) with startup autorun + daily schedule.
- Set `HF_SPACE_API_URL` in Vercel to your deployed Space endpoint.
- Use `HF_ACTIVE_SCAN_MINUTES` (default `30`) to continuously refresh append/predict cycles.
- Use `HF_ACTIVE_APPEND_DAYS` (default `3`) to keep recent results synced for all supported sports.
- Use `HF_RETRAIN_INTERVAL_MINUTES` (default `180`) to auto-retrain frequently without waiting for daily cron.
- If `HF_SPACE_API_URL` is omitted, dashboard auto-discovers Space URL from `HF_SPACE_REPO` (or `HF_MODEL_REPO` / `HF_DATASET_REPO` when in `owner/repo` format).
- If Space is unavailable, dashboard falls back to HF model-repo artifacts at `artifacts/hf_daily_predictions.json` and `artifacts/hf_pipeline_status.json`.

## Recommended free data providers by sport

| Sport | Primary free source | Fallback free source | Status in pipeline |
|---|---|---|---|
| MLB | MLB Stats API (`statsapi.mlb.com`) | Polymarket market schedule extraction | Enabled |
| NHL | NHL API (`api-web.nhle.com`) | Polymarket market schedule extraction | Enabled |
| NBA | balldontlie (free tier) | Polymarket market schedule extraction | Enabled |
| Soccer | football-data.org (free key) | TheSportsDB public key `1` | Enabled |
| Tennis | Jeff Sackmann historical CSVs | Polymarket market schedule extraction | Enabled |
| Golf | Polymarket market schedule extraction | TheSportsDB events | Partial (upcoming via Polymarket) |
| MMA/Boxing/Cricket | Polymarket market schedule extraction | TheSportsDB events | Partial (upcoming via Polymarket) |

## Dashboard API

The dashboard proxies provider endpoints:

- `GET /api/predictions/status`
- `GET /api/predictions/today`
- `GET /api/predictions/tomorrow`
- `GET /api/model/stats`
- `GET /api/polymarket/status`
- `GET /api/polymarket/submissions`
- `GET /api/polymarket/positions`

## Notes

- Polymarket defaults to dry-run mode until `POLYMARKET_DRY_RUN=false`.
- HF artifacts are written to `data/hf_pipeline_status.json`, `data/hf_daily_predictions.json`, and `data/training_history.json`.
