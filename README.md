# Bettor HF-First Zero-Cost Pipeline

Production-ready sports prediction stack using Hugging Face for dataset/model storage, HF Space API inference, a Render backend, and a Cloudflare Pages dashboard.

## Cost Plan ($0 target)

| Component | Tool | Cost |
|---|---|---|
| Dataset storage | HF Dataset Hub | Free (up to 5GB) |
| Model storage | HF Model Hub | Free |
| Training GPU | HF Spaces T4 | Free tier |
| Inference API | HF Spaces FastAPI | Free tier minutes |
| Data sources | MLB Stats API, NHL API, football-data.org, TheSportsDB, balldontlie, Jeff Sackmann, Kalshi, NewsData.io/GDELT/Google News RSS | Free |

## Architecture

```text
One-time: bootstrap 1 year history -> HF Dataset Hub
Daily: append new results -> same HF Dataset
Daily: collect multi-sport news signals (team/player/game impact) -> same HF Dataset
Daily: retrain best model -> HF Model Hub
Daily: retrain news-impact text classifier -> HF Model Hub
Anytime: call HF Space FastAPI inference endpoint
Backend: Render Flask UI -> proxies HF Space API or local HF artifacts
Frontend: Cloudflare Pages static dashboard -> proxies /api/* to Render
Execution: Kalshi (single + combo order routing)
```

## Quick start

1. Install dependencies:
   ```powershell
   pip install -r requirements.txt
   ```
2. Copy `.env.example` to `.env` and set at least:
   - `HF_API_KEY` → Your HF write token (generate at https://huggingface.co/settings/tokens)
   - `HF_DATASET_REPO` → Your HF dataset repo (e.g., `papylove/sportprediction`)
   - `HF_MODEL_REPO` → Your HF model repo (same or different, e.g., `papylove/sportprediction`)
   - `FOOTBALL_DATA_API_KEY` → Free key from https://www.football-data.org/
   - `NEWSDATA_API_KEY` → Free key from https://newsdata.io/ (recommended for richer player/team news)
   - Kalshi credentials (if running auto-bets); paste `KALSHI_PRIVATE_KEY` as a single line with `\n` escapes
       - `PREGAME_ANALYSIS_LEAD_MINUTES=90`, `PREGAME_BET_LEAD_MINUTES=60`, `PREGAME_TIMING_MINUTES=5`
3. Run pipeline:
   ```powershell
   python src\betting_bot.py --hf-bootstrap --hf-days-back 365
   python src\betting_bot.py --hf-daily-run --hf-attach-markets
   ```
4. Start dashboard:
   ```powershell
   python src\dashboard.py
   ```

## Cloudflare Pages frontend

- Build the static dashboard with `python scripts/build_cloudflare_pages.py`.
- Deploy the `public/` output to Cloudflare Pages.
- Preferred deploy command: `npm run deploy:pages`
- The script builds `public/` first, then uploads it with Wrangler.
- If the Cloudflare project is still using a custom deploy command, switch it to the Pages deploy script above.
- Set `BACKEND_BASE_URL` in Cloudflare Pages to your Render backend URL.
- Pages proxies `/api/*` to the backend through a Pages Function.

## Automation (without GitHub Actions dependency)

- Use HF Space FastAPI app (`hf_space_api/app.py`) with startup autorun + daily schedule.
- Set `HF_SPACE_API_URL` in your Render environment to your deployed Space endpoint.
- Use `HF_ACTIVE_SCAN_MINUTES` (default `30`) to continuously refresh append/predict cycles.
- Use `HF_ACTIVE_APPEND_DAYS` (default `3`) to keep recent results synced for all supported sports.
- Use `HF_RETRAIN_INTERVAL_MINUTES` (default `180`) to auto-retrain frequently without waiting for daily cron.
- If `HF_SPACE_API_URL` is omitted, dashboard auto-discovers Space URL from `HF_SPACE_REPO` (or `HF_MODEL_REPO` / `HF_DATASET_REPO` when in `owner/repo` format).
- If Space is unavailable, dashboard falls back to HF model-repo artifacts at `artifacts/hf_daily_predictions.json` and `artifacts/hf_pipeline_status.json`.

## Recommended free data providers by sport

| Sport | Primary free source | Fallback free source | Status in pipeline |
|---|---|---|---|
| MLB | MLB Stats API (`statsapi.mlb.com`) | Kalshi market schedule extraction | Enabled |
| NHL | NHL API (`api-web.nhle.com`) | Kalshi market schedule extraction | Enabled |
| NBA | balldontlie (free tier) | Kalshi market schedule extraction | Enabled |
| Soccer | football-data.org (free key) | TheSportsDB public key `1` | Enabled |
| Tennis | Jeff Sackmann historical CSVs | Kalshi market schedule extraction | Enabled |
| Golf | Kalshi market schedule extraction | TheSportsDB events | Partial (upcoming via Kalshi) |
| MMA/Boxing/Cricket | Kalshi market schedule extraction | TheSportsDB events | Partial (upcoming via Kalshi) |

## Soccer Data: football-data.org Integration

The pipeline automatically collects comprehensive soccer data from **football-data.org** and pushes it to your HF dataset.

### Supported Competitions

The pipeline fetches data from these football-data.org competition codes:

**Europe (Top 5 Leagues + Cups):**
- `PL` - English Premier League
- `PD` - La Liga (Spain)
- `SA` - Serie A (Italy)
- `BL1` - Bundesliga (Germany)
- `FL1` - Ligue 1 (France)
- `PPL` - Portuguese Liga
- `ELC` - English Championship (League 2)
- `DED` - Dutch Eredivisie
- `CL` - UEFA Champions League
- `EL` - UEFA Europa League

**Americas:**
- `MLS` - Major League Soccer (US/Canada)
- `CLI` - Copa Libertadores

**Global Tournaments:**
- `WC` - FIFA World Cup (when in progress)
- `ASC` - Asian Cup
- `AFR` - Africa Cup of Nations

### What Gets Collected

For each competition, the pipeline collects:
1. **Completed games** (scores, teams, dates, odds when available)
2. **Upcoming matches** (schedules for next predictions)
3. **Team stats** (form, goals scored/conceded, etc.)
4. **Player props** (scored, assists, shots on target, etc.)

### How It Works

1. **Daily append** (active every 30 minutes):
   - Fetches completed matches from last 3 days
   - Extracts odds from football-data.org
   - Pushes to HF dataset `games` subset

2. **Player prop generation**:
   - For soccer, generates standard props:
     - Win/Draw/Loss (moneyline)
     - First Half Winner
     - Second Half Winner
     - Match Odds (if available from football-data.org)

3. **Model training**:
   - Uses all soccer games + props as training data
   - Includes team form, league, season, day-of-week features
   - News signals (injury/suspension alerts) for enhanced predictions

4. **Predictions**:
   - Daily predictions for all upcoming soccer matches
   - Confidence scores per prop
   - Matched against Kalshi available markets

### Example: Running Soccer Pipeline Only

```powershell
# Bootstrap 30 days of soccer data
python src/betting_bot.py --hf-bootstrap --hf-days-back 30

# Run daily pipeline (all sports including soccer)
python src/betting_bot.py --hf-daily-run --hf-attach-markets

# View predictions in dashboard
python src/dashboard.py
# Open http://localhost:5000
```

### Free Tier Limits

football-data.org free tier:
- **10 requests per minute** (sufficient for 14+ competitions)
- **Up to 10 competitions per request** (pipeline fetches individually with 0.2s delays to stay well within limit)
- Historical data available

### Example Prediction

```json
{
  "prediction_id": "uuid-1234",
  "sport": "soccer",
  "league": "Premier League",
  "home_team": "Manchester City",
  "away_team": "Liverpool",
  "game_date": "2026-07-15",
  "home_win_prob": 0.58,
  "away_win_prob": 0.35,
  "draw_prob": 0.07,
  "market_type": "moneyline",
  "confidence": "elite",
  "model": "random_forest",
  "news_signals": ["injury_concern: Liverpool midfielder out"]
}
```

## Dashboard API

The dashboard proxies provider endpoints:

- `GET /api/predictions/status`
- `GET /api/predictions/today`
- `GET /api/predictions/tomorrow`
- `GET /api/model/stats`
- `GET /api/kalshi/status`
- `GET /api/kalshi/submissions`
- `GET /api/kalshi/positions`
- `GET /api/kalshi/live`
- `POST /api/kalshi/place-from-predictions`

## Notes

- Kalshi defaults to dry-run mode unless live trading is enabled in env.
- HF artifacts are written to `data/hf_pipeline_status.json`, `data/hf_daily_predictions.json`, and `data/training_history.json`.
