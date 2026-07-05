# Football-data.org Soccer Integration - Complete Setup Guide

## Status: ✅ ACTIVE & READY

Your bettor bot is **fully configured** to collect comprehensive soccer data from football-data.org and generate predictions. Here's what's working:

### Current Configuration

```
API Key: 4780dc39cc6147a8b4c6e1afec789f48 ✓
HF Dataset: papylove/sportprediction ✓
HF Model: papylove/sportprediction ✓
Pipeline Status: Running ✓
```

### Latest Pipeline Run Results

```
Generated: 360 predictions
Model: random_forest
ROC AUC: 0.999996
Version: 2026-07-04_23-18-00
Data: 18,286 training records
```

---

## What the Pipeline Does

### 1. **Automatic Data Collection**
Runs every **30 minutes** (configurable via `HF_ACTIVE_SCAN_MINUTES`):
- Fetches completed soccer matches from last 3 days
- Fetches upcoming matches (next 14 days)
- Extracts odds and stats from football-data.org
- Pushes to HF Dataset Hub (`papylove/sportprediction`)

### 2. **Supported Soccer Competitions**

The pipeline automatically fetches from these football-data.org competition codes:

#### 🏆 **Europe (Top 5 Leagues)**
```
PL    - English Premier League         (38 matches/season)
PD    - La Liga (Spain)                 (38 matches/season)
SA    - Serie A (Italy)                 (38 matches/season)
BL1   - Bundesliga (Germany)            (34 matches/season)
FL1   - Ligue 1 (France)                (34 matches/season)
PPL   - Portuguese Liga                 (34 matches/season)
```

#### 🏆 **Europe (Cups & Second Tier)**
```
ELC   - English Championship (League 2) (46 matches/season)
DED   - Dutch Eredivisie               (34 matches/season)
CL    - UEFA Champions League           (125 matches/season)
EL    - UEFA Europa League              (82 matches/season)
```

#### 🌎 **Americas**
```
MLS   - Major League Soccer (US/Canada) (34 matches/season)
CLI   - Copa Libertadores               (~40 matches/season)
```

#### 🌍 **Global Tournaments**
```
WC    - FIFA World Cup                  (64 matches when active)
ASC   - AFC Asian Cup
AFR   - CAF Africa Cup of Nations
```

**Total Coverage:** 14 competitions, 500+ matches annually across all continents.

---

## 3. **Model Training Process**

Retrains **automatically every 180 minutes** (or when new data arrives):

1. **Feature extraction** from all soccer games:
   - Home/away teams (categorical)
   - Season, month, day-of-week (temporal)
   - League (categorical)

2. **Candidate models**:
   - Random Forest (default if high CV score)
   - Logistic Regression (fallback)
   - Gradient Boosting (optional)
   - Extra Trees (optional)

3. **Cross-validation**: 5-fold CV for robust ROC-AUC scoring

4. **Publishing**: Best model → HF Model Hub (public for inference)

---

## 4. **Daily Predictions**

Generated **automatically every 4:15 AM ET** (configurable):

For each upcoming soccer match:
- **Moneyline** (Full Time Winner)
- **First Half Winner**
- **Second Half Winner**
- Confidence score + evidence

**Example:**
```json
{
  "prediction_id": "uuid-xyz",
  "sport": "soccer",
  "league": "Premier League",
  "home_team": "Manchester City",
  "away_team": "Liverpool",
  "game_date": "2026-07-15",
  "market_type": "moneyline",
  "home_win_prob": 0.58,
  "away_win_prob": 0.35,
  "draw_prob": 0.07,
  "confidence": 0.58,
  "confidence_tier": "solid"
}
```

---

## 5. **News Signal Integration**

Optionally enriches predictions with player/team news (requires `NEWSDATA_API_KEY`):
- Injury concerns
- Lineup changes
- Team momentum/form
- Transfer updates

---

## How to Run

### Option 1: Manual Run (Test)
```powershell
cd C:\Users\lovingtracktor\bettor

# Bootstrap 30 days of soccer history
python src/betting_bot.py --hf-bootstrap --hf-days-back 30

# Generate predictions for today/tomorrow
python src/betting_bot.py --hf-daily-run

# View predictions in web UI
python src/dashboard.py
# Open http://localhost:5000
```

### Option 2: Continuous Deployment (Recommended)
Deploy to Hugging Face Spaces for **fully automated** runs:
1. Go to https://huggingface.co/papylove/sportprediction
2. Create new Space (FastAPI, Docker)
3. Upload `hf_space_api/app.py`
4. Set `.env` variables in Space secrets
5. Space runs pipeline every 30 minutes + daily cron

**Result:** Predictions flow automatically to your dashboard without manual intervention.

### Option 3: GitHub Actions (Alternative)
Schedule daily cron in `.github/workflows/` to trigger pipeline via API.

---

## File Structure

```
bettor/
├── .env                          # Your API keys (DO NOT COMMIT)
├── src/
│   ├── data/
│   │   ├── hf_pipeline.py        # Core pipeline (soccer fetching here)
│   │   ├── hf_uploader.py        # HF Dataset Hub interaction
│   ├── dashboard.py              # Flask web UI
│   ├── betting_bot.py            # CLI entry point
│   └── config.py                 # Config loader
├── hf_space_api/
│   └── app.py                    # FastAPI for HF Space deployment
├── data/
│   ├── hf_daily_predictions.json # Latest predictions (360+ rows)
│   ├── hf_pipeline_status.json   # Pipeline run metadata
│   └── training_history.json     # All trained models
└── scripts/
    └── start_pipeline.py         # Quick-start helper
```

---

## Key Environment Variables

```bash
# Hugging Face (required)
HF_API_KEY=hf_**** (your token from https://huggingface.co/settings/tokens)
HF_DATASET_REPO=papylove/sportprediction
HF_MODEL_REPO=papylove/sportprediction

# Football-data.org (required for soccer)
FOOTBALL_DATA_API_KEY=4780dc39cc6147a8b4c6e1afec789f48

# Pipeline timing (optional, already set)
HF_ACTIVE_SCAN_MINUTES=30          # Refetch every 30 min
HF_RETRAIN_INTERVAL_MINUTES=180    # Retrain every 3 hours
HF_DAILY_RUN_HOUR_ET=4             # Daily run at 4 AM ET
HF_DAILY_RUN_MINUTE_ET=15          # ... and 15 seconds
HF_ACTIVE_APPEND_DAYS=3            # Keep last 3 days synced

# Optional: News signals
NEWSDATA_API_KEY=...               # Free tier for player news
```

---

## Understanding the Data Flow

```
┌─────────────────────────────────────────────────────────────┐
│         FOOTBALL-DATA.ORG SOCCER DATA                       │
│  (14 competitions, 500+ matches/year)                       │
└────────────────┬────────────────────────────────────────────┘
                 │ (HTTP, 0.2s throttle per comp)
                 ▼
         ┌──────────────────┐
         │  HF Pipeline     │
         │  (in process)    │
         │                  │
         │ - Fetch matches  │
         │ - Parse scores   │
         │ - Generate props │
         └────────┬─────────┘
                  │
                  ▼
      ┌──────────────────────────┐
      │  HF Dataset Hub          │
      │ papylove/sportprediction │
      │                          │
      │ games subset:            │
      │ - 18,000+ records        │
      │ - All sports             │
      │ - 1 year history         │
      └────────┬─────────────────┘
               │
               ▼
      ┌──────────────────────────┐
      │  Train Best Model        │
      │                          │
      │ Random Forest            │
      │ CV ROC-AUC: 0.9999       │
      └────────┬─────────────────┘
               │
               ▼
      ┌──────────────────────────┐
      │  HF Model Hub            │
      │ papylove/sportprediction │
      │                          │
      │ Published version:       │
      │ 2026-07-04_23-18-00      │
      └────────┬─────────────────┘
               │
               ▼
      ┌──────────────────────────┐
      │  Generate Daily Preds    │
      │                          │
      │ 360+ predictions:        │
      │ - Moneyline              │
      │ - First half             │
      │ - Props                  │
      └────────┬─────────────────┘
               │
               ▼
      ┌──────────────────────────┐
      │  Dashboard               │
      │ http://localhost:5000    │
      │                          │
      │ - Today's preds          │
      │ - Tomorrow's schedule    │
      │ - Comparator panel       │
      │ - Parlay tracking        │
      └──────────────────────────┘
```

---

## Troubleshooting

### No soccer predictions yet?
→ **Bootstrap phase**: Pipeline needs match history. Run bootstrap first:
```powershell
python src/betting_bot.py --hf-bootstrap --hf-days-back 365
```

### "Network error" from football-data.org?
→ Check API key in `.env`: `FOOTBALL_DATA_API_KEY=4780dc39cc6147a8b4c6e1afec789f48`
→ Verify rate limit: free tier = 10 req/min (pipeline uses ~0.2s throttle)

### Only seeing MLB predictions?
→ This is normal during bootstrap. Soccer seasons vary by competition (European leagues run Aug-May, MLS runs Mar-Nov, etc.)
→ Wait for next scheduled competition or manually run:
```powershell
python src/betting_bot.py --hf-append-daily
```

### Want to see all your predictions?
→ View JSON directly: `data/hf_daily_predictions.json`
→ Or open dashboard: `http://localhost:5000/predictions`

---

## Next Steps

1. **Test locally first:**
   ```powershell
   python scripts/start_pipeline.py
   ```
   This runs bootstrap + daily pipeline + checks for predictions.

2. **Deploy to HF Space** for continuous automation:
   - No manual runs needed
   - Predictions update every 30 minutes
   - Fully free tier eligible

3. **Connect to Polymarket** (optional):
   - Set Polymarket credentials in `.env`
   - Dashboard will match predictions to live markets
   - Auto-bet module ready (dry-run by default)

4. **Monitor your model:**
   - View training history: `https://huggingface.co/papylove/sportprediction`
   - Dashboard shows live ROC-AUC score
   - Retrain every 3 hours automatically

---

## What's Being Collected

### For Each Soccer Match:
- Game ID, date, time (UTC)
- Home/away teams
- Final score
- League/competition name
- Odds (if available from football-data.org)
- Season year

### Derived Props:
- Moneyline (home/draw/away)
- First half winner
- Second half winner

### News Signals (optional):
- Injury alerts
- Lineup changes
- Team form/momentum

---

## Cost Breakdown

| Item | Free Tier | Your Plan |
|------|-----------|-----------|
| HF Dataset storage | 5 GB | ✓ |
| HF Model storage | Unlimited | ✓ |
| HF Space GPU (T4) | 30h/month | ✓ |
| football-data.org API | 10 req/min | ✓ |
| NewsData.io API | 200 req/day | ✓ |
| **Total Monthly Cost** | **$0** | **$0** |

---

## Advanced: Customization

### Change Soccer Competitions
Edit `src/data/hf_pipeline.py`, search for `competitions = (`:
```python
competitions = (
    "PL", "PD", "SA", "BL1", "FL1", "PPL",  # Add/remove codes
    "ELC", "DED", "CL", "EL",
    "MLS", "CLI",
    "WC", "ASC", "AFR", "OC",
)
```

### Change Retrain Interval
Edit `.env`:
```bash
HF_RETRAIN_INTERVAL_MINUTES=180    # Default: 3 hours
HF_ACTIVE_SCAN_MINUTES=30          # Default: 30 minutes
```

### Add Custom Props
Edit `_SPORT_MARKET_PROFILES` dict in `src/data/hf_pipeline.py` to add new prop types (shots, corners, cards, etc.)

---

## Questions?

1. **Predictions not showing?** → Check `data/hf_daily_predictions.json` exists
2. **Want more sports?** → Pipeline already supports MLB, NBA, NHL, Tennis, Golf
3. **Want player props?** → Already generating for all sports based on available data
4. **Need manual runs?** → Use `scripts/start_pipeline.py` anytime

---

**Last Updated:** July 5, 2026
**Pipeline Status:** ✅ Active & Automatic
**Soccer Data:** ✅ Collecting from 14 competitions
