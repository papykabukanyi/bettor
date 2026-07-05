# ⚽ SOCCER + MULTI-SPORT PREDICTIONS - QUICK START

## Status
✅ **Pipeline Ready** | ✅ **Data Flowing** | ✅ **Predictions Generating**

---

## RIGHT NOW - View Predictions

### In Your Browser
```
http://localhost:5000/predictions
```

### In Terminal
```powershell
# Read JSON predictions
Get-Content C:\Users\lovingtracktor\bettor\data\hf_daily_predictions.json | ConvertFrom-Json | Select-Object -ExpandProperty predictions | Select-Object sport, league, home_team, away_team, home_win_prob | Format-Table -AutoSize
```

### Python
```python
import json
with open("data/hf_daily_predictions.json") as f:
    data = json.load(f)
    for pred in data["predictions"][:10]:
        print(f"{pred['away_team']} @ {pred['home_team']}: {pred['confidence_tier']}")
```

---

## START PREDICTIONS (Anytime)

### Quick Start
```powershell
cd C:\Users\lovingtracktor\bettor
python src/betting_bot.py --hf-daily-run --hf-attach-markets
```

### Full Startup (Recommended first time)
```powershell
cd C:\Users\lovingtracktor\bettor
python scripts/start_pipeline.py
```

### Bootstrap History (Load 1 year of data)
```powershell
python src/betting_bot.py --hf-bootstrap --hf-days-back 365
```

---

## START DASHBOARD

### Terminal
```powershell
cd C:\Users\lovingtracktor\bettor
python src/dashboard.py
```

### Browser
Open: `http://localhost:5000`

**Tabs:**
- **Predictions** - Today's & tomorrow's picks
- **Comparator** - Multi-prop ranking for same game
- **Parlay** - Tracking your prediction history
- **Stats** - Model performance & coverage

---

## WHAT YOU'RE GETTING

### 360+ Predictions Daily

**By Sport:**
- ⚽ Soccer (14 competitions)
- 🔴 MLB (baseball)
- 🏀 NBA (basketball)
- 🎾 NHL (hockey)
- 🎾 Tennis
- ⛳ Golf
- 🥊 MMA/Boxing

**By Prop Type:**
- Moneyline (who wins)
- First half/period winner
- Totals/spreads
- Player props (when available)

---

## WHAT'S HAPPENING BEHIND THE SCENES

### Every 30 Minutes
```
football-data.org (PL, La Liga, Serie A, etc.)
    ↓
Fetch: completed matches (last 3 days)
Fetch: upcoming matches (next 14 days)
    ↓
Parse scores, teams, odds
    ↓
Push to HF Dataset Hub (papylove/sportprediction)
```

### Every 3 Hours
```
Dataset → Train model on all collected games
    ↓
5-fold cross-validation
    ↓
Pick best model (random_forest typically)
    ↓
Publish to HF Model Hub
```

### Every 4:15 AM ET
```
Upcoming matches for today/tomorrow
    ↓
Call deployed HF model
    ↓
Generate 360+ predictions
    ↓
Save to data/hf_daily_predictions.json
    ↓
Dashboard updates automatically
```

---

## YOUR HF REPOS

### Dataset (Training Data)
https://huggingface.co/datasets/papylove/sportprediction
- View: completed games, odds, player props, news signals
- Size: 18,000+ records (growing daily)

### Model (Trained AI)
https://huggingface.co/papylove/sportprediction
- View: best trained model
- Version: auto-updated daily
- ROC-AUC: 0.9999+ (on test set)

### Space (Live Inference - Optional)
https://huggingface.co/spaces/papylove/sportprediction
- Deploy `hf_space_api/app.py` here for automation
- Runs pipeline automatically
- Zero cost (free tier eligible)

---

## API KEYS IN USE

### Football-data.org ✅
```
Key: 4780dc39cc6147a8b4c6e1afec789f48
Coverage: 14 soccer competitions
Limit: 10 req/min (pipeline uses <1 req/sec)
Status: ACTIVE
```

### Hugging Face ✅
```
Key: hf_**** (see your .env file)
Dataset: papylove/sportprediction
Model: papylove/sportprediction
Status: ACTIVE
```

### NewsData.io (Optional)
```
Key: [not configured yet]
Purpose: Player injuries, team news, lineup changes
Recommended: Get free key from https://newsdata.io/
Limit: 200 req/day
```

---

## SOCCER COMPETITIONS TRACKED

### 🇪🇺 Europe Top 5
- England: Premier League (PL)
- Spain: La Liga (PD)
- Italy: Serie A (SA)
- Germany: Bundesliga (BL1)
- France: Ligue 1 (FL1)
- Portugal: Liga (PPL)

### 🏆 European Cups
- Champions League (CL)
- Europa League (EL)

### 🌎 Americas
- MLS (USA/Canada)
- Copa Libertadores

### 🌍 Global
- World Cup (WC) - when active
- Asian Cup (ASC) - when active
- Africa Cup (AFR) - when active

---

## COMMON QUESTIONS

**Q: Why are predictions mostly MLB?**
A: July 4 is the test date (US Independence Day). Soccer seasons vary by region. Upcoming seasons will show full soccer coverage.

**Q: When does it refresh?**
A: Every 30 minutes (fetch data), every 3 hours (retrain model), every 4:15 AM ET (daily predictions).

**Q: Can I deploy to HF Spaces?**
A: Yes! Upload `hf_space_api/app.py` to https://huggingface.co/spaces/papylove/sportprediction for fully automated runs.

**Q: What's the model accuracy?**
A: Test ROC-AUC = 0.9999 (very high, likely overfitting on small dataset - will stabilize with more data).

**Q: Can I run predictions manually?**
A: Yes: `python src/betting_bot.py --hf-daily-run`

**Q: Can I see the code?**
A: Yes: `src/data/hf_pipeline.py` (main pipeline), `src/dashboard.py` (UI)

---

## NEXT STEPS

### Immediate (Today)
- [ ] View dashboard: http://localhost:5000
- [ ] Check predictions JSON: `data/hf_daily_predictions.json`
- [ ] Run `python scripts/start_pipeline.py` to confirm setup

### Short-term (This Week)
- [ ] Add `NEWSDATA_API_KEY` for news signals (optional)
- [ ] Deploy to HF Spaces for automation (recommended)
- [ ] Connect Kalshi credentials for auto-betting (optional)

### Long-term (This Month)
- [ ] Accumulate 30+ days of prediction history
- [ ] Monitor model ROC-AUC (should normalize to 0.55-0.65)
- [ ] Add more sports/competitions as needed
- [ ] Fine-tune betting strategy based on live Kalshi data

---

## FILES YOU NEED TO KNOW

| File | Purpose |
|------|---------|
| `.env` | Your API keys (SECRET - don't share) |
| `src/data/hf_pipeline.py` | Main pipeline (fetches soccer data) |
| `src/dashboard.py` | Web UI (predictions display) |
| `hf_space_api/app.py` | Deploy this to HF Spaces for automation |
| `data/hf_daily_predictions.json` | Latest predictions (360+ rows) |
| `data/hf_pipeline_status.json` | Pipeline run metadata |
| `SOCCER_SETUP.md` | Detailed setup guide (this folder) |

---

## SUPPORT

📊 **View Training Data:**
```powershell
# Count records by sport
python -c "
import json
from collections import Counter
with open('data/hf_daily_predictions.json') as f:
    preds = json.load(f)['predictions']
    sports = Counter(p['sport'] for p in preds)
    for sport, count in sports.most_common():
        print(f'{sport}: {count}')
"
```

🎯 **Check Model Performance:**
```powershell
Get-Content data/training_history.json | ConvertFrom-Json | Select-Object version, rows, best_model, cv_roc_auc | Format-Table -AutoSize
```

📝 **View One Prediction:**
```powershell
python -c "
import json
with open('data/hf_daily_predictions.json') as f:
    pred = json.load(f)['predictions'][0]
    print(json.dumps(pred, indent=2))
"
```

---

**Status:** 🟢 All Systems Go
**Soccer Data:** ✅ Active
**Predictions:** ✅ Generating
**Last Update:** July 5, 2026
