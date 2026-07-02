# Bettor HF-First Pipeline Commands

This project now supports a direct Hugging Face workflow:

1. One-time historical load -> HF Dataset  
2. Daily clean append -> HF Dataset  
3. Daily retrain (custom model) -> HF Model Hub  
4. Daily predictions -> JSON + dashboard status strip

## Required environment variables

- `HF_API_KEY`
- `HF_DATASET_REPO` (example: `yourname/sportprediction-data` or `sportprediction-data`)
- `HF_MODEL_REPO` (example: `yourname/sportprediction-model` or `sportprediction-model`)
- Optional:
  - `HF_INFERENCE_MODEL`
  - `HF_INFERENCE_ENDPOINT`
  - `HF_PIPELINE_STATUS_FILE`
  - `HF_DAILY_PREDICTIONS_FILE`
  - `HF_SIGNAL_LOG_FILE`
  - `HF_AUTORUN_ON_DEPLOY=1` (run HF pipeline automatically on deployment/start)
  - `HF_AUTORUN_DELAY_SEC=30`
  - `HF_DAILY_RUN_HOUR_ET=4`
  - `HF_DAILY_RUN_MINUTE_ET=15`
  - `HF_DAILY_CUSTOM_MODEL=gradient_boosting`
  - `HF_DAILY_MIN_TRAIN_ROWS=200`

## Core CLI commands

### One-time bootstrap + train custom model

```powershell
python src\betting_bot.py --hf-bootstrap --hf-days-back 365 --hf-retrain-publish --hf-custom-model gradient_boosting
```

### Daily clean + feed + retrain + predict

```powershell
python src\betting_bot.py --hf-daily-run --hf-custom-model gradient_boosting --hf-min-train-rows 200 --hf-predictions-output data\hf_daily_predictions.json
```

### Daily run using HF inference API

```powershell
python src\betting_bot.py --hf-daily-run --hf-custom-model gradient_boosting --hf-predict-via-api --hf-inference-model yourname/sportprediction-model
```

### Predict one matchup from custom model artifact

```powershell
python src\betting_bot.py --hf-predict-matchup "New York Yankees" "Boston Red Sox" --hf-predict-season 2026
```

### Predict one matchup via HF API (model swap supported)

```powershell
python src\betting_bot.py --hf-predict-matchup "New York Yankees" "Boston Red Sox" --hf-predict-via-api --hf-inference-model yourname/alternate-model
```

## Windows helper scripts

- `scripts\run_hf_bootstrap.ps1`
- `scripts\run_hf_daily.ps1`

Examples:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\run_hf_bootstrap.ps1 -CustomModel gradient_boosting
powershell -ExecutionPolicy Bypass -File scripts\run_hf_daily.ps1 -CustomModel gradient_boosting
```

## Dashboard

The dashboard now shows a simple bot-ops strip with:

- analysis status
- HF pipeline last step
- selected custom model
- daily prediction count (and error count if any)