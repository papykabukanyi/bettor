# Modal Deployment Guide

## 1. Install prerequisites

```powershell
pip install -r requirements.txt
modal setup
```

Create a `.env` from `.env.example` and ensure these values exist:

- `MODAL_TOKEN_ID`
- `MODAL_TOKEN_SECRET`
- `MODAL_API_URL` (set after deploying `api_serve.py`)
- `POLYMARKET_KEY_ID`
- `POLYMARKET_SECRET_KEY`
- `POLYMARKET_DRY_RUN=true` for first validation runs

## 2. Create Modal secrets

Use one secret for sports API keys and one for trading credentials:

```powershell
modal secret create sports-data-creds `
  BALLDONTLIE_API_KEY=$env:BALLDONTLIE_API_KEY `
  THESPORTSDB_API_KEY=$env:THESPORTSDB_API_KEY

modal secret create polymarket-creds `
  POLYMARKET_KEY_ID=$env:POLYMARKET_KEY_ID `
  POLYMARKET_SECRET_KEY=$env:POLYMARKET_SECRET_KEY `
  POLYMARKET_API_PASSPHRASE=$env:POLYMARKET_API_PASSPHRASE `
  POLYMARKET_PRIVATE_KEY=$env:POLYMARKET_PRIVATE_KEY `
  POLYMARKET_FUNDER=$env:POLYMARKET_FUNDER `
  POLYMARKET_DRY_RUN=$env:POLYMARKET_DRY_RUN
```

## 3. Deploy Modal apps

```powershell
modal deploy modal_app\daily_data_fetch.py
modal deploy modal_app\daily_train.py
modal deploy modal_app\daily_predict.py
modal deploy modal_app\api_serve.py
```

## 4. Run jobs manually

```powershell
modal run modal_app\daily_data_fetch.py
modal run modal_app\daily_train.py
modal run modal_app\daily_predict.py
modal run modal_app\polymarket_submit.py
```

## 5. Inspect logs

```powershell
modal app logs bettor-daily-data-fetch
modal app logs bettor-daily-train
modal app logs bettor-daily-predict
modal app logs bettor-api-serve
```

## 6. Vercel wiring

1. Deploy the Flask dashboard to Vercel.
2. Set `MODAL_API_URL` in Vercel to the deployed Modal API URL from `api_serve.py`.
3. Redeploy Vercel so the dashboard proxies the live Modal API.

## 7. Recommended first-run flow

1. Leave `POLYMARKET_DRY_RUN=true`
2. Run fetch -> train -> predict locally with `modal run`
3. Check `modal_data\predictions\latest.json`
4. Check `modal_data\polymarket\submissions.json`
5. Start `python src\dashboard.py`
6. After reviewing the dashboard, switch dry-run off and redeploy
