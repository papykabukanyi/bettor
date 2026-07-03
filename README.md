# Bettor Free Daily ML Pipeline

Production-ready sports prediction stack with a free daily pipeline, a Flask/Vercel dashboard, and optional Polymarket auto-submission.

## Architecture

```text
GitHub Actions (free scheduler)
├── 02:10 ET equivalent: run_free_pipeline.py -> fetch -> train -> predict
├── Commits fresh JSON snapshots to modal_data/
└── Vercel auto-redeploys with new predictions

Vercel / Flask dashboard
└── src/dashboard.py + src/templates/dashboard.html
    -> reads free local snapshots by default
    -> can proxy an optional provider API
    -> renders glassmorphism dashboard
    -> surfaces prediction + Polymarket status
```

## Quick start

1. Install dependencies:
   ```powershell
   pip install -r requirements.txt
   ```
2. Copy `.env.example` to `.env` and fill in API + Polymarket credentials.
3. Seed local data:
   ```powershell
   python scripts\run_free_pipeline.py
   ```
4. Start the dashboard locally:
   ```powershell
   python src\dashboard.py
   ```
5. Open `http://127.0.0.1:5000`.

## Daily automation (free)

Workflow file: `.github/workflows/free-daily-pipeline.yml`

- Runs every day on GitHub-hosted runner
- Updates:
  - `modal_data/pipeline/*.json`
  - `modal_data/models/*.json`
  - `modal_data/predictions/latest.json`
  - `modal_data/polymarket/*.json`
- Commits snapshots so Vercel serves fresh predictions without paid infrastructure

Detailed deployment instructions live in [`README_MODAL.md`](README_MODAL.md).

## Dashboard API

The dashboard now proxies Modal endpoints:

- `GET /api/predictions/status`
- `GET /api/predictions/today`
- `GET /api/predictions/tomorrow`
- `GET /api/model/stats`
- `GET /api/polymarket/status`
- `GET /api/polymarket/submissions`
- `GET /api/polymarket/positions`

## Notes

- Local `modal run ...` commands write to `modal_data\` for easy iteration.
- Remote Modal deployments write to the mounted Modal Volume.
- Polymarket defaults to dry-run mode until `POLYMARKET_DRY_RUN=false`.
