# Kalshi Perps Bot

A scalping bot for Kalshi's crypto perpetual futures (perps): BTC, ETH, SOL,
XRP, DOGE, LTC, BCH, LINK, SUI, NEAR, DOT, HBAR, HYPE, kSHIB, XLM, ZEC.

It watches all 16 instruments on multiple timeframes, archives their price
history + news sentiment to a Hugging Face dataset, trains a direction
classifier (up/down over the next ~30 minutes) on that history, and runs a
small-size scalping strategy: enter on a local dip that both a technical
trend filter and the model agree on, take a small profit (or cut a small
loss), repeat.

## Architecture

```text
Every  2 min : scan all 16 perp instruments -> strategy cycle (dry-run by default)
Every 15 min : pull multi-timeframe candles + news sentiment -> HF Dataset Hub
Daily 03:00 ET: retrain the direction classifier -> HF Model Hub
Backend      : Render Flask app (single gunicorn worker + in-process APScheduler)
Frontend     : the same Flask app serves a lightweight status dashboard
Execution    : Kalshi margin/perps REST API (RSA-PSS signed requests)
```

Everything runs dry-run (no real orders) until `KALSHI_PERPS_LIVE_TRADING_ENABLED=1`
is explicitly set -- this is separate from `ENABLE_PERPS_SCHEDULER`, which
only controls whether the loop runs at all.

## Data + model

- **Data source**: Kalshi's own `/margin/markets/{ticker}/candlesticks` (1-min
  and 60-min OHLC) -- this is the actual tradable instrument's price, not an
  external index that could diverge from it.
- **News**: free, no-key sources only (Google News RSS, CoinTelegraph RSS,
  Reddit JSON) scored with simple keyword polarity -- one more feature, not a
  system of its own.
- **Archive**: every collection cycle is merged into a daily parquet shard
  and pushed to the HF dataset repo (`HF_DATASET_REPO`), so training always
  has a growing history to draw from -- not just whatever window Kalshi's API
  happens to serve.
- **Model**: logistic regression / random forest / gradient boosting are
  compared on a chronological (never randomly shuffled) holdout split; the
  best one is kept, saved locally, and pushed to the HF model repo
  (`HF_MODEL_REPO`).
- **Cold start**: until enough data has been collected for a model to exist,
  the strategy runs on the technical scalper filter alone -- every result is
  flagged `model_ok: false` in that case so it's visible from the dashboard.

## Quick start

1. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

2. Copy `.env.example` to `.env` and set:
   - `HF_API_KEY` -- HF write token (generate one at huggingface.co/settings/tokens)
   - `HF_DATASET_REPO` / `HF_MODEL_REPO` -- your own `owner/repo` names
   - `KALSHI_API_KEY` / `KALSHI_PRIVATE_KEY` -- Kalshi API credentials

3. Read-only sanity check against the real account (places zero orders):

   ```bash
   python scripts/test_kalshi_perps_connectivity.py
   ```

4. Run one manual strategy cycle (dry-run):

   ```bash
   python scripts/run_perps_cycle.py
   ```

5. Start the dashboard (also starts the background scheduler):

   ```bash
   python src/dashboard.py
   # then open http://localhost:5000
   ```

## Dashboard API

- `GET /api/status` -- account balance, open position, today's/all-time P&L, model state, last scan across all instruments
- `GET /api/trades` -- trade log + realized P&L by date
- `GET /api/positions` -- live open positions from Kalshi
- `GET /api/server/activity` -- background job history (running/ok/error/skipped)
- `GET|POST /api/perps/tick` -- force an immediate strategy cycle
- `GET|POST /api/perps/collect` -- force an immediate data-collection + HF push
- `GET|POST /api/perps/train` -- force an immediate retrain

## Safety properties (see `src/data/perps_strategy.py`)

- Dry-run by default; real orders require `KALSHI_PERPS_LIVE_TRADING_ENABLED=1` AND the caller not passing `dry_run=True`.
- Exactly one open position at a time, across all 16 instruments.
- Whole-contract position sizing (`PERPS_TRADE_SIZE_CONTRACTS`) -- these markets don't support fractional contracts.
- A hard daily realized-loss cap (`PERPS_DAILY_LOSS_CAP_USD`) that blocks new entries (not exits) for the rest of the day once breached.
- Every position has a take-profit, a stop-loss, and a max hold time.

## Testing

```bash
pip install -r requirements-dev.txt
pytest
```

See `tests/README.md` for what each test file guards against.
