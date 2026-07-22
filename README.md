# Kalshi Perps Bot

A scalping bot for Kalshi's crypto perpetual futures (perps): BTC, ETH, SOL,
XRP, DOGE, LTC, BCH, LINK, SUI, NEAR, DOT, HBAR, HYPE, kSHIB, XLM, ZEC.

It watches all 16 instruments on multiple timeframes, archives their price
history + news sentiment to a Hugging Face dataset, trains a direction
classifier (up/down over the next ~30 minutes) on that history, and runs a
growth strategy: split the account into portions (up to 5, each 20% of
current balance, using each market's own embedded leverage), put a portion
into a local dip that both a technical trend filter and the model agree on,
then exit that portion either at the standard take-profit/stop-loss/max-hold
levels, OR immediately on a smaller "quick profit" if the gain arrived fast
(see Architecture below) -- repeat, compounding as the balance grows.

## Architecture

Two independent loops so a fast-moving position never waits on a full scan:

```text
Every 20s    : fast exit check -- ONLY manages an already-open position
               (one cheap price call; this is what lets the bot take profit
               quickly on a fast move instead of waiting for the next scan)
Every  2 min : entry scan -- full 16-instrument scan for a NEW entry
               (skipped entirely while a position is already open)
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
- **Live cross-check**: an independent price from Coinbase Exchange or Kraken
  (both free, no API key, live-tested 16/16 instrument coverage) is checked
  against Kalshi's quote before every new entry (rejects the entry if they
  disagree too much -- protects against a stale/erroneous Kalshi tick) and
  feeds a second, independent velocity reading into the quick-profit exit,
  since Kalshi's own perp quote can lag a deep spot venue by a tick or two.
  `API_NINJAS_API_KEY` (optional) is a last-resort third fallback only -- its
  free tier returns prices delayed by ~15 minutes, so it's never used to
  decide entry/exit timing, only as a sanity check when both live sources
  are down.
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

## Backtesting (`src/data/perps_backtest.py`)

Reuses the REAL live decision functions (not a reimplementation) against
extended historical candles chained past Kalshi's 5000-candle-per-call cap:
fits a model on a training window, then walks forward through a held-out
test window across all instruments at once, replaying the exact
concurrency/sizing/exit rules the live bot uses.

```python
from data.perps_backtest import run_backtest
report = run_backtest(days=14, tickers=None)  # None = full live watchlist
```

Known, disclosed limitation: there's no free historical archive of crypto
news, so backtests run technical + model only (sentiment held neutral) --
real-time news is still used live.

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
- `GET|POST /api/perps/tick` -- force an immediate full cycle (fast check, then entry scan if nothing was open)
- `GET|POST /api/perps/fast-check` -- force an immediate position exit check only
- `GET|POST /api/perps/collect` -- force an immediate data-collection + HF push
- `GET|POST /api/perps/train` -- force an immediate retrain

## Safety properties (see `src/data/perps_strategy.py`)

- Dry-run by default; real orders require `KALSHI_PERPS_LIVE_TRADING_ENABLED=1` AND the caller not passing `dry_run=True`.
- Up to `PERPS_MAX_CONCURRENT_POSITIONS` (default 5) open positions at a time, never more than one per instrument -- exits are the fast loop's job exclusively, so the entry-scan loop never makes a competing decision about an already-open position.
- Position sizing is intentionally aggressive at the account owner's request: each new position is sized at `PERPS_POSITION_SIZE_PCT` (default 20%) of CURRENT available balance, spent as margin at that market's own embedded leverage (e.g. ~6x on KXBTCPERP) -- whole contracts only, since these markets don't support fractional contracts.
- A daily realized-loss cap as a PERCENTAGE of the balance at the start of the day (`PERPS_DAILY_LOSS_CAP_PCT`, default 15%) that blocks new entries (not exits) for the rest of the day once breached -- scales with the account instead of a fixed dollar figure.
- A new entry is rejected if Kalshi's quote and the live exchange cross-check disagree by more than `PERPS_MAX_ENTRY_PRICE_DEVIATION_PCT` (default 2%).
- Every position has a take-profit, a stop-loss, a velocity-based quick-profit exit (from either Kalshi's own price or the external cross-check), and a max hold time.

## Testing

```bash
pip install -r requirements-dev.txt
pytest
```

See `tests/README.md` for what each test file guards against.
