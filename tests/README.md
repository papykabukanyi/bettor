# Test suite

Regression tests for the Kalshi Perps trading bot's safety properties: the
strategy must never place a real order outside dry-run without an explicit
opt-in, must never exceed its position-slot cap or open a second position in
an instrument it already holds, must respect the (percentage-based) daily
loss cap, and its engineered features must never leak future price
information into training rows. Job locking must prevent the same scheduled
job (including one that can place real orders) from running twice at once.
The backtest engine must reuse the SAME decision functions the live strategy
runs, not a reimplementation that could silently drift from it.

None of these tests touch the network. Kalshi, Hugging Face Hub, and news
feeds are all mocked or monkeypatched with synthetic data.

## Running

```bash
pip install -r requirements-dev.txt
pytest
```

Run a single file while iterating:

```bash
pytest tests/test_perps_strategy.py -v
```

## What's covered

| File | What it guards against |
|---|---|
| `test_perps_strategy.py` | Real orders placed outside dry-run; the (percentage-based) daily loss cap not blocking new entries; opening a second position in an already-held instrument, or exceeding the concurrent-position slot cap; leveraged position sizing not actually using each market's multiplier; the direction model failing to override (or correctly deferring to) the technical scalper signal; the fast exit loop and slow entry-scan loop making competing decisions about the same position; the velocity-based quick-profit exit not firing on a fast move (or firing on a slow one), including the external-exchange cross-check velocity; a large Kalshi/external price disagreement not blocking a new entry |
| `test_perps_data.py` | Leakage in the engineered multi-timeframe features (a label that peeks at its own future window); the live-instrument watchlist not falling back to a known list when Kalshi's listing call fails |
| `test_perps_model.py` | Training silently "succeeding" on too little data; a trained model failing to produce a usable prediction from a live feature row |
| `test_perps_backtest.py` | The backtest drifting from the real strategy's entry/exit/sizing functions; exceeding the concurrent-position cap during simulation; re-predicting per-row instead of reusing a batched model prediction; the chained historical-candle fetcher not actually chaining past the 5000-candle-per-call cap |
| `test_dashboard_jobs.py` | Duplicate concurrent execution of the same scheduled job; a stale (crashed-process) lock permanently wedging a job |

## Adding a new test

If you fix a bug found in production, add a test for it here first -- that's
the whole point of this suite existing. Mock the external call, reproduce the
failure mode, assert the fix holds.
