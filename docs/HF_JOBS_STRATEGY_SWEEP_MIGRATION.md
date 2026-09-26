# Strategy sweeps: moving the real, big compute onto Hugging Face Jobs

Per explicit user direction across several messages: "we need to work on
over 10000 mix of strategies... perform a forward test with real data",
"HF should have a model that generate millions of strategies... if this
work we will expand to the other bots", "let take full advantage of HF
pro... need to use all the resource[s] across and apply to each bot what
its need[s]", "let do this for all the bots all of those jobs are handle
by HF", and "we need to take full advantage of the CPU and RAM to the max
to generate multiple real strategies and combinations with real data."

This is the last piece of that request: the sweep itself
(`src/data/strategy_sweep.py`) and its per-market wiring
(`scripts/strategy_sweep_job.py`) are done and tested. What's below is
exactly how to run it as a real Hugging Face Job, the real (measured, not
guessed) cost and throughput math, and the one thing left that's a
recurring-cost decision, not an engineering one: whether/how often to
schedule it.

## What's built

- `src/data/strategy_sweep.py`: the core engine, now able to (a) accept a
  pre-built `combined` DataFrame so it works for markets whose data
  pipeline doesn't match kalshi_15m's own shape (perps, the 3 alpaca
  markets), (b) run combinations across a real `ProcessPoolExecutor`
  (`n_workers`) instead of one at a time, (c) forward-test its own top
  survivors against a slice of real data no walk-forward fold's fit or
  test window ever touched (`holdout_bounds` -- see its own module
  docstring), and (d) reject combinations whose result is a numerical
  compounding artifact rather than a real finding (see
  `MAX_PLAUSIBLE_MEAN_RETURN_PCT`'s own comment for the real, live
  incident this guards against -- a 3.09e+20% "return" the already-
  deployed 16,200-combination sweep surfaced before this fix).
- `scripts/strategy_sweep_job.py`: standalone (no Flask/APScheduler),
  covers all 6 bots (`kalshi_15m`, `kalshi_15m_metals`, `stocks`,
  `crypto`, `options`, `perps`) with a real default grid + real data-
  loading per market, publishes its result to that market's own existing
  HF model repo via `server_common.push_json_to_hf`.
- `app_kalshi.py`'s `/api/kalshi15m/strategy-sweep` route now reads the
  HF-published result FIRST (falling back to its own smaller in-Space job
  only if HF has nothing yet) -- `?market=kalshi_15m` or
  `?market=kalshi_15m_metals` (default).
- Real, live-verified end to end, twice: a small manual run (local
  machine) against the real `kalshi_15m_metals` archive (1,858 real rows
  at the time) completed in 8 seconds and published to
  `papylove/kalshi-15m-metals-model`. Then a REAL Hugging Face Job (not
  local -- `cpu-upgrade`, 6 workers) ran the FULL 544,000-combination
  default grid for 10 real minutes: 131,520 combinations actually
  evaluated (~219 combos/sec on this archive's current size -- faster
  than the "mature archive" planning table below, since this market's
  real archive is still young), 113,991 cleared the evidence bar, and
  17,529 were caught and excluded by MAX_PLAUSIBLE_MEAN_RETURN_PCT --
  which is itself how a SECOND real bug got found: the first ceiling
  (100,000%) was still too loose, letting ~98,000%-return entries through
  (same assumed_entry_price-compounding artifact, just under the old
  bar) -- tightened to 5,000% after this real run exposed it. Also caught
  something real on the way in that's a genuine finding, not a bug: the
  walk-forward-only ranking's "best" combination (`model_confidence_min=0.52`)
  returned 5.2% mean
  return across the 3 walk-forward folds, but only 0.56% on the untouched
  holdout slice -- while `0.60`, ranked *worst* on walk-forward, held up
  best out of sample (2.21%, 63% win rate). Exactly the overfitting
  pattern the holdout step exists to catch.

## Real, measured throughput (not guessed)

One `simulate()` call, one CPU core, on this codebase's own real archived
data (not synthetic):

| Archive | Real rows | Fold size | ms/call | calls/sec/core |
|---|---|---|---|---|
| kalshi_15m_metals (today, young) | 1,858 | ~280 | 14.2 | 70.7 |
| kalshi_15m crypto (mature) | 34,347 | ~5,155 | 49.7 | 20.1 |

Each combination needs 3 `simulate()` calls (one per walk-forward fold,
`DEFAULT_FOLD_BOUNDS_WITH_HOLDOUT`) plus one more for its own holdout
check if it survives ranking -- call it ~3 calls/combination for the
throughput math below. Using the MATURE-archive number (20.1 calls/sec/
core -> ~6.7 combos/sec/core) as the honest planning baseline, since every
market's archive keeps growing and per-call cost grows with it (a real
`cpu-upgrade`/6-worker HF Job run against the metals archive's CURRENT,
still-young size measured ~219 combos/sec -- notably faster than this
table's own planning number, confirming this table is the conservative,
not optimistic, estimate):

| HF flavor | vCPU | $/hour | workers used (vCPU-2) | combos/sec | combos/hour |
|---|---|---|---|---|---|
| cpu-basic | 2 | $0.01 | -- (too little headroom, not recommended) | -- | -- |
| cpu-upgrade | 8 | $0.03 | 6 | ~40 | ~144,000 |
| cpu-xl | 16 | $1.00 | 14 | ~94 | ~338,000 |
| cpu-performance | 32 | $1.90 | 30 | ~201 | ~724,000 |

(Cost/hardware pulled live from `HfApi().list_jobs_hardware()` at write
time -- re-check if this ever needs re-verifying, HF's own pricing can
change.)

Real grid sizes this session's own default grids sweep (see
`scripts/strategy_sweep_job.py`'s own `_*_grid` functions):

| Market | Combinations |
|---|---|
| kalshi_15m | 544,000 |
| kalshi_15m_metals | 544,000 |
| stocks | 144,000 |
| crypto | 172,800 |
| options | 172,800 |
| perps | 28,800 |
| **Total** | **1,606,400** |

So "millions of combinations" is a real, reachable number given a real
job -- not a promise this code fabricates. At `cpu-performance`
(~724,000 combos/hour), sweeping ALL SIX markets' full default grids in
one pass takes roughly **2.2 hours**, costing **~$4.20**. At the much
cheaper `cpu-upgrade`, the same full run takes roughly **11 hours**
(fine for an overnight/weekend window) for **~$0.35**. Either way,
`--max-seconds` is a real, hard ceiling regardless of how the real
throughput turns out to differ from this table on any given day --
you are never at risk of an open-ended bill; a job capped at
`--max-seconds 7200` costs at most `flavor_rate × 2 hours`, however many
or few combinations it actually gets through in that window.

## Running it

One-off, on-demand (uses this repo's own `HfApi` token -- the SAME one
already used to deploy this Space):

```python
from huggingface_hub import run_job

run_job(
    image="python:3.11-slim",
    command=[
        "bash", "-c",
        "apt-get update -qq && apt-get install -y -qq --no-install-recommends git >/dev/null && "
        "git clone --depth 1 https://github.com/papykabukanyi/bettor.git /app && "
        "cd /app && pip install -q -r requirements.txt && "
        "python scripts/strategy_sweep_job.py --market kalshi_15m_metals --n-workers 30 --max-seconds 7200",
    ],
    flavor="cpu-performance",
    timeout="3h",  # a bit above --max-seconds so the container has time to publish + exit cleanly
    secrets={"HF_API_KEY": "<the same value already set on the live Space>"},
    env={"KALSHI_15M_METALS_LABEL_HORIZON_MINUTES": "15"},  # only if any non-default env vars matter for this market's own data pipeline
)
```

Recurring (weekly, Sunday 10am ET -- one hour after the existing in-Space
fallback job's own Sunday 9am ET slot, so they never compete for the same
window):

```python
from huggingface_hub import create_scheduled_job

create_scheduled_job(
    image="python:3.11-slim",
    command=[
        "bash", "-c",
        "apt-get update -qq && apt-get install -y -qq --no-install-recommends git >/dev/null && "
        "git clone --depth 1 https://github.com/papykabukanyi/bettor.git /app && "
        "cd /app && pip install -q -r requirements.txt && "
        "python scripts/strategy_sweep_job.py --market kalshi_15m_metals --n-workers 30 --max-seconds 7200",
    ],
    schedule="0 14 * * 0",  # 10am ET Sunday = 14:00 UTC (13:00 during EDT -- adjust if this matters precisely)
    flavor="cpu-performance",
    timeout="3h",
    secrets={"HF_API_KEY": "..."},
    name="strategy-sweep-kalshi-15m-metals",
)
```

Repeat per market, staggering `schedule` by ~15-30 minutes each so all 6
don't compete for the account's own concurrent-job quota at once. A CLI
form also exists (`hf jobs run` / `hf jobs scheduled create`) if a shell
script is preferred over Python -- same image/command/flavor/schedule
arguments.

Local/manual run (what this session actually used to verify the above):

```bash
python scripts/strategy_sweep_job.py --market kalshi_15m_metals --n-workers 4 --max-seconds 600
python scripts/strategy_sweep_job.py --market perps --dry-run   # prints grid size only, no data load, no push
```

## Secrets each job needs

Only `HF_API_KEY` (same value as the live Space) -- every other input
(the real archive, each market's own model-selection candidates) is
pulled from that market's own existing HF dataset/model repo, no
additional credentials. The market -> repo mapping (see
`scripts/strategy_sweep_job.py`'s own `MARKET_CONFIGS`):

| Market | Published to |
|---|---|
| kalshi_15m | `papylove/kalshi-15m-model` / `strategy_sweep_kalshi_15m.json` |
| kalshi_15m_metals | `papylove/kalshi-15m-metals-model` / `strategy_sweep_kalshi_15m_metals.json` |
| stocks | `papylove/alpaca-model` / `strategy_sweep_stocks.json` |
| crypto | `papylove/alpaca-crypto-model` / `strategy_sweep_crypto.json` |
| options | `papylove/alpaca-options-model` / `strategy_sweep_options.json` |
| perps | `papylove/kalshi-perps-model` / `strategy_sweep_perps.json` |

## What's NOT done yet -- a real decision, not an engineering task

Only `kalshi_15m`/`kalshi_15m_metals` have a live Space route reading the
published result back (`/api/kalshi15m/strategy-sweep?market=...`).
Perps and the 3 alpaca markets don't have an equivalent read route yet --
straightforward to add (same `pull_json_from_hf` pattern), not done in
this pass since no one has asked to actually SEE those results in a
dashboard yet.

**Recurring scheduling itself has NOT been set up** -- `create_scheduled_job`
above is real and tested manually as a one-off `run_job`, but turning it
into a standing weekly schedule across all 6 markets is a real, ongoing
cost commitment (see the table above) even though a small one, and this
session's own standing rule is: real, outward-facing, recurring-cost
decisions get confirmed, not assumed. Pick a flavor/cadence and say the
word, or run it manually via the on-demand form above whenever you want
a fresh sweep without committing to a schedule yet.
