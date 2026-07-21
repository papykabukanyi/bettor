# Test suite

Real regression tests for the specific production incidents this bot has hit:
rate-limit cascades that silently froze predictions for two weeks, duplicate
concurrent pipeline runs that risked placing the same Kalshi bet twice,
leakage in the engineered training features, and the bankroll/combo-leg rules
that protect real money.

None of these tests touch the network. Hugging Face Hub and Kalshi are
mocked; sports-data fetchers are monkeypatched to return synthetic rows.

## Running

```bash
pip install -r requirements-dev.txt
pytest
```

Run a single file while iterating:

```bash
pytest tests/test_hf_pipeline_features.py -v
```

## What's covered

| File | What it guards against |
|---|---|
| `test_hf_uploader_resilience.py` | HF `whoami`/`repo_info`/`create_repo` rate-limiting taking the whole pipeline down (the actual cause of predictions freezing for two weeks) |
| `test_hf_pipeline_features.py` | Engineered form/rest/H2H/news features leaking future results into training rows |
| `test_hf_pipeline_training.py` | Per-sport model split respecting the min-rows threshold + fallback to the combined model; redundant HF dataset downloads within one cycle |
| `test_hf_pipeline_predict_resilience.py` | A crash in the day-over-day drift comparison blocking the actual predictions file write; same-day news adjustment staying bounded |
| `test_kalshi_rules.py` | Combos exceeding 2 legs or going negative-EV; a bet with no parseable date being matched by name similarity alone |
| `test_pregame_bankroll.py` | Placing orders the account can't cover instead of deferring to the next cycle; dry-run being incorrectly capital-gated |
| `test_dashboard_jobs.py` | Duplicate concurrent execution of the same scheduled job (the "Dashboard scheduler started" x2 bug); stale (days-old) predictions being served as if current |

## Adding a new test

If you fix a bug found in production, add a test for it here first -- that's
the whole point of this suite existing. Mock the external call, reproduce the
failure mode, assert the fix holds.
