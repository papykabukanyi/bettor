# Threads content jobs: moving trending-news + sentiment-snapshot to Hugging Face Jobs

`docs/CRON_JOB_MIGRATION.md` moved all 12 Threads content jobs off each
Render service's own internal scheduler, to cron-job.org-triggered HTTP
routes. This is the next step for 6 of those 12: running trending-news
(stocks, crypto) and sentiment-snapshot (perps, stocks, crypto, options)
as scheduled **Hugging Face Jobs** runs instead -- off Render's compute
entirely, on HF's own infrastructure (`cpu-basic` flavor). Both job types
are read-only fetch-news-and-post; neither ever touches order placement.

hourly-status (all 4 markets) and the two intentional trending-news
no-ops (options, perps -- see `CRON_JOB_MIGRATION.md`'s own note on why)
are **not** part of this move and stay exactly as they are today,
cron-job.org -> Render, since hourly-status needs live position data only
Render actually holds.

## What's already built (code-complete, tested, safe to deploy on its own)

- `scripts/threads_content_job.py` -- a standalone script with no Flask/
  APScheduler dependency. Reimplements each of the 6 job bodies against
  the portable `data.*` functions directly. `python scripts/
  threads_content_job.py --job {trending-news,sentiment-snapshot}
  --market {stocks,crypto,options,perps}`.
- `requirements-threads-job.txt` -- a trimmed dependency set (`pandas`,
  `numpy`, `pyarrow`, `requests`, `huggingface_hub`, `Pillow`). Verified:
  installs cleanly in a fresh venv, the script imports with zero
  `torch`/`scikit-learn`/`flask`/`gunicorn`/`APScheduler` pulled in, and a
  real local run (`--job trending-news --market stocks`) genuinely
  fetched a live headline end-to-end -- it only stopped short of posting
  because no Threads token was configured in that throwaway test.
- `src/data/chart_snapshot.py`'s `public_url_for()` -- now checks
  `RENDER_EXTERNAL_URL` first (100% unchanged behavior for every
  Render-hosted caller: trade entry/exit charts, hourly status, and
  sentiment snapshots posted from Render itself all keep working exactly
  as before). Only when that's unset does it fall through to uploading
  the generated PNG to a new `HF_IMAGES_REPO` and returning that repo's
  public resolve URL instead -- this is the piece that makes it possible
  for a job with no Render HTTP route of its own to still hand Threads a
  real, publicly-fetchable image URL. Uses a fresh timestamped filename
  per upload (never overwrites), auto-prunes older files down to the last
  `HF_IMAGES_MAX_STORED_PER_PREFIX` (default 3) per market/job-type, and
  verifies the URL is actually reachable (a quick HEAD request) before
  handing it to Threads.

None of the above touches live infrastructure or requires new
credentials to merge/deploy -- `HF_IMAGES_REPO` unset means `public_url_for`
behaves identically to before.

## What's NOT done yet -- needs a human with real credentials

### 1. Create the public HF images repo

A new **public** HF dataset repo, separate from the existing private
`HF_MODEL_REPO` (which stores Threads auth tokens and durable trading
state and must stay private). The repo only ever holds generated chart/
card PNGs -- nothing sensitive -- but it must be public, since Threads'
own servers fetch the image URL directly with no auth header.

`scripts/threads_content_job.py`/`chart_snapshot.py` will create this
repo automatically on first upload if it doesn't exist yet (same
`create_repo(..., exist_ok=True, private=False)` pattern already used
elsewhere in this codebase) -- so this step is really just: **pick a
repo name** (e.g. `<your-hf-username>/bettor-threads-images`) and set it
as `HF_IMAGES_REPO` wherever the job runs.

### 2. Provision the HF Job's own environment

This is a **separate environment from Render** -- copy these values in,
don't assume they're inherited:

| Variable | Needed for | Notes |
|---|---|---|
| `HF_API_KEY` | everything (tokens, dedup state, image upload) | same value as Render |
| `HF_MODEL_REPO` | Threads tokens, dedup/recent-news state | same value as Render |
| `HF_IMAGES_REPO` | chart/card image hosting | **new** -- must NOT be set on Render |
| `HF_ALPACA_DATASET_REPO` | stocks sentiment-snapshot's watchlist ranking | same value as Render |
| `ALPACA_API_KEY_ID` / `ALPACA_API_SECRET_KEY` | stocks + crypto sentiment-snapshot (live `/v2/assets` calls) | read-only use here -- this job never places orders, but Alpaca doesn't offer a narrower read-only-only key |
| `ALPACA_OPTIONS_UNDERLYINGS` | options sentiment-snapshot | only if it differs from `alpaca_options_data.py`'s own default |
| Whichever of `SERPAPI_API_KEY` / `CRYPTOPANIC_API_KEY` / `NEWSAPI_ORG_KEY` / `NEWSDATA_API_KEY` / `THENEWSAPI_TOKEN` / `WORLDNEWSAPI_KEY` are actually active on Render today | news sentiment quality | all optional/degrade gracefully per-source if missing -- copy whichever ones are actually in use |

**Deliberately NOT needed here** (real blast-radius reduction -- if this
job's environment were ever compromised, it holds Threads
content-posting reach only): `THREADS_APP_ID`/`THREADS_APP_SECRET`/
`THREADS_REDIRECT_URI` (one-time interactive login only), each Render
service's own `CRON_SECRET`, Kalshi trading credentials, and any
order-placement-scoped Alpaca permission.

### 3. Get the codebase into the job's container

HF Jobs runs a Docker image + a command -- it doesn't have this repo
checked out on its own. Recommended: a generic base image (e.g.
`python:3.11-slim`) with a command that clones this repo fresh on every
run (confirmed public: `https://github.com/papykabukanyi/bettor.git`,
no deploy token needed) and installs the trimmed requirements, e.g.

```
git clone --depth 1 https://github.com/papykabukanyi/bettor.git /app && \
cd /app && pip install -r requirements-threads-job.txt && \
python scripts/threads_content_job.py --job trending-news --market stocks
```

This always runs whatever's on `main` at execution time -- no separate
image-build/publish pipeline to keep in sync.

### 4. Schedule the 6 jobs

CRON schedule should match today's cadence (trending-news every 30 min,
sentiment-snapshot every 60 min), staggered the same way
`CRON_JOB_MIGRATION.md` recommends for the cron-job.org jobs so they
don't all land on the same tick:

| Job | Market | Suggested schedule |
|---|---|---|
| trending-news | stocks | `*/30 * * * *` |
| trending-news | crypto | `5,35 * * * *` |
| sentiment-snapshot | stocks | `10 * * * *` |
| sentiment-snapshot | crypto | `20 * * * *` |
| sentiment-snapshot | options | `30 * * * *` |
| sentiment-snapshot | perps | `40 * * * *` |

The exact `huggingface_hub`/`hf` CLI call for creating a scheduled job
should be checked against whatever `huggingface_hub` version is current
at setup time (this detail shifts across SDK releases) -- see [HF Jobs
docs](https://huggingface.co/docs/huggingface_hub) for the current
scheduled-jobs API surface.

### 5. Verify before cutover

1. Run one job/market pair manually first (e.g. `sentiment-snapshot`/
   `stocks`) and confirm it actually posts to the real Threads account
   with a real, correctly-rendered image -- a broken public URL would
   still return an HTTP success at the upload step but Threads could
   reject the media container.
2. Once confirmed, set up the remaining 5 scheduled jobs.
3. Watch at least one real *scheduled* (not manual) run land successfully
   for each of the 6 before touching cron-job.org.

### 6. Cutover

Only after step 5 above: delete the corresponding 6 jobs on
[cron-job.org](https://console.cron-job.org/jobs) so Render's routes stop
double-firing. **Leave the Render routes and `_run_*` functions in the
codebase untouched** -- same "kept for consistency, trivial to re-enable"
convention already used for the options/perps trending-news no-ops --
they're a free rollback path if HF Jobs infrastructure ever has an
outage; just point cron-job.org back at them.

Update the table in `docs/CRON_JOB_MIGRATION.md` at this point to show
only the 6 jobs that remain on cron-job.org -> Render, with a pointer
back to this doc for the 6 that moved.
