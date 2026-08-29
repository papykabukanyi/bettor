# Threads content jobs: moved to cron-job.org

Each of the 4 trading services (perps, stocks, crypto, options) used to run
3 "content" jobs -- trending-news post, sentiment snapshot, hourly status --
on their own internal APScheduler, on top of the actual trading-critical
jobs (fast_check, entry_scan, data_collect, train). None of the 3 content
jobs touch order placement or trading decisions; they only post to Threads
and read state. To cut that job/executor overhead out of each Render
process and keep the scheduler focused on the jobs that actually need to
live there, all 12 (3 jobs x 4 services) now run as externally-triggered
HTTP routes instead, meant to be called on a schedule by
[cron-job.org](https://console.cron-job.org/jobs).

## What changed

- The internal `scheduler.add_job(...)` registrations for these 12 jobs
  were removed from each service's own startup code.
- Each job function itself is unchanged -- still does exactly what it did
  before (same `_run_*_threads_trending_news`/`_sentiment_snapshot`/
  `_hourly_status` functions), just called by an incoming HTTP request
  instead of an internal timer.
- A new route per job, gated by the same `CRON_SECRET` bearer-token check
  every other manual-trigger route in these services already uses
  (`is_cron_authorized` -- see `/api/perps/tick` and its siblings).

## Routes to schedule

Each service has its **own** `CRON_SECRET` value (set directly on Render,
not in this repo) -- use the right one per service below. All routes
accept GET or POST; cron-job.org's default GET works fine.

| Service | Base URL | Trending news | Sentiment snapshot | Hourly status |
|---|---|---|---|---|
| Perps (Kalshi) | `bettor-dashboard-18ni.onrender.com` | `/api/perps/threads/trending-news` | `/api/perps/threads/sentiment-snapshot` | `/api/perps/threads/hourly-status` |
| Stocks (Alpaca) | `bettor-schwab-2uxw.onrender.com` | `/api/alpaca/threads/trending-news` | `/api/alpaca/threads/sentiment-snapshot` | `/api/alpaca/threads/hourly-status` |
| Crypto (Alpaca) | `bettor-alpaca-crypto-6tta.onrender.com` | `/api/alpaca/crypto/threads/trending-news` | `/api/alpaca/crypto/threads/sentiment-snapshot` | `/api/alpaca/crypto/threads/hourly-status` |
| Options (Alpaca) | `bettor-alpaca-options-e0tj.onrender.com` | `/api/alpaca/options/threads/trending-news` | `/api/alpaca/options/threads/sentiment-snapshot` | `/api/alpaca/options/threads/hourly-status` |

**Note on perps trending-news specifically:** this job is an intentional
no-op right now (see `_run_perps_threads_trending_news`'s own docstring) --
crypto already owns that beat to avoid a real, previously-confirmed
duplicate-post bug (the same headline going out twice under two
different labels). Scheduling it is harmless (instant no-op response) and
kept for consistency/future re-enablement, but there's nothing to actually
watch for on that one specifically.

## Recommended cron-job.org setup (per route)

- **Method:** GET
- **Headers:** `Authorization: Bearer <that service's own CRON_SECRET>`
- **Schedule:** match what each job used to run at internally --
  trending-news every 30 min, sentiment-snapshot every 60 min, hourly-status
  every 60 min. Stagger each job's minute offset within its hour (e.g.
  :05, :10, :15) the way the old in-process registration used to, so they
  don't all land on the exact same tick and pile up.

That's 12 jobs total (3 x 4 services). Each returns a small JSON body
(`{"ok": true, "posted": ..., ...}`) -- cron-job.org's own execution
history shows the response, useful for spotting a job that's silently
failing without needing to check Render's logs directly.

## Verifying a route works

```bash
curl -X POST "https://bettor-dashboard-18ni.onrender.com/api/perps/threads/hourly-status" \
  -H "Authorization: Bearer <CRON_SECRET>"
```

A `401 {"ok": false, "error": "Unauthorized"}` means the header's secret
doesn't match that service's own `CRON_SECRET` -- double-check which
service's value you're using, they're independent per service.
