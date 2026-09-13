# Migration off Render: one merged Hugging Face Docker Space

All 4 trading services (perps=Kalshi, stocks/crypto/options=Alpaca) now
run merged into ONE process on a single Hugging Face Docker Space
(`papylove/bettor-trading-bots`, private) instead of 4 separate,
individually-metered Render services -- explicit user direction, driven
by Render's per-service cost. See `/Users/bitcoinmobile/.claude/plans/
tranquil-petting-melody.md` for the original design plan.

## Architecture

`combined_app.py` (repo root) composes the 4 existing, otherwise-
unmodified Flask apps (`app_kalshi.py`, `src/alpaca_server.py`,
`src/alpaca_crypto_server.py`, `src/alpaca_options_server.py`) via
werkzeug's `DispatcherMiddleware`:

- perps stays the DEFAULT mount (`/`) -- its own existing "hub" role,
  zero URL changes from before.
- stocks/crypto/options get new mount prefixes: `/stocks`, `/crypto`,
  `/options`. Their own internal routes (`/api/alpaca/status`, `/api/
  alpaca/crypto/tick`, etc.) are unchanged; the prefix is purely where
  DispatcherMiddleware routes an incoming request before either app's
  own Flask routing ever sees it.

`Dockerfile` builds one image (`python:3.11-slim`) and runs `gunicorn
combined_app:application --workers 1 --threads 8 --timeout 300`. Still
`--workers 1`: exactly one process, so each market's own in-process
APScheduler instance is created exactly once (more workers would
duplicate live order placement). `--threads 8` (up from each individual
service's own `--threads 1` on Render) so a slow request in one market
can't block every market's web/API responsiveness at once now that they
share a process.

**Threads content jobs** (trending-news/sentiment-snapshot/hourly-status,
12 total across 4 markets) run as this process's own in-process
APScheduler jobs again -- see each server file's own
`_ensure_background_jobs_started()`. They briefly ran via external
cron-job.org triggers instead (see `docs/CRON_JOB_MIGRATION.md`) purely
to cut load off Render's metered pricing; that's moot on a flat $0.03/hr
Space, so they were restored to remove cron-job.org as an external
dependency entirely. The `CRON_SECRET`-gated HTTP routes for each job
still exist in the codebase as manual/fallback triggers, just unused by
default.

## Why the Space is private, and why that's fine now

The Space holds real trading dashboards and (via Space secrets, never in
the repo) real Kalshi/Alpaca/HF credentials. Making it public would have
been needed ONLY to let an external service like cron-job.org trigger
routes on a schedule without a full-access HF token -- since that
external-trigger dependency was removed (see above), the Space can stay
private with no operational downside. The owner can still view it
normally in their own browser while logged into Hugging Face; only
fully-anonymous/external HTTP access is blocked (HF returns 404, not 401,
for unauthenticated requests to a private Space -- confirmed live).

**One real gap this leaves**: Meta's Threads OAuth login flow
(`/threads/authorize`, `/threadscallback`) requires the user's own
browser to be redirected there by Meta, unauthenticated w.r.t. HF -- a
private Space would 404 that redirect. Threads' access token is
long-lived and auto-refreshing (see `threads_client.py`), so this is a
rare, one-time bootstrap concern, not routine traffic -- if a fresh
interactive login is ever needed again, temporarily flip the Space
public for that one flow, then back to private.

## Real bugs found and fixed while building this

1. **Template/static folder resolution.** `alpaca_server.py`/
   `alpaca_crypto_server.py`/`alpaca_options_server.py` each created
   their Flask app with `template_folder="templates"` (a relative path)
   and a made-up app-name string (not their real dotted module name).
   Flask resolves a relative `template_folder` against
   `get_root_path(import_name)`, which falls back to `os.getcwd()` when
   `import_name` isn't a real entry in `sys.modules` -- this only ever
   worked on Render by coincidence of its own `gunicorn --chdir src ...`
   startCommand making cwd literally equal to `src/`. Fixed with
   absolute paths (`str(SRC_DIR / "templates")`), mirroring
   `app_kalshi.py`'s own already-correct pattern.
2. **`.env` baked into the Docker image.** No `.dockerignore` existed
   before this work -- the first local build's bare `COPY . .` copied
   the repo's real `.env` straight into the image, confirmed live when
   the resulting container found real Kalshi credentials despite every
   env var being left empty in `docker run`. Fixed with a `.dockerignore`
   wildcard (`.env*`, with an explicit `!.env.example` carve-out).
3. **A real leaked secrets file in git history.** Separately (not part
   of this Docker work, but found in the same pass): `.env 2` (a macOS
   duplicate-file artifact with real Kalshi/HF credentials) had been
   committed and pushed to this repo's public GitHub remote for over a
   month. Removed from tracking; every credential that was ever in it
   was rotated.
4. **`ALPACA_TRADING_BASE_URL` double `/v2`.** Set to the full endpoint
   URL including `/v2` (copying Alpaca's own dashboard display exactly)
   -- the code's own request-building already appends `/v2/...` itself,
   producing `.../v2/v2/account` (404). Fixed by setting the Space
   variable to the bare `https://paper-api.alpaca.markets` (no `/v2`
   suffix), matching the code's own default value shape.

## Secrets / variables on the Space

**Secrets** (encrypted, never displayed again after being set):
`KALSHI_API_KEY`, `KALSHI_PRIVATE_KEY`, `HF_API_KEY`,
`ALPACA_API_KEY_ID`, `ALPACA_API_SECRET_KEY`, `CRON_SECRET` (unused by
default now, kept for the manual/fallback trigger routes).

**Variables** (plain, visible in Space settings): `HF_DATASET_REPO`/
`HF_MODEL_REPO` and the same pair for `_ALPACA`/`_ALPACA_CRYPTO`/
`_ALPACA_OPTIONS`; `ALPACA_TRADING_BASE_URL` (currently paper --
`https://paper-api.alpaca.markets`, no `/v2` suffix); `PUBLIC_BASE_URL`
(the Space's own `https://papylove-bettor-trading-bots.hf.space`, so
`chart_snapshot.py`'s `public_url_for()` serves chart images from this
process's own `/chart/<filename>` route exactly like it did via
`RENDER_EXTERNAL_URL` on Render); `ALPACA_SERVER_URL=/stocks`,
`ALPACA_CRYPTO_SERVER_URL=/crypto`, `ALPACA_OPTIONS_SERVER_URL=/options`
(relative paths so `app_kalshi.py`'s existing hub-redirect routes resolve
correctly through `combined_app.py`'s own mount points); every market's
own `*_LIVE_TRADING_ENABLED` flag.

**Not configured yet, optional**: `THREADS_APP_ID`/`THREADS_APP_SECRET`/
`THREADS_REDIRECT_URI` (only needed for a fresh interactive OAuth login,
not routine posting -- the existing long-lived token already works via
`HF_API_KEY`/`HF_MODEL_REPO`), and any of the optional news-source keys
(`SERPAPI_API_KEY`, `CRYPTOPANIC_API_KEY`, `NEWSDATA_API_KEY`,
`API_NINJAS_API_KEY`) -- all degrade gracefully if absent.

## Cutover status

As of this writing: the Space is deployed and confirmed running (all 4
markets' dashboards/status routes verified 200, in-process fast_check
loop confirmed executing on schedule, durable state confirmed recovering
correctly from HF on a fresh container). Every market's
`*_LIVE_TRADING_ENABLED` is `0` (dry-run) -- **Render remains the sole
live trader** until the Space has been watched handling real data/
decisions correctly and the user explicitly approves flipping live
trading on here while Render is turned off, never both live at once
(both Alpaca-based markets and Kalshi perps share one real account each
across hosts).

## Files removed as part of this migration

Render/Cloudflare-Pages-specific artifacts with no equivalent need on
the new Space: `render.yaml`, `app.py`, `app_alpaca.py`,
`app_alpaca_crypto.py`, `app_alpaca_options.py`, `Procfile`,
`wrangler.toml`, `public/`, `functions/` (a Cloudflare Pages reverse
proxy whose entire purpose was fronting the old Render backend).

**Important operational note**: `render.yaml` had `autoDeploy: true`.
Deleting `app.py` (which Render's own dashboard Start Command pointed at
directly, `gunicorn app:app`) and pushing to `main` would have triggered
an immediate live redeploy that crashes Render's still-running services
(`ModuleNotFoundError: No module named 'app'`) -- since Render was still
the sole live trader at the time these files were removed, this cleanup
was committed but the push to `origin/main` was deliberately held back
until Render's auto-deploy is disabled (or the service is paused/
cancelled outright) as a separate, explicit step.
