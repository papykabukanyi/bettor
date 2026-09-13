"""Composes the 4 existing, otherwise-unmodified Flask apps (perps=
app_kalshi.py, stocks=src/alpaca_server.py, crypto=src/alpaca_crypto_server.py,
options=src/alpaca_options_server.py) into ONE WSGI application, so all 4
can run as a single process inside one Hugging Face Docker Space instead
of 4 separate Render services -- see
/Users/bitcoinmobile/.claude/plans/tranquil-petting-melody.md (or its
eventual docs/RENDER_TO_HF_MIGRATION.md writeup) for the full migration
plan and why this is the right shape.

Uses werkzeug's own DispatcherMiddleware, not a route-prefixing rewrite:
it dispatches by matching the FIRST path segment BEFORE either mounted
app's own Flask routing ever runs, stripping that prefix via SCRIPT_NAME.
Perps is the DEFAULT app (mounted at "/", matching its existing "hub"
role linking to the other 3) -- its own routes (/api/perps/..., /api/
status, /api/trades, /chart/..., every cron-job.org URL already pointed
at it) need ZERO changes, same paths, same behavior as on Render today.
Stocks/crypto/options each gain a new external mount prefix (/stocks,
/crypto, /options) they don't have on Render (where they're each on
their own domain instead) -- their OWN internal route definitions
(already /api/alpaca/status, /api/alpaca/crypto/tick, etc.) don't need
touching either; the prefix is purely where DispatcherMiddleware decides
to route an incoming request, not something the mounted app itself has
to know about.

This resolves every confirmed route collision across the 4 apps (/,
/chart/<path:filename>, /api/server/activity all defined identically in
all 4) automatically -- each only ever receives requests that already
had its own mount prefix stripped off, so two apps both defining "/"
internally never actually collide once mounted at different prefixes.

app_kalshi.py's own /alpaca, /alpaca-crypto, /alpaca-options hub-redirect
routes (see that file) already read their target from ALPACA_SERVER_URL/
ALPACA_CRYPTO_SERVER_URL/ALPACA_OPTIONS_SERVER_URL env vars (sync: false
in render.yaml -- already manually configured per environment, not
hardcoded). Zero code change needed there either: set those same env
vars to "/stocks"/"/crypto"/"/options" (relative paths) for this merged
Space's own environment, and those existing redirects resolve correctly
through this same DispatcherMiddleware mount -- e.g. ALPACA_SERVER_URL=
"/stocks" makes /alpaca redirect to "/stocks/alpaca", which strips to
"/alpaca" and matches alpaca_server.py's own aliased dashboard route.
Render's own deployment (during the staged verify-before-cutover window)
keeps its existing absolute cross-domain URL values untouched -- this
file has no effect there at all, since Render's own render.yaml still
points each service at its own standalone app object directly.

Run locally the same way gunicorn will run it in the Docker image:
    gunicorn combined_app:application --bind 0.0.0.0:7860 --workers 1 --threads 8 --timeout 300

Still --workers 1: the safety property that matters (exactly one Python
process, so each market's in-process APScheduler instance is
initialized exactly once, never duplicated into multiple live-order-
placing copies) is about worker COUNT, not thread count, and is fully
preserved here as a single merged process. --threads 8 (up from each
individual service's own --threads 1) is a free mitigation for a real
NEW risk merging introduces: with only 1 thread across all 4 markets'
web traffic combined, a slow request in any one market would block
every market's dashboard/API responsiveness at once, not just its own
-- APScheduler's own background job execution runs on its own separate
executor threads regardless of gunicorn's thread count, so this doesn't
affect trading-loop timing at all, only concurrent HTTP request
handling.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from werkzeug.middleware.dispatcher import DispatcherMiddleware  # noqa: E402

import app_kalshi  # noqa: E402
import alpaca_server  # noqa: E402
import alpaca_crypto_server  # noqa: E402
import alpaca_options_server  # noqa: E402

application = DispatcherMiddleware(app_kalshi.app, {
    "/stocks": alpaca_server.app,
    "/crypto": alpaca_crypto_server.app,
    "/options": alpaca_options_server.app,
})
