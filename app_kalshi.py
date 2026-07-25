"""Root-level WSGI entrypoint for the Kalshi Perps service specifically
(bettor-dashboard on Render) -- the Kalshi equivalent of app_schwab.py,
which serves the Schwab stocks service instead.

NOTE: Render's actual configured Start Command for bettor-dashboard is
`gunicorn app:app` (set directly in the Render dashboard, independent of
render.yaml's own startCommand) -- confirmed live when this file briefly
didn't exist under the name app.py and production broke with
`ModuleNotFoundError: No module named 'app'`. app.py (repo root) is the
real, load-bearing entrypoint; this file is a same-content copy under a
clearer name for anything not locked to that exact filename."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from perps_server import app

