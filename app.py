"""Root-level WSGI entrypoint that Render's ACTUAL configured Start Command
for the bettor-dashboard service uses: `gunicorn app:app` (a setting made
directly in the Render dashboard, independent of render.yaml's own
startCommand -- confirmed live after removing this file broke production
with `ModuleNotFoundError: No module named 'app'`). Must keep re-exporting
the Kalshi Perps app under this exact filename/name until that dashboard
setting is changed to match render.yaml. app_kalshi.py is the same thing
under a clearer name for anything that isn't locked to this one."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from perps_server import app
