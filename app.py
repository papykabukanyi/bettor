"""Root-level WSGI entrypoint for the Kalshi Perps service specifically
(bettor-dashboard on Render) -- kept for any host/tool that expects an
app.py at the repo root rather than render.yaml's explicit
`--chdir src perps_server:app` startCommand. The Schwab service
(bettor-schwab) has its own equivalent, app_schwab.py."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from perps_server import app

