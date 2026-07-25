"""Root-level WSGI entrypoint for the Kalshi Perps service specifically
(bettor-dashboard on Render) -- the Kalshi equivalent of app_schwab.py,
which serves the Schwab stocks service instead. render.yaml's own explicit
`--chdir src perps_server:app` startCommand is what Render actually uses;
this exists for any host/tool that expects an app.py-style entrypoint at
the repo root."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from perps_server import app

