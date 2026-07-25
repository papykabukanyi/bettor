"""Root-level WSGI entrypoint for the Schwab stocks service specifically
(bettor-schwab on Render) -- the Schwab equivalent of app_kalshi.py, which
serves the Kalshi Perps service instead. render.yaml's own explicit
`--chdir src schwab_server:app` startCommand is what Render actually uses;
this exists for any host/tool that expects an app.py-style entrypoint at
the repo root."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from schwab_server import app
