"""Root-level WSGI entrypoint for the Alpaca CRYPTO service specifically --
its own separate Render service, split out from the Alpaca stocks service
after a real, confirmed OOM crash loop from running both strategies in one
512MB process (see src/alpaca_crypto_server.py's own docstring). render.yaml's
own explicit `--chdir src alpaca_crypto_server:app` startCommand is what
Render actually uses; this exists for any host/tool that expects an
app.py-style entrypoint at the repo root."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from alpaca_crypto_server import app
