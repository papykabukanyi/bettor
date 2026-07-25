"""Render's ACTUAL configured Start Command for the bettor-dashboard
service is `gunicorn app:app` (set directly in the Render dashboard's
Settings, independent of render.yaml's own startCommand) -- confirmed live
when this file didn't exist under this exact name and production broke
with `ModuleNotFoundError: No module named 'app'`.

All the real Kalshi Perps logic lives in app_kalshi.py (repo root) --
this file must keep re-exporting its `app` under the name "app" so that
Start Command keeps working. Do not remove this file unless the Render
dashboard's Start Command is also changed to point at app_kalshi:app
first."""
from app_kalshi import app
