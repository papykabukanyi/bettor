"""
Configuration loader for the Kalshi Perps trading bot.
Reads settings from .env file or environment variables.
"""
import os


def _bootstrap_env_from_dotenv() -> None:
    env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
    if not os.path.exists(env_path):
        return
    try:
        with open(env_path, "r", encoding="utf-8") as handle:
            lines = handle.read().splitlines()
    except Exception:
        return
    idx = 0
    while idx < len(lines):
        raw = lines[idx].strip()
        idx += 1
        if not raw or raw.startswith("#") or "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        if key == "KALSHI_PRIVATE_KEY" and "BEGIN RSA PRIVATE KEY" in value and "END RSA PRIVATE KEY" not in value:
            chunks = [value]
            while idx < len(lines):
                part = lines[idx].rstrip("\r")
                chunks.append(part)
                idx += 1
                if "END RSA PRIVATE KEY" in part:
                    break
            value = "\n".join(chunks)
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        os.environ.setdefault(key, value)


_bootstrap_env_from_dotenv()

# Hugging Face (dataset archive + trained direction model)
HF_API_KEY = os.getenv("HF_API_KEY", "")
HF_DATASET_REPO = os.getenv("HF_DATASET_REPO", "papylove/kalshi-perps-data")
HF_MODEL_REPO = os.getenv("HF_MODEL_REPO", "papylove/kalshi-perps-model")

# Kalshi credentials are read directly from the environment by
# data/kalshi_client.py (KALSHI_API_KEY, KALSHI_PRIVATE_KEY /
# KALSHI_PRIVATE_KEY_FILE) -- not duplicated here.

# ---------------------------------------------------------------------------
# Eastern-time date helper (used for daily retrain scheduling / partitioning)
# ---------------------------------------------------------------------------
import datetime as _dt


def et_today() -> _dt.date:
    """Return the 'effective today' in US Eastern time. After 10 PM ET rolls
    forward to the next calendar day."""
    try:
        import zoneinfo
        eastern = zoneinfo.ZoneInfo("America/New_York")
    except Exception:
        try:
            import pytz
            eastern = pytz.timezone("America/New_York")
        except Exception:
            return _dt.date.today()

    now_et = _dt.datetime.now(tz=eastern)
    if now_et.hour >= 22:
        return (now_et + _dt.timedelta(days=1)).date()
    return now_et.date()
