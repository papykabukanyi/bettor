"""Kalshi REST API auth + request core.

Implements authenticated request signing exactly as described in:
https://docs.kalshi.com/getting_started/quick_start_authenticated_requests

This is the ONLY place that knows how to sign a Kalshi request. Every other
module (kalshi_perps.py) calls `_request_json` from here rather than
reimplementing signing.
"""
from __future__ import annotations

import base64
import datetime as dt
import json
import os
import threading
import time
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

KALSHI_BASE_URL = str(
    os.getenv("KALSHI_BASE_URL", "https://external-api.kalshi.com/trade-api/v2")
).rstrip("/")
KALSHI_TIMEOUT_SEC = int(os.getenv("KALSHI_TIMEOUT_SEC", "15") or "15")
_BASE_PATH = urlparse(KALSHI_BASE_URL).path.rstrip("/")
_TS_OFFSET_MS = 0
_TS_OFFSET_LOCK = threading.Lock()
_TS_OFFSET_EXPIRY = 0.0
_TS_OFFSET_TTL_SEC = max(60, int(os.getenv("KALSHI_TIMESTAMP_SYNC_TTL_SEC", "300") or "300"))


def _clean_secret_value(raw: str | None) -> str:
    value = str(raw or "").strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        value = value[1:-1].strip()
    return value.replace("\r\n", "\n").replace("\\n", "\n").strip()


def _looks_like_pem(value: str) -> bool:
    text = str(value or "")
    return "BEGIN" in text and "PRIVATE KEY" in text and "END" in text


def _decode_base64_pem(value: str) -> str:
    try:
        decoded = base64.b64decode(value, validate=True).decode("utf-8")
    except Exception:
        return ""
    decoded = decoded.replace("\r\n", "\n").strip()
    return decoded if _looks_like_pem(decoded) else ""


def _read_private_key_file(path_value: str) -> str:
    if not path_value:
        return ""
    raw_path = Path(path_value)
    candidates = [raw_path]
    if not raw_path.is_absolute():
        candidates.append(Path.cwd() / raw_path)
        candidates.append(Path(__file__).resolve().parents[2] / raw_path)
    for candidate in candidates:
        try:
            if not candidate.exists():
                continue
            content = _clean_secret_value(candidate.read_text(encoding="utf-8"))
            if _looks_like_pem(content):
                return content
            decoded = _decode_base64_pem(content)
            if decoded:
                return decoded
        except Exception:
            continue
    return ""


def _read_dotenv_value(name: str) -> str:
    env_path = Path(__file__).resolve().parents[2] / ".env"
    if not env_path.exists():
        return ""
    try:
        lines = env_path.read_text(encoding="utf-8").splitlines()
    except Exception:
        return ""
    idx = 0
    while idx < len(lines):
        raw = lines[idx].strip()
        idx += 1
        if not raw or raw.startswith("#") or "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        key = key.strip()
        value = value.strip()
        if key != name:
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
        return _clean_secret_value(value)
    return ""


def _load_private_key_pem() -> bytes:
    inline = _clean_secret_value(os.getenv("KALSHI_PRIVATE_KEY", "")) or _read_dotenv_value("KALSHI_PRIVATE_KEY")
    if inline:
        if _looks_like_pem(inline):
            return inline.encode("utf-8")
        decoded = _decode_base64_pem(inline)
        if decoded:
            return decoded.encode("utf-8")
        raise RuntimeError("Kalshi private key format invalid in KALSHI_PRIVATE_KEY.")

    from_file = _read_private_key_file(_clean_secret_value(os.getenv("KALSHI_PRIVATE_KEY_FILE", "")))
    if from_file:
        return from_file.encode("utf-8")
    raise RuntimeError("Kalshi credentials missing. Set KALSHI_API_KEY and KALSHI_PRIVATE_KEY.")


def _load_private_key():
    pem = _load_private_key_pem()
    return serialization.load_pem_private_key(pem, password=None)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _refresh_timestamp_offset(*, force: bool = False) -> int:
    global _TS_OFFSET_MS, _TS_OFFSET_EXPIRY
    now = time.monotonic()
    with _TS_OFFSET_LOCK:
        if not force and now < _TS_OFFSET_EXPIRY:
            return _TS_OFFSET_MS
    offset = 0
    try:
        response = requests.get(f"{KALSHI_BASE_URL}/exchange/status", timeout=KALSHI_TIMEOUT_SEC)
        date_header = str(response.headers.get("Date") or "").strip()
        if date_header:
            parsed = parsedate_to_datetime(date_header)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=dt.timezone.utc)
            server_ms = int(parsed.timestamp() * 1000)
            offset = server_ms - _now_ms()
    except Exception:
        offset = 0
    with _TS_OFFSET_LOCK:
        _TS_OFFSET_MS = int(offset)
        _TS_OFFSET_EXPIRY = time.monotonic() + _TS_OFFSET_TTL_SEC
        return _TS_OFFSET_MS


def _signed_headers(method: str, sign_path: str) -> dict[str, str]:
    api_key = _clean_secret_value(os.getenv("KALSHI_API_KEY", "")) or _read_dotenv_value("KALSHI_API_KEY")
    if not api_key:
        raise RuntimeError("Kalshi API key missing. Set KALSHI_API_KEY.")
    ts_ms = str(_now_ms() + _refresh_timestamp_offset())
    key = _load_private_key()
    message = f"{ts_ms}{method.upper()}{sign_path.split('?', 1)[0]}".encode("utf-8")
    signature = key.sign(
        message,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.DIGEST_LENGTH,
        ),
        hashes.SHA256(),
    )
    return {
        "KALSHI-ACCESS-KEY": api_key,
        "KALSHI-ACCESS-TIMESTAMP": ts_ms,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode("ascii"),
    }


def _request_json(
    method: str,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    payload: dict[str, Any] | None = None,
    auth: bool = False,
) -> dict[str, Any]:
    clean_path = "/" + path.lstrip("/")
    url = f"{KALSHI_BASE_URL}{clean_path}"
    body_text = ""
    response = None
    # 3 attempts: covers a 401 timestamp-expired retry AND a short backoff
    # retry on 429 (confirmed live -- a burst of candle/market calls across
    # 16 tickers in one collection cycle can trip Kalshi's own rate limit).
    # A 429 is transient by definition, so a brief wait-and-retry is the
    # correct response, not an immediate hard failure.
    max_attempts = 3
    for attempt in range(max_attempts):
        headers: dict[str, str] = {}
        if auth:
            sign_path = f"{_BASE_PATH}{clean_path}"
            headers.update(_signed_headers(method, sign_path))
        if payload is not None:
            headers["Content-Type"] = "application/json"
        response = requests.request(
            method=method.upper(),
            url=url,
            params=params,
            data=json.dumps(payload) if payload is not None else None,
            headers=headers,
            timeout=KALSHI_TIMEOUT_SEC,
        )
        if response.status_code < 400:
            break
        body_text = response.text[:400]
        if (
            auth
            and response.status_code == 401
            and "header_timestamp_expired" in body_text.lower()
            and attempt < max_attempts - 1
        ):
            _refresh_timestamp_offset(force=True)
            continue
        if response.status_code == 429 and attempt < max_attempts - 1:
            time.sleep(1.5 * (attempt + 1))
            continue
        raise RuntimeError(f"Kalshi API error {response.status_code}: {body_text}")
    if response is None:
        raise RuntimeError("Kalshi API request failed before response.")
    data = response.json()
    if not isinstance(data, dict):
        raise RuntimeError("Kalshi API returned non-object JSON payload.")
    return data


def get_exchange_status() -> dict[str, Any]:
    return _request_json("GET", "/exchange/status", auth=False)
