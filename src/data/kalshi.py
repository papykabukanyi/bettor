"""Kalshi API helpers.

Public market data is fetched from Kalshi's external API without auth.
Order execution uses RSA key-based authentication (PKCS1v15 + SHA256).

Required environment variables for order execution:
  KALSHI_API_KEY       - Your Kalshi API key ID (UUID)
  KALSHI_PRIVATE_KEY   - PEM-encoded RSA private key (multi-line OK in .env)
"""

from __future__ import annotations

import base64
import os
import time
from typing import Any
from urllib.parse import urlparse

import requests

KALSHI_BASE_URL = os.getenv(
    "KALSHI_BASE_URL",
    "https://external-api.kalshi.com/trade-api/v2",
).rstrip("/")
KALSHI_TIMEOUT_SEC = int(os.getenv("KALSHI_TIMEOUT_SEC", "15"))

# Path prefix used when signing (everything after the hostname).
_KALSHI_BASE_PATH = urlparse(KALSHI_BASE_URL).path.rstrip("/")  # e.g. /trade-api/v2


def _load_private_key():
    """Load and cache the RSA private key from environment."""
    from cryptography.hazmat.primitives.serialization import load_pem_private_key

    pem = os.getenv("KALSHI_PRIVATE_KEY", "").strip()

    # Handle case where dotenv stores with literal \n instead of real newlines
    if pem and "\\n" in pem and "\n" not in pem:
        pem = pem.replace("\\n", "\n")

    # Fallback: load from a .pem file path if env var is a file path
    if not pem or not pem.startswith("-----"):
        key_file = os.getenv("KALSHI_PRIVATE_KEY_FILE", "").strip()
        if key_file and os.path.exists(key_file):
            with open(key_file, "r") as f:
                pem = f.read().strip()

    if not pem:
        raise RuntimeError(
            "Kalshi private key is missing. Set KALSHI_PRIVATE_KEY in environment."
        )

    return load_pem_private_key(pem.encode("ascii"), password=None)


def _auth_headers(method: str, path: str) -> dict[str, str]:
    """Build Kalshi RSA-signed request headers.

    Signing message: {timestamp_ms}{METHOD}{/full/path}
    Algorithm: RSA-PSS + SHA256 (MAX_LENGTH salt) for 2048-bit keys;
               PKCS1v15 + SHA256 for other key sizes.
    """
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import padding

    api_key_id = os.getenv("KALSHI_API_KEY", "").strip()
    if not api_key_id:
        raise RuntimeError(
            "Kalshi API key ID is missing. Set KALSHI_API_KEY in environment."
        )

    private_key = _load_private_key()

    timestamp_ms = str(int(time.time() * 1000))
    msg = (timestamp_ms + method.upper() + path).encode("ascii")

    # Kalshi uses RSA-PSS with DIGEST_LENGTH salt (per official docs)
    signature = private_key.sign(
        msg,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.DIGEST_LENGTH,
        ),
        hashes.SHA256(),
    )

    sig_b64 = base64.b64encode(signature).decode("ascii")

    return {
        "KALSHI-ACCESS-KEY": api_key_id,
        "KALSHI-ACCESS-TIMESTAMP": timestamp_ms,
        "KALSHI-ACCESS-SIGNATURE": sig_b64,
        "Content-Type": "application/json",
    }


def _request_json(
    method: str,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    payload: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    auth: bool = False,
) -> dict[str, Any]:
    url = f"{KALSHI_BASE_URL}/{path.lstrip('/')}"

    if auth:
        # The signing path must include the full URL path from /
        sign_path = _KALSHI_BASE_PATH + "/" + path.lstrip("/")
        request_headers = _auth_headers(method, sign_path)
        if headers:
            request_headers.update(headers)
    else:
        request_headers = dict(headers or {})

    resp = requests.request(
        method=method.upper(),
        url=url,
        params=params,
        json=payload,
        headers=request_headers,
        timeout=KALSHI_TIMEOUT_SEC,
    )

    data: dict[str, Any]
    try:
        data = resp.json() if resp.text else {}
    except Exception:
        data = {"raw": (resp.text or "")[:4000]}

    if resp.status_code >= 400:
        msg = (
            data.get("error")
            or data.get("message")
            or data.get("detail")
            or data.get("raw")
            or f"HTTP {resp.status_code}"
        )
        raise RuntimeError(f"Kalshi API error ({resp.status_code}): {msg}")
    return data


def _parse_list_response(
    data: dict[str, Any], preferred_key: str
) -> tuple[list[dict[str, Any]], str | None]:
    rows = data.get(preferred_key)
    if not isinstance(rows, list):
        rows = data.get("data")
    if not isinstance(rows, list):
        rows = []

    cursor = data.get("cursor") or data.get("next_cursor")
    if cursor is None:
        pagination = data.get("pagination")
        if isinstance(pagination, dict):
            cursor = pagination.get("cursor") or pagination.get("next_cursor")

    clean_rows = [r for r in rows if isinstance(r, dict)]
    return clean_rows, (str(cursor) if cursor else None)


def list_markets(
    *,
    limit: int = 200,
    cursor: str | None = None,
    status: str | None = "open",
    event_ticker: str | None = None,
    series_ticker: str | None = None,
) -> dict[str, Any]:
    params: dict[str, Any] = {"limit": max(1, min(int(limit or 200), 500))}
    if cursor:
        params["cursor"] = cursor
    if status:
        params["status"] = status
    if event_ticker:
        params["event_ticker"] = event_ticker
    if series_ticker:
        params["series_ticker"] = series_ticker

    data = _request_json("GET", "/markets", params=params)
    markets, next_cursor = _parse_list_response(data, "markets")
    return {"markets": markets, "cursor": next_cursor, "raw": data}


def list_events(
    *,
    limit: int = 200,
    cursor: str | None = None,
    status: str | None = None,
    series_ticker: str | None = None,
) -> dict[str, Any]:
    params: dict[str, Any] = {"limit": max(1, min(int(limit or 200), 500))}
    if cursor:
        params["cursor"] = cursor
    if status:
        params["status"] = status
    if series_ticker:
        params["series_ticker"] = series_ticker

    data = _request_json("GET", "/events", params=params)
    events, next_cursor = _parse_list_response(data, "events")
    return {"events": events, "cursor": next_cursor, "raw": data}


def place_order(order_payload: dict[str, Any]) -> dict[str, Any]:
    """Place a Kalshi order using RSA-signed authentication.

    Tries the primary portfolio/orders endpoint, falls back to /orders.
    """
    last_error: Exception | None = None

    for path in ("/portfolio/orders", "/orders"):
        try:
            return _request_json("POST", path, payload=order_payload, auth=True)
        except Exception as exc:
            last_error = exc
            if "(404)" in str(exc):
                continue
            raise

    if last_error:
        raise last_error
    raise RuntimeError("Kalshi order placement failed")

