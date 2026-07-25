"""Charles Schwab Trader API -- OAuth 2.0 (authorization code grant) client.

Unlike Kalshi (a static API key + RSA request signing), Schwab requires a
real interactive login: a human has to visit the authorize URL, log into
their actual Schwab account (with 2FA), and get redirected back to this
app's registered callback URL (SCHWAB_REDIRECT_URI) with a one-time code.
That code is exchanged here for an access_token (~30min lifetime) and a
refresh_token (~7-day lifetime, confirmed via Schwab's own docs). Nothing
in this module can substitute for that human step -- it can only make the
callback endpoint exist and handle the token exchange/refresh once a code
arrives.

Tokens are mirrored to the HF_STOCK_MODEL_REPO (same durable-state pattern
already used for the Kalshi bot's own state) since Render's disk is
ephemeral -- otherwise every restart would lose the refresh token and force
a fresh interactive login.
"""
from __future__ import annotations

import base64
import json
import logging
import os
import tempfile
import threading
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import requests

logger = logging.getLogger(__name__)

AUTHORIZE_URL = "https://api.schwabapi.com/v1/oauth/authorize"
TOKEN_URL = "https://api.schwabapi.com/v1/oauth/token"
API_BASE_URL = "https://api.schwabapi.com"
TIMEOUT_SEC = 15

CLIENT_ID = os.getenv("SCHWAB_CLIENT_ID", "")
CLIENT_SECRET = os.getenv("SCHWAB_CLIENT_SECRET", "")
REDIRECT_URI = os.getenv("SCHWAB_REDIRECT_URI", "")
HF_API_KEY = os.getenv("HF_API_KEY", "")
HF_STOCK_MODEL_REPO = os.getenv("HF_STOCK_MODEL_REPO", "papylove/schwab-model")
_TOKEN_HF_FILENAME = "schwab_tokens.json"

_STATE_LOCK = threading.Lock()
_token_cache: dict[str, Any] = {}

# Access tokens are refreshed a bit before their real expiry so a request
# mid-call never races an exact-boundary expiration.
_REFRESH_MARGIN_SEC = 60


def get_authorization_url(*, state: str = "") -> str:
    """The URL to send the account owner to. They log in on Schwab's own
    site and get redirected back to SCHWAB_REDIRECT_URI with ?code=..."""
    params = {"client_id": CLIENT_ID, "redirect_uri": REDIRECT_URI}
    if state:
        params["state"] = state
    return f"{AUTHORIZE_URL}?{urlencode(params)}"


def _basic_auth_header() -> dict[str, str]:
    raw = f"{CLIENT_ID}:{CLIENT_SECRET}".encode("utf-8")
    return {"Authorization": f"Basic {base64.b64encode(raw).decode('ascii')}"}


def _save_tokens(tokens: dict[str, Any]) -> None:
    now = time.time()
    record = {
        "access_token": tokens["access_token"],
        "refresh_token": tokens.get("refresh_token") or _token_cache.get("refresh_token"),
        "obtained_at": now,
        "expires_at": now + float(tokens.get("expires_in", 1800)),
    }
    _token_cache.update(record)
    _push_tokens_to_hf(record)


def exchange_code_for_tokens(code: str) -> dict[str, Any]:
    """Called from the /schcallback route the moment Schwab redirects back
    with a fresh authorization code. One-time per interactive login (or
    per re-login once the refresh token itself expires after ~7 days)."""
    resp = requests.post(
        TOKEN_URL,
        headers={**_basic_auth_header(), "Content-Type": "application/x-www-form-urlencoded"},
        data={"grant_type": "authorization_code", "code": code, "redirect_uri": REDIRECT_URI},
        timeout=TIMEOUT_SEC,
    )
    resp.raise_for_status()
    tokens = resp.json()
    _save_tokens(tokens)
    return tokens


def _refresh_tokens(refresh_token: str) -> dict[str, Any]:
    resp = requests.post(
        TOKEN_URL,
        headers={**_basic_auth_header(), "Content-Type": "application/x-www-form-urlencoded"},
        data={"grant_type": "refresh_token", "refresh_token": refresh_token},
        timeout=TIMEOUT_SEC,
    )
    resp.raise_for_status()
    tokens = resp.json()
    _save_tokens(tokens)
    return tokens


def _push_tokens_to_hf(record: dict[str, Any]) -> None:
    if not HF_API_KEY:
        return
    try:
        from huggingface_hub import HfApi
        api = HfApi(token=HF_API_KEY)
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
            tmp.write(json.dumps(record, indent=2))
            tmp_path = tmp.name
        try:
            api.upload_file(
                path_or_fileobj=tmp_path, path_in_repo=_TOKEN_HF_FILENAME,
                repo_id=HF_STOCK_MODEL_REPO, repo_type="model", commit_message="update schwab oauth tokens",
            )
        finally:
            os.unlink(tmp_path)
    except Exception as exc:
        logger.warning("[schwab_client] token push to HF failed: %s", exc)


def _pull_tokens_from_hf() -> dict[str, Any] | None:
    if not HF_API_KEY:
        return None
    try:
        from huggingface_hub import hf_hub_download
        path = hf_hub_download(
            repo_id=HF_STOCK_MODEL_REPO, filename=_TOKEN_HF_FILENAME, repo_type="model", token=HF_API_KEY,
        )
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception as exc:
        logger.info("[schwab_client] no tokens on HF yet (or fetch failed): %s", exc)
        return None


def get_valid_access_token() -> str | None:
    """Returns a currently-valid access token, refreshing it first if
    needed. None if no login has ever completed (or the 7-day refresh
    token itself has expired, in which case a fresh interactive login via
    get_authorization_url() is unavoidable -- no code can substitute for
    that)."""
    with _STATE_LOCK:
        if not _token_cache:
            pulled = _pull_tokens_from_hf()
            if pulled:
                _token_cache.update(pulled)
        if not _token_cache.get("refresh_token"):
            return None
        if time.time() < _token_cache.get("expires_at", 0) - _REFRESH_MARGIN_SEC:
            return _token_cache["access_token"]
        try:
            _refresh_tokens(_token_cache["refresh_token"])
            return _token_cache["access_token"]
        except Exception as exc:
            logger.warning("[schwab_client] token refresh failed -- likely needs a fresh interactive login: %s", exc)
            return None


def _auth_headers() -> dict[str, str]:
    token = get_valid_access_token()
    if not token:
        raise RuntimeError(
            "No valid Schwab access token -- complete the interactive login first "
            "(see get_authorization_url())."
        )
    return {"Authorization": f"Bearer {token}"}


def get(path: str, *, params: dict[str, Any] | None = None) -> Any:
    """Authenticated GET against the Schwab API. `path` is relative to
    API_BASE_URL (e.g. "/marketdata/v1/pricehistory")."""
    resp = requests.get(
        f"{API_BASE_URL}{path}", headers=_auth_headers(), params=params or {}, timeout=TIMEOUT_SEC,
    )
    resp.raise_for_status()
    return resp.json()


def post(path: str, *, json_body: dict[str, Any]) -> requests.Response:
    """Authenticated POST -- returns the raw response (not .json()) since
    order placement responses carry the new order's ID in the Location
    header, not a JSON body (confirmed Schwab/TDA-inherited API behavior)."""
    resp = requests.post(
        f"{API_BASE_URL}{path}", headers={**_auth_headers(), "Content-Type": "application/json"},
        json=json_body, timeout=TIMEOUT_SEC,
    )
    resp.raise_for_status()
    return resp


def delete(path: str) -> requests.Response:
    resp = requests.delete(f"{API_BASE_URL}{path}", headers=_auth_headers(), timeout=TIMEOUT_SEC)
    resp.raise_for_status()
    return resp


_account_hash_cache: dict[str, Any] = {"hash": None, "computed_at": 0.0}
_ACCOUNT_HASH_CACHE_TTL_SEC = 24 * 3600
SCHWAB_ACCOUNT_NUMBER = os.getenv("SCHWAB_ACCOUNT_NUMBER", "")


def get_account_hash() -> str:
    """The encrypted account hash every /accounts/{accountHash}/... call
    needs in place of the real account number -- fetched once from
    /accounts/accountNumbers and cached (it doesn't change). If
    SCHWAB_ACCOUNT_NUMBER is set (useful when the login has access to more
    than one account), that specific account's hash is used; otherwise the
    first (and normally only) account returned is used."""
    now = time.time()
    if _account_hash_cache["hash"] and (now - _account_hash_cache["computed_at"]) < _ACCOUNT_HASH_CACHE_TTL_SEC:
        return _account_hash_cache["hash"]
    accounts = get("/trader/v1/accounts/accountNumbers")
    if not accounts:
        raise RuntimeError("Schwab returned no linked accounts for this login.")
    chosen = accounts[0]
    if SCHWAB_ACCOUNT_NUMBER:
        chosen = next((a for a in accounts if a.get("accountNumber") == SCHWAB_ACCOUNT_NUMBER), accounts[0])
    account_hash = chosen["hashValue"]
    _account_hash_cache.update({"hash": account_hash, "computed_at": now})
    return account_hash


# ---------------------------------------------------------------------------
# Order placement + tracking. Confirmed via Schwab's own API + the
# (community, but schema-accurate) schwab-py client: orderStrategyType
# TRIGGER + a nested OCO child is how a single order submission places an
# entry that, once filled, automatically arms a linked take-profit (LIMIT)
# / stop-loss (STOP) pair where either one filling cancels the other --
# Schwab tracks and executes the exit itself, unlike Kalshi (which forced
# a manual poll-and-place-a-new-order loop for every exit). This is a
# materially safer mechanism: the stop-loss is live on the exchange the
# INSTANT the entry fills, not only whenever this process's own next
# scheduled tick happens to run.
# ---------------------------------------------------------------------------
def _equity_leg(instruction: str, symbol: str, quantity: float) -> dict[str, Any]:
    return {
        "instruction": instruction, "quantity": quantity,
        "instrument": {"symbol": symbol, "assetType": "EQUITY"},
    }


def build_bracket_order(
    *, symbol: str, quantity: float, entry_instruction: str, exit_instruction: str,
    take_profit_price: float, stop_loss_price: float, entry_price: float | None = None,
) -> dict[str, Any]:
    """entry_instruction/exit_instruction: "BUY"/"SELL" for a long (buy to
    open, sell to close); a short would use "SELL_SHORT"/"BUY_TO_COVER".
    entry_price=None places the entry as a MARKET order; a value places it
    as a LIMIT order at that price."""
    entry_order: dict[str, Any] = {
        "orderType": "MARKET" if entry_price is None else "LIMIT",
        "session": "NORMAL", "duration": "DAY", "orderStrategyType": "TRIGGER",
        "orderLegCollection": [_equity_leg(entry_instruction, symbol, quantity)],
        "childOrderStrategies": [{
            "orderStrategyType": "OCO",
            "childOrderStrategies": [
                {
                    "orderType": "LIMIT", "session": "NORMAL", "duration": "GOOD_TILL_CANCEL",
                    "orderStrategyType": "SINGLE", "price": f"{take_profit_price:.2f}",
                    "orderLegCollection": [_equity_leg(exit_instruction, symbol, quantity)],
                },
                {
                    "orderType": "STOP", "session": "NORMAL", "duration": "GOOD_TILL_CANCEL",
                    "orderStrategyType": "SINGLE", "stopPrice": f"{stop_loss_price:.2f}",
                    "orderLegCollection": [_equity_leg(exit_instruction, symbol, quantity)],
                },
            ],
        }],
    }
    if entry_price is not None:
        entry_order["price"] = f"{entry_price:.2f}"
    return entry_order


def place_order(order_spec: dict[str, Any]) -> str:
    """Places a real order and returns its order ID (extracted from the
    response's Location header -- Schwab/TDA-inherited APIs return no JSON
    body on a successful order placement)."""
    account_hash = get_account_hash()
    resp = post(f"/trader/v1/accounts/{account_hash}/orders", json_body=order_spec)
    location = resp.headers.get("Location", "")
    order_id = location.rstrip("/").rsplit("/", 1)[-1]
    if not order_id:
        raise RuntimeError(f"Order placed but no order ID found in response (Location={location!r})")
    return order_id


def get_order(order_id: str) -> dict[str, Any]:
    account_hash = get_account_hash()
    return get(f"/trader/v1/accounts/{account_hash}/orders/{order_id}")


def get_orders(*, max_results: int = 50, status: str | None = None) -> list[dict[str, Any]]:
    account_hash = get_account_hash()
    params: dict[str, Any] = {"maxResults": max_results}
    if status:
        params["status"] = status
    result = get(f"/trader/v1/accounts/{account_hash}/orders", params=params)
    return result if isinstance(result, list) else []


def cancel_order(order_id: str) -> None:
    account_hash = get_account_hash()
    delete(f"/trader/v1/accounts/{account_hash}/orders/{order_id}")


def get_quote(symbol: str) -> dict[str, Any]:
    """A single real-time quote -- much lighter than pulling 35 days of
    minute bars just to read the current price, used by the fast
    position-management poll."""
    data = get(f"/marketdata/v1/{symbol}/quotes")
    return (data.get(symbol) or {}).get("quote") or {}


def get_account_balance() -> dict[str, Any]:
    """Real cash + equity balances for the linked account -- used only in
    "live" mode; "simulate" mode tracks its own virtual balance in local
    state instead (see schwab_strategy.py)."""
    account_hash = get_account_hash()
    data = get(f"/trader/v1/accounts/{account_hash}", params={"fields": "positions"})
    return (data.get("securitiesAccount") or {}).get("currentBalances") or {}
