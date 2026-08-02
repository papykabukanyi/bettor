"""Meta Threads API -- OAuth 2.0 (authorization code grant) client, used to
post a note to Threads every time the perps bot (or the Alpaca stocks bot)
enters a real trade.

The App ID/Secret alone cannot post anything -- a human has to visit the
authorize URL, log into their actual Threads/Instagram account, and get
redirected back to this app's registered callback URL
(THREADS_REDIRECT_URI) with a one-time code. That code is exchanged here
for a short-lived access token, immediately exchanged again for a
long-lived one (~60 days), refreshable indefinitely thereafter without
another interactive login, as long as it's refreshed before it actually
expires.

Tokens are mirrored to HF_MODEL_REPO (same durable-state pattern already
used for the Kalshi bot's own state) since Render's disk is ephemeral --
otherwise every restart would lose the refresh-able token and force a
fresh interactive login.
"""
from __future__ import annotations

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

AUTHORIZE_URL = "https://threads.net/oauth/authorize"
TOKEN_URL = "https://graph.threads.net/oauth/access_token"
EXCHANGE_URL = "https://graph.threads.net/access_token"
REFRESH_URL = "https://graph.threads.net/refresh_access_token"
API_BASE_URL = "https://graph.threads.net"
API_VERSION = "v1.0"
TIMEOUT_SEC = 15

APP_ID = os.getenv("THREADS_APP_ID", "")
APP_SECRET = os.getenv("THREADS_APP_SECRET", "")
REDIRECT_URI = os.getenv("THREADS_REDIRECT_URI", "")
# threads_basic: read the account's own profile/user id (needed to post AS
# that account). threads_content_publish: create + publish a Threads post.
SCOPES = "threads_basic,threads_content_publish"

HF_API_KEY = os.getenv("HF_API_KEY", "")
HF_MODEL_REPO = os.getenv("HF_MODEL_REPO", "papylove/kalshi-perps-model")
_TOKEN_HF_FILENAME = "threads_tokens.json"

_STATE_LOCK = threading.Lock()
_token_cache: dict[str, Any] = {}
# Real production finding: threads_post.is_configured() (called on every
# single /api/status poll -- confirmed live, external monitoring bots hit
# it roughly every 20s) calls this, and before a login has ever completed
# `_token_cache` stays permanently empty, meaning it would otherwise retry
# the real HF network lookup on EVERY poll, forever. This cooldown makes
# a "no tokens yet" result negative-cacheable for a while instead.
_PULL_RETRY_COOLDOWN_SEC = 300
_last_pull_attempt_ts = 0.0

# Threads' own refresh endpoint requires the long-lived token to be at
# least 24h old -- refreshing too early just fails, so this margin is a
# genuine floor, not a stylistic choice. Refresh once within 5 days of the
# ~60-day expiry, giving a wide safety window without needing to refresh
# constantly.
_MIN_TOKEN_AGE_FOR_REFRESH_SEC = 25 * 3600
_REFRESH_MARGIN_SEC = 5 * 24 * 3600


def get_authorization_url(*, state: str = "") -> str:
    """The URL to send the account owner to. They log in on Threads' own
    site and get redirected back to THREADS_REDIRECT_URI with ?code=..."""
    params = {
        "client_id": APP_ID, "redirect_uri": REDIRECT_URI,
        "scope": SCOPES, "response_type": "code",
    }
    if state:
        params["state"] = state
    return f"{AUTHORIZE_URL}?{urlencode(params)}"


def _save_tokens(*, access_token: str, expires_in: float, user_id: str | None) -> None:
    now = time.time()
    record = {
        "access_token": access_token,
        "user_id": user_id or _token_cache.get("user_id"),
        "obtained_at": now,
        "expires_at": now + float(expires_in or 0),
    }
    _token_cache.update(record)
    _push_tokens_to_hf(record)


def exchange_code_for_tokens(code: str) -> dict[str, Any]:
    """Called from the /threadscallback route the moment Threads redirects
    back with a fresh authorization code. One-time per interactive login.
    Two real network round trips: the code exchanges for a SHORT-lived
    token, which is immediately exchanged again for the LONG-lived
    (~60-day) one this whole module actually operates on."""
    resp = requests.post(
        TOKEN_URL,
        data={
            "client_id": APP_ID, "client_secret": APP_SECRET, "grant_type": "authorization_code",
            "redirect_uri": REDIRECT_URI, "code": code,
        },
        timeout=TIMEOUT_SEC,
    )
    resp.raise_for_status()
    short_lived = resp.json()

    long_lived_resp = requests.get(
        EXCHANGE_URL,
        params={
            "grant_type": "th_exchange_token", "client_secret": APP_SECRET,
            "access_token": short_lived["access_token"],
        },
        timeout=TIMEOUT_SEC,
    )
    long_lived_resp.raise_for_status()
    long_lived = long_lived_resp.json()
    _save_tokens(
        access_token=long_lived["access_token"], expires_in=long_lived.get("expires_in", 0),
        user_id=short_lived.get("user_id"),
    )
    return long_lived


def _refresh_long_lived_token(access_token: str) -> dict[str, Any]:
    resp = requests.get(
        REFRESH_URL, params={"grant_type": "th_refresh_token", "access_token": access_token}, timeout=TIMEOUT_SEC,
    )
    resp.raise_for_status()
    refreshed = resp.json()
    _save_tokens(access_token=refreshed["access_token"], expires_in=refreshed.get("expires_in", 0), user_id=None)
    return refreshed


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
                repo_id=HF_MODEL_REPO, repo_type="model", commit_message="update threads oauth tokens",
            )
        finally:
            os.unlink(tmp_path)
    except Exception as exc:
        logger.warning("[threads_client] token push to HF failed: %s", exc)


def _pull_tokens_from_hf() -> dict[str, Any] | None:
    if not HF_API_KEY:
        return None
    try:
        from huggingface_hub import hf_hub_download
        path = hf_hub_download(
            repo_id=HF_MODEL_REPO, filename=_TOKEN_HF_FILENAME, repo_type="model", token=HF_API_KEY,
        )
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception as exc:
        logger.info("[threads_client] no tokens on HF yet (or fetch failed): %s", exc)
        return None


def get_valid_access_token() -> str | None:
    """Returns a currently-valid access token, refreshing it first if it's
    within _REFRESH_MARGIN_SEC of expiry (and old enough for Threads to
    actually allow a refresh). None if no login has ever completed -- no
    code can substitute for that first interactive step."""
    global _last_pull_attempt_ts
    with _STATE_LOCK:
        if not _token_cache:
            now_mono = time.monotonic()
            if now_mono - _last_pull_attempt_ts >= _PULL_RETRY_COOLDOWN_SEC:
                _last_pull_attempt_ts = now_mono
                pulled = _pull_tokens_from_hf()
                if pulled:
                    _token_cache.update(pulled)
        if not _token_cache.get("access_token"):
            return None

        now = time.time()
        token_age_sec = now - _token_cache.get("obtained_at", now)
        near_expiry = now > _token_cache.get("expires_at", 0) - _REFRESH_MARGIN_SEC
        if near_expiry and token_age_sec >= _MIN_TOKEN_AGE_FOR_REFRESH_SEC:
            try:
                _refresh_long_lived_token(_token_cache["access_token"])
            except Exception as exc:
                logger.warning(
                    "[threads_client] token refresh failed -- will keep using the current token "
                    "until it actually expires: %s", exc,
                )
        return _token_cache.get("access_token")


def get_user_id() -> str | None:
    """The Threads user id posts are made as -- captured from the initial
    OAuth exchange, needed as the {threads-user-id} path segment on every
    post call."""
    global _last_pull_attempt_ts
    with _STATE_LOCK:
        if not _token_cache:
            now_mono = time.monotonic()
            if now_mono - _last_pull_attempt_ts >= _PULL_RETRY_COOLDOWN_SEC:
                _last_pull_attempt_ts = now_mono
                pulled = _pull_tokens_from_hf()
                if pulled:
                    _token_cache.update(pulled)
        return _token_cache.get("user_id")


def create_and_publish_post(text: str) -> str:
    """Threads posting is a real two-step process: create a media
    container, then separately publish it. Raises on any failure --
    callers (threads_post.py) are responsible for catching and logging,
    same "never let this block a real trade" contract as x_post.py had."""
    token = get_valid_access_token()
    user_id = get_user_id()
    if not token or not user_id:
        raise RuntimeError(
            "No valid Threads access token/user id -- complete the interactive login first "
            "(see get_authorization_url())."
        )
    create_resp = requests.post(
        f"{API_BASE_URL}/{API_VERSION}/{user_id}/threads",
        params={"media_type": "TEXT", "text": text, "access_token": token},
        timeout=TIMEOUT_SEC,
    )
    create_resp.raise_for_status()
    creation_id = create_resp.json()["id"]

    publish_resp = requests.post(
        f"{API_BASE_URL}/{API_VERSION}/{user_id}/threads_publish",
        params={"creation_id": creation_id, "access_token": token},
        timeout=TIMEOUT_SEC,
    )
    publish_resp.raise_for_status()
    return publish_resp.json()["id"]


def create_and_publish_image_post(image_url: str, text: str = "") -> str:
    """Same create-container-then-publish flow as create_and_publish_post,
    but media_type=IMAGE with a publicly-fetchable image_url -- Threads'
    own servers fetch the image from that URL themselves (there is no raw
    binary upload step in this API), which is why chart_snapshot.py's
    public_url_for() needs this service's own public onrender.com URL, not
    a local file path. Raises on any failure, same contract as
    create_and_publish_post -- callers must catch and log."""
    token = get_valid_access_token()
    user_id = get_user_id()
    if not token or not user_id:
        raise RuntimeError(
            "No valid Threads access token/user id -- complete the interactive login first "
            "(see get_authorization_url())."
        )
    create_resp = requests.post(
        f"{API_BASE_URL}/{API_VERSION}/{user_id}/threads",
        params={"media_type": "IMAGE", "image_url": image_url, "text": text, "access_token": token},
        timeout=TIMEOUT_SEC,
    )
    create_resp.raise_for_status()
    creation_id = create_resp.json()["id"]

    publish_resp = requests.post(
        f"{API_BASE_URL}/{API_VERSION}/{user_id}/threads_publish",
        params={"creation_id": creation_id, "access_token": token},
        timeout=TIMEOUT_SEC,
    )
    publish_resp.raise_for_status()
    return publish_resp.json()["id"]
