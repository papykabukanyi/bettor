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

# Real, deliberate promotion per explicit user direction: every post this
# bot publishes to Threads, regardless of which of the 4 services or which
# caller in threads_post.py composed it, tags the bot's own site. Applied
# HERE (the 3 functions every single post funnels through, see below)
# rather than in each of threads_post.py's dozen-plus caption-builders --
# one choke point instead of a change repeated (and potentially missed) at
# every call site. Threads' own hard cap is 500 chars total -- this trims
# the CALLER's text if needed so the tag always fits, rather than letting
# Threads' API silently reject or mangle an over-length post.
PROMO_URL = "https://cumdev.onrender.com"
_PROMO_TAG = f"\n\n{PROMO_URL}"
_THREADS_MAX_CHARS = 500


def _with_promo_tag(text: str) -> str:
    text = text or ""
    if PROMO_URL in text:
        return text  # already tagged (e.g. a caller-supplied caption) -- don't duplicate
    budget = _THREADS_MAX_CHARS - len(_PROMO_TAG)
    if len(text) > budget:
        text = text[: max(0, budget - 1)] + "…"
    return f"{text}{_PROMO_TAG}" if text else PROMO_URL
_token_cache: dict[str, Any] = {}
# Real production finding: threads_post.is_configured() (called on every
# single /api/status poll -- confirmed live, external monitoring bots hit
# it roughly every 20s) calls this, and before a login has ever completed
# `_token_cache` stays permanently empty, meaning it would otherwise retry
# the real HF network lookup on EVERY poll, forever. This cooldown makes
# a "no tokens yet" result negative-cacheable for a while instead.
_PULL_RETRY_COOLDOWN_SEC = 300
_last_pull_attempt_ts = 0.0

# Real, confirmed production incident: a stored token was observed with
# only a 24h total lifetime (obtained_at -> expires_at), not the ~60 days
# Meta's docs describe for a genuine long-lived token -- yet calling the
# refresh endpoint directly on that same token, at only 21h old, succeeded
# immediately and returned a proper ~59-day expiry. The OLD 25h minimum
# age here was therefore actively harmful for a token in this state: it
# can never be satisfied before a 24h-lifetime token expires, permanently
# breaking Threads posting with no automatic recovery. Lowered to 2h --
# still a real floor in case Meta does reject a too-fresh token for some
# accounts, but with wide safety margin under any token lifetime this
# module might actually encounter, confirmed short-lived ones included.
_MIN_TOKEN_AGE_FOR_REFRESH_SEC = 2 * 3600
_REFRESH_MARGIN_SEC = 5 * 24 * 3600


# Real, confirmed production incident: all 4 services share ONE Threads
# account/token (see HF_MODEL_REPO's own docstring above), and each posts
# several DIFFERENT content types (hourly status, sentiment snapshot,
# trending news, trade entry/exit charts) on its own independent schedule.
# Every single post -- text or image -- goes through create -> poll ->
# publish (see _wait_for_container_ready below), so the REAL call volume
# against the shared account is a multiple of the visible post count, not
# 1:1. Confirmed live: "Threads API Rate Limit Exceeded" (error_subcode
# 4279002) fired across all 4 services within the same ~40-second window,
# for MULTIPLE different post types each -- i.e. once the account was
# rate-limited, every subsequent post attempt (from any of the 4
# processes) kept hitting the API and failing anyway, burning whatever
# quota headroom might otherwise have been recovering. This cooldown
# makes that failure state cheaply short-circuit locally (no network call
# at all) until it's had a real chance to clear, instead of every post
# type in every remaining scheduled cycle re-discovering the same failure
# the hard way.
#
# Real, confirmed gap found in review (2026-08-19): the cooldown below used
# to live ONLY in this process's own memory (time.monotonic(), which can't
# even be compared across processes) -- each of the 4 separate Render
# services discovered a shared-account rate limit independently, the hard
# way, instead of finding out the instant ANY of them hit it. Persisted to
# the same HF-hosted token record the OAuth tokens already use (one extra
# field, piggybacked on the existing push/pull machinery, wall-clock time
# so it's meaningful across processes) -- checked locally first (cheap,
# no network) and only refreshed from HF at most once per
# _RATE_LIMIT_HF_CHECK_COOLDOWN_SEC, so this doesn't turn every post
# attempt into an HF round trip.
_RATE_LIMIT_COOLDOWN_SEC = float(os.getenv("THREADS_RATE_LIMIT_COOLDOWN_SEC", "1200") or "1200")
_RATE_LIMIT_HF_CHECK_COOLDOWN_SEC = float(os.getenv("THREADS_RATE_LIMIT_HF_CHECK_COOLDOWN_SEC", "60") or "60")
_rate_limited_until = 0.0
_last_rate_limit_hf_check_ts = 0.0


def is_rate_limited() -> bool:
    """True while this process believes the shared Threads account is
    still within a recently-observed rate-limit cooldown window -- callers
    should skip attempting a post entirely (see create_and_publish_post/
    create_and_publish_image_post) rather than make a doomed API call.
    Also periodically checks HF for a cooldown set by a DIFFERENT one of
    the 4 processes sharing this account, so this process backs off too
    instead of discovering the same rate limit independently."""
    global _last_rate_limit_hf_check_ts
    now = time.time()
    if now < _rate_limited_until:
        return True
    if now - _last_rate_limit_hf_check_ts >= _RATE_LIMIT_HF_CHECK_COOLDOWN_SEC:
        _last_rate_limit_hf_check_ts = now
        _adopt_shared_rate_limit_from_hf()
    return time.time() < _rate_limited_until


def _adopt_shared_rate_limit_from_hf() -> None:
    global _rate_limited_until
    try:
        with _STATE_LOCK:
            pulled = _pull_tokens_from_hf()
        if pulled:
            shared_until = float(pulled.get("rate_limited_until") or 0.0)
            if shared_until > _rate_limited_until:
                _rate_limited_until = shared_until
                if shared_until > time.time():
                    logger.warning(
                        "[threads_client] adopting shared Threads rate-limit cooldown set by "
                        "another process -- pausing until %.0f more seconds from now",
                        shared_until - time.time(),
                    )
    except Exception as exc:
        logger.debug("[threads_client] could not check HF for a shared rate-limit cooldown: %s", exc)


def _note_rate_limited() -> None:
    global _rate_limited_until
    _rate_limited_until = time.time() + _RATE_LIMIT_COOLDOWN_SEC
    logger.warning(
        "[threads_client] Threads API rate limit hit -- pausing all posting from this "
        "process for %.0f minutes", _RATE_LIMIT_COOLDOWN_SEC / 60,
    )
    try:
        with _STATE_LOCK:
            record = dict(_token_cache)
            record["rate_limited_until"] = _rate_limited_until
            _token_cache["rate_limited_until"] = _rate_limited_until
        if record.get("access_token"):
            _push_tokens_to_hf(record)
    except Exception as exc:
        logger.debug("[threads_client] could not share this rate-limit cooldown via HF: %s", exc)


def _raise_for_status_with_body(resp: requests.Response) -> None:
    """Real gap found in review: plain resp.raise_for_status() raises an
    HTTPError whose message is just "400 Client Error: Bad Request for
    url: ..." -- Meta's own error detail (the actual reason a call was
    rejected: bad/expired creation_id, rate limit, content policy, etc.)
    lives in the response BODY, which raise_for_status() never includes.
    Confirmed live: a real, repeated 400 on threads_publish for a crypto
    trade-entry post was completely undiagnosable from the logs alone --
    every log line just said "400 Client Error", with no way to tell
    which of several possible causes it actually was."""
    if resp.status_code >= 400:
        body = resp.text[:500]
        if "4279002" in body or "Rate Limit Exceeded" in body:
            _note_rate_limited()
        raise requests.exceptions.HTTPError(f"{resp.status_code} error for url: {resp.url} -- body: {body}", response=resp)


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
    _raise_for_status_with_body(resp)
    short_lived = resp.json()

    long_lived_resp = requests.get(
        EXCHANGE_URL,
        params={
            "grant_type": "th_exchange_token", "client_secret": APP_SECRET,
            "access_token": short_lived["access_token"],
        },
        timeout=TIMEOUT_SEC,
    )
    _raise_for_status_with_body(long_lived_resp)
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
    _raise_for_status_with_body(resp)
    refreshed = resp.json()
    _save_tokens(access_token=refreshed["access_token"], expires_in=refreshed.get("expires_in", 0), user_id=None)
    return refreshed


def _push_tokens_to_hf(record: dict[str, Any]) -> None:
    if not HF_API_KEY:
        return

    def _upload() -> None:
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

    try:
        # Same hard-timeout protection as _pull_tokens_from_hf's own fix,
        # for the same reason -- this runs while _STATE_LOCK is held
        # (called from _save_tokens, reached from _refresh_long_lived_token
        # inside get_valid_access_token's own locked section), and
        # huggingface_hub's internal shared-session lock is a single
        # mechanism shared across its download AND upload calls alike.
        from server_common import call_with_hard_timeout
        call_with_hard_timeout(_upload, timeout_sec=_PULL_TOKENS_HF_TIMEOUT_SEC)
    except Exception as exc:
        logger.warning("[threads_client] token push to HF failed: %s", exc)


_PULL_TOKENS_HF_TIMEOUT_SEC = int(os.getenv("THREADS_PULL_TOKENS_HF_TIMEOUT_SEC", "10") or "10")


def _pull_tokens_from_hf() -> dict[str, Any] | None:
    if not HF_API_KEY:
        return None

    def _download() -> dict[str, Any] | None:
        from huggingface_hub import hf_hub_download
        path = hf_hub_download(
            repo_id=HF_MODEL_REPO, filename=_TOKEN_HF_FILENAME, repo_type="model", token=HF_API_KEY,
        )
        return json.loads(Path(path).read_text(encoding="utf-8"))

    try:
        # Real, confirmed production incident: this is called from BOTH
        # get_valid_access_token() and get_user_id() while holding
        # _STATE_LOCK -- huggingface_hub's own internal shared-session lock
        # can hang for minutes (the exact documented incident
        # server_common.call_with_hard_timeout's own docstring describes,
        # confirmed live on this same service's *_strategy.py durable-state
        # pulls), and unlike those callers this one hangs WHILE HOLDING A
        # LOCK -- so a single stuck call here doesn't just freeze itself,
        # it blocks every other caller of get_valid_access_token()/
        # get_user_id() too, including a plain /api/status health check.
        # Confirmed live: gunicorn WORKER TIMEOUT (300s) killed the perps
        # worker mid-request, stuck acquiring _STATE_LOCK, because this
        # call was never given the same hard-timeout protection every
        # other HF pull in this codebase already has.
        from server_common import call_with_hard_timeout
        return call_with_hard_timeout(_download, timeout_sec=_PULL_TOKENS_HF_TIMEOUT_SEC)
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


# Real, confirmed production incident: publishing immediately after
# create_resp returned a creation_id failed with a 400 "Media Not Found"
# (OAuthException code 24, error_subcode 4279009) on essentially EVERY
# post attempted over several hours -- text posts included, not just
# images. Threads' own container-processing step is asynchronous; Meta's
# documented pattern is to poll the container's own status field until it
# reports FINISHED before calling threads_publish, which this module
# never did. See _wait_for_container_ready below.
_CONTAINER_STATUS_POLL_INTERVAL_SEC = float(os.getenv("THREADS_CONTAINER_POLL_INTERVAL_SEC", "2") or "2")
_CONTAINER_STATUS_MAX_WAIT_SEC = float(os.getenv("THREADS_CONTAINER_MAX_WAIT_SEC", "30") or "30")


def _wait_for_container_ready(creation_id: str, token: str) -> str | None:
    """Polls the container's status until it's FINISHED (ready to publish)
    or reaches a terminal ERROR/EXPIRED state, up to
    _CONTAINER_STATUS_MAX_WAIT_SEC. Returns the last-seen status (None if
    every status check failed, or if the deadline was hit with the
    container still IN_PROGRESS) -- callers must check this before
    publishing.

    Real, confirmed live bug (2026-08-19): this used to return nothing and
    let the caller attempt threads_publish unconditionally, even on a
    container that had already reached ERROR or EXPIRED -- Meta's Graph API
    then rejects the publish with error_subcode 4279009 ("the requested
    resource does not exist"), a genuinely wasted API call against an
    already-shared, rate-limit-sensitive account (18-30 occurrences per
    service across 5 days). Returning the real status lets the caller skip
    that doomed call and fail with a clear reason instead."""
    deadline = time.monotonic() + _CONTAINER_STATUS_MAX_WAIT_SEC
    last_status: str | None = None
    while time.monotonic() < deadline:
        try:
            status_resp = requests.get(
                f"{API_BASE_URL}/{API_VERSION}/{creation_id}",
                params={"fields": "status", "access_token": token},
                timeout=TIMEOUT_SEC,
            )
            _raise_for_status_with_body(status_resp)
            last_status = status_resp.json().get("status")
        except Exception as exc:
            logger.debug("[threads_client] container status check failed, will retry: %s", exc)
            last_status = None
        if last_status in ("FINISHED", "PUBLISHED", "ERROR", "EXPIRED"):
            return last_status
        time.sleep(_CONTAINER_STATUS_POLL_INTERVAL_SEC)
    return last_status


def create_and_publish_post(text: str) -> str:
    """Threads posting is a real two-step process: create a media
    container, then separately publish it. Raises on any failure --
    callers (threads_post.py) are responsible for catching and logging,
    same "never let this block a real trade" contract as x_post.py had."""
    if is_rate_limited():
        raise RuntimeError("Threads API rate limit cooldown active -- skipping post attempt")
    text = _with_promo_tag(text)
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
    _raise_for_status_with_body(create_resp)
    creation_id = create_resp.json()["id"]
    status = _wait_for_container_ready(creation_id, token)
    if status in ("ERROR", "EXPIRED"):
        raise RuntimeError(f"Threads container {creation_id} reached terminal status {status} -- not attempting publish")

    publish_resp = requests.post(
        f"{API_BASE_URL}/{API_VERSION}/{user_id}/threads_publish",
        params={"creation_id": creation_id, "access_token": token},
        timeout=TIMEOUT_SEC,
    )
    _raise_for_status_with_body(publish_resp)
    return publish_resp.json()["id"]


def create_and_publish_image_post(image_url: str, text: str = "") -> str:
    """Same create-container-then-publish flow as create_and_publish_post,
    but media_type=IMAGE with a publicly-fetchable image_url -- Threads'
    own servers fetch the image from that URL themselves (there is no raw
    binary upload step in this API), which is why chart_snapshot.py's
    public_url_for() needs this service's own public onrender.com URL, not
    a local file path. Raises on any failure, same contract as
    create_and_publish_post -- callers must catch and log."""
    if is_rate_limited():
        raise RuntimeError("Threads API rate limit cooldown active -- skipping post attempt")
    text = _with_promo_tag(text)
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
    _raise_for_status_with_body(create_resp)
    creation_id = create_resp.json()["id"]
    status = _wait_for_container_ready(creation_id, token)
    if status in ("ERROR", "EXPIRED"):
        raise RuntimeError(f"Threads container {creation_id} reached terminal status {status} -- not attempting publish")

    publish_resp = requests.post(
        f"{API_BASE_URL}/{API_VERSION}/{user_id}/threads_publish",
        params={"creation_id": creation_id, "access_token": token},
        timeout=TIMEOUT_SEC,
    )
    _raise_for_status_with_body(publish_resp)
    return publish_resp.json()["id"]


# Threads' own documented carousel limits (same as Instagram's, which the
# Threads Graph API shares infrastructure with) -- a single-item "carousel"
# isn't a real carousel and 21+ items is rejected outright by the API, so
# callers get a clear local error instead of a wasted round trip.
CAROUSEL_MIN_ITEMS = 2
CAROUSEL_MAX_ITEMS = 20


def create_and_publish_carousel_post(image_urls: list[str], text: str = "") -> str:
    """Multiple images under ONE Threads post (a real carousel, swipeable
    on the timeline) -- distinct from calling create_and_publish_image_post
    N times, which would create N separate posts instead.

    Threads' carousel shape is a 3-step version of the plain image flow:
    1. Create one ITEM container per image (media_type=IMAGE,
       is_carousel_item=true, no text on the item itself -- the caption
       lives on the carousel container, not its children) and wait for
       each to reach FINISHED individually. Meta's API requires every
       child to already be ready before the carousel container referencing
       them can be created at all.
    2. Create the CAROUSEL container itself (children=<comma-separated
       item ids>, text=<caption>) and wait for IT to reach FINISHED.
    3. Publish the carousel container, same threads_publish call as every
       other post type here.

    Raises on any failure, including if any single item container fails --
    a partially-broken carousel isn't published at all, matching this
    module's existing "never publish something Meta will just reject"
    discipline (see the ERROR/EXPIRED check on the plain image/text posts)."""
    if len(image_urls) < CAROUSEL_MIN_ITEMS:
        raise ValueError(f"a carousel needs at least {CAROUSEL_MIN_ITEMS} images, got {len(image_urls)}")
    if len(image_urls) > CAROUSEL_MAX_ITEMS:
        raise ValueError(f"a carousel allows at most {CAROUSEL_MAX_ITEMS} images, got {len(image_urls)}")
    if is_rate_limited():
        raise RuntimeError("Threads API rate limit cooldown active -- skipping post attempt")
    text = _with_promo_tag(text)
    token = get_valid_access_token()
    user_id = get_user_id()
    if not token or not user_id:
        raise RuntimeError(
            "No valid Threads access token/user id -- complete the interactive login first "
            "(see get_authorization_url())."
        )

    item_ids: list[str] = []
    for image_url in image_urls:
        item_resp = requests.post(
            f"{API_BASE_URL}/{API_VERSION}/{user_id}/threads",
            params={"media_type": "IMAGE", "image_url": image_url, "is_carousel_item": "true", "access_token": token},
            timeout=TIMEOUT_SEC,
        )
        _raise_for_status_with_body(item_resp)
        item_id = item_resp.json()["id"]
        item_status = _wait_for_container_ready(item_id, token)
        if item_status in ("ERROR", "EXPIRED"):
            raise RuntimeError(f"Threads carousel item {item_id} ({image_url}) reached terminal status {item_status}")
        item_ids.append(item_id)

    carousel_resp = requests.post(
        f"{API_BASE_URL}/{API_VERSION}/{user_id}/threads",
        params={"media_type": "CAROUSEL", "children": ",".join(item_ids), "text": text, "access_token": token},
        timeout=TIMEOUT_SEC,
    )
    _raise_for_status_with_body(carousel_resp)
    creation_id = carousel_resp.json()["id"]
    status = _wait_for_container_ready(creation_id, token)
    if status in ("ERROR", "EXPIRED"):
        raise RuntimeError(f"Threads carousel container {creation_id} reached terminal status {status} -- not attempting publish")

    publish_resp = requests.post(
        f"{API_BASE_URL}/{API_VERSION}/{user_id}/threads_publish",
        params={"creation_id": creation_id, "access_token": token},
        timeout=TIMEOUT_SEC,
    )
    _raise_for_status_with_body(publish_resp)
    return publish_resp.json()["id"]
