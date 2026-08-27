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
# threads_manage_replies: reply to a thread, hide/unhide a reply, set who
# can reply on a new post (reply_control). threads_keyword_search: search
# public Threads posts by keyword -- per Meta's own docs (verified
# 2026-08-24), Standard Access limits this to the AUTHENTICATED USER'S OWN
# posts only; searching the wider public conversation needs Meta's App
# Review to grant Advanced Access for this specific permission first, same
# gate every one of these newer permissions sits behind.
# threads_profile_discovery: look up a public profile/its posts by
# username -- Standard Access only resolves a handful of Meta's own
# official accounts (@meta, @threads, @instagram, @facebook); Advanced
# Access is required for general public profile lookup. threads_location_tagging:
# search for a location to tag on a new post. Adding these to SCOPES only
# takes effect on the NEXT interactive login (see get_authorization_url) --
# an already-issued long-lived token keeps whatever scopes it was granted
# under, so a re-auth is required before any of the 4 new permissions'
# calls will actually succeed.
SCOPES = "threads_basic,threads_content_publish,threads_manage_replies,threads_keyword_search,threads_profile_discovery,threads_location_tagging"

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


def create_and_publish_post(
    text: str, *, reply_to_id: str | None = None, reply_control: str | None = None,
) -> str:
    """Threads posting is a real two-step process: create a media
    container, then separately publish it. Raises on any failure --
    callers (threads_post.py) are responsible for catching and logging,
    same "never let this block a real trade" contract as x_post.py had.

    `reply_to_id` (requires threads_manage_replies) makes this a REPLY to
    an existing post/reply instead of a new top-level post -- same
    container-then-publish flow, Meta's API just treats the result as a
    reply when this is set (see Meta's own "Create Replies" docs, verified
    2026-08-24). `reply_control` (also threads_manage_replies) restricts
    who can reply to THIS new post: "everyone" (default), "accounts_you_follow",
    "mentioned_only", "parent_post_author_only", or "followers_only"."""
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
    params = {"media_type": "TEXT", "text": text, "access_token": token}
    if reply_to_id:
        params["reply_to_id"] = reply_to_id
    if reply_control:
        params["reply_control"] = reply_control
    create_resp = requests.post(
        f"{API_BASE_URL}/{API_VERSION}/{user_id}/threads",
        params=params,
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


def create_and_publish_image_post(
    image_url: str, text: str = "", *, reply_to_id: str | None = None, reply_control: str | None = None,
    location_id: str | None = None,
) -> str:
    """Same create-container-then-publish flow as create_and_publish_post,
    but media_type=IMAGE with a publicly-fetchable image_url -- Threads'
    own servers fetch the image from that URL themselves (there is no raw
    binary upload step in this API), which is why chart_snapshot.py's
    public_url_for() needs this service's own public onrender.com URL, not
    a local file path. Raises on any failure, same contract as
    create_and_publish_post -- callers must catch and log.

    `reply_to_id`/`reply_control` -- see create_and_publish_post's own
    docstring, identical meaning here. `location_id` (requires
    threads_location_tagging, see search_locations) tags this post with a
    location resolved from that search."""
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
    params = {"media_type": "IMAGE", "image_url": image_url, "text": text, "access_token": token}
    if reply_to_id:
        params["reply_to_id"] = reply_to_id
    if reply_control:
        params["reply_control"] = reply_control
    if location_id:
        params["location_id"] = location_id
    create_resp = requests.post(
        f"{API_BASE_URL}/{API_VERSION}/{user_id}/threads",
        params=params,
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


def create_and_publish_carousel_post(
    image_urls: list[str], text: str = "", *, reply_to_id: str | None = None,
    reply_control: str | None = None,
) -> str:
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
    discipline (see the ERROR/EXPIRED check on the plain image/text posts).

    `reply_to_id`/`reply_control` -- see create_and_publish_post's own
    docstring, identical meaning here; applied to the top-level CAROUSEL
    container, not the individual item containers."""
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

    carousel_params = {"media_type": "CAROUSEL", "children": ",".join(item_ids), "text": text, "access_token": token}
    if reply_to_id:
        carousel_params["reply_to_id"] = reply_to_id
    if reply_control:
        carousel_params["reply_control"] = reply_control
    carousel_resp = requests.post(
        f"{API_BASE_URL}/{API_VERSION}/{user_id}/threads",
        params=carousel_params,
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


# ---------------------------------------------------------------------------
# Keyword search / location search / profile discovery / reply management --
# the 4 additional Threads permissions requested (2026-08-23). Each is its
# own read-mostly capability with its own daily quota, separate from the
# create/publish rate-limit cooldown above (is_rate_limited()/_note_rate_limited
# track error_subcode 4279002 specifically, a POSTING-only limit) -- none of
# the functions below check or set it.
#
# threads_keyword_search and threads_profile_discovery are both functionally
# crippled under Meta's default "Standard Access": keyword search only
# returns the AUTHENTICATED USER'S OWN posts, and profile_lookup only
# resolves a handful of Meta's own official accounts (@meta, @threads,
# @instagram, @facebook) -- both need Meta's App Review to grant "Advanced
# Access" for that specific permission before they do anything broader.
# Nothing in this codebase calls these on a schedule for that reason --
# they're available for on-demand/manual use until that review clears.
# ---------------------------------------------------------------------------


def search_keyword_posts(
    query: str, *, search_type: str = "TOP", search_mode: str = "KEYWORD",
    media_type: str | None = None, author_username: str | None = None,
    since: str | None = None, until: str | None = None, limit: int = 25,
) -> list[dict[str, Any]]:
    """Public Threads posts matching `query` (see Meta's "Keyword and Topic
    Tag Search" docs, verified 2026-08-23). Under Standard Access this only
    ever returns the authenticated account's OWN posts -- see module note
    above. `search_type`: TOP or RECENT. `search_mode`: KEYWORD or TAG (tag
    search omits the leading #). Raises on any failure, same contract as
    every other call in this module."""
    token = get_valid_access_token()
    if not token:
        raise RuntimeError(
            "No valid Threads access token -- complete the interactive login first "
            "(see get_authorization_url())."
        )
    params: dict[str, Any] = {
        "q": query, "search_type": search_type, "search_mode": search_mode,
        "limit": limit, "access_token": token,
    }
    if media_type:
        params["media_type"] = media_type
    if author_username:
        params["author_username"] = author_username
    if since:
        params["since"] = since
    if until:
        params["until"] = until
    resp = requests.get(f"{API_BASE_URL}/{API_VERSION}/keyword_search", params=params, timeout=TIMEOUT_SEC)
    _raise_for_status_with_body(resp)
    return resp.json().get("data", [])


def search_locations(
    *, query: str | None = None, latitude: float | None = None, longitude: float | None = None,
) -> list[dict[str, Any]]:
    """Locations matching a text query and/or a coordinate pair, for
    tagging a subsequent post via its `id` as create_and_publish_post's/
    create_and_publish_image_post's `location_id` (requires
    threads_location_tagging; see Meta's "Location Tagging" docs, verified
    2026-08-23). At least one of `query` or the (`latitude`, `longitude`)
    pair is required -- Meta's own endpoint rejects a call with neither."""
    if not query and (latitude is None or longitude is None):
        raise ValueError("search_locations needs `query` and/or both `latitude` and `longitude`")
    token = get_valid_access_token()
    if not token:
        raise RuntimeError(
            "No valid Threads access token -- complete the interactive login first "
            "(see get_authorization_url())."
        )
    params: dict[str, Any] = {"access_token": token}
    if query:
        params["q"] = query
    if latitude is not None:
        params["latitude"] = latitude
    if longitude is not None:
        params["longitude"] = longitude
    resp = requests.get(f"{API_BASE_URL}/{API_VERSION}/location_search", params=params, timeout=TIMEOUT_SEC)
    _raise_for_status_with_body(resp)
    return resp.json().get("data", [])


def lookup_public_profile(username: str) -> dict[str, Any] | None:
    """Public profile info (follower/like/quote/repost/view counts, bio,
    verification status) for `username` (requires threads_profile_discovery;
    see Meta's "Threads Profiles" docs, verified 2026-08-23). Under Standard
    Access this only resolves a handful of Meta's own official accounts
    (@meta, @threads, @instagram, @facebook) -- see module note above.
    Returns None if the profile can't be resolved (404), raises on any
    other failure."""
    token = get_valid_access_token()
    if not token:
        raise RuntimeError(
            "No valid Threads access token -- complete the interactive login first "
            "(see get_authorization_url())."
        )
    resp = requests.get(
        f"{API_BASE_URL}/{API_VERSION}/profile_lookup",
        params={"username": username, "access_token": token},
        timeout=TIMEOUT_SEC,
    )
    if resp.status_code == 404:
        return None
    _raise_for_status_with_body(resp)
    return resp.json()


def get_pending_replies(
    media_id: str, *, reverse: bool = True, approval_status: str | None = None,
) -> list[dict[str, Any]]:
    """Replies to `media_id` awaiting this account's moderation (requires
    threads_manage_replies; see Meta's "Reply Management" docs, verified
    2026-08-23) -- each item's `id` is what manage_reply below acts on.
    `approval_status`: "pending" or "ignored" to filter, omit for all."""
    token = get_valid_access_token()
    if not token:
        raise RuntimeError(
            "No valid Threads access token -- complete the interactive login first "
            "(see get_authorization_url())."
        )
    params: dict[str, Any] = {"reverse": str(reverse).lower(), "access_token": token}
    if approval_status:
        params["approval_status"] = approval_status
    resp = requests.get(
        f"{API_BASE_URL}/{API_VERSION}/{media_id}/pending_replies", params=params, timeout=TIMEOUT_SEC,
    )
    _raise_for_status_with_body(resp)
    return resp.json().get("data", [])


def manage_reply(reply_id: str, *, hide: bool) -> bool:
    """Hides (`hide=True`) or unhides (`hide=False`) a reply to one of this
    account's posts (requires threads_manage_replies). A hidden reply stays
    visible to anyone who navigates to it directly -- Meta's own documented
    behavior, not a bug in this wrapper. Returns Meta's own `success` flag."""
    token = get_valid_access_token()
    if not token:
        raise RuntimeError(
            "No valid Threads access token -- complete the interactive login first "
            "(see get_authorization_url())."
        )
    resp = requests.post(
        f"{API_BASE_URL}/{API_VERSION}/{reply_id}/manage_reply",
        params={"hide": str(hide).lower(), "access_token": token},
        timeout=TIMEOUT_SEC,
    )
    _raise_for_status_with_body(resp)
    return bool(resp.json().get("success"))


# ── Recent-posts listing (backs the public /api/threads/posts route in
# app_kalshi.py, see that route's own docstring) -- Meta's own {threads-
# user-id}/threads GET endpoint (verified live 2026-08-27) is the single
# real source of truth for "everything this account has posted," across
# ALL of this codebase's separate Render services (perps, stocks, crypto,
# options) -- they all post as the SAME Threads account/token, so there's
# no need to reconstruct a feed from each service's own separate local
# state; Meta already keeps the one real, complete, correctly-ordered list.
LIST_POSTS_FIELDS = "id,media_type,media_url,permalink,text,timestamp,shortcode,is_quote_post"
_LIST_POSTS_CACHE_TTL_SEC = float(os.getenv("THREADS_LIST_POSTS_CACHE_TTL_SEC", "20") or "20")
_list_posts_cache: tuple[list[dict[str, Any]], float] | None = None
_list_posts_lock = threading.Lock()


def list_recent_posts(*, limit: int = 25, since_id: str | None = None) -> list[dict[str, Any]]:
    """The account's own most recent Threads posts, newest first, straight
    from Meta -- not reconstructed from any one service's own internal
    call log. `limit` is capped at 50 (Meta's own single-page max used
    here; a real "load more" would need real cursor pagination, not needed
    for a recent-activity feed). `since_id` (a post id from a previous
    call) truncates the list to posts newer than it, for cheap incremental
    polling -- Meta returns newest-first, so this just stops once it's
    seen that id.

    The raw 50-post fetch is cached for _LIST_POSTS_CACHE_TTL_SEC
    (`since_id` truncation happens AFTER the cache read, so many different
    `since_id` values reuse the same one real Meta call) -- protects
    against Meta's own rate limits if something polls this frequently.
    Raises on any failure (no token, HTTP error) -- same convention as
    every other function in this module; the caller (the Flask route)
    decides how to present that to an anonymous public caller."""
    global _list_posts_cache
    now = time.time()
    with _list_posts_lock:
        cached = _list_posts_cache
        if cached and (now - cached[1]) < _LIST_POSTS_CACHE_TTL_SEC:
            posts = cached[0]
        else:
            token = get_valid_access_token()
            if not token:
                raise RuntimeError(
                    "No valid Threads access token -- complete the interactive login first "
                    "(see get_authorization_url())."
                )
            user_id = get_user_id()
            resp = requests.get(
                f"{API_BASE_URL}/{API_VERSION}/{user_id}/threads",
                params={"fields": LIST_POSTS_FIELDS, "limit": 50, "access_token": token},
                timeout=TIMEOUT_SEC,
            )
            _raise_for_status_with_body(resp)
            posts = resp.json().get("data", [])
            _list_posts_cache = (posts, now)

    if since_id:
        truncated = []
        for post in posts:
            if post.get("id") == since_id:
                break
            truncated.append(post)
        posts = truncated
    return posts[: max(1, min(limit, 50))]


# ── Durable posts archive -- Meta's own {threads-user-id}/threads endpoint
# above only ever shows a shallow recent window (a single 50-item page,
# see list_recent_posts's own docstring), so a caller wanting a real,
# ever-growing historical feed (see docs/PUBLIC_THREADS_API.md) needs
# something durable on this side. Render's own disk is ephemeral (wiped on
# every restart/redeploy), so this is mirrored to HF_MODEL_REPO -- the same
# durable-state pattern this module already uses for OAuth tokens above
# (_push_tokens_to_hf/_pull_tokens_from_hf) -- kept sync'd by an external
# scheduler (cron-job.org) hitting POST /api/threads/posts/sync (see
# app_kalshi.py) on a fixed interval, since this codebase has no
# APScheduler job of its own dedicated to Threads-only upkeep.
_POSTS_ARCHIVE_HF_FILENAME = "threads_posts_archive.json"
_POSTS_ARCHIVE_HF_TIMEOUT_SEC = int(os.getenv("THREADS_POSTS_ARCHIVE_HF_TIMEOUT_SEC", "10") or "10")
_POSTS_ARCHIVE_MAX_SIZE = int(os.getenv("THREADS_POSTS_ARCHIVE_MAX_SIZE", "5000") or "5000")
_posts_archive_lock = threading.Lock()
_posts_archive_cache: list[dict[str, Any]] | None = None


def _pull_posts_archive_from_hf() -> list[dict[str, Any]] | None:
    if not HF_API_KEY:
        return None

    def _download() -> list[dict[str, Any]]:
        from huggingface_hub import hf_hub_download
        path = hf_hub_download(
            repo_id=HF_MODEL_REPO, filename=_POSTS_ARCHIVE_HF_FILENAME, repo_type="model", token=HF_API_KEY,
        )
        return json.loads(Path(path).read_text(encoding="utf-8"))

    try:
        from server_common import call_with_hard_timeout
        return call_with_hard_timeout(_download, timeout_sec=_POSTS_ARCHIVE_HF_TIMEOUT_SEC)
    except Exception as exc:
        logger.info("[threads_client] no posts archive on HF yet (or fetch failed): %s", exc)
        return None


def _push_posts_archive_to_hf(archive: list[dict[str, Any]]) -> None:
    if not HF_API_KEY:
        return

    def _upload() -> None:
        from huggingface_hub import HfApi
        api = HfApi(token=HF_API_KEY)
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
            tmp.write(json.dumps(archive, indent=2))
            tmp_path = tmp.name
        try:
            api.upload_file(
                path_or_fileobj=tmp_path, path_in_repo=_POSTS_ARCHIVE_HF_FILENAME,
                repo_id=HF_MODEL_REPO, repo_type="model", commit_message="update threads posts archive",
            )
        finally:
            os.unlink(tmp_path)

    try:
        from server_common import call_with_hard_timeout
        call_with_hard_timeout(_upload, timeout_sec=_POSTS_ARCHIVE_HF_TIMEOUT_SEC)
    except Exception as exc:
        logger.warning("[threads_client] posts archive push to HF failed: %s", exc)


def get_posts_archive() -> list[dict[str, Any]]:
    """The full durable archive, newest first -- loaded once per process
    lifetime (see sync_posts_archive for how it grows) and cached in
    memory after that, same shape as list_recent_posts's own posts."""
    global _posts_archive_cache
    with _posts_archive_lock:
        if _posts_archive_cache is None:
            _posts_archive_cache = _pull_posts_archive_from_hf() or []
        return _posts_archive_cache


def sync_posts_archive() -> dict[str, Any]:
    """Fetches this account's current recent posts straight from Meta
    (bypassing list_recent_posts's own short cache -- this runs on its own
    external schedule, not a hot request path) and merges any not already
    in the durable archive into it, newest first, deduped by `id`. Trimmed
    to _POSTS_ARCHIVE_MAX_SIZE oldest-dropped-first so this can't grow
    unbounded. Meant to be called on a fixed interval by an external
    scheduler (see POST /api/threads/posts/sync in app_kalshi.py) -- Meta's
    own list is only ever a shallow recent window, so a sync interval
    longer than it'd take to accumulate 50 new posts risks silently
    missing some; see docs/PUBLIC_THREADS_API.md for the recommended
    cadence. Raises on failure (no token, HTTP error) -- same convention as
    every other function in this module."""
    global _posts_archive_cache
    fresh = list_recent_posts(limit=50)
    with _posts_archive_lock:
        archive = _posts_archive_cache if _posts_archive_cache is not None else (_pull_posts_archive_from_hf() or [])
        known_ids = {post.get("id") for post in archive}
        new_posts = [post for post in fresh if post.get("id") not in known_ids]
        if new_posts:
            archive = (new_posts + archive)[:_POSTS_ARCHIVE_MAX_SIZE]
            _posts_archive_cache = archive
            _push_posts_archive_to_hf(archive)
        else:
            _posts_archive_cache = archive
    return {"new_posts": len(new_posts), "total_archived": len(archive)}
