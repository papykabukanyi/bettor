"""An HF-hosted LLM ("news anchor" persona) used to rewrite trending-news
captions, generate real commentary, and draft Threads replies -- gives the
bot's own voice more personality/enthusiasm than a raw RSS headline or a
generic template reply.

Two separate HF touchpoints, doing two separate jobs:
  1. GENERATION: HF's Inference Providers router (an OpenAI-compatible
     chat-completions endpoint) with the SAME HF_API_KEY this codebase
     already reads for HF Hub uploads/downloads -- no new credential.
     Verified live 2026-08-25: meta-llama/Llama-3.1-8B-Instruct auto-routes
     to a real, working provider under this exact token.
  2. PERSONA CONFIG: a real, dedicated HF Hub MODEL repo
     (https://huggingface.co/papylove/cumdev-news-anchor) hosts this
     persona's actual system prompts as `persona_config.json` -- pulled at
     process startup (cached, hard-timeout-protected, same pattern
     threads_client.py's own token pull uses) so the bot's voice can be
     edited by updating that repo, no code redeploy required. This is a
     PROMPT-ENGINEERED persona wrapping an existing capable instruct model,
     not a from-scratch fine-tune (that would need labeled training data
     and GPU infrastructure this project doesn't have) -- see the repo's
     own README for the full explanation of that choice. The hardcoded
     _DEFAULT_PROMPTS below are the same content originally pushed to that
     repo, kept here as a safe fallback if the pull ever fails (no network,
     repo unreachable, malformed file) -- this module must never depend on
     HF being reachable to produce SOME reasonable output.

Best-effort, never-raise, same discipline as every other Threads-adjacent
module here (see threads_post.py's own docstring): a slow/failed/malformed
LLM response (or a failed persona-config pull) falls back to the caller's
own plain text, never blocks or delays a real post.
"""
from __future__ import annotations

import logging
import os
import threading
import time

import requests

logger = logging.getLogger(__name__)

HF_API_KEY = os.getenv("HF_API_KEY", "")
_ROUTER_URL = "https://router.huggingface.co/v1/chat/completions"
# Overridable so a real cost/latency/quality tradeoff can be tuned without a
# code change -- see this module's own docstring for the one confirmed-
# working model/provider combo as of the date above.
MODEL = os.getenv("THREADS_PERSONA_MODEL", "meta-llama/Llama-3.1-8B-Instruct")
_TIMEOUT_SEC = int(os.getenv("THREADS_PERSONA_TIMEOUT_SEC", "20") or "20")
_MAX_TOKENS = int(os.getenv("THREADS_PERSONA_MAX_TOKENS", "120") or "120")

# The real, dedicated persona repo -- see this module's own docstring.
PERSONA_HF_REPO = os.getenv("THREADS_PERSONA_HF_REPO", "papylove/cumdev-news-anchor")
_PERSONA_CONFIG_FILENAME = "persona_config.json"
_PERSONA_CONFIG_HF_TIMEOUT_SEC = int(os.getenv("THREADS_PERSONA_CONFIG_HF_TIMEOUT_SEC", "10") or "10")
# Same "don't hammer HF on every single call" cooldown shape as
# threads_client.py's own _PULL_RETRY_COOLDOWN_SEC -- this config changes
# rarely (a human editing the HF repo), so a process-lifetime cache
# refreshed at most this often is more than fresh enough.
_PERSONA_CONFIG_REFRESH_SEC = int(os.getenv("THREADS_PERSONA_CONFIG_REFRESH_SEC", "1800") or "1800")
_persona_config_lock = threading.Lock()
_persona_config_cache: dict | None = None
_last_persona_config_pull_ts = 0.0

_DEFAULT_PROMPTS = {
    "anchor_rewrite_headline": {
        "system": (
            "You are the on-air voice of CUMDEV, a crypto/stocks trading bot's Threads "
            "account. You break news the way a sharp, likable TV anchor would: punchy, "
            "confident, a little playful -- like a real, sharp person who actually "
            "follows this stuff, NOT a corporate broadcast voice or a press release. "
            "Write the way an actual human posts on social media: plain, direct "
            "language, contractions, a genuine reaction -- never stiff, never "
            "robotic, never salesy. You NEVER invent facts, numbers, names, or "
            "details not present in the source material -- you only restyle the "
            "delivery of what's actually there. Keep it SHORT (this is a social "
            "caption, not an article -- one or two sentences, under 150 characters). "
            "No hashtags (added separately by the caller). At most one emoji, and "
            "only if it genuinely earns its place. Output ONLY the rewritten line "
            "itself, nothing else -- no quotes, no preamble, no explanation."
        ),
        "max_tokens": _MAX_TOKENS, "temperature": 0.8,
    },
    "anchor_commentary": {
        "system": (
            "You are the voice of CUMDEV, a crypto/stocks trading bot's Threads "
            "account, giving your own take on a story that's still developing/still "
            "the talk of the timeline. Write like a real, sharp human sharing a "
            "genuine opinion with people who follow them -- NOT a corporate "
            "broadcast voice or a press release. This is NOT a headline announcement "
            "(the audience already saw the headline) -- it's a real reaction: why it "
            "matters, what to watch next, an actual opinion, in plain, direct "
            "language with contractions, like you're genuinely talking to someone, "
            "not reading a script. You NEVER invent facts, numbers, names, or "
            "details not present in the source material. Keep it SHORT (this is a "
            "social post, not an article -- one to three sentences, under 250 "
            "characters). No hashtags (added separately by the caller). At most one "
            "emoji, and only if it genuinely earns its place. Output ONLY the "
            "commentary itself, nothing else -- no quotes, no preamble, no "
            "'BREAKING:' framing (that's for the headline post, not this one)."
        ),
        "max_tokens": _MAX_TOKENS, "temperature": 0.8,
    },
    "anchor_draft_reply": {
        "system": (
            "You are the voice of CUMDEV, a crypto/stocks trading bot's Threads "
            "account, replying to someone else's post. Write like a real, genuine "
            "human replying in a conversation -- plain language, contractions, an "
            "actual opinion or real personality -- NOT a corporate voice, NOT a "
            "press release, never generic filler like 'great post!' or 'so true!', "
            "never pushy or salesy. You may naturally mention CUMDEV's own site "
            "(https://cumdev.onrender.com) ONLY when it's actually relevant to what "
            "you're saying, not as a reflex. Keep it SHORT (1-2 sentences, well "
            "under 200 characters, since you must ALSO end the reply with 1-2 "
            "specific, relevant hashtags -- something an actual person would tag "
            "this exact reply with to help the right people find it, never generic "
            "filler tags, never more than 2). Output ONLY the reply text followed by "
            "its hashtags, nothing else -- no quotes, no preamble."
        ),
        "max_tokens": _MAX_TOKENS, "temperature": 0.8,
    },
}


def _pull_persona_config_from_hf() -> dict | None:
    if not HF_API_KEY:
        return None

    def _download() -> dict:
        from huggingface_hub import hf_hub_download
        import json
        path = hf_hub_download(
            repo_id=PERSONA_HF_REPO, filename=_PERSONA_CONFIG_FILENAME, repo_type="model", token=HF_API_KEY,
        )
        with open(path, encoding="utf-8") as f:
            return json.load(f)

    try:
        from server_common import call_with_hard_timeout
        return call_with_hard_timeout(_download, timeout_sec=_PERSONA_CONFIG_HF_TIMEOUT_SEC)
    except Exception as exc:
        logger.info("[threads_persona] persona config pull from %s failed (using defaults): %s", PERSONA_HF_REPO, exc)
        return None


def _get_prompt(task: str) -> dict:
    """Returns {"system", "max_tokens", "temperature"} for `task` -- the
    HF-hosted config if it was reachable and defines this task, else the
    hardcoded default (see this module's own docstring on why a default
    must always exist). Refreshed from HF at most once per
    _PERSONA_CONFIG_REFRESH_SEC, not on every single call -- including when
    the pull itself keeps failing/finding nothing: the timestamp updates
    on every ATTEMPT, not just a successful one, so a down/misconfigured
    HF repo is negative-cacheable too, rather than retried on every single
    call forever (same pattern threads_client.py's own token pull uses)."""
    global _last_persona_config_pull_ts, _persona_config_cache
    now = time.monotonic()
    with _persona_config_lock:
        if (now - _last_persona_config_pull_ts) >= _PERSONA_CONFIG_REFRESH_SEC:
            _last_persona_config_pull_ts = now
            pulled = _pull_persona_config_from_hf()
            if pulled:
                _persona_config_cache = pulled
        config = _persona_config_cache
    prompts = (config or {}).get("prompts", {})
    task_config = prompts.get(task)
    if isinstance(task_config, dict) and task_config.get("system"):
        return task_config
    return _DEFAULT_PROMPTS[task]


def _chat(system: str, user: str, *, max_tokens: int = _MAX_TOKENS, temperature: float = 0.8) -> str | None:
    if not HF_API_KEY:
        return None
    try:
        resp = requests.post(
            _ROUTER_URL,
            headers={"Authorization": f"Bearer {HF_API_KEY}"},
            json={
                "model": MODEL,
                "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}],
                "max_tokens": max_tokens, "temperature": temperature,
            },
            timeout=_TIMEOUT_SEC,
        )
        resp.raise_for_status()
        content = (resp.json()["choices"][0]["message"]["content"] or "").strip()
        # A blank/whitespace-only completion is a real (if rare) failure
        # mode, not a valid rewrite -- treat it the same as any other
        # failure so the caller falls back to its own plain text.
        return content or None
    except Exception as exc:
        logger.warning("[threads_persona] HF chat call failed, caller should fall back to plain text: %s", exc)
        return None


def anchor_rewrite_headline(
    title: str, *, source: str | None = None, secondary: list[str] | None = None,
) -> str | None:
    """Rewrites one news headline into a short, energetic "breaking news"
    anchor line. `secondary` (up to 3 related headlines, same cycle) is
    passed as CONTEXT only -- the output is always a single rewritten line
    for the LEAD headline, not a rewrite of every secondary too (keeps this
    to one LLM call per posting cycle, not one per headline). Returns None
    (never raises) on any failure -- caller must fall back to the plain
    headline (see _clean_headline)."""
    if not title:
        return None
    context = f'Headline: "{title}"'
    if source:
        context += f"\nSource: {source}"
    if secondary:
        context += "\nAlso trending right now: " + "; ".join(secondary[:3])
    prompt = _get_prompt("anchor_rewrite_headline")
    return _chat(
        prompt["system"], context + "\n\nRewrite the HEADLINE as one punchy anchor-style line.",
        max_tokens=prompt.get("max_tokens", _MAX_TOKENS), temperature=prompt.get("temperature", 0.8),
    )


def anchor_commentary(title: str, *, source: str | None = None, secondary: list[str] | None = None) -> str | None:
    """Genuine analysis/reaction/"take" on a story the account has already
    covered (or can't re-cover as a fresh headline post right now -- see
    threads_post.post_trending_news's own no-fresh-story fallback) --
    deliberately NOT a rewrite of the headline itself (see
    anchor_rewrite_headline for that): this is new, distinct content built
    OFF the same story, not a repost/paraphrase of it, so it's genuinely
    fresh even when the underlying news item is a duplicate. Returns None
    (never raises) on any failure -- caller must fall back to something
    else (a reply round, or plain filler text) rather than post nothing."""
    if not title:
        return None
    context = f'Story: "{title}"'
    if source:
        context += f"\nSource: {source}"
    if secondary:
        context += "\nAlso trending right now: " + "; ".join(secondary[:3])
    prompt = _get_prompt("anchor_commentary")
    return _chat(
        prompt["system"], context + "\n\nGive your own take on this.",
        max_tokens=prompt.get("max_tokens", _MAX_TOKENS), temperature=prompt.get("temperature", 0.8),
    )


def anchor_draft_reply(post_text: str, *, author_username: str | None = None) -> str | None:
    """Drafts a reply to another Threads post in the same anchor persona.
    Returns None (never raises) on any failure -- caller must either fall
    back to a plain template reply or skip replying entirely."""
    if not post_text:
        return None
    context = f'Post{f" by @{author_username}" if author_username else ""}: "{post_text[:400]}"'
    prompt = _get_prompt("anchor_draft_reply")
    return _chat(
        prompt["system"], context + "\n\nDraft a reply to this post.",
        max_tokens=prompt.get("max_tokens", _MAX_TOKENS), temperature=prompt.get("temperature", 0.8),
    )
