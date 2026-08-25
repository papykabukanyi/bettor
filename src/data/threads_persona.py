"""An HF-hosted LLM ("news anchor" persona) used to rewrite trending-news
captions and draft Threads replies -- gives the bot's own voice more
personality/enthusiasm than a raw RSS headline or a generic template reply.

Uses HF's Inference Providers router (an OpenAI-compatible chat-completions
endpoint) with the SAME HF_API_KEY this codebase already reads for HF Hub
uploads/downloads -- no new credential, no separately trained or fine-tuned
model (a real fine-tune would need labeled data, GPU time, and hosting this
codebase has none of set up for). Verified live 2026-08-25:
meta-llama/Llama-3.1-8B-Instruct auto-routes to a real, working provider
under this exact token.

Best-effort, never-raise, same discipline as every other Threads-adjacent
module here (see threads_post.py's own docstring): a slow/failed/malformed
LLM response falls back to the caller's own plain text, never blocks or
delays a real post.
"""
from __future__ import annotations

import logging
import os

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

_ANCHOR_SYSTEM_PROMPT = (
    "You are the on-air voice of CUMDEV, a crypto/stocks trading bot's Threads "
    "account. You break news the way a sharp, likable TV anchor would: punchy, "
    "confident, a little playful, never robotic or salesy. You NEVER invent "
    "facts, numbers, names, or details not present in the source material -- "
    "you only restyle the delivery of what's actually there. Keep it SHORT "
    "(this is a social caption, not an article -- one or two sentences, under "
    "150 characters). No hashtags (added separately by the caller). At most "
    "one emoji, and only if it genuinely earns its place. Output ONLY the "
    "rewritten line itself, nothing else -- no quotes, no preamble, no "
    "explanation."
)

_REPLY_SYSTEM_PROMPT = (
    "You are the on-air voice of CUMDEV, a crypto/stocks trading bot's Threads "
    "account, replying to someone else's post. Be genuinely conversational and "
    "add real value, a real opinion, or real personality -- never generic "
    "filler like 'great post!' or 'so true!', never pushy or salesy. You may "
    "naturally mention CUMDEV's own site (https://cumdev.onrender.com) ONLY "
    "when it's actually relevant to what you're saying, not as a reflex. Keep "
    "it SHORT (1-2 sentences, under 200 characters). Output ONLY the reply "
    "text itself, nothing else -- no quotes, no preamble."
)


def _chat(system: str, user: str, *, max_tokens: int = _MAX_TOKENS) -> str | None:
    if not HF_API_KEY:
        return None
    try:
        resp = requests.post(
            _ROUTER_URL,
            headers={"Authorization": f"Bearer {HF_API_KEY}"},
            json={
                "model": MODEL,
                "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}],
                "max_tokens": max_tokens, "temperature": 0.8,
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
    return _chat(_ANCHOR_SYSTEM_PROMPT, context + "\n\nRewrite the HEADLINE as one punchy anchor-style line.")


def anchor_draft_reply(post_text: str, *, author_username: str | None = None) -> str | None:
    """Drafts a reply to another Threads post in the same anchor persona.
    Returns None (never raises) on any failure -- caller must either fall
    back to a plain template reply or skip replying entirely."""
    if not post_text:
        return None
    context = f'Post{f" by @{author_username}" if author_username else ""}: "{post_text[:400]}"'
    return _chat(_REPLY_SYSTEM_PROMPT, context + "\n\nDraft a reply to this post.")
