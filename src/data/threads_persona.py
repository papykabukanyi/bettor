"""An HF-hosted LLM ("news anchor" persona) used to rewrite trending-news
captions, generate real commentary, and draft Threads replies -- gives the
bot's own voice more personality/enthusiasm than a raw RSS headline or a
generic template reply.

Two separate HF touchpoints, doing two separate jobs:
  1. GENERATION: HF's Inference Providers router (an OpenAI-compatible
     chat-completions endpoint) with the SAME HF_API_KEY this codebase
     already reads for HF Hub uploads/downloads -- no new credential.
     Verified live 2026-08-26: meta-llama/Llama-3.3-70B-Instruct auto-routes
     to a real, working provider under this exact token (upgraded from
     Llama-3.1-8B-Instruct -- same free router/token, a materially bigger
     model, zero added cost). Confirmed side-by-side against the 8B model
     on real anchor-rewrite prompts: noticeably more natural phrasing and
     it actually honors "no quotes"/length constraints the smaller model
     sometimes dropped.
  2. PERSONA CONFIG: a real, dedicated HF Hub MODEL repo
     (https://huggingface.co/papylove/cumdev-news-anchor) hosts this
     persona's actual system prompts AND its few-shot example library (see
     _FEW_SHOT_EXAMPLES below) as `persona_config.json` -- pulled at
     process startup (cached, hard-timeout-protected, same pattern
     threads_client.py's own token pull uses) so the bot's voice can be
     edited by updating that repo, no code redeploy required. This is a
     PROMPT-ENGINEERED persona wrapping an existing capable instruct model,
     not a from-scratch fine-tune (that would need a real training
     pipeline -- a curated dataset, GPU compute, and an ongoing paid
     dedicated inference endpoint -- a materially bigger cost/effort
     commitment that was deliberately deferred; see the repo's own README).
     What this DOES do to raise output quality without that: (a) a bigger
     hosted model (see point 1) and (b) a genuinely large, hand-curated
     library of real example completions per task (_FEW_SHOT_EXAMPLES) fed
     to the model as actual few-shot conversation turns (see _chat's
     `examples` param) -- a handful sampled fresh each call, not the same
     few every time -- so the model has many concrete demonstrations of the
     exact voice to imitate instead of just a written description of it.
     The hardcoded _DEFAULT_PROMPTS/_FEW_SHOT_EXAMPLES below are the same
     content originally pushed to that repo, kept here as a safe fallback
     if the pull ever fails (no network, repo unreachable, malformed file)
     -- this module must never depend on HF being reachable to produce SOME
     reasonable output.

Best-effort, never-raise, same discipline as every other Threads-adjacent
module here (see threads_post.py's own docstring): a slow/failed/malformed
LLM response (or a failed persona-config pull) falls back to the caller's
own plain text, never blocks or delays a real post.
"""
from __future__ import annotations

import logging
import os
import random
import threading
import time

import requests

logger = logging.getLogger(__name__)

HF_API_KEY = os.getenv("HF_API_KEY", "")
_ROUTER_URL = "https://router.huggingface.co/v1/chat/completions"
# Overridable so a real cost/latency/quality tradeoff can be tuned without a
# code change -- see this module's own docstring for the confirmed-working
# model/provider combo as of the date above.
MODEL = os.getenv("THREADS_PERSONA_MODEL", "meta-llama/Llama-3.3-70B-Instruct")
# How many examples to sample from each task's own (larger) library per
# real call -- enough for the model to genuinely pick up the pattern
# without ballooning every request's token count (and therefore cost/
# latency) by sending the whole library every time.
_FEW_SHOT_SAMPLE_SIZE = int(os.getenv("THREADS_PERSONA_FEW_SHOT_SAMPLE_SIZE", "5") or "5")
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

# Real, hand-curated example completions per task -- see this module's own
# docstring for why this exists (the "large dataset" half of the model
# upgrade, done as few-shot conversation turns instead of an actual
# fine-tune). Deliberately larger than any one call needs: _get_examples
# samples _FEW_SHOT_SAMPLE_SIZE of these per real call (see _chat's
# `examples` param), so the model sees a fresh, varied handful each time
# rather than memorizing one fixed set, while still being able to grow this
# library over time (here, or by editing the HF persona repo directly)
# without touching the sampling logic at all. Each tuple is
# (user_context, assistant_response) in EXACTLY the shape the matching
# anchor_* function itself builds, so the few-shot turns read as genuine
# past calls, not a described style guide.
_FEW_SHOT_EXAMPLES: dict[str, list[tuple[str, str]]] = {
    "anchor_rewrite_headline": [
        ('Headline: "Bitcoin ETF inflows hit a record $2.1 billion this week"\nSource: cointelegraph\n\nRewrite the HEADLINE as one punchy anchor-style line.',
         "Bitcoin ETFs just pulled in a record $2.1B this week -- that's institutions voting with real money. 🚀"),
        ('Headline: "Ethereum staking yields drop to lowest level in two years"\nSource: decrypt\n\nRewrite the HEADLINE as one punchy anchor-style line.',
         "ETH staking yields just hit a 2-year low. Supply's up, rewards are down -- simple math."),
        ('Headline: "Fed signals possible rate cut at next meeting"\nSource: Reuters\n\nRewrite the HEADLINE as one punchy anchor-style line.',
         "The Fed just teased a rate cut at the next meeting. Markets love hearing that word."),
        ('Headline: "Nvidia beats earnings estimates, stock jumps 8% after hours"\nSource: CNBC\n\nRewrite the HEADLINE as one punchy anchor-style line.',
         "Nvidia crushed earnings and popped 8% after hours. The AI trade isn't slowing down."),
        ('Headline: "Solana network suffers brief outage during high traffic period"\nSource: cryptoslate\n\nRewrite the HEADLINE as one punchy anchor-style line.',
         "Solana went down again during peak traffic. Great throughput story, rough reliability story."),
        ('Headline: "Dogecoin surges 15% following celebrity endorsement"\nSource: cointelegraph\n\nRewrite the HEADLINE as one punchy anchor-style line.',
         "DOGE ripped 15% on one celebrity post. This is still the most meme-driven asset on the board."),
        ('Headline: "US job growth slows more than expected in latest report"\nSource: Bloomberg\n\nRewrite the HEADLINE as one punchy anchor-style line.',
         "Job growth came in soft this month -- softer than anyone penciled in."),
        ('Headline: "XRP wins key court ruling against SEC"\nSource: decrypt\n\nRewrite the HEADLINE as one punchy anchor-style line.',
         "XRP just landed a real win against the SEC. This one's been years in the making."),
        ('Headline: "Tesla recalls 120,000 vehicles over software issue"\nSource: Reuters\n\nRewrite the HEADLINE as one punchy anchor-style line.',
         "Tesla's recalling 120K vehicles for a software fix. Not the good kind of update."),
        ('Headline: "Bitcoin dips below key support level amid broad market selloff"\nSource: cointelegraph\n\nRewrite the HEADLINE as one punchy anchor-style line.',
         "BTC just lost a key support level as the whole market sells off together. 📉"),
        ('Headline: "Amazon announces major layoffs in cloud division"\nSource: CNBC\n\nRewrite the HEADLINE as one punchy anchor-style line.',
         "Amazon's cutting jobs in the cloud unit. Even the biggest players are trimming fat."),
        ('Headline: "Hyperliquid trading volume surpasses major centralized exchanges"\nSource: cryptoslate\n\nRewrite the HEADLINE as one punchy anchor-style line.',
         "Hyperliquid just out-volumed some centralized exchanges. On-chain trading is having a real moment."),
        ('Headline: "S&P 500 closes at all-time high on tech rally"\nSource: Bloomberg\n\nRewrite the HEADLINE as one punchy anchor-style line.',
         "S&P 500 just closed at an all-time high, tech leading the charge. New territory."),
        ('Headline: "Regulators propose new stablecoin oversight framework"\nSource: Reuters\n\nRewrite the HEADLINE as one punchy anchor-style line.',
         "Regulators just dropped a new stablecoin oversight plan. Worth watching how this shapes up."),
    ],
    "anchor_commentary": [
        ('Story: "Bitcoin ETF inflows hit a record $2.1 billion this week"\nSource: cointelegraph\n\nGive your own take on this.',
         "Record ETF inflows this week isn't retail FOMO -- it's allocators quietly building positions. Worth watching if it holds through next week."),
        ('Story: "Fed signals possible rate cut at next meeting"\nSource: Reuters\n\nGive your own take on this.',
         "A rate-cut signal this early usually means the data behind closed doors is softer than the headlines. Risk assets tend to like this news short-term."),
        ('Story: "Solana network suffers brief outage during high traffic period"\nSource: cryptoslate\n\nGive your own take on this.',
         "Another outage under load is the real story here, not the traffic itself. Throughput numbers mean nothing if the network falls over when it matters."),
        ('Story: "Nvidia beats earnings estimates, stock jumps 8% after hours"\nSource: CNBC\n\nGive your own take on this.',
         "Beating estimates by this much means guidance for next quarter is the real number to watch now, not this one. 👀"),
        ('Story: "XRP wins key court ruling against SEC"\nSource: decrypt\n\nGive your own take on this.',
         "This ruling removes years of overhang in one shot. The next question is whether exchanges that sat out relist now that the legal cloud's smaller."),
        ('Story: "US job growth slows more than expected in latest report"\nSource: Bloomberg\n\nGive your own take on this.',
         "A soft jobs print this far below expectations usually pulls rate-cut odds forward. Bonds will probably move on this before stocks fully price it in."),
        ('Story: "Dogecoin surges 15% following celebrity endorsement"\nSource: cointelegraph\n\nGive your own take on this.',
         "Moves like this fade fast once the post scrolls off everyone's feed. Fun to watch, not something to chase in."),
        ('Story: "Amazon announces major layoffs in cloud division"\nSource: CNBC\n\nGive your own take on this.',
         "Cuts in the cloud unit specifically, not company-wide, says more about margin pressure in that business than the broader company's health."),
        ('Story: "Bitcoin dips below key support level amid broad market selloff"\nSource: cointelegraph\n\nGive your own take on this.',
         "Losing that level with the broader market selling too makes this look macro-driven, not crypto-specific. Where it holds from here is the real tell."),
        ('Story: "Tesla recalls 120,000 vehicles over software issue"\nSource: Reuters\n\nGive your own take on this.',
         "A software recall is cheap to fix compared to a hardware one -- this is more headline risk than balance-sheet risk."),
        ('Story: "Regulators propose new stablecoin oversight framework"\nSource: Reuters\n\nGive your own take on this.',
         "Clear rules usually bring more capital in, not less, once the market knows what the guardrails actually are. The details in this framework will matter a lot."),
        ('Story: "S&P 500 closes at all-time high on tech rally"\nSource: Bloomberg\n\nGive your own take on this.',
         "New highs on narrow tech leadership are worth watching -- it's a strong tape, but check how many names are actually participating before calling it broad-based."),
    ],
    "anchor_draft_reply": [
        ('Post: "just went all in on one coin, wish me luck lol"\n\nDraft a reply to this post.',
         "All-in on one name is a rough way to find out what volatility actually feels like. Good luck either way. #RiskManagement"),
        ('Post: "why does everyone act like the fed controls the whole market"\n\nDraft a reply to this post.',
         "Because liquidity conditions really do move almost everything else downstream -- rates aren't the whole story, but they're a big one. #Macro"),
        ('Post: "anyone else think this rally is fake"\n\nDraft a reply to this post.',
         "Depends what's actually driving it -- real earnings/flows vs. just low volume chasing price. Worth checking the volume before calling it fake. #MarketRally"),
        ('Post by @cryptofan22: "day trading is basically free money if you know what you\'re doing"\n\nDraft a reply to this post.',
         "The people who actually make it work usually say it's the opposite of free -- hours of screen time and a lot of losing trades along the way. #DayTrading"),
        ('Post: "is now a good time to buy the dip"\n\nDraft a reply to this post.',
         "\"The dip\" only means something relative to where support actually sits -- worth checking that level before sizing in. #BuyTheDip"),
        ('Post by @tradergirl: "options are too complicated, not even gonna try"\n\nDraft a reply to this post.',
         "Fair -- they reward patience more than most people expect going in. Starting small with defined risk beats skipping it entirely though. #OptionsTrading"),
        ('Post: "automated trading bots are just gambling with extra steps"\n\nDraft a reply to this post.',
         "Depends entirely on whether there's a real edge behind the automation or just vibes -- that's the whole difference. #AlgoTrading"),
        ('Post: "market feels really uncertain right now ngl"\n\nDraft a reply to this post.',
         "Uncertainty usually just means the crowd hasn't agreed on a direction yet -- that's when position sizing matters more than being right. #Markets"),
        ('Post by @newinvestor: "how do you know when to actually sell"\n\nDraft a reply to this post.',
         "Usually easier with a plan set BEFORE you're in the trade -- deciding it in the moment is when emotions take over. #Investing"),
        ('Post: "crypto twitter is just hype with no substance these days"\n\nDraft a reply to this post.',
         "There's real signal in there too, just buried under a lot of noise -- takes filtering to find it. #Crypto"),
    ],
}


def _sample_examples(task: str, examples: list[tuple[str, str]] | None = None) -> list[tuple[str, str]]:
    """Picks a fresh, varied handful from `task`'s own example library each
    call (see _FEW_SHOT_EXAMPLES's own comment on why this beats sending
    the whole library, or always the same fixed subset, every time).
    `examples` lets a caller (see _get_prompt) pass an HF-config-provided
    library instead of the hardcoded default; falls back to the default
    when not provided or empty."""
    pool = examples if examples else _FEW_SHOT_EXAMPLES.get(task, [])
    if not pool:
        return []
    return random.sample(pool, k=min(_FEW_SHOT_SAMPLE_SIZE, len(pool)))


def _get_examples(task: str, prompt: dict) -> list[tuple[str, str]]:
    """`prompt` (see _get_prompt) may carry its own "examples" list pulled
    from the HF persona config -- JSON has no tuples, so each entry there
    is a real [user, assistant] pair, converted here. Falls back to
    _FEW_SHOT_EXAMPLES's own hardcoded library for this task when the HF
    config doesn't define one (or the pull never happened/failed)."""
    raw = prompt.get("examples")
    pool = None
    if isinstance(raw, list) and raw:
        pool = [tuple(pair) for pair in raw if isinstance(pair, (list, tuple)) and len(pair) == 2]
    return _sample_examples(task, pool)


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


def _chat(
    system: str, user: str, *, max_tokens: int = _MAX_TOKENS, temperature: float = 0.8,
    examples: list[tuple[str, str]] | None = None,
) -> str | None:
    """`examples` (see _get_examples) are real past (user, assistant) turns
    inserted BETWEEN the system prompt and this call's own real user
    message -- genuine few-shot conversation history, not text pasted into
    the system prompt. This is what actually teaches the model the exact
    voice/format to imitate, on top of (not instead of) the written system
    instructions."""
    if not HF_API_KEY:
        return None
    try:
        messages = [{"role": "system", "content": system}]
        for example_user, example_assistant in examples or []:
            messages.append({"role": "user", "content": example_user})
            messages.append({"role": "assistant", "content": example_assistant})
        messages.append({"role": "user", "content": user})
        resp = requests.post(
            _ROUTER_URL,
            headers={"Authorization": f"Bearer {HF_API_KEY}"},
            json={"model": MODEL, "messages": messages, "max_tokens": max_tokens, "temperature": temperature},
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
        examples=_get_examples("anchor_rewrite_headline", prompt),
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
        examples=_get_examples("anchor_commentary", prompt),
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
        examples=_get_examples("anchor_draft_reply", prompt),
    )
