---
title: Bettor Bot Status
emoji: 📊
colorFrom: blue
colorTo: indigo
sdk: static
pinned: false
short_description: Live status across the 4 trading bots
---

# Bettor Bot Status

A read-only status page for the 4 trading bots (Kalshi perps, Alpaca
stocks, Alpaca crypto, Alpaca options) -- open positions, P&L, trade
counts, and win rate, one section per market.

Static Space, no backend: `index.html` fetches each market's own
`*_durable_state.json` straight from its Hugging Face model repo's public
`resolve/main/...` URL in the browser (see `HF_REPOS` at the top of the
script) -- the exact same durable state file each bot already pushes on
every real trade, no separate pipeline or secrets needed. Free-tier
Hugging Face accounts can only host Static Spaces (Gradio/Docker need a
paid plan for real CPU hardware), and a client-side fetch of already-
public JSON is genuinely all this page needs -- no server-side code at
all.

**Requires every source repo referenced in `HF_REPOS` to stay public.**
If any of them goes private, that market's section on this page goes
dark (a private repo's `resolve/main/...` URL 401s for an anonymous
browser fetch, with no way for a static page to hold a token safely --
anything embedded in this page's own source is visible to every visitor).
See `docs/HF_JOBS_TRAINING_MIGRATION.md`'s note on this repo/page
interaction before flipping any of the 4 repos private.
