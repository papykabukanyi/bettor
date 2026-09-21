"""Project-wide, read-only Claude-powered monitoring/analysis layer --
per explicit user direction: "add Claude as a monitoring/analysis layer"
(chosen over "replace the prediction model with Claude entirely" via an
AskUserQuestion), then explicitly widened from Kalshi 15-minute markets
only to "across all of the bots... access to check all the stuff going
on" in the very same build.

Deliberately NOT a prediction engine for any of the 5 markets: each
market's own fitted, calibrated, walk-forward-validated model (see
perps_model.py/alpaca_model.py/alpaca_crypto_model.py/
alpaca_options_model.py/kalshi_15m_model.py/kalshi_15m_metals_model.py)
keeps making every real entry decision, completely unchanged by this
module. This is a read-only reviewer across the whole project: once a
day, it gathers a real data snapshot from EVERY market (state, model
meta, recent trades) -- the exact same data each market's own dashboard
already shows -- asks Claude to review all of it together, and stores
the resulting report for a human to read. It never places an order,
never touches any market's own LIVE_TRADING_ENABLED, and a failure here
(missing API key, a bad response, a network error) never blocks or
degrades any market's trading loop -- this whole module is additive, not
load-bearing, and is never imported by any *_strategy.py or *_model.py
file.

Uses the Anthropic Messages API directly via `requests` (already a
dependency everywhere else in this codebase) rather than adding the
`anthropic` SDK as a new dependency for one lightweight, low-frequency
call.

Real, disclosed distinction from a Claude.ai subscription: this needs an
Anthropic API key (console.anthropic.com), billed separately per token --
a claude.ai Pro/Max subscription has no programmatic access and cannot
be used here. ANTHROPIC_API_KEY unset means this module is a no-op
throughout (matching every other optional-integration convention in this
codebase, e.g. SERPAPI_API_KEY/CRYPTOPANIC_API_KEY).

Lives in app_kalshi.py's own process/scheduler (the default-mounted app
in this repo's combined Docker Space, see docs/RENDER_TO_HF_MIGRATION.md)
since that's the one process every market's own `data.*` modules can
already be imported into read-only, with zero Flask-route or
APScheduler-job registration side effects from those imports themselves
(those registrations live in each market's own *_server.py, never in a
bare *_strategy.py/*_model.py module) -- this module never imports any
*_server.py file, so it can never collide with another market's own
Flask app or scheduler."""
from __future__ import annotations

import datetime as dt
import json
import logging
import os
from typing import Any

import requests

from server_common import DATA_DIR, load_json, save_json

logger = logging.getLogger(__name__)

ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
ANTHROPIC_API_URL = "https://api.anthropic.com/v1/messages"
ANTHROPIC_API_VERSION = "2023-06-01"
# Sonnet, not Opus -- this is a periodic (daily, see app_kalshi.py's own
# AI_MONITOR_HOUR_ET) review of a few KB of real numbers across 5
# markets, not a task that needs the most expensive model available.
ANTHROPIC_MODEL = os.getenv("AI_MONITOR_MODEL", "claude-sonnet-5")
ANTHROPIC_TIMEOUT_SEC = int(os.getenv("AI_MONITOR_TIMEOUT_SEC", "90") or "90")
ANTHROPIC_MAX_TOKENS = int(os.getenv("AI_MONITOR_MAX_TOKENS", "2000") or "2000")

REPORT_PATH = DATA_DIR / "ai_monitor_report.json"
RECENT_TRADES_LIMIT = 15
RECENT_JOBS_LIMIT = 15


def _perps_snapshot() -> dict[str, Any]:
    from data import perps_model, perps_strategy

    state = perps_strategy._load_state()  # noqa: SLF001
    _, meta = perps_model.load_model()
    return {
        "live_trading_enabled": perps_strategy.LIVE_TRADING_ENABLED,
        "model": {k: v for k, v in (meta or {}).items() if k != "feature_importances"},
        "open_positions": state.get("positions") or [],
        "recent_trades": list(reversed(state.get("trade_log") or []))[:RECENT_TRADES_LIMIT],
        "realized_pnl_by_date": state.get("realized_pnl_by_date") or {},
    }


def _alpaca_market_snapshot(strategy_mod_name: str, model_mod_name: str) -> dict[str, Any]:
    """Shared shape for stocks/crypto/options -- all 3 alpaca_*_strategy.py/
    alpaca_*_model.py modules already follow the identical _load_state/
    load_model/LIVE_TRADING_ENABLED convention (confirmed directly, not
    assumed) so one function covers all 3 rather than three near-
    duplicates."""
    import importlib
    strategy = importlib.import_module(f"data.{strategy_mod_name}")
    model = importlib.import_module(f"data.{model_mod_name}")

    state = strategy._load_state()  # noqa: SLF001
    _, meta = model.load_model()
    return {
        "live_trading_enabled": strategy.LIVE_TRADING_ENABLED,
        "model": {k: v for k, v in (meta or {}).items() if k != "feature_importances"},
        "open_positions": state.get("positions") or [],
        "recent_trades": list(reversed(state.get("trade_log") or []))[:RECENT_TRADES_LIMIT],
        "realized_pnl_by_date": state.get("realized_pnl_by_date") or {},
    }


def _kalshi_15m_snapshot() -> dict[str, Any]:
    from data import kalshi_15m, kalshi_15m_metals_model, kalshi_15m_model, kalshi_15m_strategy

    state = kalshi_15m_strategy._load_state()  # noqa: SLF001
    _, crypto_meta = kalshi_15m_model.load_model()
    _, metals_meta = kalshi_15m_metals_model.load_model()
    try:
        shard_2_balance = kalshi_15m.get_balance_by_shard(exchange_index=2)
    except Exception as exc:
        shard_2_balance = {"error": str(exc)}
    return {
        "live_trading_enabled": kalshi_15m_strategy.LIVE_TRADING_ENABLED,
        "crypto_model": {k: v for k, v in (crypto_meta or {}).items() if k != "feature_importances"},
        "metals_model": {k: v for k, v in (metals_meta or {}).items() if k != "feature_importances"},
        "shard_2_balance": shard_2_balance,
        "open_positions": state.get("positions") or [],
        "recent_trades": list(reversed(state.get("trade_log") or []))[:RECENT_TRADES_LIMIT],
        "realized_pnl_by_date": state.get("realized_pnl_by_date") or {},
    }


# One entry per market -- each a zero-arg callable returning that
# market's own real snapshot dict. A market whose snapshot function
# raises is reported as {"error": ...} for that market alone (see
# gather_snapshot below), never aborting the other 4.
_MARKET_SNAPSHOT_FNS: dict[str, Any] = {
    "perps": _perps_snapshot,
    "stocks": lambda: _alpaca_market_snapshot("alpaca_strategy", "alpaca_model"),
    "crypto": lambda: _alpaca_market_snapshot("alpaca_crypto_strategy", "alpaca_crypto_model"),
    "options": lambda: _alpaca_market_snapshot("alpaca_options_strategy", "alpaca_options_model"),
    "kalshi_15m": _kalshi_15m_snapshot,
}


def gather_snapshot() -> dict[str, Any]:
    """Every field here is real, freshly-read data -- no synthetic/
    placeholder values -- gathered directly from each market's own
    *_strategy.py/*_model.py (the same underlying source each market's
    own dashboard API route reads), not by calling those Flask routes,
    to avoid needing this process to make an HTTP round trip to itself
    and to avoid importing another market's *_server.py file (which
    would risk Flask-route/APScheduler-job registration collisions --
    see this module's own docstring)."""
    markets: dict[str, Any] = {}
    for name, fn in _MARKET_SNAPSHOT_FNS.items():
        try:
            markets[name] = fn()
        except Exception as exc:
            logger.warning("[ai_monitor] snapshot failed for market %s: %s", name, exc)
            markets[name] = {"error": str(exc)}

    job_history_file = DATA_DIR / "perps_job_run_history.json"
    try:
        full_history = load_json(job_history_file, [])
        recent_jobs = [
            {k: v for k, v in rec.items() if k != "summary" or v} for rec in full_history if isinstance(rec, dict)
        ][-RECENT_JOBS_LIMIT:]
    except Exception as exc:
        recent_jobs = [{"error": str(exc)}]

    return {
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "markets": markets,
        "recent_job_outcomes": recent_jobs,
    }


def _build_prompt(snapshot: dict[str, Any]) -> str:
    return (
        "You are reviewing the real, current state of a live automated trading system "
        "spanning 5 markets: Kalshi perpetual futures (crypto), Alpaca stocks, Alpaca "
        "crypto, Alpaca options, and Kalshi's 15-minute event-contract markets (crypto "
        "direction + gold/silver/copper). You do NOT control any of these bots and "
        "cannot place, cancel, or modify any order on any market -- this is a read-only "
        "status review across the whole project. Every field below is real, freshly-"
        "pulled data (model metadata, recent trades, recent scheduled-job outcomes) -- "
        "do not assume anything not shown here, and say so plainly if the data given is "
        "insufficient to judge something for a given market.\n\n"
        "Write a concise report covering:\n"
        "1. Data/system health, per market: any failed job outcomes, missing model "
        "training, or data gaps visible in what's given. Call out which specific "
        "market(s) each issue belongs to.\n"
        "2. Trading performance, per market: what the recent trades and realized P&L "
        "actually show for each. If there's too little data yet for a market to "
        "conclude anything, say so rather than overreaching.\n"
        "3. Anything that looks genuinely wrong across the whole project or worth a "
        "human double-checking, especially anything that could affect MULTIPLE markets "
        "at once (e.g. a shared infrastructure issue).\n"
        "4. Concrete, specific recommendations if you have any -- skip this section "
        "entirely if you don't have a real one.\n\n"
        "Keep it factual and grounded in the data below. No filler, no generic trading "
        "advice, no disclaimers beyond what's actually warranted by the data.\n\n"
        f"DATA:\n{json.dumps(snapshot, indent=2, default=str)}"
    )


def call_claude(prompt: str) -> dict[str, Any]:
    if not ANTHROPIC_API_KEY:
        return {"ok": False, "reason": "no_anthropic_api_key"}
    try:
        resp = requests.post(
            ANTHROPIC_API_URL,
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": ANTHROPIC_API_VERSION,
                "content-type": "application/json",
            },
            json={
                "model": ANTHROPIC_MODEL,
                "max_tokens": ANTHROPIC_MAX_TOKENS,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=ANTHROPIC_TIMEOUT_SEC,
        )
        resp.raise_for_status()
        data = resp.json()
        text_parts = [block.get("text", "") for block in (data.get("content") or []) if block.get("type") == "text"]
        text = "\n".join(p for p in text_parts if p)
        if not text:
            return {"ok": False, "reason": "empty_response"}
        return {"ok": True, "text": text, "model": data.get("model", ANTHROPIC_MODEL), "usage": data.get("usage")}
    except Exception as exc:
        logger.warning("[ai_monitor] Claude API call failed: %s", exc)
        return {"ok": False, "reason": str(exc)}


def run_monitor_cycle() -> dict[str, Any]:
    """The one function app_kalshi.py's own scheduled job calls. Never
    raises -- a failure anywhere here (no API key, a network error, a
    malformed response, one market's own snapshot failing -- see
    gather_snapshot's own per-market error isolation) is reported in the
    return value, matching every other best-effort job in this codebase,
    and never blocks any market's actual trading loop."""
    snapshot = gather_snapshot()
    if not ANTHROPIC_API_KEY:
        result = {"ok": False, "reason": "no_anthropic_api_key", "generated_at": snapshot["generated_at"]}
        _save_report(result)
        return result

    prompt = _build_prompt(snapshot)
    response = call_claude(prompt)
    result = {
        "ok": response.get("ok", False),
        "generated_at": snapshot["generated_at"],
        "model": response.get("model"),
        "report": response.get("text"),
        "reason": response.get("reason"),
        "usage": response.get("usage"),
    }
    _save_report(result)
    return result


def _save_report(result: dict[str, Any]) -> None:
    try:
        save_json(REPORT_PATH, result)
    except Exception as exc:
        logger.warning("[ai_monitor] failed to save report locally: %s", exc)


def get_latest_report() -> dict[str, Any] | None:
    if not REPORT_PATH.exists():
        return None
    return load_json(REPORT_PATH, None)
