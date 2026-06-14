"""
Polymarket Auto-Betting Engine
================================
Continuously scans Polymarket for markets matching the bot's predictions,
places $2 bets on each new matched market, and monitors balance.

- Polls on a configurable interval (default: 5 min)
- Only bets on markets not already bet
- Skips if buying power < $2
- Tracks all placed bets in data/polymarket_autobet_state.json
- Logs every action to stdout and data/polymarket_autobet.log

Run:
    python src/polymarket_autobet.py

Environment overrides (set in .env or shell):
    AUTOBET_AMOUNT_USD         = 2.0   (amount per bet)
    AUTOBET_POLL_SEC           = 300   (seconds between scans)
    AUTOBET_MIN_CONFIDENCE     = 52    (minimum model confidence %, 50-98)
    AUTOBET_DRY_RUN            = 0     (set to 1 to simulate without placing)
    AUTOBET_MAX_BETS_PER_CYCLE = 10    (max new bets placed per scan)
"""

from __future__ import annotations

import datetime
import json
import logging
import os
import signal
import sys
import time
from typing import Any

# ── Path setup ────────────────────────────────────────────────────────────────
SRC_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(SRC_DIR)
sys.path.insert(0, SRC_DIR)

# Load .env
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(ROOT_DIR, ".env"))
except Exception:
    pass

# ── Configuration ─────────────────────────────────────────────────────────────
AUTOBET_AMOUNT_USD = max(1.0, float(os.getenv("AUTOBET_AMOUNT_USD", "2.0") or "2.0"))
AUTOBET_POLL_SEC = max(30, int(os.getenv("AUTOBET_POLL_SEC", "300") or "300"))
AUTOBET_MIN_CONFIDENCE = max(50, min(98, int(os.getenv("AUTOBET_MIN_CONFIDENCE", "52") or "52")))
AUTOBET_DRY_RUN = str(os.getenv("AUTOBET_DRY_RUN", "0")).strip().lower() in {"1", "true", "yes"}
AUTOBET_MAX_BETS_PER_CYCLE = max(1, int(os.getenv("AUTOBET_MAX_BETS_PER_CYCLE", "10") or "10"))

STATE_PATH = os.path.join(ROOT_DIR, "data", "polymarket_autobet_state.json")
LOG_PATH = os.path.join(ROOT_DIR, "data", "polymarket_autobet.log")

# ── Logging ───────────────────────────────────────────────────────────────────
os.makedirs(os.path.join(ROOT_DIR, "data"), exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
    ],
)
log = logging.getLogger("autobet")

_shutdown = False


def _handle_signal(signum, frame):
    global _shutdown
    log.info("Shutdown signal received — finishing current cycle then stopping.")
    _shutdown = True


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


# ── State management ──────────────────────────────────────────────────────────

def _load_state() -> dict[str, Any]:
    if os.path.exists(STATE_PATH):
        try:
            with open(STATE_PATH, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            if isinstance(data, dict):
                return data
        except Exception:
            pass
    return {"placed_bets": {}, "total_spent_usd": 0.0, "cycles": 0, "started_at": None}


def _save_state(state: dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    tmp = f"{STATE_PATH}.tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(state, fh, indent=2, sort_keys=True)
    os.replace(tmp, STATE_PATH)


def _bet_key(market_slug: str, side: str) -> str:
    return f"{market_slug}::{side}"


# ── Predictions source ────────────────────────────────────────────────────────

def _collect_predictions() -> list[dict[str, Any]]:
    """Gather today's predictions from the database, then normalise for Polymarket resolution."""
    raw: list[dict[str, Any]] = []

    # Primary: DB layer — today's predictions
    try:
        from data.db import get_predictions
        import datetime as _dt
        today_iso = _dt.date.today().isoformat()
        preds = get_predictions(days=1)
        for p in (preds or []):
            if not isinstance(p, dict):
                continue
            # Restrict to today
            if str(p.get("game_date") or "")[:10] < today_iso:
                continue
            prob = float(p.get("model_prob") or p.get("probability") or 0.0)
            if prob * 100 >= AUTOBET_MIN_CONFIDENCE:
                raw.append(p)
        log.info(f"[predictions] Loaded {len(raw)} from DB (today, ≥{AUTOBET_MIN_CONFIDENCE}% conf)")
    except Exception as e:
        log.warning(f"[predictions] DB load failed: {e}")

    # Fallback: live dashboard state (if running in same process)
    if not raw:
        try:
            from dashboard import _state  # type: ignore
            cards = list(_state.get("game_cards_today") or [])
            for card in cards:
                if not isinstance(card, dict):
                    continue
                for bet in (card.get("suggested_bets") or []):
                    if not isinstance(bet, dict):
                        continue
                    prob = float(bet.get("model_prob") or 0.0)
                    if prob * 100 >= AUTOBET_MIN_CONFIDENCE:
                        raw.append({**card, **bet})
            log.info(f"[predictions] Loaded {len(raw)} from dashboard state")
        except Exception as e:
            log.debug(f"[predictions] Dashboard state unavailable: {e}")

    if not raw:
        return []

    # Normalise using the same pipeline the dashboard uses before Polymarket resolution
    try:
        from dashboard import _clean_ready_bets_payload, _team_only_ready_bets  # type: ignore
        normalised = _clean_ready_bets_payload(raw)
        normalised = _team_only_ready_bets(normalised)
    except Exception:
        # Minimal normalisation if dashboard helpers unavailable
        normalised = []
        for idx, p in enumerate(raw):
            p.setdefault("uid", str(p.get("bet_uid") or p.get("id") or f"pred_{idx}"))
            p.setdefault("bet_uid", p["uid"])
            normalised.append(p)

    # Sort by confidence descending
    normalised.sort(key=lambda x: float(x.get("model_prob") or 0), reverse=True)
    return normalised


# ── Core betting logic ────────────────────────────────────────────────────────

def run_cycle(state: dict[str, Any]) -> dict[str, Any]:
    """Execute one scan-and-bet cycle. Returns updated state."""
    from data.polymarket import get_balance, resolve_ready_bets, place_order

    state["cycles"] = int(state.get("cycles") or 0) + 1
    cycle_num = state["cycles"]
    log.info(f"━━━ Cycle #{cycle_num} ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    # 1. Check balance
    try:
        bal_info = get_balance()
        buying_power = float(bal_info.get("buying_power_usd") or bal_info.get("balance_usd") or 0.0)
        portfolio = float(bal_info.get("portfolio_usd") or buying_power)
        log.info(f"[balance] Buying power: ${buying_power:.2f}  |  Portfolio: ${portfolio:.2f}")
        state["last_balance_usd"] = buying_power
        state["last_portfolio_usd"] = portfolio
        state["last_balance_check"] = datetime.datetime.utcnow().isoformat()
    except Exception as e:
        log.warning(f"[balance] Failed to fetch: {e}")
        buying_power = float(state.get("last_balance_usd") or 0.0)

    if buying_power < AUTOBET_AMOUNT_USD:
        log.warning(f"[balance] Insufficient funds (${buying_power:.2f} < ${AUTOBET_AMOUNT_USD:.2f}) — skipping this cycle")
        return state

    # 2. Collect predictions
    predictions = _collect_predictions()
    if not predictions:
        log.info("[predictions] No predictions available this cycle")
        return state
    log.info(f"[predictions] {len(predictions)} predictions above {AUTOBET_MIN_CONFIDENCE}% confidence")

    # 3. Resolve predictions against Polymarket markets
    try:
        resolution = resolve_ready_bets(predictions, force_refresh=(cycle_num % 3 == 1))
        resolutions = resolution.get("resolutions") or {}
        matched_count = resolution.get("matched") or 0
        market_count = resolution.get("market_count") or 0
        log.info(f"[polymarket] {matched_count} predictions matched to {market_count} live markets")
    except Exception as e:
        log.error(f"[polymarket] Market resolution failed: {e}")
        return state

    # 4. Place bets on matched, not-yet-bet markets
    placed_bets: dict[str, Any] = state.get("placed_bets") or {}
    bets_placed_this_cycle = 0
    total_spent_this_cycle = 0.0

    for idx, pred in enumerate(predictions):
        if bets_placed_this_cycle >= AUTOBET_MAX_BETS_PER_CYCLE:
            log.info(f"[autobet] Hit max bets per cycle ({AUTOBET_MAX_BETS_PER_CYCLE}) — stopping")
            break
        if buying_power - total_spent_this_cycle < AUTOBET_AMOUNT_USD:
            log.warning(f"[balance] Not enough funds for another bet — stopping")
            break

        uid = str(pred.get("uid") or pred.get("bet_uid") or pred.get("prediction_uid") or f"ready_{idx}")
        res = resolutions.get(uid) or {}
        if res.get("status") != "matched":
            continue

        market_slug = str(res.get("market_slug") or "").strip()
        market_ticker = str(res.get("market_ticker") or res.get("market_id") or "").strip()
        market_title = str(res.get("market_title") or "").strip()
        side = str(res.get("side") or "yes").strip().lower()
        price = float(res.get("price") or 0.5)

        if not market_slug:
            log.debug(f"[autobet] No slug for uid={uid}, skipping")
            continue

        bet_key = _bet_key(market_slug, side)
        if bet_key in placed_bets:
            log.debug(f"[autobet] Already bet on {market_slug} ({side}) — skipping")
            continue

        confidence = round(float(pred.get("model_prob") or 0.5) * 100, 1)
        pick = str(pred.get("pick") or pred.get("team") or "")
        sport = str(pred.get("sport") or "")
        game_date = str(pred.get("game_date") or "")
        log.info(
            f"[autobet] {'[DRY RUN] ' if AUTOBET_DRY_RUN else ''}Placing ${AUTOBET_AMOUNT_USD:.2f} {side.upper()} "
            f"on '{market_title}' ({market_slug}) | pick={pick} {confidence}% conf | {sport} {game_date}"
        )

        if AUTOBET_DRY_RUN:
            order_result = {
                "ok": True,
                "dry_run": True,
                "market_slug": market_slug,
                "side": side,
                "amount_usd": AUTOBET_AMOUNT_USD,
                "simulated": True,
            }
        else:
            try:
                order_result = place_order(
                    market_slug=market_slug,
                    amount_usd=AUTOBET_AMOUNT_USD,
                    side=side,
                    price=price,
                    order_type="ORDER_TYPE_MARKET",
                )
            except Exception as e:
                log.error(f"[autobet] Order failed for {market_slug}: {e}")
                continue

        if order_result.get("ok"):
            placed_bets[bet_key] = {
                "market_slug": market_slug,
                "market_ticker": market_ticker,
                "market_title": market_title,
                "side": side,
                "amount_usd": AUTOBET_AMOUNT_USD,
                "price": price,
                "confidence": confidence,
                "pick": pick,
                "sport": sport,
                "game_date": game_date,
                "placed_at": datetime.datetime.utcnow().isoformat(),
                "dry_run": AUTOBET_DRY_RUN,
                "order": order_result,
            }
            bets_placed_this_cycle += 1
            total_spent_this_cycle += AUTOBET_AMOUNT_USD
            state["total_spent_usd"] = round(float(state.get("total_spent_usd") or 0.0) + AUTOBET_AMOUNT_USD, 4)
            log.info(
                f"[autobet] {'[DRY RUN] ' if AUTOBET_DRY_RUN else ''}✅ Bet placed: "
                f"${AUTOBET_AMOUNT_USD:.2f} {side.upper()} | total spent: ${state['total_spent_usd']:.2f}"
            )
        else:
            log.warning(f"[autobet] Order returned not-ok for {market_slug}: {order_result}")

    state["placed_bets"] = placed_bets
    state["last_cycle_at"] = datetime.datetime.utcnow().isoformat()
    state["bets_placed_total"] = len(placed_bets)

    if bets_placed_this_cycle > 0:
        log.info(
            f"[cycle] Placed {bets_placed_this_cycle} bet(s) this cycle | "
            f"Spent ${total_spent_this_cycle:.2f} | "
            f"Total all-time: {len(placed_bets)} bets, ${state['total_spent_usd']:.2f}"
        )
    else:
        log.info("[cycle] No new bets this cycle (all matched markets already bet or insufficient funds)")

    return state


def print_status(state: dict[str, Any]) -> None:
    placed = state.get("placed_bets") or {}
    total_spent = float(state.get("total_spent_usd") or 0.0)
    last_bal = state.get("last_balance_usd")
    log.info("─── Auto-Bet Status ──────────────────────────────────────")
    log.info(f"  Total bets placed : {len(placed)}")
    log.info(f"  Total spent       : ${total_spent:.2f}")
    if last_bal is not None:
        log.info(f"  Last balance      : ${last_bal:.2f}")
    log.info(f"  Cycles run        : {state.get('cycles', 0)}")
    log.info(f"  Min confidence    : {AUTOBET_MIN_CONFIDENCE}%")
    log.info(f"  Amount per bet    : ${AUTOBET_AMOUNT_USD:.2f}")
    log.info(f"  Poll interval     : {AUTOBET_POLL_SEC}s")
    log.info(f"  Dry run mode      : {AUTOBET_DRY_RUN}")
    if placed:
        log.info("  Recent bets:")
        for bk, b in list(placed.items())[-5:]:
            dry = " [DRY]" if b.get("dry_run") else ""
            log.info(
                f"    {b.get('placed_at','?')[:19]}{dry} | {b.get('side','').upper()} "
                f"${b.get('amount_usd',0):.2f} | {b.get('market_title','')[:50]} | "
                f"{b.get('confidence',0)}% conf"
            )
    log.info("──────────────────────────────────────────────────────────")


def main() -> None:
    global _shutdown

    log.info("=" * 60)
    log.info("  Polymarket Auto-Betting Engine")
    log.info(f"  Bet amount    : ${AUTOBET_AMOUNT_USD:.2f} per market")
    log.info(f"  Poll interval : {AUTOBET_POLL_SEC}s")
    log.info(f"  Min confidence: {AUTOBET_MIN_CONFIDENCE}%")
    log.info(f"  Dry run       : {AUTOBET_DRY_RUN}")
    log.info(f"  State file    : {STATE_PATH}")
    log.info("=" * 60)

    if AUTOBET_DRY_RUN:
        log.warning("DRY RUN MODE — no real bets will be placed")

    state = _load_state()
    if not state.get("started_at"):
        state["started_at"] = datetime.datetime.utcnow().isoformat()

    while not _shutdown:
        try:
            state = run_cycle(state)
            _save_state(state)
            print_status(state)
        except KeyboardInterrupt:
            break
        except Exception as e:
            log.error(f"[cycle] Unhandled error: {e}", exc_info=True)
            _save_state(state)

        if _shutdown:
            break

        log.info(f"[sleep] Next scan in {AUTOBET_POLL_SEC}s  (Ctrl+C to stop)")
        # Sleep in short segments so Ctrl+C is responsive
        for _ in range(AUTOBET_POLL_SEC):
            if _shutdown:
                break
            time.sleep(1)

    log.info("Auto-betting engine stopped. Final state saved.")
    _save_state(state)
    print_status(state)


if __name__ == "__main__":
    main()
