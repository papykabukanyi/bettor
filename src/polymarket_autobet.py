"""
Unified Auto-Betting Engine — Polymarket
========================================
Continuously scans Polymarket for markets matching the bot's predictions,
places bets, and tracks every placed bet in real time.

How it works:
  1. Every AUTOBET_POLL_SEC, collect today's high-confidence predictions
  2. Resolve them against Polymarket
  3. For each prediction matched on Polymarket  → place $2 market order
  4. Skip any market already bet; skip if balance < $2
  6. Save state to data/polymarket_autobet_state.json after every cycle
  7. Dashboard /api/polymarket/autobet-status serves live bet history

Run:
    python src/polymarket_autobet.py

Environment overrides (.env or shell):
    AUTOBET_AMOUNT_USD         = 2.0    (USD per bet)
    AUTOBET_POLL_SEC           = 300    (seconds between cycles)
    AUTOBET_MIN_CONFIDENCE     = 52     (min model confidence %, 50-98)
    AUTOBET_DRY_RUN            = 0      (1 = simulate, no real orders)
    AUTOBET_MAX_BETS_PER_CYCLE = 10     (max new bets per cycle total)
    AUTOBET_EXCHANGES          = polymarket
"""

from __future__ import annotations

import concurrent.futures
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

try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(ROOT_DIR, ".env"))
except Exception:
    pass

# ── Configuration ─────────────────────────────────────────────────────────────
AUTOBET_AMOUNT_USD         = max(1.0, float(os.getenv("AUTOBET_AMOUNT_USD", "2.0") or "2.0"))
AUTOBET_POLL_SEC           = max(30, int(os.getenv("AUTOBET_POLL_SEC", "300") or "300"))
AUTOBET_MIN_CONFIDENCE     = max(50, min(98, int(os.getenv("AUTOBET_MIN_CONFIDENCE", "52") or "52")))
AUTOBET_DRY_RUN            = str(os.getenv("AUTOBET_DRY_RUN", "0")).strip().lower() in {"1", "true", "yes"}
AUTOBET_MAX_BETS_PER_CYCLE = max(1, int(os.getenv("AUTOBET_MAX_BETS_PER_CYCLE", "10") or "10"))
AUTOBET_EXCHANGES          = [e.strip().lower() for e in os.getenv("AUTOBET_EXCHANGES", "polymarket").split(",") if e.strip()]
DASHBOARD_PORT             = int(os.getenv("PORT", "5000") or "5000")

STATE_PATH = os.path.join(ROOT_DIR, "data", "polymarket_autobet_state.json")
LOG_PATH   = os.path.join(ROOT_DIR, "data", "polymarket_autobet.log")

# ── Logging ───────────────────────────────────────────────────────────────────
os.makedirs(os.path.join(ROOT_DIR, "data"), exist_ok=True)
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

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
    log.info("Shutdown signal — finishing current cycle then stopping.")
    _shutdown = True


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


# ── State ─────────────────────────────────────────────────────────────────────

def _load_state() -> dict[str, Any]:
    if os.path.exists(STATE_PATH):
        try:
            with open(STATE_PATH, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            if isinstance(data, dict):
                return data
        except Exception:
            pass
    return {
        "placed_bets": {},
        "total_spent_usd": 0.0,
        "total_spent_polymarket": 0.0,
        "total_spent_kalshi": 0.0,
        "cycles": 0,
        "started_at": None,
    }


def _save_state(state: dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(STATE_PATH), exist_ok=True)
    tmp = f"{STATE_PATH}.tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(state, fh, indent=2, sort_keys=True)
    os.replace(tmp, STATE_PATH)


def _bet_key(exchange: str, market_id: str, side: str) -> str:
    return f"{exchange}::{market_id}::{side}"


# ── Prediction collection ─────────────────────────────────────────────────────

def _collect_predictions() -> list[dict[str, Any]]:
    raw: list[dict[str, Any]] = []
    import datetime as _dt
    today_iso = _dt.date.today().isoformat()

    # 1. DB
    try:
        from data.db import get_predictions
        preds = get_predictions(days=1)
        for p in (preds or []):
            if not isinstance(p, dict):
                continue
            if str(p.get("game_date") or "")[:10] < today_iso:
                continue
            if float(p.get("model_prob") or 0.0) * 100 >= AUTOBET_MIN_CONFIDENCE:
                raw.append(p)
        if raw:
            log.info(f"[predictions] {len(raw)} from DB")
    except Exception as e:
        log.debug(f"[predictions] DB: {e}")

    # 2. Dashboard HTTP — /api/predictions
    if not raw:
        try:
            import urllib.request
            url = f"http://127.0.0.1:{DASHBOARD_PORT}/api/predictions?current_only=1&days=1"
            with urllib.request.urlopen(urllib.request.Request(url, headers={"Accept": "application/json"}), timeout=8) as resp:
                data = json.loads(resp.read().decode())
            for p in (data.get("predictions") or []):
                if not isinstance(p, dict):
                    continue
                if str(p.get("game_date") or "")[:10] < today_iso:
                    continue
                if float(p.get("model_prob") or 0.0) * 100 >= AUTOBET_MIN_CONFIDENCE:
                    raw.append(p)
            if raw:
                log.info(f"[predictions] {len(raw)} from dashboard /api/predictions")
        except Exception as e:
            log.debug(f"[predictions] /api/predictions: {e}")

    # 3. Dashboard HTTP — /api/cached-state game cards
    if not raw:
        try:
            import urllib.request
            url = f"http://127.0.0.1:{DASHBOARD_PORT}/api/cached-state"
            with urllib.request.urlopen(urllib.request.Request(url, headers={"Accept": "application/json"}), timeout=8) as resp:
                data = json.loads(resp.read().decode())
            for card in (data.get("game_cards_today") or []):
                if not isinstance(card, dict):
                    continue
                for bet in (card.get("suggested_bets") or []):
                    if not isinstance(bet, dict):
                        continue
                    if float(bet.get("model_prob") or 0.0) * 100 >= AUTOBET_MIN_CONFIDENCE:
                        raw.append({**card, **bet})
            if raw:
                log.info(f"[predictions] {len(raw)} from dashboard cached-state")
        except Exception as e:
            log.debug(f"[predictions] cached-state: {e}")

    if not raw:
        log.warning(
            "[predictions] No predictions found. "
            "Start the dashboard and run an analysis — picks up automatically next cycle."
        )
        return []

    # Normalise
    try:
        from dashboard import _clean_ready_bets_payload, _team_only_ready_bets  # type: ignore
        normalised = _clean_ready_bets_payload(raw)
        normalised = _team_only_ready_bets(normalised)
    except Exception:
        normalised = []
        for idx, p in enumerate(raw):
            p.setdefault("uid", str(p.get("bet_uid") or p.get("id") or f"pred_{idx}"))
            p.setdefault("bet_uid", p["uid"])
            normalised.append(p)

    normalised.sort(key=lambda x: float(x.get("model_prob") or 0), reverse=True)
    return normalised


# ── Exchange helpers ──────────────────────────────────────────────────────────

def _polymarket_resolve(predictions: list[dict], force: bool) -> dict[str, Any]:
    try:
        from data.polymarket import resolve_ready_bets
        return resolve_ready_bets(predictions, force_refresh=force)
    except Exception as e:
        log.error(f"[polymarket] resolve failed: {e}")
        return {"resolutions": {}, "matched": 0, "market_count": 0}


def _kalshi_resolve(predictions: list[dict], force: bool) -> dict[str, Any]:
    try:
        from data.kalshi import resolve_ready_bets
        return resolve_ready_bets(predictions, force_refresh=force)
    except Exception as e:
        log.error(f"[kalshi] resolve failed: {e}")
        return {"resolutions": {}, "summary": {"matched": 0}, "market_count": 0}


def _polymarket_balance() -> float:
    try:
        from data.polymarket import get_balance
        info = get_balance()
        return float(info.get("buying_power_usd") or info.get("balance_usd") or 0.0)
    except Exception as e:
        log.warning(f"[polymarket] balance error: {e}")
        return 0.0


def _kalshi_balance() -> float:
    try:
        from data.kalshi import get_balance
        info = get_balance()
        # Kalshi returns balance field names vary — try common ones
        for key in ("balance", "cash_balance", "available_balance", "buying_power"):
            val = info.get(key)
            if val is not None:
                try:
                    cents = float(val)
                    # Kalshi balance may be in cents
                    return cents / 100.0 if cents > 500 else cents
                except Exception:
                    pass
        # Try nested
        for key in ("portfolio_balance", "available"):
            sub = info.get(key)
            if isinstance(sub, dict):
                for k2 in ("balance", "cash"):
                    v = sub.get(k2)
                    if v is not None:
                        try:
                            cents = float(v)
                            return cents / 100.0 if cents > 500 else cents
                        except Exception:
                            pass
        log.debug(f"[kalshi] balance raw: {info}")
        return 0.0
    except Exception as e:
        log.warning(f"[kalshi] balance error: {e}")
        return 0.0


def _place_polymarket_bet(market_slug: str, side: str, price: float) -> dict[str, Any]:
    from data.polymarket import place_order
    return place_order(
        market_slug=market_slug,
        amount_usd=AUTOBET_AMOUNT_USD,
        side=side,
        price=price,
        order_type="ORDER_TYPE_MARKET",
    )


def _place_kalshi_bet(ticker: str, side: str, price_cents: int) -> dict[str, Any]:
    from data.kalshi import place_order
    import datetime as _dt
    # Kalshi order: contracts = (amount_usd * 100) / price_cents
    price_cents = max(1, min(99, int(price_cents or 50)))
    count = max(1, int((AUTOBET_AMOUNT_USD * 100.0) // price_cents))
    client_id = f"autobet_{_dt.datetime.utcnow().strftime('%Y%m%d%H%M%S')}_{abs(hash(ticker + side)) % 99999:05d}"
    payload: dict[str, Any] = {
        "ticker": ticker,
        "side": side,
        "action": "buy",
        "type": "limit",
        "count": count,
        "client_order_id": client_id,
    }
    if side == "yes":
        payload["yes_price"] = price_cents
    else:
        payload["no_price"] = price_cents
    result = place_order(payload)
    # Normalize response to have "ok" field
    if isinstance(result, dict):
        # Kalshi returns {"order": {...}} on success
        ok = bool(result.get("order") or result.get("id") or result.get("ticker") or result.get("client_order_id"))
        return {"ok": ok, "exchange": "kalshi", "ticker": ticker, "side": side, "count": count, "price_cents": price_cents, "response": result}
    return {"ok": False, "exchange": "kalshi", "response": result}


# ── Main cycle ────────────────────────────────────────────────────────────────

def run_cycle(state: dict[str, Any]) -> dict[str, Any]:
    state["cycles"] = int(state.get("cycles") or 0) + 1
    cycle_num = state["cycles"]
    force_refresh = (cycle_num % 3 == 1)
    log.info(f"--- Cycle #{cycle_num} [exchanges: {', '.join(AUTOBET_EXCHANGES)}] ---")

    # 1. Check balances for enabled exchanges
    balances: dict[str, float] = {}
    if "polymarket" in AUTOBET_EXCHANGES:
        bp = _polymarket_balance()
        balances["polymarket"] = bp
        log.info(f"[polymarket] Buying power: ${bp:.2f}")
        state["last_balance_polymarket"] = bp
    if "kalshi" in AUTOBET_EXCHANGES:
        kb = _kalshi_balance()
        balances["kalshi"] = kb
        log.info(f"[kalshi] Buying power: ${kb:.2f}")
        state["last_balance_kalshi"] = kb
    state["last_balance_check"] = datetime.datetime.utcnow().isoformat()

    # Primary balance for display (Polymarket)
    state["last_balance_usd"] = balances.get("polymarket") or balances.get("kalshi") or 0.0

    # 2. Collect predictions
    predictions = _collect_predictions()
    if not predictions:
        log.info("[predictions] No predictions available this cycle")
        state["last_cycle_at"] = datetime.datetime.utcnow().isoformat()
        return state
    log.info(f"[predictions] {len(predictions)} bets >={AUTOBET_MIN_CONFIDENCE}% confidence")

    # 3. Resolve predictions against both exchanges in parallel
    resolutions: dict[str, dict[str, Any]] = {}  # exchange -> resolve result

    def _resolve_exchange(ex: str) -> tuple[str, dict]:
        if ex == "polymarket":
            return ex, _polymarket_resolve(predictions, force_refresh)
        elif ex == "kalshi":
            return ex, _kalshi_resolve(predictions, force_refresh)
        return ex, {}

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        futures = {pool.submit(_resolve_exchange, ex): ex for ex in AUTOBET_EXCHANGES}
        for fut in concurrent.futures.as_completed(futures):
            try:
                ex, result = fut.result()
                resolutions[ex] = result
                matched = result.get("matched") or result.get("summary", {}).get("matched", 0)
                mkt_count = result.get("market_count") or result.get("summary", {}).get("market_count", 0) or 0
                log.info(f"[{ex}] {matched} matched / {mkt_count} markets")
            except Exception as e:
                log.error(f"[resolve] error: {e}")

    # 4. Place bets
    placed_bets: dict[str, Any] = state.get("placed_bets") or {}
    bets_placed = 0
    spent_this_cycle = 0.0

    for pred in predictions:
        if bets_placed >= AUTOBET_MAX_BETS_PER_CYCLE:
            log.info(f"[autobet] Max {AUTOBET_MAX_BETS_PER_CYCLE} bets/cycle reached")
            break

        uid = str(pred.get("uid") or pred.get("bet_uid") or pred.get("prediction_uid") or "")
        confidence = round(float(pred.get("model_prob") or 0.5) * 100, 1)
        pick = str(pred.get("pick") or pred.get("team") or "")
        sport = str(pred.get("sport") or "")
        game_date = str(pred.get("game_date") or "")

        for exchange in AUTOBET_EXCHANGES:
            if bets_placed >= AUTOBET_MAX_BETS_PER_CYCLE:
                break
            bal = balances.get(exchange, 0.0)
            if bal - spent_this_cycle < AUTOBET_AMOUNT_USD:
                log.debug(f"[{exchange}] Insufficient balance (${bal:.2f}) for another bet")
                continue

            ex_resolutions = (resolutions.get(exchange) or {}).get("resolutions") or {}
            res = ex_resolutions.get(uid) or {}
            if res.get("status") != "matched":
                continue

            # Build market identifier and bet key
            if exchange == "polymarket":
                market_id = str(res.get("market_slug") or res.get("market_id") or "").strip()
                market_title = str(res.get("market_title") or "").strip()
                side = str(res.get("side") or "yes").lower()
            else:  # kalshi
                market_id = str(res.get("market_ticker") or "").strip()
                market_title = str(res.get("market_title") or "").strip()
                side = str(res.get("side") or "yes").lower()

            if not market_id:
                continue

            bk = _bet_key(exchange, market_id, side)
            if bk in placed_bets:
                log.debug(f"[{exchange}] Already bet: {market_id} ({side})")
                continue

            log.info(
                f"[{exchange}] {'[DRY RUN] ' if AUTOBET_DRY_RUN else ''}"
                f"Placing ${AUTOBET_AMOUNT_USD:.2f} {side.upper()} on '{market_title}' | "
                f"pick={pick} {confidence}% | {sport} {game_date}"
            )

            if AUTOBET_DRY_RUN:
                order_result = {"ok": True, "dry_run": True, "exchange": exchange, "market_id": market_id, "side": side}
            else:
                try:
                    if exchange == "polymarket":
                        price = float(res.get("price") or 0.5)
                        order_result = _place_polymarket_bet(market_id, side, price)
                    else:
                        price_cents = int(res.get("price_cents") or 50)
                        order_result = _place_kalshi_bet(market_id, side, price_cents)
                except Exception as e:
                    log.error(f"[{exchange}] Order failed for {market_id}: {e}")
                    continue

            if order_result.get("ok"):
                placed_bets[bk] = {
                    "exchange": exchange,
                    "market_id": market_id,
                    "market_title": market_title,
                    "market_slug": res.get("market_slug") or "",
                    "market_ticker": res.get("market_ticker") or "",
                    "side": side,
                    "amount_usd": AUTOBET_AMOUNT_USD,
                    "price": res.get("price") or (res.get("price_cents", 50) / 100),
                    "confidence": confidence,
                    "pick": pick,
                    "sport": sport,
                    "game_date": game_date,
                    "placed_at": datetime.datetime.utcnow().isoformat(),
                    "dry_run": AUTOBET_DRY_RUN,
                    "order": order_result,
                }
                bets_placed += 1
                spent_this_cycle += AUTOBET_AMOUNT_USD
                state["total_spent_usd"] = round(float(state.get("total_spent_usd") or 0) + AUTOBET_AMOUNT_USD, 4)
                key_spent = f"total_spent_{exchange}"
                state[key_spent] = round(float(state.get(key_spent) or 0) + AUTOBET_AMOUNT_USD, 4)
                log.info(
                    f"[{exchange}] {'[DRY RUN] ' if AUTOBET_DRY_RUN else ''}[PLACED] "
                    f"${AUTOBET_AMOUNT_USD:.2f} {side.upper()} | "
                    f"total spent: ${state['total_spent_usd']:.2f}"
                )
            else:
                log.warning(f"[{exchange}] Order returned not-ok: {order_result}")

    state["placed_bets"] = placed_bets
    state["last_cycle_at"] = datetime.datetime.utcnow().isoformat()
    state["bets_placed_total"] = len(placed_bets)

    if bets_placed:
        log.info(f"[cycle] Placed {bets_placed} bet(s), ${spent_this_cycle:.2f} | All-time: {len(placed_bets)} bets, ${state['total_spent_usd']:.2f}")
    else:
        log.info("[cycle] No new bets (all markets already bet or no matches)")

    return state


def print_status(state: dict[str, Any]) -> None:
    placed = state.get("placed_bets") or {}
    by_ex: dict[str, int] = {}
    for b in placed.values():
        if isinstance(b, dict):
            ex = b.get("exchange", "?")
            by_ex[ex] = by_ex.get(ex, 0) + 1

    log.info("--- Auto-Bet Status ----------------------------------------------")
    log.info(f"  Total bets    : {len(placed)}  ({', '.join(f'{ex}:{n}' for ex,n in by_ex.items()) or 'none'})")
    log.info(f"  Total spent   : ${float(state.get('total_spent_usd') or 0):.2f}")
    for ex in AUTOBET_EXCHANGES:
        bal = state.get(f"last_balance_{ex}")
        spent = float(state.get(f"total_spent_{ex}") or 0)
        if bal is not None:
            log.info(f"  {ex:<12}: balance=${bal:.2f}  spent=${spent:.2f}")
    log.info(f"  Cycles        : {state.get('cycles', 0)}")
    log.info(f"  Min confidence: {AUTOBET_MIN_CONFIDENCE}%  |  Amount/bet: ${AUTOBET_AMOUNT_USD:.2f}  |  Dry run: {AUTOBET_DRY_RUN}")
    if placed:
        log.info("  Recent bets (last 5):")
        for bk, b in list(placed.items())[-5:]:
            if not isinstance(b, dict):
                continue
            dry = " [DRY]" if b.get("dry_run") else ""
            ex_tag = f"[{b.get('exchange','?').upper()}]"
            log.info(
                f"    {b.get('placed_at','')[:19]}{dry} {ex_tag} {b.get('side','').upper()} "
                f"${b.get('amount_usd',0):.2f} | {b.get('market_title','')[:45]} | {b.get('confidence',0)}%"
            )
    log.info("-----------------------------------------------------------------")


def main() -> None:
    global _shutdown

    log.info("=" * 60)
    log.info("  Unified Auto-Betting Engine")
    log.info(f"  Exchanges     : {', '.join(AUTOBET_EXCHANGES)}")
    log.info(f"  Bet amount    : ${AUTOBET_AMOUNT_USD:.2f} per market")
    log.info(f"  Poll interval : {AUTOBET_POLL_SEC}s")
    log.info(f"  Min confidence: {AUTOBET_MIN_CONFIDENCE}%")
    log.info(f"  Dry run       : {AUTOBET_DRY_RUN}")
    log.info(f"  State file    : {STATE_PATH}")
    log.info("=" * 60)

    if AUTOBET_DRY_RUN:
        log.warning("DRY RUN MODE - no real orders will be placed")

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
            log.error(f"[cycle] Error: {e}", exc_info=True)
            _save_state(state)

        if _shutdown:
            break

        log.info(f"[sleep] Next scan in {AUTOBET_POLL_SEC}s  (Ctrl+C to stop)")
        for _ in range(AUTOBET_POLL_SEC):
            if _shutdown:
                break
            time.sleep(1)

    log.info("Autobet engine stopped.")
    _save_state(state)
    print_status(state)


if __name__ == "__main__":
    main()
