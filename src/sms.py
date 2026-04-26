"""
ClickSend SMS helper
====================
Sends daily pick summaries (top 3 parlays + today's games/bets) to all
registered phone numbers via the ClickSend REST API.

Environment variables required:
  CLICKSEND_USERNAME  — your ClickSend account email / username
  CLICKSEND_API_KEY   — your ClickSend API key
"""

import base64
import datetime
import os

import requests

CLICKSEND_USERNAME = os.getenv("CLICKSEND_USERNAME", "")
CLICKSEND_API_KEY  = os.getenv("CLICKSEND_API_KEY", "")
_SENDER_ID         = "Bettor"   # shown as sender (max 11 chars, alphanumeric)


# ─── Auth ────────────────────────────────────────────────────────────────────

def _auth_header() -> dict:
    token = base64.b64encode(
        f"{CLICKSEND_USERNAME}:{CLICKSEND_API_KEY}".encode()
    ).decode()
    return {
        "Authorization": f"Basic {token}",
        "Content-Type":  "application/json",
    }


# ─── Low-level send ──────────────────────────────────────────────────────────

def send_sms(to: str, message: str) -> dict:
    """
    Send a single SMS via ClickSend.
    Returns the parsed JSON response dict (or {'error': '...'} on failure).
    """
    if not CLICKSEND_USERNAME or not CLICKSEND_API_KEY:
        return {"error": "CLICKSEND_USERNAME or CLICKSEND_API_KEY not configured"}

    url  = "https://rest.clicksend.com/v3/sms/send"
    body = {
        "messages": [{
            "to":     to,
            "body":   message[:918],   # SMS hard limit ~160 chars per segment; trim to ~6 segments
            "source": _SENDER_ID,
        }]
    }
    try:
        resp = requests.post(url, json=body, headers=_auth_header(), timeout=20)
        return resp.json()
    except Exception as exc:
        return {"error": str(exc)}


# ─── Message formatter ───────────────────────────────────────────────────────

def format_daily_picks(state: dict) -> str:
    """
    Build the SMS text from the current analysis state.
    Includes:
      - Top 3 parlays (n-leg, combined return, safety label)
      - Today's games + best bet per game
    Player-prop detail is kept off the text (user visits the dashboard).
    """
    today_str = datetime.date.today().strftime("%b %d")
    lines     = [f"BETTOR PICKS - {today_str}"]

    # ── Top 3 parlays ──────────────────────────────────────────────────────
    parlays = state.get("best_parlays", [])
    if parlays:
        lines.append("")
        lines.append("TOP PARLAYS:")
        for i, p in enumerate(parlays[:3], 1):
            legs      = p.get("legs", [])
            combined  = float(p.get("combined_dec", 1.0))
            pct_str   = f"+{round((combined - 1) * 100)}%" if combined > 1 else ""
            safety    = p.get("safety_label", "")
            n         = p.get("n_legs", len(legs))
            # Show first 2 leg labels then "…" if more
            labels = [l.get("label", "?") for l in legs[:2]]
            rest   = f" +{len(legs)-2} more" if len(legs) > 2 else ""
            lines.append(
                f"{i}. [{n}-leg {pct_str}] "
                + " / ".join(labels)
                + rest
                + f" [{safety}]"
            )

    # ── Today's games ──────────────────────────────────────────────────────
    cards = state.get("game_cards_today", [])
    if cards:
        lines.append("")
        lines.append("TODAY'S GAMES & BETS:")
        for card in cards[:6]:
            ht   = card.get("home_team", "?")
            at   = card.get("away_team", "?")
            when = card.get("when", "")
            wp   = card.get("win_pick")
            if wp:
                pick  = wp.get("pick_team", "")
                odds  = wp.get("odds_str", "")
                sl    = wp.get("safety_label", "")
                lines.append(f"* {at} @ {ht} ({when})")
                lines.append(f"  -> {pick} {odds} [{sl}]")
            else:
                lines.append(f"* {at} @ {ht} ({when})")

    lines.append("")
    lines.append("Player props: visit your Bettor dashboard.")
    return "\n".join(lines)


def format_parlay_message(parlay: dict) -> str:
    """Format a single parlay for SMS sending."""
    today_str = datetime.date.today().strftime("%b %d")
    name = parlay.get("name") or "Parlay"
    legs = parlay.get("legs") or []
    combined = float(parlay.get("combined_odds") or parlay.get("combined_dec") or 0)
    pct_str = f"+{round((combined - 1) * 100)}%" if combined and combined > 1 else ""
    lines = [f"BETTOR PARLAY - {today_str}", f"{name} {pct_str}".strip()]

    if legs:
        lines.append("")
        lines.append("LEGS:")
        for i, l in enumerate(legs[:8], 1):
            label = l.get("label") or l.get("pick") or "?"
            odds = l.get("dec_odds")
            odds_str = f" x{float(odds):.2f}" if odds else ""
            lines.append(f"{i}. {label}{odds_str}")
        if len(legs) > 8:
            lines.append(f"+{len(legs)-8} more")

    return "\n".join(lines)


def send_parlay_to_all(parlay: dict) -> dict:
    """
    Send a specific parlay to all active phone numbers.
    Returns: {sent, failed, errors}
    """
    try:
        import sys, os
        sys.path.insert(0, os.path.dirname(__file__))
        from data.db import get_phone_numbers
        numbers = get_phone_numbers(active_only=True)
    except Exception as exc:
        return {"sent": 0, "failed": 0, "errors": [{"phone": "db", "error": str(exc)}]}

    if not numbers:
        return {"sent": 0, "failed": 0, "errors": [], "note": "No phone numbers registered"}

    message = format_parlay_message(parlay)
    results = {"sent": 0, "failed": 0, "errors": []}

    for row in numbers:
        phone = (row.get("phone") or "").strip()
        if not phone:
            continue
        resp = send_sms(phone, message)
        ok = (
            resp.get("http_code") == 200
            or (isinstance(resp.get("data"), dict)
                and resp["data"].get("total_count") is not None)
        )
        if ok:
            results["sent"] += 1
        else:
            results["failed"] += 1
            results["errors"].append({"phone": phone, "error": str(resp)})

    return results


# ─── Bulk send ───────────────────────────────────────────────────────────────

def send_daily_picks_to_all(state: dict) -> dict:
    """
    Format the picks message and send it to every active phone number
    stored in the database.

    Returns:
        {"sent": int, "failed": int, "errors": [{"phone": ..., "error": ...}]}
    """
    try:
        import sys, os
        sys.path.insert(0, os.path.dirname(__file__))
        from data.db import get_phone_numbers
        numbers = get_phone_numbers(active_only=True)
    except Exception as exc:
        return {"sent": 0, "failed": 0, "errors": [{"phone": "db", "error": str(exc)}]}

    if not numbers:
        return {"sent": 0, "failed": 0, "errors": [], "note": "No phone numbers registered"}

    message = format_daily_picks(state)
    results = {"sent": 0, "failed": 0, "errors": []}

    for row in numbers:
        phone = (row.get("phone") or "").strip()
        if not phone:
            continue
        resp = send_sms(phone, message)
        # ClickSend returns http_code=200 on success
        ok = (
            resp.get("http_code") == 200
            or (isinstance(resp.get("data"), dict)
                and resp["data"].get("total_count") is not None)
        )
        if ok:
            results["sent"] += 1
        else:
            results["failed"] += 1
            results["errors"].append({"phone": phone, "error": str(resp)})

    return results


# Backward-compatible alias
def send_daily_picks(state: dict) -> dict:
    return send_daily_picks_to_all(state)
