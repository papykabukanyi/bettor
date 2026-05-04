"""
Email notification helper (SMTP)
=================================
Sends daily pick summaries and parlay alerts via SMTP (Gmail App Password).

Environment variables:
  EMAIL_ENABLED   — "true" / "1" to enable  (default: true)
  EMAIL_HOST      — SMTP host               (default: smtp.gmail.com)
  EMAIL_PORT      — SMTP port               (default: 587)
  EMAIL_USER      — SMTP login / From address
  EMAIL_PASS      — App password (Google: 16-char, spaces OK)
  EMAIL_TO        — Comma-separated recipient addresses
"""

import datetime
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# ── Config ────────────────────────────────────────────────────────────────────
_ENABLED  = os.getenv("EMAIL_ENABLED", "true").strip().lower() in {"true", "1", "yes", "on"}
_HOST     = os.getenv("EMAIL_HOST",    "smtp.gmail.com")
_PORT     = int(os.getenv("EMAIL_PORT", "587"))
_USER     = os.getenv("EMAIL_USER",    "")
_PASS     = os.getenv("EMAIL_PASS",    "").replace(" ", "")   # strip spaces from app password
_TO_RAW   = os.getenv("EMAIL_TO",      "")


def _recipients() -> list[str]:
    """Return the current list of recipient addresses from ENV."""
    return [e.strip() for e in _TO_RAW.split(",") if e.strip()]


# ── Low-level send ────────────────────────────────────────────────────────────

def send_email(subject: str, html_body: str, plain_body: str = "") -> dict:
    """
    Send an email to all configured recipients.
    Returns {"sent": n, "failed": n, "errors": [...]}
    """
    if not _ENABLED:
        return {"sent": 0, "failed": 0, "errors": [], "note": "Email disabled (EMAIL_ENABLED=false)"}
    if not _USER or not _PASS:
        return {"sent": 0, "failed": 0, "errors": ["EMAIL_USER or EMAIL_PASS not set"]}

    to_list = _recipients()
    if not to_list:
        return {"sent": 0, "failed": 0, "errors": [], "note": "No recipients in EMAIL_TO"}

    results = {"sent": 0, "failed": 0, "errors": []}
    try:
        with smtplib.SMTP(_HOST, _PORT, timeout=20) as server:
            server.ehlo()
            server.starttls()
            server.login(_USER, _PASS)
            for recipient in to_list:
                try:
                    msg = MIMEMultipart("alternative")
                    msg["Subject"] = subject
                    msg["From"]    = _USER
                    msg["To"]      = recipient
                    if plain_body:
                        msg.attach(MIMEText(plain_body, "plain"))
                    msg.attach(MIMEText(html_body, "html"))
                    server.sendmail(_USER, recipient, msg.as_string())
                    results["sent"] += 1
                    print(f"[email] Sent to {recipient}")
                except Exception as exc:
                    results["failed"] += 1
                    results["errors"].append({"to": recipient, "error": str(exc)})
    except Exception as exc:
        results["failed"] += len(to_list)
        results["errors"].append({"to": "smtp", "error": str(exc)})

    return results


# ── Message formatters ────────────────────────────────────────────────────────

def _html_table_row(label: str, value: str, highlight: bool = False) -> str:
    bg = "#1e3a5f" if highlight else "#0d1b2e"
    return (f'<tr><td style="padding:6px 10px;color:#7aa3c8;font-size:13px;">{label}</td>'
            f'<td style="padding:6px 10px;color:#e2e8f0;font-size:13px;font-weight:bold;'
            f'background:{bg};">{value}</td></tr>')


def _safety_badge(label: str) -> str:
    colors = {
        "ELITE":    "#22c55e",
        "SAFE":     "#3b82f6",
        "MODERATE": "#f59e0b",
        "RISKY":    "#ef4444",
    }
    color = colors.get((label or "").upper(), "#64748b")
    return (f'<span style="background:{color};color:#fff;padding:2px 8px;'
            f'border-radius:4px;font-size:11px;font-weight:bold;">{label or "?"}</span>')


def format_daily_picks_html(state: dict) -> tuple[str, str]:
    """
    Return (html_body, plain_body) from current analysis state.
    Triggered after every fresh analysis save.
    """
    today_str = datetime.date.today().strftime("%B %d, %Y")
    parlays   = state.get("best_parlays", [])
    cards     = state.get("game_cards_today", [])
    props     = state.get("player_props", [])

    # ── HTML ─────────────────────────────────────────────────────────────────
    html = f"""<!DOCTYPE html>
<html><body style="margin:0;padding:0;background:#06111e;font-family:Arial,sans-serif;">
<div style="max-width:640px;margin:0 auto;padding:24px;">
  <div style="background:#0d1b2e;border-radius:12px;padding:20px 28px;margin-bottom:20px;">
    <h1 style="color:#3b82f6;margin:0 0 4px;font-size:22px;">Bettor Daily Picks</h1>
    <p style="color:#94a3b8;margin:0;font-size:14px;">{today_str}</p>
  </div>
"""

    # Parlays
    if parlays:
        html += '<div style="background:#0d1b2e;border-radius:12px;padding:20px 28px;margin-bottom:20px;">'
        html += '<h2 style="color:#f59e0b;margin:0 0 14px;font-size:16px;">Top Parlays</h2>'
        for i, p in enumerate(parlays[:5], 1):
            legs     = p.get("legs", [])
            dec      = float(p.get("combined_dec", 1.0))
            pct      = round((dec - 1) * 100)
            safety   = p.get("safety_label", "")
            n_legs   = p.get("n_legs", len(legs))
            html += (f'<div style="border-left:3px solid #3b82f6;padding-left:12px;margin-bottom:14px;">'
                     f'<div style="color:#e2e8f0;font-weight:bold;margin-bottom:6px;">'
                     f'#{i} {n_legs}-Leg Parlay &nbsp;'
                     f'<span style="color:#22c55e;">+{pct}%</span> &nbsp;{_safety_badge(safety)}'
                     f'</div>')
            for leg in legs[:6]:
                label = leg.get("label") or leg.get("pick") or "?"
                odds  = leg.get("dec_odds")
                odds_s = f" &nbsp;<span style='color:#94a3b8;font-size:12px;'>x{float(odds):.2f}</span>" if odds else ""
                html += f'<div style="color:#94a3b8;font-size:13px;padding:2px 0;">• {label}{odds_s}</div>'
            if len(legs) > 6:
                html += f'<div style="color:#64748b;font-size:12px;">+{len(legs)-6} more legs</div>'
            html += '</div>'
        html += '</div>'

    # Today's games
    if cards:
        html += '<div style="background:#0d1b2e;border-radius:12px;padding:20px 28px;margin-bottom:20px;">'
        html += '<h2 style="color:#3b82f6;margin:0 0 14px;font-size:16px;">Today\'s Games &amp; Best Bets</h2>'
        html += '<table style="width:100%;border-collapse:collapse;">'
        for card in cards[:10]:
            ht   = card.get("home_team", "?")
            at   = card.get("away_team", "?")
            time_s = card.get("game_time", "")
            ml   = card.get("moneyline") or {}
            rl   = card.get("run_line") or {}
            best = ml or rl
            if best:
                pick   = best.get("pick", "")
                odds_v = best.get("odds_am")
                odds_s = f"({'+'if odds_v and odds_v>0 else ''}{odds_v})" if odds_v else ""
                safety = best.get("safety_label", "")
                html += _html_table_row(
                    f"{at} @ {ht}" + (f" <span style='color:#64748b;font-size:11px;'>{time_s}</span>" if time_s else ""),
                    f"{pick} {odds_s} {_safety_badge(safety)}",
                    highlight=True,
                )
            else:
                html += _html_table_row(f"{at} @ {ht}", time_s or "—")
        html += '</table></div>'

    # Player props (top 10 OVER)
    over_props = [p for p in props if (p.get("direction") or "").upper() == "OVER"][:10]
    if over_props:
        html += '<div style="background:#0d1b2e;border-radius:12px;padding:20px 28px;margin-bottom:20px;">'
        html += '<h2 style="color:#a855f7;margin:0 0 14px;font-size:16px;">Top Player Props (OVER)</h2>'
        html += '<table style="width:100%;border-collapse:collapse;">'
        for p in over_props:
            name  = p.get("name", "?")
            team  = p.get("team", "")
            label = p.get("prop_label", p.get("stat_type", ""))
            line  = p.get("line", "")
            conf  = p.get("confidence") or p.get("conf") or ""
            html += _html_table_row(
                f"{name} <span style='color:#64748b;font-size:11px;'>({team})</span>",
                f"OVER {line} {label}" + (f" &nbsp;<span style='color:#22c55e;font-size:12px;'>{conf}%</span>" if conf else ""),
            )
        html += '</table></div>'

    html += '</div></body></html>'

    # ── Plain text fallback ───────────────────────────────────────────────────
    lines = [f"BETTOR PICKS — {today_str}", ""]
    if parlays:
        lines.append("=== TOP PARLAYS ===")
        for i, p in enumerate(parlays[:5], 1):
            legs  = p.get("legs", [])
            dec   = float(p.get("combined_dec", 1.0))
            pct   = round((dec - 1) * 100)
            safety = p.get("safety_label", "")
            lines.append(f"{i}. {p.get('n_legs', len(legs))}-leg +{pct}% [{safety}]")
            for leg in legs[:4]:
                lines.append(f"   • {leg.get('label','?')}")
        lines.append("")

    if cards:
        lines.append("=== TODAY'S GAMES ===")
        for card in cards[:10]:
            ht = card.get("home_team","?"); at = card.get("away_team","?")
            ml = card.get("moneyline") or {}
            pick = ml.get("pick",""); odds = ml.get("odds_am","")
            odds_s = f"({'+' if odds and odds>0 else ''}{odds})" if odds else ""
            lines.append(f"{at} @ {ht}: {pick} {odds_s}".strip())
        lines.append("")

    if over_props:
        lines.append("=== PLAYER PROPS (OVER) ===")
        for p in over_props:
            lines.append(f"{p.get('name','?')} ({p.get('team','')}) OVER {p.get('line','')} {p.get('prop_label','')}")
        lines.append("")

    lines.append("Visit your Bettor dashboard for full details.")
    plain = "\n".join(lines)

    return html, plain


def format_parlay_html(parlay: dict) -> tuple[str, str]:
    """Return (html_body, plain_body) for a single parlay alert."""
    today_str = datetime.date.today().strftime("%B %d, %Y")
    name  = parlay.get("name") or "Parlay"
    legs  = parlay.get("legs") or []
    dec   = float(parlay.get("combined_odds") or parlay.get("combined_dec") or 0)
    pct   = round((dec - 1) * 100) if dec > 1 else 0
    safety = parlay.get("safety_label", "")

    html = f"""<!DOCTYPE html>
<html><body style="margin:0;padding:0;background:#06111e;font-family:Arial,sans-serif;">
<div style="max-width:640px;margin:0 auto;padding:24px;">
  <div style="background:#0d1b2e;border-radius:12px;padding:20px 28px;margin-bottom:20px;">
    <h1 style="color:#f59e0b;margin:0 0 4px;font-size:22px;">Parlay Alert 🏆</h1>
    <p style="color:#94a3b8;margin:0;font-size:14px;">{today_str}</p>
  </div>
  <div style="background:#0d1b2e;border-radius:12px;padding:20px 28px;">
    <div style="color:#e2e8f0;font-size:18px;font-weight:bold;margin-bottom:8px;">
      {name} &nbsp;
      <span style="color:#22c55e;">+{pct}%</span> &nbsp;
      {_safety_badge(safety)}
    </div>
    <table style="width:100%;border-collapse:collapse;margin-top:14px;">
"""
    for i, leg in enumerate(legs, 1):
        label = leg.get("label") or leg.get("pick") or "?"
        odds  = leg.get("dec_odds")
        odds_s = f"x{float(odds):.2f}" if odds else ""
        html += _html_table_row(f"Leg {i}", f"{label}  {odds_s}")
    html += '</table></div></div></body></html>'

    plain = f"BETTOR PARLAY ALERT — {today_str}\n{name} +{pct}% [{safety}]\n\nLEGS:\n"
    for i, leg in enumerate(legs, 1):
        plain += f"{i}. {leg.get('label','?')}\n"

    return html, plain


# ── Public API ────────────────────────────────────────────────────────────────

def send_daily_picks(state: dict) -> dict:
    """Format and email the full daily picks snapshot."""
    html, plain = format_daily_picks_html(state)
    today_str   = datetime.date.today().strftime("%b %d")
    subject     = f"Bettor Picks — {today_str}"
    return send_email(subject, html, plain)


def send_parlay_alert(parlay: dict) -> dict:
    """Email a single parlay alert."""
    html, plain = format_parlay_html(parlay)
    today_str   = datetime.date.today().strftime("%b %d")
    name        = (parlay.get("name") or "Parlay")
    subject     = f"Bettor Parlay Alert — {name} — {today_str}"
    return send_email(subject, html, plain)
