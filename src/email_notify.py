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

papykabukanyi@gmail.com always receives all emails regardless of EMAIL_TO.
"""

import datetime
import os
import smtplib
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Primary recipient — always gets all emails
_PRIMARY_RECIPIENT = "papykabukanyi@gmail.com"
_EMAIL_NETWORK_ERROR_BACKOFF_SEC = max(300, int(os.getenv("EMAIL_NETWORK_ERROR_BACKOFF_SEC", "1800") or "1800"))
_EMAIL_DISABLED_UNTIL = 0.0


def _is_network_error(exc: Exception | str) -> bool:
    text = str(exc or "").lower()
    return any(token in text for token in (
        "network is unreachable",
        "no route to host",
        "name or service not known",
        "temporary failure in name resolution",
        "timed out",
        "connection refused",
        "connection reset",
        "connection aborted",
    ))


def _email_settings() -> dict:
    return {
        "enabled": os.getenv("EMAIL_ENABLED", "true").strip().lower() in {"true", "1", "yes", "on"},
        "host": os.getenv("EMAIL_HOST", "smtp.gmail.com").strip() or "smtp.gmail.com",
        "port": int(os.getenv("EMAIL_PORT", "587") or "587"),
        "user": os.getenv("EMAIL_USER", "").strip(),
        "password": os.getenv("EMAIL_PASS", "").replace(" ", ""),
        "to_raw": os.getenv("EMAIL_TO", ""),
        "use_ssl": os.getenv("EMAIL_USE_SSL", "").strip().lower() in {"true", "1", "yes", "on"},
    }


def _recipients(settings: dict | None = None) -> list[str]:
    """Return deduped recipient list, always including the primary address."""
    cfg = settings or _email_settings()
    extras = [e.strip() for e in str(cfg.get("to_raw") or "").split(",") if e.strip()]
    seen = set()
    result = []
    for addr in [_PRIMARY_RECIPIENT] + extras:
        if addr not in seen:
            seen.add(addr)
            result.append(addr)
    return result


# ── Low-level send ────────────────────────────────────────────────────────────

def send_email(subject: str, html_body: str, plain_body: str = "") -> dict:
    """
    Send an email to all configured recipients.
    Returns {"sent": n, "failed": n, "errors": [...]}
    """
    settings = _email_settings()
    if not settings["enabled"]:
        return {"ok": False, "sent": 0, "failed": 0, "errors": [], "note": "Email disabled (EMAIL_ENABLED=false)"}
    if not settings["user"] or not settings["password"]:
        return {"ok": False, "sent": 0, "failed": 0, "errors": ["EMAIL_USER or EMAIL_PASS not set"]}

    global _EMAIL_DISABLED_UNTIL
    now = time.time()
    if _EMAIL_DISABLED_UNTIL and now < _EMAIL_DISABLED_UNTIL:
        remaining = int(_EMAIL_DISABLED_UNTIL - now)
        return {
            "ok": False,
            "sent": 0,
            "failed": 0,
            "errors": [],
            "note": f"SMTP temporarily disabled after network error ({remaining}s left)",
        }

    to_list = _recipients(settings)
    if not to_list:
        return {"ok": False, "sent": 0, "failed": 0, "errors": [], "note": "No recipients in EMAIL_TO"}

    results = {"ok": False, "sent": 0, "failed": 0, "errors": []}

    def _send_via(host: str, port: int, use_ssl: bool) -> Exception | None:
        """Attempt to send to all recipients via a specific host/port/ssl combo.
        Returns None on success, the exception on failure."""
        try:
            smtp_cls = smtplib.SMTP_SSL if use_ssl else smtplib.SMTP
            with smtp_cls(host, port, timeout=12) as server:
                server.ehlo()
                if not use_ssl:
                    server.starttls()
                    server.ehlo()
                server.login(settings["user"], settings["password"])
                for recipient in to_list:
                    try:
                        msg = MIMEMultipart("alternative")
                        msg["Subject"] = subject
                        msg["From"]    = settings["user"]
                        msg["To"]      = recipient
                        if plain_body:
                            msg.attach(MIMEText(plain_body, "plain"))
                        msg.attach(MIMEText(html_body, "html"))
                        server.sendmail(settings["user"], recipient, msg.as_string())
                        results["sent"] += 1
                        print(f"[email] Sent to {recipient} via {host}:{port}")
                    except Exception as exc:
                        results["failed"] += 1
                        results["errors"].append({"to": recipient, "error": str(exc)})
            return None
        except Exception as exc:
            return exc

    cfg_port    = int(settings.get("port") or 587)
    cfg_ssl     = bool(settings.get("use_ssl")) or cfg_port == 465
    cfg_host    = settings["host"]

    # Primary attempt — use whatever is configured
    err = _send_via(cfg_host, cfg_port, cfg_ssl)
    if err is not None:
        primary_err = str(err)
        # Fallback: if configured for STARTTLS (587) try SSL on 465, and vice versa
        if cfg_port == 587 and not cfg_ssl:
            fallback_err = _send_via(cfg_host, 465, True)
        elif cfg_port == 465 and cfg_ssl:
            fallback_err = _send_via(cfg_host, 587, False)
        else:
            fallback_err = err  # no sensible fallback
        if fallback_err is not None:
            if _is_network_error(primary_err) or _is_network_error(fallback_err):
                _EMAIL_DISABLED_UNTIL = time.time() + _EMAIL_NETWORK_ERROR_BACKOFF_SEC
                results["note"] = f"SMTP temporarily disabled after network error ({_EMAIL_NETWORK_ERROR_BACKOFF_SEC}s backoff)"
            else:
                results["failed"] += len(to_list)
                results["errors"].append({"to": "smtp", "error": f"{primary_err} | fallback: {fallback_err}"})

    results["ok"] = results["sent"] > 0 and results["failed"] == 0
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
    Full morning picks email: every game bet + every player prop + parlays.
    """
    today_str = datetime.date.today().strftime("%A, %B %d, %Y")
    parlays   = state.get("best_parlays", [])
    cards     = state.get("game_cards_today", [])
    props     = state.get("player_props", [])

    total_bets  = sum(1 for c in cards for k in ("moneyline","run_line","total","f5_moneyline","f5_total","home_team_total","away_team_total") if c.get(k))
    total_props = len([p for p in props if (p.get("direction","")).upper() == "OVER"])
    total_games = len(cards)

    # ── HTML ─────────────────────────────────────────────────────────────────
    html = f"""<!DOCTYPE html>
<html><body style="margin:0;padding:0;background:#06111e;font-family:Arial,sans-serif;">
<div style="max-width:680px;margin:0 auto;padding:24px;">

  <!-- Header -->
  <div style="background:linear-gradient(135deg,#1e3a5f,#0d1b2e);border-radius:14px;padding:24px 32px;margin-bottom:20px;border-left:5px solid #3b82f6;">
    <h1 style="color:#3b82f6;margin:0 0 6px;font-size:26px;letter-spacing:-0.5px;">&#127942; Bettor Daily Picks</h1>
    <p style="color:#94a3b8;margin:0 0 14px;font-size:15px;">{today_str}</p>
    <div style="display:flex;gap:20px;flex-wrap:wrap;">
      <div style="background:#0d2b45;padding:10px 18px;border-radius:8px;text-align:center;">
        <div style="color:#3b82f6;font-size:22px;font-weight:bold;">{total_games}</div>
        <div style="color:#64748b;font-size:12px;">Games</div>
      </div>
      <div style="background:#0d2b45;padding:10px 18px;border-radius:8px;text-align:center;">
        <div style="color:#22c55e;font-size:22px;font-weight:bold;">{total_bets}</div>
        <div style="color:#64748b;font-size:12px;">Bets</div>
      </div>
      <div style="background:#0d2b45;padding:10px 18px;border-radius:8px;text-align:center;">
        <div style="color:#a855f7;font-size:22px;font-weight:bold;">{total_props}</div>
        <div style="color:#64748b;font-size:12px;">Player Props</div>
      </div>
      <div style="background:#0d2b45;padding:10px 18px;border-radius:8px;text-align:center;">
        <div style="color:#f59e0b;font-size:22px;font-weight:bold;">{len(parlays)}</div>
        <div style="color:#64748b;font-size:12px;">Parlays</div>
      </div>
    </div>
  </div>
"""

    # ── Parlays section ───────────────────────────────────────────────────────
    if parlays:
        html += '<div style="background:#0d1b2e;border-radius:12px;padding:20px 28px;margin-bottom:20px;">'
        html += '<h2 style="color:#f59e0b;margin:0 0 16px;font-size:18px;">&#127920; Top Parlays</h2>'
        for i, p in enumerate(parlays[:5], 1):
            legs     = p.get("legs", [])
            dec      = float(p.get("combined_dec", 1.0))
            pct      = round((dec - 1) * 100)
            prob     = p.get("combined_prob", 0)
            safety   = p.get("safety_label", "")
            n_legs   = p.get("n_legs", len(legs))
            payout   = p.get("payout_100", round(dec * 100))
            html += (f'<div style="border:1px solid #1e3a5f;border-radius:10px;padding:14px 18px;margin-bottom:12px;">'
                     f'<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;">'
                     f'<span style="color:#e2e8f0;font-weight:bold;font-size:15px;">#{i} &mdash; {n_legs}-Leg Parlay</span>'
                     f'<span style="color:#22c55e;font-weight:bold;">+{pct}% &nbsp;(${payout} per $100)</span>'
                     f'</div>'
                     f'<div style="margin-bottom:8px;">{_safety_badge(safety)} '
                     f'<span style="color:#94a3b8;font-size:12px;">{prob}% combined probability</span></div>')
            for leg in legs:
                label = leg.get("label") or leg.get("pick") or "?"
                odds  = leg.get("dec_odds")
                odds_s = f"<span style='color:#64748b;'>&times;{float(odds):.2f}</span>" if odds else ""
                badge = leg.get("badge") or "MODERATE"
                html += f'<div style="color:#94a3b8;font-size:13px;padding:3px 0 3px 12px;border-left:2px solid #1e3a5f;">&#8226; {label} &nbsp;{odds_s} &nbsp;{_safety_badge(badge)}</div>'
            html += '</div>'
        html += '</div>'

    # ── Game-by-game bets section ─────────────────────────────────────────────
    if cards:
        html += '<div style="background:#0d1b2e;border-radius:12px;padding:20px 28px;margin-bottom:20px;">'
        html += '<h2 style="color:#3b82f6;margin:0 0 16px;font-size:18px;">&#9918; All Game Bets — Today</h2>'
        _BET_LABELS_NICE = {
            "moneyline":       "Moneyline",
            "run_line":        "Run Line (±1.5)",
            "total":           "Total Runs",
            "f5_moneyline":    "F5 Moneyline",
            "f5_total":        "F5 Total",
            "home_team_total": "Home Team Total",
            "away_team_total": "Away Team Total",
        }
        for card in cards:
            ht = card.get("home_team", "?")
            at = card.get("away_team", "?")
            time_s = card.get("game_time", "")
            hs = card.get("home_starter", "TBD")
            aws = card.get("away_starter", "TBD")
            any_bet = any(card.get(k) for k in _BET_LABELS_NICE)
            if not any_bet:
                continue
            html += (f'<div style="border-top:1px solid #1e3a5f;padding:14px 0 8px;">'
                     f'<div style="color:#e2e8f0;font-weight:bold;font-size:15px;margin-bottom:4px;">'
                     f'{at} @ {ht}')
            if time_s:
                h, m = time_s.split(":")[:2]
                hh = int(h) % 12 or 12
                ampm = "PM" if int(h) >= 12 else "AM"
                html += f' <span style="color:#64748b;font-size:12px;">&nbsp;{hh}:{m} {ampm} ET</span>'
            html += '</div>'
            if hs != "TBD" or aws != "TBD":
                html += f'<div style="color:#475569;font-size:12px;margin-bottom:8px;">{aws} vs {hs}</div>'
            html += '<table style="width:100%;border-collapse:collapse;margin-top:4px;">'
            for key, label in _BET_LABELS_NICE.items():
                bet = card.get(key)
                if not bet:
                    continue
                pick   = bet.get("pick", "")
                odds_v = bet.get("odds_am")
                odds_s = (f"({'+'if odds_v>0 else ''}{odds_v})" if odds_v else "")
                prob   = bet.get("model_prob", 0.5)
                edge   = bet.get("edge", 0.0)
                safety = bet.get("safety_label", "MODERATE")
                html += (f'<tr>'
                         f'<td style="color:#7aa3c8;font-size:12px;padding:3px 8px 3px 0;width:130px;">{label}</td>'
                         f'<td style="color:#e2e8f0;font-size:13px;font-weight:bold;">{pick} {odds_s}</td>'
                         f'<td style="text-align:right;padding-left:6px;">{_safety_badge(safety)}'
                         f'<span style="color:#64748b;font-size:11px;margin-left:6px;">{round(prob*100)}% | edge +{round(edge*100,1)}%</span></td>'
                         f'</tr>')
            html += '</table></div>'
        html += '</div>'

    # ── Player Props section ──────────────────────────────────────────────────
    over_props = [p for p in props if (p.get("direction","")).upper() == "OVER"]
    if over_props:
        html += '<div style="background:#0d1b2e;border-radius:12px;padding:20px 28px;margin-bottom:20px;">'
        html += f'<h2 style="color:#a855f7;margin:0 0 16px;font-size:18px;">&#127918; Player Props ({len(over_props)} OVER picks)</h2>'
        html += '<table style="width:100%;border-collapse:collapse;">'
        # Group by stat_type
        from collections import defaultdict
        by_type = defaultdict(list)
        for p in over_props:
            by_type[p.get("prop_label", p.get("stat_type", "Other"))].append(p)
        for ptype, plist in sorted(by_type.items()):
            plist_sorted = sorted(plist, key=lambda x: x.get("model_prob", 0), reverse=True)
            html += (f'<tr><td colspan="3" style="color:#7aa3c8;font-size:12px;font-weight:bold;'
                     f'padding:10px 0 4px;border-bottom:1px solid #1e3a5f;">{ptype.upper()}</td></tr>')
            for p in plist_sorted[:8]:
                name  = p.get("name", "?")
                team  = p.get("team", "")
                line  = p.get("line", "")
                conf  = p.get("confidence") or p.get("conf") or round((p.get("model_prob",0.5))*100)
                rationale = p.get("signal_rationale","")[:60] if p.get("signal_rationale") else ""
                safety = p.get("safety_label","MODERATE")
                html += (f'<tr>'
                         f'<td style="padding:5px 0;color:#e2e8f0;font-size:13px;">'
                         f'{name} <span style="color:#64748b;font-size:11px;">({team})</span></td>'
                         f'<td style="color:#22c55e;font-size:13px;font-weight:bold;white-space:nowrap;">OVER {line}</td>'
                         f'<td style="text-align:right;">{_safety_badge(safety)}'
                         f'<span style="color:#64748b;font-size:11px;margin-left:6px;">{conf}%</span></td>'
                         f'</tr>')
                if rationale:
                    html += (f'<tr><td colspan="3" style="color:#475569;font-size:11px;'
                             f'padding:0 0 4px 8px;">{rationale}</td></tr>')
        html += '</table></div>'

    html += '</div></body></html>'

    # ── Plain text fallback ───────────────────────────────────────────────────
    lines = [f"BETTOR DAILY PICKS — {today_str}",
             f"Summary: {total_games} games, {total_bets} bets, {total_props} props, {len(parlays)} parlays",
             "=" * 60, ""]

    if parlays:
        lines.append("=== TOP PARLAYS ===")
        for i, p in enumerate(parlays[:5], 1):
            legs = p.get("legs", [])
            dec  = float(p.get("combined_dec", 1.0))
            pct  = round((dec - 1) * 100)
            lines.append(f"{i}. {p.get('n_legs', len(legs))}-Leg Parlay  +{pct}%  [{p.get('safety_label','')}]")
            for leg in legs:
                lines.append(f"   • {leg.get('label','?')}")
        lines.append("")

    if cards:
        lines.append("=== ALL GAME BETS ===")
        for card in cards:
            ht = card.get("home_team","?"); at = card.get("away_team","?")
            gt = card.get("game_time","")
            time_label = f" @ {gt}" if gt else ""
            lines.append(f"\n{at} @ {ht}{time_label}")
            for key in ("moneyline","run_line","total","f5_moneyline","f5_total","home_team_total","away_team_total"):
                bet = card.get(key)
                if not bet:
                    continue
                pick  = bet.get("pick","")
                odds  = bet.get("odds_am")
                odds_s = f"({'+'if odds and odds>0 else ''}{odds})" if odds else ""
                prob  = round((bet.get("model_prob",0.5))*100)
                safety = bet.get("safety_label","")
                lines.append(f"  [{key.upper()}] {pick} {odds_s}  {prob}%  [{safety}]")
        lines.append("")

    if over_props:
        lines.append("=== PLAYER PROPS (OVER) ===")
        for p in over_props:
            conf = p.get("confidence") or p.get("conf") or round((p.get("model_prob",0.5))*100)
            lines.append(f"  {p.get('name','?')} ({p.get('team','')})  OVER {p.get('line','')} {p.get('prop_label','')}  {conf}%")
        lines.append("")

    lines.append("Good luck today! Results summary email will be sent after all games resolve.")
    plain = "\n".join(lines)

    return html, plain


def format_results_html(results: dict) -> tuple[str, str]:
    """
    Return (html_body, plain_body) for the end-of-day results email.

    `results` expected keys:
      date_str       — readable date
      total          — total predictions resolved today
      wins           — count of WIN
      losses         — count of LOSS
      pushes         — count of PUSH
      hit_rate       — float 0-100
      picks          — list of dicts: {pick, bet_type, outcome, game, odds_am, model_prob}
      props          — list of dicts: {name, stat_type, line, direction, outcome, actual}
      parlays        — list of dicts: {name, outcome, legs, combined_dec}
    """
    date_str   = results.get("date_str", datetime.date.today().strftime("%A, %B %d, %Y"))
    wins       = int(results.get("wins", 0))
    losses     = int(results.get("losses", 0))
    pushes     = int(results.get("pushes", 0))
    hit_rate   = float(results.get("hit_rate") or 0)
    picks      = results.get("picks", [])
    props      = results.get("props", [])
    parlays    = results.get("parlays", [])

    rate_color = "#22c55e" if hit_rate >= 55 else "#f59e0b" if hit_rate >= 45 else "#ef4444"
    _OUTCOME_ICONS = {"WIN": "&#9989;", "LOSS": "&#10060;", "PUSH": "&#11015;", "PENDING": "&#9899;"}
    _OUTCOME_COLORS = {"WIN": "#22c55e", "LOSS": "#ef4444", "PUSH": "#f59e0b", "PENDING": "#94a3b8"}

    html = f"""<!DOCTYPE html>
<html><body style="margin:0;padding:0;background:#06111e;font-family:Arial,sans-serif;">
<div style="max-width:680px;margin:0 auto;padding:24px;">

  <!-- Header -->
  <div style="background:linear-gradient(135deg,#1e3a5f,#0d1b2e);border-radius:14px;padding:24px 32px;margin-bottom:20px;border-left:5px solid {'#22c55e' if hit_rate>=55 else '#ef4444'};">
    <h1 style="color:#e2e8f0;margin:0 0 6px;font-size:26px;">&#128202; Daily Results</h1>
    <p style="color:#94a3b8;margin:0 0 16px;font-size:15px;">{date_str}</p>
    <div style="display:flex;gap:16px;flex-wrap:wrap;">
      <div style="background:#0d2b45;padding:12px 20px;border-radius:10px;text-align:center;min-width:70px;">
        <div style="color:#22c55e;font-size:26px;font-weight:bold;">{wins}</div>
        <div style="color:#64748b;font-size:12px;">WON</div>
      </div>
      <div style="background:#0d2b45;padding:12px 20px;border-radius:10px;text-align:center;min-width:70px;">
        <div style="color:#ef4444;font-size:26px;font-weight:bold;">{losses}</div>
        <div style="color:#64748b;font-size:12px;">LOST</div>
      </div>
      <div style="background:#0d2b45;padding:12px 20px;border-radius:10px;text-align:center;min-width:70px;">
        <div style="color:#f59e0b;font-size:26px;font-weight:bold;">{pushes}</div>
        <div style="color:#64748b;font-size:12px;">PUSH</div>
      </div>
      <div style="background:#0d2b45;padding:12px 20px;border-radius:10px;text-align:center;min-width:70px;">
        <div style="color:{rate_color};font-size:26px;font-weight:bold;">{hit_rate:.0f}%</div>
        <div style="color:#64748b;font-size:12px;">HIT RATE</div>
      </div>
    </div>
  </div>
"""

    # ── Game bets results ─────────────────────────────────────────────────────
    if picks:
        html += '<div style="background:#0d1b2e;border-radius:12px;padding:20px 28px;margin-bottom:20px;">'
        html += '<h2 style="color:#3b82f6;margin:0 0 14px;font-size:18px;">&#9918; Game Bets Results</h2>'
        html += '<table style="width:100%;border-collapse:collapse;">'
        for pick in sorted(picks, key=lambda x: x.get("outcome","") == "WIN", reverse=True):
            outcome  = (pick.get("outcome") or "PENDING").upper()
            icon     = _OUTCOME_ICONS.get(outcome, "&#9899;")
            color    = _OUTCOME_COLORS.get(outcome, "#94a3b8")
            bet_pick = pick.get("pick", pick.get("bet_type",""))
            game     = pick.get("game", pick.get("matchup",""))
            odds     = pick.get("odds_am")
            odds_s   = f"({'+'if odds and odds>0 else ''}{odds})" if odds else ""
            html += (f'<tr style="border-bottom:1px solid #1a2e45;">'
                     f'<td style="padding:7px 6px 7px 0;color:{color};font-size:18px;">{icon}</td>'
                     f'<td style="padding:7px 0;"><div style="color:#e2e8f0;font-size:13px;font-weight:bold;">{bet_pick} {odds_s}</div>'
                     f'<div style="color:#64748b;font-size:11px;">{game}</div></td>'
                     f'<td style="text-align:right;padding:7px 0;color:{color};font-weight:bold;font-size:13px;">{outcome}</td>'
                     f'</tr>')
        html += '</table></div>'

    # ── Prop bets results ─────────────────────────────────────────────────────
    if props:
        html += '<div style="background:#0d1b2e;border-radius:12px;padding:20px 28px;margin-bottom:20px;">'
        html += '<h2 style="color:#a855f7;margin:0 0 14px;font-size:18px;">&#127918; Player Prop Results</h2>'
        html += '<table style="width:100%;border-collapse:collapse;">'
        for p in sorted(props, key=lambda x: x.get("outcome","") == "WIN", reverse=True):
            outcome = (p.get("outcome") or "PENDING").upper()
            icon    = _OUTCOME_ICONS.get(outcome, "&#9899;")
            color   = _OUTCOME_COLORS.get(outcome, "#94a3b8")
            name    = p.get("name","?")
            line    = p.get("line","")
            st      = p.get("prop_label") or p.get("stat_type","")
            actual  = p.get("actual")
            actual_s = f" (actual: {actual})" if actual is not None else ""
            html += (f'<tr style="border-bottom:1px solid #1a2e45;">'
                     f'<td style="padding:7px 6px 7px 0;color:{color};font-size:18px;">{icon}</td>'
                     f'<td style="padding:7px 0;"><div style="color:#e2e8f0;font-size:13px;">'
                     f'{name} OVER {line} {st}</div>'
                     f'<div style="color:#64748b;font-size:11px;">{actual_s}</div></td>'
                     f'<td style="text-align:right;padding:7px 0;color:{color};font-weight:bold;font-size:13px;">{outcome}</td>'
                     f'</tr>')
        html += '</table></div>'

    # ── Parlay results ────────────────────────────────────────────────────────
    if parlays:
        html += '<div style="background:#0d1b2e;border-radius:12px;padding:20px 28px;margin-bottom:20px;">'
        html += '<h2 style="color:#f59e0b;margin:0 0 14px;font-size:18px;">&#127920; Parlay Results</h2>'
        for p in parlays:
            outcome  = (p.get("outcome") or "PENDING").upper()
            icon     = _OUTCOME_ICONS.get(outcome, "&#9899;")
            color    = _OUTCOME_COLORS.get(outcome, "#94a3b8")
            name     = p.get("name", "Parlay")
            dec      = float(p.get("combined_dec") or 1.0)
            pct      = round((dec - 1) * 100)
            html += (f'<div style="border-left:3px solid {color};padding:10px 14px;margin-bottom:10px;">'
                     f'<div style="display:flex;justify-content:space-between;">'
                     f'<span style="color:#e2e8f0;font-weight:bold;">{icon} {name}</span>'
                     f'<span style="color:{color};font-weight:bold;">{outcome} &nbsp;+{pct}%</span></div>')
            for leg in (p.get("legs") or []):
                label = leg.get("label") or leg.get("pick","?")
                leg_out = (leg.get("outcome") or "").upper()
                leg_icon = _OUTCOME_ICONS.get(leg_out, "&#9898;")
                html += f'<div style="color:#64748b;font-size:12px;padding:2px 0 2px 8px;">&#8226; {leg_icon} {label}</div>'
            html += '</div>'
        html += '</div>'

    html += '<div style="text-align:center;padding:10px;color:#334155;font-size:12px;">Bettor Bot &mdash; Automated picks &amp; results tracker</div>'
    html += '</div></body></html>'

    # ── Plain text ────────────────────────────────────────────────────────────
    lines = [
        f"BETTOR DAILY RESULTS — {date_str}",
        f"WON: {wins}  |  LOST: {losses}  |  PUSH: {pushes}  |  HIT RATE: {hit_rate:.0f}%",
        "=" * 60, "",
    ]
    if picks:
        lines.append("GAME BETS:")
        for pick in picks:
            out = (pick.get("outcome") or "PENDING").upper()
            icon = {"WIN":"✅","LOSS":"❌","PUSH":"↕️"}.get(out,"⚪")
            lines.append(f"  {icon} [{out}] {pick.get('pick','')} — {pick.get('game','')}")
        lines.append("")
    if props:
        lines.append("PLAYER PROPS:")
        for p in props:
            out  = (p.get("outcome") or "PENDING").upper()
            icon = {"WIN":"✅","LOSS":"❌","PUSH":"↕️"}.get(out,"⚪")
            actual_s = f" (actual: {p.get('actual')})" if p.get("actual") is not None else ""
            lines.append(f"  {icon} [{out}] {p.get('name','')} OVER {p.get('line','')} {p.get('stat_type','')}{actual_s}")
        lines.append("")
    if parlays:
        lines.append("PARLAYS:")
        for p in parlays:
            out = (p.get("outcome") or "PENDING").upper()
            icon = {"WIN":"✅","LOSS":"❌","PUSH":"↕️"}.get(out,"⚪")
            lines.append(f"  {icon} [{out}] {p.get('name','')}")
        lines.append("")
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
    """Format and email the full daily picks snapshot (morning email)."""
    html, plain = format_daily_picks_html(state)
    today_str   = datetime.date.today().strftime("%b %d, %Y")
    subject     = f"Bettor Morning Picks — {today_str}"
    return send_email(subject, html, plain)


def send_daily_results(results: dict) -> dict:
    """
    Format and email the end-of-day results summary.

    `results` is the same structure expected by format_results_html():
      wins, losses, pushes, hit_rate, picks, props, parlays
    """
    html, plain = format_results_html(results)
    today_str   = datetime.date.today().strftime("%b %d, %Y")
    wins   = results.get("wins", 0)
    losses = results.get("losses", 0)
    subject = f"Bettor Results — {today_str} — {wins}W / {losses}L"
    return send_email(subject, html, plain)


def send_parlay_alert(parlay: dict) -> dict:
    """Email a single parlay alert."""
    html, plain = format_parlay_html(parlay)
    today_str   = datetime.date.today().strftime("%b %d")
    name        = (parlay.get("name") or "Parlay")
    subject     = f"Bettor Parlay Alert — {name} — {today_str}"
    return send_email(subject, html, plain)
