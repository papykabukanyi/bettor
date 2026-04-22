import os

HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0,viewport-fit=cover">
<meta name="mobile-web-app-capable" content="yes">
<meta name="apple-mobile-web-app-capable" content="yes">
<meta name="theme-color" content="#070a10">
<title>Bettor AI</title>
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#08090d;--surface:#10131c;--s2:#181d2b;--s3:#1f2638;
  --border:#252d40;--border2:#2e3851;--text:#e4eaf4;--muted:#7a87a0;--muted2:#4a566a;
  --green:#22c55e;--gdim:#14291a;--yellow:#f0b429;--ydim:#2a2510;
  --red:#ef4444;--rdim:#2d1414;--blue:#3b82f6;--bdim:#141d2d;
  --purple:#a855f7;--pdim:#1e1030;--orange:#f97316;--odim:#2a1e0d;--teal:#14b8a6;
  --r:10px;--rl:14px;
}
body{background:var(--bg);color:var(--text);font-family:'Segoe UI',system-ui,sans-serif;font-size:14px;line-height:1.5;min-height:100vh;padding-bottom:130px}
.page{max-width:1320px;margin:0 auto;padding:24px 16px}
/* ── Header ── */
.header{display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:12px;padding-bottom:20px;border-bottom:1px solid var(--border);margin-bottom:20px}
.brand{display:flex;align-items:center;gap:14px}
.brand-icon{width:44px;height:44px;border-radius:10px;background:linear-gradient(135deg,#22c55e,#16a34a);display:flex;align-items:center;justify-content:center;font-size:22px;font-weight:900;color:#000;flex-shrink:0}
.brand h1{font-size:1.5rem;font-weight:800;letter-spacing:-.5px}
.brand h1 span{color:var(--green)}
.brand p{color:var(--muted);font-size:12px;margin-top:1px}
.header-right{display:flex;align-items:center;gap:10px;flex-wrap:wrap}
.status-pill{display:inline-flex;align-items:center;gap:6px;padding:5px 14px;border-radius:999px;font-size:12px;font-weight:700;border:1px solid transparent}
.status-pill.idle{background:var(--s2);color:var(--muted);border-color:var(--border)}
.status-pill.running{background:var(--gdim);color:var(--green);border-color:#22c55e44}
.status-pill.done{background:var(--gdim);color:var(--green);border-color:#22c55e44}
.status-pill.error{background:var(--rdim);color:var(--red);border-color:#ef444444}
.dot{width:7px;height:7px;border-radius:50%}
.dot.idle{background:var(--muted)}.dot.running{background:var(--green);animation:blink 1s infinite}.dot.done{background:var(--green)}.dot.error{background:var(--red)}
@keyframes blink{0%,100%{opacity:1}50%{opacity:.25}}
#run-btn{background:linear-gradient(135deg,#22c55e,#16a34a);color:#000;border:none;padding:9px 22px;border-radius:var(--r);font-weight:800;font-size:13px;cursor:pointer;transition:opacity .15s,transform .1s}
#run-btn:hover{opacity:.88;transform:translateY(-1px)}
#run-btn:disabled{opacity:.35;cursor:not-allowed;transform:none}
/* ── Progress bar ── */
#progress-wrap{margin-bottom:20px;display:none}
#progress-wrap.visible{display:block}
.progress-label{display:flex;justify-content:space-between;font-size:11px;color:var(--muted);margin-bottom:6px}
.progress-track{height:4px;background:var(--s3);border-radius:999px;overflow:hidden}
.progress-fill{height:100%;background:linear-gradient(90deg,var(--green),var(--teal));border-radius:999px;transition:width .4s ease}
.phase-steps{display:flex;gap:6px;flex-wrap:wrap;margin-top:8px}
.phase-step{font-size:10px;padding:2px 8px;border-radius:999px;background:var(--s2);color:var(--muted2);border:1px solid var(--border)}
.phase-step.active{background:var(--gdim);color:var(--green);border-color:#22c55e44}
.phase-step.done-step{background:var(--s3);color:var(--muted);border-color:var(--border)}
/* ── Stats bar ── */
.stats-bar{display:grid;grid-template-columns:repeat(auto-fill,minmax(130px,1fr));gap:10px;margin-bottom:24px}
.stat-card{background:var(--surface);border:1px solid var(--border);border-radius:var(--r);padding:14px 16px}
.stat-label{font-size:10px;text-transform:uppercase;letter-spacing:.6px;color:var(--muted);font-weight:600}
.stat-value{font-size:1.7rem;font-weight:800;margin-top:3px;line-height:1}
.stat-value.green{color:var(--green)}.stat-value.yellow{color:var(--yellow)}.stat-value.blue{color:var(--blue)}.stat-value.purple{color:var(--purple)}.stat-value.orange{color:var(--orange)}.stat-value.teal{color:var(--teal)}
/* ── Section header ── */
.section-hdr{display:flex;align-items:center;gap:10px;margin:28px 0 14px}
.section-hdr h2{font-size:.8rem;font-weight:800;text-transform:uppercase;letter-spacing:.8px;white-space:nowrap}
.section-badge{background:var(--s3);color:var(--muted);font-size:11px;font-weight:700;padding:2px 9px;border-radius:999px}
.section-line{flex:1;height:1px;background:var(--border)}
.section-action{font-size:11px;color:var(--muted);white-space:nowrap}
/* ── Upcoming schedule ── */
.schedule-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(260px,1fr));gap:10px}
.game-card{background:var(--surface);border:1px solid var(--border);border-radius:var(--rl);padding:14px;position:relative;overflow:hidden}
.game-card::before{content:'';position:absolute;top:0;left:0;width:3px;height:100%}
.game-card.mlb::before{background:var(--blue)}.game-card.soccer::before{background:var(--green)}
.gc-top{display:flex;align-items:flex-start;justify-content:space-between;padding-left:8px;margin-bottom:8px}
.gc-sport{font-size:9px;font-weight:800;text-transform:uppercase;letter-spacing:.6px;color:var(--muted)}
.gc-teams{font-size:.92rem;font-weight:700;line-height:1.3;margin-top:2px}
.gc-league{font-size:11px;color:var(--muted);margin-top:1px}
.date-badge{display:inline-flex;align-items:center;gap:4px;padding:3px 9px;border-radius:999px;font-size:10px;font-weight:800;letter-spacing:.3px;white-space:nowrap}
.date-badge.live{background:var(--rdim);color:var(--red);border:1px solid #ef444455;animation:blink 1.4s infinite}
.date-badge.today{background:var(--gdim);color:var(--green);border:1px solid #22c55e44}
.date-badge.later{background:var(--bdim);color:var(--blue);border:1px solid #3b82f644}
.date-badge.tomorrow{background:var(--odim);color:var(--orange);border:1px solid #f9731644}
.gc-starters{margin-top:8px;padding:7px 8px;background:var(--s2);border-radius:7px;padding-left:16px;font-size:11px;color:var(--muted)}
.gc-starters span{color:var(--text);font-weight:600}
.inj-flag{margin-top:6px;padding:4px 8px 4px 14px;background:var(--rdim);border-radius:6px;font-size:10px;color:var(--red);font-weight:700}
/* ── Bet cards ── */
.cards-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));gap:12px}
.bet-card{background:var(--surface);border:1px solid var(--border);border-radius:var(--rl);padding:16px 16px 14px;position:relative;overflow:hidden;transition:border-color .15s,transform .1s,box-shadow .15s}
.bet-card:hover{border-color:var(--border2);transform:translateY(-1px)}
.bet-card.selected{border-color:var(--green)!important;box-shadow:0 0 0 1px var(--green),0 4px 20px #22c55e20}
.bet-card::before{content:'';position:absolute;top:0;left:0;width:3px;height:100%}
.grade-aplus::before{background:var(--green)}.grade-a::before{background:var(--blue)}.grade-b::before{background:var(--yellow)}.grade-c::before{background:var(--muted2)}
.prop-sover::before{background:var(--green)}.prop-lover::before{background:var(--teal)}.prop-sunder::before{background:var(--red)}.prop-lunder::before{background:var(--orange)}.prop-skip::before{background:var(--muted2)}
.card-top{display:flex;align-items:flex-start;justify-content:space-between;margin-bottom:10px;padding-left:8px}
.card-meta{flex:1;min-width:0}
.card-sport-tag{font-size:9px;font-weight:800;text-transform:uppercase;letter-spacing:.7px;color:var(--muted);margin-bottom:2px}
.card-matchup{font-size:.95rem;font-weight:700;line-height:1.2;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.card-league{font-size:11px;color:var(--muted);margin-top:1px}
.card-top-right{display:flex;flex-direction:column;align-items:flex-end;gap:6px}
.grade-badge{font-size:10px;font-weight:900;padding:3px 10px;border-radius:999px;letter-spacing:.4px;white-space:nowrap}
.grade-badge.grade-aplus{background:var(--gdim);color:var(--green);border:1px solid #22c55e33}
.grade-badge.grade-a{background:var(--bdim);color:var(--blue);border:1px solid #3b82f633}
.grade-badge.grade-b{background:var(--ydim);color:var(--yellow);border:1px solid #f0b42933}
.grade-badge.grade-c{background:var(--s3);color:var(--muted);border:1px solid var(--border)}
.add-btn{width:28px;height:28px;border-radius:50%;border:1.5px solid var(--border2);background:var(--s2);color:var(--muted);font-size:16px;font-weight:700;line-height:1;cursor:pointer;display:flex;align-items:center;justify-content:center;transition:all .15s;flex-shrink:0}
.add-btn:hover{border-color:var(--green);color:var(--green);background:var(--gdim)}
.add-btn.added{border-color:var(--green);color:#000;background:var(--green)}
.pick-banner{margin:0 8px 12px;padding:9px 13px;border-radius:8px;font-weight:800;font-size:.88rem;display:flex;align-items:center;gap:8px}
.pick-banner.grade-aplus{background:var(--gdim);color:var(--green)}
.pick-banner.grade-a{background:var(--bdim);color:#6ba4ff}
.pick-banner.grade-b{background:var(--ydim);color:var(--yellow)}
.pick-banner.grade-c{background:var(--s3);color:var(--muted)}
.card-time-badge{margin:0 8px 10px;padding-left:0}
.stats-row{display:grid;grid-template-columns:repeat(3,1fr);gap:6px;padding:0 8px}
.stat-chip{background:var(--s2);border:1px solid var(--border);border-radius:7px;padding:6px 8px;text-align:center}
.sc-label{font-size:9px;text-transform:uppercase;color:var(--muted);letter-spacing:.4px}
.sc-val{font-size:.88rem;font-weight:800;margin-top:2px}
.sc-val.green{color:var(--green)}.sc-val.yellow{color:var(--yellow)}.sc-val.blue{color:var(--blue)}.sc-val.red{color:var(--red)}.sc-val.teal{color:var(--teal)}.sc-val.muted{color:var(--muted)}
.stake-row{display:flex;align-items:center;justify-content:space-between;margin:10px 8px 0;padding:8px 10px;background:var(--s2);border-radius:7px;font-size:12px}
.stake-label{color:var(--muted)}.stake-ev{color:var(--teal);font-size:11px;font-weight:700}
.prop-rec-banner{margin:0 8px 10px;padding:8px 12px;border-radius:8px;font-weight:800;font-size:.88rem;display:flex;align-items:center;gap:8px}
.prop-rec-banner.sover{background:var(--gdim);color:var(--green)}
.prop-rec-banner.lover{background:#0d2020;color:var(--teal)}
.prop-rec-banner.sunder{background:var(--rdim);color:var(--red)}
.prop-rec-banner.lunder{background:var(--odim);color:var(--orange)}
.prop-rec-banner.skip{background:var(--s3);color:var(--muted)}
.prob-bar-wrap{padding:8px 8px 2px}
.prob-bar-labels{display:flex;justify-content:space-between;font-size:10px;color:var(--muted);margin-bottom:5px;font-weight:600}
.prob-track{height:5px;background:var(--s3);border-radius:999px;overflow:hidden}
.prob-fill{height:100%;border-radius:999px;transition:width .5s ease}
.prob-fill.green{background:var(--green)}.prob-fill.red{background:var(--red)}.prob-fill.teal{background:var(--teal)}.prob-fill.yellow{background:var(--yellow)}
/* ── Skeleton loaders ── */
@keyframes shimmer{0%{background-position:-600px 0}100%{background-position:600px 0}}
.skeleton-card{background:var(--surface);border:1px solid var(--border);border-radius:var(--rl);padding:16px;height:200px;overflow:hidden;position:relative}
.skeleton-line{border-radius:6px;background:linear-gradient(90deg,var(--s2) 25%,var(--s3) 50%,var(--s2) 75%);background-size:600px 100%;animation:shimmer 1.5s infinite;margin-bottom:10px}
.skeleton-line.h8{height:8px;width:40%}.skeleton-line.h12{height:12px;width:70%}.skeleton-line.h16{height:16px;width:90%}.skeleton-line.h10{height:10px;width:55%}
.skeleton-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));gap:12px}
/* ── Parlays ── */
.parlay-tabs{display:flex;gap:6px;margin-bottom:14px;flex-wrap:wrap}
.ptab{padding:6px 16px;border-radius:999px;font-size:12px;font-weight:700;border:1.5px solid var(--border2);background:var(--s2);color:var(--muted);cursor:pointer;transition:all .15s}
.ptab:hover{border-color:var(--purple);color:var(--purple)}
.ptab.active{background:var(--pdim);border-color:var(--purple);color:var(--purple)}
.parlay-section{display:none}.parlay-section.visible{display:block}
.parlay-card{background:var(--surface);border:1px solid var(--border);border-radius:var(--rl);padding:16px 18px;margin-bottom:10px}
.parlay-header{display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin-bottom:12px}
.parlay-tag{background:var(--pdim);border:1px solid #a855f744;color:var(--purple);font-size:11px;font-weight:800;padding:4px 12px;border-radius:999px;letter-spacing:.4px}
.parlay-chips{display:flex;flex-wrap:wrap;gap:6px}
.parlay-chip{background:var(--s2);border:1px solid var(--border);border-radius:6px;padding:3px 10px;font-size:11px}
.parlay-chip .v{font-weight:800}
.parlay-chip .v.green{color:var(--green)}.parlay-chip .v.yellow{color:var(--yellow)}.parlay-chip .v.purple{color:var(--purple)}.parlay-chip .v.teal{color:var(--teal)}
.parlay-legs{margin-bottom:10px}
.parlay-leg-item{padding:5px 0;border-bottom:1px solid var(--border);font-size:12px;color:var(--muted);display:flex;align-items:center;gap:6px}
.parlay-leg-item:last-child{border-bottom:none}
.leg-dot{width:6px;height:6px;border-radius:50%;background:var(--purple);flex-shrink:0}
.parlay-payout-row{background:var(--pdim);border:1px solid #a855f722;border-radius:8px;padding:10px 14px;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px}
.pp-label{font-size:11px;color:var(--muted)}.pp-win{font-size:1.1rem;font-weight:800;color:var(--green)}.pp-stake{font-size:12px;color:var(--purple);font-weight:700}
/* ── Floating parlay builder ── */
#parlay-builder{position:fixed;bottom:0;left:0;right:0;z-index:1000;background:var(--surface);border-top:1px solid var(--purple);box-shadow:0 -8px 32px rgba(168,85,247,.2);padding:12px 20px;transform:translateY(100%);transition:transform .3s cubic-bezier(.4,0,.2,1)}
#parlay-builder.open{transform:translateY(0)}
.pb-bar{max-width:1320px;margin:0 auto;display:flex;align-items:center;gap:14px;flex-wrap:wrap}
.pb-title{font-size:12px;font-weight:800;color:var(--purple);text-transform:uppercase;letter-spacing:.6px;white-space:nowrap}
.pb-legs-list{display:flex;flex-wrap:wrap;gap:6px;flex:1}
.pb-leg-chip{background:var(--pdim);border:1px solid var(--purple);color:var(--text);border-radius:6px;padding:4px 10px;font-size:11px;display:flex;align-items:center;gap:5px}
.pb-remove{background:none;border:none;color:var(--muted);cursor:pointer;font-size:14px;line-height:1;padding:0}
.pb-remove:hover{color:var(--red)}
.pb-summary{display:flex;align-items:center;gap:12px;flex-wrap:wrap;flex-shrink:0}
.pb-odds{font-size:1rem;font-weight:800;color:var(--purple)}.pb-payout{font-size:.95rem;font-weight:800;color:var(--green)}.pb-label{font-size:11px;color:var(--muted)}
.pb-stake-wrap{display:flex;align-items:center;gap:6px}
.pb-stake-wrap label{font-size:11px;color:var(--muted)}
#pb-stake-input{width:70px;background:var(--s2);border:1px solid var(--border2);color:var(--text);border-radius:6px;padding:4px 8px;font-size:12px;font-weight:700}
.pb-clear{background:var(--rdim);border:1px solid #ef444444;color:var(--red);padding:6px 14px;border-radius:8px;font-size:12px;font-weight:700;cursor:pointer;white-space:nowrap}
.pb-clear:hover{background:#3d1414}
.pb-copy{background:var(--purple);border:none;color:#fff;padding:7px 16px;border-radius:8px;font-size:12px;font-weight:800;cursor:pointer;white-space:nowrap}
.pb-copy:hover{opacity:.87}
/* ── Misc ── */
.empty-state{grid-column:1/-1;padding:32px;text-align:center;color:var(--muted);background:var(--surface);border:1px dashed var(--border);border-radius:var(--rl)}
.empty-state .ei{font-size:2rem;margin-bottom:8px}
.empty-state p{margin-top:6px}
.error-box{background:var(--rdim);border:1px solid var(--red);border-radius:var(--r);padding:14px 16px;color:var(--red);font-family:monospace;font-size:12px;white-space:pre-wrap;overflow-x:auto;margin-bottom:20px}
.footer{margin-top:48px;padding-top:16px;border-top:1px solid var(--border);color:var(--muted2);font-size:11px;display:flex;justify-content:space-between;flex-wrap:wrap;gap:6px}
@media(max-width:600px){.cards-grid{grid-template-columns:1fr}.schedule-grid{grid-template-columns:1fr}.stats-bar{grid-template-columns:repeat(2,1fr)}.stats-row{grid-template-columns:repeat(2,1fr)}.pb-bar{flex-direction:column;align-items:flex-start}}
</style>
</head>
<body>
<div class="page">

<!-- ── Header ── -->
<div class="header">
  <div class="brand">
    <div class="brand-icon">B</div>
    <div>
      <h1>Bettor <span>Dashboard</span></h1>
      <p id="last-updated-txt">{% if state.last_updated %}Updated {{ state.last_updated }}{% else %}No analysis run yet{% endif %}</p>
    </div>
  </div>
  <div class="header-right">
    <div class="status-pill {{ state.status }}" id="status-pill">
      <div class="dot {{ state.status }}" id="status-dot"></div>
      <span id="status-text">{{ state.status | capitalize }}</span>
    </div>
    <button id="run-btn" onclick="runAnalysis()" {% if state.status == 'running' %}disabled{% endif %}>
      ⚡ Run Analysis
    </button>
  </div>
</div>

<!-- ── Progress bar (visible while running) ── -->
<div id="progress-wrap" class="{% if state.status == 'running' %}visible{% endif %}">
  <div class="progress-label">
    <span id="phase-name">{{ state.phase }}</span>
    <span id="phase-pct">{{ ((state.phase_idx / state.phase_total) * 100) | int }}%</span>
  </div>
  <div class="progress-track">
    <div class="progress-fill" id="progress-fill"
         style="width:{{ ((state.phase_idx / state.phase_total) * 100) | int }}%"></div>
  </div>
  <div class="phase-steps" id="phase-steps">
    {% set phases = ["MLB Schedule","Soccer Fixtures","Stats & Model","Injuries","Odds","Value Models","Parlays","Save DB"] %}
    {% for ph in phases %}
    <div class="phase-step {% if loop.index0 < state.phase_idx %}done-step{% elif loop.index0 == state.phase_idx %}active{% endif %}" id="pstep-{{ loop.index0 }}">{{ ph }}</div>
    {% endfor %}
  </div>
</div>

<!-- ── Error ── -->
{% if state.status == 'error' %}
<div class="error-box">{{ state.error }}</div>
{% endif %}

<!-- ── Stats bar ── -->
<div class="stats-bar">
  <div class="stat-card"><div class="stat-label">Win Bets</div><div class="stat-value green">{{ state.win_bets | length }}</div></div>
  <div class="stat-card"><div class="stat-label">Over / Under</div><div class="stat-value blue">{{ state.totals_bets | length }}</div></div>
  <div class="stat-card"><div class="stat-label">Props</div><div class="stat-value orange">{{ state.prop_stats | length }}</div></div>
  <div class="stat-card"><div class="stat-label">Auto Parlays</div><div class="stat-value purple">{{ (state.parlays_2|length)+(state.parlays_3|length)+(state.parlays_4|length) }}</div></div>
  <div class="stat-card"><div class="stat-label">MLB Games</div><div class="stat-value teal">{{ state.mlb_games }}</div></div>
  <div class="stat-card"><div class="stat-label">Soccer Games</div><div class="stat-value">{{ state.soccer_games }}</div></div>
  <div class="stat-card"><div class="stat-label">Bankroll</div><div class="stat-value yellow">${{ "{:,.0f}".format(bankroll) }}</div></div>
</div>

<!-- ════════════════════ UPCOMING SCHEDULE ════════════════════════ -->
{% set today_str = today %}
{% set tomorrow_str = tomorrow %}
<div class="section-hdr">
  <h2>📅 Upcoming Schedule</h2>
  <div class="section-badge">{{ state.upcoming_games | length }}</div>
  <div class="section-line"></div>
  <div class="section-action">Today · Tomorrow · Live status</div>
</div>

{% if state.status == 'running' %}
<div class="skeleton-grid">
  {% for _ in range(4) %}
  <div class="skeleton-card">
    <div class="skeleton-line h8"></div><div class="skeleton-line h16"></div>
    <div class="skeleton-line h10"></div><div class="skeleton-line h12"></div>
  </div>
  {% endfor %}
</div>
{% elif state.upcoming_games %}
<div class="schedule-grid">
  {% for g in state.upcoming_games %}
    {% set gdate = g.game_date or g.date or '' %}
    {% set gtime = g.game_time %}
    {% set gstatus = (g.status or '') | upper %}
    {% if 'IN PROGRESS' in gstatus or 'LIVE' in gstatus or 'WARMUP' in gstatus %}
      {% set badge_cls = 'live' %}{% set badge_txt = '🔴 LIVE' %}
    {% elif gdate == today_str and gtime %}
      {% set badge_cls = 'later' %}{% set badge_txt = '⏰ TODAY ' + gtime + ' ET' %}
    {% elif gdate == today_str %}
      {% set badge_cls = 'today' %}{% set badge_txt = '📌 TODAY' %}
    {% elif gdate == tomorrow_str %}
      {% set badge_cls = 'tomorrow' %}{% set badge_txt = '🌅 TOMORROW' + ((' ' + gtime + ' ET') if gtime else '') %}
    {% else %}
      {% set badge_cls = 'later' %}{% set badge_txt = gdate %}
    {% endif %}
    {% set sport = g.sport or 'mlb' %}
    {% set injuries_for_game = state.injuries | selectattr('team','defined') | list %}
  <div class="game-card {{ sport }}">
    <div class="gc-top">
      <div>
        <div class="gc-sport">{{ sport | upper }} · {{ g.league or '' }}</div>
        <div class="gc-teams">{{ g.away_team }} @ {{ g.home_team }}</div>
      </div>
      <div class="date-badge {{ badge_cls }}">{{ badge_txt }}</div>
    </div>
    {% if g.home_starter or g.away_starter %}
    <div class="gc-starters">
      ⚾ <span>{{ g.away_starter or 'TBD' }}</span> vs <span>{{ g.home_starter or 'TBD' }}</span>
    </div>
    {% endif %}
  </div>
  {% endfor %}
</div>
{% else %}
<div class="empty-state"><div class="ei">📅</div><strong>No upcoming games yet</strong><p>Run analysis to load today's and tomorrow's schedule.</p></div>
{% endif %}

<!-- ════════════════════ WIN BETS ════════════════════════════════ -->
<div class="section-hdr">
  <h2>🏆 Team Win Bets</h2>
  <div class="section-badge">{{ state.win_bets | length }}</div>
  <div class="section-line"></div>
  <div class="section-action">Click + to add to parlay builder</div>
</div>
<div class="cards-grid" id="win-bets-grid">
{% if state.status == 'running' %}
  {% for _ in range(3) %}<div class="skeleton-card"><div class="skeleton-line h8"></div><div class="skeleton-line h16"></div><div class="skeleton-line h10"></div><div class="skeleton-line h12"></div><div class="skeleton-line h8"></div></div>{% endfor %}
{% elif state.win_bets %}
  {% for b in state.win_bets %}
    {% if b.edge >= 0.15 %}{% set gk='aplus' %}{% set gl='A+' %}{% elif b.edge >= 0.10 %}{% set gk='a' %}{% set gl='A' %}{% elif b.edge >= 0.07 %}{% set gk='b' %}{% set gl='B' %}{% else %}{% set gk='c' %}{% set gl='C' %}{% endif %}
    {% set parts = b.matchup.split(' vs ') %}
    {% set ht = parts[0] if parts|length==2 else b.matchup %}
    {% set at = parts[1] if parts|length==2 else '?' %}
    {% set pick_team = at if b.bet=='AWAY' else (ht if b.bet=='HOME' else 'DRAW') %}
    {% set odds_s = ('+' + b.odds_am|string) if b.odds_am > 0 else b.odds_am|string %}
    {% set payout = ((b.dec_odds - 1) * b.stake_usd) | round(0) | int %}
    {% set gtime = b.game_time or '' %}
    {% set gdate = b.date or today_str %}
  <div class="bet-card grade-{{ gk }}" id="card-{{ b._id }}"
       data-id="{{ b._id }}" data-label="{{ pick_team }} WIN" data-dec="{{ b.dec_odds }}" data-prob="{{ b.model_prob }}">
    <div class="card-top">
      <div class="card-meta">
        <div class="card-sport-tag">{{ b.sport }} · Team Win</div>
        <div class="card-matchup">{{ at }} @ {{ ht }}</div>
        <div class="card-league">
          {% if 'IN PROGRESS' in (b.status or '')|upper %}<span style="color:var(--red);font-weight:800">🔴 LIVE</span>
          {% elif gdate == today_str and gtime %}<span style="color:var(--blue)">⏰ {{ gtime }} ET</span>
          {% elif gdate == today_str %}<span style="color:var(--green)">📌 TODAY</span>
          {% elif gdate == tomorrow_str %}<span style="color:var(--orange)">🌅 TOMORROW {{ gtime }}</span>
          {% else %}<span>{{ gdate }}</span>{% endif %}
        </div>
      </div>
      <div class="card-top-right">
        <div class="grade-badge grade-{{ gk }}">Grade {{ gl }}</div>
        <button class="add-btn" id="addbtn-{{ b._id }}" onclick="toggleLeg('{{ b._id }}')" title="Add to Parlay">+</button>
      </div>
    </div>
    <div class="pick-banner grade-{{ gk }}"><span>→</span> BET {{ pick_team }} WIN <span style="font-size:.75rem;opacity:.7">({{ b.bet }})</span></div>
    <div class="stats-row">
      <div class="stat-chip"><div class="sc-label">Model</div><div class="sc-val green">{{ "%.0f"|format(b.model_prob*100) }}%</div></div>
      <div class="stat-chip"><div class="sc-label">Book</div><div class="sc-val muted">{{ "%.0f"|format(b.book_prob*100) }}%</div></div>
      <div class="stat-chip"><div class="sc-label">Edge</div><div class="sc-val green">+{{ "%.0f"|format(b.edge*100) }}%</div></div>
      <div class="stat-chip"><div class="sc-label">Odds</div><div class="sc-val blue">{{ odds_s }}</div></div>
      <div class="stat-chip"><div class="sc-label">Stake</div><div class="sc-val yellow">${{ "%.0f"|format(b.stake_usd) }}</div></div>
      <div class="stat-chip"><div class="sc-label">Win</div><div class="sc-val green">${{ payout }}</div></div>
    </div>
    <div class="stake-row"><span class="stake-label">Expected Value</span><span class="stake-ev">EV {{ "%+.3f"|format(b.ev) }}</span></div>
  </div>
  {% endfor %}
{% else %}
  <div class="empty-state"><div class="ei">📊</div><strong>No win value bets found</strong><p>Click <strong>Run Analysis</strong> to start.</p></div>
{% endif %}
</div>

<!-- ════════════════════ OVER / UNDER ════════════════════════════ -->
<div class="section-hdr">
  <h2>📈 Over / Under Bets</h2>
  <div class="section-badge">{{ state.totals_bets | length }}</div>
  <div class="section-line"></div>
  <div class="section-action">Click + to add to parlay builder</div>
</div>
<div class="cards-grid" id="totals-bets-grid">
{% if state.status == 'running' %}
  {% for _ in range(3) %}<div class="skeleton-card"><div class="skeleton-line h8"></div><div class="skeleton-line h16"></div><div class="skeleton-line h10"></div><div class="skeleton-line h12"></div><div class="skeleton-line h8"></div></div>{% endfor %}
{% elif state.totals_bets %}
  {% for b in state.totals_bets %}
    {% if b.edge >= 0.15 %}{% set gk='aplus' %}{% set gl='A+' %}{% elif b.edge >= 0.10 %}{% set gk='a' %}{% set gl='A' %}{% elif b.edge >= 0.07 %}{% set gk='b' %}{% set gl='B' %}{% else %}{% set gk='c' %}{% set gl='C' %}{% endif %}
    {% set parts = b.matchup.split(' vs ') %}
    {% set ht = parts[0] if parts|length==2 else b.matchup %}
    {% set at = parts[1] if parts|length==2 else '?' %}
    {% set odds_s = ('+' + b.odds_am|string) if b.odds_am > 0 else b.odds_am|string %}
    {% set payout = ((b.dec_odds - 1) * b.stake_usd) | round(0) | int %}
    {% set ou_color = 'green' if b.bet == 'OVER' else 'red' %}
    {% set gtime = b.game_time or '' %}
    {% set gdate = b.date or today_str %}
  <div class="bet-card grade-{{ gk }}" id="card-{{ b._id }}"
       data-id="{{ b._id }}" data-label="{{ b.bet }} {{ b.total_line }} - {{ at }} @ {{ ht }}" data-dec="{{ b.dec_odds }}" data-prob="{{ b.model_prob }}">
    <div class="card-top">
      <div class="card-meta">
        <div class="card-sport-tag">{{ b.sport }} · Totals</div>
        <div class="card-matchup">{{ at }} @ {{ ht }}</div>
        <div class="card-league">
          {% if gdate == today_str and gtime %}<span style="color:var(--blue)">⏰ {{ gtime }} ET</span>
          {% elif gdate == today_str %}<span style="color:var(--green)">📌 TODAY</span>
          {% elif gdate == tomorrow_str %}<span style="color:var(--orange)">🌅 TOMORROW</span>
          {% else %}<span>{{ gdate }}</span>{% endif %}
        </div>
      </div>
      <div class="card-top-right">
        <div class="grade-badge grade-{{ gk }}">Grade {{ gl }}</div>
        <button class="add-btn" id="addbtn-{{ b._id }}" onclick="toggleLeg('{{ b._id }}')" title="Add to Parlay">+</button>
      </div>
    </div>
    <div class="pick-banner grade-{{ gk }}"><span>→</span> BET {{ b.bet }} {{ b.total_line }} runs <span style="font-size:.75rem;opacity:.6">(model: {{ b.predicted_total }})</span></div>
    <div class="stats-row">
      <div class="stat-chip"><div class="sc-label">Model</div><div class="sc-val {{ ou_color }}">{{ "%.0f"|format(b.model_prob*100) }}%</div></div>
      <div class="stat-chip"><div class="sc-label">Book</div><div class="sc-val muted">{{ "%.0f"|format(b.book_prob*100) }}%</div></div>
      <div class="stat-chip"><div class="sc-label">Edge</div><div class="sc-val green">+{{ "%.0f"|format(b.edge*100) }}%</div></div>
      <div class="stat-chip"><div class="sc-label">Odds</div><div class="sc-val blue">{{ odds_s }}</div></div>
      <div class="stat-chip"><div class="sc-label">Stake</div><div class="sc-val yellow">${{ "%.0f"|format(b.stake_usd) }}</div></div>
      <div class="stat-chip"><div class="sc-label">Win</div><div class="sc-val green">${{ payout }}</div></div>
    </div>
    <div class="stake-row"><span class="stake-label">Expected Value</span><span class="stake-ev">EV {{ "%+.3f"|format(b.ev) }}</span></div>
  </div>
  {% endfor %}
{% else %}
  <div class="empty-state"><div class="ei">📈</div><strong>No totals value found</strong><p>Lines are fair or totals odds not yet posted.</p></div>
{% endif %}
</div>

<!-- ════════════════════ PLAYER PROPS ════════════════════════════ -->
<div class="section-hdr">
  <h2>⚾ Player Props · Pitcher Strikeouts</h2>
  <div class="section-badge">{{ state.prop_stats | length }}</div>
  <div class="section-line"></div>
  <div class="section-action">Click + to mix props into your parlay</div>
</div>
<div class="cards-grid" id="props-grid">
{% if state.status == 'running' %}
  {% for _ in range(3) %}<div class="skeleton-card"><div class="skeleton-line h8"></div><div class="skeleton-line h16"></div><div class="skeleton-line h12"></div><div class="skeleton-line h10"></div></div>{% endfor %}
{% elif state.prop_stats %}
  {% for p in state.prop_stats %}
    {% set ov=p.over_prob %}{% set un=p.under_prob %}
    {% if ov >= 0.68 %}{% set rcls='sover' %}{% set rtext='STRONG OVER '+p.line|string+' Ks' %}{% set bcls='green' %}{% set bpct=(ov*100)|int %}{% set ccls='prop-sover' %}{% set dprob=ov %}
    {% elif ov >= 0.58 %}{% set rcls='lover' %}{% set rtext='LEAN OVER '+p.line|string+' Ks' %}{% set bcls='teal' %}{% set bpct=(ov*100)|int %}{% set ccls='prop-lover' %}{% set dprob=ov %}
    {% elif un >= 0.68 %}{% set rcls='sunder' %}{% set rtext='STRONG UNDER '+p.line|string+' Ks' %}{% set bcls='red' %}{% set bpct=(un*100)|int %}{% set ccls='prop-sunder' %}{% set dprob=un %}
    {% elif un >= 0.58 %}{% set rcls='lunder' %}{% set rtext='LEAN UNDER '+p.line|string+' Ks' %}{% set bcls='yellow' %}{% set bpct=(un*100)|int %}{% set ccls='prop-lunder' %}{% set dprob=un %}
    {% else %}{% set rcls='skip' %}{% set rtext='COIN FLIP - SKIP' %}{% set bcls='yellow' %}{% set bpct=50 %}{% set ccls='prop-skip' %}{% set dprob=0.5 %}{% endif %}
    {% set fdec=(1.0/dprob)|round(2) %}
  <div class="bet-card {{ ccls }}" id="card-{{ p._id }}"
       data-id="{{ p._id }}" data-label="{{ p.name }} {{ rtext }}" data-dec="{{ fdec }}" data-prob="{{ dprob }}">
    <div class="card-top">
      <div class="card-meta">
        <div class="card-sport-tag">MLB · Pitcher Prop</div>
        <div class="card-matchup">{{ p.name }}</div>
        <div class="card-league">{{ p.team }} · {{ p.game }}</div>
      </div>
      <div class="card-top-right">
        <button class="add-btn" id="addbtn-{{ p._id }}" onclick="toggleLeg('{{ p._id }}')" title="Add to Parlay">+</button>
      </div>
    </div>
    <div class="prop-rec-banner {{ rcls }}">{{ rtext }}</div>
    <div class="stats-row">
      <div class="stat-chip"><div class="sc-label">ERA</div><div class="sc-val">{{ "%.2f"|format(p.era) }}</div></div>
      <div class="stat-chip"><div class="sc-label">K/9</div><div class="sc-val {{ bcls }}">{{ "%.1f"|format(p.k9) }}</div></div>
      <div class="stat-chip"><div class="sc-label">WHIP</div><div class="sc-val">{{ "%.2f"|format(p.whip) }}</div></div>
      <div class="stat-chip"><div class="sc-label">IP/Start</div><div class="sc-val">{{ "%.1f"|format(p.ip_per_start) }}</div></div>
      <div class="stat-chip"><div class="sc-label">Avg Ks</div><div class="sc-val green">{{ "%.1f"|format(p.avg_per_game) }}</div></div>
      <div class="stat-chip"><div class="sc-label">Line</div><div class="sc-val yellow">{{ p.line }}</div></div>
    </div>
    <div class="prob-bar-wrap">
      <div class="prob-bar-labels"><span>Over {{ "%.0f"|format(p.over_prob*100) }}%</span><span>Under {{ "%.0f"|format(p.under_prob*100) }}%</span></div>
      <div class="prob-track"><div class="prob-fill {{ bcls }}" style="width:{{ bpct }}%"></div></div>
    </div>
  </div>
  {% endfor %}
{% else %}
  <div class="empty-state"><div class="ei">⚾</div><strong>No starter props available yet</strong><p>Baseball Reference may still be updating today's pitcher data.</p></div>
{% endif %}
</div>

<!-- ════════════════════ AUTO PARLAYS ════════════════════════════ -->
{% set total_parlays=(state.parlays_2|length)+(state.parlays_3|length)+(state.parlays_4|length) %}
<div class="section-hdr" style="margin-top:36px">
  <h2>🔗 Auto Parlays</h2>
  <div class="section-badge">{{ total_parlays }}</div>
  <div class="section-line"></div>
</div>
{% if state.status == 'running' %}
  <div class="skeleton-grid">
    {% for _ in range(2) %}<div class="skeleton-card" style="height:160px"><div class="skeleton-line h8"></div><div class="skeleton-line h12"></div><div class="skeleton-line h10"></div></div>{% endfor %}
  </div>
{% elif total_parlays > 0 %}
<div class="parlay-tabs">
  <button class="ptab active" onclick="filterParlays('all',this)">All</button>
  {% if state.parlays_2 %}<button class="ptab" onclick="filterParlays('2',this)">2-Leg ({{ state.parlays_2|length }})</button>{% endif %}
  {% if state.parlays_3 %}<button class="ptab" onclick="filterParlays('3',this)">3-Leg ({{ state.parlays_3|length }})</button>{% endif %}
  {% if state.parlays_4 %}<button class="ptab" onclick="filterParlays('4',this)">4-Leg ({{ state.parlays_4|length }})</button>{% endif %}
</div>
{% if state.parlays_2 %}
<div class="parlay-section visible" data-legs="2">
  {% for p in state.parlays_2 %}{% set ps=(bankroll*0.02)|round(0)|int %}{% set pw=(ps*p.combined_dec_odds)|round(0)|int %}
  <div class="parlay-card">
    <div class="parlay-header"><div class="parlay-tag">2-LEG PARLAY</div>
      <div class="parlay-chips">
        <div class="parlay-chip">Odds: <span class="v purple">{{ "%.2f"|format(p.combined_dec_odds) }}x</span></div>
        <div class="parlay-chip">Prob: <span class="v yellow">{{ "%.0f"|format(p.combined_prob*100) }}%</span></div>
        <div class="parlay-chip">EV: <span class="v {{ 'green' if p.ev > 0 else 'red' }}">{{ "%+.3f"|format(p.ev) }}</span></div>
      </div>
    </div>
    <div class="parlay-legs">{% for leg in p.legs %}<div class="parlay-leg-item"><div class="leg-dot"></div><div>{{ leg }}</div></div>{% endfor %}</div>
    <div class="parlay-payout-row"><div><span class="pp-label">Stake: </span><span class="pp-stake">${{ ps }}</span></div><div><span class="pp-label">Potential win: </span><span class="pp-win">${{ pw }}</span></div></div>
  </div>{% endfor %}
</div>{% endif %}
{% if state.parlays_3 %}
<div class="parlay-section visible" data-legs="3">
  {% for p in state.parlays_3 %}{% set ps=(bankroll*0.02)|round(0)|int %}{% set pw=(ps*p.combined_dec_odds)|round(0)|int %}
  <div class="parlay-card">
    <div class="parlay-header"><div class="parlay-tag">3-LEG PARLAY</div>
      <div class="parlay-chips">
        <div class="parlay-chip">Odds: <span class="v purple">{{ "%.2f"|format(p.combined_dec_odds) }}x</span></div>
        <div class="parlay-chip">Prob: <span class="v yellow">{{ "%.0f"|format(p.combined_prob*100) }}%</span></div>
        <div class="parlay-chip">EV: <span class="v {{ 'green' if p.ev > 0 else 'red' }}">{{ "%+.3f"|format(p.ev) }}</span></div>
      </div>
    </div>
    <div class="parlay-legs">{% for leg in p.legs %}<div class="parlay-leg-item"><div class="leg-dot"></div><div>{{ leg }}</div></div>{% endfor %}</div>
    <div class="parlay-payout-row"><div><span class="pp-label">Stake: </span><span class="pp-stake">${{ ps }}</span></div><div><span class="pp-label">Potential win: </span><span class="pp-win">${{ pw }}</span></div></div>
  </div>{% endfor %}
</div>{% endif %}
{% if state.parlays_4 %}
<div class="parlay-section visible" data-legs="4">
  {% for p in state.parlays_4 %}{% set ps=(bankroll*0.02)|round(0)|int %}{% set pw=(ps*p.combined_dec_odds)|round(0)|int %}
  <div class="parlay-card">
    <div class="parlay-header"><div class="parlay-tag">4-LEG PARLAY</div>
      <div class="parlay-chips">
        <div class="parlay-chip">Odds: <span class="v purple">{{ "%.2f"|format(p.combined_dec_odds) }}x</span></div>
        <div class="parlay-chip">Prob: <span class="v yellow">{{ "%.0f"|format(p.combined_prob*100) }}%</span></div>
        <div class="parlay-chip">EV: <span class="v {{ 'green' if p.ev > 0 else 'red' }}">{{ "%+.3f"|format(p.ev) }}</span></div>
      </div>
    </div>
    <div class="parlay-legs">{% for leg in p.legs %}<div class="parlay-leg-item"><div class="leg-dot"></div><div>{{ leg }}</div></div>{% endfor %}</div>
    <div class="parlay-payout-row"><div><span class="pp-label">Stake: </span><span class="pp-stake">${{ ps }}</span></div><div><span class="pp-label">Potential win: </span><span class="pp-win">${{ pw }}</span></div></div>
  </div>{% endfor %}
</div>{% endif %}
{% else %}
<div class="empty-state"><div class="ei">🔗</div><strong>No auto parlays yet</strong><p>Run analysis, or build your own by clicking + on any card above.</p></div>
{% endif %}

<div class="footer">
  <span>Bettor · MLB + Soccer Value Bet System · For entertainment only</span>
  <span>Min edge 5% · Kelly 25% · Bankroll ${{ "{:,.0f}".format(bankroll) }}</span>
</div>
</div>

<!-- ════════════════════ FLOATING PARLAY BUILDER ════════════════ -->
<div id="parlay-builder">
  <div class="pb-bar">
    <div class="pb-title">🔗 My Parlay</div>
    <div class="pb-legs-list" id="pb-legs-list"></div>
    <div class="pb-summary">
      <div><div class="pb-label">Odds</div><div class="pb-odds" id="pb-odds">—</div></div>
      <div class="pb-stake-wrap">
        <label for="pb-stake-input">Stake $</label>
        <input id="pb-stake-input" type="number" min="1" value="{{ (bankroll*0.02)|round(0)|int }}" oninput="updateParlayBuilder()">
      </div>
      <div><div class="pb-label">Potential Win</div><div class="pb-payout" id="pb-payout">$0</div></div>
    </div>
    <button class="pb-clear" onclick="clearParlay()">✕ Clear</button>
    <button class="pb-copy" onclick="copySlip()">📋 Copy Slip</button>
  </div>
</div>

<script>
const TODAY    = "{{ today }}";
const TOMORROW = "{{ tomorrow }}";
let selectedLegs = [];

function toggleLeg(id) {
  const idx = selectedLegs.findIndex(l => l.id === id);
  if (idx === -1) {
    const card = document.getElementById('card-' + id);
    selectedLegs.push({ id, label: card.dataset.label, decOdds: parseFloat(card.dataset.dec), prob: parseFloat(card.dataset.prob) });
    const btn = document.getElementById('addbtn-' + id);
    btn.classList.add('added'); btn.textContent = '✓';
    card.classList.add('selected');
  } else {
    selectedLegs.splice(idx, 1);
    const btn = document.getElementById('addbtn-' + id);
    btn.classList.remove('added'); btn.textContent = '+';
    document.getElementById('card-' + id).classList.remove('selected');
  }
  updateParlayBuilder();
}

function removeLeg(id) {
  const idx = selectedLegs.findIndex(l => l.id === id);
  if (idx !== -1) selectedLegs.splice(idx, 1);
  const btn  = document.getElementById('addbtn-' + id);
  const card = document.getElementById('card-' + id);
  if (btn)  { btn.classList.remove('added'); btn.textContent = '+'; }
  if (card) { card.classList.remove('selected'); }
  updateParlayBuilder();
}

function updateParlayBuilder() {
  const builder  = document.getElementById('parlay-builder');
  const legsList = document.getElementById('pb-legs-list');
  const oddsEl   = document.getElementById('pb-odds');
  const payEl    = document.getElementById('pb-payout');
  if (!selectedLegs.length) { builder.classList.remove('open'); return; }
  builder.classList.add('open');
  legsList.innerHTML = selectedLegs.map(l =>
    `<div class="pb-leg-chip"><span>${l.label}</span><button class="pb-remove" onclick="removeLeg('${l.id}')">×</button></div>`
  ).join('');
  const combinedDec = selectedLegs.reduce((acc, l) => acc * l.decOdds, 1.0);
  const stake = parseFloat(document.getElementById('pb-stake-input').value) || 20;
  oddsEl.textContent = combinedDec.toFixed(2) + 'x';
  payEl.textContent  = '$' + Math.round(stake * combinedDec).toLocaleString();
}

function clearParlay() {
  selectedLegs.forEach(l => {
    const btn  = document.getElementById('addbtn-' + l.id);
    const card = document.getElementById('card-' + l.id);
    if (btn)  { btn.classList.remove('added'); btn.textContent = '+'; }
    if (card) { card.classList.remove('selected'); }
  });
  selectedLegs = [];
  updateParlayBuilder();
}

function copySlip() {
  if (!selectedLegs.length) return;
  const combinedDec = selectedLegs.reduce((acc, l) => acc * l.decOdds, 1.0);
  const stake = parseFloat(document.getElementById('pb-stake-input').value) || 20;
  const slip = ['=== MY PARLAY (' + selectedLegs.length + '-LEG) ===',
    ...selectedLegs.map((l, i) => (i+1) + '. ' + l.label), '',
    'Combined Odds: ' + combinedDec.toFixed(2) + 'x',
    'Stake: $' + stake,
    'Potential Win: $' + Math.round(stake * combinedDec).toLocaleString(),
    'Date: ' + new Date().toLocaleDateString()
  ].join('\n');
  navigator.clipboard.writeText(slip).then(() => {
    const btn = document.querySelector('.pb-copy');
    btn.textContent = '✅ Copied!';
    setTimeout(() => btn.textContent = '📋 Copy Slip', 2000);
  });
}

function filterParlays(legs, btn) {
  document.querySelectorAll('.ptab').forEach(t => t.classList.remove('active'));
  btn.classList.add('active');
  document.querySelectorAll('.parlay-section').forEach(s => {
    s.classList.toggle('visible', legs === 'all' || s.dataset.legs === legs);
  });
}

let _polling = null;

function setStatus(s, phase, phaseIdx, phaseTotal) {
  const pill = document.getElementById('status-pill');
  pill.className = 'status-pill ' + s;
  document.getElementById('status-dot').className = 'dot ' + s;
  document.getElementById('status-text').textContent = s.charAt(0).toUpperCase() + s.slice(1);
  document.getElementById('run-btn').disabled = (s === 'running');

  const pw = document.getElementById('progress-wrap');
  if (s === 'running') {
    pw.classList.add('visible');
    if (phase) document.getElementById('phase-name').textContent = phase;
    const pct = phaseTotal > 0 ? Math.round((phaseIdx / phaseTotal) * 100) : 0;
    document.getElementById('phase-pct').textContent = pct + '%';
    document.getElementById('progress-fill').style.width = pct + '%';
    // Update step indicators
    document.querySelectorAll('.phase-step').forEach((el, i) => {
      el.className = 'phase-step' + (i < phaseIdx ? ' done-step' : (i === phaseIdx ? ' active' : ''));
    });
  } else {
    pw.classList.remove('visible');
  }
}

function runAnalysis() {
  fetch('/api/run', { method: 'POST' })
    .then(r => r.json()).then(d => {
      if (!d.ok) { alert(d.msg); return; }
      setStatus('running', '', 0, 8);
      startPolling();
    }).catch(e => alert('Error: ' + e));
}

function startPolling() {
  if (_polling) clearInterval(_polling);
  _polling = setInterval(() => {
    fetch('/api/status').then(r => r.json()).then(d => {
      setStatus(d.status, d.phase, d.phase_idx, d.phase_total);
      if (d.status === 'done' || d.status === 'error') {
        clearInterval(_polling); _polling = null;
        window.location.reload();
      }
    });
  }, 1500);
}

(function() { if ('{{ state.status }}' === 'running') startPolling(); })();
</script>
</body>
</html>"""

out = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src", "templates", "dashboard.html"))
with open(out, "w", encoding="utf-8") as fh:
    fh.write(HTML)
print(f"Written {len(HTML.splitlines())} lines to {out}")
