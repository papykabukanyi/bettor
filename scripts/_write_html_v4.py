"""Write the new dashboard.html v4 — MLB-only, 5-tab design."""
import os

HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0,viewport-fit=cover">
<meta name="mobile-web-app-capable" content="yes">
<title>Bettor — MLB Dashboard</title>
<link rel="icon" type="image/svg+xml" href="/static/favicon.svg">
<style>
*{box-sizing:border-box;margin:0;padding:0}
:root{
  --bg:#070a10;--card:#0d1117;--card2:#111820;--border:#1e2a38;
  --accent:#2563eb;--accent2:#1d4ed8;--green:#16a34a;--red:#dc2626;
  --yellow:#ca8a04;--purple:#7c3aed;--text:#e2e8f0;--muted:#64748b;
  --elite:#16a34a;--safe:#2563eb;--moderate:#ca8a04;--risky:#dc2626;
  --radius:10px;--shadow:0 2px 16px rgba(0,0,0,.5);
}
body{background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;min-height:100vh}
a{color:inherit;text-decoration:none}

/* ── Header ── */
.header{display:flex;align-items:center;justify-content:space-between;
  padding:14px 20px;border-bottom:1px solid var(--border);
  background:linear-gradient(135deg,#0d1117 0%,#111820 100%)}
.header-brand{display:flex;align-items:center;gap:10px}
.header-brand img{width:32px;height:32px}
.header-brand h1{font-size:1.25rem;font-weight:700;letter-spacing:.5px}
.header-brand span{font-size:.7rem;color:var(--muted);background:#1e2a38;
  padding:2px 8px;border-radius:20px;margin-left:6px}
.header-actions{display:flex;align-items:center;gap:10px;flex-wrap:wrap}
.btn{display:inline-flex;align-items:center;gap:6px;padding:8px 14px;
  border:1px solid var(--border);border-radius:8px;background:var(--card2);
  color:var(--text);font-size:.82rem;cursor:pointer;transition:all .2s}
.btn:hover{border-color:var(--accent);color:var(--accent)}
.btn-primary{background:var(--accent);border-color:var(--accent);color:#fff}
.btn-primary:hover{background:var(--accent2);border-color:var(--accent2);color:#fff}
.btn-sm{padding:5px 10px;font-size:.75rem}
#last-updated{font-size:.75rem;color:var(--muted)}

/* ── Loading overlay ── */
#loading-overlay{position:fixed;inset:0;background:rgba(7,10,16,.92);
  display:flex;flex-direction:column;align-items:center;justify-content:center;
  z-index:1000;gap:20px;transition:opacity .4s}
#loading-overlay.hidden{opacity:0;pointer-events:none}
.spinner{width:48px;height:48px;border:4px solid var(--border);
  border-top-color:var(--accent);border-radius:50%;animation:spin 1s linear infinite}
@keyframes spin{to{transform:rotate(360deg)}}
#load-phase{font-size:.9rem;color:var(--muted);max-width:300px;text-align:center}
.progress-bar{width:280px;height:4px;background:var(--border);border-radius:4px;overflow:hidden}
.progress-fill{height:100%;background:var(--accent);transition:width .4s;border-radius:4px}

/* ── Tabs ── */
.tabs{display:flex;gap:4px;padding:12px 20px 0;background:var(--card);
  border-bottom:1px solid var(--border);overflow-x:auto;scrollbar-width:none}
.tabs::-webkit-scrollbar{display:none}
.tab{padding:9px 16px;font-size:.85rem;cursor:pointer;border-radius:8px 8px 0 0;
  white-space:nowrap;color:var(--muted);transition:all .2s;border:1px solid transparent;
  border-bottom:none}
.tab.active{color:var(--text);background:var(--bg);border-color:var(--border)}
.tab:hover:not(.active){color:var(--text)}
.tab-badge{background:var(--accent);color:#fff;font-size:.65rem;padding:1px 5px;
  border-radius:10px;margin-left:4px}

/* ── Main content ── */
main{padding:20px;max-width:1400px;margin:0 auto}
.tab-panel{display:none}
.tab-panel.active{display:block}

/* ── Section header ── */
.section-header{display:flex;align-items:center;justify-content:space-between;
  margin-bottom:16px;flex-wrap:wrap;gap:10px}
.section-title{font-size:1rem;font-weight:600}
.section-meta{font-size:.78rem;color:var(--muted)}

/* ── Game cards grid ── */
.games-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(340px,1fr));gap:16px}

/* ── Game card ── */
.game-card{background:var(--card);border:1px solid var(--border);border-radius:var(--radius);
  overflow:hidden;transition:border-color .2s,box-shadow .2s;cursor:pointer}
.game-card:hover{border-color:var(--accent);box-shadow:var(--shadow)}
.game-card.expanded .card-body{display:block}

.card-header{padding:14px 16px 10px;display:flex;flex-direction:column;gap:10px}
.card-when{display:flex;align-items:center;justify-content:space-between}
.when-badge{font-size:.7rem;font-weight:600;padding:3px 8px;border-radius:6px;text-transform:uppercase}
.when-TODAY{background:rgba(22,163,74,.15);color:var(--green)}
.when-TOMORROW{background:rgba(37,99,235,.15);color:var(--accent)}
.when-LIVE{background:rgba(220,38,38,.15);color:var(--red);animation:pulse 2s infinite}
.when-FINAL{background:#1e2a38;color:var(--muted)}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.5}}
.safety-badge{font-size:.7rem;font-weight:700;padding:3px 8px;border-radius:6px}
.badge-ELITE{background:rgba(22,163,74,.2);color:var(--green)}
.badge-SAFE{background:rgba(37,99,235,.2);color:var(--accent)}
.badge-MODERATE{background:rgba(202,138,4,.2);color:var(--yellow)}
.badge-RISKY{background:rgba(220,38,38,.2);color:var(--red)}

.matchup{display:flex;align-items:center;justify-content:space-between;gap:8px}
.team-side{flex:1;min-width:0}
.team-name{font-size:1rem;font-weight:700;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.team-starter{font-size:.7rem;color:var(--muted);margin-top:2px}
.vs{font-size:.8rem;color:var(--muted);flex-shrink:0;padding:0 6px}
.team-side.away{text-align:right}

.quick-bets{display:flex;gap:6px;flex-wrap:wrap;padding:4px 0 2px}
.quick-chip{font-size:.72rem;padding:4px 8px;border-radius:6px;
  border:1px solid var(--border);background:var(--card2);
  display:inline-flex;align-items:center;gap:4px;white-space:nowrap}
.quick-chip strong{color:var(--text)}
.chip-ELITE{border-color:var(--green);color:var(--green)}
.chip-SAFE{border-color:var(--accent);color:var(--accent)}
.chip-MODERATE{border-color:var(--yellow);color:var(--yellow)}
.chip-RISKY{border-color:var(--red);color:var(--red)}

.card-expand-btn{width:100%;padding:8px;background:none;border:none;border-top:1px solid var(--border);
  color:var(--muted);font-size:.75rem;cursor:pointer;display:flex;align-items:center;justify-content:center;gap:4px}
.card-expand-btn:hover{color:var(--text);background:rgba(255,255,255,.03)}
.expand-icon{transition:transform .2s}
.expanded .expand-icon{transform:rotate(180deg)}

/* ── Card body (expanded) ── */
.card-body{display:none;border-top:1px solid var(--border)}

/* ── Bet rows ── */
.bet-section{padding:12px 14px}
.bet-section-title{font-size:.7rem;font-weight:600;color:var(--muted);
  text-transform:uppercase;letter-spacing:.5px;margin-bottom:8px}
.bet-row{display:flex;align-items:center;justify-content:space-between;
  padding:7px 0;border-bottom:1px solid rgba(255,255,255,.04);gap:8px}
.bet-row:last-child{border-bottom:none}
.bet-label{font-size:.75rem;color:var(--muted);min-width:90px}
.bet-pick{font-size:.82rem;font-weight:600;flex:1}
.bet-odds{font-size:.78rem;color:var(--muted)}
.bet-edge{font-size:.72rem;padding:2px 6px;border-radius:4px}
.bet-edge.pos{background:rgba(22,163,74,.15);color:var(--green)}
.bet-edge.neg{background:rgba(220,38,38,.1);color:var(--muted)}
.bet-add{padding:3px 8px;font-size:.7rem;border-radius:5px;background:var(--card2);
  border:1px solid var(--border);color:var(--muted);cursor:pointer;flex-shrink:0}
.bet-add:hover{border-color:var(--accent);color:var(--accent)}
.bet-add.added{border-color:var(--green);color:var(--green)}

/* ── Props section ── */
.props-section{padding:10px 14px 14px}
.props-team-header{font-size:.75rem;font-weight:600;color:var(--muted);
  text-transform:uppercase;letter-spacing:.5px;margin:8px 0 6px;
  display:flex;align-items:center;gap:6px}
.props-team-header::after{content:'';flex:1;height:1px;background:var(--border)}
.prop-row{display:flex;align-items:center;gap:8px;padding:6px 0;
  border-bottom:1px solid rgba(255,255,255,.04)}
.prop-row:last-child{border-bottom:none}
.prop-player{font-size:.82rem;font-weight:600;min-width:120px}
.prop-stat{font-size:.72rem;color:var(--muted);min-width:80px}
.prop-dir{font-size:.78rem;font-weight:700;min-width:70px}
.prop-dir.OVER{color:var(--green)}
.prop-dir.UNDER{color:var(--yellow)}
.prop-conf{font-size:.72rem;color:var(--muted)}
.prop-badge{font-size:.65rem;padding:2px 5px;border-radius:4px}

/* ── Empty state ── */
.empty-state{text-align:center;padding:60px 20px;color:var(--muted)}
.empty-state .icon{font-size:3rem;margin-bottom:16px;opacity:.4}
.empty-state p{font-size:.9rem}

/* ── Props table ── */
.props-table-wrap{overflow-x:auto;border-radius:var(--radius);border:1px solid var(--border)}
.props-table{width:100%;border-collapse:collapse;font-size:.82rem}
.props-table th{background:var(--card2);padding:10px 12px;text-align:left;
  font-size:.72rem;font-weight:600;color:var(--muted);text-transform:uppercase;
  letter-spacing:.5px;white-space:nowrap;border-bottom:1px solid var(--border);cursor:pointer}
.props-table th:hover{color:var(--text)}
.props-table td{padding:9px 12px;border-bottom:1px solid rgba(255,255,255,.04);
  white-space:nowrap}
.props-table tr:last-child td{border-bottom:none}
.props-table tr:hover td{background:rgba(255,255,255,.02)}
.props-table .dir-OVER{color:var(--green);font-weight:700}
.props-table .dir-UNDER{color:var(--yellow);font-weight:700}

/* ── Filter bar ── */
.filter-bar{display:flex;align-items:center;gap:10px;margin-bottom:16px;flex-wrap:wrap}
.filter-bar input,.filter-bar select{
  background:var(--card);border:1px solid var(--border);color:var(--text);
  padding:7px 10px;border-radius:8px;font-size:.82rem;outline:none}
.filter-bar input:focus,.filter-bar select:focus{border-color:var(--accent)}

/* ── Parlays ── */
.parlays-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(320px,1fr));gap:16px}
.parlay-card{background:var(--card);border:1px solid var(--border);
  border-radius:var(--radius);padding:16px;transition:border-color .2s}
.parlay-card:hover{border-color:var(--purple)}
.parlay-header{display:flex;align-items:flex-start;justify-content:space-between;
  margin-bottom:12px;gap:8px}
.parlay-meta{display:flex;flex-direction:column;gap:3px}
.parlay-legs-count{font-size:.72rem;color:var(--muted)}
.parlay-odds{font-size:1.1rem;font-weight:700;color:var(--purple)}
.parlay-payout{font-size:.78rem;color:var(--muted)}
.parlay-leg{display:flex;align-items:center;gap:8px;padding:5px 0;
  border-bottom:1px solid rgba(255,255,255,.05)}
.parlay-leg:last-child{border-bottom:none}
.leg-dot{width:6px;height:6px;border-radius:50%;background:var(--accent);flex-shrink:0}
.leg-text{font-size:.78rem;flex:1}
.leg-conf{font-size:.7rem;color:var(--muted)}
.parlay-save-btn{margin-top:12px;width:100%;padding:8px;border-radius:8px;
  background:rgba(124,58,237,.15);border:1px solid var(--purple);
  color:var(--purple);font-size:.78rem;cursor:pointer;transition:all .2s}
.parlay-save-btn:hover{background:rgba(124,58,237,.3)}

/* Parlay builder */
.parlay-builder{background:var(--card);border:1px solid var(--border);
  border-radius:var(--radius);padding:16px;margin-bottom:20px}
.parlay-builder h3{font-size:.9rem;font-weight:600;margin-bottom:12px}
#parlay-legs-list{min-height:40px;margin-bottom:12px}
.parlay-leg-item{display:flex;align-items:center;justify-content:space-between;
  padding:6px 8px;background:var(--card2);border-radius:6px;margin-bottom:6px;font-size:.82rem}
.remove-leg{padding:2px 6px;background:none;border:1px solid var(--border);
  color:var(--red);border-radius:4px;cursor:pointer;font-size:.7rem}
.remove-leg:hover{background:rgba(220,38,38,.15)}
.parlay-builder-actions{display:flex;gap:8px;flex-wrap:wrap;align-items:center}
#parlay-combined-odds{font-size:.9rem;color:var(--purple);font-weight:700}

/* Tracked parlays */
.tracked-parlay{background:var(--card2);border:1px solid var(--border);
  border-radius:var(--radius);padding:14px;margin-bottom:10px}
.tracked-parlay-header{display:flex;align-items:center;justify-content:space-between}
.tp-name{font-weight:600;font-size:.88rem}
.tp-outcome{font-size:.72rem;padding:2px 6px;border-radius:4px;font-weight:600}
.tp-PENDING{background:rgba(202,138,4,.15);color:var(--yellow)}
.tp-WIN{background:rgba(22,163,74,.15);color:var(--green)}
.tp-LOSS{background:rgba(220,38,38,.15);color:var(--red)}
.tp-meta{font-size:.75rem;color:var(--muted);margin-top:4px}

/* ── Performance ── */
.perf-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:14px;margin-bottom:24px}
.perf-card{background:var(--card);border:1px solid var(--border);border-radius:var(--radius);
  padding:16px;text-align:center}
.perf-val{font-size:2rem;font-weight:800;margin-bottom:4px}
.perf-label{font-size:.75rem;color:var(--muted);text-transform:uppercase;letter-spacing:.5px}
.perf-win{color:var(--green)}
.perf-loss{color:var(--red)}
.perf-pend{color:var(--yellow)}
.perf-rate{color:var(--accent)}

.by-type-table{width:100%;border-collapse:collapse;font-size:.82rem;
  border:1px solid var(--border);border-radius:var(--radius);overflow:hidden}
.by-type-table th{background:var(--card2);padding:9px 12px;text-align:left;
  font-size:.72rem;color:var(--muted);text-transform:uppercase}
.by-type-table td{padding:8px 12px;border-top:1px solid var(--border)}
.by-type-table tr:hover td{background:rgba(255,255,255,.02)}

.pred-history{margin-top:24px}
.pred-row{display:flex;align-items:center;gap:10px;padding:9px 0;
  border-bottom:1px solid var(--border);font-size:.8rem;flex-wrap:wrap}
.pred-row:last-child{border-bottom:none}
.pred-outcome{font-size:.7rem;font-weight:700;padding:2px 6px;border-radius:4px;flex-shrink:0}
.pred-WIN{background:rgba(22,163,74,.15);color:var(--green)}
.pred-LOSS{background:rgba(220,38,38,.15);color:var(--red)}
.pred-PUSH{background:#1e2a38;color:var(--muted)}
.pred-PENDING{background:rgba(202,138,4,.15);color:var(--yellow)}
.pred-pick{flex:1;min-width:120px}
.pred-meta{color:var(--muted);font-size:.75rem}

/* ── Phone modal ── */
.modal-overlay{position:fixed;inset:0;background:rgba(0,0,0,.7);z-index:100;
  display:flex;align-items:center;justify-content:center;padding:20px}
.modal-overlay.hidden{display:none}
.modal{background:var(--card);border:1px solid var(--border);border-radius:12px;
  padding:24px;max-width:480px;width:100%}
.modal h2{font-size:1rem;font-weight:600;margin-bottom:16px}
.modal-close{float:right;background:none;border:none;color:var(--muted);
  font-size:1.2rem;cursor:pointer;line-height:1}
.form-row{margin-bottom:12px}
.form-row label{display:block;font-size:.78rem;color:var(--muted);margin-bottom:4px}
.form-row input{width:100%;background:var(--card2);border:1px solid var(--border);
  color:var(--text);padding:8px 10px;border-radius:8px;font-size:.85rem;outline:none}
.form-row input:focus{border-color:var(--accent)}
.phone-list{margin-top:12px;max-height:200px;overflow-y:auto}
.phone-item{display:flex;align-items:center;justify-content:space-between;
  padding:6px 0;border-bottom:1px solid var(--border);font-size:.82rem}
.phone-item:last-child{border-bottom:none}
.phone-remove{padding:3px 8px;background:none;border:1px solid var(--border);
  color:var(--red);border-radius:4px;cursor:pointer;font-size:.72rem}

/* ── Toast ── */
#toast{position:fixed;bottom:24px;right:24px;background:var(--card);
  border:1px solid var(--border);border-radius:10px;padding:12px 18px;
  font-size:.85rem;max-width:320px;z-index:2000;
  transform:translateY(100px);opacity:0;transition:all .3s;pointer-events:none}
#toast.show{transform:translateY(0);opacity:1}
#toast.success{border-color:var(--green);color:var(--green)}
#toast.error{border-color:var(--red);color:var(--red)}
#toast.info{border-color:var(--accent);color:var(--accent)}

/* ── No-data helpers ── */
.no-games{text-align:center;padding:40px 20px;color:var(--muted);font-size:.9rem}

/* ── Responsive ── */
@media(max-width:640px){
  .header{padding:12px 14px}
  main{padding:14px}
  .header-brand h1{font-size:1.05rem}
  .games-grid{grid-template-columns:1fr}
  .tabs{padding:10px 14px 0}
  .tab{padding:7px 12px;font-size:.78rem}
}
</style>
</head>
<body>

<!-- Loading overlay -->
<div id="loading-overlay">
  <div class="spinner"></div>
  <div id="load-phase">Loading analysis…</div>
  <div class="progress-bar"><div class="progress-fill" id="progress-fill" style="width:5%"></div></div>
</div>

<!-- Header -->
<header class="header">
  <div class="header-brand">
    <img src="/static/favicon.svg" alt="Bettor" onerror="this.style.display='none'">
    <h1>Bettor <span>MLB</span></h1>
  </div>
  <div class="header-actions">
    <span id="last-updated">{{ last_updated or 'Not loaded' }}</span>
    <button class="btn btn-primary" id="run-btn" onclick="runAnalysis()">&#9654; Run Analysis</button>
    <button class="btn" onclick="openPhoneModal()">&#128241; SMS</button>
  </div>
</header>

<!-- Tabs -->
<div class="tabs" id="tabs">
  <div class="tab active" data-tab="today" onclick="switchTab('today')">
    &#9202; Today <span class="tab-badge" id="badge-today">0</span>
  </div>
  <div class="tab" data-tab="tomorrow" onclick="switchTab('tomorrow')">
    &#128197; Tomorrow <span class="tab-badge" id="badge-tomorrow">0</span>
  </div>
  <div class="tab" data-tab="props" onclick="switchTab('props')">
    &#128200; All Props
  </div>
  <div class="tab" data-tab="parlays" onclick="switchTab('parlays')">
    &#127917; Parlays
  </div>
  <div class="tab" data-tab="performance" onclick="switchTab('performance')">
    &#128202; Performance
  </div>
</div>

<!-- Main -->
<main>

  <!-- TODAY -->
  <div class="tab-panel active" id="panel-today">
    <div class="section-header">
      <span class="section-title">Today's Games</span>
      <span class="section-meta" id="today-meta"></span>
    </div>
    <div class="games-grid" id="today-grid"></div>
    <div class="no-games hidden" id="today-empty">No games today or analysis not yet run.</div>
  </div>

  <!-- TOMORROW -->
  <div class="tab-panel" id="panel-tomorrow">
    <div class="section-header">
      <span class="section-title">Tomorrow's Games</span>
      <span class="section-meta" id="tomorrow-meta"></span>
    </div>
    <div class="games-grid" id="tomorrow-grid"></div>
    <div class="no-games hidden" id="tomorrow-empty">No games tomorrow or analysis not yet run.</div>
  </div>

  <!-- ALL PROPS -->
  <div class="tab-panel" id="panel-props">
    <div class="section-header">
      <span class="section-title">All Player Props</span>
      <span class="section-meta" id="props-meta"></span>
    </div>
    <div class="filter-bar">
      <input type="text" id="props-search" placeholder="Search player, team…" oninput="filterProps()">
      <select id="props-stat" onchange="filterProps()">
        <option value="">All Stats</option>
        <option value="strikeouts">Pitcher Ks</option>
        <option value="hits">Hits</option>
        <option value="home_runs">Home Runs</option>
        <option value="total_bases">Total Bases</option>
        <option value="rbi">RBI</option>
        <option value="runs">Runs</option>
        <option value="walks">Walks</option>
        <option value="stolen_bases">Stolen Bases</option>
      </select>
      <select id="props-dir" onchange="filterProps()">
        <option value="">All</option>
        <option value="OVER">OVER</option>
        <option value="UNDER">UNDER</option>
      </select>
      <select id="props-safety" onchange="filterProps()">
        <option value="">All Safety</option>
        <option value="ELITE">ELITE</option>
        <option value="SAFE">SAFE</option>
        <option value="MODERATE">MODERATE</option>
      </select>
    </div>
    <div class="props-table-wrap">
      <table class="props-table">
        <thead>
          <tr>
            <th onclick="sortProps('name')">Player &#8597;</th>
            <th onclick="sortProps('team')">Team &#8597;</th>
            <th onclick="sortProps('prop_label')">Stat &#8597;</th>
            <th onclick="sortProps('line')">Line &#8597;</th>
            <th onclick="sortProps('direction')">Dir &#8597;</th>
            <th onclick="sortProps('model_prob')">Prob &#8597;</th>
            <th onclick="sortProps('safety')">Safety &#8597;</th>
            <th onclick="sortProps('ev')">EV &#8597;</th>
            <th>Add</th>
          </tr>
        </thead>
        <tbody id="props-tbody"></tbody>
      </table>
    </div>
  </div>

  <!-- PARLAYS -->
  <div class="tab-panel" id="panel-parlays">
    <!-- Builder -->
    <div class="parlay-builder">
      <h3>&#10133; Parlay Builder <span style="font-size:.75rem;color:var(--muted);font-weight:400">(click + on any pick)</span></h3>
      <div id="parlay-legs-list"><div style="color:var(--muted);font-size:.82rem">No legs added yet.</div></div>
      <div class="parlay-builder-actions">
        <span id="parlay-combined-odds"></span>
        <input type="text" id="parlay-name" placeholder="Parlay name…"
          style="background:var(--card2);border:1px solid var(--border);color:var(--text);
          padding:6px 10px;border-radius:8px;font-size:.82rem;outline:none;flex:1;max-width:200px">
        <input type="number" id="parlay-stake" placeholder="Stake $"
          style="background:var(--card2);border:1px solid var(--border);color:var(--text);
          padding:6px 10px;border-radius:8px;font-size:.82rem;outline:none;width:90px">
        <button class="btn btn-primary btn-sm" onclick="saveParlay()">&#128190; Save</button>
        <button class="btn btn-sm" onclick="clearParlay()">Clear</button>
      </div>
    </div>

    <!-- Auto-generated parlays -->
    <div class="section-header">
      <span class="section-title">&#129351; Best Auto-Parlays</span>
    </div>
    <div class="parlays-grid" id="auto-parlays-grid"></div>
    <div class="no-games hidden" id="parlays-empty">Run analysis to generate parlays.</div>

    <!-- Tracked parlays -->
    <div class="section-header" style="margin-top:28px">
      <span class="section-title">&#128204; Tracked Parlays</span>
      <button class="btn btn-sm" onclick="loadTrackedParlays()">Refresh</button>
    </div>
    <div id="tracked-parlays-list"><div class="no-games">No tracked parlays yet.</div></div>
  </div>

  <!-- PERFORMANCE -->
  <div class="tab-panel" id="panel-performance">
    <div class="section-header">
      <span class="section-title">Prediction Performance</span>
      <div style="display:flex;gap:8px;flex-wrap:wrap">
        <button class="btn btn-sm" onclick="loadPerformance()">&#8635; Refresh</button>
        <button class="btn btn-sm" onclick="resolveOutcomes()">&#9989; Resolve Outcomes</button>
      </div>
    </div>
    <div class="perf-grid" id="perf-stats"></div>
    <h3 style="font-size:.9rem;font-weight:600;margin-bottom:12px;margin-top:4px">By Bet Type</h3>
    <div style="overflow-x:auto;border:1px solid var(--border);border-radius:var(--radius)">
      <table class="by-type-table" id="by-type-table">
        <thead><tr>
          <th>Bet Type</th><th>W</th><th>L</th><th>Push</th><th>Pending</th><th>Hit Rate</th>
        </tr></thead>
        <tbody id="by-type-tbody"></tbody>
      </table>
    </div>
    <div class="pred-history">
      <div class="section-header" style="margin-top:24px">
        <span class="section-title">Recent Predictions</span>
        <select id="pred-filter-outcome" onchange="loadHistory()" style="background:var(--card);
          border:1px solid var(--border);color:var(--text);padding:5px 8px;border-radius:6px;font-size:.78rem">
          <option value="">All outcomes</option>
          <option value="WIN">WIN</option>
          <option value="LOSS">LOSS</option>
          <option value="PENDING">PENDING</option>
        </select>
      </div>
      <div id="pred-history-list"><div class="no-games">Loading…</div></div>
    </div>
  </div>

</main>

<!-- Phone modal -->
<div class="modal-overlay hidden" id="phone-modal">
  <div class="modal">
    <button class="modal-close" onclick="closePhoneModal()">&times;</button>
    <h2>&#128241; SMS Recipients</h2>
    <div class="form-row">
      <label>Phone number (+1XXXXXXXXXX)</label>
      <input type="tel" id="new-phone" placeholder="+15551234567">
    </div>
    <div class="form-row">
      <label>Label (optional)</label>
      <input type="text" id="new-phone-label" placeholder="e.g. Main">
    </div>
    <button class="btn btn-primary" onclick="addPhone()" style="width:100%;margin-bottom:12px">Add Number</button>
    <div class="phone-list" id="phone-list"></div>
    <div style="margin-top:14px;border-top:1px solid var(--border);padding-top:12px">
      <button class="btn btn-primary" onclick="sendSms()" style="width:100%">&#128232; Send Today's Picks</button>
    </div>
  </div>
</div>

<!-- Toast -->
<div id="toast"></div>

<script>
// ── Injected data ──────────────────────────────────────────────────────────
let TODAY_CARDS    = {{ today_cards|safe }};
let TOMORROW_CARDS = {{ tomorrow_cards|safe }};
let BEST_PARLAYS   = {{ best_parlays|safe }};
let ALL_PROPS      = {{ all_props|safe }};
let parlayLegs     = [];
let propsSortKey   = 'safety';
let propsSortAsc   = false;

// ── Loading / status polling ───────────────────────────────────────────────
const PHASES = {{ phases|tojson }};
let pollingId = null;

function hideOverlay() {
  document.getElementById('loading-overlay').classList.add('hidden');
}
function showOverlay(phase, pct) {
  document.getElementById('loading-overlay').classList.remove('hidden');
  document.getElementById('load-phase').textContent = phase || 'Working…';
  document.getElementById('progress-fill').style.width = (pct||5)+'%';
}

function startPolling() {
  if (pollingId) return;
  pollingId = setInterval(pollStatus, 2500);
}
function stopPolling() {
  clearInterval(pollingId); pollingId = null;
}

function pollStatus() {
  fetch('/api/status').then(r=>r.json()).then(data=>{
    if (data.status === 'running') {
      const pct = data.phase_total > 0
        ? Math.round((data.phase_idx+1)/data.phase_total*100) : 10;
      showOverlay(data.phase, pct);
    } else {
      stopPolling();
      hideOverlay();
      if (data.status === 'done') {
        loadCachedState();
      } else if (data.status === 'error') {
        toast('Analysis error — check logs', 'error');
      }
    }
  }).catch(()=>{ stopPolling(); hideOverlay(); });
}

function runAnalysis() {
  fetch('/api/run', {method:'POST'}).then(r=>r.json()).then(d=>{
    if (d.ok) { showOverlay('Starting analysis…',2); startPolling(); }
    else toast(d.msg||'Already running','info');
  });
}

function loadCachedState() {
  fetch('/api/cached-state').then(r=>r.json()).then(d=>{
    if (!d.ok && d.status !== 'done') return;
    TODAY_CARDS    = d.game_cards_today    || [];
    TOMORROW_CARDS = d.game_cards_tomorrow || [];
    BEST_PARLAYS   = d.best_parlays        || [];
    ALL_PROPS      = d.player_props        || [];
    renderAll();
    if (d.last_updated) {
      document.getElementById('last-updated').textContent = d.last_updated;
    }
  });
}

// ── Tab switching ─────────────────────────────────────────────────────────
function switchTab(name) {
  document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));
  document.querySelectorAll('.tab-panel').forEach(p=>p.classList.remove('active'));
  document.querySelector(`[data-tab="${name}"]`).classList.add('active');
  document.getElementById(`panel-${name}`).classList.add('active');
  if (name === 'performance') loadPerformance();
  if (name === 'parlays') loadTrackedParlays();
}

// ── Safety badge helpers ──────────────────────────────────────────────────
function safetyBadge(label) {
  return `<span class="safety-badge badge-${label}">${label}</span>`;
}
function whenBadge(label, when) {
  return `<span class="when-badge when-${label}">${when}</span>`;
}
function dirClass(dir) { return dir === 'OVER' ? 'dir-OVER' : 'dir-UNDER'; }

function fmtOdds(am) {
  if (!am) return '—';
  return am > 0 ? `+${am}` : `${am}`;
}
function fmtEdge(edge) {
  if (edge == null) return '';
  const pct = Math.round(edge*100);
  return pct >= 0
    ? `<span class="bet-edge pos">+${pct}%</span>`
    : `<span class="bet-edge neg">${pct}%</span>`;
}

// ── Render all ────────────────────────────────────────────────────────────
function renderAll() {
  renderGames('today-grid', TODAY_CARDS, 'today-empty', 'today-meta', 'badge-today');
  renderGames('tomorrow-grid', TOMORROW_CARDS, 'tomorrow-empty', 'tomorrow-meta', 'badge-tomorrow');
  renderPropsTable(ALL_PROPS);
  renderParlays(BEST_PARLAYS);
}

// ── Game cards ────────────────────────────────────────────────────────────
function renderGames(gridId, cards, emptyId, metaId, badgeId) {
  const grid = document.getElementById(gridId);
  const empty = document.getElementById(emptyId);
  const meta  = document.getElementById(metaId);
  const badge = document.getElementById(badgeId);
  if (!cards || !cards.length) {
    grid.innerHTML = '';
    empty.classList.remove('hidden');
    if (badge) badge.textContent = '0';
    return;
  }
  empty.classList.add('hidden');
  if (badge) badge.textContent = cards.length;
  if (meta) meta.textContent = `${cards.length} game${cards.length!==1?'s':''} · MLB`;
  grid.innerHTML = cards.map(c => gameCard(c)).join('');
}

function gameCard(c) {
  const slBadge = safetyBadge(c.overall_safety_label || 'MODERATE');
  const whenB   = whenBadge(c.when_label||'TODAY', c.when||'TODAY');

  // Quick chips — best of ML, RL, Total
  const chips = [];
  if (c.moneyline)  chips.push(quickChip('ML', c.moneyline));
  if (c.run_line)   chips.push(quickChip('RL', c.run_line));
  if (c.total)      chips.push(quickChip('O/U', c.total));
  const chipsHtml = chips.join('') || '<span style="color:var(--muted);font-size:.75rem">No picks above edge threshold</span>';

  const propCount = (c.home_props||[]).length + (c.away_props||[]).length;

  return `
<div class="game-card" id="card-${c.game_key.replace(/[@\s]/g,'_')}">
  <div class="card-header" onclick="toggleCard(this.parentElement)">
    <div class="card-when">
      ${whenB}
      ${slBadge}
    </div>
    <div class="matchup">
      <div class="team-side">
        <div class="team-name">${c.away_team}</div>
        <div class="team-starter">&#9918; ${c.away_starter||'TBD'}</div>
      </div>
      <div class="vs">@</div>
      <div class="team-side away">
        <div class="team-name">${c.home_team}</div>
        <div class="team-starter">&#9918; ${c.home_starter||'TBD'}</div>
      </div>
    </div>
    <div class="quick-bets">${chipsHtml}</div>
  </div>
  <button class="card-expand-btn" onclick="toggleCard(this.parentElement)">
    <span>All Bets${propCount?' + '+propCount+' Props':''}</span>
    <span class="expand-icon">&#9660;</span>
  </button>
  <div class="card-body">
    ${betSection('Game Bets', [
      betRow('Moneyline',       c.moneyline,       c),
      betRow('Run Line',        c.run_line,        c),
      betRow('Total',           c.total,           c),
      betRow('F5 Moneyline',    c.f5_moneyline,    c),
      betRow('F5 Total',        c.f5_total,        c),
      betRow('Home Team Total', c.home_team_total, c),
      betRow('Away Team Total', c.away_team_total, c),
    ])}
    ${propsSection(c)}
  </div>
</div>`;
}

function quickChip(label, bet) {
  if (!bet) return '';
  const sl = bet.safety_label || 'MODERATE';
  return `<span class="quick-chip chip-${sl}">
    <strong>${label}</strong> ${bet.pick||''}
    <span style="color:var(--muted)">${fmtOdds(bet.odds_am)}</span>
  </span>`;
}

function betSection(title, rows) {
  const validRows = rows.filter(Boolean);
  if (!validRows.length) return '';
  return `<div class="bet-section">
    <div class="bet-section-title">${title}</div>
    ${validRows.join('')}
  </div>`;
}

function betRow(label, bet, card) {
  if (!bet) return '';
  const pick = bet.pick || '—';
  const odds = fmtOdds(bet.odds_am);
  const edge = fmtEdge(bet.edge);
  const sb   = safetyBadge(bet.safety_label||'MODERATE');
  const addKey = JSON.stringify({
    label: pick,
    bet_type: bet.bet_type,
    game: card.game_key,
    dec_odds: bet.dec_odds||2,
    conf: bet.confidence||50,
    badge: bet.safety_label||'MODERATE',
  }).replace(/"/g,'&quot;');
  return `<div class="bet-row">
    <span class="bet-label">${label}</span>
    <span class="bet-pick">${pick}</span>
    <span class="bet-odds">${odds}</span>
    ${edge}
    ${sb}
    <button class="bet-add" onclick="addToParlay(${addKey})">+</button>
  </div>`;
}

function propsSection(c) {
  const hp = c.home_props||[];
  const ap = c.away_props||[];
  if (!hp.length && !ap.length) return '';
  let html = '<div class="props-section">';
  if (ap.length) {
    html += `<div class="props-team-header">${c.away_team}</div>`;
    html += ap.map(p => propRow(p, c.game_key)).join('');
  }
  if (hp.length) {
    html += `<div class="props-team-header">${c.home_team}</div>`;
    html += hp.map(p => propRow(p, c.game_key)).join('');
  }
  html += '</div>';
  return html;
}

function propRow(p, gameKey) {
  const dirCls = `prop-dir ${p.direction}`;
  const sb     = safetyBadge(p.safety_label||'MODERATE');
  const addKey = JSON.stringify({
    label: `${p.name} ${p.direction} ${p.line} ${p.prop_label}`,
    bet_type: 'player_prop',
    game: gameKey||p.game_key||'',
    dec_odds: p.dec_odds||1.9,
    conf: p.confidence||50,
    badge: p.safety_label||'MODERATE',
  }).replace(/"/g,'&quot;');
  return `<div class="prop-row">
    <span class="prop-player">${p.name||'—'}</span>
    <span class="prop-stat">${p.prop_label||p.stat_type||'—'}</span>
    <span class="${dirCls}">${p.direction} ${p.line}</span>
    <span class="prop-conf">${p.confidence||'?'}%</span>
    ${sb}
    <button class="bet-add" onclick="addToParlay(${addKey})">+</button>
  </div>`;
}

function toggleCard(cardEl) {
  cardEl.classList.toggle('expanded');
}

// ── Props table ───────────────────────────────────────────────────────────
let _filteredProps = [];

function renderPropsTable(props) {
  ALL_PROPS = props || [];
  filterProps();
}

function filterProps() {
  const q     = (document.getElementById('props-search').value||'').toLowerCase();
  const stat  = document.getElementById('props-stat').value;
  const dir   = document.getElementById('props-dir').value;
  const safe  = document.getElementById('props-safety').value;
  _filteredProps = ALL_PROPS.filter(p => {
    if (q && !(p.name||'').toLowerCase().includes(q) && !(p.team||'').toLowerCase().includes(q)) return false;
    if (stat && p.stat_type !== stat) return false;
    if (dir && p.direction !== dir) return false;
    if (safe && p.safety_label !== safe) return false;
    return true;
  });
  sortAndRenderProps();
}

function sortProps(key) {
  if (propsSortKey === key) propsSortAsc = !propsSortAsc;
  else { propsSortKey = key; propsSortAsc = false; }
  sortAndRenderProps();
}

function sortAndRenderProps() {
  const sorted = [..._filteredProps].sort((a,b)=>{
    const va = a[propsSortKey]||''; const vb = b[propsSortKey]||'';
    const cmp = typeof va === 'number' ? va-vb : String(va).localeCompare(String(vb));
    return propsSortAsc ? cmp : -cmp;
  });
  const tbody = document.getElementById('props-tbody');
  if (!sorted.length) {
    tbody.innerHTML = '<tr><td colspan="9" style="text-align:center;color:var(--muted);padding:24px">No props match filter.</td></tr>';
    document.getElementById('props-meta').textContent = '';
    return;
  }
  document.getElementById('props-meta').textContent = `${sorted.length} props`;
  tbody.innerHTML = sorted.map(p => {
    const addKey = JSON.stringify({
      label: `${p.name} ${p.direction} ${p.line} ${p.prop_label}`,
      bet_type: 'player_prop',
      game: p.game_key||p.game||'',
      dec_odds: p.dec_odds||1.9,
      conf: p.confidence||50,
      badge: p.safety_label||'MODERATE',
    }).replace(/"/g,'&quot;');
    return `<tr>
      <td>${p.name||'—'}</td>
      <td>${p.team||'—'}</td>
      <td>${p.prop_label||p.stat_type||'—'}</td>
      <td>${p.line!=null?p.line:'—'}</td>
      <td class="${dirClass(p.direction)}">${p.direction||'—'}</td>
      <td>${p.model_prob!=null?Math.round(p.model_prob*100)+'%':'—'}</td>
      <td>${safetyBadge(p.safety_label||'MODERATE')}</td>
      <td style="color:${(p.ev||0)>=0?'var(--green)':'var(--red)'}">${p.ev!=null?p.ev.toFixed(3):'—'}</td>
      <td><button class="bet-add" onclick="addToParlay(${addKey})">+</button></td>
    </tr>`;
  }).join('');
}

// ── Parlays ───────────────────────────────────────────────────────────────
function renderParlays(parlays) {
  BEST_PARLAYS = parlays || [];
  const grid  = document.getElementById('auto-parlays-grid');
  const empty = document.getElementById('parlays-empty');
  if (!BEST_PARLAYS.length) {
    grid.innerHTML = '';
    empty.classList.remove('hidden');
    return;
  }
  empty.classList.add('hidden');
  grid.innerHTML = BEST_PARLAYS.map(p => {
    const legs = (p.legs||[]).map(l=>
      `<div class="parlay-leg">
        <div class="leg-dot"></div>
        <span class="leg-text">${l.label||'—'}</span>
        <span class="leg-conf">${l.conf||'?'}% · x${(l.dec_odds||2).toFixed(2)}</span>
       </div>`
    ).join('');
    const saveData = JSON.stringify({
      name: `${p.n_legs||'?'}-Leg Auto Parlay`,
      legs: p.legs||[],
      combined_odds: p.combined_dec||0,
      stake_usd: 10,
    }).replace(/"/g,'&quot;');
    return `
<div class="parlay-card">
  <div class="parlay-header">
    <div class="parlay-meta">
      <span class="parlay-legs-count">${p.n_legs||'?'}-Leg Parlay · ${safetyBadge(p.safety_label||'MODERATE')}</span>
      <span class="parlay-odds">x${(p.combined_dec||0).toFixed(2)}</span>
      <span class="parlay-payout">$100 → $${(p.payout_100||0).toFixed(0)}</span>
    </div>
    <span style="font-size:.78rem;color:var(--muted)">Hit: ${p.combined_prob||'?'}%</span>
  </div>
  ${legs}
  <button class="parlay-save-btn" onclick="saveAutoParlay(${saveData})">&#128190; Save Parlay</button>
</div>`;
  }).join('');
}

// Parlay builder
function addToParlay(leg) {
  if (parlayLegs.length >= 12) { toast('Max 12 legs','info'); return; }
  if (parlayLegs.find(l=>l.label===leg.label)) { toast('Already added','info'); return; }
  parlayLegs.push(leg);
  renderParlayBuilder();
  toast(`Added: ${leg.label}`,'success');
}

function renderParlayBuilder() {
  const list = document.getElementById('parlay-legs-list');
  const odds = document.getElementById('parlay-combined-odds');
  if (!parlayLegs.length) {
    list.innerHTML = '<div style="color:var(--muted);font-size:.82rem">No legs added yet.</div>';
    odds.textContent = '';
    return;
  }
  list.innerHTML = parlayLegs.map((l,i)=>
    `<div class="parlay-leg-item">
      <span>${l.label||'—'} <span style="color:var(--muted);font-size:.72rem">x${(l.dec_odds||2).toFixed(2)}</span></span>
      <button class="remove-leg" onclick="removeLeg(${i})">✕</button>
    </div>`
  ).join('');
  const combined = parlayLegs.reduce((acc,l)=>acc*(l.dec_odds||2), 1);
  odds.textContent = `Combined: x${combined.toFixed(2)}`;
}

function removeLeg(i) {
  parlayLegs.splice(i, 1);
  renderParlayBuilder();
}
function clearParlay() { parlayLegs=[]; renderParlayBuilder(); }

function saveParlay() {
  if (!parlayLegs.length) { toast('Add legs first','error'); return; }
  const combined = parlayLegs.reduce((acc,l)=>acc*(l.dec_odds||2),1);
  const name  = document.getElementById('parlay-name').value.trim() || 'My Parlay';
  const stake = parseFloat(document.getElementById('parlay-stake').value) || 0;
  fetch('/api/parlay/save',{
    method:'POST',
    headers:{'Content-Type':'application/json'},
    body:JSON.stringify({name, legs:parlayLegs, combined_odds:combined, stake_usd:stake})
  }).then(r=>r.json()).then(d=>{
    if(d.ok){toast('Parlay saved!','success');clearParlay();loadTrackedParlays();}
    else toast(d.error||'Error saving','error');
  });
}

function saveAutoParlay(data) {
  fetch('/api/parlay/save',{
    method:'POST',
    headers:{'Content-Type':'application/json'},
    body:JSON.stringify(data)
  }).then(r=>r.json()).then(d=>{
    if(d.ok){toast('Parlay saved!','success');loadTrackedParlays();switchTab('parlays');}
    else toast(d.error||'Error','error');
  });
}

function loadTrackedParlays() {
  fetch('/api/parlay/list').then(r=>r.json()).then(d=>{
    const el = document.getElementById('tracked-parlays-list');
    if (!d.ok || !d.parlays || !d.parlays.length) {
      el.innerHTML = '<div class="no-games">No tracked parlays yet.</div>';
      return;
    }
    el.innerHTML = d.parlays.map(p=>{
      const legs = (p.legs_json||[]).map(l=>
        `<div style="font-size:.78rem;color:var(--muted);padding:3px 0">&bull; ${l.label||JSON.stringify(l)}</div>`
      ).join('');
      return `<div class="tracked-parlay">
        <div class="tracked-parlay-header">
          <span class="tp-name">${p.name||'Parlay #'+p.id}</span>
          <span class="tp-outcome tp-${p.outcome||'PENDING'}">${p.outcome||'PENDING'}</span>
        </div>
        <div class="tp-meta">x${(p.combined_odds||0).toFixed(2)} · $${p.stake_usd||0} stake · ${p.created_at||''}</div>
        ${legs}
      </div>`;
    }).join('');
  });
}

// ── Performance ───────────────────────────────────────────────────────────
function loadPerformance() {
  fetch('/api/performance').then(r=>r.json()).then(d=>{
    if (!d.ok) { return; }
    const s = d.stats||{};
    document.getElementById('perf-stats').innerHTML = `
      <div class="perf-card"><div class="perf-val perf-win">${s.wins||0}</div><div class="perf-label">Wins</div></div>
      <div class="perf-card"><div class="perf-val perf-loss">${s.losses||0}</div><div class="perf-label">Losses</div></div>
      <div class="perf-card"><div class="perf-val perf-pend">${s.pushes||0}</div><div class="perf-label">Pushes</div></div>
      <div class="perf-card"><div class="perf-val perf-pend">${s.pending||0}</div><div class="perf-label">Pending</div></div>
      <div class="perf-card"><div class="perf-val perf-rate">${s.hit_rate!=null?Math.round(s.hit_rate*100)+'%':'—'}</div><div class="perf-label">Hit Rate</div></div>
      <div class="perf-card"><div class="perf-val" style="font-size:1.2rem">${s.total||0}</div><div class="perf-label">Total Bets</div></div>
    `;
    const byType = s.by_bet_type||{};
    const tbody = document.getElementById('by-type-tbody');
    tbody.innerHTML = Object.entries(byType).map(([bt,v])=>`
      <tr>
        <td>${bt.replace(/_/g,' ')}</td>
        <td style="color:var(--green)">${v.wins||0}</td>
        <td style="color:var(--red)">${v.losses||0}</td>
        <td>${v.pushes||0}</td>
        <td style="color:var(--yellow)">${v.pending||0}</td>
        <td style="color:var(--accent)">${v.wins&&(v.wins+v.losses)?Math.round(v.wins/(v.wins+v.losses)*100)+'%':'—'}</td>
      </tr>`
    ).join('') || '<tr><td colspan="6" style="text-align:center;color:var(--muted);padding:16px">No data yet.</td></tr>';
  });
  loadHistory();
}

function loadHistory() {
  const outcome = document.getElementById('pred-filter-outcome').value;
  let url = '/api/predictions?days=30';
  if (outcome) url += `&outcome=${outcome}`;
  fetch(url).then(r=>r.json()).then(d=>{
    const el = document.getElementById('pred-history-list');
    if (!d.ok || !d.predictions || !d.predictions.length) {
      el.innerHTML = '<div class="no-games">No predictions found.</div>';
      return;
    }
    el.innerHTML = d.predictions.slice(0,100).map(p=>`
      <div class="pred-row">
        <span class="pred-outcome pred-${p.outcome||'PENDING'}">${p.outcome||'PENDING'}</span>
        <span class="pred-pick">${p.pick||'—'}</span>
        <span class="pred-meta">${p.bet_type||''} · ${p.game_date||''}</span>
        <span class="pred-meta">${p.confidence?p.confidence+'% conf':''}</span>
      </div>`
    ).join('');
  });
}

function resolveOutcomes() {
  fetch('/api/resolve-outcomes',{method:'POST'}).then(r=>r.json()).then(d=>{
    toast(d.msg||'Resolving…','info');
    setTimeout(loadPerformance, 4000);
  });
}

// ── Phone / SMS ───────────────────────────────────────────────────────────
function openPhoneModal() {
  document.getElementById('phone-modal').classList.remove('hidden');
  loadPhones();
}
function closePhoneModal() {
  document.getElementById('phone-modal').classList.add('hidden');
}

function loadPhones() {
  fetch('/api/phone-numbers').then(r=>r.json()).then(d=>{
    const list = document.getElementById('phone-list');
    if (!d.numbers || !d.numbers.length) {
      list.innerHTML = '<div style="color:var(--muted);font-size:.82rem">No numbers saved.</div>';
      return;
    }
    list.innerHTML = d.numbers.map(n=>`
      <div class="phone-item">
        <span>${n.phone_number||n.phone||n} ${n.label?'('+n.label+')':''}</span>
        <button class="phone-remove" onclick="removePhone('${n.phone_number||n.phone||n}')">Remove</button>
      </div>`
    ).join('');
  });
}

function addPhone() {
  const phone = document.getElementById('new-phone').value.trim();
  const label = document.getElementById('new-phone-label').value.trim();
  if (!phone) { toast('Enter a phone number','error'); return; }
  fetch('/api/phone-numbers/add',{
    method:'POST', headers:{'Content-Type':'application/json'},
    body:JSON.stringify({phone, label})
  }).then(r=>r.json()).then(d=>{
    if (d.ok) { toast('Number added','success'); loadPhones();
      document.getElementById('new-phone').value='';
    } else toast(d.msg||d.error||'Error','error');
  });
}

function removePhone(phone) {
  fetch('/api/phone-numbers/remove',{
    method:'POST', headers:{'Content-Type':'application/json'},
    body:JSON.stringify({phone})
  }).then(r=>r.json()).then(d=>{
    if (d.ok) { toast('Removed','success'); loadPhones(); }
    else toast('Error removing','error');
  });
}

function sendSms() {
  fetch('/api/sms/send',{method:'POST'}).then(r=>r.json()).then(d=>{
    toast(d.ok ? 'SMS blast sent!' : (d.error||'Error sending'), d.ok?'success':'error');
  });
}

// ── Toast ─────────────────────────────────────────────────────────────────
let _toastId = null;
function toast(msg, type='info') {
  const el = document.getElementById('toast');
  el.textContent = msg;
  el.className   = `show ${type}`;
  if (_toastId) clearTimeout(_toastId);
  _toastId = setTimeout(()=>{ el.className=''; }, 3000);
}

// ── Init ──────────────────────────────────────────────────────────────────
(function init() {
  // If data was injected server-side, render immediately
  if ((TODAY_CARDS && TODAY_CARDS.length) ||
      (TOMORROW_CARDS && TOMORROW_CARDS.length) ||
      (ALL_PROPS && ALL_PROPS.length)) {
    renderAll();
    hideOverlay();
  } else {
    // Poll status — if running, show progress; else try to load cache
    fetch('/api/status').then(r=>r.json()).then(d=>{
      if (d.status === 'running') {
        startPolling();
      } else {
        // Load from cache (might already be in state from server)
        loadCachedState();
        hideOverlay();
      }
    }).catch(()=>{ hideOverlay(); });
  }
})();
</script>
</body>
</html>"""

dst = os.path.join(os.path.dirname(__file__), '..', 'src', 'templates', 'dashboard.html')
dst = os.path.normpath(dst)
with open(dst, 'w', encoding='utf-8') as f:
    f.write(HTML)
print(f"Written {len(HTML)} chars to {dst}")
