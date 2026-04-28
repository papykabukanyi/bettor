
// ── Injected data ──────────────────────────────────────────────────────────
let TODAY_CARDS    = [];
let TOMORROW_CARDS = [];
let BEST_PARLAYS   = [];
let ALL_PROPS      = [];
let AUTO_PROP_PARLAY = null;
let BEST_LEGS      = [];
let AUTO_MIX_LEGS  = [];
let parlayLegs     = [];
let _parlayTouched = false;
let propsSortKey   = 'model_prob';
let propsSortAsc   = false;
const LOCAL_CACHE_KEY = 'bettor_last_session';
let _cacheUpdatedAtMs = null;
let _autoRefreshInFlight = false;

// ── SSE live connection ────────────────────────────────────────────────────
let _sse = null;
let _sseReconnectDelay = 3000;

function _connectSSE() {
  if (_sse) { try { _sse.close(); } catch(e){} }
  _sse = new EventSource('/api/stream');

  _sse.onopen = () => {
    _sseReconnectDelay = 3000;
    _updateConnBadge(true);
  };

  // Full state push (on connect + after every analysis run)
  _sse.addEventListener('state_update', e => {
    try {
      const d = JSON.parse(e.data);
      if (d.status === 'running') {
        showOverlay(d.phase || 'Working…', 5);
        return;
      }
      hideOverlay();
      if (d.game_cards_today  !== undefined) applyCachedData(d);
      if (d.live_scores       !== undefined) _applyLiveScores(d.live_scores);
      saveLocalCache(d);
      // Refresh secondary panels silently
      _silentRefreshPanels();
    } catch(err) { console.warn('SSE state_update parse error', err); }
  });

  // Live score ticks (every 90 s from backend)
  _sse.addEventListener('live_scores', e => {
    try {
      const d = JSON.parse(e.data);
      _applyLiveScores(d.scores || {});
    } catch(err) {}
  });

  // Status-only events (running / error)
  _sse.addEventListener('status', e => {
    try {
      const d = JSON.parse(e.data);
      if (d.status === 'running') showOverlay(d.phase || 'Running…', 5);
      if (d.status === 'error')   { hideOverlay(); toast('Analysis error — check logs', 'error'); }
    } catch(err) {}
  });

  // Performance push after auto-resolve
  _sse.addEventListener('performance_update', e => {
    try {
      const d = JSON.parse(e.data);
      if (d.stats)        _renderBotPerf(d.stats);
      if (d.parlay_stats) _renderParlayPerf(d.parlay_stats);
    } catch(err) {}
  });

  _sse.onerror = () => {
    _updateConnBadge(false);
    _sse.close();
    setTimeout(_connectSSE, _sseReconnectDelay);
    _sseReconnectDelay = Math.min(_sseReconnectDelay * 1.5, 30000);
  };
}

function _updateConnBadge(live) {
  const el = document.getElementById('conn-badge');
  if (!el) return;
  el.textContent = live ? '● LIVE' : '○ offline';
  el.style.color = live ? 'var(--green)' : 'var(--red)';
}

// ── Loading / status overlay ───────────────────────────────────────────────
const PHASES = [];
let pollingId = null;

function hideOverlay() {
  document.getElementById('loading-overlay').classList.add('hidden');
}
function showOverlay(phase, pct) {
  document.getElementById('loading-overlay').classList.remove('hidden');
  document.getElementById('load-phase').textContent = phase || 'Working…';
  document.getElementById('progress-fill').style.width = (pct||5)+'%';
}

/* Legacy polling kept for manual "Run" button fallback */
function startPolling() {
  if (pollingId) return;
  pollingId = setInterval(pollStatus, 2500);
}
function stopPolling() {
  clearInterval(pollingId); pollingId = null;
  stopConsolePoll();
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
    }
  }).catch(()=>{ stopPolling(); hideOverlay(); });
}

function runAnalysis() {
  openConsole();
  startConsolePoll();
  fetch('/api/run', {method:'POST'}).then(r=>r.json()).then(d=>{
    if (d.ok) { showOverlay('Starting analysis…',2); startPolling(); }
    else toast(d.msg||'Already running','info');
  });
}

function saveLocalCache(data) {
  try {
    const payload = {
      saved_at: Date.now(),
      data: {
        game_cards_today:     data.game_cards_today    || [],
        game_cards_tomorrow:  data.game_cards_tomorrow || [],
        best_parlays:         data.best_parlays        || [],
        player_props:         data.player_props        || [],
        last_updated:         data.last_updated        || '',
      }
    };
    localStorage.setItem(LOCAL_CACHE_KEY, JSON.stringify(payload));
  } catch (e) {}
}

function loadLocalCache() {
  try {
    const raw = localStorage.getItem(LOCAL_CACHE_KEY);
    if (!raw) return null;
    const obj = JSON.parse(raw);
    return obj && obj.data ? obj.data : null;
  } catch (e) { return null; }
}

function applyCachedData(d) {
  if (!d) return;
  if (d.game_cards_today    !== undefined) TODAY_CARDS    = d.game_cards_today    || [];
  if (d.game_cards_tomorrow !== undefined) TOMORROW_CARDS = d.game_cards_tomorrow || [];
  if (d.best_parlays        !== undefined) BEST_PARLAYS   = d.best_parlays        || [];
  if (d.player_props        !== undefined) ALL_PROPS      = d.player_props        || [];
  renderAll();
  const lu = d.last_updated || d._updated_at;
  if (lu) document.getElementById('last-updated').textContent = '⟳ ' + lu;
}

// ── Live score injection ────────────────────────────────────────────────────
let _liveScores = {};

function _norm(s) {
  return (s||'').replace(/ @ /g,'@').replace(/ @/g,'@').replace(/@ /g,'@').trim();
}

function _applyLiveScores(scores) {
  _liveScores = Object.assign(_liveScores, scores);
  // Update all game cards on screen
  document.querySelectorAll('[data-game-key]').forEach(card => {
    const gk = _norm(card.dataset.gameKey);
    const s  = _liveScores[gk];
    if (!s) return;
    const scoreEl  = card.querySelector('.live-score');
    const statusEl = card.querySelector('.live-status');
    if (scoreEl && s.home_score != null && s.away_score != null) {
      scoreEl.textContent  = `${s.away_score} – ${s.home_score}`;
      scoreEl.style.display = 'inline';
    }
    if (statusEl) {
      const inn = s.inning ? ` • ${s.inning_half||''} ${s.inning}` : '';
      statusEl.textContent  = `${s.status}${inn}`;
      statusEl.style.display = 'inline';
      statusEl.className    = 'live-status ' +
        (s.status === 'Final' || s.status === 'Game Over' ? 'final' : 'in-progress');
    }
  });
}

// ── Silent background panel refresh (called after state_update) ─────────────
let _silentPerfTimer = null;
function _silentRefreshPanels() {
  // Stagger to avoid hammering the server
  clearTimeout(_silentPerfTimer);
  _silentPerfTimer = setTimeout(() => {
    _fetchAndRenderPerf();
    _fetchAndRenderParlayPerf();
    loadTrackedParlays();
    autoGenParlaysFromProps();
  }, 1500);
}

// ── Periodic silent refresh every 5 minutes (covers SSE gaps) ───────────────
setInterval(() => {
  _fetchAndRenderPerf();
  _fetchAndRenderParlayPerf();
  loadTrackedParlays();
}, 5 * 60 * 1000);

// ── Performance render wrappers (called by SSE performance_update) ──────────
function _renderBotPerf(_stats) {
  // Stats from SSE; just re-fetch to keep rendering in sync with existing code
  loadBotPerformance();
}
function _renderParlayPerf(_stats) {
  loadParlayPerformance();
}
function _fetchAndRenderPerf()     { loadBotPerformance(); loadPerformance(); }
function _fetchAndRenderParlayPerf() { loadParlayPerformance(); }

// ── Tab switching ─────────────────────────────────────────────────────────
function switchTab(name) {
  document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));
  document.querySelectorAll('.tab-panel').forEach(p=>p.classList.remove('active'));
  document.querySelector(`[data-tab="${name}"]`).classList.add('active');
  document.getElementById(`panel-${name}`).classList.add('active');
  if (name === 'performance') { loadBotPerformance(); loadPerformance(); loadCalibration(); }
  if (name === 'parlays')     { autoGenParlaysFromProps(); loadTrackedParlays(); loadParlayPerformance(); }
}

// ── Elite Parlay ──────────────────────────────────────────────────────────
function buildEliteParlay() {
  const btn = document.getElementById('btn-elite-parlay');
  if (btn) { btn.disabled = true; btn.textContent = '⚡ Building…'; }
  fetch('/api/parlay/build-elite', {method: 'POST'})
    .then(r => r.json())
    .then(d => {
      if (btn) { btn.disabled = false; btn.innerHTML = '&#9889; Elite Parlay'; }
      const el = document.getElementById('elite-parlay-result');
      if (!d.ok || !d.parlay) {
        el.innerHTML = `<div class="no-games" style="color:var(--yellow);margin:8px 0">
          &#9888; ${d.msg || d.error || 'No elite picks available right now'}</div>`;
        return;
      }
      const p = d.parlay;
      const evColor = (p.combined_ev || 0) > 0 ? 'var(--green)' : 'var(--red)';
      const legs = (p.legs || []).map(l =>
        `<div style="display:flex;align-items:center;gap:8px;padding:7px 0;
              border-bottom:1px solid var(--border);font-size:.82rem">
          <span class="safety-badge badge-ELITE" style="font-size:.65rem;padding:2px 6px">ELITE</span>
          <span style="flex:1">${l.label || l.pick || ''}</span>
          <span style="color:var(--accent);font-weight:600">${((l.model_prob||0)*100).toFixed(1)}%</span>
          <span style="color:var(--muted)">x${l.dec_odds||'?'}</span>
        </div>`
      ).join('');
      el.innerHTML = `<div style="border:1px solid var(--green);border-radius:var(--radius);
            background:rgba(16,185,129,.08);padding:14px;margin-top:6px">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">
          <span style="font-weight:700;color:var(--green)">&#9889; ${p.name || 'Elite Parlay'}</span>
          <div style="text-align:right">
            <div style="font-size:1.1rem;font-weight:700">x${p.combined_dec}</div>
            <div style="font-size:.78rem;color:${evColor}">EV ${((p.combined_ev||0)*100).toFixed(1)}%</div>
          </div>
        </div>
        ${legs}
        <div style="font-size:.75rem;color:var(--muted);margin-top:10px">
          ${p.n_legs} legs · ${p.combined_prob}% combined · $100 → $${p.payout_100} · ✅ Auto-saved to Tracked Parlays
        </div>
      </div>`;
      toast('Elite parlay built & auto-saved!', 'success');
      setTimeout(() => { loadTrackedParlays(); loadParlayPerformance(); }, 800);
    })
    .catch(e => {
      if (btn) { btn.disabled = false; btn.innerHTML = '&#9889; Elite Parlay'; }
      toast('Error: ' + e, 'error');
    });
}

// ── Parlay Performance ────────────────────────────────────────────────────
function loadParlayPerformance() {
  fetch('/api/parlay/performance').then(r => r.json()).then(d => {
    const targets = [
      document.getElementById('parlay-perf-stats'),
      document.getElementById('parlay-perf-stats-perf')
    ].filter(Boolean);
    if (!targets.length) return;
    if (!d.ok) {
      targets.forEach(el => el.innerHTML = '<div class="no-games">Could not load parlay stats.</div>');
      return;
    }
    const s = d.stats || {};
    const hr = s.hit_rate != null ? s.hit_rate + '%' : '—';
    const roi = s.roi != null ? s.roi + '%' : '—';
    const hrC  = s.hit_rate >= 55 ? 'var(--green)' : s.hit_rate >= 45 ? 'var(--yellow)' : 'var(--red)';
    const roiC = (s.roi || 0) >= 0 ? 'var(--green)' : 'var(--red)';
    const html = `
      <div class="perf-card"><div class="perf-val perf-win">${s.wins||0}</div><div class="perf-label">Parlay Wins</div></div>
      <div class="perf-card"><div class="perf-val perf-loss">${s.losses||0}</div><div class="perf-label">Parlay Losses</div></div>
      <div class="perf-card"><div class="perf-val" style="color:var(--yellow)">${s.pending||0}</div><div class="perf-label">Pending</div></div>
      <div class="perf-card"><div class="perf-val" style="color:${hrC}">${hr}</div><div class="perf-label">Hit Rate</div></div>
      <div class="perf-card"><div class="perf-val" style="color:${roiC}">${roi}</div><div class="perf-label">ROI</div></div>
      <div class="perf-card"><div class="perf-val">${s.total||0}</div><div class="perf-label">Total Parlays</div></div>`;
    targets.forEach(el => { el.innerHTML = html; });
  });
}

// ── Calibration ───────────────────────────────────────────────────────────
function loadCalibration() {
  fetch('/api/calibration').then(r => r.json()).then(d => {
    if (!d.ok) return;
    const cal = d.calibration || {};
    const ece = cal.ece;
    const badge = document.getElementById('ece-badge');
    if (badge) {
      const c = ece == null ? 'var(--card2)' : ece < 0.05 ? 'var(--green)' : ece < 0.10 ? 'var(--yellow)' : 'var(--red)';
      badge.textContent = `ECE: ${ece != null ? (ece * 100).toFixed(1) + '%' : '—'}`;
      badge.style.background = c;
      badge.style.color = ece == null ? 'var(--text)' : '#fff';
    }
    const tbody = document.getElementById('calibration-tbody');
    if (!tbody) return;
    const bins = cal.bins || [];
    if (!bins.length) {
      tbody.innerHTML = '<tr><td colspan="5" style="text-align:center;padding:20px;color:var(--muted)">No resolved predictions yet.</td></tr>';
      return;
    }
    tbody.innerHTML = bins.map(b => {
      const gc = b.gap < 0.05 ? 'var(--green)' : b.gap < 0.10 ? 'var(--yellow)' : 'var(--red)';
      return `<tr>
        <td>${b.bin}</td>
        <td>${b.n}</td>
        <td>${(b.avg_pred * 100).toFixed(1)}%</td>
        <td>${(b.avg_actual * 100).toFixed(1)}%</td>
        <td style="color:${gc};font-weight:700">${(b.gap * 100).toFixed(1)}%</td>
      </tr>`;
    }).join('');
  }).catch(() => {});
}

function triggerAutoImprove() {
  toast('Triggering auto-improve check…', 'info');
  fetch('/api/auto-improve', {method: 'POST'})
    .then(r => r.json())
    .then(d => {
      if (d.retrained) {
        toast('Model retrained! ECE was high — refreshing calibration…', 'success');
      } else {
        toast(d.msg || 'Auto-improve complete', 'info');
      }
      setTimeout(loadCalibration, 1500);
    })
    .catch(e => toast('Auto-improve error: ' + e, 'error'));
}

function triggerBackfill(days) {
  days = days || 3;
  toast(`Backfilling last ${days} days of data and retraining model…`, 'info');
  fetch('/api/backfill', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({days_back: days})
  })
    .then(r => r.json())
    .then(d => {
      if (d.ok) {
        toast(d.msg || 'Backfill started — check Console for progress', 'success');
        // Open console so user can watch progress
        const con = document.getElementById('live-console');
        if (con && con.style.display === 'none') toggleConsole();
        // Poll until idle
        const poll = setInterval(() => {
          fetch('/api/status').then(r=>r.json()).then(s => {
            if (s.status !== 'running') {
              clearInterval(poll);
              toast('Backfill complete!', 'success');
            }
          });
        }, 3000);
      } else {
        toast(d.msg || 'Backfill error', 'error');
      }
    })
    .catch(e => toast('Backfill error: ' + e, 'error'));
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
function cleanPickTeam(pick) {
  return (pick||'').replace(/\s+ML$/,'').trim();
}

// ── Render all ────────────────────────────────────────────────────────────
function renderAll() {
  renderGames('today-grid', TODAY_CARDS, 'today-empty', 'today-meta', 'badge-today');
  renderGames('tomorrow-grid', TOMORROW_CARDS, 'tomorrow-empty', 'tomorrow-meta', 'badge-tomorrow');
  renderPropsTable(ALL_PROPS);
  autoGenParlaysFromProps();
}

// ── Game cards ────────────────────────────────────────────────────────────
function renderGames(gridId, cards, emptyId, metaId, badgeId) {
  const grid = document.getElementById(gridId);
  const empty = document.getElementById(emptyId);
  const meta  = document.getElementById(metaId);
  const badge = document.getElementById(badgeId);
  if (!cards || !cards.length) {
    if (grid) grid.innerHTML = '';
    if (empty) empty.classList.remove('hidden');
    if (badge) badge.textContent = '0';
    return;
  }
  if (empty) empty.classList.add('hidden');
  if (badge) badge.textContent = String(cards.length);
  if (meta) meta.textContent = `${cards.length} game${cards.length!==1?'s':''} &middot; MLB`;
  if (grid) grid.innerHTML = cards.map(c => gameCard(c)).join('');
}

// ── Plain-English helpers ─────────────────────────────────────────────────
function plainEnglishBet(bet, homeTeam, awayTeam) {
  if (!bet) return null;
  const bt   = (bet.bet_type||'').toLowerCase();
  const pick = bet.pick || '';
  const line = parseFloat(bet.line) || 0;
  const isOver = pick.toUpperCase().includes('OVER');
  const team = cleanPickTeam(pick);
  if (bt === 'moneyline') return { text: `${team} will WIN`, emoji: '&#127942;', color: 'var(--green)' };
  if (bt === 'run_line') {
    if (pick.includes('+1.5')) return { text: `${team} wins or stays within 1 run`, emoji: '&#128202;', color: '#818cf8' };
    return { text: `${team} wins by 2+ runs`, emoji: '&#128202;', color: '#818cf8' };
  }
  if (bt === 'total' || bt === 'game_total') {
    return isOver
      ? { text: `Game will have MORE than ${line} total runs`, emoji: '&#127919;', color: 'var(--accent)' }
      : { text: `Game will have FEWER than ${line} total runs`, emoji: '&#127919;', color: 'var(--yellow)' };
  }
  if (bt === 'f5_total') {
    return isOver
      ? { text: `First 5 innings: MORE than ${line} runs`, emoji: '&#9203;', color: 'var(--accent)' }
      : { text: `First 5 innings: FEWER than ${line} runs`, emoji: '&#9203;', color: 'var(--yellow)' };
  }
  if (bt === 'f5_moneyline') return { text: `${team} leads after 5 innings`, emoji: '&#9203;', color: '#a78bfa' };
  if (bt === 'home_team_total') {
    return isOver
      ? { text: `${homeTeam} scores MORE than ${line} runs`, emoji: '&#127968;', color: 'var(--accent)' }
      : { text: `${homeTeam} scores fewer than ${line} runs`, emoji: '&#127968;', color: 'var(--yellow)' };
  }
  if (bt === 'away_team_total') {
    return isOver
      ? { text: `${awayTeam} scores MORE than ${line} runs`, emoji: '&#9992;', color: 'var(--accent)' }
      : { text: `${awayTeam} scores fewer than ${line} runs`, emoji: '&#9992;', color: 'var(--yellow)' };
  }
  return { text: pick, emoji: '&#127919;', color: 'var(--text)' };
}

function plainEnglishPropText(p) {
  const name = p.name || '—';
  const line = parseFloat(p.line) || 0;
  const st   = p.stat_type || '';
  if (st === 'strikeouts')        return { text: `${name} strikes out ${line}+ batters`,            emoji: '&#128293;', color: '#a78bfa' };
  if (st === 'hits')              return { text: `${name} gets ${line}+ hit${line!==1?'s':''}`,     emoji: '&#9918;',   color: 'var(--accent)' };
  if (st === 'home_runs')         return { text: line<=1?`${name} hits a Home Run`:`${name} hits ${line}+ Home Runs`, emoji: '&#128165;', color: 'var(--green)' };
  if (st === 'rbi')               return { text: `${name} drives in ${line}+ run${line!==1?'s':''}`,emoji: '&#127919;', color: 'var(--accent)' };
  if (st === 'runs')              return { text: `${name} scores ${line}+ run${line!==1?'s':''}`,   emoji: '&#127939;', color: 'var(--green)' };
  if (st === 'walks')             return { text: `${name} draws ${line}+ walk${line!==1?'s':''}`,   emoji: '&#128694;', color: '#94a3b8' };
  if (st === 'stolen_bases')      return { text: `${name} steals ${line}+ base${line!==1?'s':''}`,  emoji: '&#128168;', color: 'var(--yellow)' };
  if (st === 'total_bases')       return { text: `${name} has ${line}+ total bases`,                emoji: '&#128208;', color: 'var(--accent)' };
  if (st === 'batter_strikeouts') return { text: `${name} strikes out ${line}+ times as batter`,   emoji: '&#10060;',  color: 'var(--red)' };
  if (st === 'doubles')           return { text: `${name} hits ${line}+ double${line!==1?'s':''}`,  emoji: '&#128640;', color: 'var(--accent)' };
  return { text: `${name} OVER ${line} ${p.prop_label||st}`, emoji: '&#9650;', color: 'var(--accent)' };
}

function gameCard(c) {
  const slBadge = safetyBadge(c.overall_safety_label || 'MODERATE');
  const whenB   = whenBadge(c.when_label||'TODAY', c.when||'TODAY');

  // Main WIN pick — big coloured headline
  let mainPickHtml = '';
  if (c.moneyline) {
    const pbt  = plainEnglishBet(c.moneyline, c.home_team, c.away_team);
    const prob = Math.round((c.moneyline.model_prob||0.5)*100);
    const probColor = prob >= 65 ? 'var(--green)' : prob >= 55 ? 'var(--accent)' : 'var(--yellow)';
    mainPickHtml = `<div class="pick-card win-card">
      <div class="pick-headline" style="color:${pbt.color}">${pbt.emoji} ${pbt.text}</div>
      <div class="pick-conf">
        <span style="color:${probColor};font-weight:700">${prob}% confident</span>
        <span style="color:var(--muted)">&middot; ${fmtOdds(c.moneyline.odds_am)}</span>
        ${fmtEdge(c.moneyline.edge)}
        ${safetyBadge(c.moneyline.safety_label||'MODERATE')}
      </div>
    </div>`;
  } else {
    mainPickHtml = `<div class="pick-card" style="color:var(--muted);font-size:.82rem">No moneyline edge yet</div>`;
  }

  // Other predictions as plain-English chips
  const otherBets = [];
  for (const key of ['total','run_line','f5_moneyline','f5_total']) {
    if (c[key]) {
      const pbt  = plainEnglishBet(c[key], c.home_team, c.away_team);
      const prob = Math.round((c[key].model_prob||0.5)*100);
      otherBets.push(`<div class="plain-bet-chip" style="color:${pbt.color}">${pbt.emoji} ${pbt.text} <span style="color:var(--muted);font-size:.72rem">(${prob}%)</span></div>`);
    }
  }
  const otherHtml = otherBets.length ? `<div class="plain-bets-list">${otherBets.join('')}</div>` : '';

  // Top prop picks preview — plain English, no chip clutter
  const topProps = [...(c.home_props||[]), ...(c.away_props||[])]
    .filter(p => (p.direction||'').toUpperCase() === 'OVER')
    .filter(p => { const lv = _lineValue(p.line); return !(Number.isFinite(lv) && lv <= 0.5); })
    .sort((a,b) => (b.model_prob||0) - (a.model_prob||0))
    .slice(0, 3);
  const propsHtml = topProps.length ? `<div class="plain-props-preview">${topProps.map(p => {
    const ppt = plainEnglishPropText(p);
    const pct = Math.round((p.model_prob||0.5)*100);
    return `<div class="plain-prop-chip" style="color:${ppt.color}">${ppt.emoji} ${ppt.text} <span style="color:var(--muted);font-size:.7rem">${pct}%</span></div>`;
  }).join('')}</div>` : '';

  const propCount = [...(c.home_props||[]), ...(c.away_props||[])]
    .filter(p => (p.direction||'').toUpperCase() === 'OVER')
    .filter(p => { const lv = _lineValue(p.line); return !(Number.isFinite(lv) && lv <= 0.5); })
    .length;

  return `<div class="game-card" data-game-key="${c.game_key}" id="card-${c.game_key.replace(/[@\s]/g,'_')}">
  <div class="card-header" onclick="toggleCard(this.parentElement)">
    <div class="card-when">${whenB}${slBadge}</div>
    <div class="matchup">
      <div class="team-side">
        <div class="team-name">${c.away_team}</div>
        <div class="team-starter">&#9918; ${c.away_starter||'TBD'}</div>
      </div>
      <div class="vs">vs</div>
      <div class="team-side away">
        <div class="team-name">${c.home_team}</div>
        <div class="team-starter">&#9918; ${c.home_starter||'TBD'}</div>
      </div>
    </div>
    <div>
      <span class="live-score" style="display:none"></span>
      <span class="live-status" style="display:none"></span>
    </div>
    ${mainPickHtml}
    ${otherHtml}
    ${propsHtml}
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
  const bt = (bet.bet_type || '').toLowerCase();
  let pick = bet.pick || '—';
  if (bt.includes('moneyline')) {
    const team = cleanPickTeam(pick);
    pick = team ? `${team} will win` : pick;
  }
  const pickCls = bt.includes('moneyline') ? 'pick-phrase win' : 'pick-phrase';
  const odds = fmtOdds(bet.odds_am);
  const edge = fmtEdge(bet.edge);
  const sb   = safetyBadge(bet.safety_label||'MODERATE');
  return `<div class="bet-row">
    <span class="bet-label">${label}</span>
    <span class="bet-pick ${pickCls}">${pick}</span>
    <span class="bet-odds">${odds}</span>
    ${edge}
    ${sb}
  </div>`;
}

function propsSection(c) {
  // Show ALL OVER props — no probability or safety gate
  function qualifies(p) {
    if ((p.direction || '').toUpperCase() !== 'OVER') return false;
    const lv = _lineValue(p.line);
    if (Number.isFinite(lv) && lv <= 0.5) return false;
    return true;
  }
  const hp = (c.home_props || []).filter(qualifies).sort((a,b)=>(b.model_prob||0)-(a.model_prob||0));
  const ap = (c.away_props || []).filter(qualifies).sort((a,b)=>(b.model_prob||0)-(a.model_prob||0));
  if (!hp.length && !ap.length) return '';
  let html = '<div class="props-section">';
  html += `<div style="font-size:.68rem;color:var(--accent);font-weight:700;padding:4px 0 6px;letter-spacing:.04em;">&#9650; ALL OVER PROPS (sorted by probability)</div>`;
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
  const dirCls  = `prop-dir ${p.direction}`;
  const sb      = safetyBadge(p.safety_label||'MODERATE');
  const rationale = p.signal_rationale || '';
  const stat = (p.prop_label || p.stat_type || '').replace(/_/g,' ');
  const line = p.line != null ? `${p.line}+` : '';
  const phrase = `${p.name||'Player'} will have ${line} ${stat}`.replace(/\s+/g,' ').trim();
  return `<div class="prop-row">
    <span class="prop-player">${p.name||'—'}</span>
    <span class="prop-stat prop-phrase">${phrase}</span>
    <span class="${dirCls}">${p.direction} ${p.line}</span>
    <span class="prop-conf">${p.model_prob!=null?(p.model_prob*100).toFixed(1):(p.confidence||'?')}%</span>
    ${sb}
    ${rationale ? `<span class="prop-rationale" title="${rationale.replace(/"/g,"&quot;")}">&#128202; ${rationale}</span>` : ''}
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
  const dir   = document.getElementById('props-dir').value || 'OVER';
  const safe  = document.getElementById('props-safety').value;
  _filteredProps = ALL_PROPS.filter(p => {
    if (q && !(p.name||'').toLowerCase().includes(q) && !(p.team||'').toLowerCase().includes(q)) return false;
    if (stat && p.stat_type !== stat) return false;
    if ((p.direction||'').toUpperCase() !== dir.toUpperCase()) return false;
    if (safe && p.safety_label !== safe) return false;
    const lv = _lineValue(p.line);
    if (Number.isFinite(lv) && lv <= 0.5) return false;
    return true;
  });
  sortAndRenderProps();
}

// ── Auto Parlay Combos ────────────────────────────────────────────────────────────────────
function autoGenParlaysFromProps() {
  const qualified = (ALL_PROPS || []).filter(p => {
    if ((p.direction||'').toUpperCase() !== 'OVER') return false;
    const lv = _lineValue(p.line);
    if (Number.isFinite(lv) && lv <= 0.5) return false;
    return _legProb(p) >= 0.61;
  }).sort((a,b) => _legProb(b) - _legProb(a));

  const grid  = document.getElementById('parlay-combos-grid');
  const empty = document.getElementById('parlay-combos-empty');
  const meta  = document.getElementById('parlay-auto-meta');
  if (!grid) return;

  if (!qualified.length) {
    grid.innerHTML = '';
    if (empty) empty.classList.remove('hidden');
    if (meta) meta.textContent = 'No qualifying props yet — run analysis first.';
    return;
  }
  if (empty) empty.classList.add('hidden');

  const combos = [];
  const maxLegs = Math.min(5, qualified.length);
  for (let n = 2; n <= maxLegs; n++) {
    const legs = [];
    const usedGames   = new Set();
    const usedPlayers = new Set();
    for (let i = 0; i < qualified.length && legs.length < n; i++) {
      const p = qualified[i];
      const gk = p.game_key || p.game || '';
      if (usedGames.has(gk) && usedGames.size >= 2) continue;
      if (usedPlayers.has(p.name)) continue;
      legs.push(p);
      usedGames.add(gk);
      usedPlayers.add(p.name);
    }
    if (legs.length === n) {
      const prob = legs.reduce((acc,l) => acc * _legProb(l), 1);
      const dec  = legs.reduce((acc,l) => acc * (l.dec_odds || 1.9), 1);
      const amer = dec >= 2 ? Math.round((dec-1)*100) : Math.round(-100/(dec-1));
      combos.push({ legs, prob, dec, amer, n });
    }
  }

  if (!combos.length) {
    grid.innerHTML = '<div class="no-games" style="grid-column:1/-1">Not enough props to build combos yet.</div>';
    if (meta) meta.textContent = '';
    return;
  }
  if (meta) meta.textContent = `${combos.length} auto-combo${combos.length>1?'s':''} from ${qualified.length} props`;
  grid.innerHTML = combos.map(c => parlayComboCard(c)).join('');
}


function parlayComboCard(c) {
  const probPct  = (c.combinedProb * 100).toFixed(1);
  const payout   = (c.combinedDec * 100).toFixed(0);
  const evRaw    = _legEv(c.combinedProb, c.combinedDec);
  const evColor  = evRaw >= 0 ? 'var(--green)' : 'var(--yellow)';
  const legsHtml = c.legs.map(p => {
    const ppt = plainEnglishPropText(p);
    const pct = (_legProb(p) * 100).toFixed(1);
    const dec = (_legDec(p) || 1.9).toFixed(2);
    return `<div class="combo-leg">
      <div style="display:flex;justify-content:space-between;align-items:center;gap:8px">
        <span style="color:${ppt.color};font-weight:600">${ppt.emoji} ${ppt.text}</span>
        <span style="color:var(--muted);font-size:.72rem;white-space:nowrap">${pct}% &middot; x${dec}</span>
      </div>
    </div>`;
  }).join('');
  const saveData = JSON.stringify({
    name: `Auto ${c.n_legs}-Leg Props Parlay`,
    legs: c.legs.map(p => {
      const ppt = plainEnglishPropText(p);
      return {
        label:    ppt.text,
        bet_type: 'player_prop',
        game:     p.game_key || p.game || '',
        dec_odds: _legDec(p) || 1.9,
        conf:     Math.round(_legProb(p) * 100),
        badge:    p.safety_label || 'MODERATE',
      };
    }),
    combined_odds: c.combinedDec,
    stake_usd: 10,
  }).replace(/"/g,'&quot;');
  return `<div class="parlay-combo-card">
  <div class="combo-header">
    <div>
      <div class="combo-title">&#9650; ${c.n_legs}-Leg OVER Parlay</div>
      <div class="combo-prob">Hit chance: ${probPct}% &middot; <span style="color:${evColor}">EV ${(evRaw*100).toFixed(1)}%</span></div>
    </div>
    <div style="text-align:right">
      <div class="combo-odds">x${c.combinedDec.toFixed(2)}</div>
      <div style="font-size:.75rem;color:var(--muted)">$100 &#8594; $${payout}</div>
    </div>
  </div>
  <div class="combo-legs">${legsHtml}</div>
  <div class="combo-actions">
    <button class="btn btn-primary btn-sm" style="flex:1" onclick="saveAutoParlay(${saveData})">&#128190; Save &amp; Track</button>
    <button class="btn btn-sm" style="flex:1" onclick="sendParlaySms(${saveData})">&#128241; SMS</button>
  </div>
</div>`;
}

// Legacy renderParlays (no-op, parlays now come from props only)
function renderParlays(parlays) {
  BEST_PARLAYS = parlays || [];
  autoGenParlaysFromProps();
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
function _legProb(l) {
  if (l == null) return 0;
  if (l.model_prob != null) return Number(l.model_prob) || 0;
  if (l.conf != null) return (Number(l.conf) || 0) / 100;
  if (l.confidence != null) return (Number(l.confidence) || 0) / 100;
  return 0;
}
function _legDec(l) {
  return l && l.dec_odds != null ? Number(l.dec_odds) : 2.0;
}
function _legEv(prob, dec) {
  const p = Number(prob) || 0;
  const d = Number(dec) || 2.0;
  return (d - 1) * p - (1 - p);
}

function buildLegPool() {
  const map = new Map();
  const addLeg = (leg) => {
    const key = `${leg.label}|${leg.game||''}`;
    const prev = map.get(key);
    if (!prev || leg.ev > prev.ev || (leg.ev === prev.ev && leg.model_prob > prev.model_prob)) {
      map.set(key, leg);
    }
  };

  // Player props (OVER only)
  (ALL_PROPS || []).forEach(p => {
    if ((p.direction || '').toUpperCase() !== 'OVER') return;
    const lv = _lineValue(p.line);
    if (Number.isFinite(lv) && lv <= 0.5) return;
    const prob = _legProb(p);
    const dec  = _legDec(p) || 1.9;
    const ev   = p.ev != null ? Number(p.ev) : _legEv(prob, dec);
    const label = `${p.name} ${p.direction} ${p.line} ${p.prop_label}`.trim();
    if (!label || !p.name) return;
    addLeg({
      label,
      game: p.game_key || p.game || '',
      bet_type: 'player_prop',
      dec_odds: dec,
      conf: p.confidence || Math.round(prob * 100),
      model_prob: prob,
      ev,
      badge: p.safety_label || 'MODERATE',
      source: 'prop',
    });
  });

  // Game picks from cards
  const betKeys = [
    'moneyline','run_line','total','f5_moneyline','f5_total','home_team_total','away_team_total'
  ];
  const cards = ([]).concat(TODAY_CARDS || [], TOMORROW_CARDS || []);
  cards.forEach(c => {
    betKeys.forEach(k => {
      const b = c[k];
      if (!b || !b.pick) return;
      const prob = _legProb(b);
      const dec  = _legDec(b) || 2.0;
      const ev   = b.ev != null ? Number(b.ev) : _legEv(prob, dec);
      addLeg({
        label: b.pick,
        game: c.game_key || b.game_key || '',
        bet_type: b.bet_type || k,
        dec_odds: dec,
        conf: b.confidence || Math.round(prob * 100),
        model_prob: prob,
        ev,
        badge: b.safety_label || 'MODERATE',
        source: 'game',
      });
    });
  });

  // Auto-parlay legs (as additional coverage)
  (BEST_PARLAYS || []).forEach(p => {
    (p.legs || []).forEach(l => {
      const label = (l.label || l.pick || '').trim();
      if (!label) return;
      const prob = _legProb(l);
      const dec  = _legDec(l) || 2.0;
      const ev   = _legEv(prob, dec);
      addLeg({
        label,
        game: l.game || '',
        bet_type: l.bet_type || 'player_prop',
        dec_odds: dec,
        conf: l.conf != null ? l.conf : Math.round(prob * 100),
        model_prob: prob,
        ev,
        badge: l.badge || l.safety_label || 'MODERATE',
        source: 'parlay',
      });
    });
  });

  const legs = Array.from(map.values());
  legs.sort((a,b)=>{
    if (b.ev !== a.ev) return b.ev - a.ev;
    return b.model_prob - a.model_prob;
  });
  return legs;
}

function renderTopProps(legs) {
  const grid  = document.getElementById('top-props-grid');
  const empty = document.getElementById('top-props-empty');
  const meta  = document.getElementById('top-props-meta');
  if (!grid) return;
  const props = (legs || []).filter(l => l.source === 'prop').slice(0, 24);
  if (!props.length) {
    grid.innerHTML = '';
    if (empty) empty.classList.remove('hidden');
    if (meta) meta.textContent = '';
    return;
  }
  if (empty) empty.classList.add('hidden');
  if (meta) meta.textContent = `${props.length} props`;
  grid.innerHTML = props.map(l => _renderLegCard(l)).join('');
}

function renderBestLegs(legs) {
  BEST_LEGS = legs || [];
  const grid  = document.getElementById('best-legs-grid');
  const empty = document.getElementById('best-legs-empty');
  const meta  = document.getElementById('best-legs-meta');
  if (!grid) return;
  const best = BEST_LEGS.slice(0, 36);
  if (!best.length) {
    grid.innerHTML = '';
    if (empty) empty.classList.remove('hidden');
    if (meta) meta.textContent = '';
    return;
  }
  if (empty) empty.classList.add('hidden');
  if (meta) meta.textContent = `${best.length} legs`;
  grid.innerHTML = best.map(l => _renderLegCard(l)).join('');
}

function _renderLegCard(l) {
  const probPct = l.model_prob ? (l.model_prob * 100).toFixed(1) + '%' : (l.conf || '?') + '%';
  const evPct   = (l.ev * 100).toFixed(1) + '%';
  const evColor = l.ev >= 0 ? 'var(--green)' : 'var(--red)';
  const typeCls = l.source === 'prop' ? 'prop' : l.source === 'game' ? 'game' : 'parlay';
  const typeLbl = l.source === 'prop' ? 'PROP' : l.source === 'game' ? 'GAME' : 'AUTO';
  const addKey  = JSON.stringify({
    label: l.label,
    bet_type: l.bet_type,
    game: l.game,
    dec_odds: l.dec_odds,
    conf: l.conf,
    badge: l.badge || 'MODERATE',
  }).replace(/"/g,'&quot;');
  return `<div class="best-leg-card">
    <div class="best-leg-title">${l.label}</div>
    <div class="best-leg-meta">
      <span class="best-leg-type ${typeCls}">${typeLbl}</span>
      <span>${l.game || '—'}</span>
      <span style="color:var(--accent)">${probPct}</span>
      <span class="best-leg-ev" style="color:${evColor}">EV ${evPct}</span>
      <span>x${(l.dec_odds||2).toFixed(2)}</span>
    </div>
    <div class="best-leg-actions">
      ${safetyBadge(l.badge || 'MODERATE')}
      <button class="bet-add" onclick="addToParlay(${addKey})">+</button>
    </div>
  </div>`;
}

function buildAutoMix(legs, count) {
  const selected = [];
  const usedGames = new Set();
  const tryAdd = (l) => {
    if (selected.length >= count) return;
    if (l.game && usedGames.has(l.game)) return;
    selected.push(l);
    if (l.game) usedGames.add(l.game);
  };
  const props = (legs || []).filter(l => l.source === 'prop');
  props.forEach(tryAdd);
  (legs || []).forEach(tryAdd);
  if (selected.length < count) {
    (legs || []).forEach(l => {
      if (selected.length >= count) return;
      if (selected.find(s => s.label === l.label)) return;
      selected.push(l);
    });
  }
  return selected;
}

function renderAutoMixCard(mix) {
  const el = document.getElementById('auto-mix-card');
  const empty = document.getElementById('auto-mix-empty');
  if (!el) return;
  if (!mix.length) {
    el.innerHTML = '';
    if (empty) empty.classList.remove('hidden');
    return;
  }
  if (empty) empty.classList.add('hidden');
  const combinedProb = mix.reduce((acc,l)=>acc*_legProb(l), 1);
  const combinedDec  = mix.reduce((acc,l)=>acc*_legDec(l), 1);
  const combinedEv   = _legEv(combinedProb, combinedDec);
  const evColor = combinedEv >= 0 ? 'var(--green)' : 'var(--red)';
  const legsHtml = mix.map(l => {
    const probPct = l.model_prob ? (l.model_prob * 100).toFixed(1) + '%' : (l.conf || '?') + '%';
    return `<div class="parlay-leg">
      <div class="leg-dot" style="background:var(--green)"></div>
      <span class="leg-text">${l.label}</span>
      <span class="leg-conf">${probPct} · x${(l.dec_odds||2).toFixed(2)}</span>
    </div>`;
  }).join('');
  el.innerHTML = `
    <div class="auto-mix-card">
      <div class="auto-mix-head">
        <div>
          <div class="auto-mix-title">Best Mix · ${mix.length} legs</div>
          <div class="auto-mix-meta">Hit: ${(combinedProb*100).toFixed(1)}% · EV ${((combinedEv||0)*100).toFixed(1)}%</div>
        </div>
        <div style="text-align:right">
          <div style="font-size:1.1rem;font-weight:700">x${combinedDec.toFixed(2)}</div>
          <div style="font-size:.78rem;color:${evColor}">$100 → $${(combinedDec*100).toFixed(0)}</div>
        </div>
      </div>
      ${legsHtml}
    </div>
  `;
}

function refreshAutoMix(force) {
  const count = parseInt(document.getElementById('auto-mix-count').value, 10) || 4;
  const mix = buildAutoMix(BEST_LEGS, count);
  AUTO_MIX_LEGS = mix;
  if (!mix.length) {
    renderAutoMixCard([]);
    return;
  }
  if (force || !_parlayTouched || !parlayLegs.length) {
    parlayLegs = mix.map(l => ({
      label: l.label,
      bet_type: l.bet_type,
      game: l.game,
      dec_odds: l.dec_odds,
      conf: l.conf,
      badge: l.badge || 'MODERATE',
    }));
    renderParlayBuilder();
    const nameEl = document.getElementById('parlay-name');
    if (nameEl) nameEl.value = `Auto Mix ${parlayLegs.length}-Leg`;
  }
  renderAutoMixCard(mix);
}

function saveAutoMixParlay() {
  if (!AUTO_MIX_LEGS.length) { toast('No auto-mix yet','info'); return; }
  const combined = AUTO_MIX_LEGS.reduce((acc,l)=>acc*(_legDec(l)||2),1);
  const name = `Auto Mix ${AUTO_MIX_LEGS.length}-Leg`;
  fetch('/api/parlay/save',{
    method:'POST',
    headers:{'Content-Type':'application/json'},
    body:JSON.stringify({name, legs:AUTO_MIX_LEGS, combined_odds:combined, stake_usd:10})
  }).then(r=>r.json()).then(d=>{
    if(d.ok){toast('Auto-mix saved!','success');loadTrackedParlays();}
    else toast(d.error||'Error saving','error');
  });
}

function sendAutoMixSms() {
  if (!AUTO_MIX_LEGS.length) { toast('No auto-mix yet','info'); return; }
  const combined = AUTO_MIX_LEGS.reduce((acc,l)=>acc*(_legDec(l)||2),1);
  const name = `Auto Mix ${AUTO_MIX_LEGS.length}-Leg`;
  sendParlaySms({name, legs: AUTO_MIX_LEGS, combined_odds: combined, stake_usd: 10});
}

// Legacy renderParlays — now delegates to auto-combo generator
function renderParlays(parlays) {
  BEST_PARLAYS = parlays || [];
  autoGenParlaysFromProps();
}

// Parlay builder
function addToParlay(leg) {
  if (parlayLegs.length >= 12) { toast('Max 12 legs','info'); return; }
  if (parlayLegs.find(l=>l.label===leg.label)) { toast('Already added','info'); return; }
  _parlayTouched = true;
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
  _parlayTouched = true;
  parlayLegs.splice(i, 1);
  renderParlayBuilder();
}
function clearParlay() { _parlayTouched = true; parlayLegs=[]; renderParlayBuilder(); }

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

function sendParlaySms(data) {
  fetch('/api/sms/send-parlay',{
    method:'POST',
    headers:{'Content-Type':'application/json'},
    body:JSON.stringify(data)
  }).then(r=>r.json()).then(d=>{
    if (d.ok) toast(`SMS sent (${d.sent||0} delivered)`, 'success');
    else toast(d.error||'Error sending SMS','error');
  });
}

function sendBuilderParlay() {
  if (!parlayLegs.length) { toast('Add legs first','error'); return; }
  const combined = parlayLegs.reduce((acc,l)=>acc*(l.dec_odds||2),1);
  const name  = document.getElementById('parlay-name').value.trim() || 'My Parlay';
  const stake = parseFloat(document.getElementById('parlay-stake').value) || 0;
  sendParlaySms({name, legs: parlayLegs, combined_odds: combined, stake_usd: stake});
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
function loadBotPerformance() {
  const el = document.getElementById('bot-perf-stats');
  if (!el) return;
  Promise.all([
    fetch('/api/performance').then(r=>r.json()),
    fetch('/api/prop-performance').then(r=>r.json()),
    fetch('/api/parlay/performance').then(r=>r.json()),
  ]).then(([g, p, pr]) => {
    if (!g.ok || !p.ok || !pr.ok) {
      el.innerHTML = '<div class="no-games">Could not load bot performance.</div>';
      return;
    }
    const gs = g.stats || {};
    const ps = p.stats || {};
    const rs = pr.stats || {};
    const wins    = (gs.wins||0)   + (ps.wins||0)   + (rs.wins||0);
    const losses  = (gs.losses||0) + (ps.losses||0) + (rs.losses||0);
    const pushes  = (gs.pushes||0) + (ps.pushes||0) + (rs.pushes||0);
    const pending = (gs.pending||0)+ (ps.pending||0)+ (rs.pending||0);
    const total   = (gs.total||0)  + (ps.total||0)  + (rs.total||0);
    const hitRate = (wins + losses) > 0 ? Math.round(wins / (wins + losses) * 100) : null;
    const hrColor = hitRate >= 55 ? 'var(--green)' : hitRate >= 45 ? 'var(--yellow)' : 'var(--red)';
    el.innerHTML = `
      <div class="perf-card"><div class="perf-val perf-win">${wins}</div><div class="perf-label">Bot Wins</div></div>
      <div class="perf-card"><div class="perf-val perf-loss">${losses}</div><div class="perf-label">Bot Losses</div></div>
      <div class="perf-card"><div class="perf-val perf-pend">${pushes}</div><div class="perf-label">Pushes</div></div>
      <div class="perf-card"><div class="perf-val perf-pend">${pending}</div><div class="perf-label">Pending</div></div>
      <div class="perf-card"><div class="perf-val" style="color:${hrColor}">${hitRate!=null?hitRate+'%':'—'}</div><div class="perf-label">Bot Hit Rate</div></div>
      <div class="perf-card"><div class="perf-val" style="font-size:1.2rem">${total}</div><div class="perf-label">Total Picks</div></div>
    `;
  }).catch(() => {
    el.innerHTML = '<div class="no-games">Could not load bot performance.</div>';
  });
}

function loadPerformance() {
  loadBotPerformance();
  fetch('/api/performance').then(r=>r.json()).then(d=>{
    if (!d.ok) { return; }
    const s = d.stats||{};
    document.getElementById('perf-stats').innerHTML = `
      <div class="perf-card"><div class="perf-val perf-win">${s.wins||0}</div><div class="perf-label">Game Wins</div></div>
      <div class="perf-card"><div class="perf-val perf-loss">${s.losses||0}</div><div class="perf-label">Game Losses</div></div>
      <div class="perf-card"><div class="perf-val perf-pend">${s.pushes||0}</div><div class="perf-label">Pushes</div></div>
      <div class="perf-card"><div class="perf-val perf-pend">${s.pending||0}</div><div class="perf-label">Pending</div></div>
      <div class="perf-card"><div class="perf-val perf-rate">${s.hit_rate!=null?s.hit_rate+'%':'—'}</div><div class="perf-label">Hit Rate</div></div>
      <div class="perf-card"><div class="perf-val" style="font-size:1.2rem">${s.total||0}</div><div class="perf-label">Total Bets</div></div>
    `;
    const byType = s.by_bet_type||[];
    const tbody = document.getElementById('by-type-tbody');
    tbody.innerHTML = byType.map(v=>`
      <tr>
        <td>${(v.bet_type||'').replace(/_/g,' ')}</td>
        <td style="color:var(--green)">${v.wins||0}</td>
        <td style="color:var(--red)">${v.losses||0}</td>
        <td>${v.pushes||0}</td>
        <td style="color:var(--yellow)">${v.pending||0}</td>
        <td style="color:var(--accent)">${v.wins&&(v.wins+v.losses)?Math.round(v.wins/(v.wins+v.losses)*100)+'%':'—'}</td>
      </tr>`
    ).join('') || '<tr><td colspan="6" style="text-align:center;color:var(--muted);padding:16px">No data yet.</td></tr>';
  });
  loadHistory();
  loadPropPerformance();
  loadParlayPerformance();
}

function loadPropPerformance() {
  fetch('/api/prop-performance').then(r=>r.json()).then(d=>{
    if (!d.ok) return;
    const s = d.stats||{};
    const hitRateColor = s.hit_rate >= 55 ? 'var(--green)' : s.hit_rate >= 45 ? 'var(--yellow)' : 'var(--red)';
    document.getElementById('prop-perf-stats').innerHTML = `
      <div class="perf-card"><div class="perf-val perf-win">${s.wins||0}</div><div class="perf-label">Prop Wins</div></div>
      <div class="perf-card"><div class="perf-val perf-loss">${s.losses||0}</div><div class="perf-label">Prop Losses</div></div>
      <div class="perf-card"><div class="perf-val perf-pend">${s.pushes||0}</div><div class="perf-label">Pushes</div></div>
      <div class="perf-card"><div class="perf-val perf-pend">${s.pending||0}</div><div class="perf-label">Pending</div></div>
      <div class="perf-card"><div class="perf-val" style="color:${hitRateColor}">${s.hit_rate!=null?s.hit_rate+'%':'—'}</div><div class="perf-label">Prop Hit Rate</div></div>
      <div class="perf-card"><div class="perf-val" style="font-size:1.2rem">${s.total||0}</div><div class="perf-label">Total Props</div></div>
    `;
    const byType = s.by_prop_type||[];
    const tbody = document.getElementById('prop-type-tbody');
    tbody.innerHTML = byType.map(v=>{
      const hr = v.wins&&(v.wins+v.losses) ? Math.round(v.wins/(v.wins+v.losses)*100) : null;
      const hrColor = hr>=55?'var(--green)':hr>=45?'var(--yellow)':'var(--red)';
      return `<tr>
        <td>${(v.prop_type||'').replace(/_/g,' ')}</td>
        <td><span class="badge ${v.recommendation==='OVER'?'badge-blue':'badge-purple'}">${v.recommendation||'—'}</span></td>
        <td style="color:var(--green)">${v.wins||0}</td>
        <td style="color:var(--red)">${v.losses||0}</td>
        <td>${v.total||0}</td>
        <td style="color:${hrColor};font-weight:700">${hr!=null?hr+'%':'—'}</td>
      </tr>`;
    }).join('') || '<tr><td colspan="6" style="text-align:center;color:var(--muted);padding:16px">No prop results yet — runs automatically after games finish.</td></tr>';
  });
}

function resolveOutcomes() {
  fetch('/api/resolve-outcomes',{method:'POST'}).then(r=>r.json()).then(d=>{
    toast(d.msg||'Resolving…','info');
    setTimeout(loadPerformance, 4000);
  });
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

// ── Phone / SMS — stored in localStorage, no DB needed ───────────────────
const _PHONES_KEY = 'bettor_sms_recipients';

function _getStoredPhones() {
  try { return JSON.parse(localStorage.getItem(_PHONES_KEY) || '[]'); }
  catch(e) { return []; }
}
function _saveStoredPhones(arr) {
  localStorage.setItem(_PHONES_KEY, JSON.stringify(arr));
}

function openPhoneModal() {
  document.getElementById('phone-modal').classList.remove('hidden');
  loadPhones();
}
function closePhoneModal() {
  document.getElementById('phone-modal').classList.add('hidden');
}

function loadPhones() {
  const list = document.getElementById('phone-list');
  const numbers = _getStoredPhones();
  if (!numbers.length) {
    list.innerHTML = '<div style="color:var(--muted);font-size:.82rem">No numbers saved yet.</div>';
    return;
  }
  list.innerHTML = numbers.map((n,i) =>
    `<div class="phone-item">
       <span>${n.phone} ${n.label ? '('+n.label+')' : ''}</span>
       <button class="phone-remove" onclick="removePhone('${n.phone}')">Remove</button>
     </div>`
  ).join('');
}

function addPhone() {
  const phone = (document.getElementById('new-phone').value || '').trim();
  const label = (document.getElementById('new-phone-label').value || '').trim();
  if (!phone) { toast('Enter a phone number','error'); return; }
  if (!/^\+?[0-9]{7,15}$/.test(phone.replace(/[\s\-]/g,''))) {
    toast('Invalid phone format (use +1XXXXXXXXXX)','error'); return;
  }
  const numbers = _getStoredPhones();
  if (numbers.find(n => n.phone === phone)) { toast('Number already saved','info'); return; }
  numbers.push({phone, label});
  _saveStoredPhones(numbers);
  toast('Number saved!','success');
  document.getElementById('new-phone').value = '';
  document.getElementById('new-phone-label').value = '';
  loadPhones();
}

function removePhone(phone) {
  const numbers = _getStoredPhones().filter(n => n.phone !== phone);
  _saveStoredPhones(numbers);
  toast('Removed','success');
  loadPhones();
}

function sendSms() {
  const numbers = _getStoredPhones();
  if (!numbers.length) { toast('No phone numbers saved — add one first','error'); return; }
  fetch('/api/sms/send', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({numbers})
  }).then(r => r.json()).then(d => {
    toast(d.ok ? `SMS sent to ${numbers.length} number(s)!` : (d.error||'Error sending'), d.ok?'success':'error');
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

// ── Console ───────────────────────────────────────────────────────────────
let _consoleOpen = false;
let _consolePollId = null;
let _lastLogCount  = 0;

function toggleConsole() {
  _consoleOpen = !_consoleOpen;
  document.getElementById('console-panel').classList.toggle('open', _consoleOpen);
  document.body.classList.toggle('console-open', _consoleOpen);
  if (_consoleOpen) refreshConsole();
}
function openConsole() {
  _consoleOpen = true;
  document.getElementById('console-panel').classList.add('open');
  document.body.classList.add('console-open');
}
function clearConsole() {
  document.getElementById('console-body').innerHTML = '';
  _lastLogCount = 0;
}
function copyLogs() {
  const text = document.getElementById('console-body').innerText;
  navigator.clipboard.writeText(text).then(()=>toast('Logs copied','success')).catch(()=>{});
}

function escHtml(s) {
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

function refreshConsole() {
  fetch('/api/logs').then(r=>r.json()).then(d=>{
    const logs = d.logs || [];
    if (logs.length === _lastLogCount) return;
    _lastLogCount = logs.length;
    const body = document.getElementById('console-body');
    body.innerHTML = logs.map(l => {
      let cls = 'clog-info';
      const ll = l.toLowerCase();
      if (ll.includes('error')||ll.includes('failed')||ll.includes('exception')||ll.includes('traceback')) cls='clog-error';
      else if (ll.includes('warn')||ll.includes('skip')||ll.includes('no data')||ll.includes('none found')) cls='clog-warn';
      else if (ll.includes('complete')||ll.includes('done')||ll.includes('saved')||ll.includes('found')||ll.includes('✓')||ll.includes(' ok')) cls='clog-ok';
      return `<div class="clog-line ${cls}">${escHtml(l)}</div>`;
    }).join('');
    body.scrollTop = body.scrollHeight;
    document.getElementById('console-count').textContent = `${logs.length} lines`;
  }).catch(()=>{});
}

function startConsolePoll() {
  if (_consolePollId) return;
  _consolePollId = setInterval(()=>{ if (_consoleOpen) refreshConsole(); }, 2000);
  const dot = document.getElementById('console-dot');
  if (dot) { dot.classList.remove('idle'); }
}
function stopConsolePoll() {
  clearInterval(_consolePollId); _consolePollId = null;
  const dot = document.getElementById('console-dot');
  if (dot) { dot.classList.add('idle'); }
  if (_consoleOpen) refreshConsole(); // final refresh
}

// ── Init ──────────────────────────────────────────────────────────────────────────
(function init() {
  // Show local cache immediately so page is not blank
  const local = loadLocalCache();
  if (local && (local.game_cards_today||[]).length) {
    applyCachedData(local);
  }
  hideOverlay();

  // Connect SSE — server sends full state on connect + after every analysis run.
  _connectSSE();

  // If analysis is already running when we load, show the progress overlay
  fetch('/api/status').then(r=>r.json()).then(d=>{
    if (d.status === 'running') {
      showOverlay(d.phase || 'Running…', 5);
      startPolling();
    }
  }).catch(()=>{});
})();
