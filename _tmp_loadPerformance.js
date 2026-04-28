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
