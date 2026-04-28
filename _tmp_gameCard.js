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
