"""
Definitive fix: rewrite filterProps and autoGenParlaysFromProps cleanly.
The current state has filterProps with missing closing, and autoGen partially broken.
"""

path = r"c:\Users\lovingtracktor\bettor\src\templates\dashboard.html"

with open(path, encoding='utf-8') as f:
    content = f.read()

# Find the start of filterProps
fi_start = content.find('\nfunction filterProps() {')
if fi_start == -1:
    print("ERROR: cannot find filterProps")
    exit(1)

# Find the end of autoGenParlaysFromProps (look for parlayComboCard which comes after)
ag_end_marker = '\nfunction parlayComboCard('
ag_end = content.find(ag_end_marker)
if ag_end == -1:
    print("ERROR: cannot find parlayComboCard")
    exit(1)

# Get everything before filterProps and after autoGenParlaysFromProps
before = content[:fi_start]
after = content[ag_end:]

# Now check what the content between fi_start and ag_end looks like
current = content[fi_start:ag_end]
print("Current section length:", len(current))
print("First 200:", repr(current[:200]))
print("Last 200:", repr(current[-200:]))

# Build the correct replacement
replacement = '''
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

// \u2500\u2500 Auto Parlay Combos \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
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
    if (meta) meta.textContent = 'No qualifying props yet \u2014 run analysis first.';
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

'''

new_content = before + replacement + after
with open(path, 'w', encoding='utf-8') as f:
    f.write(new_content)
print("SUCCESS: Rewrote filterProps + autoGenParlaysFromProps")
