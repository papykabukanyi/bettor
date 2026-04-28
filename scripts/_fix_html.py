"""Fix the corrupted HTML in dashboard.html"""
import re

path = r"c:\Users\lovingtracktor\bettor\src\templates\dashboard.html"

with open(path, encoding='utf-8') as f:
    content = f.read()

# Find the broken section: from props-table-wrap through the end of the corrupted parlay section
# We need to replace the broken <thead> block and restore the panel-parlays div

old = '''    <div class="props-table-wrap">
      <table class="props-table">
        <thead>
    <div class="section-header">
      <span class="section-title">&#127917; Auto Parlays \u2014 Built from Props</span>
      <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">
        <span class="section-meta" id="parlay-auto-meta"></span>
        <button class="btn btn-sm" onclick="autoGenParlaysFromProps()">&#8635; Refresh</button>
      </div>
    </div>
    <div id="parlay-combos-empty" class="no-games hidden">Run analysis to generate props first.</div>
    <div class="parlay-combo-grid" id="parlay-combos-grid"></div>

    <div class="section-header" style="margin-top:28px">
      <span class="section-title">&#128204; Saved &amp; Tracked Parlays</span>
      <div style="display:flex;gap:8px;flex-wrap:wrap">
        <button class="btn btn-sm" onclick="loadTrackedParlays()">&#8635; Refresh</button>
        <button class="btn btn-sm" onclick="resolveOutcomes()">&#9989; Resolve</button>
      </div>
    </div>
    <div id="tracked-parlays-list"><div class="no-games">No tracked parlays yet.</div></div>

    <div class="section-header" style="margin-top:24px">
      <span class="section-title">&#128200; Parlay Performance</span>
      <button class="btn btn-sm" onclick="loadParlayPerformance()">&#8635; 
    <div class="section-header">
      <span class="section-title">Auto Props Parlay</span>
      <div style="display:flex;gap:8px;flex-wrap:wrap">
        <button class="btn btn-sm" onclick="refreshAutoPropsParlay()">Refresh</button>
      </div>
    </div>
    <div id="auto-prop-parlay-card"><div class="no-games">Run analysis to generate props.</div></div>

    <div class="section-header" style="margin-top:24px">
      <span class="section-title">Live Tracking</span>
      <div style="display:flex;gap:8px;flex-wrap:wrap">
        <button class="btn btn-sm" onclick="loadTrackedParlays()">Refresh</button>
        <button class="btn btn-sm" onclick="resolveOutcomes()">Resolve Outcomes</button>
      </div>
    </div>
    <div id="tracked-parlays-list"><div class="no-games">No tracked parlays yet.</div></div>

    <div class="section-header" style="margin-top:24px">
      <span class="section-title">Parlay Performance</span>
      <button class="btn btn-sm" onclick="loadParlayPerformance()">Refresh</button>
    </div>
    <div class="perf-grid" id="parlay-perf-stats" style="margin-bottom:16px">
      <div class="no-games" style="grid-column:1/-1">Loading parlay stats\u2026</div>
    </div>
  </div>'''

new = '''    <div class="props-table-wrap">
      <table class="props-table">
        <thead><tr>
          <th>Player</th><th>Team</th><th>Stat</th><th>Line</th><th>Dir</th>
          <th>Prob</th><th>Safety</th><th>EV</th><th>Signal</th><th></th>
        </tr></thead>
        <tbody id="props-tbody"></tbody>
      </table>
    </div>
  </div>

  <!-- PARLAYS -->
  <div class="tab-panel" id="panel-parlays">
    <div class="section-header">
      <span class="section-title">&#127917; Auto Parlays \u2014 Built from Props</span>
      <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">
        <span class="section-meta" id="parlay-auto-meta"></span>
        <button class="btn btn-sm" onclick="autoGenParlaysFromProps()">&#8635; Refresh</button>
      </div>
    </div>
    <div id="parlay-combos-empty" class="no-games hidden">Run analysis to generate props first.</div>
    <div class="parlay-combo-grid" id="parlay-combos-grid"></div>

    <div class="section-header" style="margin-top:28px">
      <span class="section-title">&#128204; Saved &amp; Tracked Parlays</span>
      <div style="display:flex;gap:8px;flex-wrap:wrap">
        <button class="btn btn-sm" onclick="loadTrackedParlays()">&#8635; Refresh</button>
        <button class="btn btn-sm" onclick="resolveOutcomes()">&#9989; Resolve</button>
      </div>
    </div>
    <div id="tracked-parlays-list"><div class="no-games">No tracked parlays yet.</div></div>

    <div class="section-header" style="margin-top:24px">
      <span class="section-title">&#128200; Parlay Performance</span>
      <button class="btn btn-sm" onclick="loadParlayPerformance()">&#8635; Refresh</button>
    </div>
    <div class="perf-grid" id="parlay-perf-stats" style="margin-bottom:16px">
      <div class="no-games" style="grid-column:1/-1">Loading parlay stats\u2026</div>
    </div>
  </div>'''

# Normalize line endings for comparison
old_norm = old.replace('\r\n', '\n')
content_norm = content.replace('\r\n', '\n')

if old_norm in content_norm:
    content_fixed = content_norm.replace(old_norm, new)
    with open(path, 'w', encoding='utf-8') as f:
        f.write(content_fixed)
    print("SUCCESS: HTML fixed")
else:
    # Try to find the actual content with a broader search
    start = content_norm.find('    <div class="props-table-wrap">\n      <table class="props-table">\n        <thead>\n    <div class="section-header">')
    if start != -1:
        end = content_norm.find('  </div>\n\n  <!-- PERFORMANCE -->', start)
        if end != -1:
            actual = content_norm[start:end]
            print("FOUND section:")
            print(repr(actual[:200]))
        else:
            print("Could not find end marker")
    else:
        # Check what's actually in the area
        idx = content_norm.find('<thead>')
        print(f"thead at index {idx}")
        print(repr(content_norm[idx-100:idx+500]))
