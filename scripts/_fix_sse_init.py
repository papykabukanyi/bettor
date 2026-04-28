"""
Fix dashboard.html for SSE-based live updates:
1. Remove duplicate _liveScores/startLiveScorePoll (old HTTP polling)
2. Replace init() IIFE with SSE-based init
3. Remove duplicate HTML after </html>
"""

path = r"c:\Users\lovingtracktor\bettor\src\templates\dashboard.html"

with open(path, encoding='utf-8') as f:
    content = f.read()

original_len = len(content)

# ── 1. Remove old live-score polling block ────────────────────────────────
# Find it by anchor strings
start_anchor = "let _livePollId  = null;"
end_anchor   = "\nfunction updateLiveScoreCards()"

si = content.find(start_anchor)
ei = content.find(end_anchor)
if si != -1 and ei != -1 and si < ei:
    # Walk back to the comment line before _livePollId
    comment_start = content.rfind('\n', 0, si)  # newline before the line
    content = content[:comment_start] + content[ei:]
    print(f"Removed old polling block ({ei - comment_start} chars)")
else:
    print(f"Old polling block: si={si}, ei={ei} — skipping")

# ── 2. Replace init IIFE ──────────────────────────────────────────────────
init_start = content.find('// \u2500\u2500 Init \u2500')
if init_start == -1:
    # Try ASCII dashes
    init_start = content.find('// -- Init')
if init_start == -1:
    # Search for the IIFE pattern
    init_start = content.find('(function init()')
    if init_start != -1:
        # Back up to find the comment
        comment_back = content.rfind('\n//', 0, init_start)
        if comment_back != -1:
            init_start = comment_back

if init_start != -1:
    iife_close = content.find('})();', init_start)
    if iife_close != -1:
        iife_end = iife_close + len('})();')
        new_init = """\n// \u2500\u2500 Init \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\n(function init() {\n  // Show local cache immediately so page is not blank\n  const local = loadLocalCache();\n  if (local && (local.game_cards_today||[]).length) {\n    applyCachedData(local);\n  }\n  hideOverlay();\n\n  // Connect SSE \u2014 server sends full state on connect + after every analysis run.\n  _connectSSE();\n\n  // If analysis is already running when we load, show the progress overlay\n  fetch('/api/status').then(r=>r.json()).then(d=>{\n    if (d.status === 'running') {\n      showOverlay(d.phase || 'Running\u2026', 5);\n      startPolling();\n    }\n  }).catch(()=>{});\n})();"""
        content = content[:init_start] + new_init + content[iife_end:]
        print("Replaced init IIFE")
    else:
        print("ERROR: could not find })(); for init IIFE")
else:
    print("ERROR: could not find init IIFE start")

# ── 3. Remove duplicate content after first </html> ───────────────────────
first_close = content.find('</html>')
if first_close != -1:
    remainder = content[first_close + len('</html>'):]
    if len(remainder.strip()) > 0:
        content = content[:first_close + len('</html>')]
        print(f"Trimmed {len(remainder)} duplicate chars after </html>")
    else:
        print("No duplicate after </html>")

with open(path, 'w', encoding='utf-8') as f:
    f.write(content)

print(f"Done. {original_len} -> {len(content)} chars")
