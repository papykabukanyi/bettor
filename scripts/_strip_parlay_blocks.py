"""One-off: remove three large dead parlay code blocks from dashboard.html.

Each block is bounded by a unique start-marker line and a unique
end-marker line (exclusive). Markers are asserted to appear exactly once.
"""
import io
import os

PATH = os.path.join(os.path.dirname(__file__), "..", "src", "templates", "dashboard.html")
PATH = os.path.abspath(PATH)

with io.open(PATH, "r", encoding="utf-8") as fh:
    lines = fh.readlines()


def idx_of(substr):
    hits = [i for i, ln in enumerate(lines) if substr in ln]
    if len(hits) != 1:
        raise SystemExit(f"Marker {substr!r} found {len(hits)} times (expected 1): {hits}")
    return hits[0]


# (start_marker, end_marker_exclusive)
blocks = [
    ("// \u2500\u2500 Elite Parlay", "function loadBackendLiveFeedHealth() {"),
    ("function _autoParlayPayload(c) {", "function _getLiveGameKeySet() {"),
    ("function renderTopProps(legs) {", "function _fetchFreshJson(url) {"),
]

ranges = []
for start_sub, end_sub in blocks:
    s = idx_of(start_sub)
    e = idx_of(end_sub)
    if not (s < e):
        raise SystemExit(f"Bad range for {start_sub!r}: start={s} end={e}")
    ranges.append((s, e))
    print(f"Block start L{s+1}: {lines[s].rstrip()}")
    print(f"  -> remove up to (excl) L{e+1}: {lines[e].rstrip()}")
    print(f"  removing {e - s} lines")

# Remove from highest start to lowest so indices stay valid.
for s, e in sorted(ranges, key=lambda r: r[0], reverse=True):
    del lines[s:e]

with io.open(PATH, "w", encoding="utf-8", newline="") as fh:
    fh.writelines(lines)

print(f"Done. New line count: {len(lines)}")
