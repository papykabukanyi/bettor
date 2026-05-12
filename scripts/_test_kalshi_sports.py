"""Quick test: verify Kalshi sports market catalog fetching."""
import sys
import os
import time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from collections import Counter
from src.data.kalshi import get_open_market_catalog, _market_sport_tag

print("Fetching sports market catalog...")
t0 = time.time()
catalog = get_open_market_catalog(force_refresh=True)
elapsed = time.time() - t0
print(f"Fetched {catalog['count']} markets in {elapsed:.1f}s")

sports = Counter()
for m in catalog['markets']:
    sport = _market_sport_tag(m)
    sports[sport] += 1

print("Markets by sport:")
for sport, count in sports.most_common():
    print(f"  {sport or '(untagged)'}: {count}")

# Show a few NBA basketball props
print("\nSample NBA markets:")
shown = 0
for m in catalog['markets']:
    if _market_sport_tag(m) == "basketball" and shown < 5:
        print(f"  {m.get('ticker')} | {m.get('title')}")
        shown += 1

# Show a few MLB markets
print("\nSample MLB markets:")
shown = 0
for m in catalog['markets']:
    if _market_sport_tag(m) == "baseball" and shown < 5:
        print(f"  {m.get('ticker')} | {m.get('title')}")
        shown += 1
