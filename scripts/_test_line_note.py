import sys
sys.path.insert(0, 'src')
from data.kalshi import (
    get_open_market_catalog, _build_event_index, _score_event_group,
    _score_single_market, _resolve_single_bet, _is_combo_market
)

catalog = get_open_market_catalog()
markets = [m for m in catalog["markets"] if not _is_combo_market(m)]
event_index = _build_event_index(markets)

print(f"Total markets: {len(markets)}, events: {len(event_index)}")

bet = {
    'kind': 'player_prop',
    'bet_type': 'player_prop',
    'player_name': 'Victor Wembanyama',
    'prop_type': 'points',
    'line': 32.5,
    'direction': 'over',
    'sport': 'basketball',
    'game_date': '2026-05-12',
    'game_time': '20:00',
    'team': 'San Antonio Spurs',
}

# Score all events
scored = []
for key, grp in event_index.items():
    s = _score_event_group(bet, grp)
    if s > 0:
        scored.append((s, key))
scored.sort(reverse=True)
print('Top events:')
for s, k in scored[:5]:
    print(f'  {s:.2f} {k}')
    grp = event_index[k]
    for m in grp.get('markets', []):
        ms = _score_single_market(bet, m)
        if ms > 5:
            ticker = m.get('ticker', '')
            print(f'    market {ms:.2f}: {ticker}')

# Full resolution
res = _resolve_single_bet(bet, markets, event_index)
print('\nResolution:', res.get('status'))
print('  ticker:', res.get('market_ticker'))
print('  bet_line:', res.get('bet_line'))
print('  kalshi_line:', res.get('kalshi_line'))
print('  line_note:', res.get('line_note'))
