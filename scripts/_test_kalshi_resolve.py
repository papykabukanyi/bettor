"""Test Kalshi bet resolution for NBA player props."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.data.kalshi import resolve_ready_bets

# Test bet: Victor Wembanyama OVER 24.5 points, MIN vs SAS, May 12
test_bets = [
    {
        "uid": "test-wemby-pts",
        "kind": "player prop",
        "player_name": "Victor Wembanyama",
        "prop_type": "points",
        "direction": "OVER",
        "line": 24.5,
        "team": "San Antonio Spurs",
        "home_team": "San Antonio Spurs",
        "away_team": "Minnesota Timberwolves",
        "game_date": "2026-05-12",
        "sport": "basketball",
        "label": "Victor Wembanyama OVER 24.5 Points",
    },
    {
        "uid": "test-wemby-reb",
        "kind": "player prop",
        "player_name": "Victor Wembanyama",
        "prop_type": "rebounds",
        "direction": "OVER",
        "line": 12.5,
        "team": "San Antonio Spurs",
        "home_team": "San Antonio Spurs",
        "away_team": "Minnesota Timberwolves",
        "game_date": "2026-05-12",
        "sport": "basketball",
        "label": "Victor Wembanyama OVER 12.5 Rebounds",
    },
    {
        "uid": "test-luke-kennard",
        "kind": "player prop",
        "player_name": "Luke Kennard",
        "prop_type": "points",
        "direction": "OVER",
        "line": 14.5,
        "team": "Los Angeles Lakers",
        "home_team": "Los Angeles Lakers",
        "away_team": "Oklahoma City Thunder",
        "game_date": "2026-05-11",
        "sport": "basketball",
        "label": "Luke Kennard OVER 14.5 Points",
    },
    {
        "uid": "test-game-total",
        "kind": "game total",
        "bet_type": "total",
        "direction": "OVER",
        "line": 219.5,
        "home_team": "San Antonio Spurs",
        "away_team": "Minnesota Timberwolves",
        "game_date": "2026-05-12",
        "sport": "basketball",
        "label": "MIN vs SAS OVER 219.5",
    },
    {
        "uid": "test-mlb-total",
        "kind": "game total",
        "bet_type": "total",
        "direction": "OVER",
        "line": 7.5,
        "home_team": "Boston Red Sox",
        "away_team": "Philadelphia Phillies",
        "game_date": "2026-05-12",
        "sport": "baseball",
        "label": "PHI vs BOS OVER 7.5 Runs",
    },
]

result = resolve_ready_bets(test_bets, force_refresh=False)
print(f"Market catalog size: {result['market_count']}")
print()
for uid, res in result['resolutions'].items():
    status = res.get('status')
    msg = res.get('message', '')
    ticker = res.get('market_ticker', '')
    score = res.get('score', 0)
    side = res.get('side', '')
    print(f"[{uid}] {status} (score={score:.1f}) side={side}")
    if ticker:
        print(f"  → {ticker}")
    if status != 'matched':
        print(f"  msg: {msg}")
    print()
