"""Test actual NBA/MLB bets against live Kalshi events to verify matching."""
import os, sys, datetime
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ".")

from src.data.kalshi import (
    get_open_market_catalog, _build_event_index, _score_event_group,
    _resolve_single_bet, _is_combo_market, _norm_text,
    _entity_match_score, _entity_aliases, _bet_sport_tag,
    _time_match_score, _bet_start_dt, _market_time,
)

catalog = get_open_market_catalog()
markets = [m for m in catalog["markets"] if not _is_combo_market(m)]
event_index = _build_event_index(markets)

# Print ALL basketball and baseball events
print("=== ALL KALSHI EVENTS ===")
for ev_key, ev in sorted(event_index.items()):
    sp = ev.get("sport","?")
    print(f"[{sp}] {ev_key}: {ev['text'][:180]}")
    if ev.get("occurrence_datetime"):
        print(f"    time: {ev['occurrence_datetime']}")
print()

# Now test representative bets
today = "2026-05-11"
tomorrow = "2026-05-12"
test_bets = [
    # NBA game bets
    {
        "uid": "test1", "kind": "moneyline", "bet_type": "moneyline",
        "home_team": "Los Angeles Lakers", "away_team": "Oklahoma City Thunder",
        "sport": "basketball", "game_date": today, "pick": "Oklahoma City Thunder", "label": "Oklahoma City Thunder",
    },
    {
        "uid": "test2", "kind": "total", "bet_type": "total",
        "home_team": "Los Angeles Lakers", "away_team": "Oklahoma City Thunder",
        "sport": "basketball", "game_date": today, "pick": "Over", "line": 211.5,
    },
    {
        "uid": "test3", "kind": "moneyline", "bet_type": "moneyline",
        "home_team": "San Antonio Spurs", "away_team": "Minnesota Timberwolves",
        "sport": "basketball", "game_date": tomorrow, "pick": "San Antonio Spurs",
    },
    # Player props
    {
        "uid": "test4", "kind": "player_prop", "prop_type": "points",
        "player_name": "Victor Wembanyama", "team": "San Antonio Spurs",
        "sport": "basketball", "game_date": tomorrow, "line": 32.5, "direction": "over",
    },
    {
        "uid": "test5", "kind": "player_prop", "prop_type": "points",
        "player_name": "Luke Kennard", "team": "Los Angeles Lakers",
        "sport": "basketball", "game_date": today, "line": 20.5, "direction": "over",
    },
    # MLB
    {
        "uid": "test6", "kind": "total", "bet_type": "total",
        "home_team": "Houston Astros", "away_team": "Seattle Mariners",
        "sport": "baseball", "game_date": today, "pick": "Over", "line": 7.5,
    },
    # MLB HRR prop
    {
        "uid": "test7", "kind": "player_prop", "prop_type": "hits runs rbis",
        "player_name": "Yordan Alvarez", "team": "Houston Astros",
        "sport": "baseball", "game_date": today, "line": 3.5, "direction": "over",
    },
    {
        "uid": "test8", "kind": "player_prop", "prop_type": "hits",
        "player_name": "Mike Trout", "team": "Los Angeles Angels",
        "sport": "baseball", "game_date": today, "line": 2.5, "direction": "over",
    },
]

print("\n=== MATCHING RESULTS ===")
for bet in test_bets:
    label = f"{bet.get('player_name') or bet['home_team']} | {bet.get('prop_type') or bet['bet_type']}"
    result = _resolve_single_bet(bet, markets, event_index)
    status = result["status"]
    if status == "matched":
        print(f"[MATCHED] {label}")
        print(f"   ticker={result.get('market_ticker')} side={result.get('side')} score={result.get('score')}")
    else:
        print(f"[{status.upper()}] {label} — {result.get('message','')}")
        # Debug: what events scored?
        scored = [(s, k) for k, ev in event_index.items() for s in [_score_event_group(bet, ev)] if s > 0]
        scored.sort(reverse=True)
        if scored:
            for s, k in scored[:2]:
                print(f"   event: [{s:.2f}] {k}")
        else:
            print(f"   NO events scored > 0")
            bball = [(k, ev) for k, ev in event_index.items() if ev.get("sport") == bet.get("sport","basketball")][:2]
            for k, ev in bball:
                h = _entity_match_score(ev["text"], bet.get("home_team",""))
                a = _entity_match_score(ev["text"], bet.get("away_team",""))
                p = _entity_match_score(ev["text"], bet.get("player_name",""))
                t = _time_match_score(bet, {"occurrence_datetime": ev.get("occurrence_datetime")})
                print(f"   {k}: home={h:.2f} away={a:.2f} player={p:.2f} time={t:.2f}")
