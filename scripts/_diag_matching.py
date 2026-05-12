"""Diagnostic: show actual Kalshi event texts and score sample bets against them."""
import os, sys
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ".")

from src.data.kalshi import (
    get_open_market_catalog, _build_event_index, _score_event_group,
    _score_single_market, _is_combo_market, _norm_text, _entity_match_score,
    _entity_aliases, _resolve_single_bet,
)
from src.data.db import get_conn
import json

# 1. Show sample Kalshi event texts per sport
catalog = get_open_market_catalog()
markets = [m for m in catalog["markets"] if not _is_combo_market(m)]
event_index = _build_event_index(markets)
print(f"Total events: {len(event_index)}, markets: {len(markets)}")
print()

for sport in ("basketball", "baseball", "hockey"):
    events = [(k,v) for k,v in event_index.items() if v.get("sport") == sport][:3]
    print(f"=== {sport.upper()} sample events ===")
    for k, v in events:
        print(f"  {k}: {v['text'][:180]}")
    print()

# 2. Pull today's actual predictions from DB and try to resolve them
print("=== LIVE BET RESOLUTION TRACE ===")
try:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT prediction_uid, player_name, team, home_team, away_team,
                       prop_type, line, direction, model_prob, scheduled_start,
                       label, bet_type, pick
                FROM predictions
                WHERE DATE(scheduled_start AT TIME ZONE 'UTC') >= CURRENT_DATE - INTERVAL '1 day'
                  AND model_prob >= 0.52
                ORDER BY model_prob DESC
                LIMIT 10
            """)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
except Exception as e:
    print(f"DB error: {e}")
    rows, cols = [], []

for row in rows:
    bet = dict(zip(cols, row))
    bet["uid"] = bet.get("prediction_uid", "")
    bet["kind"] = "player_prop" if bet.get("player_name") else "moneyline"
    
    print(f"\nBET: {bet.get('player_name') or bet.get('home_team')} vs {bet.get('away_team')}")
    print(f"  prop={bet.get('prop_type')} line={bet.get('line')} dir={bet.get('direction')}")
    print(f"  start={bet.get('scheduled_start')} prob={bet.get('model_prob')}")
    
    # Score top 5 events
    scored = []
    for ev_key, ev_grp in event_index.items():
        s = _score_event_group(bet, ev_grp)
        if s > 0:
            scored.append((s, ev_key, ev_grp["text"][:100]))
    scored.sort(reverse=True)
    
    if scored:
        print(f"  Top event scores:")
        for sc, ek, txt in scored[:3]:
            print(f"    [{sc:.2f}] {ek}: {txt}")
    else:
        print("  --> NO EVENTS SCORED > 0")
        # Debug: check entity match specifically
        if bet.get("player_name"):
            aliases = _entity_aliases(bet["player_name"])
            print(f"  player aliases: {aliases}")
            # Check first 3 basketball events
            bball = [(k,v) for k,v in event_index.items() if v.get("sport")=="basketball"][:3]
            for k, v in bball:
                pscore = _entity_match_score(v["text"], bet.get("player_name"))
                print(f"    entity score vs {k}: {pscore}")
        if bet.get("home_team"):
            print(f"  home aliases: {_entity_aliases(bet['home_team'])}")
            print(f"  away aliases: {_entity_aliases(bet.get('away_team',''))}")

    # Full resolve
    result = _resolve_single_bet(bet, markets, event_index)
    print(f"  RESULT: {result['status']} — {result['message']}")
    if result.get("market_ticker"):
        print(f"    ticker: {result['market_ticker']} (score={result.get('score')})")
