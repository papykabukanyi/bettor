"""Full live diagnosis: what bets exist, what Kalshi has, why nothing matches."""
import os, sys
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ".")
from src.data.kalshi import (
    get_open_market_catalog, _build_event_index, _score_event_group,
    _resolve_single_bet, _is_combo_market, _bet_sport_tag, _bet_kind_tag,
)
from src.data.db import get_conn

# --- 1. Pull ALL recent predictions from DB ---
with get_conn() as conn:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT bet_uid, sport, bet_type, pick, line, home_team, away_team,
                   game_date, game_time, model_prob, safety_label
            FROM predictions
            WHERE game_date >= CURRENT_DATE - INTERVAL '1 day'
            ORDER BY game_date, model_prob DESC
            LIMIT 30
        """)
        pred_rows = cur.fetchall()
        pred_cols = [d[0] for d in cur.description]

        cur.execute("""
            SELECT bet_uid, player_name, team, sport, prop_type, line,
                   over_prob, under_prob, recommendation, game_date, run_date
            FROM prop_history
            WHERE game_date >= CURRENT_DATE - INTERVAL '1 day'
            ORDER BY game_date, over_prob DESC
            LIMIT 20
        """)
        prop_rows = cur.fetchall()
        prop_cols = [d[0] for d in cur.description]

print(f"\n=== GAME PREDICTIONS ({len(pred_rows)}) ===")
for row in pred_rows:
    d = dict(zip(pred_cols, row))
    print(f"  [{d['sport']}] {d['home_team']} vs {d['away_team']} | {d['bet_type']} {d['pick']} | {d['game_date']} | prob={d['model_prob']}")

print(f"\n=== PLAYER PROPS ({len(prop_rows)}) ===")
for row in prop_rows:
    d = dict(zip(prop_cols, row))
    print(f"  [{d['sport']}] {d['player_name']} ({d['team']}) {d['prop_type']} {d['line']} | over={d['over_prob']} under={d['under_prob']} | {d['game_date']}")

# --- 2. What Kalshi has ---
catalog = get_open_market_catalog()
markets = [m for m in catalog["markets"] if not _is_combo_market(m)]
event_index = _build_event_index(markets)

sport_counts = {}
for ev in event_index.values():
    sp = ev.get("sport", "unknown")
    sport_counts[sp] = sport_counts.get(sp, 0) + 1
print(f"\n=== KALSHI EVENTS by sport: {sport_counts} ===")
print(f"Total markets: {len(markets)}, events: {len(event_index)}")

# --- 3. Try to resolve actual game predictions ---
print("\n=== RESOLUTION RESULTS ===")
all_bets = []
for row in pred_rows:
    d = dict(zip(pred_cols, row))
    bet = {
        "uid": d.get("bet_uid", ""),
        "kind": "moneyline",
        "bet_type": d.get("bet_type", ""),
        "pick": d.get("pick", ""),
        "line": d.get("line"),
        "home_team": d.get("home_team", ""),
        "away_team": d.get("away_team", ""),
        "sport": d.get("sport", ""),
        "game_date": str(d.get("game_date", "")),
        "game_time": d.get("game_time", ""),
        "model_prob": float(d.get("model_prob", 0)),
    }
    all_bets.append(bet)

for row in prop_rows:
    d = dict(zip(prop_cols, row))
    direction = "over" if float(d.get("over_prob") or 0) > float(d.get("under_prob") or 0) else "under"
    bet = {
        "uid": d.get("bet_uid", ""),
        "kind": "player_prop",
        "player_name": d.get("player_name", ""),
        "team": d.get("team", ""),
        "sport": d.get("sport", ""),
        "prop_type": d.get("prop_type", ""),
        "line": d.get("line"),
        "direction": direction,
        "game_date": str(d.get("game_date", "")),
        "model_prob": max(float(d.get("over_prob") or 0), float(d.get("under_prob") or 0)),
    }
    all_bets.append(bet)

for bet in all_bets:
    result = _resolve_single_bet(bet, markets, event_index)
    status = result["status"]
    label = f"{bet.get('home_team') or bet.get('player_name')} | {bet.get('bet_type') or bet.get('prop_type')} | {bet.get('sport')}"
    if status == "matched":
        print(f"  [MATCHED] {label} -> {result.get('market_ticker')} (score={result.get('score')})")
    else:
        print(f"  [{status.upper()}] {label} -> {result.get('message', '')}")
