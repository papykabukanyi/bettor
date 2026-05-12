"""Trace exactly what bets the frontend has and why they fail to match."""
import os, sys
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ".")

from src.data.kalshi import (
    get_open_market_catalog, _build_event_index, _score_event_group,
    _resolve_single_bet, _is_combo_market, _norm_text,
    _entity_match_score, _entity_aliases, _bet_sport_tag, _bet_kind_tag,
    _time_match_score, _bet_start_dt, _market_time,
)
from src.dashboard import _state

# Get current app state
import json
with open("scripts/_state_dump.json","w") as f:
    import datetime
    def _ser(o):
        if isinstance(o, (datetime.date, datetime.datetime)): return str(o)
        if isinstance(o, set): return list(o)
        return repr(o)
    json.dump(_state, f, default=_ser, indent=2)
print("State dumped")

cards = _state.get("game_cards_today") or []
props = _state.get("player_props") or []
print(f"State: {len(cards)} today cards, {len(props)} player_props")
print(f"Status: {_state.get('status')}, last_updated: {_state.get('last_updated')}")

# Build sample bets like _deriveReadyBets would
sample_bets = []
for card in cards[:10]:
    home = card.get("home_team","")
    away = card.get("away_team","")
    sport = card.get("sport","")
    gd = card.get("game_date","")
    gt = card.get("game_time","")
    gk = card.get("game_key","")
    for kind in ("moneyline", "total"):
        sub = card.get(kind) or card.get("run_line") or {}
        if sub:
            bet = {
                "uid": sub.get("bet_uid",""),
                "kind": kind,
                "bet_type": kind,
                "pick": sub.get("pick",""),
                "label": sub.get("label",""),
                "line": sub.get("line"),
                "home_team": home,
                "away_team": away,
                "sport": sport,
                "game_date": gd,
                "game_time": gt,
                "game_key": gk,
                "model_prob": sub.get("model_prob", sub.get("prob", 0.5)),
            }
            sample_bets.append(bet)

print(f"\nDerived {len(sample_bets)} game bets from today cards:")
for b in sample_bets[:6]:
    print(f"  [{b['sport']}] {b['home_team']} vs {b['away_team']} | {b['bet_type']} {b['pick']} | date={b['game_date']} time={b['game_time']}")

# Now resolve
catalog = get_open_market_catalog()
markets = [m for m in catalog["markets"] if not _is_combo_market(m)]
event_index = _build_event_index(markets)

print(f"\nKalshi: {len(markets)} markets, {len(event_index)} events")

for bet in sample_bets[:6]:
    print(f"\n--- BET: {bet['home_team']} vs {bet['away_team']} ({bet['sport']}) ---")
    print(f"  bet_sport_tag={_bet_sport_tag(bet)}")
    print(f"  start_dt={_bet_start_dt(bet)}")
    
    # Score all events
    scored = []
    for ev_key, ev in event_index.items():
        s = _score_event_group(bet, ev)
        if s > 0:
            scored.append((s, ev_key))
    scored.sort(reverse=True)
    
    if scored:
        for sc, ek in scored[:3]:
            print(f"  event score [{sc:.2f}] {ek}")
    else:
        print("  NO EVENTS > 0")
        # Debug first 3 basketball events
        bball = [(k,v) for k,v in event_index.items() if v.get("sport")=="basketball"][:3]
        for ek, ev in bball:
            text = ev["text"]
            h = _entity_match_score(text, bet.get("home_team"))
            a = _entity_match_score(text, bet.get("away_team"))
            t = _time_match_score(bet, {"occurrence_datetime": ev.get("occurrence_datetime")})
            bt = _bet_sport_tag(bet)
            ms = ev.get("sport","?")
            print(f"    {ek}: home={h:.2f} away={a:.2f} time={t:.2f} bet_sport={bt} mkt_sport={ms}")
    
    result = _resolve_single_bet(bet, markets, event_index)
    print(f"  RESULT: {result['status']} — {result.get('message','')}")
    if result.get("market_ticker"):
        print(f"    ticker: {result['market_ticker']}")
