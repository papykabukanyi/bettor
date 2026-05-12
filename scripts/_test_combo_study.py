"""Test combo suggestion engine."""
import sys
sys.path.insert(0, "src")

from data.kalshi import suggest_combo_bets

bets = [
    {"bet_uid": "nba1", "label": "Victor Wembanyama 30+ pts", "model_prob": 0.62, "dec_odds": 2.1, "sport": "basketball", "game": "SAS vs MIN", "game_date": "2026-05-12", "prop_type": "points"},
    {"bet_uid": "nba2", "label": "Quinn Hughes 1+ assists", "model_prob": 0.71, "dec_odds": 1.7, "sport": "hockey", "game": "COL vs MIN", "game_date": "2026-05-11", "prop_type": "assists"},
    {"bet_uid": "mlb1", "label": "Vlad Guerrero Jr 2+ hits", "model_prob": 0.68, "dec_odds": 1.85, "sport": "baseball", "game": "TB vs TOR", "game_date": "2026-05-12", "prop_type": "hits"},
    {"bet_uid": "nba3", "label": "Luke Kennard 20+ pts", "model_prob": 0.55, "dec_odds": 2.3, "sport": "basketball", "game": "OKC vs LAL", "game_date": "2026-05-11", "prop_type": "points"},
    {"bet_uid": "nhl1", "label": "Ryan Hartman 1+ point", "model_prob": 0.64, "dec_odds": 1.9, "sport": "hockey", "game": "COL vs MIN", "game_date": "2026-05-11", "prop_type": "points"},
]

combos = suggest_combo_bets(bets, max_legs=3, min_legs=2)
print(f"Generated {len(combos)} combos")
for c in combos[:8]:
    legs_str = " + ".join(l["label"] for l in c["legs"])
    all_match = "MATCHED" if c["all_matched"] else "partial"
    print(f"  [{all_match}] {c['label']} | EV {c['ev']*100:.1f}% | prob {c['combined_prob']*100:.0f}% | x{c['combined_dec_odds']:.2f}")
    print(f"    Legs: {legs_str}")
