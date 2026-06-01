import os
import sys
from datetime import datetime, timedelta
from unittest import mock


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(ROOT, "src")
for path in (SRC, ROOT):
    if path not in sys.path:
        sys.path.insert(0, path)

os.environ.setdefault("EAGER_WORKER_INIT", "0")
os.environ.setdefault("BOOT_FORCE_ANALYSIS", "0")
os.environ.setdefault("AUTO_ANALYSIS_INTERVAL_MIN", "0")

import dashboard


class _Resp:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload or {}

    def json(self):
        return self._payload


def _mock_collect_wnba_history(days_back=365):
    rows = []
    players = [
        ("Aja Wilson", "Las Vegas Aces"),
        ("Jackie Young", "Las Vegas Aces"),
        ("Kelsey Plum", "Las Vegas Aces"),
        ("Breanna Stewart", "New York Liberty"),
        ("Sabrina Ionescu", "New York Liberty"),
        ("Jonquel Jones", "New York Liberty"),
    ]
    for idx in range(6):
        for player, team in players:
            rows.append({"player_name": player, "team": team, "stat_type": "points", "stat_value": 14 + (idx % 4)})
            rows.append({"player_name": player, "team": team, "stat_type": "rebounds", "stat_value": 5 + (idx % 3)})
            rows.append({"player_name": player, "team": team, "stat_type": "assists", "stat_value": 3 + (idx % 3)})
            rows.append({"player_name": player, "team": team, "stat_type": "steals", "stat_value": 1 + (idx % 2) * 0.2})
    return {"player_rows": rows}


def _mock_requests_get(url, params=None, timeout=8):
    if "scoreboard" in url:
        return _Resp(200, {"events": []})
    return _Resp(404, {})


def main() -> int:
    tomorrow = (datetime.utcnow() + timedelta(days=1)).strftime("%Y-%m-%d")
    game = {
        "sport": "basketball",
        "competition": "WNBA",
        "league": "WNBA",
        "home_team": "Las Vegas Aces",
        "away_team": "New York Liberty",
        "game_date": tomorrow,
        "game_time": "20:00",
    }

    with mock.patch("data.history_wnba.collect_wnba_history", side_effect=_mock_collect_wnba_history):
        with mock.patch("requests.get", side_effect=_mock_requests_get):
            rows = dashboard._build_model_player_props_fallback([game], max_per_game=10)

    assert len(rows) > 10, f"Expected WNBA depth > 10 rows, got {len(rows)}"
    assert len(rows) <= 18, f"Expected capped WNBA rows <= 18, got {len(rows)}"
    assert any(str(r.get("sentiment_sources") or "") == "wnba_recent_player_profile" for r in rows), "Expected WNBA recent profile rows"
    assert all(str(r.get("sport") or "") == "basketball" for r in rows), "Expected basketball props"

    print("WNBA tomorrow depth regression passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
