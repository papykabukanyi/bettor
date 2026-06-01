import os
import sys


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(ROOT, "src")
for path in (SRC, ROOT):
    if path not in sys.path:
        sys.path.insert(0, path)

os.environ.setdefault("EAGER_WORKER_INIT", "0")
os.environ.setdefault("BOOT_FORCE_ANALYSIS", "0")
os.environ.setdefault("AUTO_ANALYSIS_INTERVAL_MIN", "0")

from dashboard import _normalize_dashboard_card_buckets, _card_status_phase


def _base_key(card: dict) -> str:
    return f"{card.get('away_team', '')}@{card.get('home_team', '')}"


def main() -> int:
    today_cards = [
        {
            "home_team": "Alpha",
            "away_team": "Beta",
            "game_date": "2026-05-31",
            "game_time": "19:00",
            "status": "Scheduled",
            "game_key": "Beta@Alpha#2026-05-31T19:00",
            "match_key": "BETA@ALPHA",
        }
    ]
    tomorrow_cards = [
        {
            "home_team": "Gamma",
            "away_team": "Delta",
            "game_date": "2026-06-01",
            "game_time": "18:30",
            "status": "Scheduled",
            "game_key": "Delta@Gamma#2026-06-01T18:30",
            "match_key": "DELTA@GAMMA",
        },
        {
            "home_team": "Epsilon",
            "away_team": "Zeta",
            "game_date": "2026-06-01",
            "game_time": "20:00",
            "status": "In Progress",
            "game_key": "Zeta@Epsilon#2026-06-01T20:00",
            "match_key": "ZETA@EPSILON",
        },
        {
            "home_team": "Epsilon",
            "away_team": "Zeta",
            "game_date": "2026-06-01",
            "game_time": "20:00",
            "status": "In Progress",
            "game_key": "Zeta@Epsilon#2026-06-01T20:00",
            "match_key": "ZETA@EPSILON",
        },
    ]

    normalized_today, normalized_tomorrow = _normalize_dashboard_card_buckets(today_cards, tomorrow_cards)

    assert len(normalized_today) == 2
    assert len(normalized_tomorrow) == 1
    assert any(_base_key(card) == "Zeta@Epsilon" or _base_key(card) == "Zeta@Epsilon" for card in normalized_today)
    assert all(_card_status_phase(card.get("status") or "") == "upcoming" for card in normalized_tomorrow)
    assert _card_status_phase("In Progress") == "live"
    assert _card_status_phase("Final") == "final"

    print("Dashboard card bucket regression passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())