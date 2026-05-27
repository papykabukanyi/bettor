import os
import sys


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from dashboard import _compose_game_key, _datetime_to_et_parts, _normalize_card_list


def _assert_equal(actual, expected, label):
    if actual != expected:
        raise AssertionError(f"{label}: expected {expected!r}, got {actual!r}")


def test_datetime_to_et_parts():
    z_date, z_time = _datetime_to_et_parts("2026-05-27T00:10:00Z")
    off_date, off_time = _datetime_to_et_parts("2026-05-27T00:10:00+00:00")

    _assert_equal((z_date, z_time), ("2026-05-26", "20:10"), "Z conversion")
    _assert_equal((off_date, off_time), ("2026-05-26", "20:10"), "Offset conversion")


def test_normalize_card_list_timezone_aware_overwrites_bad_time():
    cards = [
        {
            "away_team": "A",
            "home_team": "B",
            "game_datetime": "2026-05-27T00:10:00Z",
            "game_time": "00:10",
            "sport": "mlb",
        }
    ]

    normalized = _normalize_card_list(cards)
    card = normalized[0]
    _assert_equal(card.get("game_date"), "2026-05-26", "Aware card date")
    _assert_equal(card.get("game_time"), "20:10", "Aware card time")


def test_normalize_card_list_naive_keeps_existing_time():
    cards = [
        {
            "away_team": "A",
            "home_team": "B",
            "game_datetime": "2026-05-27T00:10:00",
            "game_time": "00:10",
            "sport": "mlb",
        }
    ]

    normalized = _normalize_card_list(cards)
    card = normalized[0]
    _assert_equal(card.get("game_time"), "00:10", "Naive card time preserved")


def test_compose_game_key_stable_for_utc_equivalents():
    key_z = _compose_game_key("A", "B", "2026-05-27T00:10:00Z", None, None)
    key_off = _compose_game_key("A", "B", "2026-05-27T00:10:00+00:00", None, None)

    _assert_equal(key_z, "A@B#2026-05-26T20:10", "ET-normalized key from Z")
    _assert_equal(key_off, "A@B#2026-05-26T20:10", "ET-normalized key from offset")


def main():
    test_datetime_to_et_parts()
    test_normalize_card_list_timezone_aware_overwrites_bad_time()
    test_normalize_card_list_naive_keeps_existing_time()
    test_compose_game_key_stable_for_utc_equivalents()
    print("MLB time normalization regression checks passed.")


if __name__ == "__main__":
    main()
