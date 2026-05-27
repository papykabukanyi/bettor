import os
import sys


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(ROOT, "src")
for path in (SRC, ROOT):
    if path not in sys.path:
        sys.path.insert(0, path)

os.environ.setdefault("TENNIS_REFERENCE_YEARS", "1")
os.environ.setdefault("TENNIS_DATA_CACHE_TTL_SEC", "1")

from data.history_tennis import collect_tennis_history
from data.tennis_data_sources import build_tennis_prediction_context, load_tennis_reference_rows


def main() -> int:
    refs = load_tennis_reference_rows(limit_years=1)
    ctx = build_tennis_prediction_context(
        home_player="Player A",
        away_player="Player B",
        surface="hard",
        match_date="2026-01-01",
        reference_rows=refs,
    )

    history = collect_tennis_history(days_back=7)

    assert isinstance(refs, list)
    assert isinstance(ctx, dict)
    assert "surface_win_rate_home" in ctx
    assert isinstance(history, dict)
    assert "game_rows" in history and "player_rows" in history and "injury_rows" in history
    assert "live_rows" in history and "live_player_rows" in history

    print("Tennis source smoke test passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())