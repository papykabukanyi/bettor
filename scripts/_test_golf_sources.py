import os
import sys
import math


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(ROOT, "src")
for path in (SRC, ROOT):
    if path not in sys.path:
        sys.path.insert(0, path)

os.environ.setdefault("GOLF_REFERENCE_YEARS", "1")
os.environ.setdefault("GOLF_DATA_CACHE_TTL_SEC", "1")

from data.golf_data_sources import build_golf_prediction_context, load_golf_reference_rows
from data.history_golf import collect_golf_history


def main() -> int:
    refs = load_golf_reference_rows(limit_years=1)
    ctx = build_golf_prediction_context(
        player_name="Player A",
        event_name="Sample Tournament",
        course_name="Sample Course",
        game_date="2026-01-01",
        weather="windy",
        reference_rows=refs,
    )
    history = collect_golf_history(days_back=7)

    assert isinstance(refs, list)
    assert isinstance(ctx, dict)
    assert "sg_total" in ctx
    assert "sg_approach" in ctx
    assert "course_fit" in ctx
    assert "recent_form" in ctx
    assert math.isfinite(float(ctx.get("sg_total") or 0.0))
    assert math.isfinite(float(ctx.get("sg_approach") or 0.0))
    assert isinstance(history, dict)
    assert "game_rows" in history and "player_rows" in history and "injury_rows" in history
    assert isinstance(history.get("game_rows") or [], list)
    assert isinstance(history.get("player_rows") or [], list)
    if refs:
        first_ref = refs[0]
        assert "game_key" in first_ref or "player_name" in first_ref

    print("Golf source smoke test passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())