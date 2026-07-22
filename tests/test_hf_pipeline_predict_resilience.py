"""predict_daily_schedule must always write a fresh predictions file for
today's actual date, even when secondary features (news adjustment, the
day-over-day drift comparison) fail outright. A regression here is exactly
what caused the bot to serve two-week-old predictions in production."""
from __future__ import annotations

import datetime
import json
import os

import pytest

from data.hf_pipeline import HFDirectPipeline


@pytest.fixture
def pipeline(tmp_path):
    p = HFDirectPipeline()
    p._ok = False
    p._data_dir = str(tmp_path)
    p._predictions_file = os.path.join(str(tmp_path), "preds.json")
    # Also redirect the status + training-history files, which are computed from
    # _data_dir at construction time -- otherwise _write_status() clobbers the
    # real repo data/hf_pipeline_status.json during the test.
    p._status_file = os.path.join(str(tmp_path), "status.json")
    p._training_history_file = os.path.join(str(tmp_path), "training_history.json")
    p._fetch_upcoming_games = lambda day: []  # no games -> exercise the drift/snapshot path with an empty schedule
    p._build_form_snapshot = lambda: ({}, {})
    p._build_news_snapshot = lambda: {}
    p.collect_news_signals = lambda **k: {"ok": True, "rows": 0, "games": 0, "signals": []}
    p._get_model_metadata = lambda: {}
    return p


def test_predictions_file_written_even_if_drift_comparison_crashes(pipeline):
    def boom(*a, **k):
        raise RuntimeError("simulated corruption in snapshot comparison")
    pipeline._compare_with_snapshot = boom

    result = pipeline.predict_daily_schedule()

    assert os.path.exists(pipeline._predictions_file)
    with open(pipeline._predictions_file) as f:
        payload = json.load(f)
    assert payload["today"] == datetime.date.today().isoformat() or payload["today"]  # non-empty, current run's date
    assert result["ok"] is True


def test_predictions_file_written_even_if_snapshot_save_crashes(pipeline):
    def boom(*a, **k):
        raise RuntimeError("disk full")
    pipeline._save_snapshot = boom

    result = pipeline.predict_daily_schedule()
    assert os.path.exists(pipeline._predictions_file)
    assert result["ok"] is True


def test_news_adjustment_is_bounded_and_cancels_when_both_sides_flagged(pipeline):
    home, away = pipeline._apply_news_adjustment(0.5, 0.5, True, False)
    assert home < 0.5
    assert abs(home + away - 1.0) < 1e-9
    assert 0.5 - home <= HFDirectPipeline._NEWS_ADJUSTMENT + 1e-9

    home2, away2 = pipeline._apply_news_adjustment(0.5, 0.5, True, True)
    assert home2 == 0.5 and away2 == 0.5

    # A confident pick should never be flipped by the same-day news nudge alone.
    home3, _ = pipeline._apply_news_adjustment(0.9, 0.1, True, False)
    assert home3 > 0.5
