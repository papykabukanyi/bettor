"""Feature-engineering correctness: rolling team form, rest days, and
head-to-head win rate must never leak future information into a training row,
and must degrade to neutral defaults for teams/games with no history yet."""
from __future__ import annotations

import pandas as pd
import pytest

from data.hf_pipeline import HFDirectPipeline


@pytest.fixture
def pipeline():
    p = HFDirectPipeline()
    p._ok = False  # never try to touch the network
    return p


def _synthetic_games():
    # Team A hosts B and wins, then A visits C and wins, then B hosts A and
    # wins, then A hosts B again and loses -- deliberately reuses A vs B twice
    # so h2h/recent-form have real history to reflect by the last row.
    return pd.DataFrame([
        {"sport": "mlb", "game_date": "2026-07-01", "home_team": "A", "away_team": "B", "home_score": 5, "away_score": 2},
        {"sport": "mlb", "game_date": "2026-07-03", "home_team": "C", "away_team": "A", "home_score": 1, "away_score": 4},
        {"sport": "mlb", "game_date": "2026-07-05", "home_team": "B", "away_team": "A", "home_score": 0, "away_score": 3},
        {"sport": "mlb", "game_date": "2026-07-10", "home_team": "A", "away_team": "B", "home_score": 1, "away_score": 6},
    ])


def test_first_meeting_defaults_to_neutral(pipeline):
    out = pipeline._add_form_features(_synthetic_games())
    first = out.iloc[0]
    assert first["h2h_home_win_rate"] == 0.5
    assert first["home_recent_win_rate"] == 0.5
    assert first["away_recent_win_rate"] == 0.5


def test_form_features_never_leak_future_results(pipeline):
    out = pipeline._add_form_features(_synthetic_games())
    # Row 3 (2026-07-05): B hosts A. Entering this game, A has won both prior
    # games (2/2 = 1.0 recent win rate) and B has lost the only prior meeting
    # to A (0/1 = 0.0 h2h win rate from B's home perspective).
    row = out[out["game_date"] == "2026-07-05"].iloc[0]
    assert row["away_recent_win_rate"] == 1.0
    assert row["h2h_home_win_rate"] == 0.0
    assert row["away_rest_days"] == 2.0  # last played 07-03, this game 07-05

    # Row 4 (2026-07-10): A hosts B again. A is 3-0 all-time entering this row
    # (the model must NOT see that A is about to lose this exact game).
    last = out[out["game_date"] == "2026-07-10"].iloc[0]
    assert last["home_recent_win_rate"] == 1.0
    assert last["h2h_home_win_rate"] == 1.0
    assert last["home_rest_days"] == 5.0


def test_news_features_default_neutral_with_no_news_data(pipeline, monkeypatch):
    monkeypatch.setattr(pipeline, "_load_news_signals_dataframe_from_hub", lambda: pd.DataFrame())
    df = _synthetic_games().copy()
    df["sport"] = "mlb"
    out = pipeline._add_news_features(df)
    for col in HFDirectPipeline._NEWS_FEATURES:
        assert (out[col] == 0.0).all()


def test_news_features_only_use_news_strictly_before_game_date(pipeline, monkeypatch):
    # Negative news about team A on 2026-07-09 (1 day before the 07-10 game)
    # must affect that game's feature, but must NOT leak into the earlier
    # 07-01 game's feature (which happened before the news existed).
    news_df = pd.DataFrame([
        {
            "sport": "mlb", "entity_team": "A", "game_date": "2026-07-09",
            "sentiment_score": -0.8, "impact_type": "injury_concern",
        }
    ])
    monkeypatch.setattr(pipeline, "_load_news_signals_dataframe_from_hub", lambda: news_df)
    df = _synthetic_games().copy()
    out = pipeline._add_news_features(df)

    early_row = out[out["game_date"] == "2026-07-01"].iloc[0]
    assert early_row["home_news_sentiment"] == 0.0
    assert early_row["home_negative_news_flag"] == 0.0

    later_row = out[out["game_date"] == "2026-07-10"].iloc[0]
    assert later_row["home_news_sentiment"] < 0
    assert later_row["home_negative_news_flag"] == 1.0
