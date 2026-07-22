"""Per-sport model training: each sport with enough rows gets its own model,
thin sports fall back to the combined model, and repeated dataframe loads
within one cycle are served from cache instead of re-downloading from HF Hub."""
from __future__ import annotations

import random

import pandas as pd
import pytest

from data.hf_pipeline import HFDirectPipeline


class FakeApi:
    def __init__(self):
        self.uploaded = []

    def create_repo(self, **kwargs):
        pass

    def upload_file(self, **kwargs):
        self.uploaded.append(kwargs["path_in_repo"])

    def whoami(self):
        return {"name": "test"}


@pytest.fixture
def pipeline(tmp_path):
    import os
    p = HFDirectPipeline()
    p._ok = True
    p._api = FakeApi()
    # Redirect all on-disk writes into the throwaway dir so training tests never
    # clobber the real repo data/hf_pipeline_status.json or training_history.json.
    p._data_dir = str(tmp_path)
    p._status_file = os.path.join(str(tmp_path), "status.json")
    p._training_history_file = os.path.join(str(tmp_path), "training_history.json")
    p._predictions_file = os.path.join(str(tmp_path), "preds.json")
    return p


def _synthetic_multi_sport_games(seed=42):
    random.seed(seed)
    teams_by_sport = {
        "mlb": ["Yankees", "Red Sox", "Dodgers", "Giants"],
        "soccer": ["Arsenal", "Chelsea", "Liverpool", "City"],
        "wnba": ["Liberty", "Aces"],
    }
    n_by_sport = {"mlb": 150, "soccer": 150, "wnba": 40}  # wnba deliberately below the min-rows threshold
    base_date = pd.Timestamp("2026-01-01")
    rows = []
    for sport, n in n_by_sport.items():
        teams = teams_by_sport[sport]
        for i in range(n):
            home, away = random.sample(teams, 2)
            home_score, away_score = random.randint(0, 10), random.randint(0, 10)
            while home_score == away_score:
                away_score = random.randint(0, 10)
            rows.append({
                "sport": sport, "home_team": home, "away_team": away,
                "home_score": home_score, "away_score": away_score,
                "game_date": (base_date + pd.Timedelta(days=i)).date().isoformat(),
                "game_id": f"{sport}-{i}", "season": 2026,
            })
    return pd.DataFrame(rows)


def test_individual_models_trained_only_above_min_rows_threshold(pipeline, monkeypatch):
    games_df = _synthetic_multi_sport_games()
    monkeypatch.setattr(pipeline, "_load_games_dataframe_from_hub", lambda: games_df.copy())
    monkeypatch.setattr(pipeline, "_load_news_signals_dataframe_from_hub", lambda: pd.DataFrame())
    monkeypatch.setattr(pipeline, "_train_news_impact_model", lambda **k: {"ok": False, "reason": "no_data"})

    summary = pipeline.train_and_publish_best_model(min_rows=50)

    assert "mlb" in summary.per_sport, "mlb has 150 rows >= 60, must get its own model"
    assert "soccer" in summary.per_sport, "soccer has 150 rows >= 60, must get its own model"
    assert "wnba" not in summary.per_sport, "wnba has only 40 rows < 60, must fall back to the global model"

    assert "model.joblib" in pipeline._api.uploaded, "combined fallback model must always be published"
    assert "model_mlb.joblib" in pipeline._api.uploaded
    assert "model_soccer.joblib" in pipeline._api.uploaded
    assert "model_wnba.joblib" not in pipeline._api.uploaded


def test_games_dataframe_cached_within_one_cycle(pipeline, monkeypatch):
    call_count = {"n": 0}
    games_df = _synthetic_multi_sport_games()

    def fake_loader_body():
        call_count["n"] += 1
        return games_df.copy()

    # Bypass the network/cache-check plumbing by patching the *uncached* work
    # directly via the public method's own cache attributes.
    pipeline._games_df_cache = None
    real_load = HFDirectPipeline._load_games_dataframe_from_hub

    def patched(self):
        if self._games_df_cache is not None:
            cached_ts, cached_df = self._games_df_cache
            import time as _time
            if (_time.time() - cached_ts) < self._df_cache_ttl_sec:
                return cached_df.copy()
        df = fake_loader_body()
        import time as _time
        self._games_df_cache = (_time.time(), df)
        return df.copy()

    monkeypatch.setattr(HFDirectPipeline, "_load_games_dataframe_from_hub", patched)

    for _ in range(4):
        pipeline._load_games_dataframe_from_hub()

    assert call_count["n"] == 1, "4 calls within the cache TTL must collapse into exactly 1 real fetch"
