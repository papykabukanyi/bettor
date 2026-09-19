"""Data pipeline for Kalshi's 15-minute GOLD/SILVER/COPPER markets. See
kalshi_15m_metals_data.py's own module docstring for why this builds its
own price history from scratch (no perps-contract proxy exists for
metals) via a genuinely free, no-API-key spot-price endpoint."""
from __future__ import annotations

import pandas as pd
import pytest

from data import kalshi_15m_metals_data as k


@pytest.fixture(autouse=True)
def _isolated_data_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(k, "DATA_DIR", tmp_path)
    monkeypatch.setattr(k, "HF_API_KEY", "")


def test_get_universe_is_gold_silver_copper():
    assert set(k.get_universe()) == {"GOLD", "SILVER", "COPPER"}


def test_fetch_latest_price_returns_none_for_an_unknown_metal():
    assert k.fetch_latest_price("PLATINUM") is None


def test_fetch_latest_price_parses_a_real_response(monkeypatch):
    class _FakeResponse:
        def raise_for_status(self):
            pass

        def json(self):
            return {"price": 4379.0, "updatedAt": "2026-09-19T22:34:17Z"}

    monkeypatch.setattr(k.requests, "get", lambda url, timeout: _FakeResponse())
    result = k.fetch_latest_price("GOLD")
    assert result["price"] == 4379.0
    assert result["ts"] > 0


def test_fetch_latest_price_returns_none_on_a_network_failure(monkeypatch):
    def fail(url, timeout):
        raise RuntimeError("network error")

    monkeypatch.setattr(k.requests, "get", fail)
    assert k.fetch_latest_price("GOLD") is None


def test_append_price_point_dedupes_by_minute_keeping_the_newest():
    k._append_price_point("GOLD", ts=1_700_000_000, price=100.0)  # noqa: SLF001
    # Same minute (within 60s), different price -- must overwrite, not duplicate.
    result = k._append_price_point("GOLD", ts=1_700_000_030, price=101.0)  # noqa: SLF001
    assert len(result) == 1
    assert result["close"].iloc[0] == 101.0


def test_append_price_point_trims_to_max_rows(monkeypatch):
    monkeypatch.setattr(k, "PRICE_HISTORY_MAX_ROWS", 5)
    for i in range(10):
        k._append_price_point("GOLD", ts=1_700_000_000 + i * 60, price=100.0 + i)  # noqa: SLF001
    history = k._load_price_history("GOLD")  # noqa: SLF001
    assert len(history) == 5
    assert history["close"].iloc[-1] == 109.0  # the most recent point survived


def test_load_price_history_returns_empty_frame_when_missing():
    history = k._load_price_history("GOLD")  # noqa: SLF001
    assert history.empty
    assert list(history.columns) == ["ts", "close"]


def _synthetic_price_df(prices: list[float], start_ts: int = 1_700_000_000, step: int = 60) -> pd.DataFrame:
    return pd.DataFrame({"ts": [start_ts + i * step for i in range(len(prices))], "close": prices})


def test_engineer_metals_features_empty_below_the_minimum_window():
    result = k.engineer_metals_features(_synthetic_price_df([100.0] * 50))
    assert result.empty


def test_engineer_metals_features_computes_a_leaner_column_set():
    prices = [100.0 + i * 0.01 for i in range(300)]
    result = k.engineer_metals_features(_synthetic_price_df(prices))
    assert not result.empty
    for col in k.METALS_FEATURE_COLUMNS:
        assert col in result.columns
    # Real, deliberate omissions -- see this module's own docstring.
    for col in ("atr_pct", "stoch_k", "volume_ratio_5", "dollar_volume_z", "oi_change_pct", "spread_pct", "sentiment_score"):
        assert col not in result.columns


def test_engineer_metals_features_label_is_nan_for_recent_rows():
    prices = [100.0 + i * 0.01 for i in range(300)]
    result = k.engineer_metals_features(_synthetic_price_df(prices))
    tail = result.tail(k.LABEL_HORIZON_MINUTES)
    assert tail["label_up"].isna().all()


def test_engineer_metals_features_label_matches_future_direction():
    prices = [100.0] * 280 + [200.0] * 20
    result = k.engineer_metals_features(_synthetic_price_df(prices))
    labeled = result.dropna(subset=["label_up"])
    jump_crossing = labeled[labeled["close"] == 100.0]
    if not jump_crossing.empty:
        assert (jump_crossing["label_up"] == 1).any()


def test_collect_dataset_rows_tags_each_metal(monkeypatch):
    prices = [100.0 + i * 0.01 for i in range(300)]

    def fake_fetch(metal):
        return {"price": 105.0, "ts": 1_700_000_000 + 300 * 60}

    # Pre-seed history so the very first collect() call already clears
    # the minimum window (a real cold start would take 245+ real minutes
    # to reach this point -- see this module's own docstring).
    for metal in ("GOLD", "SILVER"):
        for i, p in enumerate(prices):
            k._append_price_point(metal, ts=1_700_000_000 + i * 60, price=p)  # noqa: SLF001

    monkeypatch.setattr(k, "fetch_latest_price", fake_fetch)
    result = k.collect_dataset_rows(["GOLD", "SILVER"])
    assert not result.empty
    assert set(result["symbol"]) == {"GOLD", "SILVER"}


def test_collect_dataset_rows_one_metal_failing_does_not_block_the_others(monkeypatch):
    prices = [100.0 + i * 0.01 for i in range(300)]
    for i, p in enumerate(prices):
        k._append_price_point("SILVER", ts=1_700_000_000 + i * 60, price=p)  # noqa: SLF001

    def fake_fetch(metal):
        if metal == "GOLD":
            return None
        return {"price": 105.0, "ts": 1_700_000_000 + 300 * 60}

    monkeypatch.setattr(k, "fetch_latest_price", fake_fetch)
    result = k.collect_dataset_rows(["GOLD", "SILVER"])
    assert set(result["symbol"]) == {"SILVER"}


def test_latest_feature_row_returns_none_with_no_history():
    assert k.latest_feature_row("GOLD") is None


def test_latest_feature_row_returns_feature_columns_plus_symbol_and_price():
    prices = [100.0 + i * 0.01 for i in range(300)]
    for i, p in enumerate(prices):
        k._append_price_point("COPPER", ts=1_700_000_000 + i * 60, price=p)  # noqa: SLF001

    row = k.latest_feature_row("COPPER")
    assert row is not None
    assert row["symbol"] == "COPPER"
    assert row["current_price"] > 0
    for col in k.METALS_FEATURE_COLUMNS:
        assert col in row


def test_push_dataset_snapshot_with_no_rows_returns_not_ok():
    result = k.push_dataset_snapshot(pd.DataFrame())
    assert result == {"ok": False, "reason": "no_rows"}


def test_push_dataset_snapshot_writes_a_local_shard_and_dedupes():
    df = pd.DataFrame({"symbol": ["GOLD", "GOLD"], "ts": [1, 1], "close": [100.0, 101.0]})
    result = k.push_dataset_snapshot(df)
    assert result["ok"] is True
    assert result["hf_uploaded"] is False
    assert result["rows_written"] == 1
    shard = pd.read_parquet(result["shard"])
    assert shard["close"].iloc[0] == 101.0


def test_load_training_dataset_with_no_local_shards_and_no_hf_key_returns_empty():
    result = k.load_training_dataset()
    assert result.empty


def test_load_training_dataset_reads_local_shards(tmp_path):
    shard_dir = k.DATA_DIR / "kalshi_15m_metals_dataset"
    shard_dir.mkdir(parents=True)
    pd.DataFrame({"symbol": ["GOLD"], "ts": [1], "close": [100.0]}).to_parquet(shard_dir / "2026-01-01.parquet")
    result = k.load_training_dataset()
    assert len(result) == 1
    assert result["symbol"].iloc[0] == "GOLD"
