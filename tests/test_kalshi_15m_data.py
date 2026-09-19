"""Data pipeline for Kalshi's 15-minute markets. The core, genuinely new
piece this module adds on top of perps_data.py's own (already-tested)
feature engineering is the 15-minute relabeling -- see
_relabel_for_horizon's own docstring."""
from __future__ import annotations

import pandas as pd
import pytest

from data import kalshi_15m_data
from data import perps_data


def _make_candles(prices: list[float], start_ts: int = 1_700_000_000, step: int = 60):
    return [{"end_period_ts": start_ts + i * step, "price": {"close": p}} for i, p in enumerate(prices)]


def _make_hourly_before(one_min_start_ts: int, base: float = 100.0, count: int = 10):
    start_ts = one_min_start_ts - count * 3600
    prices = [base + i * 0.1 for i in range(count)]
    return _make_candles(prices, start_ts=start_ts, step=3600)


@pytest.fixture(autouse=True)
def _no_real_sentiment_network_calls(monkeypatch):
    monkeypatch.setattr(kalshi_15m_data, "get_sentiment", lambda coin, **kw: {"symbol": coin, "sentiment_score": 0.0, "headline_volume": 0})


def _feats(prices: list[float]) -> pd.DataFrame:
    one_min_df = perps_data._candles_to_frame(_make_candles(prices))  # noqa: SLF001
    hourly_df = perps_data._candles_to_frame(_make_hourly_before(1_700_000_000))  # noqa: SLF001
    return perps_data.engineer_features(one_min_df, hourly_df, sentiment_score=0.0)


def test_get_universe_is_the_5_perps_coins():
    universe = kalshi_15m_data.get_universe()
    assert set(universe) == {"BTC", "ETH", "SOL", "XRP", "DOGE"}


def test_relabel_for_horizon_uses_15_minutes_not_perps_own_1_minute():
    """The core, genuinely new behavior this module adds -- confirms the
    label horizon actually changed, not just that SOME label exists."""
    feats = _feats([100.0 + i * 0.01 for i in range(300)])
    assert not feats.empty
    relabeled = kalshi_15m_data._relabel_for_horizon(feats)  # noqa: SLF001

    # perps' OWN label (horizon=1) should have a real value 1 row before
    # the very end; this module's relabeled version needs 15 rows of
    # buffer, so that SAME row must now be NaN under the new horizon.
    perps_horizon = perps_data.LABEL_HORIZON_MINUTES
    assert perps_horizon == 1  # the constant this test's own contrast relies on
    row_idx = -perps_horizon - 1
    assert feats["label_up"].iloc[row_idx] in (0, 1)  # perps' own label is defined here
    assert pd.isna(relabeled["label_up"].iloc[row_idx])  # but 15m's own isn't yet

    # The last 15 rows can't know their own 15-minute-future outcome.
    assert relabeled["label_up"].tail(kalshi_15m_data.LABEL_HORIZON_MINUTES).isna().all()
    # A row with 15 real rows of future data available should have a real label.
    earlier = relabeled.iloc[[-kalshi_15m_data.LABEL_HORIZON_MINUTES - 1]]
    assert earlier["label_up"].notna().all()


def test_relabel_for_horizon_matches_the_real_future_direction():
    feats = _feats([100.0] * 280 + [200.0] * 20)
    relabeled = kalshi_15m_data._relabel_for_horizon(feats)  # noqa: SLF001
    labeled = relabeled.dropna(subset=["label_up"])
    jump_crossing = labeled[labeled["close"] == 100.0]
    if not jump_crossing.empty:
        assert (jump_crossing["label_up"] == 1).any()


def test_relabel_for_horizon_does_not_mutate_the_input_frame():
    feats = _feats([100.0 + i * 0.01 for i in range(300)])
    original_labels = feats["label_up"].copy()
    kalshi_15m_data._relabel_for_horizon(feats)  # noqa: SLF001
    pd.testing.assert_series_equal(feats["label_up"], original_labels)


def test_collect_dataset_rows_tags_each_coin_and_relabels(monkeypatch):
    def fake_fetch(perps_ticker):
        return perps_data._candles_to_frame(_make_candles([100.0 + i * 0.01 for i in range(300)])), \
            perps_data._candles_to_frame(_make_hourly_before(1_700_000_000))  # noqa: SLF001

    monkeypatch.setattr(perps_data, "fetch_candle_frames", fake_fetch)
    result = kalshi_15m_data.collect_dataset_rows(["BTC", "ETH"])
    assert not result.empty
    assert set(result["symbol"]) == {"BTC", "ETH"}
    # Confirms relabeling actually ran on the collected rows, not just
    # perps' own 1-minute label passed straight through.
    assert result["label_up"].tail(kalshi_15m_data.LABEL_HORIZON_MINUTES * 2).isna().any()


def test_collect_dataset_rows_skips_a_coin_with_no_perps_ticker_mapping(monkeypatch):
    monkeypatch.setattr(perps_data, "fetch_candle_frames", lambda t: (perps_data._candles_to_frame([]), perps_data._candles_to_frame([])))  # noqa: SLF001
    result = kalshi_15m_data.collect_dataset_rows(["NOT_A_REAL_COIN"])
    assert result.empty


def test_collect_dataset_rows_one_coin_failing_does_not_block_the_others(monkeypatch):
    def fake_fetch(perps_ticker):
        if perps_ticker == "KXBTCPERP":
            raise RuntimeError("network error")
        return perps_data._candles_to_frame(_make_candles([100.0 + i * 0.01 for i in range(300)])), \
            perps_data._candles_to_frame(_make_hourly_before(1_700_000_000))  # noqa: SLF001

    monkeypatch.setattr(perps_data, "fetch_candle_frames", fake_fetch)
    result = kalshi_15m_data.collect_dataset_rows(["BTC", "ETH"])
    assert not result.empty
    assert set(result["symbol"]) == {"ETH"}


def test_latest_feature_row_returns_none_for_an_unknown_coin():
    assert kalshi_15m_data.latest_feature_row("NOT_A_REAL_COIN") is None


def test_latest_feature_row_returns_none_when_not_enough_history(monkeypatch):
    monkeypatch.setattr(perps_data, "fetch_candle_frames", lambda t: (perps_data._candles_to_frame(_make_candles([100.0] * 5)), perps_data._candles_to_frame([])))  # noqa: SLF001
    assert kalshi_15m_data.latest_feature_row("BTC") is None


def test_latest_feature_row_returns_feature_columns_plus_symbol_and_price(monkeypatch):
    def fake_fetch(perps_ticker):
        return perps_data._candles_to_frame(_make_candles([100.0 + i * 0.01 for i in range(300)])), \
            perps_data._candles_to_frame(_make_hourly_before(1_700_000_000))  # noqa: SLF001

    monkeypatch.setattr(perps_data, "fetch_candle_frames", fake_fetch)
    row = kalshi_15m_data.latest_feature_row("BTC")
    assert row is not None
    assert row["symbol"] == "BTC"
    assert row["current_price"] > 0
    for col in perps_data.FEATURE_COLUMNS:
        assert col in row


def test_push_dataset_snapshot_with_no_rows_returns_not_ok():
    result = kalshi_15m_data.push_dataset_snapshot(pd.DataFrame())
    assert result == {"ok": False, "reason": "no_rows"}


def test_push_dataset_snapshot_writes_a_local_shard_and_dedupes(tmp_path, monkeypatch):
    monkeypatch.setattr(kalshi_15m_data, "DATA_DIR", tmp_path)
    monkeypatch.setattr(kalshi_15m_data, "HF_API_KEY", "")  # skip the HF upload leg entirely
    df = pd.DataFrame({"symbol": ["BTC", "BTC"], "ts": [1, 1], "close": [100.0, 101.0]})
    result = kalshi_15m_data.push_dataset_snapshot(df)
    assert result["ok"] is True
    assert result["hf_uploaded"] is False
    assert result["rows_written"] == 1  # deduped on (symbol, ts), keep="last"

    shard = pd.read_parquet(result["shard"])
    assert shard["close"].iloc[0] == 101.0  # the LATER-in-the-input-order row won


def test_load_training_dataset_with_no_local_shards_and_no_hf_key_returns_empty(tmp_path, monkeypatch):
    monkeypatch.setattr(kalshi_15m_data, "DATA_DIR", tmp_path)
    monkeypatch.setattr(kalshi_15m_data, "HF_API_KEY", "")
    result = kalshi_15m_data.load_training_dataset()
    assert result.empty


def test_load_training_dataset_reads_local_shards(tmp_path, monkeypatch):
    monkeypatch.setattr(kalshi_15m_data, "DATA_DIR", tmp_path)
    monkeypatch.setattr(kalshi_15m_data, "HF_API_KEY", "")
    shard_dir = tmp_path / "kalshi_15m_dataset"
    shard_dir.mkdir(parents=True)
    pd.DataFrame({"symbol": ["BTC"], "ts": [1], "close": [100.0]}).to_parquet(shard_dir / "2026-01-01.parquet")
    result = kalshi_15m_data.load_training_dataset()
    assert len(result) == 1
    assert result["symbol"].iloc[0] == "BTC"
