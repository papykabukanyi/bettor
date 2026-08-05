"""Alpaca crypto market data pipeline -- universe acquisition, bar parsing,
and leakage-free feature engineering. Mirrors test_alpaca_data.py's
discipline; focuses on what's genuinely DIFFERENT here: the v1beta3 crypto
bars path, symbol_to_coin() mapping into crypto_news, and hour/day-of-week
cyclical time encoding in place of a market-session-anchored feature."""
from __future__ import annotations

import pandas as pd
import pytest

from data import alpaca_crypto_data as acd


@pytest.fixture(autouse=True)
def _isolated_universe_cache(monkeypatch):
    monkeypatch.setattr(acd, "_universe_cache", {"symbols": None, "computed_at": 0.0})


@pytest.fixture(autouse=True)
def _isolated_minute_bar_cache(monkeypatch):
    monkeypatch.setattr(acd, "_minute_bar_cache", {})


@pytest.fixture(autouse=True)
def _no_real_sentiment_network_calls(monkeypatch):
    monkeypatch.setattr(acd, "get_sentiment", lambda coin: {"coin": coin, "sentiment_score": 0.0, "headline_volume": 0})


def test_symbol_to_coin():
    assert acd.symbol_to_coin("BTC/USD") == "BTC"
    assert acd.symbol_to_coin("eth/usd") == "ETH"


def test_get_crypto_universe_uses_tradable_usd_equivalent_quoted_assets(monkeypatch):
    assets = [
        {"symbol": "BTC/USD", "tradable": True},
        {"symbol": "ETH/USD", "tradable": True},
        {"symbol": "XRP/USDT", "tradable": True},  # dollar-pegged stablecoin quote -- included
        {"symbol": "SOL/USDC", "tradable": True},  # dollar-pegged stablecoin quote -- included
        {"symbol": "ETH/BTC", "tradable": True},  # BTC-quoted, not a dollar figure -- excluded
        {"symbol": "LTC/USD", "tradable": False},  # not tradable -- excluded
    ]
    monkeypatch.setattr(acd.alpaca_client, "get_assets", lambda **kw: assets)
    universe = acd.get_crypto_universe()
    assert universe == ["BTC/USD", "ETH/USD", "SOL/USDC", "XRP/USDT"]


def test_get_crypto_universe_is_cached(monkeypatch):
    calls = {"n": 0}

    def fake_get_assets(**kw):
        calls["n"] += 1
        return [{"symbol": "BTC/USD", "tradable": True}]

    monkeypatch.setattr(acd.alpaca_client, "get_assets", fake_get_assets)
    acd.get_crypto_universe()
    acd.get_crypto_universe()
    assert calls["n"] == 1


def test_get_crypto_universe_survives_a_failed_fetch(monkeypatch):
    def fail(**kw):
        raise RuntimeError("network down")

    monkeypatch.setattr(acd.alpaca_client, "get_assets", fail)
    assert acd.get_crypto_universe() == []


def test_fetch_crypto_bars_uses_the_crypto_client(monkeypatch):
    captured = {}

    def fake_get_crypto_bars(symbols, *, timeframe, start, end):
        captured["symbols"] = symbols
        captured["timeframe"] = timeframe
        return {symbols[0]: [{"t": "2024-01-01T00:00:00Z", "o": 1, "h": 1, "l": 1, "c": 1, "v": 1}]}

    monkeypatch.setattr(acd.alpaca_client, "get_crypto_bars", fake_get_crypto_bars)
    df = acd.fetch_crypto_bars("BTC/USD", days=5)
    assert captured["symbols"] == ["BTC/USD"]
    assert captured["timeframe"] == "1Min"
    assert not df.empty


def test_fetch_crypto_bars_survives_a_failed_call(monkeypatch):
    def fail(symbols, **kw):
        raise RuntimeError("network error")

    monkeypatch.setattr(acd.alpaca_client, "get_crypto_bars", fail)
    assert acd.fetch_crypto_bars("BTC/USD").empty


def test_fetch_recent_crypto_bars_caches_within_the_ttl(monkeypatch):
    calls = []
    monkeypatch.setattr(acd, "fetch_crypto_bars", lambda symbol, days=5: calls.append(symbol) or pd.DataFrame({"ts": [1], "close": [1.0]}))
    first = acd.fetch_recent_crypto_bars("BTC/USD")
    second = acd.fetch_recent_crypto_bars("BTC/USD")
    assert len(calls) == 1
    pd.testing.assert_frame_equal(first, second)


def _synthetic_one_min_df(n=100, base=100.0, vol_base=1000.0):
    closes = [base + i * 0.01 for i in range(n)]
    return pd.DataFrame({
        "ts": range(n),
        "open": closes, "high": [c + 0.05 for c in closes], "low": [c - 0.05 for c in closes],
        "close": closes,
        "volume": [vol_base] * n,
    })


def test_engineer_features_requires_minimum_rows():
    assert acd.engineer_features(_synthetic_one_min_df(n=10)).empty


def test_engineer_features_has_no_market_session_feature_but_has_cyclical_time():
    """Unlike alpaca_data.py (equities), crypto trades 24/7 -- there's no
    time_of_day_pct here, replaced by hour/day-of-week cyclical encoding."""
    feats = acd.engineer_features(_synthetic_one_min_df(n=100))
    assert not feats.empty
    assert "time_of_day_pct" not in feats.columns
    for col in ("hour_sin", "hour_cos", "dow_sin", "dow_cos"):
        assert col in feats.columns
        assert feats[col].abs().max() <= 1.0 + 1e-9


def test_engineer_features_broadcasts_sentiment_score():
    feats = acd.engineer_features(_synthetic_one_min_df(n=100), sentiment_score=0.42)
    assert (feats["sentiment_score"] == 0.42).all()


def test_engineer_features_label_is_nan_for_the_most_recent_row():
    feats = acd.engineer_features(_synthetic_one_min_df(n=100))
    assert feats["label_up"].iloc[-1] is pd.NA or pd.isna(feats["label_up"].iloc[-1])
    assert feats["label_up"].iloc[-2] in (0, 1)


def test_engineer_features_all_columns_present_in_feature_columns():
    feats = acd.engineer_features(_synthetic_one_min_df(n=100))
    for col in acd.FEATURE_COLUMNS:
        assert col in feats.columns


def test_collect_dataset_rows_uses_the_given_symbols(monkeypatch):
    fetched = []
    monkeypatch.setattr(acd, "fetch_recent_crypto_bars", lambda symbol: fetched.append(symbol) or pd.DataFrame())
    result = acd.collect_dataset_rows(["BTC/USD", "ETH/USD"])
    assert result.empty
    assert fetched == ["BTC/USD", "ETH/USD"]


def test_collect_dataset_rows_defaults_to_the_full_universe(monkeypatch):
    monkeypatch.setattr(acd, "get_crypto_universe", lambda: ["BTC/USD"])
    fetched = []
    monkeypatch.setattr(acd, "fetch_recent_crypto_bars", lambda symbol: fetched.append(symbol) or pd.DataFrame())
    acd.collect_dataset_rows()
    assert fetched == ["BTC/USD"]


def test_collect_dataset_rows_one_symbol_failing_does_not_block_the_others(monkeypatch):
    def fake_fetch(symbol):
        if symbol == "BAD/USD":
            raise RuntimeError("network error")
        return _synthetic_one_min_df(n=100)

    monkeypatch.setattr(acd, "fetch_recent_crypto_bars", fake_fetch)
    result = acd.collect_dataset_rows(["BAD/USD", "BTC/USD"])
    assert not result.empty
    assert set(result["symbol"]) == {"BTC/USD"}


def test_collect_dataset_rows_uses_sentiment_keyed_by_coin(monkeypatch):
    monkeypatch.setattr(acd, "fetch_recent_crypto_bars", lambda symbol: _synthetic_one_min_df(n=100))
    captured = []
    monkeypatch.setattr(acd, "get_sentiment", lambda coin: captured.append(coin) or {"sentiment_score": 0.0})
    acd.collect_dataset_rows(["BTC/USD"])
    assert captured == ["BTC"]


def test_latest_feature_row_returns_none_when_not_enough_history(monkeypatch):
    monkeypatch.setattr(acd, "fetch_recent_crypto_bars", lambda symbol: _synthetic_one_min_df(n=5))
    assert acd.latest_feature_row("BTC/USD") is None


def test_latest_feature_row_returns_feature_columns_plus_symbol_and_price(monkeypatch):
    monkeypatch.setattr(acd, "fetch_recent_crypto_bars", lambda symbol: _synthetic_one_min_df(n=100))
    row = acd.latest_feature_row("BTC/USD")
    assert row is not None
    assert row["symbol"] == "BTC/USD"
    assert "current_price" in row
    assert "short_ma" in row
    for col in acd.FEATURE_COLUMNS:
        assert col in row


def test_load_training_dataset_returns_empty_frame_without_an_hf_key(monkeypatch):
    monkeypatch.setattr(acd, "HF_API_KEY", "")
    assert acd.load_training_dataset().empty


class _FakeHfApi:
    captured_upload: dict = {}

    def __init__(self, token=None):
        pass

    def repo_info(self, *, repo_id, repo_type):
        return {"id": repo_id}

    def upload_file(self, *, path_or_fileobj, path_in_repo, repo_id, repo_type, commit_message):
        _FakeHfApi.captured_upload["df"] = pd.read_parquet(path_or_fileobj)
        _FakeHfApi.captured_upload["path_in_repo"] = path_in_repo


def test_push_minute_snapshot_merges_with_the_existing_shard(monkeypatch):
    monkeypatch.setattr(acd, "HF_API_KEY", "fake-token")
    existing_df = pd.DataFrame({"symbol": ["BTC/USD"], "ts": [100], "close": [65000.0]})

    import tempfile
    existing_path = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False).name
    existing_df.to_parquet(existing_path, index=False)

    import huggingface_hub
    monkeypatch.setattr(huggingface_hub, "hf_hub_download", lambda **kw: existing_path)
    _FakeHfApi.captured_upload = {}
    monkeypatch.setattr(huggingface_hub, "HfApi", _FakeHfApi)

    new_df = pd.DataFrame({"symbol": ["ETH/USD"], "ts": [200], "close": [3500.0]})
    result = acd.push_minute_snapshot(new_df)

    assert result["ok"] is True
    merged = _FakeHfApi.captured_upload["df"]
    assert set(merged["symbol"]) == {"BTC/USD", "ETH/USD"}


def test_push_minute_snapshot_returns_ok_false_for_an_empty_frame():
    assert acd.push_minute_snapshot(pd.DataFrame()) == {"ok": False, "reason": "no_rows"}


def test_upload_shard_creates_the_dataset_repo_if_missing(monkeypatch):
    monkeypatch.setattr(acd, "HF_API_KEY", "fake-token")
    import huggingface_hub

    created = []

    class _FakeApiRepoMissing:
        def __init__(self, token=None):
            pass

        def repo_info(self, *, repo_id, repo_type):
            raise RuntimeError("404 repo not found")

        def create_repo(self, *, repo_id, repo_type, exist_ok, private):
            created.append((repo_id, repo_type))

        def upload_file(self, *, path_or_fileobj, path_in_repo, repo_id, repo_type, commit_message):
            pass

    monkeypatch.setattr(huggingface_hub, "HfApi", _FakeApiRepoMissing)
    result = acd._upload_shard(  # noqa: SLF001
        pd.DataFrame({"symbol": ["BTC/USD"], "ts": [1], "close": [1.0]}),
        path_in_repo="minute/2026-01-01.parquet", commit_message="test",
    )
    assert result["ok"] is True
    assert created == [(acd.HF_ALPACA_CRYPTO_DATASET_REPO, "dataset")]
