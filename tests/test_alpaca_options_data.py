"""Alpaca OPTIONS data pipeline. Mirrors test_alpaca_crypto_data.py's
discipline; focuses on what's genuinely different here: a fixed
options-friendly underlying universe (not a ranked-from-thousands
watchlist), contract selection (call/put, nearest strike, nearest
expiration within the configured window), and reuse of alpaca_data.py's
own feature engineering rather than duplicating it."""
from __future__ import annotations

import datetime as dt

import pandas as pd
import pytest

from data import alpaca_options_data as aod


@pytest.fixture(autouse=True)
def _no_real_sentiment_network_calls(monkeypatch):
    monkeypatch.setattr(aod, "get_sentiment", lambda symbol, **kw: {"symbol": symbol, "sentiment_score": 0.0, "headline_volume": 0})


def test_get_options_universe_is_a_fixed_configurable_list():
    universe = aod.get_options_universe()
    assert "AAPL" in universe
    assert isinstance(universe, list)


def _contract(**overrides):
    base = {"symbol": "AAPL240223C00195000", "type": "call", "strike_price": 195.0, "expiration_date": "2024-02-23", "tradable": True}
    base.update(overrides)
    return base


def test_select_contract_picks_a_call_for_up_direction(monkeypatch):
    captured = {}

    def fake_get_option_contracts(*, underlying_symbols, expiration_date_gte, expiration_date_lte, option_type):
        captured["option_type"] = option_type
        return [_contract()]

    monkeypatch.setattr(aod.alpaca_client, "get_option_contracts", fake_get_option_contracts)
    contract = aod.select_contract("AAPL", direction="up", current_price=195.0)
    assert captured["option_type"] == "call"
    assert contract["symbol"] == "AAPL240223C00195000"


def test_select_contract_picks_a_put_for_down_direction(monkeypatch):
    captured = {}

    def fake_get_option_contracts(*, underlying_symbols, expiration_date_gte, expiration_date_lte, option_type):
        captured["option_type"] = option_type
        return [_contract(type="put")]

    monkeypatch.setattr(aod.alpaca_client, "get_option_contracts", fake_get_option_contracts)
    aod.select_contract("AAPL", direction="down", current_price=195.0)
    assert captured["option_type"] == "put"


def test_select_contract_filters_out_untradable_contracts(monkeypatch):
    monkeypatch.setattr(
        aod.alpaca_client, "get_option_contracts",
        lambda **kw: [_contract(tradable=False), _contract(symbol="AAPL240223C00200000", strike_price=200.0, tradable=True)],
    )
    contract = aod.select_contract("AAPL", direction="up", current_price=195.0)
    assert contract["symbol"] == "AAPL240223C00200000"


def test_select_contract_picks_the_nearest_strike_to_current_price(monkeypatch):
    monkeypatch.setattr(
        aod.alpaca_client, "get_option_contracts",
        lambda **kw: [
            _contract(symbol="far", strike_price=220.0),
            _contract(symbol="near", strike_price=196.0),
        ],
    )
    contract = aod.select_contract("AAPL", direction="up", current_price=195.0)
    assert contract["symbol"] == "near"


def test_select_contract_returns_none_when_no_contracts_are_tradable(monkeypatch):
    monkeypatch.setattr(aod.alpaca_client, "get_option_contracts", lambda **kw: [_contract(tradable=False)])
    assert aod.select_contract("AAPL", direction="up", current_price=195.0) is None


def test_select_contract_returns_none_on_a_failed_lookup(monkeypatch):
    def fail(**kw):
        raise RuntimeError("network down")

    monkeypatch.setattr(aod.alpaca_client, "get_option_contracts", fail)
    assert aod.select_contract("AAPL", direction="up", current_price=195.0) is None


def _synthetic_one_min_df(n=100, base=190.0):
    closes = [base + i * 0.01 for i in range(n)]
    return pd.DataFrame({
        "ts": range(n),
        "open": closes, "high": [c + 0.05 for c in closes], "low": [c - 0.05 for c in closes],
        "close": closes,
        "volume": [1000.0] * n,
    })


def test_collect_dataset_rows_uses_the_given_symbols(monkeypatch):
    fetched = []
    monkeypatch.setattr(aod, "fetch_recent_minute_bars", lambda symbol: fetched.append(symbol) or pd.DataFrame())
    result = aod.collect_dataset_rows(["AAPL", "MSFT"])
    assert result.empty
    assert fetched == ["AAPL", "MSFT"]


def test_collect_dataset_rows_defaults_to_the_options_universe(monkeypatch):
    monkeypatch.setattr(aod, "get_options_universe", lambda: ["AAPL"])
    fetched = []
    monkeypatch.setattr(aod, "fetch_recent_minute_bars", lambda symbol: fetched.append(symbol) or pd.DataFrame())
    aod.collect_dataset_rows()
    assert fetched == ["AAPL"]


def test_collect_dataset_rows_one_symbol_failing_does_not_block_the_others(monkeypatch):
    def fake_fetch(symbol):
        if symbol == "BAD":
            raise RuntimeError("network error")
        return _synthetic_one_min_df(n=100)

    monkeypatch.setattr(aod, "fetch_recent_minute_bars", fake_fetch)
    result = aod.collect_dataset_rows(["BAD", "AAPL"])
    assert not result.empty
    assert set(result["symbol"]) == {"AAPL"}


def test_latest_feature_row_returns_none_when_not_enough_history(monkeypatch):
    monkeypatch.setattr(aod, "fetch_recent_minute_bars", lambda symbol: _synthetic_one_min_df(n=5))
    assert aod.latest_feature_row("AAPL") is None


def test_latest_feature_row_returns_feature_columns_plus_symbol_and_price(monkeypatch):
    monkeypatch.setattr(aod, "fetch_recent_minute_bars", lambda symbol: _synthetic_one_min_df(n=100))
    row = aod.latest_feature_row("AAPL")
    assert row is not None
    assert row["symbol"] == "AAPL"
    assert "current_price" in row
    assert "short_ma" in row
    for col in aod.FEATURE_COLUMNS:
        assert col in row


def test_load_training_dataset_returns_empty_frame_without_an_hf_key(monkeypatch):
    monkeypatch.setattr(aod, "HF_API_KEY", "")
    assert aod.load_training_dataset().empty


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
    monkeypatch.setattr(aod, "HF_API_KEY", "fake-token")
    existing_df = pd.DataFrame({"symbol": ["AAPL"], "ts": [100], "close": [190.0]})

    import tempfile
    existing_path = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False).name
    existing_df.to_parquet(existing_path, index=False)

    import huggingface_hub
    monkeypatch.setattr(huggingface_hub, "hf_hub_download", lambda **kw: existing_path)
    _FakeHfApi.captured_upload = {}
    monkeypatch.setattr(huggingface_hub, "HfApi", _FakeHfApi)

    new_df = pd.DataFrame({"symbol": ["MSFT"], "ts": [200], "close": [400.0]})
    result = aod.push_minute_snapshot(new_df)

    assert result["ok"] is True
    merged = _FakeHfApi.captured_upload["df"]
    assert set(merged["symbol"]) == {"AAPL", "MSFT"}


def test_push_minute_snapshot_returns_ok_false_for_an_empty_frame():
    assert aod.push_minute_snapshot(pd.DataFrame()) == {"ok": False, "reason": "no_rows"}


def test_upload_shard_creates_the_dataset_repo_if_missing(monkeypatch):
    monkeypatch.setattr(aod, "HF_API_KEY", "fake-token")
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
    result = aod._upload_shard(  # noqa: SLF001
        pd.DataFrame({"symbol": ["AAPL"], "ts": [1], "close": [190.0]}),
        path_in_repo="minute/2026-01-01.parquet", commit_message="test",
    )
    assert result["ok"] is True
    assert created == [(aod.HF_ALPACA_OPTIONS_DATASET_REPO, "dataset")]
