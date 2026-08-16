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


def test_collect_dataset_rows_prewarms_sentiment_for_the_full_symbol_list(monkeypatch):
    """Same real fix as scan_and_enter's own -- this job runs across the
    FULL universe, an even bigger sequential-fetch cost than entry_scan's
    watchlist-only loop. See crypto_news.prewarm_sentiment's own docstring."""
    monkeypatch.setattr(acd, "fetch_recent_crypto_bars", lambda symbol: _synthetic_one_min_df(n=100))
    monkeypatch.setattr(acd, "get_sentiment", lambda coin: {"sentiment_score": 0.0})
    prewarmed_with = []
    monkeypatch.setattr(acd, "prewarm_sentiment", lambda coins, **kw: prewarmed_with.extend(coins))
    acd.collect_dataset_rows(["BTC/USD", "ETH/USD"])
    assert sorted(prewarmed_with) == ["BTC", "ETH"]


def test_collect_dataset_rows_still_works_if_sentiment_prewarm_fails(monkeypatch):
    monkeypatch.setattr(acd, "fetch_recent_crypto_bars", lambda symbol: _synthetic_one_min_df(n=100))
    monkeypatch.setattr(acd, "get_sentiment", lambda coin: {"sentiment_score": 0.0})

    def raise_error(coins, **kw):
        raise RuntimeError("simulated prewarm failure")

    monkeypatch.setattr(acd, "prewarm_sentiment", raise_error)
    result = acd.collect_dataset_rows(["BTC/USD"])
    assert not result.empty


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


def test_load_training_dataset_returns_empty_frame_when_listing_hangs(monkeypatch):
    """Real, confirmed production incident on the equities sibling of this
    function: called synchronously from a Flask request handler,
    huggingface_hub's own internal shared-session lock can hang
    indefinitely inside list_repo_files -- confirmed live: gunicorn's own
    WORKER TIMEOUT fired and SIGABRT-killed the worker. A hard timeout
    must convert that into a clean empty-result degradation instead."""
    monkeypatch.setattr(acd, "HF_API_KEY", "fake-token")
    monkeypatch.setattr(acd, "_LOAD_TRAINING_DATASET_LIST_TIMEOUT_SEC", 0.05)

    class HangingApi:
        def __init__(self, token=None):
            pass

        def list_repo_files(self, repo_id, repo_type):
            import time as t
            t.sleep(0.5)
            return []

    import huggingface_hub
    monkeypatch.setattr(huggingface_hub, "HfApi", HangingApi)

    import time as real_time
    start = real_time.monotonic()
    result = acd.load_training_dataset()
    elapsed = real_time.monotonic() - start

    assert result.empty
    assert elapsed < 0.4


def test_load_training_dataset_skips_a_hanging_shard_and_continues(monkeypatch):
    monkeypatch.setattr(acd, "HF_API_KEY", "fake-token")
    monkeypatch.setattr(acd, "_LOAD_TRAINING_DATASET_SHARD_TIMEOUT_SEC", 2.0)
    shard_names = ["minute/2026-07-10.parquet", "minute/2026-07-11.parquet", "minute/2026-07-12.parquet"]

    class FakeApi:
        def __init__(self, token=None):
            pass

        def list_repo_files(self, repo_id, repo_type):
            return shard_names

    def fake_hf_hub_download(repo_id, filename, repo_type, token):
        if filename == "minute/2026-07-11.parquet":
            import time as t
            t.sleep(5.0)  # the hanging shard -- comfortably longer than the 2.0s timeout above even under heavy system load (full-suite runs)
        import tempfile
        idx = shard_names.index(filename)
        day_df = pd.DataFrame({"symbol": ["BTC/USD"] * 10, "ts": [idx * 1000 + i for i in range(10)], "close": [1.0] * 10})
        f = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
        day_df.to_parquet(f.name, index=False)
        return f.name

    import huggingface_hub
    monkeypatch.setattr(huggingface_hub, "HfApi", FakeApi)
    monkeypatch.setattr(huggingface_hub, "hf_hub_download", fake_hf_hub_download)

    result = acd.load_training_dataset()

    assert len(result) == 20  # 2 successful shards x 10 rows -- the hanging one was skipped, not waited on


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


# ---------------------------------------------------------------------------
# backfill_minute_history -- deep historical catch-up, ported from
# alpaca_options_data's own version (itself independent of alpaca_data.py's
# -- separate HF repo, separate universe). Real gap found in review: unlike
# stocks/options/perps, crypto never got this treatment -- confirmed live,
# its real archive held only 7 daily shards (a week) matching exactly the
# organic collect-one-day-at-a-time growth since this pipeline started.
# ---------------------------------------------------------------------------
class _FakeHfApiBatch(_FakeHfApi):
    """create_commit-based fake -- backfill_minute_history batches many
    date-shards into a single multi-file commit (same rate-limit incident
    and fix as alpaca_data's/alpaca_options_data's own versions), so its
    tests must mock create_commit, not upload_file."""

    def create_commit(self, *, repo_id, repo_type, operations, commit_message):
        for op in operations:
            _FakeHfApiBatch.captured_upload.setdefault("commits", []).append(
                {"path_in_repo": op.path_in_repo, "df": pd.read_parquet(op.path_or_fileobj)}
            )


def test_backfill_minute_history_splits_rows_by_calendar_date_and_uploads_one_shard_per_date(monkeypatch):
    monkeypatch.setattr(acd, "HF_API_KEY", "fake-token")
    fake_feats = pd.DataFrame({
        "ts": [1767225600, 1767312000], "close": [100.0, 101.0], "label_up": [1, 0],
    })
    monkeypatch.setattr(acd, "fetch_crypto_bars", lambda symbol, *, days: pd.DataFrame({"ts": [1], "close": [1.0]}))
    monkeypatch.setattr(acd, "engineer_features", lambda df, *, sentiment_score: fake_feats.copy())

    import huggingface_hub

    def fail(**kw):
        raise RuntimeError("no existing shard")

    monkeypatch.setattr(huggingface_hub, "hf_hub_download", fail)
    _FakeHfApiBatch.captured_upload = {}
    monkeypatch.setattr(huggingface_hub, "HfApi", _FakeHfApiBatch)

    result = acd.backfill_minute_history(["BTC/USD"], days=90)

    assert result["ok"] is True
    assert result["symbols_processed"] == 1
    assert result["dates_written"] == 2
    uploads = _FakeHfApiBatch.captured_upload["commits"]
    paths = sorted(u["path_in_repo"] for u in uploads)
    assert paths == ["minute/2026-01-01.parquet", "minute/2026-01-02.parquet"]


def test_backfill_minute_history_batches_many_dates_into_few_commits(monkeypatch):
    """Real, confirmed incident on the stocks/options siblings: one commit
    per calendar date hit HF's 128-commits/hour repo cap partway through a
    long backfill and silently dropped most dates. 45 dates at the
    20-per-batch chunk size must land in 3 commits, not 45."""
    monkeypatch.setattr(acd, "HF_API_KEY", "fake-token")
    base_ts = 1767225600
    ts_values = [base_ts + i * 86400 for i in range(45)]
    fake_feats = pd.DataFrame({"ts": ts_values, "close": [100.0] * 45, "label_up": [1] * 45})
    monkeypatch.setattr(acd, "fetch_crypto_bars", lambda symbol, *, days: pd.DataFrame({"ts": [1], "close": [1.0]}))
    monkeypatch.setattr(acd, "engineer_features", lambda df, *, sentiment_score: fake_feats.copy())

    import huggingface_hub
    monkeypatch.setattr(huggingface_hub, "hf_hub_download", lambda **kw: (_ for _ in ()).throw(RuntimeError("no shard")))
    commit_calls = []

    class _CommitCountingApi(_FakeHfApi):
        def create_commit(self, *, repo_id, repo_type, operations, commit_message):
            commit_calls.append(len(list(operations)))

    monkeypatch.setattr(huggingface_hub, "HfApi", _CommitCountingApi)

    result = acd.backfill_minute_history(["BTC/USD"], days=90)

    assert result["ok"] is True
    assert result["dates_written"] == 45
    assert commit_calls == [20, 20, 5]


def test_backfill_minute_history_merges_with_an_existing_shard_for_that_date(monkeypatch):
    monkeypatch.setattr(acd, "HF_API_KEY", "fake-token")
    fake_feats = pd.DataFrame({"ts": [1767225600], "close": [100.0], "label_up": [1]})
    monkeypatch.setattr(acd, "fetch_crypto_bars", lambda symbol, *, days: pd.DataFrame({"ts": [1], "close": [1.0]}))
    monkeypatch.setattr(acd, "engineer_features", lambda df, *, sentiment_score: fake_feats.copy())

    import tempfile
    existing_df = pd.DataFrame({"symbol": ["ETH/USD"], "ts": [999], "close": [3500.0]})
    existing_path = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False).name
    existing_df.to_parquet(existing_path, index=False)

    import huggingface_hub
    monkeypatch.setattr(huggingface_hub, "hf_hub_download", lambda **kw: existing_path)
    _FakeHfApiBatch.captured_upload = {}
    monkeypatch.setattr(huggingface_hub, "HfApi", _FakeHfApiBatch)

    acd.backfill_minute_history(["BTC/USD"], days=90)

    uploads = _FakeHfApiBatch.captured_upload["commits"]
    assert len(uploads) == 1
    assert set(uploads[0]["df"]["symbol"]) == {"BTC/USD", "ETH/USD"}


def test_backfill_minute_history_holds_sentiment_at_neutral(monkeypatch):
    monkeypatch.setattr(acd, "HF_API_KEY", "fake-token")
    monkeypatch.setattr(acd, "fetch_crypto_bars", lambda symbol, *, days: pd.DataFrame({"ts": [1], "close": [1.0]}))
    captured_sentiment = {}

    def fake_engineer(df, *, sentiment_score):
        captured_sentiment["value"] = sentiment_score
        return pd.DataFrame({"ts": [1767225600], "close": [100.0], "label_up": [1]})

    monkeypatch.setattr(acd, "engineer_features", fake_engineer)

    import huggingface_hub
    monkeypatch.setattr(huggingface_hub, "hf_hub_download", lambda **kw: (_ for _ in ()).throw(RuntimeError("no shard")))
    monkeypatch.setattr(huggingface_hub, "HfApi", _FakeHfApi)

    acd.backfill_minute_history(["BTC/USD"], days=90)

    assert captured_sentiment["value"] == 0.0


def test_backfill_minute_history_returns_ok_false_without_an_hf_key(monkeypatch):
    monkeypatch.setattr(acd, "HF_API_KEY", "")
    result = acd.backfill_minute_history(["BTC/USD"], days=90)
    assert result == {"ok": False, "reason": "no_hf_api_key"}


def test_backfill_minute_history_continues_past_a_symbol_that_fails_to_fetch(monkeypatch):
    monkeypatch.setattr(acd, "HF_API_KEY", "fake-token")

    def fake_fetch(symbol, *, days):
        if symbol == "BAD/USD":
            raise RuntimeError("simulated fetch failure")
        return pd.DataFrame({"ts": [1], "close": [1.0]})

    monkeypatch.setattr(acd, "fetch_crypto_bars", fake_fetch)
    monkeypatch.setattr(acd, "engineer_features", lambda df, *, sentiment_score: pd.DataFrame({"ts": [1767225600], "close": [100.0], "label_up": [1]}))

    import huggingface_hub
    monkeypatch.setattr(huggingface_hub, "hf_hub_download", lambda **kw: (_ for _ in ()).throw(RuntimeError("no shard")))
    monkeypatch.setattr(huggingface_hub, "HfApi", _FakeHfApi)

    result = acd.backfill_minute_history(["BAD/USD", "BTC/USD"], days=90)

    assert result["symbols_processed"] == 1
    assert result["symbols_requested"] == 2


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
