"""Alpaca stock data pipeline -- universe acquisition, bar parsing, and
leakage-free feature engineering. Mirrors test_perps_data.py's discipline:
every rolling feature must only look backward, the forward label must be
NaN for rows too recent to know their own outcome yet. All network mocked."""
from __future__ import annotations

import pandas as pd
import pytest

from data import alpaca_data


@pytest.fixture(autouse=True)
def _isolated_universe_cache(monkeypatch):
    monkeypatch.setattr(alpaca_data, "_universe_cache", {"symbols": None, "names": {}, "computed_at": 0.0})


@pytest.fixture(autouse=True)
def _isolated_minute_bar_cache(monkeypatch):
    monkeypatch.setattr(alpaca_data, "_minute_bar_cache", {})


@pytest.fixture(autouse=True)
def _no_real_sentiment_network_calls(monkeypatch):
    """collect_dataset_rows/latest_feature_row call get_sentiment() (real
    Google News RSS) and get_company_name() (which can trigger a real
    /v2/assets call on a cold cache) internally -- stub both to neutral,
    no-network defaults so the whole suite never depends on network access
    or real credentials (whatever happens to be in .env). Tests that care
    about sentiment specifically override these per-test."""
    monkeypatch.setattr(alpaca_data, "get_sentiment", lambda symbol, **kw: {"symbol": symbol, "sentiment_score": 0.0, "headline_volume": 0})
    monkeypatch.setattr(alpaca_data, "get_company_name", lambda symbol: None)


def test_get_us_stock_universe_uses_tradable_active_assets(monkeypatch):
    assets = [
        {"symbol": "AAPL", "tradable": True},
        {"symbol": "MSFT", "tradable": True},
        {"symbol": "HALTED", "tradable": False},  # not tradable -- must be excluded
    ]
    monkeypatch.setattr(alpaca_data.alpaca_client, "get_assets", lambda **kw: assets)
    universe = alpaca_data.get_us_stock_universe()
    assert universe == ["AAPL", "MSFT"]


def test_get_us_stock_universe_is_cached(monkeypatch):
    calls = {"n": 0}

    def fake_get_assets(**kw):
        calls["n"] += 1
        return [{"symbol": "AAPL", "tradable": True}]

    monkeypatch.setattr(alpaca_data.alpaca_client, "get_assets", fake_get_assets)
    alpaca_data.get_us_stock_universe()
    alpaca_data.get_us_stock_universe()
    assert calls["n"] == 1


def test_get_us_stock_universe_survives_a_failed_fetch(monkeypatch):
    def fail(**kw):
        raise RuntimeError("network down")

    monkeypatch.setattr(alpaca_data.alpaca_client, "get_assets", fail)
    assert alpaca_data.get_us_stock_universe() == []


# ---------------------------------------------------------------------------
# Market session detection -- gates when off-hours intensive training runs
# vs. live trading checks, so this must never raise (a failure here would
# otherwise stall the scheduler entirely).
# ---------------------------------------------------------------------------
def test_get_market_session_regular_hours_from_alpaca_clock(monkeypatch):
    monkeypatch.setattr(alpaca_data.alpaca_client, "get_clock", lambda: {"is_open": True})
    result = alpaca_data.get_market_session()
    assert result == {"session": "regular", "is_open": True, "source": "alpaca"}


def test_get_market_session_closed_uses_et_fallback_for_the_sub_session(monkeypatch):
    monkeypatch.setattr(alpaca_data.alpaca_client, "get_clock", lambda: {"is_open": False})
    result = alpaca_data.get_market_session()
    assert result["is_open"] is False
    assert result["source"] == "alpaca"
    assert result["session"] in {"closed", "pre_market", "post_market"}


def test_get_market_session_falls_back_when_clock_call_fails(monkeypatch):
    def fail():
        raise RuntimeError("not configured")

    monkeypatch.setattr(alpaca_data.alpaca_client, "get_clock", fail)
    result = alpaca_data.get_market_session()
    assert result["source"] == "fallback"
    assert result["session"] in {"closed", "pre_market", "regular", "post_market"}


def test_fallback_market_session_weekend_is_closed():
    saturday = alpaca_data.dt.datetime(2026, 7, 25, 12, 0)  # a real Saturday
    assert saturday.weekday() == 5
    import unittest.mock
    with unittest.mock.patch.object(alpaca_data, "_now_et", return_value=saturday):
        result = alpaca_data._fallback_market_session()  # noqa: SLF001
    assert result == {"session": "closed", "is_open": False, "source": "fallback"}


def test_fallback_market_session_weekday_regular_hours():
    tuesday_at_noon = alpaca_data.dt.datetime(2026, 7, 21, 12, 0)  # a real Tuesday
    assert tuesday_at_noon.weekday() == 1
    import unittest.mock
    with unittest.mock.patch.object(alpaca_data, "_now_et", return_value=tuesday_at_noon):
        result = alpaca_data._fallback_market_session()  # noqa: SLF001
    assert result == {"session": "regular", "is_open": True, "source": "fallback"}


def test_fallback_market_session_weekday_pre_market():
    tuesday_early = alpaca_data.dt.datetime(2026, 7, 21, 6, 0)
    import unittest.mock
    with unittest.mock.patch.object(alpaca_data, "_now_et", return_value=tuesday_early):
        result = alpaca_data._fallback_market_session()  # noqa: SLF001
    assert result == {"session": "pre_market", "is_open": False, "source": "fallback"}


def _make_bars(closes, start_iso="2024-01-01T00:00:00Z", step_sec=60):
    import datetime as dt
    start = dt.datetime.fromisoformat(start_iso.replace("Z", "+00:00"))
    return [
        {"t": (start + dt.timedelta(seconds=i * step_sec)).isoformat().replace("+00:00", "Z"),
         "o": c, "h": c, "l": c, "c": c, "v": 1000.0}
        for i, c in enumerate(closes)
    ]


def test_bars_to_df_converts_rfc3339_timestamp_to_epoch_seconds():
    bars = _make_bars([100.0, 101.0])
    df = alpaca_data._bars_to_df(bars)  # noqa: SLF001
    assert list(df["close"]) == [100.0, 101.0]
    import datetime as dt
    expected_ts = int(dt.datetime.fromisoformat(bars[0]["t"].replace("Z", "+00:00")).timestamp())
    assert df["ts"].iloc[0] == expected_ts


def test_fetch_daily_bars_requests_up_to_the_capped_year_range(monkeypatch):
    captured = {}

    def fake_get_bars(symbols, *, timeframe, start, end):
        captured["symbols"] = symbols
        captured["timeframe"] = timeframe
        captured["start"] = start
        return {symbols[0]: []}

    monkeypatch.setattr(alpaca_data.alpaca_client, "get_bars", fake_get_bars)
    alpaca_data.fetch_daily_bars("AAPL", years=50)
    assert captured["symbols"] == ["AAPL"]
    assert captured["timeframe"] == "1Day"
    # 50 is capped to 20 years -- start should be roughly 20*365 days back
    import datetime as dt
    start_date = dt.date.fromisoformat(captured["start"])
    days_back = (dt.datetime.now(dt.timezone.utc).date() - start_date).days
    assert 19 * 365 <= days_back <= 20 * 365 + 1


def test_fetch_minute_bars_requests_the_given_lookback_window(monkeypatch):
    captured = {}

    def fake_get_bars(symbols, *, timeframe, start, end):
        captured["timeframe"] = timeframe
        captured["start"] = start
        return {symbols[0]: _make_bars([1.0])}

    monkeypatch.setattr(alpaca_data.alpaca_client, "get_bars", fake_get_bars)
    df = alpaca_data.fetch_minute_bars("AAPL", days=5)
    assert captured["timeframe"] == "1Min"
    assert not df.empty


def test_fetch_minute_bars_survives_a_failed_call(monkeypatch):
    def fail(symbols, **kw):
        raise RuntimeError("network error")

    monkeypatch.setattr(alpaca_data.alpaca_client, "get_bars", fail)
    df = alpaca_data.fetch_minute_bars("AAPL")
    assert df.empty


# ── fetch_recent_minute_bars: short-window, short-TTL-cached live fetch ────
def test_fetch_recent_minute_bars_uses_a_single_call(monkeypatch):
    calls = []
    monkeypatch.setattr(alpaca_data, "fetch_minute_bars", lambda symbol, days=5: calls.append(symbol) or pd.DataFrame({"ts": [1], "close": [1.0]}))
    alpaca_data.fetch_recent_minute_bars("AAPL")
    assert calls == ["AAPL"]


def test_fetch_recent_minute_bars_caches_within_the_ttl_window(monkeypatch):
    calls = []
    monkeypatch.setattr(alpaca_data, "fetch_minute_bars", lambda symbol, days=5: calls.append(symbol) or pd.DataFrame({"ts": [1], "close": [1.0]}))
    first = alpaca_data.fetch_recent_minute_bars("AAPL")
    second = alpaca_data.fetch_recent_minute_bars("AAPL")
    assert len(calls) == 1  # second call served from cache, no new fetch
    pd.testing.assert_frame_equal(first, second)


def test_fetch_recent_minute_bars_refetches_once_the_ttl_expires(monkeypatch):
    calls = []
    monkeypatch.setattr(alpaca_data, "fetch_minute_bars", lambda symbol, days=5: calls.append(symbol) or pd.DataFrame({"ts": [1], "close": [1.0]}))
    monkeypatch.setattr(alpaca_data, "_MINUTE_BAR_CACHE_TTL_SEC", 0)
    alpaca_data.fetch_recent_minute_bars("AAPL")
    alpaca_data.fetch_recent_minute_bars("AAPL")
    assert len(calls) == 2


def test_fetch_recent_minute_bars_caches_each_symbol_independently(monkeypatch):
    calls = []
    monkeypatch.setattr(alpaca_data, "fetch_minute_bars", lambda symbol, days=5: calls.append(symbol) or pd.DataFrame({"ts": [1], "close": [1.0]}))
    alpaca_data.fetch_recent_minute_bars("AAPL")
    alpaca_data.fetch_recent_minute_bars("MSFT")
    assert len(calls) == 2


def _synthetic_one_min_df(n=100, base=100.0, vol_base=1000.0):
    closes = [base + i * 0.01 for i in range(n)]
    return pd.DataFrame({
        "ts": range(n),
        "open": closes, "high": [c + 0.05 for c in closes], "low": [c - 0.05 for c in closes],
        "close": closes,
        "volume": [vol_base] * n,
    })


def test_engineer_features_requires_minimum_rows():
    small_df = _synthetic_one_min_df(n=10)
    assert alpaca_data.engineer_features(small_df).empty


def test_engineer_features_label_is_nan_for_the_most_recent_row():
    df = _synthetic_one_min_df(n=100)
    feats = alpaca_data.engineer_features(df)
    assert not feats.empty
    assert feats["label_up"].iloc[-1] is pd.NA or pd.isna(feats["label_up"].iloc[-1])
    assert feats["label_up"].iloc[-2] in (0, 1)


def test_engineer_features_volume_ratio_reflects_a_recent_spike():
    n = 100
    volumes = [1000.0] * (n - 5) + [5000.0] * 5  # a recent volume spike
    df = _synthetic_one_min_df(n=n)
    df["volume"] = volumes
    feats = alpaca_data.engineer_features(df)
    assert feats["volume_ratio_5"].iloc[-1] > 1.5


def test_engineer_features_all_columns_present_in_feature_columns():
    df = _synthetic_one_min_df(n=100)
    feats = alpaca_data.engineer_features(df)
    for col in alpaca_data.FEATURE_COLUMNS:
        assert col in feats.columns


def test_time_of_day_pct_is_zero_at_open_and_one_at_close():
    import zoneinfo
    eastern = zoneinfo.ZoneInfo("America/New_York")
    open_ts = int(alpaca_data.dt.datetime(2026, 7, 21, 9, 30, tzinfo=eastern).timestamp())
    close_ts = int(alpaca_data.dt.datetime(2026, 7, 21, 16, 0, tzinfo=eastern).timestamp())
    midday_ts = int(alpaca_data.dt.datetime(2026, 7, 21, 12, 45, tzinfo=eastern).timestamp())

    result = alpaca_data._time_of_day_pct(pd.Series([open_ts, close_ts, midday_ts]))  # noqa: SLF001
    assert abs(result.iloc[0] - 0.0) < 1e-9
    assert abs(result.iloc[1] - 1.0) < 1e-9
    assert abs(result.iloc[2] - 0.5) < 1e-9


def test_time_of_day_pct_is_negative_pre_market_and_above_one_post_market():
    import zoneinfo
    eastern = zoneinfo.ZoneInfo("America/New_York")
    pre_market_ts = int(alpaca_data.dt.datetime(2026, 7, 21, 7, 30, tzinfo=eastern).timestamp())
    post_market_ts = int(alpaca_data.dt.datetime(2026, 7, 21, 18, 0, tzinfo=eastern).timestamp())

    result = alpaca_data._time_of_day_pct(pd.Series([pre_market_ts, post_market_ts]))  # noqa: SLF001
    assert result.iloc[0] < 0
    assert result.iloc[1] > 1


def _activity_df(rows):
    frames = []
    for symbol, dv, vol in rows:
        frames.append(pd.DataFrame({
            "symbol": [symbol] * 5, "close": [100.0] * 5, "volume": [dv / 100.0] * 5,
            "volatility_15": [vol] * 5,
        }))
    return pd.concat(frames, ignore_index=True)


def test_get_stock_watchlist_falls_back_without_any_recent_data():
    watchlist = alpaca_data.get_stock_watchlist(None)
    assert "AAPL" in watchlist


def test_get_stock_watchlist_ranks_by_combined_volume_and_volatility():
    original_top_n = alpaca_data.WATCHLIST_TOP_N
    alpaca_data.WATCHLIST_TOP_N = 2
    try:
        df = _activity_df([
            ("MEGA", 10_000.0, 0.0001),
            ("FLAT", 100.0, 0.0001),
            ("CHOP", 100.0, 0.01),
        ])
        watchlist = alpaca_data.get_stock_watchlist(df)
        assert watchlist == ["CHOP", "MEGA"]
    finally:
        alpaca_data.WATCHLIST_TOP_N = original_top_n


def test_collect_dataset_rows_uses_the_cached_short_window_fetch(monkeypatch):
    called = []
    monkeypatch.setattr(alpaca_data, "fetch_recent_minute_bars", lambda symbol: called.append(symbol) or pd.DataFrame())
    alpaca_data.collect_dataset_rows(["AAPL"])
    assert called == ["AAPL"]


def test_collect_dataset_rows_one_symbol_failing_does_not_block_the_others(monkeypatch):
    def fake_fetch(symbol):
        if symbol == "BAD":
            raise RuntimeError("network error")
        return _synthetic_one_min_df(n=100)

    monkeypatch.setattr(alpaca_data, "fetch_recent_minute_bars", fake_fetch)
    result = alpaca_data.collect_dataset_rows(["BAD", "AAPL"])
    assert not result.empty
    assert set(result["symbol"]) == {"AAPL"}


def test_latest_feature_row_returns_none_when_not_enough_history(monkeypatch):
    monkeypatch.setattr(alpaca_data, "fetch_recent_minute_bars", lambda symbol: _synthetic_one_min_df(n=5))
    assert alpaca_data.latest_feature_row("AAPL") is None


def test_latest_feature_row_returns_feature_columns_plus_symbol_and_price(monkeypatch):
    monkeypatch.setattr(alpaca_data, "fetch_recent_minute_bars", lambda symbol: _synthetic_one_min_df(n=100))
    row = alpaca_data.latest_feature_row("AAPL")
    assert row is not None
    assert row["symbol"] == "AAPL"
    assert "current_price" in row
    assert "short_ma" in row
    for col in alpaca_data.FEATURE_COLUMNS:
        assert col in row


def test_load_training_dataset_returns_empty_frame_without_an_hf_key(monkeypatch):
    monkeypatch.setattr(alpaca_data, "HF_API_KEY", "")
    assert alpaca_data.load_training_dataset().empty


def test_load_training_dataset_stops_once_the_cap_is_covered(monkeypatch):
    monkeypatch.setattr(alpaca_data, "HF_API_KEY", "fake-token")
    shard_names = [f"minute/2026-07-{10 + i:02d}.parquet" for i in range(10)]
    downloaded = []

    class FakeApi:
        def __init__(self, token=None):
            pass

        def list_repo_files(self, repo_id, repo_type):
            return shard_names

    def fake_hf_hub_download(repo_id, filename, repo_type, token):
        downloaded.append(filename)
        idx = shard_names.index(filename)
        import tempfile
        day_df = pd.DataFrame({
            "symbol": ["AAPL"] * 100, "ts": [idx * 1000 + i for i in range(100)], "close": [1.0] * 100,
        })
        f = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
        day_df.to_parquet(f.name, index=False)
        return f.name

    import huggingface_hub
    monkeypatch.setattr(huggingface_hub, "HfApi", FakeApi)
    monkeypatch.setattr(huggingface_hub, "hf_hub_download", fake_hf_hub_download)

    result = alpaca_data.load_training_dataset(max_rows=150)
    assert len(result) == 150
    assert len(downloaded) < len(shard_names)


# ---------------------------------------------------------------------------
# push_minute_snapshot -- must MERGE with whatever's already in today's HF
# shard, never overwrite it (same discipline as the perps archive).
# ---------------------------------------------------------------------------
class _FakeHfApi:
    captured_upload: dict = {}

    def __init__(self, token=None):
        pass

    def repo_info(self, *, repo_id, repo_type):
        return {"id": repo_id}  # repo already exists -- create_repo fallback not exercised here

    def upload_file(self, *, path_or_fileobj, path_in_repo, repo_id, repo_type, commit_message):
        _FakeHfApi.captured_upload["df"] = pd.read_parquet(path_or_fileobj)
        _FakeHfApi.captured_upload["path_in_repo"] = path_in_repo


def test_push_minute_snapshot_merges_with_the_existing_shard(monkeypatch):
    monkeypatch.setattr(alpaca_data, "HF_API_KEY", "fake-token")
    existing_df = pd.DataFrame({"symbol": ["AAPL"], "ts": [100], "close": [150.0]})

    import tempfile
    existing_path = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False).name
    existing_df.to_parquet(existing_path, index=False)

    import huggingface_hub
    monkeypatch.setattr(huggingface_hub, "hf_hub_download", lambda **kw: existing_path)
    _FakeHfApi.captured_upload = {}
    monkeypatch.setattr(huggingface_hub, "HfApi", _FakeHfApi)

    new_df = pd.DataFrame({"symbol": ["MSFT"], "ts": [200], "close": [400.0]})
    result = alpaca_data.push_minute_snapshot(new_df)

    assert result["ok"] is True
    merged = _FakeHfApi.captured_upload["df"]
    assert set(merged["symbol"]) == {"AAPL", "MSFT"}


def test_push_minute_snapshot_dedupes_overlapping_rows(monkeypatch):
    monkeypatch.setattr(alpaca_data, "HF_API_KEY", "fake-token")
    existing_df = pd.DataFrame({"symbol": ["AAPL"], "ts": [100], "close": [150.0]})

    import tempfile
    existing_path = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False).name
    existing_df.to_parquet(existing_path, index=False)

    import huggingface_hub
    monkeypatch.setattr(huggingface_hub, "hf_hub_download", lambda **kw: existing_path)
    _FakeHfApi.captured_upload = {}
    monkeypatch.setattr(huggingface_hub, "HfApi", _FakeHfApi)

    duplicate_df = pd.DataFrame({"symbol": ["AAPL"], "ts": [100], "close": [151.0]})
    alpaca_data.push_minute_snapshot(duplicate_df)

    assert len(_FakeHfApi.captured_upload["df"]) == 1


def test_push_minute_snapshot_starts_fresh_when_no_shard_exists_yet(monkeypatch):
    monkeypatch.setattr(alpaca_data, "HF_API_KEY", "fake-token")

    import huggingface_hub

    def fail(**kw):
        raise RuntimeError("404 not found")

    monkeypatch.setattr(huggingface_hub, "hf_hub_download", fail)
    _FakeHfApi.captured_upload = {}
    monkeypatch.setattr(huggingface_hub, "HfApi", _FakeHfApi)

    new_df = pd.DataFrame({"symbol": ["AAPL"], "ts": [100], "close": [150.0]})
    result = alpaca_data.push_minute_snapshot(new_df)

    assert result["ok"] is True


def test_upload_shard_creates_the_dataset_repo_if_it_does_not_exist_yet(monkeypatch):
    """Real bug found and fixed: unlike the model-repo push (which already
    falls back to create_repo), this upload used to assume the dataset repo
    already existed -- a brand-new HF_ALPACA_DATASET_REPO name would fail
    silently on every single push until a human created it by hand."""
    monkeypatch.setattr(alpaca_data, "HF_API_KEY", "fake-token")

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
    result = alpaca_data._upload_shard(  # noqa: SLF001
        pd.DataFrame({"symbol": ["AAPL"], "ts": [1], "close": [1.0]}),
        path_in_repo="daily/AAPL.parquet", commit_message="test",
    )
    assert result["ok"] is True
    assert created == [(alpaca_data.HF_ALPACA_DATASET_REPO, "dataset")]
    assert list(_FakeHfApi.captured_upload["df"]["symbol"]) == ["AAPL"]


def test_push_minute_snapshot_returns_ok_false_without_an_hf_key(monkeypatch):
    monkeypatch.setattr(alpaca_data, "HF_API_KEY", "")
    result = alpaca_data.push_minute_snapshot(pd.DataFrame({"symbol": ["AAPL"], "ts": [1], "close": [1.0]}))
    assert result == {"ok": False, "reason": "no_hf_api_key"}


def test_push_minute_snapshot_returns_ok_false_for_an_empty_frame():
    result = alpaca_data.push_minute_snapshot(pd.DataFrame())
    assert result == {"ok": False, "reason": "no_rows"}


def test_get_symbols_with_daily_bars_lists_existing_daily_shards(monkeypatch):
    monkeypatch.setattr(alpaca_data, "HF_API_KEY", "fake-token")

    class FakeApi:
        def __init__(self, token=None):
            pass

        def list_repo_files(self, repo_id, repo_type):
            return ["daily/AAPL.parquet", "daily/MSFT.parquet", "minute/2026-07-25.parquet", "README.md"]

    import huggingface_hub
    monkeypatch.setattr(huggingface_hub, "HfApi", FakeApi)
    result = alpaca_data.get_symbols_with_daily_bars()
    assert result == {"AAPL", "MSFT"}


def test_get_symbols_with_daily_bars_returns_empty_set_without_an_hf_key(monkeypatch):
    monkeypatch.setattr(alpaca_data, "HF_API_KEY", "")
    assert alpaca_data.get_symbols_with_daily_bars() == set()
