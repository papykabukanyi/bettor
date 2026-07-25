"""Schwab stock data pipeline -- universe acquisition, candle parsing, and
leakage-free feature engineering. Mirrors test_perps_data.py's discipline:
every rolling feature must only look backward, the forward label must be
NaN for rows too recent to know their own outcome yet. All network mocked."""
from __future__ import annotations

import pandas as pd
import pytest

from data import schwab_data


class _FakeResponse:
    def __init__(self, text, status_code=200):
        self.text = text
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


NASDAQ_SAMPLE = (
    "Symbol|Security Name|Market Category|Test Issue|Financial Status|Round Lot Size|ETF|NextShares\n"
    "AAPL|Apple Inc. Common Stock|Q|N|N|100|N|N\n"
    "ZTEST|Some Test Issue|Q|Y|N|100|N|N\n"
    "File Creation Time: 0725202608:00|||||||\n"
)
OTHER_SAMPLE = (
    "ACT Symbol|Security Name|Exchange|CQS Symbol|ETF|Round Lot Size|Test Issue|NASDAQ Symbol\n"
    "A|Agilent Technologies, Inc. Common Stock|N|A|N|100|N|A\n"
    "File Creation Time: 0725202608:00||||||\n"
)


@pytest.fixture(autouse=True)
def _isolated_universe_cache(monkeypatch):
    monkeypatch.setattr(schwab_data, "_universe_cache", {"symbols": None, "computed_at": 0.0})


def test_get_us_stock_universe_merges_both_exchanges_and_excludes_test_issues(monkeypatch):
    def fake_get(url, timeout):
        if url == schwab_data._NASDAQ_LISTED_URL:  # noqa: SLF001
            return _FakeResponse(NASDAQ_SAMPLE)
        return _FakeResponse(OTHER_SAMPLE)

    monkeypatch.setattr(schwab_data.requests, "get", fake_get)
    universe = schwab_data.get_us_stock_universe()
    assert "AAPL" in universe
    assert "A" in universe
    assert "ZTEST" not in universe  # Test Issue == Y, excluded
    assert not any("File Creation Time" in s for s in universe)  # footer line must not leak in as a symbol


def test_get_us_stock_universe_is_cached(monkeypatch):
    calls = {"n": 0}

    def fake_get(url, timeout):
        calls["n"] += 1
        return _FakeResponse(NASDAQ_SAMPLE if "nasdaqlisted" in url else OTHER_SAMPLE)

    monkeypatch.setattr(schwab_data.requests, "get", fake_get)
    schwab_data.get_us_stock_universe()
    schwab_data.get_us_stock_universe()
    assert calls["n"] == 2  # one fetch per URL, NOT four (i.e. cached across the second call)


def test_get_us_stock_universe_survives_one_exchange_failing(monkeypatch):
    def fake_get(url, timeout):
        if "nasdaqlisted" in url:
            raise RuntimeError("network down")
        return _FakeResponse(OTHER_SAMPLE)

    monkeypatch.setattr(schwab_data.requests, "get", fake_get)
    universe = schwab_data.get_us_stock_universe()
    assert universe == ["A"]


# ---------------------------------------------------------------------------
# Market session detection -- gates when off-hours intensive training runs
# vs. live trading checks, so this must never raise (a failure here would
# otherwise stall the scheduler entirely).
# ---------------------------------------------------------------------------
def _schwab_hours_response(is_open, pre=None, regular=None, post=None):
    session_hours = {}
    if pre:
        session_hours["preMarket"] = [{"start": pre[0], "end": pre[1]}]
    if regular:
        session_hours["regularMarket"] = [{"start": regular[0], "end": regular[1]}]
    if post:
        session_hours["postMarket"] = [{"start": post[0], "end": post[1]}]
    return {"equity": {"EQ": {"isOpen": is_open, "sessionHours": session_hours}}}


def test_get_market_session_regular_hours_from_schwab(monkeypatch):
    now = schwab_data.dt.datetime.now(schwab_data.dt.timezone.utc)
    start = (now - schwab_data.dt.timedelta(hours=1)).isoformat()
    end = (now + schwab_data.dt.timedelta(hours=1)).isoformat()
    monkeypatch.setattr(
        schwab_data.schwab_client, "get",
        lambda path, params=None: _schwab_hours_response(True, regular=(start, end)),
    )
    result = schwab_data.get_market_session()
    assert result == {"session": "regular", "is_open": True, "source": "schwab"}


def test_get_market_session_outside_any_window_is_closed(monkeypatch):
    now = schwab_data.dt.datetime.now(schwab_data.dt.timezone.utc)
    start = (now - schwab_data.dt.timedelta(hours=10)).isoformat()
    end = (now - schwab_data.dt.timedelta(hours=8)).isoformat()
    monkeypatch.setattr(
        schwab_data.schwab_client, "get",
        lambda path, params=None: _schwab_hours_response(False, regular=(start, end)),
    )
    result = schwab_data.get_market_session()
    assert result == {"session": "closed", "is_open": False, "source": "schwab"}


def test_get_market_session_falls_back_when_schwab_call_fails(monkeypatch):
    def fail(path, params=None):
        raise RuntimeError("not logged in")

    monkeypatch.setattr(schwab_data.schwab_client, "get", fail)
    result = schwab_data.get_market_session()
    assert result["source"] == "fallback"
    assert result["session"] in {"closed", "pre_market", "regular", "post_market"}


def test_fallback_market_session_weekend_is_closed(monkeypatch):
    saturday = schwab_data.dt.datetime(2026, 7, 25, 12, 0)  # a real Saturday
    assert saturday.weekday() == 5
    monkeypatch.setattr(schwab_data, "_now_et", lambda: saturday)
    result = schwab_data._fallback_market_session()  # noqa: SLF001
    assert result == {"session": "closed", "is_open": False, "source": "fallback"}


def test_fallback_market_session_weekday_regular_hours():
    tuesday_at_noon = schwab_data.dt.datetime(2026, 7, 21, 12, 0)  # a real Tuesday
    assert tuesday_at_noon.weekday() == 1

    import unittest.mock
    with unittest.mock.patch.object(schwab_data, "_now_et", return_value=tuesday_at_noon):
        result = schwab_data._fallback_market_session()  # noqa: SLF001
    assert result == {"session": "regular", "is_open": True, "source": "fallback"}


def test_fallback_market_session_weekday_pre_market():
    tuesday_early = schwab_data.dt.datetime(2026, 7, 21, 6, 0)
    import unittest.mock
    with unittest.mock.patch.object(schwab_data, "_now_et", return_value=tuesday_early):
        result = schwab_data._fallback_market_session()  # noqa: SLF001
    assert result == {"session": "pre_market", "is_open": False, "source": "fallback"}


def test_fallback_market_session_weekday_late_night_is_closed():
    tuesday_midnight = schwab_data.dt.datetime(2026, 7, 21, 2, 0)
    import unittest.mock
    with unittest.mock.patch.object(schwab_data, "_now_et", return_value=tuesday_midnight):
        result = schwab_data._fallback_market_session()  # noqa: SLF001
    assert result == {"session": "closed", "is_open": False, "source": "fallback"}


def _make_daily_candles(closes, start_ms=1_700_000_000_000, step_ms=86_400_000):
    return [
        {"datetime": start_ms + i * step_ms, "open": c, "high": c, "low": c, "close": c, "volume": 1000.0}
        for i, c in enumerate(closes)
    ]


def test_candles_to_df_converts_ms_datetime_to_second_ts():
    candles = _make_daily_candles([100.0, 101.0])
    df = schwab_data._candles_to_df(candles)  # noqa: SLF001
    assert list(df["close"]) == [100.0, 101.0]
    assert df["ts"].iloc[0] == candles[0]["datetime"] // 1000


def test_fetch_daily_bars_caps_years_at_twenty(monkeypatch):
    captured = {}

    def fake_get(path, *, params):
        captured.update(params)
        return {"candles": []}

    monkeypatch.setattr(schwab_data.schwab_client, "get", fake_get)
    schwab_data.fetch_daily_bars("AAPL", years=50)
    assert captured["period"] == 20
    assert captured["periodType"] == "year"
    assert captured["frequencyType"] == "daily"


def test_fetch_minute_bars_chains_calls_to_cover_the_full_window(monkeypatch):
    """periodType=day caps at period=10 per call (Schwab's own enum), so
    covering 35 days needs multiple chained startDate/endDate calls."""
    calls = []

    def fake_get(path, *, params):
        calls.append(params)
        return {"candles": _make_daily_candles([1.0], start_ms=params["startDate"], step_ms=60_000)}

    monkeypatch.setattr(schwab_data.schwab_client, "get", fake_get)
    schwab_data.fetch_minute_bars("AAPL", days=35)
    assert len(calls) == 4  # 10 + 10 + 10 + 5 days
    for c in calls:
        assert c["periodType"] == "day"
        assert c["frequencyType"] == "minute"


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
    assert schwab_data.engineer_features(small_df).empty


def test_engineer_features_label_is_nan_for_the_most_recent_row():
    df = _synthetic_one_min_df(n=100)
    feats = schwab_data.engineer_features(df)
    assert not feats.empty
    assert feats["label_up"].iloc[-1] is pd.NA or pd.isna(feats["label_up"].iloc[-1])
    assert feats["label_up"].iloc[-2] in (0, 1)


def test_engineer_features_volume_ratio_reflects_a_recent_spike():
    """A stock trading at 5x its normal recent volume right now must show
    volume_ratio_5 meaningfully above 1 -- the whole point of the feature."""
    n = 100
    volumes = [1000.0] * (n - 5) + [5000.0] * 5  # a recent volume spike
    df = _synthetic_one_min_df(n=n)
    df["volume"] = volumes
    feats = schwab_data.engineer_features(df)
    assert feats["volume_ratio_5"].iloc[-1] > 1.5


def test_engineer_features_all_columns_present_in_feature_columns():
    df = _synthetic_one_min_df(n=100)
    feats = schwab_data.engineer_features(df)
    for col in schwab_data.FEATURE_COLUMNS:
        assert col in feats.columns


def test_time_of_day_pct_is_zero_at_open_and_one_at_close():
    import zoneinfo
    eastern = zoneinfo.ZoneInfo("America/New_York")
    # A real Tuesday -- open/close times are unambiguous (no DST edge, no holiday).
    open_ts = int(schwab_data.dt.datetime(2026, 7, 21, 9, 30, tzinfo=eastern).timestamp())
    close_ts = int(schwab_data.dt.datetime(2026, 7, 21, 16, 0, tzinfo=eastern).timestamp())
    midday_ts = int(schwab_data.dt.datetime(2026, 7, 21, 12, 45, tzinfo=eastern).timestamp())  # halfway

    result = schwab_data._time_of_day_pct(pd.Series([open_ts, close_ts, midday_ts]))  # noqa: SLF001
    assert abs(result.iloc[0] - 0.0) < 1e-9
    assert abs(result.iloc[1] - 1.0) < 1e-9
    assert abs(result.iloc[2] - 0.5) < 1e-9


def test_time_of_day_pct_is_negative_pre_market_and_above_one_post_market():
    import zoneinfo
    eastern = zoneinfo.ZoneInfo("America/New_York")
    pre_market_ts = int(schwab_data.dt.datetime(2026, 7, 21, 7, 30, tzinfo=eastern).timestamp())
    post_market_ts = int(schwab_data.dt.datetime(2026, 7, 21, 18, 0, tzinfo=eastern).timestamp())

    result = schwab_data._time_of_day_pct(pd.Series([pre_market_ts, post_market_ts]))  # noqa: SLF001
    assert result.iloc[0] < 0
    assert result.iloc[1] > 1


def _activity_df(rows):
    """rows: list of (symbol, dollar_volume_per_row, volatility_15)."""
    frames = []
    for symbol, dv, vol in rows:
        frames.append(pd.DataFrame({
            "symbol": [symbol] * 5, "close": [100.0] * 5, "volume": [dv / 100.0] * 5,
            "volatility_15": [vol] * 5,
        }))
    return pd.concat(frames, ignore_index=True)


def test_get_stock_watchlist_falls_back_without_any_recent_data():
    watchlist = schwab_data.get_stock_watchlist(None)
    assert "AAPL" in watchlist


def test_get_stock_watchlist_ranks_by_combined_volume_and_volatility():
    """Confirmed-live-style pattern (same as the Kalshi perps watchlist):
    volume and volatility can pull in different directions -- a symbol with
    the SAME volume as another but much higher volatility should win the
    tiebreak for a narrowed top-N watchlist."""
    monkeypatch_top_n = schwab_data.WATCHLIST_TOP_N
    schwab_data.WATCHLIST_TOP_N = 2
    try:
        df = _activity_df([
            ("MEGA", 10_000.0, 0.0001),  # highest volume by far, flat
            ("FLAT", 100.0, 0.0001),     # same volume as CHOP, but flat
            ("CHOP", 100.0, 0.01),       # same volume as FLAT, but far more volatile
        ])
        watchlist = schwab_data.get_stock_watchlist(df)
        assert watchlist == ["CHOP", "MEGA"]  # FLAT loses the tiebreak on volatility
    finally:
        schwab_data.WATCHLIST_TOP_N = monkeypatch_top_n


def test_collect_dataset_rows_uses_the_given_symbols_not_the_full_universe(monkeypatch):
    fetched = []
    monkeypatch.setattr(schwab_data, "fetch_minute_bars", lambda symbol, days=35: fetched.append(symbol) or pd.DataFrame())
    result = schwab_data.collect_dataset_rows(["AAPL", "MSFT"])
    assert result.empty  # no real data faked here -- just confirming which symbols got processed
    assert fetched == ["AAPL", "MSFT"]


def test_collect_dataset_rows_one_symbol_failing_does_not_block_the_others(monkeypatch):
    def fake_fetch(symbol, days=35):
        if symbol == "BAD":
            raise RuntimeError("network error")
        return _synthetic_one_min_df(n=100)

    monkeypatch.setattr(schwab_data, "fetch_minute_bars", fake_fetch)
    result = schwab_data.collect_dataset_rows(["BAD", "AAPL"])
    assert not result.empty
    assert set(result["symbol"]) == {"AAPL"}


def test_latest_feature_row_returns_none_when_not_enough_history(monkeypatch):
    monkeypatch.setattr(schwab_data, "fetch_minute_bars", lambda symbol, days=35: _synthetic_one_min_df(n=5))
    assert schwab_data.latest_feature_row("AAPL") is None


def test_latest_feature_row_returns_feature_columns_plus_symbol_and_price(monkeypatch):
    monkeypatch.setattr(schwab_data, "fetch_minute_bars", lambda symbol, days=35: _synthetic_one_min_df(n=100))
    row = schwab_data.latest_feature_row("AAPL")
    assert row is not None
    assert row["symbol"] == "AAPL"
    assert "current_price" in row
    assert "short_ma" in row
    for col in schwab_data.FEATURE_COLUMNS:
        assert col in row


def test_load_training_dataset_returns_empty_frame_without_an_hf_key(monkeypatch):
    monkeypatch.setattr(schwab_data, "HF_API_KEY", "")
    assert schwab_data.load_training_dataset().empty


def test_load_training_dataset_stops_once_the_cap_is_covered(monkeypatch):
    monkeypatch.setattr(schwab_data, "HF_API_KEY", "fake-token")
    shard_names = [f"minute/2026-07-{10 + i:02d}.parquet" for i in range(10)]
    downloaded = []

    class FakeApi:
        def __init__(self, token=None):
            pass

        def list_repo_files(self, repo_id, repo_type):
            return shard_names

    def fake_hf_hub_download(repo_id, filename, repo_type, token, tmp_path_holder=[]):
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

    result = schwab_data.load_training_dataset(max_rows=150)
    assert len(result) == 150
    assert len(downloaded) < len(shard_names)
