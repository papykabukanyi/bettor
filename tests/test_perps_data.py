"""Feature engineering + candle parsing for the perps data pipeline.

These are the leakage-free-ness checks: every technical feature must only
look backward from its own row, and the forward label must be NaN for rows
too close to "now" to know the outcome yet (that's the live-prediction row).
A regression here would silently train the model on future information.
"""
from __future__ import annotations

import pandas as pd
import pytest

from data import perps_data


def _make_candles(prices: list[float], start_ts: int = 1_700_000_000, step: int = 60):
    return [{"end_period_ts": start_ts + i * step, "price": {"close": p}} for i, p in enumerate(prices)]


def _make_hourly_before(one_min_start_ts: int, base: float = 100.0, count: int = 10):
    """Hourly candles that all END before the one-minute window starts, so
    every one-minute row's backward-merge lands on the SAME last hourly
    point -- giving every row a defined (non-NaN) trend_pct instead of the
    first N rows falling in the "no hourly history yet" gap."""
    start_ts = one_min_start_ts - count * 3600
    prices = [base + i * 0.1 for i in range(count)]
    return _make_candles(prices, start_ts=start_ts, step=3600)


def test_candles_to_frame_dedupes_and_sorts():
    candles = _make_candles([100.0, 101.0, 102.0])
    # Shuffle + duplicate one entry
    candles = [candles[2], candles[0], candles[1], candles[0]]
    df = perps_data._candles_to_frame(candles)  # noqa: SLF001
    assert list(df["close"]) == [100.0, 101.0, 102.0]
    assert list(df["ts"]) == sorted(df["ts"])


def test_candles_to_frame_skips_missing_close():
    candles = [{"end_period_ts": 1, "price": {}}, {"end_period_ts": 2, "price": {"close": 5.0}}]
    df = perps_data._candles_to_frame(candles)  # noqa: SLF001
    assert len(df) == 1


def test_candles_to_frame_empty_result_has_numeric_dtypes():
    """A newly-listed ticker's older lookback window returns zero candles.
    concat()-ing that empty frame with a populated one must not upcast the
    whole result to object dtype (which breaks pd.merge_asof downstream) --
    only an explicit numeric dtype on the empty case prevents that."""
    empty_df = perps_data._candles_to_frame([])  # noqa: SLF001
    assert str(empty_df["ts"].dtype) == "int64"
    assert str(empty_df["close"].dtype) == "float64"

    populated_df = perps_data._candles_to_frame(_make_candles([100.0, 101.0]))  # noqa: SLF001
    combined = pd.concat([empty_df, populated_df], ignore_index=True)
    assert str(combined["ts"].dtype) == "int64"


def test_engineer_features_label_is_nan_for_recent_rows():
    # 300 rows of steadily rising price, well past the minimum window (245 --
    # the 4-hour/240-minute lookback feature is the longest one).
    prices = [100.0 + i * 0.01 for i in range(300)]
    one_min_df = perps_data._candles_to_frame(_make_candles(prices))  # noqa: SLF001
    hourly_df = perps_data._candles_to_frame(_make_hourly_before(1_700_000_000))  # noqa: SLF001

    feats = perps_data.engineer_features(one_min_df, hourly_df, sentiment_score=0.0)
    assert not feats.empty

    horizon = perps_data.LABEL_HORIZON_MINUTES
    # The last `horizon` rows can't know their own future outcome yet.
    tail = feats.tail(horizon)
    assert tail["label_up"].isna().all()

    # A row with enough future data available should have a real 0/1 label.
    if len(feats) > horizon:
        earlier = feats.iloc[[-horizon - 1]]
        assert earlier["label_up"].notna().all()


def test_engineer_features_label_matches_future_direction():
    # Construct prices where the "future" close is deterministically higher.
    # Sharp jump well past the 245-row minimum window so the row right
    # before the jump still has valid (non-NaN) features.
    prices = [100.0] * 280 + [200.0] * 20
    one_min_df = perps_data._candles_to_frame(_make_candles(prices))  # noqa: SLF001
    hourly_df = perps_data._candles_to_frame(_make_hourly_before(1_700_000_000))  # noqa: SLF001

    feats = perps_data.engineer_features(one_min_df, hourly_df, sentiment_score=0.0)
    horizon = perps_data.LABEL_HORIZON_MINUTES
    labeled = feats.dropna(subset=["label_up"])
    # Rows sitting in the flat-100 region whose horizon lands in the flat-200
    # region must be labeled "up".
    jump_crossing = labeled[(labeled["close"] == 100.0)]
    if not jump_crossing.empty:
        assert (jump_crossing["label_up"] == 1).any()


def test_retry_on_rate_limit_retries_then_succeeds(monkeypatch):
    monkeypatch.setattr(perps_data.time, "sleep", lambda s: None)
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] < 2:
            raise RuntimeError("429 Too Many Requests: rate limit exceeded")
        return "ok"

    assert perps_data.retry_on_rate_limit(flaky) == "ok"
    assert calls["n"] == 2


def test_retry_on_rate_limit_does_not_retry_non_rate_limit_errors(monkeypatch):
    monkeypatch.setattr(perps_data.time, "sleep", lambda s: None)
    calls = {"n": 0}

    def always_fails():
        calls["n"] += 1
        raise RuntimeError("connection refused")

    try:
        perps_data.retry_on_rate_limit(always_fails)
        assert False, "expected the error to propagate"
    except RuntimeError:
        pass
    assert calls["n"] == 1


def _market(ticker, *, status="active", volume_24h_usd=1000.0):
    return {
        "ticker": ticker, "status": status,
        "volume_24h_notional_value_dollars": str(volume_24h_usd),
    }


@pytest.fixture(autouse=True)
def _no_volatility_ranking_network_calls(monkeypatch):
    """_rank_tickers_by_volume_and_volatility() needs the archive for its
    volatility half -- every watchlist test here defaults it to "no data"
    so ranking falls back to volume alone (deterministic, no network) and
    the suite never touches HF; tests targeting volatility specifically
    override this explicitly."""
    monkeypatch.setattr(perps_data, "_recent_volatility_by_ticker", lambda: {})
    monkeypatch.setattr(perps_data, "_TICKER_ACTIVITY_CACHE", {"volatility_by_ticker": None, "computed_at": 0.0})


def test_get_watchlist_falls_back_to_known_list_on_failure(monkeypatch):
    def _raise():
        raise RuntimeError("network down")

    monkeypatch.setattr(perps_data, "list_margin_markets", _raise)
    watchlist = perps_data.get_watchlist()
    assert watchlist == list(perps_data.KNOWN_PERP_TICKERS)


def test_get_watchlist_uses_live_listing_when_available(monkeypatch):
    monkeypatch.setattr(perps_data, "WATCHLIST_TOP_N", 0)  # no top-N narrowing for this test
    monkeypatch.setattr(perps_data, "list_margin_markets", lambda: [_market("KXBTCPERP"), _market("KXETHPERP")])
    watchlist = perps_data.get_watchlist()
    assert watchlist == ["KXBTCPERP", "KXETHPERP"]


def test_get_watchlist_excludes_inactive_instruments(monkeypatch):
    """Confirmed live: 3 of Kalshi's 16 listed perps are status=inactive
    with zero volume -- every scan cycle was wasting API calls on them."""
    monkeypatch.setattr(perps_data, "WATCHLIST_TOP_N", 0)
    monkeypatch.setattr(perps_data, "list_margin_markets", lambda: [
        _market("KXBTCPERP"), _market("KXDOTPERP", status="inactive", volume_24h_usd=0.0),
    ])
    watchlist = perps_data.get_watchlist()
    assert watchlist == ["KXBTCPERP"]


def test_get_watchlist_narrows_to_top_n_by_volume(monkeypatch):
    monkeypatch.setattr(perps_data, "WATCHLIST_TOP_N", 2)
    monkeypatch.setattr(perps_data, "list_margin_markets", lambda: [
        _market("KXBTCPERP", volume_24h_usd=100.0),
        _market("KXETHPERP", volume_24h_usd=50.0),
        _market("KXDOGEPERP", volume_24h_usd=1.0),
    ])
    watchlist = perps_data.get_watchlist()
    assert watchlist == ["KXBTCPERP", "KXETHPERP"]  # KXDOGEPERP dropped -- lowest volume, out of the top 2


def test_get_watchlist_combines_volume_and_volatility_rank(monkeypatch):
    """Confirmed live: volume and volatility are almost inversely
    correlated (the highest-volume instruments are the LEAST volatile).
    Here KXETHPERP and KXZECPERP have the SAME volume, so only volatility
    can separate them -- KXZECPERP is far more volatile and must make the
    top-2 cut ahead of KXETHPERP, even though pure volume alone would
    leave them tied."""
    monkeypatch.setattr(perps_data, "WATCHLIST_TOP_N", 2)
    monkeypatch.setattr(perps_data, "_recent_volatility_by_ticker", lambda: {
        "KXBTCPERP": 0.0001, "KXETHPERP": 0.0001, "KXZECPERP": 0.01,
    })
    perps_data.refresh_ticker_activity_cache(force=True)  # populates the cache get_watchlist() reads
    monkeypatch.setattr(perps_data, "list_margin_markets", lambda: [
        _market("KXBTCPERP", volume_24h_usd=10_000.0),  # highest volume by far
        _market("KXETHPERP", volume_24h_usd=100.0),      # same volume as ZEC, but flat
        _market("KXZECPERP", volume_24h_usd=100.0),       # same volume as ETH, but far more volatile
    ])
    watchlist = perps_data.get_watchlist()
    assert watchlist == ["KXBTCPERP", "KXZECPERP"]  # KXETHPERP loses the tiebreak on volatility


def test_get_watchlist_never_triggers_a_synchronous_archive_load(monkeypatch):
    """Confirmed live root cause of a fresh Render OOM: get_watchlist() is
    called inline from /api/status on every dashboard poll. It used to
    trigger a full archive download+load itself whenever the volatility
    cache was empty/stale, which could run concurrently with the startup
    training thread's own full-size archive load right after a cold start --
    doubling the heaviest operation this process does at the moment memory
    is already tightest. Now it must only ever read whatever's cached."""
    def fail_if_called():
        raise AssertionError("get_watchlist() must never call _recent_volatility_by_ticker itself")

    monkeypatch.setattr(perps_data, "_recent_volatility_by_ticker", fail_if_called)
    monkeypatch.setattr(perps_data, "WATCHLIST_TOP_N", 0)
    monkeypatch.setattr(perps_data, "list_margin_markets", lambda: [_market("KXBTCPERP")])
    watchlist = perps_data.get_watchlist()  # cache is empty (autouse fixture) -- must fall back, not load
    assert watchlist == ["KXBTCPERP"]


def test_refresh_ticker_activity_cache_skips_when_fresh(monkeypatch):
    calls = {"n": 0}

    def _fake():
        calls["n"] += 1
        return {"KXBTCPERP": 0.001}

    monkeypatch.setattr(perps_data, "_recent_volatility_by_ticker", _fake)
    perps_data.refresh_ticker_activity_cache(force=True)
    assert calls["n"] == 1
    perps_data.refresh_ticker_activity_cache()  # still fresh -- should skip
    assert calls["n"] == 1


def test_refresh_ticker_activity_cache_force_always_recomputes(monkeypatch):
    calls = {"n": 0}

    def _fake():
        calls["n"] += 1
        return {"KXBTCPERP": 0.001}

    monkeypatch.setattr(perps_data, "_recent_volatility_by_ticker", _fake)
    perps_data.refresh_ticker_activity_cache(force=True)
    perps_data.refresh_ticker_activity_cache(force=True)
    assert calls["n"] == 2


def test_recent_volatility_by_ticker_bounds_the_archive_load(monkeypatch):
    """Deliberately far smaller than load_training_dataset()'s training-grade
    default (90 shards / MAX_TRAIN_ROWS rows) -- see this function's own
    docstring for why an unbounded load here caused a live OOM."""
    monkeypatch.undo()  # this test targets the REAL _recent_volatility_by_ticker, not the autouse fixture's stub
    captured = {}

    def _fake_load(*, max_shards=90, max_rows=None):
        captured["max_shards"] = max_shards
        captured["max_rows"] = max_rows
        return pd.DataFrame()

    monkeypatch.setattr(perps_data, "load_training_dataset", _fake_load)
    perps_data._recent_volatility_by_ticker()
    assert captured["max_shards"] <= 5
    assert captured["max_rows"] <= 5000


def test_get_active_tickers_includes_everything_active_unnarrowed(monkeypatch):
    """Data collection must keep archiving history for every active coin,
    not just today's top-N watchlist -- otherwise a coin left out of the
    watchlist could never accumulate the data needed to prove it deserves
    a spot later (get_watchlist()'s own ranking depends on this archive)."""
    monkeypatch.setattr(perps_data, "list_margin_markets", lambda: [
        _market("KXBTCPERP", volume_24h_usd=10_000.0),
        _market("KXDOGEPERP", volume_24h_usd=1.0),  # would be dropped by get_watchlist()'s top-N
        _market("KXDOTPERP", status="inactive", volume_24h_usd=0.0),
    ])
    tickers = perps_data.get_active_tickers()
    assert tickers == ["KXBTCPERP", "KXDOGEPERP"]  # inactive excluded, but nothing narrowed by rank


def test_collect_dataset_rows_uses_active_tickers_not_the_narrowed_watchlist(monkeypatch):
    monkeypatch.setattr(perps_data, "get_active_tickers", lambda: ["KXBTCPERP", "KXDOGEPERP"])

    def fail_if_called():
        raise AssertionError("collect_dataset_rows must use get_active_tickers(), not the narrowed get_watchlist()")

    monkeypatch.setattr(perps_data, "get_watchlist", fail_if_called)
    monkeypatch.setattr(perps_data, "fetch_candle_frames", lambda ticker: (pd.DataFrame(), pd.DataFrame()))
    result = perps_data.collect_dataset_rows()
    assert result.empty  # no real data faked here -- just confirming which ticker source got called


def test_load_training_dataset_merges_local_and_hf_not_just_fallback(monkeypatch, tmp_path):
    """A long-running deployment accumulates its own local shards from its
    rolling recent-window collection -- load_training_dataset must still
    pull in the deeper HF-archived history rather than treating local
    shards (however many there are) as reason enough to skip HF entirely."""
    monkeypatch.setattr(perps_data, "DATA_DIR", tmp_path)
    monkeypatch.setattr(perps_data, "HF_API_KEY", "fake-token")
    monkeypatch.setattr(perps_data, "HF_DATASET_REPO", "someuser/kalshi-perps-data")

    shard_dir = tmp_path / "perps_dataset"
    shard_dir.mkdir()
    local_df = pd.DataFrame({"ticker": ["KXBTCPERP"], "ts": [100], "close": [1.0]})
    local_df.to_parquet(shard_dir / "2026-07-22.parquet", index=False)
    # A second local file so the OLD "local >= 2 shortcut" logic (if it were
    # still there) would have skipped HF entirely.
    local_df2 = pd.DataFrame({"ticker": ["KXBTCPERP"], "ts": [200], "close": [1.1]})
    local_df2.to_parquet(shard_dir / "2026-07-21.parquet", index=False)

    hf_df = pd.DataFrame({"ticker": ["KXBTCPERP"], "ts": [1], "close": [0.5]})
    hf_shard_path = tmp_path / "hf_shard.parquet"
    hf_df.to_parquet(hf_shard_path, index=False)

    class FakeApi:
        def __init__(self, token=None):
            pass

        def list_repo_files(self, repo_id, repo_type):
            return ["data/2026-06-01.parquet", "unrelated/other_pipeline_shard.parquet"]

    def fake_hf_hub_download(repo_id, filename, repo_type, token):
        return str(hf_shard_path)

    import huggingface_hub
    monkeypatch.setattr(huggingface_hub, "HfApi", FakeApi)
    monkeypatch.setattr(huggingface_hub, "hf_hub_download", fake_hf_hub_download)

    result = perps_data.load_training_dataset()
    # All three ts values (100, 200, 1) must be present -- HF wasn't skipped
    # just because 2 local shards already existed, and the unrelated-prefix
    # file was correctly excluded (only one HF file matched "data/").
    assert sorted(result["ts"].tolist()) == [1, 100, 200]


def test_load_training_dataset_caps_to_max_rows_keeping_most_recent(monkeypatch, tmp_path):
    """The HF archive grows every day forever; without a cap this would
    eventually load more data into memory than a memory-constrained
    deployment can hold. The cap must keep the MOST RECENT rows (by ts),
    not an arbitrary slice."""
    monkeypatch.setattr(perps_data, "DATA_DIR", tmp_path)
    monkeypatch.setattr(perps_data, "HF_API_KEY", "")

    shard_dir = tmp_path / "perps_dataset"
    shard_dir.mkdir()
    local_df = pd.DataFrame({
        "ticker": ["KXBTCPERP"] * 10,
        "ts": list(range(10)),
        "close": [float(i) for i in range(10)],
    })
    local_df.to_parquet(shard_dir / "2026-07-22.parquet", index=False)

    result = perps_data.load_training_dataset(max_rows=4)
    assert len(result) == 4
    assert sorted(result["ts"].tolist()) == [6, 7, 8, 9]


def test_load_training_dataset_excludes_lookalike_paths_from_another_pipeline(monkeypatch, tmp_path):
    """A previously-used HF account had an UNRELATED pipeline that also
    wrote parquet files under a "data/" prefix (e.g.
    "data/pregame_schedule/shard_....parquet") -- a bare "data/" prefix
    match isn't unique enough to exclude those, and they'd get fully
    downloaded (not just flagged after the fact) before any schema check
    ever ran. Only the exact "data/YYYY-MM-DD.parquet" shape this module
    itself writes should ever be fetched."""
    monkeypatch.setattr(perps_data, "DATA_DIR", tmp_path)
    monkeypatch.setattr(perps_data, "HF_API_KEY", "fake-token")
    monkeypatch.setattr(perps_data, "HF_DATASET_REPO", "someuser/kalshi-perps-data")

    hf_df = pd.DataFrame({"ticker": ["KXBTCPERP"], "ts": [1], "close": [0.5]})
    hf_shard_path = tmp_path / "hf_shard.parquet"
    hf_df.to_parquet(hf_shard_path, index=False)

    downloaded = []

    class FakeApi:
        def __init__(self, token=None):
            pass

        def list_repo_files(self, repo_id, repo_type):
            return [
                "data/2026-06-01.parquet",
                "data/pregame_schedule/shard_20260713_084300.parquet",
                "data/pregame_schedule/shard_20260720_023912.parquet",
            ]

    def fake_hf_hub_download(repo_id, filename, repo_type, token):
        downloaded.append(filename)
        return str(hf_shard_path)

    import huggingface_hub
    monkeypatch.setattr(huggingface_hub, "HfApi", FakeApi)
    monkeypatch.setattr(huggingface_hub, "hf_hub_download", fake_hf_hub_download)

    perps_data.load_training_dataset()
    assert downloaded == ["data/2026-06-01.parquet"]
