"""Data pipeline for Kalshi's 15-minute commodity markets, priced on each
commodity's ETF from Alpaca (SIP bars, live stream) only."""
from __future__ import annotations

import pandas as pd
import pytest

from data import kalshi_15m_metals_data as k


@pytest.fixture(autouse=True)
def _isolated_data_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(k, "DATA_DIR", tmp_path)
    monkeypatch.setattr(k, "HF_API_KEY", "")
    # Alpaca news otherwise -- every test in this file gets a safe,
    # deterministic default; tests that specifically care about sentiment
    # wiring override this themselves.
    monkeypatch.setattr(k, "get_generic_sentiment", lambda query, cache_key: {"query": query, "sentiment_score": 0.0, "headline_volume": 0, "computed_at": 0.0})


def test_get_universe_is_every_commodity_with_an_alpaca_etf():
    assert set(k.get_universe()) == {"GOLD", "SILVER", "COPPER", "PLATINUM", "PALLADIUM", "WTI", "NATGAS"}
    assert k.ETF_SYMBOL["GOLD"] == "GLD" and k.ETF_SYMBOL["WTI"] == "USO"


def test_fetch_latest_price_returns_none_for_an_unknown_metal():
    assert k.fetch_latest_price("NOT_A_REAL_METAL") is None


def test_fetch_latest_price_reads_the_alpaca_stream_first(monkeypatch):
    from data import alpaca_stream
    monkeypatch.setattr(alpaca_stream, "latest_price", lambda kind, symbol, max_age_sec: {"price": 380.5, "at": 1_700_000_100.0}
                        if (kind, symbol) == ("stocks", "GLD") else None)
    monkeypatch.setattr(k, "_fetch_alpaca_1m_chunk", lambda *a: (_ for _ in ()).throw(AssertionError("no REST when the stream is fresh")))
    assert k.fetch_latest_price("GOLD") == {"price": 380.5, "ts": 1_700_000_100}


def test_fetch_latest_price_falls_back_to_the_last_sip_bar(monkeypatch):
    from data import alpaca_stream
    monkeypatch.setattr(alpaca_stream, "latest_price", lambda *a, **kw: None)
    monkeypatch.setattr(k, "_fetch_alpaca_1m_chunk", lambda symbol, start, end: pd.DataFrame(
        {"ts": [1_700_000_000, 1_700_000_060], "close": [4378.0, 4379.0]}))
    assert k.fetch_latest_price("GOLD") == {"price": 4379.0, "ts": 1_700_000_060}


def test_fetch_latest_price_returns_none_without_any_alpaca_data(monkeypatch):
    from data import alpaca_stream
    monkeypatch.setattr(alpaca_stream, "latest_price", lambda *a, **kw: None)
    monkeypatch.setattr(k, "_fetch_alpaca_1m_chunk", lambda *a: pd.DataFrame())
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


# ---------------------------------------------------------------------------
# Real gap found and fixed: the raw price-history window used to be local-
# disk-only, so a container restart (common: redeploys, the recurring
# dead-HF-key fix cycle) wiped it and reset the 245-row/~4h clock back to
# zero every time -- confirmed live: this pipeline went days without ever
# once reaching MIN_ROWS_FOR_FEATURES. Now also backed up to/restored from
# HF -- see this module's own docstring.
# ---------------------------------------------------------------------------
class _FakeHfApi:
    captured_upload: dict = {}

    def __init__(self, token=None):
        pass

    def repo_info(self, *, repo_id, repo_type):
        return {"id": repo_id}

    def upload_file(self, *, path_or_fileobj, path_in_repo, repo_id, repo_type, commit_message):
        _FakeHfApi.captured_upload.setdefault("uploads", []).append(
            {"path_in_repo": path_in_repo, "df": pd.read_parquet(path_or_fileobj)},
        )


def test_load_price_history_restores_from_hf_when_local_file_is_missing(monkeypatch):
    monkeypatch.setattr(k, "HF_API_KEY", "fake-token")
    backup = pd.DataFrame({"ts": [1_700_000_000, 1_700_000_060], "close": [100.0, 101.0]})

    import huggingface_hub
    tmp_file = k.DATA_DIR / "hf_backup.parquet"
    backup.to_parquet(tmp_file, index=False)
    monkeypatch.setattr(huggingface_hub, "hf_hub_download", lambda **kw: str(tmp_file))

    result = k._load_price_history("GOLD")  # noqa: SLF001
    assert len(result) == 2
    assert result["close"].tolist() == [100.0, 101.0]
    # Restored data is also written back to local disk, so the very next
    # local load doesn't need another HF round trip.
    assert k._price_history_path("GOLD").exists()  # noqa: SLF001


def test_load_price_history_falls_back_to_empty_when_hf_has_no_backup_either(monkeypatch):
    monkeypatch.setattr(k, "HF_API_KEY", "fake-token")
    import huggingface_hub
    monkeypatch.setattr(huggingface_hub, "hf_hub_download", lambda **kw: (_ for _ in ()).throw(RuntimeError("404")))

    result = k._load_price_history("GOLD")  # noqa: SLF001
    assert result.empty


def test_save_price_history_pushes_to_hf_when_the_rate_limit_window_has_elapsed(monkeypatch):
    monkeypatch.setattr(k, "HF_API_KEY", "fake-token")
    import huggingface_hub
    _FakeHfApi.captured_upload = {}
    monkeypatch.setattr(huggingface_hub, "HfApi", _FakeHfApi)
    k._last_price_history_push_at.clear()  # noqa: SLF001

    df = pd.DataFrame({"ts": [1_700_000_000], "close": [100.0]})
    k._save_price_history("GOLD", df)  # noqa: SLF001

    uploads = _FakeHfApi.captured_upload["uploads"]
    assert len(uploads) == 1
    assert uploads[0]["path_in_repo"] == "price_history_alpaca/GOLD.parquet"


def test_save_price_history_skips_the_hf_push_within_the_rate_limit_window(monkeypatch):
    monkeypatch.setattr(k, "HF_API_KEY", "fake-token")
    import huggingface_hub
    _FakeHfApi.captured_upload = {}
    monkeypatch.setattr(huggingface_hub, "HfApi", _FakeHfApi)
    k._last_price_history_push_at.clear()  # noqa: SLF001

    df = pd.DataFrame({"ts": [1_700_000_000], "close": [100.0]})
    k._save_price_history("GOLD", df)  # first push -- goes through  # noqa: SLF001
    k._save_price_history("GOLD", df)  # second, immediately after -- rate-limited  # noqa: SLF001

    assert len(_FakeHfApi.captured_upload["uploads"]) == 1


def test_save_price_history_with_push_to_hf_false_never_pushes(monkeypatch):
    monkeypatch.setattr(k, "HF_API_KEY", "fake-token")
    import huggingface_hub
    _FakeHfApi.captured_upload = {}
    monkeypatch.setattr(huggingface_hub, "HfApi", _FakeHfApi)
    k._last_price_history_push_at.clear()  # noqa: SLF001

    df = pd.DataFrame({"ts": [1_700_000_000], "close": [100.0]})
    k._save_price_history("GOLD", df, push_to_hf=False)  # noqa: SLF001

    assert _FakeHfApi.captured_upload.get("uploads", []) == []


def test_save_price_history_push_is_a_no_op_without_an_hf_key():
    # _isolated_data_dir already sets HF_API_KEY = "" -- confirms this
    # never even tries to import huggingface_hub in that case.
    df = pd.DataFrame({"ts": [1_700_000_000], "close": [100.0]})
    k._save_price_history("GOLD", df)  # noqa: SLF001 -- must not raise


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
    # sentiment_score is NOT one of them (it was a real, undisclosed gap,
    # since fixed) -- see test_engineer_metals_features_broadcasts_
    # sentiment_score_across_every_row below for that coverage instead.
    for col in ("atr_pct", "stoch_k", "volume_ratio_5", "dollar_volume_z", "oi_change_pct", "spread_pct"):
        assert col not in result.columns


def test_engineer_metals_features_broadcasts_sentiment_score_across_every_row():
    prices = [100.0 + i * 0.01 for i in range(300)]
    result = k.engineer_metals_features(_synthetic_price_df(prices), sentiment_score=0.42)
    assert not result.empty
    assert (result["sentiment_score"] == 0.42).all()


def test_engineer_metals_features_defaults_sentiment_score_to_zero():
    prices = [100.0 + i * 0.01 for i in range(300)]
    result = k.engineer_metals_features(_synthetic_price_df(prices))
    assert (result["sentiment_score"] == 0.0).all()


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


def test_collect_dataset_rows_wires_the_fetched_sentiment_score_through(monkeypatch):
    prices = [100.0 + i * 0.01 for i in range(300)]
    for i, p in enumerate(prices):
        k._append_price_point("GOLD", ts=1_700_000_000 + i * 60, price=p)  # noqa: SLF001

    captured = {}

    def fake_sentiment(query, cache_key):
        captured["query"] = query
        captured["cache_key"] = cache_key
        return {"sentiment_score": 0.73, "headline_volume": 4}

    monkeypatch.setattr(k, "get_generic_sentiment", fake_sentiment)
    monkeypatch.setattr(k, "fetch_latest_price", lambda metal: {"price": 105.0, "ts": 1_700_000_000 + 300 * 60})

    result = k.collect_dataset_rows(["GOLD"])
    assert (result["sentiment_score"] == 0.73).all()
    assert captured["cache_key"] == "GOLD"
    assert captured["query"] == "GOLD"


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


def test_latest_feature_row_fetches_a_fresh_sentiment_reading(monkeypatch):
    prices = [100.0 + i * 0.01 for i in range(300)]
    for i, p in enumerate(prices):
        k._append_price_point("SILVER", ts=1_700_000_000 + i * 60, price=p)  # noqa: SLF001

    monkeypatch.setattr(k, "get_generic_sentiment", lambda query, cache_key: {"sentiment_score": -0.5, "headline_volume": 2})
    row = k.latest_feature_row("SILVER")
    assert row["sentiment_score"] == -0.5


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


# ---------------------------------------------------------------------------
# backfill_minute_history / _fetch_alpaca_1m_chunk -- the commodity ETF's
# SIP 1-minute bars from Alpaca.
# ---------------------------------------------------------------------------
def test_fetch_alpaca_1m_chunk_reads_sip_bars_by_their_start(monkeypatch):
    from data import alpaca_client
    calls = []

    def fake_bars(symbols, *, timeframe, start, end, feed):
        calls.append((symbols, feed))
        return {"GLD": [{"t": "2023-11-14T22:13:20Z", "c": 100.0}, {"t": "2023-11-14T22:14:20Z", "c": 101.0}]}

    monkeypatch.setattr(alpaca_client, "get_bars", fake_bars)
    result = k._fetch_alpaca_1m_chunk("GLD", 1_700_000_000, 1_700_000_100)  # noqa: SLF001
    assert calls == [(["GLD"], "sip")]
    assert list(result["ts"]) == [1_700_000_000, 1_700_000_060] and list(result["close"]) == [100.0, 101.0]


def test_fetch_alpaca_1m_chunk_returns_empty_on_a_failure(monkeypatch):
    from data import alpaca_client
    monkeypatch.setattr(alpaca_client, "get_bars", lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("403")))
    assert k._fetch_alpaca_1m_chunk("GLD", 1_700_000_000, 1_700_000_100).empty  # noqa: SLF001
    monkeypatch.setattr(alpaca_client, "get_bars", lambda *a, **kw: {})
    assert k._fetch_alpaca_1m_chunk("GLD", 1_700_000_000, 1_700_000_100).empty  # noqa: SLF001


def test_backfill_minute_history_requires_hf_api_key():
    result = k.backfill_minute_history(["GOLD"], days=5)  # _isolated_data_dir's own default: HF_API_KEY=""
    assert result == {"ok": False, "reason": "no_hf_api_key"}


def test_backfill_minute_history_caps_days(monkeypatch):
    monkeypatch.setattr(k, "HF_API_KEY", "fake-token")
    captured_windows = []

    def fake_chunk(symbol, start_ts, end_ts):
        captured_windows.append((start_ts, end_ts))
        return pd.DataFrame()

    monkeypatch.setattr(k, "_fetch_alpaca_1m_chunk", fake_chunk)
    k.backfill_minute_history(["GOLD"], days=9999)  # absurdly large -- must be silently capped

    total_span = captured_windows[-1][1] - captured_windows[0][0]
    assert total_span <= k._MAX_BACKFILL_DAYS * 86400 + 60  # noqa: SLF001 -- small slack for wall-clock jitter between now() calls


def test_backfill_minute_history_chunks_requests_within_the_real_per_request_limit(monkeypatch):
    monkeypatch.setattr(k, "HF_API_KEY", "fake-token")
    captured_windows = []

    def fake_chunk(symbol, start_ts, end_ts):
        captured_windows.append((start_ts, end_ts))
        return pd.DataFrame()

    monkeypatch.setattr(k, "_fetch_alpaca_1m_chunk", fake_chunk)
    k.backfill_minute_history(["GOLD"], days=20)  # bigger than one chunk's own real per-request limit

    assert len(captured_windows) >= 2
    for start_ts, end_ts in captured_windows:
        assert (end_ts - start_ts) <= k._MAX_DAYS_PER_REQUEST * 86400  # noqa: SLF001


def test_backfill_minute_history_writes_real_feature_rows_to_hf(monkeypatch):
    monkeypatch.setattr(k, "HF_API_KEY", "fake-token")
    import huggingface_hub

    _FakeHfApi.captured_upload = {}
    monkeypatch.setattr(huggingface_hub, "HfApi", _FakeHfApi)
    monkeypatch.setattr(huggingface_hub, "hf_hub_download", lambda **kw: (_ for _ in ()).throw(RuntimeError("no existing shard")))

    n = 300  # enough real rows to clear MIN_ROWS_FOR_FEATURES (245)
    base_ts = 1_700_000_000
    ts_list = [base_ts + i * 60 for i in range(n)]
    closes = [100.0 + (i % 10) * 0.1 for i in range(n)]
    monkeypatch.setattr(
        k, "_fetch_alpaca_1m_chunk",
        lambda symbol, start_ts, end_ts: pd.DataFrame({"ts": ts_list, "close": closes}),
    )

    result = k.backfill_minute_history(["GOLD"], days=5)

    assert result["ok"] is True
    assert result["metals_processed"] == 1
    assert result["dates_written"] >= 1
    uploads = _FakeHfApi.captured_upload["uploads"]
    assert uploads
    written_df = uploads[0]["df"]
    assert (written_df["symbol"] == "GOLD").all()
    for col in k.METALS_FEATURE_COLUMNS:
        assert col in written_df.columns


def test_backfill_minute_history_skips_a_metal_with_no_real_data(monkeypatch):
    monkeypatch.setattr(k, "HF_API_KEY", "fake-token")
    monkeypatch.setattr(k, "_fetch_alpaca_1m_chunk", lambda symbol, start_ts, end_ts: pd.DataFrame())

    result = k.backfill_minute_history(["GOLD"], days=5)

    assert result == {"ok": True, "metals_processed": 0, "metals_requested": 1, "dates_written": 0}


def test_backfill_minute_history_skips_an_unknown_metal(monkeypatch):
    monkeypatch.setattr(k, "HF_API_KEY", "fake-token")
    result = k.backfill_minute_history(["NOT_A_REAL_METAL"], days=5)
    assert result["metals_requested"] == 1
    assert result["metals_processed"] == 0
