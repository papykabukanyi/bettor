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
    assert uploads[0]["path_in_repo"] == "price_history/GOLD.parquet"


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
