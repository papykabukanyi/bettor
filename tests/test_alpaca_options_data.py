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


def test_select_contract_prefers_a_liquid_contract_over_a_numerically_nearer_illiquid_one(monkeypatch):
    """Real improvement: a contract with zero open interest can be
    numerically "nearest the money" and still be nearly unfillable at a
    real price -- Alpaca's own contract objects include open_interest
    directly, so there's no excuse to ignore it."""
    monkeypatch.setattr(
        aod.alpaca_client, "get_option_contracts",
        lambda **kw: [
            _contract(symbol="nearest_but_illiquid", strike_price=195.5, open_interest=0),
            _contract(symbol="slightly_farther_but_liquid", strike_price=197.0, open_interest=500),
        ],
    )
    contract = aod.select_contract("AAPL", direction="up", current_price=195.0)
    assert contract["symbol"] == "slightly_farther_but_liquid"


# ---------------------------------------------------------------------------
# Vertical debit spreads -- confirmed via Alpaca's own docs that this
# account's real options_approved_level (3) supports "Buy a call spread"/
# "Buy a put spread" as a genuine order_class="mleg" order.
# ---------------------------------------------------------------------------
def _spread_universe(*, option_type: str, long_strike: float, long_symbol: str):
    """A fake get_option_contracts() that answers BOTH calls
    select_spread_contracts() makes: the wide long-leg lookup (via
    select_contract, whatever expiration window) and the narrow same-
    expiration short-leg lookup (expiration_date_gte == expiration_date_lte).
    Distinguishes them by that narrowing, exactly like the real underlying
    /v2/options/contracts filters would."""
    long_contract = _contract(symbol=long_symbol, type=option_type, strike_price=long_strike, expiration_date="2024-02-23")
    short_pool = [
        _contract(symbol="short_near", type=option_type, strike_price=long_strike + 5.0 if option_type == "call" else long_strike - 5.0, expiration_date="2024-02-23"),
        _contract(symbol="short_far", type=option_type, strike_price=long_strike + 10.0 if option_type == "call" else long_strike - 10.0, expiration_date="2024-02-23"),
    ]

    def fake(*, underlying_symbols, expiration_date_gte, expiration_date_lte, option_type):
        if expiration_date_gte == expiration_date_lte:
            return short_pool
        return [long_contract]

    return fake


def test_select_spread_contracts_picks_a_call_spread_with_short_strike_above_long(monkeypatch):
    monkeypatch.setattr(
        aod.alpaca_client, "get_option_contracts",
        _spread_universe(option_type="call", long_strike=195.0, long_symbol="AAPL240223C00195000"),
    )
    result = aod.select_spread_contracts("AAPL", direction="up", current_price=195.0)
    assert result is not None
    long_contract, short_contract = result
    assert long_contract["symbol"] == "AAPL240223C00195000"
    assert float(short_contract["strike_price"]) > float(long_contract["strike_price"])
    assert short_contract["symbol"] == "short_near"  # nearest strike at/beyond the configured width


def test_select_spread_contracts_picks_a_put_spread_with_short_strike_below_long(monkeypatch):
    monkeypatch.setattr(
        aod.alpaca_client, "get_option_contracts",
        _spread_universe(option_type="put", long_strike=195.0, long_symbol="AAPL240223P00195000"),
    )
    result = aod.select_spread_contracts("AAPL", direction="down", current_price=195.0)
    assert result is not None
    long_contract, short_contract = result
    assert float(short_contract["strike_price"]) < float(long_contract["strike_price"])
    assert short_contract["symbol"] == "short_near"


def test_select_spread_contracts_returns_none_when_no_short_leg_is_far_enough_out(monkeypatch):
    def fake(*, underlying_symbols, expiration_date_gte, expiration_date_lte, option_type):
        if expiration_date_gte == expiration_date_lte:
            return [_contract(symbol="too_close", type="call", strike_price=196.0, expiration_date="2024-02-23")]
        return [_contract(symbol="AAPL240223C00195000", type="call", strike_price=195.0, expiration_date="2024-02-23")]

    monkeypatch.setattr(aod.alpaca_client, "get_option_contracts", fake)
    assert aod.select_spread_contracts("AAPL", direction="up", current_price=195.0) is None


def test_select_spread_contracts_returns_none_without_a_long_leg(monkeypatch):
    monkeypatch.setattr(aod.alpaca_client, "get_option_contracts", lambda **kw: [])
    assert aod.select_spread_contracts("AAPL", direction="up", current_price=195.0) is None


def test_select_spread_contracts_prefers_a_liquid_short_leg(monkeypatch):
    def fake(*, underlying_symbols, expiration_date_gte, expiration_date_lte, option_type):
        if expiration_date_gte == expiration_date_lte:
            return [
                _contract(symbol="illiquid_near", type="call", strike_price=200.0, expiration_date="2024-02-23", open_interest=0),
                _contract(symbol="liquid_farther", type="call", strike_price=202.0, expiration_date="2024-02-23", open_interest=500),
            ]
        return [_contract(symbol="AAPL240223C00195000", type="call", strike_price=195.0, expiration_date="2024-02-23")]

    monkeypatch.setattr(aod.alpaca_client, "get_option_contracts", fake)
    result = aod.select_spread_contracts("AAPL", direction="up", current_price=195.0)
    assert result[1]["symbol"] == "liquid_farther"


def test_select_contract_falls_back_to_the_full_pool_when_none_are_liquid(monkeypatch):
    """No contract clears the liquidity bar -- a real fill still beats no
    trade at all, so this must fall back to ranking the full tradable set
    by strike proximity, not return None."""
    monkeypatch.setattr(
        aod.alpaca_client, "get_option_contracts",
        lambda **kw: [
            _contract(symbol="far", strike_price=220.0, open_interest=0),
            _contract(symbol="near", strike_price=196.0, open_interest=0),
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


def test_load_training_dataset_returns_empty_frame_when_listing_hangs(monkeypatch):
    """Real, confirmed production incident on the equities sibling of this
    function: called synchronously from a Flask request handler,
    huggingface_hub's own internal shared-session lock can hang
    indefinitely inside list_repo_files -- confirmed live: gunicorn's own
    WORKER TIMEOUT fired and SIGABRT-killed the worker. A hard timeout
    must convert that into a clean empty-result degradation instead."""
    monkeypatch.setattr(aod, "HF_API_KEY", "fake-token")
    monkeypatch.setattr(aod, "_LOAD_TRAINING_DATASET_LIST_TIMEOUT_SEC", 0.05)

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
    result = aod.load_training_dataset()
    elapsed = real_time.monotonic() - start

    assert result.empty
    assert elapsed < 0.4


def test_load_training_dataset_skips_a_hanging_shard_and_continues(monkeypatch):
    monkeypatch.setattr(aod, "HF_API_KEY", "fake-token")
    monkeypatch.setattr(aod, "_LOAD_TRAINING_DATASET_SHARD_TIMEOUT_SEC", 0.3)
    shard_names = ["minute/2026-07-10.parquet", "minute/2026-07-11.parquet", "minute/2026-07-12.parquet"]

    class FakeApi:
        def __init__(self, token=None):
            pass

        def list_repo_files(self, repo_id, repo_type):
            return shard_names

    def fake_hf_hub_download(repo_id, filename, repo_type, token):
        if filename == "minute/2026-07-11.parquet":
            import time as t
            t.sleep(1.5)  # the hanging shard -- comfortably longer than the 0.3s timeout above even under system load
        import tempfile
        idx = shard_names.index(filename)
        day_df = pd.DataFrame({"symbol": ["AAPL"] * 10, "ts": [idx * 1000 + i for i in range(10)], "close": [1.0] * 10})
        f = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
        day_df.to_parquet(f.name, index=False)
        return f.name

    import huggingface_hub
    monkeypatch.setattr(huggingface_hub, "HfApi", FakeApi)
    monkeypatch.setattr(huggingface_hub, "hf_hub_download", fake_hf_hub_download)

    result = aod.load_training_dataset()

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

    def create_commit(self, *, repo_id, repo_type, operations, commit_message):
        pass  # backfill_minute_history's batched uploader -- no-op unless a test overrides it


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


# ---------------------------------------------------------------------------
# backfill_minute_history -- deep historical catch-up for the options-
# underlying archive. Independent copy of alpaca_data's own version
# (deliberately not shared -- separate HF repo, separate universe), same
# rationale: the live collector only ever writes TODAY's shard.
# ---------------------------------------------------------------------------
class _FakeHfApiBatch(_FakeHfApi):
    """create_commit-based fake -- backfill_minute_history batches many
    date-shards into a single multi-file commit (same rate-limit incident
    and fix as alpaca_data's own version), so its tests must mock
    create_commit, not upload_file."""

    def create_commit(self, *, repo_id, repo_type, operations, commit_message):
        for op in operations:
            _FakeHfApiBatch.captured_upload.setdefault("commits", []).append(
                {"path_in_repo": op.path_in_repo, "df": pd.read_parquet(op.path_or_fileobj)}
            )


def test_backfill_minute_history_splits_rows_by_calendar_date_and_uploads_one_shard_per_date(monkeypatch):
    monkeypatch.setattr(aod, "HF_API_KEY", "fake-token")
    fake_feats = pd.DataFrame({
        "ts": [1767225600, 1767312000], "close": [100.0, 101.0], "label_up": [1, 0],
    })
    monkeypatch.setattr(aod, "fetch_minute_bars", lambda symbol, *, days: pd.DataFrame({"ts": [1], "close": [1.0]}))
    monkeypatch.setattr(aod, "engineer_features", lambda df, *, sentiment_score: fake_feats.copy())

    import huggingface_hub

    def fail(**kw):
        raise RuntimeError("no existing shard")

    monkeypatch.setattr(huggingface_hub, "hf_hub_download", fail)
    _FakeHfApiBatch.captured_upload = {}
    monkeypatch.setattr(huggingface_hub, "HfApi", _FakeHfApiBatch)

    result = aod.backfill_minute_history(["AAPL"], days=90)

    assert result["ok"] is True
    assert result["symbols_processed"] == 1
    assert result["dates_written"] == 2
    uploads = _FakeHfApiBatch.captured_upload["commits"]
    paths = sorted(u["path_in_repo"] for u in uploads)
    assert paths == ["minute/2026-01-01.parquet", "minute/2026-01-02.parquet"]


def test_backfill_minute_history_batches_many_dates_into_few_commits(monkeypatch):
    """Real, confirmed incident on the stocks sibling: one commit per
    calendar date hit HF's 128-commits/hour repo cap partway through a
    251-date backfill and silently dropped ~118 dates. 45 dates at the
    20-per-batch chunk size must land in 3 commits, not 45."""
    monkeypatch.setattr(aod, "HF_API_KEY", "fake-token")
    base_ts = 1767225600
    ts_values = [base_ts + i * 86400 for i in range(45)]
    fake_feats = pd.DataFrame({"ts": ts_values, "close": [100.0] * 45, "label_up": [1] * 45})
    monkeypatch.setattr(aod, "fetch_minute_bars", lambda symbol, *, days: pd.DataFrame({"ts": [1], "close": [1.0]}))
    monkeypatch.setattr(aod, "engineer_features", lambda df, *, sentiment_score: fake_feats.copy())

    import huggingface_hub
    monkeypatch.setattr(huggingface_hub, "hf_hub_download", lambda **kw: (_ for _ in ()).throw(RuntimeError("no shard")))
    commit_calls = []

    class _CommitCountingApi(_FakeHfApi):
        def create_commit(self, *, repo_id, repo_type, operations, commit_message):
            commit_calls.append(len(list(operations)))

    monkeypatch.setattr(huggingface_hub, "HfApi", _CommitCountingApi)

    result = aod.backfill_minute_history(["AAPL"], days=90)

    assert result["ok"] is True
    assert result["dates_written"] == 45
    assert commit_calls == [20, 20, 5]


def test_backfill_minute_history_merges_with_an_existing_shard_for_that_date(monkeypatch):
    monkeypatch.setattr(aod, "HF_API_KEY", "fake-token")
    fake_feats = pd.DataFrame({"ts": [1767225600], "close": [100.0], "label_up": [1]})
    monkeypatch.setattr(aod, "fetch_minute_bars", lambda symbol, *, days: pd.DataFrame({"ts": [1], "close": [1.0]}))
    monkeypatch.setattr(aod, "engineer_features", lambda df, *, sentiment_score: fake_feats.copy())

    import tempfile
    existing_df = pd.DataFrame({"symbol": ["MSFT"], "ts": [999], "close": [400.0]})
    existing_path = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False).name
    existing_df.to_parquet(existing_path, index=False)

    import huggingface_hub
    monkeypatch.setattr(huggingface_hub, "hf_hub_download", lambda **kw: existing_path)
    _FakeHfApiBatch.captured_upload = {}
    monkeypatch.setattr(huggingface_hub, "HfApi", _FakeHfApiBatch)

    aod.backfill_minute_history(["AAPL"], days=90)

    uploads = _FakeHfApiBatch.captured_upload["commits"]
    assert len(uploads) == 1
    assert set(uploads[0]["df"]["symbol"]) == {"AAPL", "MSFT"}


def test_backfill_minute_history_holds_sentiment_at_neutral(monkeypatch):
    monkeypatch.setattr(aod, "HF_API_KEY", "fake-token")
    monkeypatch.setattr(aod, "fetch_minute_bars", lambda symbol, *, days: pd.DataFrame({"ts": [1], "close": [1.0]}))
    captured_sentiment = {}

    def fake_engineer(df, *, sentiment_score):
        captured_sentiment["value"] = sentiment_score
        return pd.DataFrame({"ts": [1767225600], "close": [100.0], "label_up": [1]})

    monkeypatch.setattr(aod, "engineer_features", fake_engineer)

    import huggingface_hub
    monkeypatch.setattr(huggingface_hub, "hf_hub_download", lambda **kw: (_ for _ in ()).throw(RuntimeError("no shard")))
    monkeypatch.setattr(huggingface_hub, "HfApi", _FakeHfApi)

    aod.backfill_minute_history(["AAPL"], days=90)

    assert captured_sentiment["value"] == 0.0


def test_backfill_minute_history_returns_ok_false_without_an_hf_key(monkeypatch):
    monkeypatch.setattr(aod, "HF_API_KEY", "")
    result = aod.backfill_minute_history(["AAPL"], days=90)
    assert result == {"ok": False, "reason": "no_hf_api_key"}


def test_backfill_minute_history_continues_past_a_symbol_that_fails_to_fetch(monkeypatch):
    monkeypatch.setattr(aod, "HF_API_KEY", "fake-token")

    def fetch(symbol, *, days):
        if symbol == "BAD":
            raise RuntimeError("fetch failed")
        return pd.DataFrame({"ts": [1], "close": [1.0]})

    monkeypatch.setattr(aod, "fetch_minute_bars", fetch)
    monkeypatch.setattr(aod, "engineer_features", lambda df, *, sentiment_score: pd.DataFrame({"ts": [1767225600], "close": [100.0], "label_up": [1]}))

    import huggingface_hub
    monkeypatch.setattr(huggingface_hub, "hf_hub_download", lambda **kw: (_ for _ in ()).throw(RuntimeError("no shard")))
    monkeypatch.setattr(huggingface_hub, "HfApi", _FakeHfApi)

    result = aod.backfill_minute_history(["BAD", "AAPL"], days=90)

    assert result["ok"] is True
    assert result["symbols_processed"] == 1
    assert result["symbols_requested"] == 2
