"""kalshi_15m_spot: real Alpaca (Kraken US feed) 1-minute spot history,
features, and its accuracy grade against Kalshi's own settlement values."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from data import kalshi_15m_spot as ks

T0 = 1_790_000_000 - (1_790_000_000 % 900)


@pytest.fixture(autouse=True)
def _isolated(tmp_path, monkeypatch):
    monkeypatch.setattr(ks, "LOCAL_DIR", tmp_path / "spot")
    monkeypatch.setattr(ks, "HF_API_KEY", "")
    monkeypatch.setattr(ks, "REQUEST_SPACING_SEC", 0.0)
    monkeypatch.setattr(ks, "_live_cache", {})


def _minutes(coin, closes, start=T0, volume=1.0, skip=()):
    return pd.DataFrame([
        {"coin": coin, "ts": start + 60 * (i + 1), "open": c, "high": c, "low": c, "close": c, "volume": volume}
        for i, c in enumerate(closes) if i not in skip
    ], columns=ks.COLUMNS)


def _alpaca_bars(pair_starts: dict[str, list[int]], close: float = 1.6) -> dict[str, list[dict]]:
    return {pair: [{"t": pd.Timestamp(t, unit="s", tz="UTC").isoformat().replace("+00:00", "Z"),
                    "o": 1.5, "h": 2.0, "l": 1.0, "c": close, "v": 10.0} for t in starts]
            for pair, starts in pair_starts.items()}


def test_fetch_reads_alpacas_kraken_feed_and_stores_bars_by_their_end(monkeypatch):
    from data import alpaca_client
    calls = []

    def fake_bars(symbols, *, start, end, loc, max_pages, **kw):
        calls.append((symbols, start, end, loc))
        return _alpaca_bars({"BTC/USD": [T0 - 60 + 60 * i for i in range(402)]})

    monkeypatch.setattr(alpaca_client, "get_crypto_bars", fake_bars)
    df = ks.fetch_candles("BTC", T0, T0 + 400 * 60)
    assert calls[0][0] == ["BTC/USD"] and calls[0][3] == alpaca_client.CHART_CRYPTO_LOC == "us-1"
    assert df.ts.iloc[0] == T0 + 60 and df.ts.iloc[-1] == T0 + 400 * 60  # START + 60, inside (start, end]
    assert df.ts.is_monotonic_increasing and df.close.iloc[0] == 1.6 and set(df.coin) == {"BTC"}


def test_collect_reads_every_coin_in_one_request(monkeypatch):
    from data import alpaca_client
    calls = []
    monkeypatch.setattr(alpaca_client, "get_crypto_bars", lambda symbols, **kw: calls.append(symbols) or _alpaca_bars(
        {p: [T0] for p in symbols}))
    df = ks.collect_since(T0, until_ts=T0 + 120)
    assert len(calls) == 1 and sorted(calls[0]) == sorted(ks.SPOT_PRODUCTS.values())
    assert set(df.coin) == set(ks.SPOT_PRODUCTS)


def test_a_coin_alpaca_does_not_list_is_remembered(monkeypatch):
    from data import alpaca_client
    monkeypatch.setattr(alpaca_client, "get_crypto_bars", lambda symbols, **kw: {})
    monkeypatch.setattr(ks, "_unlisted", {})
    assert ks.fetch_candles("NEAR", T0, T0 + 7200).empty
    assert not ks.is_listed("NEAR") and ks.is_listed("BTC")


def test_live_reads_carry_the_streams_newest_bar(monkeypatch):
    from data import alpaca_stream
    now = int(ks.time.time()) // 60 * 60
    monkeypatch.setattr(ks, "fetch_candles", lambda coin, start, end: _minutes(coin, [100.0] * 3, start=now - 240))

    class FakeStream:
        def bars(self, symbol):
            assert symbol == "ETH/USD"
            return pd.DataFrame([{"ts": now - 60, "open": 101.0, "high": 101.0, "low": 101.0, "close": 101.0, "volume": 2.0}])

    monkeypatch.setattr(alpaca_stream, "_streams", {"crypto": FakeStream()})
    monkeypatch.setattr(ks, "_last_refresh", {})
    series = ks.recent_series("ETH")
    assert int(series.ts.iloc[-1]) == now and series.close.iloc[-1] == 101.0


def test_complete_minutes_forward_fills_no_trade_minutes():
    df = ks.complete_minutes(_minutes("DOGE", [1.0, 2.0, 3.0, 4.0], skip=(1, 2)))
    assert df.close.tolist() == [1.0, 1.0, 1.0, 4.0]
    assert df.volume.tolist() == [1.0, 0.0, 0.0, 1.0]


def test_spot_features_match_their_definitions_and_never_look_ahead():
    closes = list(100 * np.exp(np.cumsum(np.random.default_rng(0).normal(0, 0.001, 1600))))
    full = ks.engineer_spot_features(_minutes("BTC", closes)).set_index("ts")
    early = ks.engineer_spot_features(_minutes("BTC", closes[:1500])).set_index("ts")
    t = T0 + 60 * 1500
    assert full.loc[t, "trend_1d"] == pytest.approx(closes[1499] / closes[1499 - 1440] - 1)
    assert full.loc[t, "trend_8h"] == pytest.approx(closes[1499] / closes[1499 - 480] - 1)
    for col in ks.FEATURE_COLUMNS:
        assert full.loc[t, col] == pytest.approx(early.loc[t, col], nan_ok=True)


def test_push_merges_with_the_hf_shard_after_a_restart(tmp_path, monkeypatch):
    remote = tmp_path / "remote.parquet"
    _minutes("BTC", [1.0, 2.0]).to_parquet(remote, index=False)
    monkeypatch.setattr(ks, "_hf_download", lambda path: str(remote))
    ks.push_spot_history(_minutes("BTC", [9.0], start=T0 + 120))
    df = pd.read_parquet(next(ks.LOCAL_DIR.glob("*.parquet")))
    assert df.close.tolist() == [1.0, 2.0, 9.0]


def test_recent_series_tops_up_the_live_cache(monkeypatch):
    from data import alpaca_stream
    monkeypatch.setattr(alpaca_stream, "_streams", {})
    calls = []

    def fake_fetch(coin, start, end):
        calls.append((start, end))
        return _minutes(coin, [100.0] * 3, start=end - 180)

    monkeypatch.setattr(ks, "fetch_candles", fake_fetch)
    monkeypatch.setattr(ks, "_last_refresh", {})
    ks.recent_series("BTC")
    first_span = calls[0][1] - calls[0][0]
    assert first_span == ks.LIVE_CACHE_HOURS * 3600
    ks._live_cache["BTC"] = _minutes("BTC", [100.0] * 1700, start=int(ks.time.time()) - 1700 * 60)  # noqa: SLF001
    ks._last_refresh.clear()  # noqa: SLF001
    ks.recent_series("BTC")
    assert calls[1][1] - calls[1][0] <= ks.MAX_CANDLES_PER_CALL * 60


def test_bots_reading_the_same_coin_within_seconds_share_one_request(monkeypatch):
    from data import alpaca_stream
    monkeypatch.setattr(alpaca_stream, "_streams", {})
    calls = []
    monkeypatch.setattr(ks, "fetch_candles", lambda coin, start, end: calls.append(coin) or _minutes(coin, [100.0] * 3, start=end - 180))
    monkeypatch.setattr(ks, "_last_refresh", {})
    ks.recent_series("ETH")
    ks.recent_series("ETH")
    assert calls == ["ETH"]
    monkeypatch.setattr(ks, "_last_refresh", {"ETH": ks.time.time() - ks.LIVE_REFRESH_MIN_SEC - 1})
    ks.recent_series("ETH")
    assert calls == ["ETH", "ETH"]


def test_live_underlying_row_carries_the_strike(monkeypatch):
    monkeypatch.setattr(ks, "recent_series", lambda coin: _minutes(coin, list(np.linspace(100, 101, 300))))
    row = ks.live_underlying_row("BTC", {"floor_strike": 100.5})
    assert row["floor_strike"] == 100.5
    assert row["close"] == pytest.approx(101.0)
    assert row["volatility_15"] is not None
    assert ks.live_underlying_row("GOLD", {}) is None


def test_grade_against_settlements():
    spot = _minutes("BTC", [100.0] * 15 + [100.2], start=T0 - 60)
    quotes = pd.DataFrame([
        {"coin": "BTC", "ticker": "A", "open_ts": T0, "close_ts": T0 + 900, "result": "yes", "floor_strike": 100.0, "expiration_value": 100.2},
        {"coin": "BTC", "ticker": "B", "open_ts": T0, "close_ts": T0 + 900, "result": "no", "floor_strike": 100.01, "expiration_value": 100.0},
    ])
    report = ks.grade_against_settlements(quotes, spot)
    assert report["graded"] == 2
    assert report["per_coin"]["BTC"]["outcome_match"] == 0.5
    assert report["per_coin"]["BTC"]["open_err_bps_median"] == pytest.approx(0.5)


def test_live_underlying_row_refuses_stale_spot(monkeypatch):
    monkeypatch.setattr(ks, "recent_series", lambda coin: _minutes(coin, [100.0] * 300))
    fresh_as_of = T0 + 60 * 300
    assert ks.live_underlying_row("BTC", {"floor_strike": 100.0}, as_of_ts=fresh_as_of)["ts"] == fresh_as_of
    assert ks.live_underlying_row("BTC", {"floor_strike": 100.0}, as_of_ts=fresh_as_of + 600) is None
    assert ks.live_underlying_row("BTC", {"floor_strike": 100.0}, as_of_ts=T0 + 60 * 100)["ts"] == T0 + 60 * 100


def test_new_product_backfill_uploads_in_a_few_commits_not_one_per_day(monkeypatch, tmp_path):
    import huggingface_hub
    monkeypatch.setattr(ks, "LOCAL_DIR", tmp_path)
    monkeypatch.setattr(ks, "HF_API_KEY", "token")
    monkeypatch.setattr(ks, "list_hf_shard_dates", lambda: ["2026-09-30"])
    monkeypatch.setattr(ks, "load_spot_history", lambda days: pd.DataFrame({"coin": [c for c in ks.SPOT_PRODUCTS if c != "DOT"]}))
    monkeypatch.setattr(ks, "_hf_download", lambda path: None)
    monkeypatch.setattr(ks, "collect_since", lambda start, until_ts=None, coins=None: _minutes(coins[0], [1.0] * 3, start=start + 60))
    commits = []

    class FakeApi:
        def __init__(self, token):
            pass

        def create_commit(self, **kw):
            commits.append(len(kw["operations"]))

    monkeypatch.setattr(huggingface_hub, "HfApi", FakeApi)
    r = ks.backfill_missing_products(days=60, files_per_commit=25)
    assert r["missing"] == ["DOT"] and len(r["dates_written"]) == 60
    assert commits == [25, 25, 10]


def test_an_upload_keeps_rows_another_writer_put_on_hf(monkeypatch, tmp_path):
    """A gap backfilled into HF while this process held its own local copy
    of the day must survive this process's next upload."""
    monkeypatch.setattr(ks, "LOCAL_DIR", tmp_path)
    monkeypatch.setattr(ks, "HF_API_KEY", "")
    day0 = int(pd.Timestamp("2026-10-02", tz="UTC").timestamp())
    local = _minutes("BTC", [1.0] * 5, start=day0 + 3600)          # this process, after its restart
    gap = _minutes("BTC", [2.0] * 5, start=day0 + 60)               # recovered by a backfill, on HF only
    local.to_parquet(tmp_path / "2026-10-02.parquet", index=False)
    remote_file = tmp_path / "remote.parquet"
    gap.to_parquet(remote_file, index=False)
    monkeypatch.setattr(ks, "_hf_download", lambda path: str(remote_file))
    new = _minutes("BTC", [3.0] * 2, start=day0 + 7200)
    ks.push_spot_history(new)
    merged = pd.read_parquet(tmp_path / "2026-10-02.parquet")
    assert set(gap.ts) | set(local.ts) | set(new.ts) == set(merged.ts)


def test_the_archive_is_rebuilt_from_alpaca_once(monkeypatch, tmp_path):
    import json

    import huggingface_hub
    monkeypatch.setattr(ks, "LOCAL_DIR", tmp_path)
    monkeypatch.setattr(ks, "HF_API_KEY", "token")
    monkeypatch.setattr(ks, "list_hf_shard_dates", lambda: ["2026-09-01"])
    marker: dict = {}
    monkeypatch.setattr(ks, "archive_source", lambda: marker.get("info"))
    monkeypatch.setattr(ks, "_hf_download", lambda path: None)
    fetched = []

    def fake_collect(start, until_ts=None, coins=None):
        fetched.append(coins)
        return _minutes("BTC", [2.0] * 3, start=start + 60) if len(fetched) == 1 else pd.DataFrame(columns=ks.COLUMNS)

    monkeypatch.setattr(ks, "collect_since", fake_collect)
    commits = []

    class FakeApi:
        def __init__(self, token):
            pass

        def create_commit(self, **kw):
            commits.append([op.path_in_repo for op in kw["operations"]])
            for op in kw["operations"]:
                if op.path_in_repo == ks.SOURCE_MARKER:
                    marker["info"] = json.loads(op.path_or_fileobj)

    monkeypatch.setattr(huggingface_hub, "HfApi", FakeApi)
    r = ks.rebuild_from_alpaca(days=3)
    assert r["ok"] and r["dates_written"] == 1 and fetched[0] == list(ks.SPOT_PRODUCTS)
    assert ks.SOURCE_MARKER in commits[-1] and marker["info"]["source"] == "alpaca"
    assert ks.rebuild_from_alpaca(days=3)["action"] == "already_rebuilt"
