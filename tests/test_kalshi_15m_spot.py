"""kalshi_15m_spot: real Coinbase 1-minute spot history, features, and its
accuracy grade against Kalshi's own settlement values."""
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


class _Resp:
    def __init__(self, payload, status=200):
        self._payload, self.status_code = payload, status

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(self.status_code)


def test_fetch_candles_pages_and_converts_start_to_candle_end(monkeypatch):
    calls = []

    def fake_get(url, params, headers, timeout):
        calls.append(params)
        rows = [[t, 1.0, 2.0, 1.5, 1.6, 10.0] for t in range(params["start"], params["end"], 60)]
        return _Resp(list(reversed(rows)))

    monkeypatch.setattr(ks.requests, "get", fake_get)
    df = ks.fetch_candles("BTC", T0, T0 + 400 * 60)
    assert len(calls) == 2
    assert df.ts.iloc[0] == T0 + 60 and df.ts.iloc[-1] == T0 + 400 * 60
    assert df.ts.is_monotonic_increasing and df.close.iloc[0] == 1.6


def test_fetch_candles_retries_rate_limits(monkeypatch):
    responses = [_Resp({}, 429), _Resp([[T0, 1, 1, 1, 1, 1]])]
    monkeypatch.setattr(ks.requests, "get", lambda *a, **k: responses.pop(0))
    monkeypatch.setattr(ks.time, "sleep", lambda s: None)
    assert len(ks.fetch_candles("BTC", T0, T0 + 60)) == 1


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
    monkeypatch.setattr(ks, "load_spot_history", lambda days: pd.DataFrame({"coin": [c for c in ks.COINBASE_PRODUCTS if c != "DOT"]}))
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
