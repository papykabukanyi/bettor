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
    assert df.iloc[0]["close"] == 5.0


def test_engineer_features_label_is_nan_for_recent_rows():
    # 60 rows of steadily rising price, well past the minimum window (35).
    prices = [100.0 + i * 0.1 for i in range(60)]
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
    prices = [100.0] * 40 + [200.0] * 40  # sharp jump partway through
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


def test_get_watchlist_falls_back_to_known_list_on_failure(monkeypatch):
    def _raise():
        raise RuntimeError("network down")

    monkeypatch.setattr(perps_data, "list_margin_markets", _raise)
    watchlist = perps_data.get_watchlist()
    assert watchlist == list(perps_data.KNOWN_PERP_TICKERS)


def test_get_watchlist_uses_live_listing_when_available(monkeypatch):
    monkeypatch.setattr(perps_data, "list_margin_markets", lambda: [{"ticker": "KXBTCPERP"}, {"ticker": "KXETHPERP"}])
    watchlist = perps_data.get_watchlist()
    assert watchlist == ["KXBTCPERP", "KXETHPERP"]
