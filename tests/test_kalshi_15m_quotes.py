"""kalshi_15m_quotes: settled-window quote history into HF shards."""
from __future__ import annotations

import pandas as pd
import pytest

from data import kalshi_15m_quotes as kq


@pytest.fixture(autouse=True)
def _isolated(tmp_path, monkeypatch):
    monkeypatch.setattr(kq, "LOCAL_DIR", tmp_path / "quotes")
    monkeypatch.setattr(kq, "HF_API_KEY", "")


def test_batches_stay_under_the_candle_cap():
    opens = [1_790_000_000 + 900 * i for i in range(40)]
    tickers = {o: [f"S{j}-{o}" for j in range(14)] for o in opens}
    groups = kq._batches(opens, tickers)
    assert [o for g in groups for o in g] == opens
    for g in groups:
        n = sum(len(tickers[o]) for o in g)
        span = (g[-1] + kq.WINDOW_SECONDS - g[0]) // 60
        assert n * span <= kq.MAX_CANDLES_PER_CALL and n <= kq.MAX_TICKERS_PER_CALL


def test_candles_to_rows_parses_minutes_and_thousands_separators():
    meta = {"KXGOLD15M-A": {
        "_coin": "GOLD", "_series": "KXGOLD15M", "open_time": "2026-09-30T00:00:00Z", "close_time": "2026-09-30T00:15:00Z",
        "result": "yes", "floor_strike": 1806.5, "expiration_value": "1,806.967",
    }}
    open_ts = kq._iso_ts("2026-09-30T00:00:00Z")
    payload = {"markets": [{"market_ticker": "KXGOLD15M-A", "candlesticks": [
        {"end_period_ts": open_ts + 120, "yes_bid": {"close_dollars": "0.40"}, "yes_ask": {"close_dollars": "0.42"},
         "price": {"close_dollars": "0.41", "mean_dollars": "0.41"}, "volume_fp": "12.5", "open_interest_fp": "300"},
        {"end_period_ts": open_ts + 2000, "yes_bid": {"close_dollars": "0.9"}, "yes_ask": {"close_dollars": "0.95"}, "price": {}},
    ]}]}
    rows = kq.candles_to_rows(meta, payload)
    assert len(rows) == 1
    assert rows[0]["minute"] == 2 and rows[0]["yes_bid"] == 0.40 and rows[0]["expiration_value"] == pytest.approx(1806.967)


def _rows(ticker, minutes, open_ts=1_790_000_100, bid=0.4):
    return pd.DataFrame([{
        "coin": "BTC", "series": "KXBTC15M", "ticker": ticker, "open_ts": open_ts, "close_ts": open_ts + 900,
        "end_period_ts": open_ts + 60 * m, "minute": m, "yes_bid": bid, "yes_ask": bid + 0.02, "last": bid, "mean": bid,
        "volume": 1.0, "open_interest": 1.0, "result": "yes", "floor_strike": 1.0, "expiration_value": 1.0,
    } for m in minutes], columns=kq.COLUMNS)


def test_push_merges_and_dedupes_by_ticker_and_minute():
    kq.push_quote_history(_rows("A", [1, 2, 3]))
    kq.push_quote_history(_rows("A", [3, 4], bid=0.5))
    shard = next(kq.LOCAL_DIR.glob("*.parquet"))
    df = pd.read_parquet(shard)
    assert sorted(df.minute) == [1, 2, 3, 4]
    assert df[df.minute == 3].yes_bid.item() == 0.5


def test_push_seeds_from_hf_when_the_local_shard_is_missing(tmp_path, monkeypatch):
    remote = tmp_path / "remote.parquet"
    _rows("OLD", [1, 2]).to_parquet(remote, index=False)
    monkeypatch.setattr(kq, "_hf_download", lambda path: str(remote))
    kq.push_quote_history(_rows("NEW", [1]))
    df = pd.read_parquet(next(kq.LOCAL_DIR.glob("*.parquet")))
    assert set(df.ticker) == {"OLD", "NEW"}


def test_load_quote_history_reads_local_shards():
    kq.push_quote_history(_rows("A", [1, 2]))
    df = kq.load_quote_history(days=5)
    assert len(df) == 2
