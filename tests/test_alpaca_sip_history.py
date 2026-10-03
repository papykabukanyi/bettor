"""SIP 1-minute bar archive (alpaca_sip_history)."""
from __future__ import annotations

import pandas as pd

from data import alpaca_sip_history as h


class _Resp:
    def __init__(self, body, status=200):
        self._body, self.status_code = body, status

    def json(self):
        return self._body

    def raise_for_status(self):
        pass


def test_bars_keep_their_real_unix_seconds_and_every_minute(monkeypatch):
    pages = [
        {"bars": [{"t": "2026-10-01T13:30:00Z", "o": 1, "h": 2, "l": 0.5, "c": 1.5, "v": 100, "n": 5, "vw": 1.2},
                  {"t": "2026-10-01T13:31:00Z", "o": 1.5, "h": 2, "l": 1, "c": 1.8, "v": 50, "n": 3, "vw": 1.6}],
         "next_page_token": "p2"},
        {"bars": [{"t": "2026-10-01T13:32:00Z", "o": 1.8, "h": 1.9, "l": 1.7, "c": 1.75, "v": 20, "n": 2, "vw": 1.8}],
         "next_page_token": None},
    ]
    seen = []
    monkeypatch.setattr(h.requests, "get", lambda url, headers, params, timeout: seen.append(dict(params)) or _Resp(pages[len(seen) - 1]))
    df = h.fetch_bars("AAPL", pd.Timestamp("2026-10-01", tz="UTC"), pd.Timestamp("2026-10-02", tz="UTC"), headers={})
    assert list(df.ts) == [1790861400, 1790861460, 1790861520]  # 2026-10-01 13:30/13:31/13:32 UTC
    assert seen[0]["feed"] == "sip" and seen[0]["adjustment"] == "split" and seen[1]["page_token"] == "p2"
    assert list(df.columns) == h.COLUMNS


def test_writing_a_year_unions_local_and_hf_rows(monkeypatch, tmp_path):
    monkeypatch.setattr(h, "LOCAL_DIR", tmp_path)
    base = pd.DataFrame({"ts": [1, 2], "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "volume": 1.0, "trade_count": 1, "vwap": 1.0})
    remote = base.assign(ts=[3, 4])
    monkeypatch.setattr(h, "_download", lambda symbol, year: remote)
    h.write_year("AAPL", 2026, base)
    path = h.write_year("AAPL", 2026, base.assign(ts=[5, 6]))
    assert list(pd.read_parquet(path).ts) == [1, 2, 3, 4, 5, 6]


def test_the_archive_covers_every_symbol_the_stock_and_options_bots_trade():
    from data import alpaca_data, alpaca_options_data
    u = set(h.universe())
    assert set(alpaca_data.BROAD_CANDIDATE_UNIVERSE) <= u and set(alpaca_options_data.OPTIONS_UNDERLYINGS) <= u and "SPY" in u
