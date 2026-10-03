"""Live Alpaca minute-bar streams (alpaca_stream)."""
from __future__ import annotations

import json

import pandas as pd

from data import alpaca_stream as s


def _stream(symbols=("AAPL", "SPY")):
    return s.BarStream("stocks", "wss://example/v2/sip", lambda: list(symbols))


def test_minute_and_corrected_bars_are_kept_by_bar_start_time():
    st = _stream()
    st._handle([{"T": "b", "S": "AAPL", "t": "2026-10-05T13:30:00Z", "o": 1, "h": 2, "l": 0.5, "c": 1.5, "v": 100},  # noqa: SLF001
                {"T": "b", "S": "AAPL", "t": "2026-10-05T13:31:00Z", "o": 1.5, "h": 2, "l": 1, "c": 1.8, "v": 50}])
    st._handle([{"T": "u", "S": "AAPL", "t": "2026-10-05T13:31:00Z", "o": 1.5, "h": 2.2, "l": 1, "c": 1.9, "v": 60}])  # noqa: SLF001
    bars = st.bars("AAPL")
    assert list(bars.ts) == [1791207000, 1791207060]
    assert bars.iloc[-1]["close"] == 1.9 and bars.iloc[-1]["volume"] == 60  # the correction replaced the bar
    assert st.status()["bars"] == 3 and st.status()["last_bar_age_sec"] is not None


def test_stream_bars_go_on_top_of_rest_history(monkeypatch):
    st = _stream()
    st._handle([{"T": "b", "S": "SPY", "t": "2026-10-05T13:31:00Z", "o": 2, "h": 2, "l": 2, "c": 2, "v": 9}])  # noqa: SLF001
    monkeypatch.setitem(s._streams, "stocks", st)  # noqa: SLF001
    rest = pd.DataFrame({"ts": [1791207000, 1791207060], "open": 1.0, "high": 1.0, "low": 1.0, "close": 1.0, "volume": 1.0})
    merged = s.merge_live("stocks", "SPY", rest)
    assert list(merged.ts) == [1791207000, 1791207060] and merged.iloc[-1]["close"] == 2.0
    assert s.merge_live("crypto", "BTC/USD", rest) is rest  # no crypto stream running: unchanged


class _FakeWs:
    def __init__(self):
        self.sent = []

    def send(self, raw):
        self.sent.append(json.loads(raw))


def test_subscriptions_follow_the_trading_universe():
    universe = ["AAPL", "SPY"]
    st = s.BarStream("stocks", "wss://example", lambda: list(universe))
    ws = _FakeWs()
    st._subscribe(ws, force=True)  # noqa: SLF001
    assert ws.sent[-1] == {"action": "subscribe", "bars": ["AAPL", "SPY"], "updatedBars": ["AAPL", "SPY"]}
    universe[:] = ["SPY", "NVDA"]
    st._subscribe(ws)  # noqa: SLF001
    assert {"action": "unsubscribe", "bars": ["AAPL"], "updatedBars": ["AAPL"]} in ws.sent
    assert ws.sent[-1]["bars"] == ["NVDA", "SPY"]


def test_streams_stay_off_without_keys(monkeypatch):
    monkeypatch.delenv("ALPACA_API_KEY_ID", raising=False)
    assert s.start_all() == {"ok": False, "reason": "disabled_or_no_keys"}
