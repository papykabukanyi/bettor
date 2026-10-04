"""Live Alpaca market-data streams: real-time 1-minute bars and ticks over
WebSocket.

One connection per feed for the whole process (Alpaca allows one stream
connection per feed per account): stocks on the account's feed
(ALPACA_DATA_FEED, "sip" with Algo Trader Plus = every US exchange,
consolidated) and crypto at the chart venue (alpaca_client.CHART_CRYPTO_LOC,
Kraken US). Each subscribes to minute bars and corrected ("updated") bars
for every symbol the bots trade or chart -- the Alpaca bots' universes, the
Kalshi bots' coins, the commodity ETFs -- and keeps the last KEEP_HOURS of
bars per symbol in memory. The setup modules merge these into their REST
history (merge_live), so a bot sees a minute bar the second it closes
instead of on its next poll.

Ticks: crypto quotes for the Kalshi coins and trades for the commodity
ETFs, so latest_price() gives the price as of now (a quote mid or last
print, seconds old) instead of the last minute's close.

Runs in daemon threads with automatic reconnect (backoff up to 60s) and
re-subscription when the trading universe changes. status() reports
connection, subscriptions and the age of the last message for dashboards.
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from typing import Any, Callable

import pandas as pd

logger = logging.getLogger(__name__)

STREAM_BASE = os.getenv("ALPACA_STREAM_BASE_URL", "wss://stream.data.alpaca.markets")
KEEP_HOURS = float(os.getenv("ALPACA_STREAM_KEEP_HOURS", "8") or "8")
RESUBSCRIBE_EVERY_SEC = 900
BAR_COLUMNS = ["ts", "open", "high", "low", "close", "volume"]


class BarStream:
    """One WebSocket connection streaming minute bars for a set of symbols."""

    def __init__(self, name: str, url: str, symbols_fn: Callable[[], list[str]],
                 ticks_fn: Callable[[], dict[str, list[str]]] | None = None):
        self.name, self.url, self.symbols_fn, self.ticks_fn = name, url, symbols_fn, ticks_fn
        self._lock = threading.Lock()
        self._bar_arrived = threading.Condition(self._lock)
        self._delays: list[float] = []  # seconds from a bar's close to its arrival (recent bars)
        self._bars: dict[str, dict[int, dict[str, float]]] = {}
        self._last: dict[str, dict[str, float]] = {}  # symbol -> {"price", "at", "bid", "ask"}
        self._subscribed: list[str] = []
        self._subscribed_ticks: dict[str, list[str]] = {}
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._ws = None
        self.stats: dict[str, Any] = {"connected": False, "connects": 0, "bars": 0, "ticks": 0, "last_message_at": None,
                                      "last_bar_at": None, "last_error": None, "subscribed": 0, "subscribed_ticks": 0}

    # -- lifecycle ---------------------------------------------------------
    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name=f"alpaca-stream-{self.name}", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        try:
            if self._ws is not None:
                self._ws.close()
        except Exception:
            pass

    def _run(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            try:
                self._session()
                backoff = 1.0
            except Exception as exc:
                self.stats["last_error"] = f"{type(exc).__name__}: {exc}"[:300]
                logger.warning("[alpaca_stream] %s stream dropped: %s", self.name, exc)
            self.stats["connected"] = False
            if self._stop.wait(backoff):
                break
            backoff = min(60.0, backoff * 2)

    def _session(self) -> None:
        import websocket
        ws = websocket.create_connection(self.url, timeout=30)
        self._ws = ws
        try:
            self._expect(ws, "connected")
            ws.send(json.dumps({"action": "auth", "key": os.getenv("ALPACA_API_KEY_ID", ""),
                                "secret": os.getenv("ALPACA_API_SECRET_KEY", "")}))
            self._expect(ws, "authenticated")
            self.stats["connected"] = True
            self.stats["connects"] += 1
            self.stats["last_error"] = None
            self._subscribe(ws, force=True)
            last_sub_check = time.time()
            ws.settimeout(70)  # minute bars arrive at least once a minute while anything trades
            while not self._stop.is_set():
                try:
                    raw = ws.recv()
                except websocket.WebSocketTimeoutException:
                    ws.ping()
                    continue
                if not raw:
                    raise ConnectionError("stream closed by server")
                self._handle(json.loads(raw))
                if time.time() - last_sub_check > RESUBSCRIBE_EVERY_SEC:
                    self._subscribe(ws)
                    last_sub_check = time.time()
        finally:
            self._ws = None
            try:
                ws.close()
            except Exception:
                pass

    def _expect(self, ws, msg: str) -> None:
        for _ in range(5):
            for m in json.loads(ws.recv()):
                if m.get("T") == "error":
                    raise ConnectionError(f"{m.get('code')}: {m.get('msg')}")
                if m.get("T") == "success" and m.get("msg") == msg:
                    return
        raise ConnectionError(f"no '{msg}' from {self.name} stream")

    def _subscribe(self, ws, *, force: bool = False) -> None:
        try:
            wanted = sorted(set(self.symbols_fn()))
            ticks = {ch: sorted(set(syms)) for ch, syms in (self.ticks_fn() if self.ticks_fn else {}).items() if syms}
        except Exception as exc:
            logger.warning("[alpaca_stream] %s universe unavailable: %s", self.name, exc)
            return
        if not wanted or (not force and wanted == self._subscribed and ticks == self._subscribed_ticks):
            return
        if not force:
            gone = sorted(set(self._subscribed) - set(wanted))
            gone_ticks = {ch: sorted(set(syms) - set(ticks.get(ch, []))) for ch, syms in self._subscribed_ticks.items()}
            msg = {ch: syms for ch, syms in gone_ticks.items() if syms}
            if gone:
                msg.update(bars=gone, updatedBars=gone)
            if msg:
                ws.send(json.dumps({"action": "unsubscribe", **msg}))
        ws.send(json.dumps({"action": "subscribe", "bars": wanted, "updatedBars": wanted, **ticks}))
        self._subscribed, self._subscribed_ticks = wanted, ticks
        self.stats["subscribed"] = len(wanted)
        self.stats["subscribed_ticks"] = sum(len(v) for v in ticks.values())

    # -- messages ----------------------------------------------------------
    def _handle(self, messages: list[dict[str, Any]]) -> None:
        now = time.time()
        self.stats["last_message_at"] = now
        cutoff = int(now - KEEP_HOURS * 3600)
        for m in messages:
            kind = m.get("T")
            if kind in ("b", "u"):
                try:
                    ts = int(pd.Timestamp(m["t"]).timestamp())
                    bar = {"open": float(m["o"]), "high": float(m["h"]), "low": float(m["l"]), "close": float(m["c"]),
                           "volume": float(m.get("v") or 0.0)}
                except (KeyError, TypeError, ValueError):
                    continue
                with self._lock:
                    series = self._bars.setdefault(m["S"], {})
                    series[ts] = bar
                    if len(series) > KEEP_HOURS * 60 + 120:
                        for old in [t for t in series if t < cutoff]:
                            del series[old]
                    if kind == "b":
                        self._delays = (self._delays + [now - (ts + 60)])[-300:]
                    self._bar_arrived.notify_all()
                self.stats["bars"] += 1
                self.stats["last_bar_at"] = now
            elif kind in ("q", "t"):
                try:
                    at = pd.Timestamp(m["t"]).timestamp()
                    if kind == "q":
                        bid, ask = float(m["bp"]), float(m["ap"])
                        if not (0 < bid <= ask):
                            continue
                        tick = {"price": (bid + ask) / 2.0, "bid": bid, "ask": ask, "at": at, "kind": "quote_mid"}
                    else:
                        tick = {"price": float(m["p"]), "at": at, "kind": "trade"}
                except (KeyError, TypeError, ValueError):
                    continue
                with self._lock:
                    prev = self._last.get(m["S"])
                    if prev is None or at >= prev["at"]:
                        self._last[m["S"]] = tick
                self.stats["ticks"] += 1
            elif kind == "error":
                self.stats["last_error"] = f"{m.get('code')}: {m.get('msg')}"
                logger.warning("[alpaca_stream] %s error: %s", self.name, m)
            elif kind == "subscription":
                self.stats["subscribed"] = len(m.get("bars") or [])

    # -- reads -------------------------------------------------------------
    def bars(self, symbol: str) -> pd.DataFrame:
        """Streamed minute bars for a symbol, Alpaca's shape (ts = bar START)."""
        with self._lock:
            series = dict(self._bars.get(symbol) or {})
        if not series:
            return pd.DataFrame(columns=BAR_COLUMNS)
        return pd.DataFrame([{"ts": ts, **bar} for ts, bar in sorted(series.items())], columns=BAR_COLUMNS)

    def wait_for_bars(self, symbols: list[str], start_ts: int, timeout: float) -> tuple[int, int]:
        """Block until each active symbol has its bar starting at start_ts
        (the minute that just closed), or until timeout. A symbol with no
        bar in the 3 minutes before isn't trading (a closed ETF, a thin
        coin) and is not waited for. Returns (have, active)."""
        deadline = time.time() + max(0.0, timeout)
        with self._bar_arrived:
            active = [s for s in symbols if max(self._bars.get(s) or {0: None}) >= start_ts - 180]
            while True:
                have = sum(1 for s in active if max(self._bars.get(s) or {0: None}) >= start_ts)
                left = deadline - time.time()
                if have >= len(active) or left <= 0:
                    return have, len(active)
                self._bar_arrived.wait(left)

    def last_tick(self, symbol: str) -> dict[str, float] | None:
        with self._lock:
            tick = self._last.get(symbol)
        return dict(tick) if tick else None

    def status(self) -> dict[str, Any]:
        s = dict(self.stats)
        now = time.time()
        for key in ("last_message_at", "last_bar_at"):
            s[key.replace("_at", "_age_sec")] = None if s[key] is None else round(now - s[key], 1)
        s["url"] = self.url
        with self._lock:
            delays = sorted(self._delays)
        s["bar_delay_sec"] = round(delays[len(delays) // 2], 2) if delays else None  # median, close -> arrival
        return s


_streams: dict[str, BarStream] = {}
_streams_lock = threading.Lock()


def _commodity_etfs() -> list[str]:
    from data import kalshi_15m_setup
    return sorted(set(kalshi_15m_setup.METAL_CHART_SYMBOL.values()))


def _stock_symbols() -> list[str]:
    from data import alpaca_sip_history
    return sorted(set(alpaca_sip_history.universe()) | set(_commodity_etfs()))


def _stock_ticks() -> dict[str, list[str]]:
    return {"trades": _commodity_etfs()}


def _kalshi_crypto_pairs() -> list[str]:
    from data import kalshi_15m_spot
    return sorted(set(kalshi_15m_spot.SPOT_PRODUCTS.values()))


_venue_cache: dict[str, Any] = {"at": 0.0, "key": None, "pairs": None}
VENUE_CHECK_SEC = 6 * 3600


def _live_on_chart_venue(pairs: list[str]) -> list[str]:
    """The pairs the chart venue printed a bar for in the last 2 days (it
    still answers for pairs it stopped listing long ago -- NEAR/USD's last
    Kraken bar is from Oct 2025). Re-checked every VENUE_CHECK_SEC."""
    key = ",".join(sorted(pairs))
    if _venue_cache["key"] == key and time.time() - _venue_cache["at"] < VENUE_CHECK_SEC:
        return list(_venue_cache["pairs"])
    from data import alpaca_client
    cutoff = time.time() - 2 * 86400
    live: list[str] = []
    for i in range(0, len(pairs), 100):
        data = alpaca_client._crypto_data_get(  # noqa: SLF001
            f"/v1beta3/crypto/{alpaca_client.CHART_CRYPTO_LOC}/latest/bars", params={"symbols": ",".join(pairs[i:i + 100])})
        for pair, bar in (data.get("bars") or {}).items():
            try:
                if pd.Timestamp(bar["t"]).timestamp() >= cutoff:
                    live.append(pair)
            except (KeyError, TypeError, ValueError):
                continue
    _venue_cache.update(at=time.time(), key=key, pairs=sorted(live))
    return sorted(live)


def _crypto_symbols() -> list[str]:
    from data import alpaca_crypto_data
    from data.alpaca_crypto_setup import STABLECOINS
    try:
        pairs = [s for s in alpaca_crypto_data.get_crypto_universe()
                 if s.endswith("/USD") and s.split("/")[0].upper() not in STABLECOINS]
    except Exception as exc:
        logger.warning("[alpaca_stream] crypto bot universe unavailable: %s", exc)
        pairs = []
    wanted = sorted(set(pairs) | set(_kalshi_crypto_pairs()))
    try:
        return _live_on_chart_venue(wanted)
    except Exception as exc:
        logger.warning("[alpaca_stream] chart venue check failed: %s", exc)
        return _kalshi_crypto_pairs()


def _crypto_ticks() -> dict[str, list[str]]:
    return {"quotes": _kalshi_crypto_pairs()}


def enabled() -> bool:
    return os.getenv("ALPACA_STREAM_ENABLED", "1").strip().lower() in {"1", "true", "yes"} and bool(os.getenv("ALPACA_API_KEY_ID"))


def start_all() -> dict[str, Any]:
    """Start the stock and crypto streams once per process (idempotent)."""
    if not enabled():
        return {"ok": False, "reason": "disabled_or_no_keys"}
    from data import alpaca_client
    with _streams_lock:
        if "stocks" not in _streams:
            _streams["stocks"] = BarStream("stocks", f"{STREAM_BASE}/v2/{alpaca_client.DATA_FEED}", _stock_symbols, _stock_ticks)
        if "crypto" not in _streams:
            _streams["crypto"] = BarStream("crypto", f"{STREAM_BASE}/v1beta3/crypto/{alpaca_client.CHART_CRYPTO_LOC}",
                                           _crypto_symbols, _crypto_ticks)
        for s in _streams.values():
            s.start()
    return {"ok": True, "streams": list(_streams)}


def merge_live(kind: str, symbol: str, bars: pd.DataFrame | None) -> pd.DataFrame:
    """REST bars (ts = bar START) with the stream's newer/corrected bars on
    top. Unchanged when the stream isn't running."""
    stream = _streams.get(kind)
    live = stream.bars(symbol) if stream is not None else pd.DataFrame(columns=BAR_COLUMNS)
    if live.empty:
        return bars if bars is not None else live
    if bars is None or bars.empty:
        return live
    base = bars[[c for c in BAR_COLUMNS if c in bars.columns]]
    return (pd.concat([base, live], ignore_index=True).drop_duplicates("ts", keep="last")
            .sort_values("ts").reset_index(drop=True))


def latest_price(kind: str, symbol: str, *, max_age_sec: float = 15.0, now: float | None = None) -> dict[str, Any] | None:
    """The symbol's price as of now from the stream's ticks (a quote mid or
    the last trade), with its age; None when the stream has nothing that
    fresh -- callers then use the last minute bar's close."""
    stream = _streams.get(kind)
    tick = stream.last_tick(symbol) if stream is not None else None
    if not tick:
        return None
    age = (time.time() if now is None else now) - tick["at"]
    if age > max_age_sec:
        return None
    return {**tick, "age_sec": round(max(age, 0.0), 2)}


def wait_for_closed_minute(wanted: dict[str, list[str]], *, timeout: float, now: float | None = None) -> dict[str, Any]:
    """A decision made right after a minute closes should read that
    minute: wait (up to timeout seconds in all) until each stream has
    delivered the just-closed bar for the wanted symbols ({stream: symbols}).
    Returns {"have", "of", "waited_sec", "minute_end"}."""
    t0 = time.time()
    minute_end = int((now if now is not None else t0) // 60 * 60)
    have = of = 0
    for kind, symbols in wanted.items():
        stream = _streams.get(kind)
        if stream is None or not symbols:
            continue
        got, active = stream.wait_for_bars(list(symbols), minute_end - 60, timeout - (time.time() - t0))
        have, of = have + got, of + active
    return {"have": have, "of": of, "waited_sec": round(time.time() - t0, 2), "minute_end": minute_end}


def status() -> dict[str, Any]:
    return {name: s.status() for name, s in _streams.items()} or {"streams": "not started"}
