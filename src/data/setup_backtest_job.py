"""Each bot's setup strategy, re-tested on real data on the HF Space and
published to that bot's own HF model repo under setup_strategy/:

  config.json            the bot's strategy card (method + every parameter)
  backtest_latest.json   the latest replay on the last SETUP_BACKTEST_DAYS
  backtests/{date}.json  one per day, the running record

Each bot is replayed with its own module (perps_setup, kalshi_15m_setup,
alpaca_setup, alpaca_crypto_setup, alpaca_options_setup) on that bot's own
real data -- Coinbase 1m spot (HF archive) for perps, Kalshi's per-minute
quote archive plus Coinbase spot for the 15-minute bot, Alpaca 1m bars for
stocks/crypto/options -- once with the correlation rule (the live system)
and once without it, so the rule's contribution is visible.

Runs as a low-priority subprocess (launch() -> python -m
data.setup_backtest_job <bot>): the replay is CPU-heavy and must never
compete with the trading loops in the server process.
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

_ET_ZONE = ZoneInfo("America/New_York")

logger = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT_DIR / "src"
LOCAL_DIR = Path(os.getenv("SETUP_BACKTEST_DIR", str(ROOT_DIR / "data" / "setup_backtests")))
DAYS = int(os.getenv("SETUP_BACKTEST_DAYS", "30") or "30")
MAX_SYMBOLS = int(os.getenv("SETUP_BACKTEST_MAX_SYMBOLS", "15") or "15")
REPOS = {
    "perps": os.getenv("HF_MODEL_REPO", "papylove/kalshi-perps-model"),
    "kalshi15m": os.getenv("HF_KALSHI_15M_MODEL_REPO", "papylove/kalshi-15m-model"),
    "stocks": os.getenv("HF_ALPACA_MODEL_REPO", "papylove/alpaca-model"),
    "crypto": os.getenv("HF_ALPACA_CRYPTO_MODEL_REPO", "papylove/alpaca-crypto-model"),
    "options": os.getenv("HF_ALPACA_OPTIONS_MODEL_REPO", "papylove/alpaca-options-model"),
}
MODULES = {"perps": "perps_setup", "kalshi15m": "kalshi_15m_setup", "stocks": "alpaca_setup",
           "crypto": "alpaca_crypto_setup", "options": "alpaca_options_setup"}
SPOT_COLUMNS = ["ts", "open", "high", "low", "close", "volume"]


def summarize(trades: pd.DataFrame, pnl_col: str, *, symbol_col: str = "symbol", unit: str = "return") -> dict[str, Any]:
    if trades is None or trades.empty:
        return {"trades": 0, "unit": unit}
    x = trades[pnl_col].astype(float)
    wins, losses = x[x > 0], x[x <= 0]
    t_stat = float(x.mean() / (x.std(ddof=1) / np.sqrt(len(x)))) if len(x) > 2 and x.std(ddof=1) > 0 else None
    ts_col = "entry_ts" if "entry_ts" in trades else "open_ts"
    split = trades[ts_col].min() + (trades[ts_col].max() - trades[ts_col].min()) / 2
    halves = {}
    for name, part in (("first_half", x[trades[ts_col] < split]), ("second_half", x[trades[ts_col] >= split])):
        halves[name] = {"trades": int(len(part)), "avg": round(float(part.mean()), 5) if len(part) else None,
                        "win_rate": round(float((part > 0).mean()), 4) if len(part) else None}
    out = {
        "unit": unit, "trades": int(len(x)), "win_rate": round(float(len(wins) / len(x)), 4),
        "avg": round(float(x.mean()), 5), "total": round(float(x.sum()), 4),
        "profit_factor": round(float(wins.sum() / -losses.sum()), 3) if len(losses) and losses.sum() < 0 else None,
        "t_stat": None if t_stat is None else round(t_stat, 2), **halves,
    }
    if "exit" in trades:
        out["exits"] = {str(k): int(v) for k, v in trades["exit"].value_counts().items()}
    if symbol_col in trades:
        out["by_symbol"] = {str(k): {"trades": int(len(g)), "total": round(float(g[pnl_col].sum()), 4)}
                            for k, g in trades.groupby(symbol_col)}
    return out


def _both(fn) -> dict[str, Any]:
    """Run a replay with the correlation rule (the live system) and without."""
    return {"with_correlation": fn(True), "without_correlation": fn(False)}


# ---------------------------------------------------------------------------
# Per-bot replays on that bot's own real data
# ---------------------------------------------------------------------------

def run_perps(days: int) -> dict[str, Any]:
    from data import kalshi_15m_spot, perps_data, perps_setup, perps_strategy
    from data.kalshi_perps import get_margin_market
    spot = kalshi_15m_spot.load_spot_history(days=days)
    tickers = {kalshi_15m_spot.chart_coin(perps_data.coin_for_ticker(t)): t for t in perps_data.get_watchlist()}
    coins = [c for c in tickers if c in set(spot["coin"])]
    sides = ("long", "short") if perps_strategy.ENABLE_SHORTS else ("long",)
    costs = {}
    for coin in coins:
        try:
            spread = perps_setup.market_spread_bps(get_margin_market(tickers[coin]).get("market") or {})
        except Exception:
            spread = None
        costs[coin] = {"fee_rate_roundtrip": perps_strategy.setup_fee_rate_roundtrip(tickers[coin]),
                       "spread_bps": spread if spread is not None else 5.0}

    def go(with_corr: bool) -> dict[str, Any]:
        frames = []
        for coin in coins:
            leader = perps_setup.leader_for(coin)
            t = perps_setup.replay(spot[spot.coin == coin][SPOT_COLUMNS], sides=sides, **costs[coin],
                                   leader_df=spot[spot.coin == leader][SPOT_COLUMNS] if with_corr else None,
                                   leader_symbol=leader)
            if not t.empty:
                frames.append(t.assign(symbol=coin))
        return summarize(pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(), "net_return")

    return {"universe": coins, "costs": costs, "sides": list(sides), **_both(go)}


# The 15m bot's real-price replay reads every day of Kalshi's quote archive
# (it starts Sep 2026), not just the last DAYS: its trades are few.
KALSHI15M_REPLAY_DAYS = int(os.getenv("SETUP_KALSHI15M_REPLAY_DAYS", "120") or "120")
PRICE_EDGE_THRESHOLDS = (None, 0.0, 0.02, 0.04, 0.06, 0.08)


def run_kalshi15m(days: int) -> dict[str, Any]:
    from data import kalshi_15m_quotes, kalshi_15m_setup, kalshi_15m_spot, kalshi_15m_strategy
    days = max(days, KALSHI15M_REPLAY_DAYS)
    quotes = kalshi_15m_quotes.load_quote_history(days=days)
    spot = kalshi_15m_spot.load_spot_history(days=days + 2)
    since = int(time.time()) - (days + 2) * 86400
    charts: dict[str, pd.DataFrame] = {}

    def chart(coin: str) -> pd.DataFrame:
        """Crypto: the Kraken-via-Alpaca spot archive; commodities: their
        SIP ETF's regular session (Alpaca archive)."""
        if coin not in charts:
            if coin in kalshi_15m_setup.METAL_CHART_SYMBOL:
                c = _study_candles(coin)[0]
                charts[coin] = c[c["ts"] >= since][SPOT_COLUMNS] if not c.empty else pd.DataFrame(columns=SPOT_COLUMNS)
            else:
                charts[coin] = spot[spot.coin == coin][SPOT_COLUMNS]
        return charts[coin]

    coins = sorted(c for c in kalshi_15m_strategy.ACTIVE_ENTRY_COINS if c in set(quotes["coin"])
                   and (c in set(spot["coin"]) or c in kalshi_15m_setup.METAL_CHART_SYMBOL))
    live_trades: dict[str, pd.DataFrame] = {}

    def go(with_corr: bool) -> dict[str, Any]:
        frames = []
        for coin in coins:
            leader = kalshi_15m_setup.leader_for(coin)
            if chart(coin).empty:
                continue
            t = kalshi_15m_setup.replay_contracts(
                quotes[quotes.coin == coin], chart(coin),
                leader_1m=chart(leader) if with_corr else None, leader_symbol=leader,
                session=kalshi_15m_setup.session_for(coin), leader_session=kalshi_15m_setup.session_for(leader),
                minute_average=kalshi_15m_setup.settles_on_minute_average(coin),
            )
            if not t.empty:
                frames.append(t.assign(symbol=coin))
        trades = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        if with_corr:
            live_trades["t"] = trades
        return summarize(trades, "pnl_per_contract", unit="usd_per_contract")

    out = {"universe": coins, "days": days, "sides": list(kalshi_15m_setup.SIDES), **_both(go)}
    out["price_edge"] = price_edge_study(live_trades.get("t", pd.DataFrame()))
    return out


def price_edge_study(trades: pd.DataFrame, *, min_train_trades: int = 10, min_trades: int = 20) -> dict[str, Any]:
    """Does requiring Kalshi's ask to sit below the contract's fair value
    (priced off Alpaca's chart at entry) improve the setup's record on
    real Kalshi prices? Per threshold over the whole replay, and walk-
    forward by week: each week trades the threshold that made the most
    over the weeks before it (needing min_train_trades), scored on the
    week itself. Enforced only when that beats no filter, positive, on
    >= min_trades unseen trades."""
    label = lambda th: "none" if th is None else f">= {th:+.2f}"  # noqa: E731
    if trades is None or trades.empty or "edge" not in trades:
        return {"trades": 0, "enforce": False, "min_edge_now": None}
    t = trades.dropna(subset=["edge"]).copy()
    t["week"] = pd.to_datetime(t["open_ts"], unit="s", utc=True).dt.strftime("%G-W%V")
    pick = lambda df, th: df if th is None else df[df["edge"] >= th]  # noqa: E731

    def best(window: pd.DataFrame):
        scored = [(pick(window, th)["pnl_per_contract"].sum(), th) for th in PRICE_EDGE_THRESHOLDS
                  if len(pick(window, th)) >= min_train_trades]
        return max(scored, key=lambda x: x[0])[1] if scored else None

    weeks = sorted(t["week"].unique())
    rows, oos = [], []
    for w in weeks[1:]:
        th = best(t[t["week"] < w])
        test = pick(t[t["week"] == w], th)
        oos.append(test)
        rows.append({"week": w, "min_edge": th, "result": _trade_stats(test["pnl_per_contract"])})
    oos_all = pd.concat(oos, ignore_index=True) if oos else pd.DataFrame(columns=["pnl_per_contract"])
    base = t[t["week"] > weeks[0]] if weeks else t.iloc[0:0]
    res, plain = _trade_stats(oos_all["pnl_per_contract"]), _trade_stats(base["pnl_per_contract"])
    enforce = bool(res.get("trades", 0) >= min_trades and (res.get("avg") or 0) > 0 and (res.get("avg") or 0) > (plain.get("avg") or 0))
    return {"trades": int(len(t)), "weeks": rows, "out_of_sample": res, "no_filter": plain, "enforce": enforce,
            "min_edge_now": best(t), "by_threshold": {label(th): _trade_stats(pick(t, th)["pnl_per_contract"])
                                                       for th in PRICE_EDGE_THRESHOLDS},
            "edge_quantiles": [round(float(x), 4) for x in t["edge"].quantile([0.1, 0.5, 0.9])] if len(t) else None,
            "rule": "enter only when fair value - ask >= the threshold chosen on the weeks before (unseen week scored)"}


def _rule_words(bot: str, params: dict[str, Any]) -> str:
    """A bot's plan setting in a few words."""
    out = []
    if params.get("STOP_BUFFER_ATR15") is not None:
        out.append(f"stop {params['STOP_BUFFER_ATR15']:g}x 15m range")
    if params.get("MIN_RR") is not None:
        out.append(f"target {params['MIN_RR']:g}R")
    if params.get("MAX_HOLD_HOURS") is not None:
        out.append(f"hold <= {params['MAX_HOLD_HOURS']:g} h")
    if params.get("BREAKEVEN_R"):
        out.append(f"break-even at {params['BREAKEVEN_R']:g}R")
    return " · ".join(out)


def rule_in_force(bot: str) -> str:
    """The rule a bot trades right now, in words (the study's choice while it
    is in force, else the setup defaults)."""
    elig = eligibility(bot) or {}
    params = dict(elig.get("params") or {}) if elig.get("enforce") else {}
    words = _rule_words(bot, params or _param_values(default_param(bot), param_keys(bot)))
    return words + (" (learned on unseen years)" if elig.get("enforce") else " (setup defaults)")


def strategy_board() -> list[dict[str, Any]]:
    """What each bot trades right now and the evidence behind it: the rule
    in force (the study's choice when it won on unseen data, else the
    setup's defaults), its multi-year study on unseen years, and its
    replay on recent real data."""
    board = []
    for bot in STUDY_PRIORITY:
        elig = eligibility(bot) or {}
        enforce = bool(elig.get("enforce"))
        params = dict(elig.get("params") or {}) if enforce else {}
        if not params:
            params = _param_values(default_param(bot))
        row: dict[str, Any] = {"bot": bot, "source": elig.get("source") if enforce else "defaults", "rule": _rule_words(bot, params),
                               "params": params, "symbols": elig.get("symbols") if enforce else "all",
                               "blocked": (elig.get("blocked") or {}) if enforce else {}}
        if bot == "kalshi15m":
            from data import kalshi_15m_setup
            row["sides"] = list(kalshi_15m_setup.SIDES)
            row["min_price_edge"] = price_edge_min()
        elif bot in MULTIYEAR:
            row["sides"] = list(MULTIYEAR[bot]["sides"])
        st = multiyear_status(bot) if bot in MULTIYEAR else {}
        latest_study = st.get("latest") or {}
        best = ((latest_study.get("patterns") or {}).get("with_patterns") or (latest_study.get("trained") or {}).get("out_of_sample")
                or (latest_study.get("walk_forward") or {}).get("out_of_sample") or {})
        yrs = latest_study.get("patterns") or latest_study.get("trained") or latest_study.get("walk_forward") or {}
        row["study"] = {"computed_at": latest_study.get("computed_at"), "unseen": best, "positive_years": yrs.get("positive_years"),
                        "test_years": yrs.get("test_years"), "in_force": enforce, "running": bool(st.get("running")),
                        "progress": st.get("progress") if st.get("running") else None, "error": st.get("error")}
        rp = latest(bot) or {}
        wc = rp.get("with_correlation") or {}
        row["replay"] = {"computed_at": rp.get("computed_at"), "days": rp.get("days"), "trades": wc.get("trades"),
                         "avg": wc.get("avg"), "unit": wc.get("unit"), "profit_factor": wc.get("profit_factor"), "ok": rp.get("ok")}
        board.append(row)
    return board


def price_edge_min() -> float | None:
    """The live 15m bot's minimum price edge: the replay's chosen threshold
    while the walk-forward proves it, else None (no price filter)."""
    pe = (latest("kalshi15m") or {}).get("price_edge") or {}
    return float(pe["min_edge_now"]) if pe.get("enforce") and pe.get("min_edge_now") is not None else None


def _alpaca_stock_replay(module_name: str, symbols: list[str], days: int, sides: tuple[str, ...]) -> dict[str, Any]:
    import importlib

    from data import alpaca_data
    m = importlib.import_module(f"data.{module_name}")
    leaders = {s: m.leader_for(s) for s in symbols}

    def history(sym: str) -> pd.DataFrame:
        # The consolidated (SIP) archive on HF when it has the symbol, else
        # Alpaca's REST bars.
        from data import alpaca_sip_history
        import datetime as _d
        cutoff = int(time.time()) - int(days * 1.45) * 86400  # trading days -> calendar days
        years = sorted({_d.datetime.fromtimestamp(cutoff, _d.timezone.utc).year, _d.datetime.now(_d.timezone.utc).year})
        stored = alpaca_sip_history.load(sym, years=list(range(years[0], years[-1] + 1)))
        if not stored.empty:
            return m.regular_session_candles(stored[stored.ts >= cutoff])
        return m.regular_session_candles(alpaca_data.fetch_minute_bars(sym, days=days))

    bars = {s: history(s) for s in sorted(set(symbols) | set(leaders.values()))}

    def go(with_corr: bool) -> dict[str, Any]:
        frames = []
        for s in symbols:
            if bars[s].empty:
                continue
            t = m.replay(bars[s], sides=sides, fee_rate_roundtrip=0.0, spread_bps=m.SPREAD_BPS, entry_allowed=m.entry_allowed,
                         force_exit=m.must_be_flat, leader_df=bars[leaders[s]] if with_corr else None, leader_symbol=leaders[s])
            if not t.empty:
                frames.append(t.assign(symbol=s))
        return summarize(pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(), "net_return")

    return {"universe": symbols, "sides": list(sides), **_both(go)}


STOCK_REPLAY_DAYS = int(os.getenv("SETUP_BACKTEST_STOCK_DAYS", "120") or "120")


def run_stocks(days: int) -> dict[str, Any]:
    from data import alpaca_data
    return _alpaca_stock_replay("alpaca_setup", alpaca_data.get_stock_watchlist(None)[:MAX_SYMBOLS], max(days, STOCK_REPLAY_DAYS),
                                ("long",))


def run_options(days: int) -> dict[str, Any]:
    from data import alpaca_options_data
    return _alpaca_stock_replay("alpaca_options_setup", alpaca_options_data.get_options_universe()[:MAX_SYMBOLS],
                                max(days, STOCK_REPLAY_DAYS), ("long", "short"))


def run_crypto(days: int) -> dict[str, Any]:
    from data import alpaca_crypto_data, alpaca_crypto_setup, alpaca_crypto_strategy
    from data import kalshi_15m_spot
    symbols = [s for s in alpaca_crypto_data.get_crypto_universe()
               if s.split("/")[0].upper() not in alpaca_crypto_setup.STABLECOINS][:MAX_SYMBOLS]
    leaders = {s: alpaca_crypto_setup.leader_for(s) for s in symbols}
    # Same chart the live bot reads: the coin's Coinbase history (HF archive)
    # when it has one, else Alpaca's own bars.
    spot = kalshi_15m_spot.load_spot_history(days=days)
    archived = set(spot["coin"]) if not spot.empty else set()

    def chart(sym: str) -> pd.DataFrame:
        coin = sym.split("/")[0].upper()
        if coin in archived:
            return spot[spot.coin == coin][SPOT_COLUMNS].sort_values("ts").reset_index(drop=True)
        return alpaca_crypto_setup.candles_from_bars(alpaca_crypto_data.fetch_crypto_bars(sym, days=days))

    bars = {s: chart(s) for s in sorted(set(symbols) | set(leaders.values()))}
    fee = 2 * alpaca_crypto_strategy.TAKER_FEE_RATE

    def go(with_corr: bool) -> dict[str, Any]:
        frames = []
        for s in symbols:
            if bars[s].empty:
                continue
            t = alpaca_crypto_setup.replay(bars[s], sides=("long",), fee_rate_roundtrip=fee, spread_bps=alpaca_crypto_setup.SPREAD_BPS,
                                           leader_df=bars[leaders[s]] if with_corr else None, leader_symbol=leaders[s])
            if not t.empty:
                frames.append(t.assign(symbol=s))
        return summarize(pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(), "net_return")

    return {"universe": symbols, "fee_rate_roundtrip": fee, **_both(go)}


# ---------------------------------------------------------------------------
# Multi-year study (stocks/options): every symbol over the whole SIP archive
# ---------------------------------------------------------------------------
MULTIYEAR_WORKERS = int(os.getenv("SETUP_MULTIYEAR_WORKERS", "5") or "5")
ELIGIBILITY_MIN_TRADES = int(os.getenv("SETUP_ELIGIBILITY_MIN_TRADES", "6") or "6")
ELIGIBILITY_LOOKBACK_YEARS = int(os.getenv("SETUP_ELIGIBILITY_LOOKBACK_YEARS", "2") or "2")
# Plan settings the method leaves open (stop distance beyond invalidation,
# minimum reward/risk), trained walk-forward on the archive.
PARAM_GRID = [tuple(float(x) for x in item.split(":")) for item in
              os.getenv("SETUP_MULTIYEAR_PARAM_GRID", "0.5:2.0,1.0:2.0,1.0:3.0,1.5:3.0").split(",") if ":" in item]
MULTIYEAR = {
    "stocks": {"module": "alpaca_setup", "sides": ("long",)},
    "options": {"module": "alpaca_options_setup", "sides": ("long", "short")},
}


class _SettingFreeCache:
    """A setup module's evaluate, shared across the plan settings of one
    symbol's study: a bar where every side fails a rule before the
    reward/risk step comes out the same at every setting (the stop distance
    and minimum reward/risk only enter the plan), so it is computed once and
    remembered as a small marker; bars that reach the plan are evaluated per
    setting. The replays only read `valid` (and the plan/setup_id of valid
    results), so the marker is all they need."""

    def __init__(self, module):
        self.module, self.original = module, module.evaluate
        self.early = set(module.CHECK_ORDER[:module.CHECK_ORDER.index("risk_reward")])
        self.cache: dict[tuple, dict[str, Any]] = {}
        self.hits = self.misses = 0

    def __call__(self, ctx, as_of, **kw):
        key = (int(as_of), tuple(kw.get("sides") or ()), kw.get("leader") is not None, bool(kw.get("require_leader")))
        hit = self.cache.get(key)
        if hit is not None:
            self.hits += 1
            return hit
        self.misses += 1
        r = self.original(ctx, as_of, **kw)
        sides = r.get("by_side") or {}
        if not r.get("valid") and all(side.get("reason") in self.early for side in sides.values()):
            self.cache[key] = {"valid": False, "reason": r.get("reason")}
        return r

    def __enter__(self):
        self.module.evaluate = self
        # The same chart is prepared once for every setting replayed on it
        # (indicators, 5m/15m frames, VWAP: seconds per year of minutes).
        self.prepare_original = self.module.prepare
        prepared: dict[tuple, Any] = {}

        def prepare(df1, *a, **kw):
            ts = df1["ts"].to_numpy("int64") if len(df1) else None
            key = (len(df1), int(ts[0]) if ts is not None else None, int(ts[-1]) if ts is not None else None,
                   float(df1["close"].iloc[-1]) if len(df1) else None, a, tuple(sorted(kw.items())))
            if key not in prepared:
                prepared[key] = self.prepare_original(df1, *a, **kw)
            return prepared[key]

        self.module.prepare = prepare
        return self

    def __exit__(self, *exc):
        self.module.evaluate = self.original
        self.module.prepare = self.prepare_original


def _multiyear_symbol(args: tuple) -> pd.DataFrame:
    """One symbol's full-archive replay through its bot's own module, once
    per plan setting (data loaded once)."""
    sym, module, sides = args[:3]
    grid = args[3] if len(args) > 3 else [None]
    try:
        os.nice(15)
    except (AttributeError, OSError):
        pass
    import importlib

    from data import alpaca_sip_history
    m = importlib.import_module(f"data.{module}")
    bars = alpaca_sip_history.load(sym)
    if bars.empty:
        return pd.DataFrame()
    lead_sym = m.leader_for(sym)
    lead = alpaca_sip_history.load(lead_sym)
    rth, lead_rth = m.regular_session_candles(bars), (m.regular_session_candles(lead) if not lead.empty else None)
    default = (m.STOP_BUFFER_ATR15, m.MIN_RR)
    frames = []
    with _SettingFreeCache(m):
        for setting in grid:
            stop_buffer, min_rr = setting or default
            m.STOP_BUFFER_ATR15, m.MIN_RR = stop_buffer, min_rr
            t = m.replay(rth, sides=sides, fee_rate_roundtrip=0.0, spread_bps=m.SPREAD_BPS, entry_allowed=m.entry_allowed,
                         force_exit=m.must_be_flat, leader_df=lead_rth, leader_symbol=lead_sym)
            if not t.empty:
                frames.append(t.assign(symbol=sym, param=f"{stop_buffer}:{min_rr}"))
    m.STOP_BUFFER_ATR15, m.MIN_RR = default
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# ---------------------------------------------------------------------------
# Perps and the 15m bot study on Alpaca's archives: Kraken-via-Alpaca minute
# bars (crypto, since 2023) and SIP (commodity ETFs, since 2016). Every trade
# carries what was known at its entry -- hour and weekday, the news in the
# hours before (Alpaca news archive, no lookahead), the leader's reading --
# so the walk-forward can learn which conditions the setup pays in.
# ---------------------------------------------------------------------------
KALSHI_STUDY_BOTS = ("perps", "kalshi15m")
# Bots whose multi-year study replays Alpaca's minute-bar archives with the
# entry-condition learning (the Kalshi bots and the Alpaca crypto bot).
ARCHIVE_STUDY_BOTS = KALSHI_STUDY_BOTS + ("crypto", "stocks", "options")
MULTIYEAR.update({
    # lookback 0: each year's choices are learned from every year before it
    # (the whole archive, expanding), not only the last one.
    "perps": {"module": "perps_setup", "sides": ("long", "short"), "lookback": 0},
    "kalshi15m": {"module": "kalshi_15m_setup", "sides": ("long",), "lookback": 0},  # buying NO lost every year
    # Alpaca crypto is spot: long only, every pair it trades.
    "crypto": {"module": "alpaca_crypto_setup", "sides": ("long",), "lookback": 0},
    # Stocks and options on the SIP archive since 2016, with the same
    # every-prior-year learning, settings and entry conditions (news since
    # 2016 included) as the other bots.
    "stocks": {"module": "alpaca_setup", "sides": ("long",), "lookback": 0},
    "options": {"module": "alpaca_options_setup", "sides": ("long", "short"), "lookback": 0},
})
# Every archive bot searches a thousand-plus combinations around its own
# method, each crossed with every symbol and entry condition and scored
# only on years the choice never saw:
#   plan     stop distance beyond the invalidation (x 15m ATR) x reward/risk;
#   exits    for bots that hold positions -- a time limit (hours) x a
#            break-even stop (moves to entry once the trade is that many R
#            in profit; 0 = off), all replayed in one pass;
#   15m      entry window (the last minute of the window an entry may come
#            in) x exit style (0 sell at the planned stop or target,
#            1 sell only at the target, 2 hold to settlement).
STOP_BUFFERS = tuple(float(x) for x in os.getenv("SETUP_STUDY_STOP_BUFFERS", "0.5,1,1.5,2,2.5,3,4,5").split(","))
TARGETS = tuple(float(x) for x in os.getenv("SETUP_STUDY_TARGETS", "1.25,1.5,2,2.5,3,4,5").split(","))
TARGETS_15M = tuple(float(x) for x in os.getenv("SETUP_STUDY_TARGETS_15M", "1.25,1.5,1.75,2,2.5,3,3.5,4,5").split(","))
BOT_EXITS = {
    bot: [(float(h), float(be)) for h in holds for be in bes]
    for bot, holds, bes in (("perps", (2, 4, 8, 12, 24), (0, 0.5, 0.75, 1, 1.5, 2)),
                            ("crypto", (2, 4, 8, 12, 24), (0, 0.5, 0.75, 1, 1.5, 2)),
                            ("stocks", (0.5, 1, 2, 4, 6.5), (0, 0.5, 1, 1.5)),
                            ("options", (0.5, 1, 2, 4, 6.5), (0, 0.5, 1, 1.5)))
}
VARIANTS_15M = [(float(m), float(x)) for m in (2, 3, 5) for x in (0, 1, 2)]
# Legacy names kept for anything still reading them.
KALSHI_PARAM_GRID = [(sb, rr) for sb in STOP_BUFFERS for rr in TARGETS]
PERPS_PARAM_GRID = KALSHI_PARAM_GRID
PERPS_EXITS = BOT_EXITS["perps"]
PARAM_KEYS = ("STOP_BUFFER_ATR15", "MIN_RR", "MAX_HOLD_HOURS", "BREAKEVEN_R")
PARAM_KEYS_15M = ("STOP_BUFFER_ATR15", "MIN_RR", "ENTRY_MAX_MINUTE", "EXIT_MODE")


def _param_label(*values: float) -> str:
    return ":".join(str(float(v)) for v in values)


def _param_values(param: str | None, keys: tuple[str, ...] = PARAM_KEYS) -> dict[str, float | None]:
    """A study setting's label as the bot's parameters ("1.5:3.0" ->
    stop buffer and minimum reward/risk; perps adds hold hours and the
    break-even trigger)."""
    if not param:
        return {"STOP_BUFFER_ATR15": None, "MIN_RR": None}
    return dict(zip(keys, (float(x) for x in str(param).split(":"))))


def param_keys(bot: str) -> tuple[str, ...]:
    return PARAM_KEYS_15M if bot == "kalshi15m" else PARAM_KEYS


def grid_labels(bot: str) -> list[str]:
    """Every combination a bot's study scores, as the labels its trades carry."""
    grid = study_grid(bot)
    if bot in BOT_EXITS:
        return [_param_label(sb, rr, hold, be) for sb, rr in grid for hold, be in BOT_EXITS[bot]]
    if bot == "kalshi15m":
        return [_param_label(sb, rr, minute, mode) for sb, rr in grid for minute, mode in VARIANTS_15M]
    return [f"{a}:{b}" for a, b in grid]


def default_param(bot: str) -> str:
    """The bot's current (untrained) setting, as a study label."""
    import importlib
    m = importlib.import_module(f"data.{MULTIYEAR[bot]['module']}")
    if bot in BOT_EXITS:
        return _param_label(m.STOP_BUFFER_ATR15, m.MIN_RR, m.MAX_HOLD_HOURS, m.BREAKEVEN_R)
    if bot == "kalshi15m":
        return _param_label(m.STOP_BUFFER_ATR15, m.MIN_RR, m.ENTRY_MAX_MINUTE, m.EXIT_MODE)
    return f"{m.STOP_BUFFER_ATR15}:{m.MIN_RR}"


PATTERN_FEATURES = ("hour_block", "weekday", "news", "leader", "side", "vol_regime", "us_market")
VOL_WINDOW_BARS = {"utc_day": 1440, "us_equity": 390}  # one day of 1-minute bars
VOL_LOOKBACK_DAYS = 90
PATTERN_MIN_TRADES = int(os.getenv("SETUP_PATTERN_MIN_TRADES", "20") or "20")
PATTERN_MAX_T = float(os.getenv("SETUP_PATTERN_MAX_T", "-1.0") or "-1.0")
NEWS_HOURS = 6.0
_WEEKDAYS = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")


def pattern_features(*, ts: int, side: str, news_count: float | None, news_score: float | None,
                     leader_corr: float | None, leader_dir: str | None, vol_regime: str | None = None,
                     us_market: str | None = None) -> dict[str, str]:
    """The conditions a trade entered in, bucketed the same way in the
    studies and live: 4-hour UTC block, weekday, the news over the prior
    NEWS_HOURS relative to the trade's side, the leader, the side itself and
    the volatility regime (see vol_regimes)."""
    t = dt.datetime.fromtimestamp(int(ts), dt.timezone.utc)
    sign = 1.0 if side == "long" else -1.0
    if not news_count:
        news = "none"
    else:
        aligned = (news_score or 0.0) * sign
        news = "with" if aligned > 0.05 else ("against" if aligned < -0.05 else "neutral")
    if leader_corr is None or leader_dir is None:
        leader = "n/a"
    elif abs(float(leader_corr)) < 0.5:
        leader = "independent"
    else:
        implied = {"up": 1, "down": -1}.get(leader_dir, 0) * (1 if float(leader_corr) > 0 else -1)
        leader = "with" if implied == sign else ("mixed" if implied == 0 else "against")
    return {"hour_block": f"h{t.hour // 4 * 4:02d}", "weekday": _WEEKDAYS[t.weekday()], "news": news, "leader": leader,
            "side": side, "vol_regime": vol_regime or "n/a", "us_market": us_market or "n/a"}


def blocked_reason(blocked: dict[str, list[str]] | None, features: dict[str, str]) -> str | None:
    """Which learned losing condition (if any) this entry falls in."""
    for f, buckets in (blocked or {}).items():
        if features.get(f) in set(buckets):
            return f"{f}={features[f]}"
    return None


def _study_candles(sym: str, bot: str | None = None) -> tuple[pd.DataFrame, str]:
    """(1-minute candles with ts = END, session) from Alpaca's archives."""
    from data import alpaca_crypto_history, alpaca_setup, alpaca_sip_history, kalshi_15m_setup
    if bot in ("stocks", "options"):
        return alpaca_setup.regular_session_candles(alpaca_sip_history.load(sym)), "us_equity"
    if sym in kalshi_15m_setup.METAL_CHART_SYMBOL:
        return alpaca_setup.regular_session_candles(alpaca_sip_history.load(kalshi_15m_setup.METAL_CHART_SYMBOL[sym])), "us_equity"
    return alpaca_crypto_history.candles(sym.split("/")[0].upper()), "utc_day"


def _daily_vol(candles: pd.DataFrame, session: str) -> pd.Series:
    """Rolling one-day realized volatility of 1-minute log returns, indexed
    by candle end time."""
    c = candles.sort_values("ts").drop_duplicates("ts")
    r = np.log(c["close"].astype(float)).diff()
    w = VOL_WINDOW_BARS.get(session, 1440)
    return pd.Series(r.rolling(w, min_periods=w // 2).std().to_numpy(), index=c["ts"].to_numpy("int64"))


def vol_prep(candles: pd.DataFrame, session: str) -> tuple | None:
    """The coin's daily-volatility series and its rolling thirds over the
    prior VOL_LOOKBACK_DAYS (sampled hourly) -- computed once per coin."""
    v = _daily_vol(candles, session)
    if v.dropna().empty:
        return None
    hourly = v.iloc[::60].dropna()
    per_day = 24 if session == "utc_day" else 7
    win = VOL_LOOKBACK_DAYS * per_day
    q1 = hourly.rolling(win, min_periods=win // 3).quantile(1 / 3)
    q2 = hourly.rolling(win, min_periods=win // 3).quantile(2 / 3)
    return v, hourly, q1, q2


def vol_regimes(candles: pd.DataFrame | None, entry_ts, session: str, *, prep: tuple | None = None) -> tuple[list[str], list[float] | None]:
    """Each entry's volatility regime -- 'low' / 'normal' / 'high' by where
    the last day's volatility sat in the thirds of the prior
    VOL_LOOKBACK_DAYS (sampled hourly, no lookahead) -- and today's
    thresholds [low|normal, normal|high] for the live bot."""
    prep = prep if prep is not None else vol_prep(candles, session)
    if prep is None:
        return ["n/a"] * len(entry_ts), None
    v, hourly, q1, q2 = prep
    vi, hi = v.index.to_numpy(), hourly.index.to_numpy()
    out = []
    for ts in entry_ts:
        i, j = int(np.searchsorted(vi, int(ts), side="right")) - 1, int(np.searchsorted(hi, int(ts), side="right")) - 1
        if i < 0 or j < 0 or np.isnan(v.iat[i]) or np.isnan(q1.iat[j]) or np.isnan(q2.iat[j]):
            out.append("n/a")
            continue
        out.append("low" if v.iat[i] < q1.iat[j] else "high" if v.iat[i] > q2.iat[j] else "normal")
    now = [float(q1.iat[-1]), float(q2.iat[-1])] if not (np.isnan(q1.iat[-1]) or np.isnan(q2.iat[-1])) else None
    return out, now


def vol_regime_now(candles: pd.DataFrame | None, thresholds: list[float] | None, session: str) -> str:
    """Live: the chart's last-day volatility against the study's thresholds."""
    if candles is None or candles.empty or not thresholds:
        return "n/a"
    v = _daily_vol(candles, session).dropna()
    if v.empty:
        return "n/a"
    x = float(v.iat[-1])
    return "low" if x < thresholds[0] else "high" if x > thresholds[1] else "normal"


def us_market_states(spy: pd.DataFrame | None, entry_ts) -> list[str]:
    """Where the US stock market stood at each entry: 'up' / 'down' (SPY's
    last regular-session close vs that session's open) or 'closed' --
    SPY's regular-session 1-minute candles (ts = END), no lookahead."""
    out = []
    if spy is None or spy.empty:
        return ["n/a"] * len(entry_ts)
    d = spy.sort_values("ts")
    ts = d["ts"].to_numpy("int64")
    day = pd.to_datetime(d["ts"] - 60, unit="s", utc=True).dt.tz_convert("America/New_York").dt.strftime("%Y-%m-%d").to_numpy()
    first_open = pd.Series(d["open"].to_numpy(float)).groupby(day).transform("first").to_numpy()
    close = d["close"].to_numpy(float)
    for t in entry_ts:
        i = int(np.searchsorted(ts, int(t), side="right")) - 1
        et = dt.datetime.fromtimestamp(int(t), dt.timezone.utc).astimezone(_ET_ZONE)
        in_session = et.weekday() < 5 and (9 * 60 + 30) <= et.hour * 60 + et.minute < 16 * 60
        if not in_session:
            out.append("closed")
        elif i < 0 or day[i] != et.strftime("%Y-%m-%d") or int(t) - ts[i] > 600:
            out.append("n/a")
        else:
            out.append("up" if close[i] >= first_open[i] else "down")
    return out


def us_market_now(now: float | None = None) -> str:
    """Live: the same reading from SPY's Alpaca bars (stream on top)."""
    try:
        from data import alpaca_data, alpaca_setup, alpaca_stream
        bars = alpaca_stream.merge_live("stocks", "SPY", alpaca_data.fetch_recent_minute_bars("SPY"))
        return us_market_states(alpaca_setup.regular_session_candles(bars), [int(now or time.time())])[0]
    except Exception:
        return "n/a"


def _annotate(trades: pd.DataFrame, sym: str, news_idx, regimes: list[str] | None = None,
              us_market: list[str] | None = None) -> pd.DataFrame:
    rows = []
    for k, r in enumerate(trades.itertuples(index=False)):
        n = news_idx.at(int(r.entry_ts), hours=NEWS_HOURS) if news_idx is not None else {"count": 0.0, "score": 0.0}
        rows.append(pattern_features(ts=int(r.entry_ts), side=r.side, news_count=n["count"], news_score=n["score"],
                                     leader_corr=getattr(r, "leader_corr", None), leader_dir=getattr(r, "leader_dir", None),
                                     vol_regime=regimes[k] if regimes else None, us_market=us_market[k] if us_market else None)
                    | {"news_count": n["count"], "news_score": n["score"]})
    feats = pd.DataFrame(rows)
    # The replay already carries `side` (and the same value): one column each.
    feats = feats.drop(columns=[c for c in feats.columns if c in trades.columns])
    return pd.concat([trades.reset_index(drop=True), feats], axis=1)


def _one_column_each(df: pd.DataFrame) -> pd.DataFrame:
    return df.loc[:, ~df.columns.duplicated()] if df.columns.duplicated().any() else df


def _multiyear_kalshi_symbol(args: tuple) -> pd.DataFrame:
    """One perps / 15m / Alpaca-crypto study symbol over Alpaca's whole
    archive, once per plan setting, each trade annotated with its entry
    conditions."""
    bot, sym, grid, cost = args
    try:
        os.nice(15)
    except (AttributeError, OSError):
        pass
    import importlib

    from data import alpaca_news, alpaca_news_history
    m = importlib.import_module(f"data.{MULTIYEAR[bot]['module']}")
    candles, session = _study_candles(sym, bot)
    if candles.empty:
        return pd.DataFrame()
    lead_sym = m.leader_for(sym)
    lead, lead_session = _study_candles(lead_sym, bot)
    # Only this asset's articles, read month by month (small in memory).
    news_idx = alpaca_news_history.index_for(alpaca_news.news_symbols(sym))
    from data import alpaca_setup, alpaca_sip_history
    spy = alpaca_setup.regular_session_candles(alpaca_sip_history.load("SPY"))
    default = (m.STOP_BUFFER_ATR15, m.MIN_RR)
    frames = []
    vprep = vol_prep(candles, session)  # the coin's volatility, once for every setting
    cache = _SettingFreeCache(m).__enter__()
    for setting in grid:
        m.STOP_BUFFER_ATR15, m.MIN_RR = setting
        exits = [(hold * 60.0, be) for hold, be in BOT_EXITS.get(bot, [])]
        if bot == "perps":
            t = m.replay(candles, sides=MULTIYEAR[bot]["sides"], fee_rate_roundtrip=cost["fee_rate_roundtrip"],
                         spread_bps=cost["spread_bps"], leader_df=lead if not lead.empty else None, leader_symbol=lead_sym,
                         session=session, leader_session=lead_session, exits=exits)
        elif bot == "crypto":
            t = m.replay(candles, sides=MULTIYEAR[bot]["sides"], fee_rate_roundtrip=cost["fee_rate_roundtrip"],
                         spread_bps=cost["spread_bps"], leader_df=lead if not lead.empty else None, leader_symbol=lead_sym,
                         exits=exits)
        elif bot in ("stocks", "options"):
            t = m.replay(candles, sides=MULTIYEAR[bot]["sides"], fee_rate_roundtrip=cost["fee_rate_roundtrip"],
                         spread_bps=cost["spread_bps"], entry_allowed=m.entry_allowed, force_exit=m.must_be_flat,
                         leader_df=lead if not lead.empty else None, leader_symbol=lead_sym, exits=exits)
        else:
            t = m.replay_windows(candles, half_spread=cost["half_spread"], leader_1m=lead if not lead.empty else None,
                                 leader_symbol=lead_sym, session=session, leader_session=lead_session,
                                 minute_average=m.settles_on_minute_average(sym), variants=VARIANTS_15M)
        if not t.empty:
            regimes, now = vol_regimes(None, t["entry_ts"].to_numpy("int64"), session, prep=vprep)
            if "hold_h" in t:
                label = [_param_label(setting[0], setting[1], hold, be) for hold, be in zip(t["hold_h"], t["be_r"])]
            elif "entry_max_minute" in t:
                label = [_param_label(setting[0], setting[1], mm, xm) for mm, xm in zip(t["entry_max_minute"], t["exit_mode"])]
            else:
                label = f"{setting[0]}:{setting[1]}"
            frames.append(_annotate(t, sym, news_idx, regimes, us_market_states(spy, t["entry_ts"].to_numpy("int64"))).assign(
                symbol=sym, param=label, vol_q_low=(now or [None, None])[0], vol_q_high=(now or [None, None])[1]))
    cache.__exit__()
    m.STOP_BUFFER_ATR15, m.MIN_RR = default
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _kalshi_study_costs(bot: str, symbols: list[str]) -> dict[str, dict[str, float]]:
    """Real costs per symbol, read once before the study: perps' fee
    schedule and live spread; the 15m bot's median half-spread at minutes
    1-5 in the Kalshi quote archive (the overall median for coins without
    quotes yet)."""
    costs: dict[str, dict[str, float]] = {}
    if bot in ("stocks", "options"):
        import importlib
        m = importlib.import_module(f"data.{MULTIYEAR[bot]['module']}")
        return {sym: {"fee_rate_roundtrip": 0.0, "spread_bps": float(m.SPREAD_BPS)} for sym in symbols}
    if bot == "crypto":
        from data import alpaca_client, alpaca_crypto_setup, alpaca_crypto_strategy
        fee = 2 * float(alpaca_crypto_strategy.TAKER_FEE_RATE)
        for sym in symbols:
            spread = None
            try:
                q = alpaca_client.get_crypto_latest_quote(sym)  # the venue its orders execute on
                bid, ask = float(q.get("bp") or 0), float(q.get("ap") or 0)
                if 0 < bid <= ask:
                    spread = (ask - bid) / ((ask + bid) / 2) * 1e4
            except Exception:
                spread = None
            costs[sym] = {"fee_rate_roundtrip": fee, "spread_bps": spread if spread is not None else float(alpaca_crypto_setup.SPREAD_BPS)}
        return costs
    if bot == "perps":
        from data import kalshi_15m_spot, perps_setup, perps_strategy
        from data.kalshi_perps import KNOWN_PERP_TICKERS, get_margin_market
        from data.perps_data import coin_for_ticker
        tickers = {kalshi_15m_spot.chart_coin(coin_for_ticker(t)): t for t in KNOWN_PERP_TICKERS}
        tickers.update({c: f"KX{c}PERP" for c in ("GOLD", "SILVER")})
        for sym in symbols:
            ticker = tickers.get(sym, f"KX{sym}PERP")
            try:
                fee = float(perps_strategy.setup_fee_rate_roundtrip(ticker))
            except Exception:
                fee = 0.0015
            try:
                spread = perps_setup.market_spread_bps(get_margin_market(ticker).get("market") or {})
            except Exception:
                spread = None
            costs[sym] = {"fee_rate_roundtrip": fee, "spread_bps": spread if spread is not None else 5.0}
        return costs
    from data import kalshi_15m_quotes
    try:
        q = kalshi_15m_quotes.load_quote_history(days=30)
        q = q[(q["minute"] >= 1) & (q["minute"] <= 5) & (q["yes_ask"] > q["yes_bid"])]
        half = ((q["yes_ask"] - q["yes_bid"]) / 2.0).groupby(q["coin"]).median().to_dict()
        overall = float(((q["yes_ask"] - q["yes_bid"]) / 2.0).median()) if not q.empty else 0.01
    except Exception:
        half, overall = {}, 0.01
    return {sym: {"half_spread": float(half.get(sym, overall))} for sym in symbols}


def learn_blocked(window: pd.DataFrame, *, min_trades: int = PATTERN_MIN_TRADES, max_t: float = PATTERN_MAX_T) -> dict[str, list[str]]:
    """Conditions the setup clearly lost money in over `window`: per
    feature, buckets with >= min_trades and a t-stat of the mean net return
    at or below max_t."""
    blocked: dict[str, list[str]] = {}
    for f in PATTERN_FEATURES:
        if f not in window or window.empty:
            continue
        per = window.groupby(f)["net_return"].agg(["size", "mean", "std"])
        t = per["mean"] / (per["std"] / np.sqrt(per["size"]))
        bad = per[(per["size"] >= min_trades) & (per["mean"] < 0) & (t <= max_t)]
        if len(bad):
            blocked[f] = sorted(str(b) for b in bad.index)
    return blocked


def _keep(df: pd.DataFrame, blocked: dict[str, list[str]]) -> pd.Series:
    keep = pd.Series(True, index=df.index)
    for f, buckets in blocked.items():
        if f in df:
            keep &= ~df[f].astype(str).isin(buckets)
    return keep


def _choose_setting(window: pd.DataFrame, *, min_trades: int, min_train_trades: int) -> tuple[str | None, set[str], float]:
    """The plan setting whose eligible symbols made the most over `window`."""
    best: tuple[str | None, set[str], float] = (None, set(), float("-inf"))
    for param, g in window.groupby("param"):
        per = g.groupby("symbol")["net_return"].agg(["size", "mean"])
        eligible = set(per[(per["size"] >= min_trades) & (per["mean"] > 0)].index)
        picked = g[g["symbol"].isin(eligible)]["net_return"]
        if len(picked) >= min_train_trades and picked.sum() > best[2]:
            best = (str(param), eligible, float(picked.sum()))
    return best


def _train_window(t: pd.DataFrame, y: int, lookback_years: int) -> pd.DataFrame:
    """The years a choice for year y is learned from: the prior
    lookback_years, or with 0 every year before y (expanding window)."""
    return t[(t["year"] < y) & ((t["year"] >= y - lookback_years) if lookback_years else True)]


def _lookback_words(lookback_years: int) -> str:
    return "every prior year" if not lookback_years else f"the prior {lookback_years} year(s)"


def _first_test_year(t: pd.DataFrame, lookback_years: int) -> int:
    return int(t["year"].min()) + (lookback_years or 1)


def _recent(t: pd.DataFrame, lookback_years: int) -> pd.DataFrame:
    """What today's choice is learned from: the last lookback_years, or all."""
    return t[t["year"] > int(t["year"].max()) - lookback_years] if lookback_years else t


def walk_forward_patterns(trades: pd.DataFrame, *, default_param: str, lookback_years: int,
                          min_trades: int = ELIGIBILITY_MIN_TRADES, min_train_trades: int = 30,
                          keys: tuple[str, ...] = PARAM_KEYS) -> dict[str, Any]:
    """Training on top of the trained setting and symbols: each year, also
    learn from the prior `lookback_years` which entry conditions lost
    (learn_blocked), and skip them in the year itself. Reports the
    out-of-sample record with and without the learned conditions, per
    condition what the setup earned, and today's blocked conditions."""
    t = trades.copy()
    t["year"] = pd.to_datetime(t["entry_ts"], unit="s", utc=True).dt.year
    years, plain, filtered = [], [], []
    for y in range(_first_test_year(t, lookback_years), int(t["year"].max()) + 1):
        window = _train_window(t, y, lookback_years)
        param, eligible, _ = _choose_setting(window, min_trades=min_trades, min_train_trades=min_train_trades)
        if not param:
            years.append({"year": y, "param": None})
            continue
        blocked = learn_blocked(window[(window["param"] == param) & window["symbol"].isin(eligible)])
        test = t[(t["year"] == y) & (t["param"] == param) & t["symbol"].isin(eligible)]
        kept = test[_keep(test, blocked)]
        plain.append(test)
        filtered.append(kept)
        years.append({"year": y, "param": param, "eligible": len(eligible), "blocked": blocked,
                      "trained": _trade_stats(test["net_return"]), "with_patterns": _trade_stats(kept["net_return"])})
    cat = lambda fr: pd.concat(fr, ignore_index=True) if fr else pd.DataFrame(columns=["net_return"])  # noqa: E731
    p_all, f_all = cat(plain), cat(filtered)
    trained, with_patterns = _trade_stats(p_all["net_return"]), _trade_stats(f_all["net_return"])
    positive_years = sum(1 for y in years if (y.get("with_patterns") or {}).get("avg", 0) > 0)
    recent = _recent(t, lookback_years)
    param_now, eligible_now, _ = _choose_setting(recent, min_trades=min_trades, min_train_trades=min_train_trades)
    blocked_now = learn_blocked(recent[(recent["param"] == param_now) & recent["symbol"].isin(eligible_now)]) if param_now else {}
    base = t[(t["year"] >= _first_test_year(t, lookback_years)) & (t["param"] == default_param)]
    by_condition = {f: {str(k): _trade_stats(g["net_return"]) for k, g in base.groupby(f)} for f in PATTERN_FEATURES if f in base}
    enforce = bool(with_patterns.get("trades", 0) >= 30 and (with_patterns.get("avg") or 0) > 0
                   and (with_patterns.get("avg") or 0) >= (trained.get("avg") or 0) and positive_years * 2 >= len(years))
    return {"years": years, "trained": trained, "with_patterns": with_patterns,
            "default_every_symbol": _trade_stats(base["net_return"]), "positive_years": positive_years,
            "test_years": len(years), "by_condition": by_condition, "param_now": _param_values(param_now, keys),
            "eligible_now": sorted(eligible_now), "blocked_now": blocked_now, "enforce": enforce,
            "rule": (f"setting, symbols and losing entry conditions learned on {_lookback_words(lookback_years)}; "
                     f"a condition is skipped when >= {PATTERN_MIN_TRADES} trades lost with t <= {PATTERN_MAX_T}")}


def _trade_stats(x: pd.Series) -> dict[str, Any]:
    x = pd.Series(x, dtype=float)
    if x.empty:
        return {"trades": 0}
    t = x.mean() / (x.std(ddof=1) / np.sqrt(len(x))) if len(x) > 2 and x.std(ddof=1) > 0 else None
    return {"trades": int(len(x)), "win_rate": round(float((x > 0).mean()), 4), "avg": round(float(x.mean()), 6),
            "total": round(float(x.sum()), 4), "t_stat": None if t is None else round(float(t), 2)}


def walk_forward_eligibility(trades: pd.DataFrame, *, min_trades: int = ELIGIBILITY_MIN_TRADES,
                             lookback_years: int = ELIGIBILITY_LOOKBACK_YEARS) -> dict[str, Any]:
    """Each year, trade only symbols whose setup made money (>= min_trades,
    average net return > 0) over the prior `lookback_years`; score that on
    the year itself, which the selection never saw. Returns the per-year
    record, the out-of-sample total vs trading every symbol, today's
    eligible list, and whether the evidence supports enforcing it."""
    t = trades.copy()
    t["year"] = pd.to_datetime(t["entry_ts"], unit="s", utc=True).dt.year
    years, picked_frames = [], []
    for y in range(_first_test_year(t, lookback_years), int(t["year"].max()) + 1):
        train = _train_window(t, y, lookback_years)
        per = train.groupby("symbol")["net_return"].agg(["size", "mean"])
        eligible = set(per[(per["size"] >= min_trades) & (per["mean"] > 0)].index)
        test = t[t["year"] == y]
        picked = test[test["symbol"].isin(eligible)]
        picked_frames.append(picked)
        years.append({"year": y, "eligible": len(eligible), "picked": _trade_stats(picked["net_return"]),
                      "all": _trade_stats(test["net_return"])})
    picked_all = pd.concat(picked_frames, ignore_index=True) if picked_frames else pd.DataFrame(columns=["net_return"])
    first_test = _first_test_year(t, lookback_years)
    every = t[t["year"] >= first_test]
    oos, base = _trade_stats(picked_all["net_return"]), _trade_stats(every["net_return"])
    positive_years = sum(1 for y in years if (y["picked"].get("avg") or 0) > 0)
    latest_year = int(t["year"].max())
    recent = _recent(t, lookback_years)
    per = recent.groupby("symbol")["net_return"].agg(["size", "mean"])
    current = sorted(per[(per["size"] >= min_trades) & (per["mean"] > 0)].index)
    enforce = bool(oos.get("trades", 0) >= 30 and (oos.get("avg") or 0) > 0 and (oos.get("avg") or 0) > (base.get("avg") or 0)
                   and positive_years * 2 >= len(years))
    return {"years": years, "out_of_sample": oos, "every_symbol": base, "positive_years": positive_years,
            "test_years": len(years), "eligible_now": current, "enforce": enforce,
            "rule": f">= {min_trades} trades and average net > 0 over {_lookback_words(lookback_years)}"}


def walk_forward_trained(trades: pd.DataFrame, *, default_param: str, min_trades: int = ELIGIBILITY_MIN_TRADES,
                         lookback_years: int = ELIGIBILITY_LOOKBACK_YEARS, min_train_trades: int = 30,
                         keys: tuple[str, ...] = PARAM_KEYS) -> dict[str, Any]:
    """Each year, choose the plan setting and the eligible symbols from the
    prior `lookback_years` only (the setting whose eligible symbols made the
    most in that window), then trade exactly that on the year itself. The
    out-of-sample record is what training would actually have earned."""
    t = trades.copy()
    t["year"] = pd.to_datetime(t["entry_ts"], unit="s", utc=True).dt.year

    def choose(window: pd.DataFrame) -> tuple[str | None, set[str], float]:
        best: tuple[str | None, set[str], float] = (None, set(), float("-inf"))
        for param, g in window.groupby("param"):
            per = g.groupby("symbol")["net_return"].agg(["size", "mean"])
            eligible = set(per[(per["size"] >= min_trades) & (per["mean"] > 0)].index)
            picked = g[g["symbol"].isin(eligible)]["net_return"]
            if len(picked) >= min_train_trades and picked.sum() > best[2]:
                best = (str(param), eligible, float(picked.sum()))
        return best

    years, oos = [], []
    for y in range(_first_test_year(t, lookback_years), int(t["year"].max()) + 1):
        param, eligible, _ = choose(_train_window(t, y, lookback_years))
        test = t[(t["year"] == y) & (t["param"] == param) & (t["symbol"].isin(eligible))] if param else t.iloc[0:0]
        oos.append(test)
        years.append({"year": y, "param": param, "eligible": len(eligible), "result": _trade_stats(test["net_return"])})
    oos_all = pd.concat(oos, ignore_index=True) if oos else pd.DataFrame(columns=["net_return"])
    first = _first_test_year(t, lookback_years)
    baseline = t[(t["year"] >= first) & (t["param"] == default_param)]
    trained, base = _trade_stats(oos_all["net_return"]), _trade_stats(baseline["net_return"])
    positive_years = sum(1 for y in years if (y["result"].get("avg") or 0) > 0)
    latest = int(t["year"].max())
    param_now, eligible_now, _ = choose(_recent(t, lookback_years))
    enforce = bool(trained.get("trades", 0) >= 30 and (trained.get("avg") or 0) > 0
                   and (trained.get("avg") or 0) > (base.get("avg") or 0) and positive_years * 2 >= len(years))
    return {"years": years, "out_of_sample": trained, "default_every_symbol": base, "positive_years": positive_years,
            "test_years": len(years), "param_now": _param_values(param_now, keys),
            "eligible_now": sorted(eligible_now), "enforce": enforce,
            "rule": f"setting and symbols chosen on {_lookback_words(lookback_years)}: >= {min_trades} trades, average net > 0"}


def _rss_mb() -> float | None:
    """This process's peak memory (MB), for the study's progress record."""
    try:
        import resource
        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return round(peak / (1024 * 1024 if sys.platform == "darwin" else 1024), 1)
    except Exception:
        return None


PARTS_PATH = "setup_strategy/multiyear/parts"


def _parts_dir(bot: str) -> Path:
    return LOCAL_DIR / f"{bot}_multiyear_parts"


def _hf_parts(fn, timeout_sec: float = 300):
    """Best-effort call against the bot's HF model repo (None without a
    token, on an error or after timeout_sec)."""
    token = os.getenv("HF_API_KEY", "")
    if not token:
        return None
    try:
        from huggingface_hub import HfApi

        from server_common import call_with_hard_timeout
        return call_with_hard_timeout(lambda: fn(HfApi(token=token)), timeout_sec=timeout_sec)
    except Exception as exc:
        logger.warning("[setup_backtest] study parts on HF: %s", exc)
        return None


def _part_bytes(trades: pd.DataFrame) -> bytes:
    import io
    buf = io.BytesIO()
    try:
        trades.to_parquet(buf, index=False)
    except Exception:  # a mixed-type column: keep it as text
        buf = io.BytesIO()
        trades.astype({c: str for c in trades.select_dtypes(include="object").columns}).to_parquet(buf, index=False)
    return buf.getvalue()


def _restore_parts_from_hf(bot: str, key: dict[str, Any]) -> dict[str, pd.DataFrame] | None:
    """The replays a study had finished before the Space restarted (a
    deploy wipes local disk), from the bot's HF model repo."""
    def fetch(api):
        from huggingface_hub import hf_hub_download
        repo = REPOS[bot]
        token = os.getenv("HF_API_KEY", "")
        if f"{PARTS_PATH}/key.json" not in set(api.list_repo_files(repo, repo_type="model")):
            return None
        saved = json.loads(Path(hf_hub_download(repo, f"{PARTS_PATH}/key.json", repo_type="model", token=token)).read_text())
        if {k: saved.get(k) for k in key} != key or time.time() - float(saved.get("at", 0)) > 2 * 86400:
            return None
        out = {}
        for f in api.list_repo_files(repo, repo_type="model"):
            if f.startswith(f"{PARTS_PATH}/") and f.endswith(".parquet"):
                sym = f.rsplit("/", 1)[1][: -len(".parquet")].replace("__", "/")
                out[sym] = _one_column_each(pd.read_parquet(hf_hub_download(repo, f, repo_type="model", token=token)))
        return out
    return _hf_parts(fetch, timeout_sec=600)


def _load_parts(bot: str, key: dict[str, Any]) -> dict[str, pd.DataFrame]:
    """Symbols this study already replayed (same version and settings, in
    the last two days) -- on local disk, or on HF after a restart: a run
    that died or was restarted resumes where it stopped instead of
    replaying the whole archive again."""
    d = _parts_dir(bot)
    try:
        saved = json.loads((d / "key.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        saved = None
    if saved is None or {k: saved.get(k) for k in key} != key or time.time() - float(saved.get("at", 0)) > 2 * 86400:
        import shutil
        shutil.rmtree(d, ignore_errors=True)
        d.mkdir(parents=True, exist_ok=True)
        restored = _restore_parts_from_hf(bot, key)
        (d / "key.json").write_text(json.dumps(key | {"at": time.time()}), encoding="utf-8")
        if restored:
            for sym, t in restored.items():
                _save_part(bot, sym, t, upload=False)
            return restored

        def fresh(api):
            from huggingface_hub import CommitOperationAdd, CommitOperationDelete
            ops = [CommitOperationAdd(f"{PARTS_PATH}/key.json", json.dumps(key | {"at": time.time()}).encode())]
            if any(f.startswith(f"{PARTS_PATH}/") for f in api.list_repo_files(REPOS[bot], repo_type="model")):
                ops.insert(0, CommitOperationDelete(f"{PARTS_PATH}/"))
            api.create_commit(repo_id=REPOS[bot], repo_type="model", operations=ops, commit_message=f"{bot} study: new run")
            return True
        _hf_parts(fresh)
        return {}
    done = {}
    for f in d.glob("*.pkl"):
        try:
            done[f.stem.replace("__", "/")] = _one_column_each(pd.read_pickle(f))
        except Exception:
            f.unlink(missing_ok=True)
    return done


def _save_part(bot: str, sym: str, trades: pd.DataFrame, *, upload: bool = True) -> None:
    """Keep one symbol's replay (local disk, and the bot's HF repo) --
    best effort: a failed save never stops the study."""
    name = sym.replace("/", "__")
    trades = _one_column_each(trades)
    try:
        tmp = _parts_dir(bot) / f"{name}.pkl.tmp"
        trades.to_pickle(tmp)
        tmp.rename(tmp.with_suffix(""))
        if upload:
            data = _part_bytes(trades)
            _hf_parts(lambda api: api.upload_file(path_or_fileobj=data, path_in_repo=f"{PARTS_PATH}/{name}.parquet", repo_id=REPOS[bot],
                                                  repo_type="model", commit_message=f"{bot} study: {sym} replayed"))
    except Exception as exc:
        logger.warning("[setup_backtest] could not keep %s's %s replay: %s", bot, sym, exc)


def run_multiyear(bot: str, *, publish: bool = True) -> dict[str, Any]:
    from concurrent.futures import ProcessPoolExecutor, as_completed

    import importlib

    cfg = MULTIYEAR[bot]
    symbols = _study_symbols(bot)
    grid = study_grid(bot)
    started = time.time()
    LOCAL_DIR.mkdir(parents=True, exist_ok=True)
    progress_path = LOCAL_DIR / f"{bot}_multiyear_progress.json"
    version = STUDY_VERSION.get(bot, 1)
    parts = _load_parts(bot, {"version": version, "grid": grid_labels(bot)})
    parts = {s: t for s, t in parts.items() if s in symbols}
    progress: dict[str, Any] = {"bot": bot, "done": len(parts), "total": len(symbols), "workers": MULTIYEAR_WORKERS,
                                "started_at": dt.datetime.fromtimestamp(started, dt.timezone.utc).isoformat(),
                                "resumed": sorted(parts), "stage": "replaying"}

    def mark(**kw: Any) -> None:
        progress.update(kw, elapsed_sec=round(time.time() - started), peak_mb=_rss_mb(),
                        trades_so_far=int(sum(len(f) for f in parts.values())))
        progress_path.write_text(json.dumps(progress, default=str), encoding="utf-8")
        logger.info("[setup_backtest] %s study: %s", bot, {k: progress.get(k) for k in ("stage", "done", "elapsed_sec", "peak_mb")})

    mark()
    todo = [s for s in symbols if s not in parts]
    if todo and bot in ARCHIVE_STUDY_BOTS:
        # Archives to local disk once, before the workers read them.
        from data import alpaca_news_history
        alpaca_news_history.index_for([])  # every month on local disk once, before the workers read it
        for sym in sorted(set(todo) | {importlib.import_module(f"data.{cfg['module']}").leader_for(s) for s in todo}):
            _study_candles(sym, bot)
        costs = _kalshi_study_costs(bot, todo)
        from data import alpaca_sip_history
        alpaca_sip_history.load("SPY")
        jobs = {sym: (_multiyear_kalshi_symbol, (bot, sym, grid, costs[sym])) for sym in todo}
    else:
        jobs = {sym: (_multiyear_symbol, (sym, cfg["module"], cfg["sides"], grid or [None])) for sym in todo}
    if jobs:
        pool = ProcessPoolExecutor(MULTIYEAR_WORKERS)
        try:
            futures = {pool.submit(fn, args): sym for sym, (fn, args) in jobs.items()}
            for fut in as_completed(futures):
                sym = futures[fut]
                try:
                    t = fut.result()
                except Exception as exc:
                    logger.warning("[setup_backtest] multi-year replay failed for %s: %s", sym, exc)
                    t = pd.DataFrame()
                parts[sym] = _one_column_each(t)
                _save_part(bot, sym, t)
                mark(done=len(parts), last_symbol=sym)
        finally:
            _stop_pool(pool)
    mark(stage="analysing")
    frames = [t for t in parts.values() if not t.empty]
    trades = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    result = _analyse_multiyear(bot, trades, symbols=symbols, grid=grid, started=started, mark=mark)
    mark(stage="writing")
    LOCAL_DIR.mkdir(parents=True, exist_ok=True)
    if not trades.empty:
        try:
            trades.to_parquet(LOCAL_DIR / f"{bot}_multiyear_trades.parquet", index=False)
        except Exception as exc:  # a mixed-type column must not cost the whole study
            logger.warning("[setup_backtest] %s trades parquet failed (%s); writing it as text columns", bot, exc)
            obj = trades.select_dtypes(include="object").columns
            trades.astype({c: str for c in obj}).to_parquet(LOCAL_DIR / f"{bot}_multiyear_trades.parquet", index=False)
    (LOCAL_DIR / f"{bot}_multiyear.json").write_text(json.dumps(result, default=str), encoding="utf-8")
    if publish and not trades.empty:
        mark(stage="publishing")
        result["published"] = _publish_multiyear(bot, result)
        (LOCAL_DIR / f"{bot}_multiyear.json").write_text(json.dumps(result, default=str), encoding="utf-8")
    import shutil
    shutil.rmtree(_parts_dir(bot), ignore_errors=True)
    mark(stage="done")
    return result


def _stop_pool(pool) -> None:
    """Shut the study's worker pool now: never wait on its workers, and
    stop any still replaying (only after a failure)."""
    procs = list((getattr(pool, "_processes", None) or {}).values())
    pool.shutdown(wait=False, cancel_futures=True)
    for proc in procs:
        try:
            if proc.is_alive():
                proc.terminate()
        except Exception:
            pass


def _analyse_multiyear(bot: str, trades: pd.DataFrame, *, symbols: list[str], grid: list, started: float,
                       mark=lambda **kw: None) -> dict[str, Any]:
    """Every replayed trade -> the walk-forward record the bot learns from."""
    trades = _one_column_each(trades)

    cfg = MULTIYEAR[bot]
    result: dict[str, Any] = {"ok": not trades.empty, "bot": bot, "symbols": len(symbols), "module": cfg["module"],
                              "grid": grid_labels(bot), "universe": symbols, "version": STUDY_VERSION.get(bot, 1),
                              "computed_at": dt.datetime.now(dt.timezone.utc).isoformat(), "seconds": round(time.time() - started)}
    if trades.empty:
        return result
    default = default_param(bot)
    base = trades[trades["param"] == default] if "param" in trades and default in set(trades["param"]) else trades
    lookback = int(cfg.get("lookback", ELIGIBILITY_LOOKBACK_YEARS))
    result["default_param"] = default
    result["lookback_years"] = lookback
    result["all_trades"] = summarize(base, "net_return")
    mark(stage="analysing: walk-forward")
    result["walk_forward"] = walk_forward_eligibility(base, lookback_years=lookback)
    if "param" in trades and trades["param"].nunique() > 1:
        mark(stage="analysing: trained settings")
        result["trained"] = walk_forward_trained(trades, default_param=default, lookback_years=lookback, keys=param_keys(bot))
    if {"hour_block", "weekday"} <= set(trades.columns):
        mark(stage="analysing: entry conditions")
        result["patterns"] = walk_forward_patterns(trades, default_param=default, lookback_years=lookback, keys=param_keys(bot))
    if {"vol_q_low", "vol_q_high"} <= set(trades.columns):
        th = trades.dropna(subset=["vol_q_low", "vol_q_high"]).groupby("symbol")[["vol_q_low", "vol_q_high"]].first()
        result["vol_thresholds"] = {sym: [float(r.vol_q_low), float(r.vol_q_high)] for sym, r in th.iterrows()}
    result["seconds"] = round(time.time() - started)
    return result


def _eligibility_from(result: dict[str, Any]) -> dict[str, Any]:
    """What the bot enforces: the trained setting + symbols when training
    beat the untrained walk-forward out of sample, else the plain list --
    and, when learning the losing entry conditions beat that too, the
    trained setting + symbols + those conditions."""
    wf, tr, pt = result.get("walk_forward") or {}, result.get("trained") or {}, result.get("patterns") or {}
    use_trained = bool(tr.get("enforce") and (tr.get("out_of_sample", {}).get("avg") or 0) > (wf.get("out_of_sample", {}).get("avg") or 0))
    best_avg = max((tr.get("out_of_sample", {}).get("avg") or 0) if use_trained else float("-inf"),
                   (wf.get("out_of_sample", {}).get("avg") or 0))
    if pt.get("enforce") and (pt.get("with_patterns", {}).get("avg") or 0) > best_avg:
        return {"enforce": True, "symbols": pt.get("eligible_now", []), "rule": pt.get("rule"), "params": pt.get("param_now"),
                "blocked": pt.get("blocked_now") or {}, "vol_thresholds": result.get("vol_thresholds") or {},
                "source": "patterns", "computed_at": result.get("computed_at"),
                "out_of_sample": pt.get("with_patterns"), "grid": result.get("grid"), "version": result.get("version", 1)}
    src = tr if use_trained else wf
    return {"enforce": bool(src.get("enforce")), "symbols": src.get("eligible_now", []), "rule": src.get("rule"),
            "params": tr.get("param_now") if use_trained else None, "blocked": {}, "source": "trained" if use_trained else "walk_forward",
            "computed_at": result.get("computed_at"), "out_of_sample": src.get("out_of_sample"), "grid": result.get("grid"),
            "version": result.get("version", 1)}


def _publish_multiyear(bot: str, result: dict[str, Any]) -> bool:
    token = os.getenv("HF_API_KEY", "")
    if not token:
        return False
    try:
        from huggingface_hub import CommitOperationAdd, CommitOperationDelete, HfApi
        eligibility = _eligibility_from(result)
        ops = [CommitOperationAdd("setup_strategy/multiyear/report.json", json.dumps(result, indent=2, default=str).encode()),
               CommitOperationAdd("setup_strategy/multiyear/trades.parquet", str(LOCAL_DIR / f"{bot}_multiyear_trades.parquet")),
               CommitOperationAdd("setup_strategy/eligibility.json", json.dumps(eligibility, indent=2).encode())]
        api = HfApi(token=token)
        if _hf_parts(lambda a: any(f.startswith(f"{PARTS_PATH}/") for f in a.list_repo_files(REPOS[bot], repo_type="model")), 120):
            ops.append(CommitOperationDelete(f"{PARTS_PATH}/"))
        from server_common import call_with_hard_timeout
        done = call_with_hard_timeout(lambda: api.create_commit(
            repo_id=REPOS[bot], repo_type="model", operations=ops,
            commit_message=f"{bot} multi-year setup study {result['computed_at'][:10]}") or True, timeout_sec=900, on_timeout=False)
        if not done:
            logger.warning("[setup_backtest] multi-year publish for %s timed out", bot)
        return bool(done)
    except Exception as exc:
        logger.warning("[setup_backtest] multi-year publish failed for %s: %s", bot, exc)
        return False


# Bumped when a bot's study replay changes in a way that changes results;
# a published study of an older version is re-run on the next start check.
# perps 2 / kalshi15m 3: every prior year (expanding), side and volatility
# regime learned too; kalshi15m 2: settles on Kalshi's reference.
# Bumped together on 2026-10-08: every bot re-studies its whole history
# (crypto from Jan 2021) on the faster replay; stocks/options on the full method.
STUDY_VERSION = {"perps": 4, "kalshi15m": 5, "crypto": 2, "stocks": 2, "options": 2}


def study_grid(bot: str) -> list[tuple[float, float]]:
    """The plan settings a bot's study replays: PARAM_GRID for stocks and
    options; KALSHI_PARAM_GRID plus the module's own current setting (so the
    baseline is real) for perps and the 15m bot."""
    if bot not in ARCHIVE_STUDY_BOTS:
        return list(PARAM_GRID)
    import importlib
    m = importlib.import_module(f"data.{MULTIYEAR[bot]['module']}")
    base = [(sb, rr) for sb in STOP_BUFFERS for rr in (TARGETS_15M if bot == "kalshi15m" else TARGETS)]
    return sorted(set(base) | {(float(m.STOP_BUFFER_ATR15), float(m.MIN_RR))})


def _study_symbols(bot: str) -> list[str]:
    if bot == "perps":
        from data import kalshi_15m_spot
        from data.kalshi_perps import KNOWN_PERP_TICKERS
        from data.perps_data import coin_for_ticker
        coins = {kalshi_15m_spot.chart_coin(coin_for_ticker(t)) for t in KNOWN_PERP_TICKERS}
        return sorted(c for c in coins if c in kalshi_15m_spot.SPOT_PRODUCTS) + ["GOLD", "SILVER"]
    if bot == "crypto":
        # Every coin the crypto bot can trade, read as its /USD pair on the
        # Alpaca archive (pairs quoted in USDT/USDC chart the same coin).
        from data import alpaca_crypto_history
        year = dt.datetime.now(dt.timezone.utc).year
        coins = sorted(set(alpaca_crypto_history.universe()) | set(alpaca_crypto_history.crypto_bot_coins()))
        have = [c for c in coins if alpaca_crypto_history.load(c, years=[year]).shape[0] > 0]
        return [f"{c}/USD" for c in have]
    if bot == "kalshi15m":
        from data import kalshi_15m_setup, kalshi_15m_spot, kalshi_15m_strategy
        return sorted(c for c in kalshi_15m_strategy.ACTIVE_ENTRY_COINS
                      if c in kalshi_15m_spot.SPOT_PRODUCTS or c in kalshi_15m_setup.METAL_CHART_SYMBOL)
    from data import alpaca_data, alpaca_options_data
    return (sorted(set(alpaca_data.BROAD_CANDIDATE_UNIVERSE) | {"SPY", "QQQ"}) if bot == "stocks"
            else list(alpaca_options_data.OPTIONS_UNDERLYINGS))


def archive_ready(bot: str, *, min_fraction: float = 0.98) -> bool:
    """True once the SIP archive on HF holds this year's file for (nearly)
    every symbol the study replays -- never start on a partial upload. The
    Kalshi bots also need the Alpaca crypto and news archives complete."""
    if bot == "crypto":
        from data import alpaca_crypto_history, alpaca_news_history, alpaca_sip_history
        try:
            tickers = [f"{c}USD" for c in alpaca_crypto_history.crypto_bot_coins()]
            return (alpaca_crypto_history.archive_ready(min_fraction=min_fraction) and alpaca_crypto_history.crypto_bot_ready()
                    and alpaca_crypto_history.history_deepened()
                    and alpaca_news_history.archive_ready() and alpaca_news_history.covers(tickers)
                    and "SPY" not in alpaca_sip_history.missing_symbols())
        except Exception:
            return False
    if bot in KALSHI_STUDY_BOTS:
        from data import alpaca_crypto_history, alpaca_news_history, alpaca_sip_history
        try:
            return (alpaca_crypto_history.archive_ready(min_fraction=min_fraction) and alpaca_crypto_history.history_deepened()
                    and alpaca_news_history.archive_ready()
                    and not set(alpaca_sip_history.commodity_etfs()) & set(alpaca_sip_history.missing_symbols()))
        except Exception:
            return False
    token = os.getenv("HF_API_KEY", "")
    if not token:
        return False
    try:
        from huggingface_hub import HfApi

        from data import alpaca_sip_history
        files = set(HfApi(token=token).list_repo_files(alpaca_sip_history.HF_REPO, repo_type="dataset"))
    except Exception:
        return False
    year = dt.datetime.now(dt.timezone.utc).year
    symbols = _study_symbols(bot)
    present = sum(1 for s in symbols if f"bars_1m/{s}/{year}.parquet" in files and f"bars_1m/{s}/{year - 1}.parquet" in files)
    if not (symbols and present >= min_fraction * len(symbols)):
        return False
    try:  # the entry-condition learning reads the news archive too, with these tickers in it
        from data import alpaca_news_history
        return alpaca_news_history.archive_ready() and alpaca_news_history.covers(alpaca_news_history.stock_tickers())
    except Exception:
        return False


def _pid_alive(pid: int) -> tuple[bool, int | None]:
    """(running, exit status if this server just reaped it). A study the
    server launched but never waited on stays a zombie after it exits --
    that is finished, not running."""
    try:
        reaped, status = os.waitpid(pid, os.WNOHANG)
        if reaped == pid:
            return False, status
    except ChildProcessError:
        pass
    except OSError:
        pass
    try:
        with open(f"/proc/{pid}/stat", encoding="utf-8") as f:
            if f.read().rsplit(")", 1)[1].split()[0] == "Z":
                return False, None
    except (OSError, IndexError):
        pass
    try:
        os.kill(pid, 0)
        return True, None
    except (ProcessLookupError, PermissionError):
        return False, None


def _write_error(name: str, error: str) -> None:
    try:
        LOCAL_DIR.mkdir(parents=True, exist_ok=True)
        (LOCAL_DIR / f"{name}_error.json").write_text(json.dumps({"at": dt.datetime.now(dt.timezone.utc).isoformat(),
                                                                  "error": error[-4000:]}), encoding="utf-8")
    except OSError:
        pass


def _running(name: str) -> bool:
    pid_file = LOCAL_DIR / f"{name}.pid"
    if not pid_file.exists():
        return False
    try:
        pid = int(pid_file.read_text())
    except (ValueError, OSError):
        return False
    alive, status = _pid_alive(pid)
    if alive:
        return True
    # The study removes its pid file on every normal or failed exit; one
    # left behind means it was killed (out of memory, a signal).
    how = (f"killed by signal {os.WTERMSIG(status)}" + (" (out of memory)" if os.WTERMSIG(status) == 9 else "")
           if status is not None and os.WIFSIGNALED(status) else
           f"exited with code {os.WEXITSTATUS(status)}" if status is not None else "ended without finishing")
    err = LOCAL_DIR / f"{name}_error.json"
    if not err.exists() or err.stat().st_mtime < pid_file.stat().st_mtime:
        _write_error(name, f"study process {how}; see {name}.log")
    pid_file.unlink(missing_ok=True)
    return False


FAILED_RETRY_HOURS = float(os.getenv("SETUP_MULTIYEAR_RETRY_HOURS", "6") or "6")


def request_multiyear(bot: str) -> dict[str, Any]:
    """The weekly refresh: queue this bot's study; it starts as soon as no
    other study is running (never two at once on the Space's cores)."""
    LOCAL_DIR.mkdir(parents=True, exist_ok=True)
    (LOCAL_DIR / f"{bot}_multiyear.queued").write_text(dt.datetime.now(dt.timezone.utc).isoformat(), encoding="utf-8")
    return maybe_start_multiyear(bot)


def maybe_start_multiyear(bot: str) -> dict[str, Any]:
    """Checked every few minutes on the Space: launch this bot's study once
    the archive is complete, if it has no published study yet (or a weekly
    refresh is queued) and no other study is running (one study at a time
    gets every core). A study that failed waits FAILED_RETRY_HOURS."""
    if any(_running(f"{b}_multiyear") for b in MULTIYEAR):
        return {"ok": True, "action": "a_study_is_running"}
    need = _needs_study(bot)
    if need != "due":
        return {"ok": True, "action": need}
    # The real-money Kalshi bots' studies go first when several are due.
    for first in STUDY_PRIORITY[:STUDY_PRIORITY.index(bot)] if bot in STUDY_PRIORITY else []:
        if _needs_study(first) == "due":
            return {"ok": True, "action": f"after_{first}"}
    (LOCAL_DIR / f"{bot}_multiyear.queued").unlink(missing_ok=True)
    _eligibility_cache.pop(bot, None)
    return launch(f"{bot}_multiyear")


STUDY_PRIORITY = ["perps", "kalshi15m", "crypto", "stocks", "options"]


def _needs_study(bot: str) -> str:
    """'due' when this bot's study should run now (a weekly refresh is
    queued, or nothing is published at the current version and settings,
    and its archive is complete); else why not."""
    err = LOCAL_DIR / f"{bot}_multiyear_error.json"
    if err.exists() and time.time() - err.stat().st_mtime < FAILED_RETRY_HOURS * 3600:
        return "failed_recently"
    if not (LOCAL_DIR / f"{bot}_multiyear.queued").exists():
        wanted = grid_labels(bot)
        version = STUDY_VERSION.get(bot, 1)
        try:
            # The study's own result file first: it runs in a separate process,
            # so this server's cached eligibility can predate the result.
            local = json.loads((LOCAL_DIR / f"{bot}_multiyear.json").read_text(encoding="utf-8"))
            if local.get("grid") == wanted and local.get("version", 1) == version:
                return "already_published"
        except (OSError, ValueError):
            pass
        current = eligibility(bot)
        if current is not None and current.get("grid") == wanted and current.get("version", 1) == version:
            return "already_published"
    if not archive_ready(bot):
        return "waiting_for_archive"
    return "due"


_report_checked: dict[str, float] = {}


def _restore_published_report(bot: str) -> None:
    """After a restart the local result file is gone: bring back the
    published report from the bot's HF model repo (checked at most every
    10 minutes when there is none yet)."""
    local = LOCAL_DIR / f"{bot}_multiyear.json"
    token = os.getenv("HF_API_KEY", "")
    if local.exists() or not token or time.time() - _report_checked.get(bot, 0.0) < 600:
        return
    _report_checked[bot] = time.time()
    try:
        from huggingface_hub import hf_hub_download

        from server_common import call_with_hard_timeout
        path = call_with_hard_timeout(lambda: hf_hub_download(REPOS[bot], "setup_strategy/multiyear/report.json",
                                                              repo_type="model", token=token), timeout_sec=20)
        if path:
            LOCAL_DIR.mkdir(parents=True, exist_ok=True)
            local.write_text(Path(path).read_text(encoding="utf-8"), encoding="utf-8")
    except Exception:
        pass


def multiyear_status(bot: str) -> dict[str, Any]:
    """Progress of a running study and the latest finished one (restored
    from HF after a restart)."""
    _restore_published_report(bot)
    out: dict[str, Any] = {"bot": bot}
    for key, name in (("progress", f"{bot}_multiyear_progress.json"), ("latest", f"{bot}_multiyear.json")):
        try:
            out[key] = json.loads((LOCAL_DIR / name).read_text(encoding="utf-8"))
        except Exception:
            out[key] = None
    if out["latest"]:
        wf = out["latest"].get("walk_forward") or {}
        out["latest"] = {k: out["latest"].get(k) for k in ("computed_at", "seconds", "symbols", "all_trades", "published")}
        out["latest"]["walk_forward"] = {k: wf.get(k) for k in ("out_of_sample", "every_symbol", "positive_years", "test_years",
                                                                 "enforce", "rule", "years")}
        out["latest"]["walk_forward"]["eligible_now"] = len(wf.get("eligible_now") or [])
        full = json.loads((LOCAL_DIR / f"{bot}_multiyear.json").read_text(encoding="utf-8"))
        pt = full.get("patterns") or {}
        if pt:
            out["latest"]["patterns"] = {k: pt.get(k) for k in ("trained", "with_patterns", "default_every_symbol", "positive_years",
                                                                "test_years", "param_now", "blocked_now", "by_condition", "enforce")}
            out["latest"]["patterns"]["eligible_now"] = pt.get("eligible_now") or []
        tr = full.get("trained") or {}
        if tr:
            out["latest"]["trained"] = {k: tr.get(k) for k in ("out_of_sample", "default_every_symbol", "positive_years",
                                                                "test_years", "param_now", "enforce", "years")}
            out["latest"]["trained"]["eligible_now"] = len(tr.get("eligible_now") or [])
    out["running"] = _running(f"{bot}_multiyear")
    try:
        out["error"] = json.loads((LOCAL_DIR / f"{bot}_multiyear_error.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        out["error"] = None
    return out


def multiyear_log(bot: str, lines: int = 200) -> list[str]:
    """The last lines the study process wrote (its own log file)."""
    try:
        with open(LOCAL_DIR / f"{bot}_multiyear.log", encoding="utf-8", errors="replace") as f:
            return [line.rstrip("\n") for line in f.readlines()[-max(1, min(int(lines), 2000)):]]
    except OSError:
        return []


_eligibility_cache: dict[str, tuple[float, dict[str, Any] | None]] = {}


def eligibility(bot: str) -> dict[str, Any] | None:
    """The bot's published symbol eligibility (cached an hour), or None."""
    cached = _eligibility_cache.get(bot)
    local = LOCAL_DIR / f"{bot}_multiyear.json"
    try:
        newer_result = local.stat().st_mtime > (cached[0] if cached else 0.0)
    except OSError:
        newer_result = False
    if cached and time.time() - cached[0] < 3600 and not newer_result:
        return cached[1]
    data = None
    try:
        data = _eligibility_from(json.loads(local.read_text(encoding="utf-8")))
    except Exception:
        token = os.getenv("HF_API_KEY", "")
        if token:
            try:
                from huggingface_hub import hf_hub_download

                from server_common import call_with_hard_timeout
                path = call_with_hard_timeout(lambda: hf_hub_download(REPOS[bot], "setup_strategy/eligibility.json",
                                                                      repo_type="model", token=token), timeout_sec=10)
                data = json.loads(Path(path).read_text(encoding="utf-8"))
            except Exception:
                data = None
    _eligibility_cache[bot] = (time.time(), data)
    return data


RUNNERS = {"perps": run_perps, "kalshi15m": run_kalshi15m, "stocks": run_stocks, "crypto": run_crypto, "options": run_options}


# ---------------------------------------------------------------------------
# Run, publish, read back
# ---------------------------------------------------------------------------

def strategy_card(bot: str) -> dict[str, Any]:
    import importlib
    return importlib.import_module(f"data.{MODULES[bot]}").strategy_card()


def run(bot: str, *, days: int = DAYS, publish: bool = True) -> dict[str, Any]:
    started = time.time()
    try:
        body = RUNNERS[bot](days)
        result = {"ok": True, **body}
    except Exception as exc:
        logger.exception("[setup_backtest] %s replay failed", bot)
        result = {"ok": False, "error": str(exc)}
    result.update({"bot": bot, "days": result.get("days", days), "computed_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                   "seconds": round(time.time() - started, 1), "module": MODULES[bot], "hf_repo": REPOS[bot]})
    LOCAL_DIR.mkdir(parents=True, exist_ok=True)
    (LOCAL_DIR / f"{bot}.json").write_text(json.dumps(result), encoding="utf-8")
    if publish:
        result["published"] = publish_to_hf(bot, result)
        (LOCAL_DIR / f"{bot}.json").write_text(json.dumps(result), encoding="utf-8")
    return result


def publish_to_hf(bot: str, result: dict[str, Any]) -> bool:
    token = os.getenv("HF_API_KEY", "")
    if not token:
        return False
    try:
        from huggingface_hub import CommitOperationAdd, HfApi
        api = HfApi(token=token)
        api.create_repo(repo_id=REPOS[bot], repo_type="model", exist_ok=True, private=True)
        day = result["computed_at"][:10]
        payload = json.dumps(result, indent=2).encode()
        ops = [
            CommitOperationAdd("setup_strategy/config.json", json.dumps(strategy_card(bot), indent=2, default=str).encode()),
            CommitOperationAdd("setup_strategy/backtest_latest.json", payload),
            CommitOperationAdd(f"setup_strategy/backtests/{day}.json", payload),
        ]
        api.create_commit(repo_id=REPOS[bot], repo_type="model", operations=ops,
                          commit_message=f"{bot} setup strategy: daily real-data backtest {day}")
        return True
    except Exception as exc:
        logger.warning("[setup_backtest] HF publish failed for %s: %s", bot, exc)
        return False


def latest(bot: str) -> dict[str, Any] | None:
    """The bot's most recent backtest: the local copy, else the published one
    on HF (cached locally once read)."""
    path = LOCAL_DIR / f"{bot}.json"
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        pass
    token = os.getenv("HF_API_KEY", "")
    if not token:
        return None
    try:
        from huggingface_hub import hf_hub_download

        from server_common import call_with_hard_timeout
        local = call_with_hard_timeout(lambda: hf_hub_download(REPOS[bot], "setup_strategy/backtest_latest.json",
                                                               repo_type="model", token=token), timeout_sec=10)
        data = json.loads(Path(local).read_text(encoding="utf-8"))
        LOCAL_DIR.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data), encoding="utf-8")
        return data
    except Exception:
        return None


def launch(bot: str) -> dict[str, Any]:
    """Start the replay in its own low-priority process (never in the server
    process); one at a time per bot."""
    LOCAL_DIR.mkdir(parents=True, exist_ok=True)
    pid_file = LOCAL_DIR / f"{bot}.pid"
    if _running(bot):
        return {"ok": True, "action": "already_running", "pid": int(pid_file.read_text())}
    log = open(LOCAL_DIR / f"{bot}.log", "w")  # noqa: SIM115 -- handed to the child process
    proc = subprocess.Popen([sys.executable, "-m", "data.setup_backtest_job", bot], cwd=str(SRC_DIR), env=dict(os.environ),
                            stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    pid_file.write_text(str(proc.pid))
    return {"ok": True, "action": "launched", "pid": proc.pid}


if __name__ == "__main__":
    try:
        os.nice(15)
    except (AttributeError, OSError):
        pass
    logging.basicConfig(level=logging.INFO)
    sys.path.insert(0, str(SRC_DIR))
    bot_arg = sys.argv[1]
    try:
        (LOCAL_DIR / f"{bot_arg}_error.json").unlink(missing_ok=True)
        out = run_multiyear(bot_arg.removesuffix("_multiyear")) if bot_arg.endswith("_multiyear") else run(bot_arg)
        print(json.dumps({k: v for k, v in out.items() if k in ("ok", "bot", "seconds", "published", "error")}))
    except BaseException:
        import traceback
        _write_error(bot_arg, traceback.format_exc())
        raise
    finally:
        (LOCAL_DIR / f"{bot_arg}.pid").unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Evidence gate: a gated bot opens new positions only while its own latest
# real-data replay (the live system, correlation rule included) is
# profitable after costs. Exits are never gated.
# ---------------------------------------------------------------------------
# Off by default (user decision 2026-10-01: all bots trade the setup system
# live); SETUP_EVIDENCE_GATE_BOTS="perps,kalshi15m" turns it on per bot.
EVIDENCE_GATE_BOTS = frozenset(b.strip() for b in os.getenv("SETUP_EVIDENCE_GATE_BOTS", "").split(",") if b.strip())
EVIDENCE_MIN_TRADES = int(os.getenv("SETUP_EVIDENCE_MIN_TRADES", "20") or "20")
EVIDENCE_MIN_T = float(os.getenv("SETUP_EVIDENCE_MIN_T", "1.0") or "1.0")
EVIDENCE_MAX_AGE_HOURS = float(os.getenv("SETUP_EVIDENCE_MAX_AGE_HOURS", "48") or "48")


def evidence_gate(bot: str) -> dict[str, Any]:
    """{"open": bool, "reason": str, ...}: always open for an ungated bot."""
    if bot not in EVIDENCE_GATE_BOTS:
        return {"open": True, "gated": False, "reason": "not_gated"}
    bt = latest(bot)
    base = {"gated": True, "min_trades": EVIDENCE_MIN_TRADES, "min_t": EVIDENCE_MIN_T}
    if not bt or not bt.get("ok"):
        return {**base, "open": False, "reason": "no_replay_yet" if not bt else "last_replay_failed"}
    try:
        age_h = (dt.datetime.now(dt.timezone.utc) - dt.datetime.fromisoformat(bt["computed_at"])).total_seconds() / 3600
    except (KeyError, TypeError, ValueError):
        age_h = None
    s = bt.get("with_correlation") or {}
    info = {**base, "trades": s.get("trades"), "avg": s.get("avg"), "t_stat": s.get("t_stat"), "computed_at": bt.get("computed_at")}
    if age_h is None or age_h > EVIDENCE_MAX_AGE_HOURS:
        return {**info, "open": False, "reason": "replay_stale"}
    if (s.get("trades") or 0) < EVIDENCE_MIN_TRADES:
        return {**info, "open": False, "reason": "too_few_replay_trades"}
    if (s.get("avg") or 0) <= 0 or (s.get("t_stat") or 0) < EVIDENCE_MIN_T:
        return {**info, "open": False, "reason": "replay_not_profitable"}
    return {**info, "open": True, "reason": "replay_profitable"}
