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

import numpy as np
import pandas as pd

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


def run_kalshi15m(days: int) -> dict[str, Any]:
    from data import kalshi_15m_quotes, kalshi_15m_setup, kalshi_15m_spot, kalshi_15m_strategy
    quotes = kalshi_15m_quotes.load_quote_history(days=days)
    spot = kalshi_15m_spot.load_spot_history(days=days + 2)
    coins = sorted(c for c in kalshi_15m_strategy.ACTIVE_ENTRY_COINS if c in set(spot["coin"]) and c in set(quotes["coin"]))

    def go(with_corr: bool) -> dict[str, Any]:
        frames = []
        for coin in coins:
            leader = kalshi_15m_setup.leader_for(coin)
            t = kalshi_15m_setup.replay_contracts(
                quotes[quotes.coin == coin], spot[spot.coin == coin][SPOT_COLUMNS],
                leader_1m=spot[spot.coin == leader][SPOT_COLUMNS] if with_corr else None, leader_symbol=leader,
                session=kalshi_15m_setup.session_for(coin), leader_session=kalshi_15m_setup.session_for(leader),
            )
            if not t.empty:
                frames.append(t.assign(symbol=coin))
        return summarize(pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(), "pnl_per_contract",
                         unit="usd_per_contract")

    return {"universe": coins, "metals": "not replayed: the only free metals chart (Yahoo COMEX) is ~10 minutes delayed",
            **_both(go)}


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
    for setting in grid:
        stop_buffer, min_rr = setting or default
        m.STOP_BUFFER_ATR15, m.MIN_RR = stop_buffer, min_rr
        t = m.replay(rth, sides=sides, fee_rate_roundtrip=0.0, spread_bps=m.SPREAD_BPS, entry_allowed=m.entry_allowed,
                     force_exit=m.must_be_flat, leader_df=lead_rth, leader_symbol=lead_sym)
        if not t.empty:
            frames.append(t.assign(symbol=sym, param=f"{stop_buffer}:{min_rr}"))
    m.STOP_BUFFER_ATR15, m.MIN_RR = default
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


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
    for y in range(int(t["year"].min()) + lookback_years, int(t["year"].max()) + 1):
        train = t[(t["year"] >= y - lookback_years) & (t["year"] < y)]
        per = train.groupby("symbol")["net_return"].agg(["size", "mean"])
        eligible = set(per[(per["size"] >= min_trades) & (per["mean"] > 0)].index)
        test = t[t["year"] == y]
        picked = test[test["symbol"].isin(eligible)]
        picked_frames.append(picked)
        years.append({"year": y, "eligible": len(eligible), "picked": _trade_stats(picked["net_return"]),
                      "all": _trade_stats(test["net_return"])})
    picked_all = pd.concat(picked_frames, ignore_index=True) if picked_frames else pd.DataFrame(columns=["net_return"])
    first_test = int(t["year"].min()) + lookback_years
    every = t[t["year"] >= first_test]
    oos, base = _trade_stats(picked_all["net_return"]), _trade_stats(every["net_return"])
    positive_years = sum(1 for y in years if (y["picked"].get("avg") or 0) > 0)
    latest_year = int(t["year"].max())
    recent = t[t["year"] > latest_year - lookback_years]
    per = recent.groupby("symbol")["net_return"].agg(["size", "mean"])
    current = sorted(per[(per["size"] >= min_trades) & (per["mean"] > 0)].index)
    enforce = bool(oos.get("trades", 0) >= 30 and (oos.get("avg") or 0) > 0 and (oos.get("avg") or 0) > (base.get("avg") or 0)
                   and positive_years * 2 >= len(years))
    return {"years": years, "out_of_sample": oos, "every_symbol": base, "positive_years": positive_years,
            "test_years": len(years), "eligible_now": current, "enforce": enforce,
            "rule": f">= {min_trades} trades and average net > 0 over the prior {lookback_years} years"}


def walk_forward_trained(trades: pd.DataFrame, *, default_param: str, min_trades: int = ELIGIBILITY_MIN_TRADES,
                         lookback_years: int = ELIGIBILITY_LOOKBACK_YEARS, min_train_trades: int = 30) -> dict[str, Any]:
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
    for y in range(int(t["year"].min()) + lookback_years, int(t["year"].max()) + 1):
        param, eligible, _ = choose(t[(t["year"] >= y - lookback_years) & (t["year"] < y)])
        test = t[(t["year"] == y) & (t["param"] == param) & (t["symbol"].isin(eligible))] if param else t.iloc[0:0]
        oos.append(test)
        years.append({"year": y, "param": param, "eligible": len(eligible), "result": _trade_stats(test["net_return"])})
    oos_all = pd.concat(oos, ignore_index=True) if oos else pd.DataFrame(columns=["net_return"])
    first = int(t["year"].min()) + lookback_years
    baseline = t[(t["year"] >= first) & (t["param"] == default_param)]
    trained, base = _trade_stats(oos_all["net_return"]), _trade_stats(baseline["net_return"])
    positive_years = sum(1 for y in years if (y["result"].get("avg") or 0) > 0)
    latest = int(t["year"].max())
    param_now, eligible_now, _ = choose(t[t["year"] > latest - lookback_years])
    enforce = bool(trained.get("trades", 0) >= 30 and (trained.get("avg") or 0) > 0
                   and (trained.get("avg") or 0) > (base.get("avg") or 0) and positive_years * 2 >= len(years))
    stop_buffer, min_rr = (float(x) for x in param_now.split(":")) if param_now else (None, None)
    return {"years": years, "out_of_sample": trained, "default_every_symbol": base, "positive_years": positive_years,
            "test_years": len(years), "param_now": {"STOP_BUFFER_ATR15": stop_buffer, "MIN_RR": min_rr},
            "eligible_now": sorted(eligible_now), "enforce": enforce,
            "rule": f"setting and symbols chosen on the prior {lookback_years} years: >= {min_trades} trades, average net > 0"}


def run_multiyear(bot: str, *, publish: bool = True) -> dict[str, Any]:
    from concurrent.futures import ProcessPoolExecutor

    from data import alpaca_data, alpaca_options_data
    cfg = MULTIYEAR[bot]
    symbols = (sorted(set(alpaca_data.BROAD_CANDIDATE_UNIVERSE) | {"SPY", "QQQ"}) if bot == "stocks"
               else list(alpaca_options_data.OPTIONS_UNDERLYINGS))
    started = time.time()
    frames = []
    LOCAL_DIR.mkdir(parents=True, exist_ok=True)
    progress_path = LOCAL_DIR / f"{bot}_multiyear_progress.json"
    from concurrent.futures import as_completed
    with ProcessPoolExecutor(MULTIYEAR_WORKERS) as pool:
        futures = {pool.submit(_multiyear_symbol, (sym, cfg["module"], cfg["sides"], PARAM_GRID or [None])): sym for sym in symbols}
        done = 0
        for fut in as_completed(futures):
            done += 1
            try:
                t = fut.result()
            except Exception as exc:
                logger.warning("[setup_backtest] multi-year replay failed for %s: %s", futures[fut], exc)
                t = pd.DataFrame()
            if not t.empty:
                frames.append(t)
            progress_path.write_text(json.dumps({
                "bot": bot, "done": done, "total": len(symbols), "trades_so_far": int(sum(len(f) for f in frames)),
                "workers": MULTIYEAR_WORKERS, "started_at": dt.datetime.fromtimestamp(started, dt.timezone.utc).isoformat(),
                "elapsed_sec": round(time.time() - started), "last_symbol": futures[fut],
            }), encoding="utf-8")
    trades = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    result: dict[str, Any] = {"ok": not trades.empty, "bot": bot, "symbols": len(symbols), "module": cfg["module"],
                              "grid": [f"{a}:{b}" for a, b in PARAM_GRID],
                              "computed_at": dt.datetime.now(dt.timezone.utc).isoformat(), "seconds": round(time.time() - started)}
    if not trades.empty:
        import importlib
        mod = importlib.import_module(f"data.{cfg['module']}")
        default_param = f"{mod.STOP_BUFFER_ATR15}:{mod.MIN_RR}"
        base = trades[trades["param"] == default_param] if "param" in trades and default_param in set(trades["param"]) else trades
        result["default_param"] = default_param
        result["all_trades"] = summarize(base, "net_return")
        result["walk_forward"] = walk_forward_eligibility(base)
        if "param" in trades and trades["param"].nunique() > 1:
            result["trained"] = walk_forward_trained(trades, default_param=default_param)
    LOCAL_DIR.mkdir(parents=True, exist_ok=True)
    (LOCAL_DIR / f"{bot}_multiyear.json").write_text(json.dumps(result, default=str), encoding="utf-8")
    if not trades.empty:
        trades.to_parquet(LOCAL_DIR / f"{bot}_multiyear_trades.parquet", index=False)
    if publish and not trades.empty:
        result["published"] = _publish_multiyear(bot, result)
        (LOCAL_DIR / f"{bot}_multiyear.json").write_text(json.dumps(result, default=str), encoding="utf-8")
    return result


def _eligibility_from(result: dict[str, Any]) -> dict[str, Any]:
    """What the bot enforces: the trained setting + symbols when training
    beat the untrained walk-forward out of sample, else the plain list."""
    wf, tr = result.get("walk_forward") or {}, result.get("trained") or {}
    use_trained = bool(tr.get("enforce") and (tr.get("out_of_sample", {}).get("avg") or 0) > (wf.get("out_of_sample", {}).get("avg") or 0))
    src = tr if use_trained else wf
    return {"enforce": bool(src.get("enforce")), "symbols": src.get("eligible_now", []), "rule": src.get("rule"),
            "params": tr.get("param_now") if use_trained else None, "source": "trained" if use_trained else "walk_forward",
            "computed_at": result.get("computed_at"), "out_of_sample": src.get("out_of_sample"), "grid": result.get("grid")}


def _publish_multiyear(bot: str, result: dict[str, Any]) -> bool:
    token = os.getenv("HF_API_KEY", "")
    if not token:
        return False
    try:
        from huggingface_hub import CommitOperationAdd, HfApi
        eligibility = _eligibility_from(result)
        ops = [CommitOperationAdd("setup_strategy/multiyear/report.json", json.dumps(result, indent=2, default=str).encode()),
               CommitOperationAdd("setup_strategy/multiyear/trades.parquet", str(LOCAL_DIR / f"{bot}_multiyear_trades.parquet")),
               CommitOperationAdd("setup_strategy/eligibility.json", json.dumps(eligibility, indent=2).encode())]
        HfApi(token=token).create_commit(repo_id=REPOS[bot], repo_type="model", operations=ops,
                                         commit_message=f"{bot} multi-year setup study {result['computed_at'][:10]}")
        return True
    except Exception as exc:
        logger.warning("[setup_backtest] multi-year publish failed for %s: %s", bot, exc)
        return False


def _study_symbols(bot: str) -> list[str]:
    from data import alpaca_data, alpaca_options_data
    return (sorted(set(alpaca_data.BROAD_CANDIDATE_UNIVERSE) | {"SPY", "QQQ"}) if bot == "stocks"
            else list(alpaca_options_data.OPTIONS_UNDERLYINGS))


def archive_ready(bot: str, *, min_fraction: float = 0.98) -> bool:
    """True once the SIP archive on HF holds this year's file for (nearly)
    every symbol the study replays -- never start on a partial upload."""
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
    return bool(symbols) and present >= min_fraction * len(symbols)


def _running(name: str) -> bool:
    pid_file = LOCAL_DIR / f"{name}.pid"
    if not pid_file.exists():
        return False
    try:
        os.kill(int(pid_file.read_text()), 0)
        return True
    except (ValueError, ProcessLookupError, PermissionError):
        return False


def maybe_start_multiyear(bot: str) -> dict[str, Any]:
    """Checked every few minutes on the Space: launch this bot's study once
    the archive is complete, if it has no published study yet and no other
    study is running (one study at a time gets every core)."""
    current = eligibility(bot)
    if current is not None and current.get("grid") == [f"{a}:{b}" for a, b in PARAM_GRID]:
        return {"ok": True, "action": "already_published"}
    if any(_running(f"{b}_multiyear") for b in MULTIYEAR):
        return {"ok": True, "action": "a_study_is_running"}
    if not archive_ready(bot):
        return {"ok": True, "action": "waiting_for_archive"}
    _eligibility_cache.pop(bot, None)
    return launch(f"{bot}_multiyear")


def multiyear_status(bot: str) -> dict[str, Any]:
    """Progress of a running study and the latest finished one."""
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
        tr = (json.loads((LOCAL_DIR / f"{bot}_multiyear.json").read_text(encoding="utf-8")).get("trained") or {})
        if tr:
            out["latest"]["trained"] = {k: tr.get(k) for k in ("out_of_sample", "default_every_symbol", "positive_years",
                                                                "test_years", "param_now", "enforce", "years")}
            out["latest"]["trained"]["eligible_now"] = len(tr.get("eligible_now") or [])
    out["running"] = _running(f"{bot}_multiyear")
    return out


_eligibility_cache: dict[str, tuple[float, dict[str, Any] | None]] = {}


def eligibility(bot: str) -> dict[str, Any] | None:
    """The bot's published symbol eligibility (cached an hour), or None."""
    cached = _eligibility_cache.get(bot)
    if cached and time.time() - cached[0] < 3600:
        return cached[1]
    data = None
    local = LOCAL_DIR / f"{bot}_multiyear.json"
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
    result.update({"bot": bot, "days": days, "computed_at": dt.datetime.now(dt.timezone.utc).isoformat(),
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
    if pid_file.exists():
        try:
            pid = int(pid_file.read_text())
            os.kill(pid, 0)
            return {"ok": True, "action": "already_running", "pid": pid}
        except (ValueError, ProcessLookupError, PermissionError):
            pass
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
    out = run_multiyear(bot_arg.removesuffix("_multiyear")) if bot_arg.endswith("_multiyear") else run(bot_arg)
    print(json.dumps({k: v for k, v in out.items() if k in ("ok", "bot", "seconds", "published", "error")}))
    try:
        (LOCAL_DIR / f"{bot_arg}.pid").unlink()
    except FileNotFoundError:
        pass


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
