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
    out = run(bot_arg)
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
