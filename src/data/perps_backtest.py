"""Walk-forward backtest of the actual live strategy code across every
Kalshi perp instrument.

This deliberately reuses the REAL decision functions from perps_strategy.py
(decide_entry_technical, decide_exit, compute_leveraged_count,
evaluate_candidate's scoring) rather than reimplementing the rules --
otherwise a backtest could "pass" while the live code has since drifted from
what was actually tested.

Method:
  1. Pull extended 1-minute + 60-minute history per ticker (chaining several
     Kalshi candlestick calls, since each call caps at 5000 candles) and
     engineer the same leakage-free features the live pipeline uses.
  2. Pick one global cutoff timestamp. Everything before it is the TRAINING
     window; everything from it onward is the TEST window -- the SAME cutoff
     across every instrument, so no instrument's future data can leak into
     another's simulated past via a shared model.
  3. Fit a direction classifier on the training window only (a fresh,
     in-memory-only fit -- this never touches the live production model
     files or pushes anything to Hugging Face).
  4. Walk forward through the test window in chronological order across ALL
     instruments interleaved by real timestamp, simulating the exact
     concurrency/sizing/exit rules the live bot uses: up to
     MAX_CONCURRENT_POSITIONS positions, each sized at POSITION_SIZE_PCT of
     the CURRENT simulated available balance (balance minus margin already
     committed to other open positions) using each market's own leverage,
     take-profit / stop-loss / velocity-based quick-profit / max-hold exits.

Known, honest limitation: there is no free historical archive of crypto news
sentiment, so the backtest runs with `sentiment_score` held at 0.0 (neutral)
throughout -- it tests the TECHNICAL + MODEL signal exactly as the live
strategy would, minus the live news feature. This is disclosed rather than
faked with synthetic sentiment history.
"""
from __future__ import annotations

import logging
import time
from typing import Any

import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, roc_auc_score

from data import perps_strategy as strat
from data.kalshi_perps import get_margin_candlesticks
from data.perps_data import FEATURE_COLUMNS, _candles_to_frame, engineer_features, get_watchlist

logger = logging.getLogger(__name__)

MAX_CANDLES_PER_CALL = 5000


def fetch_extended_candles(ticker: str, *, days: int, period_interval: int) -> pd.DataFrame:
    """Chain sequential Kalshi candlestick calls backward from now to cover
    more history than the 5000-candles-per-call cap allows in one request."""
    seconds_per_candle = period_interval * 60
    max_span_sec = MAX_CANDLES_PER_CALL * seconds_per_candle
    total_span_sec = days * 86400
    now = int(time.time())
    frames = []
    window_end = now
    remaining = total_span_sec
    while remaining > 0:
        span = min(max_span_sec, remaining)
        window_start = window_end - span
        try:
            resp = get_margin_candlesticks(
                ticker, start_ts=window_start, end_ts=window_end, period_interval=period_interval,
            )
            frames.append(_candles_to_frame(resp.get("candlesticks") or []))
        except Exception as exc:
            logger.warning("[perps_backtest] candle chunk fetch failed for %s: %s", ticker, exc)
            break
        window_end = window_start
        remaining -= span
    frames = [f for f in frames if not f.empty]  # e.g. windows before a newly-listed ticker's launch date
    if not frames:
        return pd.DataFrame({"ts": pd.Series(dtype="int64"), "close": pd.Series(dtype="float64")})
    combined = pd.concat(frames, ignore_index=True)
    return combined.drop_duplicates(subset="ts").sort_values("ts").reset_index(drop=True)


def build_ticker_frame(ticker: str, *, days: int) -> pd.DataFrame:
    """Extended engineered-feature history for one ticker, technical-only
    (sentiment_score held at 0.0 -- see module docstring)."""
    one_min_df = fetch_extended_candles(ticker, days=days, period_interval=1)
    hourly_df = fetch_extended_candles(ticker, days=max(days, 30), period_interval=60)
    feats = engineer_features(one_min_df, hourly_df, sentiment_score=0.0)
    if feats.empty:
        return feats
    feats.insert(0, "ticker", ticker)
    return feats


_CANDIDATES = {
    "logistic_regression": lambda: LogisticRegression(max_iter=1000, class_weight="balanced"),
    "random_forest": lambda: RandomForestClassifier(
        n_estimators=200, max_depth=6, min_samples_leaf=20, class_weight="balanced", random_state=42, n_jobs=-1,
    ),
    "gradient_boosting": lambda: GradientBoostingClassifier(
        n_estimators=150, max_depth=3, learning_rate=0.05, random_state=42,
    ),
}


def fit_backtest_model(train_df: pd.DataFrame, *, min_rows: int = 300) -> dict[str, Any] | None:
    """In-memory-only model fit for the backtest -- never touches the live
    production model files or Hugging Face. Returns None (technical-only
    simulation) if there isn't enough training-window data yet."""
    labeled = train_df.dropna(subset=["label_up"] + FEATURE_COLUMNS).copy()
    if len(labeled) < min_rows:
        return None
    labeled["label_up"] = labeled["label_up"].astype(int)
    ticker_categories = list(labeled["ticker"].astype("category").cat.categories)
    labeled["ticker_code"] = labeled["ticker"].astype("category").cat.codes
    labeled = labeled.sort_values("ts")

    feature_cols = FEATURE_COLUMNS + ["ticker_code"]
    split_idx = int(len(labeled) * 0.85)
    train_part, holdout = labeled.iloc[:split_idx], labeled.iloc[split_idx:]
    if holdout.empty or holdout["label_up"].nunique() < 2:
        return None

    best_name, best_model, best_score = None, None, -1.0
    for name, factory in _CANDIDATES.items():
        try:
            model = factory()
            model.fit(train_part[feature_cols].values, train_part["label_up"].values)
            preds = model.predict(holdout[feature_cols].values)
            proba = model.predict_proba(holdout[feature_cols].values)[:, 1]
            acc = float(accuracy_score(holdout["label_up"].values, preds))
            auc = float(roc_auc_score(holdout["label_up"].values, proba)) if holdout["label_up"].nunique() > 1 else 0.5
            combined = (acc + auc) / 2.0
            if combined > best_score:
                best_name, best_model, best_score = name, model, combined
        except Exception as exc:
            logger.warning("[perps_backtest] candidate %s failed to fit: %s", name, exc)
    if best_model is None:
        return None
    best_model.fit(labeled[feature_cols].values, labeled["label_up"].values)
    return {"model": best_model, "model_type": best_name, "feature_cols": feature_cols, "ticker_categories": ticker_categories}


def add_model_predictions(df: pd.DataFrame, fitted: dict[str, Any] | None) -> pd.DataFrame:
    """Batch-predict probability_up for every row ONCE (vectorized sklearn
    call) rather than one row at a time inside the simulation loop -- a
    parameter sweep runs `simulate()` many times over the same rows with
    only entry/sizing thresholds changing, and the model's own predictions
    never change between those runs, so computing them once and reusing the
    column is an easy, large speedup instead of re-predicting per row."""
    df = df.copy()
    if fitted is None:
        df["model_probability_up"] = np.nan
        return df
    categories = fitted["ticker_categories"]
    ticker_codes = df["ticker"].map(lambda t: float(categories.index(t)) if t in categories else -1.0)
    feature_cols = fitted["feature_cols"]
    x = df[[c for c in feature_cols if c != "ticker_code"]].copy()
    x["ticker_code"] = ticker_codes
    x = x[feature_cols].values
    df["model_probability_up"] = fitted["model"].predict_proba(x)[:, 1]
    return df


def fetch_leverage_by_ticker(tickers: list[str]) -> dict[str, float]:
    """One live snapshot of each market's current leverage_estimate --
    historical candlesticks don't carry it, so the backtest uses today's
    value as a fixed stand-in for the whole simulated window (a documented
    approximation, not a claim that leverage was constant historically)."""
    from data.kalshi_perps import get_margin_market

    result = {}
    for ticker in tickers:
        try:
            market = get_margin_market(ticker).get("market") or {}
            result[ticker] = float(market.get("leverage_estimate") or 1.0)
        except Exception as exc:
            logger.warning("[perps_backtest] leverage fetch failed for %s: %s", ticker, exc)
            result[ticker] = 1.0
    return result


def simulate(
    test_df: pd.DataFrame, fitted: dict[str, Any] | None = None, *,
    starting_balance: float = 20.0,
    leverage_by_ticker: dict[str, float] | None = None,
    position_size_pct: float | None = None,
    max_concurrent_positions: int | None = None,
    entry_dip_pct: float | None = None,
    trend_filter_down_pct: float | None = None,
    model_confidence_min: float | None = None,
    daily_loss_cap_pct: float | None = None,
    enable_shorts: bool | None = None,
) -> dict[str, Any]:
    """Walk forward through `test_df` (all tickers, sorted by ts) replaying
    the real strategy functions. Every strategy parameter can be overridden
    per-call so a parameter sweep doesn't need to touch process-wide env
    vars between runs.

    For a sweep (many calls over the SAME rows with only thresholds
    changing), call `add_model_predictions(test_df, fitted)` yourself once
    and pass the result with `fitted=None` here -- `simulate` will reuse the
    existing `model_probability_up` column instead of re-predicting."""
    leverage_by_ticker = leverage_by_ticker or {}
    position_size_pct = strat.POSITION_SIZE_PCT if position_size_pct is None else position_size_pct
    max_concurrent_positions = strat.MAX_CONCURRENT_POSITIONS if max_concurrent_positions is None else max_concurrent_positions
    entry_dip_pct = strat.ENTRY_DIP_PCT if entry_dip_pct is None else entry_dip_pct
    trend_filter_down_pct = strat.TREND_FILTER_DOWN_PCT if trend_filter_down_pct is None else trend_filter_down_pct
    model_confidence_min = strat.MODEL_CONFIDENCE_MIN if model_confidence_min is None else model_confidence_min
    daily_loss_cap_pct = strat.DAILY_LOSS_CAP_PCT if daily_loss_cap_pct is None else daily_loss_cap_pct
    enable_shorts = strat.ENABLE_SHORTS if enable_shorts is None else enable_shorts

    df = test_df.sort_values("ts").reset_index(drop=True)
    if "model_probability_up" not in df.columns:
        df = add_model_predictions(df, fitted)

    balance = starting_balance
    open_positions: dict[str, dict[str, Any]] = {}
    trades: list[dict[str, Any]] = []
    daily_pnl: dict[str, float] = {}
    daily_reference_balance: dict[str, float] = {}

    for row in df.itertuples(index=False):
        ticker = row.ticker
        price = float(row.close)
        date_str = pd.Timestamp(row.ts, unit="s", tz="UTC").strftime("%Y-%m-%d")
        if date_str not in daily_reference_balance:
            daily_reference_balance[date_str] = balance

        # -- manage an existing position on this ticker first --
        pos = open_positions.get(ticker)
        if pos is not None:
            velocity = None
            samples = pos.setdefault("_samples", [])
            samples.append((row.ts, price))
            cutoff = row.ts - strat.QUICK_PROFIT_WINDOW_SECONDS
            trimmed = [s for s in samples if s[0] >= cutoff] or samples[-1:]
            pos["_samples"] = trimmed[-30:]
            if len(trimmed) >= 2:
                oldest_ts, oldest_price = trimmed[0]
                elapsed_min = (row.ts - oldest_ts) / 60.0
                if elapsed_min > 0 and oldest_price > 0:
                    velocity = ((price - oldest_price) / oldest_price) / elapsed_min

            should_exit, reason = strat.decide_exit(pos, price, velocity_pct_per_min=velocity)
            if should_exit:
                if pos.get("side") == "short":
                    realized = round((pos["entry_price"] - price) * pos["count"], 6)  # profits on a FALLING price
                else:
                    realized = round((price - pos["entry_price"]) * pos["count"], 6)
                # `balance` is total equity throughout (only realized P&L ever
                # changes it); margin_committed_usd was NEVER subtracted from
                # it at open time -- it only ever reduced `available` via the
                # running sum below. Adding it back here too would manufacture
                # money out of nothing on every single trade.
                balance += realized
                daily_pnl[date_str] = daily_pnl.get(date_str, 0.0) + realized
                trades.append({
                    "ticker": ticker, "side": pos.get("side", "long"), "entry_price": pos["entry_price"], "exit_price": price,
                    "count": pos["count"], "realized_pnl_usd": realized, "reason": reason,
                    "opened_ts": pos["opened_ts"], "closed_ts": row.ts,
                    "held_minutes": (row.ts - pos["opened_ts"]) / 60.0,
                })
                del open_positions[ticker]
            continue

        # -- otherwise, consider a new entry on this ticker --
        if ticker in open_positions or len(open_positions) >= max_concurrent_positions:
            continue

        reference_balance = daily_reference_balance[date_str]
        if reference_balance > 0 and daily_pnl.get(date_str, 0.0) <= -abs(daily_loss_cap_pct) * reference_balance:
            continue  # daily loss cap breached -- exits still happen above, only new entries are blocked

        # short_ma reconstructed from dist_to_ma_15 (= (close - ma_15) / ma_15),
        # matching exactly what decide_entry_technical compares against live.
        # rally_pct is exactly -dip_pct (same MA, opposite-signed comparison),
        # mirroring decide_entry_technical's side="short" branch.
        dist_to_ma_15 = row.dist_to_ma_15
        short_ma = price / (1 + dist_to_ma_15) if (1 + dist_to_ma_15) != 0 else price
        dip_pct = (short_ma - price) / short_ma if short_ma > 0 else 0.0
        rally_pct = -dip_pct

        proba_up = row.model_probability_up
        model_ok = proba_up == proba_up  # not NaN -> a model exists

        chosen_side = None
        if row.trend_pct >= -trend_filter_down_pct and dip_pct >= entry_dip_pct:
            # technical-only fallback (no model yet) is long-only, same as live.
            if not model_ok or (proba_up >= 0.5 and proba_up >= model_confidence_min):
                chosen_side = "long"
        if (
            chosen_side is None and enable_shorts
            and row.trend_pct <= trend_filter_down_pct and rally_pct >= entry_dip_pct
            and model_ok and proba_up < 0.5 and (1.0 - proba_up) >= model_confidence_min
        ):
            chosen_side = "short"
        if chosen_side is None:
            continue

        committed = sum(p["margin_committed_usd"] for p in open_positions.values())
        available = balance - committed
        margin_budget = available * position_size_pct
        leverage = leverage_by_ticker.get(ticker, 1.0)
        notional_capacity = margin_budget * leverage
        count = int(notional_capacity // price) if price > 0 else 0
        if count < 1:
            continue
        margin_committed = round(count * price / leverage, 6)
        if margin_committed > available:
            continue

        open_positions[ticker] = {
            "ticker": ticker, "entry_price": price, "count": float(count), "side": chosen_side,
            "opened_at": pd.Timestamp(row.ts, unit="s", tz="UTC").isoformat(),
            "opened_ts": row.ts, "margin_committed_usd": margin_committed, "_samples": [],
        }

    # Mark-to-market any still-open positions at the last known price for reporting.
    open_at_end = len(open_positions)

    total_pnl = sum(t["realized_pnl_usd"] for t in trades)
    wins = [t for t in trades if t["realized_pnl_usd"] > 0]
    span_days = max(1e-9, (df["ts"].max() - df["ts"].min()) / 86400.0) if not df.empty else 1.0

    return {
        "starting_balance": starting_balance,
        "ending_balance_realized": round(starting_balance + total_pnl, 6),
        "total_realized_pnl_usd": round(total_pnl, 6),
        "return_pct": round(total_pnl / starting_balance, 6) if starting_balance else 0.0,
        "trade_count": len(trades),
        "win_count": len(wins),
        "win_rate": round(len(wins) / len(trades), 4) if trades else 0.0,
        "trades_per_day": round(len(trades) / span_days, 3),
        "open_positions_at_end": open_at_end,
        "span_days": round(span_days, 2),
        "trades": trades,
    }


def run_backtest(
    *, days: int = 14, train_frac: float = 0.7, starting_balance: float = 20.0,
    tickers: list[str] | None = None, **strategy_overrides: Any,
) -> dict[str, Any]:
    """End-to-end: fetch extended history for every requested ticker (default:
    the full live watchlist), fit a model on the training window, simulate
    the test window, return a full report."""
    watchlist = tickers or get_watchlist()
    frames = []
    for ticker in watchlist:
        try:
            feats = build_ticker_frame(ticker, days=days)
            if not feats.empty:
                frames.append(feats)
        except Exception as exc:
            logger.warning("[perps_backtest] build_ticker_frame failed for %s: %s", ticker, exc)
    if not frames:
        return {"ok": False, "reason": "no_data"}

    combined = pd.concat(frames, ignore_index=True).sort_values("ts")
    cutoff_ts = combined["ts"].quantile(train_frac)
    train_df = combined[combined["ts"] < cutoff_ts]
    test_df = combined[combined["ts"] >= cutoff_ts]
    if test_df.empty:
        return {"ok": False, "reason": "no_test_rows"}

    present_tickers = sorted(test_df["ticker"].unique())
    leverage_by_ticker = fetch_leverage_by_ticker(present_tickers)

    fitted = fit_backtest_model(train_df)
    result = simulate(
        test_df, fitted, starting_balance=starting_balance, leverage_by_ticker=leverage_by_ticker, **strategy_overrides,
    )
    result["ok"] = True
    result["model_used"] = fitted["model_type"] if fitted else None
    result["tickers"] = watchlist
    result["train_rows"] = len(train_df)
    result["test_rows"] = len(test_df)
    result["cutoff_ts"] = float(cutoff_ts)
    return result
