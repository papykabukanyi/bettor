"""Walk-forward backtest of the actual live crypto strategy code across
every pair in the tradable universe.

This deliberately reuses the REAL decision functions from
alpaca_crypto_strategy.py (decide_entry_technical, decide_exit,
compute_position_notional, evaluate_candidate's scoring, adaptive_exit_pcts,
round_trip_fee_usd) rather than reimplementing the rules -- otherwise a
backtest could "pass" while the live code has since drifted from what was
actually tested. Same discipline as perps_backtest.py, this codebase's own
proven template for this exact shape (self-managed exits, no broker-native
bracket order, adaptive per-instrument take-profit/stop-loss, velocity-based
quick-profit).

Method:
  1. Pull extended 1-minute history per pair -- a SINGLE fetch_crypto_bars
     call per pair covers a multi-week window in one shot (Alpaca's own
     pagination handles it internally, confirmed via alpaca_client.py's own
     docstring), unlike Kalshi's 5000-candle-per-call cap that forces
     perps_backtest.py to chain multiple calls.
  2. Pick one global cutoff timestamp. Everything before it is the TRAINING
     window; everything from it onward is the TEST window -- the SAME cutoff
     across every pair, so no pair's future data can leak into another's
     simulated past via a shared model.
  3. Fit a direction classifier on the training window only (a fresh,
     in-memory-only fit -- this never touches the live production model
     files or pushes anything to Hugging Face).
  4. Walk forward through the test window in chronological order across ALL
     pairs interleaved by real timestamp, simulating the exact
     concurrency/sizing/exit rules the live bot uses: up to
     MAX_CONCURRENT_POSITIONS positions, each sized at POSITION_SIZE_PCT of
     the CURRENT simulated available balance (notional, not leveraged --
     crypto here is real spot-only, no margin), take-profit / stop-loss /
     velocity-based quick-profit / max-hold exits, real round-trip taker
     fees.

Known, honest limitation: there is no free historical archive of crypto news
sentiment, so the backtest runs with `sentiment_score` held at 0.0 (neutral)
throughout -- it tests the TECHNICAL + MODEL signal exactly as the live
strategy would, minus the live news feature. This is disclosed rather than
faked with synthetic sentiment history, same as perps_backtest.py's own
identical limitation.

`use_correlation_study` (see crypto_correlation.py's own module docstring)
validates the FULL local chart-study composite here -- peer confirmation,
leader divergence, and breadth across Alpaca's own multi-pair historical
frame (already walked by this backtest, unlike perps_backtest.py which
lacks an equivalent cross-service historical archive), plus multi-timeframe
confluence. Leakage-free: each simulated decision point only ever sees
data up to and including its own `ts` (see build_study_from_wide).
"""
from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, roc_auc_score

from data import alpaca_crypto_strategy as strat
from data import crypto_correlation as cc
from data import walkforward
from data.alpaca_crypto_data import FEATURE_COLUMNS, engineer_features, fetch_crypto_bars, get_crypto_universe, symbol_to_coin

logger = logging.getLogger(__name__)


def build_pair_frame(symbol: str, *, days: int) -> pd.DataFrame:
    """Extended engineered-feature history for one crypto pair, technical-
    only (sentiment_score held at 0.0 -- see module docstring)."""
    one_min_df = fetch_crypto_bars(symbol, days=days)
    feats = engineer_features(one_min_df, sentiment_score=0.0)
    if feats.empty:
        return feats
    feats.insert(0, "symbol", symbol)
    return feats


# n_estimators=200/150, n_jobs=-1 (not the live model's 60/n_jobs=1): this
# module is never deployed as a scheduled job on the memory-capped 512MB
# crypto dyno (see alpaca_crypto_server.py -- no backtest-sweep job is
# registered there, deliberately, given that service's own real OOM
# incidents this session and crypto's 24/7 trading leaving no safe
# "off-hours" window the way alpaca_options_backtest.py hides its own
# sweep in). Run locally or via a one-off manual trigger only -- same
# affordable-bigger-hyperparameters reasoning perps_backtest.py already
# uses for the identical reason.
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
    symbol_categories = list(labeled["symbol"].astype("category").cat.categories)
    labeled["symbol_code"] = labeled["symbol"].astype("category").cat.codes
    labeled = labeled.sort_values("ts")

    feature_cols = FEATURE_COLUMNS + ["symbol_code"]
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
            logger.warning("[alpaca_crypto_backtest] candidate %s failed to fit: %s", name, exc)
    if best_model is None:
        return None
    best_model.fit(labeled[feature_cols].values, labeled["label_up"].values)
    return {"model": best_model, "model_type": best_name, "feature_cols": feature_cols, "symbol_categories": symbol_categories}


def add_model_predictions(df: pd.DataFrame, fitted: dict[str, Any] | None) -> pd.DataFrame:
    """Batch-predict probability_up for every row ONCE (vectorized sklearn
    call) rather than one row at a time inside the simulation loop -- a
    parameter sweep runs `simulate()` many times over the same rows with
    only entry/sizing thresholds changing, and the model's own predictions
    never change between those runs, so computing them once and reusing the
    column is a large speedup instead of re-predicting per row."""
    df = df.copy()
    if fitted is None:
        df["model_probability_up"] = np.nan
        return df
    categories = fitted["symbol_categories"]
    symbol_codes = df["symbol"].map(lambda s: float(categories.index(s)) if s in categories else -1.0)
    feature_cols = fitted["feature_cols"]
    x = df[[c for c in feature_cols if c != "symbol_code"]].copy()
    x["symbol_code"] = symbol_codes
    x = x[feature_cols].values
    df["model_probability_up"] = fitted["model"].predict_proba(x)[:, 1]
    return df


def simulate(
    test_df: pd.DataFrame, fitted: dict[str, Any] | None = None, *,
    starting_balance: float = 500.0,
    position_size_pct: float | None = None,
    max_concurrent_positions: int | None = None,
    entry_dip_pct: float | None = None,
    min_volume_z: float | None = None,
    min_volatility_ratio: float | None = None,
    model_confidence_min: float | None = None,
    daily_loss_cap_pct: float | None = None,
    taker_fee_rate: float | None = None,
    take_profit_pct: float | None = None,
    stop_loss_pct: float | None = None,
    max_hold_minutes: int | None = None,
    use_correlation_study: bool | None = None,
    correlation_confidence_max_adjustment: float | None = None,
) -> dict[str, Any]:
    """Walk forward through `test_df` (all pairs, sorted by ts) replaying
    the real strategy functions. Every strategy parameter can be overridden
    per-call so a parameter sweep doesn't need to touch process-wide env
    vars between runs.

    decide_exit()/adaptive_exit_pcts() read TAKE_PROFIT_PCT/STOP_LOSS_PCT/
    MAX_HOLD_MINUTES as MODULE-LEVEL globals on alpaca_crypto_strategy, not
    as function parameters -- same real constraint alpaca_options_backtest.py's
    own simulate() already documents and handles: a sweep over these has to
    temporarily overwrite those globals for the duration of this call and
    restore them afterward no matter what (never leave live trading running
    on leftover sweep values).

    For a sweep (many calls over the SAME rows with only thresholds
    changing), call `add_model_predictions(test_df, fitted)` yourself once
    and pass the result with `fitted=None` here -- `simulate` will reuse the
    existing `model_probability_up` column instead of re-predicting."""
    position_size_pct = strat.POSITION_SIZE_PCT if position_size_pct is None else position_size_pct
    max_concurrent_positions = strat.MAX_CONCURRENT_POSITIONS if max_concurrent_positions is None else max_concurrent_positions
    entry_dip_pct = strat.ENTRY_DIP_PCT if entry_dip_pct is None else entry_dip_pct
    min_volume_z = strat.MIN_VOLUME_Z if min_volume_z is None else min_volume_z
    min_volatility_ratio = strat.MIN_VOLATILITY_RATIO if min_volatility_ratio is None else min_volatility_ratio
    model_confidence_min = strat.MODEL_CONFIDENCE_MIN if model_confidence_min is None else model_confidence_min
    daily_loss_cap_pct = strat.DAILY_LOSS_CAP_PCT if daily_loss_cap_pct is None else daily_loss_cap_pct
    taker_fee_rate = strat.TAKER_FEE_RATE if taker_fee_rate is None else taker_fee_rate
    take_profit_pct = strat.TAKE_PROFIT_PCT if take_profit_pct is None else take_profit_pct
    stop_loss_pct = strat.STOP_LOSS_PCT if stop_loss_pct is None else stop_loss_pct
    max_hold_minutes = strat.MAX_HOLD_MINUTES if max_hold_minutes is None else max_hold_minutes
    use_correlation_study = strat.USE_CORRELATION_STUDY if use_correlation_study is None else use_correlation_study
    correlation_confidence_max_adjustment = (
        strat.CORRELATION_CONFIDENCE_MAX_ADJUSTMENT if correlation_confidence_max_adjustment is None
        else correlation_confidence_max_adjustment
    )

    original_globals = {
        "TAKE_PROFIT_PCT": strat.TAKE_PROFIT_PCT, "STOP_LOSS_PCT": strat.STOP_LOSS_PCT,
        "MAX_HOLD_MINUTES": strat.MAX_HOLD_MINUTES,
    }
    strat.TAKE_PROFIT_PCT = take_profit_pct
    strat.STOP_LOSS_PCT = stop_loss_pct
    strat.MAX_HOLD_MINUTES = max_hold_minutes
    try:
        return _simulate_inner(
            test_df, fitted, starting_balance=starting_balance, position_size_pct=position_size_pct,
            max_concurrent_positions=max_concurrent_positions, entry_dip_pct=entry_dip_pct, min_volume_z=min_volume_z,
            min_volatility_ratio=min_volatility_ratio, model_confidence_min=model_confidence_min,
            daily_loss_cap_pct=daily_loss_cap_pct, taker_fee_rate=taker_fee_rate,
            use_correlation_study=use_correlation_study,
            correlation_confidence_max_adjustment=correlation_confidence_max_adjustment,
        )
    finally:
        strat.TAKE_PROFIT_PCT = original_globals["TAKE_PROFIT_PCT"]
        strat.STOP_LOSS_PCT = original_globals["STOP_LOSS_PCT"]
        strat.MAX_HOLD_MINUTES = original_globals["MAX_HOLD_MINUTES"]


def _simulate_inner(
    test_df: pd.DataFrame, fitted: dict[str, Any] | None, *,
    starting_balance: float, position_size_pct: float, max_concurrent_positions: int,
    entry_dip_pct: float, min_volume_z: float, min_volatility_ratio: float, model_confidence_min: float,
    daily_loss_cap_pct: float, taker_fee_rate: float,
    use_correlation_study: bool = False, correlation_confidence_max_adjustment: float = 0.06,
) -> dict[str, Any]:
    """The actual walk-forward loop -- pulled out of simulate() purely so
    that function's try/finally global-restore wrapper doesn't have to
    nest the entire loop body a level deeper. Only ever called from
    simulate(), with the real alpaca_crypto_strategy globals it depends on
    (TAKE_PROFIT_PCT/STOP_LOSS_PCT/MAX_HOLD_MINUTES) already set for this run."""
    df = test_df.sort_values("ts").reset_index(drop=True)
    if "model_probability_up" not in df.columns:
        df = add_model_predictions(df, fitted)

    # See crypto_correlation.py's own module docstring. Unlike
    # perps_backtest.py, this backtest already walks a real multi-pair
    # historical frame across Alpaca's OWN universe, so it can validate the
    # FULL local composite (peer confirmation + leader divergence + breadth
    # + multi-timeframe) -- no cross-service data gap here, since this IS
    # the Alpaca side. Pivoted once up front; each simulated decision point
    # re-slices it by its OWN `ts` via build_study_from_wide's `as_of_ts`,
    # so a later timestamp's return can never leak into an earlier
    # decision. Recomputed on a coarse ~15min bucket, same reasoning as
    # perps_backtest.py's own identical cache.
    correlation_wide = cc._pivot_returns(df, id_col="symbol", ts_col="ts", ret_col=cc.RET_COL) if use_correlation_study else pd.DataFrame()  # noqa: SLF001
    _correlation_study_cache: dict[str, Any] = {"bucket": None, "study": {}}
    _CORRELATION_RECOMPUTE_BUCKET_SEC = 900

    def _correlation_study_as_of(ts: float) -> dict[str, Any]:
        bucket = int(ts // _CORRELATION_RECOMPUTE_BUCKET_SEC)
        if _correlation_study_cache["bucket"] != bucket:
            _correlation_study_cache["study"] = cc.build_study_from_wide(correlation_wide, as_of_ts=ts, leader_id="BTC")
            _correlation_study_cache["bucket"] = bucket
        return _correlation_study_cache["study"]

    def _correlation_bullishness_as_of(ts: float, symbol: str, row) -> float:
        study = _correlation_study_as_of(ts)
        coin = symbol_to_coin(symbol)
        peer_score, _ = cc._peer_confirmation_bullishness(study, coin)  # noqa: SLF001
        divergence_score, _ = cc._leader_divergence_bullishness(study, coin)  # noqa: SLF001
        breadth_score, _ = cc._breadth_bullishness(study)  # noqa: SLF001
        timeframe_row = {field: getattr(row, field, None) for field in cc._TIMEFRAME_REFERENCE_SCALES}  # noqa: SLF001
        timeframe_row["rsi_14"] = getattr(row, "rsi_14", None)
        multi_timeframe_score, _ = cc.multi_timeframe_bullishness(timeframe_row)
        return max(-1.0, min(1.0, 0.3 * peer_score + 0.2 * divergence_score + 0.2 * breadth_score + 0.3 * multi_timeframe_score))

    balance = starting_balance
    open_positions: dict[str, dict[str, Any]] = {}
    trades: list[dict[str, Any]] = []
    daily_pnl: dict[str, float] = {}
    daily_reference_balance: dict[str, float] = {}

    for row in df.itertuples(index=False):
        symbol = row.symbol
        price = float(row.close)
        date_str = pd.Timestamp(row.ts, unit="s", tz="UTC").strftime("%Y-%m-%d")
        if date_str not in daily_reference_balance:
            daily_reference_balance[date_str] = balance

        # -- manage an existing position on this pair first --
        pos = open_positions.get(symbol)
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
            current_volatility = strat._sample_volatility([list(s) for s in trimmed])  # noqa: SLF001

            # Same real bug class perps_backtest.py already found and fixed:
            # decide_exit()'s max_hold_time check must use the SIMULATED
            # current time (this row's own timestamp), not real wall-clock
            # time, or almost every position force-exits on its first tick.
            sim_now = pd.Timestamp(row.ts, unit="s", tz="UTC").to_pydatetime()
            exit_correlation_score = _correlation_bullishness_as_of(row.ts, symbol, row) if use_correlation_study else None
            should_exit, reason = strat.decide_exit(
                pos, price, velocity_pct_per_min=velocity, current_volatility=current_volatility, now=sim_now,
                correlation_score=exit_correlation_score,
            )
            if should_exit:
                gross = round((price - pos["entry_price"]) * pos["count"], 6)  # long-only
                fee = strat.round_trip_fee_usd(pos["entry_price"], price, pos["count"], taker_fee_rate=taker_fee_rate)
                realized = round(gross - fee, 6)
                balance += realized
                daily_pnl[date_str] = daily_pnl.get(date_str, 0.0) + realized
                trades.append({
                    "symbol": symbol, "entry_price": pos["entry_price"], "exit_price": price,
                    "count": pos["count"], "gross_pnl_usd": gross, "fee_usd": fee, "realized_pnl_usd": realized, "reason": reason,
                    "opened_ts": pos["opened_ts"], "closed_ts": row.ts,
                    "held_minutes": (row.ts - pos["opened_ts"]) / 60.0,
                })
                del open_positions[symbol]
            continue

        # -- otherwise, consider a new entry on this pair --
        # Dedupe by BASE COIN, not exact symbol -- get_crypto_universe()
        # returns multiple quote currencies for the same coin (e.g. BTC/USD
        # and BTC/USDT), and the live strategy explicitly refuses to hold
        # both at once (two bets on the identical underlying move, not real
        # diversification -- see alpaca_crypto_strategy.scan_and_enter's own
        # docstring). Replicated here so the backtest doesn't overstate
        # diversification/return by triple-betting the same coin via BTC's
        # 3 listed quote pairs.
        coin = symbol_to_coin(symbol)
        held_coins = {symbol_to_coin(s) for s in open_positions}
        if coin in held_coins or len(open_positions) >= max_concurrent_positions:
            continue

        reference_balance = daily_reference_balance[date_str]
        if reference_balance > 0 and daily_pnl.get(date_str, 0.0) <= -abs(daily_loss_cap_pct) * reference_balance:
            continue  # daily loss cap breached -- exits still happen above, only new entries are blocked

        # Entry gates reimplemented inline with overridable PARAMETERS,
        # rather than calling decide_entry_technical/evaluate_candidate
        # directly -- same deliberate choice perps_backtest.py's own
        # simulate() already makes for the identical reason: those live
        # functions read MIN_VOLUME_Z/ENTRY_DIP_PCT/etc. as module-level
        # constants, and a parameter sweep needs to vary these per-call
        # without mutating process-wide state between runs. The formulas
        # themselves are copied verbatim from decide_entry_technical.
        #
        # short_ma reconstructed from dist_to_ma_15 (= (close - ma_15) / ma_15
        # => ma_15 = close / (1 + dist_to_ma_15)), matching exactly what
        # decide_entry_technical compares against live.
        dollar_volume_z = row.dollar_volume_z
        volume_ok = dollar_volume_z is not None and dollar_volume_z == dollar_volume_z and dollar_volume_z >= min_volume_z
        volatility_ratio_ok = True
        if row.volatility_30 > 0:
            volatility_ratio_ok = (row.volatility_5 / row.volatility_30) >= min_volatility_ratio
        short_ma = price / (1 + row.dist_to_ma_15) if (1 + row.dist_to_ma_15) != 0 else price
        dip_pct = (short_ma - price) / short_ma if short_ma > 0 else 0.0
        if not (volume_ok and volatility_ratio_ok and short_ma > 0 and dip_pct >= entry_dip_pct):
            continue

        proba_up = row.model_probability_up
        model_ok = proba_up == proba_up  # not NaN -> a model exists
        correlation_score = _correlation_bullishness_as_of(row.ts, symbol, row) if use_correlation_study else None
        if not model_ok:
            # Same extra caution live evaluate_candidate applies to its own
            # technical-only fallback (no model yet, riskiest path this
            # function takes): skip if the correlation study actively
            # disagrees strongly enough.
            if use_correlation_study and correlation_score is not None and correlation_score <= -strat.PROMISING_CORRELATION_SCORE:
                continue
        else:
            effective_model_confidence_min = model_confidence_min
            if use_correlation_study and correlation_score is not None:
                effective_model_confidence_min = max(
                    0.5, min(0.95, effective_model_confidence_min - correlation_score * correlation_confidence_max_adjustment),
                )
            if proba_up < effective_model_confidence_min:
                continue

        available = balance
        notional = round(max(0.0, available) * position_size_pct, 2)
        if notional < 1.0 or price <= 0:
            continue
        count = notional / price

        open_positions[symbol] = {
            "symbol": symbol, "entry_price": price, "count": float(count),
            "opened_at": pd.Timestamp(row.ts, unit="s", tz="UTC").isoformat(),
            "opened_ts": row.ts, "_samples": [],
            "entry_volatility_30": row.volatility_30,
        }

    open_at_end = len(open_positions)
    total_pnl = sum(t["realized_pnl_usd"] for t in trades)
    total_fees = sum(t.get("fee_usd", 0.0) for t in trades)
    wins = [t for t in trades if t["realized_pnl_usd"] > 0]
    span_days = max(1e-9, (df["ts"].max() - df["ts"].min()) / 86400.0) if not df.empty else 1.0

    return {
        "starting_balance": starting_balance,
        "ending_balance_realized": round(starting_balance + total_pnl, 6),
        "total_realized_pnl_usd": round(total_pnl, 6),
        "total_fees_usd": round(total_fees, 6),
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
    *, days: int = 14, train_frac: float = 0.7, starting_balance: float = 500.0,
    symbols: list[str] | None = None, **strategy_overrides: Any,
) -> dict[str, Any]:
    """End-to-end: fetch extended history for every requested pair (default:
    the full live tradable universe), fit a model on the training window,
    simulate the test window, return a full report."""
    universe = symbols or get_crypto_universe()
    frames = []
    for symbol in universe:
        try:
            feats = build_pair_frame(symbol, days=days)
            if not feats.empty:
                frames.append(feats)
        except Exception as exc:
            logger.warning("[alpaca_crypto_backtest] build_pair_frame failed for %s: %s", symbol, exc)
    if not frames:
        return {"ok": False, "reason": "no_data"}

    combined = pd.concat(frames, ignore_index=True).sort_values("ts")
    cutoff_ts = combined["ts"].quantile(train_frac)
    train_df = combined[combined["ts"] < cutoff_ts]
    test_df = combined[combined["ts"] >= cutoff_ts]
    if test_df.empty:
        return {"ok": False, "reason": "no_test_rows"}

    fitted = fit_backtest_model(train_df)
    result = simulate(test_df, fitted, starting_balance=starting_balance, **strategy_overrides)
    result["ok"] = True
    result["model_used"] = fitted["model_type"] if fitted else None
    result["symbols"] = universe
    result["train_rows"] = len(train_df)
    result["test_rows"] = len(test_df)
    result["cutoff_ts"] = float(cutoff_ts)
    return result


def run_walkforward_backtest(
    *, days: int = 30, fold_bounds: list[tuple[float, float, float]] | None = None,
    starting_balance: float = 500.0, symbols: list[str] | None = None, **strategy_overrides: Any,
) -> dict[str, Any]:
    """Same data pipeline as run_backtest() (fetch extended history for
    every requested pair, default the full tradable universe), but replays
    MULTIPLE expanding-window train/test folds (see walkforward.py) instead
    of a single 70/30 split -- see perps_backtest.run_walkforward_backtest's
    own docstring for why this matters more than a single split. `days`
    defaults higher than run_backtest's 14 (30) since the same history now
    gets carved into 4 folds instead of 1.

    Returns `{"ok", "folds": [...], "fold_count", "profitable_fold_ratio",
    "mean_return_pct", "std_return_pct", ...}`."""
    universe = symbols or get_crypto_universe()
    frames = []
    for symbol in universe:
        try:
            feats = build_pair_frame(symbol, days=days)
            if not feats.empty:
                frames.append(feats)
        except Exception as exc:
            logger.warning("[alpaca_crypto_backtest] build_pair_frame failed for %s: %s", symbol, exc)
    if not frames:
        return {"ok": False, "reason": "no_data"}

    combined = pd.concat(frames, ignore_index=True).sort_values("ts")
    result = walkforward.run_walkforward_folds(
        combined, fit_fn=fit_backtest_model, simulate_fn=simulate, fold_bounds=fold_bounds,
        simulate_kwargs={"starting_balance": starting_balance, **strategy_overrides},
    )
    result["symbols"] = universe
    return result


# Real evidence found in review: a full-universe (68 pairs, 21-day) backtest
# against current defaults returned -13.7% with a 26.5% win rate and $56.33
# in fees against a $68.28 total loss -- fees alone consumed most of the
# loss, at 23.3 trades/day. TAKE_PROFIT_PCT=1%/STOP_LOSS_PCT=0.8% vs. a
# ~0.5% round-trip taker fee (2 x TAKER_FEE_RATE=0.25%) leaves take-profit
# barely covering costs even on a win. The wider-target and higher-
# confidence variants below are direct responses to that finding, not
# arbitrary guesses.
_SWEEP_GRID = [
    {"label": "current_defaults"},
    {"label": "wider_take_profit", "take_profit_pct": 0.018, "stop_loss_pct": 0.012},
    {"label": "wider_tp_higher_confidence", "take_profit_pct": 0.018, "stop_loss_pct": 0.012, "model_confidence_min": 0.6},
    {"label": "higher_confidence_only", "model_confidence_min": 0.62},
    {"label": "deeper_dip_fewer_trades", "entry_dip_pct": 0.003, "min_volatility_ratio": 1.0},
    {"label": "quality_over_quantity", "entry_dip_pct": 0.003, "model_confidence_min": 0.6, "take_profit_pct": 0.018, "stop_loss_pct": 0.012},
    {"label": "shorter_hold_tighter_scalp", "entry_dip_pct": 0.001, "max_hold_minutes": 45, "take_profit_pct": 0.008, "stop_loss_pct": 0.006},
]


def run_config_sweep(test_with_preds: pd.DataFrame, *, starting_balance: float = 500.0, min_trades: int = 5) -> dict[str, Any]:
    """Tries a small set of parameter variants against the SAME already-
    predicted test rows (see add_model_predictions) and reports each --
    mirrors alpaca_options_backtest.py's own run_config_sweep shape. Configs
    producing fewer than min_trades are still reported but flagged
    low_sample so a lucky/unlucky handful of trades doesn't get mistaken for
    a real edge."""
    results = []
    for config in _SWEEP_GRID:
        label = config["label"]
        overrides = {k: v for k, v in config.items() if k != "label"}
        result = simulate(test_with_preds, fitted=None, starting_balance=starting_balance, **overrides)
        result["label"] = label
        result["low_sample"] = result["trade_count"] < min_trades
        results.append(result)
    results.sort(key=lambda r: (not r["low_sample"], r["return_pct"]), reverse=True)
    return {"ok": True, "configs": results, "best": results[0] if results else None}
