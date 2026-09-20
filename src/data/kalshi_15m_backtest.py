"""Walk-forward backtest of the real Kalshi 15-minute-market strategy code
(kalshi_15m_strategy.evaluate_candidate's own decision rule), crypto only.

Metals are NOT covered here -- there is no historical price archive for
gold/silver/copper at all (gold-api.com has no free /history endpoint, see
kalshi_15m_metals_data.py's own module docstring), so there is nothing to
backtest against yet. This module covers the 5 crypto coins
(kalshi_15m.KNOWN_15M_SERIES), which DO have a real, growing HF archive
(kalshi_15m_data.load_training_dataset, backfillable via
kalshi_15m_data.backfill_minute_history).

Deliberately much simpler than perps_backtest.py, because the product
itself is much simpler: a Kalshi 15-minute event contract has NO exit
mechanism at all (no stop-loss, no take-profit, no scale-in/partial-exit,
no early close -- see kalshi_15m_strategy.py's own check_settlements, the
ONLY place a position ever closes here) -- you hold every position to
settlement, period. So unlike perps' minute-by-minute open-position
management loop, this backtest only needs: pick an entry point once per
15-minute window per coin (see ONE_ROW_PER_WINDOW below), decide whether
the real strategy would have entered, and resolve it deterministically
against that same window's own already-known label_up outcome exactly
MODEL_CONFIDENCE_MIN, MIN_SECONDS_TO_CLOSE_FOR_ENTRY minutes later.

Two disclosed, real limitations (same "disclose, don't fake" convention as
perps_backtest.py's own sentiment_score=0.0 / fixed leverage-snapshot
disclosures):

1. **No historical Kalshi contract quote archive exists.** kalshi_15m.py
   has no candlestick endpoint for the standard (non-margin) market the
   way kalshi_perps.py's margin markets do -- a CLOSED market's own
   yes_bid/no_bid fields reflect their last value before settlement, not
   what they were ~5-10 minutes earlier when a real entry would fire, and
   there is no way to reconstruct that history for now-expired 15-minute
   windows. This backtest instead uses a single, disclosed
   `assumed_entry_price` (default 0.50 -- a neutral coin-flip price, not a
   number chosen to flatter results) for every simulated fill. Real
   markets for a genuinely uncertain 15-minute crypto-direction question
   often DO trade close to that range most of the window, but this is an
   approximation, not measured history -- `run_backtest`/
   `run_walkforward_backtest` accept `assumed_entry_price` so a sensitivity
   check across a few values (e.g. 0.45/0.50/0.55) is one parameter away.
   `directional_accuracy`/`calibration` in every report below need NO
   pricing assumption at all and are the most trustworthy numbers here --
   read those first.
2. **No Kalshi per-trade fee is modeled**, matching kalshi_15m_strategy.
   check_settlements's own current live behavor (it does not subtract a
   fee from realized_pnl_usd either -- a related, disclosed gap in the
   live code, not something invented here to look better). Real fills pay
   Kalshi's own "quadratic" fee schedule for this series (distinct from
   perps' linear one); real net returns will be lower than this reports.

Same "reuse the real decision functions, never reimplement the rules"
principle as perps_backtest.py: entry gating (MODEL_CONFIDENCE_MIN, side
selection) mirrors kalshi_15m_strategy.evaluate_candidate exactly, and
sizing mirrors kalshi_15m_strategy.scan_and_enter's own
`account_budget_usd * POSITION_SIZE_PCT / price` formula and
`check_settlements`'s own `count * (1 - price)` / `-count * price` P&L
formula, line for line.
"""
from __future__ import annotations

import logging
import math
from typing import Any

import numpy as np
import pandas as pd
from sklearn.metrics import accuracy_score, brier_score_loss, roc_auc_score

from data import kalshi_15m_strategy as strat
from data import walkforward
from data.kalshi_15m_data import load_training_dataset
from data.kalshi_15m_model import _CANDIDATES  # noqa: SLF001 -- reuse, don't fork a second copy that can drift
from data.perps_data import FEATURE_COLUMNS

logger = logging.getLogger(__name__)

# See module docstring point 1 -- one decision point per 15-minute window
# (the window's own opening-minute row), not a re-check every
# KALSHI_15M_CYCLE_MINUTES the way live scan_and_enter does. The window's
# own trailing technicals are already fully computed as of that first
# minute, and its label_up already IS the real, deterministic 15-minutes-
# later outcome -- re-checking mid-window would only ever change WHEN
# within the window an entry fires, never the eventual settlement, so it
# would add simulation complexity without changing what this backtest can
# actually measure (there's no historical quote to make "price got better/
# worse by minute 8" mean anything anyway -- see limitation 1 above).
WINDOW_SECONDS = 15 * 60


def _one_row_per_window(df: pd.DataFrame) -> pd.DataFrame:
    """Downsamples a per-minute engineered-feature frame to one row per
    (symbol, 15-minute-aligned window), keeping the EARLIEST row in each
    window (closest to "the window just opened", the earliest a real entry
    could ever fire)."""
    windowed = df.copy()
    windowed["_window_bucket"] = (windowed["ts"] // WINDOW_SECONDS) * WINDOW_SECONDS
    windowed = windowed.sort_values("ts")
    return (
        windowed.groupby(["symbol", "_window_bucket"], as_index=False)
        .first()
        .drop(columns=["_window_bucket"])
        .sort_values("ts")
        .reset_index(drop=True)
    )


def fit_backtest_model(train_df: pd.DataFrame, *, min_rows: int = 300) -> dict[str, Any] | None:
    """In-memory-only fit for the backtest -- never touches the live
    kalshi_15m_model.joblib file or pushes anything to Hugging Face. Same
    shape as perps_backtest.fit_backtest_model / kalshi_15m_model's own
    single-split candidate comparison. Returns None (no-model / all-skip
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
            auc = float(roc_auc_score(holdout["label_up"].values, proba))
            combined = (acc + auc) / 2.0
            if combined > best_score:
                best_name, best_model, best_score = name, model, combined
        except Exception as exc:
            logger.warning("[kalshi_15m_backtest] candidate %s failed to fit: %s", name, exc)
    if best_model is None:
        return None
    best_model.fit(labeled[feature_cols].values, labeled["label_up"].values)
    return {"model": best_model, "model_type": best_name, "feature_cols": feature_cols, "symbol_categories": symbol_categories}


def add_model_predictions(df: pd.DataFrame, fitted: dict[str, Any] | None) -> pd.DataFrame:
    """Batch-predict probability_up once, vectorized -- same reasoning as
    perps_backtest's own identical helper (a sweep/multi-fold run reuses
    the same rows many times with only thresholds changing)."""
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
    starting_balance: float = 100.0,
    assumed_entry_price: float = 0.50,
    position_size_pct: float | None = None,
    max_concurrent_positions: int | None = None,
    model_confidence_min: float | None = None,
) -> dict[str, Any]:
    """Walk forward through `test_df` (all coins, sorted by ts, already one
    row per 15-minute window -- see `_one_row_per_window`), replaying
    kalshi_15m_strategy's own real entry rule and check_settlements' own
    real P&L formula. See module docstring for the 2 disclosed pricing/fee
    limitations. Every strategy parameter can be overridden per-call, same
    "no env var needed for a sweep" convention as perps_backtest.simulate."""
    position_size_pct = strat.POSITION_SIZE_PCT if position_size_pct is None else position_size_pct
    max_concurrent_positions = strat.MAX_CONCURRENT_POSITIONS if max_concurrent_positions is None else max_concurrent_positions
    model_confidence_min = strat.MODEL_CONFIDENCE_MIN if model_confidence_min is None else model_confidence_min
    if not (0.0 < assumed_entry_price < 1.0):
        raise ValueError(f"assumed_entry_price must be strictly between 0 and 1, got {assumed_entry_price}")

    df = test_df.sort_values("ts").reset_index(drop=True)
    if "model_probability_up" not in df.columns:
        df = add_model_predictions(df, fitted)

    balance = starting_balance
    # coin -> {"close_ts", "side", "count", "entry_price", "label_up", "opened_ts"}
    open_positions: dict[str, dict[str, Any]] = {}
    trades: list[dict[str, Any]] = []
    calibration_rows: list[tuple[float, int]] = []  # (predicted probability_up, actual label_up) for every row a model existed on

    def _settle_due(as_of_ts: float) -> None:
        nonlocal balance
        for coin in [c for c, p in open_positions.items() if p["close_ts"] <= as_of_ts]:
            pos = open_positions.pop(coin)
            won = bool(pos["label_up"]) == (pos["side"] == "yes")
            gross = pos["count"] * (1.0 - pos["entry_price"]) if won else -pos["count"] * pos["entry_price"]
            realized = round(gross, 6)
            balance += realized
            trades.append({
                "coin": coin, "side": pos["side"], "count": pos["count"], "entry_price": pos["entry_price"],
                "won": won, "realized_pnl_usd": realized, "opened_ts": pos["opened_ts"], "closed_ts": pos["close_ts"],
                "confidence": pos["confidence"],
            })

    for row in df.itertuples(index=False):
        _settle_due(row.ts)

        proba_up = row.model_probability_up
        model_ok = proba_up == proba_up  # not NaN
        if model_ok:
            calibration_rows.append((float(proba_up), int(row.label_up)))

        coin = row.symbol
        if coin in open_positions or len(open_positions) >= max_concurrent_positions:
            continue
        if not model_ok:
            continue  # mirrors evaluate_candidate: no trade at all without a trained model (no technical-only fallback for this market)

        # Mirrors evaluate_candidate exactly: side + confidence, then the
        # confidence floor -- never a separate long/short-specific
        # threshold, since "yes"/"no" are just the two sides of one
        # binary contract, not a leverage direction choice.
        if proba_up >= 0.5:
            side, confidence = "yes", float(proba_up)
        else:
            side, confidence = "no", 1.0 - float(proba_up)
        if confidence < model_confidence_min:
            continue

        committed = sum(p["count"] * p["entry_price"] for p in open_positions.values())
        available = balance - committed
        budget = available * position_size_pct
        count = max(1, int(budget / assumed_entry_price)) if budget > 0 else 0
        cost = round(count * assumed_entry_price, 6)
        if count < 1 or cost > available:
            continue

        open_positions[coin] = {
            "close_ts": row.ts + WINDOW_SECONDS, "side": side, "count": float(count),
            "entry_price": assumed_entry_price, "label_up": int(row.label_up), "opened_ts": row.ts,
            "confidence": confidence,
        }

    _settle_due(float("inf"))  # resolve every remaining position at its own already-known outcome (no "mark to market" concept for a binary contract)

    total_pnl = sum(t["realized_pnl_usd"] for t in trades)
    wins = [t for t in trades if t["won"]]
    span_days = max(1e-9, (df["ts"].max() - df["ts"].min()) / 86400.0) if not df.empty else 1.0

    # Directional accuracy/calibration need NO pricing assumption at all --
    # see module docstring on why these are the most trustworthy numbers
    # here, computed over every row a model existed for, not just the ones
    # confidence-gated into an actual simulated trade.
    directional_accuracy = None
    brier_score = None
    auc = None
    if calibration_rows:
        probas = np.array([r[0] for r in calibration_rows])
        actuals = np.array([r[1] for r in calibration_rows])
        directional_accuracy = float(accuracy_score(actuals, (probas >= 0.5).astype(int)))
        brier_score = float(brier_score_loss(actuals, probas))
        if len(set(actuals.tolist())) > 1:
            auc = float(roc_auc_score(actuals, probas))

    return {
        "starting_balance": starting_balance,
        "ending_balance_realized": round(starting_balance + total_pnl, 6),
        "total_realized_pnl_usd": round(total_pnl, 6),
        "return_pct": round(total_pnl / starting_balance, 6) if starting_balance else 0.0,
        "trade_count": len(trades),
        "win_count": len(wins),
        "win_rate": round(len(wins) / len(trades), 4) if trades else 0.0,
        "trades_per_day": round(len(trades) / span_days, 3),
        "span_days": round(span_days, 2),
        "assumed_entry_price": assumed_entry_price,
        "directional_accuracy": directional_accuracy,
        "auc": auc,
        "brier_score": brier_score,
        "rows_with_model": len(calibration_rows),
        "trades": trades,
    }


def run_backtest(
    *, days: int | None = None, train_frac: float = 0.7, starting_balance: float = 100.0,
    coins: list[str] | None = None, **strategy_overrides: Any,
) -> dict[str, Any]:
    """End-to-end single 70/30 split over the real, already-archived HF
    dataset (kalshi_15m_data.load_training_dataset -- no live API calls,
    unlike perps_backtest.run_backtest, since the archive already exists
    here). `days` trims to the most recent N days of archive if given;
    None uses everything available."""
    combined = load_training_dataset()
    if combined.empty:
        return {"ok": False, "reason": "no_data"}
    if coins:
        combined = combined[combined["symbol"].isin(coins)]
    if days:
        cutoff = combined["ts"].max() - days * 86400
        combined = combined[combined["ts"] >= cutoff]
    combined = _one_row_per_window(combined)
    if combined.empty:
        return {"ok": False, "reason": "no_data"}

    cutoff_ts = combined["ts"].quantile(train_frac)
    train_df = combined[combined["ts"] < cutoff_ts]
    test_df = combined[combined["ts"] >= cutoff_ts]
    if test_df.empty:
        return {"ok": False, "reason": "no_test_rows"}

    fitted = fit_backtest_model(train_df)
    result = simulate(test_df, fitted, starting_balance=starting_balance, **strategy_overrides)
    result["ok"] = True
    result["model_used"] = fitted["model_type"] if fitted else None
    result["coins"] = sorted(combined["symbol"].unique().tolist())
    result["train_rows"] = len(train_df)
    result["test_rows"] = len(test_df)
    result["cutoff_ts"] = float(cutoff_ts)
    return result


def run_walkforward_backtest(
    *, days: int | None = None, fold_bounds: list[tuple[float, float, float]] | None = None,
    starting_balance: float = 100.0, coins: list[str] | None = None, **strategy_overrides: Any,
) -> dict[str, Any]:
    """Same data source as run_backtest (the real HF archive), but replays
    MULTIPLE expanding-window train/test folds via walkforward.py -- same
    "a strategy that only looks good on one lucky split hasn't actually
    learned anything durable" reasoning as perps_backtest's own
    run_walkforward_backtest. Returns {"ok", "folds": [...], "fold_count",
    "profitable_fold_ratio", "mean_return_pct", "std_return_pct", ...} --
    see walkforward.summarize_folds for the full cross-fold report."""
    combined = load_training_dataset()
    if combined.empty:
        return {"ok": False, "reason": "no_data"}
    if coins:
        combined = combined[combined["symbol"].isin(coins)]
    if days:
        cutoff = combined["ts"].max() - days * 86400
        combined = combined[combined["ts"] >= cutoff]
    combined = _one_row_per_window(combined)
    if combined.empty:
        return {"ok": False, "reason": "no_data"}

    result = walkforward.run_walkforward_folds(
        combined, fit_fn=fit_backtest_model, simulate_fn=simulate, fold_bounds=fold_bounds,
        simulate_kwargs={"starting_balance": starting_balance, **strategy_overrides},
    )
    result["coins"] = sorted(combined["symbol"].unique().tolist())
    return result
