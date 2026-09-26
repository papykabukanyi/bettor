"""Walk-forward backtest of the real Kalshi 15-minute GOLD/SILVER/COPPER/
PLATINUM/PALLADIUM strategy code (kalshi_15m_strategy.evaluate_candidate's
own decision rule), metals only -- the direct counterpart to
kalshi_15m_backtest.py's own crypto-only module.

REAL GAP THIS CLOSES: kalshi_15m_backtest.py's own module docstring
explicitly says "Metals are NOT covered here -- there is no historical
price archive for gold/silver/copper at all (gold-api.com has no free
/history endpoint)". That was true the day this market first went live,
but is no longer true today: kalshi_15m_metals_data.py's own data-collect
job has been running for weeks, continuously appending real price points
via _append_price_point and archiving them to HF
(kalshi_15m_metals_data.load_training_dataset) -- exactly the same
"accumulate our own history since no bulk API exists" pattern this
account's own real GOLD/SILVER/COPPER trade_log already relies on. Per
explicit user direction ("we need to work on over 10000 mix of
strategies in the backtest and... perform a forward test with real data
and a huge historical data of the main 3 we will trade"): GOLD/SILVER/
COPPER ARE now this account's entire live entry universe
(kalshi_15m_strategy.ACTIVE_ENTRY_COINS) -- a backtest that only ever
covered crypto was backtesting a market this account no longer trades at
all. This module, not kalshi_15m_backtest.py, is now the one that
actually matters for this account's own live risk.

Same "reuse the real decision functions, never reimplement the rules"
principle as kalshi_15m_backtest.py's own -- entry gating
(MODEL_CONFIDENCE_MIN, YES_CONFIDENCE_EXTRA_REQUIRED, side selection)
mirrors kalshi_15m_strategy.evaluate_candidate exactly, and sizing/P&L
formulas mirror scan_and_enter's/check_settlements' own line for line.

Same two disclosed, real limitations as kalshi_15m_backtest.py (see its
own module docstring for the full reasoning): no historical Kalshi
CONTRACT quote archive exists (a fixed, disclosed `assumed_entry_price`
stands in for one), and no Kalshi per-trade fee is modeled here either
(see kalshi_15m_trade_analysis.estimate_kalshi_15m_entry_fee_usd for
this account's own SEPARATE, already-shipped real-fee estimator -- not
wired into this simulation loop, a real, disclosed gap this module
inherits rather than silently fixing as a side effect).

THREE MORE gates this account's live strategy has grown since
kalshi_15m_backtest.py was first built are DELIBERATELY NOT replayed
here either, disclosed rather than faked: volume_and_price_action_confirmed's
own metals counterpart (metals_volume_proxy_confirmed) needs a LIVE
cross-asset correlation study snapshot (crypto_correlation.get_metals_study/
get_latest_kalshi_15m_crypto_df) that was never archived historically --
there is no way to reconstruct "what would the correlation study have
read at that exact past moment" from today's dataset alone. The
correlation-study confidence nudge (USE_CORRELATION_STUDY) has the same
problem. Per-coin/hour trust gates (coin_is_trusted/hour_is_trusted) are
inherently ACCOUNT-STATE-dependent (they read this account's own
trade_log as of decision time), not a property of one row's own
features, so replaying them faithfully would require simulating the
ENTIRE account's own trade history move-by-move, not just one coin's
technical entry rule -- a materially bigger, different kind of backtest
than this one. `directional_accuracy`/`calibration` (needing no pricing
assumption or any of the above) remain the most trustworthy numbers
here, exactly as kalshi_15m_backtest.py's own docstring already advises.
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
from data.kalshi_15m_metals_data import METALS_FEATURE_COLUMNS as FEATURE_COLUMNS
from data.kalshi_15m_metals_data import load_training_dataset
from data.kalshi_15m_metals_model import _CANDIDATES  # noqa: SLF001 -- reuse, don't fork a second copy that can drift

logger = logging.getLogger(__name__)

# Same reasoning as kalshi_15m_backtest.WINDOW_SECONDS -- one decision
# point per 15-minute window, no historical intra-window quote to make a
# finer-grained replay mean anything.
WINDOW_SECONDS = 15 * 60


def _one_row_per_window(df: pd.DataFrame) -> pd.DataFrame:
    """Identical logic to kalshi_15m_backtest._one_row_per_window -- kept
    as this module's own copy rather than a cross-import, matching this
    codebase's own established "independent per-market module" convention
    (see kalshi_15m_data.py's own module docstring)."""
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
    """In-memory-only fit -- never touches the live kalshi_15m_metals_model.joblib
    file or pushes anything to Hugging Face. Identical shape to
    kalshi_15m_backtest.fit_backtest_model, metals' own candidates/features."""
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
            logger.warning("[kalshi_15m_metals_backtest] candidate %s failed to fit: %s", name, exc)
    if best_model is None:
        return None
    best_model.fit(labeled[feature_cols].values, labeled["label_up"].values)
    return {"model": best_model, "model_type": best_name, "feature_cols": feature_cols, "symbol_categories": symbol_categories}


def add_model_predictions(df: pd.DataFrame, fitted: dict[str, Any] | None) -> pd.DataFrame:
    """Batch-predict probability_up once, vectorized -- see
    kalshi_15m_backtest.add_model_predictions's own comment on why this
    matters for a sweep (fit once per fold, re-used across every
    threshold/gating combination in that fold)."""
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
    yes_confidence_extra_required: float | None = None,
) -> dict[str, Any]:
    """Walk forward through `test_df` (all metals, sorted by ts, already
    one row per 15-minute window), replaying kalshi_15m_strategy's own
    real entry rule and check_settlements' own real P&L formula. See
    module docstring for the disclosed pricing/fee/cross-asset-gate
    limitations. `yes_confidence_extra_required` (new vs.
    kalshi_15m_backtest.simulate) replays YES_CONFIDENCE_EXTRA_REQUIRED's
    own real, live per-side floor -- omitted from the ORIGINAL crypto
    backtest simply because it didn't exist yet when that module was
    first built. Every strategy parameter can be overridden per-call,
    same "no env var needed for a sweep" convention as kalshi_15m_backtest.simulate."""
    position_size_pct = strat.POSITION_SIZE_PCT if position_size_pct is None else position_size_pct
    max_concurrent_positions = strat.MAX_CONCURRENT_POSITIONS if max_concurrent_positions is None else max_concurrent_positions
    model_confidence_min = strat.MODEL_CONFIDENCE_MIN if model_confidence_min is None else model_confidence_min
    yes_confidence_extra_required = (
        strat.YES_CONFIDENCE_EXTRA_REQUIRED if yes_confidence_extra_required is None else yes_confidence_extra_required
    )
    if not (0.0 < assumed_entry_price < 1.0):
        raise ValueError(f"assumed_entry_price must be strictly between 0 and 1, got {assumed_entry_price}")

    # Same real archive-boundary edge as kalshi_15m_backtest.simulate's
    # own identical comment -- the most recent rows have no label_up yet.
    df = test_df.dropna(subset=["label_up"]).sort_values("ts").reset_index(drop=True)
    if "model_probability_up" not in df.columns:
        df = add_model_predictions(df, fitted)

    balance = starting_balance
    open_positions: dict[str, dict[str, Any]] = {}
    trades: list[dict[str, Any]] = []
    calibration_rows: list[tuple[float, int]] = []

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
            continue

        # Mirrors evaluate_candidate exactly: side + confidence, then
        # YES_CONFIDENCE_EXTRA_REQUIRED's own per-side floor bump, then
        # the (possibly bumped) confidence floor -- see this account's
        # own real 319-trade finding ("no" wins 45.6% vs "yes" 32.8%)
        # that motivated the live per-side adjustment in the first place.
        if proba_up >= 0.5:
            side, confidence = "yes", float(proba_up)
        else:
            side, confidence = "no", 1.0 - float(proba_up)
        effective_floor = model_confidence_min + (yes_confidence_extra_required if side == "yes" else 0.0)
        if confidence < effective_floor:
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

    _settle_due(float("inf"))

    total_pnl = sum(t["realized_pnl_usd"] for t in trades)
    wins = [t for t in trades if t["won"]]
    span_days = max(1e-9, (df["ts"].max() - df["ts"].min()) / 86400.0) if not df.empty else 1.0

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
        "model_confidence_min": model_confidence_min,
        "yes_confidence_extra_required": yes_confidence_extra_required,
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
    metals dataset (kalshi_15m_metals_data.load_training_dataset). See
    kalshi_15m_backtest.run_backtest's own docstring for the identical
    shape/reasoning."""
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
    """Same data source as run_backtest (the real HF metals archive), but
    replays MULTIPLE expanding-window train/test folds via walkforward.py
    -- see kalshi_15m_backtest.run_walkforward_backtest's own docstring
    for the identical shape/reasoning (this IS the "forward test" per
    explicit user direction: each fold's own test window is strictly
    later in time than its own train window, and multiple folds check
    this holds up across different real market stretches, not just one
    lucky split)."""
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
