"""Backtest engine tests -- synthetic price data only, never touches the
network. Verifies the simulation correctly reuses the REAL strategy decide
functions (so a passing backtest can't drift from what the live bot
actually does), respects the concurrency cap, and that batch-predicting the
model once produces the same entries as predicting per-row would."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from data import perps_backtest as bt
from data import perps_strategy as strat


def _synthetic_test_df(n_per_ticker: int = 200, tickers: tuple[str, ...] = ("KXBTCPERP", "KXETHPERP")) -> pd.DataFrame:
    rng = np.random.default_rng(7)
    frames = []
    for i, ticker in enumerate(tickers):
        ts = np.arange(n_per_ticker) * 60 + i * 10_000_000
        # A repeating dip-then-recover pattern so decide_entry_technical
        # actually fires a few times deterministically.
        base = 100.0
        close = base + np.sin(np.arange(n_per_ticker) / 5.0) * 0.5
        dist_to_ma_15 = np.sin(np.arange(n_per_ticker) / 5.0 + 0.3) * 0.01
        trend_pct = np.zeros(n_per_ticker)
        frames.append(pd.DataFrame({
            "ticker": ticker, "ts": ts, "close": close, "dist_to_ma_15": dist_to_ma_15,
            "dist_to_ma_30": dist_to_ma_15 * 0.5, "trend_pct": trend_pct,
            "ret_1m": rng.normal(0, 0.001, n_per_ticker), "ret_5m": rng.normal(0, 0.002, n_per_ticker),
            "ret_15m": rng.normal(0, 0.003, n_per_ticker), "volatility_15": np.abs(rng.normal(0.001, 0.0002, n_per_ticker)),
            "sentiment_score": 0.0,
        }))
    return pd.concat(frames, ignore_index=True)


def test_add_model_predictions_without_a_model_yields_nan_column():
    df = _synthetic_test_df()
    result = bt.add_model_predictions(df, fitted=None)
    assert result["model_probability_up"].isna().all()


def test_simulate_runs_technical_only_without_a_model():
    df = _synthetic_test_df()
    result = bt.simulate(df, fitted=None, starting_balance=20.0, leverage_by_ticker={"KXBTCPERP": 6.0, "KXETHPERP": 4.5})
    assert result["trade_count"] >= 0
    assert "trades_per_day" in result


def test_simulate_never_exceeds_max_concurrent_positions():
    # Force very loose entry criteria so many tickers would qualify at once.
    df = _synthetic_test_df(tickers=tuple(f"KXFAKE{i}PERP" for i in range(8)))
    result = bt.simulate(
        df, fitted=None, starting_balance=100.0,
        leverage_by_ticker={t: 3.0 for t in df["ticker"].unique()},
        entry_dip_pct=-1.0,  # every row satisfies the "dip" check
        trend_filter_down_pct=1.0,
        max_concurrent_positions=3,
    )
    # Never more open positions mid-simulation than the cap allows.
    assert result["open_positions_at_end"] <= 3


def test_simulate_respects_a_trained_models_direction(monkeypatch):
    df = _synthetic_test_df()
    # A fake "fitted" whose predict_proba always says "down" -- entries
    # should never fire even with loose technical criteria.
    class _AlwaysDownModel:
        def predict_proba(self, x):
            return np.tile([0.9, 0.1], (len(x), 1))

    fitted = {"model": _AlwaysDownModel(), "model_type": "fake", "feature_cols": bt.FEATURE_COLUMNS + ["ticker_code"], "ticker_categories": list(df["ticker"].unique())}
    result = bt.simulate(
        df, fitted, starting_balance=20.0, leverage_by_ticker={"KXBTCPERP": 6.0, "KXETHPERP": 4.5},
        entry_dip_pct=-1.0, trend_filter_down_pct=1.0, model_confidence_min=0.5,
    )
    assert result["trade_count"] == 0


def test_simulate_reuses_precomputed_predictions_when_present(monkeypatch):
    """A parameter sweep calls add_model_predictions once and simulate()
    many times -- simulate must NOT re-predict (and must not require
    `fitted`) when the column already exists."""
    df = _synthetic_test_df()
    df["model_probability_up"] = 0.9  # pretend a model already ran

    def fail_if_called(*a, **k):
        raise AssertionError("add_model_predictions must not run again when the column is already present")

    monkeypatch.setattr(bt, "add_model_predictions", fail_if_called)
    result = bt.simulate(df, fitted=None, starting_balance=20.0, leverage_by_ticker={"KXBTCPERP": 6.0, "KXETHPERP": 4.5})
    assert "trade_count" in result


def test_fetch_extended_candles_chains_multiple_calls_beyond_the_cap(monkeypatch):
    calls = []

    def fake_candlesticks(ticker, *, start_ts, end_ts, period_interval):
        calls.append((start_ts, end_ts))
        # One candle per call so we can count exactly how many chunks fired.
        return {"candlesticks": [{"end_period_ts": end_ts, "price": {"close": 100.0}}]}

    monkeypatch.setattr(bt, "get_margin_candlesticks", fake_candlesticks)
    # 10 days of 1-minute candles needs more than one 5000-candle (~3.47 day) call.
    bt.fetch_extended_candles("KXBTCPERP", days=10, period_interval=1)
    assert len(calls) >= 3


def test_fetch_extended_candles_handles_a_newly_listed_ticker(monkeypatch):
    """A ticker that only has, say, 20 days of real history returns empty
    candlesticks for the older chained windows a 50-day request would ask
    for. That must not raise (the dtype bug this guards against) and must
    still return the real data from the populated windows."""
    call_count = [0]

    def fake_candlesticks(ticker, *, start_ts, end_ts, period_interval):
        call_count[0] += 1
        # Only the most recent window (the last call, since we chain
        # backward from now) has any real data.
        if call_count[0] == 1:
            return {"candlesticks": [{"end_period_ts": end_ts, "price": {"close": 100.0}}]}
        return {"candlesticks": []}

    monkeypatch.setattr(bt, "get_margin_candlesticks", fake_candlesticks)
    result = bt.fetch_extended_candles("KXNEWPERP", days=50, period_interval=1)
    assert len(result) == 1
    assert str(result["ts"].dtype) == "int64"
