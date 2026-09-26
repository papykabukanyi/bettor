"""scripts/strategy_sweep_job.py -- the standalone, Flask-free script meant
to run as a Hugging Face Job instead of a background thread on the live,
shared, multi-market trading Space. Verifies the per-market wiring
(MARKET_CONFIGS' own grid/combined/extra-kwargs/repo entries actually
resolve, run_market_sweep drives strategy_sweep.run_parameter_sweep
correctly, publishing goes through server_common.push_json_to_hf with the
right repo/filename) and the script's own CLI dispatch -- not
strategy_sweep.py's own sweep/ranking logic, which test_strategy_sweep.py
already covers directly."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import strategy_sweep_job as job  # noqa: E402


def test_market_configs_cover_all_six_bots():
    assert set(job.MARKET_CONFIGS.keys()) == {
        "kalshi_15m", "kalshi_15m_metals", "stocks", "crypto", "options", "perps",
    }


@pytest.mark.parametrize("market", sorted(job.MARKET_CONFIGS.keys()))
def test_every_market_grid_and_backtest_module_resolve(market):
    """Each market's own grid_fn must produce a non-empty grid, and its
    backtest_module/repo_module import path must actually import --
    catches a typo'd module path or grid_fn crash for any of the 6
    without needing a real data fetch."""
    import importlib

    cfg = job.MARKET_CONFIGS[market]
    grid = cfg["grid_fn"]()
    assert grid and all(isinstance(v, list) and v for v in grid.values())
    importlib.import_module(cfg["backtest_module"])
    repo_module = importlib.import_module(cfg["repo_module"])
    assert hasattr(repo_module, cfg["repo_attr"])


def test_dry_run_reports_combination_count_without_loading_data(monkeypatch):
    def fail_if_called(days):
        raise AssertionError("dry-run must never build the combined dataset")

    monkeypatch.setitem(job.MARKET_CONFIGS["kalshi_15m_metals"], "combined_fn", fail_if_called)
    result = job.run_market_sweep(
        "kalshi_15m_metals", n_workers=1, max_seconds=10.0, max_combinations=10,
        days=None, param_grid={"model_confidence_min": [0.55, 0.60]}, dry_run=True,
    )
    assert result == {
        "ok": True, "dry_run": True, "market": "kalshi_15m_metals",
        "combinations": 2, "grid": {"model_confidence_min": [0.55, 0.60]},
    }


def test_run_market_sweep_reports_no_data_when_combined_is_empty(monkeypatch):
    monkeypatch.setitem(job.MARKET_CONFIGS["kalshi_15m_metals"], "combined_fn", lambda days: pd.DataFrame())
    result = job.run_market_sweep(
        "kalshi_15m_metals", n_workers=1, max_seconds=10.0, max_combinations=10,
        days=None, param_grid={"model_confidence_min": [0.55]}, dry_run=False,
    )
    assert result == {"ok": False, "reason": "no_data", "market": "kalshi_15m_metals"}


def test_run_market_sweep_calls_run_parameter_sweep_with_combined_and_publishes(monkeypatch):
    from data import strategy_sweep

    fake_combined = pd.DataFrame({"ts": [1, 2, 3], "symbol": ["GOLD"] * 3})
    monkeypatch.setitem(job.MARKET_CONFIGS["kalshi_15m_metals"], "combined_fn", lambda days: fake_combined)

    captured = {}

    def fake_sweep(backtest_module, grid, *, combined, **kw):
        captured["combined"] = combined
        captured["kw"] = kw
        return {"ok": True, "combinations_with_evidence": 3, "top_strategies": []}

    monkeypatch.setattr(strategy_sweep, "run_parameter_sweep", fake_sweep)

    published = {}
    monkeypatch.setattr(job, "_publish_result", lambda market, result: published.update(market=market, result=result))

    result = job.run_market_sweep(
        "kalshi_15m_metals", n_workers=4, max_seconds=99.0, max_combinations=10,
        days=None, param_grid={"model_confidence_min": [0.55]}, dry_run=False,
    )

    assert result["ok"] is True
    assert result["market"] == "kalshi_15m_metals"
    assert captured["combined"] is fake_combined
    assert captured["kw"]["n_workers"] == 4
    assert captured["kw"]["max_seconds"] == 99.0
    assert captured["kw"]["fold_bounds"] == strategy_sweep.DEFAULT_FOLD_BOUNDS_WITH_HOLDOUT
    assert captured["kw"]["holdout_bounds"] == strategy_sweep.DEFAULT_HOLDOUT_BOUNDS
    assert published["market"] == "kalshi_15m_metals"


def test_run_market_sweep_never_publishes_a_failed_result(monkeypatch):
    from data import strategy_sweep

    monkeypatch.setitem(job.MARKET_CONFIGS["kalshi_15m_metals"], "combined_fn", lambda days: pd.DataFrame({"ts": [1], "symbol": ["GOLD"]}))
    monkeypatch.setattr(strategy_sweep, "run_parameter_sweep", lambda *a, **kw: {"ok": False, "reason": "no_qualifying_folds"})

    def fail_if_called(market, result):
        raise AssertionError("must not publish a failed sweep result")

    monkeypatch.setattr(job, "_publish_result", fail_if_called)
    result = job.run_market_sweep(
        "kalshi_15m_metals", n_workers=1, max_seconds=10.0, max_combinations=10,
        days=None, param_grid={"model_confidence_min": [0.55]}, dry_run=False,
    )
    assert result["ok"] is False


# ---------------------------------------------------------------------------
# _one_row_per_window -- real bug found and fixed: passing combined=
# explicitly (needed so this script's own days-filtering happens exactly
# once) bypasses run_parameter_sweep's own auto-load path, which is the
# ONLY place that normally calls _one_row_per_window. Without calling it
# here too, a real sweep replays multiple rows per 15-minute window --
# more decision points than the live strategy would ever separately act
# on -- inflating trade_count and skewing every downstream number.
# ---------------------------------------------------------------------------
def test_kalshi_15m_combined_dedupes_to_one_row_per_window(monkeypatch):
    from data import kalshi_15m_backtest, kalshi_15m_data

    raw = pd.DataFrame({"ts": [0, 0, 900, 900], "symbol": ["BTC"] * 4})
    monkeypatch.setattr(kalshi_15m_data, "load_training_dataset", lambda: raw)
    deduped = pd.DataFrame({"ts": [0, 900], "symbol": ["BTC", "BTC"]})
    captured = {}

    def fake_dedupe(df):
        captured["input_rows"] = len(df)
        return deduped

    monkeypatch.setattr(kalshi_15m_backtest, "_one_row_per_window", fake_dedupe)
    result = job._kalshi_15m_combined(None)  # noqa: SLF001
    assert captured["input_rows"] == 4
    assert result is deduped


def test_kalshi_15m_metals_combined_dedupes_to_one_row_per_window(monkeypatch):
    from data import kalshi_15m_metals_backtest, kalshi_15m_metals_data

    raw = pd.DataFrame({"ts": [0, 0, 900, 900], "symbol": ["GOLD"] * 4})
    monkeypatch.setattr(kalshi_15m_metals_data, "load_training_dataset", lambda: raw)
    deduped = pd.DataFrame({"ts": [0, 900], "symbol": ["GOLD", "GOLD"]})
    captured = {}

    def fake_dedupe(df):
        captured["input_rows"] = len(df)
        return deduped

    monkeypatch.setattr(kalshi_15m_metals_backtest, "_one_row_per_window", fake_dedupe)
    result = job._kalshi_15m_metals_combined(None)  # noqa: SLF001
    assert captured["input_rows"] == 4
    assert result is deduped


def test_perps_extra_kwargs_computed_from_the_combined_frame(monkeypatch):
    from data import perps_backtest

    combined = pd.DataFrame({"ticker": ["BTC-PERP", "ETH-PERP", "BTC-PERP"]})
    monkeypatch.setattr(perps_backtest, "fetch_leverage_by_ticker", lambda tickers: {t: 5.0 for t in tickers})

    result = job._perps_extra_kwargs(combined)  # noqa: SLF001

    assert result == {"leverage_by_ticker": {"BTC-PERP": 5.0, "ETH-PERP": 5.0}}


def test_publish_result_skips_when_hf_api_key_is_not_set(monkeypatch):
    monkeypatch.delenv("HF_API_KEY", raising=False)

    def fail_if_called(*a, **kw):
        raise AssertionError("must not attempt an HF push with no token")

    monkeypatch.setattr("server_common.push_json_to_hf", fail_if_called)
    job._publish_result("kalshi_15m_metals", {"ok": True, "combinations_with_evidence": 1})  # noqa: SLF001


def test_publish_result_pushes_to_that_markets_own_repo(monkeypatch):
    from data import kalshi_15m_metals_model

    monkeypatch.setenv("HF_API_KEY", "fake-token")
    captured = {}

    def fake_push(repo_id, filename, data, *, token, timeout_sec, commit_message):
        captured.update(repo_id=repo_id, filename=filename, data=data, token=token)

    monkeypatch.setattr("server_common.push_json_to_hf", fake_push)
    job._publish_result("kalshi_15m_metals", {"ok": True, "combinations_with_evidence": 7})  # noqa: SLF001

    assert captured["repo_id"] == kalshi_15m_metals_model.HF_KALSHI_15M_METALS_MODEL_REPO
    assert captured["filename"] == "strategy_sweep_kalshi_15m_metals.json"
    assert captured["data"]["combinations_with_evidence"] == 7
    assert captured["token"] == "fake-token"


def test_main_dispatches_a_dry_run(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["strategy_sweep_job.py", "--market", "perps", "--dry-run"])
    monkeypatch.setattr(job, "_load_dotenv", lambda: None)

    exit_code = job.main()

    assert exit_code == 0
    printed = capsys.readouterr().out
    assert '"dry_run": true' in printed


def test_main_returns_a_nonzero_exit_code_on_an_unexpected_exception(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["strategy_sweep_job.py", "--market", "kalshi_15m_metals"])
    monkeypatch.setattr(job, "_load_dotenv", lambda: None)

    def raise_error(*a, **kw):
        raise RuntimeError("simulated crash")

    monkeypatch.setattr(job, "run_market_sweep", raise_error)
    assert job.main() == 1


def test_main_passes_a_json_param_grid_override_through(monkeypatch):
    monkeypatch.setattr(
        sys, "argv",
        ["strategy_sweep_job.py", "--market", "perps", "--dry-run", "--param-grid", '{"model_confidence_min": [0.6]}'],
    )
    monkeypatch.setattr(job, "_load_dotenv", lambda: None)
    captured = {}

    def fake_run(market, **kw):
        captured.update(kw)
        return {"ok": True}

    monkeypatch.setattr(job, "run_market_sweep", fake_run)
    assert job.main() == 0
    assert captured["param_grid"] == {"model_confidence_min": [0.6]}
