"""scripts/training_job.py -- the standalone, Flask-free script meant to
run as a Hugging Face Jobs scheduled run instead of a job registered on a
Render service's own in-process APScheduler. Each job function is a
near-verbatim copy of the corresponding `_run_*_train`/`_run_*_backtest_
sweep`/`_run_*_torch_train` body already covered by test_app_kalshi_jobs.py
and friends -- these tests verify the SAME behavior survived the copy
(right trade_log wiring, right data.* functions called), plus the
script's own CLI dispatch/exit-code logic that has no equivalent in the
Flask-hosted originals."""
from __future__ import annotations

import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

import training_job as job  # noqa: E402


def test_perps_train_passes_the_trade_log_through(monkeypatch):
    from data import perps_model, perps_strategy

    monkeypatch.setattr(perps_strategy, "_load_state", lambda: {"trade_log": [{"realized_pnl_usd": 1.0}]})
    captured = {}
    monkeypatch.setattr(perps_model, "train_model", lambda trade_log=None: captured.update(trade_log=trade_log) or {"ok": True, "rows": 500})

    result = job._perps_train()  # noqa: SLF001

    assert result == {"ok": True, "rows": 500}
    assert captured["trade_log"] == [{"realized_pnl_usd": 1.0}]


def test_perps_train_still_trains_when_the_trade_log_read_fails(monkeypatch):
    """Real production intent (mirrors _run_perps_train's own try/except):
    a failure to read trade_log for outcome-aware sample weighting must
    not block training outright -- it should fall back to trade_log=None
    (train_model's own documented default: no outcome weighting, not "no
    training at all")."""
    from data import perps_model, perps_strategy

    def raise_error():
        raise RuntimeError("state file corrupted")

    monkeypatch.setattr(perps_strategy, "_load_state", raise_error)
    captured = {}
    monkeypatch.setattr(perps_model, "train_model", lambda trade_log=None: captured.update(trade_log=trade_log) or {"ok": True, "rows": 500})

    result = job._perps_train()  # noqa: SLF001

    assert result == {"ok": True, "rows": 500}
    assert captured["trade_log"] is None


def test_stocks_train_calls_train_model(monkeypatch):
    from data import alpaca_model

    monkeypatch.setattr(alpaca_model, "train_model", lambda: {"ok": True, "rows": 500})
    assert job._stocks_train() == {"ok": True, "rows": 500}  # noqa: SLF001


def test_crypto_train_calls_train_model(monkeypatch):
    from data import alpaca_crypto_model

    monkeypatch.setattr(alpaca_crypto_model, "train_model", lambda: {"ok": True, "rows": 500})
    assert job._crypto_train() == {"ok": True, "rows": 500}  # noqa: SLF001


def test_options_train_calls_train_model_off_hours(monkeypatch):
    from data import alpaca_data, alpaca_options_model

    monkeypatch.setattr(alpaca_data, "get_market_session", lambda: {"session": "closed"})
    monkeypatch.setattr(alpaca_options_model, "train_model", lambda: {"ok": True, "rows": 500})
    assert job._options_train() == {"ok": True, "rows": 500}  # noqa: SLF001


def test_options_train_skips_during_regular_hours(monkeypatch):
    """Real behavior this must preserve from _run_alpaca_options_train: a
    multi-minute retrain has no business competing with live entry-scan/
    fast-check for CPU/memory while real option orders may be in flight."""
    from data import alpaca_data, alpaca_options_model

    monkeypatch.setattr(alpaca_data, "get_market_session", lambda: {"session": "regular"})

    def fail_if_called():
        raise AssertionError("must not train during regular trading hours")

    monkeypatch.setattr(alpaca_options_model, "train_model", fail_if_called)
    assert job._options_train() == {"ok": True, "skipped": "regular_hours"}  # noqa: SLF001


def test_jobs_table_covers_exactly_the_train_jobs_implemented_so_far():
    assert set(job._JOBS.keys()) == {  # noqa: SLF001
        ("train", "perps"), ("train", "stocks"), ("train", "crypto"), ("train", "options"),
    }


def test_main_dispatches_to_the_right_job_function(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["training_job.py", "--job", "train", "--market", "perps"])
    monkeypatch.setattr(job, "_load_dotenv", lambda: None)
    monkeypatch.setitem(job._JOBS, ("train", "perps"), lambda: {"ok": True, "rows": 500})  # noqa: SLF001

    exit_code = job.main()

    assert exit_code == 0
    printed = capsys.readouterr().out
    assert '"rows": 500' in printed


def test_main_returns_a_nonzero_exit_code_when_the_job_reports_not_ok(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["training_job.py", "--job", "train", "--market", "perps"])
    monkeypatch.setattr(job, "_load_dotenv", lambda: None)
    monkeypatch.setitem(job._JOBS, ("train", "perps"), lambda: {"ok": False, "error": "boom"})  # noqa: SLF001

    assert job.main() == 1


def test_main_returns_a_nonzero_exit_code_when_the_job_raises_unexpectedly(monkeypatch):
    monkeypatch.setattr(sys, "argv", ["training_job.py", "--job", "train", "--market", "perps"])
    monkeypatch.setattr(job, "_load_dotenv", lambda: None)

    def raise_error():
        raise RuntimeError("unexpected failure")

    monkeypatch.setitem(job._JOBS, ("train", "perps"), raise_error)  # noqa: SLF001
    assert job.main() == 1


def test_main_rejects_a_job_market_combination_not_implemented_yet(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["training_job.py", "--job", "backtest-sweep", "--market", "crypto"])

    assert job.main() == 1
    printed = capsys.readouterr().out
    assert "no such job/market combination implemented yet" in printed
