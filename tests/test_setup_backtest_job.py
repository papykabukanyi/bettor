"""Each bot's daily real-data replay on the Space (setup_backtest_job)."""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from data import setup_backtest_job as job

FIXTURE = Path(__file__).parent / "fixtures" / "setup_btc_1m.parquet"


@pytest.fixture(autouse=True)
def _local_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(job, "LOCAL_DIR", tmp_path)
    monkeypatch.delenv("HF_API_KEY", raising=False)


def test_summary_reports_both_halves_and_significance():
    t = pd.DataFrame({"entry_ts": range(10), "net_return": [0.01, -0.005] * 5, "exit": ["target", "stop"] * 5,
                      "symbol": ["BTC", "ETH"] * 5})
    s = job.summarize(t, "net_return")
    assert s["trades"] == 10 and s["win_rate"] == 0.5 and s["profit_factor"] == 2.0
    assert s["first_half"]["trades"] + s["second_half"]["trades"] == 10
    assert s["exits"] == {"target": 5, "stop": 5} and s["by_symbol"]["BTC"]["trades"] == 5
    assert job.summarize(pd.DataFrame(), "net_return") == {"trades": 0, "unit": "return"}


def test_perps_replay_runs_with_and_without_the_correlation_rule(monkeypatch):
    from data import kalshi_15m_spot, perps_data, perps_strategy
    from data import kalshi_perps
    btc = pd.read_parquet(FIXTURE)
    spot = pd.concat([btc.assign(coin="BTC"), btc.assign(coin="ETH")], ignore_index=True)
    monkeypatch.setattr(kalshi_15m_spot, "load_spot_history", lambda days: spot)
    monkeypatch.setattr(perps_data, "get_watchlist", lambda: ["KXBTCPERP", "KXETHPERP"])
    monkeypatch.setattr(kalshi_perps, "get_margin_market", lambda t: (_ for _ in ()).throw(RuntimeError("offline")))
    monkeypatch.setattr(perps_strategy, "setup_fee_rate_roundtrip", lambda ticker: 0.001)
    result = job.run("perps", days=3, publish=False)
    assert result["ok"] is True and result["universe"] == ["BTC", "ETH"]
    assert result["costs"]["BTC"] == {"fee_rate_roundtrip": 0.001, "spread_bps": 5.0}
    assert {"with_correlation", "without_correlation"} <= set(result)
    assert json.loads((job.LOCAL_DIR / "perps.json").read_text())["bot"] == "perps"
    assert job.latest("perps")["computed_at"] == result["computed_at"]


def test_a_failed_replay_is_recorded_not_raised(monkeypatch):
    monkeypatch.setitem(job.RUNNERS, "stocks", lambda days: (_ for _ in ()).throw(RuntimeError("no bars")))
    result = job.run("stocks", days=3, publish=False)
    assert result["ok"] is False and "no bars" in result["error"]


def test_publishing_needs_an_hf_token():
    assert job.publish_to_hf("perps", {"computed_at": "2026-10-01T00:00:00+00:00"}) is False


def test_every_bot_publishes_its_own_strategy_card():
    for bot in job.RUNNERS:
        card = job.strategy_card(bot)
        assert card["module"].endswith(job.MODULES[bot]) and "MIN_RR" in card["params"] and "CORR_MIN" in card["params"]


def test_launch_runs_one_replay_at_a_time_per_bot(monkeypatch):
    launched = []

    class FakeProc:
        pid = 4242

    monkeypatch.setattr(job.subprocess, "Popen", lambda *a, **k: launched.append(a[0]) or FakeProc())
    first = job.launch("crypto")
    assert first["action"] == "launched" and launched[0][-3:] == ["-m", "data.setup_backtest_job", "crypto"]
    monkeypatch.setattr(job.os, "kill", lambda pid, sig: None)  # pid 4242 "alive"
    assert job.launch("crypto")["action"] == "already_running" and len(launched) == 1


def _bt(**over):
    base = {"ok": True, "computed_at": __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat(),
            "with_correlation": {"trades": 33, "avg": 0.158, "t_stat": 1.87}}
    return {**base, **over}


@pytest.mark.parametrize("bt,open_,reason", [
    (None, False, "no_replay_yet"),
    (_bt(ok=False), False, "last_replay_failed"),
    (_bt(with_correlation={"trades": 5, "avg": 0.2, "t_stat": 3.0}), False, "too_few_replay_trades"),
    (_bt(with_correlation={"trades": 101, "avg": -0.004, "t_stat": -3.1}), False, "replay_not_profitable"),
    (_bt(computed_at="2026-01-01T00:00:00+00:00"), False, "replay_stale"),
    (_bt(), True, "replay_profitable"),
])
def test_evidence_gate_opens_only_on_a_fresh_profitable_replay(monkeypatch, bt, open_, reason):
    monkeypatch.setattr(job, "latest", lambda bot: bt)
    monkeypatch.setattr(job, "EVIDENCE_GATE_BOTS", frozenset({"perps"}))
    g = job.evidence_gate("perps")
    assert g["open"] is open_ and g["reason"] == reason
    assert job.evidence_gate("stocks") == {"open": True, "gated": False, "reason": "not_gated"}


def test_no_bot_is_gated_by_default():
    assert job.EVIDENCE_GATE_BOTS == frozenset()


def _multiyear_trades():
    """Symbol A makes money every year, B loses every year, C is mixed."""
    rows = []
    for year in range(2016, 2026):
        ts = int(pd.Timestamp(f"{year}-06-01", tz="UTC").timestamp())
        for i in range(8):
            rows.append({"symbol": "A", "entry_ts": ts + i, "net_return": 0.004 if i % 4 else -0.002})
            rows.append({"symbol": "B", "entry_ts": ts + i, "net_return": -0.004 if i % 4 else 0.002})
            rows.append({"symbol": "C", "entry_ts": ts + i, "net_return": 0.003 if (year + i) % 2 else -0.003})
    return pd.DataFrame(rows)


def test_walk_forward_eligibility_scores_only_unseen_years_and_enforces_when_it_helps():
    wf = job.walk_forward_eligibility(_multiyear_trades(), min_trades=6, lookback_years=2)
    assert wf["test_years"] == 8 and wf["years"][0]["year"] == 2018
    assert "A" in wf["eligible_now"] and "B" not in wf["eligible_now"]
    assert wf["out_of_sample"]["avg"] > wf["every_symbol"]["avg"] and wf["enforce"] is True


def test_eligibility_stays_off_when_picking_does_not_beat_trading_everything():
    t = _multiyear_trades()
    t = t[t.symbol == "C"]  # past results say nothing about the next year
    wf = job.walk_forward_eligibility(t, min_trades=6, lookback_years=2)
    assert wf["enforce"] is False


def test_an_enforced_eligibility_list_skips_other_stocks(monkeypatch):
    from data import alpaca_setup, alpaca_strategy
    monkeypatch.setattr(job, "eligibility", lambda bot: {"enforce": True, "symbols": ["NVDA"], "rule": "r"})
    monkeypatch.setattr(alpaca_setup, "live_setup", lambda *a, **k: (_ for _ in ()).throw(AssertionError("not evaluated")))
    c = alpaca_strategy.evaluate_setup_candidate("COST")
    assert c["should_enter"] is False and c["setup_reason"] == "eligibility"
    monkeypatch.setattr(job, "eligibility", lambda bot: {"enforce": False, "symbols": ["NVDA"], "rule": "r"})
    monkeypatch.setattr(alpaca_setup, "live_setup", lambda *a, **k: {"valid": False, "reason": "trend", "checks": {}})
    assert alpaca_strategy.evaluate_setup_candidate("COST")["setup_reason"] == "trend"


def test_a_study_starts_only_on_a_complete_archive_and_one_at_a_time(monkeypatch):
    launched = []
    monkeypatch.setattr(job, "launch", lambda bot: launched.append(bot) or {"action": "launched"})
    monkeypatch.setattr(job, "eligibility", lambda bot: None)
    monkeypatch.setattr(job, "_running", lambda name: False)
    monkeypatch.setattr(job, "archive_ready", lambda bot: False)
    assert job.maybe_start_multiyear("stocks")["action"] == "waiting_for_archive"
    monkeypatch.setattr(job, "archive_ready", lambda bot: True)
    job.maybe_start_multiyear("stocks")
    assert launched == ["stocks_multiyear"]
    monkeypatch.setattr(job, "_running", lambda name: name == "stocks_multiyear")
    assert job.maybe_start_multiyear("options")["action"] == "a_study_is_running"
    monkeypatch.setattr(job, "_running", lambda name: False)
    grid = [f"{a}:{b}" for a, b in job.PARAM_GRID]
    monkeypatch.setattr(job, "eligibility", lambda bot: {"enforce": False, "grid": grid})
    assert job.maybe_start_multiyear("stocks")["action"] == "already_published"
    # A study run without the current training grid is re-run with it.
    monkeypatch.setattr(job, "eligibility", lambda bot: {"enforce": False})
    launched.clear()
    job.maybe_start_multiyear("stocks")
    assert launched == ["stocks_multiyear"]



def test_training_picks_the_setting_and_symbols_from_prior_years_only():
    rows = []
    for year in range(2016, 2026):
        ts = int(pd.Timestamp(f"{year}-06-01", tz="UTC").timestamp())
        for i in range(10):
            # setting "1.5:3.0" makes money on A every year; "0.5:2.0" loses on everything
            rows.append({"symbol": "A", "param": "1.5:3.0", "entry_ts": ts + i, "net_return": 0.005 if i % 3 else -0.003})
            rows.append({"symbol": "A", "param": "0.5:2.0", "entry_ts": ts + i, "net_return": -0.002})
            rows.append({"symbol": "B", "param": "1.5:3.0", "entry_ts": ts + i, "net_return": -0.004})
            rows.append({"symbol": "B", "param": "0.5:2.0", "entry_ts": ts + i, "net_return": -0.004})
    tr = job.walk_forward_trained(pd.DataFrame(rows), default_param="0.5:2.0", min_trades=6, min_train_trades=10)
    assert all(y["param"] == "1.5:3.0" for y in tr["years"])
    assert tr["eligible_now"] == ["A"] and tr["param_now"] == {"STOP_BUFFER_ATR15": 1.5, "MIN_RR": 3.0}
    assert tr["out_of_sample"]["avg"] > 0 > tr["default_every_symbol"]["avg"] and tr["enforce"] is True


def test_an_enforced_trained_result_sets_the_bots_plan_settings(monkeypatch):
    from data import alpaca_setup, alpaca_strategy
    monkeypatch.setattr(alpaca_setup, "STOP_BUFFER_ATR15", alpaca_setup.STOP_BUFFER_ATR15)
    monkeypatch.setattr(alpaca_setup, "MIN_RR", alpaca_setup.MIN_RR)
    monkeypatch.setattr(job, "eligibility", lambda bot: {"enforce": True, "symbols": ["NVDA"], "rule": "r",
                                                          "params": {"STOP_BUFFER_ATR15": 1.5, "MIN_RR": 3.0}})
    alpaca_strategy.evaluate_setup_candidate("COST")
    assert alpaca_setup.STOP_BUFFER_ATR15 == 1.5 and alpaca_setup.MIN_RR == 3.0


def test_a_just_published_study_is_not_launched_again(monkeypatch):
    """The study runs in its own process: the server's eligibility cache can
    still hold "nothing published" from before it finished. The fresh result
    file wins, so the study isn't re-run (which blocked the next bot's)."""
    import time as _t
    job._eligibility_cache["perps"] = (_t.time() - 60, None)  # noqa: SLF001 -- cached before the result landed
    launched = []
    monkeypatch.setattr(job, "launch", lambda name: launched.append(name) or {"action": "launched"})
    monkeypatch.setattr(job, "archive_ready", lambda bot: True)
    grid = [f"{a}:{b}" for a, b in job.study_grid("perps")]
    (job.LOCAL_DIR / "perps_multiyear.json").write_text(json.dumps({"grid": grid, "computed_at": "x",
                                                                   "version": job.STUDY_VERSION.get("perps", 1)}))
    assert job.maybe_start_multiyear("perps")["action"] == "already_published" and launched == []
    assert job.eligibility("perps") is not None  # the newer result file refreshes the cache too
    job._eligibility_cache.pop("perps", None)  # noqa: SLF001


def test_a_published_study_survives_a_restart_on_the_dashboard(monkeypatch, tmp_path):
    import huggingface_hub
    report = tmp_path / "report.json"
    report.write_text(json.dumps({"computed_at": "2026-10-04T00:48:28", "grid": ["1.5:3.0"], "walk_forward": {}, "trained": {},
                                  "patterns": {"enforce": False, "with_patterns": {"trades": 114}}}))
    monkeypatch.setenv("HF_API_KEY", "token")
    monkeypatch.setattr(job, "_report_checked", {})
    monkeypatch.setattr(huggingface_hub, "hf_hub_download", lambda repo, path, **kw: str(report))
    status = job.multiyear_status("perps")
    assert status["latest"]["computed_at"] == "2026-10-04T00:48:28" and status["latest"]["patterns"]["with_patterns"]["trades"] == 114
