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
    monkeypatch.setattr(job, "STUDY_PRIORITY", [])  # the order between bots is tested on its own
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
    grid = job.grid_labels("stocks")
    monkeypatch.setattr(job, "eligibility", lambda bot: {"enforce": False, "grid": grid, "version": job.STUDY_VERSION["stocks"]})
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
    grid = job.grid_labels("perps")
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


def test_a_killed_study_is_finished_not_running_and_says_why(monkeypatch):
    """The server launches a study and never waits on it: when the study's
    process dies it lingers as a zombie, which must count as finished (it
    held every later study back) -- with the reason on the dashboard."""
    (job.LOCAL_DIR / "perps_multiyear.pid").write_text("4242")
    monkeypatch.setattr(job.os, "waitpid", lambda pid, flags: (pid, 9))  # reaped: killed by SIGKILL
    assert job._running("perps_multiyear") is False  # noqa: SLF001
    assert not (job.LOCAL_DIR / "perps_multiyear.pid").exists()
    assert "killed by signal 9 (out of memory)" in job.multiyear_status("perps")["error"]["error"]


def _fake_part(sym: str) -> pd.DataFrame:
    rows = []
    for year in range(2023, 2027):
        ts = int(pd.Timestamp(f"{year}-03-01", tz="UTC").timestamp())
        rows += [{"symbol": sym, "param": "1.0:2.0", "entry_ts": ts + 3600 * i, "side": "long",
                  "net_return": 0.004 if i % 3 else -0.002, "hour_block": "h00", "weekday": "mon"} for i in range(12)]
    return pd.DataFrame(rows)


def test_a_study_that_died_after_its_replays_resumes_at_the_analysis(monkeypatch):
    """Every replayed symbol is saved as it finishes: a rerun replays only
    what is missing, so a failure in the analysis never costs the hours of
    replay again."""
    monkeypatch.setattr(job, "_study_symbols", lambda bot: ["BTC", "ETH"])
    monkeypatch.setattr(job, "study_grid", lambda bot: [(1.0, 2.0)])
    job._load_parts("perps", {"version": job.STUDY_VERSION.get("perps", 1), "grid": job.grid_labels("perps")})  # noqa: SLF001
    for sym in ("BTC", "ETH"):
        job._save_part("perps", sym, _fake_part(sym))  # noqa: SLF001

    def no_pool(*a, **k):
        raise AssertionError("nothing left to replay")

    monkeypatch.setattr("concurrent.futures.ProcessPoolExecutor", no_pool)
    result = job.run_multiyear("perps", publish=False)
    progress = json.loads((job.LOCAL_DIR / "perps_multiyear_progress.json").read_text())
    assert result["ok"] and result["walk_forward"]["test_years"] == 3 and "patterns" in result
    assert progress["resumed"] == ["BTC", "ETH"] and progress["stage"] == "done"
    assert (job.LOCAL_DIR / "perps_multiyear_trades.parquet").exists() and not job._parts_dir("perps").exists()  # noqa: SLF001


def test_saved_replays_from_other_settings_are_not_reused():
    job._load_parts("perps", {"version": 1, "grid": ["1.0:2.0"]})  # noqa: SLF001
    job._save_part("perps", "BTC", _fake_part("BTC"))  # noqa: SLF001
    assert set(job._load_parts("perps", {"version": 1, "grid": ["1.0:2.0"]})) == {"BTC"}  # noqa: SLF001
    assert job._load_parts("perps", {"version": 2, "grid": ["1.0:2.0"]}) == {}  # noqa: SLF001


def test_a_restarted_study_restores_its_finished_replays_from_hf(monkeypatch, tmp_path):
    """A deploy restarts the Space and wipes local disk: the replays a study
    had finished come back from the bot's HF model repo (same version and
    settings), so it does not start over."""
    import huggingface_hub
    key = {"version": 2, "grid": ["1.0:2.0"]}
    remote = tmp_path / "remote"
    remote.mkdir()
    (remote / "key.json").write_text(json.dumps(key | {"at": __import__("time").time()}))
    (remote / "BTC.parquet").write_bytes(job._part_bytes(_fake_part("BTC")))  # noqa: SLF001

    class FakeApi:
        def __init__(self, token=None):
            pass

        def list_repo_files(self, repo, repo_type=None):
            return [f"{job.PARTS_PATH}/{p.name}" for p in remote.iterdir()]

    monkeypatch.setenv("HF_API_KEY", "token")
    monkeypatch.setattr(huggingface_hub, "HfApi", FakeApi)
    monkeypatch.setattr(huggingface_hub, "hf_hub_download", lambda repo, path, **kw: str(remote / path.rsplit("/", 1)[1]))
    parts = job._load_parts("perps", key)  # noqa: SLF001
    assert list(parts) == ["BTC"] and len(parts["BTC"]) == len(_fake_part("BTC"))
    assert (job._parts_dir("perps") / "BTC.pkl").exists()  # noqa: SLF001 -- and kept locally from here on
    assert job._load_parts("perps", {"version": 3, "grid": ["1.0:2.0"]}) == {}  # noqa: SLF001 -- another study's replays are not reused


def test_replays_saved_with_a_doubled_column_still_analyse(monkeypatch):
    """Parts kept by the run that had two `side` columns are read back with
    one, so the restarted study finishes instead of failing again."""
    monkeypatch.setattr(job, "_study_symbols", lambda bot: ["BTC"])
    monkeypatch.setattr(job, "study_grid", lambda bot: [(1.0, 2.0)])
    job._load_parts("perps", {"version": job.STUDY_VERSION.get("perps", 1), "grid": job.grid_labels("perps")})  # noqa: SLF001
    part = _fake_part("BTC")
    doubled = pd.concat([part, part[["side"]]], axis=1)
    doubled.to_pickle(job._parts_dir("perps") / "BTC.pkl")  # noqa: SLF001 -- as the failed run left it
    result = job.run_multiyear("perps", publish=False)
    assert result["ok"] and "patterns" in result and "side" in result["patterns"]["by_condition"]


def test_a_weekly_refresh_waits_for_the_running_study(monkeypatch):
    """The weekly timers queued their study straight away -- three ran at
    once on the Space's cores. A refresh now waits its turn."""
    monkeypatch.setattr(job, "STUDY_PRIORITY", [])  # the order between bots is tested on its own
    launched, running = [], {"perps_multiyear"}
    monkeypatch.setattr(job, "launch", lambda name: launched.append(name) or running.add(name) or {"action": "launched"})
    monkeypatch.setattr(job, "_running", lambda name: name in running)
    monkeypatch.setattr(job, "archive_ready", lambda bot: True)
    assert job.request_multiyear("crypto")["action"] == "a_study_is_running" and launched == []
    running.clear()
    assert job.maybe_start_multiyear("crypto")["action"] == "launched" and launched == ["crypto_multiyear"]
    assert not (job.LOCAL_DIR / "crypto_multiyear.queued").exists()


def test_a_failed_study_is_not_relaunched_in_a_loop(monkeypatch):
    launched = []
    monkeypatch.setattr(job, "launch", lambda name: launched.append(name) or {"action": "launched"})
    monkeypatch.setattr(job, "_running", lambda name: False)
    monkeypatch.setattr(job, "archive_ready", lambda bot: True)
    monkeypatch.setattr(job, "eligibility", lambda bot: None)
    job._write_error("perps_multiyear", "Traceback ...")  # noqa: SLF001
    assert job.maybe_start_multiyear("perps")["action"] == "failed_recently" and launched == []
    import os as _os
    old = __import__("time").time() - (job.FAILED_RETRY_HOURS + 1) * 3600
    _os.utime(job.LOCAL_DIR / "perps_multiyear_error.json", (old, old))
    assert job.maybe_start_multiyear("perps")["action"] == "launched"


def test_a_failed_run_stops_its_workers():
    """A study that failed kept its pool replaying beside its relaunch."""
    import time as _t
    from concurrent.futures import ProcessPoolExecutor
    pool = ProcessPoolExecutor(1)
    pool.submit(_t.sleep, 60)
    deadline = _t.time() + 30
    while not pool._processes and _t.time() < deadline:  # noqa: SLF001
        _t.sleep(0.1)
    procs = list(pool._processes.values())  # noqa: SLF001
    job._stop_pool(pool)  # noqa: SLF001
    for proc in procs:
        proc.join(10)
        assert not proc.is_alive()


def test_the_real_money_bots_studies_go_first(monkeypatch):
    """With the 15m and crypto studies both due, whichever timer fired first
    used to take the slot; the Kalshi bots' studies now always go first."""
    launched = []
    monkeypatch.setattr(job, "launch", lambda name: launched.append(name) or {"action": "launched"})
    monkeypatch.setattr(job, "_running", lambda name: False)
    monkeypatch.setattr(job, "archive_ready", lambda bot: True)
    monkeypatch.setattr(job, "eligibility", lambda bot: None)
    grid = job.grid_labels("perps")
    (job.LOCAL_DIR / "perps_multiyear.json").write_text(json.dumps({"grid": grid, "version": job.STUDY_VERSION["perps"]}))
    assert job.maybe_start_multiyear("crypto")["action"] == "after_kalshi15m" and launched == []
    assert job.maybe_start_multiyear("kalshi15m")["action"] == "launched" and launched == ["kalshi15m_multiyear"]


def _edge_trades():
    """Four weeks of real-price trades: cheap contracts (edge >= 0.04) win,
    the rest lose."""
    rows = []
    for week in range(4):
        base = int(pd.Timestamp("2026-09-07", tz="UTC").timestamp()) + week * 7 * 86400
        for i in range(12):
            edge = 0.05 if i % 2 else -0.01
            rows.append({"open_ts": base + i * 3600, "edge": edge, "pnl_per_contract": 0.10 if edge > 0.04 else -0.12})
    return pd.DataFrame(rows)


def test_the_price_edge_threshold_is_chosen_on_earlier_weeks_only():
    pe = job.price_edge_study(_edge_trades(), min_train_trades=5, min_trades=10)
    assert [w["min_edge"] for w in pe["weeks"]] == [0.0, 0.0, 0.0]  # the first threshold that keeps only the winners
    assert pe["out_of_sample"]["avg"] == pytest.approx(0.10) and pe["no_filter"]["avg"] < 0 and pe["enforce"] is True
    assert pe["by_threshold"]["none"]["trades"] == 48 and pe["min_edge_now"] == 0.0


def test_the_live_price_edge_minimum_comes_only_from_a_proven_replay(monkeypatch):
    monkeypatch.setattr(job, "latest", lambda bot: {"price_edge": {"enforce": False, "min_edge_now": 0.04}})
    assert job.price_edge_min() is None
    monkeypatch.setattr(job, "latest", lambda bot: {"price_edge": {"enforce": True, "min_edge_now": 0.04}})
    assert job.price_edge_min() == 0.04


def test_the_strategy_board_shows_each_bots_rule_and_evidence(monkeypatch):
    """One board: the rule each bot trades now (the study's choice only when
    it won on unseen years, else the setup defaults) and its evidence."""
    learned = {"enforce": True, "source": "trained", "symbols": ["BTC", "ETH"], "blocked": {"weekday": ["sat"]},
               "params": {"STOP_BUFFER_ATR15": 3.0, "MIN_RR": 2.0, "MAX_HOLD_HOURS": 8.0, "BREAKEVEN_R": 1.0}}
    monkeypatch.setattr(job, "eligibility", lambda bot: learned if bot == "perps" else None)
    monkeypatch.setattr(job, "latest", lambda bot: {"ok": True, "days": 120, "with_correlation": {"trades": 19, "avg": 0.1155,
                                                                                                    "unit": "usd_per_contract"}})
    board = {r["bot"]: r for r in job.strategy_board()}
    assert list(board) == ["perps", "kalshi15m", "crypto", "stocks", "options"]
    assert board["perps"]["rule"] == "stop 3x 15m range · target 2R · hold <= 8 h · break-even at 1R"
    assert board["perps"]["source"] == "trained" and board["perps"]["blocked"] == {"weekday": ["sat"]}
    assert board["kalshi15m"]["source"] == "defaults" and board["kalshi15m"]["sides"] == ["long"]
    assert board["kalshi15m"]["replay"]["trades"] == 19


def test_the_15m_rule_reads_as_its_own_entry_window_and_exit_style(monkeypatch):
    """The 15m bot's settings are its entry window and exit style, never
    another bot's hold limit; a walk-forward win keeps the defaults and only
    learns which symbols to trade."""
    from data import kalshi_15m_setup
    monkeypatch.setattr(kalshi_15m_setup, "ENTRY_MAX_MINUTE", 5.0)
    monkeypatch.setattr(kalshi_15m_setup, "EXIT_MODE", 0.0)
    walk_forward = {"enforce": True, "source": "walk_forward", "symbols": ["BTC", "GOLD"], "params": None}
    monkeypatch.setattr(job, "eligibility", lambda bot: walk_forward if bot == "kalshi15m" else None)
    monkeypatch.setattr(job, "latest", lambda bot: {})
    row = {r["bot"]: r for r in job.strategy_board()}["kalshi15m"]
    assert "enter by minute 5 · sell at stop or target" in row["rule"] and "hold" not in row["rule"]
    assert row["source"] == "walk_forward" and row["symbols"] == ["BTC", "GOLD"]
    assert job.rule_in_force("kalshi15m").endswith("(setup defaults, symbols learned on unseen years)")
    trained = {"enforce": True, "source": "trained", "symbols": ["BTC"],
               "params": {"STOP_BUFFER_ATR15": 1.0, "MIN_RR": 3.0, "ENTRY_MAX_MINUTE": 2.0, "EXIT_MODE": 2.0}}
    monkeypatch.setattr(job, "eligibility", lambda bot: trained)
    assert job.rule_in_force("kalshi15m") == ("stop 1x 15m range · target 3R · enter by minute 2 · hold to settlement"
                                              " (learned on unseen years)")
    monkeypatch.setattr(job, "eligibility", lambda bot: None)
    assert job.rule_in_force("perps").endswith("(setup defaults)")


def test_a_running_study_reports_live_progress_and_a_finish_estimate():
    """The dashboard's progress bar: finished symbols plus the ones in flight
    (each worker reports after every setting; the first pass weighs most),
    with a finish estimate from this run's pace."""
    import datetime as _dt
    import time as _t
    now = _t.time()
    started = _dt.datetime.fromtimestamp(now - 3600, _dt.timezone.utc).isoformat()
    progress = {"bot": "perps", "done": 4, "total": 10, "started_at": started, "stage": "replaying", "resumed": [], "trades_so_far": 900}
    (job.LOCAL_DIR / "perps_multiyear_progress.json").write_text(json.dumps(progress))
    job._work_progress("perps", "BTC", 1, 57, 50)   # noqa: SLF001 -- first (heaviest) pass done
    job._work_progress("perps", "ETH", 57, 57, 70)  # noqa: SLF001 -- finished, about to be saved
    sp = job.study_progress("perps", progress, now=now)
    first = job.FIRST_PASS_WEIGHT / (job.FIRST_PASS_WEIGHT + 56)
    assert sp["fraction"] == pytest.approx(0.95 * (4 + first) / 10, abs=1e-3)
    assert [s for s, _ in sp["in_flight"]] == ["BTC"] and sp["trades_so_far"] == 950
    assert 3600 < sp["eta_sec"] < 3 * 3600 and sp["stalled"] is False and sp["percent"] == pytest.approx(100 * sp["fraction"], abs=0.1)


def test_the_watchdog_restarts_a_stalled_study_where_it_stopped(monkeypatch):
    """No progress for STALL_MINUTES: the study is stopped (its saved symbols
    stay) and the next launcher tick relaunches it; after too many stalls
    it is marked failed instead."""
    import os as _os
    import time as _t
    killed = []
    (job.LOCAL_DIR / "perps_multiyear.pid").write_text("4242")
    progress = job.LOCAL_DIR / "perps_multiyear_progress.json"
    progress.write_text(json.dumps({"done": 3, "total": 13, "started_at": "2026-10-08T01:00:00+00:00", "stage": "replaying"}))
    old = _t.time() - (job.STALL_MINUTES + 5) * 60
    _os.utime(progress, (old, old))
    alive = {"v": True}
    monkeypatch.setattr(job, "_running", lambda name: name == "perps_multiyear" and alive["v"])
    monkeypatch.setattr(job, "_pid_alive", lambda pid: (False, None))
    monkeypatch.setattr(job.os, "getpgid", lambda pid: pid)
    monkeypatch.setattr(job.os, "killpg", lambda pgid, sig: killed.append((pgid, sig)))
    out = job.watch_studies()
    assert "relaunching where it stopped" in out["acted"]["perps"] and killed and not (job.LOCAL_DIR / "perps_multiyear.pid").exists()
    for _ in range(job.WATCHDOG_MAX_RESTARTS - 1):
        (job.LOCAL_DIR / "perps_multiyear.pid").write_text("4242")
        out = job.watch_studies()
    assert out["acted"]["perps"] == "stalled_too_often" and "stalled" in json.loads((job.LOCAL_DIR / "perps_multiyear_error.json").read_text())["error"]


def test_the_processing_overview_shows_every_study_in_queue_order(monkeypatch):
    monkeypatch.setattr(job, "_running", lambda name: name == "perps_multiyear")
    monkeypatch.setattr(job, "_restore_published_report", lambda bot: None)
    (job.LOCAL_DIR / "perps_multiyear_progress.json").write_text(json.dumps(
        {"done": 2, "total": 13, "started_at": "2026-10-08T01:00:00+00:00", "stage": "replaying", "resumed": []}))
    needs = {"kalshi15m": ("due", []), "crypto": ("waiting_for_archive", ["crypto history back to 2021"]),
             "stocks": ("waiting_for_archive", ["news for the stock and options tickers"]), "options": ("already_published", [])}
    monkeypatch.setattr(job, "_cached_need", lambda bot: needs[bot])
    rows = {r["bot"]: r for r in job.processing_overview()["studies"]}
    assert list(rows) == ["perps", "kalshi15m", "crypto", "stocks", "options"]
    assert rows["perps"]["state"] == "running" and rows["perps"]["progress"]["done"] == 2 and rows["perps"]["combinations"] == 1680
    assert rows["kalshi15m"]["state"] == "queued" and rows["kalshi15m"]["queue_position"] == 2
    assert rows["crypto"]["waiting_for"] == ["crypto history back to 2021"] and rows["options"]["state"] == "done"


def test_perps_typical_spread_is_the_median_of_real_two_sided_quotes(monkeypatch, tmp_path):
    """What a fill really costs: the median of the recorded quotes, never a
    candle without a quote (bid = ask = close) or a pulled bid."""
    import huggingface_hub

    from data import perps_data
    shard = tmp_path / "2026-10-07.parquet"
    pd.DataFrame({"ticker": ["KXBTCPERP"] * 4 + ["KXADAPERP"] * 3,
                  "bid_close": [100.0, 100.0, 100.0, 0.0, 0.2632, 0.2633, 0.25],
                  "ask_close": [100.004, 100.006, 100.0, 100.0, 0.2635, 0.2636, 0.25]}).to_parquet(shard)

    class FakeApi:
        def __init__(self, token=None):
            pass

        def list_repo_files(self, repo_id, repo_type):
            return ["data/2026-10-07.parquet", "data/pregame_schedule/x.parquet", "README.md"]

    monkeypatch.setattr(huggingface_hub, "HfApi", FakeApi)
    monkeypatch.setattr(huggingface_hub, "hf_hub_download", lambda **kw: str(shard))
    monkeypatch.setattr(perps_data, "HF_API_KEY", "token")
    perps_data._typical_spread_cache.clear()  # noqa: SLF001
    spreads = perps_data.typical_spread_bps()
    assert spreads["KXBTCPERP"] == pytest.approx(0.5, abs=0.01)   # median of 0.4 and 0.6 bps
    assert spreads["KXADAPERP"] == pytest.approx(11.4, abs=0.1)   # median of 11.4 and 11.4 bps


def test_the_perps_study_charges_the_median_spread_not_one_moments_book(monkeypatch):
    """A pulled book once read ADA at 20,000 bps, so its study never traded
    it; every year is now charged the recorded median (a live snapshot only
    for a perp the archive lacks)."""
    from data import kalshi_perps, perps_data, perps_strategy
    monkeypatch.setattr(perps_data, "typical_spread_bps", lambda: {"KXADAPERP": 9.9})
    monkeypatch.setattr(perps_data, "chartable_tickers", lambda: ["KXADAPERP", "KXGOLDPERP"])
    monkeypatch.setattr(perps_strategy, "setup_fee_rate_roundtrip", lambda ticker: 0.0008)
    monkeypatch.setattr(kalshi_perps, "get_margin_market", lambda ticker: {"market": {"bid": "0.0001", "ask": "0.25"}})
    costs = job._kalshi_study_costs("perps", ["ADA", "GOLD"])  # noqa: SLF001
    assert costs["ADA"] == {"fee_rate_roundtrip": 0.0008, "spread_bps": 9.9, "spread_source": "median of the recorded quotes"}
    assert costs["GOLD"]["spread_source"] == "live snapshot" and costs["GOLD"]["spread_bps"] > 19_000


def test_the_crypto_replay_reads_each_coin_once():
    """The live bot holds one position per coin and every quote currency of
    a coin reads the same chart: one pair per coin, /USD when listed."""
    from data import alpaca_crypto_setup
    universe = ["AAVE/USDC", "AAVE/USD", "AAVE/USDT", "USDT/USD", "BTC/USDC", "BTC/USD", "PEPE/USDT", "USDC/USD"]
    assert job.crypto_replay_symbols(universe, alpaca_crypto_setup.STABLECOINS) == ["AAVE/USD", "BTC/USD", "PEPE/USDT"]


def test_the_stocks_replay_reads_what_the_live_bot_can_enter():
    watch = ["AAPL", "ABNB", "TSLA", "XOM"]
    assert job.stock_replay_symbols(watch, {"enforce": True, "symbols": ["TSLA", "AAPL", "NVDA"]}) == ["AAPL", "TSLA"]
    assert job.stock_replay_symbols(watch, {"enforce": False, "symbols": ["TSLA"]}) == watch
    assert job.stock_replay_symbols(watch, None) == watch
