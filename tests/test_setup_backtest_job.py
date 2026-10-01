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
