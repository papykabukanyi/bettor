"""kalshi_funding: the user's 50/50 split of Kalshi cash between perps and
the 15m bot, moved with Kalshi's intra-account transfer API."""
from __future__ import annotations

import pytest

from data import kalshi_funding as f

FREE = {"perps": False, "kalshi15m": False}


@pytest.fixture(autouse=True)
def _isolated(tmp_path, monkeypatch):
    monkeypatch.setattr(f, "STATE_PATH", tmp_path / "funding.json")
    monkeypatch.setattr(f.time, "sleep", lambda s: None)


def _b(ec0, ec2, margined):
    return {"event_contract": {0: ec0, 1: 0.0, 2: ec2, 3: 0.0}, "margined_available": margined, "margined_position_value": 0.0}


def test_one_pool_splits_perps_cash_half_to_the_15m_shard():
    p = f.plan(_b(19.61, 1.45, 19.61), "event_contract", FREE)
    assert p["total_usd"] == pytest.approx(21.06) and p["target_15m_usd"] == pytest.approx(10.53)
    (mv,) = p["moves"]
    assert mv["source"] == ("event_contract", 0) and mv["destination"] == ("event_contract", 2)
    assert mv["amount_usd"] == pytest.approx(9.08)


def test_a_separate_margined_account_counts_idle_exchange0_cash_first():
    p = f.plan(_b(19.60, 1.46, 19.61), "margined", FREE)
    assert p["total_usd"] == pytest.approx(40.67) and p["idle_usd"] == pytest.approx(19.60)
    (mv,) = p["moves"]
    assert mv["source"] == ("event_contract", 0) and mv["amount_usd"] == pytest.approx(40.67 / 2 - 1.46)


def test_money_backing_an_open_trade_is_never_moved():
    p = f.plan(_b(19.61, 1.45, 19.61), "event_contract", {"perps": True, "kalshi15m": False})
    assert p["moves"] == [] and p["action"] == "perps_has_open_positions"
    p = f.plan(_b(1.0, 30.0, 1.0), "event_contract", {"perps": False, "kalshi15m": True})
    assert p["moves"] == [] and p["action"] == "15m_has_open_positions"


def test_a_15m_surplus_goes_back_to_perps_and_small_drifts_are_left_alone():
    (mv,) = f.plan(_b(5.0, 25.0, 5.0), "margined", FREE)["moves"]
    assert mv["source"] == ("event_contract", 2) and mv["destination"] == ("margined", 0)
    assert f.plan(_b(10.0, 10.4, 10.0), "event_contract", FREE)["action"] == "in_band"


def test_the_pool_is_found_with_a_one_cent_probe(monkeypatch):
    reads = [_b(19.61, 1.45, 19.61), _b(19.60, 1.46, 19.60)]
    monkeypatch.setattr(f, "balances", lambda: reads.pop(0))
    sent = []
    monkeypatch.setattr(f, "transfer", lambda amt, *, source, destination: sent.append((amt, source, destination)) or {"amount_usd": amt})
    state = {}
    assert f.detect_pool(state) == "event_contract" and sent == [(0.01, ("event_contract", 0), ("event_contract", 2))]
    assert f.detect_pool(state) == "event_contract" and len(sent) == 1  # remembered


def test_a_transfer_is_one_signed_post_in_centicents(monkeypatch):
    from data import kalshi_client
    calls = []
    monkeypatch.setattr(kalshi_client, "_request_json", lambda method, path, **kw: calls.append((method, path, kw)) or {"transfer_id": "t1"})
    t = f.transfer(9.08, source=("event_contract", 0), destination=("event_contract", 2))
    method, path, kw = calls[0]
    assert (method, path) == ("POST", "/portfolio/intra_exchange_instance_transfer") and kw["auth"] is True
    assert kw["payload"] == {"source": "event_contract", "source_exchange_shard": 0, "destination": "event_contract",
                             "destination_exchange_shard": 2, "amount": 90800}
    assert t["transfer_id"] == "t1"


def test_rebalance_runs_only_with_live_trading_on(monkeypatch):
    from data import kalshi_15m_strategy, perps_strategy
    monkeypatch.setattr(perps_strategy, "LIVE_TRADING_ENABLED", False)
    assert f.rebalance()["action"] == "live_trading_off"
    monkeypatch.setattr(perps_strategy, "LIVE_TRADING_ENABLED", True)
    monkeypatch.setattr(kalshi_15m_strategy, "LIVE_TRADING_ENABLED", True)
    monkeypatch.setattr(f, "detect_pool", lambda state: "event_contract")
    monkeypatch.setattr(f, "balances", lambda: _b(19.61, 1.45, 19.61))
    monkeypatch.setattr(f, "_open_positions", lambda: FREE)
    monkeypatch.setattr(f, "transfer", lambda amt, *, source, destination: {"transfer_id": "x", "amount_usd": amt})
    r = f.rebalance()
    assert r["ok"] and r["action"] == "rebalance" and r["transfers"][0]["amount_usd"] == pytest.approx(9.08)
    assert f.status()["recent_transfers"][-1]["why"] == "rebalance to 15m"
