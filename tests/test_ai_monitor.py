"""Project-wide, read-only Claude-powered status review across all 5
markets. See ai_monitor.py's own module docstring: this NEVER places an
order or touches any market's own LIVE_TRADING_ENABLED -- added per
explicit user direction (chosen over "replace the prediction model with
Claude entirely" via an AskUserQuestion, then explicitly widened from
Kalshi 15-minute markets only to "across all of the bots" in the same
build). Real network calls are always mocked here; no test should ever
hit the real Anthropic API."""
from __future__ import annotations

import pytest

from data import (
    ai_monitor as m,
    alpaca_crypto_strategy,
    alpaca_options_strategy,
    alpaca_strategy,
    kalshi_15m_strategy,
    perps_strategy,
)


@pytest.fixture(autouse=True)
def _isolated(tmp_path, monkeypatch):
    monkeypatch.setattr(m, "DATA_DIR", tmp_path)
    monkeypatch.setattr(m, "REPORT_PATH", tmp_path / "report.json")
    monkeypatch.setattr(m, "ANTHROPIC_API_KEY", "")
    # Every market's own state, isolated to this test's own tmp dir --
    # same convention each market's own test suite already uses.
    monkeypatch.setattr(perps_strategy, "STATE_FILE", tmp_path / "perps_state.json")
    monkeypatch.setattr(alpaca_strategy, "STATE_FILE", tmp_path / "alpaca_state.json")
    monkeypatch.setattr(alpaca_crypto_strategy, "STATE_FILE", tmp_path / "alpaca_crypto_state.json")
    monkeypatch.setattr(alpaca_options_strategy, "STATE_FILE", tmp_path / "alpaca_options_state.json")
    monkeypatch.setattr(kalshi_15m_strategy, "STATE_FILE", tmp_path / "kalshi_15m_state.json")
    # _load_state() falls back to a real HF pull when HF_API_KEY is set
    # and the local state file is missing -- keep every market's own key
    # empty here regardless of what's in the real environment.
    for mod in (perps_strategy, alpaca_strategy, alpaca_crypto_strategy, alpaca_options_strategy, kalshi_15m_strategy):
        monkeypatch.setattr(mod, "HF_API_KEY", "", raising=False)


def test_gather_snapshot_covers_all_5_markets_with_no_state_anywhere():
    snapshot = m.gather_snapshot()
    assert set(snapshot["markets"].keys()) == {"perps", "stocks", "crypto", "options", "kalshi_15m"}
    for name, market in snapshot["markets"].items():
        assert "error" not in market, f"{name} snapshot unexpectedly errored: {market}"
        assert market["open_positions"] == []
        assert market["recent_trades"] == []
    assert "generated_at" in snapshot


def test_gather_snapshot_reflects_a_real_perps_position():
    perps_strategy._save_state({  # noqa: SLF001
        "positions": [{"ticker": "KXBTCPERP", "side": "long", "count": 1.0}],
        "trade_log": [{"ticker": "KXBTCPERP", "realized_pnl_usd": 2.5}],
        "realized_pnl_by_date": {"2026-09-21": 2.5},
    })
    snapshot = m.gather_snapshot()
    assert snapshot["markets"]["perps"]["open_positions"] == [{"ticker": "KXBTCPERP", "side": "long", "count": 1.0}]
    assert snapshot["markets"]["perps"]["realized_pnl_by_date"] == {"2026-09-21": 2.5}


def test_gather_snapshot_reflects_a_real_kalshi_15m_position():
    kalshi_15m_strategy._save_state({  # noqa: SLF001
        "positions": [{"coin": "BTC", "side": "yes", "count": 10}],
        "trade_log": [],
        "realized_pnl_by_date": {},
    })
    snapshot = m.gather_snapshot()
    assert snapshot["markets"]["kalshi_15m"]["open_positions"] == [{"coin": "BTC", "side": "yes", "count": 10}]


def test_gather_snapshot_isolates_one_markets_failure_from_the_other_4(monkeypatch):
    def fail():
        raise RuntimeError("boom")

    monkeypatch.setitem(m._MARKET_SNAPSHOT_FNS, "stocks", fail)  # noqa: SLF001
    snapshot = m.gather_snapshot()
    assert "error" in snapshot["markets"]["stocks"]
    assert "boom" in snapshot["markets"]["stocks"]["error"]
    # The other 4 markets are completely unaffected.
    for name in ("perps", "crypto", "options", "kalshi_15m"):
        assert "error" not in snapshot["markets"][name]


def test_gather_snapshot_never_raises_when_kalshi_shard_balance_check_fails(monkeypatch):
    from data import kalshi_15m

    def fail(**kw):
        raise RuntimeError("Kalshi API error 500")

    monkeypatch.setattr(kalshi_15m, "get_balance_by_shard", fail)
    snapshot = m.gather_snapshot()
    assert "error" in snapshot["markets"]["kalshi_15m"]["shard_2_balance"]


def test_call_claude_is_a_no_op_without_an_api_key():
    result = m.call_claude("some prompt")
    assert result == {"ok": False, "reason": "no_anthropic_api_key"}


def test_call_claude_parses_a_real_response_shape(monkeypatch):
    monkeypatch.setattr(m, "ANTHROPIC_API_KEY", "fake-key")

    class _FakeResponse:
        def raise_for_status(self):
            pass

        def json(self):
            return {
                "model": "claude-sonnet-5",
                "content": [{"type": "text", "text": "All 5 markets look healthy."}],
                "usage": {"input_tokens": 800, "output_tokens": 60},
            }

    captured = {}

    def fake_post(url, headers, json, timeout):
        captured.update(url=url, headers=headers, json=json, timeout=timeout)
        return _FakeResponse()

    monkeypatch.setattr(m.requests, "post", fake_post)
    result = m.call_claude("some prompt")
    assert result["ok"] is True
    assert result["text"] == "All 5 markets look healthy."
    assert captured["headers"]["x-api-key"] == "fake-key"
    assert captured["json"]["messages"] == [{"role": "user", "content": "some prompt"}]


def test_call_claude_handles_a_network_failure(monkeypatch):
    monkeypatch.setattr(m, "ANTHROPIC_API_KEY", "fake-key")

    def fail(url, headers, json, timeout):
        raise RuntimeError("connection refused")

    monkeypatch.setattr(m.requests, "post", fail)
    result = m.call_claude("some prompt")
    assert result["ok"] is False
    assert "connection refused" in result["reason"]


def test_call_claude_reports_empty_response_as_not_ok(monkeypatch):
    monkeypatch.setattr(m, "ANTHROPIC_API_KEY", "fake-key")

    class _EmptyResponse:
        def raise_for_status(self):
            pass

        def json(self):
            return {"content": []}

    monkeypatch.setattr(m.requests, "post", lambda url, **kw: _EmptyResponse())
    result = m.call_claude("some prompt")
    assert result == {"ok": False, "reason": "empty_response"}


def test_run_monitor_cycle_saves_and_returns_a_no_key_result_without_calling_claude(monkeypatch):
    def fail_if_called(prompt):
        raise AssertionError("must not call Claude without an API key")

    monkeypatch.setattr(m, "call_claude", fail_if_called)
    result = m.run_monitor_cycle()
    assert result["ok"] is False
    assert result["reason"] == "no_anthropic_api_key"
    assert m.get_latest_report() == result


def test_run_monitor_cycle_saves_a_successful_report(monkeypatch):
    monkeypatch.setattr(m, "ANTHROPIC_API_KEY", "fake-key")
    monkeypatch.setattr(m, "call_claude", lambda prompt: {"ok": True, "text": "All good.", "model": "claude-sonnet-5", "usage": {}})

    result = m.run_monitor_cycle()
    assert result["ok"] is True
    assert result["report"] == "All good."
    assert m.get_latest_report() == result


def test_run_monitor_cycle_never_places_an_order_or_touches_live_trading_enabled():
    # Real safety property, locked in as a test: this module has no
    # import of any market's own create_order/submit_order function
    # anywhere, and never assigns to any market's own LIVE_TRADING_ENABLED.
    import inspect

    source = inspect.getsource(m)
    for banned in ("create_order", "submit_order", "place_order"):
        assert banned not in source
    assert "LIVE_TRADING_ENABLED =" not in source  # a read reference is fine; an assignment would not be


def test_get_latest_report_returns_none_when_nothing_has_run_yet():
    assert m.get_latest_report() is None


def test_build_prompt_mentions_every_market_by_name():
    snapshot = m.gather_snapshot()
    prompt = m._build_prompt(snapshot)  # noqa: SLF001
    assert snapshot["generated_at"] in prompt
    assert "read-only" in prompt.lower()
    for name in ("perpetual", "stocks", "crypto", "options", "15-minute"):
        assert name.lower() in prompt.lower()
