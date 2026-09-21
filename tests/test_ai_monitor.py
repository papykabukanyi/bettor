"""Project-wide, read-only AI-powered status review across all 5 markets.
See ai_monitor.py's own module docstring: this NEVER places an order or
touches any market's own LIVE_TRADING_ENABLED -- added per explicit user
direction (chosen over "replace the prediction model with Claude
entirely" via an AskUserQuestion, widened to "across all of the bots" in
the same build, then moved off the Anthropic API entirely onto HF's own
Inference Providers -- reusing the existing HF_API_KEY -- per explicit
user direction once the separate Anthropic billing requirement turned
out to be an unwanted surprise). Real network calls are always mocked
here; no test should ever hit the real HF Inference API."""
from __future__ import annotations

import json

import pytest

from data import (
    ai_monitor as m,
    alpaca_crypto_model,
    alpaca_crypto_strategy,
    alpaca_model,
    alpaca_options_model,
    alpaca_options_strategy,
    alpaca_strategy,
    kalshi_15m_metals_model,
    kalshi_15m_model,
    kalshi_15m_strategy,
    perps_model,
    perps_strategy,
)

_MODEL_MODULES = (perps_model, alpaca_model, alpaca_crypto_model, alpaca_options_model, kalshi_15m_model, kalshi_15m_metals_model)


@pytest.fixture(autouse=True)
def _isolated(tmp_path, monkeypatch):
    monkeypatch.setattr(m, "DATA_DIR", tmp_path)
    monkeypatch.setattr(m, "REPORT_PATH", tmp_path / "report.json")
    monkeypatch.setattr(m, "HF_API_KEY", "")
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
    # Every market's own MODEL_PATH/MODEL_META_PATH + in-memory cache,
    # isolated the same way each market's own test suite already does --
    # without this, a real model.joblib left on this dev machine's own
    # DATA_DIR from ordinary local use gets picked up here instead of the
    # "no model trained yet" state these tests actually want.
    for mod in _MODEL_MODULES:
        monkeypatch.setattr(mod, "MODEL_PATH", tmp_path / f"{mod.__name__.rsplit('.', 1)[-1]}.joblib")
        monkeypatch.setattr(mod, "MODEL_META_PATH", tmp_path / f"{mod.__name__.rsplit('.', 1)[-1]}_meta.json")
        monkeypatch.setattr(mod, "HF_API_KEY", "", raising=False)
        if hasattr(mod, "_model_cache"):
            monkeypatch.setitem(mod._model_cache, "model", None)  # noqa: SLF001
            monkeypatch.setitem(mod._model_cache, "meta", None)  # noqa: SLF001
            monkeypatch.setitem(mod._model_cache, "loaded_at", 0.0)  # noqa: SLF001


def test_gather_snapshot_covers_all_5_markets_with_no_state_anywhere():
    snapshot = m.gather_snapshot()
    assert set(snapshot["markets"].keys()) == {"perps", "stocks", "crypto", "options", "kalshi_15m"}
    for name, market in snapshot["markets"].items():
        assert "error" not in market, f"{name} snapshot unexpectedly errored: {market}"
        assert market["open_positions"] == []
        assert market["recent_trades"] == []
    assert "generated_at" in snapshot


def test_model_summary_marks_an_untrained_model_explicitly_rather_than_going_empty():
    # Real bug found from the very first live run: {} (no model yet) used
    # to be indistinguishable from "this market's model data is missing/
    # broken" -- the model itself misread kalshi_15m's own untrained
    # metals model as "missing entirely" in its first real report.
    assert m._model_summary(None) == {"trained": False}  # noqa: SLF001
    assert m._model_summary({}) == {"trained": False}  # noqa: SLF001


def test_model_summary_marks_a_trained_model_and_strips_feature_importances():
    result = m._model_summary({"model_type": "gradient_boosting", "rows": 500, "feature_importances": {"x": 1.0}})  # noqa: SLF001
    assert result == {"trained": True, "model_type": "gradient_boosting", "rows": 500}


def test_gather_snapshot_reports_trained_false_for_every_market_with_no_model_yet():
    snapshot = m.gather_snapshot()
    assert snapshot["markets"]["perps"]["model"] == {"trained": False}
    assert snapshot["markets"]["kalshi_15m"]["crypto_model"] == {"trained": False}
    assert snapshot["markets"]["kalshi_15m"]["metals_model"] == {"trained": False}


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


def test_call_model_is_a_no_op_without_an_api_key():
    result = m.call_model("some prompt")
    assert result == {"ok": False, "reason": "no_hf_api_key"}


class _FakeMessage:
    def __init__(self, content):
        self.content = content


class _FakeChoice:
    def __init__(self, content):
        self.message = _FakeMessage(content)


class _FakeUsage:
    def __init__(self, prompt_tokens=800, completion_tokens=60, total_tokens=860):
        self.prompt_tokens = prompt_tokens
        self.completion_tokens = completion_tokens
        self.total_tokens = total_tokens


class _FakeChatCompletionOutput:
    def __init__(self, text, model="moonshotai/Kimi-K2-Instruct", usage=None):
        self.choices = [_FakeChoice(text)] if text is not None else []
        self.model = model
        self.usage = usage if usage is not None else _FakeUsage()


class _FakeInferenceClient:
    captured: dict = {}

    def __init__(self, token=None, timeout=None):
        _FakeInferenceClient.captured["token"] = token
        _FakeInferenceClient.captured["timeout"] = timeout

    def chat_completion(self, *, messages, model, max_tokens):
        _FakeInferenceClient.captured.update(messages=messages, model=model, max_tokens=max_tokens)
        return _FakeInferenceClient.response


def test_call_model_parses_a_real_response_shape(monkeypatch):
    monkeypatch.setattr(m, "HF_API_KEY", "fake-hf-token")
    import huggingface_hub

    _FakeInferenceClient.captured = {}
    _FakeInferenceClient.response = _FakeChatCompletionOutput("All 5 markets look healthy.")
    monkeypatch.setattr(huggingface_hub, "InferenceClient", _FakeInferenceClient)

    result = m.call_model("some prompt")
    assert result["ok"] is True
    assert result["text"] == "All 5 markets look healthy."
    assert result["usage"] == {"prompt_tokens": 800, "completion_tokens": 60, "total_tokens": 860}
    assert _FakeInferenceClient.captured["token"] == "fake-hf-token"
    assert _FakeInferenceClient.captured["messages"] == [{"role": "user", "content": "some prompt"}]


def test_call_model_handles_a_network_failure(monkeypatch):
    monkeypatch.setattr(m, "HF_API_KEY", "fake-hf-token")
    import huggingface_hub

    class _FailingClient:
        def __init__(self, token=None, timeout=None):
            pass

        def chat_completion(self, **kw):
            raise RuntimeError("connection refused")

    monkeypatch.setattr(huggingface_hub, "InferenceClient", _FailingClient)
    result = m.call_model("some prompt")
    assert result["ok"] is False
    assert "connection refused" in result["reason"]


def test_call_model_reports_empty_response_as_not_ok(monkeypatch):
    monkeypatch.setattr(m, "HF_API_KEY", "fake-hf-token")
    import huggingface_hub

    _FakeInferenceClient.captured = {}
    _FakeInferenceClient.response = _FakeChatCompletionOutput(None)
    monkeypatch.setattr(huggingface_hub, "InferenceClient", _FakeInferenceClient)

    result = m.call_model("some prompt")
    assert result == {"ok": False, "reason": "empty_response"}


def test_run_monitor_cycle_saves_and_returns_a_no_key_result_without_calling_the_model(monkeypatch):
    def fail_if_called(prompt):
        raise AssertionError("must not call the model without an API key")

    monkeypatch.setattr(m, "call_model", fail_if_called)
    result = m.run_monitor_cycle()
    assert result["ok"] is False
    assert result["reason"] == "no_hf_api_key"
    assert m.get_latest_report() == result


def test_run_monitor_cycle_saves_a_successful_report(monkeypatch):
    monkeypatch.setattr(m, "HF_API_KEY", "fake-hf-token")
    monkeypatch.setattr(m, "call_model", lambda prompt: {"ok": True, "text": "All good.", "model": "moonshotai/Kimi-K2-Instruct", "usage": {}})

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


# ---------------------------------------------------------------------------
# Real bug found live: the report used to be local-disk-only -- a
# manually-triggered report was generated successfully, then genuinely
# lost on the very next restart (a routine redeploy), leaving the hub
# page showing "No review has run yet" minutes after a real report had
# existed. Now also backed up to/restored from a dedicated PRIVATE HF
# repo -- see _save_report's own docstring.
# ---------------------------------------------------------------------------
class _FakeHfApi:
    captured_upload: dict = {}

    def __init__(self, token=None):
        pass

    def repo_info(self, *, repo_id, repo_type):
        return {"id": repo_id}

    def create_repo(self, *, repo_id, repo_type, exist_ok, private):
        _FakeHfApi.captured_upload["create_repo_private"] = private

    def upload_file(self, *, path_or_fileobj, path_in_repo, repo_id, repo_type, commit_message):
        _FakeHfApi.captured_upload.setdefault("uploads", []).append({
            "path_in_repo": path_in_repo, "repo_id": repo_id,
            "content": json.loads(open(path_or_fileobj, encoding="utf-8").read()),
        })


def test_run_monitor_cycle_pushes_the_report_to_the_private_hf_repo(monkeypatch):
    import huggingface_hub

    monkeypatch.setattr(m, "HF_API_KEY", "fake-hf-token")
    monkeypatch.setattr(m, "call_model", lambda prompt: {"ok": True, "text": "All good.", "model": "moonshotai/Kimi-K2-Instruct", "usage": {}})
    _FakeHfApi.captured_upload = {}
    monkeypatch.setattr(huggingface_hub, "HfApi", _FakeHfApi)

    result = m.run_monitor_cycle()

    uploads = _FakeHfApi.captured_upload["uploads"]
    assert len(uploads) == 1
    assert uploads[0]["repo_id"] == m.HF_AI_MONITOR_REPO
    assert uploads[0]["path_in_repo"] == m.REPORT_HF_FILENAME
    assert uploads[0]["content"] == result


def test_get_latest_report_restores_from_hf_when_local_file_is_missing(monkeypatch):
    monkeypatch.setattr(m, "HF_API_KEY", "fake-hf-token")
    backup = {"ok": True, "report": "restored from HF", "generated_at": "2026-09-21T00:00:00+00:00"}

    import huggingface_hub
    tmp_file = m.DATA_DIR / "hf_backup_report.json"
    tmp_file.write_text(json.dumps(backup), encoding="utf-8")
    monkeypatch.setattr(huggingface_hub, "hf_hub_download", lambda **kw: str(tmp_file))

    result = m.get_latest_report()
    assert result == backup
    # Restored data is also written back to local disk.
    assert m.REPORT_PATH.exists()


def test_get_latest_report_returns_none_when_hf_has_no_backup_either(monkeypatch):
    monkeypatch.setattr(m, "HF_API_KEY", "fake-hf-token")
    import huggingface_hub
    monkeypatch.setattr(huggingface_hub, "hf_hub_download", lambda **kw: (_ for _ in ()).throw(RuntimeError("404")))
    assert m.get_latest_report() is None


def test_report_push_is_a_no_op_without_an_hf_key():
    # HF_API_KEY == "" via the autouse fixture -- must not raise.
    m._save_report({"ok": True, "report": "x"})  # noqa: SLF001


def test_build_prompt_mentions_every_market_by_name():
    snapshot = m.gather_snapshot()
    prompt = m._build_prompt(snapshot)  # noqa: SLF001
    assert snapshot["generated_at"] in prompt
    assert "read-only" in prompt.lower()
    for name in ("perpetual", "stocks", "crypto", "options", "15-minute"):
        assert name.lower() in prompt.lower()
