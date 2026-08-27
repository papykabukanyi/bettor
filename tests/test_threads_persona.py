"""HF-hosted "news anchor" persona -- headline rewrite and reply drafting.
Every real network call is mocked; must never raise (only ever return None
on failure), and must never even attempt a call without an API key."""
from __future__ import annotations

import pytest

from data import threads_persona


@pytest.fixture(autouse=True)
def _isolated_persona_state(monkeypatch):
    monkeypatch.setattr(threads_persona, "HF_API_KEY", "test-hf-key")
    yield


class _FakeResponse:
    def __init__(self, json_body, status_code=200):
        self._json_body = json_body
        self.status_code = status_code
        self.text = str(json_body)

    def raise_for_status(self):
        if self.status_code >= 400:
            raise threads_persona.requests.exceptions.HTTPError(f"{self.status_code} error")

    def json(self):
        return self._json_body


def _chat_response(content: str):
    return _FakeResponse({"choices": [{"message": {"content": content}}]})


def test_chat_returns_none_without_an_api_key(monkeypatch):
    monkeypatch.setattr(threads_persona, "HF_API_KEY", "")

    def fail_if_called(*a, **k):
        raise AssertionError("must not attempt a network call without an API key")

    monkeypatch.setattr(threads_persona.requests, "post", fail_if_called)
    assert threads_persona._chat("system", "user") is None  # noqa: SLF001


def test_chat_returns_the_completion_text(monkeypatch):
    captured = {}

    def fake_post(url, *, headers, json, timeout):
        captured.update({"url": url, "headers": headers, "json": json, "timeout": timeout})
        return _chat_response("  Rewritten line  ")

    monkeypatch.setattr(threads_persona.requests, "post", fake_post)
    result = threads_persona._chat("system prompt", "user prompt")  # noqa: SLF001
    assert result == "Rewritten line"
    assert captured["url"] == threads_persona._ROUTER_URL  # noqa: SLF001
    assert captured["headers"]["Authorization"] == "Bearer test-hf-key"
    assert captured["json"]["messages"][0] == {"role": "system", "content": "system prompt"}
    assert captured["json"]["messages"][1] == {"role": "user", "content": "user prompt"}


def test_chat_returns_none_on_a_blank_completion(monkeypatch):
    monkeypatch.setattr(threads_persona.requests, "post", lambda *a, **k: _chat_response("   "))
    assert threads_persona._chat("s", "u") is None  # noqa: SLF001


def test_chat_returns_none_on_a_network_failure(monkeypatch):
    def raise_error(*a, **k):
        raise RuntimeError("network down")

    monkeypatch.setattr(threads_persona.requests, "post", raise_error)
    assert threads_persona._chat("s", "u") is None  # noqa: SLF001


def test_chat_returns_none_on_an_http_error(monkeypatch):
    monkeypatch.setattr(threads_persona.requests, "post", lambda *a, **k: _FakeResponse({"error": "bad"}, status_code=500))
    assert threads_persona._chat("s", "u") is None  # noqa: SLF001


def test_anchor_rewrite_headline_returns_none_for_an_empty_title(monkeypatch):
    def fail_if_called(*a, **k):
        raise AssertionError("must not call the model for an empty title")

    monkeypatch.setattr(threads_persona.requests, "post", fail_if_called)
    assert threads_persona.anchor_rewrite_headline("") is None


def test_anchor_rewrite_headline_includes_source_and_secondary_as_context(monkeypatch):
    captured = {}

    def fake_post(url, *, headers, json, timeout):
        captured["user_content"] = json["messages"][1]["content"]
        return _chat_response("BREAKING: real punchy line")

    monkeypatch.setattr(threads_persona.requests, "post", fake_post)
    result = threads_persona.anchor_rewrite_headline(
        "Bitcoin surges", source="cointelegraph", secondary=["ETF inflows accelerate"],
    )
    assert result == "BREAKING: real punchy line"
    assert "Bitcoin surges" in captured["user_content"]
    assert "cointelegraph" in captured["user_content"]
    assert "ETF inflows accelerate" in captured["user_content"]


def test_anchor_draft_reply_returns_none_for_empty_post_text(monkeypatch):
    def fail_if_called(*a, **k):
        raise AssertionError("must not call the model for empty post text")

    monkeypatch.setattr(threads_persona.requests, "post", fail_if_called)
    assert threads_persona.anchor_draft_reply("") is None


def test_anchor_draft_reply_includes_the_username_as_context(monkeypatch):
    captured = {}

    def fake_post(url, *, headers, json, timeout):
        captured["user_content"] = json["messages"][1]["content"]
        return _chat_response("Nice call on that trade!")

    monkeypatch.setattr(threads_persona.requests, "post", fake_post)
    result = threads_persona.anchor_draft_reply("BTC to the moon", author_username="cryptofan")
    assert result == "Nice call on that trade!"
    assert "cryptofan" in captured["user_content"]
    assert "BTC to the moon" in captured["user_content"]


def test_anchor_commentary_returns_none_for_an_empty_title(monkeypatch):
    def fail_if_called(*a, **k):
        raise AssertionError("must not call the model for an empty title")

    monkeypatch.setattr(threads_persona.requests, "post", fail_if_called)
    assert threads_persona.anchor_commentary("") is None


def test_anchor_commentary_includes_source_and_secondary_as_context(monkeypatch):
    captured = {}

    def fake_post(url, *, headers, json, timeout):
        captured["system_content"] = json["messages"][0]["content"]
        captured["user_content"] = json["messages"][1]["content"]
        return _chat_response("This is bigger than it looks -- here's why.")

    monkeypatch.setattr(threads_persona.requests, "post", fake_post)
    result = threads_persona.anchor_commentary(
        "Bitcoin surges", source="cointelegraph", secondary=["ETF inflows accelerate"],
    )
    assert result == "This is bigger than it looks -- here's why."
    assert "Bitcoin surges" in captured["user_content"]
    assert "cointelegraph" in captured["user_content"]
    assert "ETF inflows accelerate" in captured["user_content"]
    # Distinct persona/prompt from the headline rewrite -- explicitly NOT a
    # "BREAKING:"-style announcement, since the audience already saw that.
    assert captured["system_content"] != threads_persona._ANCHOR_SYSTEM_PROMPT  # noqa: SLF001
