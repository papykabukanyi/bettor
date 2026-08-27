"""HF-hosted "news anchor" persona -- headline rewrite and reply drafting.
Every real network call is mocked; must never raise (only ever return None
on failure), and must never even attempt a call without an API key."""
from __future__ import annotations

import pytest

from data import threads_persona


@pytest.fixture(autouse=True)
def _isolated_persona_state(monkeypatch):
    monkeypatch.setattr(threads_persona, "HF_API_KEY", "test-hf-key")
    # _get_prompt's own persona-config cache is a module-level global meant
    # to persist for a real process's lifetime -- reset between tests, and
    # default the pull itself to "nothing found" (falls back to
    # _DEFAULT_PROMPTS) so a test doesn't silently attempt a real network
    # call against the live HF persona repo just because HF_API_KEY above
    # is a real (if fake-valued) truthy string. Tests that specifically
    # exercise the pulled-config path override this explicitly.
    monkeypatch.setattr(threads_persona, "_persona_config_cache", None)
    monkeypatch.setattr(threads_persona, "_last_persona_config_pull_ts", 0.0)
    monkeypatch.setattr(threads_persona, "_pull_persona_config_from_hf", lambda: None)
    # Real few-shot examples get inserted as extra (user, assistant) message
    # pairs between the system prompt and the real user message (see
    # _chat's own `examples` param) -- off by default here so every
    # existing test asserting on a fixed messages[0]/messages[1] shape
    # doesn't silently break just because the example library is
    # non-empty. Tests that specifically exercise few-shot injection
    # override this explicitly.
    monkeypatch.setattr(threads_persona, "_FEW_SHOT_SAMPLE_SIZE", 0)
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
    assert captured["system_content"] != threads_persona._DEFAULT_PROMPTS["anchor_rewrite_headline"]["system"]  # noqa: SLF001


# ── HF-hosted persona config (_get_prompt) ───────────────────────────────────

def test_get_prompt_falls_back_to_the_default_when_the_pull_returns_nothing(monkeypatch):
    monkeypatch.setattr(threads_persona, "_pull_persona_config_from_hf", lambda: None)
    prompt = threads_persona._get_prompt("anchor_rewrite_headline")  # noqa: SLF001
    assert prompt == threads_persona._DEFAULT_PROMPTS["anchor_rewrite_headline"]  # noqa: SLF001


def test_get_prompt_uses_the_pulled_config_when_available(monkeypatch):
    pulled = {"prompts": {"anchor_rewrite_headline": {"system": "Custom pulled prompt", "max_tokens": 50, "temperature": 0.5}}}
    monkeypatch.setattr(threads_persona, "_pull_persona_config_from_hf", lambda: pulled)
    prompt = threads_persona._get_prompt("anchor_rewrite_headline")  # noqa: SLF001
    assert prompt["system"] == "Custom pulled prompt"
    assert prompt["max_tokens"] == 50


def test_get_prompt_falls_back_to_default_when_the_pulled_config_is_missing_that_task(monkeypatch):
    monkeypatch.setattr(threads_persona, "_pull_persona_config_from_hf", lambda: {"prompts": {}})
    prompt = threads_persona._get_prompt("anchor_commentary")  # noqa: SLF001
    assert prompt == threads_persona._DEFAULT_PROMPTS["anchor_commentary"]  # noqa: SLF001


def test_get_prompt_only_pulls_once_within_the_refresh_window(monkeypatch):
    calls = {"n": 0}

    def fake_pull():
        calls["n"] += 1
        return None

    monkeypatch.setattr(threads_persona, "_pull_persona_config_from_hf", fake_pull)
    threads_persona._get_prompt("anchor_rewrite_headline")  # noqa: SLF001
    threads_persona._get_prompt("anchor_commentary")  # noqa: SLF001
    threads_persona._get_prompt("anchor_draft_reply")  # noqa: SLF001
    assert calls["n"] == 1


def test_anchor_rewrite_headline_uses_the_pulled_prompts_max_tokens_and_temperature(monkeypatch):
    pulled = {"prompts": {"anchor_rewrite_headline": {"system": "Custom system prompt", "max_tokens": 33, "temperature": 0.2}}}
    monkeypatch.setattr(threads_persona, "_pull_persona_config_from_hf", lambda: pulled)
    captured = {}

    def fake_post(url, *, headers, json, timeout):
        captured.update(json)
        return _chat_response("A punchy line")

    monkeypatch.setattr(threads_persona.requests, "post", fake_post)
    threads_persona.anchor_rewrite_headline("Bitcoin surges")
    assert captured["messages"][0]["content"] == "Custom system prompt"
    assert captured["max_tokens"] == 33
    assert captured["temperature"] == 0.2


def test_pull_persona_config_from_hf_returns_none_without_an_api_key(monkeypatch):
    monkeypatch.setattr(threads_persona, "HF_API_KEY", "")
    assert threads_persona._pull_persona_config_from_hf() is None  # noqa: SLF001


# ── few-shot example library (_sample_examples / _get_examples / _chat) ────
# See threads_persona.py's own module docstring: the "large dataset" half
# of the model-quality upgrade is a genuinely large, hand-curated example
# library fed to the model as real (user, assistant) conversation turns, a
# fresh handful sampled each real call rather than the same fixed subset
# (or the whole library) every time.

def test_every_task_has_a_real_few_shot_example_library():
    for task in threads_persona._DEFAULT_PROMPTS:  # noqa: SLF001
        examples = threads_persona._FEW_SHOT_EXAMPLES.get(task)  # noqa: SLF001
        assert examples, f"{task} has no few-shot examples"
        assert len(examples) >= 10  # a genuinely large pool, not a token gesture
        for user_turn, assistant_turn in examples:
            assert isinstance(user_turn, str) and user_turn
            assert isinstance(assistant_turn, str) and assistant_turn


def test_sample_examples_returns_a_capped_random_subset(monkeypatch):
    monkeypatch.setattr(threads_persona, "_FEW_SHOT_SAMPLE_SIZE", 3)
    result = threads_persona._sample_examples("anchor_rewrite_headline")  # noqa: SLF001
    assert len(result) == 3
    pool = threads_persona._FEW_SHOT_EXAMPLES["anchor_rewrite_headline"]  # noqa: SLF001
    assert all(item in pool for item in result)


def test_sample_examples_never_exceeds_the_pool_size(monkeypatch):
    monkeypatch.setattr(threads_persona, "_FEW_SHOT_SAMPLE_SIZE", 999)
    result = threads_persona._sample_examples("anchor_draft_reply")  # noqa: SLF001
    assert len(result) == len(threads_persona._FEW_SHOT_EXAMPLES["anchor_draft_reply"])  # noqa: SLF001


def test_sample_examples_returns_empty_for_an_unknown_task():
    assert threads_persona._sample_examples("not_a_real_task") == []  # noqa: SLF001


def test_get_examples_uses_the_hardcoded_library_when_the_prompt_has_none(monkeypatch):
    monkeypatch.setattr(threads_persona, "_FEW_SHOT_SAMPLE_SIZE", 2)
    result = threads_persona._get_examples("anchor_commentary", {"system": "x"})  # noqa: SLF001
    assert len(result) == 2
    pool = threads_persona._FEW_SHOT_EXAMPLES["anchor_commentary"]  # noqa: SLF001
    assert all(item in pool for item in result)


def test_get_examples_uses_the_hf_configs_own_examples_when_present(monkeypatch):
    monkeypatch.setattr(threads_persona, "_FEW_SHOT_SAMPLE_SIZE", 5)
    prompt = {"system": "x", "examples": [["custom user turn", "custom assistant turn"]]}
    result = threads_persona._get_examples("anchor_commentary", prompt)  # noqa: SLF001
    assert result == [("custom user turn", "custom assistant turn")]


def test_chat_inserts_examples_as_real_conversation_turns_before_the_final_user_message(monkeypatch):
    captured = {}

    def fake_post(url, *, headers, json, timeout):
        captured["messages"] = json["messages"]
        return _chat_response("A reply")

    monkeypatch.setattr(threads_persona.requests, "post", fake_post)
    threads_persona._chat(  # noqa: SLF001
        "system prompt", "real user prompt",
        examples=[("example user 1", "example assistant 1"), ("example user 2", "example assistant 2")],
    )
    messages = captured["messages"]
    assert messages[0] == {"role": "system", "content": "system prompt"}
    assert messages[1] == {"role": "user", "content": "example user 1"}
    assert messages[2] == {"role": "assistant", "content": "example assistant 1"}
    assert messages[3] == {"role": "user", "content": "example user 2"}
    assert messages[4] == {"role": "assistant", "content": "example assistant 2"}
    assert messages[5] == {"role": "user", "content": "real user prompt"}


def test_anchor_rewrite_headline_includes_few_shot_examples_by_default(monkeypatch):
    """End-to-end: with the real (non-zeroed) sample size, a real call
    through the public anchor_* function actually carries few-shot turns,
    not just the system+final-user shape the other tests isolate this
    from."""
    monkeypatch.setattr(threads_persona, "_FEW_SHOT_SAMPLE_SIZE", 4)
    captured = {}

    def fake_post(url, *, headers, json, timeout):
        captured["messages"] = json["messages"]
        return _chat_response("A punchy line")

    monkeypatch.setattr(threads_persona.requests, "post", fake_post)
    threads_persona.anchor_rewrite_headline("Bitcoin surges", source="cointelegraph")
    messages = captured["messages"]
    assert len(messages) == 1 + 4 * 2 + 1  # system + 4 example pairs + the real user message
    assert messages[-1]["content"].startswith('Headline: "Bitcoin surges"')


def test_model_default_is_the_upgraded_larger_model():
    """Real, live-verified upgrade from Llama-3.1-8B-Instruct (see this
    module's own docstring for the confirmed side-by-side comparison) --
    same free router/token, no new cost."""
    assert threads_persona.MODEL == "meta-llama/Llama-3.3-70B-Instruct"
