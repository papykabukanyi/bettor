"""These reproduce the exact production incident: HF's /whoami-v2 and
repo_info/create_repo endpoints got rate-limited (429), and the pipeline
treated that as "not configured at all", which silently froze predictions
for two weeks. Both must now fail open when the repo/owner is already known,
and never re-verify on every single construction."""
from __future__ import annotations

import sys
import types

import pytest


class FakeHfApi:
    """An HfApi stand-in whose every metadata call raises, simulating a fully
    rate-limited Hugging Face account exactly like the production logs showed."""

    def __init__(self, token=None):
        self.token = token

    def whoami(self):
        FakeHfApi.calls["whoami"] += 1
        raise Exception("429 Too Many Requests: /whoami-v2")

    def repo_info(self, **kwargs):
        FakeHfApi.calls["repo_info"] += 1
        raise Exception("429 Too Many Requests: GET /api/datasets/...")

    def create_repo(self, **kwargs):
        FakeHfApi.calls["create_repo"] += 1
        raise Exception("429 Too Many Requests: POST /api/repos/create")

    def upload_file(self, **kwargs):
        FakeHfApi.calls["upload_file"] += 1

    calls: dict[str, int] = {}


@pytest.fixture
def fake_hf_hub(monkeypatch):
    FakeHfApi.calls = {"whoami": 0, "repo_info": 0, "create_repo": 0, "upload_file": 0}
    fake_module = types.ModuleType("huggingface_hub")
    fake_module.HfApi = FakeHfApi
    monkeypatch.setitem(sys.modules, "huggingface_hub", fake_module)
    # Force a fresh import of hf_pipeline/hf_uploader so they bind to the fake module.
    for mod_name in ("data.hf_pipeline", "data.hf_uploader"):
        monkeypatch.delitem(sys.modules, mod_name, raising=False)
    yield FakeHfApi
    for mod_name in ("data.hf_pipeline", "data.hf_uploader"):
        monkeypatch.delitem(sys.modules, mod_name, raising=False)


def test_fully_qualified_repo_never_calls_whoami_even_when_rate_limited(fake_hf_hub, monkeypatch):
    monkeypatch.setenv("HF_API_KEY", "fake-token")
    monkeypatch.setenv("HF_DATASET_REPO", "papylove/sportprediction")
    monkeypatch.setenv("HF_MODEL_REPO", "papylove/sportprediction")
    from data.hf_pipeline import HFDirectPipeline

    p = HFDirectPipeline()
    assert p.ok is True, "pipeline must stay configured when the repo id is already owner-qualified"
    assert p.model_repo_id == "papylove/sportprediction"
    assert fake_hf_hub.calls["whoami"] == 0


def test_repeated_construction_does_not_repeatedly_hammer_repo_verification(fake_hf_hub, monkeypatch):
    monkeypatch.setenv("HF_API_KEY", "fake-token")
    monkeypatch.setenv("HF_DATASET_REPO", "papylove/sportprediction")
    monkeypatch.setenv("HF_MODEL_REPO", "papylove/sportprediction")
    from data.hf_pipeline import HFDirectPipeline

    for _ in range(5):
        p = HFDirectPipeline()
        assert p.ok is True
        assert getattr(p.uploader, "_ok", None) is True

    # Only the FIRST construction should attempt repo_info/create_repo; the
    # rest must be short-circuited by the failure-backoff cache, not each
    # independently hammer a rate-limited endpoint again.
    assert fake_hf_hub.calls["repo_info"] == 1
    assert fake_hf_hub.calls["create_repo"] == 1


def test_repo_without_owner_still_fails_closed_when_truly_unverifiable(fake_hf_hub, monkeypatch):
    monkeypatch.setenv("HF_API_KEY", "fake-token")
    from data.hf_pipeline import HFDirectPipeline

    p = HFDirectPipeline(dataset_repo="dataset-with-no-owner", model_repo="dataset-with-no-owner")
    assert getattr(p.uploader, "_ok", None) is False
    assert p.ok is False
    assert fake_hf_hub.calls["whoami"] >= 1, "whoami SHOULD be attempted when the repo genuinely has no owner"
