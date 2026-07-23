"""Kalshi REST request core -- retry behavior. Confirmed live: a burst of
calls across many perp tickers in one collection cycle can trip Kalshi's own
429 rate limit; a transient 429 must be retried with a short backoff rather
than failing the whole cycle immediately."""
from __future__ import annotations

from data import kalshi_client


class _FakeResponse:
    def __init__(self, status_code, body="", json_body=None):
        self.status_code = status_code
        self._body = body
        self._json_body = json_body if json_body is not None else {}
        self.text = body

    def json(self):
        return self._json_body


def test_retries_on_429_then_succeeds(monkeypatch):
    monkeypatch.setattr(kalshi_client.time, "sleep", lambda s: None)
    calls = {"n": 0}

    def fake_request(method, url, **kwargs):
        calls["n"] += 1
        if calls["n"] < 2:
            return _FakeResponse(429, "too many requests")
        return _FakeResponse(200, json_body={"ok": True})

    monkeypatch.setattr(kalshi_client.requests, "request", fake_request)
    result = kalshi_client._request_json("GET", "/exchange/status", auth=False)  # noqa: SLF001
    assert result == {"ok": True}
    assert calls["n"] == 2


def test_gives_up_after_max_attempts_on_persistent_429(monkeypatch):
    monkeypatch.setattr(kalshi_client.time, "sleep", lambda s: None)
    monkeypatch.setattr(
        kalshi_client.requests, "request",
        lambda method, url, **kwargs: _FakeResponse(429, "too many requests"),
    )
    try:
        kalshi_client._request_json("GET", "/exchange/status", auth=False)  # noqa: SLF001
        assert False, "expected a RuntimeError after exhausting retries"
    except RuntimeError as exc:
        assert "429" in str(exc)


def test_non_429_error_raises_immediately_without_retrying(monkeypatch):
    calls = {"n": 0}

    def fake_request(method, url, **kwargs):
        calls["n"] += 1
        return _FakeResponse(500, "server error")

    monkeypatch.setattr(kalshi_client.requests, "request", fake_request)
    try:
        kalshi_client._request_json("GET", "/exchange/status", auth=False)  # noqa: SLF001
        assert False, "expected a RuntimeError"
    except RuntimeError:
        pass
    assert calls["n"] == 1
