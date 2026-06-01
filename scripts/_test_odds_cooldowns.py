import os
import sys

import requests


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC = os.path.join(ROOT, "src")
for path in (SRC, ROOT):
    if path not in sys.path:
        sys.path.insert(0, path)

os.environ.setdefault("ODDS_API_KEY", "test-key")

import data.odds_fetcher as odds_fetcher


class _FakeResponse:
    def __init__(self, status_code, payload=None, headers=None):
        self.status_code = status_code
        self._payload = payload if payload is not None else []
        self.headers = headers or {}

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            err = requests.HTTPError(f"HTTP {self.status_code}")
            err.response = self
            raise err


def main() -> int:
    original_get = odds_fetcher.requests.get
    original_key = odds_fetcher.ODDS_API_KEY
    try:
        odds_fetcher.ODDS_API_KEY = "test-key"
        odds_fetcher._ODDS_API_DISABLED = False
        odds_fetcher._ODDS_GLOBAL_COOLDOWN_UNTIL = 0.0
        odds_fetcher._ODDS_SPORT_MARKET_COOLDOWNS.clear()
        odds_fetcher._ODDS_BUDGET["spent_estimate"] = 0
        odds_fetcher._ODDS_BUDGET["last_remaining"] = 99
        odds_fetcher._ODDS_BUDGET_BLOCKED_MONTH = None

        calls = []

        def fake_get(url, params=None, timeout=None):
            markets = str((params or {}).get("markets") or "")
            calls.append(markets)
            if markets == "h2h,spreads":
                return _FakeResponse(422, payload={"error": "unsupported market"}, headers={"x-requests-remaining": "99"})
            if markets == "h2h":
                payload = [{"home_team": "A", "away_team": "B", "bookmakers": []}]
                return _FakeResponse(200, payload=payload, headers={"x-requests-remaining": "98"})
            return _FakeResponse(200, payload=[], headers={"x-requests-remaining": "98"})

        odds_fetcher.requests.get = fake_get

        result = odds_fetcher.get_live_odds("nba", markets="h2h,spreads")

        assert isinstance(result, list)
        assert calls[:2] == ["h2h,spreads", "h2h"]
        assert any(k.startswith("basketball_nba|h2h,spreads") or k.startswith("basketball_nba|h2h") for k in odds_fetcher._ODDS_SPORT_MARKET_COOLDOWNS)
        assert not odds_fetcher._ODDS_API_DISABLED

        print("Odds cooldown regression passed.")
        return 0
    finally:
        odds_fetcher.requests.get = original_get
        odds_fetcher.ODDS_API_KEY = original_key


if __name__ == "__main__":
    raise SystemExit(main())