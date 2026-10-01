"""The cross-bot correlation monitor: returns, matrix, same-bet clusters and
the exposure check every bot runs before an entry."""
from __future__ import annotations

import json

import numpy as np
import pandas as pd
import pytest

from data import global_correlation_monitor as gcm


def _minutes(close: np.ndarray, start: int = 1_790_000_100) -> pd.DataFrame:
    ts = start + 60 * np.arange(len(close))
    return pd.DataFrame({"ts": ts, "open": close, "high": close, "low": close, "close": close, "volume": 1.0})


def test_15m_returns_only_span_consecutive_closed_candles(monkeypatch):
    monkeypatch.setattr(gcm.time, "time", lambda: 1_790_100_000)
    rng = np.random.default_rng(1)
    a = _minutes(100 * np.exp(np.cumsum(rng.normal(0, 1e-3, 600))))
    gap = pd.concat([a.iloc[:300], a.iloc[420:]])  # a two-hour hole
    r_full, r_gap = gcm.returns_15m(a), gcm.returns_15m(gap)
    assert len(r_full) == 40 and len(r_gap) < len(r_full)
    assert (np.diff(r_gap.index.to_numpy()) >= 900).all()


def test_matrix_measures_real_co_movement(monkeypatch):
    monkeypatch.setattr(gcm.time, "time", lambda: 1_800_000_000)
    rng = np.random.default_rng(2)
    common = rng.normal(0, 1e-3, 1440 * 2)
    a = _minutes(100 * np.exp(np.cumsum(common)))
    b = _minutes(50 * np.exp(np.cumsum(common + rng.normal(0, 2e-4, len(common)))))
    c = _minutes(10 * np.exp(np.cumsum(rng.normal(0, 1e-3, len(common)))))
    m = gcm.correlation_matrix({k: gcm.returns_15m(v) for k, v in {"A": a, "B": b, "C": c}.items()})
    assert m["A"]["B"] > 0.9 and abs(m["A"]["C"]) < 0.3 and m["A"]["B"] == m["B"]["A"]


def test_too_little_overlap_is_unmeasured():
    idx = pd.Index(range(0, 900 * 10, 900))
    m = gcm.correlation_matrix({"A": pd.Series(np.arange(10.0), index=idx), "B": pd.Series(np.arange(10.0), index=idx)})
    assert m["A"]["B"] is None


MATRIX = {"BTC": {"BTC": 1.0, "ETH": 0.9, "GOLD": 0.3}, "ETH": {"BTC": 0.9, "ETH": 1.0, "GOLD": 0.35},
          "GOLD": {"BTC": 0.3, "ETH": 0.35, "GOLD": 1.0}}


def test_same_bet_clusters_join_correlated_same_direction_positions():
    exposures = [{"bot": "perps", "symbol": "BTC", "direction": "long"}, {"bot": "kalshi15m", "symbol": "ETH", "direction": "long"},
                 {"bot": "crypto", "symbol": "GOLD", "direction": "long"}, {"bot": "perps", "symbol": "ETH", "direction": "short"}]
    clusters = gcm.same_bet_clusters(exposures, MATRIX)
    assert len(clusters) == 1 and {p["symbol"] for p in clusters[0]["positions"]} == {"BTC", "ETH"}


@pytest.mark.parametrize("existing,blocked", [
    ([], False),
    ([{"bot": "perps", "symbol": "BTC", "direction": "long"}], False),  # 2 on the bet = the cap
    ([{"bot": "perps", "symbol": "BTC", "direction": "long"}, {"bot": "crypto", "symbol": "ETH", "direction": "long"}], True),
    ([{"bot": "perps", "symbol": "BTC", "direction": "short"}, {"bot": "crypto", "symbol": "GOLD", "direction": "long"}], False),
])
def test_exposure_check_caps_positions_on_the_same_bet(monkeypatch, existing, blocked):
    monkeypatch.setattr(gcm, "open_exposures", lambda: existing)
    monkeypatch.setattr(gcm, "latest", lambda: {"matrix": MATRIX})
    r = gcm.correlated_exposure("ETH", "long", bot="kalshi15m")
    assert r["blocked"] is blocked and r["ok"] is (not blocked)


def test_unmeasured_pairs_only_match_on_the_same_symbol(monkeypatch):
    monkeypatch.setattr(gcm, "open_exposures", lambda: [{"bot": "perps", "symbol": "SOL", "direction": "long"},
                                                        {"bot": "crypto", "symbol": "SOL", "direction": "long"}])
    monkeypatch.setattr(gcm, "latest", lambda: {"ok": False})
    assert gcm.correlated_exposure("SOL", "long", bot="kalshi15m")["blocked"] is True
    assert gcm.correlated_exposure("DOGE", "long", bot="kalshi15m")["blocked"] is False


def test_open_exposures_reads_every_bot_state_file(monkeypatch, tmp_path):
    from data import alpaca_crypto_strategy, alpaca_options_strategy, alpaca_strategy, kalshi_15m_strategy, perps_strategy
    files = {
        perps_strategy: {"positions": [{"ticker": "KXETHPERP", "side": "short"}]},
        kalshi_15m_strategy: {"positions": [{"coin": "BTC", "side": "no"}]},
        alpaca_strategy: {"positions": [{"symbol": "NVDA"}]},
        alpaca_crypto_strategy: {"positions": [{"symbol": "SOL/USD", "dry_run": True}]},
        alpaca_options_strategy: {"positions": [{"underlying_symbol": "AAPL", "option_type": "put", "strategy": "debit_spread"}]},
    }
    for i, (mod, body) in enumerate(files.items()):
        path = tmp_path / f"s{i}.json"
        path.write_text(json.dumps(body))
        monkeypatch.setattr(mod, "STATE_FILE", path)
    got = {(e["bot"], e["symbol"], e["direction"]) for e in gcm.open_exposures()}
    assert got == {("perps", "ETH", "short"), ("kalshi15m", "BTC", "short"), ("stocks", "NVDA", "long"),
                   ("crypto", "SOL", "long"), ("options", "AAPL", "short")}


def test_option_direction_follows_the_setup_or_the_spread():
    assert gcm._option_direction({"setup_side": "long", "option_type": "put"}) == "long"  # noqa: SLF001
    assert gcm._option_direction({"option_type": "call", "strategy": "debit_spread"}) == "long"  # noqa: SLF001
    assert gcm._option_direction({"option_type": "call", "strategy": "credit_spread"}) == "short"  # noqa: SLF001
