"""Alpaca archives behind the perps/15m studies: Kraken-via-Alpaca minute
bars and Benzinga news, with no-lookahead news reads."""
from __future__ import annotations

import pandas as pd
import pytest

from data import alpaca_client, alpaca_crypto_history as ch, alpaca_news_history as nh


@pytest.fixture(autouse=True)
def _isolated(tmp_path, monkeypatch):
    monkeypatch.setattr(ch, "LOCAL_DIR", tmp_path / "crypto")
    monkeypatch.setattr(nh, "LOCAL_DIR", tmp_path / "news")
    monkeypatch.delenv("HF_API_KEY", raising=False)


def test_crypto_history_reads_the_kraken_venue_a_month_at_a_time(monkeypatch):
    import datetime as dt
    calls = []

    def fake_bars(symbols, *, start, end, loc, max_pages):
        calls.append((symbols, start[:7], loc))
        return {symbols[0]: [{"t": start, "o": 1, "h": 2, "l": 0.5, "c": 1.5, "v": 3, "n": 4, "vw": 1.2}]}

    monkeypatch.setattr(alpaca_client, "get_crypto_bars", fake_bars)
    df = ch.fetch_bars("SOL", dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc), dt.datetime(2024, 4, 1, tzinfo=dt.timezone.utc))
    assert [c[1] for c in calls] == ["2024-01", "2024-02", "2024-03"] and {c[2] for c in calls} == {"us-1"}
    assert calls[0][0] == ["SOL/USD"] and list(df.columns) == ch.COLUMNS and len(df) == 3
    ch.write_year("SOL", 2024, df, merge_remote=False)
    candles = ch.candles("SOL", years=[2024])
    assert (candles.ts.to_numpy() == df.ts.to_numpy() + 60).all()  # stored by START, read by END


def test_news_months_are_fetched_scored_and_tagged(monkeypatch):
    pages = []

    def fake_get(path, *, params):
        pages.append(params.get("page_token"))
        if params.get("page_token") is None:
            return {"news": [{"id": 1, "created_at": "2024-03-01T10:00:00Z", "headline": "Bitcoin surges to record high",
                              "summary": "", "symbols": ["BTCUSD"], "source": "benzinga", "url": "u1"}], "next_page_token": "p2"}
        return {"news": [{"id": 2, "created_at": "2024-03-02T10:00:00Z", "headline": "Gold plunges", "summary": "selloff deepens",
                          "symbols": ["GLD"], "source": "benzinga", "url": "u2"}], "next_page_token": None}

    monkeypatch.setattr(alpaca_client, "_data_get", fake_get)
    df = nh.fetch_month(2024, 3, symbols=["BTCUSD", "GLD"])
    assert pages == [None, "p2"] and list(df.columns) == nh.COLUMNS
    assert df.loc[df.id == 1, "score"].item() > 0 and df.loc[df.id == 2, "score"].item() < 0
    assert df.loc[df.id == 2, "symbols"].item() == "GLD"


def test_news_reads_never_look_ahead():
    t0 = 1_790_000_000
    archive = pd.DataFrame({"id": [1, 2, 3], "created_at": [t0 - 7200, t0 - 60, t0 + 60], "headline": "", "summary": "",
                            "symbols": ["BTCUSD", "BTCUSD,ETHUSD", "BTCUSD"], "source": "", "url": "",
                            "score": [0.5, -1.0, 1.0]})
    idx = nh.NewsIndex(archive, ["BTCUSD"])
    assert idx.at(t0, hours=6) == {"count": 2.0, "score": -0.25}  # the article after t0 is not seen
    assert idx.at(t0, hours=1) == {"count": 1.0, "score": -1.0}
    assert nh.NewsIndex(archive, ["SOLUSD"]).at(t0) == {"count": 0.0, "score": 0.0}


def test_news_universe_covers_every_kalshi_coin_and_commodity():
    u = nh.universe()
    assert {"BTCUSD", "ETHUSD", "HYPEUSD", "GLD", "USO", "UNG", "SPY"} <= set(u)


def test_one_rejected_ticker_does_not_cost_the_month(monkeypatch):
    def fake_get(path, *, params):
        if "BADUSD" in params["symbols"].split(","):
            raise RuntimeError("400 invalid symbol")
        return {"news": [{"id": 7, "created_at": "2024-03-01T10:00:00Z", "headline": "Bitcoin rallies", "summary": "",
                          "symbols": ["BTCUSD"]}], "next_page_token": None}

    monkeypatch.setattr(alpaca_client, "_data_get", fake_get)
    df = nh.fetch_month(2024, 3, symbols=["BADUSD", "BTCUSD"])
    assert df.id.tolist() == [7]


def test_added_news_tickers_are_merged_into_every_stored_month(monkeypatch):
    import datetime as dt
    now = dt.datetime.now(dt.timezone.utc)
    months = [(2016, 1), (2016, 2), (now.year, now.month)]
    monkeypatch.setattr(nh, "_months", lambda start_year: months)
    monkeypatch.setattr(nh, "_hf_files", lambda: {"news/2016-01.parquet", "news/2016-02.parquet"})
    monkeypatch.setattr(nh, "universe", lambda: ["BTCUSD", "AVAXUSD"])
    monkeypatch.setattr(nh, "covered_symbols", lambda: ["BTCUSD"])
    monkeypatch.setattr(nh, "upload", lambda keys, message: list(keys))
    asked = []

    def fake_fetch(y, m, symbols=None):
        asked.append(((y, m), list(symbols)))
        return pd.DataFrame({"id": [y * 100 + m], "created_at": [1], "headline": ["x"], "summary": [""], "symbols": ["AVAXUSD"],
                             "source": [""], "url": [""], "score": [0.0]})

    monkeypatch.setattr(nh, "fetch_month", fake_fetch)
    old = pd.DataFrame({"id": [1], "created_at": [0], "headline": ["old"], "summary": [""], "symbols": ["BTCUSD"],
                        "source": [""], "url": [""], "score": [0.0]})
    nh._write("2016-01", old)  # noqa: SLF001
    r = nh.backfill(start_year=2016, refresh_recent=1)
    assert asked == [((2016, 1), ["AVAXUSD"]), ((2016, 2), ["AVAXUSD"]), ((now.year, now.month), ["BTCUSD", "AVAXUSD"])]
    merged = pd.read_parquet(nh._local_path("2016-01"))  # noqa: SLF001
    assert set(merged.id) == {1, 201601} and r["added_tickers"] == ["AVAXUSD"]


def test_the_crypto_bots_coins_join_the_archive_without_blocking_the_kalshi_studies(monkeypatch):
    import datetime as dt
    year = dt.datetime.now(dt.timezone.utc).year
    monkeypatch.setenv("HF_API_KEY", "token")
    monkeypatch.setattr(ch, "crypto_bot_coins", lambda: ["AVAX", "UNI"])
    monkeypatch.setattr(ch, "_no_data_coins", lambda: {"UNI"})
    import huggingface_hub

    class FakeApi:
        def __init__(self, token):
            pass

        def list_repo_files(self, repo, repo_type):
            return [f"bars_1m/{c}/{year}.parquet" for c in ch.universe()]

    monkeypatch.setattr(huggingface_hub, "HfApi", FakeApi)
    assert ch.missing_coins() == ["AVAX"]  # UNI is known to have no bars on this venue
    assert ch.archive_ready() is True and ch.crypto_bot_ready() is False  # Kalshi coins complete; AVAX still to come


def test_crypto_history_is_deepened_to_the_start_of_alpacas_data(monkeypatch):
    """Alpaca's Kraken US bars begin January 2021: every stored coin gets
    its earlier years once (a coin listed later simply has none)."""
    import huggingface_hub
    monkeypatch.setenv("HF_API_KEY", "token")
    assert ch.START_YEAR == 2021
    files = ["bars_1m/BTC/2022.parquet", "bars_1m/BTC/2023.parquet", "bars_1m/HYPE/2024.parquet"]
    uploads, fetched = [], []

    class FakeApi:
        def __init__(self, token=None):
            pass

        def list_repo_files(self, repo, repo_type=None):
            return files

        def upload_file(self, **kw):
            uploads.append(kw["path_in_repo"])

    monkeypatch.setattr(huggingface_hub, "HfApi", FakeApi)
    monkeypatch.setattr(ch, "_deepened", lambda: {})
    monkeypatch.setattr(ch, "upload", lambda paths, message: list(paths))

    def fake_fetch(coin, start, end):
        fetched.append((coin, start.year))
        if coin == "BTC":
            return pd.DataFrame({"ts": [int(start.timestamp())], "open": [1.0], "high": [1.0], "low": [1.0], "close": [1.0],
                                 "volume": [1.0], "trade_count": [1.0], "vwap": [1.0]})
        return pd.DataFrame(columns=ch.COLUMNS)  # HYPE didn't trade there yet

    monkeypatch.setattr(ch, "fetch_bars", fake_fetch)
    out = ch.deepen_history()
    assert fetched == [("BTC", 2021), ("HYPE", 2021), ("HYPE", 2022), ("HYPE", 2023)]
    assert out["files"] == 1 and out["coins_checked"] == 2 and uploads == [ch.DEEPENED_PATH]
    monkeypatch.setattr(ch, "_deepened", lambda: {"BTC": 2021, "HYPE": 2021})
    assert ch.history_deepened()


def test_a_study_worker_reads_only_its_own_news(monkeypatch):
    """The archive grows with every bot's tickers: a worker keeps only its
    asset's article times and scores, month by month."""
    import datetime as dt
    monkeypatch.setattr(nh, "_months", lambda start_year: [(2024, 1), (2024, 2)])
    for key, rows in (("2024-01", [(1, "AAPL,MSFT", 0.5), (2, "TSLA", -1.0)]), ("2024-02", [(3, "AAPL", 1.0)])):
        nh._write(key, pd.DataFrame([{"id": i, "created_at": int(dt.datetime(2024, 1, 15).timestamp()) + i * 86400 * 20,  # noqa: SLF001
                                       "headline": "h", "summary": "s", "symbols": sy, "source": "b", "url": "u", "score": sc}
                                      for i, sy, sc in rows]))
    idx = nh.index_for(["AAPL"])
    assert len(idx.t) == 2 and idx.cum_score[-1] == pytest.approx(1.5)
    assert "AAPL" in nh.universe() and set(nh.stock_tickers()) <= set(nh.universe())
