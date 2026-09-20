"""Alpaca-crypto-server-specific job wiring and scheduler shutdown
behavior -- its own separate Render service, split out from
alpaca_server.py (equities) after a real, confirmed OOM crash loop from
running both strategies in one 512MB process. Generic job-locking
mechanics are covered in test_server_common.py instead."""
from __future__ import annotations

import pandas as pd
import pytest

import alpaca_crypto_server


def test_fast_check_job_calls_manage_open_positions(monkeypatch):
    from data import alpaca_crypto_strategy as strat

    monkeypatch.setattr(strat, "manage_open_positions", lambda: {"action": "no_position", "checks": []})
    result = alpaca_crypto_server._run_alpaca_crypto_fast_check.__wrapped__()  # noqa: SLF001
    assert result == {"action": "no_position", "checks": []}


def test_entry_scan_job_calls_scan_and_enter(monkeypatch):
    from data import alpaca_crypto_strategy as strat

    monkeypatch.setattr(strat, "scan_and_enter", lambda: {"opened": [{"symbol": "BTC/USD", "action": "opened"}]})
    result = alpaca_crypto_server._run_alpaca_crypto_entry_scan.__wrapped__()  # noqa: SLF001
    assert result["opened"][0]["symbol"] == "BTC/USD"


def test_data_collect_job_returns_no_rows_when_nothing_collected(monkeypatch):
    from data import alpaca_crypto_data

    monkeypatch.setattr(alpaca_crypto_data, "collect_dataset_rows", lambda: pd.DataFrame())
    result = alpaca_crypto_server._run_alpaca_crypto_data_collect.__wrapped__()  # noqa: SLF001
    assert result == {"ok": False, "reason": "no_rows_collected"}


def test_data_collect_job_pushes_collected_rows(monkeypatch):
    from data import alpaca_crypto_data

    df = pd.DataFrame({"symbol": ["BTC/USD"], "ts": [1]})
    monkeypatch.setattr(alpaca_crypto_data, "collect_dataset_rows", lambda: df)
    pushed = {}

    def fake_push(d):
        pushed["df"] = d
        return {"ok": True}

    monkeypatch.setattr(alpaca_crypto_data, "push_minute_snapshot", fake_push)
    result = alpaca_crypto_server._run_alpaca_crypto_data_collect.__wrapped__()  # noqa: SLF001
    assert result == {"ok": True}
    assert list(pushed["df"]["symbol"]) == ["BTC/USD"]


def test_train_job_calls_train_model(monkeypatch):
    from data import alpaca_crypto_meta_model, alpaca_crypto_model, alpaca_crypto_strategy

    monkeypatch.setattr(alpaca_crypto_strategy, "_load_state", lambda: {"trade_log": []})
    monkeypatch.setattr(alpaca_crypto_model, "train_model", lambda **kw: {"ok": True, "rows": 500})
    monkeypatch.setattr(alpaca_crypto_meta_model, "train_meta_model", lambda **kw: {"ok": False, "reason": "no_data"})
    result = alpaca_crypto_server._run_alpaca_crypto_train.__wrapped__()  # noqa: SLF001
    assert result == {"ok": True, "rows": 500}


def test_train_job_passes_the_real_trade_log_for_outcome_aware_weighting(monkeypatch):
    """alpaca_crypto_model.py never imports alpaca_crypto_strategy.py
    directly (circular import risk -- see this job's own comment), so
    alpaca_crypto_server.py is responsible for reading trade_log and
    threading it through -- same pattern as app_kalshi.py's own
    _run_perps_train."""
    from data import alpaca_crypto_meta_model, alpaca_crypto_model, alpaca_crypto_strategy

    fake_trade_log = [{"symbol": "BTC/USD", "opened_at": "x", "realized_pnl_usd": 1.0, "dry_run": False}]
    monkeypatch.setattr(alpaca_crypto_strategy, "_load_state", lambda: {"trade_log": fake_trade_log})
    captured = {}
    monkeypatch.setattr(alpaca_crypto_model, "train_model", lambda **kw: captured.update(kw) or {"ok": True})
    monkeypatch.setattr(alpaca_crypto_meta_model, "train_meta_model", lambda **kw: {"ok": False, "reason": "no_data"})

    alpaca_crypto_server._run_alpaca_crypto_train.__wrapped__()  # noqa: SLF001

    assert captured["trade_log"] == fake_trade_log


def test_train_job_survives_a_state_read_failure(monkeypatch):
    from data import alpaca_crypto_meta_model, alpaca_crypto_model, alpaca_crypto_strategy

    def fail():
        raise RuntimeError("state read failed")

    monkeypatch.setattr(alpaca_crypto_strategy, "_load_state", fail)
    captured = {}
    monkeypatch.setattr(alpaca_crypto_model, "train_model", lambda **kw: captured.update(kw) or {"ok": True})
    monkeypatch.setattr(alpaca_crypto_meta_model, "train_meta_model", lambda **kw: {"ok": False, "reason": "no_data"})

    result = alpaca_crypto_server._run_alpaca_crypto_train.__wrapped__()  # noqa: SLF001

    assert result["ok"] is True
    assert captured["trade_log"] is None  # degrades to plain (non-outcome-weighted) training, doesn't crash the job


# ── Meta-labeling training (see alpaca_crypto_meta_model.py's own module
# docstring) -- additive and best-effort: must never affect this job's own
# primary result either way. ─────────────────────────────────────────────

def test_train_job_also_trains_the_meta_model_after_a_successful_primary_train(monkeypatch):
    from data import alpaca_crypto_meta_model, alpaca_crypto_model, alpaca_crypto_strategy

    monkeypatch.setattr(alpaca_crypto_strategy, "_load_state", lambda: {"trade_log": []})
    monkeypatch.setattr(alpaca_crypto_model, "train_model", lambda **kw: {"ok": True, "model_type": "random_forest"})
    called = []
    monkeypatch.setattr(alpaca_crypto_meta_model, "train_meta_model", lambda **kw: called.append(kw) or {"ok": True})

    result = alpaca_crypto_server._run_alpaca_crypto_train.__wrapped__()  # noqa: SLF001

    assert result["ok"] is True
    assert len(called) == 1


def test_train_job_skips_the_meta_model_when_the_primary_train_failed(monkeypatch):
    """Nothing new to build out-of-fold labels from without a fresh primary
    model -- must not even attempt it."""
    from data import alpaca_crypto_meta_model, alpaca_crypto_model, alpaca_crypto_strategy

    monkeypatch.setattr(alpaca_crypto_strategy, "_load_state", lambda: {"trade_log": []})
    monkeypatch.setattr(alpaca_crypto_model, "train_model", lambda **kw: {"ok": False, "reason": "insufficient_rows"})

    def fail_if_called(**kw):
        raise AssertionError("must not train the meta-model after a failed primary train")

    monkeypatch.setattr(alpaca_crypto_meta_model, "train_meta_model", fail_if_called)

    result = alpaca_crypto_server._run_alpaca_crypto_train.__wrapped__()  # noqa: SLF001

    assert result["ok"] is False


def test_train_job_survives_a_meta_model_training_failure(monkeypatch):
    """Best-effort only -- a meta-model training crash must never take down
    the primary job's own (already-successful) result."""
    from data import alpaca_crypto_meta_model, alpaca_crypto_model, alpaca_crypto_strategy

    monkeypatch.setattr(alpaca_crypto_strategy, "_load_state", lambda: {"trade_log": []})
    monkeypatch.setattr(alpaca_crypto_model, "train_model", lambda **kw: {"ok": True, "model_type": "random_forest"})

    def raise_error(**kw):
        raise RuntimeError("simulated meta-model training crash")

    monkeypatch.setattr(alpaca_crypto_meta_model, "train_meta_model", raise_error)

    result = alpaca_crypto_server._run_alpaca_crypto_train.__wrapped__()  # noqa: SLF001

    assert result["ok"] is True


def test_threads_trending_news_job_posts_the_fetched_story(monkeypatch):
    from data import crypto_news, threads_post

    story = {"title": "Bitcoin rallies", "link": "https://x.com/a", "image_url": "https://x.com/i.jpg", "source": "cointelegraph", "secondary": []}
    monkeypatch.setattr(crypto_news, "get_trending_story", lambda **kw: story)
    captured = {}
    monkeypatch.setattr(threads_post, "post_trending_news", lambda s, *, market: captured.update(story=s, market=market) or True)

    result = alpaca_crypto_server._run_alpaca_crypto_threads_trending_news.__wrapped__()  # noqa: SLF001

    assert result == {"ok": True, "posted": True, "story": "Bitcoin rallies"}
    assert captured["market"] == "crypto"


def test_threads_trending_news_job_never_raises_on_failure(monkeypatch):
    from data import crypto_news

    def raise_error(**kw):
        raise RuntimeError("rss down")

    monkeypatch.setattr(crypto_news, "get_trending_story", raise_error)
    result = alpaca_crypto_server._run_alpaca_crypto_threads_trending_news.__wrapped__()  # noqa: SLF001
    assert result["ok"] is False


def test_threads_sentiment_snapshot_job_posts_per_ticker_sentiment(monkeypatch):
    from data import alpaca_crypto_data, crypto_news, threads_post

    monkeypatch.setattr(alpaca_crypto_data, "get_crypto_universe", lambda: ["BTC/USD", "ETH/USD"])
    monkeypatch.setattr(alpaca_crypto_data, "symbol_to_coin", lambda symbol: symbol.split("/")[0])
    monkeypatch.setattr(crypto_news, "get_sentiment", lambda coin, **kw: {"sentiment_score": 0.6 if coin == "BTC" else -0.3})

    captured = {}
    monkeypatch.setattr(threads_post, "post_sentiment_snapshot", lambda *, market, ticker_sentiments: captured.update(market=market, ticker_sentiments=ticker_sentiments) or True)

    result = alpaca_crypto_server._run_alpaca_crypto_threads_sentiment_snapshot.__wrapped__()  # noqa: SLF001

    assert result == {"ok": True, "posted": True, "ticker_count": 2}
    assert captured["market"] == "crypto"
    assert {"ticker": "BTC/USD", "sentiment_score": 0.6} in captured["ticker_sentiments"]
    assert {"ticker": "ETH/USD", "sentiment_score": -0.3} in captured["ticker_sentiments"]


def test_threads_sentiment_snapshot_job_never_raises_on_failure(monkeypatch):
    from data import alpaca_crypto_data

    def raise_error():
        raise RuntimeError("universe unavailable")

    monkeypatch.setattr(alpaca_crypto_data, "get_crypto_universe", raise_error)
    result = alpaca_crypto_server._run_alpaca_crypto_threads_sentiment_snapshot.__wrapped__()  # noqa: SLF001
    assert result["ok"] is False


def test_threads_hourly_status_job_posts_open_positions(monkeypatch):
    import datetime as dt
    from data import alpaca_crypto_strategy, threads_post

    opened_at = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=15)).isoformat()
    monkeypatch.setattr(alpaca_crypto_strategy, "_load_state", lambda: {
        "positions": [{"symbol": "BTC/USD", "entry_price": 65000.0, "count": 0.001, "opened_at": opened_at}],
        "realized_pnl_by_date": {},
    })

    captured = {}
    monkeypatch.setattr(threads_post, "post_hourly_status", lambda *, positions, today_realized_pnl_usd, market: captured.update(positions=positions, market=market) or True)

    result = alpaca_crypto_server._run_alpaca_crypto_threads_hourly_status.__wrapped__()  # noqa: SLF001

    assert result == {"ok": True, "posted": True, "open_position_count": 1}
    assert captured["market"] == "crypto"
    assert captured["positions"][0]["ticker"] == "BTC/USD"
    assert captured["positions"][0]["held_minutes"] == pytest.approx(15.0, abs=0.5)


def test_threads_hourly_status_job_never_raises_on_failure(monkeypatch):
    from data import alpaca_crypto_strategy

    def raise_error():
        raise RuntimeError("state file corrupted")

    monkeypatch.setattr(alpaca_crypto_strategy, "_load_state", raise_error)
    result = alpaca_crypto_server._run_alpaca_crypto_threads_hourly_status.__wrapped__()  # noqa: SLF001
    assert result["ok"] is False


def test_api_alpaca_crypto_status_reports_configured_flag(monkeypatch):
    from data import alpaca_client

    monkeypatch.setattr(alpaca_client, "is_configured", lambda: True)
    monkeypatch.setattr(alpaca_client, "get_account", lambda: {"cash": "500.0", "equity": "500.0"})
    with alpaca_crypto_server.app.test_client() as client:
        resp = client.get("/api/alpaca/crypto/status")
        assert resp.status_code == 200
        assert resp.get_json()["alpaca_configured"] is True


def test_api_alpaca_crypto_status_surfaces_correlation_study_health(monkeypatch):
    """Real diagnostic gap closed -- see crypto_correlation.study_health's
    own docstring / app_kalshi.py's identical field for perps."""
    from data import crypto_correlation

    monkeypatch.setattr(
        crypto_correlation, "get_alpaca_study",
        lambda: {"computed_at": "2026-01-01T00:00:00+00:00", "ids": ["BTC", "ETH", "WLD"], "corr": {"BTC": {"ETH": 0.8}}, "divergence_z": {"ETH": 0.3}, "breadth": -0.2},
    )
    with alpaca_crypto_server.app.test_client() as client:
        resp = client.get("/api/alpaca/crypto/status")
        assert resp.status_code == 200
        assert resp.get_json()["correlation_study_health"] == {
            "computed_at": "2026-01-01T00:00:00+00:00", "num_ids": 3,
            "num_with_peer_data": 1, "num_with_divergence_data": 1, "breadth": -0.2,
        }


def test_api_alpaca_crypto_status_attaches_unrealized_pnl_and_exit_check_to_open_positions(monkeypatch):
    """Real gap found in review: the dashboard showed entry price + static
    TP/SL levels for every open position but never its CURRENT price,
    unrealized P&L, or the real exit_check reason text -- even though
    manage_open_positions() already computes both every fast_check cycle.
    Long-only: entry 100.0, current 105.0, count 2 -> +$10.00."""
    from data import alpaca_client
    from data import alpaca_crypto_strategy as strat

    monkeypatch.setattr(alpaca_client, "is_configured", lambda: True)
    monkeypatch.setattr(alpaca_client, "get_account", lambda: {"cash": "500.0", "equity": "500.0"})
    monkeypatch.setattr(strat, "_load_state", lambda: {
        "positions": [{"symbol": "BTC/USD", "entry_price": 100.0, "count": 2.0, "opened_at": "2026-08-19T00:00:00+00:00"}],
        "trade_log": [], "realized_pnl_by_date": {},
    })

    def fake_load_json(path, default):
        if str(path).endswith("alpaca_crypto_latest_position_check.json"):
            return {"checks": [{"symbol": "BTC/USD", "ok": True, "exit_check": "holding (+5.00%)", "current_price": 105.0}]}
        return default

    monkeypatch.setattr(alpaca_crypto_server, "load_json", fake_load_json)

    with alpaca_crypto_server.app.test_client() as client:
        resp = client.get("/api/alpaca/crypto/status")
        position = resp.get_json()["positions"][0]
        assert position["current_price"] == 105.0
        assert position["unrealized_pnl_usd"] == pytest.approx(10.0, abs=0.01)
        assert position["unrealized_pnl_pct"] == pytest.approx(0.05, abs=0.001)
        assert position["exit_check"] == "holding (+5.00%)"


def test_api_alpaca_crypto_status_omits_unrealized_pnl_when_no_current_price_is_available(monkeypatch):
    from data import alpaca_client
    from data import alpaca_crypto_strategy as strat

    monkeypatch.setattr(alpaca_client, "is_configured", lambda: True)
    monkeypatch.setattr(alpaca_client, "get_account", lambda: {"cash": "500.0", "equity": "500.0"})
    monkeypatch.setattr(strat, "_load_state", lambda: {
        "positions": [{"symbol": "BTC/USD", "entry_price": 100.0, "count": 2.0, "opened_at": "2026-08-19T00:00:00+00:00"}],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    monkeypatch.setattr(alpaca_crypto_server, "load_json", lambda path, default: default)

    with alpaca_crypto_server.app.test_client() as client:
        resp = client.get("/api/alpaca/crypto/status")
        position = resp.get_json()["positions"][0]
        assert "unrealized_pnl_usd" not in position


def test_walkforward_backtest_job_saves_the_result(monkeypatch, tmp_path):
    from data import alpaca_crypto_backtest

    fake_result = {"ok": True, "fold_count": 4, "profitable_fold_count": 2, "mean_return_pct": 0.03}
    original = dict(fake_result)  # maybe_auto_improve_from_backtest mutates the SAME dict run_walkforward_backtest returns
    monkeypatch.setattr(alpaca_crypto_backtest, "run_walkforward_backtest", lambda: fake_result)
    monkeypatch.setattr(alpaca_crypto_server, "ALPACA_CRYPTO_LATEST_WALKFORWARD_FILE", tmp_path / "walkforward.json")

    result = alpaca_crypto_server._run_alpaca_crypto_walkforward_backtest.__wrapped__()  # noqa: SLF001

    assert result["fold_count"] == original["fold_count"]
    assert result["mean_return_pct"] == original["mean_return_pct"]
    # The saved cache file predates the auto-improvement check (see
    # maybe_auto_improve_from_backtest's own docstring) -- it stays in the
    # exact shape api_alpaca_crypto_report_pdf's own "Walk-Forward Backtest"
    # section already expects, not retroactively grown with metadata that
    # was never part of this file's own contract.
    saved = alpaca_crypto_server.load_json(tmp_path / "walkforward.json", {})
    assert saved == original
    assert "auto_improvement" not in saved
    assert "auto_improvement" in result


def test_walkforward_backtest_job_returns_ok_false_on_failure(monkeypatch):
    from data import alpaca_crypto_backtest

    def raise_error():
        raise RuntimeError("no data available")

    monkeypatch.setattr(alpaca_crypto_backtest, "run_walkforward_backtest", raise_error)
    result = alpaca_crypto_server._run_alpaca_crypto_walkforward_backtest.__wrapped__()  # noqa: SLF001
    assert result["ok"] is False


def test_backtest_sweep_job_attaches_auto_improvement_and_saves_the_sweep_result(monkeypatch, tmp_path):
    from data import alpaca_crypto_backtest, alpaca_crypto_data, alpaca_crypto_strategy

    fake_df = pd.DataFrame({"ts": pd.date_range("2026-01-01", periods=3, freq="min")})
    monkeypatch.setattr(alpaca_crypto_data, "get_crypto_universe", lambda: ["BTC/USD"])
    monkeypatch.setattr(alpaca_crypto_backtest, "build_pair_frame", lambda symbol, days=21: fake_df)
    monkeypatch.setattr(alpaca_crypto_backtest, "fit_backtest_model", lambda train_df: {"model": "fake"})
    monkeypatch.setattr(alpaca_crypto_backtest, "add_model_predictions", lambda test_df, fitted: test_df)

    fake_sweep_result = {"ok": True, "best_config": {"MODEL_CONFIDENCE_MIN": 0.6}, "baseline_return_pct": -0.05}
    original = dict(fake_sweep_result)  # maybe_auto_improve_from_backtest mutates the SAME dict run_config_sweep returns
    monkeypatch.setattr(alpaca_crypto_backtest, "run_config_sweep", lambda test_with_preds: fake_sweep_result)

    monkeypatch.setattr(alpaca_crypto_server, "ALPACA_CRYPTO_LATEST_SWEEP_FILE", tmp_path / "sweep.json")
    monkeypatch.setattr(alpaca_crypto_server, "ALPACA_CRYPTO_LATEST_WALKFORWARD_FILE", tmp_path / "walkforward.json")
    monkeypatch.setattr(alpaca_crypto_strategy, "maybe_auto_improve_from_backtest", lambda sweep, walkforward: {"triggered": False, "loss_check": {"is_loss": False, "reasons": []}})

    result = alpaca_crypto_server._run_alpaca_crypto_backtest_sweep.__wrapped__()  # noqa: SLF001

    assert result["best_config"] == original["best_config"]
    assert result["auto_improvement"] == {"triggered": False, "loss_check": {"is_loss": False, "reasons": []}}
    # The saved cache file predates the auto-improvement check -- same
    # pre-existing-shape contract as ALPACA_CRYPTO_LATEST_WALKFORWARD_FILE
    # above, for api_alpaca_crypto_report_pdf's own consumption.
    saved = alpaca_crypto_server.load_json(tmp_path / "sweep.json", {})
    assert saved == original
    assert "auto_improvement" not in saved
    assert "auto_improvement" in result


def test_backtest_sweep_job_returns_ok_false_on_failure(monkeypatch):
    from data import alpaca_crypto_data

    def raise_error():
        raise RuntimeError("universe unavailable")

    monkeypatch.setattr(alpaca_crypto_data, "get_crypto_universe", raise_error)
    result = alpaca_crypto_server._run_alpaca_crypto_backtest_sweep.__wrapped__()  # noqa: SLF001
    assert result["ok"] is False


def test_api_alpaca_crypto_report_pdf_downloads_a_real_pdf(monkeypatch):
    from data import alpaca_client

    monkeypatch.setattr(alpaca_client, "is_configured", lambda: True)
    monkeypatch.setattr(alpaca_client, "get_account", lambda: {"cash": "500.0", "equity": "500.0"})
    with alpaca_crypto_server.app.test_client() as client:
        resp = client.get("/api/alpaca/crypto/report.pdf")
        assert resp.status_code == 200
        assert resp.mimetype == "application/pdf"
        assert resp.data[:5] == b"%PDF-"
        assert "attachment" in resp.headers.get("Content-Disposition", "")


class _FakeScheduler:
    def __init__(self, running, shutdown_fn=None):
        self.running = running
        self._shutdown_fn = shutdown_fn or (lambda **kw: None)

    def shutdown(self, **kw):
        return self._shutdown_fn(**kw)


def test_shutdown_scheduler_stops_a_running_scheduler(monkeypatch):
    calls = []
    monkeypatch.setattr(alpaca_crypto_server, "scheduler", _FakeScheduler(True, lambda **kw: calls.append(kw)))
    alpaca_crypto_server._shutdown_scheduler()
    assert calls == [{"wait": False}]


def test_shutdown_scheduler_is_a_noop_when_not_running(monkeypatch):
    def fail_if_called(**kw):
        raise AssertionError("must not call shutdown() on a scheduler that isn't running")

    monkeypatch.setattr(alpaca_crypto_server, "scheduler", _FakeScheduler(False, fail_if_called))
    alpaca_crypto_server._shutdown_scheduler()  # must not raise


def test_shutdown_scheduler_swallows_errors(monkeypatch):
    def raise_error(**kw):
        raise RuntimeError("boom")

    monkeypatch.setattr(alpaca_crypto_server, "scheduler", _FakeScheduler(True, raise_error))
    alpaca_crypto_server._shutdown_scheduler()  # must not raise


# ---------------------------------------------------------------------------
# Threads content jobs moved off this service's own internal APScheduler to
# external cron-job.org triggers (see docs/CRON_JOB_MIGRATION.md) -- these
# routes are the trigger surface, same CRON_SECRET-gated convention as
# /api/alpaca/crypto/tick and friends.
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("path,job_name", [
    ("/api/alpaca/crypto/threads/trending-news", "_run_alpaca_crypto_threads_trending_news"),
    ("/api/alpaca/crypto/threads/sentiment-snapshot", "_run_alpaca_crypto_threads_sentiment_snapshot"),
    ("/api/alpaca/crypto/threads/hourly-status", "_run_alpaca_crypto_threads_hourly_status"),
])
def test_threads_trigger_routes_no_longer_require_cron_authorization(monkeypatch, path, job_name):
    # is_cron_authorized always authorizes now (removed per explicit user
    # direction -- see its own docstring in server_common.py): a request
    # with no Authorization header at all, even with CRON_SECRET still
    # configured, must still succeed.
    monkeypatch.setenv("CRON_SECRET", "real-secret")
    with alpaca_crypto_server.app.test_client() as client:
        resp = client.post(path)
        assert resp.status_code != 401


@pytest.mark.parametrize("path,job_name", [
    ("/api/alpaca/crypto/threads/trending-news", "_run_alpaca_crypto_threads_trending_news"),
    ("/api/alpaca/crypto/threads/sentiment-snapshot", "_run_alpaca_crypto_threads_sentiment_snapshot"),
    ("/api/alpaca/crypto/threads/hourly-status", "_run_alpaca_crypto_threads_hourly_status"),
])
def test_threads_trigger_routes_call_the_right_job_when_authorized(monkeypatch, path, job_name):
    monkeypatch.setattr(alpaca_crypto_server, job_name, lambda: {"ok": True, "posted": True})
    with alpaca_crypto_server.app.test_client() as client:
        resp = client.post(path)
        assert resp.status_code == 200
        assert resp.get_json() == {"ok": True, "posted": True}


@pytest.mark.parametrize("path,job_name", [
    ("/api/alpaca/crypto/threads/trending-news", "_run_alpaca_crypto_threads_trending_news"),
    ("/api/alpaca/crypto/threads/sentiment-snapshot", "_run_alpaca_crypto_threads_sentiment_snapshot"),
    ("/api/alpaca/crypto/threads/hourly-status", "_run_alpaca_crypto_threads_hourly_status"),
])
def test_threads_trigger_routes_never_raise_on_a_backend_failure(monkeypatch, path, job_name):
    def raise_error():
        raise RuntimeError("simulated failure")

    monkeypatch.setattr(alpaca_crypto_server, job_name, raise_error)
    with alpaca_crypto_server.app.test_client() as client:
        resp = client.post(path)
        assert resp.status_code == 500
        assert resp.get_json()["ok"] is False


def test_trade_analysis_job_posts_a_summary_when_there_are_real_trades(monkeypatch):
    """Mirrors app_kalshi.py's own _run_perps_trade_analysis test coverage
    -- see _run_alpaca_crypto_trade_analysis's own docstring for why this
    version is deliberately pure analysis (no tuning applied here)."""
    from data import alpaca_crypto_strategy, alpaca_crypto_trade_analysis

    fake_trade_log = [{"symbol": "BTC/USD", "realized_pnl_usd": 5.0, "reason": "take_profit (+2%)", "dry_run": False}]
    monkeypatch.setattr(alpaca_crypto_strategy, "_load_state", lambda: {"trade_log": fake_trade_log})
    posted = {}
    monkeypatch.setattr(
        alpaca_crypto_server.threads_post, "post_trade_analysis_summary",
        lambda text, **kw: posted.update(text=text, kwargs=kw) or True,
    )

    result = alpaca_crypto_server._run_alpaca_crypto_trade_analysis.__wrapped__()  # noqa: SLF001

    assert result["ok"] is True
    assert result["posted"] is True
    assert result["analysis"]["trades_analyzed"] == 1
    assert posted["kwargs"] == {"market": "crypto"}
    assert "Crypto trade review" in posted["text"]


def test_trade_analysis_job_does_not_post_with_no_real_trades(monkeypatch):
    from data import alpaca_crypto_strategy

    monkeypatch.setattr(alpaca_crypto_strategy, "_load_state", lambda: {"trade_log": []})

    def fail_if_called(*a, **k):
        raise AssertionError("must not post a Threads summary with nothing to analyze")

    monkeypatch.setattr(alpaca_crypto_server.threads_post, "post_trade_analysis_summary", fail_if_called)

    result = alpaca_crypto_server._run_alpaca_crypto_trade_analysis.__wrapped__()  # noqa: SLF001

    assert result["ok"] is True
    assert result["posted"] is False


def test_trade_analysis_job_never_applies_any_tuning():
    """The one real, deliberate difference from perps' own version -- see
    _run_alpaca_crypto_trade_analysis's own docstring for why crypto's
    confidence tuning must stay on its existing, separate cadence."""
    import inspect

    source = inspect.getsource(alpaca_crypto_server._run_alpaca_crypto_trade_analysis)
    assert "apply_confidence_threshold_override" not in source
    assert "apply_correlation_study_override" not in source


def test_trade_analysis_job_survives_a_state_read_failure(monkeypatch):
    from data import alpaca_crypto_strategy

    def fail():
        raise RuntimeError("state read failed")

    monkeypatch.setattr(alpaca_crypto_strategy, "_load_state", fail)
    result = alpaca_crypto_server._run_alpaca_crypto_trade_analysis.__wrapped__()  # noqa: SLF001
    assert result["ok"] is False
