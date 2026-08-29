"""Perps-server-specific job wiring + scheduler shutdown behavior. Generic
job-locking mechanics (shared with alpaca_server.py) are covered in
test_server_common.py instead -- this file only tests things that are
actually specific to app_kalshi.py: that its production job functions
honor the live-trading dry_run gate, that its data-collect job refreshes the
volatility-ranking cache off the request path, and that its scheduler
shutdown handler is safe."""
from __future__ import annotations

import pandas as pd
import pytest

import app_kalshi


def test_production_jobs_actually_honor_the_live_trading_flag(monkeypatch):
    """perps_strategy's dry_run default is safe-by-default (None -> True)
    specifically so ad-hoc/manual callers never go live by accident -- but
    that means the REAL production scheduler must explicitly pass
    dry_run=False, or KALSHI_PERPS_LIVE_TRADING_ENABLED=1 would silently do
    nothing forever. Lock in that the three production entry points
    (the two scheduled jobs + the manual tick handler) all pass it."""
    from data import perps_strategy as strat

    captured = {}
    monkeypatch.setattr(strat, "manage_open_positions", lambda **kw: captured.setdefault("fast_check", kw) or {"action": "no_position"})
    monkeypatch.setattr(strat, "scan_and_enter", lambda **kw: captured.setdefault("entry_scan", kw) or {"action": "none"})
    monkeypatch.setattr(strat, "run_cycle", lambda **kw: captured.setdefault("manual_cycle", kw) or {})

    app_kalshi._run_perps_fast_check.__wrapped__()  # noqa: SLF001
    app_kalshi._run_perps_entry_scan.__wrapped__()  # noqa: SLF001
    app_kalshi._run_perps_manual_cycle.__wrapped__()  # noqa: SLF001

    assert captured["fast_check"].get("dry_run") is False
    assert captured["entry_scan"].get("dry_run") is False
    assert captured["manual_cycle"].get("dry_run") is False


def test_api_status_surfaces_feature_importances_from_the_trained_model(monkeypatch):
    """Real bug found in review: perps_model.train_model() computes and
    persists feature_importances (same observability already proven on
    every Alpaca service), but this route's hand-built "model" dict never
    forwarded it to the JSON response -- meaning the field was silently
    unreachable via the API even though the data existed in meta."""
    from data import perps_model

    fake_meta = {
        "model_type": "random_forest", "trained_at": 1700000000.0, "rows": 500,
        "scores": {"random_forest": {"accuracy": 0.55, "auc": 0.56}},
        "feature_importances": {"random_forest": {"sentiment_score": 0.05}},
    }
    monkeypatch.setattr(perps_model, "load_model", lambda: (object(), fake_meta))

    with app_kalshi.app.test_client() as client:
        resp = client.get("/api/status")
        assert resp.status_code == 200
        assert resp.get_json()["model"]["feature_importances"] == {"random_forest": {"sentiment_score": 0.05}}


def test_api_status_attaches_unrealized_pnl_and_exit_check_to_open_positions(monkeypatch):
    """Real gap found in review: the dashboard showed entry price + static
    TP/SL levels for every open position but never its CURRENT price,
    unrealized P&L, or the real exit_check reason text -- even though
    manage_open_positions() already computes both every fast_check cycle.
    A long position up 5%: entry 6.60, current 6.93, count 10 -> +$3.30."""
    from data import perps_strategy as strat

    monkeypatch.setattr(strat, "_load_state", lambda: {
        "positions": [{
            "ticker": "KXBTCPERP", "side": "long", "entry_price": 6.60, "count": 10.0,
            "opened_at": "2026-08-19T00:00:00+00:00",
        }],
        "trade_log": [], "realized_pnl_by_date": {},
    })

    def fake_load_json(path, default):
        if str(path).endswith("perps_latest_position_check.json"):
            return {"checks": [{"ticker": "KXBTCPERP", "ok": True, "exit_check": "holding (+5.00%)", "current_price": 6.93}]}
        return default

    monkeypatch.setattr(app_kalshi, "load_json", fake_load_json)

    with app_kalshi.app.test_client() as client:
        resp = client.get("/api/status")
        assert resp.status_code == 200
        position = resp.get_json()["positions"][0]
        assert position["current_price"] == 6.93
        assert position["unrealized_pnl_usd"] == pytest.approx(3.30, abs=0.01)
        assert position["unrealized_pnl_pct"] == pytest.approx(0.05, abs=0.001)
        assert position["exit_check"] == "holding (+5.00%)"


def test_api_status_unrealized_pnl_is_negated_for_a_short_position(monkeypatch):
    """A short profits when price FALLS -- entry 6.60, current 6.27 (-5%),
    count 10 -> a SHORT position must show a POSITIVE unrealized P&L here,
    not a negative one (same sign convention as the real exit-booking
    gross_pnl computation)."""
    from data import perps_strategy as strat

    monkeypatch.setattr(strat, "_load_state", lambda: {
        "positions": [{
            "ticker": "KXBTCPERP", "side": "short", "entry_price": 6.60, "count": 10.0,
            "opened_at": "2026-08-19T00:00:00+00:00",
        }],
        "trade_log": [], "realized_pnl_by_date": {},
    })

    def fake_load_json(path, default):
        if str(path).endswith("perps_latest_position_check.json"):
            return {"checks": [{"ticker": "KXBTCPERP", "ok": True, "exit_check": "holding", "current_price": 6.27}]}
        return default

    monkeypatch.setattr(app_kalshi, "load_json", fake_load_json)

    with app_kalshi.app.test_client() as client:
        resp = client.get("/api/status")
        position = resp.get_json()["positions"][0]
        assert position["unrealized_pnl_usd"] == pytest.approx(3.30, abs=0.01)


def test_api_status_omits_unrealized_pnl_when_no_current_price_is_available(monkeypatch):
    """No matching check for this ticker yet (e.g. right after a fresh
    adopt/reconcile, before the next fast_check cycle runs) -- must not
    crash or fabricate a P&L number from missing data."""
    from data import perps_strategy as strat

    monkeypatch.setattr(strat, "_load_state", lambda: {
        "positions": [{
            "ticker": "KXBTCPERP", "side": "long", "entry_price": 6.60, "count": 10.0,
            "opened_at": "2026-08-19T00:00:00+00:00",
        }],
        "trade_log": [], "realized_pnl_by_date": {},
    })
    monkeypatch.setattr(app_kalshi, "load_json", lambda path, default: default)

    with app_kalshi.app.test_client() as client:
        resp = client.get("/api/status")
        position = resp.get_json()["positions"][0]
        assert "unrealized_pnl_usd" not in position


def test_perps_report_pdf_route_returns_a_downloadable_pdf(monkeypatch):
    from data import perps_strategy

    monkeypatch.setattr(perps_strategy, "_load_state", lambda: {
        "positions": [], "trade_log": [], "realized_pnl_by_date": {},
    })
    monkeypatch.setattr(app_kalshi, "_cached_account_snapshot", lambda: {"available_balance_usd": 22.18})

    with app_kalshi.app.test_client() as client:
        resp = client.get("/api/perps/report.pdf")
        assert resp.status_code == 200
        assert resp.mimetype == "application/pdf"
        assert resp.data[:5] == b"%PDF-"
        assert "attachment" in resp.headers.get("Content-Disposition", "")
        assert ".pdf" in resp.headers.get("Content-Disposition", "")


def test_api_threads_posts_serves_the_durable_archive(monkeypatch):
    from data import threads_client

    monkeypatch.setattr(threads_client, "get_posts_archive", lambda: [
        {"id": "p3", "text": "third"}, {"id": "p2", "text": "second"}, {"id": "p1", "text": "first"},
    ])
    with app_kalshi.app.test_client() as client:
        resp = client.get("/api/threads/posts")
        body = resp.get_json()
        assert resp.status_code == 200
        assert body["ok"] is True
        assert body["count"] == 3
        assert [p["id"] for p in body["posts"]] == ["p3", "p2", "p1"]
        assert resp.headers["Access-Control-Allow-Origin"] == "*"


def test_api_threads_posts_respects_limit_and_since_id(monkeypatch):
    from data import threads_client

    monkeypatch.setattr(threads_client, "get_posts_archive", lambda: [
        {"id": "p4"}, {"id": "p3"}, {"id": "p2"}, {"id": "p1"},
    ])
    with app_kalshi.app.test_client() as client:
        resp = client.get("/api/threads/posts?since_id=p2&limit=10")
        body = resp.get_json()
        assert [p["id"] for p in body["posts"]] == ["p4", "p3"]


def test_api_threads_posts_falls_back_to_a_live_fetch_when_the_archive_is_empty(monkeypatch):
    from data import threads_client

    monkeypatch.setattr(threads_client, "get_posts_archive", lambda: [])
    monkeypatch.setattr(threads_client, "list_recent_posts", lambda limit=50: [{"id": "live-1"}])
    with app_kalshi.app.test_client() as client:
        resp = client.get("/api/threads/posts")
        body = resp.get_json()
        assert body["posts"] == [{"id": "live-1"}]


def test_api_threads_posts_never_raises_on_a_backend_failure(monkeypatch):
    from data import threads_client

    def raise_error():
        raise RuntimeError("no valid token")

    monkeypatch.setattr(threads_client, "get_posts_archive", raise_error)
    with app_kalshi.app.test_client() as client:
        resp = client.get("/api/threads/posts")
        body = resp.get_json()
        assert resp.status_code == 200
        assert body["ok"] is False
        assert body["posts"] == []


def test_api_threads_posts_sync_requires_cron_authorization(monkeypatch):
    monkeypatch.setenv("CRON_SECRET", "real-secret")
    with app_kalshi.app.test_client() as client:
        resp = client.post("/api/threads/posts/sync")
        assert resp.status_code == 401


def test_api_threads_posts_sync_runs_when_authorized(monkeypatch):
    from data import threads_client

    monkeypatch.setenv("CRON_SECRET", "real-secret")
    monkeypatch.setattr(threads_client, "sync_posts_archive", lambda: {"new_posts": 2, "total_archived": 10})
    with app_kalshi.app.test_client() as client:
        resp = client.post("/api/threads/posts/sync", headers={"Authorization": "Bearer real-secret"})
        body = resp.get_json()
        assert resp.status_code == 200
        assert body == {"ok": True, "new_posts": 2, "total_archived": 10}


def test_api_threads_posts_sync_never_raises_on_a_backend_failure(monkeypatch):
    from data import threads_client

    def raise_error():
        raise RuntimeError("HF push failed")

    monkeypatch.setattr(threads_client, "sync_posts_archive", raise_error)
    with app_kalshi.app.test_client() as client:
        resp = client.post("/api/threads/posts/sync")
        body = resp.get_json()
        assert resp.status_code == 200
        assert body["ok"] is False


# ---------------------------------------------------------------------------
# Threads content jobs moved off this service's own internal APScheduler to
# external cron-job.org triggers (see docs/CRON_JOB_MIGRATION.md) -- these
# routes are the trigger surface, same CRON_SECRET-gated convention as
# /api/perps/tick and friends.
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("path,job_name", [
    ("/api/perps/threads/trending-news", "_run_perps_threads_trending_news"),
    ("/api/perps/threads/sentiment-snapshot", "_run_perps_threads_sentiment_snapshot"),
    ("/api/perps/threads/hourly-status", "_run_perps_threads_hourly_status"),
])
def test_threads_trigger_routes_require_cron_authorization(monkeypatch, path, job_name):
    monkeypatch.setenv("CRON_SECRET", "real-secret")
    with app_kalshi.app.test_client() as client:
        resp = client.post(path)
        assert resp.status_code == 401


@pytest.mark.parametrize("path,job_name", [
    ("/api/perps/threads/trending-news", "_run_perps_threads_trending_news"),
    ("/api/perps/threads/sentiment-snapshot", "_run_perps_threads_sentiment_snapshot"),
    ("/api/perps/threads/hourly-status", "_run_perps_threads_hourly_status"),
])
def test_threads_trigger_routes_call_the_right_job_when_authorized(monkeypatch, path, job_name):
    monkeypatch.setattr(app_kalshi, job_name, lambda: {"ok": True, "posted": True})
    with app_kalshi.app.test_client() as client:
        resp = client.post(path)
        assert resp.status_code == 200
        assert resp.get_json() == {"ok": True, "posted": True}


@pytest.mark.parametrize("path,job_name", [
    ("/api/perps/threads/trending-news", "_run_perps_threads_trending_news"),
    ("/api/perps/threads/sentiment-snapshot", "_run_perps_threads_sentiment_snapshot"),
    ("/api/perps/threads/hourly-status", "_run_perps_threads_hourly_status"),
])
def test_threads_trigger_routes_never_raise_on_a_backend_failure(monkeypatch, path, job_name):
    def raise_error():
        raise RuntimeError("simulated failure")

    monkeypatch.setattr(app_kalshi, job_name, raise_error)
    with app_kalshi.app.test_client() as client:
        resp = client.post(path)
        assert resp.status_code == 500
        assert resp.get_json()["ok"] is False


def test_data_collect_job_refreshes_ticker_activity_cache_off_the_request_path(monkeypatch):
    """The volatility-ranking cache must only ever be refreshed from here
    (a scheduled background job) -- confirmed live that refreshing it
    inline from /api/status caused a fresh Render OOM, since that request
    path could run concurrently with the startup training thread's own
    full-size archive load right when memory is already tightest."""
    from data import perps_data

    calls = []
    monkeypatch.setattr(perps_data, "refresh_ticker_activity_cache", lambda **kw: calls.append(kw))
    monkeypatch.setattr(perps_data, "collect_dataset_rows", lambda: pd.DataFrame())

    app_kalshi._run_perps_data_collect.__wrapped__()  # noqa: SLF001

    assert len(calls) == 1


def test_data_collect_job_still_collects_if_cache_refresh_fails(monkeypatch):
    from data import perps_data

    def fail():
        raise RuntimeError("HF archive listing failed")

    collected = []
    monkeypatch.setattr(perps_data, "refresh_ticker_activity_cache", fail)
    monkeypatch.setattr(perps_data, "collect_dataset_rows", lambda: collected.append(1) or pd.DataFrame())

    app_kalshi._run_perps_data_collect.__wrapped__()  # noqa: SLF001

    assert collected == [1]


def test_threads_hourly_status_job_reports_open_positions_with_held_minutes(monkeypatch):
    import datetime as dt
    from data import perps_strategy as strat, threads_post

    opened_at = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=30)).isoformat()
    monkeypatch.setattr(strat, "_load_state", lambda: {
        "positions": [{"ticker": "KXBTCPERP", "side": "long", "entry_price": 6.5, "opened_at": opened_at}],
        "realized_pnl_by_date": {},
    })
    monkeypatch.setattr(strat, "position_exit_levels", lambda p: {"take_profit_price": 6.6, "stop_loss_price": 6.4})
    captured = {}
    monkeypatch.setattr(threads_post, "post_hourly_status", lambda **kw: captured.update(kw) or True)

    result = app_kalshi._run_perps_threads_hourly_status.__wrapped__()  # noqa: SLF001

    assert result["ok"] is True
    assert result["open_position_count"] == 1
    assert captured["positions"][0]["ticker"] == "KXBTCPERP"
    assert captured["positions"][0]["take_profit_price"] == 6.6
    assert captured["positions"][0]["held_minutes"] == pytest.approx(30.0, abs=1.0)


def test_threads_hourly_status_job_reports_flat_with_no_positions(monkeypatch):
    from data import perps_strategy as strat, threads_post

    monkeypatch.setattr(strat, "_load_state", lambda: {"positions": [], "realized_pnl_by_date": {}})
    captured = {}
    monkeypatch.setattr(threads_post, "post_hourly_status", lambda **kw: captured.update(kw) or True)

    result = app_kalshi._run_perps_threads_hourly_status.__wrapped__()  # noqa: SLF001

    assert result["ok"] is True
    assert result["open_position_count"] == 0
    assert captured["positions"] == []


def test_threads_hourly_status_job_never_raises_on_failure(monkeypatch):
    from data import perps_strategy as strat

    def raise_error():
        raise RuntimeError("state file corrupted")

    monkeypatch.setattr(strat, "_load_state", raise_error)
    result = app_kalshi._run_perps_threads_hourly_status.__wrapped__()  # noqa: SLF001
    assert result["ok"] is False


def test_threads_trending_news_job_skips_posting_since_alpaca_crypto_owns_this_beat(monkeypatch):
    """Real, confirmed duplication bug: this job and Alpaca crypto's own
    trending-news job both posted the SAME crypto story to the SAME
    shared Threads account (all 4 services share one account). Fixed by
    having this job intentionally no-op -- must never call
    crypto_news.get_trending_story()/threads_post.post_trending_news at all."""
    from data import crypto_news, threads_post

    def fail_if_called(*a, **k):
        raise AssertionError("must not fetch/post -- alpaca_crypto owns this beat now")

    monkeypatch.setattr(crypto_news, "get_trending_story", fail_if_called)
    monkeypatch.setattr(threads_post, "post_trending_news", fail_if_called)

    result = app_kalshi._run_perps_threads_trending_news.__wrapped__()  # noqa: SLF001

    assert result == {"ok": True, "posted": False, "action": "skipped_duplicate_beat", "owner": "alpaca_crypto"}


def test_threads_sentiment_snapshot_job_posts_per_ticker_sentiment(monkeypatch):
    from data import crypto_news, perps_data, threads_post

    monkeypatch.setattr(perps_data, "get_watchlist", lambda: ["KXBTCPERP", "KXETHPERP"])
    monkeypatch.setattr(perps_data, "coin_for_ticker", lambda ticker: {"KXBTCPERP": "BTC", "KXETHPERP": "ETH"}[ticker])
    monkeypatch.setattr(crypto_news, "get_sentiment", lambda coin, **kw: {"coin": coin, "sentiment_score": 0.5 if coin == "BTC" else -0.2})

    captured = {}
    monkeypatch.setattr(threads_post, "post_sentiment_snapshot", lambda *, market, ticker_sentiments: captured.update(market=market, ticker_sentiments=ticker_sentiments) or True)

    result = app_kalshi._run_perps_threads_sentiment_snapshot.__wrapped__()  # noqa: SLF001

    assert result == {"ok": True, "posted": True, "ticker_count": 2}
    assert captured["market"] == "perps"
    assert {"ticker": "KXBTCPERP", "sentiment_score": 0.5} in captured["ticker_sentiments"]
    assert {"ticker": "KXETHPERP", "sentiment_score": -0.2} in captured["ticker_sentiments"]


def test_threads_sentiment_snapshot_job_one_ticker_failing_does_not_block_the_others(monkeypatch):
    from data import crypto_news, perps_data, threads_post

    monkeypatch.setattr(perps_data, "get_watchlist", lambda: ["KXBADPERP", "KXBTCPERP"])

    def fake_coin_for_ticker(ticker):
        if ticker == "KXBADPERP":
            raise RuntimeError("unmapped ticker")
        return "BTC"

    monkeypatch.setattr(perps_data, "coin_for_ticker", fake_coin_for_ticker)
    monkeypatch.setattr(crypto_news, "get_sentiment", lambda coin, **kw: {"coin": coin, "sentiment_score": 0.1})
    captured = {}
    monkeypatch.setattr(threads_post, "post_sentiment_snapshot", lambda *, market, ticker_sentiments: captured.update(ticker_sentiments=ticker_sentiments) or True)

    result = app_kalshi._run_perps_threads_sentiment_snapshot.__wrapped__()  # noqa: SLF001
    assert result["ok"] is True
    assert result["ticker_count"] == 1
    assert captured["ticker_sentiments"] == [{"ticker": "KXBTCPERP", "sentiment_score": 0.1}]


def test_threads_sentiment_snapshot_job_never_raises_on_failure(monkeypatch):
    from data import perps_data

    def raise_error():
        raise RuntimeError("watchlist unavailable")

    monkeypatch.setattr(perps_data, "get_watchlist", raise_error)
    result = app_kalshi._run_perps_threads_sentiment_snapshot.__wrapped__()  # noqa: SLF001
    assert result["ok"] is False


class _FakeScheduler:
    def __init__(self, running, shutdown_fn=None):
        self.running = running
        self._shutdown_fn = shutdown_fn or (lambda **kw: None)

    def shutdown(self, **kw):
        return self._shutdown_fn(**kw)


def test_shutdown_scheduler_stops_a_running_scheduler(monkeypatch):
    """Confirmed live: on SIGTERM (a normal restart/redeploy), APScheduler's
    own background thread could still be mid-cycle and try to submit a job
    to its thread pool right as the interpreter tears it down, raising
    "cannot schedule new futures after interpreter shutdown". Shutting the
    scheduler down at exit prevents that race."""
    calls = []
    monkeypatch.setattr(app_kalshi, "scheduler", _FakeScheduler(True, lambda **kw: calls.append(kw)))
    app_kalshi._shutdown_scheduler()
    assert calls == [{"wait": False}]


def test_shutdown_scheduler_is_a_noop_when_not_running(monkeypatch):
    def fail_if_called(**kw):
        raise AssertionError("must not call shutdown() on a scheduler that isn't running")

    monkeypatch.setattr(app_kalshi, "scheduler", _FakeScheduler(False, fail_if_called))
    app_kalshi._shutdown_scheduler()  # must not raise


def test_shutdown_scheduler_swallows_errors(monkeypatch):
    """This runs at interpreter shutdown -- it must never itself raise and
    block/interfere with the process actually exiting."""
    def raise_error(**kw):
        raise RuntimeError("boom")

    monkeypatch.setattr(app_kalshi, "scheduler", _FakeScheduler(True, raise_error))
    app_kalshi._shutdown_scheduler()  # must not raise


def test_run_perps_train_passes_the_real_trade_log_for_outcome_aware_weighting(monkeypatch):
    """perps_model.py never imports perps_strategy.py directly (circular
    import risk -- see this job's own comment), so app_kalshi.py is
    responsible for reading trade_log and threading it through."""
    from data import perps_model, perps_strategy

    fake_trade_log = [{"ticker": "KXBTCPERP", "opened_at": "x", "realized_pnl_usd": 1.0, "dry_run": False}]
    monkeypatch.setattr(perps_strategy, "_load_state", lambda: {"trade_log": fake_trade_log})
    captured = {}
    monkeypatch.setattr(perps_model, "train_model", lambda **kw: captured.update(kw) or {"ok": True})

    app_kalshi._run_perps_train.__wrapped__()  # noqa: SLF001

    assert captured["trade_log"] == fake_trade_log


def test_run_perps_train_survives_a_state_read_failure(monkeypatch):
    from data import perps_model, perps_strategy

    def fail():
        raise RuntimeError("state read failed")

    monkeypatch.setattr(perps_strategy, "_load_state", fail)
    captured = {}
    monkeypatch.setattr(perps_model, "train_model", lambda **kw: captured.update(kw) or {"ok": True})

    result = app_kalshi._run_perps_train.__wrapped__()  # noqa: SLF001

    assert result["ok"] is True
    assert captured["trade_log"] is None  # degrades to plain (non-outcome-weighted) training, doesn't crash the job


def test_run_perps_trade_analysis_posts_a_summary_and_applies_evidence_gated_tuning(monkeypatch):
    from data import perps_strategy, perps_trade_analysis, threads_post

    monkeypatch.setattr(perps_strategy, "_load_state", lambda: {"trade_log": [{"fake": "trade"}], "tuning": {}})
    monkeypatch.setattr(perps_strategy, "MODEL_CONFIDENCE_MIN", 0.58)
    monkeypatch.setattr(
        perps_trade_analysis, "analyze_trade_history",
        lambda trade_log, **kw: {"ok": True, "trades_analyzed": 20, "overall": {"win_rate": 0.6, "total_pnl_usd": 1.0, "avg_pnl_usd": 0.05}, "insights": []},
    )
    monkeypatch.setattr(
        perps_trade_analysis, "recommend_confidence_threshold",
        lambda trade_log, **kw: {"should_apply": True, "recommended_threshold": 0.63, "current_threshold": 0.58, "candidate": {"trades": 16}, "baseline": {"trades": 19}},
    )
    applied = {}
    monkeypatch.setattr(perps_strategy, "apply_confidence_threshold_override", lambda threshold, *, reason: applied.update(threshold=threshold, reason=reason) or {"model_confidence_min": threshold})
    posted = {}

    def fake_post(text, **kw):
        posted["text"] = text
        return True

    monkeypatch.setattr(threads_post, "post_trade_analysis_summary", fake_post)

    result = app_kalshi._run_perps_trade_analysis.__wrapped__()  # noqa: SLF001

    assert result["ok"] is True
    assert applied["threshold"] == 0.63
    assert result["posted"] is True
    assert "text" in posted


def test_run_perps_trade_analysis_applies_a_position_management_trial(monkeypatch):
    from data import perps_strategy, perps_trade_analysis, threads_post

    monkeypatch.setattr(perps_strategy, "_load_state", lambda: {"trade_log": [{"fake": "trade"}], "tuning": {}})
    monkeypatch.setattr(perps_strategy, "MODEL_CONFIDENCE_MIN", 0.58)
    monkeypatch.setattr(
        perps_trade_analysis, "analyze_trade_history",
        lambda trade_log, **kw: {"ok": True, "trades_analyzed": 20, "overall": {"win_rate": 0.6, "total_pnl_usd": 1.0, "avg_pnl_usd": 0.05}, "insights": []},
    )
    monkeypatch.setattr(perps_trade_analysis, "recommend_confidence_threshold", lambda trade_log, **kw: {"should_apply": False, "reason": "insufficient_trade_history"})
    monkeypatch.setattr(perps_trade_analysis, "recommend_correlation_study_weight", lambda trade_log, **kw: {"should_apply": False, "reason": "insufficient_trade_history"})

    def fake_recommend(trade_log, *, feature, current_enabled):
        if feature == "partial_exit":
            return {"should_apply": True, "action": "start_trial", "recommended_enabled": True}
        return {"should_apply": False, "reason": "insufficient_trade_history"}

    monkeypatch.setattr(perps_trade_analysis, "recommend_position_management_trial", fake_recommend)
    applied = {}
    monkeypatch.setattr(
        perps_strategy, "apply_position_management_override",
        lambda feature, *, enabled, reason: applied.update(feature=feature, enabled=enabled, reason=reason) or {"partial_exit_enabled": enabled},
    )
    monkeypatch.setattr(threads_post, "post_trade_analysis_summary", lambda text, **kw: True)

    result = app_kalshi._run_perps_trade_analysis.__wrapped__()  # noqa: SLF001

    assert result["ok"] is True
    assert applied["feature"] == "partial_exit"
    assert applied["enabled"] is True
    assert result["position_management_applied"]["partial_exit"] == {"partial_exit_enabled": True}


def test_run_perps_trade_analysis_does_not_apply_tuning_when_evidence_is_thin(monkeypatch):
    from data import perps_strategy, perps_trade_analysis, threads_post

    monkeypatch.setattr(perps_strategy, "_load_state", lambda: {"trade_log": [], "tuning": {}})
    monkeypatch.setattr(perps_strategy, "MODEL_CONFIDENCE_MIN", 0.58)
    monkeypatch.setattr(
        perps_trade_analysis, "analyze_trade_history",
        lambda trade_log, **kw: {"ok": True, "trades_analyzed": 0, "overall": {}, "insights": []},
    )
    monkeypatch.setattr(
        perps_trade_analysis, "recommend_confidence_threshold",
        lambda trade_log, **kw: {"should_apply": False, "reason": "insufficient_trade_history", "current_threshold": 0.58},
    )

    def fail_if_called(*a, **k):
        raise AssertionError("must not apply a tuning override without evidence")

    monkeypatch.setattr(perps_strategy, "apply_confidence_threshold_override", fail_if_called)
    post_calls = []
    monkeypatch.setattr(threads_post, "post_trade_analysis_summary", lambda text, **kw: post_calls.append(text) or True)

    result = app_kalshi._run_perps_trade_analysis.__wrapped__()  # noqa: SLF001

    assert result["ok"] is True
    assert result["tuning_applied"] is None
    assert post_calls == []  # nothing worth posting with zero trades analyzed


def test_run_perps_trade_analysis_survives_a_state_read_failure(monkeypatch):
    from data import perps_strategy

    def fail():
        raise RuntimeError("state read failed")

    monkeypatch.setattr(perps_strategy, "_load_state", fail)
    result = app_kalshi._run_perps_trade_analysis.__wrapped__()  # noqa: SLF001
    assert result["ok"] is False
