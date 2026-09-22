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
from data import kalshi_15m_data


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


def test_api_status_surfaces_correlation_study_health(monkeypatch):
    """Real diagnostic gap closed: whether the chart-study layer actually
    has enough coverage right now used to be invisible outside individual
    Threads posts' own "no ... data" reason text -- see
    crypto_correlation.study_health's own docstring."""
    from data import crypto_correlation

    monkeypatch.setattr(
        crypto_correlation, "get_perps_study",
        lambda: {"computed_at": "2026-01-01T00:00:00+00:00", "ids": ["BTC", "ETH"], "corr": {"BTC": {"ETH": 0.8}}, "divergence_z": {}, "breadth": 0.1},
    )
    monkeypatch.setattr(crypto_correlation, "get_remote_alpaca_study", lambda: {})

    with app_kalshi.app.test_client() as client:
        resp = client.get("/api/status")
        assert resp.status_code == 200
        health = resp.get_json()["correlation_study_health"]
        assert health["perps_study"] == {
            "computed_at": "2026-01-01T00:00:00+00:00", "num_ids": 2,
            "num_with_peer_data": 1, "num_with_divergence_data": 0, "breadth": 0.1,
        }
        assert health["remote_alpaca_study"] == {
            "computed_at": None, "num_ids": 0, "num_with_peer_data": 0,
            "num_with_divergence_data": 0, "breadth": None,
        }


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


def test_api_threads_posts_sync_no_longer_requires_cron_authorization(monkeypatch):
    # is_cron_authorized always authorizes now (removed per explicit user
    # direction -- see its own docstring in server_common.py).
    monkeypatch.setenv("CRON_SECRET", "real-secret")
    with app_kalshi.app.test_client() as client:
        resp = client.post("/api/threads/posts/sync")
        assert resp.status_code != 401


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
def test_threads_trigger_routes_no_longer_require_cron_authorization(monkeypatch, path, job_name):
    # is_cron_authorized always authorizes now (removed per explicit user
    # direction -- see its own docstring in server_common.py).
    monkeypatch.setenv("CRON_SECRET", "real-secret")
    with app_kalshi.app.test_client() as client:
        resp = client.post(path)
        assert resp.status_code != 401


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


# ---------------------------------------------------------------------------
# kalshi_15m_data_collect -- Kalshi's own 15-minute event-contract markets
# (a genuinely new product, see kalshi_15m.py's own module docstring).
# Simpler than perps' own data_collect job above (no ticker-activity-cache
# refresh, no correlation-study wiring -- see this job's own docstring).
# ---------------------------------------------------------------------------
def test_kalshi_15m_data_collect_job_pushes_a_snapshot_when_rows_are_collected(monkeypatch):
    df = pd.DataFrame({"symbol": ["BTC"], "ts": [1], "close": [100.0]})
    monkeypatch.setattr(kalshi_15m_data, "collect_dataset_rows", lambda: df)
    pushed = []
    monkeypatch.setattr(kalshi_15m_data, "push_dataset_snapshot", lambda d: pushed.append(d) or {"ok": True, "rows_written": 1})

    result = app_kalshi._run_kalshi_15m_data_collect.__wrapped__()  # noqa: SLF001

    assert result == {"ok": True, "rows_written": 1}
    assert len(pushed) == 1
    pd.testing.assert_frame_equal(pushed[0], df)


def test_kalshi_15m_data_collect_job_reports_no_rows_without_pushing(monkeypatch):
    monkeypatch.setattr(kalshi_15m_data, "collect_dataset_rows", lambda: pd.DataFrame())
    pushed = []
    monkeypatch.setattr(kalshi_15m_data, "push_dataset_snapshot", lambda d: pushed.append(d))

    result = app_kalshi._run_kalshi_15m_data_collect.__wrapped__()  # noqa: SLF001

    assert result == {"ok": False, "reason": "no_rows_collected"}
    assert pushed == []


def test_kalshi_15m_data_collect_job_survives_a_collection_failure(monkeypatch):
    def fail():
        raise RuntimeError("candle fetch failed")

    monkeypatch.setattr(kalshi_15m_data, "collect_dataset_rows", fail)

    with pytest.raises(RuntimeError):
        app_kalshi._run_kalshi_15m_data_collect.__wrapped__()  # noqa: SLF001
    # Not swallowed -- matches perps_data_collect's own contract (the
    # locked_job wrapper + scheduler's own exception handling is what
    # keeps one failed cycle from taking the process down, not the job
    # body itself pretending nothing went wrong).


# ---------------------------------------------------------------------------
# kalshi_15m_reconcile -- real gap this closes: the live collector only
# ever archives what it observes going forward, so a missed cycle (a
# restart, a transient API failure) is a permanent archive hole unless
# something re-heals it. Runs a small trailing-window backfill daily,
# crypto only (see KALSHI_15M_RECONCILE_DAYS's own comment).
# ---------------------------------------------------------------------------
def test_kalshi_15m_reconcile_job_calls_backfill_with_the_configured_window(monkeypatch):
    captured = {}
    monkeypatch.setattr(kalshi_15m_data, "backfill_minute_history", lambda **kw: captured.update(kw) or {"ok": True, "dates_written": 2})

    result = app_kalshi._run_kalshi_15m_reconcile.__wrapped__()  # noqa: SLF001

    assert result == {"ok": True, "dates_written": 2}
    assert captured == {"days": app_kalshi.KALSHI_15M_RECONCILE_DAYS}


def test_kalshi_15m_reconcile_job_reports_failure_without_raising(monkeypatch):
    monkeypatch.setattr(kalshi_15m_data, "backfill_minute_history", lambda **kw: {"ok": False, "reason": "no_hf_api_key"})
    result = app_kalshi._run_kalshi_15m_reconcile.__wrapped__()  # noqa: SLF001
    assert result == {"ok": False, "reason": "no_hf_api_key"}


def test_kalshi_15m_reconcile_hour_is_thirty_minutes_before_train_hour():
    # Locks in the wraparound arithmetic used at scheduler-registration
    # time (hour=0 train would need to wrap to 23:30 the previous day).
    assert divmod((4 * 60 - 30) % (24 * 60), 60) == (3, 30)
    assert divmod((0 * 60 - 30) % (24 * 60), 60) == (23, 30)


# ---------------------------------------------------------------------------
# kalshi_15m_metals_data_collect -- GOLD/SILVER/COPPER's own data
# collection (a genuinely different pipeline, see
# kalshi_15m_metals_data.py's own module docstring), same job contract.
# ---------------------------------------------------------------------------
def test_kalshi_15m_metals_data_collect_job_pushes_a_snapshot_when_rows_are_collected(monkeypatch):
    from data import kalshi_15m_metals_data

    df = pd.DataFrame({"symbol": ["GOLD"], "ts": [1], "close": [4379.0]})
    monkeypatch.setattr(kalshi_15m_metals_data, "collect_dataset_rows", lambda: df)
    pushed = []
    monkeypatch.setattr(kalshi_15m_metals_data, "push_dataset_snapshot", lambda d: pushed.append(d) or {"ok": True, "rows_written": 1})

    result = app_kalshi._run_kalshi_15m_metals_data_collect.__wrapped__()  # noqa: SLF001

    assert result == {"ok": True, "rows_written": 1}
    assert len(pushed) == 1
    pd.testing.assert_frame_equal(pushed[0], df)


def test_kalshi_15m_metals_data_collect_job_reports_no_rows_without_pushing(monkeypatch):
    from data import kalshi_15m_metals_data

    monkeypatch.setattr(kalshi_15m_metals_data, "collect_dataset_rows", lambda: pd.DataFrame())
    pushed = []
    monkeypatch.setattr(kalshi_15m_metals_data, "push_dataset_snapshot", lambda d: pushed.append(d))

    result = app_kalshi._run_kalshi_15m_metals_data_collect.__wrapped__()  # noqa: SLF001

    assert result == {"ok": False, "reason": "no_rows_collected"}
    assert pushed == []


def test_kalshi_15m_metals_data_collect_job_survives_a_collection_failure(monkeypatch):
    from data import kalshi_15m_metals_data

    def fail():
        raise RuntimeError("price fetch failed")

    monkeypatch.setattr(kalshi_15m_metals_data, "collect_dataset_rows", fail)

    with pytest.raises(RuntimeError):
        app_kalshi._run_kalshi_15m_metals_data_collect.__wrapped__()  # noqa: SLF001


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
    from data import perps_meta_model, perps_model, perps_strategy

    fake_trade_log = [{"ticker": "KXBTCPERP", "opened_at": "x", "realized_pnl_usd": 1.0, "dry_run": False}]
    monkeypatch.setattr(perps_strategy, "_load_state", lambda: {"trade_log": fake_trade_log})
    captured = {}
    monkeypatch.setattr(perps_model, "train_model", lambda **kw: captured.update(kw) or {"ok": True})
    monkeypatch.setattr(perps_meta_model, "train_meta_model", lambda **kw: {"ok": False, "reason": "no_data"})

    app_kalshi._run_perps_train.__wrapped__()  # noqa: SLF001

    assert captured["trade_log"] == fake_trade_log


def test_run_perps_train_survives_a_state_read_failure(monkeypatch):
    from data import perps_meta_model, perps_model, perps_strategy

    def fail():
        raise RuntimeError("state read failed")

    monkeypatch.setattr(perps_strategy, "_load_state", fail)
    captured = {}
    monkeypatch.setattr(perps_model, "train_model", lambda **kw: captured.update(kw) or {"ok": True})
    monkeypatch.setattr(perps_meta_model, "train_meta_model", lambda **kw: {"ok": False, "reason": "no_data"})

    result = app_kalshi._run_perps_train.__wrapped__()  # noqa: SLF001

    assert result["ok"] is True
    assert captured["trade_log"] is None  # degrades to plain (non-outcome-weighted) training, doesn't crash the job


# ── Meta-labeling training (see perps_meta_model.py's own module docstring)
# -- additive and best-effort: must never affect this job's own primary
# result either way. ─────────────────────────────────────────────────────

def test_run_perps_train_also_trains_the_meta_model_after_a_successful_primary_train(monkeypatch):
    from data import perps_meta_model, perps_model, perps_strategy

    monkeypatch.setattr(perps_strategy, "_load_state", lambda: {"trade_log": []})
    monkeypatch.setattr(perps_model, "train_model", lambda **kw: {"ok": True, "model_type": "random_forest"})
    called = []
    monkeypatch.setattr(perps_meta_model, "train_meta_model", lambda **kw: called.append(kw) or {"ok": True})

    result = app_kalshi._run_perps_train.__wrapped__()  # noqa: SLF001

    assert result["ok"] is True
    assert len(called) == 1


def test_run_perps_train_skips_the_meta_model_when_the_primary_train_failed(monkeypatch):
    """Nothing new to build out-of-fold labels from without a fresh primary
    model -- must not even attempt it."""
    from data import perps_meta_model, perps_model, perps_strategy

    monkeypatch.setattr(perps_strategy, "_load_state", lambda: {"trade_log": []})
    monkeypatch.setattr(perps_model, "train_model", lambda **kw: {"ok": False, "reason": "insufficient_rows"})

    def fail_if_called(**kw):
        raise AssertionError("must not train the meta-model after a failed primary train")

    monkeypatch.setattr(perps_meta_model, "train_meta_model", fail_if_called)

    result = app_kalshi._run_perps_train.__wrapped__()  # noqa: SLF001

    assert result["ok"] is False


def test_run_perps_train_survives_a_meta_model_training_failure(monkeypatch):
    """Best-effort only -- a meta-model training crash must never take down
    the primary job's own (already-successful) result."""
    from data import perps_meta_model, perps_model, perps_strategy

    monkeypatch.setattr(perps_strategy, "_load_state", lambda: {"trade_log": []})
    monkeypatch.setattr(perps_model, "train_model", lambda **kw: {"ok": True, "model_type": "random_forest"})

    def raise_error(**kw):
        raise RuntimeError("simulated meta-model training crash")

    monkeypatch.setattr(perps_meta_model, "train_meta_model", raise_error)

    result = app_kalshi._run_perps_train.__wrapped__()  # noqa: SLF001

    assert result["ok"] is True


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


# ---------------------------------------------------------------------------
# kalshi_15m_cycle / kalshi_15m_train
# ---------------------------------------------------------------------------
def test_kalshi_15m_cycle_job_checks_settlements_then_manages_then_scans_for_entries(monkeypatch):
    from data import kalshi_15m_strategy

    order = []
    monkeypatch.setattr(kalshi_15m_strategy, "check_settlements", lambda: order.append("settlements") or {"ok": True, "checks": []})
    monkeypatch.setattr(kalshi_15m_strategy, "manage_open_positions", lambda **kw: order.append("management") or {"ok": True, "checks": []})
    monkeypatch.setattr(kalshi_15m_strategy, "scan_and_enter", lambda **kw: order.append("entries") or {"ok": True, "checks": []})

    result = app_kalshi._run_kalshi_15m_cycle.__wrapped__()  # noqa: SLF001

    assert order == ["settlements", "management", "entries"]
    assert result["ok"] is True
    assert "management" in result


def test_kalshi_15m_cycle_job_never_bypasses_the_dry_run_floor(monkeypatch):
    """scan_and_enter/manage_open_positions are always called with
    dry_run=False here -- the real gate is
    kalshi_15m_strategy.LIVE_TRADING_ENABLED's own hard floor (for
    entries) and each function's own fresh-env re-check (for real order
    placement), not this job pretending to force live trading (same
    contract as every other market's identical fast_check/entry_scan
    job)."""
    from data import kalshi_15m_strategy

    captured = {}
    monkeypatch.setattr(kalshi_15m_strategy, "check_settlements", lambda: {"ok": True, "checks": []})
    monkeypatch.setattr(kalshi_15m_strategy, "manage_open_positions", lambda **kw: captured.update(management=kw) or {"ok": True, "checks": []})
    monkeypatch.setattr(kalshi_15m_strategy, "scan_and_enter", lambda **kw: captured.update(entries=kw) or {"ok": True, "checks": []})

    app_kalshi._run_kalshi_15m_cycle.__wrapped__()  # noqa: SLF001

    assert captured == {"management": {"dry_run": False}, "entries": {"dry_run": False}}


def test_kalshi_15m_train_job_passes_the_real_trade_log(monkeypatch):
    """Trains BOTH models (crypto + metals) off the SAME real trade_log --
    see _run_kalshi_15m_train's own docstring for why these are two
    independent models sharing one job/cadence."""
    from data import kalshi_15m_metals_model, kalshi_15m_model, kalshi_15m_strategy

    monkeypatch.setattr(kalshi_15m_strategy, "_load_state", lambda: {"trade_log": [{"coin": "BTC"}]})
    crypto_captured, metals_captured = {}, {}
    monkeypatch.setattr(kalshi_15m_model, "train_model", lambda **kw: crypto_captured.update(kw) or {"ok": True})
    monkeypatch.setattr(kalshi_15m_metals_model, "train_model", lambda **kw: metals_captured.update(kw) or {"ok": True})

    result = app_kalshi._run_kalshi_15m_train.__wrapped__()  # noqa: SLF001

    assert crypto_captured["trade_log"] == [{"coin": "BTC"}]
    assert metals_captured["trade_log"] == [{"coin": "BTC"}]
    assert result == {"ok": True, "crypto": {"ok": True}, "metals": {"ok": True}}


def test_kalshi_15m_train_job_metals_failure_does_not_block_crypto_training(monkeypatch):
    from data import kalshi_15m_metals_model, kalshi_15m_model, kalshi_15m_strategy

    monkeypatch.setattr(kalshi_15m_strategy, "_load_state", lambda: {"trade_log": []})
    monkeypatch.setattr(kalshi_15m_model, "train_model", lambda **kw: {"ok": True, "model_type": "gradient_boosting"})

    def fail(**kw):
        raise RuntimeError("insufficient metals data")

    monkeypatch.setattr(kalshi_15m_metals_model, "train_model", fail)

    result = app_kalshi._run_kalshi_15m_train.__wrapped__()  # noqa: SLF001

    assert result["crypto"] == {"ok": True, "model_type": "gradient_boosting"}
    assert result["metals"]["ok"] is False


def test_kalshi_15m_train_job_survives_a_state_read_failure(monkeypatch):
    from data import kalshi_15m_metals_model, kalshi_15m_model, kalshi_15m_strategy

    def fail():
        raise RuntimeError("state file corrupted")

    monkeypatch.setattr(kalshi_15m_strategy, "_load_state", fail)
    captured = {}
    monkeypatch.setattr(kalshi_15m_model, "train_model", lambda **kw: captured.update(kw) or {"ok": True})
    monkeypatch.setattr(kalshi_15m_metals_model, "train_model", lambda **kw: {"ok": True})

    app_kalshi._run_kalshi_15m_train.__wrapped__()  # noqa: SLF001
    assert captured["trade_log"] is None


# ── Meta-labeling training (see kalshi_15m_meta_model.py's own module
# docstring) -- additive and best-effort: must never affect this job's
# own primary result either way. Mirrors _run_perps_train's own identical
# 3-test coverage. ────────────────────────────────────────────────────

def test_run_kalshi_15m_train_also_trains_the_meta_model_after_a_successful_primary_train(monkeypatch):
    from data import kalshi_15m_meta_model, kalshi_15m_metals_model, kalshi_15m_model, kalshi_15m_strategy

    monkeypatch.setattr(kalshi_15m_strategy, "_load_state", lambda: {"trade_log": []})
    monkeypatch.setattr(kalshi_15m_model, "train_model", lambda **kw: {"ok": True, "model_type": "random_forest"})
    monkeypatch.setattr(kalshi_15m_metals_model, "train_model", lambda **kw: {"ok": True})
    called = []
    monkeypatch.setattr(kalshi_15m_meta_model, "train_meta_model", lambda **kw: called.append(kw) or {"ok": True})

    result = app_kalshi._run_kalshi_15m_train.__wrapped__()  # noqa: SLF001

    assert result["ok"] is True
    assert len(called) == 1


def test_run_kalshi_15m_train_skips_the_meta_model_when_the_primary_crypto_train_failed(monkeypatch):
    """Nothing new to build out-of-fold labels from without a fresh
    primary CRYPTO model -- must not even attempt it (a metals-only
    success is not enough; no metals meta-model exists)."""
    from data import kalshi_15m_meta_model, kalshi_15m_metals_model, kalshi_15m_model, kalshi_15m_strategy

    monkeypatch.setattr(kalshi_15m_strategy, "_load_state", lambda: {"trade_log": []})
    monkeypatch.setattr(kalshi_15m_model, "train_model", lambda **kw: {"ok": False, "reason": "insufficient_rows"})
    monkeypatch.setattr(kalshi_15m_metals_model, "train_model", lambda **kw: {"ok": True})

    def fail_if_called(**kw):
        raise AssertionError("must not train the meta-model after a failed primary crypto train")

    monkeypatch.setattr(kalshi_15m_meta_model, "train_meta_model", fail_if_called)

    app_kalshi._run_kalshi_15m_train.__wrapped__()  # noqa: SLF001


def test_run_kalshi_15m_train_survives_a_meta_model_training_failure(monkeypatch):
    """Best-effort only -- a meta-model training crash must never take
    down the primary job's own (already-successful) result."""
    from data import kalshi_15m_meta_model, kalshi_15m_metals_model, kalshi_15m_model, kalshi_15m_strategy

    monkeypatch.setattr(kalshi_15m_strategy, "_load_state", lambda: {"trade_log": []})
    monkeypatch.setattr(kalshi_15m_model, "train_model", lambda **kw: {"ok": True, "model_type": "random_forest"})
    monkeypatch.setattr(kalshi_15m_metals_model, "train_model", lambda **kw: {"ok": True})

    def raise_error(**kw):
        raise RuntimeError("simulated meta-model training crash")

    monkeypatch.setattr(kalshi_15m_meta_model, "train_meta_model", raise_error)

    result = app_kalshi._run_kalshi_15m_train.__wrapped__()  # noqa: SLF001
    assert result["ok"] is True


# ---------------------------------------------------------------------------
# kalshi_15m_backtest -- regularly-scheduled walk-forward backtest +
# forward test, per explicit user direction: "it need a backtest and a
# forward test to be regularly implemented." Auto-retrains on a
# confirmed losing result, same "use everything the bot has as a
# resource" posture as every sibling market's own equivalent.
# ---------------------------------------------------------------------------
def test_kalshi_15m_backtest_job_saves_a_winning_result_without_retraining(monkeypatch):
    from data import kalshi_15m_backtest, kalshi_15m_model

    monkeypatch.setattr(kalshi_15m_backtest, "run_walkforward_backtest", lambda: {"ok": True, "mean_return_pct": 0.02})

    def fail_if_called(**kw):
        raise AssertionError("must not retrain after a winning backtest")

    monkeypatch.setattr(kalshi_15m_model, "train_model", fail_if_called)
    monkeypatch.setattr(kalshi_15m_model, "train_torch_candidate_model", fail_if_called)

    result = app_kalshi._run_kalshi_15m_backtest.__wrapped__()  # noqa: SLF001

    assert result["ok"] is True
    assert "auto_retrain" not in result


def test_kalshi_15m_backtest_job_retrains_both_models_on_a_losing_result(monkeypatch):
    from data import kalshi_15m_backtest, kalshi_15m_model, kalshi_15m_strategy

    monkeypatch.setattr(kalshi_15m_backtest, "run_walkforward_backtest", lambda: {"ok": True, "mean_return_pct": -0.03})
    monkeypatch.setattr(kalshi_15m_strategy, "_load_state", lambda: {"trade_log": []})
    monkeypatch.setattr(kalshi_15m_model, "train_model", lambda **kw: {"ok": True, "rows": 1000})
    monkeypatch.setattr(kalshi_15m_model, "train_torch_candidate_model", lambda **kw: {"ok": True, "promoted": False})

    result = app_kalshi._run_kalshi_15m_backtest.__wrapped__()  # noqa: SLF001

    assert result["ok"] is True
    assert result["auto_retrain"] == {"ok": True, "rows": 1000}
    assert result["auto_torch_retrain"] == {"ok": True, "promoted": False}


def test_kalshi_15m_backtest_job_survives_a_backtest_failure(monkeypatch):
    from data import kalshi_15m_backtest

    def raise_error():
        raise RuntimeError("simulated backtest crash")

    monkeypatch.setattr(kalshi_15m_backtest, "run_walkforward_backtest", raise_error)
    result = app_kalshi._run_kalshi_15m_backtest.__wrapped__()  # noqa: SLF001
    assert result["ok"] is False


def test_kalshi_15m_backtest_job_survives_a_retrain_failure_after_a_losing_result(monkeypatch):
    """Best-effort only -- a retrain crash must never take down the
    backtest job's own (already-computed) result."""
    from data import kalshi_15m_backtest, kalshi_15m_model, kalshi_15m_strategy

    monkeypatch.setattr(kalshi_15m_backtest, "run_walkforward_backtest", lambda: {"ok": True, "mean_return_pct": -0.03})
    monkeypatch.setattr(kalshi_15m_strategy, "_load_state", lambda: {"trade_log": []})

    def raise_error(**kw):
        raise RuntimeError("simulated retrain crash")

    monkeypatch.setattr(kalshi_15m_model, "train_model", raise_error)
    monkeypatch.setattr(kalshi_15m_model, "train_torch_candidate_model", raise_error)

    result = app_kalshi._run_kalshi_15m_backtest.__wrapped__()  # noqa: SLF001
    assert result["ok"] is True
    assert "auto_retrain" not in result


def test_kalshi_15m_backtest_route_returns_the_cached_result_on_get(monkeypatch):
    monkeypatch.setattr(app_kalshi, "load_json", lambda path, default: {"ok": True, "mean_return_pct": 0.01, "cached": True})
    with app_kalshi.app.test_client() as client:
        resp = client.get("/api/kalshi15m/backtest")
        body = resp.get_json()
        assert resp.status_code == 200
        assert body["cached"] is True


def test_kalshi_15m_backtest_route_runs_a_fresh_backtest_on_post(monkeypatch):
    from data import kalshi_15m_backtest

    monkeypatch.setattr(kalshi_15m_backtest, "run_walkforward_backtest", lambda: {"ok": True, "mean_return_pct": 0.05, "fresh": True})
    with app_kalshi.app.test_client() as client:
        resp = client.post("/api/kalshi15m/backtest")
        body = resp.get_json()
        assert resp.status_code == 200
        assert body["fresh"] is True


# ---------------------------------------------------------------------------
# kalshi_15m_torch_train -- custom PyTorch MLP challenger, crypto only
# (metals excluded for now -- see KALSHI_15M_TORCH_TRAIN_HOUR_ET's own
# comment). Champion/challenger promotion logic lives in
# kalshi_15m_model.train_torch_candidate_model itself; this job is just
# the trade-log-passing + failure-isolation wiring around it.
# ---------------------------------------------------------------------------
def test_kalshi_15m_torch_train_job_passes_the_real_trade_log(monkeypatch):
    from data import kalshi_15m_model, kalshi_15m_strategy

    monkeypatch.setattr(kalshi_15m_strategy, "_load_state", lambda: {"trade_log": [{"coin": "BTC"}]})
    captured = {}
    monkeypatch.setattr(kalshi_15m_model, "train_torch_candidate_model", lambda **kw: captured.update(kw) or {"ok": True, "promoted": True})

    result = app_kalshi._run_kalshi_15m_torch_train.__wrapped__()  # noqa: SLF001

    assert captured["trade_log"] == [{"coin": "BTC"}]
    assert result == {"ok": True, "promoted": True}


def test_kalshi_15m_torch_train_job_survives_a_state_read_failure(monkeypatch):
    from data import kalshi_15m_model, kalshi_15m_strategy

    def fail():
        raise RuntimeError("state file corrupted")

    monkeypatch.setattr(kalshi_15m_strategy, "_load_state", fail)
    captured = {}
    monkeypatch.setattr(kalshi_15m_model, "train_torch_candidate_model", lambda **kw: captured.update(kw) or {"ok": True})

    app_kalshi._run_kalshi_15m_torch_train.__wrapped__()  # noqa: SLF001
    assert captured["trade_log"] is None


def test_kalshi_15m_torch_train_job_survives_a_training_failure(monkeypatch):
    from data import kalshi_15m_model

    def fail(**kw):
        raise RuntimeError("torch training blew up")

    monkeypatch.setattr(kalshi_15m_model, "train_torch_candidate_model", fail)
    result = app_kalshi._run_kalshi_15m_torch_train.__wrapped__()  # noqa: SLF001
    assert result == {"ok": False, "error": "torch training blew up"}


# ---------------------------------------------------------------------------
# kalshi_15m_trade_analysis -- daily, read-only aggregate win/loss report
# (the faster, evidence-gated confidence-floor auto-tune already runs
# every 5 real trades via kalshi_15m_strategy._maybe_run_batch_trade_analysis;
# this job never writes to state). No Threads post -- kalshi_15m has no
# Threads presence today (see _run_kalshi_15m_trade_analysis's own
# docstring).
# ---------------------------------------------------------------------------
def test_kalshi_15m_trade_analysis_job_analyzes_the_real_trade_log(monkeypatch):
    from data import kalshi_15m_strategy

    trade_log = [
        {"coin": "BTC", "side": "yes", "realized_pnl_usd": 1.0, "entry_confidence": 0.6, "dry_run": False,
         "opened_at": "2026-08-01T12:00:00+00:00", "closed_at": "2026-08-01T12:10:00+00:00"},
    ]
    monkeypatch.setattr(kalshi_15m_strategy, "_load_state", lambda: {"trade_log": trade_log})

    result = app_kalshi._run_kalshi_15m_trade_analysis.__wrapped__()  # noqa: SLF001

    assert result["ok"] is True
    assert result["analysis"]["trades_analyzed"] == 1


def test_kalshi_15m_trade_analysis_job_survives_a_state_read_failure(monkeypatch):
    from data import kalshi_15m_strategy

    def fail():
        raise RuntimeError("state file corrupted")

    monkeypatch.setattr(kalshi_15m_strategy, "_load_state", fail)
    result = app_kalshi._run_kalshi_15m_trade_analysis.__wrapped__()  # noqa: SLF001
    assert result == {"ok": False, "error": "state file corrupted"}


def test_kalshi_15m_trade_analysis_route_returns_the_daily_report():
    with app_kalshi.app.test_client() as client:
        resp = client.get("/api/kalshi15m/trade_analysis")
        body = resp.get_json()
        assert resp.status_code == 200
        assert body["ok"] is True
        assert "analysis" in body


# ---------------------------------------------------------------------------
# /api/kalshi15m/balance-by-shard -- read-only diagnostic, never places an
# order or moves money. See the route's own docstring for why
# /portfolio/balance alone can't answer "does shard 2 have collateral".
# ---------------------------------------------------------------------------
def test_balance_by_shard_returns_both_the_subaccount_breakdown_and_shard_2(monkeypatch):
    from data import kalshi_15m

    monkeypatch.setattr(kalshi_15m, "get_subaccount_balances", lambda: [{"exchange_index": 2, "balance": "0"}])
    monkeypatch.setattr(kalshi_15m, "get_balance_by_shard", lambda exchange_index=None: {"balance_dollars": "0.00", "exchange_index": exchange_index})

    with app_kalshi.app.test_client() as client:
        resp = client.get("/api/kalshi15m/balance-by-shard")
        body = resp.get_json()
        assert resp.status_code == 200
        assert body["ok"] is True
        assert body["subaccount_balances"] == [{"exchange_index": 2, "balance": "0"}]
        assert body["shard_2_crypto_commodities"] == {"balance_dollars": "0.00", "exchange_index": 2}


def test_balance_by_shard_survives_a_kalshi_api_failure(monkeypatch):
    from data import kalshi_15m

    def raise_error():
        raise RuntimeError("Kalshi API error 500: boom")

    monkeypatch.setattr(kalshi_15m, "get_subaccount_balances", raise_error)
    with app_kalshi.app.test_client() as client:
        resp = client.get("/api/kalshi15m/balance-by-shard")
        assert resp.status_code == 500
        assert resp.get_json()["ok"] is False


# ---------------------------------------------------------------------------
# /api/kalshi15m/real-positions -- read-only, compares Kalshi's own real
# account state against this app's locally-recorded bookkeeping. Never
# places an order or moves money.
# ---------------------------------------------------------------------------
def test_real_positions_returns_kalshis_own_positions_and_orders(monkeypatch):
    from data import kalshi_15m

    monkeypatch.setattr(kalshi_15m, "get_portfolio_positions", lambda: [{"ticker": "KXGOLD15M-X", "position": 11}])
    monkeypatch.setattr(kalshi_15m, "get_orders", lambda: [{"order_id": "abc", "status": "resting"}])

    with app_kalshi.app.test_client() as client:
        resp = client.get("/api/kalshi15m/real-positions")
        body = resp.get_json()
        assert resp.status_code == 200
        assert body["ok"] is True
        assert body["market_positions"] == [{"ticker": "KXGOLD15M-X", "position": 11}]
        assert body["orders"] == [{"order_id": "abc", "status": "resting"}]


def test_real_positions_survives_a_kalshi_api_failure(monkeypatch):
    from data import kalshi_15m

    def raise_error():
        raise RuntimeError("Kalshi API error 500: boom")

    monkeypatch.setattr(kalshi_15m, "get_portfolio_positions", raise_error)
    with app_kalshi.app.test_client() as client:
        resp = client.get("/api/kalshi15m/real-positions")
        assert resp.status_code == 500
        assert resp.get_json()["ok"] is False


# ---------------------------------------------------------------------------
# /api/kalshi15m/verify-order-mechanics -- a one-off, manually-triggered
# diagnostic (never wired into any scheduled job) that places a real,
# structurally-safe (IOC, 1 contract, price=0.01) test order to confirm
# kalshi_15m.create_order's payload is actually accepted by this account.
# See the route's own docstring for the full safety reasoning.
# ---------------------------------------------------------------------------
def test_verify_order_mechanics_requires_cron_auth(monkeypatch):
    monkeypatch.setattr(app_kalshi, "is_cron_authorized", lambda request: False)
    with app_kalshi.app.test_client() as client:
        resp = client.post("/api/kalshi15m/verify-order-mechanics")
        assert resp.status_code == 401


def test_verify_order_mechanics_places_a_real_ioc_order_on_the_first_open_market(monkeypatch):
    from data import kalshi_15m

    monkeypatch.setattr(app_kalshi, "is_cron_authorized", lambda request: True)

    def fake_get_current_window_market(series_ticker):
        if series_ticker == "KXBTC15M":
            return {"ticker": "KXBTC15M-1"}
        return None

    monkeypatch.setattr(kalshi_15m, "get_current_window_market", fake_get_current_window_market)
    captured = {}

    def fake_create_order(**kw):
        captured.update(kw)
        return {"order_id": "o1", "fill_count": "0", "remaining_count": "0"}

    monkeypatch.setattr(kalshi_15m, "create_order", fake_create_order)

    with app_kalshi.app.test_client() as client:
        resp = client.post("/api/kalshi15m/verify-order-mechanics")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["ok"] is True
        assert data["coin"] == "BTC"
        assert data["order_result"]["order_id"] == "o1"

    # The real safety properties this route depends on -- deliberately
    # asserted explicitly, not just "some order got placed".
    assert captured["ticker"] == "KXBTC15M-1"
    assert captured["side"] == "bid"
    assert captured["count"] == 1
    assert captured["price"] == 0.01
    assert captured["time_in_force"] == "immediate_or_cancel"


def test_verify_order_mechanics_reports_no_open_window_gracefully(monkeypatch):
    from data import kalshi_15m

    monkeypatch.setattr(app_kalshi, "is_cron_authorized", lambda request: True)
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: None)

    with app_kalshi.app.test_client() as client:
        resp = client.post("/api/kalshi15m/verify-order-mechanics")
        assert resp.status_code == 200
        assert resp.get_json() == {"ok": False, "reason": "no_open_window_on_any_asset_right_now"}


def test_verify_order_mechanics_never_touches_live_trading_enabled(monkeypatch):
    """This route's whole point is to answer the verification question
    WITHOUT itself flipping the hard safety floor -- that stays a
    separate, deliberate decision."""
    from data import kalshi_15m, kalshi_15m_strategy

    monkeypatch.setattr(app_kalshi, "is_cron_authorized", lambda request: True)
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: {"ticker": "KXBTC15M-1"} if series_ticker == "KXBTC15M" else None)
    monkeypatch.setattr(kalshi_15m, "create_order", lambda **kw: {"order_id": "o1"})

    with app_kalshi.app.test_client() as client:
        client.post("/api/kalshi15m/verify-order-mechanics")

    assert kalshi_15m_strategy.LIVE_TRADING_ENABLED is False


def test_verify_order_mechanics_survives_an_order_placement_failure(monkeypatch):
    from data import kalshi_15m

    monkeypatch.setattr(app_kalshi, "is_cron_authorized", lambda request: True)
    monkeypatch.setattr(kalshi_15m, "get_current_window_market", lambda series_ticker: {"ticker": "KXBTC15M-1"} if series_ticker == "KXBTC15M" else None)

    def fail(**kw):
        raise RuntimeError("exchange rejected order")

    monkeypatch.setattr(kalshi_15m, "create_order", fail)

    with app_kalshi.app.test_client() as client:
        resp = client.post("/api/kalshi15m/verify-order-mechanics")
        assert resp.status_code == 500
        assert resp.get_json()["ok"] is False
