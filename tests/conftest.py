"""Shared pytest fixtures for the Kalshi Perps bot test suite.

These tests are unit/integration tests that never touch the real network --
Hugging Face Hub, Kalshi, and news feeds are all mocked or bypassed. They
verify the strategy's safety properties (dry-run gating, daily loss cap,
single-position constraint, leakage-free feature engineering) and job
concurrency safety, so a regression in any of them is caught automatically
instead of only showing up against the real funded account.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

# Safe defaults so importing modules never trips "not configured" branches or
# makes real network calls during collection.
os.environ.setdefault("HF_API_KEY", "")
os.environ.setdefault("HF_DATASET_REPO", "test/kalshi-perps-data")
os.environ.setdefault("HF_MODEL_REPO", "test/kalshi-perps-model")
os.environ.setdefault("KALSHI_API_KEY", "")
os.environ.setdefault("KALSHI_PRIVATE_KEY", "")
os.environ.setdefault("KALSHI_PERPS_LIVE_TRADING_ENABLED", "0")
os.environ.setdefault("ENABLE_PERPS_SCHEDULER", "0")
os.environ.setdefault("DASHBOARD_LOCAL_AUTORUN", "0")  # never start the real scheduler in tests
# Real, confirmed gap: THREADS_POST_ENABLED defaults to "1" (see
# threads_post.py's own module-level read) when unset, so any strategy
# test that reaches a real open/close event without explicitly mocking
# threads_post's own posting functions made a REAL, unmocked call to
# Threads' API -- create_and_publish_post() with no real OAuth token
# configured here either failed slowly or, on this sandbox's own
# intermittent outbound-network conditions, genuinely hung a full
# test-suite run (confirmed: test_alpaca_strategy.py's own
# test_manage_open_positions_suppresses_a_duplicate_exit_from_repeated_re_adoption
# hung this way despite passing cleanly in isolation moments earlier).
# Tests that specifically verify a Threads post happened still work fine
# with this default off -- they explicitly monkeypatch the specific
# threads_post function they're checking, which simply overrides this.
os.environ.setdefault("THREADS_POST_ENABLED", "0")

import pytest


@pytest.fixture
def isolated_data_dir(tmp_path, monkeypatch):
    """A throwaway directory tests can point _data_dir / lock dirs / job
    history files at, so tests never read or write real production files."""
    return tmp_path
