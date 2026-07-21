"""Shared pytest fixtures for the bettor test suite.

These tests are unit/integration tests that never touch the real network --
Hugging Face Hub, Kalshi, and sports-data APIs are all mocked or bypassed.
They verify the specific bugs found and fixed during operation (rate-limit
cascades, duplicate concurrent runs, leakage-free feature engineering,
bankroll pacing, combo-leg limits, matching safety) so a regression in any
of them is caught automatically instead of only showing up in production logs.
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
os.environ.setdefault("HF_API_KEY", "test-token-not-real")
os.environ.setdefault("HF_DATASET_REPO", "papylove/sportprediction")
os.environ.setdefault("HF_MODEL_REPO", "papylove/sportprediction")
os.environ.setdefault("KALSHI_API_KEY", "")
os.environ.setdefault("KALSHI_PRIVATE_KEY", "")
os.environ.setdefault("KALSHI_LIVE_TRADING_ENABLED", "0")
os.environ.setdefault("AUTOBET_DRY_RUN", "1")
os.environ.setdefault("DASHBOARD_LOCAL_AUTORUN", "0")  # never start the real scheduler in tests
os.environ.setdefault("HF_AUTORUN_ON_STARTUP", "0")

import pytest


@pytest.fixture
def isolated_data_dir(tmp_path, monkeypatch):
    """A throwaway directory tests can point _data_dir / lock dirs / job
    history files at, so tests never read or write real production files."""
    return tmp_path
