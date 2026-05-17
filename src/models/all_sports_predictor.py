"""Unified predictor helpers for the dashboard.

This module provides a sport-neutral import surface so the rest of the app
can use non-MLB-specific names for shared parlay/tracking helpers.
"""

from __future__ import annotations

from models.mlb_predictor import build_elite_parlay, build_parlays, resolve_tracked_parlays
from models.mlb_model import auto_improve as calibrate_daily_model

__all__ = [
    "build_parlays",
    "build_elite_parlay",
    "resolve_tracked_parlays",
    "calibrate_daily_model",
]
