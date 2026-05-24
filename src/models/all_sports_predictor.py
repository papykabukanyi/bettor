"""All-sports prediction utilities for ranking, parlays, and self-learning.

This module centralizes quality scoring across sports so dashboard outputs are
consistent and can adapt from resolved prediction performance.
"""

from __future__ import annotations

import datetime
import json
import math
import os
from itertools import combinations
from typing import Any

from models.mlb_model import auto_improve as calibrate_daily_model
from models.mlb_predictor import (
    build_elite_parlay,
    build_parlays as _legacy_build_parlays,
    resolve_tracked_parlays,
)

_ROOT = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", ".."))
_LEARNING_STATE_PATH = os.path.join(_ROOT, "data", "all_sports_learning_state.json")


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, float(value)))


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return float(default)
        return float(value)
    except Exception:
        return float(default)


def _sport_key(row: dict[str, Any]) -> str:
    return str(row.get("sport") or row.get("competition") or row.get("league") or "other").strip().lower() or "other"


def _bet_type_key(row: dict[str, Any]) -> str:
    return str(
        row.get("bet_type")
        or row.get("stat_type")
        or row.get("prop_type")
        or "single"
    ).strip().lower() or "single"


def _read_learning_state() -> dict[str, Any]:
    if not os.path.exists(_LEARNING_STATE_PATH):
        return {}
    try:
        with open(_LEARNING_STATE_PATH, "r", encoding="utf-8") as fh:
            payload = json.load(fh)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _write_learning_state(payload: dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(_LEARNING_STATE_PATH), exist_ok=True)
    tmp = f"{_LEARNING_STATE_PATH}.tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, sort_keys=True)
    os.replace(tmp, _LEARNING_STATE_PATH)


def _load_team_stats_for_training():
    from config import MLB_SEASONS

    try:
        from data.mlb_fetcher import fetch_team_stats

        return fetch_team_stats(MLB_SEASONS)
    except Exception:
        from data.mlb_fetcher import get_team_batting_stats, get_team_pitching_stats
        import pandas as pd

        seasons = MLB_SEASONS if isinstance(MLB_SEASONS, (list, tuple)) else [int(MLB_SEASONS)]
        frames = []
        for season in seasons:
            bat = get_team_batting_stats(int(season))
            pit = get_team_pitching_stats(int(season))
            if bat is None or getattr(bat, "empty", True):
                continue
            if pit is None or getattr(pit, "empty", True):
                frames.append(bat)
            else:
                frames.append(pd.merge(bat, pit, on=["team", "season"], how="inner"))
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def daily_self_train(run_date: datetime.date | None = None) -> dict[str, Any]:
    """Run once-per-day self-training tasks from resolved outcomes."""
    day = (run_date or datetime.date.today()).isoformat()
    state = _read_learning_state()
    if state.get("last_run_date") == day:
        return {
            "ok": True,
            "skipped": True,
            "last_run_date": day,
            "msg": "self-train already ran today",
            "mlb": state.get("mlb"),
            "soccer": state.get("soccer"),
        }

    result: dict[str, Any] = {
        "ok": True,
        "skipped": False,
        "last_run_date": day,
        "mlb": {},
        "soccer": {},
    }

    try:
        team_stats = _load_team_stats_for_training()
        min_resolved = max(30, int(os.getenv("AUTO_LEARN_MIN_RESOLVED", "45") or "45"))
        ece_threshold = _as_float(os.getenv("AUTO_LEARN_ECE_THRESHOLD", "0.09"), 0.09)
        mlb_result = calibrate_daily_model(
            team_stats,
            min_resolved=min_resolved,
            ece_threshold=ece_threshold,
            verbose=False,
        )
        result["mlb"] = mlb_result if isinstance(mlb_result, dict) else {"msg": str(mlb_result)}
    except Exception as exc:
        result["ok"] = False
        result["mlb"] = {"error": str(exc)}

    try:
        soccer_enabled = str(os.getenv("AUTO_SOCCER_RETRAIN_DAILY", "1")).strip().lower() in {
            "1",
            "true",
            "yes",
            "on",
        }
        if soccer_enabled:
            from models.soccer_model import train as train_soccer_model

            train_soccer_model(save=True)
            result["soccer"] = {"ok": True, "retrained": True}
        else:
            result["soccer"] = {"ok": True, "retrained": False, "msg": "disabled by AUTO_SOCCER_RETRAIN_DAILY"}
    except Exception as exc:
        result["ok"] = False
        result["soccer"] = {"error": str(exc)}

    state.update(
        {
            "last_run_date": day,
            "last_run_ts": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "mlb": result.get("mlb"),
            "soccer": result.get("soccer"),
        }
    )
    _write_learning_state(state)
    return result


def _fetch_reliability_profile(days_back: int = 180, min_samples: int = 24) -> dict[tuple[str, str], dict[str, float]]:
    """Compute sport+bet-type reliability from resolved predictions."""
    try:
        from data.db import get_conn
        import psycopg2.extras

        conn = get_conn()
        if conn is None:
            return {}
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(
            """
            SELECT
                LOWER(COALESCE(sport, 'other')) AS sport,
                LOWER(COALESCE(bet_type, 'single')) AS bet_type,
                COUNT(*) AS n,
                AVG(COALESCE(model_prob, 0.5)) AS avg_pred,
                AVG(CASE WHEN outcome = 'WIN' THEN 1.0 ELSE 0.0 END) AS hit_rate
            FROM predictions
            WHERE outcome IN ('WIN','LOSS')
              AND game_date >= CURRENT_DATE - (%s::int)
            GROUP BY LOWER(COALESCE(sport, 'other')), LOWER(COALESCE(bet_type, 'single'))
            """,
            (int(max(30, days_back)),),
        )
        rows = cur.fetchall() or []
        conn.close()
    except Exception:
        return {}

    profile: dict[tuple[str, str], dict[str, float]] = {}
    for row in rows:
        n = int(row.get("n") or 0)
        if n < min_samples:
            continue
        avg_pred = _clamp(_as_float(row.get("avg_pred"), 0.5), 0.01, 0.99)
        hit_rate = _clamp(_as_float(row.get("hit_rate"), 0.5), 0.01, 0.99)
        gap = abs(avg_pred - hit_rate)
        sample_weight = _clamp(math.sqrt(n / 160.0), 0.25, 1.0)
        calibration = _clamp(1.0 - (gap * 1.8), 0.55, 1.08)
        profile[(str(row.get("sport") or "other"), str(row.get("bet_type") or "single"))] = {
            "n": float(n),
            "avg_pred": avg_pred,
            "hit_rate": hit_rate,
            "gap": gap,
            "sample_weight": sample_weight,
            "calibration": calibration,
            "multiplier": _clamp((0.72 + 0.28 * sample_weight) * calibration, 0.50, 1.10),
        }
    return profile


def _quality_reason(prob: float, ev: float, reliability: float, market_ready: float) -> str:
    tags = []
    if prob >= 0.64:
        tags.append("high model confidence")
    elif prob >= 0.58:
        tags.append("solid model confidence")
    if ev >= 0.08:
        tags.append("strong expected value")
    elif ev >= 0.02:
        tags.append("positive expected value")
    if reliability >= 0.92:
        tags.append("historically reliable market")
    if market_ready >= 0.6:
        tags.append("exchange-ready")
    return "; ".join(tags[:3]) or "balanced multi-factor score"


def rank_best_bets(rows: list[dict[str, Any]], raw_bets: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    """Re-rank all candidate rows with probability, EV, reliability, and market factors."""
    reliability = _fetch_reliability_profile()
    enriched: list[dict[str, Any]] = []

    for row in rows or []:
        if not isinstance(row, dict):
            continue
        item = dict(row)

        prob = _clamp(_as_float(item.get("model_prob"), 0.5), 0.01, 0.99)
        dec_odds = _as_float(item.get("dec_odds"), 0.0)
        if dec_odds <= 1.01:
            am = _as_float(item.get("odds_am"), 0.0)
            if am > 0:
                dec_odds = 1.0 + (am / 100.0)
            elif am < 0:
                dec_odds = 1.0 + (100.0 / abs(am))
            else:
                dec_odds = 2.0

        ev = _as_float(item.get("ev"), None)
        if ev is None:
            ev = prob * dec_odds - 1.0

        reliability_key = (_sport_key(item), _bet_type_key(item))
        rel = reliability.get(reliability_key) or {}
        rel_mult = _as_float(rel.get("multiplier"), 0.90)

        safety_label = str(item.get("safety_label") or "MODERATE").upper()
        safety_bonus = {
            "ELITE": 1.00,
            "SAFE": 0.92,
            "MODERATE": 0.84,
            "RISKY": 0.72,
        }.get(safety_label, 0.82)

        market_ready = 0.0
        if str(item.get("kalshi_status") or "").strip().lower() == "matched":
            market_ready = max(market_ready, 1.0)
        if str(item.get("market_slug") or "").strip():
            market_ready = max(market_ready, 0.85)
        if str(item.get("market_ticker") or "").strip():
            market_ready = max(market_ready, 0.65)

        ev_norm = _clamp((ev + 0.10) / 0.35, 0.0, 1.0)
        prob_norm = _clamp((prob - 0.50) / 0.40, 0.0, 1.0)
        quality = (
            (0.47 * prob_norm)
            + (0.23 * ev_norm)
            + (0.15 * rel_mult)
            + (0.10 * safety_bonus)
            + (0.05 * market_ready)
        )

        # Mild anti-longshot penalty keeps "hot" but unstable picks lower.
        if dec_odds >= 3.50:
            quality *= 0.93
        if prob < 0.55:
            quality *= 0.95

        item["model_prob"] = round(prob, 4)
        item["dec_odds"] = round(dec_odds, 4)
        item["ev"] = round(ev, 4)
        item["quality_score"] = round(_clamp(quality, 0.0, 1.0), 4)
        item["quality_reason"] = _quality_reason(prob, ev, rel_mult, market_ready)
        item["reliability_multiplier"] = round(rel_mult, 4)
        item["market_ready_score"] = round(market_ready, 4)
        enriched.append(item)

    enriched.sort(
        key=lambda x: (
            float(x.get("quality_score") or 0.0),
            float(x.get("model_prob") or 0.0),
            float(x.get("ev") or 0.0),
        ),
        reverse=True,
    )
    for idx, item in enumerate(enriched, start=1):
        item["quality_rank"] = idx
    return enriched


def build_best_parlays(
    ranked_rows: list[dict[str, Any]],
    *,
    max_legs: int = 5,
    top_n: int = 8,
) -> list[dict[str, Any]]:
    """Create diversified high-quality parlays from ranked rows."""
    candidates = []
    for row in ranked_rows or []:
        if not isinstance(row, dict):
            continue
        prob = _as_float(row.get("model_prob"), 0.0)
        quality = _as_float(row.get("quality_score"), 0.0)
        ev = _as_float(row.get("ev"), -1.0)
        if prob < 0.56 or quality < 0.57 or ev < -0.03:
            continue
        candidates.append(dict(row))

    # Hard cap to keep combination search fast and stable.
    candidates = sorted(
        candidates,
        key=lambda r: (
            float(r.get("quality_score") or 0.0),
            float(r.get("model_prob") or 0.0),
        ),
        reverse=True,
    )[:18]

    if len(candidates) < 2:
        return []

    results: list[dict[str, Any]] = []
    for n_legs in range(2, min(max_legs, len(candidates)) + 1):
        for combo in combinations(candidates, n_legs):
            games = set()
            players = set()
            sports = set()
            valid = True
            for leg in combo:
                gk = str(leg.get("game_key") or leg.get("game") or "").strip().lower()
                if gk and gk in games:
                    valid = False
                    break
                if gk:
                    games.add(gk)

                player = str(leg.get("name") or leg.get("player_name") or "").strip().lower()
                if player and player in players:
                    valid = False
                    break
                if player:
                    players.add(player)

                sports.add(_sport_key(leg))

            if not valid:
                continue

            combined_prob = 1.0
            combined_dec = 1.0
            quality_sum = 0.0
            for leg in combo:
                combined_prob *= _clamp(_as_float(leg.get("model_prob"), 0.5), 0.01, 0.99)
                combined_dec *= max(1.01, _as_float(leg.get("dec_odds"), 2.0))
                quality_sum += _clamp(_as_float(leg.get("quality_score"), 0.6), 0.0, 1.0)

            avg_quality = quality_sum / float(n_legs)
            corr_penalty = 0.985 ** max(0, n_legs - len(sports))
            adjusted_prob = _clamp(combined_prob * corr_penalty, 0.001, 0.95)
            expected_roi = adjusted_prob * combined_dec - 1.0

            if expected_roi < -0.04:
                continue

            score = adjusted_prob * avg_quality * (1.0 + max(expected_roi, 0.0))
            legs = []
            for leg in combo:
                legs.append(
                    {
                        "label": str(leg.get("pick") or leg.get("name") or "").strip(),
                        "bet_type": leg.get("bet_type") or leg.get("stat_type") or "single",
                        "conf": int(round(_as_float(leg.get("model_prob"), 0.5) * 100.0)),
                        "badge": leg.get("safety_label") or "SAFE",
                        "game": leg.get("game_key") or leg.get("game") or "",
                        "dec_odds": round(max(1.01, _as_float(leg.get("dec_odds"), 2.0)), 2),
                        "sport": _sport_key(leg),
                        "source": "prop" if "prop" in _bet_type_key(leg) else "game",
                        "quality_score": round(_as_float(leg.get("quality_score"), 0.0), 4),
                    }
                )

            results.append(
                {
                    "n_legs": n_legs,
                    "legs": legs,
                    "combined_prob": round(adjusted_prob * 100.0, 1),
                    "combined_dec": round(combined_dec, 3),
                    "avg_safety": round(avg_quality, 3),
                    "score": round(score, 6),
                    "expected_roi": round(expected_roi, 4),
                    "payout_100": round(combined_dec * 100.0, 0),
                    "safety_label": "ELITE" if avg_quality >= 0.82 else "SAFE" if avg_quality >= 0.68 else "MODERATE",
                }
            )

    results.sort(
        key=lambda p: (
            float(p.get("score") or 0.0),
            float(p.get("expected_roi") or 0.0),
            float(p.get("combined_prob") or 0.0),
        ),
        reverse=True,
    )
    return results[: max(3, int(top_n))]


def build_parlays(all_picks: list[dict], max_legs: int = 8, top_n: int = 5) -> list[dict]:
    """Backwards-compatible alias to legacy parlay builder."""
    return _legacy_build_parlays(all_picks, max_legs=max_legs, top_n=top_n)


__all__ = [
    "build_parlays",
    "build_elite_parlay",
    "resolve_tracked_parlays",
    "calibrate_daily_model",
    "daily_self_train",
    "rank_best_bets",
    "build_best_parlays",
]
