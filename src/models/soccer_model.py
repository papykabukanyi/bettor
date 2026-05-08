"""
soccer_model.py — GBM model for WC 2026 match predictions
==========================================================
Three independent classifiers:
  1. result_1x2   → home win / draw / away win  (3-class)
  2. over25       → total goals > 2.5           (binary)
  3. btts         → both teams score            (binary)

Features used:
  elo_diff         Elo rating difference (home - away)
  home_elo         Home team Elo
  away_elo         Away team Elo
  elo_ratio        home_elo / away_elo
  goals_for_h      Home team avg goals scored last 10 int'l
  goals_ag_h       Home team avg goals conceded last 10 int'l
  goals_for_a      Away team avg goals scored last 10 int'l
  goals_ag_a       Away team avg goals conceded last 10 int'l
  xg_for_h         Aggregated xG for home (from club stats)
  xg_ag_h          Aggregated xG against home
  xg_for_a         Aggregated xG for away
  xg_ag_a          Aggregated xG against away
  h2h_home_wins    Head-to-head: home wins / total (last 5)
  h2h_draws        Head-to-head: draws / total
  h2h_away_wins    Head-to-head: away wins / total
  stage            0=group, 1=r16, 2=qf, 3=sf, 4=f
  days_rest_h      Days since last match (home)
  days_rest_a      Days since last match (away)
  neutral          1 if neutral venue (always 1 for WC)
"""

from __future__ import annotations

import os
import joblib
import numpy as np

from sklearn.ensemble import GradientBoostingClassifier
from sklearn.preprocessing import LabelEncoder
from typing import Any

MODEL_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "models", "soccer_wc_model.joblib"
)
MODEL_PATH = os.path.normpath(MODEL_PATH)

FEATURE_COLS = [
    "elo_diff", "home_elo", "away_elo", "elo_ratio",
    "goals_for_h", "goals_ag_h", "goals_for_a", "goals_ag_a",
    "xg_for_h",   "xg_ag_h",   "xg_for_a",   "xg_ag_a",
    "h2h_home_wins", "h2h_draws", "h2h_away_wins",
    "stage", "days_rest_h", "days_rest_a", "neutral",
]

STAGE_MAP = {"group": 0, "r16": 1, "qf": 2, "sf": 3, "final": 4, "third": 3}


# ── Historical training data ──────────────────────────────────────────────────
def _build_training_data() -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """
    Build training arrays from embedded historical data.
    Returns X, y_1x2, y_over25, y_btts.
    """
    from data.wc2026_fetcher import _get_embedded_wc_history

    rows = _get_embedded_wc_history()

    X_rows:     list[list[float]] = []
    y_1x2:      list[str]         = []
    y_over25:   list[int]         = []
    y_btts:     list[int]         = []

    for r in rows:
        hg = r.get("home_goals", 0)
        ag = r.get("away_goals", 0)
        he = r.get("home_elo", 1900.0)
        ae = r.get("away_elo", 1900.0)
        stage = STAGE_MAP.get(r.get("stage", "group"), 0)

        # 1X2 label
        if hg > ag:
            label_1x2 = "H"
        elif hg == ag:
            label_1x2 = "D"
        else:
            label_1x2 = "A"

        X_rows.append(_make_feature_vector(
            home_elo=he, away_elo=ae,
            goals_for_h=1.4,  goals_ag_h=1.0,
            goals_for_a=1.4,  goals_ag_a=1.0,
            xg_for_h=1.3,     xg_ag_h=1.0,
            xg_for_a=1.3,     xg_ag_a=1.0,
            h2h_hw=0.33,      h2h_d=0.33, h2h_aw=0.33,
            stage=stage,      days_rest_h=7, days_rest_a=7,
        ))

        y_1x2.append(label_1x2)
        y_over25.append(1 if (hg + ag) > 2 else 0)
        y_btts.append(1 if hg > 0 and ag > 0 else 0)

    return (np.array(X_rows),
            np.array(y_1x2),
            np.array(y_over25),
            np.array(y_btts))


def _make_feature_vector(
    home_elo: float, away_elo: float,
    goals_for_h: float, goals_ag_h: float,
    goals_for_a: float, goals_ag_a: float,
    xg_for_h: float,   xg_ag_h: float,
    xg_for_a: float,   xg_ag_a: float,
    h2h_hw: float,     h2h_d: float,  h2h_aw: float,
    stage: int = 0,    days_rest_h: int = 7, days_rest_a: int = 7,
) -> list[float]:
    return [
        home_elo - away_elo,          # elo_diff
        home_elo, away_elo,           # home_elo, away_elo
        home_elo / max(away_elo, 1),  # elo_ratio
        goals_for_h, goals_ag_h,      # home attack / defence
        goals_for_a, goals_ag_a,      # away attack / defence
        xg_for_h,   xg_ag_h,         # home xG
        xg_for_a,   xg_ag_a,         # away xG
        h2h_hw,     h2h_d,  h2h_aw,  # head-to-head
        float(stage),                 # stage
        float(days_rest_h), float(days_rest_a),
        1.0,                          # neutral (always 1 in WC)
    ]


# ── Train ─────────────────────────────────────────────────────────────────────
def train(save: bool = True) -> dict[str, Any]:
    X, y_1x2, y_over25, y_btts = _build_training_data()

    le = LabelEncoder()
    y_1x2_enc = le.fit_transform(y_1x2)  # A→0, D→1, H→2

    gbm_params = dict(n_estimators=120, max_depth=3, learning_rate=0.05,
                      subsample=0.85, min_samples_leaf=2, random_state=42)

    model_1x2   = GradientBoostingClassifier(**gbm_params)
    model_over25 = GradientBoostingClassifier(**gbm_params)
    model_btts   = GradientBoostingClassifier(**gbm_params)

    model_1x2.fit(X,   y_1x2_enc)
    model_over25.fit(X, y_over25)
    model_btts.fit(X,  y_btts)

    bundle = {
        "model_1x2":    model_1x2,
        "model_over25": model_over25,
        "model_btts":   model_btts,
        "label_encoder": le,
        "feature_cols": FEATURE_COLS,
        "version":      "wc2026-v1",
    }

    if save:
        os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
        joblib.dump(bundle, MODEL_PATH)
        print(f"[soccer_model] Saved -> {MODEL_PATH}")

    return bundle


# ── Load / lazy singleton ─────────────────────────────────────────────────────
_bundle: dict[str, Any] | None = None

def _get_bundle() -> dict[str, Any]:
    global _bundle
    if _bundle is None:
        if os.path.exists(MODEL_PATH):
            try:
                _bundle = joblib.load(MODEL_PATH)
                print(f"[soccer_model] Loaded from {MODEL_PATH}")
            except Exception as e:
                print(f"[soccer_model] Load failed ({e}) — retraining")
                _bundle = train(save=True)
        else:
            print("[soccer_model] No saved model — training now")
            _bundle = train(save=True)
    return _bundle


# ── Predict ───────────────────────────────────────────────────────────────────
def predict(
    home_elo: float, away_elo: float,
    goals_for_h: float = 1.4,  goals_ag_h: float = 1.0,
    goals_for_a: float = 1.4,  goals_ag_a: float = 1.0,
    xg_for_h: float  = 1.3,   xg_ag_h: float   = 1.0,
    xg_for_a: float  = 1.3,   xg_ag_a: float   = 1.0,
    h2h_hw: float    = 0.33,  h2h_d: float = 0.33,  h2h_aw: float = 0.33,
    stage: int       = 0,
    days_rest_h: int = 7,      days_rest_a: int = 7,
) -> dict[str, float]:
    """
    Return probability dict:
      home_prob, draw_prob, away_prob, over25_prob, btts_prob
    """
    bundle = _get_bundle()
    X = np.array([_make_feature_vector(
        home_elo, away_elo,
        goals_for_h, goals_ag_h,
        goals_for_a, goals_ag_a,
        xg_for_h, xg_ag_h,
        xg_for_a, xg_ag_a,
        h2h_hw, h2h_d, h2h_aw,
        stage, days_rest_h, days_rest_a,
    )])

    le     = bundle["label_encoder"]
    m1x2   = bundle["model_1x2"]
    mover  = bundle["model_over25"]
    mbtts  = bundle["model_btts"]

    proba_1x2 = m1x2.predict_proba(X)[0]   # [A, D, H] or however le encoded
    classes   = list(le.classes_)           # e.g. ['A', 'D', 'H']
    prob_map  = dict(zip(classes, proba_1x2))

    over25_prob = float(mover.predict_proba(X)[0][1])
    btts_prob   = float(mbtts.predict_proba(X)[0][1])

    return {
        "home_prob":   round(float(prob_map.get("H", 0.33)), 4),
        "draw_prob":   round(float(prob_map.get("D", 0.28)), 4),
        "away_prob":   round(float(prob_map.get("A", 0.33)), 4),
        "over25_prob": round(over25_prob, 4),
        "btts_prob":   round(btts_prob,   4),
    }


if __name__ == "__main__":
    bundle = train(save=True)
    result = predict(home_elo=2078, away_elo=2040, stage=4)
    print("France vs Argentina (final) probabilities:", result)
