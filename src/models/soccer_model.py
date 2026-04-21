"""
Soccer Prediction Model  (Poisson / Dixon-Coles + ML ensemble)
===============================================================

Two-layer approach:
  1. Poisson model  – uses attack/defence strength ratings to predict
                      expected goals, then integrates over score distributions
                      for 1X2 and over/under probabilities.
  2. GBM overlay    – trained on historical match features + closing odds
                      to refine match-outcome probabilities.

References:
  - Dixon & Coles (1997) – "Modelling Association Football Scores"
  - Maher (1982) – "Modelling Association Football Scores"

Usage:
    from models.soccer_model import SoccerModel
    model = SoccerModel()
    model.fit(matches_df, strength_df)
    probs = model.predict("Arsenal", "Chelsea", venue="home")
"""

import os
import sys
import warnings
import numpy as np
import pandas as pd
import joblib
from scipy.stats import poisson
from scipy.optimize import minimize

from sklearn.ensemble import GradientBoostingClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.model_selection import cross_val_score

warnings.filterwarnings("ignore")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

MODEL_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "models", "soccer_model.joblib"
)

MAX_GOALS = 10  # max goals per team to consider in Poisson sum


class SoccerModel:
    """
    Poisson-based soccer match outcome predictor with optional ML overlay.
    """

    def __init__(self):
        self.strength_df: pd.DataFrame = pd.DataFrame()
        self.avg_home_goals: float = 1.5
        self.avg_away_goals: float = 1.2
        self.home_advantage: float = 1.25
        self.ml_pipeline: Pipeline | None = None
        self.fitted = False

    # ------------------------------------------------------------------ #
    # Fitting
    # ------------------------------------------------------------------ #

    def fit(self, matches: pd.DataFrame, strength_df: pd.DataFrame | None = None) -> None:
        """
        Fit the model using historical match data.

        matches   : DataFrame from soccer_fetcher.get_historical_matches()
        strength_df: optional pre-computed strengths; computed if None
        """
        if matches.empty:
            print("[soccer_model] Empty matches DataFrame – cannot fit.")
            return

        from data.soccer_fetcher import compute_team_strength

        if strength_df is None or strength_df.empty:
            strength_df = compute_team_strength(matches)
        self.strength_df = strength_df

        self.avg_home_goals = matches["fthg"].mean()
        self.avg_away_goals = matches["ftag"].mean()

        # Home advantage = mean home attack / mean away attack
        home_attack_mean = strength_df.get("home_adv", pd.Series([1.25])).mean()
        self.home_advantage = float(home_attack_mean) if home_attack_mean > 0 else 1.25

        # Train ML overlay
        self._fit_ml_overlay(matches, strength_df)
        self.fitted = True

        os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
        joblib.dump(self, MODEL_PATH)
        print(f"[soccer_model] Model fitted and saved to {MODEL_PATH}")
        print(f"[soccer_model] Teams covered: {len(strength_df)}, matches: {len(matches)}")

    def _fit_ml_overlay(self, matches: pd.DataFrame, strength_df: pd.DataFrame) -> None:
        """Train a GBM on match features to predict outcome (H/D/A)."""
        strength_idx = strength_df.set_index("team")
        rows = []

        for _, row in matches.iterrows():
            ht = row["home_team"]
            at = row["away_team"]
            if ht not in strength_idx.index or at not in strength_idx.index:
                continue

            h = strength_idx.loc[ht]
            a = strength_idx.loc[at]
            ftr = row.get("ftr", None)
            if ftr not in ("H", "D", "A"):
                continue

            # Add closing odds as features if available
            b365h = float(row.get("b365h") or 0)
            b365d = float(row.get("b365d") or 0)
            b365a = float(row.get("b365a") or 0)
            imp_h = (1 / b365h) if b365h > 0 else 0
            imp_d = (1 / b365d) if b365d > 0 else 0
            imp_a = (1 / b365a) if b365a > 0 else 0

            rows.append({
                "h_attack":  h.get("attack_strength", 1.0),
                "h_defence": h.get("defence_strength", 1.0),
                "a_attack":  a.get("attack_strength", 1.0),
                "a_defence": a.get("defence_strength", 1.0),
                "h_home_adv": h.get("home_adv", 1.0),
                "imp_h": imp_h,
                "imp_d": imp_d,
                "imp_a": imp_a,
                "label": {"H": 2, "D": 1, "A": 0}[ftr],
            })

        if len(rows) < 50:
            print("[soccer_model] Too few rows for ML overlay – using Poisson only.")
            return

        df = pd.DataFrame(rows)
        feat_cols = ["h_attack", "h_defence", "a_attack", "a_defence",
                     "h_home_adv", "imp_h", "imp_d", "imp_a"]
        X = df[feat_cols].fillna(0)
        y = df["label"]

        pipeline = Pipeline([
            ("scaler", StandardScaler()),
            ("clf", GradientBoostingClassifier(
                n_estimators=200, max_depth=3,
                learning_rate=0.05, random_state=42,
            )),
        ])
        cv = cross_val_score(pipeline, X, y, cv=5, scoring="accuracy")
        pipeline.fit(X, y)
        self.ml_pipeline = pipeline
        self._ml_feature_cols = feat_cols
        print(f"[soccer_model] ML overlay CV accuracy: {cv.mean():.3f} ± {cv.std():.3f}")

    # ------------------------------------------------------------------ #
    # Prediction helpers
    # ------------------------------------------------------------------ #

    def _get_strength(self, team: str) -> dict:
        """Lookup team strengths. Returns league average if team not found."""
        if self.strength_df.empty:
            return {"attack_strength": 1.0, "defence_strength": 1.0, "home_adv": 1.25}
        row = self.strength_df[self.strength_df["team"].str.lower() == team.lower()]
        if row.empty:
            # fuzzy: partial match
            row = self.strength_df[self.strength_df["team"].str.contains(team, case=False, na=False)]
        if row.empty:
            print(f"[soccer_model] Team not found: '{team}' – using league average.")
            return {"attack_strength": 1.0, "defence_strength": 1.0, "home_adv": 1.25}
        return row.iloc[0].to_dict()

    def _poisson_probs(self, lambda_home: float, lambda_away: float) -> dict:
        """
        Integrate Poisson distributions for home win / draw / away win
        and over/under 2.5 goals probabilities.
        """
        score_matrix = np.outer(
            poisson.pmf(range(MAX_GOALS + 1), lambda_home),
            poisson.pmf(range(MAX_GOALS + 1), lambda_away),
        )
        home_win = float(np.sum(np.tril(score_matrix, -1)))
        draw     = float(np.sum(np.diag(score_matrix)))
        away_win = float(np.sum(np.triu(score_matrix, 1)))

        # Over / under 2.5
        total_goals = sum(
            score_matrix[h, a]
            for h in range(MAX_GOALS + 1)
            for a in range(MAX_GOALS + 1)
            if h + a > 2
        )
        over25 = float(total_goals)
        under25 = 1.0 - over25

        return {
            "home_win":  round(home_win, 4),
            "draw":      round(draw, 4),
            "away_win":  round(away_win, 4),
            "over_2_5":  round(over25, 4),
            "under_2_5": round(under25, 4),
            "lambda_home": round(lambda_home, 3),
            "lambda_away": round(lambda_away, 3),
        }

    def predict(
        self,
        home_team: str,
        away_team: str,
        blend_ml: float = 0.35,
    ) -> dict:
        """
        Predict match outcome probabilities.

        home_team : team name (must approximately match training data)
        away_team : team name
        blend_ml  : weight for ML overlay (0 = pure Poisson, 1 = pure ML)

        Returns:
          home_win_prob, draw_prob, away_win_prob,
          over_2_5_prob, under_2_5_prob,
          lambda_home, lambda_away, method
        """
        if not self.fitted:
            print("[soccer_model] Model not fitted – call fit() first.")
            return {}

        h = self._get_strength(home_team)
        a = self._get_strength(away_team)

        # Expected goals using attack / defence strengths + home advantage
        lambda_home = (
            h["attack_strength"] * a["defence_strength"]
            * self.avg_home_goals * float(h.get("home_adv", self.home_advantage))
        )
        lambda_away = (
            a["attack_strength"] * h["defence_strength"]
            * self.avg_away_goals
        )

        poisson_result = self._poisson_probs(lambda_home, lambda_away)
        result = dict(poisson_result)

        # Optionally blend in ML overlay
        if self.ml_pipeline is not None and blend_ml > 0:
            row = {
                "h_attack":   h.get("attack_strength", 1.0),
                "h_defence":  h.get("defence_strength", 1.0),
                "a_attack":   a.get("attack_strength", 1.0),
                "a_defence":  a.get("defence_strength", 1.0),
                "h_home_adv": h.get("home_adv", 1.0),
                "imp_h": 0, "imp_d": 0, "imp_a": 0,  # no live odds at predict time
            }
            X = pd.DataFrame([row])[self._ml_feature_cols].fillna(0)
            ml_probs = self.ml_pipeline.predict_proba(X)[0]
            # ml class order: 0=A, 1=D, 2=H
            classes = list(self.ml_pipeline.named_steps["clf"].classes_)
            ml_map = {c: float(ml_probs[i]) for i, c in enumerate(classes)}
            ml_h = ml_map.get(2, 0.33)
            ml_d = ml_map.get(1, 0.33)
            ml_a = ml_map.get(0, 0.34)

            w = blend_ml
            result["home_win"] = round((1 - w) * result["home_win"] + w * ml_h, 4)
            result["draw"]     = round((1 - w) * result["draw"]     + w * ml_d, 4)
            result["away_win"] = round((1 - w) * result["away_win"] + w * ml_a, 4)
            # renormalise
            total = result["home_win"] + result["draw"] + result["away_win"]
            result["home_win"] /= total
            result["draw"]     /= total
            result["away_win"] /= total
            result["method"] = "Poisson+ML"
        else:
            result["method"] = "Poisson"

        result["home_team"] = home_team
        result["away_team"] = away_team
        return result


# ---------------------------------------------------------------------------
# Module-level convenience functions
# ---------------------------------------------------------------------------

def load_model() -> SoccerModel | None:
    """Load saved SoccerModel from disk."""
    if os.path.exists(MODEL_PATH):
        return joblib.load(MODEL_PATH)
    print("[soccer_model] No saved model – call SoccerModel().fit() first.")
    return None


def predict_match(home_team: str, away_team: str) -> dict:
    """Shortcut: load saved model and predict a match."""
    m = load_model()
    if m is None:
        return {}
    return m.predict(home_team, away_team)
