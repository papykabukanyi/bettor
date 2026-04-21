"""
MLB Prediction Model
=====================
Uses a Gradient Boosting classifier trained on team-level season stats
to predict game win probabilities.

Features used:
  Offensive: runs_scored, bat_avg, obp, slg, wrc_plus
  Pitching : era, whip, k_per_9, bb_per_9, fip
  Context  : home advantage (binary flag)

Training:
  - Call train() with a DataFrame from mlb_fetcher.build_game_dataset()
  - Model is saved to models/mlb_model.joblib

Prediction:
  - Call predict_game(home_stats_dict, away_stats_dict) → (home_win_prob, away_win_prob)
  - Call predict_game_from_teams(home, away, season) for convenience

Player Props:
  - compare_prop(player_stat, line) → over/under confidence
"""

import os
import sys
import warnings
import numpy as np
import pandas as pd
import joblib

from sklearn.ensemble import GradientBoostingClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.model_selection import cross_val_score
from sklearn.metrics import log_loss

warnings.filterwarnings("ignore")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

MODEL_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "models", "mlb_model.joblib"
)

# Features the model expects (must match what build_game_dataset returns)
FEATURE_COLS = [
    "runs_scored", "bat_avg", "obp", "slg",
    "era", "whip", "k_per_9", "bb_per_9", "fip",
    "is_home",  # 1 = home team, 0 = away team
]


def _build_matchup_rows(team_stats: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    """
    Convert team-season stats into (X, y) matchup pairs for training.

    Strategy: for each pair of teams in the same season, create a synthetic
    matchup row = home_features – away_features + is_home flag.
    The target is 1 if the home team has better run differential.

    NOTE: This is a synthetic approach because we don't have game-by-game
    results paired with season stats directly.  Once you have more data,
    replace this with actual game-level outcomes.
    """
    if team_stats.empty:
        return pd.DataFrame(), pd.Series()

    rows_X, rows_y = [], []

    for season, grp in team_stats.groupby("season"):
        teams = grp.reset_index(drop=True)
        for i in range(len(teams)):
            for j in range(i + 1, len(teams)):
                home = teams.iloc[i]
                away = teams.iloc[j]

                def diff(col):
                    return float(home.get(col, 0) or 0) - float(away.get(col, 0) or 0)

                row = {
                    "runs_scored": diff("runs_scored"),
                    "bat_avg":     diff("bat_avg"),
                    "obp":         diff("obp"),
                    "slg":         diff("slg"),
                    "era":         diff("era"),
                    "whip":        diff("whip"),
                    "k_per_9":     diff("k_per_9"),
                    "bb_per_9":    diff("bb_per_9"),
                    "fip":         diff("fip"),
                    "is_home":     1,
                }
                # Home team wins if better run diff (simplified label)
                label = 1 if float(home.get("runs_scored", 0) or 0) > float(away.get("runs_scored", 0) or 0) else 0
                rows_X.append(row)
                rows_y.append(label)

                # Also add the reverse matchup (away perspective)
                row_rev = {k: -v if k != "is_home" else 0 for k, v in row.items()}
                row_rev["is_home"] = 0
                rows_X.append(row_rev)
                rows_y.append(1 - label)

    return pd.DataFrame(rows_X), pd.Series(rows_y)


def train(team_stats: pd.DataFrame, verbose: bool = True) -> Pipeline:
    """
    Train and save the MLB win probability model.
    Returns the fitted sklearn Pipeline.
    """
    X, y = _build_matchup_rows(team_stats)
    if X.empty:
        raise ValueError("No training data – run mlb_fetcher.build_game_dataset() first.")

    X = X[FEATURE_COLS].fillna(0)

    pipeline = Pipeline([
        ("scaler", StandardScaler()),
        ("clf", GradientBoostingClassifier(
            n_estimators=300,
            max_depth=3,
            learning_rate=0.05,
            subsample=0.8,
            random_state=42,
        )),
    ])

    cv_scores = cross_val_score(pipeline, X, y, cv=5, scoring="roc_auc")
    pipeline.fit(X, y)

    os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
    joblib.dump(pipeline, MODEL_PATH)

    if verbose:
        print(f"[mlb_model] Trained on {len(X)} matchup rows ({team_stats['season'].nunique()} seasons)")
        print(f"[mlb_model] CV ROC-AUC: {cv_scores.mean():.3f} ± {cv_scores.std():.3f}")
        print(f"[mlb_model] Saved to {MODEL_PATH}")

    return pipeline


def load_model() -> Pipeline | None:
    """Load saved model from disk. Returns None if not found."""
    if os.path.exists(MODEL_PATH):
        return joblib.load(MODEL_PATH)
    print("[mlb_model] No saved model found – call train() first.")
    return None


def predict_game(home_stats: dict, away_stats: dict, model: Pipeline | None = None) -> dict:
    """
    Predict win probability for a single matchup.

    home_stats / away_stats: dicts with keys matching FEATURE_COLS
      (runs_scored, bat_avg, obp, slg, era, whip, k_per_9, bb_per_9, fip)

    Returns:
      {home_win_prob, away_win_prob, model_edge_home, model_edge_away}
    """
    if model is None:
        model = load_model()
    if model is None:
        return {"home_win_prob": 0.5, "away_win_prob": 0.5}

    row = {}
    for col in FEATURE_COLS:
        if col == "is_home":
            row[col] = 1
            continue
        row[col] = float(home_stats.get(col, 0) or 0) - float(away_stats.get(col, 0) or 0)

    X = pd.DataFrame([row])[FEATURE_COLS].fillna(0)
    probs = model.predict_proba(X)[0]
    home_prob = float(probs[1])
    away_prob = 1.0 - home_prob

    return {
        "home_win_prob": round(home_prob, 4),
        "away_win_prob": round(away_prob, 4),
    }


def predict_from_season_stats(
    home_team: str,
    away_team: str,
    team_stats: pd.DataFrame,
    model: Pipeline | None = None,
) -> dict:
    """
    Look up both teams' stats from the team_stats DataFrame and predict.
    Uses most recent season available for each team.
    """
    def get_stats(team_name: str) -> dict:
        rows = team_stats[team_stats["team"].str.contains(team_name, case=False, na=False)]
        if rows.empty:
            return {}
        return rows.sort_values("season", ascending=False).iloc[0].to_dict()

    home_s = get_stats(home_team)
    away_s = get_stats(away_team)

    if not home_s or not away_s:
        missing = home_team if not home_s else away_team
        print(f"[mlb_model] No stats found for: {missing}")
        return {"home_win_prob": 0.5, "away_win_prob": 0.5}

    return predict_game(home_s, away_s, model)


# ---------------------------------------------------------------------------
# Player Props
# ---------------------------------------------------------------------------

def evaluate_player_prop(
    stat_per_game: float,
    line: float,
    stat_std: float | None = None,
) -> dict:
    """
    Estimate over/under probability for a player prop using a normal distribution.

    stat_per_game : player's historical average (e.g. 0.28 H/game)
    line          : prop line set by the bookmaker (e.g. 0.5 hits)
    stat_std      : optional standard deviation; defaults to 30% of mean

    Returns: {over_prob, under_prob, expected_value, recommendation}
    """
    from scipy.stats import norm

    if stat_std is None or stat_std <= 0:
        stat_std = max(stat_per_game * 0.30, 0.05)

    over_prob = float(1 - norm.cdf(line, loc=stat_per_game, scale=stat_std))
    under_prob = 1.0 - over_prob

    recommendation = "OVER" if over_prob > 0.55 else ("UNDER" if under_prob > 0.55 else "NO EDGE")

    return {
        "avg_per_game": round(stat_per_game, 3),
        "prop_line": line,
        "over_prob": round(over_prob, 4),
        "under_prob": round(under_prob, 4),
        "recommendation": recommendation,
    }
