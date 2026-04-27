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
from sklearn.calibration import CalibratedClassifierCV
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

    base = Pipeline([
        ("scaler", StandardScaler()),
        ("clf", GradientBoostingClassifier(
            n_estimators=300,
            max_depth=3,
            learning_rate=0.05,
            subsample=0.8,
            random_state=42,
        )),
    ])
    # Wrap with isotonic calibration so probs don't collapse to 0/1
    pipeline = CalibratedClassifierCV(base, method="isotonic", cv=5)

    cv_scores = cross_val_score(base, X, y, cv=5, scoring="roc_auc")
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


# Extra features added by retrain_with_history (must be kept in sync)
_EXTRA_FEATURE_COLS = ["sentiment_score", "injury_count"]

# Combined feature list used by the sentiment-enhanced model
ENHANCED_FEATURE_COLS = FEATURE_COLS + _EXTRA_FEATURE_COLS

# Path for the enhanced model (separate so the base model stays usable as fallback)
_ENHANCED_MODEL_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "models", "mlb_model_enhanced.joblib"
)


def _load_team_sentiment_from_db() -> dict[str, float]:
    """
    Pull average sentiment per team from the last 30 days of news_articles.
    Returns {team_name_lower: avg_sentiment}.
    """
    try:
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
        from data.db import get_conn
        import psycopg2.extras
        conn = get_conn()
        if not conn:
            return {}
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT lower(team) AS team,
                   AVG(sentiment) AS avg_sentiment
            FROM   news_articles
            WHERE  fetched_at > NOW() - INTERVAL '30 days'
            GROUP  BY lower(team)
        """)
        result = {r["team"]: float(r["avg_sentiment"]) for r in cur.fetchall()}
        conn.close()
        return result
    except Exception as e:
        print(f"[mlb_model] sentiment DB read error: {e}")
        return {}


def _load_team_injury_counts_from_db() -> dict[str, int]:
    """
    Count injuries per team from injury_reports in the last 14 days.
    Returns {team_name_lower: count}.
    """
    try:
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
        from data.db import get_conn
        import psycopg2.extras
        conn = get_conn()
        if not conn:
            return {}
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT lower(team_name) AS team,
                   COUNT(*) AS cnt
            FROM   injury_reports
            WHERE  updated_at > NOW() - INTERVAL '14 days'
            GROUP  BY lower(team_name)
        """)
        result = {r["team"]: int(r["cnt"]) for r in cur.fetchall()}
        conn.close()
        return result
    except Exception as e:
        print(f"[mlb_model] injury DB read error: {e}")
        return {}


def _fuzzy_team_key(name: str, lookup: dict) -> str | None:
    """Find best matching key in lookup dict for a team name."""
    name_l = name.lower()
    if name_l in lookup:
        return name_l
    # Try last word (e.g. "Yankees")
    last = name_l.split()[-1] if name_l.split() else ""
    if last and any(last in k for k in lookup):
        return next(k for k in lookup if last in k)
    return None


def retrain_with_history(team_stats: pd.DataFrame, verbose: bool = True) -> Pipeline | None:
    """
    Retrain the model with sentiment + injury features pulled from the DB.
    Saves as mlb_model_enhanced.joblib for the dashboard to pick up.
    Falls back to the base model if DB data is insufficient.
    """
    sentiment_map  = _load_team_sentiment_from_db()
    injury_map     = _load_team_injury_counts_from_db()

    if not sentiment_map:
        print("[mlb_model] No sentiment data in DB — training base model only")
        return train(team_stats, verbose=verbose)

    print(f"[mlb_model] Sentiment coverage: {len(sentiment_map)} teams")
    print(f"[mlb_model] Injury coverage:    {len(injury_map)} teams")

    if team_stats.empty:
        print("[mlb_model] No team stats — cannot retrain")
        return None

    rows_X, rows_y = [], []

    for season, grp in team_stats.groupby("season"):
        teams = grp.reset_index(drop=True)
        for i in range(len(teams)):
            for j in range(i + 1, len(teams)):
                home = teams.iloc[i]
                away = teams.iloc[j]

                def diff(col):
                    return float(home.get(col, 0) or 0) - float(away.get(col, 0) or 0)

                # Base stat features (differentials)
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

                # Sentiment differential (0 when not in DB)
                hk = _fuzzy_team_key(str(home.get("team", "")), sentiment_map)
                ak = _fuzzy_team_key(str(away.get("team", "")), sentiment_map)
                h_sent = sentiment_map.get(hk, 0.0) if hk else 0.0
                a_sent = sentiment_map.get(ak, 0.0) if ak else 0.0
                row["sentiment_score"] = round(h_sent - a_sent, 4)

                # Injury differential (more injuries = disadvantage)
                hik = _fuzzy_team_key(str(home.get("team", "")), injury_map)
                aik = _fuzzy_team_key(str(away.get("team", "")), injury_map)
                h_inj = injury_map.get(hik, 0) if hik else 0
                a_inj = injury_map.get(aik, 0) if aik else 0
                row["injury_count"] = h_inj - a_inj

                label = 1 if float(home.get("runs_scored", 0) or 0) > float(away.get("runs_scored", 0) or 0) else 0
                rows_X.append(row)
                rows_y.append(label)

                row_rev = {k: -v if k != "is_home" else 0 for k, v in row.items()}
                row_rev["is_home"] = 0
                rows_X.append(row_rev)
                rows_y.append(1 - label)

    X = pd.DataFrame(rows_X)[ENHANCED_FEATURE_COLS].fillna(0)
    y = pd.Series(rows_y)

    if len(X) < 20:
        print("[mlb_model] Not enough data for enhanced training")
        return train(team_stats, verbose=verbose)

    base = Pipeline([
        ("scaler", StandardScaler()),
        ("clf", GradientBoostingClassifier(
            n_estimators=300,
            max_depth=3,
            learning_rate=0.05,
            subsample=0.8,
            random_state=42,
        )),
    ])
    pipeline = CalibratedClassifierCV(base, method="isotonic", cv=5)

    try:
        cv_scores = cross_val_score(base, X, y, cv=5, scoring="roc_auc")
        pipeline.fit(X, y)
    except Exception as e:
        print(f"[mlb_model] Enhanced training error: {e} — falling back to base model")
        return train(team_stats, verbose=verbose)

    os.makedirs(os.path.dirname(_ENHANCED_MODEL_PATH), exist_ok=True)
    joblib.dump(pipeline, _ENHANCED_MODEL_PATH)
    # Also overwrite the primary model so dashboard picks it up immediately
    joblib.dump(pipeline, MODEL_PATH)

    if verbose:
        print(f"[mlb_model] Enhanced model trained on {len(X)} rows ({team_stats['season'].nunique()} seasons)")
        print(f"[mlb_model] CV ROC-AUC: {cv_scores.mean():.3f} ± {cv_scores.std():.3f}")
        print(f"[mlb_model] Saved to {_ENHANCED_MODEL_PATH} + {MODEL_PATH}")

    return pipeline


# ─── Self-improvement / calibration ──────────────────────────────────────────

def auto_improve(
    team_stats: pd.DataFrame,
    min_resolved: int = 50,
    ece_threshold: float = 0.10,
    verbose: bool = True,
) -> dict:
    """
    Self-improvement loop:
      1. Compute Expected Calibration Error (ECE) from resolved predictions.
      2. If ECE > ece_threshold AND we have >= min_resolved predictions,
         trigger retrain_with_history() to improve the model.
      3. Log what happened and return a summary dict.

    ECE is the average |predicted_prob − actual_win_rate| weighted by bucket size.
    ECE < 0.05 = excellent, 0.05-0.10 = acceptable, > 0.10 = retrain.
    """
    cal = _compute_calibration()
    total = cal.get("total_resolved", 0)
    ece   = cal.get("expected_calibration_error")

    result = {
        "total_resolved": total,
        "ece":            ece,
        "bins":           cal.get("calibration_bins", []),
        "retrained":      False,
        "msg":            "",
    }

    if total < min_resolved:
        result["msg"] = (
            f"Only {total} resolved predictions — need {min_resolved} "
            f"before auto-improvement triggers."
        )
        if verbose:
            print(f"[mlb_model] auto_improve: {result['msg']}")
        return result

    if ece is None:
        result["msg"] = "Calibration unavailable (no resolved predictions)"
        return result

    if verbose:
        print(f"[mlb_model] auto_improve: ECE={ece:.4f}, resolved={total}")
        for b in cal.get("calibration_bins", []):
            bar = "█" * int(b["gap"] * 20)
            print(f"  {b['bin']:10s} n={b['n']:4d}  "
                  f"pred={b['avg_pred']:.2f} actual={b['avg_actual']:.2f}  "
                  f"gap={b['gap']:.3f} {bar}")

    if ece > ece_threshold:
        if verbose:
            print(f"[mlb_model] ECE={ece:.4f} > threshold={ece_threshold} — triggering retrain")
        try:
            retrain_with_history(team_stats, verbose=verbose)
            result["retrained"] = True
            result["msg"] = (
                f"Auto-retrained (ECE was {ece:.4f}). "
                f"Model refreshed with {total} resolved examples."
            )
        except Exception as e:
            result["msg"] = f"Auto-retrain failed: {e}"
    else:
        result["msg"] = (
            f"Model calibration is good (ECE={ece:.4f}). No retrain needed."
        )

    if verbose:
        print(f"[mlb_model] auto_improve: {result['msg']}")

    return result


def _compute_calibration() -> dict:
    """
    Compute Expected Calibration Error (ECE) from the predictions DB table.
    Returns dict with total_resolved, expected_calibration_error, calibration_bins.
    """
    try:
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
        from data.db import get_conn
        import psycopg2.extras
        conn = get_conn()
        if not conn:
            return {}
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT model_prob, outcome
            FROM   predictions
            WHERE  outcome IN ('WIN', 'LOSS')
              AND  model_prob IS NOT NULL
            ORDER  BY model_prob
        """)
        rows = cur.fetchall()
        conn.close()

        if len(rows) < 10:
            return {"total_resolved": len(rows)}

        probs   = np.array([float(r["model_prob"]) for r in rows])
        actuals = np.array([1.0 if r["outcome"] == "WIN" else 0.0 for r in rows])
        n = len(probs)

        bins = np.linspace(0, 1, 11)
        ece  = 0.0
        calibration_bins = []
        for lo, hi in zip(bins[:-1], bins[1:]):
            mask = (probs >= lo) & (probs < hi)
            if not mask.any():
                continue
            bn       = int(mask.sum())
            avg_pred = float(probs[mask].mean())
            avg_act  = float(actuals[mask].mean())
            ece     += (bn / n) * abs(avg_pred - avg_act)
            calibration_bins.append({
                "bin":        f"{lo:.1f}-{hi:.1f}",
                "n":          bn,
                "avg_pred":   round(avg_pred, 3),
                "avg_actual": round(avg_act, 3),
                "gap":        round(abs(avg_pred - avg_act), 3),
            })

        return {
            "total_resolved":            n,
            "expected_calibration_error": round(float(ece), 4),
            "calibration_bins":          calibration_bins,
        }
    except Exception as e:
        print(f"[mlb_model] _compute_calibration error: {e}")
        return {}


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

    # Clamp to realistic MLB range (best teams win ~65% of games)
    home_prob = min(max(home_prob, 0.30), 0.70)
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
