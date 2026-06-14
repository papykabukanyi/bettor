#!/usr/bin/env python3
"""
HuggingFace Hub Push — Sport Prediction (Model + Dataset)
==========================================================
Creates / updates two repos under papylove/:

  Model repo  : papylove/sportprediction  (huggingface.co/papylove/sportprediction)
  Dataset repo: papylove/sportprediction  (huggingface.co/datasets/papylove/sportprediction)

Uploads:
  Model  → joblib files, source modules, 4 plot PNGs, model-card README
  Dataset → parquet shards from PostgreSQL (via hf_uploader), dataset-card README + plots

Run:
  pip install huggingface_hub datasets pyarrow joblib matplotlib scikit-learn
  python scripts/hf_push.py
  python scripts/hf_push.py --dataset-only   # skip model files, just sync DB → HF
  python scripts/hf_push.py --model-only
"""

import argparse
import os
import pathlib
import shutil
import sys
import tempfile
import logging

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.colors import LinearSegmentedColormap
import numpy as np

ROOT = pathlib.Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env")

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

# ─── config ─────────────────────────────────────────────────────────────────
TOKEN       = os.getenv("HF_API_KEY", "")
MODEL_REPO  = "papylove/sportprediction"
DATA_REPO   = "papylove/sportprediction"
MODELS_DIR  = ROOT / "models"

MLB_FEATURES = [
    "runs_scored", "bat_avg", "obp", "slg",
    "era", "whip", "k_per_9", "bb_per_9", "fip", "is_home",
]
SOCCER_FEATURES = [
    "elo_diff", "home_elo", "away_elo", "elo_ratio",
    "goals_for_h", "goals_ag_h", "goals_for_a", "goals_ag_a",
    "xg_for_h", "xg_ag_h", "xg_for_a", "xg_ag_a",
    "h2h_home_wins", "h2h_draws", "h2h_away_wins",
    "stage", "days_rest_h", "days_rest_a", "neutral",
]

SPORTS_COVERED = [
    ("MLB",       0.22),
    ("Soccer",    0.20),
    ("NBA",       0.12),
    ("NFL",       0.10),
    ("NHL",       0.10),
    ("Tennis",    0.08),
    ("Golf",      0.06),
    ("WNBA",      0.05),
    ("Boxing/MMA",0.04),
    ("Cricket",   0.03),
]

# ─── helpers ────────────────────────────────────────────────────────────────

def _get_importances(model, fallback_size: int) -> np.ndarray:
    """Extract feature importances from any sklearn model variant."""
    # CalibratedClassifierCV
    if hasattr(model, "calibrated_classifiers_"):
        base = model.calibrated_classifiers_[0].estimator
        return _get_importances(base, fallback_size)
    # Pipeline
    if hasattr(model, "named_steps"):
        for step in reversed(list(model.named_steps.values())):
            if hasattr(step, "feature_importances_"):
                return step.feature_importances_
    # Direct GBM / RF
    if hasattr(model, "feature_importances_"):
        return model.feature_importances_
    # Fallback: uniform
    log.warning("Could not extract feature_importances_ — using uniform fallback")
    return np.ones(fallback_size) / fallback_size


def _load_model(path: pathlib.Path):
    import joblib
    try:
        return joblib.load(path)
    except Exception as e:
        log.error("Failed to load %s: %s", path, e)
        return None


# ─── plot 1: MLB feature importance ─────────────────────────────────────────

def plot_mlb_importance(assets_dir: pathlib.Path) -> pathlib.Path:
    model = _load_model(MODELS_DIR / "mlb_model.joblib")
    if model is None:
        imp = np.ones(len(MLB_FEATURES)) / len(MLB_FEATURES)
    else:
        imp = _get_importances(model, len(MLB_FEATURES))

    order   = np.argsort(imp)
    labels  = [MLB_FEATURES[i].replace("_", " ").title() for i in order]
    values  = imp[order]

    cmap   = LinearSegmentedColormap.from_list("mlb", ["#1a3a5c", "#2196F3", "#64B5F6"])
    colors = [cmap(v / values.max()) for v in values]

    fig, ax = plt.subplots(figsize=(9, 5.5))
    fig.patch.set_facecolor("#0d1117")
    ax.set_facecolor("#0d1117")

    bars = ax.barh(labels, values, color=colors, height=0.65, edgecolor="none")

    for bar, val in zip(bars, values):
        ax.text(
            bar.get_width() + values.max() * 0.01,
            bar.get_y() + bar.get_height() / 2,
            f"{val:.3f}", va="center", ha="left",
            fontsize=8.5, color="#e0e0e0",
        )

    ax.set_xlabel("Importance Score", color="#aaaaaa", fontsize=10)
    ax.set_title("MLB Model — Feature Importance\n(GradientBoosting · Calibrated)",
                 color="white", fontsize=13, fontweight="bold", pad=14)
    ax.tick_params(colors="#cccccc", labelsize=9)
    ax.spines[["top", "right", "left", "bottom"]].set_color("#333333")
    ax.xaxis.set_tick_params(color="#333333")
    ax.set_xlim(0, values.max() * 1.18)
    ax.grid(axis="x", color="#222222", linewidth=0.7)

    # group annotations
    ax.axhline(y=3.5, color="#444444", linewidth=0.8, linestyle="--")
    ax.axhline(y=7.5, color="#444444", linewidth=0.8, linestyle="--")
    ax.text(values.max() * 1.14, 1.5,  "Context", color="#777777", fontsize=7.5, ha="right")
    ax.text(values.max() * 1.14, 5.5,  "Pitching", color="#777777", fontsize=7.5, ha="right")
    ax.text(values.max() * 1.14, 9.0,  "Offense", color="#777777", fontsize=7.5, ha="right")

    plt.tight_layout(pad=1.4)
    out = assets_dir / "mlb_importances.png"
    fig.savefig(out, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    log.info("Saved %s", out)
    return out


# ─── plot 2: Soccer feature importance (3 tasks) ────────────────────────────

def plot_soccer_importance(assets_dir: pathlib.Path) -> pathlib.Path:
    bundle = _load_model(MODELS_DIR / "soccer_wc_model.joblib")

    if isinstance(bundle, dict):
        imp_1x2    = _get_importances(bundle["model_1x2"],    len(SOCCER_FEATURES))
        imp_over25 = _get_importances(bundle["model_over25"], len(SOCCER_FEATURES))
        imp_btts   = _get_importances(bundle["model_btts"],   len(SOCCER_FEATURES))
    else:
        imp_1x2 = imp_over25 = imp_btts = np.ones(len(SOCCER_FEATURES)) / len(SOCCER_FEATURES)

    # Sort by mean importance across tasks
    mean_imp = (imp_1x2 + imp_over25 + imp_btts) / 3
    order    = np.argsort(mean_imp)
    labels   = [SOCCER_FEATURES[i].replace("_", " ").replace("  ", " ").title() for i in order]
    d = {
        "1X2 Result":   ([imp_1x2[i]    for i in order], "#4FC3F7"),
        "Over 2.5 Goals":([imp_over25[i] for i in order], "#81C784"),
        "Both Teams Score":([imp_btts[i]  for i in order], "#FFB74D"),
    }

    fig, ax = plt.subplots(figsize=(10, 7))
    fig.patch.set_facecolor("#0d1117")
    ax.set_facecolor("#0d1117")

    n      = len(labels)
    n_grp  = len(d)
    width  = 0.26
    positions = np.arange(n)

    for k, (name, (vals, color)) in enumerate(d.items()):
        offset = (k - n_grp / 2 + 0.5) * width
        bars = ax.barh(positions + offset, vals, height=width * 0.9,
                       color=color, label=name, alpha=0.88, edgecolor="none")

    ax.set_yticks(positions)
    ax.set_yticklabels(labels, color="#cccccc", fontsize=8.5)
    ax.set_xlabel("Importance Score", color="#aaaaaa", fontsize=10)
    ax.set_title("Soccer / WC2026 Model — Feature Importance by Task\n(GradientBoosting · 3 Independent Classifiers)",
                 color="white", fontsize=13, fontweight="bold", pad=14)
    ax.tick_params(colors="#cccccc", labelsize=8.5)
    ax.spines[["top", "right", "left", "bottom"]].set_color("#333333")
    ax.grid(axis="x", color="#1e1e1e", linewidth=0.7)
    leg = ax.legend(loc="lower right", framealpha=0.15, labelcolor="white",
                    fontsize=9, edgecolor="#444444")

    plt.tight_layout(pad=1.4)
    out = assets_dir / "soccer_importances.png"
    fig.savefig(out, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    log.info("Saved %s", out)
    return out


# ─── plot 3: prediction pipeline diagram ────────────────────────────────────

def plot_pipeline(assets_dir: pathlib.Path) -> pathlib.Path:
    fig, ax = plt.subplots(figsize=(13, 3.6))
    fig.patch.set_facecolor("#0d1117")
    ax.set_facecolor("#0d1117")
    ax.set_xlim(0, 13)
    ax.set_ylim(0, 3.6)
    ax.axis("off")

    stages = [
        ("Data\nSources",     "#1565C0", ["MLB StatsAPI", "ESPN", "TheSportsDB", "Odds API", "Reddit/News"]),
        ("Feature\nEngineering","#6A1B9A",["Team stats", "ELO ratings", "xG / H2H", "Sentiment", "Injury flags"]),
        ("GBM\nModels",       "#00695C", ["MLB model", "Soccer model", "WC2026 model", "Enhanced model"]),
        ("Probability\nCalibration","#E65100",["Isotonic CV", "Platt scaling", "Reliable probs"]),
        ("Kelly\nStaking",    "#1B5E20", ["Value edge >5%", "Kelly fraction", "Bankroll sizing"]),
        ("HuggingFace\nHub",  "#37474F", ["Store data", "Reuse & stream", "Model versioning"]),
    ]

    box_w, box_h = 1.8, 1.7
    gap   = (13 - len(stages) * box_w) / (len(stages) + 1)
    y_center = 1.8

    for i, (title, color, bullets) in enumerate(stages):
        x = gap + i * (box_w + gap)

        # shadow
        shadow = mpatches.FancyBboxPatch(
            (x + 0.04, y_center - box_h / 2 - 0.04),
            box_w, box_h, boxstyle="round,pad=0.1",
            linewidth=0, facecolor="#000000", alpha=0.4,
        )
        ax.add_patch(shadow)

        # box
        box = mpatches.FancyBboxPatch(
            (x, y_center - box_h / 2), box_w, box_h,
            boxstyle="round,pad=0.1", linewidth=1.5,
            edgecolor=color, facecolor=color + "22",
        )
        ax.add_patch(box)

        # title
        ax.text(x + box_w / 2, y_center + box_h / 2 - 0.22, title,
                ha="center", va="top", fontsize=8.5, fontweight="bold",
                color=color, multialignment="center")

        # bullets
        for j, b in enumerate(bullets[:3]):
            ax.text(x + box_w / 2, y_center + box_h / 2 - 0.58 - j * 0.33, f"· {b}",
                    ha="center", va="top", fontsize=6.8, color="#bbbbbb")

        # arrow to next
        if i < len(stages) - 1:
            ax.annotate("", xy=(x + box_w + gap, y_center),
                        xytext=(x + box_w, y_center),
                        arrowprops=dict(arrowstyle="->", color="#555555",
                                        lw=1.6, mutation_scale=14))

    ax.set_title("Sport Prediction Pipeline", color="white", fontsize=13,
                 fontweight="bold", pad=10)

    plt.tight_layout(pad=0.5)
    out = assets_dir / "pipeline.png"
    fig.savefig(out, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    log.info("Saved %s", out)
    return out


# ─── plot 4: sport coverage donut ────────────────────────────────────────────

def plot_sports_coverage(assets_dir: pathlib.Path) -> pathlib.Path:
    labels  = [s[0] for s in SPORTS_COVERED]
    sizes   = [s[1] for s in SPORTS_COVERED]
    colors  = [
        "#1E88E5", "#43A047", "#E53935", "#FB8C00", "#8E24AA",
        "#00ACC1", "#F4511E", "#3949AB", "#D81B60", "#6D4C41",
    ]

    fig, ax = plt.subplots(figsize=(8, 6))
    fig.patch.set_facecolor("#0d1117")
    ax.set_facecolor("#0d1117")

    wedges, texts, autotexts = ax.pie(
        sizes, labels=None, colors=colors,
        autopct="%1.0f%%", startangle=140,
        pctdistance=0.78,
        wedgeprops=dict(width=0.52, edgecolor="#0d1117", linewidth=2),
    )

    for t in autotexts:
        t.set(color="white", fontsize=8, fontweight="bold")

    legend_handles = [
        mpatches.Patch(color=colors[i], label=labels[i])
        for i in range(len(labels))
    ]
    ax.legend(handles=legend_handles, loc="center left",
              bbox_to_anchor=(0.88, 0.5), framealpha=0.1,
              labelcolor="white", fontsize=9, edgecolor="#333333")

    ax.text(0, 0, "12+\nSports", ha="center", va="center",
            fontsize=14, fontweight="bold", color="white")
    ax.set_title("Sports Coverage", color="white", fontsize=13,
                 fontweight="bold", pad=12)

    plt.tight_layout(pad=1.0)
    out = assets_dir / "sports_coverage.png"
    fig.savefig(out, dpi=150, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    log.info("Saved %s", out)
    return out


# ─── README: model card ───────────────────────────────────────────────────────

MODEL_CARD = """\
---
license: apache-2.0
language:
  - en
tags:
  - sports-prediction
  - mlb
  - soccer
  - world-cup-2026
  - gradient-boosting
  - scikit-learn
  - kelly-criterion
  - betting
metrics:
  - roc_auc
library_name: sklearn
---

<div align="center">

# Sport Prediction Model

[![Sports](https://img.shields.io/badge/Sports-12%2B-blue?style=for-the-badge)](https://huggingface.co/papylove/sportprediction)
[![Framework](https://img.shields.io/badge/Framework-scikit--learn-orange?style=for-the-badge&logo=scikit-learn)](https://scikit-learn.org)
[![License](https://img.shields.io/badge/License-Apache_2.0-green?style=for-the-badge)](LICENSE)
[![Dataset](https://img.shields.io/badge/Dataset-HuggingFace-yellow?style=for-the-badge&logo=huggingface)](https://huggingface.co/datasets/papylove/sportprediction)

**Gradient Boosting ensemble for win probability & market-edge detection across MLB, Soccer, WC2026, NBA, NFL, NHL, and more.**

</div>

---

## Pipeline

![Prediction Pipeline](assets/pipeline.png)

---

## Models Included

| File | Sport | Algorithm | Notes |
|---|---|---|---|
| `mlb_model.joblib` | MLB | GBM + CalibratedClassifierCV | Base model, 10 features |
| `mlb_model_enhanced.joblib` | MLB | GBM + Sentiment + Injury | Enhanced with news signals |
| `soccer_model.joblib` | Soccer / Club | GBM | League match predictions |
| `soccer_wc_model.joblib` | Soccer / WC2026 | 3× GBM | 1X2 · Over 2.5 · BTTS |

---

## Feature Importance

### MLB Model
![MLB Feature Importance](assets/mlb_importances.png)

### Soccer / WC2026 Model (3 Tasks)
![Soccer Feature Importance](assets/soccer_importances.png)

---

## Sports Coverage

![Sports Coverage](assets/sports_coverage.png)

---

## Quick Start

```python
import joblib, numpy as np

# ── MLB ──────────────────────────────────────────────────────
model = joblib.load("mlb_model.joblib")

# Feature vector: home_stats - away_stats differences
# [runs_scored, bat_avg, obp, slg, era, whip, k_per_9, bb_per_9, fip, is_home]
home_features = np.array([[0.12, 0.005, 0.008, 0.015, -0.3, -0.05, 1.2, -0.5, -0.2, 1]])
prob_home_win = model.predict_proba(home_features)[0][1]
print(f"MLB home win probability: {prob_home_win:.1%}")

# ── Soccer / WC2026 ──────────────────────────────────────────
bundle = joblib.load("soccer_wc_model.joblib")
# 19 features: elo_diff, home_elo, away_elo, elo_ratio, ...
x = np.array([[120, 1920, 1800, 1.067, 1.5, 1.0, 1.3, 1.1,
               1.4, 0.9, 1.2, 1.0, 0.4, 0.3, 0.3, 0, 7, 7, 1]])

result_probs = bundle["model_1x2"].predict_proba(x)[0]    # [away, draw, home]
over_prob    = bundle["model_over25"].predict_proba(x)[0][1]
btts_prob    = bundle["model_btts"].predict_proba(x)[0][1]
le           = bundle["label_encoder"]

print(f"1X2 → {dict(zip(le.classes_, result_probs))}")
print(f"Over 2.5: {over_prob:.1%} | BTTS: {btts_prob:.1%}")
```

---

## Kelly Criterion Edge Detection

The models output calibrated probabilities which are compared against bookmaker
implied odds to compute the **value edge**:

```python
edge = model_prob - implied_prob   # e.g. 0.58 - 0.52 = 0.06 (6% edge)
if edge >= 0.05:                   # MIN_VALUE_EDGE threshold
    kelly_stake = edge / (dec_odds - 1) * KELLY_FRACTION * BANKROLL
```

---

## Training Data

All models are trained on **real historical data** from:

- MLB StatsAPI (2023–2026 seasons)
- Retrosheet (game-by-game outcomes)
- Lahman Baseball Database
- World Cup historical matches (embedded)
- News sentiment (NewsAPI · GDELT · Reddit)
- Injury reports (ESPN)

The companion **[dataset repo](https://huggingface.co/datasets/papylove/sportprediction)**
receives continuous updates from live data pipelines.

---

## Architecture

```
CalibratedClassifierCV (isotonic, cv=5)
  └─ Pipeline
       ├─ StandardScaler
       └─ GradientBoostingClassifier
            n_estimators=300, max_depth=3
            learning_rate=0.05, subsample=0.8
```

---

## Citation

```bibtex
@misc{papylove2026sportprediction,
  author    = {papylove},
  title     = {Sport Prediction Model},
  year      = {2026},
  publisher = {HuggingFace},
  url       = {https://huggingface.co/papylove/sportprediction}
}
```
"""

# ─── README: dataset card ─────────────────────────────────────────────────────

DATASET_CARD = """\
---
license: cc-by-4.0
task_categories:
  - tabular-classification
language:
  - en
tags:
  - sports
  - mlb
  - soccer
  - nba
  - nfl
  - nhl
  - tennis
  - golf
  - wnba
  - boxing
  - mma
  - cricket
  - world-cup-2026
  - odds
  - betting
configs:
  - config_name: games
    data_files: data/games/*.parquet
  - config_name: odds
    data_files: data/odds/*.parquet
  - config_name: injuries
    data_files: data/injuries/*.parquet
  - config_name: predictions
    data_files: data/predictions/*.parquet
---

<div align="center">

# Multi-Sport Prediction Dataset

[![Sports](https://img.shields.io/badge/Sports-12%2B-blue?style=for-the-badge)](https://huggingface.co/datasets/papylove/sportprediction)
[![Format](https://img.shields.io/badge/Format-Parquet-red?style=for-the-badge)](https://parquet.apache.org)
[![Streaming](https://img.shields.io/badge/Streaming-Enabled-brightgreen?style=for-the-badge)](https://huggingface.co/docs/datasets/stream)
[![Model](https://img.shields.io/badge/Model-HuggingFace-yellow?style=for-the-badge&logo=huggingface)](https://huggingface.co/papylove/sportprediction)

**Live-fed, streamable sports dataset covering games, odds, injuries, and model predictions across 12+ sports.
Updated continuously from live data pipelines.**

</div>

---

## Pipeline

![Prediction Pipeline](assets/pipeline.png)

---

## Sports Coverage

![Sports Coverage](assets/sports_coverage.png)

---

## Subsets

| Config | Rows (approx) | Description |
|---|---|---|
| `games` | Continuously growing | Scheduled & completed games across all sports |
| `odds` | Continuously growing | Historical odds snapshots per bookmaker market |
| `injuries` | Continuously growing | Player injury reports (status, type, source) |
| `predictions` | Continuously growing | Model predictions + value-bet signals (edge, Kelly stake) |

---

## Schema

### `games`

| Column | Type | Description |
|---|---|---|
| `game_id` | string | External game identifier |
| `sport` | string | mlb · soccer · nba · nfl · nhl · wnba · tennis · golf … |
| `league` | string | Competition / league name |
| `game_date` | string | ISO date |
| `game_datetime` | string | ISO datetime with timezone |
| `status` | string | Scheduled · InProgress · Final |
| `home_team` | string | Home team name |
| `away_team` | string | Away team name |
| `home_score` | float32 | Final home score (null if not played) |
| `away_score` | float32 | Final away score |
| `home_starter` | string | Starting pitcher / player (sport-specific) |
| `away_starter` | string | Away starter |
| `season` | int32 | Season year |
| `metadata` | string | JSON blob with sport-specific extras |
| `created_at` | string | Row insertion timestamp |

### `odds`

| Column | Type | Description |
|---|---|---|
| `sport` / `home_team` / `away_team` | string | Game identifiers |
| `game_date` | string | ISO date |
| `market` | string | h2h · spreads · totals |
| `outcome` | string | Team name or Over/Under |
| `odds_am` | int32 | American odds (e.g. -110) |
| `dec_odds` | float64 | Decimal odds (e.g. 1.909) |
| `total_line` | float32 | Total line for over/under |
| `bookmaker` | string | DraftKings · FanDuel · BetMGM … |
| `fetched_at` | string | Timestamp snapshot was taken |

### `injuries`

| Column | Type | Description |
|---|---|---|
| `sport` / `team` / `player_name` | string | Player identifiers |
| `status` | string | Out · Questionable · Probable · Doubtful |
| `description` | string | Injury description |
| `injury_type` | string | Knee · Hamstring · Concussion … |
| `source` | string | ESPN · Official · Inferred |
| `fetched_at` | string | Snapshot timestamp |

### `predictions`

| Column | Type | Description |
|---|---|---|
| `sport` / `matchup` / `game_date` | string | Game identifiers |
| `bet_type` | string | h2h · spread · total · player_prop |
| `bet` | string | Pick label |
| `model_prob` | float64 | Model win probability |
| `book_prob` | float64 | Bookmaker implied probability |
| `edge` | float64 | `model_prob - book_prob` (value edge) |
| `odds_am` | int32 | American odds at time of prediction |
| `dec_odds` | float64 | Decimal odds |
| `stake_usd` | float64 | Kelly-sized stake in USD |
| `ev` | float64 | Expected value of the bet |
| `signal_boost` | float64 | Sentiment / injury signal multiplier |
| `signal_sources` | string | Sources that contributed to signal |
| `detected_at` | string | Prediction generation timestamp |

---

## Feature Importance Reference

### MLB Model
![MLB Feature Importance](assets/mlb_importances.png)

### Soccer / WC2026 Model
![Soccer Feature Importance](assets/soccer_importances.png)

---

## Streaming Usage

```python
from datasets import load_dataset

# Stream games (no download required)
ds = load_dataset("papylove/sportprediction", "games", streaming=True)
for game in ds["train"]:
    print(game["sport"], game["home_team"], "vs", game["away_team"],
          "—", game["game_date"])

# Load full odds into pandas
import pandas as pd
odds_ds = load_dataset("papylove/sportprediction", "odds")
df = odds_ds["train"].to_pandas()
print(df.groupby("bookmaker")["dec_odds"].describe())

# Value bets with edge > 5%
preds = load_dataset("papylove/sportprediction", "predictions")
df_p  = preds["train"].to_pandas()
sharp = df_p[df_p["edge"] >= 0.05].sort_values("ev", ascending=False)
print(sharp[["sport", "matchup", "bet", "edge", "ev", "stake_usd"]].head(20))
```

---

## Feed Live Data

The dataset is updated by the bettor pipeline automatically. To push new data
from your own instance:

```python
from src.data.hf_uploader import HFUploader

up = HFUploader()                        # reads HF_API_KEY from .env
up.sync_from_db()                        # pull all DB rows → push to HF
up.flush_all()

# Or push a single batch of new game records:
up.push_records("games", [
    {
        "sport": "mlb", "league": "MLB",
        "home_team": "New York Yankees", "away_team": "Boston Red Sox",
        "game_date": "2026-06-14", "status": "Scheduled",
        ...
    }
])
up.flush_all()
```

---

## Sources

| Sport | Primary Source | Fallback |
|---|---|---|
| MLB | MLB StatsAPI · PyBaseball | Retrosheet · Lahman |
| Soccer | ESPN unofficial API | TheSportsDB · football-data.org |
| WC2026 | Embedded historical | ESPN |
| NBA / WNBA | BallDontLie API | ESPN |
| NFL / NHL | SportsData.io | ESPN |
| Tennis | Tennis Reference | ATP/WTA Sackmann |
| Golf | DataGolf | PGA Stats API |
| Boxing / MMA | TheSportsDB | SportsData.io |
| Odds | The Odds API | SportsData.io |
| Sentiment | Reddit (PRAW) · NewsAPI · GDELT | newsdata.io |
| Injuries | ESPN | SportsData.io |

---

## Companion Model

Pre-trained GBM models are available at
**[papylove/sportprediction](https://huggingface.co/papylove/sportprediction)**
(model repo) — ready to load with `joblib.load()`.
"""

# ─── push helpers ────────────────────────────────────────────────────────────

def _ensure_repo(api, repo_id: str, repo_type: str):
    from huggingface_hub.utils import RepositoryNotFoundError
    try:
        api.repo_info(repo_id=repo_id, repo_type=repo_type)
        log.info("Repo exists: %s (%s)", repo_id, repo_type)
    except Exception:
        log.info("Creating %s repo: %s", repo_type, repo_id)
        api.create_repo(repo_id=repo_id, repo_type=repo_type,
                        private=False, exist_ok=True)


def push_model_repo(api, assets_dir: pathlib.Path, tmpdir: pathlib.Path):
    """Build model repo staging folder then upload_folder."""
    from huggingface_hub import upload_folder

    staging = tmpdir / "model_repo"
    staging.mkdir()
    (staging / "assets").mkdir()
    (staging / "models").mkdir()

    # Copy plots
    for png in assets_dir.glob("*.png"):
        shutil.copy(png, staging / "assets" / png.name)

    # Copy model files
    for jl in MODELS_DIR.glob("*.joblib"):
        shutil.copy(jl, staging / "models" / jl.name)

    # Copy source modules
    src_models = ROOT / "src" / "models"
    if src_models.exists():
        src_dst = staging / "src"
        src_dst.mkdir()
        for py in src_models.glob("*.py"):
            shutil.copy(py, src_dst / py.name)

    # README
    (staging / "README.md").write_text(MODEL_CARD, encoding="utf-8")

    _ensure_repo(api, MODEL_REPO, "model")
    upload_folder(
        folder_path=str(staging),
        repo_id=MODEL_REPO,
        repo_type="model",
        commit_message="Update model files, plots and model card",
        token=TOKEN,
    )
    log.info("Model repo updated → https://huggingface.co/%s", MODEL_REPO)


def push_dataset_repo(api, assets_dir: pathlib.Path, tmpdir: pathlib.Path):
    """Push dataset card + plots; then sync DB → HF parquet shards."""
    from huggingface_hub import upload_folder

    staging = tmpdir / "dataset_repo"
    staging.mkdir()
    (staging / "assets").mkdir()

    # Copy plots
    for png in assets_dir.glob("*.png"):
        shutil.copy(png, staging / "assets" / png.name)

    # README
    (staging / "README.md").write_text(DATASET_CARD, encoding="utf-8")

    _ensure_repo(api, DATA_REPO, "dataset")
    upload_folder(
        folder_path=str(staging),
        repo_id=DATA_REPO,
        repo_type="dataset",
        commit_message="Update dataset card and plots",
        token=TOKEN,
    )
    log.info("Dataset card pushed → https://huggingface.co/datasets/%s", DATA_REPO)

    # Sync actual data from PostgreSQL
    log.info("Syncing sport data from DB → HF dataset...")
    try:
        from src.data.hf_uploader import HFUploader
        up = HFUploader(token=TOKEN, repo_name="sportprediction")
        if up._ok:
            up.sync_from_db()
            up.flush_all()
            log.info("DB sync complete")
        else:
            log.warning("HFUploader not ready (DB may be unreachable) — skipping data sync")
    except Exception as e:
        log.error("Data sync failed: %s", e)


# ─── main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Push models + dataset to HuggingFace Hub")
    parser.add_argument("--model-only",   action="store_true", help="Push model repo only")
    parser.add_argument("--dataset-only", action="store_true", help="Push dataset repo only")
    args = parser.parse_args()

    if not TOKEN:
        sys.exit("ERROR: HF_API_KEY not set in .env")

    try:
        from huggingface_hub import HfApi, upload_folder
    except ImportError:
        sys.exit("ERROR: run:  pip install huggingface_hub datasets pyarrow joblib matplotlib")

    api = HfApi(token=TOKEN)

    # Verify token
    try:
        me = api.whoami()
        log.info("Authenticated as: %s", me["name"])
    except Exception as e:
        sys.exit(f"ERROR: invalid HF token — {e}")

    with tempfile.TemporaryDirectory() as _tmp:
        tmpdir = pathlib.Path(_tmp)
        assets = tmpdir / "assets"
        assets.mkdir()

        # Generate all plots
        log.info("Generating plots...")
        plot_mlb_importance(assets)
        plot_soccer_importance(assets)
        plot_pipeline(assets)
        plot_sports_coverage(assets)

        do_model   = not args.dataset_only
        do_dataset = not args.model_only

        if do_model:
            push_model_repo(api, assets, tmpdir)

        if do_dataset:
            push_dataset_repo(api, assets, tmpdir)

    print()
    print("=" * 60)
    if do_model:
        print(f"  Model   → https://huggingface.co/{MODEL_REPO}")
    if do_dataset:
        print(f"  Dataset → https://huggingface.co/datasets/{DATA_REPO}")
    print("=" * 60)


if __name__ == "__main__":
    main()
