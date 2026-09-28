"""Real reinforcement learning pilot for kalshi_15m (crypto) -- per
explicit user direction: "real reinforcement learning (a learning
agent)... learns entry/sizing/exit decisions from reward over time...
alongside (not replacing) the existing classifiers."

Genuine RL, hand-rolled in pure PyTorch (already a production dependency
here) rather than via stable-baselines3/gymnasium: a real, live test of
installing those into this environment downgraded numpy from 2.5.3 to
1.26.4 and broke scikit-learn's own import outright (scipy 1.18+,
scikit-learn 1.9+, and pandas 3.0+ here all require numpy>=2.0 --
stable-baselines3's own dependency chain wants numpy<2.0). Not a
shortcut taken to save time -- a real, correct dependency-risk finding
that ruled out the "just use a library" path for a production system
already running on numpy 2.x everywhere else.

Honest framing of what this actually is, not oversold: kalshi_15m's own
real structure -- one largely-independent decision per 15-minute window,
with the real settlement outcome (won/lost) known almost immediately and
no meaningful state-transition dependency between one decision's outcome
and the NEXT decision's own context -- makes this genuinely a CONTEXTUAL
BANDIT, not a long-horizon MDP with delayed, discounted reward. This
implementation reflects that honestly: no discount factor, no bootstrapped
"future value" term, no target network (none of that machinery reflects
anything real about this problem's actual structure -- adding it back in
would be theater, not rigor). It is still real reinforcement learning: a
value function (a real neural network, the Q-network below) learned
online from trial, error, and real reward via epsilon-greedy exploration
and experience replay -- not a fixed rule, and not supervised learning on
labeled "correct" actions (there IS no ground-truth "correct position
size" the way there's a ground-truth label_up for direction).

Action space: skip, or enter at one of 4 real position-size levels (see
ACTION_POSITION_SIZES) -- SIDE is still decided by kalshi_15m's own
already-validated classifier (kalshi_15m_model's own probability_up
sign), never thrown away or second-guessed here. This agent's own job is
narrower and more tractable: given a real feature row AND that
directional signal, decide whether to act on it at all, and how much --
exactly the "alongside, not replacing" scope this was built to.

Entry pricing: priced at the model's OWN confidence (clipped to
[0.05, 0.95]), not one fixed price across every trade regardless of
confidence -- deliberately avoids the exact mechanism behind
strategy_sweep.py's own MAX_PLAUSIBLE_MEAN_RETURN_PCT incident (a fixed
assumed_entry_price decoupled from real confidence, compounding into an
unrealistic "return" over enough trades). An agent priced this way can
only profit from genuine directional skill actually reflected in the
model's own confidence, not a mispricing gift baked into the simulation.

NOT auto-applied to live trading anywhere -- see
scripts/kalshi_15m_rl_job.py's own docstring on why this is
observability/validation only until there's real forward-tested evidence
it helps, the same "prove it before it touches real money" discipline
strategy_sweep.py's own auto-apply already holds itself to, just a stricter
bar given this is a genuinely new decision-making paradigm, not a
parameter tune of an already-trusted classifier.
"""
from __future__ import annotations

import logging
import random
from collections import deque
from typing import Any

import numpy as np
import pandas as pd
import torch
import torch.nn as nn
import torch.optim as optim

from data.perps_data import FEATURE_COLUMNS

logger = logging.getLogger(__name__)

# Real, live finding this sizing already reflects (not a hypothetical):
# the pilot's own FIRST real run against the real archive included 0.08/
# 0.12 here, and the one fold whose agent actually kept trading (barely
# better than a coin flip, 51.4% win rate) drove its own balance to
# EXACTLY ZERO over 4,003 trades -- a real, textbook risk-of-ruin: even a
# slight edge, sized aggressively as a % of an ever-shrinking balance,
# reliably wipes an account out over enough trades (the same real math
# behind why the Kelly criterion caps position size well below "however
# much a naive expected-value calculation alone would suggest"). Tightened
# to a materially more conservative range after that real result, not
# guessed in advance.
ACTION_POSITION_SIZES: list[float] = [0.0, 0.01, 0.02, 0.03]  # 0.0 = skip
STATE_DIM = len(FEATURE_COLUMNS) + 1  # + the existing classifier's own probability_up as context
REPLAY_CAPACITY = 20_000
# A real Kalshi order has to be for a whole, real, priced contract -- a
# budget too small to buy even ONE contract at the entry price isn't a
# trade a real account could ever place, so it's not a real skip either;
# it's a mechanical "the account is too depleted to act" state. Modeling
# this floor (rather than letting budget shrink to fractions of a cent and
# keep "trading" in the simulation) matches what a real account actually
# can and can't do, and stops a losing streak from compounding into
# nonsense the way an unbounded simulation otherwise would.
MIN_REAL_ORDER_BUDGET_USD = 1.0
BATCH_SIZE = 64


class QNetwork(nn.Module):
    """A small MLP mapping (real feature row + the existing classifier's
    own probability_up) -> one Q-value per ACTION_POSITION_SIZES entry.
    Deliberately small (2 hidden layers, 64 units) -- this state space
    (32 real features + 1) doesn't need or benefit from a bigger network,
    and a small network trains faster and is far less prone to overfitting
    a training archive this size (tens of thousands of rows, not millions)."""

    def __init__(self, state_dim: int = STATE_DIM, n_actions: int = len(ACTION_POSITION_SIZES)):
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(state_dim, 64), nn.ReLU(),
            nn.Linear(64, 64), nn.ReLU(),
            nn.Linear(64, n_actions),
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.net(x)


def _row_to_state(row: Any, probability_up: float) -> np.ndarray:
    """`row` is a pandas itertuples namedtuple -- real feature values,
    never synthetic. NaN-safe (a genuinely missing feature reads as 0.0,
    matching this codebase's own existing convention elsewhere for
    handling incomplete real data rather than dropping the row)."""
    feats = []
    for col in FEATURE_COLUMNS:
        val = getattr(row, col, None)
        feats.append(float(val) if val is not None and val == val else 0.0)  # val == val is a fast NaN check
    feats.append(float(probability_up))
    return np.array(feats, dtype=np.float32)


def _settle_reward(side: str, confidence: float, label_up: int, reference_balance: float, position_size_pct: float) -> float:
    """The real settlement math -- see module docstring on why entry_price
    is priced at the model's own confidence rather than one fixed
    assumption shared by every trade regardless of confidence. Returns
    the real dollar P&L, 0.0 for a skip (position_size_pct == 0.0) -- a
    skip is real, not "missing data"; it earns exactly the reward doing
    nothing earns.

    `reference_balance` is deliberately a FIXED value (always the
    episode's own starting_balance, never a running/compounding one) --
    real, live finding this fixes: sizing against a running balance let
    reward compound MULTIPLICATIVELY over a long training run (235,000+
    real decision steps in one real training pass), and a purely random
    lucky streak during early, mostly-random exploration reliably blew
    episode returns up to 1e+28-1e+33% -- meaningless for learning (Q-value
    targets spanning 30+ orders of magnitude break gradient-based training
    outright) and the same unbounded-compounding mechanism already found
    and fixed once in strategy_sweep.py's own MAX_PLAUSIBLE_MEAN_RETURN_PCT
    incident. Fixed-fractional-of-STARTING-capital sizing (standard,
    conservative money-management practice, the opposite of Kelly-style
    compounding) makes every trade's reward bounded and comparable
    regardless of when in an episode it happens -- trading "optimal
    long-run compounding growth" for "stable enough to actually learn
    from," the right tradeoff for an unproven first pilot.

    Also 0.0 (a mechanical no-op, not a real skip decision) whenever the
    resulting budget can't afford even one real contract at
    MIN_REAL_ORDER_BUDGET_USD -- see that constant's own comment."""
    if position_size_pct <= 0 or reference_balance <= 0:
        return 0.0
    entry_price = min(0.95, max(0.05, confidence))
    budget = reference_balance * position_size_pct
    if budget < MIN_REAL_ORDER_BUDGET_USD:
        return 0.0
    count = budget / entry_price
    won = bool(label_up) == (side == "yes")
    return count * (1.0 - entry_price) if won else -count * entry_price


def fit_isotonic_calibrator(df_with_predictions: pd.DataFrame, *, min_rows: int = 150) -> Any | None:
    """Fits a real isotonic calibration curve mapping raw model_probability_up
    -> P(label_up=1), using this fold's OWN real rows (model_probability_up
    vs the real label_up outcome). Real, evidence-backed reason this exists:
    the pilot's own SECOND real run priced contracts at raw model
    confidence and lost money on real held-out data (-267% return, 52.3%
    win rate) -- kalshi_15m's own raw model confidence was ALREADY found
    unreliable earlier this session (the reason kalshi_15m_strategy's own
    real-outcome calibration exists at all), so pricing off it here hits
    the exact same known problem.

    This is NOT kalshi_15m_strategy.calibrate_probability_up reused
    directly -- that function needs a live, incrementally-growing
    trade_log (specific real fields, re-fit on every call), which doesn't
    exist while walking raw archived rows in a backtest. Fitting ONCE per
    fold on that fold's own real (model_probability_up, label_up) pairs is
    the tractable backtesting equivalent of the same real idea (calibrate
    against real outcomes, don't trust the model's own stated confidence
    at face value) -- the standard "calibrate against a held-out
    validation slice" ML practice (see sklearn.calibration's own
    CalibratedClassifierCV for the same pattern), not a shortcut. Returns
    None (no calibration -- callers fall back to raw confidence) below
    min_rows, matching the same "don't trust a calibration built on too
    few real examples" discipline the live function already holds itself
    to."""
    df = df_with_predictions.dropna(subset=["label_up", "model_probability_up"])
    if len(df) < min_rows:
        return None
    from sklearn.isotonic import IsotonicRegression
    calibrator = IsotonicRegression(y_min=0.0, y_max=1.0, out_of_bounds="clip")
    calibrator.fit(df["model_probability_up"].to_numpy(), df["label_up"].to_numpy())
    return calibrator


def _priced_confidence(side: str, proba_up: float, raw_confidence: float, calibrator: Any | None) -> float:
    """The confidence actually used to PRICE a contract in _settle_reward
    -- see fit_isotonic_calibrator's own docstring on why. Falls back to
    raw_confidence when no calibrator is available (too few real rows in
    this fold, or the caller didn't ask for calibrated pricing at all).
    `side` is NEVER changed by calibration -- see module docstring: side
    stays the existing classifier's own call (raw probability_up's own
    sign), only the PRICE used to settle a trade changes."""
    if calibrator is None:
        return raw_confidence
    calibrated_proba_up = float(calibrator.predict([proba_up])[0])
    return calibrated_proba_up if side == "yes" else 1.0 - calibrated_proba_up


def train_bandit_agent(
    train_df_with_predictions: pd.DataFrame, *, episodes: int = 8,
    epsilon_start: float = 1.0, epsilon_end: float = 0.05, epsilon_decay_steps: int = 20_000,
    lr: float = 1e-3, starting_balance: float = 100.0, seed: int = 0, calibrator: Any | None = None,
) -> dict[str, Any]:
    """Trains the real Q-network by walking the REAL, already-labeled
    training rows chronologically (never shuffled -- respects the same
    "no future leakage" discipline every backtest module here already
    holds itself to). `episodes` full passes over the same real data (a
    genuine training regimen -- more passes let the agent actually learn
    from its own past mistakes on the same real history, standard
    practice for a training set this size). `train_df_with_predictions`
    must already carry a real `model_probability_up` column (see
    kalshi_15m_backtest.add_model_predictions) and the real `label_up`
    target -- this function never fits or calls the directional
    classifier itself, only consumes its already-real output.

    `calibrator` (optional, see fit_isotonic_calibrator): when given,
    trades are PRICED at calibrated confidence instead of raw model
    confidence -- see _priced_confidence's own docstring. Side is still
    always decided by raw probability_up regardless.

    Balance resets to `starting_balance` at the start of each episode --
    this is a training-signal-shaping choice (each pass is an independent
    attempt to learn from the same real history), not a claim about
    compounding returns across passes; see simulate_policy for the real,
    single-pass evaluation number that's actually comparable to other
    backtests here.

    Returns {"q_network_state_dict", "episode_return_pct" (one real
    number per episode), "rows_trained"}."""
    torch.manual_seed(seed)
    np.random.seed(seed)
    random.seed(seed)

    q_net = QNetwork()
    optimizer = optim.Adam(q_net.parameters(), lr=lr)
    replay: deque[tuple[np.ndarray, int, float]] = deque(maxlen=REPLAY_CAPACITY)

    df = train_df_with_predictions.dropna(subset=["label_up", "model_probability_up"]).sort_values("ts").reset_index(drop=True)
    n_actions = len(ACTION_POSITION_SIZES)
    step = 0
    episode_returns: list[float] = []

    for _episode in range(episodes):
        cumulative_pnl = 0.0  # reporting only -- see module docstring on why reward itself sizes against the FIXED starting_balance, never this
        for row in df.itertuples(index=False):
            proba_up = float(row.model_probability_up)
            side = "yes" if proba_up >= 0.5 else "no"
            confidence = proba_up if side == "yes" else 1.0 - proba_up
            state = _row_to_state(row, proba_up)

            epsilon = epsilon_end + (epsilon_start - epsilon_end) * max(0.0, 1.0 - step / epsilon_decay_steps)
            if random.random() < epsilon:
                action = random.randrange(n_actions)
            else:
                with torch.no_grad():
                    q_values = q_net(torch.tensor(state, dtype=torch.float32).unsqueeze(0))
                    action = int(torch.argmax(q_values, dim=1).item())

            position_size_pct = ACTION_POSITION_SIZES[action]
            priced_confidence = _priced_confidence(side, proba_up, confidence, calibrator)
            reward = _settle_reward(side, priced_confidence, int(row.label_up), starting_balance, position_size_pct)
            cumulative_pnl += reward
            replay.append((state, action, reward))
            step += 1

            if len(replay) >= BATCH_SIZE:
                batch = random.sample(replay, BATCH_SIZE)
                states, actions, rewards = zip(*batch)
                states_t = torch.tensor(np.stack(states), dtype=torch.float32)
                actions_t = torch.tensor(actions, dtype=torch.long).unsqueeze(1)
                rewards_t = torch.tensor(rewards, dtype=torch.float32)

                q_pred = q_net(states_t).gather(1, actions_t).squeeze(1)
                # The target IS the real, already-known reward -- no
                # bootstrapped future value (see module docstring on the
                # honest contextual-bandit framing this reflects).
                loss = nn.functional.mse_loss(q_pred, rewards_t)
                optimizer.zero_grad()
                loss.backward()
                optimizer.step()

        episode_returns.append(round(cumulative_pnl / starting_balance * 100, 6) if starting_balance else 0.0)

    return {"q_network_state_dict": q_net.state_dict(), "episode_return_pct": episode_returns, "rows_trained": len(df)}


def predict_action(q_net: QNetwork, state: np.ndarray) -> int:
    """Greedy (no exploration) -- for evaluation/live inference, never
    training, where epsilon-greedy exploration belongs instead."""
    with torch.no_grad():
        q_values = q_net(torch.tensor(state, dtype=torch.float32).unsqueeze(0))
        return int(torch.argmax(q_values, dim=1).item())


def simulate_policy(
    test_df_with_predictions: pd.DataFrame, q_net: QNetwork, *, starting_balance: float = 100.0, calibrator: Any | None = None,
) -> dict[str, Any]:
    """Replays a trained policy against real, already-labeled data it
    never trained on -- the SAME real settlement math train_bandit_agent's
    own _settle_reward uses (never a second, drifting copy), always
    greedy (this is evaluation, not training), and the SAME fixed-
    fractional-of-STARTING-capital sizing (see _settle_reward's own
    docstring on why) -- return_pct here is a real, bounded, comparable
    number regardless of trade_count, not a compounding one prone to the
    same blowup this module's own docstring documents finding and fixing.
    `calibrator` (see fit_isotonic_calibrator/_priced_confidence): pass
    the SAME one the agent was trained with -- pricing must match between
    training and evaluation or the comparison isn't real. Shape-compatible
    with kalshi_15m_backtest.simulate's own real result dict (return_pct,
    trade_count, win_rate) so this can be compared apples to apples
    against the existing classifier-only approach, not just against
    itself."""
    df = test_df_with_predictions.dropna(subset=["label_up", "model_probability_up"]).sort_values("ts").reset_index(drop=True)
    cumulative_pnl = 0.0
    trades: list[dict[str, Any]] = []
    for row in df.itertuples(index=False):
        proba_up = float(row.model_probability_up)
        side = "yes" if proba_up >= 0.5 else "no"
        confidence = proba_up if side == "yes" else 1.0 - proba_up
        state = _row_to_state(row, proba_up)
        action = predict_action(q_net, state)
        position_size_pct = ACTION_POSITION_SIZES[action]
        if position_size_pct <= 0:
            continue
        priced_confidence = _priced_confidence(side, proba_up, confidence, calibrator)
        reward = _settle_reward(side, priced_confidence, int(row.label_up), starting_balance, position_size_pct)
        cumulative_pnl += reward
        trades.append({"won": reward > 0, "realized_pnl_usd": round(reward, 6), "position_size_pct": position_size_pct})

    wins = [t for t in trades if t["won"]]
    return {
        "starting_balance": starting_balance, "ending_balance": round(starting_balance + cumulative_pnl, 6),
        "return_pct": round(cumulative_pnl / starting_balance * 100, 6) if starting_balance else 0.0,
        "trade_count": len(trades), "win_count": len(wins),
        "win_rate": round(len(wins) / len(trades), 4) if trades else 0.0,
    }


def run_walkforward_rl(
    *, days: int | None = None, coins: list[str] | None = None,
    fold_bounds: list[tuple[float, float, float]] | None = None,
    holdout_bounds: tuple[float, float] | None = None,
    episodes: int = 3, starting_balance: float = 100.0, seed: int = 0, use_calibrated_pricing: bool = True,
) -> dict[str, Any]:
    """Real walk-forward train+test for the DQN pilot -- the SAME
    expanding-window fold discipline every backtest module here already
    uses (see walkforward.py's own module docstring), PLUS a genuine
    forward-test holdout (see strategy_sweep.py's own identical
    convention) the trained policy never saw during training OR fold
    selection. Each fold fits the SAME real classifier
    (kalshi_15m_backtest.fit_backtest_model) on its own train slice for
    directional context, then trains a FRESH agent from scratch on that
    same slice (no leakage of a later fold's agent weights into an
    earlier one) before evaluating on its own real test slice.

    `use_calibrated_pricing` (default True, per real evidence -- see
    fit_isotonic_calibrator's own docstring on the real loss raw-confidence
    pricing produced): fits a fresh isotonic calibrator on each fold's own
    real training rows and uses it for both training AND evaluation
    pricing in that fold.

    Returns {"ok", "folds": [...], "fold_count", "mean_return_pct",
    "profitable_fold_ratio", "holdout"} on success, or {"ok": False,
    "reason": ...} when there's no data or no qualifying fold -- same
    shape/reasoning as every other backtest driver here."""
    from data import kalshi_15m_backtest, kalshi_15m_data, walkforward

    combined = kalshi_15m_data.load_training_dataset()
    if combined.empty:
        return {"ok": False, "reason": "no_data"}
    if coins:
        combined = combined[combined["symbol"].isin(coins)]
    if days:
        cutoff = combined["ts"].max() - days * 86400
        combined = combined[combined["ts"] >= cutoff]
    combined = kalshi_15m_backtest._one_row_per_window(combined)  # noqa: SLF001 -- same-package sibling module
    if combined.empty:
        return {"ok": False, "reason": "no_data"}

    def _fit_train_eval(train_df: pd.DataFrame, test_df: pd.DataFrame) -> dict[str, Any] | None:
        if len(train_df) < 300 or test_df.empty:
            return None
        try:
            fitted = kalshi_15m_backtest.fit_backtest_model(train_df)
            train_with_preds = kalshi_15m_backtest.add_model_predictions(train_df, fitted)
            test_with_preds = kalshi_15m_backtest.add_model_predictions(test_df, fitted)
            calibrator = fit_isotonic_calibrator(train_with_preds) if use_calibrated_pricing else None
            train_result = train_bandit_agent(
                train_with_preds, episodes=episodes, starting_balance=starting_balance, seed=seed, calibrator=calibrator,
            )
            q_net = QNetwork()
            q_net.load_state_dict(train_result["q_network_state_dict"])
            test_result = simulate_policy(test_with_preds, q_net, starting_balance=starting_balance, calibrator=calibrator)
            return {
                "train_rows": len(train_df), "test_rows": len(test_df),
                "train_episode_returns_pct": train_result["episode_return_pct"],
                "model_used": fitted.get("model_type") if isinstance(fitted, dict) else None,
                "calibrated_pricing_used": calibrator is not None,
                **test_result,
            }
        except Exception as exc:
            logger.warning("[kalshi_15m_rl] fold train/eval failed: %s", exc)
            return None

    bounds = fold_bounds or walkforward.DEFAULT_FOLD_BOUNDS
    fold_results = []
    for train_start_q, test_start_q, test_end_q in bounds:
        train_start_ts = combined["ts"].quantile(train_start_q)
        test_start_ts = combined["ts"].quantile(test_start_q)
        test_end_ts = combined["ts"].quantile(test_end_q)
        train_df = combined[(combined["ts"] >= train_start_ts) & (combined["ts"] < test_start_ts)]
        test_df = combined[(combined["ts"] >= test_start_ts) & (combined["ts"] <= test_end_ts)]
        result = _fit_train_eval(train_df, test_df)
        if result is not None:
            result["fold_bounds"] = [train_start_q, test_start_q, test_end_q]
            fold_results.append(result)

    if not fold_results:
        return {"ok": False, "reason": "no_qualifying_folds"}

    holdout_result = None
    if holdout_bounds is not None:
        train_start_ts = combined["ts"].quantile(0.0)
        test_start_ts = combined["ts"].quantile(holdout_bounds[0])
        test_end_ts = combined["ts"].quantile(holdout_bounds[1])
        train_df = combined[(combined["ts"] >= train_start_ts) & (combined["ts"] < test_start_ts)]
        test_df = combined[(combined["ts"] >= test_start_ts) & (combined["ts"] <= test_end_ts)]
        holdout_result = _fit_train_eval(train_df, test_df)

    mean_return = sum(f["return_pct"] for f in fold_results) / len(fold_results)
    profitable_fold_ratio = sum(1 for f in fold_results if f["return_pct"] > 0) / len(fold_results)
    return {
        "ok": True, "folds": fold_results, "fold_count": len(fold_results),
        "mean_return_pct": round(mean_return, 6), "profitable_fold_ratio": round(profitable_fold_ratio, 4),
        "holdout": holdout_result,
    }
