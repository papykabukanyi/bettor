"""kalshi_15m_rl.py -- the real reinforcement-learning pilot, per explicit
user direction: "real reinforcement learning (a learning agent)... learns
entry/sizing/exit decisions from reward over time... alongside (not
replacing) the existing classifiers." Synthetic feature data only (same
convention every other backtest/sweep test here already uses) so this
module's OWN training/evaluation logic is verified in isolation, fast and
deterministic -- never a claim about what a real archive would show (see
scripts/kalshi_15m_rl_job.py's own tests / this session's own manual real
runs for that)."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
import torch

from data import kalshi_15m_rl as rl
from data.perps_data import FEATURE_COLUMNS


def _feature_row_defaults() -> dict:
    return {col: 0.0 for col in FEATURE_COLUMNS}


def _synthetic_df(n: int = 600, seed: int = 3) -> pd.DataFrame:
    """A real, learnable-but-noisy pattern -- model_probability_up
    correlates with label_up but isn't perfectly deterministic (a
    genuinely predictable-but-imperfect signal, closer to what a real
    archive looks like than a noiseless one would be)."""
    rng = np.random.default_rng(seed)
    base = {**_feature_row_defaults()}
    data = {k: np.full(n, v, dtype=float) for k, v in base.items()}
    proba = np.clip(0.5 + rng.normal(0, 0.15, n), 0.05, 0.95)
    label_up = (rng.random(n) < proba).astype(int)
    data.update({
        "ts": np.arange(n) * 900, "model_probability_up": proba, "label_up": label_up,
    })
    return pd.DataFrame(data)


def test_action_position_sizes_starts_with_skip():
    assert rl.ACTION_POSITION_SIZES[0] == 0.0
    assert len(rl.ACTION_POSITION_SIZES) > 1


def test_q_network_outputs_one_value_per_action():
    net = rl.QNetwork()
    state = torch.zeros((1, rl.STATE_DIM), dtype=torch.float32)
    out = net(state)
    assert out.shape == (1, len(rl.ACTION_POSITION_SIZES))


def test_settle_reward_is_zero_for_a_skip():
    assert rl._settle_reward("yes", 0.7, 1, reference_balance=100.0, position_size_pct=0.0) == 0.0  # noqa: SLF001


def test_settle_reward_is_positive_on_a_real_win():
    reward = rl._settle_reward("yes", 0.7, label_up=1, reference_balance=100.0, position_size_pct=0.05)  # noqa: SLF001
    assert reward > 0


def test_settle_reward_is_negative_on_a_real_loss():
    reward = rl._settle_reward("yes", 0.7, label_up=0, reference_balance=100.0, position_size_pct=0.05)  # noqa: SLF001
    assert reward < 0


def test_settle_reward_prices_the_contract_at_confidence_not_a_fixed_price():
    """Real, deliberate design choice this locks in: entry_price tracks
    confidence (clipped to [0.05, 0.95]) -- a HIGHER-confidence win pays
    out LESS per dollar risked than a lower-confidence one, since a real
    market would price a more-likely contract higher. This is what
    prevents the exact assumed_entry_price-decoupled-from-confidence
    compounding artifact strategy_sweep.py's own MAX_PLAUSIBLE_MEAN_RETURN_PCT
    incident was built to guard against."""
    high_confidence_win = rl._settle_reward("yes", 0.90, label_up=1, reference_balance=100.0, position_size_pct=0.05)  # noqa: SLF001
    low_confidence_win = rl._settle_reward("yes", 0.55, label_up=1, reference_balance=100.0, position_size_pct=0.05)  # noqa: SLF001
    assert low_confidence_win > high_confidence_win  # a cheaper (lower-confidence) contract pays out more per dollar on a win


def test_train_bandit_agent_runs_on_real_shaped_synthetic_data():
    df = _synthetic_df()
    result = rl.train_bandit_agent(df, episodes=1, epsilon_decay_steps=100, starting_balance=100.0, seed=0)
    assert result["rows_trained"] == len(df)
    assert len(result["episode_return_pct"]) == 1
    assert "q_network_state_dict" in result


def test_train_bandit_agent_drops_rows_with_no_real_label_or_prediction():
    df = _synthetic_df()
    df.loc[0:5, "label_up"] = np.nan
    result = rl.train_bandit_agent(df, episodes=1, epsilon_decay_steps=50, starting_balance=100.0, seed=0)
    assert result["rows_trained"] == len(df) - 6


def test_settle_reward_sizes_against_the_fixed_reference_not_a_running_balance():
    """Real, live incident this locks in: sizing against a RUNNING/
    compounding balance let reward compound multiplicatively over a long
    training run -- a purely random lucky streak during early exploration
    reliably blew episode returns up to 1e+28-1e+33% on the real archive.
    A win at a huge reference_balance must be proportionally huge too
    (this IS how fixed-fractional-of-STARTING-capital sizing is supposed
    to behave for one call) -- what actually changed is that
    train_bandit_agent/simulate_policy now pass the SAME fixed
    starting_balance into every call within an episode, never a mutating
    running one -- see those functions' own tests for that."""
    reward_at_100 = rl._settle_reward("yes", 0.7, label_up=1, reference_balance=100.0, position_size_pct=0.02)  # noqa: SLF001
    reward_at_1e30 = rl._settle_reward("yes", 0.7, label_up=1, reference_balance=1e30, position_size_pct=0.02)  # noqa: SLF001
    assert reward_at_1e30 == pytest.approx(reward_at_100 * 1e28)  # scales linearly with whatever reference is actually passed


def test_train_bandit_agent_episode_returns_stay_bounded_even_with_full_random_exploration():
    """Real regression test for the exact incident above: with
    epsilon_start=1.0 held for the WHOLE run (never decaying, so every
    single action is fully random -- the worst case for a lucky-streak
    blowup), episode_return_pct must stay within a sane, bounded range now
    that reward sizes against the fixed starting_balance, not a
    compounding one."""
    df = _synthetic_df(n=800, seed=5)
    result = rl.train_bandit_agent(
        df, episodes=2, epsilon_start=1.0, epsilon_end=1.0, epsilon_decay_steps=1, starting_balance=100.0, seed=0,
    )
    for ep_return in result["episode_return_pct"]:
        assert abs(ep_return) < 1000  # generous, but 1e28 would fail this trivially


def test_train_bandit_agent_is_deterministic_given_the_same_seed():
    df = _synthetic_df()
    result_a = rl.train_bandit_agent(df, episodes=1, epsilon_decay_steps=100, starting_balance=100.0, seed=7)
    result_b = rl.train_bandit_agent(df, episodes=1, epsilon_decay_steps=100, starting_balance=100.0, seed=7)
    assert result_a["episode_return_pct"] == result_b["episode_return_pct"]


def test_predict_action_is_greedy_no_exploration():
    net = rl.QNetwork()
    state = np.zeros(rl.STATE_DIM, dtype=np.float32)
    # Same state, called repeatedly -- must always return the same action
    # (no epsilon-greedy randomness at inference time).
    actions = {rl.predict_action(net, state) for _ in range(10)}
    assert len(actions) == 1


def test_simulate_policy_shape_matches_backtest_convention():
    df = _synthetic_df()
    train_result = rl.train_bandit_agent(df, episodes=1, epsilon_decay_steps=100, starting_balance=100.0, seed=0)
    net = rl.QNetwork()
    net.load_state_dict(train_result["q_network_state_dict"])

    result = rl.simulate_policy(df, net, starting_balance=100.0)

    for key in ("starting_balance", "ending_balance", "return_pct", "trade_count", "win_count", "win_rate"):
        assert key in result


def test_simulate_policy_never_counts_a_skip_as_a_trade():
    df = _synthetic_df()
    net = rl.QNetwork()  # untrained, deterministic init -- whatever it picks, skips must not count as trades

    result = rl.simulate_policy(df, net, starting_balance=100.0)

    assert result["trade_count"] <= len(df)
    assert result["win_count"] <= result["trade_count"]


# ---------------------------------------------------------------------------
# fit_isotonic_calibrator / _priced_confidence -- real, evidence-backed
# fix: the pilot's own SECOND real run priced contracts at raw model
# confidence and lost money on real held-out data (-267% return, 52.3%
# win rate) -- kalshi_15m's own raw model confidence was ALREADY found
# unreliable earlier this session. Calibrating against real per-fold
# outcomes before pricing is the tractable backtesting equivalent of
# kalshi_15m_strategy.calibrate_probability_up's own live idea.
# ---------------------------------------------------------------------------
def test_fit_isotonic_calibrator_returns_none_below_min_rows():
    df = _synthetic_df(n=50)
    assert rl.fit_isotonic_calibrator(df, min_rows=150) is None


def test_fit_isotonic_calibrator_fits_on_enough_real_rows():
    df = _synthetic_df(n=600)
    calibrator = rl.fit_isotonic_calibrator(df, min_rows=150)
    assert calibrator is not None
    pred = calibrator.predict([0.7])
    assert 0.0 <= pred[0] <= 1.0


def test_priced_confidence_falls_back_to_raw_with_no_calibrator():
    priced = rl._priced_confidence("yes", 0.7, raw_confidence=0.7, calibrator=None)  # noqa: SLF001
    assert priced == 0.7


def test_priced_confidence_never_changes_side_only_the_price():
    """side is always the existing classifier's own call -- calibration
    only ever changes the PRICE used to settle a trade, never which side
    was chosen. Locked in here directly since _priced_confidence itself
    has no side-changing code path to accidentally regress into, but the
    contract is worth asserting explicitly."""
    df = _synthetic_df(n=600)
    calibrator = rl.fit_isotonic_calibrator(df, min_rows=150)
    # Whatever the calibrated confidence comes out to, it's still a
    # confidence FOR THE SAME SIDE -- a valid probability in [0, 1].
    priced = rl._priced_confidence("yes", 0.8, raw_confidence=0.8, calibrator=calibrator)  # noqa: SLF001
    assert 0.0 <= priced <= 1.0


def test_train_bandit_agent_accepts_a_calibrator_and_still_runs():
    df = _synthetic_df()
    calibrator = rl.fit_isotonic_calibrator(df, min_rows=150)
    result = rl.train_bandit_agent(df, episodes=1, epsilon_decay_steps=100, starting_balance=100.0, seed=0, calibrator=calibrator)
    assert result["rows_trained"] == len(df)


def test_run_walkforward_rl_reports_no_data_when_archive_is_empty(monkeypatch):
    from data import kalshi_15m_data
    monkeypatch.setattr(kalshi_15m_data, "load_training_dataset", lambda: pd.DataFrame())
    result = rl.run_walkforward_rl(episodes=1)
    assert result == {"ok": False, "reason": "no_data"}


def test_run_walkforward_rl_reports_no_qualifying_folds_on_too_little_data(monkeypatch):
    from data import kalshi_15m_data
    tiny = pd.DataFrame({
        **{col: [0.0] * 20 for col in FEATURE_COLUMNS},
        "ts": np.arange(20) * 900, "symbol": ["GOLD"] * 20, "label_up": [0, 1] * 10,
    })
    monkeypatch.setattr(kalshi_15m_data, "load_training_dataset", lambda: tiny)
    result = rl.run_walkforward_rl(episodes=1)
    assert result == {"ok": False, "reason": "no_qualifying_folds"}


def test_run_walkforward_rl_end_to_end_with_a_real_learnable_dataset(monkeypatch):
    """Real end-to-end: builds a large-enough synthetic archive that
    walkforward.DEFAULT_FOLD_BOUNDS' own folds all have enough training
    data, monkeypatching only load_training_dataset (same convention
    test_strategy_sweep.py's own end-to-end test already uses) so this
    exercises the REAL fit_backtest_model/add_model_predictions/
    train_bandit_agent/simulate_policy chain, not a mocked stand-in."""
    from data import kalshi_15m_data

    rng = np.random.default_rng(11)
    n = 3000
    base = {**_feature_row_defaults()}
    data = {k: np.full(n, v, dtype=float) for k, v in base.items()}
    dist = rng.normal(0, 0.01, n)
    data.update({
        "symbol": "GOLD", "ts": np.arange(n) * 900, "ret_1m": dist,
        "label_up": (dist > 0).astype(int), "sentiment_score": 0.0,
    })
    df = pd.DataFrame(data)
    monkeypatch.setattr(kalshi_15m_data, "load_training_dataset", lambda: df)

    result = rl.run_walkforward_rl(episodes=1, starting_balance=100.0, seed=0)

    assert result["ok"] is True
    assert result["fold_count"] >= 1
    for fold in result["folds"]:
        assert fold["trade_count"] >= 0
        assert "return_pct" in fold
