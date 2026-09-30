"""Coinbase spot leads the Kalshi perp: a short-horizon predictor of the
perp's own next move from real spot data (kalshi_15m_spot).

Measured on 72 days of real minute data across 9 coins (perps archive vs
Coinbase 1-minute candles, bad prints removed, walk-forward by day): when
Coinbase has moved and the perp hasn't caught up yet, the perp follows
(rank IC +0.07 at 1 minute, +0.04 at 5); when the perp is rich vs spot it
falls back (-0.10 / -0.05). Strongest on thin perps (HYPE, NEAR, BCH, DOGE),
weakest on BTC/ETH. Out of sample, predictions of 5+ bps got the 5-minute
direction right 63% of the time, 10+ bps 70%.

Those moves are 8-12 bps, below Kalshi's perp costs (maker 5 bps/leg plus a
4-10 bps spread on the coins where the signal is strongest; taker 80
bps/leg), so this is not a standalone strategy. perps_strategy uses it for
entry timing: skip an entry the spot lead says is about to move against it.

Bad prints: a minute where the perp sits more than BAD_PRINT_BASIS_BPS from
spot, or moved more than BAD_PRINT_MOVE_BPS in one minute, is dropped in
training and refused live -- NEAR's archive has single-minute spikes up to
7% that otherwise manufacture fake predictability.
"""
from __future__ import annotations

import json
import logging
import math
import os
import re
import time
from typing import Any

import numpy as np
import pandas as pd

from server_common import DATA_DIR

logger = logging.getLogger(__name__)

HF_API_KEY = os.getenv("HF_API_KEY", "")
HF_PERPS_DATASET_REPO = os.getenv("HF_DATASET_REPO", "papylove/kalshi-perps-data")
HF_ARTIFACT_PATH = "spot_lead/latest.json"
LOCAL_ARTIFACT_PATH = DATA_DIR / "perps_spot_lead.json"

HORIZON_MINUTES = 5
FEATURES = ["lag_gap", "basis_dev", "spot_r1", "perp_r1", "spot_r5", "perp_r5"]
FEATURE_CLIP_BPS = 500.0
BAD_PRINT_BASIS_BPS = 100.0
BAD_PRINT_MOVE_BPS = 200.0
BASIS_MEDIAN_WINDOW = 60
TRAIN_WINDOW_DAYS = 30
MIN_TRAIN_DAYS = 10
RIDGE_ALPHA = 10.0
STRONG_SIGNAL_BPS = 5.0
MIN_STRONG_HIT_RATE = 0.55
MIN_STRONG_DECISIONS = 300
HISTORY_DAYS = int(os.getenv("PERPS_SPOT_LEAD_HISTORY_DAYS", "75") or "75")
LIVE_MAX_AGE_SEC = 180
_SHARD_RE = re.compile(r"^data/(\d{4}-\d{2}-\d{2})\.parquet$")
_REMOTE_RETRY_SEC = 900.0
_cache: dict[str, Any] = {"artifact": None, "last_remote_attempt": 0.0}


def coin_for_perp(ticker: str) -> str | None:
    from data import kalshi_15m_spot, perps_data
    coin = perps_data.coin_for_ticker(ticker)
    return coin if coin in kalshi_15m_spot.COINBASE_PRODUCTS else None


def build_features(perp: pd.DataFrame, spot: pd.DataFrame, *, with_target: bool = True) -> pd.DataFrame:
    """One coin on a minute grid (candle-END timestamps on both sides). perp:
    ts, close[, bid_close, ask_close]; spot: ts, close. Moves are in bps of
    log price; the perp's scale is matched to spot by its power of ten."""
    if perp.empty or spot.empty:
        return pd.DataFrame()
    p = perp.drop_duplicates("ts").set_index("ts").sort_index()
    s = spot.drop_duplicates("ts").set_index("ts").sort_index()
    start, end = int(max(p.index.min(), s.index.min())), int(min(p.index.max(), s.index.max()))
    if end <= start:
        return pd.DataFrame()
    grid = pd.RangeIndex(start, end + 60, 60)
    g = pd.DataFrame(index=grid)
    mid = p["close"].astype(float)
    if "bid_close" in p.columns and "ask_close" in p.columns:
        mid = ((p["bid_close"].astype(float) + p["ask_close"].astype(float)) / 2.0).fillna(mid)
        g["spread_bps"] = ((p["ask_close"] - p["bid_close"]) / mid * 1e4).reindex(grid)
    g["pmid"] = mid.reindex(grid)
    g["spot"] = s["close"].astype(float).reindex(grid).ffill(limit=5)
    ratio = (g["spot"] / g["pmid"]).median()
    if not ratio or ratio != ratio:
        return pd.DataFrame()
    scale = 10.0 ** round(math.log10(ratio))
    lp, ls = np.log(g["pmid"] * scale), np.log(g["spot"])
    g["basis_bps"] = (lp - ls) * 1e4
    g["basis_dev"] = g["basis_bps"] - g["basis_bps"].rolling(BASIS_MEDIAN_WINDOW, min_periods=20).median()
    g["spot_r1"], g["spot_r5"] = ls.diff(1) * 1e4, ls.diff(5) * 1e4
    g["perp_r1"], g["perp_r5"] = lp.diff(1) * 1e4, lp.diff(5) * 1e4
    g["lag_gap"] = g["spot_r5"] - g["perp_r5"]
    g["bad_print"] = (g["basis_bps"].abs() > BAD_PRINT_BASIS_BPS) | (g["perp_r1"].abs() > BAD_PRINT_MOVE_BPS)
    if with_target:
        g["fwd"] = (lp.shift(-HORIZON_MINUTES) - lp) * 1e4
    return g.rename_axis("ts").reset_index()


def _clip(x: np.ndarray) -> np.ndarray:
    return np.clip(x, -FEATURE_CLIP_BPS, FEATURE_CLIP_BPS)


def load_perp_history(*, days: int = HISTORY_DAYS) -> pd.DataFrame:
    """ticker, ts, close, bid_close, ask_close from the perps archive (older
    shards predate bid/ask; those rows fall back to close)."""
    if not HF_API_KEY:
        return pd.DataFrame()
    from concurrent.futures import ThreadPoolExecutor

    from server_common import call_with_hard_timeout

    def _list() -> list[str]:
        from huggingface_hub import HfApi
        return HfApi(token=HF_API_KEY).list_repo_files(repo_id=HF_PERPS_DATASET_REPO, repo_type="dataset")

    files = sorted(f for f in (call_with_hard_timeout(_list, timeout_sec=45, on_timeout=[]) or []) if _SHARD_RE.match(f))[-days:]

    def _one(f: str) -> pd.DataFrame | None:
        from huggingface_hub import hf_hub_download
        try:
            d = pd.read_parquet(hf_hub_download(repo_id=HF_PERPS_DATASET_REPO, filename=f, repo_type="dataset", token=HF_API_KEY))
        except Exception as exc:
            logger.warning("[perps_spot_lead] shard %s unreadable: %s", f, exc)
            return None
        for col in ("bid_close", "ask_close"):
            if col not in d.columns:
                d[col] = np.nan
        return d[["ticker", "ts", "close", "bid_close", "ask_close"]]

    with ThreadPoolExecutor(max_workers=8) as pool:
        frames = [f for f in pool.map(_one, files) if f is not None]
    return pd.concat(frames, ignore_index=True).drop_duplicates(["ticker", "ts"]) if frames else pd.DataFrame()


def build_training_frame(perp_history: pd.DataFrame, spot_history: pd.DataFrame) -> pd.DataFrame:
    from data import perps_data
    rows = []
    for ticker, p in perp_history.groupby("ticker"):
        coin = perps_data.coin_for_ticker(str(ticker))
        s = spot_history[spot_history["coin"] == coin]
        if s.empty:
            continue
        f = build_features(p, s[["ts", "close"]])
        if f.empty:
            continue
        f = f[~f["bad_print"]].dropna(subset=FEATURES + ["fwd"])
        f = f[f["fwd"].abs() < 300]
        f["coin"] = coin
        rows.append(f)
    if not rows:
        return pd.DataFrame()
    df = pd.concat(rows, ignore_index=True)
    df["day"] = pd.to_datetime(df["ts"], unit="s", utc=True).dt.strftime("%Y-%m-%d")
    return df


def _ridge(X: np.ndarray, y: np.ndarray) -> tuple[np.ndarray, float]:
    x_mean, y_mean = X.mean(axis=0), y.mean()
    Xc = X - x_mean
    coef = np.linalg.solve(Xc.T @ Xc + RIDGE_ALPHA * np.eye(X.shape[1]), Xc.T @ (y - y_mean))
    return coef, float(y_mean - x_mean @ coef)


def _hit_stats(frame: pd.DataFrame, threshold: float) -> dict[str, Any]:
    t = frame[(frame["pred"].abs() >= threshold) & (frame["fwd"] != 0)]
    if t.empty:
        return {"decisions": 0}
    gross = np.sign(t["pred"]) * t["fwd"]
    return {"decisions": int(len(t)), "hit_rate": round(float((gross > 0).mean()), 4), "gross_bps": round(float(gross.mean()), 2)}


def train_and_evaluate(perp_history: pd.DataFrame | None = None, spot_history: pd.DataFrame | None = None) -> dict[str, Any]:
    from data import kalshi_15m_spot

    if perp_history is None:
        perp_history = load_perp_history()
    if spot_history is None:
        spot_history = kalshi_15m_spot.load_spot_history(days=HISTORY_DAYS)
    df = build_training_frame(perp_history, spot_history)
    if df.empty:
        return {"ok": False, "certified": False, "reason": "no_aligned_history"}
    days = sorted(df["day"].unique())
    parts = []
    for i, day in enumerate(days):
        if i < MIN_TRAIN_DAYS:
            continue
        train = df[df["day"].isin(days[max(0, i - TRAIN_WINDOW_DAYS):i])]
        test = df[df["day"] == day].copy()
        coef, intercept = _ridge(_clip(train[FEATURES].to_numpy(float)), _clip(train["fwd"].to_numpy(float)))
        test["pred"] = _clip(test[FEATURES].to_numpy(float)) @ coef + intercept
        parts.append(test[(test["ts"] // 60) % HORIZON_MINUTES == 0])
    if not parts:
        return {"ok": False, "certified": False, "reason": "not_enough_history", "days": len(days)}
    oos = pd.concat(parts, ignore_index=True)
    strong = _hit_stats(oos, STRONG_SIGNAL_BPS)
    certified = strong.get("decisions", 0) >= MIN_STRONG_DECISIONS and strong.get("hit_rate", 0.0) >= MIN_STRONG_HIT_RATE
    recent = df[df["day"].isin(days[-TRAIN_WINDOW_DAYS:])]
    coef, intercept = _ridge(_clip(recent[FEATURES].to_numpy(float)), _clip(recent["fwd"].to_numpy(float)))
    artifact = {
        "built_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "horizon_minutes": HORIZON_MINUTES, "features": FEATURES, "coef": [float(c) for c in coef], "intercept": intercept,
        "days": len(days), "aligned_minutes": int(len(df)), "coins": sorted(df["coin"].unique()),
        "certified": bool(certified),
        "oos": {
            "decisions": int(len(oos)), "days": int(oos["day"].nunique()),
            "rank_ic": round(float(oos["pred"].rank().corr(oos["fwd"].rank())), 4),
            "direction_hit_rate": round(float((np.sign(oos["pred"]) == np.sign(oos["fwd"]))[oos["fwd"] != 0].mean()), 4),
            f"strong_{int(STRONG_SIGNAL_BPS)}bps": strong, "strong_10bps": _hit_stats(oos, 10.0),
            "by_coin": {c: _hit_stats(g, STRONG_SIGNAL_BPS) for c, g in oos.groupby("coin")},
        },
    }
    save_artifact(artifact)
    return {"ok": True, **artifact}


def save_artifact(artifact: dict[str, Any]) -> None:
    LOCAL_ARTIFACT_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = LOCAL_ARTIFACT_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(artifact), encoding="utf-8")
    tmp.replace(LOCAL_ARTIFACT_PATH)
    _cache["artifact"] = artifact
    if not HF_API_KEY:
        return
    try:
        from huggingface_hub import HfApi
        HfApi(token=HF_API_KEY).upload_file(
            path_or_fileobj=str(LOCAL_ARTIFACT_PATH), path_in_repo=HF_ARTIFACT_PATH, repo_id=HF_PERPS_DATASET_REPO,
            repo_type="dataset", commit_message=f"perps spot-lead model {artifact.get('built_at_utc')}",
        )
    except Exception as exc:
        logger.warning("[perps_spot_lead] artifact upload failed: %s", exc)


def load_artifact() -> dict[str, Any] | None:
    if _cache["artifact"] is not None:
        return _cache["artifact"]
    if LOCAL_ARTIFACT_PATH.exists():
        try:
            _cache["artifact"] = json.loads(LOCAL_ARTIFACT_PATH.read_text(encoding="utf-8"))
            return _cache["artifact"]
        except Exception as exc:
            logger.warning("[perps_spot_lead] local artifact unreadable: %s", exc)
    now = time.time()
    if not HF_API_KEY or now - _cache["last_remote_attempt"] < _REMOTE_RETRY_SEC:
        return None
    _cache["last_remote_attempt"] = now

    def _download() -> dict[str, Any] | None:
        from huggingface_hub import hf_hub_download
        path = hf_hub_download(repo_id=HF_PERPS_DATASET_REPO, filename=HF_ARTIFACT_PATH, repo_type="dataset", token=HF_API_KEY)
        with open(path, encoding="utf-8") as f:
            return json.load(f)

    try:
        from server_common import call_with_hard_timeout
        artifact = call_with_hard_timeout(_download, timeout_sec=20)
    except Exception as exc:
        logger.info("[perps_spot_lead] no published artifact yet: %s", exc)
        return None
    if artifact:
        _cache["artifact"] = artifact
    return artifact


def predict_from_rows(perp: pd.DataFrame, spot: pd.DataFrame, artifact: dict[str, Any], *, now: float | None = None) -> dict[str, Any]:
    """Pure: the perp's predicted next-HORIZON_MINUTES move in bps from the
    latest minute both series share."""
    f = build_features(perp, spot, with_target=False).dropna(subset=FEATURES)
    if f.empty:
        return {"ok": False, "reason": "not_enough_aligned_minutes"}
    last = f.iloc[-1]
    now = time.time() if now is None else now
    if now - float(last["ts"]) > LIVE_MAX_AGE_SEC:
        return {"ok": False, "reason": "stale_data", "ts": int(last["ts"])}
    if bool(last["bad_print"]):
        return {"ok": False, "reason": "bad_print", "basis_bps": round(float(last["basis_bps"]), 2)}
    x = _clip(last[artifact["features"]].to_numpy(float))
    pred = float(x @ np.asarray(artifact["coef"]) + artifact["intercept"])
    return {
        "ok": True, "pred_bps": round(pred, 2), "horizon_minutes": artifact.get("horizon_minutes", HORIZON_MINUTES),
        "certified": bool(artifact.get("certified")), "ts": int(last["ts"]),
        "features": {k: round(float(last[k]), 3) for k in FEATURES},
    }


def live_prediction(ticker: str) -> dict[str, Any]:
    from data import kalshi_15m_spot, perps_data

    coin = coin_for_perp(ticker)
    if coin is None:
        return {"ok": False, "reason": "no_coinbase_market"}
    artifact = load_artifact()
    if not artifact:
        return {"ok": False, "reason": "no_spot_lead_model_yet"}
    one_min, _ = perps_data.fetch_candle_frames(ticker)
    perp = one_min.tail(BASIS_MEDIAN_WINDOW * 2)
    if perp.empty:
        return {"ok": False, "reason": "no_perp_candles"}
    spot = kalshi_15m_spot.recent_series(coin)
    spot = spot[spot["ts"] >= int(perp["ts"].min())][["ts", "close"]]
    return {**predict_from_rows(perp, spot, artifact), "coin": coin}


def summary() -> dict[str, Any]:
    a = load_artifact()
    if not a:
        return {"available": False}
    return {"available": True, **{k: a.get(k) for k in ("built_at_utc", "horizon_minutes", "days", "aligned_minutes", "coins", "certified", "oos")}}
