"""Alpaca OPTIONS data -- a new, separate pipeline from alpaca_data.py
(equities) and alpaca_crypto_data.py (crypto), with its own HF dataset
repo as explicitly requested, but built the same recipe as every other
strategy here: real technical indicators + real news sentiment feeding a
trained direction classifier.

The signal itself is about the UNDERLYING stock, not the option contract:
predicting whether AAPL goes up or down over the next minute is exactly
the same problem alpaca_data.py already solves for the equities strategy,
so this module reuses alpaca_data.py's engineer_features()/FEATURE_COLUMNS/
fetch_recent_minute_bars() directly for that piece (identical formulas,
identical discipline) rather than duplicating it -- what's genuinely new
here is the OPTIONS-specific layer on top: choosing a tradable contract
(near-the-money, near-term expiration, right side for the predicted
direction) once the underlying's direction is known, and archiving that
combined (underlying features + chosen contract) dataset to its OWN HF
repo (HF_ALPACA_OPTIONS_DATASET_REPO) for its OWN model to train on.

Universe: a small, fixed list of large, highly liquid, options-friendly
underlyings (same fallback list already proven for the equities
strategy's own watchlist) -- unlike thousands of equities, not every
stock has a liquid options chain, and building a second full watchlist-
ranking pipeline just to pick options underlyings would repeat real
memory cost for no real benefit here.
"""
from __future__ import annotations

import datetime as dt
import gc
import logging
import os
import re
from typing import Any

import pandas as pd

from data import alpaca_client
from data.alpaca_data import FEATURE_COLUMNS, engineer_features, fetch_recent_minute_bars
from data.stock_news import get_sentiment

logger = logging.getLogger(__name__)

HF_API_KEY = os.getenv("HF_API_KEY", "")
HF_ALPACA_OPTIONS_DATASET_REPO = os.getenv("HF_ALPACA_OPTIONS_DATASET_REPO", "papylove/alpaca-options-data")

# Large, highly liquid, options-friendly underlyings -- deep chains, tight
# bid/ask spreads, always has near-term weekly expirations. A fixed list
# (not a ranked-from-thousands watchlist) is a deliberate, lighter-memory
# choice -- see this module's own docstring.
OPTIONS_UNDERLYINGS = [
    s.strip().upper() for s in os.getenv(
        "ALPACA_OPTIONS_UNDERLYINGS", "AAPL,MSFT,NVDA,AMZN,GOOGL,META,TSLA,AVGO,AMD,NFLX",
    ).split(",") if s.strip()
]


def get_options_universe() -> list[str]:
    return list(OPTIONS_UNDERLYINGS)


# ---------------------------------------------------------------------------
# Contract selection -- once the underlying's predicted direction is known,
# pick ONE tradable contract: near-the-money (closest strike to the
# current price) and near-term (a window a few weeks out, so there's
# still real time value/liquidity left, not an expiring-tomorrow
# contract whose price is nearly all intrinsic value and gamma risk).
# ---------------------------------------------------------------------------
MIN_DAYS_TO_EXPIRATION = int(os.getenv("ALPACA_OPTIONS_MIN_DAYS_TO_EXPIRATION", "7") or "7")
MAX_DAYS_TO_EXPIRATION = int(os.getenv("ALPACA_OPTIONS_MAX_DAYS_TO_EXPIRATION", "45") or "45")


# A contract with zero open interest can be numerically "nearest the
# money" and still be nearly unfillable at a real price -- Alpaca's own
# /v2/options/contracts response includes open_interest directly on each
# contract (no extra call needed), so there's no excuse to ignore it.
MIN_OPEN_INTEREST = int(os.getenv("ALPACA_OPTIONS_MIN_OPEN_INTEREST", "10") or "10")


def select_contract(underlying: str, *, direction: str, current_price: float) -> dict[str, Any] | None:
    """Picks the single best contract for a directional bet on `underlying`:
    "call" if direction == "up", "put" if direction == "down". Prefers a
    real, liquid market over a numerically-perfect strike: contracts with
    at least MIN_OPEN_INTEREST are ranked first (nearest strike, then
    nearest expiration, among those), falling back to the full tradable
    set only if NONE clear that bar -- a real fill matters more than a
    theoretically ideal strike nobody is actually trading. None if no
    contracts are available (e.g. this underlying doesn't actually have
    listed options, or none fall in the expiration window)."""
    option_type = "call" if direction == "up" else "put"
    today = dt.datetime.now(dt.timezone.utc).date()
    exp_gte = (today + dt.timedelta(days=MIN_DAYS_TO_EXPIRATION)).isoformat()
    exp_lte = (today + dt.timedelta(days=MAX_DAYS_TO_EXPIRATION)).isoformat()
    try:
        contracts = alpaca_client.get_option_contracts(
            underlying_symbols=[underlying], expiration_date_gte=exp_gte, expiration_date_lte=exp_lte,
            option_type=option_type,
        )
    except Exception as exc:
        logger.warning("[alpaca_options_data] contract lookup failed for %s: %s", underlying, exc)
        return None
    tradable = [c for c in contracts if c.get("tradable")]
    if not tradable:
        return None

    def _sort_key(c: dict[str, Any]) -> tuple[float, str]:
        strike = float(c.get("strike_price") or 0.0)
        return (abs(strike - current_price), c.get("expiration_date") or "")

    liquid = [c for c in tradable if int(c.get("open_interest") or 0) >= MIN_OPEN_INTEREST]
    pool = liquid if liquid else tradable
    pool.sort(key=_sort_key)
    return pool[0]


# ---------------------------------------------------------------------------
# Vertical DEBIT SPREADS -- confirmed via Alpaca's own docs (options-level-3
# -trading page): this account's real options_approved_level is 3, which
# explicitly supports "Buy a call spread"/"Buy a put spread" as a genuine
# order_class="mleg" order (see alpaca_client.build_option_spread_order).
# A debit spread buys the same near-the-money contract select_contract()
# already picks, then SELLS a further-out-of-the-money contract (same
# underlying, same expiration) against it -- the premium collected on that
# short leg partially offsets the long leg's cost, so the spread is
# CHEAPER to enter than the naked long option alone, and both the max gain
# AND max loss are capped at a known amount the instant it's opened
# (defined risk -- a real difference from a naked long call/put, whose
# max loss is the full premium and whose max gain is technically
# unbounded). The tradeoff: capped upside in exchange for a cheaper entry
# and a smaller max loss -- a genuinely different, valid way to express
# the exact same directional read this strategy's model already produces,
# not a different signal.
#
# Width in DOLLARS, not a fixed number of strikes -- strike increments
# vary by underlying/price (e.g. $1 increments near a $150 stock, $5 or
# more near a $600 one), so a fixed strike-count offset would mean a
# wildly different real width across this module's own OPTIONS_UNDERLYINGS
# list.
SPREAD_WIDTH_DOLLARS = float(os.getenv("ALPACA_OPTIONS_SPREAD_WIDTH_DOLLARS", "5.0") or "5.0")


def select_spread_contracts(
    underlying: str, *, direction: str, current_price: float,
) -> tuple[dict[str, Any], dict[str, Any]] | None:
    """Picks BOTH legs of a vertical debit spread for a directional bet:
    "up" buys the near-the-money call and sells a further-OTM call against
    it (short strike ABOVE the long strike); "down" buys the near-the-money
    put and sells a further-OTM put (short strike BELOW the long strike).
    Both legs come from the SAME expiration as select_contract()'s own
    near-the-money pick -- a true vertical spread, not a calendar spread.
    Returns (long_contract, short_contract), or None if either leg can't be
    found (e.g. this underlying's chain has no strike far enough out, or no
    listed options at all)."""
    long_contract = select_contract(underlying, direction=direction, current_price=current_price)
    if long_contract is None:
        return None
    option_type = long_contract["type"]
    expiration_date = long_contract["expiration_date"]
    long_strike = float(long_contract["strike_price"])

    try:
        same_expiry = alpaca_client.get_option_contracts(
            underlying_symbols=[underlying], expiration_date_gte=expiration_date,
            expiration_date_lte=expiration_date, option_type=option_type,
        )
    except Exception as exc:
        logger.warning("[alpaca_options_data] short-leg lookup failed for %s: %s", underlying, exc)
        return None
    tradable = [c for c in same_expiry if c.get("tradable") and c.get("symbol") != long_contract["symbol"]]
    if not tradable:
        return None

    if option_type == "call":
        candidates = [c for c in tradable if float(c.get("strike_price") or 0.0) >= long_strike + SPREAD_WIDTH_DOLLARS]
        candidates.sort(key=lambda c: float(c["strike_price"]))  # nearest strike above the width first
    else:
        candidates = [c for c in tradable if float(c.get("strike_price") or 0.0) <= long_strike - SPREAD_WIDTH_DOLLARS]
        candidates.sort(key=lambda c: -float(c["strike_price"]))  # nearest strike below the width first
    if not candidates:
        return None

    liquid = [c for c in candidates if int(c.get("open_interest") or 0) >= MIN_OPEN_INTEREST]
    pool = liquid if liquid else candidates
    return long_contract, pool[0]


# ---------------------------------------------------------------------------
# Dataset archival -- same shard-per-day-across-all-underlyings convention
# as every other pipeline here, own HF repo.
# ---------------------------------------------------------------------------
def _upload_shard(df: pd.DataFrame, *, path_in_repo: str, commit_message: str) -> dict[str, Any]:
    if not HF_API_KEY:
        return {"ok": False, "reason": "no_hf_api_key"}
    import tempfile
    from huggingface_hub import HfApi

    try:
        api = HfApi(token=HF_API_KEY)
        try:
            api.repo_info(repo_id=HF_ALPACA_OPTIONS_DATASET_REPO, repo_type="dataset")
        except Exception:
            api.create_repo(repo_id=HF_ALPACA_OPTIONS_DATASET_REPO, repo_type="dataset", exist_ok=True, private=False)
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            df.to_parquet(tmp.name, index=False)
            tmp_path = tmp.name
        try:
            api.upload_file(
                path_or_fileobj=tmp_path, path_in_repo=path_in_repo,
                repo_id=HF_ALPACA_OPTIONS_DATASET_REPO, repo_type="dataset", commit_message=commit_message,
            )
        finally:
            os.unlink(tmp_path)
        return {"ok": True, "rows": len(df), "path": path_in_repo}
    except Exception as exc:
        logger.warning("[alpaca_options_data] HF upload failed for %s: %s", path_in_repo, exc)
        return {"ok": False, "error": str(exc)}


_DATE_SHARD_RE = re.compile(r"^minute/\d{4}-\d{2}-\d{2}\.parquet$")


def push_minute_snapshot(df: pd.DataFrame) -> dict[str, Any]:
    """Minute bars (of the UNDERLYING stocks -- the model trains on
    underlying direction, not option-contract price series) sharded by
    calendar day across all underlyings, same merge-not-overwrite
    discipline as every other pipeline here.

    Explicit `del`+gc.collect() mirrors a real, confirmed OOM fix needed on
    this exact function's sibling in alpaca_crypto_data.py (downloading the
    existing shard, concatenating, then uploading held existing+df+combined
    simultaneously with nothing freed) -- applied here proactively before
    this newer, smaller pipeline's own shards grow enough to hit it too."""
    if df.empty:
        return {"ok": False, "reason": "no_rows"}
    if not HF_API_KEY:
        return {"ok": False, "reason": "no_hf_api_key"}

    today = dt.datetime.now(dt.timezone.utc).date().isoformat()
    path_in_repo = f"minute/{today}.parquet"
    combined = df
    try:
        from huggingface_hub import hf_hub_download
        existing_path = hf_hub_download(repo_id=HF_ALPACA_OPTIONS_DATASET_REPO, filename=path_in_repo, repo_type="dataset", token=HF_API_KEY)
        existing = pd.read_parquet(existing_path)
        combined = pd.concat([existing, df], ignore_index=True)
        del existing
    except Exception as exc:
        logger.info("[alpaca_options_data] no existing minute shard for %s yet (or fetch failed), starting fresh: %s", today, exc)

    if "symbol" in combined.columns and "ts" in combined.columns:
        combined = combined.drop_duplicates(subset=["symbol", "ts"]).sort_values(["symbol", "ts"]).reset_index(drop=True)
    del df
    gc.collect()
    try:
        return _upload_shard(combined, path_in_repo=path_in_repo, commit_message=f"append options-underlying minute bars: {today}")
    finally:
        del combined
        gc.collect()


MAX_TRAIN_ROWS = int(os.getenv("ALPACA_OPTIONS_MAX_TRAIN_ROWS", "150000") or "150000")


def collect_dataset_rows(symbols: list[str] | None = None) -> pd.DataFrame:
    """Fetch + engineer features for the given underlyings (default: the
    fixed options-friendly universe)."""
    target_symbols = symbols if symbols is not None else get_options_universe()
    frames = []
    for symbol in target_symbols:
        try:
            one_min_df = fetch_recent_minute_bars(symbol)
            sentiment = get_sentiment(symbol, use_limited_sources=True)
            feats = engineer_features(one_min_df, sentiment_score=sentiment["sentiment_score"])
            if feats.empty:
                continue
            feats.insert(0, "symbol", symbol)
            frames.append(feats)
        except Exception as exc:
            logger.warning("[alpaca_options_data] collect failed for %s: %s", symbol, exc)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def latest_feature_row(symbol: str) -> dict[str, Any] | None:
    """The single most-recent feature row for one underlying, for live
    prediction. Its label is always NaN (the future outcome hasn't
    happened yet) -- expected, we only need the feature columns here."""
    try:
        one_min_df = fetch_recent_minute_bars(symbol)
        sentiment = get_sentiment(symbol, use_limited_sources=True)
        feats = engineer_features(one_min_df, sentiment_score=sentiment["sentiment_score"])
        if feats.empty:
            return None
        last = feats.iloc[-1]
        row = {col: float(last[col]) for col in FEATURE_COLUMNS}
        row["symbol"] = symbol
        row["current_price"] = float(one_min_df["close"].iloc[-1])
        row["short_ma"] = float(last["ma_15"])
        return row
    except Exception as exc:
        logger.warning("[alpaca_options_data] latest_feature_row failed for %s: %s", symbol, exc)
        return None


def load_training_dataset(*, max_shards: int = 90, max_rows: int | None = None) -> pd.DataFrame:
    """Downloads minute-bar shards from HF_ALPACA_OPTIONS_DATASET_REPO,
    most-recent-first, stopping once enough rows are in hand to cover the
    cap -- same discipline as every other load_training_dataset here."""
    if not HF_API_KEY:
        return pd.DataFrame()
    cap = MAX_TRAIN_ROWS if max_rows is None else max_rows
    stop_after_rows = int(cap * 1.5) if cap else None
    frames: list[pd.DataFrame] = []
    accumulated_rows = 0
    try:
        from huggingface_hub import HfApi, hf_hub_download
        api = HfApi(token=HF_API_KEY)
        hf_files = [f for f in api.list_repo_files(repo_id=HF_ALPACA_OPTIONS_DATASET_REPO, repo_type="dataset") if _DATE_SHARD_RE.match(f)]
        hf_files = sorted(hf_files, reverse=True)[:max_shards]
        for f in hf_files:
            if stop_after_rows and accumulated_rows >= stop_after_rows:
                break
            try:
                local_path = hf_hub_download(repo_id=HF_ALPACA_OPTIONS_DATASET_REPO, filename=f, repo_type="dataset", token=HF_API_KEY)
                shard = pd.read_parquet(local_path)
                if "symbol" in shard.columns and "ts" in shard.columns:
                    frames.append(shard)
                    accumulated_rows += len(shard)
                else:
                    logger.warning("[alpaca_options_data] skipping shard with unexpected schema: %s", f)
            except Exception as exc:
                logger.warning("[alpaca_options_data] failed to read shard %s: %s", f, exc)
    except Exception as exc:
        logger.warning("[alpaca_options_data] HF dataset listing failed: %s", exc)

    if not frames:
        return pd.DataFrame()
    combined = pd.concat(frames, ignore_index=True)
    del frames
    if "symbol" in combined.columns and "ts" in combined.columns:
        combined = combined.drop_duplicates(subset=["symbol", "ts"])
        combined["symbol"] = combined["symbol"].astype("category")
        if cap and len(combined) > cap:
            combined = combined.sort_values("ts").tail(cap).reset_index(drop=True)
    return combined
