"""
Multi-Sport Data Pipeline Scheduler
====================================

Integrates multi_sport_hf_manager into background scheduler.
Ensures:
1. One-time bootstrap of historical data per sport
2. Every 30 minutes: fetch and push live games
3. Automatic model retraining on HF after data push
"""

import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .multi_sport_hf_manager import get_multi_sport_manager, bootstrap_all_sports, fetch_and_push_live_games

logger = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).resolve().parents[3]
DATA_DIR = ROOT_DIR / "data"
BOOTSTRAP_STATE_FILE = DATA_DIR / "multi_sport_bootstrap.json"


def _load_bootstrap_state() -> dict[str, Any]:
    """Load bootstrap state."""
    import json
    try:
        if BOOTSTRAP_STATE_FILE.exists():
            with open(BOOTSTRAP_STATE_FILE) as f:
                return json.load(f)
    except Exception as e:
        logger.warning(f"Failed to load bootstrap state: {e}")
    return {"completed": False, "timestamp": None, "result": {}}


def _save_bootstrap_state(state: dict[str, Any]) -> None:
    """Save bootstrap state."""
    import json
    try:
        with open(BOOTSTRAP_STATE_FILE, "w") as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        logger.error(f"Failed to save bootstrap state: {e}")


def run_multi_sport_bootstrap() -> dict[str, Any]:
    """
    Bootstrap all sports with historical data.
    Should be called once on startup or manually.
    
    Returns bootstrap result with sport-by-sport status.
    """
    # Check if already done
    state = _load_bootstrap_state()
    if state.get("completed"):
        logger.info("Multi-sport bootstrap already completed")
        return {"ok": True, "message": "already_completed", "state": state}
    
    logger.info("Starting multi-sport bootstrap...")
    
    try:
        result = bootstrap_all_sports()
        
        # Save state
        state = {
            "completed": True,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "result": result,
        }
        _save_bootstrap_state(state)
        
        logger.info(f"Multi-sport bootstrap completed: {result}")
        return {"ok": True, "result": result}
    
    except Exception as e:
        logger.error(f"Multi-sport bootstrap failed: {e}")
        return {"ok": False, "error": str(e)}


def run_multi_sport_live_fetch() -> dict[str, Any]:
    """
    Fetch and push live games for all sports.
    Called every 30 minutes.
    
    Returns:
    {
        "ok": bool,
        "cricket": {"fetched": int, "pushed": int},
        "baseball": {"fetched": int, "pushed": int},
        "soccer": {"fetched": int, "pushed": int},
    }
    """
    try:
        result = fetch_and_push_live_games()
        
        total_pushed = sum(
            sport_data.get("pushed", 0)
            for sport_data in [result.get(s, {}) for s in ["cricket", "baseball", "soccer"]]
        )
        
        if total_pushed > 0:
            logger.info(f"Multi-sport live fetch: {total_pushed} games pushed to HF")
            
            # After pushing new data, trigger model retraining
            # This will be picked up by the active cycle
            logger.info("New data pushed to HF. Active cycle will retrain model.")
        else:
            logger.debug("No new games fetched for any sport")
        
        return result
    
    except Exception as e:
        logger.error(f"Multi-sport live fetch error: {e}")
        return {
            "ok": False,
            "error": str(e),
            "cricket": {"fetched": 0, "pushed": 0},
            "baseball": {"fetched": 0, "pushed": 0},
            "soccer": {"fetched": 0, "pushed": 0},
        }


def get_multi_sport_scheduler_status() -> dict[str, Any]:
    """Get current status of multi-sport data pipeline."""
    bootstrap_state = _load_bootstrap_state()
    
    return {
        "bootstrap_completed": bootstrap_state.get("completed", False),
        "bootstrap_timestamp": bootstrap_state.get("timestamp"),
        "bootstrap_result": bootstrap_state.get("result", {}),
        "enabled": True,
    }


if __name__ == "__main__":
    # Quick test
    print("Testing multi-sport scheduler...")
    
    # Bootstrap test
    print("\n1. Testing bootstrap...")
    result = run_multi_sport_bootstrap()
    print(f"Bootstrap: {result}")
    
    # Live fetch test
    print("\n2. Testing live fetch...")
    result = run_multi_sport_live_fetch()
    print(f"Live fetch: {result}")
    
    # Status test
    print("\n3. Checking status...")
    status = get_multi_sport_scheduler_status()
    print(f"Status: {status}")
