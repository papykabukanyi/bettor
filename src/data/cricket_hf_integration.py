"""
Cricket Data Integration with Hugging Face
============================================

Fetches live cricket data and pushes it to Hugging Face dataset for training.
This ensures cricket predictions are generated alongside MLB and Soccer.

Cricket data sources:
- RapidAPI Cricket Live Line (free tier, primary)
- ESPNCricinfo (historical, fallback)
- Cricbuzz (live scores, fallback)

Flow:
1. Fetch live cricket matches every 30 minutes
2. Push to HF dataset `games` subset
3. HF model ingests cricket games as training data
4. Model generates cricket predictions
5. Predictions auto-formatted for Kalshi
6. Singles + Combos placed 60 min before match start
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
from typing import Any

from .hf_uploader import HFUploader
from .unified_sport_fetcher import UnifiedSportFetcher

logger = logging.getLogger(__name__)


class CricketHFIntegration:
    """Fetch cricket data and push to HF dataset."""

    def __init__(self):
        """Initialize cricket fetcher and HF uploader."""
        self.fetcher = UnifiedSportFetcher()
        self.uploader = HFUploader() if os.getenv("HF_API_KEY") else None

    def push_cricket_games_to_hf(self) -> dict[str, Any]:
        """
        Fetch live cricket matches and push to HF dataset.
        
        Returns:
            {
                "ok": bool,
                "fetched_count": int,
                "pushed_count": int,
                "error": str | None,
                "games": [...]
            }
        """
        try:
            # Fetch live cricket data
            cricket_matches = self.fetcher.fetch_cricket_live()
            
            if not cricket_matches:
                return {
                    "ok": True,
                    "fetched_count": 0,
                    "pushed_count": 0,
                    "error": None,
                    "games": [],
                }
            
            # Transform cricket data to games schema for HF
            games_for_hf = []
            now = dt.datetime.now(dt.timezone.utc)
            
            for match in cricket_matches:
                try:
                    # Parse scheduled_start if available
                    scheduled_start_str = str(match.get("scheduled_start") or match.get("start_date") or "").strip()
                    try:
                        if "T" in scheduled_start_str:
                            scheduled_start = dt.datetime.fromisoformat(scheduled_start_str.replace("Z", "+00:00"))
                        else:
                            scheduled_start = dt.datetime.fromisoformat(f"{scheduled_start_str}T00:00:00+00:00")
                    except Exception:
                        scheduled_start = now + dt.timedelta(hours=2)
                    
                    game_date = scheduled_start.date().isoformat()
                    game_time = scheduled_start.isoformat()
                    
                    game_record = {
                        # Core identifiers
                        "sport": "cricket",
                        "league": str(match.get("league") or match.get("format") or "international").lower(),
                        "game_id": str(match.get("match_id") or match.get("id") or ""),
                        "game_key": str(match.get("match_id") or match.get("id") or ""),
                        
                        # Teams
                        "home_team": str(match.get("team1") or match.get("home_team") or "Home"),
                        "away_team": str(match.get("team2") or match.get("away_team") or "Away"),
                        
                        # Timing
                        "game_date": game_date,
                        "game_time": game_time,
                        "scheduled_start": game_time,
                        
                        # Status
                        "status": str(match.get("status") or "scheduled").lower(),
                        "current_score": str(match.get("current_score") or ""),
                        "venue": str(match.get("venue") or ""),
                        
                        # Metadata
                        "created_at": now.isoformat(),
                        "updated_at": now.isoformat(),
                    }
                    
                    games_for_hf.append(game_record)
                    
                except Exception as match_err:
                    logger.warning(f"Failed to transform cricket match: {match_err}")
                    continue
            
            # Push to HF dataset
            if not games_for_hf:
                return {
                    "ok": True,
                    "fetched_count": len(cricket_matches),
                    "pushed_count": 0,
                    "error": "No valid games to push",
                    "games": [],
                }
            
            if self.uploader and self.uploader._ok:
                pushed = self.uploader.push_records("games", games_for_hf)
                if not pushed:
                    logger.warning("HF upload returned False, but records may have been queued")
            else:
                logger.warning("HF uploader not available")
            
            return {
                "ok": True,
                "fetched_count": len(cricket_matches),
                "pushed_count": len(games_for_hf),
                "error": None,
                "games": games_for_hf,
            }
            
        except Exception as exc:
            logger.error(f"Cricket HF integration error: {exc}")
            return {
                "ok": False,
                "fetched_count": 0,
                "pushed_count": 0,
                "error": str(exc),
                "games": [],
            }

    def push_all_sports_to_hf(self) -> dict[str, Any]:
        """
        Fetch ALL sports (cricket, MLB, soccer) and push to HF.
        
        This is called by the main scheduler to keep HF dataset fresh.
        
        Returns aggregated result with counts per sport.
        """
        try:
            result = {
                "ok": True,
                "cricket": self.push_cricket_games_to_hf(),
                "error": None,
            }
            
            # Fetch and push MLB data (if available)
            try:
                mlb_matches = self.fetcher.fetch_mlb_live()
                if mlb_matches:
                    mlb_for_hf = []
                    now = dt.datetime.now(dt.timezone.utc)
                    
                    for match in mlb_matches:
                        try:
                            game_date = str(match.get("game_date") or "").strip()[:10]
                            game_time = str(match.get("game_time") or match.get("scheduled_start") or "").strip()
                            
                            game_record = {
                                "sport": "mlb",
                                "league": "mlb",
                                "game_id": str(match.get("game_id") or ""),
                                "game_key": str(match.get("game_key") or match.get("game_id") or ""),
                                "home_team": str(match.get("home_team") or ""),
                                "away_team": str(match.get("away_team") or ""),
                                "game_date": game_date or now.date().isoformat(),
                                "game_time": game_time or now.isoformat(),
                                "status": str(match.get("status") or "scheduled").lower(),
                                "venue": str(match.get("venue") or ""),
                                "created_at": now.isoformat(),
                                "updated_at": now.isoformat(),
                            }
                            mlb_for_hf.append(game_record)
                        except Exception:
                            continue
                    
                    if mlb_for_hf and self.uploader and self.uploader._ok:
                        self.uploader.push_records("games", mlb_for_hf)
                    
                    result["mlb"] = {
                        "ok": True,
                        "fetched_count": len(mlb_matches),
                        "pushed_count": len(mlb_for_hf),
                    }
            except Exception as mlb_err:
                logger.error(f"MLB data fetch error: {mlb_err}")
                result["mlb"] = {"ok": False, "error": str(mlb_err)}
            
            # Fetch and push Soccer data (if available)
            try:
                soccer_matches = self.fetcher.fetch_soccer_live()
                if soccer_matches:
                    soccer_for_hf = []
                    now = dt.datetime.now(dt.timezone.utc)
                    
                    for match in soccer_matches:
                        try:
                            game_date = str(match.get("game_date") or "").strip()[:10]
                            game_time = str(match.get("game_time") or match.get("utcDate") or "").strip()
                            
                            game_record = {
                                "sport": "soccer",
                                "league": str(match.get("league") or ""),
                                "game_id": str(match.get("game_id") or match.get("id") or ""),
                                "game_key": str(match.get("game_key") or match.get("id") or ""),
                                "home_team": str(match.get("home_team") or ""),
                                "away_team": str(match.get("away_team") or ""),
                                "game_date": game_date or now.date().isoformat(),
                                "game_time": game_time or now.isoformat(),
                                "status": str(match.get("status") or "scheduled").lower(),
                                "venue": str(match.get("venue") or ""),
                                "created_at": now.isoformat(),
                                "updated_at": now.isoformat(),
                            }
                            soccer_for_hf.append(game_record)
                        except Exception:
                            continue
                    
                    if soccer_for_hf and self.uploader and self.uploader._ok:
                        self.uploader.push_records("games", soccer_for_hf)
                    
                    result["soccer"] = {
                        "ok": True,
                        "fetched_count": len(soccer_matches),
                        "pushed_count": len(soccer_for_hf),
                    }
            except Exception as soccer_err:
                logger.error(f"Soccer data fetch error: {soccer_err}")
                result["soccer"] = {"ok": False, "error": str(soccer_err)}
            
            return result
            
        except Exception as exc:
            logger.error(f"Multi-sport HF integration error: {exc}")
            return {
                "ok": False,
                "error": str(exc),
            }


# ============================================================================
# Module-level helper functions
# ============================================================================

def get_cricket_hf_integrator() -> CricketHFIntegration:
    """Get or create cricket HF integrator singleton."""
    return CricketHFIntegration()


def push_cricket_to_hf() -> dict[str, Any]:
    """Quick push of cricket data to HF. Called by scheduler."""
    integrator = get_cricket_hf_integrator()
    return integrator.push_cricket_games_to_hf()


def push_all_sports_to_hf() -> dict[str, Any]:
    """Push all sports to HF. Called by scheduler."""
    integrator = get_cricket_hf_integrator()
    return integrator.push_all_sports_to_hf()


if __name__ == "__main__":
    # Test: python -m src.data.cricket_hf_integration
    integrator = CricketHFIntegration()
    result = integrator.push_all_sports_to_hf()
    print(json.dumps(result, indent=2, default=str))
