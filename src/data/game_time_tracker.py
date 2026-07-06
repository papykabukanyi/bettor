"""
Game Time Tracking & Pre-Game Analysis
=======================================
Tracks upcoming games and triggers prediction analysis at the right times:
- 90 minutes before start: trigger detailed analysis
- 60 minutes before start: place predictions if confident
- At start: mark game as live

Key responsibilities:
- Query HF dataset for upcoming fixtures
- Track game start times
- Detect games within analysis window (90 min before)
- Request missing historical data
- Trigger model analysis and prediction generation
- Auto-place singles + combos at bet window (60 min before)

Usage
-----
    from src.data.game_time_tracker import GameTimeTracker
    
    tracker = GameTimeTracker()
    
    # Detect upcoming games
    upcoming = tracker.get_upcoming_games(
        minutes_ahead=90,  # Look 90 min ahead
        sports=["mlb", "nba", "cricket"],
    )
    
    # Trigger analysis for games in analysis window
    analysis_jobs = tracker.get_games_needing_analysis(
        analysis_window_minutes=90,
    )
    
    # Get games ready for betting
    bet_ready = tracker.get_games_ready_for_betting(
        bet_window_minutes=60,
    )
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import logging
import os
from collections import defaultdict
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT_DIR / "data"
GAME_TRACKING_FILE = DATA_DIR / "game_tracking_cache.json"


class GameTimeTracker:
    """Track upcoming games and trigger pre-game analysis."""

    def __init__(self):
        """Initialize game tracking state."""
        self.tracking_cache = self._load_tracking_cache()
        self.last_update_utc = dt.datetime.now(dt.timezone.utc)

    def _load_tracking_cache(self) -> dict[str, Any]:
        """Load cached game tracking state."""
        if not GAME_TRACKING_FILE.exists():
            return {"games": {}, "last_update": ""}
        
        try:
            with open(GAME_TRACKING_FILE, "r") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load tracking cache: {e}")
            return {"games": {}, "last_update": ""}

    def _save_tracking_cache(self) -> None:
        """Persist game tracking state to file."""
        try:
            self.tracking_cache["last_update"] = self.last_update_utc.isoformat()
            with open(GAME_TRACKING_FILE, "w") as f:
                json.dump(self.tracking_cache, f, indent=2, default=str)
        except Exception as e:
            logger.error(f"Failed to save tracking cache: {e}")

    def _game_key(
        self,
        sport: str,
        game_id: str,
        game_date: str,
        home_team: str,
        away_team: str,
    ) -> str:
        """Generate unique game key for tracking."""
        raw = "|".join([
            sport.lower().strip(),
            game_id.lower().strip(),
            game_date.lower().strip(),
            home_team.lower().strip(),
            away_team.lower().strip(),
        ])
        return "gk_" + hashlib.sha1(raw.encode("utf-8")).hexdigest()[:20]

    def get_upcoming_games(
        self,
        minutes_ahead: int = 120,
        sports: list[str] | None = None,
        games_list: list[dict[str, Any]] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Filter upcoming games from a list or HF dataset.
        
        Args:
            minutes_ahead: Look this many minutes into the future
            sports: Filter to these sports (if None, all sports)
            games_list: List of game dicts; if None, would query HF
            
        Returns:
            List of games sorted by start time, soonest first.
            Each game includes:
            {
                "game_key": "...",
                "sport": "...",
                "game_id": "...",
                "game_date": "...",
                "game_time": "...",
                "home_team": "...",
                "away_team": "...",
                "minutes_until_start": N,
                "status": "not_started" | "live" | "final",
                "tracking_state": {...},
            }
        """
        if games_list is None:
            games_list = []

        now_utc = dt.datetime.now(dt.timezone.utc)
        cutoff_utc = now_utc + dt.timedelta(minutes=minutes_ahead)
        
        upcoming = []

        for game in games_list:
            try:
                sport = str(game.get("sport") or "").lower().strip()
                
                # Filter by sport if requested
                if sports and sport not in sports:
                    continue

                # Parse game time
                game_time_str = str(game.get("game_time") or "").strip()
                game_date_str = str(game.get("game_date") or "").strip()
                
                game_start = self._parse_game_time(game_time_str, game_date_str)
                if not game_start:
                    logger.debug(f"Could not parse game time: {game_time_str}, {game_date_str}")
                    continue

                # Check if game is in upcoming window
                if game_start <= now_utc:
                    continue  # Game already started
                
                if game_start > cutoff_utc:
                    continue  # Game too far in future

                game_key = self._game_key(
                    sport,
                    str(game.get("game_id") or ""),
                    game_date_str,
                    str(game.get("home_team") or ""),
                    str(game.get("away_team") or ""),
                )

                minutes_until = max(0, int((game_start - now_utc).total_seconds() / 60))
                
                # Determine status
                status = "not_started"
                game_status = str(game.get("status") or "").lower()
                if "live" in game_status or "in_progress" in game_status:
                    status = "live"
                elif "final" in game_status or "completed" in game_status:
                    status = "final"

                upcoming.append({
                    "game_key": game_key,
                    "sport": sport,
                    "game_id": str(game.get("game_id") or ""),
                    "game_date": game_date_str,
                    "game_time": game_time_str,
                    "game_start_utc": game_start.isoformat(),
                    "home_team": str(game.get("home_team") or ""),
                    "away_team": str(game.get("away_team") or ""),
                    "league": str(game.get("league") or ""),
                    "minutes_until_start": minutes_until,
                    "status": status,
                    "tracking_state": self.tracking_cache.get("games", {}).get(game_key, {
                        "analysis_triggered_at": None,
                        "predictions_generated_at": None,
                        "bets_placed_at": None,
                        "live_marked_at": None,
                        "final_marked_at": None,
                    }),
                })
            except Exception as e:
                logger.error(f"Error processing game {game}: {e}", exc_info=True)
                continue

        # Sort by start time, soonest first
        upcoming.sort(key=lambda g: g["minutes_until_start"])
        return upcoming

    def get_games_needing_analysis(
        self,
        analysis_window_minutes: int = 90,
        games_list: list[dict[str, Any]] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Get games that are within the analysis window but haven't been analyzed yet.
        
        Returns:
            Games ready for: request missing data → train model → generate predictions
        """
        upcoming = self.get_upcoming_games(
            minutes_ahead=analysis_window_minutes,
            games_list=games_list,
        )

        ready_for_analysis = []
        for game in upcoming:
            tracking = game.get("tracking_state", {})
            analysis_triggered = tracking.get("analysis_triggered_at")
            
            # If we haven't triggered analysis yet, this game needs it
            if not analysis_triggered:
                ready_for_analysis.append(game)

        return ready_for_analysis

    def get_games_ready_for_betting(
        self,
        bet_window_minutes: int = 60,
        games_list: list[dict[str, Any]] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Get games that are within the bet window and have predictions ready.
        
        Returns:
            Games ready for: place singles + combos
        """
        upcoming = self.get_upcoming_games(
            minutes_ahead=bet_window_minutes,
            games_list=games_list,
        )

        ready_for_betting = []
        for game in upcoming:
            tracking = game.get("tracking_state", {})
            predictions_generated = tracking.get("predictions_generated_at")
            bets_placed = tracking.get("bets_placed_at")
            
            # If we have predictions but haven't placed bets yet, this game is ready
            if predictions_generated and not bets_placed:
                ready_for_betting.append(game)

        return ready_for_betting

    def mark_analysis_triggered(self, game_key: str) -> None:
        """Mark that analysis has been triggered for a game."""
        now = dt.datetime.now(dt.timezone.utc).isoformat()
        
        if "games" not in self.tracking_cache:
            self.tracking_cache["games"] = {}
        
        if game_key not in self.tracking_cache["games"]:
            self.tracking_cache["games"][game_key] = {}
        
        self.tracking_cache["games"][game_key]["analysis_triggered_at"] = now
        self._save_tracking_cache()
        logger.info(f"Marked analysis triggered for {game_key}")

    def mark_predictions_generated(self, game_key: str, prediction_count: int = 0) -> None:
        """Mark that predictions have been generated for a game."""
        now = dt.datetime.now(dt.timezone.utc).isoformat()
        
        if "games" not in self.tracking_cache:
            self.tracking_cache["games"] = {}
        
        if game_key not in self.tracking_cache["games"]:
            self.tracking_cache["games"][game_key] = {}
        
        self.tracking_cache["games"][game_key]["predictions_generated_at"] = now
        self.tracking_cache["games"][game_key]["prediction_count"] = prediction_count
        self._save_tracking_cache()
        logger.info(f"Marked predictions generated for {game_key}: {prediction_count} predictions")

    def mark_bets_placed(self, game_key: str, single_orders: int = 0, combo_orders: int = 0) -> None:
        """Mark that bets have been placed for a game."""
        now = dt.datetime.now(dt.timezone.utc).isoformat()
        
        if "games" not in self.tracking_cache:
            self.tracking_cache["games"] = {}
        
        if game_key not in self.tracking_cache["games"]:
            self.tracking_cache["games"][game_key] = {}
        
        self.tracking_cache["games"][game_key]["bets_placed_at"] = now
        self.tracking_cache["games"][game_key]["single_orders_placed"] = single_orders
        self.tracking_cache["games"][game_key]["combo_orders_placed"] = combo_orders
        self._save_tracking_cache()
        logger.info(f"Marked bets placed for {game_key}: {single_orders} singles, {combo_orders} combos")

    def mark_game_live(self, game_key: str) -> None:
        """Mark that a game has started and is live."""
        now = dt.datetime.now(dt.timezone.utc).isoformat()
        
        if "games" not in self.tracking_cache:
            self.tracking_cache["games"] = {}
        
        if game_key not in self.tracking_cache["games"]:
            self.tracking_cache["games"][game_key] = {}
        
        self.tracking_cache["games"][game_key]["live_marked_at"] = now
        self._save_tracking_cache()
        logger.info(f"Marked game live: {game_key}")

    def _parse_game_time(self, game_time: str, game_date: str) -> dt.datetime | None:
        """Parse game time string to UTC datetime."""
        raw = str(game_time or "").strip()
        date_raw = str(game_date or "").strip()

        # Try parsing as ISO format first (with timezone)
        if raw:
            candidate = raw.replace("Z", "+00:00")
            try:
                parsed = dt.datetime.fromisoformat(candidate)
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=dt.timezone.utc)
                return parsed
            except Exception:
                pass

        # Try date + time separately
        if date_raw and raw:
            try:
                parsed_date = dt.date.fromisoformat(date_raw[:10])
                time_parts = raw.split(":")
                if len(time_parts) >= 2:
                    hour = int(time_parts[0])
                    minute = int(time_parts[1])
                    parsed_dt = dt.datetime(
                        parsed_date.year, parsed_date.month, parsed_date.day,
                        hour, minute, tzinfo=dt.timezone.utc
                    )
                    return parsed_dt
            except Exception:
                pass

        # Last resort: just use date as midnight UTC
        if date_raw:
            try:
                parsed_date = dt.date.fromisoformat(date_raw[:10])
                return dt.datetime(
                    parsed_date.year, parsed_date.month, parsed_date.day,
                    tzinfo=dt.timezone.utc
                )
            except Exception:
                pass

        return None

    def get_tracking_summary(self) -> dict[str, Any]:
        """Get summary of tracked games and their analysis states."""
        games = self.tracking_cache.get("games", {})
        
        summary = {
            "total_tracked": len(games),
            "analysis_triggered": 0,
            "predictions_generated": 0,
            "bets_placed": 0,
            "live_marked": 0,
            "by_status": defaultdict(int),
            "games": games,
        }

        for game_key, state in games.items():
            if state.get("analysis_triggered_at"):
                summary["analysis_triggered"] += 1
            if state.get("predictions_generated_at"):
                summary["predictions_generated"] += 1
            if state.get("bets_placed_at"):
                summary["bets_placed"] += 1
            if state.get("live_marked_at"):
                summary["live_marked"] += 1

        return summary


# Singleton instance
_tracker: GameTimeTracker | None = None


def get_tracker() -> GameTimeTracker:
    """Get or create singleton tracker instance."""
    global _tracker
    if _tracker is None:
        _tracker = GameTimeTracker()
    return _tracker


if __name__ == "__main__":
    # Demo usage
    tracker = get_tracker()

    # Demo games
    demo_games = [
        {
            "sport": "mlb",
            "game_id": "2026-07-06-nyr-bos",
            "game_date": "2026-07-06",
            "game_time": "19:05",
            "home_team": "NYY",
            "away_team": "BOS",
            "league": "MLB",
            "status": "not_started",
        },
        {
            "sport": "cricket",
            "game_id": "2026-07-06-ind-pak",
            "game_date": "2026-07-06",
            "game_time": "14:30Z",
            "home_team": "IND",
            "away_team": "PAK",
            "league": "ICC",
            "status": "not_started",
        },
    ]

    # Get upcoming games
    upcoming = tracker.get_upcoming_games(minutes_ahead=180, games_list=demo_games)
    print("Upcoming games (next 3 hours):")
    for game in upcoming:
        print(f"  {game['sport'].upper()}: {game['home_team']} vs {game['away_team']} in {game['minutes_until_start']} min")

    # Get games needing analysis
    analysis_needed = tracker.get_games_needing_analysis(analysis_window_minutes=90, games_list=demo_games)
    print(f"\nGames needing analysis: {len(analysis_needed)}")
    for game in analysis_needed:
        tracker.mark_analysis_triggered(game["game_key"])
        tracker.mark_predictions_generated(game["game_key"], prediction_count=5)

    # Get games ready for betting
    bet_ready = tracker.get_games_ready_for_betting(bet_window_minutes=60, games_list=demo_games)
    print(f"\nGames ready for betting: {len(bet_ready)}")
    for game in bet_ready:
        tracker.mark_bets_placed(game["game_key"], single_orders=3, combo_orders=1)

    # Get tracking summary
    summary = tracker.get_tracking_summary()
    print(f"\nTracking summary: {summary['total_tracked']} total, {summary['predictions_generated']} with predictions")
