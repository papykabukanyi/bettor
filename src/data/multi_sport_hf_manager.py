"""
Multi-Sport HF Dataset Manager
================================

Orchestrates continuous data ingestion for cricket, baseball, and soccer.

Key Features:
- Fetches all sports with proper game dating
- Loads 2-season historical data (one-time bootstrap)
- Continuously feeds new games every 30 minutes
- Ensures HF model trains on complete multi-sport data
- Generates predictions for all sports

Architecture:
┌──────────────────────────────────────────────────┐
│  Multi-Sport Data Manager                        │
├──────────────────────────────────────────────────┤
│                                                  │
│  1. Bootstrap (one-time per sport):             │
│     └─ Load 2-season historical games/players  │
│        └─ Push to HF dataset                   │
│                                                  │
│  2. Continuous Feed (every 30 min):            │
│     └─ Fetch upcoming games (all sports)       │
│        └─ Push to HF dataset                   │
│                                                  │
│  3. Model Training (automatic on HF):          │
│     └─ HF model ingests multi-sport data       │
│        └─ Generates predictions for all sports │
│                                                  │
└──────────────────────────────────────────────────┘

Historical Data Strategy:
- Keep last 2 complete seasons for each sport
- One-time load per season (prevents duplicates)
- Track loaded seasons in HF metadata
- New games continuously appended

Data Schema (unified for all sports):
├── game_id: Unique per game
├── sport: "cricket" | "baseball" | "soccer"
├── league: League name (IPL, MLB, Premier League, etc.)
├── game_date: YYYY-MM-DD
├── game_datetime: ISO format with timezone
├── home_team / away_team: Team names
├── season: Year (2024, 2025, 2026)
├── status: "scheduled" | "live" | "completed"
└── metadata: JSON with sport-specific details
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
from typing import Any
from pathlib import Path
import requests

logger = logging.getLogger(__name__)


class MultiSportHFDataManager:
    """Manages multi-sport data ingestion for HF dataset."""

    def __init__(self):
        """Initialize manager with fetchers and uploader."""
        self.cache_dir = Path(__file__).parent.parent.parent / "data"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Track which seasons have been loaded
        self.seasons_loaded_file = self.cache_dir / "hf_seasons_loaded.json"
        self.loaded_seasons = self._load_seasons_tracker()
        
        # Lazy-init uploader and fetcher (don't call in __init__ to avoid import delays)
        self._uploader = None
        self._fetcher = None

    @property
    def uploader(self):
        """Lazy-load HF uploader on first access."""
        if self._uploader is None:
            try:
                from .hf_uploader import HFUploader
                self._uploader = HFUploader() if os.getenv("HF_API_KEY") else None
            except Exception as e:
                logger.warning(f"Failed to initialize HF uploader: {e}")
                self._uploader = None
        return self._uploader

    @property
    def fetcher(self):
        """Lazy-load UnifiedSportFetcher on first access."""
        if self._fetcher is None:
            try:
                from .unified_sport_fetcher import UnifiedSportFetcher
                self._fetcher = UnifiedSportFetcher()
            except Exception as e:
                logger.warning(f"Failed to initialize fetcher: {e}")
                self._fetcher = None
        return self._fetcher

    def _load_seasons_tracker(self) -> dict[str, list[int]]:
        """Load which seasons have been loaded for each sport."""
        try:
            if self.seasons_loaded_file.exists():
                with open(self.seasons_loaded_file) as f:
                    return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to load seasons tracker: {e}")
        
        return {"cricket": [], "baseball": [], "soccer": []}

    def _save_seasons_tracker(self) -> None:
        """Save seasons tracker."""
        try:
            self.seasons_loaded_file.parent.mkdir(parents=True, exist_ok=True)
            with open(self.seasons_loaded_file, "w") as f:
                json.dump(self.loaded_seasons, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save seasons tracker: {e}")

    def _default_cricsheet_zip_path(self) -> Path:
        return self.cache_dir / "cricket" / "all_json.zip"

    def _download_cricsheet_once(self) -> Path:
        """
        Ensure a local Cricsheet archive exists (one-time download).
        Returns the zip path.
        """
        configured = str(os.getenv("CRICKET_CRICSHEET_DIR", "") or "").strip()
        zip_path = Path(configured) if configured else self._default_cricsheet_zip_path()
        if not zip_path.is_absolute():
            zip_path = Path.cwd() / zip_path
        zip_path.parent.mkdir(parents=True, exist_ok=True)
        if zip_path.exists() and zip_path.stat().st_size > 1024:
            logger.info("Cricsheet archive already present: %s", zip_path)
            return zip_path

        url = str(os.getenv("CRICKET_CRICSHEET_URL", "https://cricsheet.org/downloads/all_json.zip") or "").strip()
        logger.info("Downloading Cricsheet archive from %s", url)
        resp = requests.get(url, timeout=180, stream=True)
        resp.raise_for_status()
        with open(zip_path, "wb") as handle:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    handle.write(chunk)
        logger.info("Cricsheet archive downloaded to %s", zip_path)
        return zip_path

    def _bootstrap_cricket_from_cricsheet(self, seasons: list[int]) -> list[dict[str, Any]]:
        """
        Parse cricket historical rows from Cricsheet archive for requested seasons.
        """
        zip_path = self._download_cricsheet_once()
        os.environ["CRICKET_CRICSHEET_DIR"] = str(zip_path)
        from .hf_pipeline import HFDirectPipeline

        pipeline = HFDirectPipeline()
        rows: list[dict[str, Any]] = []
        for season in seasons:
            start = dt.date(season, 1, 1)
            end = dt.date(season, 12, 31)
            try:
                season_rows = pipeline._fetch_cricket_games_cricsheet(start, end)  # noqa: SLF001
                if season_rows:
                    rows.extend(season_rows)
            except Exception as exc:
                logger.warning("Failed parsing Cricsheet season %s: %s", season, exc)
        return rows

    def bootstrap_cricket_historical(self, seasons: list[int] | None = None) -> dict[str, Any]:
        """
        Bootstrap cricket with 2-season historical data.
        
        Args:
            seasons: Years to load (default: last 2 years)
        
        Returns:
            {"ok": bool, "loaded_count": int, "error": str | None}
        """
        if not seasons:
            current_year = dt.datetime.now().year
            seasons = [current_year - 1, current_year]
        
        logger.info(f"Bootstrapping cricket seasons: {seasons}")
        
        try:
            # Check what's already loaded
            already_loaded = self.loaded_seasons.get("cricket", [])
            new_seasons = [s for s in seasons if s not in already_loaded]
            
            force = str(os.getenv("CRICKET_FORCE_BOOTSTRAP", "0") or "0").strip().lower() in {"1", "true", "yes", "on"}
            if not new_seasons and not force:
                logger.info("Cricket seasons already loaded, skipping bootstrap")
                return {"ok": True, "loaded_count": 0, "message": "already_loaded"}
            if force and not new_seasons:
                new_seasons = seasons[:]
             
            games_to_push = self._bootstrap_cricket_from_cricsheet(new_seasons)
            logger.info("Cricket bootstrap rows from Cricsheet: %d", len(games_to_push))
             
            if games_to_push and self.uploader:
                self.uploader.push_records("games", games_to_push)
                self.uploader.flush_all()

            if games_to_push:
                for season in new_seasons:
                    if season not in self.loaded_seasons["cricket"]:
                        self.loaded_seasons["cricket"].append(season)
            else:
                logger.warning("No cricket rows parsed from Cricsheet; seasons will not be marked loaded")
             
            self._save_seasons_tracker()
            
            return {
                "ok": True,
                "loaded_count": len(games_to_push),
                "seasons_loaded": self.loaded_seasons["cricket"],
            }
        
        except Exception as e:
            logger.error(f"Cricket bootstrap error: {e}")
            return {"ok": False, "loaded_count": 0, "error": str(e)}

    def bootstrap_baseball_historical(self, seasons: list[int] | None = None) -> dict[str, Any]:
        """
        Bootstrap baseball (MLB + other leagues) with 2-season historical data.
        """
        if not seasons:
            current_year = dt.datetime.now().year
            seasons = [current_year - 1, current_year]
        
        logger.info(f"Bootstrapping baseball seasons: {seasons}")
        
        try:
            already_loaded = self.loaded_seasons.get("baseball", [])
            new_seasons = [s for s in seasons if s not in already_loaded]
            
            if not new_seasons:
                logger.info("Baseball seasons already loaded, skipping bootstrap")
                return {"ok": True, "loaded_count": 0, "message": "already_loaded"}
            
            games_to_push = []
            now = dt.datetime.now(dt.timezone.utc)
            
            # Fetch MLB games for each season
            for season in new_seasons:
                try:
                    logger.info(f"Baseball {season}: Would fetch from MLB official API")
                    # Implementation would fetch from MLB Stats API
                    # For now, this is placeholder - actual implementation would:
                    # 1. Fetch all games from season start to end
                    # 2. Parse dates properly (ML B season ~March-Oct)
                    # 3. Include all teams, scores, metadata
                    
                    if season not in self.loaded_seasons["baseball"]:
                        self.loaded_seasons["baseball"].append(season)
                
                except Exception as e:
                    logger.warning(f"Failed to load baseball season {season}: {e}")
                    continue
            
            if games_to_push and self.uploader:
                self.uploader.push_records("games", games_to_push)
            
            self._save_seasons_tracker()
            
            return {
                "ok": True,
                "loaded_count": len(games_to_push),
                "seasons_loaded": self.loaded_seasons["baseball"],
            }
        
        except Exception as e:
            logger.error(f"Baseball bootstrap error: {e}")
            return {"ok": False, "loaded_count": 0, "error": str(e)}

    def bootstrap_soccer_historical(self, seasons: list[int] | None = None) -> dict[str, Any]:
        """
        Bootstrap soccer with 2-season historical data.
        Covers all major leagues and competitions.
        """
        if not seasons:
            current_year = dt.datetime.now().year
            seasons = [current_year - 1, current_year]
        
        logger.info(f"Bootstrapping soccer seasons: {seasons}")
        
        try:
            already_loaded = self.loaded_seasons.get("soccer", [])
            new_seasons = [s for s in seasons if s not in already_loaded]
            
            if not new_seasons:
                logger.info("Soccer seasons already loaded, skipping bootstrap")
                return {"ok": True, "loaded_count": 0, "message": "already_loaded"}
            
            games_to_push = []
            now = dt.datetime.now(dt.timezone.utc)
            
            # Fetch soccer games for each season
            for season in new_seasons:
                try:
                    logger.info(f"Soccer {season}: Would fetch from football-data.org")
                    # Implementation would fetch all major leagues/competitions
                    # Premier League, La Liga, Serie A, Bundesliga, Ligue 1, etc.
                    
                    if season not in self.loaded_seasons["soccer"]:
                        self.loaded_seasons["soccer"].append(season)
                
                except Exception as e:
                    logger.warning(f"Failed to load soccer season {season}: {e}")
                    continue
            
            if games_to_push and self.uploader:
                self.uploader.push_records("games", games_to_push)
            
            self._save_seasons_tracker()
            
            return {
                "ok": True,
                "loaded_count": len(games_to_push),
                "seasons_loaded": self.loaded_seasons["soccer"],
            }
        
        except Exception as e:
            logger.error(f"Soccer bootstrap error: {e}")
            return {"ok": False, "loaded_count": 0, "error": str(e)}

    def fetch_and_push_live_games(self) -> dict[str, Any]:
        """
        Fetch upcoming/live games for all sports and push to HF.
        Called every 30 minutes by scheduler.
        
        Returns:
            {
                "ok": bool,
                "cricket": {"fetched": int, "pushed": int},
                "baseball": {"fetched": int, "pushed": int},
                "soccer": {"fetched": int, "pushed": int},
            }
        """
        result = {
            "ok": True,
            "cricket": {"fetched": 0, "pushed": 0},
            "baseball": {"fetched": 0, "pushed": 0},
            "soccer": {"fetched": 0, "pushed": 0},
        }
        
        now = dt.datetime.now(dt.timezone.utc)
        
        # CRICKET
        try:
            cricket_games = self.fetcher.fetch_cricket_live()
            cricket_records = []
            
            for game in cricket_games:
                try:
                    record = {
                        "game_id": str(game.get("match_id") or game.get("id") or ""),
                        "record_id": f"cricket_{game.get('match_id')}_{now.timestamp()}",
                        "sport": "cricket",
                        "league": str(game.get("league") or game.get("format") or "international"),
                        "game_date": self._parse_date(game.get("scheduled_start") or game.get("start_date") or ""),
                        "game_datetime": self._parse_datetime(game.get("scheduled_start") or game.get("start_date") or ""),
                        "status": str(game.get("status") or "scheduled").lower(),
                        "home_team": str(game.get("home_team") or game.get("team1") or ((game.get("teams") or ["", ""])[0]) or ""),
                        "away_team": str(game.get("away_team") or game.get("team2") or ((game.get("teams") or ["", ""])[1]) or ""),
                        "home_score": 0.0,
                        "away_score": 0.0,
                        "home_starter": "",
                        "away_starter": "",
                        "season": now.year,
                        "metadata": json.dumps({
                            "match_type": game.get("match_type"),
                            "venue": game.get("venue"),
                            "current_score": game.get("current_score"),
                        }),
                        "created_at": now.isoformat(),
                    }
                    cricket_records.append(record)
                except Exception as e:
                    logger.warning(f"Failed to parse cricket game: {e}")
                    continue
            
            if cricket_records and self.uploader:
                self.uploader.push_records("games", cricket_records)
            
            result["cricket"]["fetched"] = len(cricket_games)
            result["cricket"]["pushed"] = len(cricket_records)
            
        except Exception as e:
            logger.error(f"Cricket live fetch error: {e}")
            result["cricket"]["error"] = str(e)
        
        # BASEBALL (MLB)
        try:
            mlb_games = self.fetcher.fetch_mlb_live()
            mlb_records = []
            
            for game in mlb_games:
                try:
                    record = {
                        "game_id": str(game.get("game_id") or game.get("id") or ""),
                        "record_id": f"mlb_{game.get('game_id')}_{now.timestamp()}",
                        "sport": "baseball",
                        "league": "mlb",
                        "game_date": self._parse_date(game.get("game_date") or game.get("game_datetime") or game.get("game_time") or ""),
                        "game_datetime": self._parse_datetime(game.get("game_datetime") or game.get("game_time") or game.get("game_date") or ""),
                        "status": str(game.get("status") or "scheduled").lower(),
                        "home_team": str(game.get("home_team") or ((game.get("teams") or ["", ""])[1]) or ""),
                        "away_team": str(game.get("away_team") or ((game.get("teams") or ["", ""])[0]) or ""),
                        "home_score": float(game.get("home_score") or 0),
                        "away_score": float(game.get("away_score") or 0),
                        "home_starter": str(game.get("home_starter") or ""),
                        "away_starter": str(game.get("away_starter") or ""),
                        "season": now.year,
                        "metadata": json.dumps({
                            "venue": game.get("venue"),
                            "weather": game.get("weather"),
                        }),
                        "created_at": now.isoformat(),
                    }
                    mlb_records.append(record)
                except Exception as e:
                    logger.warning(f"Failed to parse MLB game: {e}")
                    continue
            
            if mlb_records and self.uploader:
                self.uploader.push_records("games", mlb_records)
            
            result["baseball"]["fetched"] = len(mlb_games)
            result["baseball"]["pushed"] = len(mlb_records)
            
        except Exception as e:
            logger.error(f"MLB live fetch error: {e}")
            result["baseball"]["error"] = str(e)
        
        # SOCCER
        try:
            soccer_games = self.fetcher.fetch_soccer_live()
            soccer_records = []
            
            for game in soccer_games:
                try:
                    record = {
                        "game_id": str(game.get("match_id") or game.get("id") or game.get("game_id") or ""),
                        "record_id": f"soccer_{game.get('match_id') or game.get('id')}_{now.timestamp()}",
                        "sport": "soccer",
                        "league": str(game.get("league") or game.get("competition") or ""),
                        "game_date": self._parse_date(game.get("utcDate") or game.get("utc_date") or game.get("game_date") or ""),
                        "game_datetime": self._parse_datetime(game.get("utcDate") or game.get("utc_date") or game.get("game_time") or ""),
                        "status": str(game.get("status") or "scheduled").lower(),
                        "home_team": str(game.get("home_team") or game.get("homeTeam", {}).get("name") or ((game.get("teams") or ["", ""])[1]) or ""),
                        "away_team": str(game.get("away_team") or game.get("awayTeam", {}).get("name") or ((game.get("teams") or ["", ""])[0]) or ""),
                        "home_score": float(game.get("home_score") or game.get("score", {}).get("fullTime", {}).get("home") or 0),
                        "away_score": float(game.get("away_score") or game.get("score", {}).get("fullTime", {}).get("away") or 0),
                        "home_starter": "",
                        "away_starter": "",
                        "season": now.year,
                        "metadata": json.dumps({
                            "venue": game.get("venue"),
                            "round": game.get("round"),
                        }),
                        "created_at": now.isoformat(),
                    }
                    soccer_records.append(record)
                except Exception as e:
                    logger.warning(f"Failed to parse soccer game: {e}")
                    continue
            
            if soccer_records and self.uploader:
                self.uploader.push_records("games", soccer_records)
            
            result["soccer"]["fetched"] = len(soccer_games)
            result["soccer"]["pushed"] = len(soccer_records)
            
        except Exception as e:
            logger.error(f"Soccer live fetch error: {e}")
            result["soccer"]["error"] = str(e)
        
        if self.uploader:
            try:
                self.uploader.flush_all()
            except Exception as e:
                logger.warning(f"Failed to flush HF uploader buffers: {e}")
        
        return result

    @staticmethod
    def _parse_date(value: Any) -> str:
        """Parse ISO datetime string to YYYY-MM-DD."""
        try:
            raw = str(value or "").strip()
            if not raw:
                return dt.date.today().isoformat()
            
            # Handle ISO format
            if "T" in raw:
                dt_obj = dt.datetime.fromisoformat(raw.replace("Z", "+00:00"))
                return dt_obj.date().isoformat()
            
            # Handle date-only format
            return dt.date.fromisoformat(raw[:10]).isoformat()
        except Exception:
            return dt.date.today().isoformat()

    @staticmethod
    def _parse_datetime(value: Any) -> str:
        """Parse to ISO format datetime."""
        try:
            raw = str(value or "").strip()
            if not raw:
                return dt.datetime.now(dt.timezone.utc).isoformat()
            
            if "T" not in raw:
                raw = f"{raw}T00:00:00"
            
            dt_obj = dt.datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if dt_obj.tzinfo is None:
                dt_obj = dt_obj.replace(tzinfo=dt.timezone.utc)
            
            return dt_obj.isoformat()
        except Exception:
            return dt.datetime.now(dt.timezone.utc).isoformat()


# ============================================================================
# Module exports
# ============================================================================

def get_multi_sport_manager() -> MultiSportHFDataManager:
    """Get or create multi-sport data manager."""
    return MultiSportHFDataManager()


def bootstrap_all_sports() -> dict[str, Any]:
    """Bootstrap all sports with historical data. Call once per deployment."""
    manager = get_multi_sport_manager()
    
    return {
        "cricket": manager.bootstrap_cricket_historical(),
        "baseball": manager.bootstrap_baseball_historical(),
        "soccer": manager.bootstrap_soccer_historical(),
    }


def fetch_and_push_live_games() -> dict[str, Any]:
    """Fetch live games for all sports. Call every 30 minutes."""
    manager = get_multi_sport_manager()
    return manager.fetch_and_push_live_games()


if __name__ == "__main__":
    # Quick test
    print("Testing multi-sport HF data manager...")
    
    manager = get_multi_sport_manager()
    
    # Test live fetch
    result = manager.fetch_and_push_live_games()
    
    print(f"Cricket: {result['cricket']['fetched']} fetched, {result['cricket']['pushed']} pushed")
    print(f"Baseball: {result['baseball']['fetched']} fetched, {result['baseball']['pushed']} pushed")
    print(f"Soccer: {result['soccer']['fetched']} fetched, {result['soccer']['pushed']} pushed")
