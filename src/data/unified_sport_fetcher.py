"""
Unified Sport Fetcher - Cricket + MLB + Soccer
===============================================
Fetches data for ALL sports simultaneously without interference.

Sports supported:
- Cricket: RapidAPI Cricket Live Line (free)
- MLB: MLB official API (free) + Stats API
- Soccer: ESPN, API-Football free tier

Data flows directly to HF dataset. Each sport is independent.
No conflicts. All sports updated together every 30 minutes.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
from typing import Any

import requests

logger = logging.getLogger(__name__)

# ============================================================================
# API Keys & Configuration
# ============================================================================

CRICKET_RAPIDAPI_KEY = os.getenv("CRICKET_RAPIDAPI_KEY", "")
CRICKET_RAPIDAPI_HOST = os.getenv("CRICKET_RAPIDAPI_HOST", "cricket-live-line1.p.rapidapi.com")
FOOTBALL_DATA_API_KEY = os.getenv("FOOTBALL_DATA_API_KEY", "")


class UnifiedSportFetcher:
    """Fetch live data for cricket, MLB, and soccer simultaneously."""

    def __init__(self):
        """Initialize fetcher for all sports."""
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        })

    # ========================================================================
    # CRICKET - RapidAPI Cricket Live Line (FREE)
    # ========================================================================

    def fetch_cricket_live(self) -> list[dict[str, Any]]:
        """
        Fetch live cricket matches from RapidAPI.
        
        API: https://rapidapi.com/apiservicesprovider/api/cricket-live-line1
        Key: CRICKET_RAPIDAPI_KEY
        Status: Free tier available (generous requests)
        """
        if not CRICKET_RAPIDAPI_KEY:
            logger.warning("CRICKET_RAPIDAPI_KEY not set")
            return []

        try:
            url = f"https://{CRICKET_RAPIDAPI_HOST}/cricket-live-line"
            headers = {
                "X-RapidAPI-Key": CRICKET_RAPIDAPI_KEY,
                "X-RapidAPI-Host": CRICKET_RAPIDAPI_HOST,
            }

            resp = self.session.get(url, headers=headers, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            matches = []
            items = data if isinstance(data, list) else data.get("data", [])

            for item in items:
                try:
                    team1 = str(item.get("team1", "") or "")
                    team2 = str(item.get("team2", "") or "")
                    start_iso = str(item.get("dateTimeGMT", "") or item.get("match_time", "") or "")
                    match = {
                        "sport": "cricket",
                        "match_id": str(item.get("match_id", "") or ""),
                        "teams": [team1, team2],
                        "home_team": team1,
                        "away_team": team2,
                        "team1": team1,
                        "team2": team2,
                        "format": str(item.get("format", "t20") or "t20").lower(),
                        "status": str(item.get("status", "live") or "live").lower(),
                        "score_team1": str(item.get("score1", "") or ""),
                        "score_team2": str(item.get("score2", "") or ""),
                        "start_date": start_iso,
                        "scheduled_start": start_iso,
                        "venue": str(item.get("venue", "") or ""),
                        "fetched_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                        "source": "rapidapi_cricket_live_line",
                    }
                    if match["match_id"] and match["teams"][0] and match["teams"][1]:
                        matches.append(match)
                except Exception as e:
                    logger.debug(f"Failed to parse cricket match: {e}")
                    continue

            logger.info(f"[CRICKET] Fetched {len(matches)} live matches from RapidAPI")
            return matches

        except requests.exceptions.RequestException as e:
            logger.error(f"[CRICKET] Failed to fetch from RapidAPI: {e}")
            return []
        except Exception as e:
            logger.error(f"[CRICKET] Error parsing RapidAPI: {e}")
            return []

    # ========================================================================
    # MLB - MLB Official Stats API (FREE)
    # ========================================================================

    def fetch_mlb_live(self) -> list[dict[str, Any]]:
        """
        Fetch live MLB games from official Stats API (completely FREE).
        
        API: https://statsapi.mlb.com/api/v1/
        Status: No authentication required
        """
        try:
            today = dt.date.today()
            tomorrow = today + dt.timedelta(days=1)
            url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&startDate={today.isoformat()}&endDate={tomorrow.isoformat()}"
            
            resp = self.session.get(url, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            games = []
            for date_entry in data.get("dates", []):
                for game in date_entry.get("games", []):
                    try:
                        away = str(game.get("teams", {}).get("away", {}).get("team", {}).get("name", "") or "")
                        home = str(game.get("teams", {}).get("home", {}).get("team", {}).get("name", "") or "")
                        game_dt = str(game.get("gameDate", "") or "")
                        game_info = {
                            "sport": "mlb",
                            "game_id": str(game.get("gamePk", "") or ""),
                            "teams": [away, home],
                            "away_team": away,
                            "home_team": home,
                            "status": str(game.get("status", {}).get("abstractGameState", "").lower() or "scheduled"),
                            "venue": str(game.get("venue", {}).get("name", "") or ""),
                            "game_datetime": game_dt,
                            "game_date": game_dt[:10] if game_dt else today.isoformat(),
                            "away_score": int(game.get("teams", {}).get("away", {}).get("score", 0) or 0),
                            "home_score": int(game.get("teams", {}).get("home", {}).get("score", 0) or 0),
                            "fetched_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                            "source": "mlb_statsapi",
                        }
                        if game_info["game_id"] and game_info["away_team"] and game_info["home_team"]:
                            games.append(game_info)
                    except Exception as e:
                        logger.debug(f"Failed to parse MLB game: {e}")
                        continue

            logger.info(f"[MLB] Fetched {len(games)} live games from Stats API")
            return games

        except requests.exceptions.RequestException as e:
            logger.error(f"[MLB] Failed to fetch from Stats API: {e}")
            return []
        except Exception as e:
            logger.error(f"[MLB] Error parsing Stats API: {e}")
            return []

    # ========================================================================
    # SOCCER - Football Data API (FREE tier)
    # ========================================================================

    def fetch_soccer_live(self) -> list[dict[str, Any]]:
        """
        Fetch live soccer matches from football-data.org.
        
        API: https://www.football-data.org/
        Free tier: 10 requests per minute, live data included
        """
        if not FOOTBALL_DATA_API_KEY:
            logger.warning("FOOTBALL_DATA_API_KEY not set")
            return []

        try:
            today = dt.date.today()
            tomorrow = today + dt.timedelta(days=1)
            url = f"https://api.football-data.org/v4/matches?dateFrom={today.isoformat()}&dateTo={tomorrow.isoformat()}"
            headers = {"X-Auth-Token": FOOTBALL_DATA_API_KEY}

            resp = self.session.get(url, headers=headers, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            matches = []
            for match in data.get("matches", []):
                try:
                    status = str(match.get("status", "").upper() or "SCHEDULED")
                    if status in {"FINISHED", "POSTPONED", "CANCELLED", "SUSPENDED"}:
                        continue
                    away = str(match.get("awayTeam", {}).get("name", "") or "")
                    home = str(match.get("homeTeam", {}).get("name", "") or "")
                    utc_date = str(match.get("utcDate", "") or "")
                    full_time = match.get("score", {}).get("fullTime", {}) if isinstance(match.get("score"), dict) else {}
                    match_info = {
                        "sport": "soccer",
                        "match_id": str(match.get("id", "") or ""),
                        "teams": [away, home],
                        "away_team": away,
                        "home_team": home,
                        "status": status.lower(),
                        "league": str(match.get("competition", {}).get("name", "") or ""),
                        "away_score": int(full_time.get("away") or 0),
                        "home_score": int(full_time.get("home") or 0),
                        "utc_date": utc_date,
                        "utcDate": utc_date,
                        "venue": str(match.get("venue", "") or ""),
                        "fetched_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                        "source": "football_data_api",
                    }
                    if match_info["match_id"] and match_info["away_team"] and match_info["home_team"]:
                        matches.append(match_info)
                except Exception as e:
                    logger.debug(f"Failed to parse soccer match: {e}")
                    continue

            logger.info(f"[SOCCER] Fetched {len(matches)} live matches from football-data.org")
            return matches

        except requests.exceptions.RequestException as e:
            logger.error(f"[SOCCER] Failed to fetch from football-data.org: {e}")
            return []
        except Exception as e:
            logger.error(f"[SOCCER] Error parsing football-data.org: {e}")
            return []

    # ========================================================================
    # UNIFIED: Fetch All Sports Together
    # ========================================================================

    def fetch_all_sports(self) -> dict[str, Any]:
        """
        Fetch live data for ALL sports simultaneously.
        
        Returns:
            {
                "cricket": [{...}, {...}],
                "mlb": [{...}, {...}],
                "soccer": [{...}, {...}],
                "fetched_at": "2026-07-06T16:15:00Z",
                "summary": {
                    "cricket": 5,
                    "mlb": 3,
                    "soccer": 12,
                    "total": 20,
                }
            }
        """
        logger.info("=== Fetching All Sports Data (Cricket + MLB + Soccer) ===")
        
        result = {
            "cricket": self.fetch_cricket_live(),
            "mlb": self.fetch_mlb_live(),
            "soccer": self.fetch_soccer_live(),
            "fetched_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "summary": {
                "cricket": 0,
                "mlb": 0,
                "soccer": 0,
                "total": 0,
            }
        }

        # Calculate summary
        result["summary"]["cricket"] = len(result["cricket"])
        result["summary"]["mlb"] = len(result["mlb"])
        result["summary"]["soccer"] = len(result["soccer"])
        result["summary"]["total"] = sum([
            result["summary"]["cricket"],
            result["summary"]["mlb"],
            result["summary"]["soccer"],
        ])

        logger.info(f"[SUMMARY] Cricket: {result['summary']['cricket']} | "
                   f"MLB: {result['summary']['mlb']} | "
                   f"Soccer: {result['summary']['soccer']} | "
                   f"TOTAL: {result['summary']['total']}")

        return result

    def fetch_upcoming_all_sports(self, days_ahead: int = 3) -> dict[str, Any]:
        """
        Fetch upcoming matches for all sports.
        
        Args:
            days_ahead: Look this many days into the future
            
        Returns:
            {
                "cricket": [upcoming matches],
                "mlb": [upcoming games],
                "soccer": [upcoming matches],
            }
        """
        logger.info(f"Fetching upcoming matches for next {days_ahead} days (all sports)...")
        
        try:
            result = {
                "cricket": self._fetch_cricket_upcoming(days_ahead),
                "mlb": self._fetch_mlb_upcoming(days_ahead),
                "soccer": self._fetch_soccer_upcoming(days_ahead),
                "fetched_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            }
            
            total = sum([len(v) for k, v in result.items() if k != "fetched_at"])
            logger.info(f"Found {total} upcoming matches across all sports")
            return result
        except Exception as e:
            logger.error(f"Failed to fetch upcoming: {e}")
            return {
                "cricket": [],
                "mlb": [],
                "soccer": [],
                "fetched_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            }

    def _fetch_cricket_upcoming(self, days_ahead: int) -> list[dict[str, Any]]:
        """Fetch upcoming cricket matches (next N days)."""
        # For MVP: return empty (requires separate API or scraping)
        # In production: query for scheduled matches
        return []

    def _fetch_mlb_upcoming(self, days_ahead: int) -> list[dict[str, Any]]:
        """Fetch upcoming MLB games (next N days)."""
        try:
            today = dt.date.today()
            end_date = today + dt.timedelta(days=days_ahead)
            
            url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&startDate={today}&endDate={end_date}"
            resp = self.session.get(url, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            upcoming = []
            for game in data.get("games", []):
                if game.get("status", {}).get("abstractGameState", "").lower() == "scheduled":
                    upcoming.append({
                        "sport": "mlb",
                        "game_id": str(game.get("gamePk", "") or ""),
                        "teams": [
                            str(game.get("teams", {}).get("away", {}).get("team", {}).get("name", "") or ""),
                            str(game.get("teams", {}).get("home", {}).get("team", {}).get("name", "") or ""),
                        ],
                        "game_datetime": str(game.get("gameDateTime", "") or ""),
                    })
            
            return upcoming
        except Exception as e:
            logger.debug(f"Failed to fetch upcoming MLB: {e}")
            return []

    def _fetch_soccer_upcoming(self, days_ahead: int) -> list[dict[str, Any]]:
        """Fetch upcoming soccer matches (next N days)."""
        if not FOOTBALL_DATA_API_KEY:
            return []

        try:
            url = "https://api.football-data.org/v4/matches?status=SCHEDULED"
            headers = {"X-Auth-Token": FOOTBALL_DATA_API_KEY}

            resp = self.session.get(url, headers=headers, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            upcoming = []
            now = dt.datetime.now(dt.timezone.utc)
            cutoff = now + dt.timedelta(days=days_ahead)

            for match in data.get("matches", []):
                try:
                    utc_date = dt.datetime.fromisoformat(
                        str(match.get("utcDate", "")).replace("Z", "+00:00")
                    )
                    if utc_date <= cutoff:
                        upcoming.append({
                            "sport": "soccer",
                            "match_id": str(match.get("id", "") or ""),
                            "teams": [
                                str(match.get("awayTeam", {}).get("name", "") or ""),
                                str(match.get("homeTeam", {}).get("name", "") or ""),
                            ],
                            "utc_date": str(match.get("utcDate", "") or ""),
                            "league": str(match.get("competition", {}).get("name", "") or ""),
                        })
                except Exception:
                    continue
            
            return upcoming
        except Exception as e:
            logger.debug(f"Failed to fetch upcoming soccer: {e}")
            return []


# Singleton
_fetcher: UnifiedSportFetcher | None = None


def get_unified_fetcher() -> UnifiedSportFetcher:
    """Get or create singleton fetcher."""
    global _fetcher
    if _fetcher is None:
        _fetcher = UnifiedSportFetcher()
    return _fetcher


if __name__ == "__main__":
    fetcher = get_unified_fetcher()

    print("\n=== UNIFIED SPORT FETCHER ===")
    print("Fetching live data for Cricket + MLB + Soccer...\n")

    result = fetcher.fetch_all_sports()

    print(f"\nCricket: {result['summary']['cricket']} matches")
    for match in result["cricket"][:3]:
        print(f"  • {match['teams'][0]} vs {match['teams'][1]} ({match['format']})")

    print(f"\nMLB: {result['summary']['mlb']} games")
    for game in result["mlb"][:3]:
        print(f"  • {game['teams'][1]} vs {game['teams'][0]} - {game['status']}")

    print(f"\nSoccer: {result['summary']['soccer']} matches")
    for match in result["soccer"][:3]:
        print(f"  • {match['teams'][0]} vs {match['teams'][1]} ({match['league']})")

    print(f"\n[TOTAL] {result['summary']['total']} matches/games across all sports")
    print(f"[TIME] {result['fetched_at']}")
