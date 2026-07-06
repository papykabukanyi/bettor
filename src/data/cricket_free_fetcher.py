"""
Cricket Data Fetcher - FREE Sources Only
========================================
Fetches live cricket data from completely free sources:

1. ESPNCricinfo (scraping) - all formats, detailed stats
2. Cricbuzz (scraping) - live scores, commentary
3. Cricsheet (JSON dumps) - 22k+ historical matches
4. RapidAPI Cricket Live Line - free tier (fallback)

All free. No subscriptions required.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
import re
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

RAPID_API_KEY = os.getenv("CRICKET_RAPIDAPI_KEY", "")
RAPID_API_HOST = os.getenv("CRICKET_RAPIDAPI_HOST", "cricket-live-line1.p.rapidapi.com")


class CricketDataFetcher:
    """Fetch cricket data from free sources."""

    def __init__(self):
        """Initialize fetcher with free data source configs."""
        self.rapidapi_key = RAPID_API_KEY
        self.rapidapi_host = RAPID_API_HOST
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        })

    # =========================================================================
    # FREE Source #1: ESPNCricinfo (Scraping)
    # =========================================================================

    def fetch_live_matches_espn(self) -> list[dict[str, Any]]:
        """
        Scrape live cricket matches from ESPNCricinfo.
        
        Returns:
            [
                {
                    "match_id": "...",
                    "teams": ["IND", "PAK"],
                    "format": "odi",  # test, odi, t20, ipl
                    "status": "live",  # live, upcoming, completed
                    "score_home": "245/8",
                    "score_away": "pending",
                    "venue": "Arun Jaitley Stadium",
                    "toss": "IND won toss, chose to bat",
                    "fetched_at": "2026-07-06T16:15:00Z",
                }
            ]
        """
        try:
            url = "https://www.espncricinfo.com/"
            resp = self.session.get(url, timeout=10)
            resp.raise_for_status()

            soup = BeautifulSoup(resp.content, "html.parser")
            matches = []

            # Find live match divs
            match_divs = soup.find_all("div", class_=re.compile("match-card|match-item"))
            
            for div in match_divs:
                try:
                    match_data = self._parse_espn_match_card(div)
                    if match_data:
                        matches.append(match_data)
                except Exception as e:
                    logger.debug(f"Failed to parse match card: {e}")
                    continue

            logger.info(f"Fetched {len(matches)} live matches from ESPNCricinfo")
            return matches

        except Exception as e:
            logger.error(f"Failed to fetch from ESPNCricinfo: {e}")
            return []

    def _parse_espn_match_card(self, div) -> dict[str, Any] | None:
        """Parse a single match card from ESPNCricinfo HTML."""
        try:
            # Extract teams
            team_elem = div.find("span", class_=re.compile("team"))
            if not team_elem:
                return None
            
            teams_text = team_elem.get_text(strip=True)
            teams = [t.strip() for t in teams_text.split("vs")][:2]
            if len(teams) != 2:
                return None

            # Extract format
            format_text = div.get_text(strip=True).lower()
            if "test" in format_text:
                fmt = "test"
            elif "t20" in format_text.lower():
                fmt = "t20"
            elif "ipl" in format_text.lower():
                fmt = "ipl"
            else:
                fmt = "odi"

            # Extract status
            status = "live" if "live" in format_text else "upcoming"

            return {
                "match_id": hashlib.sha1(teams_text.encode()).hexdigest()[:16],
                "teams": teams,
                "format": fmt,
                "status": status,
                "source": "espncricinfo",
                "fetched_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            }
        except Exception:
            return None

    # =========================================================================
    # FREE Source #2: Cricbuzz (Scraping)
    # =========================================================================

    def fetch_live_matches_cricbuzz(self) -> list[dict[str, Any]]:
        """
        Scrape live cricket matches from Cricbuzz (completely free).
        
        Returns: List of match dictionaries
        """
        try:
            url = "https://www.cricbuzz.com/cricket"
            resp = self.session.get(url, timeout=10)
            resp.raise_for_status()

            soup = BeautifulSoup(resp.content, "html.parser")
            matches = []

            # Find match cards
            match_items = soup.find_all("div", class_=re.compile("matchScoreContainer|cbMatch"))
            
            for item in match_items:
                try:
                    match_data = self._parse_cricbuzz_match(item)
                    if match_data:
                        matches.append(match_data)
                except Exception as e:
                    logger.debug(f"Failed to parse Cricbuzz match: {e}")
                    continue

            logger.info(f"Fetched {len(matches)} live matches from Cricbuzz")
            return matches

        except Exception as e:
            logger.error(f"Failed to fetch from Cricbuzz: {e}")
            return []

    def _parse_cricbuzz_match(self, item) -> dict[str, Any] | None:
        """Parse a single match from Cricbuzz HTML."""
        try:
            match_text = item.get_text(strip=True)
            
            # Extract teams (typically "TEAM1 vs TEAM2")
            if " vs " in match_text:
                teams = match_text.split(" vs ")[:2]
                teams = [t.strip() for t in teams]
            else:
                return None

            return {
                "match_id": hashlib.sha1(match_text.encode()).hexdigest()[:16],
                "teams": teams,
                "format": "t20" if "t20" in match_text.lower() else "odi",
                "status": "live",
                "source": "cricbuzz",
                "fetched_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            }
        except Exception:
            return None

    # =========================================================================
    # FREE Source #3: RapidAPI Cricket Live Line (Your Key - Free Tier)
    # =========================================================================

    def fetch_live_matches_rapidapi(self) -> list[dict[str, Any]]:
        """
        Fetch live cricket matches using RapidAPI Cricket Live Line.
        
        Your key: b65cec1d35msh240f423a84de0abp19075ejsn7f2de12fbc00
        API: https://rapidapi.com/apiservicesprovider/api/cricket-live-line1
        
        Free tier: Plenty of requests for live scores.
        """
        if not self.rapidapi_key:
            logger.warning("CRICKET_RAPIDAPI_KEY not set, skipping RapidAPI fetch")
            return []

        try:
            url = f"https://{self.rapidapi_host}/cricket-live-line"
            
            headers = {
                "X-RapidAPI-Key": self.rapidapi_key,
                "X-RapidAPI-Host": self.rapidapi_host,
            }

            resp = self.session.get(url, headers=headers, timeout=10)
            resp.raise_for_status()

            data = resp.json()
            matches = []

            # RapidAPI returns list of matches
            if isinstance(data, list):
                items = data
            elif isinstance(data, dict) and "data" in data:
                items = data.get("data", [])
            else:
                items = []

            for item in items:
                try:
                    match_data = self._parse_rapidapi_match(item)
                    if match_data:
                        matches.append(match_data)
                except Exception as e:
                    logger.debug(f"Failed to parse RapidAPI match: {e}")
                    continue

            logger.info(f"Fetched {len(matches)} live matches from RapidAPI")
            return matches

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch from RapidAPI: {e}")
            return []
        except Exception as e:
            logger.error(f"Error parsing RapidAPI response: {e}")
            return []

    def _parse_rapidapi_match(self, item: dict) -> dict[str, Any] | None:
        """Parse RapidAPI match response."""
        try:
            team1 = str(item.get("team1", "") or "").strip()
            team2 = str(item.get("team2", "") or "").strip()
            
            if not team1 or not team2:
                return None

            return {
                "match_id": str(item.get("match_id", "") or ""),
                "teams": [team1, team2],
                "format": str(item.get("format", "t20") or "t20").lower(),
                "status": str(item.get("status", "live") or "live").lower(),
                "score_team1": str(item.get("score1", "") or ""),
                "score_team2": str(item.get("score2", "") or ""),
                "venue": str(item.get("venue", "") or ""),
                "source": "rapidapi",
                "fetched_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            }
        except Exception:
            return None

    # =========================================================================
    # FREE Source #4: Cricsheet Historical Data (One-time Download)
    # =========================================================================

    def fetch_cricsheet_historical(self) -> list[dict[str, Any]]:
        """
        Fetch Cricsheet historical cricket data (completely free).
        
        Cricsheet provides 22,000+ historical matches in JSON format.
        Download once, store in HF dataset.
        
        URL: https://cricsheet.org/downloads/
        or: https://github.com/cricsheet/cricsheet-json
        
        Returns: Historical match data
        """
        logger.info("Cricsheet data should be downloaded manually from https://cricsheet.org/")
        logger.info("22,000+ matches available free - one-time download")
        logger.info("Store in HF dataset, use for training only")
        return []

    # =========================================================================
    # Main: Fetch from All FREE Sources
    # =========================================================================

    def fetch_all_live_matches(self) -> list[dict[str, Any]]:
        """
        Fetch live cricket matches from all free sources.
        
        Try in order:
        1. ESPNCricinfo (scraping)
        2. Cricbuzz (scraping)
        3. RapidAPI Cricket Live Line (your key)
        
        Returns deduplicated list of matches.
        """
        all_matches = []

        # Try RapidAPI first (most reliable)
        logger.info("Fetching from RapidAPI Cricket Live Line...")
        rapidapi_matches = self.fetch_live_matches_rapidapi()
        all_matches.extend(rapidapi_matches)

        # Fallback to scraping if RapidAPI returns nothing
        if not all_matches:
            logger.info("RapidAPI returned no matches, trying ESPNCricinfo...")
            espn_matches = self.fetch_live_matches_espn()
            all_matches.extend(espn_matches)

        if not all_matches:
            logger.info("ESPNCricinfo returned no matches, trying Cricbuzz...")
            cricbuzz_matches = self.fetch_live_matches_cricbuzz()
            all_matches.extend(cricbuzz_matches)

        # Deduplicate by match_id
        seen = set()
        unique_matches = []
        for match in all_matches:
            mid = match.get("match_id", "")
            if mid not in seen:
                seen.add(mid)
                unique_matches.append(match)

        logger.info(f"Total unique matches from all sources: {len(unique_matches)}")
        return unique_matches

    def fetch_upcoming_cricket_fixtures(self) -> list[dict[str, Any]]:
        """
        Fetch upcoming cricket fixtures (next 7 days) from free sources.
        
        Returns:
            [
                {
                    "match_id": "...",
                    "teams": ["IND", "AUS"],
                    "start_time": "2026-07-07T19:30:00Z",
                    "venue": "MCG",
                    "format": "odi",
                    "league": "ICC",
                    "status": "upcoming",
                }
            ]
        """
        logger.info("Fetching upcoming cricket fixtures from free sources...")
        
        # For MVP: combine live + upcoming from available sources
        # RapidAPI usually includes both live and upcoming
        fixtures = self.fetch_all_live_matches()
        
        # Filter to upcoming only
        now = dt.datetime.now(dt.timezone.utc)
        upcoming = []
        for match in fixtures:
            if match.get("status") in {"upcoming", "scheduled"}:
                upcoming.append(match)
        
        logger.info(f"Found {len(upcoming)} upcoming cricket matches")
        return upcoming


# Singleton instance
_fetcher: CricketDataFetcher | None = None


def get_cricket_fetcher() -> CricketDataFetcher:
    """Get or create singleton fetcher."""
    global _fetcher
    if _fetcher is None:
        _fetcher = CricketDataFetcher()
    return _fetcher


if __name__ == "__main__":
    import hashlib
    
    fetcher = get_cricket_fetcher()
    
    print("\n=== Fetching Live Cricket Matches (All FREE Sources) ===\n")
    
    matches = fetcher.fetch_all_live_matches()
    for match in matches:
        print(f"  {match['source'].upper()}: {match['teams'][0]} vs {match['teams'][1]} ({match['format'].upper()}) - {match['status']}")
    
    print(f"\nTotal: {len(matches)} matches from free sources")
    
    print("\n=== Available Free Cricket Data Sources ===")
    print("1. RapidAPI Cricket Live Line (Your Key) - Live scores + upcoming")
    print("2. ESPNCricinfo (Free scraping) - All formats, detailed stats")
    print("3. Cricbuzz (Free scraping) - Live scores, commentary")
    print("4. Cricsheet (Free historical) - 22k+ historical matches")
    print("\nNo subscriptions required!")
