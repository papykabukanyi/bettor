"""
Historical data ingestion for news and injuries.
Uses free sources:
  - NewsAPI /everything (if key allows)
  - Google News RSS fallback (free)
  - MLB Stats API transactions (injury-related)
  - ESPN injuries snapshot (free)
"""

import datetime


def _date_str(d: datetime.date) -> str:
    return d.strftime("%Y-%m-%d")


def get_mlb_team_names() -> list[str]:
    try:
        import statsapi as mlbstatsapi
        data = mlbstatsapi.get("teams", {"sportId": 1}) or {}
        teams = data.get("teams", []) if isinstance(data, dict) else []
        names = [t.get("name") for t in teams if t.get("name")]
        return names or _FALLBACK_TEAMS
    except Exception:
        return _FALLBACK_TEAMS


_FALLBACK_TEAMS = [
    "Arizona Diamondbacks", "Atlanta Braves", "Baltimore Orioles",
    "Boston Red Sox", "Chicago Cubs", "Chicago White Sox",
    "Cincinnati Reds", "Cleveland Guardians", "Colorado Rockies",
    "Detroit Tigers", "Houston Astros", "Kansas City Royals",
    "Los Angeles Angels", "Los Angeles Dodgers", "Miami Marlins",
    "Milwaukee Brewers", "Minnesota Twins", "New York Mets",
    "New York Yankees", "Oakland Athletics", "Philadelphia Phillies",
    "Pittsburgh Pirates", "San Diego Padres", "San Francisco Giants",
    "Seattle Mariners", "St. Louis Cardinals", "Tampa Bay Rays",
    "Texas Rangers", "Toronto Blue Jays", "Washington Nationals",
]


def backfill_news(days_back: int = 30, end_date: datetime.date | None = None) -> int:
    from data.sentiment import fetch_news_history
    end = end_date or datetime.date.today()
    start = end - datetime.timedelta(days=days_back)

    saved = 0
    for team in get_mlb_team_names():
        rows = fetch_news_history(team, _date_str(start), _date_str(end), entity_type="team")
        saved += len(rows)
    return saved


def backfill_injuries(days_back: int = 30, end_date: datetime.date | None = None) -> int:
    from data.injury_fetcher import fetch_injury_history
    from data.db import save_injuries

    end = end_date or datetime.date.today()
    start = end - datetime.timedelta(days=days_back)

    injuries = fetch_injury_history(_date_str(start), _date_str(end))
    if injuries:
        save_injuries("mlb", injuries, keep_history=True)
    return len(injuries)


def backfill_game_results(days_back: int = 30,
                          end_date: datetime.date | None = None) -> int:
    """
    Fetch completed MLB games (with scores) from the MLB Stats API and
    persist them to the `games` table so they can be used as real W/L
    training labels instead of synthetic season-run comparisons.

    Returns the number of completed games saved.
    """
    import requests as _req
    from data.db import upsert_game

    end = end_date or datetime.date.today()
    start = end - datetime.timedelta(days=days_back)

    # Collect day-by-day to avoid huge single request
    saved = 0
    current = start
    while current <= end:
        date_str = current.strftime("%Y-%m-%d")
        try:
            resp = _req.get(
                "https://statsapi.mlb.com/api/v1/schedule",
                params={
                    "sportId": 1,
                    "date": date_str,
                    "hydrate": "linescore",
                    "gameType": "R",
                },
                timeout=20,
            )
            resp.raise_for_status()
            data = resp.json()
            for date_entry in data.get("dates", []):
                season = int(date_entry.get("date", date_str)[:4])
                for game in date_entry.get("games", []):
                    status = (
                        game.get("status", {}).get("detailedState", "")
                        or game.get("status", {}).get("abstractGameState", "")
                    )
                    if status not in (
                        "Final", "Game Over", "Completed Early", "Completed"
                    ):
                        continue
                    teams  = game.get("teams", {})
                    home   = teams.get("home", {})
                    away   = teams.get("away", {})
                    h_name = (home.get("team") or {}).get("name", "")
                    a_name = (away.get("team") or {}).get("name", "")
                    h_score = home.get("score")
                    a_score = away.get("score")
                    if not h_name or not a_name or h_score is None or a_score is None:
                        continue
                    upsert_game(
                        sport="mlb", league="MLB",
                        home_team=h_name, away_team=a_name,
                        game_date=date_str,
                        home_score=int(h_score),
                        away_score=int(a_score),
                        status=status,
                        season=season,
                        external_id=game.get("gamePk"),
                    )
                    saved += 1
        except Exception as e:
            print(f"[history_ingest] game_results error for {date_str}: {e}")
        current += datetime.timedelta(days=1)

    print(f"[history_ingest] Saved {saved} completed games to DB")
    return saved
