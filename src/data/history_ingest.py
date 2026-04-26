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
