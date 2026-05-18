# Kalshi Multi-Sport Data Sources

This project now supports broader Kalshi sport discovery and matching (including tennis and boxing). To generate better predictions per sport, wire providers by sport family using the table below.

## Provider Strategy

- Primary odds feed: The Odds API (`src/data/odds_fetcher.py`)
- Primary schedule/live fallback: ESPN public scoreboards + TheSportsDB (`src/dashboard.py`)
- Kalshi market universe + matching: `src/data/kalshi.py`
- Optional deeper stats by sport: specialized providers listed below

## Recommended Data Sources By Sport

| Sport family | Existing in repo | Recommended APIs for deeper modeling |
|---|---|---|
| Baseball (MLB/KBO/NPB) | MLB Statcast/pybaseball + Odds + Kalshi | MLB Stats API, Baseball Savant, Sportradar Baseball |
| Basketball (NBA/WNBA/NCAAB) | Odds + ESPN fallback + Kalshi | balldontlie, NBA stats endpoints, SportsData/Sportradar |
| Hockey (NHL) | Odds + ESPN fallback + Kalshi | NHL public stats API, SportsData/Sportradar |
| Football (NFL/NCAAF) | Odds + ESPN fallback + Kalshi | nflverse data, SportsData/Sportradar |
| Soccer | football-data + Odds + ESPN/TSDB fallback + Kalshi | Understat, FBref-derived feeds, Sportradar Soccer |
| Tennis (ATP/WTA) | Odds + ESPN/TSDB fallback + Kalshi classification | API-Tennis, Tennis Abstract data, Sportradar Tennis |
| Boxing | Odds + ESPN/TSDB fallback + Kalshi classification | BoxRec data sources, Fight-level APIs, Sportradar Combat |
| MMA/UFC | Odds + ESPN/TSDB fallback + Kalshi classification | UFCStats, mma data APIs, Sportradar Combat |
| Golf | Odds alias + TSDB fallback + Kalshi classification | PGA Tour stats feeds, DataGolf, Sportradar Golf |
| Motorsports (F1/NASCAR) | Odds alias + TSDB fallback + Kalshi classification | Ergast (F1), NASCAR APIs/data, Sportradar Motorsports |
| Cricket | Odds alias + TSDB fallback + Kalshi classification | CricAPI, Cricsheet, Sportradar Cricket |

## Integration Priority

1. Keep odds + schedule ingestion broad for all sports (already in place).
2. Add sport-specific feature builders (`src/data/*_fetcher.py`) for each new sport.
3. Add model adapters in `src/models/` for each sport family.
4. Feed standardized predictions into Ready Bets payload (`kind`, `sport`, `bet_type`, `line`, `direction`, `home_team`, `away_team`, `player_name`).

## Minimum Fields For Kalshi Matching

Every prediction row should include:

- `sport`
- `kind` (`single`, `team_prop`, `player_prop`, `combo`)
- `bet_type`
- `label` and/or `pick`
- `line` (if applicable)
- `direction` (`OVER`/`UNDER` when applicable)
- `home_team`, `away_team`, `game`/`game_key`
- `game_date` and ideally `scheduled_start`
- `player_name` and `prop_type` for player props

## Notes

- Not every Kalshi series is always open; discovery is dynamic and cached.
- The Odds API sport keys vary by availability/plan; aliases are normalized in `SPORT_MAP`.
- Use optional providers only when API keys are present, so free-mode fallback remains operational.
