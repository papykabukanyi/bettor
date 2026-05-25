# Soccer Multi-Source Data Integration

This project now supports a unified soccer data context for prediction enrichment.

## Integrated Sources

- football-data.co.uk: historical odds CSV for backtesting
- StatsBomb Open Data: event-level analytics (xG, passing, pressure)
- BSD API: live scores and multi-book odds (provider endpoint via env)
- Understat: current season xG/xGA team metrics
- Transfermarkt: injury history (via proxy endpoint)
- API-Football: fixtures, lineups, H2H, odds, and predictions

## Files Added/Updated

- src/data/soccer_data_sources.py
- src/models/soccer_predictor.py
- src/config.py

## Environment Variables

Set these in your .env file as needed:

- SOCCER_MULTI_SOURCE_ENABLED=1
- SOCCER_UNDERSTAT_LEAGUE=EPL
- SOCCER_DS_CACHE_TTL_SEC=900
- API_FOOTBALL_KEY=...
- API_FOOTBALL_HOST=api-football-v1.p.rapidapi.com
- API_FOOTBALL_BASE=https://api-football-v1.p.rapidapi.com/v3
- BSD_API_BASE=https://your-bsd-provider.example.com
- BSD_API_KEY=...
- TRANSFERMARKT_PROXY_URL=https://your-transfermarkt-proxy.example.com/injuries

## How It Is Used

The predictor now calls build_soccer_prediction_context(home_team, away_team) and applies a bounded probability shift based on:

- Understat xG and xGA deltas
- Transfermarkt injury pressure (home vs away)
- API-Football lineup confirmation and prediction disagreement signal

The shift is capped to +/-0.05 to avoid unstable jumps when external feeds are noisy.

## Notes

- If a provider key is missing, the integration degrades gracefully.
- Requests are cached by SOCCER_DS_CACHE_TTL_SEC.
- No new Python dependency was required (requests/csv/json only).
