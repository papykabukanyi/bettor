# Tennis Multi-Source Data

This project now treats tennis as a first-class sport family with a layered source strategy.

## Historical Sources

- Jeff Sackmann ATP/WTA match archives are used as the main historical backbone when CSV exports or the public GitHub mirrors are available.
- tennis-data.co.uk CSV exports can be loaded from a local data directory for historical matches and odds-style backtests.
- Slam point-by-point exports can be loaded locally for rally and serve-pattern context.

## Live Sources

- API-Tennis compatible endpoints can provide live scores, draws, head-to-head context, rankings, and injuries when configured.
- ESPN tennis scoreboard/summary endpoints remain the fallback for live match context.

## Tennis Features

The tennis history builder stores match context needed for modeling and ranking:

- `surface`
- `rank_diff`
- `h2h_record_surface`
- `recent_form`
- `serve_stats`
- `fatigue_days`

These values are also preserved in `raw_json` for downstream model work and debugging.

## Configuration

Relevant environment variables are exposed in `src/config.py`:

- `TENNIS_DATA_CACHE_TTL_SEC`
- `TENNIS_REFERENCE_YEARS`
- `TENNIS_SACKMANN_START_YEAR`
- `TENNIS_SACKMANN_END_YEAR`
- `TENNIS_JEFF_SACKMANN_DIR`
- `TENNIS_TENNIS_DATA_CO_UK_DIR`
- `TENNIS_SLAM_POINTBYP_PBP_DIR`
- `TENNIS_API_BASE`
- `TENNIS_API_KEY`

The fallback behavior is intentionally permissive: if a source is unavailable, the collector keeps going with the sources that are present.