# Golf Multi-Source Daily Signals

This project now has a dedicated golf source layer and daily feature context.

## Sources wired in

- DataGolf API bundle: strokes-gained and ranking context when configured.
- PGA statdata endpoint: official round-level stats when configured.
- ESPN Golf API fallback: leaderboard event context for daily cards.
- GolfAPI.io endpoint: course metadata when configured.
- Kaggle historical exports: local CSV reference rows (2010-2025 datasets).
- The Odds API: live outrights and matchup odds already used by the all-sports snapshot.

## Core model features produced

- `sg_total`
- `sg_approach`
- `sg_putting` (recent window)
- `course_fit`
- `course_type`
- `recent_form`
- `driving_distance`
- `cut_streak`
- `owgr_rank`
- `weather`

Additional context includes `fatigue_days`, `event_history_count`, and `course_history_count`.

## Today/Tomorrow card behavior

Golf now follows the all-sports today/tomorrow card path with individual-event matchup parsing
for event labels and market outcomes, so cards are created for near-date golf events with
player context when available.

## Config keys

- `GOLF_DATA_CACHE_TTL_SEC`
- `GOLF_REFERENCE_YEARS`
- `GOLF_DATAGOLF_API_BASE`
- `GOLF_DATAGOLF_API_KEY`
- `GOLF_PGA_STATDATA_BASE`
- `GOLF_GOLFAPI_BASE`
- `GOLF_GOLFAPI_KEY`
- `GOLF_KAGGLE_DATA_DIR`
