# WNBA Data Sources

This project now supports WNBA-specific data ingestion from:

- wnba_stats_api: live scoreboard, box score, play-by-play
- basketball-reference: historical WNBA games (1997 -> today)
- kaggle_datasets: local pre-cleaned CSV files for ML training

## Files

- src/data/wnba_data_sources.py
- src/data/history_wnba.py
- src/data/multi_sport_history.py
- src/config.py

## Environment Variables

Add to .env:

- WNBA_DATA_CACHE_TTL_SEC=300
- WNBA_STATS_API_BASE=https://stats.wnba.com/stats
- WNBA_STATS_API_TIMEOUT_SEC=10
- WNBA_BREF_START_YEAR=1997
- WNBA_KAGGLE_DATA_DIR=C:/path/to/kaggle/wnba_csvs

## Usage

Python usage example:

```python
from data.multi_sport_history import ingest_multi_sport_history
summary = ingest_multi_sport_history(days_back=365, sports=["wnba"])
print(summary)
```

## Notes

- Kaggle integration expects CSVs already downloaded locally.
- WNBA live endpoints can rate-limit or block aggressive clients; requests are cached and use browser-like headers.
- basketball-reference extraction uses pandas.read_html; if unavailable at runtime, that source is skipped gracefully.
