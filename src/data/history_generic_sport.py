from __future__ import annotations

import os

from data.history_boxscore_parsers import (
    fetch_espn_scoreboard_events,
    fetch_espn_summary_player_rows,
    normalize_espn_event_to_game_row,
)
from data.history_common import (
    build_injury_rows_from_history,
    build_player_rows_from_season_stats,
    iter_lookback_days,
)


def collect_sport_history_from_espn(
    *,
    sport_tag: str,
    espn_paths: list[str],
    league_fallback: str,
    days_back: int = 120,
) -> dict:
    """Generic collector for Kalshi sport families using ESPN boxscores.

    For each day + path, it pulls scoreboard events and then per-event summary
    boxscore player stats into per-game training_player_history rows.
    """
    from data.db import get_injury_history, get_player_season_stats

    game_rows = []
    player_rows = []
    seen_game_keys: set[str] = set()
    log_every_days = max(1, int(os.getenv("HISTORY_PROGRESS_LOG_EVERY_DAYS", "1") or "1"))
    lookback_days = iter_lookback_days(days_back)

    for day_idx, d in enumerate(lookback_days, 1):
        day_events = 0
        day_games_before = len(game_rows)
        day_players_before = len(player_rows)
        for path in espn_paths:
            events = fetch_espn_scoreboard_events(path, d) or []
            day_events += len(events)
            for ev in events:
                game_row = normalize_espn_event_to_game_row(
                    ev,
                    sport_tag=sport_tag,
                    league_fallback=league_fallback,
                    source="espn_scoreboard",
                )
                if not game_row:
                    continue
                gk = str(game_row.get("game_key") or "")
                if gk in seen_game_keys:
                    continue
                seen_game_keys.add(gk)
                game_rows.append(game_row)

                event_id = str(ev.get("id") or gk)
                game_date = str(game_row.get("game_date") or "")
                player_rows.extend(
                    fetch_espn_summary_player_rows(
                        sport_path=path,
                        event_id=event_id,
                        sport_tag=sport_tag,
                        game_key=gk,
                        game_date=game_date,
                        source="espn_summary",
                    )
                )

        if day_idx == 1 or day_idx == len(lookback_days) or (day_idx % log_every_days == 0):
            print(
                f"[history][{sport_tag}] day {day_idx}/{len(lookback_days)} {d.isoformat()} "
                f"events={day_events} games+{len(game_rows)-day_games_before} "
                f"players+{len(player_rows)-day_players_before}"
            )

    # Keep season-level fallback rows too, but per-game boxscore rows will now exist.
    player_rows.extend(
        build_player_rows_from_season_stats(
            sport=sport_tag,
            stats=get_player_season_stats(sport_tag) or [],
            source="player_season_stats",
        )
    )

    injury_rows = build_injury_rows_from_history(
        sport_tag,
        get_injury_history(sport_tag, days_back=days_back) or [],
        source_fallback="injury_reports",
    )

    return {
        "sport": sport_tag,
        "game_rows": game_rows,
        "player_rows": player_rows,
        "injury_rows": injury_rows,
    }
