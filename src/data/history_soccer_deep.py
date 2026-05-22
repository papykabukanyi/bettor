from __future__ import annotations

import datetime
import os

from data.history_boxscore_parsers import (
    fetch_espn_scoreboard_events,
    fetch_espn_summary_player_rows,
    normalize_espn_event_to_game_row,
)
from data.history_common import (
    build_injury_rows_from_history,
    build_player_rows_from_season_stats,
)


def _score_from_match(match: dict, side: str) -> int | None:
    score = match.get("score") if isinstance(match.get("score"), dict) else {}
    full_time = score.get("fullTime") if isinstance(score.get("fullTime"), dict) else {}
    value = full_time.get(side)
    try:
        return int(value) if value is not None else None
    except Exception:
        return None


def collect_soccer_history(days_back: int = 180) -> dict:
    """Deep soccer historical collector using football-data range windows + injuries."""
    from data.db import get_injury_history, get_player_season_stats
    from data.soccer_fetcher import get_matches_range_all
    from data.sportsdata_fetcher import get_soccer_injuries

    today = datetime.date.today()
    start = today - datetime.timedelta(days=max(1, int(days_back)))

    matches = get_matches_range_all(start.isoformat(), today.isoformat()) or []
    game_rows = []
    for m in matches:
        if not isinstance(m, dict):
            continue
        home = str(m.get("home_team") or m.get("home") or "").strip()
        away = str(m.get("away_team") or m.get("away") or "").strip()
        game_date = str(m.get("date") or m.get("game_date") or "").strip()
        if not home or not away or not game_date:
            continue
        game_key = str(m.get("game_key") or m.get("match_id") or f"{away}@{home}#{game_date}")
        game_rows.append(
            {
                "sport": "soccer",
                "league": str(m.get("competition") or "soccer"),
                "season": int(game_date[:4]) if game_date[:4].isdigit() else None,
                "game_date": game_date[:10],
                "game_key": game_key,
                "home_team": home,
                "away_team": away,
                "home_score": _score_from_match(m, "home") if "score" in m else m.get("home_score"),
                "away_score": _score_from_match(m, "away") if "score" in m else m.get("away_score"),
                "status": str(m.get("status") or "Scheduled"),
                "source": "football-data",
                "raw_json": m,
            }
        )

    player_rows = build_player_rows_from_season_stats(
        sport="soccer",
        stats=get_player_season_stats("soccer") or [],
        source="player_season_stats",
    )

    # Add per-game soccer player rows from ESPN summaries where available.
    log_every_days = max(1, int(os.getenv("HISTORY_PROGRESS_LOG_EVERY_DAYS", "1") or "1"))
    espn_paths = [
        "soccer/eng.1",
        "soccer/esp.1",
        "soccer/ger.1",
        "soccer/ita.1",
        "soccer/fra.1",
        "soccer/usa.1",
        "soccer/uefa.champions",
    ]
    seen_game_keys = {str(g.get("game_key") or "") for g in game_rows if isinstance(g, dict)}
    total_days = max(1, int(days_back))
    for offset in range(total_days):
        day_events = 0
        day_games_before = len(game_rows)
        day_players_before = len(player_rows)
        day = today - datetime.timedelta(days=offset)
        for path in espn_paths:
            events = fetch_espn_scoreboard_events(path, day) or []
            day_events += len(events)
            for ev in events:
                g = normalize_espn_event_to_game_row(
                    ev,
                    sport_tag="soccer",
                    league_fallback="soccer",
                    source="espn_scoreboard",
                )
                if not g:
                    continue
                gk = str(g.get("game_key") or "")
                if gk and gk not in seen_game_keys:
                    seen_game_keys.add(gk)
                    game_rows.append(g)
                player_rows.extend(
                    fetch_espn_summary_player_rows(
                        sport_path=path,
                        event_id=str(ev.get("id") or gk),
                        sport_tag="soccer",
                        game_key=gk or str(ev.get("id") or ""),
                        game_date=str(g.get("game_date") or day.isoformat())[:10],
                        source="espn_summary",
                    )
                )
        day_idx = offset + 1
        if day_idx == 1 or day_idx == total_days or (day_idx % log_every_days == 0):
            print(
                f"[history][soccer] day {day_idx}/{total_days} {day.isoformat()} "
                f"events={day_events} games+{len(game_rows)-day_games_before} "
                f"players+{len(player_rows)-day_players_before}"
            )

    # Soccer injury timeline from SportsData.io competitions + existing DB history.
    injury_rows = build_injury_rows_from_history(
        "soccer",
        get_injury_history("soccer", days_back=days_back) or [],
        source_fallback="injury_reports",
    )
    try:
        for comp in (5, 12, 10, 11, 8, 6):  # EPL, Ligue 1, Bundesliga, Serie A, La Liga, MLS
            for inj in (get_soccer_injuries(comp) or []):
                player = str(inj.get("Name") or inj.get("ShortName") or "").strip()
                if not player:
                    continue
                injury_rows.append(
                    {
                        "sport": "soccer",
                        "injury_date": today.isoformat(),
                        "team": str(inj.get("Team") or "").strip(),
                        "player_name": player,
                        "status": str(inj.get("Status") or inj.get("InjuryStatus") or "").strip(),
                        "injury_type": str(inj.get("InjuryBodyPart") or "").strip(),
                        "detail": str(inj.get("InjuryDescription") or "").strip(),
                        "source": "sportsdata",
                        "raw_json": inj,
                    }
                )
    except Exception:
        pass

    return {
        "sport": "soccer",
        "game_rows": game_rows,
        "player_rows": player_rows,
        "injury_rows": injury_rows,
    }
