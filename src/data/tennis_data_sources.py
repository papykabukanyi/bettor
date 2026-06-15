from __future__ import annotations

import csv
import datetime
import glob
import io
import os
import re
import time
from collections import defaultdict, deque
from typing import Any

import requests

from config import (
    TENNIS_DATA_CACHE_TTL_SEC,
    TENNIS_JEFF_SACKMANN_DIR,
    TENNIS_SACKMANN_END_YEAR,
    TENNIS_SACKMANN_START_YEAR,
    TENNIS_SLAM_POINTBYP_PBP_DIR,
    TENNIS_TENNIS_DATA_CO_UK_DIR,
)

_DEFAULT_TIMEOUT = 12
_cache: dict[str, tuple[Any, float]] = {}


def _cached(key: str, ttl: int, fn, *args, **kwargs):
    now = time.time()
    found = _cache.get(key)
    if found and (now - found[1]) < ttl:
        return found[0]
    value = fn(*args, **kwargs)
    _cache[key] = (value, now)
    return value


def _as_float(value: Any) -> float | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        return float(value)
    except Exception:
        return None


def _as_int(value: Any) -> int | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        return int(float(value))
    except Exception:
        return None


def _safe_get(url: str, *, headers: dict[str, str] | None = None, params: dict[str, Any] | None = None, timeout: int = _DEFAULT_TIMEOUT) -> Any:
    try:
        r = requests.get(url, headers=headers or {}, params=params or {}, timeout=timeout)
        if r.status_code != 200:
            return None
        ctype = str(r.headers.get("Content-Type") or "").lower()
        if "application/json" in ctype:
            return r.json()
        return r.text
    except Exception:
        return None


def _norm(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip().lower()


def _parse_date(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    if raw.isdigit() and len(raw) == 8:
        try:
            return datetime.datetime.strptime(raw, "%Y%m%d").date().isoformat()
        except Exception:
            pass
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%a, %b %d, %Y"):
        try:
            return datetime.datetime.strptime(raw, fmt).date().isoformat()
        except Exception:
            continue
    try:
        dt = datetime.datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return dt.date().isoformat()
    except Exception:
        return raw[:10]


def _read_local_csvs(base_dir: str, patterns: list[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not base_dir or not os.path.isdir(base_dir):
        return rows
    for pattern in patterns:
        for path in sorted(glob.glob(os.path.join(base_dir, pattern))):
            try:
                with open(path, encoding="utf-8-sig", newline="") as fh:
                    reader = csv.DictReader(fh)
                    rows.extend([dict(row) for row in reader])
            except Exception:
                continue
    return rows


def _remote_sackmann_rows(tour: str, start_year: int, end_year: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    repo = "JeffSackmann/tennis_atp" if tour == "atp" else "JeffSackmann/tennis_wta"
    prefix = "atp_matches" if tour == "atp" else "wta_matches"
    for year in range(start_year, end_year + 1):
        url = f"https://raw.githubusercontent.com/{repo}/master/{prefix}_{year}.csv"
        text = _safe_get(url)
        if not isinstance(text, str) or not text:
            continue
        try:
            reader = csv.DictReader(io.StringIO(text))
            rows.extend([dict(row) for row in reader])
        except Exception:
            continue
    return rows


def _canonical_player_row(raw: dict[str, Any], *, source: str, tour: str) -> dict[str, Any] | None:
    if not isinstance(raw, dict):
        return None
    winner = str(raw.get("winner_name") or raw.get("winner") or raw.get("player1") or raw.get("home_player") or "").strip()
    loser = str(raw.get("loser_name") or raw.get("loser") or raw.get("player2") or raw.get("away_player") or "").strip()
    if not winner or not loser:
        return None

    game_date = _parse_date(raw.get("tourney_date") or raw.get("date") or raw.get("game_date") or raw.get("event_date"))
    surface = str(raw.get("surface") or raw.get("court_surface") or raw.get("surface_type") or "").strip().lower()
    round_name = str(raw.get("round") or raw.get("stage") or "").strip().lower()
    tourney = str(raw.get("tourney_name") or raw.get("tournament") or raw.get("event_name") or raw.get("league") or tour).strip()
    winner_rank = _as_int(raw.get("winner_rank") or raw.get("rank_w") or raw.get("player1_rank"))
    loser_rank = _as_int(raw.get("loser_rank") or raw.get("rank_l") or raw.get("player2_rank"))

    game_key = str(
        raw.get("game_key")
        or raw.get("match_id")
        or raw.get("match_key")
        or f"{tour}:{game_date}:{winner}@{loser}:{surface or 'any'}"
    )

    return {
        "sport": "tennis",
        "league": f"Tennis {tour.upper()}" if tour.upper() in {"ATP", "WTA"} else str(raw.get("league") or "Tennis"),
        "season": _as_int(raw.get("season") or raw.get("year") or (game_date[:4] if game_date[:4].isdigit() else None)),
        "game_date": game_date[:10],
        "game_key": game_key,
        "home_team": winner,
        "away_team": loser,
        "home_score": 1,
        "away_score": 0,
        "status": str(raw.get("status") or "Final"),
        "source": source,
        "surface": surface,
        "round": round_name,
        "tourney_name": tourney,
        "winner_name": winner,
        "loser_name": loser,
        "winner_rank": winner_rank,
        "loser_rank": loser_rank,
        "winner_aces": _as_float(raw.get("w_ace") or raw.get("winner_aces") or raw.get("aces_w")),
        "loser_aces": _as_float(raw.get("l_ace") or raw.get("loser_aces") or raw.get("aces_l")),
        "winner_double_faults": _as_float(raw.get("w_df") or raw.get("winner_double_faults")),
        "loser_double_faults": _as_float(raw.get("l_df") or raw.get("loser_double_faults")),
        "winner_first_serve_in": _as_float(raw.get("w_1stIn") or raw.get("winner_first_serve_in")),
        "winner_first_serve_won": _as_float(raw.get("w_1stWon") or raw.get("winner_first_serve_won")),
        "winner_second_serve_won": _as_float(raw.get("w_2ndWon") or raw.get("winner_second_serve_won")),
        "winner_bp_saved": _as_float(raw.get("w_bpSaved") or raw.get("winner_bp_saved")),
        "winner_bp_faced": _as_float(raw.get("w_bpFaced") or raw.get("winner_bp_faced")),
        "loser_first_serve_in": _as_float(raw.get("l_1stIn") or raw.get("loser_first_serve_in")),
        "loser_first_serve_won": _as_float(raw.get("l_1stWon") or raw.get("loser_first_serve_won")),
        "loser_second_serve_won": _as_float(raw.get("l_2ndWon") or raw.get("loser_second_serve_won")),
        "loser_bp_saved": _as_float(raw.get("l_bpSaved") or raw.get("loser_bp_saved")),
        "loser_bp_faced": _as_float(raw.get("l_bpFaced") or raw.get("loser_bp_faced")),
        "surface": surface,
        "rank_diff": (loser_rank - winner_rank) if (winner_rank is not None and loser_rank is not None) else None,
        "raw_json": raw,
    }


def _iter_reference_rows(raw_rows: list[dict[str, Any]], *, source: str, tour: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for raw in raw_rows:
        row = _canonical_player_row(raw, source=source, tour=tour)
        if row:
            out.append(row)
    out.sort(key=lambda r: (str(r.get("game_date") or ""), str(r.get("game_key") or "")))
    return out


def fetch_jeff_sackmann_atp_history(
    data_dir: str | None = None,
    *,
    start_year: int | None = None,
    end_year: int | None = None,
) -> list[dict[str, Any]]:
    """Historical ATP/WTA match rows from Jeff Sackmann CSV exports or GitHub raw data."""
    base_dir = data_dir or TENNIS_JEFF_SACKMANN_DIR
    ttl = max(60, int(TENNIS_DATA_CACHE_TTL_SEC or 300))

    def _pull() -> list[dict[str, Any]]:
        if base_dir and os.path.isdir(base_dir):
            return _read_local_csvs(base_dir, ["*atp_matches*.csv", "*wta_matches*.csv", "*matches*.csv"])
        start = int(start_year or TENNIS_SACKMANN_START_YEAR or (datetime.date.today().year - 8))
        end = int(end_year or TENNIS_SACKMANN_END_YEAR or datetime.date.today().year)
        rows = _remote_sackmann_rows("atp", start, end)
        rows.extend(_remote_sackmann_rows("wta", start, end))
        return rows

    return _cached(f"tennis_sackmann::{base_dir or 'remote'}", ttl, _pull) or []


def fetch_tennis_data_co_uk_history(data_dir: str | None = None) -> list[dict[str, Any]]:
    """Historical match rows from tennis-data.co.uk CSV exports when available locally."""
    base_dir = data_dir or TENNIS_TENNIS_DATA_CO_UK_DIR
    ttl = max(60, int(TENNIS_DATA_CACHE_TTL_SEC or 300))

    def _pull() -> list[dict[str, Any]]:
        if base_dir and os.path.isdir(base_dir):
            return _read_local_csvs(base_dir, ["*.csv"])
        return []

    return _cached(f"tennis_data_co_uk::{base_dir or 'none'}", ttl, _pull) or []


def fetch_slam_pointbypoint_history(data_dir: str | None = None) -> list[dict[str, Any]]:
    """Point-by-point or rally-level tennis archives when exported locally."""
    base_dir = data_dir or TENNIS_SLAM_POINTBYP_PBP_DIR
    ttl = max(60, int(TENNIS_DATA_CACHE_TTL_SEC or 300))

    def _pull() -> list[dict[str, Any]]:
        if base_dir and os.path.isdir(base_dir):
            return _read_local_csvs(base_dir, ["*.csv", "*.tsv"])
        return []

    return _cached(f"tennis_slam_pbp::{base_dir or 'none'}", ttl, _pull) or []


def fetch_espn_tennis_live_bundle(game_date: datetime.date | str | None = None) -> dict[str, Any]:
    """Wrapper around ESPN tennis scoreboard paths used by the rest of the app."""
    from data.history_boxscore_parsers import fetch_espn_scoreboard_events, fetch_espn_summary_player_rows

    if isinstance(game_date, datetime.date):
        date_obj = game_date
    else:
        try:
            date_obj = datetime.date.fromisoformat(str(game_date or ""))
        except Exception:
            date_obj = datetime.date.today()

    bundle: dict[str, Any] = {"games": [], "player_rows": []}
    for path, league in (("tennis/atp", "ATP"), ("tennis/wta", "WTA")):
        for ev in fetch_espn_scoreboard_events(path, date_obj) or []:
            bundle["games"].append({"league": league, "event": ev, "source": "espn_scoreboard"})
            bundle["player_rows"].extend(
                fetch_espn_summary_player_rows(
                    sport_path=path,
                    event_id=str(ev.get("id") or ""),
                    sport_tag="tennis",
                    game_key=str(ev.get("id") or ""),
                    game_date=date_obj.isoformat(),
                    source="espn_summary",
                )
            )
    return bundle


def build_tennis_prediction_context(
    *,
    home_player: str,
    away_player: str,
    surface: str = "",
    match_date: str = "",
    reference_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Compute tennis-specific model context from historical rows."""
    refs = reference_rows or load_tennis_reference_rows()
    home = _norm(home_player)
    away = _norm(away_player)
    surface_norm = _norm(surface)

    home_history = [r for r in refs if _norm(r.get("winner_name") or r.get("home_team") or r.get("player_name")) == home or _norm(r.get("loser_name") or r.get("away_team")) == home]
    away_history = [r for r in refs if _norm(r.get("winner_name") or r.get("home_team") or r.get("player_name")) == away or _norm(r.get("loser_name") or r.get("away_team")) == away]
    pair_history = [r for r in refs if {home, away} == {_norm(r.get("winner_name") or r.get("home_team") or r.get("player_name")), _norm(r.get("loser_name") or r.get("away_team"))}]

    def _win_rate(rows: list[dict[str, Any]], player: str, surface_filter: str = "") -> tuple[float, int]:
        matches = 0
        wins = 0
        for r in rows:
            if surface_filter and _norm(r.get("surface") or r.get("court_surface")) != surface_filter:
                continue
            winner = _norm(r.get("winner_name") or r.get("home_team") or r.get("player_name"))
            loser = _norm(r.get("loser_name") or r.get("away_team"))
            if player not in {winner, loser}:
                continue
            matches += 1
            if winner == player:
                wins += 1
        return (wins / matches) if matches else 0.5, matches

    def _recent_form(rows: list[dict[str, Any]], player: str, window: int = 5) -> float:
        seq: list[int] = []
        for r in rows:
            winner = _norm(r.get("winner_name") or r.get("home_team") or r.get("player_name"))
            loser = _norm(r.get("loser_name") or r.get("away_team"))
            if player not in {winner, loser}:
                continue
            seq.append(1 if winner == player else 0)
        last = seq[-window:]
        return (sum(last) / len(last)) if last else 0.5

    def _serve_avgs(rows: list[dict[str, Any]], player: str) -> dict[str, float]:
        aces: list[float] = []
        first_pct: list[float] = []
        bp_saved: list[float] = []
        for r in rows:
            winner = _norm(r.get("winner_name") or r.get("home_team") or r.get("player_name"))
            loser = _norm(r.get("loser_name") or r.get("away_team"))
            is_winner = player == winner
            is_loser = player == loser
            if not (is_winner or is_loser):
                continue
            prefix = "winner" if is_winner else "loser"
            aces.append(_as_float(r.get(f"{prefix}_aces") or r.get("aces")) or 0.0)
            first_in = _as_float(r.get(f"{prefix}_first_serve_in"))
            first_won = _as_float(r.get(f"{prefix}_first_serve_won"))
            if first_in and first_in > 0 and first_won is not None:
                first_pct.append(max(0.0, min(1.0, first_won / first_in)))
            saved = _as_float(r.get(f"{prefix}_bp_saved"))
            faced = _as_float(r.get(f"{prefix}_bp_faced"))
            if faced and faced > 0 and saved is not None:
                bp_saved.append(max(0.0, min(1.0, saved / faced)))
        return {
            "aces": round(sum(aces) / len(aces), 3) if aces else 0.0,
            "first_serve_pct": round(sum(first_pct) / len(first_pct), 3) if first_pct else 0.0,
            "break_points_saved_pct": round(sum(bp_saved) / len(bp_saved), 3) if bp_saved else 0.0,
        }

    home_surface_rate, home_surface_matches = _win_rate(home_history, home, surface_norm)
    away_surface_rate, away_surface_matches = _win_rate(away_history, away, surface_norm)
    home_recent = _recent_form(home_history, home)
    away_recent = _recent_form(away_history, away)
    home_serve = _serve_avgs(home_history, home)
    away_serve = _serve_avgs(away_history, away)

    home_last_date = None
    away_last_date = None
    for r in reversed(refs):
        gd = str(r.get("game_date") or "").strip()[:10]
        if not home_last_date and _norm(r.get("winner_name") or r.get("home_team") or r.get("player_name")) == home or _norm(r.get("loser_name") or r.get("away_team")) == home:
            home_last_date = gd
        if not away_last_date and _norm(r.get("winner_name") or r.get("home_team") or r.get("player_name")) == away or _norm(r.get("loser_name") or r.get("away_team")) == away:
            away_last_date = gd
        if home_last_date and away_last_date:
            break

    def _days_since(last_date: str | None) -> int | None:
        if not last_date:
            return None
        try:
            dt = datetime.date.fromisoformat(last_date)
            md = datetime.date.fromisoformat(match_date[:10]) if match_date else datetime.date.today()
            return max(0, (md - dt).days)
        except Exception:
            return None

    h2h_wins_home = 0
    h2h_wins_away = 0
    for r in pair_history:
        winner = _norm(r.get("winner_name") or r.get("home_team") or r.get("player_name"))
        if winner == home:
            h2h_wins_home += 1
        elif winner == away:
            h2h_wins_away += 1

    home_rank = next((
        _as_int(r.get("winner_rank")) if _norm(r.get("winner_name") or r.get("home_team") or r.get("player_name")) == home else _as_int(r.get("loser_rank"))
        for r in reversed(refs)
        if home in {_norm(r.get("winner_name") or r.get("home_team") or r.get("player_name")), _norm(r.get("loser_name") or r.get("away_team"))}
    ), None)
    away_rank = next((
        _as_int(r.get("winner_rank")) if _norm(r.get("winner_name") or r.get("home_team") or r.get("player_name")) == away else _as_int(r.get("loser_rank"))
        for r in reversed(refs)
        if away in {_norm(r.get("winner_name") or r.get("home_team") or r.get("player_name")), _norm(r.get("loser_name") or r.get("away_team"))}
    ), None)

    # ── Elo-based win probability (surface-specific) ─────────────────────────
    elos = compute_player_elos(refs)
    _home_elo_key = next((k for k in elos if _norm(k) == home), None)
    _away_elo_key = next((k for k in elos if _norm(k) == away), None)

    surf_key = surface_norm if surface_norm in _ELO_SURFACES else "hard"
    home_elo_surf = elos.get(_home_elo_key or "", {}).get(surf_key, _DEFAULT_ELO) if _home_elo_key else _DEFAULT_ELO
    away_elo_surf = elos.get(_away_elo_key or "", {}).get(surf_key, _DEFAULT_ELO) if _away_elo_key else _DEFAULT_ELO
    home_elo_overall = elos.get(_home_elo_key or "", {}).get("overall", _DEFAULT_ELO) if _home_elo_key else _DEFAULT_ELO
    away_elo_overall = elos.get(_away_elo_key or "", {}).get("overall", _DEFAULT_ELO) if _away_elo_key else _DEFAULT_ELO
    home_elo_matches = int(elos.get(_home_elo_key or "", {}).get("matches", 0)) if _home_elo_key else 0
    away_elo_matches = int(elos.get(_away_elo_key or "", {}).get("matches", 0)) if _away_elo_key else 0

    elo_prob_home = elo_win_probability(home_elo_surf, away_elo_surf)

    # Blend Elo (data-quality-weighted) with surface win rate for robustness.
    # Weight Elo more heavily as we accumulate more historical matches.
    data_confidence = min(1.0, (home_elo_matches + away_elo_matches) / 60.0)
    elo_weight = 0.35 + 0.45 * data_confidence  # 0.35 → 0.80 as matches accumulate
    win_prob_home = round(elo_weight * elo_prob_home + (1.0 - elo_weight) * home_surface_rate, 4)
    win_prob_away = round(1.0 - win_prob_home, 4)

    return {
        "surface": surface_norm or surface,
        "home_player": home_player,
        "away_player": away_player,
        "surface_win_rate_home": round(home_surface_rate, 4),
        "surface_win_rate_away": round(away_surface_rate, 4),
        "surface_matches_home": home_surface_matches,
        "surface_matches_away": away_surface_matches,
        "recent_form_home": round(home_recent, 4),
        "recent_form_away": round(away_recent, 4),
        "serve_stats_home": home_serve,
        "serve_stats_away": away_serve,
        "h2h_record_surface_home": h2h_wins_home,
        "h2h_record_surface_away": h2h_wins_away,
        "rank_home": home_rank,
        "rank_away": away_rank,
        "rank_diff": (away_rank - home_rank) if (home_rank is not None and away_rank is not None) else None,
        "fatigue_home_days": _days_since(home_last_date),
        "fatigue_away_days": _days_since(away_last_date),
        "recent_form_gap": round(home_recent - away_recent, 4),
        # Elo fields
        "elo_home": home_elo_surf,
        "elo_away": away_elo_surf,
        "elo_home_overall": home_elo_overall,
        "elo_away_overall": away_elo_overall,
        "elo_matches_home": home_elo_matches,
        "elo_matches_away": away_elo_matches,
        "elo_prob_home": elo_prob_home,
        "elo_prob_away": round(1.0 - elo_prob_home, 4),
        "win_prob_home": win_prob_home,
        "win_prob_away": win_prob_away,
    }


def load_tennis_reference_rows(*, limit_years: int | None = None) -> list[dict[str, Any]]:
    """Return cached raw tennis match rows across supported historical sources."""
    years = int(limit_years or os.getenv("TENNIS_REFERENCE_YEARS", "8") or "8")
    cache_key = f"tennis_reference::{years}"

    def _pull() -> list[dict[str, Any]]:
        raw_rows: list[dict[str, Any]] = []
        raw_rows.extend(fetch_jeff_sackmann_atp_history(start_year=datetime.date.today().year - years, end_year=datetime.date.today().year))
        raw_rows.extend(fetch_tennis_data_co_uk_history())
        raw_rows.extend(fetch_slam_pointbypoint_history())
        # Convert any already-normalized rows to the shared player-history shape.
        out = _iter_reference_rows(raw_rows, source="tennis_history", tour="tennis")
        return out

    return _cached(cache_key, max(120, int(TENNIS_DATA_CACHE_TTL_SEC or 300)), _pull) or []


def build_tennis_history_rows(raw_rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """Convert raw tennis match rows into training_game_history and training_player_history rows."""
    refs = _iter_reference_rows(raw_rows or [], source="tennis_history", tour="tennis")
    game_rows: list[dict[str, Any]] = []
    player_rows: list[dict[str, Any]] = []
    seen_games: set[str] = set()
    recent_state: dict[str, deque[int]] = defaultdict(lambda: deque(maxlen=5))
    surface_totals: dict[tuple[str, str], list[int]] = defaultdict(lambda: [0, 0])
    pair_surface_wins: dict[tuple[str, str, str], list[int]] = defaultdict(lambda: [0, 0])
    last_played: dict[str, str] = {}

    for row in refs:
        winner = str(row.get("winner_name") or row.get("home_team") or "").strip()
        loser = str(row.get("loser_name") or row.get("away_team") or "").strip()
        if not winner or not loser:
            continue
        game_date = str(row.get("game_date") or "").strip()[:10]
        surface = str(row.get("surface") or "").strip().lower()
        game_key = str(row.get("game_key") or f"tennis:{game_date}:{winner}@{loser}:{surface or 'any'}")
        if game_key in seen_games:
            continue
        seen_games.add(game_key)

        winner_rank = _as_int(row.get("winner_rank"))
        loser_rank = _as_int(row.get("loser_rank"))
        rank_diff = (loser_rank - winner_rank) if (winner_rank is not None and loser_rank is not None) else None

        def _player_context(player: str, opponent: str, won: bool) -> dict[str, Any]:
            surface_key = (player, surface or "any")
            totals = surface_totals[surface_key]
            wins, games = totals[0], totals[1]
            recent = list(recent_state[player])
            recent_form = (sum(recent) / len(recent)) if recent else 0.5
            pair_key = tuple(sorted([player, opponent]) + [surface or "any"])  # type: ignore[list-item]
            h2h = pair_surface_wins[pair_key]
            last_date = last_played.get(player)
            fatigue = None
            if last_date and game_date:
                try:
                    fatigue = max(0, (datetime.date.fromisoformat(game_date) - datetime.date.fromisoformat(last_date)).days)
                except Exception:
                    fatigue = None

            serve_prefix = "winner" if won else "loser"
            first_in = row.get(f"{serve_prefix}_first_serve_in")
            first_won = row.get(f"{serve_prefix}_first_serve_won")
            bp_saved = row.get(f"{serve_prefix}_bp_saved")
            bp_faced = row.get(f"{serve_prefix}_bp_faced")
            serve_stats = {
                "aces": row.get(f"{serve_prefix}_aces"),
                "double_faults": row.get(f"{serve_prefix}_double_faults"),
                "first_serve_pct": (float(first_won) / float(first_in)) if first_in and first_won is not None and float(first_in) > 0 else None,
                "break_points_saved_pct": (float(bp_saved) / float(bp_faced)) if bp_faced and bp_saved is not None and float(bp_faced) > 0 else None,
            }
            return {
                "surface_win_rate": round((wins / games), 4) if games else 0.5,
                "surface_matches": games,
                "recent_form": round(recent_form, 4),
                "h2h_record_surface": h2h[0] if won else h2h[1],
                "rank": winner_rank if won else loser_rank,
                "rank_diff": rank_diff if won else (-rank_diff if rank_diff is not None else None),
                "fatigue_days": fatigue,
                "serve_stats": serve_stats,
            }

        winner_ctx = _player_context(winner, loser, True)
        loser_ctx = _player_context(loser, winner, False)

        game_rows.append(
            {
                **row,
                "sport": "tennis",
                "league": str(row.get("league") or "Tennis"),
                "home_team": winner,
                "away_team": loser,
                "home_score": 1,
                "away_score": 0,
                "status": str(row.get("status") or "Final"),
                "feature_snapshot": {
                    "winner": winner_ctx,
                    "loser": loser_ctx,
                },
            }
        )

        for player_name, opponent_name, ctx, won in ((winner, loser, winner_ctx, True), (loser, winner, loser_ctx, False)):
            for stat_name, stat_value in {
                "surface_win_rate": ctx["surface_win_rate"],
                "surface_matches": ctx["surface_matches"],
                "recent_form": ctx["recent_form"],
                "h2h_record_surface": ctx["h2h_record_surface"],
                "rank": ctx["rank"],
                "rank_diff": ctx["rank_diff"],
                "fatigue_days": ctx["fatigue_days"],
                "serve_first_serve_pct": ctx["serve_stats"].get("first_serve_pct"),
                "serve_break_points_saved_pct": ctx["serve_stats"].get("break_points_saved_pct"),
                "serve_aces": ctx["serve_stats"].get("aces"),
                "serve_double_faults": ctx["serve_stats"].get("double_faults"),
            }.items():
                if stat_value is None:
                    continue
                player_rows.append(
                    {
                        "sport": "tennis",
                        "season": _as_int(row.get("season") or (game_date[:4] if game_date[:4].isdigit() else None)),
                        "game_date": game_date,
                        "game_key": game_key,
                        "player_name": player_name,
                        "team": opponent_name,
                        "stat_type": stat_name,
                        "stat_value": float(stat_value) if isinstance(stat_value, (int, float)) else stat_value,
                        "source": str(row.get("source") or "tennis_history"),
                        "raw_json": {
                            "game_key": game_key,
                            "surface": surface,
                            "winner": winner,
                            "loser": loser,
                            "player": player_name,
                            "opponent": opponent_name,
                            "won": won,
                            "feature_snapshot": ctx,
                            "match_row": row,
                        },
                    }
                )

        # Update state after features are captured.
        for player_name, won in ((winner, 1), (loser, 0)):
            surface_key = (player_name, surface or "any")
            surface_totals[surface_key][1] += 1
            surface_totals[surface_key][0] += int(won)
            recent_state[player_name].append(int(won))
            last_played[player_name] = game_date
        pair_key = tuple(sorted([winner, loser]) + [surface or "any"])  # type: ignore[list-item]
        pair_surface_wins[pair_key][0] += 1

    return {"game_rows": game_rows, "player_rows": player_rows}


# ── Surface-specific Elo rating system ─────────────────────────────────────
# Based on the well-established Elo method used by FiveThirtyEight for tennis.
# K-factor adapts by experience; surface-specific ratings capture court preferences.

_DEFAULT_ELO = 1500.0
_ELO_K_BASE = 32.0          # K-factor for < 30 career matches (high learning rate)
_ELO_K_EXPERIENCED = 24.0   # K-factor for 30+ matches (stable ratings)
_ELO_SURFACES = ("hard", "clay", "grass", "carpet")


def _elo_expected(rating_a: float, rating_b: float) -> float:
    return 1.0 / (1.0 + 10.0 ** ((rating_b - rating_a) / 400.0))


def elo_win_probability(elo_a: float, elo_b: float) -> float:
    """Win probability for player A given their Elo vs player B's Elo."""
    return round(_elo_expected(float(elo_a or _DEFAULT_ELO), float(elo_b or _DEFAULT_ELO)), 4)


def compute_player_elos(
    reference_rows: list[dict[str, Any]],
) -> dict[str, dict[str, float]]:
    """Compute surface-specific Elo ratings from historical match rows (chronological).

    Returns {player_name: {overall, hard, clay, grass, carpet, matches}}.
    """
    elos: dict[str, dict[str, float]] = {}
    match_counts: dict[str, dict[str, int]] = {}

    def _get(player: str, surf: str) -> float:
        return elos.setdefault(player, {}).get(surf, _DEFAULT_ELO)

    def _set(player: str, surf: str, val: float) -> None:
        elos.setdefault(player, {})[surf] = round(val, 2)

    def _cnt(player: str, surf: str) -> int:
        mc = match_counts.setdefault(player, {})
        mc[surf] = mc.get(surf, 0) + 1
        return mc[surf]

    for row in reference_rows:
        winner = str(row.get("winner_name") or row.get("home_team") or "").strip()
        loser = str(row.get("loser_name") or row.get("away_team") or "").strip()
        if not winner or not loser:
            continue

        surf = str(row.get("surface") or "").strip().lower()
        if surf not in _ELO_SURFACES:
            surf = "hard"

        # Surface-specific Elo update
        ew = _get(winner, surf)
        el = _get(loser, surf)
        exp_w = _elo_expected(ew, el)
        wc = _cnt(winner, surf)
        lc = _cnt(loser, surf)
        k_w = _ELO_K_EXPERIENCED if wc > 30 else _ELO_K_BASE
        k_l = _ELO_K_EXPERIENCED if lc > 30 else _ELO_K_BASE
        _set(winner, surf, ew + k_w * (1.0 - exp_w))
        _set(loser, surf, el + k_l * (0.0 - (1.0 - exp_w)))

        # Overall cross-surface Elo
        ew_ov = _get(winner, "overall")
        el_ov = _get(loser, "overall")
        exp_w_ov = _elo_expected(ew_ov, el_ov)
        wc_ov = _cnt(winner, "overall")
        lc_ov = _cnt(loser, "overall")
        k_w_ov = _ELO_K_EXPERIENCED if wc_ov > 30 else _ELO_K_BASE
        k_l_ov = _ELO_K_EXPERIENCED if lc_ov > 30 else _ELO_K_BASE
        _set(winner, "overall", ew_ov + k_w_ov * (1.0 - exp_w_ov))
        _set(loser, "overall", el_ov + k_l_ov * (0.0 - (1.0 - exp_w_ov)))

    for player, surf_elos in elos.items():
        surf_elos["matches"] = match_counts.get(player, {}).get("overall", 0)

    return elos