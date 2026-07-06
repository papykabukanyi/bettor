from __future__ import annotations

import csv
import datetime as dt
import json
import os
import zipfile
from pathlib import Path
from typing import Any

from data.history_generic_sport import collect_sport_history_from_espn


def _parse_iso_date(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%Y/%m/%d"):
        try:
            return dt.datetime.strptime(raw[:10], fmt).date().isoformat()
        except Exception:
            continue
    try:
        return dt.date.fromisoformat(raw[:10]).isoformat()
    except Exception:
        return ""


def _to_score(value: Any) -> float:
    raw = str(value or "").strip()
    if not raw:
        return 0.0
    if "/" in raw:
        raw = raw.split("/", 1)[0]
    try:
        return float(raw)
    except Exception:
        return 0.0


def _winner_scores(home_team: str, away_team: str, winner: str) -> tuple[float, float]:
    wn = str(winner or "").strip().lower()
    ht = str(home_team or "").strip().lower()
    at = str(away_team or "").strip().lower()
    if wn and wn == ht:
        return 1.0, 0.0
    if wn and wn == at:
        return 0.0, 1.0
    return 0.5, 0.5


def _load_cricsheet_rows(cricsheet_dir: str, *, start_date: dt.date, end_date: dt.date) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    base = Path(str(cricsheet_dir or "").strip())
    if not base.exists():
        return rows
    json_files: list[Path] = []
    if base.is_file() and base.suffix.lower() == ".zip":
        zip_paths = [base]
    elif base.is_dir():
        zip_paths = sorted(base.glob("*.zip"))
        json_files = sorted(base.rglob("*.json"))
    else:
        zip_paths = []

    def _append_match(payload: dict[str, Any], source_name: str) -> None:
        info = payload.get("info") if isinstance(payload.get("info"), dict) else {}
        dates = info.get("dates") if isinstance(info.get("dates"), list) else []
        game_date = _parse_iso_date(dates[0] if dates else info.get("date"))
        if not game_date:
            return
        try:
            gd = dt.date.fromisoformat(game_date)
        except Exception:
            return
        if gd < start_date or gd > end_date:
            return
        teams = info.get("teams") if isinstance(info.get("teams"), list) else []
        if len(teams) < 2:
            return
        home_team = str(teams[0] or "").strip()
        away_team = str(teams[1] or "").strip()
        if not home_team or not away_team:
            return
        match_type = str(info.get("match_type") or "").strip().upper()
        event = info.get("event") if isinstance(info.get("event"), dict) else {}
        league = str(event.get("name") or match_type or "Cricket").strip()
        outcome = info.get("outcome") if isinstance(info.get("outcome"), dict) else {}
        winner = str(outcome.get("winner") or "").strip()
        by = outcome.get("by") if isinstance(outcome.get("by"), dict) else {}
        margin = ""
        if isinstance(by, dict):
            margin_parts = []
            if by.get("runs") is not None:
                margin_parts.append(f"{by.get('runs')} runs")
            if by.get("wickets") is not None:
                margin_parts.append(f"{by.get('wickets')} wickets")
            margin = ", ".join(margin_parts)
        home_score, away_score = _winner_scores(home_team, away_team, winner)
        rows.append(
            {
                "sport": "cricket",
                "league": league[:80],
                "game_key": f"cricsheet:{source_name}",
                "game_date": game_date,
                "home_team": home_team[:120],
                "away_team": away_team[:120],
                "home_score": home_score,
                "away_score": away_score,
                "metadata": json.dumps(
                    {
                        "source": "cricsheet",
                        "match_type": match_type,
                        "winner": winner,
                        "margin": margin,
                        "venue": info.get("venue"),
                    },
                    ensure_ascii=True,
                ),
            }
        )

    for zip_path in zip_paths:
        try:
            with zipfile.ZipFile(zip_path, "r") as archive:
                for name in archive.namelist():
                    if not name.lower().endswith(".json"):
                        continue
                    try:
                        payload = json.loads(archive.read(name).decode("utf-8"))
                    except Exception:
                        continue
                    if isinstance(payload, dict):
                        _append_match(payload, name)
        except Exception:
            continue

    for file_path in json_files:
        try:
            payload = json.loads(file_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if isinstance(payload, dict):
            _append_match(payload, file_path.name)

    return rows


def _load_kaggle_rows(kaggle_dir: str, *, start_date: dt.date, end_date: dt.date) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    base = Path(str(kaggle_dir or "").strip())
    if not base.exists() or not base.is_dir():
        return rows
    for csv_path in sorted(base.rglob("*.csv")):
        try:
            with csv_path.open("r", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                field_map = {str(k or "").strip().lower(): k for k in (reader.fieldnames or [])}
                if not field_map:
                    continue
                home_key = field_map.get("team1") or field_map.get("home_team")
                away_key = field_map.get("team2") or field_map.get("away_team")
                date_key = field_map.get("date") or field_map.get("match_date")
                winner_key = field_map.get("winner")
                if not home_key or not away_key or not date_key:
                    continue
                league_key = field_map.get("tournament") or field_map.get("series") or field_map.get("league")
                for row in reader:
                    game_date = _parse_iso_date(row.get(date_key))
                    if not game_date:
                        continue
                    try:
                        gd = dt.date.fromisoformat(game_date)
                    except Exception:
                        continue
                    if gd < start_date or gd > end_date:
                        continue
                    home_team = str(row.get(home_key) or "").strip()
                    away_team = str(row.get(away_key) or "").strip()
                    if not home_team or not away_team:
                        continue
                    winner = str(row.get(winner_key) or "").strip() if winner_key else ""
                    home_score, away_score = _winner_scores(home_team, away_team, winner)
                    league = str(row.get(league_key) or "Cricket").strip() if league_key else "Cricket"
                    rows.append(
                        {
                            "sport": "cricket",
                            "league": league[:80],
                            "game_key": f"kaggle:{csv_path.name}:{game_date}:{home_team}:{away_team}",
                            "game_date": game_date,
                            "home_team": home_team[:120],
                            "away_team": away_team[:120],
                            "home_score": home_score,
                            "away_score": away_score,
                            "metadata": json.dumps({"source": "kaggle", "winner": winner}, ensure_ascii=True),
                        }
                    )
        except Exception:
            continue
    return rows


def collect_cricket_history(days_back: int = 180) -> dict:
    """Cricket history collector using Cricsheet/Kaggle plus ESPN fallback."""
    days = max(1, int(days_back or 1))
    end_date = dt.date.today()
    start_date = end_date - dt.timedelta(days=days)

    espn = collect_sport_history_from_espn(
        sport_tag="cricket",
        espn_paths=["cricket/ipl", "cricket/icc"],
        league_fallback="Cricket",
        days_back=days,
    )
    game_rows = list(espn.get("game_rows") or [])
    player_rows = list(espn.get("player_rows") or [])
    injury_rows = list(espn.get("injury_rows") or [])

    cricsheet_rows = _load_cricsheet_rows(
        os.getenv("CRICKET_CRICSHEET_DIR", ""),
        start_date=start_date,
        end_date=end_date,
    )
    kaggle_rows = _load_kaggle_rows(
        os.getenv("CRICKET_KAGGLE_DATA_DIR", ""),
        start_date=start_date,
        end_date=end_date,
    )

    game_rows.extend(cricsheet_rows)
    game_rows.extend(kaggle_rows)
    return {
        "sport": "cricket",
        "game_rows": game_rows,
        "player_rows": player_rows,
        "injury_rows": injury_rows,
        "sources": {
            "espn_games": len(espn.get("game_rows") or []),
            "cricsheet_games": len(cricsheet_rows),
            "kaggle_games": len(kaggle_rows),
        },
    }
