from __future__ import annotations

import datetime as dt
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any, Iterable

import requests

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

LOGGER = logging.getLogger("modal_app")
if not logging.getLogger().handlers:
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

LOCAL_DATA_ROOT = ROOT_DIR / "modal_data"
REMOTE_DATA_ROOT = Path("/root/modal-data")
VOLUME_NAME = os.getenv("MODAL_VOLUME_NAME", "bettor-data")
REMOTE_VOLUME_MOUNT = str(REMOTE_DATA_ROOT)
REQUEST_TIMEOUT = int(os.getenv("MODAL_HTTP_TIMEOUT", "25") or "25")
BALLDONTLIE_API_KEY = str(os.getenv("BALLDONTLIE_API_KEY", "") or "").strip()
THESPORTSDB_API_KEY = str(os.getenv("THESPORTSDB_API_KEY", "1") or "1").strip()

CONFIDENCE_TIERS = ((0.70, "Elite"), (0.60, "Solid"), (0.55, "Lean"), (0.0, "Watch"))
FEATURE_COLUMNS = ["home_team", "away_team", "sport", "league", "season", "month", "day_of_week"]
CAT_COLUMNS = ["home_team", "away_team", "sport", "league"]
NUM_COLUMNS = ["season", "month", "day_of_week"]
SUPPORTED_SPORTS = ("mlb", "nba", "nhl", "soccer")

try:
    import modal
except Exception:
    modal = None

if modal is not None:
    image = (
        modal.Image.debian_slim(python_version="3.11")
        .pip_install_from_requirements(str(ROOT_DIR / "modal_app" / "requirements.txt"))
        .add_local_dir(str(SRC_DIR), remote_path="/root/project/src")
    )
    volume = modal.Volume.from_name(VOLUME_NAME, create_if_missing=True)
else:
    class _LocalApp:
        def function(self, *args, **kwargs):
            def _decorator(func):
                return func

            return _decorator

        def local_entrypoint(self, *args, **kwargs):
            def _decorator(func):
                return func

            return _decorator

    class _LocalModal:
        class Cron:
            def __init__(self, *_args, **_kwargs):
                pass

        @staticmethod
        def wsgi_app():
            def _decorator(func):
                return func

            return _decorator

        @staticmethod
        def App(_name: str):
            return _LocalApp()

    modal = _LocalModal()
    image = None
    volume = None


def modal_secrets() -> list[Any]:
    if not hasattr(modal, "Secret"):
        return []
    names = []
    for env_name, default_name in (
        ("MODAL_POLYMARKET_SECRET_NAME", "polymarket-creds"),
        ("MODAL_SPORTS_SECRET_NAME", "sports-data-creds"),
    ):
        secret_name = str(os.getenv(env_name, default_name) or "").strip()
        if secret_name and secret_name not in names:
            names.append(secret_name)
    secrets = []
    for name in names:
        try:
            secrets.append(modal.Secret.from_name(name))
        except Exception:
            continue
    return secrets


def make_app(name: str):
    return modal.App(name)


def in_modal_runtime() -> bool:
    return bool(os.getenv("MODAL_TASK_ID") or os.getenv("MODAL_CONTAINER_ID") or os.getenv("MODAL_IS_REMOTE"))


def data_root() -> Path:
    override = str(os.getenv("BETTOR_DATA_ROOT", "") or "").strip()
    if override:
        return Path(override)
    return REMOTE_DATA_ROOT if in_modal_runtime() else LOCAL_DATA_ROOT


def path_for(*parts: str) -> Path:
    return data_root().joinpath(*parts)


def now_utc_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def today_et() -> dt.date:
    try:
        import pytz

        eastern = pytz.timezone("America/New_York")
        return dt.datetime.now(eastern).date()
    except Exception:
        return dt.date.today()


def ensure_directories() -> None:
    for subdir in (
        path_for("raw"),
        path_for("pipeline"),
        path_for("predictions"),
        path_for("models"),
        path_for("polymarket"),
        path_for("schedules"),
    ):
        subdir.mkdir(parents=True, exist_ok=True)


def load_json(path: Path, default: Any) -> Any:
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:
        return default


def save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            raw = line.strip()
            if not raw:
                continue
            try:
                row = json.loads(raw)
            except Exception:
                continue
            if isinstance(row, dict):
                rows.append(row)
    return rows


def write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def dedupe_records(records: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for record in records:
        if not isinstance(record, dict):
            continue
        game_id = str(record.get("game_id") or "").strip()
        key = game_id or "|".join(
            [
                str(record.get("sport") or ""),
                str(record.get("league") or ""),
                str(record.get("game_date") or ""),
                str(record.get("away_team") or ""),
                str(record.get("home_team") or ""),
            ]
        )
        if not key or key in seen:
            continue
        seen.add(key)
        deduped.append(record)
    return deduped


def update_pipeline_status(section: str, payload: dict[str, Any]) -> dict[str, Any]:
    ensure_directories()
    status_path = path_for("pipeline", "status.json")
    status = load_json(status_path, {})
    if not isinstance(status, dict):
        status = {}
    status[section] = payload
    status["updated_at"] = now_utc_iso()
    save_json(status_path, status)
    return status


def confidence_tier(probability: float) -> str:
    for threshold, label in CONFIDENCE_TIERS:
        if probability >= threshold:
            return label
    return "Watch"


def requests_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": "bettor-modal-pipeline/1.0"})
    if BALLDONTLIE_API_KEY:
        session.headers.setdefault("Authorization", BALLDONTLIE_API_KEY)
    return session


def _game_record(*, game_id: str, sport: str, league: str, game_date: str, home_team: str, away_team: str, status: str, game_time: str = "", scheduled_start: str = "", home_score: float | None = None, away_score: float | None = None) -> dict[str, Any]:
    game_date = str(game_date or "")[:10]
    season = int(game_date[:4] or today_et().year)
    return {
        "game_id": str(game_id or "").strip(),
        "sport": str(sport or "").strip().lower(),
        "league": str(league or "").strip(),
        "game_date": game_date,
        "game_time": str(game_time or "").strip(),
        "scheduled_start": str(scheduled_start or game_time or "").strip(),
        "status": str(status or "").strip(),
        "home_team": " ".join(str(home_team or "").split()),
        "away_team": " ".join(str(away_team or "").split()),
        "home_score": home_score,
        "away_score": away_score,
        "season": season,
        "fetched_at": now_utc_iso(),
    }


def fetch_completed_games(start: dt.date, end: dt.date) -> list[dict[str, Any]]:
    session = requests_session()
    rows: list[dict[str, Any]] = []
    rows.extend(_fetch_mlb_completed(session, start, end))
    rows.extend(_fetch_nba_completed(session, start, end))
    rows.extend(_fetch_nhl_completed(session, start, end))
    rows.extend(_fetch_soccer_completed(session, start, end))
    return dedupe_records(rows)


def fetch_upcoming_games(day: dt.date) -> list[dict[str, Any]]:
    session = requests_session()
    rows: list[dict[str, Any]] = []
    rows.extend(_fetch_mlb_upcoming(session, day))
    rows.extend(_fetch_nba_upcoming(session, day))
    rows.extend(_fetch_nhl_upcoming(session, day))
    rows.extend(_fetch_soccer_upcoming(session, day))
    return dedupe_records(rows)


def _daterange(start: dt.date, end: dt.date):
    current = start
    while current <= end:
        yield current
        current += dt.timedelta(days=1)


def _fetch_mlb_completed(session: requests.Session, start: dt.date, end: dt.date) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for current in _daterange(start, end):
        day = current.isoformat()
        try:
            response = session.get(
                "https://statsapi.mlb.com/api/v1/schedule",
                params={"sportId": 1, "date": day, "hydrate": "linescore", "gameType": "R"},
                timeout=REQUEST_TIMEOUT,
            )
            response.raise_for_status()
            for day_row in (response.json() or {}).get("dates", []):
                for game in day_row.get("games", []):
                    status = str((game.get("status") or {}).get("detailedState") or "")
                    if status not in {"Final", "Game Over", "Completed Early", "Completed", "F"}:
                        continue
                    teams = game.get("teams") or {}
                    home = teams.get("home") or {}
                    away = teams.get("away") or {}
                    home_team = str(((home.get("team") or {}).get("name")) or "").strip()
                    away_team = str(((away.get("team") or {}).get("name")) or "").strip()
                    home_score = home.get("score")
                    away_score = away.get("score")
                    if not home_team or not away_team or home_score is None or away_score is None:
                        continue
                    rows.append(
                        _game_record(
                            game_id=str(game.get("gamePk") or ""),
                            sport="mlb",
                            league="MLB",
                            game_date=day,
                            home_team=home_team,
                            away_team=away_team,
                            status=status,
                            game_time=str(game.get("gameDate") or ""),
                            scheduled_start=str(game.get("gameDate") or ""),
                            home_score=float(home_score),
                            away_score=float(away_score),
                        )
                    )
        except Exception as exc:
            LOGGER.warning("MLB completed fetch failed for %s: %s", day, exc)
    return rows


def _fetch_nba_completed(session: requests.Session, start: dt.date, end: dt.date) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for current in _daterange(start, end):
        day = current.isoformat()
        try:
            response = session.get(
                "https://www.balldontlie.io/api/v1/games",
                params={"start_date": day, "end_date": day, "per_page": 100},
                timeout=REQUEST_TIMEOUT,
            )
            response.raise_for_status()
            for game in (response.json() or {}).get("data", []):
                if str(game.get("status") or "").strip() != "Final":
                    continue
                home_team = str((game.get("home_team") or {}).get("full_name") or "").strip()
                away_team = str((game.get("visitor_team") or {}).get("full_name") or "").strip()
                home_score = game.get("home_team_score")
                away_score = game.get("visitor_team_score")
                if not home_team or not away_team or home_score is None or away_score is None:
                    continue
                played_on = str(game.get("date") or day)[:10]
                rows.append(
                    _game_record(
                        game_id=str(game.get("id") or ""),
                        sport="nba",
                        league="NBA",
                        game_date=played_on,
                        home_team=home_team,
                        away_team=away_team,
                        status="Final",
                        game_time=str(game.get("date") or ""),
                        scheduled_start=str(game.get("date") or ""),
                        home_score=float(home_score),
                        away_score=float(away_score),
                    )
                )
            time.sleep(0.2)
        except Exception as exc:
            LOGGER.warning("NBA completed fetch failed for %s: %s", day, exc)
    return rows


def _fetch_nhl_completed(session: requests.Session, start: dt.date, end: dt.date) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for current in _daterange(start, end):
        day = current.isoformat()
        try:
            response = session.get(f"https://api-web.nhle.com/v1/schedule/{day}", timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            for week in (response.json() or {}).get("gameWeek", []):
                for game in week.get("games", []):
                    state = str(game.get("gameState") or "")
                    if state not in {"OFF", "FINAL", "OVER"}:
                        continue
                    home = game.get("homeTeam") or {}
                    away = game.get("awayTeam") or {}
                    home_team = str((home.get("commonName") or {}).get("default") or home.get("abbrev") or "").strip()
                    away_team = str((away.get("commonName") or {}).get("default") or away.get("abbrev") or "").strip()
                    home_score = home.get("score")
                    away_score = away.get("score")
                    if not home_team or not away_team or home_score is None or away_score is None:
                        continue
                    rows.append(
                        _game_record(
                            game_id=str(game.get("id") or ""),
                            sport="nhl",
                            league="NHL",
                            game_date=str(game.get("gameDate") or day)[:10],
                            home_team=home_team,
                            away_team=away_team,
                            status="Final",
                            game_time=str(game.get("startTimeUTC") or ""),
                            scheduled_start=str(game.get("startTimeUTC") or ""),
                            home_score=float(home_score),
                            away_score=float(away_score),
                        )
                    )
        except Exception as exc:
            LOGGER.warning("NHL completed fetch failed for %s: %s", day, exc)
    return rows


def _fetch_soccer_completed(session: requests.Session, start: dt.date, end: dt.date) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for current in _daterange(start, end):
        day = current.isoformat()
        try:
            response = session.get(
                f"https://www.thesportsdb.com/api/v1/json/{THESPORTSDB_API_KEY}/eventsday.php",
                params={"d": day, "s": "Soccer"},
                timeout=REQUEST_TIMEOUT,
            )
            response.raise_for_status()
            for event in ((response.json() or {}).get("events") or []):
                home_score = event.get("intHomeScore")
                away_score = event.get("intAwayScore")
                home_team = str(event.get("strHomeTeam") or "").strip()
                away_team = str(event.get("strAwayTeam") or "").strip()
                if not home_team or not away_team or home_score is None or away_score is None:
                    continue
                rows.append(
                    _game_record(
                        game_id=str(event.get("idEvent") or ""),
                        sport="soccer",
                        league=str(event.get("strLeague") or "Soccer"),
                        game_date=day,
                        home_team=home_team,
                        away_team=away_team,
                        status="Final",
                        game_time=str(event.get("strTime") or ""),
                        scheduled_start=str(event.get("strTimestamp") or event.get("strTime") or ""),
                        home_score=float(home_score),
                        away_score=float(away_score),
                    )
                )
        except Exception as exc:
            LOGGER.warning("Soccer completed fetch failed for %s: %s", day, exc)
    return rows


def _fetch_mlb_upcoming(session: requests.Session, day: dt.date) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        response = session.get(
            "https://statsapi.mlb.com/api/v1/schedule",
            params={"sportId": 1, "date": day.isoformat(), "gameType": "R"},
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        for day_row in (response.json() or {}).get("dates", []):
            for game in day_row.get("games", []):
                teams = game.get("teams") or {}
                home_team = str((((teams.get("home") or {}).get("team") or {}).get("name")) or "").strip()
                away_team = str((((teams.get("away") or {}).get("team") or {}).get("name")) or "").strip()
                if not home_team or not away_team:
                    continue
                rows.append(
                    _game_record(
                        game_id=str(game.get("gamePk") or ""),
                        sport="mlb",
                        league="MLB",
                        game_date=day.isoformat(),
                        home_team=home_team,
                        away_team=away_team,
                        status="Scheduled",
                        game_time=str(game.get("gameDate") or ""),
                        scheduled_start=str(game.get("gameDate") or ""),
                    )
                )
    except Exception as exc:
        LOGGER.warning("MLB upcoming fetch failed for %s: %s", day, exc)
    return rows


def _fetch_nba_upcoming(session: requests.Session, day: dt.date) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        response = session.get(
            "https://www.balldontlie.io/api/v1/games",
            params={"start_date": day.isoformat(), "end_date": day.isoformat(), "per_page": 100},
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        for game in (response.json() or {}).get("data", []):
            home_team = str((game.get("home_team") or {}).get("full_name") or "").strip()
            away_team = str((game.get("visitor_team") or {}).get("full_name") or "").strip()
            if not home_team or not away_team:
                continue
            rows.append(
                _game_record(
                    game_id=str(game.get("id") or ""),
                    sport="nba",
                    league="NBA",
                    game_date=day.isoformat(),
                    home_team=home_team,
                    away_team=away_team,
                    status="Scheduled",
                    game_time=str(game.get("date") or ""),
                    scheduled_start=str(game.get("date") or ""),
                )
            )
    except Exception as exc:
        LOGGER.warning("NBA upcoming fetch failed for %s: %s", day, exc)
    return rows


def _fetch_nhl_upcoming(session: requests.Session, day: dt.date) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        response = session.get(f"https://api-web.nhle.com/v1/schedule/{day.isoformat()}", timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        for week in (response.json() or {}).get("gameWeek", []):
            for game in week.get("games", []):
                home = game.get("homeTeam") or {}
                away = game.get("awayTeam") or {}
                home_team = str((home.get("commonName") or {}).get("default") or home.get("abbrev") or "").strip()
                away_team = str((away.get("commonName") or {}).get("default") or away.get("abbrev") or "").strip()
                if not home_team or not away_team:
                    continue
                rows.append(
                    _game_record(
                        game_id=str(game.get("id") or ""),
                        sport="nhl",
                        league="NHL",
                        game_date=str(game.get("gameDate") or day.isoformat())[:10],
                        home_team=home_team,
                        away_team=away_team,
                        status="Scheduled",
                        game_time=str(game.get("startTimeUTC") or ""),
                        scheduled_start=str(game.get("startTimeUTC") or ""),
                    )
                )
    except Exception as exc:
        LOGGER.warning("NHL upcoming fetch failed for %s: %s", day, exc)
    return rows


def _fetch_soccer_upcoming(session: requests.Session, day: dt.date) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        response = session.get(
            f"https://www.thesportsdb.com/api/v1/json/{THESPORTSDB_API_KEY}/eventsday.php",
            params={"d": day.isoformat(), "s": "Soccer"},
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        for event in ((response.json() or {}).get("events") or []):
            if event.get("intHomeScore") is not None or event.get("intAwayScore") is not None:
                continue
            home_team = str(event.get("strHomeTeam") or "").strip()
            away_team = str(event.get("strAwayTeam") or "").strip()
            if not home_team or not away_team:
                continue
            rows.append(
                _game_record(
                    game_id=str(event.get("idEvent") or ""),
                    sport="soccer",
                    league=str(event.get("strLeague") or "Soccer"),
                    game_date=day.isoformat(),
                    home_team=home_team,
                    away_team=away_team,
                    status="Scheduled",
                    game_time=str(event.get("strTime") or ""),
                    scheduled_start=str(event.get("strTimestamp") or event.get("strTime") or ""),
                )
            )
    except Exception as exc:
        LOGGER.warning("Soccer upcoming fetch failed for %s: %s", day, exc)
    return rows


def sport_counts(rows: Iterable[dict[str, Any]]) -> dict[str, int]:
    counts = {sport: 0 for sport in SUPPORTED_SPORTS}
    for row in rows:
        sport = str((row or {}).get("sport") or "").lower()
        if sport in counts:
            counts[sport] += 1
    return counts


def load_history_records() -> list[dict[str, Any]]:
    return read_jsonl(path_for("raw", "games_history.jsonl"))


def save_history_records(records: list[dict[str, Any]]) -> None:
    write_jsonl(path_for("raw", "games_history.jsonl"), dedupe_records(records))


def load_schedule_payload() -> dict[str, Any]:
    return load_json(path_for("schedules", "upcoming.json"), {})


def save_schedule_payload(payload: dict[str, Any]) -> None:
    save_json(path_for("schedules", "upcoming.json"), payload)


def load_predictions_payload() -> dict[str, Any]:
    return load_json(path_for("predictions", "latest.json"), {})


def save_predictions_payload(payload: dict[str, Any]) -> None:
    save_json(path_for("predictions", "latest.json"), payload)


def load_model_stats() -> dict[str, Any]:
    return load_json(path_for("models", "model_stats.json"), {})


def save_model_stats(payload: dict[str, Any]) -> None:
    save_json(path_for("models", "model_stats.json"), payload)


def load_training_history() -> list[dict[str, Any]]:
    data = load_json(path_for("models", "training_history.json"), [])
    return data if isinstance(data, list) else []


def save_training_history(rows: list[dict[str, Any]]) -> None:
    save_json(path_for("models", "training_history.json"), rows[-100:])


def load_submissions_payload() -> dict[str, Any]:
    payload = load_json(path_for("polymarket", "submissions.json"), {})
    return payload if isinstance(payload, dict) else {}


def save_submissions_payload(payload: dict[str, Any]) -> None:
    save_json(path_for("polymarket", "submissions.json"), payload)


def load_positions_payload() -> dict[str, Any]:
    payload = load_json(path_for("polymarket", "positions.json"), {})
    return payload if isinstance(payload, dict) else {}


def save_positions_payload(payload: dict[str, Any]) -> None:
    save_json(path_for("polymarket", "positions.json"), payload)


def commit_volume() -> None:
    if volume is None:
        return
    try:
        volume.commit()
    except Exception:
        pass


def reload_volume() -> None:
    if volume is None:
        return
    try:
        volume.reload()
    except Exception:
        pass


def predictions_for_label(label: str) -> dict[str, Any]:
    payload = load_predictions_payload()
    predictions = payload.get("predictions") or []
    if not isinstance(predictions, list):
        predictions = []
    label = str(label or "today").strip().lower()
    target_date = str(payload.get("tomorrow") if label == "tomorrow" else payload.get("today") or "")
    if label not in {"today", "tomorrow"} and label:
        target_date = label
    filtered = [row for row in predictions if isinstance(row, dict) and str(row.get("game_date") or "") == target_date]
    return {
        "ok": True,
        "date": target_date,
        "generated_at": payload.get("generated_at", ""),
        "prediction_count": len(filtered),
        "model_version": payload.get("model_version", ""),
        "model_name": payload.get("model_name", payload.get("model_type", "")),
        "predictions": filtered,
    }


def build_api_status() -> dict[str, Any]:
    status = load_json(path_for("pipeline", "status.json"), {})
    model_stats = load_model_stats()
    predictions = load_predictions_payload()
    submissions = load_submissions_payload()
    positions = load_positions_payload()
    today_payload = predictions_for_label("today")
    tomorrow_payload = predictions_for_label("tomorrow")
    submission_summary = submissions.get("summary") if isinstance(submissions, dict) else {}
    position_summary = positions.get("summary") if isinstance(positions, dict) else {}
    return {
        "ok": True,
        "updated_at": status.get("updated_at") or now_utc_iso(),
        "pipeline": status,
        "metrics": {
            "total_predictions": int(predictions.get("prediction_count") or 0),
            "today_predictions": int(today_payload.get("prediction_count") or 0),
            "tomorrow_predictions": int(tomorrow_payload.get("prediction_count") or 0),
            "win_rate": float(model_stats.get("best_score") or 0.0),
            "polymarket_bets": int(submission_summary.get("placed") or 0),
            "active_models": int(model_stats.get("candidate_count") or 0),
            "active_positions": int(position_summary.get("active_positions") or 0),
        },
        "model": model_stats,
        "polymarket": {
            "submissions": submission_summary or {},
            "positions": position_summary or {},
        },
    }
