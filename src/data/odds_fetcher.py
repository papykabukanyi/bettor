"""
Odds Fetcher
============
Source: The Odds API  (https://the-odds-api.com)
Free tier: 500 requests / month  (resets monthly)
No commitment – just register for a key.

Supported sports:
  - baseball_mlb   : MLB moneyline, runline, totals
  - soccer_*       : MLS, EPL, La Liga, etc.

Also provides:
  - american_to_prob()    : convert American odds → implied probability
  - decimal_to_prob()     : convert decimal odds → implied probability
  - remove_vig()          : strip bookmaker margin from raw implied probs
"""

import os
import sys
import requests
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config import ODDS_API_KEY, ODDS_API_BASE, ODDS_REGIONS, et_today as _et_today

# Sports codes accepted by The Odds API
SPORT_MAP = {
    "mlb":      "baseball_mlb",
    "mls":      "soccer_usa_mls",
    "epl":      "soccer_epl",
    "laliga":   "soccer_spain_la_liga",
    "bundesliga": "soccer_germany_bundesliga",
    "seriea":   "soccer_italy_serie_a",
    "ligue1":   "soccer_france_ligue_1",
    "ucl":      "soccer_uefa_champs_league",
}


def _headers() -> dict:
    return {}  # API key is passed as a query param


def get_live_odds(sport_key: str = "mlb", markets: str = "h2h") -> list[dict]:
    """
    Fetch live / upcoming odds for a sport.

    sport_key : one of keys in SPORT_MAP or a raw odds-api sport key
    markets   : comma-separated, e.g. 'h2h' | 'spreads' | 'totals'

    Returns list of game dicts:
      {id, sport, commence_time, home_team, away_team,
       bookmakers: [{key, title, markets: [{key, outcomes: [{name, price}]}]}]}
    """
    if not ODDS_API_KEY or ODDS_API_KEY == "your_odds_api_key_here":
        print("[odds_fetcher] ODDS_API_KEY not set in .env – returning empty.")
        return []

    raw_sport = SPORT_MAP.get(sport_key.lower(), sport_key)
    url = f"{ODDS_API_BASE}/sports/{raw_sport}/odds"
    params = {
        "apiKey": ODDS_API_KEY,
        "regions": ODDS_REGIONS,
        "markets": markets,
        "oddsFormat": "american",
    }
    try:
        resp = requests.get(url, params=params, timeout=10)
        remaining = resp.headers.get("x-requests-remaining", "?")
        print(f"[odds_fetcher] Requests remaining this month: {remaining}")
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"[odds_fetcher] fetch error: {e}")
        return []


def odds_to_dataframe(games: list[dict], preferred_book: str = "draftkings") -> pd.DataFrame:
    """
    Flatten the nested odds response into a clean DataFrame.
    Picks the preferred bookmaker; falls back to the first available.

    Columns: sport, home_team, away_team, commence_time,
             home_odds, away_odds, draw_odds (soccer only)
    """
    rows = []
    for game in games:
        home = game.get("home_team", "")
        away = game.get("away_team", "")
        sport = game.get("sport_key", "")
        commence = game.get("commence_time", "")
        books = game.get("bookmakers", [])
        if not books:
            continue

        # prefer a specific book, else use first
        book = next((b for b in books if b["key"] == preferred_book), books[0])
        for market in book.get("markets", []):
            if market["key"] != "h2h":
                continue
            outcomes = {o["name"]: o["price"] for o in market.get("outcomes", [])}
            rows.append({
                "sport": sport,
                "home_team": home,
                "away_team": away,
                "commence_time": commence,
                "home_odds": outcomes.get(home),
                "away_odds": outcomes.get(away),
                "draw_odds": outcomes.get("Draw"),
                "book": book["title"],
            })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Odds math utilities
# ---------------------------------------------------------------------------

def american_to_prob(odds: float) -> float:
    """Convert American moneyline odds to raw implied probability (includes vig)."""
    if odds is None or pd.isna(odds):
        return float("nan")
    if odds > 0:
        return 100.0 / (odds + 100.0)
    else:
        return abs(odds) / (abs(odds) + 100.0)


def decimal_to_prob(odds: float) -> float:
    """Convert decimal odds (European) to implied probability."""
    if odds is None or pd.isna(odds) or odds <= 0:
        return float("nan")
    return 1.0 / odds


def remove_vig(home_prob: float, away_prob: float, draw_prob: float = 0.0) -> tuple[float, float, float]:
    """
    Normalise implied probabilities by removing bookmaker overround (vig).
    Returns (true_home_prob, true_away_prob, true_draw_prob) that sum to 1.
    """
    total = home_prob + away_prob + draw_prob
    if total <= 0:
        return home_prob, away_prob, draw_prob
    return home_prob / total, away_prob / total, draw_prob / total


def get_totals_odds(sport_key: str = "mlb") -> list[dict]:
    """Fetch over/under totals lines for a sport (same format as get_live_odds)."""
    return get_live_odds(sport_key, markets="totals")


def totals_to_dataframe(games: list[dict], preferred_book: str = "draftkings") -> pd.DataFrame:
    """
    Flatten totals market response into a clean DataFrame.

    Columns: sport, home_team, away_team, total_line, over_odds, under_odds, book
    """
    rows = []
    for game in games:
        home   = game.get("home_team", "")
        away   = game.get("away_team", "")
        sport  = game.get("sport_key", "")
        books  = game.get("bookmakers", [])
        if not books:
            continue

        book = next((b for b in books if b["key"] == preferred_book), books[0])
        for market in book.get("markets", []):
            if market["key"] != "totals":
                continue
            outcomes = {
                o["name"]: {"price": o["price"], "point": o.get("point")}
                for o in market.get("outcomes", [])
            }
            over_data  = outcomes.get("Over",  {})
            under_data = outcomes.get("Under", {})
            total_line = over_data.get("point") or under_data.get("point")
            rows.append({
                "sport":       sport,
                "home_team":   home,
                "away_team":   away,
                "total_line":  total_line,
                "over_odds":   over_data.get("price"),
                "under_odds":  under_data.get("price"),
                "book":        book["title"],
            })
    return pd.DataFrame(rows)


def get_available_sports() -> list[dict]:
    """List all sports currently available on The Odds API."""
    if not ODDS_API_KEY or ODDS_API_KEY == "your_odds_api_key_here":
        return []
    url = f"{ODDS_API_BASE}/sports"
    try:
        resp = requests.get(url, params={"apiKey": ODDS_API_KEY}, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"[odds_fetcher] sports list error: {e}")
        return []


def get_player_props_odds(
    sport_key: str = "mlb",
    markets: str = "pitcher_strikeouts,batter_hits,batter_home_runs,batter_total_bases",
    max_events: int = 15,
) -> list[dict]:
    """
    Fetch player prop odds from The Odds API for today's games.

    Uses the event-level endpoint which supports player prop markets.
    Costs 1 API credit per event (max_events caps spend).

    Returns flat list of dicts:
      {player, market, line, over_odds, under_odds, game, sport}
    """
    import datetime, time

    if not ODDS_API_KEY or ODDS_API_KEY == "your_odds_api_key_here":
        print("[odds_fetcher] ODDS_API_KEY not set – skipping player props odds.")
        return []

    raw_sport = SPORT_MAP.get(sport_key.lower(), sport_key)
    import datetime as _ods_dt
    _et_date   = _et_today()
    _et_dates  = {_et_date.isoformat(), (_et_date + _ods_dt.timedelta(days=1)).isoformat()}

    # Step 1: get event list (0 credits used)
    events_url = f"{ODDS_API_BASE}/sports/{raw_sport}/events"
    try:
        resp = requests.get(events_url, params={"apiKey": ODDS_API_KEY}, timeout=10)
        resp.raise_for_status()
        events = resp.json()
    except Exception as e:
        print(f"[odds_fetcher] events fetch error: {e}")
        return []

    # Filter to today+tomorrow ET window, cap count
    today_events = [e for e in events if str(e.get("commence_time", ""))[:10] in _et_dates]
    today_events = today_events[:max_events]

    if not today_events:
        print(f"[odds_fetcher] No {sport_key} events today for player props.")
        return []

    props = []
    for event in today_events:
        eid  = event.get("id")
        home = event.get("home_team", "")
        away = event.get("away_team", "")
        if not eid:
            continue
        url = f"{ODDS_API_BASE}/sports/{raw_sport}/events/{eid}/odds"
        params = {
            "apiKey":     ODDS_API_KEY,
            "regions":    ODDS_REGIONS,
            "markets":    markets,
            "oddsFormat": "american",
        }
        try:
            r = requests.get(url, params=params, timeout=10)
            remaining = r.headers.get("x-requests-remaining", "?")
            print(f"[odds_fetcher] player props {away}@{home}: {remaining} API credits left")
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            print(f"[odds_fetcher] player props error {eid}: {e}")
            time.sleep(0.5)
            continue

        game_label = f"{away} @ {home}"
        books = data.get("bookmakers", [])
        # prefer DraftKings, fallback to first
        book = next((b for b in books if b["key"] == "draftkings"), books[0] if books else None)
        if not book:
            time.sleep(0.5)
            continue

        for market in book.get("markets", []):
            mkey     = market.get("key", "")
            outcomes = market.get("outcomes", [])
            # Group outcomes by player name (description field)
            player_map: dict[str, dict] = {}
            for o in outcomes:
                pname = o.get("description") or o.get("name", "")
                side  = o.get("name", "")       # "Over" / "Under"
                price = o.get("price", 0)
                point = o.get("point", 0.5)
                if pname not in player_map:
                    player_map[pname] = {"line": point, "game": game_label, "sport": raw_sport}
                if side == "Over":
                    player_map[pname]["over_odds"]  = price
                elif side == "Under":
                    player_map[pname]["under_odds"] = price
            for pname, po in player_map.items():
                if "over_odds" in po or "under_odds" in po:
                    props.append({
                        "player":     pname,
                        "market":     mkey,
                        "line":       po.get("line", 0.5),
                        "over_odds":  po.get("over_odds"),
                        "under_odds": po.get("under_odds"),
                        "game":       po["game"],
                        "sport":      po["sport"],
                    })
        time.sleep(0.4)

    print(f"[odds_fetcher] Player props: {len(props)} lines fetched across {len(today_events)} games")
    return props


# ── Soccer player prop markets available in The Odds API ─────────────────────
# These are "yes" markets (anytime scorer, cards, assists) and over/under markets
_SOCCER_PROP_MARKETS = (
    "player_goal_scorer_anytime,"
    "player_first_goal_scorer,"
    "player_to_receive_red_card,"
    "player_cards,"
    "player_shots_on_target,"
    "player_assists,"
    "player_to_score_or_assist,"
)

_SOCCER_PROP_SPORT_KEYS = {
    "EPL":      "soccer_epl",
    "ESP":      "soccer_spain_la_liga",
    "GER":      "soccer_germany_bundesliga",
    "ITA":      "soccer_italy_serie_a",
    "FRA":      "soccer_france_ligue_1",
}

# Map Odds API market key → our internal stat_type
_SOCCER_MARKET_TO_STAT = {
    "player_goal_scorer_anytime":  "goals_scored",
    "player_first_goal_scorer":    "goals_scored",
    "player_to_score_or_assist":   "goal_or_assist",
    "player_assists":              "assists",
    "player_shots_on_target":      "shots_on_target",
    "player_cards":                "cards",
    "player_to_receive_red_card":  "cards",
}


def _american_to_implied(odds: int | float) -> float:
    """Convert American odds to raw implied probability (includes vig)."""
    if odds is None:
        return 0.5
    odds = float(odds)
    if odds >= 0:
        return 100 / (odds + 100)
    else:
        return abs(odds) / (abs(odds) + 100)


def get_soccer_player_props_from_odds(
    league_keys: list[str] | None = None,
    max_events_per_league: int = 4,
) -> list[dict]:
    """
    Fetch soccer player props directly from The Odds API.
    Used as primary (or fallback when FBRef is blocked).

    Returns list of dicts compatible with get_soccer_player_props_batch output:
      {name, team, game, league, sport, stat_type, line, over_prob, under_prob,
       over_odds_am, under_odds_am, avg_per_game, xg, xa, goals_pg, assists_pg,
       card_pg, mp, ...}
    """
    import datetime, time

    if not ODDS_API_KEY or ODDS_API_KEY == "your_odds_api_key_here":
        return []

    use_leagues = league_keys or list(_SOCCER_PROP_SPORT_KEYS.keys())
    _et_date   = _et_today()
    _et_dates  = {_et_date.isoformat(), (_et_date + datetime.timedelta(days=1)).isoformat()}
    results: list[dict] = []
    seen: set = set()

    for lk in use_leagues:
        raw_sport = _SOCCER_PROP_SPORT_KEYS.get(lk)
        if not raw_sport:
            continue

        # Get today's events for this league
        events_url = f"{ODDS_API_BASE}/sports/{raw_sport}/events"
        try:
            resp = requests.get(events_url, params={"apiKey": ODDS_API_KEY}, timeout=10)
            resp.raise_for_status()
            events = resp.json()
        except Exception as e:
            print(f"[odds_fetcher] soccer events error ({lk}): {e}")
            continue

        today_events = [e for e in events
                        if str(e.get("commence_time", ""))[:10] in _et_dates][:max_events_per_league]
        if not today_events:
            print(f"[odds_fetcher] No {lk} events today/tomorrow")
            continue

        for event in today_events:
            eid  = event.get("id")
            home = event.get("home_team", "")
            away = event.get("away_team", "")
            if not eid:
                continue
            game_label = f"{away} @ {home}"

            url = f"{ODDS_API_BASE}/sports/{raw_sport}/events/{eid}/odds"
            params = {
                "apiKey":     ODDS_API_KEY,
                "regions":    ODDS_REGIONS,
                "markets":    _SOCCER_PROP_MARKETS.rstrip(","),
                "oddsFormat": "american",
            }
            try:
                r = requests.get(url, params=params, timeout=12)
                remaining = r.headers.get("x-requests-remaining", "?")
                print(f"[odds_fetcher] soccer props {game_label} ({lk}): {remaining} credits left")
                if r.status_code == 422:
                    # market not available for this event
                    time.sleep(0.5)
                    continue
                r.raise_for_status()
                data = r.json()
            except Exception as e:
                print(f"[odds_fetcher] soccer props error ({lk} {eid}): {e}")
                time.sleep(0.5)
                continue

            books = data.get("bookmakers", [])
            book = next((b for b in books if b["key"] in ("draftkings", "fanduel", "betmgm", "williamhill_us")), books[0] if books else None)
            if not book:
                time.sleep(0.5)
                continue

            for market in book.get("markets", []):
                mkey     = market.get("key", "")
                stat_type = _SOCCER_MARKET_TO_STAT.get(mkey)
                if not stat_type:
                    continue
                outcomes  = market.get("outcomes", [])

                # Check if it's an over/under market or yes/no market
                sides = {o.get("name", "") for o in outcomes}
                is_over_under = "Over" in sides and "Under" in sides

                if is_over_under:
                    # Group by player (description field)
                    player_map: dict = {}
                    for o in outcomes:
                        pname = o.get("description") or ""
                        side  = o.get("name", "")
                        price = o.get("price", 0)
                        point = o.get("point", 0.5)
                        if not pname:
                            continue
                        if pname not in player_map:
                            player_map[pname] = {"line": point}
                        if side == "Over":
                            player_map[pname]["over_odds"] = price
                        elif side == "Under":
                            player_map[pname]["under_odds"] = price
                    for pname, po in player_map.items():
                        ov_odds = po.get("over_odds")
                        un_odds = po.get("under_odds")
                        if not ov_odds and not un_odds:
                            continue
                        raw_ov = _american_to_implied(ov_odds) if ov_odds else 0.5
                        raw_un = _american_to_implied(un_odds) if un_odds else 0.5
                        total  = raw_ov + raw_un
                        ov_p   = round(raw_ov / total, 4) if total > 0 else 0.5
                        un_p   = round(raw_un / total, 4) if total > 0 else 0.5
                        key = (pname, game_label, stat_type)
                        if key in seen:
                            continue
                        seen.add(key)
                        line = po.get("line", 0.5)
                        results.append({
                            "name":        pname, "team": home, "game": game_label,
                            "league": lk, "sport": "soccer",
                            "stat_type":   stat_type, "line": line,
                            "over_prob":   ov_p, "under_prob": un_p,
                            "over_odds_am": ov_odds, "under_odds_am": un_odds,
                            "avg_per_game": round(line, 2),
                            "xg": 0.0, "xa": 0.0, "goals_pg": 0.0,
                            "assists_pg": 0.0, "card_pg": 0.0, "mp": 0,
                            "era": 0, "xfip": 0, "k9": 0, "k_pct": 0,
                            "whip": 0, "avg_ks": 0, "avg": 0, "ops": 0,
                            "wrc_plus": 0,
                            "over_pct":  round(ov_p * 100),
                            "under_pct": round(un_p * 100),
                        })
                else:
                    # Yes/no market (anytime scorer, cards etc.)
                    for o in outcomes:
                        pname = o.get("description") or o.get("name", "")
                        side  = o.get("name", "")
                        price = o.get("price", 0)
                        if side not in ("Yes", pname):
                            continue
                        if not pname or pname in ("Yes", "No"):
                            pname = side  # skip malformed
                            continue
                        raw_p = _american_to_implied(price)
                        # Slight vig removal: assume market is ~105% over
                        ov_p  = round(min(raw_p / 1.05, 0.97), 4)
                        un_p  = round(1.0 - ov_p, 4)
                        key   = (pname, game_label, stat_type)
                        if key in seen:
                            continue
                        seen.add(key)
                        results.append({
                            "name":        pname, "team": home, "game": game_label,
                            "league": lk, "sport": "soccer",
                            "stat_type":   stat_type, "line": 0.5,
                            "over_prob":   ov_p, "under_prob": un_p,
                            "over_odds_am": price, "under_odds_am": None,
                            "avg_per_game": 0.0,
                            "xg": 0.0, "xa": 0.0, "goals_pg": 0.0,
                            "assists_pg": 0.0, "card_pg": 0.0, "mp": 0,
                            "era": 0, "xfip": 0, "k9": 0, "k_pct": 0,
                            "whip": 0, "avg_ks": 0, "avg": 0, "ops": 0,
                            "wrc_plus": 0,
                            "over_pct":  round(ov_p * 100),
                            "under_pct": round(un_p * 100),
                        })
            time.sleep(0.4)

    print(f"[odds_fetcher] Soccer player props from Odds API: {len(results)} lines")
    return results

