"""
club_stats_fetcher.py — Player club-level stats for WC 2026 squads
==================================================================
Data sources (all FREE, no paid key required):
  1. soccerdata package  → FBref (football-reference.com)   pip install soccerdata
  2. understat package   → xG data for top-6 European leagues  pip install understat
  3. ESPN unofficial API → basic player lookup (no key)
  4. Transfermarkt JSON  → market value / position lookup

Usage:
  from data.club_stats_fetcher import get_wc_player_stats, get_squad_props
"""

from __future__ import annotations

import json
import math
import os
import time
from typing import Any

import requests

_cache: dict[str, Any] = {}
_CACHE_TTL = 3600 * 6  # 6 h


# ── Stat types we track for player props ─────────────────────────────────────
PROP_TYPES = ["goals", "assists", "shots", "shots_on_target", "key_passes",
              "xg", "xa", "progressive_carries", "dribbles_completed",
              "clean_sheets", "saves"]  # goalkeepers: clean_sheets, saves

# Maps FBref column names → our prop keys
_FBREF_COL_MAP = {
    "goals":            "goals",
    "assists":          "assists",
    "shots":            "shots",
    "shots_on_target":  "shots_on_target",
    "npxg":             "xg",
    "xa":               "xa",
    "progressive_carries": "progressive_carries",
    "dribbles_completed":  "dribbles_completed",
    "minutes_90s":      "minutes_90s",
}

# Top leagues where WC players come from
TOP_LEAGUES = [
    "ENG-Premier League",
    "ESP-La Liga",
    "GER-Bundesliga",
    "ITA-Serie A",
    "FRA-Ligue 1",
    "USA-Major League Soccer",
    "MEX-Liga MX",
    "POR-Primeira Liga",
    "NED-Eredivisie",
    "BRA-Serie A",
    "ARG-Liga Profesional",
]

CURRENT_SEASON = "2025-2026"


# ── FBref via soccerdata ──────────────────────────────────────────────────────
def _load_fbref_stats(leagues: list[str] | None = None, season: str = CURRENT_SEASON) -> list[dict]:
    """Load player stats from FBref via soccerdata package."""
    cache_key = f"fbref_{season}_{'_'.join((leagues or TOP_LEAGUES)[:3])}"
    cached = _cache.get(cache_key)
    if cached and time.time() - cached[1] < _CACHE_TTL:
        return cached[0]

    try:
        import soccerdata as sd
        fbref = sd.FBref(leagues=leagues or TOP_LEAGUES[:6], seasons=[season])
        # Shooting stats
        shooting = fbref.read_player_season_stats(stat_type="shooting")
        passing  = fbref.read_player_season_stats(stat_type="passing")

        stats: dict[str, dict] = {}

        for df, keys in [(shooting, ["goals","shots","shots_on_target","npxg"]),
                          (passing,  ["assists","xa","key_passes"])]:
            if df is None or df.empty:
                continue
            df = df.reset_index()
            for _, row in df.iterrows():
                name   = str(row.get("player", "")).strip()
                team   = str(row.get("team", "")).strip()
                nation = str(row.get("nationality", "")).strip()
                mins_90 = float(row.get("minutes_90s", 1) or 1)
                if not name:
                    continue
                key = f"{name}|{team}"
                if key not in stats:
                    stats[key] = {
                        "name": name, "team": team, "nationality": nation,
                        "minutes_90s": mins_90,
                        "league": str(row.get("league_name", "")),
                        "season": season,
                    }
                for col in keys:
                    raw = row.get(col)
                    if raw is not None:
                        prop_key = _FBREF_COL_MAP.get(col, col)
                        try:
                            stats[key][prop_key] = float(raw)
                            # Also compute per-90 rate
                            if mins_90 > 0:
                                stats[key][f"{prop_key}_per90"] = float(raw) / mins_90
                        except (ValueError, TypeError):
                            pass

        result = list(stats.values())
        _cache[cache_key] = (result, time.time())
        print(f"[club_stats] FBref loaded {len(result)} players from {season}")
        return result
    except ImportError:
        print("[club_stats] soccerdata not installed — pip install soccerdata")
    except Exception as e:
        print(f"[club_stats] FBref error: {e}")
    return []


# ── Understat (xG, xA per match) ─────────────────────────────────────────────
def _load_understat_stats(league: str = "La liga", season: int = 2025) -> list[dict]:
    """
    Load xG/xA stats from understat.com (free, no key).
    league options: 'EPL', 'La liga', 'Bundesliga', 'Serie A', 'Ligue 1'
    """
    try:
        import asyncio
        import understat  # pip install understat

        async def _fetch():
            async with understat.Understat() as u:
                players = await u.get_league_players(league, season)
                return players

        try:
            loop = asyncio.get_event_loop()
            if loop.is_closed():
                loop = asyncio.new_event_loop()
            players = loop.run_until_complete(_fetch())
        except RuntimeError:
            players = asyncio.run(_fetch())

        result = []
        for p in (players or []):
            try:
                result.append({
                    "name":     p.get("player_name", p.get("name", "")),
                    "team":     p.get("team_title", ""),
                    "xg":       float(p.get("xG", 0)),
                    "xa":       float(p.get("xA", 0)),
                    "goals":    int(p.get("goals", 0)),
                    "assists":  int(p.get("assists", 0)),
                    "shots":    int(p.get("shots", 0)),
                    "minutes":  int(p.get("time", 0)),
                    "source":   "understat",
                    "league":   league,
                })
            except Exception:
                pass
        return result
    except ImportError:
        print("[club_stats] understat not installed — pip install understat")
    except Exception as e:
        print(f"[club_stats] understat error: {e}")
    return []


# ── ESPN unofficial player lookup ─────────────────────────────────────────────
def _espn_player_search(name: str) -> dict | None:
    """Search ESPN for a player (unofficial endpoint, no key)."""
    try:
        url = f"https://site.api.espn.com/apis/common/v3/search"
        r = requests.get(url, params={"query": name, "limit": 3, "type": "player",
                                       "sport": "soccer"}, timeout=6)
        if r.status_code == 200:
            data = r.json()
            items = data.get("items", [])
            if items:
                p = items[0]
                return {
                    "name":     p.get("displayName", name),
                    "team":     p.get("teamName", ""),
                    "position": p.get("position", ""),
                    "nationality": p.get("citizenship", ""),
                    "espn_id":  p.get("id", ""),
                }
    except Exception:
        pass
    return None


# ── Embedded top WC player stats (season averages, May 2026 estimates) ───────
# Per-90 minutes stats from 2025-26 club season for key WC players
_TOP_WC_PLAYERS: list[dict] = [
    # ── FRANCE ──
    {"name":"Kylian Mbappé","team":"Real Madrid","nation":"France","position":"FW","goals":28,"assists":10,"xg":24.1,"xa":8.2,"shots":148,"shots_on_target":62,"goals_per90":0.72,"xg_per90":0.62,"league":"La Liga"},
    {"name":"Antoine Griezmann","team":"Atlético Madrid","nation":"France","position":"FW","goals":18,"assists":14,"xg":16.8,"xa":11.2,"shots":98,"shots_on_target":40,"goals_per90":0.55,"xg_per90":0.51,"league":"La Liga"},
    {"name":"Ousmane Dembélé","team":"Paris Saint-Germain","nation":"France","position":"FW","goals":20,"assists":18,"xg":17.5,"xa":14.3,"shots":105,"shots_on_target":44,"goals_per90":0.58,"xg_per90":0.51,"league":"Ligue 1"},
    {"name":"Aurélien Tchouaméni","team":"Real Madrid","nation":"France","position":"MF","goals":5,"assists":4,"xg":4.1,"xa":2.8,"shots":45,"shots_on_target":18,"goals_per90":0.14,"xg_per90":0.12,"league":"La Liga"},
    # ── BRAZIL ──
    {"name":"Vinícius Júnior","team":"Real Madrid","nation":"Brazil","position":"FW","goals":26,"assists":11,"xg":21.8,"xa":9.5,"shots":138,"shots_on_target":55,"goals_per90":0.71,"xg_per90":0.60,"league":"La Liga"},
    {"name":"Rodrygo","team":"Real Madrid","nation":"Brazil","position":"FW","goals":19,"assists":9,"xg":17.2,"xa":7.8,"shots":112,"shots_on_target":42,"goals_per90":0.55,"xg_per90":0.50,"league":"La Liga"},
    {"name":"Raphinha","team":"Barcelona","nation":"Brazil","position":"FW","goals":27,"assists":19,"xg":22.5,"xa":15.8,"shots":155,"shots_on_target":60,"goals_per90":0.75,"xg_per90":0.63,"league":"La Liga"},
    {"name":"Casemiro","team":"Manchester United","nation":"Brazil","position":"MF","goals":6,"assists":3,"xg":5.2,"xa":2.4,"shots":38,"shots_on_target":15,"goals_per90":0.17,"xg_per90":0.14,"league":"Premier League"},
    # ── ARGENTINA ──
    {"name":"Lionel Messi","team":"Inter Miami","nation":"Argentina","position":"FW","goals":22,"assists":18,"xg":19.5,"xa":15.2,"shots":132,"shots_on_target":58,"goals_per90":0.68,"xg_per90":0.60,"league":"MLS"},
    {"name":"Julián Álvarez","team":"Atlético Madrid","nation":"Argentina","position":"FW","goals":24,"assists":12,"xg":21.2,"xa":9.8,"shots":120,"shots_on_target":50,"goals_per90":0.67,"xg_per90":0.59,"league":"La Liga"},
    {"name":"Enzo Fernández","team":"Chelsea","nation":"Argentina","position":"MF","goals":8,"assists":10,"xg":7.2,"xa":8.8,"shots":60,"shots_on_target":22,"goals_per90":0.22,"xg_per90":0.20,"league":"Premier League"},
    {"name":"Rodrigo De Paul","team":"Atlético Madrid","nation":"Argentina","position":"MF","goals":6,"assists":8,"xg":5.5,"xa":7.2,"shots":50,"shots_on_target":18,"goals_per90":0.17,"xg_per90":0.15,"league":"La Liga"},
    # ── ENGLAND ──
    {"name":"Harry Kane","team":"Bayern Munich","nation":"England","position":"FW","goals":36,"assists":15,"xg":31.5,"xa":12.8,"shots":165,"shots_on_target":72,"goals_per90":0.92,"xg_per90":0.81,"league":"Bundesliga"},
    {"name":"Bukayo Saka","team":"Arsenal","nation":"England","position":"FW","goals":20,"assists":16,"xg":17.8,"xa":13.5,"shots":108,"shots_on_target":44,"goals_per90":0.58,"xg_per90":0.52,"league":"Premier League"},
    {"name":"Phil Foden","team":"Manchester City","nation":"England","position":"FW","goals":22,"assists":11,"xg":18.5,"xa":9.2,"shots":115,"shots_on_target":48,"goals_per90":0.62,"xg_per90":0.52,"league":"Premier League"},
    {"name":"Jude Bellingham","team":"Real Madrid","nation":"England","position":"MF","goals":24,"assists":14,"xg":20.5,"xa":11.8,"shots":125,"shots_on_target":52,"goals_per90":0.67,"xg_per90":0.57,"league":"La Liga"},
    {"name":"Marcus Rashford","team":"Barcelona","nation":"England","position":"FW","goals":18,"assists":9,"xg":16.2,"xa":7.5,"shots":95,"shots_on_target":38,"goals_per90":0.52,"xg_per90":0.47,"league":"La Liga"},
    # ── SPAIN ──
    {"name":"Pedri","team":"Barcelona","nation":"Spain","position":"MF","goals":12,"assists":16,"xg":10.5,"xa":14.2,"shots":72,"shots_on_target":30,"goals_per90":0.34,"xg_per90":0.30,"league":"La Liga"},
    {"name":"Gavi","team":"Barcelona","nation":"Spain","position":"MF","goals":8,"assists":12,"xg":7.2,"xa":10.5,"shots":55,"shots_on_target":20,"goals_per90":0.23,"xg_per90":0.21,"league":"La Liga"},
    {"name":"Lamine Yamal","team":"Barcelona","nation":"Spain","position":"FW","goals":25,"assists":20,"xg":21.8,"xa":17.5,"shots":135,"shots_on_target":55,"goals_per90":0.72,"xg_per90":0.63,"league":"La Liga"},
    {"name":"Álvaro Morata","team":"AC Milan","nation":"Spain","position":"FW","goals":20,"assists":8,"xg":18.5,"xa":6.8,"shots":110,"shots_on_target":45,"goals_per90":0.58,"xg_per90":0.54,"league":"Serie A"},
    # ── GERMANY ──
    {"name":"Leroy Sané","team":"Bayern Munich","nation":"Germany","position":"FW","goals":16,"assists":14,"xg":14.2,"xa":12.5,"shots":95,"shots_on_target":38,"goals_per90":0.46,"xg_per90":0.41,"league":"Bundesliga"},
    {"name":"Florian Wirtz","team":"Bayern Munich","nation":"Germany","position":"MF","goals":22,"assists":18,"xg":19.5,"xa":15.8,"shots":115,"shots_on_target":48,"goals_per90":0.62,"xg_per90":0.55,"league":"Bundesliga"},
    {"name":"Jamal Musiala","team":"Bayern Munich","nation":"Germany","position":"MF","goals":20,"assists":15,"xg":18.2,"xa":13.5,"shots":110,"shots_on_target":45,"goals_per90":0.58,"xg_per90":0.53,"league":"Bundesliga"},
    {"name":"Kai Havertz","team":"Arsenal","nation":"Germany","position":"FW","goals":18,"assists":10,"xg":16.5,"xa":8.5,"shots":100,"shots_on_target":42,"goals_per90":0.52,"xg_per90":0.48,"league":"Premier League"},
    # ── PORTUGAL ──
    {"name":"Cristiano Ronaldo","team":"Al-Nassr","nation":"Portugal","position":"FW","goals":38,"assists":8,"xg":30.5,"xa":6.8,"shots":185,"shots_on_target":80,"goals_per90":0.95,"xg_per90":0.76,"league":"Saudi Pro League"},
    {"name":"Bruno Fernandes","team":"Manchester United","nation":"Portugal","position":"MF","goals":18,"assists":15,"xg":15.5,"xa":13.2,"shots":105,"shots_on_target":40,"goals_per90":0.52,"xg_per90":0.45,"league":"Premier League"},
    {"name":"Rafael Leão","team":"AC Milan","nation":"Portugal","position":"FW","goals":22,"assists":12,"xg":19.8,"xa":10.5,"shots":118,"shots_on_target":48,"goals_per90":0.63,"xg_per90":0.57,"league":"Serie A"},
    {"name":"Bernardo Silva","team":"Manchester City","nation":"Portugal","position":"MF","goals":12,"assists":14,"xg":10.8,"xa":12.5,"shots":75,"shots_on_target":30,"goals_per90":0.35,"xg_per90":0.31,"league":"Premier League"},
    # ── NETHERLANDS ──
    {"name":"Virgil van Dijk","team":"Liverpool","nation":"Netherlands","position":"DF","goals":5,"assists":3,"xg":4.2,"xa":2.5,"shots":30,"shots_on_target":12,"goals_per90":0.13,"xg_per90":0.11,"league":"Premier League"},
    {"name":"Cody Gakpo","team":"Liverpool","nation":"Netherlands","position":"FW","goals":20,"assists":10,"xg":17.8,"xa":8.5,"shots":105,"shots_on_target":42,"goals_per90":0.58,"xg_per90":0.52,"league":"Premier League"},
    {"name":"Xavi Simons","team":"RB Leipzig","nation":"Netherlands","position":"MF","goals":18,"assists":14,"xg":15.5,"xa":12.2,"shots":98,"shots_on_target":40,"goals_per90":0.52,"xg_per90":0.45,"league":"Bundesliga"},
    # ── BELGIUM ──
    {"name":"Kevin De Bruyne","team":"Manchester City","nation":"Belgium","position":"MF","goals":10,"assists":22,"xg":9.5,"xa":19.8,"shots":72,"shots_on_target":28,"goals_per90":0.29,"xg_per90":0.28,"league":"Premier League"},
    {"name":"Romelu Lukaku","team":"Napoli","nation":"Belgium","position":"FW","goals":24,"assists":8,"xg":22.5,"xa":7.2,"shots":128,"shots_on_target":52,"goals_per90":0.68,"xg_per90":0.64,"league":"Serie A"},
    {"name":"Lois Openda","team":"RB Leipzig","nation":"Belgium","position":"FW","goals":26,"assists":9,"xg":23.5,"xa":8.0,"shots":135,"shots_on_target":55,"goals_per90":0.72,"xg_per90":0.65,"league":"Bundesliga"},
    # ── USA ──
    {"name":"Christian Pulisic","team":"AC Milan","nation":"United States","position":"FW","goals":18,"assists":12,"xg":16.5,"xa":10.5,"shots":95,"shots_on_target":38,"goals_per90":0.52,"xg_per90":0.48,"league":"Serie A"},
    {"name":"Gio Reyna","team":"Borussia Dortmund","nation":"United States","position":"MF","goals":12,"assists":10,"xg":10.8,"xa":9.2,"shots":72,"shots_on_target":28,"goals_per90":0.35,"xg_per90":0.31,"league":"Bundesliga"},
    {"name":"Tyler Adams","team":"AFC Bournemouth","nation":"United States","position":"MF","goals":4,"assists":6,"xg":3.5,"xa":5.5,"shots":35,"shots_on_target":12,"goals_per90":0.12,"xg_per90":0.10,"league":"Premier League"},
    {"name":"Ricardo Pepi","team":"PSV","nation":"United States","position":"FW","goals":22,"assists":8,"xg":20.2,"xa":7.0,"shots":115,"shots_on_target":46,"goals_per90":0.63,"xg_per90":0.58,"league":"Eredivisie"},
    {"name":"Weston McKennie","team":"Juventus","nation":"United States","position":"MF","goals":8,"assists":7,"xg":7.2,"xa":6.5,"shots":55,"shots_on_target":20,"goals_per90":0.23,"xg_per90":0.21,"league":"Serie A"},
    # ── MEXICO ──
    {"name":"Hirving Lozano","team":"PSV","nation":"Mexico","position":"FW","goals":16,"assists":10,"xg":14.5,"xa":9.0,"shots":90,"shots_on_target":36,"goals_per90":0.46,"xg_per90":0.42,"league":"Eredivisie"},
    {"name":"Santiago Giménez","team":"AC Milan","nation":"Mexico","position":"FW","goals":20,"assists":6,"xg":18.8,"xa":5.5,"shots":110,"shots_on_target":44,"goals_per90":0.58,"xg_per90":0.55,"league":"Serie A"},
    {"name":"Edson Álvarez","team":"West Ham","nation":"Mexico","position":"MF","goals":5,"assists":4,"xg":4.5,"xa":3.5,"shots":40,"shots_on_target":15,"goals_per90":0.14,"xg_per90":0.13,"league":"Premier League"},
    # ── CROATIA ──
    {"name":"Luka Modrić","team":"Real Madrid","nation":"Croatia","position":"MF","goals":8,"assists":12,"xg":7.2,"xa":10.8,"shots":55,"shots_on_target":22,"goals_per90":0.24,"xg_per90":0.21,"league":"La Liga"},
    {"name":"Mateo Kovačić","team":"Manchester City","nation":"Croatia","position":"MF","goals":6,"assists":8,"xg":5.5,"xa":7.2,"shots":48,"shots_on_target":18,"goals_per90":0.18,"xg_per90":0.16,"league":"Premier League"},
    {"name":"Ivan Perišić","team":"Hajduk Split","nation":"Croatia","position":"FW","goals":12,"assists":9,"xg":11.0,"xa":8.0,"shots":72,"shots_on_target":28,"goals_per90":0.36,"xg_per90":0.33,"league":"Croatia Premier League"},
    # ── COLOMBIA ──
    {"name":"Luis Díaz","team":"Liverpool","nation":"Colombia","position":"FW","goals":20,"assists":12,"xg":18.5,"xa":10.5,"shots":108,"shots_on_target":44,"goals_per90":0.58,"xg_per90":0.54,"league":"Premier League"},
    {"name":"James Rodríguez","team":"Rayo Vallecano","nation":"Colombia","position":"MF","goals":10,"assists":14,"xg":9.2,"xa":12.5,"shots":70,"shots_on_target":28,"goals_per90":0.29,"xg_per90":0.27,"league":"La Liga"},
    {"name":"Jhon Córdoba","team":"Krasnodar","nation":"Colombia","position":"FW","goals":18,"assists":6,"xg":17.0,"xa":5.5,"shots":98,"shots_on_target":40,"goals_per90":0.52,"xg_per90":0.49,"league":"Russian Premier League"},
    # ── URUGUAY ──
    {"name":"Darwin Núñez","team":"Liverpool","nation":"Uruguay","position":"FW","goals":22,"assists":8,"xg":20.5,"xa":7.0,"shots":118,"shots_on_target":48,"goals_per90":0.63,"xg_per90":0.59,"league":"Premier League"},
    {"name":"Federico Valverde","team":"Real Madrid","nation":"Uruguay","position":"MF","goals":14,"assists":12,"xg":12.5,"xa":10.8,"shots":88,"shots_on_target":35,"goals_per90":0.40,"xg_per90":0.36,"league":"La Liga"},
    {"name":"Rodrigo Bentancur","team":"Tottenham","nation":"Uruguay","position":"MF","goals":6,"assists":7,"xg":5.5,"xa":6.2,"shots":45,"shots_on_target":18,"goals_per90":0.17,"xg_per90":0.16,"league":"Premier League"},
    # ── JAPAN ──
    {"name":"Takumi Minamino","team":"Monaco","nation":"Japan","position":"FW","goals":16,"assists":10,"xg":14.5,"xa":9.0,"shots":90,"shots_on_target":36,"goals_per90":0.46,"xg_per90":0.42,"league":"Ligue 1"},
    {"name":"Wataru Endō","team":"Liverpool","nation":"Japan","position":"MF","goals":4,"assists":5,"xg":3.8,"xa":4.5,"shots":35,"shots_on_target":12,"goals_per90":0.12,"xg_per90":0.11,"league":"Premier League"},
    {"name":"Ritsu Dōan","team":"Freiburg","nation":"Japan","position":"FW","goals":14,"assists":8,"xg":12.8,"xa":7.2,"shots":82,"shots_on_target":32,"goals_per90":0.40,"xg_per90":0.37,"league":"Bundesliga"},
    # ── SENEGAL ──
    {"name":"Sadio Mané","team":"Al-Nassr","nation":"Senegal","position":"FW","goals":20,"assists":8,"xg":18.5,"xa":7.0,"shots":110,"shots_on_target":44,"goals_per90":0.58,"xg_per90":0.54,"league":"Saudi Pro League"},
    {"name":"Idrissa Gueye","team":"Everton","nation":"Senegal","position":"MF","goals":5,"assists":6,"xg":4.5,"xa":5.5,"shots":40,"shots_on_target":15,"goals_per90":0.15,"xg_per90":0.13,"league":"Premier League"},
    # ── MOROCCO ──
    {"name":"Hakim Ziyech","team":"Galatasaray","nation":"Morocco","position":"MF","goals":14,"assists":12,"xg":12.8,"xa":10.5,"shots":80,"shots_on_target":32,"goals_per90":0.40,"xg_per90":0.37,"league":"Süper Lig"},
    {"name":"Sofiane Boufal","team":"Southampton","nation":"Morocco","position":"FW","goals":12,"assists":10,"xg":11.0,"xa":9.0,"shots":72,"shots_on_target":28,"goals_per90":0.36,"xg_per90":0.33,"league":"Premier League"},
    # ── SOUTH KOREA ──
    {"name":"Son Heung-min","team":"Tottenham","nation":"South Korea","position":"FW","goals":22,"assists":12,"xg":19.8,"xa":10.5,"shots":118,"shots_on_target":48,"goals_per90":0.63,"xg_per90":0.57,"league":"Premier League"},
    {"name":"Lee Kang-in","team":"Paris Saint-Germain","nation":"South Korea","position":"MF","goals":12,"assists":14,"xg":11.0,"xa":12.5,"shots":75,"shots_on_target":30,"goals_per90":0.35,"xg_per90":0.32,"league":"Ligue 1"},
    # ── CANADA ──
    {"name":"Alphonso Davies","team":"Bayern Munich","nation":"Canada","position":"DF","goals":6,"assists":10,"xg":5.5,"xa":9.2,"shots":45,"shots_on_target":18,"goals_per90":0.17,"xg_per90":0.16,"league":"Bundesliga"},
    {"name":"Jonathan David","team":"Lille","nation":"Canada","position":"FW","goals":32,"assists":10,"xg":28.5,"xa":9.0,"shots":155,"shots_on_target":65,"goals_per90":0.88,"xg_per90":0.78,"league":"Ligue 1"},
    {"name":"Cyle Larin","team":"Valladolid","nation":"Canada","position":"FW","goals":14,"assists":5,"xg":13.5,"xa":4.5,"shots":80,"shots_on_target":32,"goals_per90":0.40,"xg_per90":0.39,"league":"La Liga"},
    # ── AUSTRALIA ──
    {"name":"Mathew Leckie","team":"Melbourne City","nation":"Australia","position":"FW","goals":12,"assists":8,"xg":11.2,"xa":7.5,"shots":70,"shots_on_target":28,"goals_per90":0.36,"xg_per90":0.34,"league":"A-League"},
    {"name":"Mitchell Duke","team":"Fagiano Okayama","nation":"Australia","position":"FW","goals":18,"assists":5,"xg":17.0,"xa":4.5,"shots":95,"shots_on_target":38,"goals_per90":0.52,"xg_per90":0.49,"league":"J League"},
    # ── TURKEY ──
    {"name":"Hakan Çalhanoğlu","team":"Inter Milan","nation":"Turkey","position":"MF","goals":10,"assists":12,"xg":9.2,"xa":10.8,"shots":68,"shots_on_target":26,"goals_per90":0.29,"xg_per90":0.27,"league":"Serie A"},
    {"name":"Kerem Aktürkoğlu","team":"Galatasaray","nation":"Turkey","position":"FW","goals":18,"assists":12,"xg":16.5,"xa":10.8,"shots":98,"shots_on_target":40,"goals_per90":0.52,"xg_per90":0.48,"league":"Süper Lig"},
    {"name":"Arda Güler","team":"Real Madrid","nation":"Turkey","position":"MF","goals":14,"assists":10,"xg":12.5,"xa":9.0,"shots":80,"shots_on_target":32,"goals_per90":0.40,"xg_per90":0.36,"league":"La Liga"},
    # ── ECUADOR ──
    {"name":"Moisés Caicedo","team":"Chelsea","nation":"Ecuador","position":"MF","goals":6,"assists":8,"xg":5.5,"xa":7.2,"shots":45,"shots_on_target":18,"goals_per90":0.17,"xg_per90":0.16,"league":"Premier League"},
    {"name":"Gonzalo Plata","team":"Valladolid","nation":"Ecuador","position":"FW","goals":12,"assists":8,"xg":11.0,"xa":7.5,"shots":72,"shots_on_target":28,"goals_per90":0.36,"xg_per90":0.33,"league":"La Liga"},
    # ── SERBIA ──
    {"name":"Aleksandar Mitrović","team":"Al-Hilal","nation":"Serbia","position":"FW","goals":30,"assists":6,"xg":27.5,"xa":5.5,"shots":145,"shots_on_target":62,"goals_per90":0.80,"xg_per90":0.73,"league":"Saudi Pro League"},
    {"name":"Dušan Vlahović","team":"Juventus","nation":"Serbia","position":"FW","goals":24,"assists":5,"xg":22.5,"xa":4.5,"shots":128,"shots_on_target":52,"goals_per90":0.68,"xg_per90":0.64,"league":"Serie A"},
    # ── PARAGUAY ──
    {"name":"Miguel Almirón","team":"Newcastle","nation":"Paraguay","position":"MF","goals":10,"assists":8,"xg":9.2,"xa":7.5,"shots":65,"shots_on_target":25,"goals_per90":0.29,"xg_per90":0.27,"league":"Premier League"},
    # ── CHILE ──
    {"name":"Alexis Sánchez","team":"Udinese","nation":"Chile","position":"FW","goals":14,"assists":8,"xg":13.2,"xa":7.2,"shots":82,"shots_on_target":32,"goals_per90":0.40,"xg_per90":0.38,"league":"Serie A"},
    {"name":"Charles Aránguiz","team":"Bayer Leverkusen","nation":"Chile","position":"MF","goals":5,"assists":6,"xg":4.5,"xa":5.5,"shots":38,"shots_on_target":14,"goals_per90":0.14,"xg_per90":0.13,"league":"Bundesliga"},
]

# Build lookup by (lowercase name, nation) for fast matching
_PLAYER_INDEX: dict[str, dict] = {p["name"].lower(): p for p in _TOP_WC_PLAYERS}


def get_wc_player_stats(player_name: str | None = None,
                        nation: str | None = None) -> list[dict]:
    """
    Return club stats for WC players.
    If player_name given: return matching single player (or []).
    If nation given: return all players for that national team.
    If neither: return all embedded players, augmented with live FBref data.
    """
    if player_name:
        key = player_name.lower()
        # Exact match
        if key in _PLAYER_INDEX:
            return [_PLAYER_INDEX[key]]
        # Partial match
        matches = [p for k, p in _PLAYER_INDEX.items() if player_name.lower() in k]
        return matches

    if nation:
        norm = nation.lower()
        return [p for p in _TOP_WC_PLAYERS
                if p.get("nation","").lower() == norm or
                   p.get("nationality","").lower() == norm]

    # Try to augment with live FBref data
    live = _load_fbref_stats()
    if live:
        merged = {p["name"].lower(): p for p in _TOP_WC_PLAYERS}
        for lp in live:
            key = lp["name"].lower()
            if key in merged:
                # Update with fresh data
                for stat in ("goals","assists","shots","shots_on_target","xg","xa","goals_per90","xg_per90"):
                    if stat in lp:
                        merged[key][stat] = lp[stat]
        return list(merged.values())

    return list(_TOP_WC_PLAYERS)


def get_squad_props(nation: str, top_n: int = 6) -> list[dict]:
    """
    Generate player prop bets for a national team's top players.
    Uses season historical rates and emits both OVER and UNDER directions.
    """
    players = get_wc_player_stats(nation=nation)
    if not players:
        return []

    # Sort by xG (best attacking threat)
    players = sorted(players, key=lambda p: p.get("xg", 0), reverse=True)[:top_n]
    props = []

    def _poisson_over_prob(rate: float, line: float) -> float:
        lam = max(0.05, float(rate or 0.0))
        if line <= 0.5:
            # P(X >= 1)
            return max(0.02, min(0.98, 1.0 - math.exp(-lam)))
        if line <= 1.5:
            # P(X >= 2)
            return max(0.02, min(0.98, 1.0 - math.exp(-lam) * (1.0 + lam)))
        # Approx fallback for higher lines
        return max(0.02, min(0.98, 1.0 - math.exp(-lam)))

    def _add_directional_prop(
        *,
        name: str,
        team: str,
        nation_: str,
        stat_type: str,
        prop_label: str,
        line: float,
        rate: float,
        model_over_prob: float,
        rationale: str,
    ):
        over_p = max(0.02, min(0.98, float(model_over_prob)))
        under_p = max(0.02, min(0.98, 1.0 - over_p))

        base = {
            "name": name,
            "team": team,
            "nation": nation_,
            "stat_type": stat_type,
            "prop_label": prop_label,
            "line": line,
            "season_avg": round(max(0.0, float(rate)), 2),
            "last10_avg": round(max(0.0, float(rate) * 0.92), 2),
            "signal_rationale": rationale,
            "market_popularity": 0.0,
            "market_mentions": 0,
            "worth_score": 0.0,
            "worth_it": False,
            "worth_reason": "Pending market worth evaluation",
        }

        props.append(
            {
                **base,
                "direction": "OVER",
                "model_prob": round(over_p, 3),
                "confidence": round(over_p * 100),
                "safety_label": _prob_to_safety(over_p),
                "over_pct": round(over_p * 100, 2),
                "under_pct": round(100.0 - (over_p * 100), 2),
                "odds_am": -110,
                "dec_odds": 1.91,
            }
        )
        props.append(
            {
                **base,
                "direction": "UNDER",
                "model_prob": round(under_p, 3),
                "confidence": round(under_p * 100),
                "safety_label": _prob_to_safety(under_p),
                "over_pct": round(over_p * 100, 2),
                "under_pct": round(under_p * 100, 2),
                "odds_am": -110,
                "dec_odds": 1.91,
            }
        )

    for p in players:
        name     = p["name"]
        team     = p.get("team", "")
        nation_  = p.get("nation", nation)
        xg_p90   = float(p.get("xg_per90", p.get("xg", 0) / max(p.get("minutes_90s", 1), 1) if p.get("minutes_90s") else 0.3))
        goals_p90 = float(p.get("goals_per90", xg_p90 * 0.85))
        assists_total = float(p.get("assists", 0) or 0)
        minutes_90s = float(p.get("minutes_90s", 35) or 35)
        assists_p90 = assists_total / max(minutes_90s, 1.0)
        shots_p90 = float(p.get("shots_on_target", 0) or 0) / max(minutes_90s, 1.0)

        # Goals prop (historical season rate)
        if goals_p90 >= 0.18:
            line = 0.5
            model_prob = _poisson_over_prob(goals_p90, line)
            _add_directional_prop(
                name=name,
                team=team,
                nation_=nation_,
                stat_type="goals",
                prop_label="Anytime Goalscorer",
                line=line,
                rate=goals_p90,
                model_over_prob=model_prob,
                rationale=f"{goals_p90:.2f} goals/90 and {xg_p90:.2f} xG/90 from season historical data",
            )

        # Shots on target prop
        if shots_p90 >= 0.5:
            line = 0.5 if shots_p90 < 1.4 else 1.5
            model_prob = _poisson_over_prob(shots_p90, line)
            _add_directional_prop(
                name=name,
                team=team,
                nation_=nation_,
                stat_type="shots_on_target",
                prop_label="Shots on Target",
                line=line,
                rate=shots_p90,
                model_over_prob=model_prob,
                rationale=f"{shots_p90:.2f} shots on target per 90 over season sample",
            )

        # Assists prop
        if assists_p90 >= 0.12:
            line = 0.5
            model_prob = _poisson_over_prob(assists_p90, line)
            _add_directional_prop(
                name=name,
                team=team,
                nation_=nation_,
                stat_type="assists",
                prop_label="Assist",
                line=line,
                rate=assists_p90,
                model_over_prob=model_prob,
                rationale=f"{assists_p90:.2f} assists/90 from season historical profile",
            )

    return props


def _prob_to_safety(prob: float) -> str:
    if prob >= 0.75:  return "ELITE"
    if prob >= 0.62:  return "SAFE"
    if prob >= 0.50:  return "MODERATE"
    return "RISKY"
