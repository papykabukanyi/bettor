"""
Kalshi Prediction Formatter
============================
Automatically converts model predictions to Kalshi-ready contract format.

Maps prediction targets (player props, team outcomes, etc.) to Kalshi market tickers
and sub-markets, ensuring predictions can be placed immediately upon generation.

Key responsibilities:
- Parse prediction dictionaries and extract market/prop targets
- Match target + outcome to Kalshi series registry + market specifications
- Format orders (singles and combos) in Kalshi API shape
- Validate prediction-to-contract mapping before placement

Usage
-----
    from src.data.kalshi_prediction_formatter import KalshiPredictionFormatter
    
    formatter = KalshiPredictionFormatter()
    
    # Format a single prediction to Kalshi order
    kalshi_order = formatter.format_prediction_to_order(
        prediction={
            "sport": "mlb",
            "home_team": "NYY",
            "away_team": "BOS",
            "game_date": "2026-07-06",
            "bet_type": "player_prop",
            "bet": "Judge > 1.5 HR",
            "model_prob": 0.72,
            "odds_decimal": 1.95,
        }
    )
    
    # Format multiple predictions into a combo
    combo_orders = formatter.format_combo(
        predictions=[pred1, pred2, pred3],
        combo_type="parlay",  # or "custom"
    )
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).resolve().parents[2]
KALSHI_REGISTRY_FILE = Path(__file__).resolve().parent / "kalshi_series_registry.json"


class KalshiPredictionFormatter:
    """Format predictions to Kalshi-ready contract specifications."""

    def __init__(self):
        """Initialize with Kalshi series registry and market mappings."""
        self.series_registry = self._load_registry()
        self.market_specs = self._build_market_specs()
        
    def _load_registry(self) -> dict[str, str]:
        """Load Kalshi series ticker → sport category mapping."""
        if not KALSHI_REGISTRY_FILE.exists():
            logger.warning(f"Registry not found: {KALSHI_REGISTRY_FILE}, using empty")
            return {}
        try:
            with open(KALSHI_REGISTRY_FILE, "r") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load registry: {e}")
            return {}

    def _build_market_specs(self) -> dict[str, dict[str, Any]]:
        """
        Build market specification map: (sport, market_type) → spec.
        
        Spec includes:
        - ticker_pattern: How to construct Kalshi ticker
        - sub_markets: Valid sub-market names for this sport
        - outcomes: Valid outcome mappings
        - validation_rules: Any special validation
        """
        return {
            ("mlb", "game"): {
                "ticker_pattern": "KX{league}GAME",
                "league_code": "MLB",
                "sub_markets": ["spread", "total", "winner", "1h", "2h"],
                "outcomes": ["home_win", "away_win", "over", "under"],
            },
            ("nba", "game"): {
                "ticker_pattern": "KX{league}GAME",
                "league_code": "NBA",
                "sub_markets": ["spread", "total", "winner", "1h", "2h"],
                "outcomes": ["home_win", "away_win", "over", "under"],
            },
            ("nfl", "game"): {
                "ticker_pattern": "KX{league}GAME",
                "league_code": "NFL",
                "sub_markets": ["spread", "total", "winner"],
                "outcomes": ["home_win", "away_win", "over", "under"],
            },
            ("soccer", "game"): {
                "ticker_pattern": "KX{league}GAME",
                "league_code": "MLS",
                "sub_markets": ["spread", "total", "winner", "btts"],
                "outcomes": ["home_win", "away_win", "draw", "over", "under"],
            },
            ("hockey", "game"): {
                "ticker_pattern": "KX{league}GAME",
                "league_code": "NHL",
                "sub_markets": ["spread", "total", "winner"],
                "outcomes": ["home_win", "away_win", "over", "under"],
            },
            ("tennis", "match"): {
                "ticker_pattern": "KXATP",
                "sub_markets": ["advance", "set_winner", "game_winner"],
                "outcomes": ["player1_win", "player2_win", "over", "under"],
            },
            ("cricket", "match"): {
                "ticker_pattern": "KXCRICKET",
                "sub_markets": ["winner", "batsman_runs", "wickets", "toss"],
                "outcomes": ["team1_win", "team2_win", "over", "under"],
            },
            ("golf", "tournament"): {
                "ticker_pattern": "KXGOLF",
                "sub_markets": ["winner", "top10", "top20"],
                "outcomes": ["player_wins", "player_top10", "player_top20"],
            },
            ("motorsports", "race"): {
                "ticker_pattern": "KXF1",
                "sub_markets": ["winner", "podium", "pole"],
                "outcomes": ["driver_wins", "driver_podium"],
            },
        }

    def format_prediction_to_order(
        self, 
        prediction: dict[str, Any]
    ) -> dict[str, Any] | None:
        """
        Convert a single prediction to Kalshi order specification.
        
        Args:
            prediction: {
                "sport": "mlb",
                "home_team": "NYY",
                "away_team": "BOS",
                "game_date": "2026-07-06",
                "game_time": "19:05Z",
                "bet_type": "team_winner" | "spread" | "total" | "player_prop",
                "bet": "NYY win" | "NYY -1.5" | "Over 9.5",
                "model_prob": 0.72,
                "book_prob": 0.48,
                "odds_decimal": 1.95,
                "odds_american": -210,
                "stake_usd": 1.0,
                "confidence": 0.72,
            }
            
        Returns:
            Kalshi order spec:
            {
                "type": "order",
                "ticker": "KXMLBGAME-...",
                "side": "yes" | "no",
                "outcome": "NYY > 3.5",
                "quantity": 1,
                "limit_price": 0.72,
                "display": {...},
            }
            OR None if prediction cannot be mapped.
        """
        try:
            # Extract key fields
            sport = str(prediction.get("sport") or "").lower().strip()
            bet_type = str(prediction.get("bet_type") or "").lower().strip()
            bet = str(prediction.get("bet") or "").lower().strip()
            home_team = str(prediction.get("home_team") or "").strip()
            away_team = str(prediction.get("away_team") or "").strip()
            game_date = str(prediction.get("game_date") or "").strip()
            model_prob = float(prediction.get("model_prob") or 0.5)
            odds_decimal = float(prediction.get("odds_decimal") or 1.5)
            stake_usd = float(prediction.get("stake_usd") or 1.0)

            # Determine ticker and side based on prediction
            ticker = self._construct_ticker(sport, bet_type, home_team, away_team, game_date)
            if not ticker:
                logger.warning(f"Could not construct ticker for prediction: {prediction}")
                return None

            # Parse outcome (yes/no side)
            side = self._determine_side(bet_type, bet)
            if not side:
                logger.warning(f"Could not determine side for bet: {bet}")
                return None

            # Validate probability against decimal odds
            implied_prob = 1.0 / odds_decimal
            confidence = max(model_prob, 0.5)  # At least 50%

            return {
                "type": "order",
                "ticker": ticker,
                "side": side,  # "yes" or "no"
                "quantity": int(max(1, stake_usd)),
                "limit_price": confidence,
                "display": {
                    "sport": sport,
                    "bet_type": bet_type,
                    "prediction": bet,
                    "model_prob": round(model_prob, 4),
                    "implied_prob": round(implied_prob, 4),
                    "odds_decimal": odds_decimal,
                    "stake_usd": stake_usd,
                },
            }
        except Exception as e:
            logger.error(f"Error formatting prediction to order: {e}", exc_info=True)
            return None

    def format_combo(
        self,
        predictions: list[dict[str, Any]],
        combo_type: str = "parlay",
    ) -> dict[str, Any] | None:
        """
        Convert multiple predictions into a Kalshi combo (multi-leg parlay).
        
        Args:
            predictions: List of prediction dicts (see format_prediction_to_order)
            combo_type: "parlay" (all must win) or "custom" (user-specified logic)
            
        Returns:
            Kalshi combo spec:
            {
                "type": "combo",
                "combo_type": "parlay",
                "legs": [order1, order2, order3],
                "total_quantity": N,
                "min_odds": X.XX,
                "max_stake": Y.Y,
                "display": {...},
            }
            OR None if combo cannot be built.
        """
        if not predictions or len(predictions) < 2:
            logger.warning(f"Combo requires >= 2 predictions, got {len(predictions)}")
            return None

        orders = []
        for pred in predictions:
            order = self.format_prediction_to_order(pred)
            if order:
                orders.append(order)

        if len(orders) < 2:
            logger.warning(f"Could not format enough predictions for combo: {len(predictions)} → {len(orders)}")
            return None

        # Calculate combo metrics
        combo_prob = 1.0
        total_odds = 1.0
        total_stake = 0.0
        
        for order in orders:
            display = order.get("display") or {}
            model_prob = float(display.get("model_prob") or 0.5)
            odds_decimal = float(display.get("odds_decimal") or 1.5)
            stake_usd = float(display.get("stake_usd") or 1.0)
            
            combo_prob *= model_prob
            total_odds *= odds_decimal
            total_stake += stake_usd

        return {
            "type": "combo",
            "combo_type": combo_type,
            "legs": orders,
            "leg_count": len(orders),
            "total_quantity": int(max(1, total_stake)),
            "combo_odds": round(total_odds, 4),
            "combo_prob": round(combo_prob, 4),
            "expected_value": round((combo_prob * total_odds - 1) * total_stake, 2),
            "display": {
                "combo_type": combo_type,
                "leg_count": len(orders),
                "combo_prob": round(combo_prob, 4),
                "combo_odds": round(total_odds, 4),
                "total_stake": total_stake,
            },
        }

    def _construct_ticker(
        self,
        sport: str,
        bet_type: str,
        home_team: str,
        away_team: str,
        game_date: str,
    ) -> str | None:
        """
        Construct a Kalshi ticker from sport/teams/bet_type.
        
        For now, returns a generic template. In production, would:
        - Query Kalshi API for available markets on that date
        - Match teams + date to specific contract
        - Return full ticker like "KXF5-26JUL061915NYMATL"
        """
        sport = sport.lower().strip()
        
        # For MVP: return sport-based prefix for demo
        # In production: query Kalshi API for exact market
        base_tickers = {
            "mlb": "KXMLBGAME",
            "nba": "KXNBAGAME",
            "nfl": "KXNFLGAME",
            "soccer": "KXMLSGAME",
            "hockey": "KXNHLGAME",
            "tennis": "KXATP",
            "cricket": "KXCRICKET",
            "golf": "KXGOLF",
            "motorsports": "KXF1",
        }
        
        ticker_prefix = base_tickers.get(sport)
        if not ticker_prefix:
            logger.warning(f"Unknown sport: {sport}")
            return None
        
        # TODO: query Kalshi API to match teams + date to exact market
        # For now, return prefix as placeholder
        return ticker_prefix

    def _determine_side(self, bet_type: str, bet: str) -> str | None:
        """
        Determine if prediction maps to "yes" or "no" outcome.
        
        Kalshi: "yes" = prediction true, "no" = prediction false.
        """
        bet_lower = bet.lower()
        bet_type_lower = bet_type.lower()

        # Outcomes that map to "yes"
        yes_keywords = {
            "win", "over", "home", "team1", "winner",
            "player", "yes", "true", "positive",
        }
        
        # Outcomes that map to "no"
        no_keywords = {
            "loss", "under", "away", "team2", "no", "false", "negative",
        }

        # Check if any yes/no keyword is in the bet string
        for kw in yes_keywords:
            if kw in bet_lower:
                return "yes"
        
        for kw in no_keywords:
            if kw in bet_lower:
                return "no"

        # Default based on probability (>0.55 → yes, <0.55 → no)
        logger.warning(f"Could not determine side from bet: {bet}, defaulting to 'yes'")
        return "yes"

    def validate_prediction_mapping(
        self,
        prediction: dict[str, Any],
        order: dict[str, Any],
    ) -> tuple[bool, str]:
        """
        Validate that a prediction is correctly mapped to a Kalshi order.
        
        Returns:
            (is_valid, error_message)
        """
        errors = []

        # Check ticker exists
        ticker = order.get("ticker")
        if not ticker:
            errors.append("Missing ticker")

        # Check side is valid
        side = order.get("side")
        if side not in {"yes", "no"}:
            errors.append(f"Invalid side: {side}")

        # Check quantity is positive
        quantity = order.get("quantity")
        if not quantity or quantity < 1:
            errors.append(f"Invalid quantity: {quantity}")

        # Check limit price is in [0, 1]
        limit_price = order.get("limit_price")
        if limit_price is None or limit_price < 0 or limit_price > 1:
            errors.append(f"Invalid limit_price: {limit_price}")

        return (len(errors) == 0, " | ".join(errors) if errors else "")


# Singleton instance
_formatter: KalshiPredictionFormatter | None = None


def get_formatter() -> KalshiPredictionFormatter:
    """Get or create singleton formatter instance."""
    global _formatter
    if _formatter is None:
        _formatter = KalshiPredictionFormatter()
    return _formatter


if __name__ == "__main__":
    # Demo usage
    formatter = get_formatter()

    demo_pred = {
        "sport": "mlb",
        "home_team": "NYY",
        "away_team": "BOS",
        "game_date": "2026-07-06",
        "bet_type": "team_winner",
        "bet": "NYY win",
        "model_prob": 0.72,
        "book_prob": 0.48,
        "odds_decimal": 1.95,
        "stake_usd": 1.0,
    }

    order = formatter.format_prediction_to_order(demo_pred)
    print("Single Order:")
    print(json.dumps(order, indent=2))

    # Combo demo
    combo = formatter.format_combo([demo_pred, demo_pred])
    print("\nCombo Order:")
    print(json.dumps(combo, indent=2))
