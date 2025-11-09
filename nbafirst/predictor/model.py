import json
import logging
import os
import random
from collections import defaultdict
from datetime import datetime
from typing import Dict, List

logger = logging.getLogger(__name__)


class FirstBasketPredictor:
    """Lightweight model that ranks players by historical first baskets."""

    def __init__(self) -> None:
        self.data_dir = "data"
        self.model_data: Dict = {}
        self.trained = False

    # ------------------------------------------------------------------
    # Training pipeline
    # ------------------------------------------------------------------
    def train_model(self) -> None:
        """Load scraped data and build probability tables."""
        try:
            logger.info("Training first basket prediction model")
            players_data = self._load_players_data()
            games_data = self._load_games_data()
            self.model_data = self._create_model(players_data, games_data)
            self.trained = True
            logger.info("Model training completed")
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("Error training model: %s", exc)
            self.trained = False

    # ------------------------------------------------------------------
    # Data loading helpers
    # ------------------------------------------------------------------
    def _load_players_data(self) -> List[Dict]:
        players_data: List[Dict] = []
        if not os.path.isdir(self.data_dir):
            logger.warning("Data directory %s does not exist", self.data_dir)
            return players_data

        for filename in os.listdir(self.data_dir):
            if not (filename.startswith("players_") and filename.endswith(".json")):
                continue

            path = os.path.join(self.data_dir, filename)
            try:
                with open(path, "r", encoding="utf-8") as handle:
                    payload = json.load(handle)
            except (OSError, json.JSONDecodeError) as exc:
                logger.warning("Failed to read %s: %s", path, exc)
                continue

            if isinstance(payload, list):
                players_data.extend(payload)

        return players_data

    def _load_games_data(self) -> List[Dict]:
        games_data: List[Dict] = []
        if not os.path.isdir(self.data_dir):
            return games_data

        for filename in os.listdir(self.data_dir):
            if not (filename.startswith("games_") and filename.endswith(".json")):
                continue

            path = os.path.join(self.data_dir, filename)
            try:
                with open(path, "r", encoding="utf-8") as handle:
                    payload = json.load(handle)
            except (OSError, json.JSONDecodeError) as exc:
                logger.warning("Failed to read %s: %s", path, exc)
                continue

            if isinstance(payload, list):
                games_data.extend(payload)

        return games_data

    # ------------------------------------------------------------------
    # Model construction
    # ------------------------------------------------------------------
    def _create_model(self, players_data: List[Dict], games_data: List[Dict]) -> Dict:
        team_game_counts = self._calculate_team_game_counts(games_data)

        normalised_players: List[Dict] = []
        for player in players_data:
            cleaned = self._normalise_player_record(player, team_game_counts)
            if cleaned:
                normalised_players.append(cleaned)

        player_lookup = {
            f"{player['team']}::{player['name']}": player for player in normalised_players
        }

        team_players: Dict[str, List[Dict]] = defaultdict(list)
        for player in normalised_players:
            team_players[player["team"]].append(player)

        for team, roster in team_players.items():
            roster.sort(key=lambda item: item["first_basket_probability"], reverse=True)
            logger.debug("Prepared %d players for team %s", len(roster), team)

        return {
            "players": player_lookup,
            "teams": dict(team_players),
            "last_updated": datetime.now().isoformat(),
        }

    def _calculate_team_game_counts(self, games_data: List[Dict]) -> Dict[str, int]:
        counts: Dict[str, int] = defaultdict(int)
        for game in games_data:
            home = game.get("home_team") or game.get("homeTeam")
            away = game.get("away_team") or game.get("awayTeam")
            if home:
                counts[home] += 1
            if away:
                counts[away] += 1
        return counts

    def _normalise_player_record(self, player: Dict, team_game_counts: Dict[str, int]) -> Dict:
        name = player.get("name")
        team = player.get("team")
        if not name or not team:
            return {}

        games_played = int(player.get("games_played") or 0)
        games_played = max(games_played, team_game_counts.get(team, 0))
        first_baskets = int(player.get("first_baskets") or 0)

        probability = player.get("first_basket_probability")
        if probability is None or probability <= 0:
            if games_played > 0:
                probability = first_baskets / games_played if first_baskets else 0.0
            else:
                probability = 0.0

        # Apply a small prior so that probability is never zero
        if probability <= 0:
            denominator = max(games_played, 1)
            probability = 1.0 / denominator

        record = dict(player)
        record["games_played"] = games_played
        record["first_baskets"] = first_baskets
        record["first_basket_probability"] = float(probability)
        record.setdefault("player_id", f"{team}_{name}")
        return record

    # ------------------------------------------------------------------
    # Prediction interface
    # ------------------------------------------------------------------
    def predict_first_basket(self, home_team: str, away_team: str) -> Dict:
        if not self.trained:
            raise RuntimeError("Model not trained yet")

        try:
            home_players = list(self.model_data.get("teams", {}).get(home_team, []))
            away_players = list(self.model_data.get("teams", {}).get(away_team, []))

            if not home_players and not away_players:
                logger.warning("No player data for %s vs %s", home_team, away_team)
                return {
                    "player": "Unknown Player",
                    "team": home_team,
                    "probability": 0.5,
                    "confidence": "low",
                }

            all_players = home_players + away_players
            probabilities = [max(player["first_basket_probability"], 0.0) for player in all_players]
            total_probability = sum(probabilities)

            if total_probability <= 0:
                probabilities = [1.0 / len(all_players)] * len(all_players)
            else:
                probabilities = [value / total_probability for value in probabilities]

            selected_player = random.choices(all_players, probabilities)[0]
            confidence = self._confidence_from_samples(selected_player)

            return {
                "player": selected_player["name"],
                "team": selected_player["team"],
                "probability": selected_player["first_basket_probability"],
                "confidence": confidence,
            }
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("Error predicting first basket: %s", exc)
            return {
                "player": "Unknown Player",
                "team": home_team,
                "probability": 0.5,
                "confidence": "low",
            }

    def _confidence_from_samples(self, player: Dict) -> str:
        games_played = player.get("games_played", 0)
        if games_played >= 60:
            return "high"
        if games_played >= 30:
            return "medium"
        return "low"
