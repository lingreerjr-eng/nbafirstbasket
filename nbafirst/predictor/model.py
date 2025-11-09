import json
import os
import logging
from typing import Dict, List, Tuple
from datetime import datetime
import random

logger = logging.getLogger(__name__)

class FirstBasketPredictor:
    def __init__(self):
        self.data_dir = "data"
        self.model_data = {}
        self.trained = False
    
    def train_model(self):
        """Train the first basket prediction model"""
        try:
            logger.info("Training first basket prediction model")
            
            # Load all player data
            players_data = self._load_players_data()
            
            # Load all game data
            games_data = self._load_games_data()
            
            # Process data and create model
            self.model_data = self._create_model(players_data, games_data)
            self.trained = True
            
            logger.info("Model training completed")
        except Exception as e:
            logger.error(f"Error training model: {e}")
    
    def _load_players_data(self) -> List[Dict]:
        """Load all players data from files"""
        players_data = []
        
        for filename in os.listdir(self.data_dir):
            if filename.startswith("players_") and filename.endswith(".json"):
                file_path = os.path.join(self.data_dir, filename)
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    players_data.extend(data)
        
        return players_data
    
    def _load_games_data(self) -> List[Dict]:
        """Load all games data from files"""
        games_data = []
        
        for filename in os.listdir(self.data_dir):
            if filename.startswith("games_") and filename.endswith(".json"):
                file_path = os.path.join(self.data_dir, filename)
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    games_data.extend(data)
        
        return games_data
    
    def _create_model(self, players_data: List[Dict], games_data: List[Dict]) -> Dict:
        """Create prediction model from data"""
        # In a real implementation, this would be a more sophisticated model
        # For this example, we'll create a simple probability model
        
        # Create player lookup
        player_lookup = {player["name"]: player for player in players_data}
        
        # Calculate team-based probabilities
        team_players = {}
        for player in players_data:
            team = player["team"]
            if team not in team_players:
                team_players[team] = []
            team_players[team].append(player)
        
        # Sort players by first basket probability within each team
        for team in team_players:
            team_players[team].sort(key=lambda x: x["first_basket_probability"], reverse=True)
        
        return {
            "players": player_lookup,
            "teams": team_players,
            "last_updated": datetime.now().isoformat()
        }
    
    def predict_first_basket(self, home_team: str, away_team: str) -> Dict:
        """Predict who will get the first basket in a game"""
        if not self.trained:
            raise Exception("Model not trained yet")
        
        try:
            # Get players for each team
            home_players = self.model_data.get("teams", {}).get(home_team, [])
            away_players = self.model_data.get("teams", {}).get(away_team, [])
            
            if not home_players and not away_players:
                # If no data, return a random prediction
                logger.warning(f"No player data for teams {home_team} vs {away_team}")
                return {
                    "player": "Unknown Player",
                    "team": home_team,
                    "probability": 0.5,
                    "confidence": "low"
                }
            
            # Simple weighted selection based on first basket probability
            all_players = home_players + away_players
            probabilities = [p["first_basket_probability"] for p in all_players]
            
            # Normalize probabilities
            total_prob = sum(probabilities)
            if total_prob > 0:
                probabilities = [p/total_prob for p in probabilities]
            
            # Select player based on probabilities
            selected_player = random.choices(all_players, probabilities)[0]
            
            return {
                "player": selected_player["name"],
                "team": selected_player["team"],
                "probability": selected_player["first_basket_probability"],
                "confidence": "high" if selected_player["games_played"] > 50 else "medium"
            }
        except Exception as e:
            logger.error(f"Error predicting first basket: {e}")
            return {
                "player": "Unknown Player",
                "team": home_team,
                "probability": 0.5,
                "confidence": "low"
            }
