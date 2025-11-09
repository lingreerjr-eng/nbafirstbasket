import requests
import json
import os
from datetime import datetime, timedelta
import logging
from typing import Dict, List

logger = logging.getLogger(__name__)

class NBAScraper:
    def __init__(self):
        self.data_dir = "data"
        self.base_url = "https://data.nba.net"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        # Create data directory if it doesn't exist
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
    
    def scrape_season_data(self):
        """Scrape data for current and last season"""
        try:
            # Get current season
            current_year = datetime.now().year
            if datetime.now().month < 10:  # NBA season starts in October
                current_season = f"{current_year-1}-{current_year}"
            else:
                current_season = f"{current_year}-{current_year+1}"
            
            # Get last season
            last_year = current_year - 1
            last_season = f"{last_year-1}-{last_year}"
            
            logger.info(f"Scraping data for seasons: {last_season}, {current_season}")
            
            # Scrape both seasons
            self._scrape_season(last_season)
            self._scrape_season(current_season)
            
            logger.info("Season data scraping completed")
        except Exception as e:
            logger.error(f"Error scraping season data: {e}")
    
    def _scrape_season(self, season: str):
        """Scrape data for a specific season"""
        try:
            # In a real implementation, we would scrape actual NBA data
            # For this example, we'll generate sample data
            
            # Generate sample player data
            players_data = self._generate_sample_players()
            
            # Generate sample game data
            games_data = self._generate_sample_games(season)
            
            # Save data to files
            players_file = os.path.join(self.data_dir, f"players_{season}.json")
            games_file = os.path.join(self.data_dir, f"games_{season}.json")
            
            with open(players_file, 'w') as f:
                json.dump(players_data, f, indent=2)
                
            with open(games_file, 'w') as f:
                json.dump(games_data, f, indent=2)
                
            logger.info(f"Saved data for season {season}")
        except Exception as e:
            logger.error(f"Error scraping season {season}: {e}")
    
    def _generate_sample_players(self) -> List[Dict]:
        """Generate sample player data"""
        return [
            {
                "player_id": 1,
                "name": "LeBron James",
                "team": "LAL",
                "position": "F",
                "avg_first_basket_time": 15.2,
                "first_basket_probability": 0.08,
                "games_played": 75
            },
            {
                "player_id": 2,
                "name": "Stephen Curry",
                "team": "GSW",
                "position": "G",
                "avg_first_basket_time": 12.8,
                "first_basket_probability": 0.12,
                "games_played": 72
            },
            {
                "player_id": 3,
                "name": "Kevin Durant",
                "team": "BKN",
                "position": "F",
                "avg_first_basket_time": 18.5,
                "first_basket_probability": 0.06,
                "games_played": 68
            },
            {
                "player_id": 4,
                "name": "Giannis Antetokounmpo",
                "team": "MIL",
                "position": "F",
                "avg_first_basket_time": 14.3,
                "first_basket_probability": 0.09,
                "games_played": 70
            },
            {
                "player_id": 5,
                "name": "Luka Doncic",
                "team": "DAL",
                "position": "G",
                "avg_first_basket_time": 16.7,
                "first_basket_probability": 0.07,
                "games_played": 65
            }
        ]
    
    def _generate_sample_games(self, season: str) -> List[Dict]:
        """Generate sample game data"""
        games = []
        teams = ["LAL", "GSW", "BKN", "MIL", "DAL", "BOS", "MIA", "PHX"]
        
        # Generate 50 sample games
        for i in range(50):
            date = (datetime.now() - timedelta(days=i*2)).strftime("%Y-%m-%d")
            home_team = teams[i % len(teams)]
            away_team = teams[(i + 1) % len(teams)]
            
            game = {
                "game_id": f"{season}_{i}",
                "date": date,
                "home_team": home_team,
                "away_team": away_team,
                "first_basket_player": "LeBron James" if i % 2 == 0 else "Stephen Curry",
                "first_basket_time": 12.5 + (i % 20),
                "first_basket_team": home_team if i % 2 == 0 else away_team
            }
            games.append(game)
        
        return games
    
    def get_todays_games(self) -> List[Dict]:
        """Get today's NBA games"""
        # In a real implementation, this would fetch today's games
        # For this example, we'll return sample data
        return [
            {
                "game_id": "20230001",
                "date": datetime.now().strftime("%Y-%m-%d"),
                "home_team": "LAL",
                "away_team": "GSW",
                "time": "19:30"
            },
            {
                "game_id": "20230002",
                "date": datetime.now().strftime("%Y-%m-%d"),
                "home_team": "MIL",
                "away_team": "BKN",
                "time": "20:00"
            }
        ]
