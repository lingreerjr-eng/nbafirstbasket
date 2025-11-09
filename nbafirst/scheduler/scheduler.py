import asyncio
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class ScheduleManager:
    def __init__(self):
        self.running = False
        self.scraper_task = None
        self.prediction_task = None
    
    async def start(self, scraper, predictor):
        """Start the scheduler"""
        self.running = True
        
        # Start background tasks
        self.scraper_task = asyncio.create_task(self._scrape_data_periodically(scraper))
        self.prediction_task = asyncio.create_task(self._make_predictions_periodically(predictor, scraper))
        
        logger.info("Scheduler started")
        
        # Wait for tasks to complete (they won't unless cancelled)
        try:
            await asyncio.gather(self.scraper_task, self.prediction_task)
        except asyncio.CancelledError:
            logger.info("Scheduler tasks cancelled")
    
    def stop(self):
        """Stop the scheduler"""
        self.running = False
        
        if self.scraper_task:
            self.scraper_task.cancel()
        
        if self.prediction_task:
            self.prediction_task.cancel()
        
        logger.info("Scheduler stopped")
    
    async def _scrape_data_periodically(self, scraper):
        """Scrape data every 24 hours"""
        while self.running:
            try:
                logger.info("Starting periodic data scraping")
                scraper.scrape_season_data()
                logger.info("Data scraping completed")
                
                # Wait for 24 hours
                await asyncio.sleep(24 * 60 * 60)
            except asyncio.CancelledError:
                logger.info("Data scraping task cancelled")
                break
            except Exception as e:
                logger.error(f"Error in data scraping: {e}")
                # Wait 1 hour before retrying
                await asyncio.sleep(60 * 60)
    
    async def _make_predictions_periodically(self, predictor, scraper):
        """Make predictions for upcoming games"""
        while self.running:
            try:
                logger.info("Checking for upcoming games and making predictions")
                
                # Get today's games
                games = scraper.get_todays_games()
                
                # Make predictions for each game
                for game in games:
                    prediction = predictor.predict_first_basket(
                        game["home_team"], 
                        game["away_team"]
                    )
                    
                    logger.info(f"\nGame: {game['away_team']} @ {game['home_team']}")
                    logger.info(f"Predicted first basket: {prediction['player']} ({prediction['team']})")
                    logger.info(f"Probability: {prediction['probability']:.2%} (Confidence: {prediction['confidence']})")
                
                # Wait for 1 hour
                await asyncio.sleep(60 * 60)
            except asyncio.CancelledError:
                logger.info("Prediction task cancelled")
                break
            except Exception as e:
                logger.error(f"Error in predictions: {e}")
                # Wait 10 minutes before retrying
                await asyncio.sleep(10 * 60)
