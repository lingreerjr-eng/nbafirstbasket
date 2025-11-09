import asyncio
import logging
from datetime import datetime, timedelta
from scraper.nba_scraper import NBAScraper
from predictor.model import FirstBasketPredictor
from scheduler.scheduler import ScheduleManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('app.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def main():
    logger.info("Starting NBA First Basket Predictor")
    
    # Initialize components
    scraper = NBAScraper()
    predictor = FirstBasketPredictor()
    scheduler = ScheduleManager()
    
    # Initial data scraping
    logger.info("Performing initial data scraping...")
    scraper.scrape_season_data()
    
    # Train initial model
    logger.info("Training initial model...")
    predictor.train_model()
    
    # Start scheduler
    logger.info("Starting scheduler...")
    
    # Create and set a new event loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    # Start the scheduler within the event loop
    loop.create_task(scheduler.start(scraper, predictor))
    
    try:
        # Keep the application running
        loop.run_forever()
    except KeyboardInterrupt:
        logger.info("Application stopped by user")
    except Exception as e:
        logger.error(f"Application error: {e}")
    finally:
        scheduler.stop()
        loop.close()
        logger.info("Application shutdown complete")

if __name__ == "__main__":
    main()
