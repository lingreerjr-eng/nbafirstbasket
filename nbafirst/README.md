# NBA First Basket Predictor

This application predicts which player will score the first basket in upcoming NBA games by scraping data and training a machine learning model.

## Features

- Scrapes NBA data from current and previous seasons
- Automatically updates data every 24 hours
- Predicts first basket scorer for upcoming games
- Runs continuously in the background

## Installation

1. Clone the repository
2. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

## Usage

Run the application:
```bash
python main.py
```

The application will:
1. Scrape initial NBA data
2. Train the prediction model
3. Start making predictions for upcoming games
4. Automatically update data every 24 hours

## How It Works

1. **Data Scraping**: The scraper collects player statistics and game data
2. **Model Training**: The predictor trains a model based on historical data
3. **Predictions**: The model predicts first basket scorers for upcoming games
4. **Scheduling**: All processes run automatically on a schedule

## Output

Predictions are logged to the console and saved in `app.log`:

```
Game: GSW @ LAL
Predicted first basket: LeBron James (LAL)
Probability: 8.00% (Confidence: high)
```

## Data Sources & Compliance

The scraper only uses publicly documented NBA endpoints (`data.nba.com` and `cdn.nba.com`) after
checking the league's `robots.txt`. Requests are rate-limited and identify as a
standard desktop browser to comply with access policies.

## Stored Data

Scraped play-by-play results are persisted to `data/first_baskets.db` and
exported as season-specific JSON snapshots (`players_<season>.json` and
`games_<season>.json`). Running the scraper repeatedly will incrementally update
this database, allowing the application to build a historical record of first
basket events over time.
