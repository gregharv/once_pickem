import os
import requests
import pandas as pd
# Modal import removed for Railway deployment
from database import update_spreads_in_database
from pathlib import Path
import logging
import pytz

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Railway deployment - no Modal setup needed

def fetch_and_process_spreads():
    api_key = os.environ['ODDS_API_KEY']
    if not api_key:
        raise ValueError("API key not found. Please set the ODDS_API_KEY environment variable.")

    url = f"https://api.the-odds-api.com/v4/sports/americanfootball_nfl/odds/?apiKey={api_key}&regions=us&markets=spreads&Format=american"
    response = requests.get(url)
    response.raise_for_status()  # Raise an exception for bad responses

    data = response.json()
    
    # Create a list to store spread data
    spreads_data = []

    for game in data:
        game_id = game['id']
        home_team = game['home_team']
        away_team = game['away_team']
        commence_time = game['commence_time']

        for bookmaker in game['bookmakers']:
            bookmaker_key = bookmaker['key']
            for market in bookmaker['markets']:
                if market['key'] == 'spreads':
                    for outcome in market['outcomes']:
                        team = outcome['name']
                        point = outcome['point']
                        price = outcome['price']
                        spreads_data.append({
                            'game_id': game_id,
                            'home_team': home_team,
                            'away_team': away_team,
                            'commence_time': commence_time,
                            'bookmaker': bookmaker_key,
                            'team': team,
                            'point': point,
                            'price': price
                        })

    # Create a DataFrame from the spreads data
    spreads_df = pd.DataFrame(spreads_data)

    # Load the schedule data
    schedule_df = pd.read_parquet("schedule.parquet")

    # Convert commence_time to EST
    est = pytz.timezone('US/Eastern')
    utc = pytz.UTC
    spreads_df['commence_date'] = pd.to_datetime(spreads_df['commence_time']).dt.tz_convert(est).dt.date
    
    # Localize schedule datetime to UTC, then convert to EST
    schedule_df['game_date'] = pd.to_datetime(schedule_df['datetime']).dt.tz_localize(utc).dt.tz_convert(est).dt.date

    # Merge spreads with schedule to get game_id
    merged_spreads = pd.merge(
        spreads_df,
        schedule_df[['game_id', 'home_team', 'away_team', 'game_date']],
        left_on=['home_team', 'away_team', 'commence_date'],
        right_on=['home_team', 'away_team', 'game_date'],
        how='left'
    )

    # Use the game_id from the schedule if available, otherwise use the API's game_id
    merged_spreads['game_id'] = merged_spreads['game_id_y'].fillna(merged_spreads['game_id_x'])
    merged_spreads = merged_spreads.drop(['game_id_x', 'game_id_y', 'game_date'], axis=1)

    # Convert game_id to integer type
    merged_spreads['game_id'] = merged_spreads['game_id'].astype('Int64')  # This allows for NaN values

    logger.info(f"Merged spreads shape: {merged_spreads.shape}")
    logger.info(f"Merged spreads columns: {merged_spreads.columns}")
    logger.info(f"Sample of merged spreads:\n{merged_spreads.head().to_string()}")

    # Update the database with the new spreads data
    update_spreads_in_database(merged_spreads)

# For Railway deployment, this can be run as a standalone script
# or called via HTTP endpoint for scheduled execution
if __name__ == "__main__":
    fetch_and_process_spreads()