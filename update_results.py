import os
import requests
import pandas as pd
import modal
from database import update_game_results, update_pick_correctness
from pathlib import Path
from datetime import datetime
import pytz
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create a Modal app
update_results_app = modal.App("update_results")

# Create a Modal volume
volume = modal.Volume.from_name("once-pickem-db", create_if_missing=True)

# Define the image
image = (modal.Image.debian_slim()
         .pip_install_from_requirements(Path(__file__).parent / "requirements.txt")
         .copy_local_file("schedule.parquet", "/app/schedule.parquet"))

def fetch_and_process_results():
    api_key = os.environ['ODDS_API_KEY']
    if not api_key:
        raise ValueError("API key not found. Please set the ODDS_API_KEY environment variable.")

    r = requests.get(f"https://api.the-odds-api.com/v4/sports/americanfootball_nfl/scores/?daysFrom=3&apiKey={api_key}")
    results = pd.DataFrame(r.json())

    # Explode the 'scores' column to create separate rows for each team's score
    results_exploded = results.explode('scores')

    # Extract 'name' and 'score' from the 'scores' dictionary
    results_exploded['team'] = results_exploded['scores'].apply(lambda x: x['name'] if isinstance(x, dict) and 'name' in x else None)
    results_exploded['score'] = results_exploded['scores'].apply(lambda x: x['score'] if isinstance(x, dict) and 'score' in x else None)

    # Drop the original 'scores' column
    results_exploded = results_exploded.drop('scores', axis=1)

    # Create separate columns for home and away scores
    def get_scores(group):
        home_team = group['home_team'].iloc[0] if 'home_team' in group.columns and not group.empty else None
        away_team = group['away_team'].iloc[0] if 'away_team' in group.columns and not group.empty else None
        
        home_score = group[group['team'] == home_team]['score'].iloc[0] if home_team and not group[group['team'] == home_team].empty else None
        away_score = group[group['team'] == away_team]['score'].iloc[0] if away_team and not group[group['team'] == away_team].empty else None
        
        return pd.Series({'home_team_score': home_score, 'away_team_score': away_score})

    results_fixed = results_exploded.groupby('id', as_index=False).apply(get_scores)

    # Merge the scores back with the original data
    results_fixed = results.merge(results_fixed, on='id', how='left')

    # Reorder columns for better readability
    column_order = ['id', 'sport_key', 'sport_title', 'commence_time', 'completed', 'home_team', 'away_team', 'home_team_score', 'away_team_score', 'last_update']
    results_fixed = results_fixed[column_order]

    # Load the schedule data
    schedule_df = pd.read_parquet("/app/schedule.parquet")

    # Convert commence_time to EST
    est = pytz.timezone('US/Eastern')
    utc = pytz.UTC
    results_fixed['commence_date'] = pd.to_datetime(results_fixed['commence_time']).dt.tz_convert(est).dt.date
    
    # Localize schedule datetime to UTC, then convert to EST
    schedule_df['game_date'] = pd.to_datetime(schedule_df['datetime']).dt.tz_localize(utc).dt.tz_convert(est).dt.date

    # Merge results with schedule to get game_id
    merged_results = pd.merge(
        results_fixed,
        schedule_df[['game_id', 'home_team', 'away_team', 'game_date']],
        left_on=['home_team', 'away_team', 'commence_date'],
        right_on=['home_team', 'away_team', 'game_date'],
        how='left'
    )

    # Convert game_id to integer type
    merged_results['game_id'] = merged_results['game_id'].astype('Int64')  # This allows for NaN values

    logger.info(f"Merged results shape: {merged_results.shape}")
    logger.info(f"Merged results columns: {merged_results.columns}")
    logger.info(f"Sample of merged results:\n{merged_results.head().to_string()}")

    # Update the database with the new results
    update_game_results(merged_results)

    # Update pick correctness for each game
    for _, row in merged_results.iterrows():
        if row['completed'] and not pd.isna(row['game_id']):
            update_pick_correctness(row)

# Create a Modal app
update_results_app = modal.App("update_results")

odds_api_secret = modal.Secret.from_name("odds-api-key")

@update_results_app.function(
    image=image,
    schedule=modal.Cron("0 8 * * 5,1"),  # Runs at 3:00 AM EST every Friday and Monday
    volumes={"/data": volume},
    secrets=[odds_api_secret]
)
def update_results_friday_monday():
    fetch_and_process_results()

@update_results_app.function(
    image=image,
    schedule=modal.Cron("0 22 * * 0,1"),  # Runs at 5:00 PM EST every Sunday and 8:00 PM EST every Sunday (1 AM UTC Monday)
    volumes={"/data": volume},
    secrets=[odds_api_secret]
)
def update_results_sunday():
    fetch_and_process_results()

if __name__ == "__main__":
    with update_results_app.run():
        update_results_friday_monday.remote()
        update_results_sunday.remote()