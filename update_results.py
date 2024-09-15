import os
import requests
import pandas as pd
import modal
from database import update_game_results, update_pick_correctness
from pathlib import Path

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
    results_exploded['team'] = results_exploded['scores'].apply(lambda x: x['name'] if x else None)
    results_exploded['score'] = results_exploded['scores'].apply(lambda x: x['score'] if x else None)

    # Drop the original 'scores' column
    results_exploded = results_exploded.drop('scores', axis=1)

    # Pivot the table to have home and away scores in separate columns
    results_fixed = results_exploded.pivot(index=['id', 'sport_key', 'sport_title', 'commence_time', 'completed', 'home_team', 'away_team', 'last_update'],
                                           columns='team',
                                           values='score').reset_index()

    # Rename columns to distinguish between home and away scores
    results_fixed.columns.name = None
    results_fixed = results_fixed.rename(columns={
        results_fixed.columns[-2]: 'away_score',
        results_fixed.columns[-1]: 'home_score'
    })

    # Update the database with the new results
    update_game_results(results_fixed)

    # Update pick correctness for each game
    for _, row in results_fixed.iterrows():
        if row['completed']:
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