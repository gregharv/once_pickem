from fasthtml.common import *
import pandas as pd
from datetime import datetime, timedelta
from dataclasses import dataclass
import modal
import os
import pytz

@dataclass
class Pick:
    id: int
    user_id: str
    game_id: int
    pick: str
    timestamp: str
    correct: bool = None  # Add this new field with a default value of None

# Create a Modal volume
volume = modal.Volume.from_name("once-pickem-db", create_if_missing=True)

# Set up the main database
if os.environ.get('MODAL_ENVIRONMENT'):
    # When running on Modal
    db_path = '/data/main.db'
else:
    # When running locally
    db_path = 'data/main.db'

db = database(db_path)

# Schedule table
schedule = db.t.schedule
if schedule not in db.t:
    # Read the schedule from the parquet file
    if os.environ.get('MODAL_ENVIRONMENT'):
        # When running on Modal
        schedule_path = '/app/schedule.parquet'
    else:
        # When running locally
        schedule_path = 'schedule.parquet'
    
    df = pd.read_parquet(schedule_path)
    
    # Create the schedule table with the appropriate columns
    schedule.create(dict(
        game_id=int,
        datetime=str,
        home_team=str,
        away_team=str
    ), pk='game_id')
    
    # Insert the data from the DataFrame into the table
    for _, row in df.iterrows():
        schedule.insert(dict(
            game_id=row['game_id'],
            datetime=row['datetime'],
            home_team=row['home_team'],
            away_team=row['away_team']
        ))

# Picks table (existing)
picks = db.t.picks
if picks not in db.t:
    picks.create(dict(
        id=int,
        user_id=str,
        game_id=int,
        pick=str,
        timestamp=str,
        correct=bool  # Add this new column
    ), pk='id')
else:
    # Check if the 'correct' column exists, if not, add it, needed for adding this after database was created
    try:
        db.execute('SELECT correct FROM picks LIMIT 1')
    except Exception:
        db.execute('ALTER TABLE picks ADD COLUMN correct BOOLEAN')

# Users table (existing)
users = db.t.users
if users not in db.t:
    users.create(dict(
        user_id=str,
        name=str
    ), pk='user_id')
Users = users.dataclass()

# Create dataclass for Schedule and Pick
Schedule = schedule.dataclass()
Pick = picks.dataclass()

# Function to add a new pick
def add_pick(user_id: str, game_id: int, pick: str):
    # Check if the game exists
    game = get_game(game_id)
    if not game:
        raise ValueError(f"Game with ID {game_id} does not exist")

    # Check if the user has already picked this team
    user_picks = get_user_picks(user_id)
    if any(p.pick == pick for p in user_picks):
        raise ValueError(f"You have already picked {pick} in a previous week")

    # Get the game's week
    game_week = get_game_week(game.datetime)

    # Check if the user has already made 2 picks for this week
    week_picks = [p for p in user_picks if get_game_week(get_game(p.game_id).datetime) == game_week]
    if len(week_picks) >= 2:
        raise ValueError(f"You have already made 2 picks for week {game_week}")

    # Remove any existing pick for this user and game
    user_id = str(user_id)
    existing_picks = [p for p in picks() if p.user_id == user_id and p.game_id == game_id]
    for old_pick in existing_picks:
        picks.delete(old_pick.id)
        print(f"Removed old pick: {old_pick}")

    # Create a new pick
    new_pick = picks.insert({
        "user_id": user_id,
        "game_id": game_id,
        "pick": pick,
        "timestamp": datetime.now().isoformat(),
        "correct": None  # Initialize as None
    })
    print(f"New pick: {new_pick}")
    return Pick(id=new_pick.id, user_id=new_pick.user_id, game_id=new_pick.game_id, 
                pick=new_pick.pick, timestamp=new_pick.timestamp, correct=new_pick.correct)

# Helper function to get the week number of a game
def get_game_week(game_datetime):
    game_date = datetime.fromisoformat(game_datetime)
    season_start = datetime(game_date.year, 9, 4)  # Assuming season starts on September 4th
    return (game_date - season_start).days // 7 + 1

# Function to get picks for a user
def get_user_picks(user_id: str):
    user_picks = picks.rows_where("user_id = ?", [user_id])
    return [Pick(id=p['id'], user_id=p['user_id'], game_id=p['game_id'], pick=p['pick'], timestamp=p['timestamp']) 
            for p in user_picks]

# Function to get all games
def get_all_games():
    return schedule()

# Function to get a specific game
def get_game(game_id: int):
    games = [game for game in schedule() if game.game_id == game_id]
    if games:
        game = games[0]
        print(f"Getting game with ID {game_id}: {game}")
        return game
    else:
        print(f"No game found with ID {game_id}")
        return None

# Game Results table
game_results = db.t.game_results
if game_results not in db.t:
    game_results.create(dict(
        id=str,
        sport_key=str,
        sport_title=str,
        commence_time=str,
        completed=bool,
        home_team=str,
        away_team=str,
        home_score=int,
        away_score=int,
        last_update=str
    ), pk='id')

GameResult = game_results.dataclass()

# Function to update game results
def update_game_results(results_df):
    for _, row in results_df.iterrows():
        game_results.upsert(dict(
            id=row['id'],
            sport_key=row['sport_key'],
            sport_title=row['sport_title'],
            commence_time=row['commence_time'],
            completed=row['completed'],
            home_team=row['home_team'],
            away_team=row['away_team'],
            home_score=row['home_score'] if pd.notna(row['home_score']) else None,
            away_score=row['away_score'] if pd.notna(row['away_score']) else None,
            last_update=row['last_update']
        ), pk='id')

# Function to update pick correctness
def update_pick_correctness(game_result):
    # Convert commence_time to EST
    est = pytz.timezone('US/Eastern')
    game_date = datetime.fromisoformat(game_result['commence_time']).astimezone(est).date()
    home_team = game_result['home_team']
    away_team = game_result['away_team']
    
    # Find the corresponding game in the schedule
    matching_games = [game for game in schedule() 
                      if datetime.fromisoformat(game.datetime).astimezone(est).date() == game_date 
                      and game.home_team == home_team 
                      and game.away_team == away_team]
    
    if not matching_games:
        print(f"No matching game found for {away_team} @ {home_team} on {game_date}")
        return
    
    game = matching_games[0]
    game_picks = picks.rows_where("game_id = ?", [game.game_id])
    
    home_score = game_result['home_score']
    away_score = game_result['away_score']
    winner = home_team if home_score > away_score else away_team if away_score > home_score else None
    
    for pick in game_picks:
        correct = pick['pick'] == winner if winner else None
        picks.update(pick['id'], {"correct": correct})