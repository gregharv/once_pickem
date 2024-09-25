from fasthtml.common import *
import pandas as pd
from datetime import datetime, timedelta
from dataclasses import dataclass
import modal
import os
import pytz
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

__all__ = ['db', 'Schedule', 'Pick', 'add_pick', 'get_user_picks', 'get_all_games', 'get_game', 'update_game_results', 'update_pick_correctness']

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
    
    # Create the schedule table with the additional columns
    schedule.create(dict(
        game_id=int,
        datetime=str,
        home_team=str,
        away_team=str,
        home_team_score=int,
        away_team_score=int,
        completed=bool
    ), pk='game_id')
    
    # Insert the data from the DataFrame into the table
    for _, row in df.iterrows():
        schedule.insert(dict(
            game_id=row['game_id'],
            datetime=row['datetime'],
            home_team=row['home_team'],
            away_team=row['away_team'],
            home_team_score=None,
            away_team_score=None,
            completed=False
        ))
else:
    # Check if the new columns exist, if not, add them
    try:
        db.execute('SELECT home_team_score, away_team_score, completed FROM schedule LIMIT 1')
    except Exception:
        db.execute('ALTER TABLE schedule ADD COLUMN home_team_score INTEGER')
        db.execute('ALTER TABLE schedule ADD COLUMN away_team_score INTEGER')
        db.execute('ALTER TABLE schedule ADD COLUMN completed BOOLEAN DEFAULT FALSE')

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
        name=str,
        dname=str  # Add this new column
    ), pk='user_id')
else:
    # Check if the 'dname' column exists, if not, add it
    try:
        db.execute('SELECT dname FROM users LIMIT 1')
    except Exception:
        db.execute('ALTER TABLE users ADD COLUMN dname TEXT')

# Add the initial mappings
initial_mappings = {
    '18033555': 'Greg',
    '51495669': 'Jason',
    '182435157': 'Roger'
}

for user_id, dname in initial_mappings.items():
    users.upsert({"user_id": user_id, "dname": dname}, pk='user_id')

Users = users.dataclass()

# Create dataclass for Schedule and Pick
Schedule = schedule.dataclass()
Pick = picks.dataclass()

# Function to add a new pick
def to_est(dt):
    eastern = pytz.timezone('US/Eastern')
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt)
    if dt.tzinfo is None:
        return eastern.localize(dt)
    else:
        return dt.astimezone(eastern)

def get_game_week(game_datetime):
    eastern = pytz.timezone('US/Eastern')
    if isinstance(game_datetime, str):
        game_date = datetime.fromisoformat(game_datetime)
    else:
        game_date = game_datetime
    
    if game_date.tzinfo is None:
        game_date = eastern.localize(game_date)
    else:
        game_date = game_date.astimezone(eastern)
    
    season_start = eastern.localize(datetime(game_date.year, 9, 4))  # Assuming season starts on September 4th
    return (game_date - season_start).days // 7 + 1

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
    game_week = get_game_week(to_est(game['datetime']))

    # Check if the user has already made 2 picks for this week
    week_picks = [p for p in user_picks if get_game_week(to_est(get_game(p.game_id)['datetime'])) == game_week]
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
def get_all_games():
    games = schedule()
    return [Schedule(
        game_id=game.game_id,
        datetime=game.datetime,
        home_team=game.home_team,
        away_team=game.away_team,
        home_team_score=game.home_team_score,
        away_team_score=game.away_team_score,
        completed=game.completed
    ) for game in games]

# Function to get a specific game
def get_game(game_id: int):
    game = schedule.get(game_id)
    if game:
        return {
            'game_id': game.game_id,
            'home_team': game.home_team,
            'away_team': game.away_team,
            'datetime': game.datetime.isoformat() if isinstance(game.datetime, datetime) else game.datetime,
            'home_team_score': game.home_team_score,
            'away_team_score': game.away_team_score,
            'completed': game.completed
        }
    return None

# Function to update game results
def update_game_results(results_df):
    for _, row in results_df.iterrows():
        try:
            if pd.notna(row['game_id']):
                game_id = int(row['game_id'])
                update_dict = {
                    'game_id': game_id,
                    'home_team': row['home_team'],
                    'away_team': row['away_team'],
                    'datetime': row['commence_time']
                }
                if pd.notna(row['home_team_score']):
                    update_dict['home_team_score'] = int(row['home_team_score'])
                if pd.notna(row['away_team_score']):
                    update_dict['away_team_score'] = int(row['away_team_score'])
                if pd.notna(row['completed']):
                    update_dict['completed'] = bool(row['completed'])
                
                logger.info(f"Upserting game {game_id} with data: {update_dict}")
                schedule.upsert(update_dict, pk='game_id')
        except Exception as e:
            logger.error(f"Error updating game {row.get('game_id', 'unknown')}: {str(e)}")
            logger.error(f"Row data: {row.to_dict()}")

# Function to update pick correctness
def update_pick_correctness(game_result):
    game_id = int(game_result['game_id'])
    game = get_game(game_id)
    if game is None:
        logger.error(f"Game with ID {game_id} not found")
        return
    
    game_picks = picks.rows_where("game_id = ?", [game_id])
    
    home_score = game['home_team_score']
    away_score = game['away_team_score']
    
    logger.info(f"Updating pick correctness for game {game_id}: {game['home_team']} {home_score} - {game['away_team']} {away_score}")
    
    # Only determine winner if both scores are available and the game is completed
    if game['completed'] and home_score is not None and away_score is not None:
        winner = game['home_team'] if home_score > away_score else game['away_team'] if away_score > home_score else None
        
        for pick in game_picks:
            correct = pick['pick'] == winner if winner else None
            logger.info(f"Updating pick {pick['id']} for user {pick['user_id']}: picked {pick['pick']}, correct: {correct}")
            picks.upsert({
                "id": pick['id'],
                "user_id": pick['user_id'],
                "game_id": pick['game_id'],
                "pick": pick['pick'],
                "timestamp": pick['timestamp'],
                "correct": correct
            }, pk='id')
    else:
        logger.info(f"Game {game_id} is not completed or scores are not available. Skipping pick correctness update.")

def get_user_picks(user_id: str):
    return [Pick(**p) for p in picks.rows_where("user_id = ?", [user_id])]

# Add a new function to update user's display name
def update_user_dname(user_id: str, new_dname: str):
    users.update({"user_id": user_id, "dname": new_dname}, pk='user_id')

# Modify the existing function to include dname
def get_user_info(user_id: str):
    user = users.get(user_id)
    if user:
        return {
            'user_id': user['user_id'],
            'name': user['name'],
            'dname': user['dname']
        }
    return None