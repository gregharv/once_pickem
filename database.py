from fasthtml.common import *
import pandas as pd
from datetime import datetime, timedelta
from dataclasses import dataclass
import os
import pytz
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

__all__ = ['db', 'Schedule', 'Pick', 'add_pick', 'get_user_picks', 'get_all_games', 'get_game', 'update_game_results', 'update_pick_correctness']

TEAM_ABBREVIATIONS = {
    "Arizona Cardinals": "ARI",
    "Atlanta Falcons": "ATL",
    "Baltimore Ravens": "BAL",
    "Buffalo Bills": "BUF",
    "Carolina Panthers": "CAR",
    "Chicago Bears": "CHI",
    "Cincinnati Bengals": "CIN",
    "Cleveland Browns": "CLE",
    "Dallas Cowboys": "DAL",
    "Denver Broncos": "DEN",
    "Detroit Lions": "DET",
    "Green Bay Packers": "GB",
    "Houston Texans": "HOU",
    "Indianapolis Colts": "IND",
    "Jacksonville Jaguars": "JAX",
    "Kansas City Chiefs": "KC",
    "Las Vegas Raiders": "LV",
    "Los Angeles Chargers": "LAC",
    "Los Angeles Rams": "LAR",
    "Miami Dolphins": "MIA",
    "Minnesota Vikings": "MIN",
    "New England Patriots": "NE",
    "New Orleans Saints": "NO",
    "New York Giants": "NYG",
    "New York Jets": "NYJ",
    "Philadelphia Eagles": "PHI",
    "Pittsburgh Steelers": "PIT",
    "San Francisco 49ers": "SF",
    "Seattle Seahawks": "SEA",
    "Tampa Bay Buccaneers": "TB",
    "Tennessee Titans": "TEN",
    "Washington Commanders": "WAS"
}

@dataclass
class Pick:
    id: int
    user_id: str
    game_id: int
    pick: str
    timestamp: str
    correct: bool = None
    pick_type: str = 'lock'  # 'lock' or 'upset'
    points: float = 3.0  # Default to 3 points for lock picks

@dataclass
class User:
    user_id: str
    name: str
    dname: str

@dataclass
class ScheduleGame:
    game_id: int
    datetime: str
    home_team: str
    away_team: str
    home_team_score: int
    away_team_score: int
    completed: bool
    home_team_short: str
    away_team_short: str

# Set up the main database
# Railway provides persistent storage in the /data directory
if os.environ.get('RAILWAY_ENVIRONMENT'):
    # When running on Railway
    db_path = '/data/main.db'
    # Ensure the /data directory exists
    os.makedirs('/data', exist_ok=True)
else:
    # When running locally
    db_path = 'data/main.db'
    # Ensure the data directory exists
    os.makedirs('data', exist_ok=True)

db = database(db_path)

# Schedule table
schedule = db.t.schedule
if schedule not in db.t:
    # Read the schedule from the parquet file
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
        correct=bool,
        pick_type=str,
        points=float
    ), pk='id')
else:
    # Check if the new columns exist, if not, add them
    try:
        db.execute('SELECT pick_type, points FROM picks LIMIT 1')
    except Exception:
        db.execute('ALTER TABLE picks ADD COLUMN pick_type TEXT DEFAULT "lock"')
        db.execute('ALTER TABLE picks ADD COLUMN points FLOAT DEFAULT 3.0')

# Users table (existing)
users = db.t.users
if users not in db.t:
    users.create(dict(
        user_id=str,
        name=str,
        dname=str,
        username=str  # Add this new column
    ), pk='user_id')
else:
    # Check if the 'username' column exists, if not, add it
    try:
        db.execute('SELECT username FROM users LIMIT 1')
    except Exception:
        db.execute('ALTER TABLE users ADD COLUMN username TEXT')

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

def add_pick(user_id: str, game_id: int, pick: str, pick_type: str = 'lock', points: float = 3.0):
    # Check if the game exists
    game = get_game(game_id)
    if not game:
        raise ValueError(f"Game with ID {game_id} does not exist")

    # Check if the user has already picked this team (only for lock picks)
    user_picks = get_user_picks(user_id)
    if pick_type == 'lock' and any(p.pick == pick and p.pick_type == 'lock' for p in user_picks):
        raise ValueError(f"You have already made a lock pick for {pick} in a previous week")

    # Get the game's week
    game_week = get_game_week(to_est(game['datetime']))

    # Check if the user has already made 2 lock picks and 1 upset pick for this week
    week_picks = [p for p in user_picks if get_game_week(to_est(get_game(p.game_id)['datetime'])) == game_week]
    lock_picks = [p for p in week_picks if p.pick_type == 'lock']
    upset_picks = [p for p in week_picks if p.pick_type == 'upset']

    if pick_type == 'lock' and len(lock_picks) >= 2:
        raise ValueError(f"You have already made 2 lock picks for week {game_week}")
    elif pick_type == 'upset' and len(upset_picks) >= 1:
        raise ValueError(f"You have already made an upset pick for week {game_week}")

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
        "correct": None,  # Initialize as None
        "pick_type": pick_type,
        "points": points
    })
    print(f"New pick: {new_pick}")
    return Pick(id=new_pick.id, user_id=new_pick.user_id, game_id=new_pick.game_id, 
                pick=new_pick.pick, timestamp=new_pick.timestamp, correct=new_pick.correct, 
                pick_type=new_pick.pick_type, points=new_pick.points)

# Helper function to get the week number of a game
def get_all_games():
    games = schedule()
    return [ScheduleGame(
        game_id=game.game_id,
        datetime=game.datetime,
        home_team=game.home_team,
        away_team=game.away_team,
        home_team_score=game.home_team_score,
        away_team_score=game.away_team_score,
        completed=game.completed,
        home_team_short=TEAM_ABBREVIATIONS.get(game.home_team, game.home_team),
        away_team_short=TEAM_ABBREVIATIONS.get(game.away_team, game.away_team)
    ) for game in games]

# Modify the get_game function
def get_game(game_id: int):
    game = schedule.get(game_id)
    if game:
        return {
            'game_id': game.game_id,
            'home_team': game.home_team,
            'away_team': game.away_team,
            'home_team_short': TEAM_ABBREVIATIONS.get(game.home_team, game.home_team),
            'away_team_short': TEAM_ABBREVIATIONS.get(game.away_team, game.away_team),
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
    
    if game['completed'] and home_score is not None and away_score is not None:
        winner = game['home_team'] if home_score > away_score else game['away_team'] if away_score > home_score else None
        
        for pick in game_picks:
            correct = pick['pick'] == winner if winner else None
            picks.upsert({
                "id": pick['id'],
                "user_id": pick['user_id'],
                "game_id": pick['game_id'],
                "pick": pick['pick'],
                "timestamp": pick['timestamp'],
                "correct": correct,
                "pick_type": pick['pick_type'],
                "points": pick['points']
            }, pk='id')
    else:
        logger.info(f"Game {game_id} is not completed or scores are not available. Skipping pick correctness update.")

def get_user_picks(user_id: str):
    return [Pick(**p) for p in picks.rows_where("user_id = ?", [user_id])]

# Add a new function to update user's display name
def update_user_dname(user_id: str, new_dname: str):
    users.upsert({"user_id": user_id, "dname": new_dname}, pk='user_id')
    logger.info(f"Updated display name for user {user_id} to {new_dname}")

# Modify the existing function to include dname
def get_user_info(user_id: str):
    user = users.get(user_id)
    if user:
        return {
            'user_id': user.user_id,
            'name': user.name,
            'dname': user.dname,
            'username': user.username
        }
    return None

# Add this new function to get user info by username
def get_user_info_by_username(username: str):
    user = next(users.rows_where("username = ?", [username]), None)
    if user:
        return {
            'user_id': user['user_id'],
            'name': user['name'],
            'dname': user['dname'],
            'username': user['username']
        }
    return None

# Add this new table definition after the other table definitions
spreads = db.t.spreads
if spreads not in db.t:
    spreads.create(dict(
        id=int,
        game_id=int,
        bookmaker=str,
        team=str,
        point=float,
        price=int,
        timestamp=str
    ), pk='id')

# Add this new function at the end of the file
def update_spreads_in_database(spreads_df):
    est = pytz.timezone('US/Eastern')
    current_time = datetime.now(est).isoformat()

    logger.info(f"Starting to update {len(spreads_df)} spread records in the database.")
    logger.info(f"Spreads table exists: {spreads in db.t}")
    
    # Check if spreads table exists, create if not
    if spreads not in db.t:
        logger.info("Creating spreads table...")
        spreads.create(dict(
            id=int,
            game_id=int,
            bookmaker=str,
            team=str,
            point=float,
            price=int,
            timestamp=str
        ), pk='id')
        logger.info("Spreads table created successfully.")

    inserted_count = 0
    for _, row in spreads_df.iterrows():
        try:
            spread_data = {
                'game_id': row['game_id'],
                'bookmaker': row['bookmaker'],
                'team': row['team'],
                'point': row['point'],
                'price': row['price'],
                'timestamp': current_time
            }
            spreads.insert(spread_data)
            inserted_count += 1
        except Exception as e:
            logger.error(f"Error inserting spread data: {e}")
            logger.error(f"Spread data: {spread_data}")

    logger.info(f"Successfully inserted {inserted_count} spread records in the database.")

# Add this new function to retrieve spreads for a specific game
def get_game_spreads(game_id: int):
    return [dict(s) for s in spreads.rows_where("game_id = ?", [game_id])]

# Add a new function to calculate user scores
def calculate_user_score(user_id: str):
    user_picks = get_user_picks(user_id)
    total_score = 0
    for pick in user_picks:
        if pick.correct:
            total_score += pick.points
    return total_score

# Add this new function to get leaderboard data
def get_leaderboard():
    users_list = users.rows
    leaderboard = []
    for user in users_list:
        score = calculate_user_score(user['user_id'])
        leaderboard.append({
            'user_id': user['user_id'],
            'name': user['dname'] or user['name'] or user['username'],
            'username': user['username'],
            'score': score
        })
    return sorted(leaderboard, key=lambda x: x['score'], reverse=True)

def get_user_lock_picks(user_id: str):
    user_picks = get_user_picks(user_id)
    return set(pick.pick for pick in user_picks if pick.pick_type == 'lock')

# Add this new function at the end of the file
def delete_picks_before_date(target_date=datetime(2024, 9, 16, tzinfo=pytz.UTC)):
    eastern = pytz.timezone('US/Eastern')
    target_date_est = target_date.astimezone(eastern)
    
    # Format the target date as ISO8601 string
    target_date_str = target_date_est.isoformat()
    
    logger.info(f"Attempting to delete picks before {target_date_str}")
    
    try:
        # Use the date() function in SQLite for comparison
        deleted_picks = picks.delete_where("date(timestamp) < date(?)", [target_date_str])
        logger.info(f"Deleted picks using custom wrapper: {deleted_picks}")
    except Exception as e:
        logger.error(f"Error deleting picks with custom wrapper: {str(e)}")
        raise

    # Check remaining picks
    remaining_picks = picks.rows_where("date(timestamp) < date(?)", [target_date_str])
    remaining_count = len(list(remaining_picks))
    logger.info(f"Remaining picks before {target_date_str}: {remaining_count}")

    if remaining_count > 0:
        logger.warning(f"There are still {remaining_count} picks before the target date that were not deleted.")
        # Log a few remaining picks for debugging
        for pick in list(remaining_picks)[:5]:
            logger.warning(f"Remaining pick: {pick}")
    else:
        logger.info("All picks before the target date were successfully deleted.")

    return remaining_count