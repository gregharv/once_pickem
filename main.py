from fasthtml.common import *
from auth import bware, login, logout, auth_redirect, set_github_secret, get_github_client
from database import db, Schedule, Pick, add_pick, get_user_picks, get_all_games, get_game, update_game_results, update_pick_correctness, GameResult
from datetime import datetime, timedelta
from itertools import groupby
import modal
from pathlib import Path
import requests
import pandas as pd
import pytz

app = FastHTML(before=bware)
rt = app.route

# Helper function to get the current time in EST
def get_current_est_time():
    return datetime.now(pytz.timezone('US/Eastern'))

# Helper function to convert a naive datetime to EST
def to_est(dt):
    eastern = pytz.timezone('US/Eastern')
    return eastern.localize(dt)

# Helper function to get the week number of a game
def get_game_week(game_datetime):
    game_date = to_est(datetime.fromisoformat(game_datetime))
    season_start = to_est(datetime(game_date.year, 9, 4))  # Assuming season starts on September 4th
    return (game_date - season_start).days // 7 + 1

# Homepage (only visible if logged in)
@rt('/')
def home(auth, session):
    games = get_all_games()
    try:
        user = db.t.users.get(auth)
        user_name = user.name
    except:
        user_name = auth  # Fallback to using auth (user_id) if user not found in database
    
    title = f"Welcome, {user_name}"
    login_or_user = A("Logout", href='/logout', style='text-align: right')
    top = Grid(H1(title), login_or_user)

    # Sort games by datetime
    sorted_games = sorted(games, key=lambda g: to_est(datetime.fromisoformat(g.datetime)))
    
    # Group games by week
    grouped_games = groupby(sorted_games, key=lambda g: get_game_week(g.datetime))

    # Get user's picks, defaulting to an empty dictionary if there are none
    user_picks = {p.game_id: p.pick for p in get_user_picks(auth) or []}

    # Get game results
    game_results = list(db.t.game_results())

    game_list = []
    for week, week_games in grouped_games:
        week_games = list(week_games)
        user_week_picks = sum(1 for game in week_games if game.game_id in user_picks)
        week_header = H2(f"Week {week} - {user_week_picks}/2 picks made")
        week_games_list = []
        for game in week_games:
            user_pick = user_picks.get(game.game_id, 'Not picked')
            remove_link = ""
            if user_pick != 'Not picked':
                remove_link = A("Remove", 
                                href=f"/remove_pick/{game.game_id}", 
                                hx_post=f"/remove_pick/{game.game_id}",
                                hx_target=f"#game-{game.game_id}",
                                hx_swap="outerHTML")
            
            # Check if the game has a result
            est = pytz.timezone('US/Eastern')
            game_date = datetime.fromisoformat(game.datetime).astimezone(est).date()
            game_result = next((gr for gr in game_results 
                                if datetime.fromisoformat(gr.commence_time).astimezone(est).date() == game_date
                                and gr.home_team == game.home_team
                                and gr.away_team == game.away_team), None)
            
            result_info = ""
            if game_result and game_result.completed:
                home_score = game_result.home_score
                away_score = game_result.away_score
                winner = game.home_team if home_score > away_score else game.away_team if away_score > home_score else "Tie"
                result_info = f" - Final: {game.away_team} {away_score}, {game.home_team} {home_score} - Winner: {winner}"
            
            game_item = Li(f"{game.away_team} @ {game.home_team} - {game.datetime}",
                           A("Pick", href=f"/pick/{game.game_id}") if to_est(datetime.fromisoformat(game.datetime)) >= get_current_est_time() else "",
                           f" - Your pick: {user_pick}", remove_link, result_info,
                           id=f"game-{game.game_id}")
            week_games_list.append(game_item)
        game_list.extend([week_header, Ul(*week_games_list)])

    return Container(top, 
                    A("View Leaderboard", href="/leaderboard"),
                     *game_list)

@rt('/pick/{game_id:int}')
def get(game_id: int, auth):
    game = get_game(game_id)
    if game is None:
        return RedirectResponse('/', status_code=303)
    
    if to_est(datetime.fromisoformat(game.datetime)) < get_current_est_time():
        return Titled("Pick Not Allowed", P("Sorry, the game time has passed. You can no longer make a pick for this game."))
    
    frm = Form(
        H3(f"{game.away_team} @ {game.home_team}"),
        Select(Option(game.home_team, value=game.home_team),
               Option(game.away_team, value=game.away_team),
               id='pick', name='pick'),
        Button("Submit Pick"),
        action=f'/pick/{game_id}', method='post'
    )
    return Titled("Make Your Pick", frm)

@rt('/pick/{game_id:int}')
def post(game_id: int, pick: str, auth):
    game = get_game(game_id)
    if game is None:
        return RedirectResponse('/', status_code=303)
    
    if to_est(datetime.fromisoformat(game.datetime)) < get_current_est_time():
        return Titled("Pick Not Allowed", P("Sorry, the game time has passed. You can no longer make a pick for this game."))
    
    try:
        add_pick(auth, game_id, pick)
        return RedirectResponse('/', status_code=303)
    except ValueError as e:
        return Titled("Pick Not Allowed", P(str(e)), A("Back to Picks", href="/"))

@rt('/remove_pick/{game_id:int}')
def post(game_id: int, auth):
    try:
        # Get the game information
        game = get_game(game_id)
        
        # Check if the game has already started
        if to_est(datetime.fromisoformat(game.datetime)) < get_current_est_time():
            return "Baited Bitch, nice try: You cannot remove a pick after the game has started."
        
        # Remove the pick
        user_picks = get_user_picks(auth)
        for pick in user_picks:
            if pick.game_id == game_id:
                db.t.picks.delete(pick.id)
                break
        
        return Li(f"{game.away_team} @ {game.home_team} - {game.datetime}",
                  A("Pick", href=f"/pick/{game.game_id}"),
                  " - Your pick: Not picked",
                  id=f"game-{game_id}")
    except Exception as e:
        return f"Error removing pick: {str(e)}"
    
@rt('/leaderboard')
def get(auth):
    user_scores = {}
    user_correct_picks = {}
    user_total_picks = {}
    all_picks = db.t.picks()
    for pick in all_picks:
        game = get_game(pick.game_id)
        if game and to_est(datetime.fromisoformat(game.datetime)) < get_current_est_time():
            user_scores[pick.user_id] = user_scores.get(pick.user_id, 0) + (1 if pick.correct else 0)
            user_total_picks[pick.user_id] = user_total_picks.get(pick.user_id, 0) + 1
            if pick.correct:
                week = get_game_week(game.datetime)
                user_correct_picks.setdefault(pick.user_id, []).append(f"Week {week}: {game.away_team} @ {game.home_team} - Picked: {pick.pick}")
    
    # Fetch user names from the database
    user_names = {}
    for user in db.t.users():
        user_names[user.user_id] = user.name if hasattr(user, 'name') else user.user_id
    
    leaderboard = Ol(*[Li(f"{user_names.get(user_id, user_id)}: Score {score} ({user_total_picks.get(user_id, 0)} picks)", 
                          Ul(*[Li(game) for game in user_correct_picks.get(user_id, [])])) 
                       for user_id, score in sorted(user_scores.items(), key=lambda x: x[1], reverse=True)])
    
    return Titled("Leaderboard", leaderboard, A("Back to Picks", href="/"))

# Add the login route
@rt('/login')
def get():
    return login()

# Add the logout route
@rt('/logout')
def get(session):
    return logout(session)

# Add the auth_redirect route
@rt('/auth_redirect')
def get(code: str, session, state: str = None):
    return auth_redirect(code, session, state)

if __name__ == "__main__":  # if invoked with `python`, run locally
    serve()
else:  # create a modal app, which can be imported in another file or used with modal commands as in README
    modal_app = modal.App("once_pickem")

    # Create a Modal volume
    volume = modal.Volume.from_name("once-pickem-db", create_if_missing=True)

    # Create Modal secrets
    odds_api_secret = modal.Secret.from_name("odds-api-key")
    github_secret = modal.Secret.from_name("github-client-secret")

    image = (modal.Image.debian_slim()
             .pip_install_from_requirements(Path(__file__).parent / "requirements.txt")
             .copy_local_file("schedule.parquet", "/app/schedule.parquet"))

    @modal_app.function(
        image=image,
        allow_concurrent_inputs=1000,  # async functions can handle multiple inputs
        volumes={"/data": volume},  # Mount the volume to /data
        secrets=[odds_api_secret, github_secret],  # Include both secrets
    )
    @modal.asgi_app()
    def fastapi_app():
        import os
        os.environ['MODAL_ENVIRONMENT'] = 'true'  # Set this environment variable
        set_github_secret(github_secret)  # Pass the github_secret to auth.py
        return app

    # Export the ASGI app as the public interface of the Modal app
    asgi_app = fastapi_app