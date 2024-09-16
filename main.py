from fasthtml.common import *
from auth import bware, login, logout, auth_redirect, set_github_secret, get_github_client
from database import db, Schedule, Pick, add_pick, get_user_picks, get_all_games, get_game, update_game_results, update_pick_correctness
from datetime import datetime, timedelta
from itertools import groupby
import modal
from pathlib import Path
import requests
import pandas as pd
import pytz
from fastapi.staticfiles import StaticFiles

import os
import modal

# Check if we're running in a Modal environment
is_modal = os.environ.get('MODAL_ENVIRONMENT') == 'true'

if is_modal:
    print(f"Contents of /app: {os.listdir('/app')}")
    print(f"Contents of /app/assets: {os.listdir('/app/assets')}")
    assets_dir = "/app/assets"
else:
    print("Running in local environment")
    assets_dir = "assets"

# Define the _not_found function
def _not_found(request, exc):
    return Titled("404 Not Found", P("The page you're looking for doesn't exist."))

app = FastHTML(before=bware,
               exception_handlers={404: _not_found},
               hdrs=(picolink,
                     Link(rel='stylesheet', href='/assets/styles.css', type='text/css'),
                     Style(':root { --pico-font-size: 100%; }'),
                     SortableJS('.sortable'))
                )
rt = app.route

# Helper function to get the current time in EST
def get_current_est_time():
    return datetime.now(pytz.timezone('US/Eastern'))

# Helper function to convert a datetime to EST
def to_est(dt):
    eastern = pytz.timezone('US/Eastern')
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt)
    if dt.tzinfo is None:
        return eastern.localize(dt)
    else:
        return dt.astimezone(eastern)

# Helper function to get the week number of a game
def get_game_week(game_datetime):
    eastern = pytz.timezone('US/Eastern')
    game_date = datetime.fromisoformat(game_datetime)
    if game_date.tzinfo is None:
        game_date = eastern.localize(game_date)
    else:
        game_date = game_date.astimezone(eastern)
    
    season_start = eastern.localize(datetime(game_date.year, 9, 4))  # Assuming season starts on September 4th
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
    sorted_games = sorted(games, key=lambda g: to_est(g.datetime))
    
    # Group games by week
    grouped_games = groupby(sorted_games, key=lambda g: get_game_week(g.datetime))

    # Get user's picks, defaulting to an empty dictionary if there are none
    user_picks = {p.game_id: p.pick for p in get_user_picks(auth) or []}

    # Create sidebar with leaderboard link and links to each week
    sidebar_links = [
        A("Leaderboard", href="/leaderboard"),
        Br(),  # Add a line break
        H3("Weeks")
    ]
    week_tables = []
    for week, week_games in grouped_games:
        week_games = list(week_games)
        user_week_picks = sum(1 for game in week_games if game.game_id in user_picks)
        week_header = H2(f"Week {week} - {user_week_picks}/2 picks made", id=f"week-{week}")
        sidebar_links.append(A(f"Week {week}", href=f"#week-{week}"))
        
        table = Table(
            Tr(Th("Away Team"), Th("Home Team"), Th("Date/Time"), Th("Your Pick"), Th("Action"), Th("Result")),
            *[Tr(
                Td(game.away_team),
                Td(game.home_team),
                Td(game.datetime),
                Td(user_picks.get(game.game_id, 'Not picked')),
                Td(
                    A("Pick", href=f"/pick/{game.game_id}") if to_est(datetime.fromisoformat(game.datetime)) >= get_current_est_time() else "",
                    " ",
                    A("Remove", 
                      href=f"/remove_pick/{game.game_id}", 
                      hx_post=f"/remove_pick/{game.game_id}",
                      hx_target=f"#game-{game.game_id}",
                      hx_swap="outerHTML") if user_picks.get(game.game_id, 'Not picked') != 'Not picked' else ""
                ),
                Td(
                    f"{game.away_team} {game.away_team_score}", Br(),
                    f"{game.home_team} {game.home_team_score}", Br(),
                    Span(
                        f"Winner: {game.home_team if game.home_team_score > game.away_team_score else game.away_team if game.away_team_score > game.home_team_score else 'Tie'}",
                        style=f"color: {'green' if game.completed and user_picks.get(game.game_id) == (game.home_team if game.home_team_score > game.away_team_score else game.away_team) else 'red' if game.completed and game.game_id in user_picks else 'blue'}; font-weight: bold;"
                    ) if game.completed else ""
                ),
                id=f"game-{game.game_id}"
            ) for game in week_games],
            style="width: 100%; border-collapse: collapse;",
        )
        week_tables.extend([week_header, Br(), table, Br()])  # Add line breaks between elements

    # Create sidebar
    sidebar = Div(
        *sidebar_links,
        cls="sidebar"
    )

    # Adjust main content to make room for sidebar
    main_content = Div(
        top, 
        Br(),  # Add a line break after the top section
        *week_tables,
        cls="main-content"
    )

    return Container(sidebar, main_content)

@rt('/pick/{game_id:int}')
def get(game_id: int, auth):
    game = get_game(game_id)
    if game is None:
        return RedirectResponse('/', status_code=303)
    
    if to_est(datetime.fromisoformat(game['datetime'])) < get_current_est_time():
        return Titled("Pick Not Allowed", P("Sorry, the game time has passed. You can no longer make a pick for this game."))
    
    frm = Form(
        H3(f"{game['away_team']} @ {game['home_team']}"),
        Select(Option(game['home_team'], value=game['home_team']),
               Option(game['away_team'], value=game['away_team']),
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
    
    if to_est(datetime.fromisoformat(game['datetime'])) < get_current_est_time():
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
        if to_est(datetime.fromisoformat(game['datetime'])) < get_current_est_time():
            return "Baited Bitch, nice try: You cannot remove a pick after the game has started."
        
        # Remove the pick
        user_picks = get_user_picks(auth)
        for pick in user_picks:
            if pick.game_id == game_id:
                db.t.picks.delete(pick.id)
                break
        
        return Li(f"{game['away_team']} @ {game['home_team']} - {game['datetime']}",
                  A("Pick", href=f"/pick/{game['game_id']}"),
                  " - Your pick: Not picked",
                  id=f"game-{game['game_id']}")
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
        if game and game.get('completed', False):  # Use .get() method with a default value
            user_scores[pick.user_id] = user_scores.get(pick.user_id, 0) + (1 if pick.correct else 0)
            user_total_picks[pick.user_id] = user_total_picks.get(pick.user_id, 0) + 1
            if pick.correct:
                week = get_game_week(game['datetime'])  # Access datetime as a dictionary key
                user_correct_picks.setdefault(pick.user_id, []).append(f"Week {week}: {game['away_team']} @ {game['home_team']} - Picked: {pick.pick}")
    
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
    rules_explanation = Div(
        H2("Welcome to Once Pickem!"),
        P("Here are the rules of the game:"),
        Ul(
            Li("You can pick 2 teams per week."),
            Li("You can't pick the same team twice throughout the year."),
            Li("Make your picks before the game starts."),
            Li("You cannot change your picks once the game have started."),
            Li("Need help? Watch our ", A("video tutorial", href="https://www.youtube.com/watch?v=cvh0nX08nRw"))
        ),
        style="margin-bottom: 20px;"
    )
    return login(extra_content=rules_explanation)

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
             .copy_local_file("schedule.parquet", "/app/schedule.parquet")
             .copy_local_dir("assets", "/app/assets"))  # Add this line

    @modal_app.function(
        image=image,
        allow_concurrent_inputs=1000,  # async functions can handle multiple inputs
        volumes={"/data": volume},  # Mount the volume to /data
        secrets=[odds_api_secret, github_secret],  # Include both secrets
    )
    @modal.asgi_app()
    def fastapi_app():
        import os
        import logging
        from fastapi.staticfiles import StaticFiles

        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger(__name__)

        # Log the current working directory and its contents
        logger.info(f"Current working directory: {os.getcwd()}")
        logger.info(f"Contents of current directory: {os.listdir()}")

        # Check if the assets directory exists
        if os.path.exists("/app/assets"):
            logger.info("Assets directory exists")
            logger.info(f"Contents of assets directory: {os.listdir('/app/assets')}")
            app.mount("/assets", StaticFiles(directory="/app/assets"), name="assets")
        else:
            logger.warning("Assets directory does not exist")

        os.environ['MODAL_ENVIRONMENT'] = 'true'
        set_github_secret(github_secret)
        return app

    # Export the ASGI app as the public interface of the Modal app
    asgi_app = fastapi_app