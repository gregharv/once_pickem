from fasthtml.common import *
from auth import bware, login, logout, auth_redirect, set_github_secret, get_github_client
from database import db, Schedule, Pick, add_pick, get_user_picks, get_all_games, get_game, update_game_results, update_pick_correctness, update_user_dname, get_user_info
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

# Add this near the top of your file, after the imports
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

# Define your JavaScript code
js_code = """
document.body.addEventListener('htmx:afterOnLoad', function(event) {
    if (event.detail.elt.id === 'error-modal' && event.detail.xhr.status !== 200) {
        document.getElementById('error-modal').setAttribute('open', 'true');
    }
});

function closeErrorModal() {
    document.getElementById('error-modal').removeAttribute('open');
}
"""

# Define the _not_found function
def _not_found(request, exc):
    return Titled("404 Not Found", P("The page you're looking for doesn't exist."))

app = FastHTML(before=bware,
               exception_handlers={404: _not_found},
               hdrs=(picolink,
                     Link(rel='stylesheet', href='/assets/styles.css', type='text/css'),
                     Style(':root { --pico-font-size: 100%; }'),
                     SortableJS('.sortable'),
                     Script(js_code))
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
    
    # Determine the year of the season based on the game date
    if game_date.month < 3:  # If the game is in January or February, it's part of the previous year's season
        season_year = game_date.year - 1
    else:
        season_year = game_date.year

    # Set the season start to the first Thursday of September
    season_start = eastern.localize(datetime(season_year, 9, 1))
    while season_start.weekday() != 3:  # 3 represents Thursday
        season_start += timedelta(days=1)

    # Calculate the week number
    week = (game_date - season_start).days // 7 + 1

    # Handle the case for week 18 (which occurs in the next calendar year)
    if week <= 0:
        week = 18

    return week

# Helper function to convert a datetime to EST and format it nicely
def format_est_time(dt):
    eastern = pytz.timezone('US/Eastern')
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt)
    if dt.tzinfo is None:
        dt = pytz.utc.localize(dt)
    est_time = dt.astimezone(eastern)
    return est_time.strftime("%a, %b %d, %Y at %I:%M %p")

# Add this function near the other helper functions
def get_current_week():
    current_time = get_current_est_time()
    games = get_all_games()
    for game in sorted(games, key=lambda g: to_est(g.datetime)):
        if to_est(game.datetime) > current_time:
            return get_game_week(game.datetime)
    return 18  # Return the last week if all games have passed

# Homepage (only visible if logged in)
@rt('/')
def home(auth, session):
    games = get_all_games()
    try:
        user = db.t.users.get(auth)
        user_name = user.dname or user.name or auth
    except:
        user_name = auth
    
    welcome_message = f"Welcome, {user_name}"
    login_or_user = Grid(
        A("Change Display Name", href=f"/change_dname/{auth}", hx_get=f"/change_dname/{auth}", hx_target="#dname-form"),
        A("Logout", href='/logout'),
        columns="1fr 1fr",
        style="text-align: right; gap: 10px;"
    )
    
    # Sort games by datetime
    sorted_games = sorted(games, key=lambda g: to_est(g.datetime))
    
    # Group games by week
    grouped_games = groupby(sorted_games, key=lambda g: get_game_week(g.datetime))

    # Get user's picks, defaulting to an empty dictionary if there are none
    user_picks = {p.game_id: p.pick for p in get_user_picks(auth) or []}

    # Create sidebar with leaderboard link and links to each week
    sidebar = Div(
        A("Leaderboard", href="/leaderboard"),
        H3("Weeks"),
        *[A(f"Week {week}", href=f"#week-{week}") for week in range(1, 19)],  # Assuming 18 weeks in NFL season
        cls="sidebar"
    )

    week_tables = []
    for week, week_games in grouped_games:
        week_games = list(week_games)
        user_week_picks = sum(1 for game in week_games if game.game_id in user_picks)
        week_header = H2(f"Week {week} - {user_week_picks}/2 picks made", id=f"week-{week}")
        
        table = create_week_table(week_games, user_picks, auth)
        week_tables.extend([week_header, Br(), table, Br()])

    # Adjust main content to make room for sidebar
    main_content = Div(
        Grid(
            Div(H2(welcome_message)),
            Div(login_or_user, style="text-align: right;"),
            columns="1fr 1fr"
        ),
        Br(),  # Add a line break after the top section
        *week_tables,
        cls="main-content"
    )

    error_modal = Dialog(
        Article(
            Header(
                P(Strong("Error"))
            ),
            P(id="error-modal-content"),
            Footer(
                Button("Close", cls="secondary", onclick="closeErrorModal()")
            )
        ),
        id="error-modal"
    )

    return Titled(
        "Once Pickem",
        sidebar,
        main_content,
        error_modal,
        Div(id="dname-form"),
        Script(js_code)
    )

@rt('/close-modal')
def close_modal():
    return Dialog(id="error-modal")

def error_response(message, game_id, auth):
    error_modal = Dialog(
        Article(
            Header(
                P(Strong("Error"))
            ),
            P(message),
            Footer(
                Button("Close", cls="secondary", onclick="closeErrorModal()")
            )
        ),
        id="error-modal",
        open=True,
        hx_swap_oob="true"
    )
    
    # Get the original game row
    game = get_game(game_id)
    week = get_game_week(game['datetime'])
    week_games = [g for g in get_all_games() if get_game_week(g.datetime) == week]
    user_picks = get_user_picks(auth)
    user_picks_dict = {p.game_id: p.pick for p in user_picks}
    
    # Create the updated week table
    updated_table = create_week_table(week_games, user_picks_dict, auth)
    
    # Set the hx-swap-oob attribute on the table
    updated_table.attrs['hx_swap_oob'] = "true"
    
    # Return both the error modal and the updated table
    return error_modal, updated_table

def create_week_table(games, user_picks, auth):
    return Div(
        Table(
            Tr(
                Th("Away Team", style="width: 20%;"),
                Th("Home Team", style="width: 20%;"),
                Th("Date/Time", style="width: 20%;"),
                Th("Your Pick", style="width: 20%;"),
                Th("Result", style="width: 20%;")
            ),
            *[create_game_row(game, user_picks.get(game.game_id, "Not picked"), auth) for game in games],
            id=f"week-{get_game_week(games[0].datetime)}-table",
            style="width: 100%; border-collapse: collapse;",
        ),
        cls="table-container"
    )

def create_game_row(game, pick, auth):
    game_time = to_est(datetime.fromisoformat(game.datetime))
    current_time = get_current_est_time()
    game_started = game_time < current_time

    away_team_full = game.away_team
    home_team_full = game.home_team
    away_team_short = TEAM_ABBREVIATIONS.get(away_team_full, away_team_full)
    home_team_short = TEAM_ABBREVIATIONS.get(home_team_full, home_team_full)

    # Format the date for both full and short versions
    full_date = format_est_time(game.datetime)
    short_date = game_time.strftime("%a")  # This will give us the abbreviated day of the week

    # Get the short name for the picked team
    pick_short = TEAM_ABBREVIATIONS.get(pick, pick) if pick != "Not picked" else ""

    return Tr(
        Td(
            A(
                Span(away_team_full, cls="team-name-full"),
                Span(away_team_short, cls="team-name-short"),
                hx_post=f"/pick/{game.game_id}/{game.away_team}",
                hx_target=f"#week-{get_game_week(game.datetime)}-table",
                hx_swap="outerHTML",
                **{"hx-on::after-request": "if(event.detail.failed) document.getElementById('error-modal').setAttribute('open', 'true');"}
            ) if not game_started else Span(
                Span(away_team_full, cls="team-name-full"),
                Span(away_team_short, cls="team-name-short")
            )
        ),
        Td(
            A(
                Span(home_team_full, cls="team-name-full"),
                Span(home_team_short, cls="team-name-short"),
                hx_post=f"/pick/{game.game_id}/{game.home_team}",
                hx_target=f"#week-{get_game_week(game.datetime)}-table",
                hx_swap="outerHTML",
                **{"hx-on::after-request": "if(event.detail.failed) document.getElementById('error-modal').setAttribute('open', 'true');"}
            ) if not game_started else Span(
                Span(home_team_full, cls="team-name-full"),
                Span(home_team_short, cls="team-name-short")
            )
        ),
        Td(
            Span(full_date, cls="date-full"),
            Span(short_date, cls="date-short")
        ),
        Td(
            Span(
                Span(pick, cls="team-name-full"),
                Span(pick_short, cls="team-name-short")
            ) if pick != "Not picked" else "",
            " ",
            A("×", 
              hx_post=f"/remove_pick/{game.game_id}",
              hx_target=f"#week-{get_game_week(game.datetime)}-table",
              hx_swap="outerHTML",
              hx_indicator="#error-message"
            ) if pick != "Not picked" and not game_started else "",
            id=f"pick-{game.game_id}"
        ),
        Td(
            (Span(
                f"{away_team_short} {game.away_team_score}",
                style=f"font-weight: {'bold' if game.away_team_score > game.home_team_score else 'normal'}; "
                      f"color: {'green' if game.completed and pick == game.away_team and game.away_team_score > game.home_team_score else 'red' if game.completed and pick == game.away_team and game.away_team_score < game.home_team_score else 'inherit'};"
            ),
            " - ",
            Span(
                f"{home_team_short} {game.home_team_score}",
                style=f"font-weight: {'bold' if game.home_team_score > game.away_team_score else 'normal'}; "
                      f"color: {'green' if game.completed and pick == game.home_team and game.home_team_score > game.away_team_score else 'red' if game.completed and pick == game.home_team and game.home_team_score < game.away_team_score else 'inherit'};"
            )) if game.completed else ""
        ),
        id=f"game-{game.game_id}"
    )

@rt('/pick/{game_id:int}/{team}')
def post(game_id: int, team: str, auth):
    game = get_game(game_id)
    if game is None:
        return error_response("Game not found", game_id, auth)
    
    if to_est(datetime.fromisoformat(game['datetime'])) < get_current_est_time():
        return error_response("Pick not allowed: The game time has passed.", game_id, auth)
    
    try:
        user_picks = get_user_picks(auth)
        week = get_game_week(game['datetime'])
        week_games = [g for g in get_all_games() if get_game_week(g.datetime) == week]
        week_picks = [p for p in user_picks if get_game_week(get_game(p.game_id)['datetime']) == week]
        
        # Check if the user has already picked this team
        if any(p.pick == team for p in user_picks):
            return error_response("Error: You have already picked this team this season.", game_id, auth)
        
        # Check if the user has already made 2 picks for this week
        if len(week_picks) >= 2 and game_id not in [p.game_id for p in week_picks]:
            return error_response("Error: You have already made 2 picks for this week.", game_id, auth)
        
        add_pick(auth, game_id, team)
        
        # Update user_picks after adding the new pick
        user_picks = get_user_picks(auth)
        user_picks_dict = {p.game_id: p.pick for p in user_picks}
        
        return create_week_table(week_games, user_picks_dict, auth)
    except ValueError as e:
        return error_response(str(e), game_id, auth)

@rt('/remove_pick/{game_id:int}')
def post(game_id: int, auth):
    try:
        game = get_game(game_id)
        
        if to_est(datetime.fromisoformat(game['datetime'])) < get_current_est_time():
            return error_response("You cannot remove a pick after the game has started.", game_id, auth)
        
        user_picks = get_user_picks(auth)
        for pick in user_picks:
            if pick.game_id == game_id:
                db.t.picks.delete(pick.id)
                break
        
        week = get_game_week(game['datetime'])
        week_games = [g for g in get_all_games() if get_game_week(g.datetime) == week]
        
        # Update user_picks after removing the pick
        user_picks = get_user_picks(auth)
        user_picks_dict = {p.game_id: p.pick for p in user_picks}
        
        return create_week_table(week_games, user_picks_dict, auth)
    except Exception as e:
        return error_response(str(e), game_id, auth)

@rt('/leaderboard')
def get(auth):
    user_scores = {}
    user_correct_picks = {}
    user_total_picks = {}
    all_picks = db.t.picks()
    for pick in all_picks:
        game = get_game(pick.game_id)
        if game and game.get('completed', False):
            user_scores[pick.user_id] = user_scores.get(pick.user_id, 0) + (1 if pick.correct else 0)
            user_total_picks[pick.user_id] = user_total_picks.get(pick.user_id, 0) + 1
            if pick.correct:
                week = get_game_week(game['datetime'])
                user_correct_picks.setdefault(pick.user_id, []).append(f"Week {week}: {game['away_team']} @ {game['home_team']} - Picked: {pick.pick}")
    
    # Fetch user names from the database
    user_names = {}
    for user in db.t.users():
        user_names[user.user_id] = user.dname or user.name or user.user_id
    
    leaderboard = Ol(*[Li(
        Span(f"{user_names.get(user_id, user_id)}: Score {score} ({user_total_picks.get(user_id, 0)} picks)"),
        A("Change Display Name", hx_get=f"/change_dname/{user_id}", hx_target="#dname-form") if user_id == auth else "",
        Ul(*[Li(game) for game in user_correct_picks.get(user_id, [])])
    ) for user_id, score in sorted(user_scores.items(), key=lambda x: x[1], reverse=True)])
    
    dname_form = Div(id="dname-form")
    
    # Create sidebar with updated 'Back to Picks' link
    current_week = get_current_week()
    sidebar = Div(
        A("Back to Picks", href=f"/#week-{current_week}"),
        H3("Weeks"),
        *[A(f"Week {week}", href=f"/#week-{week}") for week in range(1, 19)],  # Assuming 18 weeks in NFL season
        cls="sidebar"
    )

    # Adjust main content to make room for sidebar
    main_content = Div(
        leaderboard,
        dname_form,
        cls="main-content"
    )

    return Titled(
        "Leaderboard",
        sidebar,
        main_content
    )

@rt('/change_dname/{user_id}')
def get(user_id: str, auth):
    if user_id != auth:
        return "Unauthorized"
    
    user_info = get_user_info(user_id)
    if not user_info:
        return "User not found"
    
    return Form(
        Label("New Display Name:"),
        Input(type="text", name="new_dname", value=user_info['dname'] or user_info['name'] or user_id),
        Input(type="submit", value="Update"),
        hx_post=f"/update_dname/{user_id}",
        hx_target="#dname-form",
        hx_swap="outerHTML"
    )

@rt('/update_dname/{user_id}')
def post(user_id: str, new_dname: str, auth):
    if user_id != auth:
        return "Unauthorized"
    
    update_user_dname(user_id, new_dname)
    return P("Display name updated successfully!")

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

# Modify the auth_redirect function
@rt('/auth_redirect')
def get(code: str, session, state: str = None):
    auth_result = auth_redirect(code, session, state)
    if isinstance(auth_result, RedirectResponse):
        current_week = get_current_week()
        return RedirectResponse(url=f"/#week-{current_week}", status_code=302)
    return auth_result

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
        container_idle_timeout=300,
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