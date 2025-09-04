from fasthtml.common import *
from auth import bware, login, logout, auth_redirect, set_google_secret, get_google_client
from database import db, ScheduleGame, Pick, add_pick, get_user_picks, get_all_games, get_game, update_game_results, update_pick_correctness, update_user_dname, get_user_info, get_game_spreads, calculate_user_score, get_leaderboard, get_user_info_by_username, get_user_lock_picks
from update_results import fetch_and_process_results
from update_spreads import fetch_and_process_spreads
from datetime import datetime, timedelta
from itertools import groupby
import os
import requests
import pandas as pd
import pytz
# FastAPI static files removed - using FastHTML native static serving
import logging
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
    
# Helper function to convert a datetime to EST and format it nicely
def format_est_time(dt):
    eastern = pytz.timezone('US/Eastern')
    if isinstance(dt, str):
        dt = datetime.fromisoformat(dt)
    if dt.tzinfo is None:
        dt = pytz.utc.localize(dt)
    est_time = dt.astimezone(eastern)
    return est_time.strftime("%a, %b %d, %Y at %I:%M %p")

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

# Add this function near the other helper functions
def get_current_week():
    current_time = get_current_est_time()
    games = get_all_games()
    for game in sorted(games, key=lambda g: to_est(g.datetime)):
        if to_est(game.datetime) > current_time:
            return get_game_week(game.datetime)
    return 18  # Return the last week if all games have passed

# Add this new function near the other helper functions
def get_games_for_week(week):
    all_games = get_all_games()
    return [game for game in all_games if get_game_week(game.datetime) == week]

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
        A("Logout", href='/logout'),
        columns="1fr",
        style="text-align: right; gap: 10px;"
    )
    
    # Sort games by datetime
    sorted_games = sorted(games, key=lambda g: to_est(g.datetime))
    
    # Group games by week
    grouped_games = groupby(sorted_games, key=lambda g: get_game_week(g.datetime))

    # Get user's picks, and create a dictionary with game_id as key and Pick object as value
    user_picks = {p.game_id: p for p in get_user_picks(auth) or []}

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
        week_header = H2(f"Week {week} - {user_week_picks}/3 picks made", id=f"week-{week}")
        
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
    user_picks_dict = {p.game_id: p for p in user_picks}
    
    # Create the updated week table
    updated_table = create_week_table(week_games, user_picks_dict, auth)
    
    # Set the hx-swap-oob attribute on the table
    updated_table.attrs['hx_swap_oob'] = "true"
    
    # Return both the error modal and the updated table
    return error_modal, updated_table

def create_week_table(games, user_picks, auth):
    return Table(
        Tr(
            Th("Away Team"),
            Th("Home Team"),
            Th("Date/Time"),
            Th("Your Pick"),
            Th("Result")
        ),
        *[create_game_row(game, user_picks.get(game.game_id), auth) for game in games],
        id=f"week-{get_game_week(games[0].datetime)}-table"
    )

def create_game_row(game, pick, auth):
    game_time = to_est(datetime.fromisoformat(game.datetime))
    current_time = get_current_est_time()
    game_started = game_time < current_time

    away_team_full = game.away_team
    home_team_full = game.home_team
    away_team_short = game.away_team_short
    home_team_short = game.home_team_short

    full_date = format_est_time(game.datetime)
    short_date = game_time.strftime("%a")

    pick_short = get_game(pick.game_id)['away_team_short'] if pick and pick.pick == away_team_full else get_game(pick.game_id)['home_team_short'] if pick else ""

    # Get the spreads for this game
    spreads = get_game_spreads(game.game_id)
    
    # Find the most recent spread for each team
    away_spread = None
    home_spread = None
    for spread in sorted(spreads, key=lambda s: s['timestamp'], reverse=True):
        if spread['team'] == away_team_full and away_spread is None:
            away_spread = spread
        elif spread['team'] == home_team_full and home_spread is None:
            home_spread = spread
        if away_spread and home_spread:
            break

    # Get all lock picks for the user
    user_lock_picks = get_user_lock_picks(auth)

    def create_team_cell(team_full, team_short, spread, is_lock_pick):
        team_style = "color: purple;" if is_lock_pick else ""
        team_element = A(
            Span(team_full, cls="team-name-full", style=team_style),
            Span(team_short, cls="team-name-short", style=team_style),
            hx_post=f"/pick/{game.game_id}/{team_full}/lock",
            hx_target=f"#week-{get_game_week(game.datetime)}-table",
            hx_swap="outerHTML",
            cls="team-pick"
        ) if not game_started else Span(
            Span(team_full, cls="team-name-full", style=team_style),
            Span(team_short, cls="team-name-short", style=team_style)
        )

        spread_element = ""
        if spread and spread['point'] > 0:
            spread_element = A(
                f" (+{spread['point']})",
                hx_post=f"/pick/{game.game_id}/{team_full}/upset/{spread['point']}",
                hx_target=f"#week-{get_game_week(game.datetime)}-table",
                hx_swap="outerHTML",
                cls="upset-pick"
            ) if not game_started else f" (+{spread['point']})"

        return Td(team_element, spread_element)

    return Tr(
        create_team_cell(away_team_full, away_team_short, away_spread, away_team_full in user_lock_picks),
        create_team_cell(home_team_full, home_team_short, home_spread, home_team_full in user_lock_picks),
        Td(
            Span(full_date, cls="date-full"),
            Span(short_date, cls="date-short")
        ),
        Td(
            Span(
                Span(pick.pick, cls="team-name-full"),
                Span(pick_short, cls="team-name-short")
            ) if pick else "",
            " ",
            Span("(Lock)", cls="pick-type") if pick and pick.pick_type == 'lock' else "",
            Span("(Upset)", cls="pick-type") if pick and pick.pick_type == 'upset' else "",
            " ",
            A("×", 
              hx_post=f"/remove_pick/{game.game_id}",
              hx_target=f"#week-{get_game_week(game.datetime)}-table",
              hx_swap="outerHTML",
              hx_indicator="#error-message"
            ) if pick and not game_started else "",
            id=f"pick-{game.game_id}"
        ),
        Td(
            (Span(
                f"{away_team_short} {game.away_team_score}",
                style=f"font-weight: {'bold' if game.away_team_score > game.home_team_score else 'normal'}; "
                      f"color: {'green' if game.completed and pick and pick.pick == game.away_team and game.away_team_score > game.home_team_score else 'red' if game.completed and pick and pick.pick == game.away_team and game.away_team_score < game.home_team_score else 'inherit'};"
            ),
            " - ",
            Span(
                f"{home_team_short} {game.home_team_score}",
                style=f"font-weight: {'bold' if game.home_team_score > game.away_team_score else 'normal'}; "
                      f"color: {'green' if game.completed and pick and pick.pick == game.home_team and game.home_team_score > game.away_team_score else 'red' if game.completed and pick and pick.pick == game.home_team and game.home_team_score < game.away_team_score else 'inherit'};"
            )) if game.completed else ""
        ),
        id=f"game-{game.game_id}"
    )

@rt('/pick/{game_id:int}/{team}/lock')
def post(game_id: int, team: str, auth):
    try:
        add_pick(auth, game_id, team, pick_type='lock', points=3.0)
        game = get_game(game_id)
        week = get_game_week(game['datetime'])
        week_games = get_games_for_week(week)
        user_picks = get_user_picks(auth)
        user_picks_dict = {p.game_id: p for p in user_picks}
        return create_week_table(week_games, user_picks_dict, auth)
    except ValueError as e:
        return error_response(str(e), game_id, auth)

@rt('/pick/{game_id:int}/{team}/upset/{points:float}')
def post(game_id: int, team: str, points: float, auth):
    try:
        add_pick(auth, game_id, team, pick_type='upset', points=points)
        game = get_game(game_id)
        week = get_game_week(game['datetime'])
        week_games = get_games_for_week(week)
        user_picks = get_user_picks(auth)
        user_picks_dict = {p.game_id: p for p in user_picks}
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
        user_picks_dict = {p.game_id: p for p in user_picks}
        
        return create_week_table(week_games, user_picks_dict, auth)
    except Exception as e:
        return error_response(str(e), game_id, auth)

@rt('/leaderboard')
def get(auth):
    leaderboard_data = get_leaderboard()
    
    # Get the current week
    current_week = get_current_week()
    
    # Create the sidebar
    sidebar = Div(
        A("Picks", href=f"/#week-{current_week}"),
        H3("Weeks"),
        *[A(f"Week {week}", href=f"#week-{week}") for week in range(1, 19)],  # Assuming 18 weeks in NFL season
        cls="sidebar"
    )

    # Add the "Change Display Name" link
    change_name_link = A("Change Display Name", 
                         href="/change_dname")

    # Create the leaderboard table
    leaderboard_table = Table(
        Tr(Th("Rank"), Th("Name"), Th("Score")),
        *[Tr(
            Td(i+1), 
            Td(A(entry['name'], href=f"/user/{entry['username']}")),  # Use username here
            Td(entry['score'])
        ) for i, entry in enumerate(leaderboard_data)]
    )

    # Create the main content
    main_content = Div(
        H1("Leaderboard"),
        change_name_link,
        Br(),
        leaderboard_table,
        cls="main-content"
    )

    return Titled(
        "Leaderboard - Once Pickem",
        sidebar,
        main_content
    )

# Add this new route for the user page
@rt('/user/{username}')
def get(username: str, auth):
    user_info = get_user_info_by_username(username)
    if not user_info:
        return "User not found"
    
    user_picks = get_user_picks(user_info['user_id'])
    user_score = calculate_user_score(user_info['user_id'])
    
    # Group picks by week
    picks_by_week = {}
    for pick in user_picks:
        game = get_game(pick.game_id)
        week = get_game_week(game['datetime'])
        if week not in picks_by_week:
            picks_by_week[week] = []
        picks_by_week[week].append((pick, game))
    
    # Create the sidebar
    sidebar = Div(
        A("Back to Leaderboard", href="/leaderboard"),
        H3("Weeks"),
        *[A(f"Week {week}", href=f"#week-{week}") for week in range(1, 19)],
        cls="sidebar"
    )
    
    # Create tables for each week
    week_tables = []
    for week in sorted(picks_by_week.keys()):
        week_picks = picks_by_week[week]
        week_table = Table(
            Tr(Th("Game"), Th("Pick"), Th("Type"), Th("Points"), Th("Result")),
            *[Tr(
                Td(f"{game['away_team_short']} @ {game['home_team_short']}"),
                Td(pick.pick),
                Td(pick.pick_type.capitalize()),
                Td(f"{pick.points:.1f}"),
                Td("Correct" if pick.correct else "Incorrect" if pick.correct is not None else "Pending")
            ) for pick, game in week_picks],
            id=f"week-{week}"
        )
        week_tables.append(H3(f"Week {week}"))
        week_tables.append(week_table)
        week_tables.append(Br())
    
    # Create the main content
    main_content = Div(
        P(f"Total Score: {user_score}"),
        *week_tables,
        cls="main-content"
    )
    
    return Titled(
        f"{user_info['dname'] or user_info['name']}'s Picks - Once Pickem",
        sidebar,
        main_content
    )

@rt('/change_dname')
def get(auth):
    if not auth:
        return "Please log in to change your display name."
    
    user_info = get_user_info(auth)
    if not user_info:
        return "User not found"
    
    return Titled(
        "Change Display Name",
        Form(
            Label("New Display Name:"),
            Input(type="text", name="new_dname", value=user_info['dname'] or user_info['name'] or auth),
            Input(type="submit", value="Update"),
            action="/update_dname",
            method="post"
        )
    )

@rt('/update_dname')
def post(new_dname: str, auth):
    logger.info(f"Updating display name for user {auth} to {new_dname}")
    if not auth:
        logger.warning("User not authenticated")
        return "Please log in to change your display name."
    
    try:
        update_user_dname(auth, new_dname)
        logger.info(f"Display name updated successfully for user {auth}")
        return RedirectResponse('/leaderboard', status_code=303)
    except Exception as e:
        logger.error(f"Error updating display name for user {auth}: {str(e)}")
        return f"An error occurred: {str(e)}"

# Add the login route
@rt('/login')
def get():
    rules_explanation = Div(
        H2("Welcome to Once Pickem!"),
        P("Here are the rules of the game:"),
        Ul(
            Li("You can pick 3 teams per week, 2 lock picks and 1 upset pick."),
            Li("You can't pick the same team twice throughout the year for the lock picks."),
            Li("You can pick the same team for the upset pick if you want to."),
            Li("You get 3 points for picking a lock winner correctly."),
            Li("You get the spread amount of points for picking an upset correctly."),
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

# FastHTML handles static files automatically from the assets directory
# No need to mount them manually - FastHTML serves static files from /assets by default
if os.path.exists("assets"):
    logger.info("Assets directory found - FastHTML will serve static files automatically")
else:
    logger.warning("Assets directory not found")

# Set up Google OAuth credentials from environment variables (Railway will provide these)
google_client_secret = os.environ.get('GOOGLE_CLIENT_SECRET')
google_client_id = os.environ.get('GOOGLE_CLIENT_ID')
if google_client_secret and google_client_id:
    set_google_secret(google_client_secret)
    logger.info("Google OAuth credentials configured from environment")
else:
    logger.warning("GOOGLE_CLIENT_SECRET or GOOGLE_CLIENT_ID environment variables not set")

# Admin endpoints for cron jobs
@rt('/admin/update_results')
def update_results_endpoint():
    """Endpoint for cron-job.org to update game results"""
    try:
        logger.info("Starting game results update via cron job")
        fetch_and_process_results()
        logger.info("Game results update completed successfully")
        return {"status": "success", "message": "Game results updated successfully"}
    except Exception as e:
        logger.error(f"Error updating game results: {str(e)}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        return {"status": "error", "message": str(e)}, 500

@rt('/admin/update_spreads')
def update_spreads_endpoint():
    """Endpoint for cron-job.org to update game spreads"""
    try:
        logger.info("Starting game spreads update via cron job")
        fetch_and_process_spreads()
        logger.info("Game spreads update completed successfully")
        return {"status": "success", "message": "Game spreads updated successfully"}
    except Exception as e:
        logger.error(f"Error updating game spreads: {str(e)}")
        import traceback
        logger.error(f"Full traceback: {traceback.format_exc()}")
        return {"status": "error", "message": str(e)}, 500

@rt('/admin/health')
def health_check():
    """Health check endpoint for monitoring"""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

if __name__ == "__main__":
    # For Railway deployment, use the PORT environment variable
    port = int(os.environ.get('PORT', 8000))
    serve(port=port)
