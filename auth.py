from fasthtml.common import *
from fasthtml.oauth import GitHubAppClient
from database import db
from sqlite_minutils.db import NotFoundError
import os
import modal

github_secret = None
client = None

def set_github_secret(secret):
    global github_secret
    github_secret = secret

# Set the base URL based on the environment
if os.environ.get('MODAL_ENVIRONMENT'):
    base_url = "https://gregharv--once-pickem-fastapi-app.modal.run"
else:
    base_url = "http://localhost:5001"

# Set up the GitHub OAuth client
def get_github_client():
    global client
    if client is not None:
        return client
    
    client_secret = os.environ['GITHUB_CLIENT_SECRET']

    if not client_secret:
        raise ValueError("GitHub client secret is not available")

    client = GitHubAppClient(
        client_id="Ov23liSrrMn8z5gaKkPd",
        client_secret=client_secret,
        redirect_uri=f"{base_url}/auth_redirect",
    )
    return client

# Beforeware function for authentication
def before(req, session):
    auth = req.scope['auth'] = session.get('user_id', None)
    if not auth:
        return RedirectResponse('/login', status_code=303)
    try:
        user = db.t.users.get(auth)
        if not user:
            # User not found in database, clear session and redirect to login
            session.clear()
            return RedirectResponse('/login', status_code=303)
    except NotFoundError:
        # User not found in database, clear session and redirect to login
        session.clear()
        return RedirectResponse('/login', status_code=303)
    # User authenticated successfully
    return None

# Create Beforeware object
bware = Beforeware(before, skip=['/login', '/auth_redirect'])

# Login page
def login(): 
    return Div(P("You are not logged in."), 
               A('Log in with GitHub', href=get_github_client().login_link(redirect_uri=f"{base_url}/auth_redirect")))

# Logout function
def logout(session):
    session.pop('user_id', None)
    return RedirectResponse('/login', status_code=303)

# Auth redirect function
def auth_redirect(code:str, session, state:str=None):
    if not code: return "No code provided!"
    try:
        client = get_github_client()
        info = client.retr_info(code, redirect_uri=f"{base_url}/auth_redirect")
        user_id = info[client.id_key]
        user_name = info.get('name', user_id)  # Get the user's name, fallback to user_id if not available
        token = client.token["access_token"]
        session['user_id'] = user_id
        session['user_name'] = user_name  # Store the user's name in the session
        
        # Always update or insert user information
        db.t.users.upsert(dict(user_id=user_id, name=user_name), pk='user_id')
        
        return RedirectResponse('/', status_code=303)
    except Exception as e:
        print(f"Error: {e}")
        return f"Could not log in."