from fasthtml.common import *
from fasthtml.oauth import GoogleAppClient
from database import db
from sqlite_minutils.db import NotFoundError
import os

google_secret = None
client = None

def set_google_secret(secret):
    global google_secret
    google_secret = secret

# Set the base URL based on the environment
if os.environ.get('RAILWAY_ENVIRONMENT'):
    # When running on Railway, use the Railway URL
    base_url = os.environ.get('RAILWAY_PUBLIC_DOMAIN', 'https://nfl.critjecture.com')
else:
    # Local development
    base_url = "http://localhost:8000"

# Set up the Google OAuth client
def get_google_client():
    global client
    if client is not None:
        return client
    
    client_secret = os.environ.get('GOOGLE_CLIENT_SECRET')
    client_id = os.environ.get('GOOGLE_CLIENT_ID')

    if not client_secret or not client_id:
        raise ValueError("Google OAuth credentials are not available")

    client = GoogleAppClient(
        client_id=client_id,
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
def login(extra_content=None):
    client = get_google_client()
    login_url = client.login_link(redirect_uri=f"{base_url}/auth_redirect")
    login_button = A("Login with Google", href=login_url, cls="button")
    content = ["Login", login_button]
    if extra_content:
        content.insert(1, extra_content)  # Insert extra_content after the H1 but before the login button
    return Titled(*content)

# Logout function
def logout(session):
    session.pop('user_id', None)
    return RedirectResponse('/login', status_code=303)

# Auth redirect function
def auth_redirect(code:str, session, state:str=None):
    if not code: return "No code provided!"
    try:
        client = get_google_client()
        info = client.retr_info(code, redirect_uri=f"{base_url}/auth_redirect")
        user_id = info[client.id_key]
        user_name = info.get('name', user_id)  # Get the user's name, fallback to user_id if not available
        email = info.get('email', '')  # Get the Google email
        username = email.split('@')[0] if email else user_id  # Use email prefix as username
        token = client.token["access_token"]
        session['user_id'] = user_id
        session['user_name'] = user_name  # Store the user's name in the session
        session['username'] = username  # Store the username in the session
        
        # Always update or insert user information
        db.t.users.upsert(dict(user_id=user_id, name=user_name, username=username), pk='user_id')
        
        return RedirectResponse('/', status_code=303)
    except Exception as e:
        print(f"Error: {e}")
        return f"Could not log in."