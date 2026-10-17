"""Independent DiscordArcade OAuth/session routes, mounted in the archive API."""
import hashlib
import hmac
import os
import secrets
import time
from urllib.parse import urlencode

import httpx
from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse, RedirectResponse
from arcade_chat import create_arcade_router

COOKIE = '__Host-arcade_session'
STATE_COOKIE = '__Host-arcade_oauth'
TTL = 60 * 60 * 24 * 7


def install_arcade(app, transport=None):
    secret = os.getenv('ARCADE_SESSION_SECRET', '').encode()
    site = 'https://discordarcade.com'
    callback = 'https://api.discordarcade.com/api/arcade/auth/callback'
    guild = os.getenv('DISCORD_GUILD_ID', '')
    token = os.getenv('BOT_TOKEN') or os.getenv('DISCORD_TOKEN', '')
    client_id = os.getenv('ARCADE_DISCORD_CLIENT_ID') or os.getenv('DISCORD_CLIENT_ID', '')
    client_secret = os.getenv('ARCADE_DISCORD_CLIENT_SECRET') or os.getenv('DISCORD_CLIENT_SECRET', '')
    origins = {site, 'https://www.discordarcade.com'}

    def sign(value):
        return value + '.' + hmac.new(secret, value.encode(), hashlib.sha256).hexdigest()

    def verify(raw, age):
        if len(secret) < 32 or not raw:
            return None
        try:
            value, mac = raw.rsplit('.', 1)
            if not hmac.compare_digest(sign(value), raw):
                return None
            identity, issued = value.rsplit(':', 1)
            elapsed = time.time() - int(issued)
            return identity if 0 <= elapsed <= age else None
        except (ValueError, TypeError):
            return None

    def session_user(request):
        value = verify(request.cookies.get(COOKIE), TTL)
        return value if value and value.isdigit() else None

    def configured():
        if len(secret) < 32 or not client_id or not client_secret or not token or not guild.isdigit():
            raise HTTPException(503, 'Arcade login is not configured yet.')

    @app.get('/api/arcade/auth/login')
    async def login():
        configured()
        state = sign(secrets.token_urlsafe(32) + ':' + str(int(time.time())))
        query = urlencode({'client_id': client_id, 'redirect_uri': callback, 'response_type': 'code',
                           'scope': 'identify', 'state': state})
        response = JSONResponse({'url': 'https://discord.com/oauth2/authorize?' + query}, headers={'Cache-Control': 'no-store'})
        response.set_cookie(STATE_COOKIE, state, max_age=600, secure=True, httponly=True, samesite='lax', path='/')
        return response

    @app.get('/api/arcade/auth/callback')
    async def oauth_callback(request: Request):
        configured()
        state = request.query_params.get('state', '')
        cookie = request.cookies.get(STATE_COOKIE, '')
        if not verify(state, 600) or not hmac.compare_digest(state, cookie):
            raise HTTPException(400, 'Invalid or expired login. Start sign-in again.')
        response = RedirectResponse(site + '/?auth_error=discord', status_code=303)
        response.delete_cookie(STATE_COOKIE, secure=True, httponly=True, samesite='lax', path='/')
        code = request.query_params.get('code')
        if not code:
            return response
        try:
            async with httpx.AsyncClient(timeout=15, transport=transport) as client:
                result = await client.post('https://discord.com/api/v10/oauth2/token', data={
                    'client_id': client_id, 'client_secret': client_secret, 'grant_type': 'authorization_code',
                    'code': code, 'redirect_uri': callback})
                result.raise_for_status()
                user = await client.get('https://discord.com/api/v10/users/@me',
                                        headers={'Authorization': 'Bearer ' + result.json()['access_token']})
                user.raise_for_status()
                uid = str(user.json()['id'])
                member = await client.get(f'https://discord.com/api/v10/guilds/{guild}/members/{uid}',
                                          headers={'Authorization': 'Bot ' + token})
                if member.status_code == 404:
                    response.headers['location'] = site + '/?auth_error=membership'
                    return response
                member.raise_for_status()
                response.headers['location'] = site + '/'
                response.set_cookie(COOKIE, sign(uid + ':' + str(int(time.time()))), max_age=TTL,
                                    secure=True, httponly=True, samesite='lax', path='/')
        except (httpx.HTTPError, KeyError, ValueError):
            pass
        return response

    @app.post('/api/arcade/auth/logout')
    async def logout(request: Request):
        raw = request.cookies.get(COOKIE, '')
        csrf = hmac.new(secret, ('arcade-csrf:' + raw).encode(), hashlib.sha256).hexdigest()
        if request.headers.get('origin') not in origins or not hmac.compare_digest(request.headers.get('x-csrf-token', ''), csrf):
            raise HTTPException(403, 'Invalid request.')
        response = JSONResponse({'ok': True})
        response.delete_cookie(COOKIE, secure=True, httponly=True, samesite='lax', path='/')
        return response

    app.include_router(create_arcade_router(session_user=session_user, is_booster=None, sub_status=None,
        settings={'guild_id': guild, 'bot_token': token, 'session_secret': secret, 'cookie_name': COOKIE,
                  'channel_ids': os.getenv('ARCADE_CHANNEL_IDS', ''), 'site_url': site,
                  'relay_secret': os.getenv('ARCADE_RELAY_SECRET', '')}, transport=transport))
