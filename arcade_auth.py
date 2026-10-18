"""Public Token OAuth; server membership is checked by each chat operation."""
import hashlib
import hmac
import json
import os
import secrets
import time
from urllib.parse import urlencode
import httpx
from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse, RedirectResponse
from arcade_chat import create_arcade_router, public_user
from arcade_sessions import SessionStore
from arcade_leaderboards import create_leaderboard_router

COOKIE = '__Host-token_session'
STATE_COOKIE = '__Host-arcade_oauth'
TTL = 60 * 60 * 24 * 7

def install_arcade(app, transport=None, store=None):
    secret = os.getenv('ARCADE_SESSION_SECRET', '').encode()
    site = 'https://discordarcade.com'
    callback = 'https://api.discordarcade.com/api/arcade/auth/callback'
    booster_guild = os.getenv('ARCADE_BOOST_GUILD_ID') or os.getenv('DISCORD_GUILD_ID', '')
    token = os.getenv('BOT_TOKEN') or os.getenv('DISCORD_TOKEN', '')
    client_id = os.getenv('ARCADE_DISCORD_CLIENT_ID') or os.getenv('DISCORD_CLIENT_ID', '')
    client_secret = os.getenv('ARCADE_DISCORD_CLIENT_SECRET') or os.getenv('DISCORD_CLIENT_SECRET', '')
    store = store or SessionStore(os.getenv('DATABASE_URL', ''))
    origins = {site, 'https://www.discordarcade.com'}
    headers = {'Cache-Control': 'no-store'}

    def configured():
        if len(secret) < 32 or not client_id or not client_secret or not token:
            raise HTTPException(503, 'Token login is not configured.')

    def sign(value):
        return value + '.' + hmac.new(secret, value.encode(), hashlib.sha256).hexdigest()

    def valid_state(raw):
        try:
            value, _ = raw.rsplit('.', 1)
            age = time.time() - int(value.rsplit(':', 1)[1])
            return len(secret) >= 32 and 0 <= age <= 600 and hmac.compare_digest(sign(value), raw)
        except (ValueError, TypeError):
            return False

    async def session_user(request):
        row = await store.get(request.cookies.get(COOKIE))
        return row['uid'] if row else None

    async def profile(request):
        row = await store.get(request.cookies.get(COOKIE))
        if not row:
            raise HTTPException(401, 'Sign in with Discord.')
        return public_user({'user': json.loads(row['profile'])})

    async def exchange(data):
        async with httpx.AsyncClient(timeout=15, transport=transport) as client:
            result = await client.post('https://discord.com/api/v10/oauth2/token', data={
                'client_id': client_id, 'client_secret': client_secret, **data})
        if result.status_code in (400, 401):
            raise HTTPException(401, 'Discord authorization expired. Sign in again.')
        result.raise_for_status()
        return result.json()

    async def user_guilds(request):
        async def refresh(value):
            return await exchange({'grant_type': 'refresh_token', 'refresh_token': value})
        try:
            access = await store.access(request.cookies.get(COOKIE, ''), refresh)
            values, after = [], '0'
            async with httpx.AsyncClient(timeout=15, transport=transport) as client:
                while True:
                    result = await client.get('https://discord.com/api/v10/users/@me/guilds',
                        params={'limit': 200, 'after': after}, headers={'Authorization': 'Bearer ' + access})
                    if result.status_code in (401, 403):
                        raise HTTPException(401, 'Sign in again to allow Token to view your server list.')
                    if result.status_code == 429:
                        raise HTTPException(429, 'Discord is busy. Try refreshing servers shortly.')
                    result.raise_for_status()
                    page = result.json()
                    values.extend(page)
                    if len(page) < 200:
                        return values
                    next_after = str(max(int(g['id']) for g in page))
                    if next_after == after:
                        raise HTTPException(502, 'Could not load the server list.')
                    after = next_after
        except httpx.HTTPError:
            raise HTTPException(502, 'Discord is temporarily unavailable.') from None

    @app.get('/api/arcade/auth/login')
    async def login():
        configured()
        state = sign(secrets.token_urlsafe(32) + ':' + str(int(time.time())))
        query = urlencode({'client_id': client_id, 'redirect_uri': callback, 'response_type': 'code',
                           'scope': 'identify guilds', 'state': state})
        response = JSONResponse({'url': 'https://discord.com/oauth2/authorize?' + query}, headers=headers)
        response.set_cookie(STATE_COOKIE, state, max_age=600, secure=True, httponly=True, samesite='lax', path='/')
        return response

    @app.get('/api/arcade/auth/callback')
    async def oauth_callback(request: Request):
        configured()
        state = request.query_params.get('state', '')
        if not valid_state(state) or not hmac.compare_digest(state, request.cookies.get(STATE_COOKIE, '')):
            raise HTTPException(400, 'Invalid or expired login. Start sign-in again.')
        response = RedirectResponse(site + '/?auth_error=discord', status_code=303, headers=headers)
        response.delete_cookie(STATE_COOKIE, secure=True, httponly=True, samesite='lax', path='/')
        code = request.query_params.get('code')
        if not code:
            return response
        try:
            tokens = await exchange({'grant_type': 'authorization_code', 'code': code, 'redirect_uri': callback})
            if not {'identify', 'guilds'}.issubset(set(tokens.get('scope', '').split())):
                return response
            async with httpx.AsyncClient(timeout=15, transport=transport) as client:
                result = await client.get('https://discord.com/api/v10/users/@me',
                    headers={'Authorization': 'Bearer ' + tokens['access_token']})
                result.raise_for_status()
                user = result.json()
            sid = secrets.token_urlsafe(32)
            await store.create(sid, user, tokens, TTL)
            await store.delete(request.cookies.get(COOKIE))
            response.headers['location'] = site + '/'
            response.set_cookie(COOKIE, sid, max_age=TTL, secure=True, httponly=True, samesite='lax', path='/')
            response.delete_cookie('__Host-arcade_session', secure=True, httponly=True, samesite='lax', path='/')
        except (httpx.HTTPError, HTTPException, KeyError, ValueError):
            pass
        return response

    @app.post('/api/arcade/auth/logout')
    async def logout(request: Request):
        raw = request.cookies.get(COOKIE, '')
        csrf = hmac.new(secret, ('arcade-csrf:' + raw).encode(), hashlib.sha256).hexdigest()
        if request.headers.get('origin') not in origins or not hmac.compare_digest(request.headers.get('x-csrf-token', ''), csrf):
            raise HTTPException(403, 'Invalid request.')
        await store.delete(raw)
        response = JSONResponse({'ok': True}, headers=headers)
        response.delete_cookie(COOKIE, secure=True, httponly=True, samesite='lax', path='/')
        return response

    @app.get('/api/arcade/access')
    async def arcade_access(request: Request):
        uid = await session_user(request)
        if not uid:
            return JSONResponse({'authenticated': False, 'allowed': False, 'reason': 'login'}, headers=headers)
        configured()
        if not booster_guild.isdigit():
            return JSONResponse({'authenticated': True, 'allowed': False, 'reason': 'premium'}, headers=headers)
        try:
            async with httpx.AsyncClient(timeout=10, transport=transport) as client:
                result = await client.get(f'https://discord.com/api/v10/guilds/{booster_guild}/members/{uid}',
                                          headers={'Authorization': 'Bot ' + token})
            if result.status_code == 404:
                allowed = False
            else:
                result.raise_for_status()
                allowed = bool(result.json().get('premium_since'))
            # Do not advertise the private booster benefit in the browser response.
            return JSONResponse({'authenticated': True, 'allowed': allowed,
                                 'reason': 'premium' if not allowed else 'active'}, headers=headers)
        except (httpx.HTTPError, ValueError):
            return JSONResponse({'authenticated': True, 'allowed': False, 'reason': 'unavailable'}, status_code=503, headers=headers)

    app.include_router(create_arcade_router(session_user=session_user, is_booster=None, sub_status=None,
        settings={'bot_token': token, 'session_secret': secret, 'cookie_name': COOKIE,
                  'site_url': site, 'relay_secret': os.getenv('ARCADE_RELAY_SECRET', ''),
                  'session_profile': profile, 'user_guilds': user_guilds}, transport=transport))

    app.include_router(create_leaderboard_router(
        store=store,
        session_user=session_user,
        profile=profile,
        secret=secret,
        cookie_name=COOKIE,
        origins=origins
    ))
