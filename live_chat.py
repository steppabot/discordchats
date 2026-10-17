"""Wolfpac arcade: permission-checked live chat using the existing site login.

Only ARCADE_CHANNEL_IDS in DISCORD_GUILD_ID are exposed. Browser sends are
attributed bot messages, never user-account automation. No archive writes.
"""
import asyncio
import hashlib
import hmac
import json
import re
import time
from datetime import datetime, timezone, date
from urllib.parse import urlparse

import httpx
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

VIEW, SEND, HISTORY, ADMIN = 1024, 2048, 65536, 8
MANAGE_CHANNELS, MANAGE_MESSAGES, BYPASS_SLOWMODE = 16, 8192, 1 << 52


def permission_bits(member, guild, roles, channel):
    """Discord's everyone -> aggregated roles -> member overwrite order."""
    if str(member['user']['id']) == str(guild.get('owner_id')):
        return (1 << 62) - 1
    member_roles = set(member.get('roles', []))
    bits = 0
    for role in roles:
        if role['id'] == guild['id'] or role['id'] in member_roles:
            bits |= int(role['permissions'])
    if bits & ADMIN:
        return (1 << 62) - 1
    overwrites = channel.get('permission_overwrites', [])
    for overwrite in overwrites:
        if overwrite['type'] == 0 and overwrite['id'] == guild['id']:
            bits = (bits & ~int(overwrite['deny'])) | int(overwrite['allow'])
    deny = allow = 0
    for overwrite in overwrites:
        if overwrite['type'] == 0 and overwrite['id'] in member_roles:
            deny |= int(overwrite['deny'])
            allow |= int(overwrite['allow'])
    bits = (bits & ~deny) | allow
    for overwrite in overwrites:
        if overwrite['type'] == 1 and overwrite['id'] == member['user']['id']:
            bits = (bits & ~int(overwrite['deny'])) | int(overwrite['allow'])
    until = member.get('communication_disabled_until')
    if until and datetime.fromisoformat(until.replace('Z', '+00:00')) > datetime.now(timezone.utc):
        bits &= VIEW | HISTORY
    return bits


def subscription_active(subscription):
    if not subscription or subscription.get('status') not in ('active', 'trialing'):
        return False
    end = subscription.get('current_period_end')
    if end is None:
        return True
    if isinstance(end, str):
        end = datetime.fromisoformat(end.replace('Z', '+00:00'))
    if isinstance(end, datetime):
        return end.replace(tzinfo=end.tzinfo or timezone.utc) > datetime.now(timezone.utc)
    return isinstance(end, date) and end > datetime.now(timezone.utc).date()


def public_user(member):
    user = member['user']
    avatar = user.get('avatar')
    return {'id': str(user['id']), 'name': member.get('nick') or user.get('global_name') or user.get('username') or 'Member',
            'avatar': f"https://cdn.discordapp.com/avatars/{user['id']}/{avatar}.png?size=64" if avatar else None}


def create_arcade_router(*, session_user, is_booster, sub_status, settings, transport=None):
    router = APIRouter(prefix='/api/arcade', tags=['arcade'])
    guild_id = str(settings.get('guild_id') or '')
    bot_token = settings.get('bot_token') or ''
    secret = settings.get('session_secret') or b''
    cookie_name = settings.get('cookie_name', 'dc_sid')
    channel_ids = tuple(dict.fromkeys(x.strip() for x in settings.get('channel_ids', '').split(',') if x.strip()))
    allowed = set(channel_ids)
    origins = {'https://www.discordchats.com', 'https://discordchats.com'}
    site = settings.get('site_url') or ''
    parsed_site = urlparse(site)
    if parsed_site.scheme == 'https' and parsed_site.netloc:
        origins.add(f'{parsed_site.scheme}://{parsed_site.netloc}')
    configured = bool(bot_token and guild_id.isdigit() and int(guild_id) > 0 and allowed and len(allowed) <= 20
                      and all(x.isdigit() for x in allowed) and len(secret) >= 32 and secret != b'dev_dev_dev')
    cache, rates, cooldowns, sending = {}, {}, {}, set()
    blocked_until = 0.0
    cache_lock = asyncio.Lock()

    def require_config():
        if not configured:
            raise HTTPException(503, 'Arcade chat setup is pending. Set ARCADE_CHANNEL_IDS and a strong SESSION_SECRET on the existing backend.')

    def limit(key, maximum, seconds):
        now = time.monotonic()
        if len(rates) > 1000:
            for old_key, old in list(rates.items()):
                if old[1] < now:
                    rates.pop(old_key, None)
        count, until = rates.get(key, (0, now + seconds))
        if until < now:
            count, until = 0, now + seconds
        rates[key] = (count + 1, until)
        if count >= maximum:
            raise HTTPException(429, 'Please wait before trying again.', headers={'Retry-After': str(max(1, int(until - now) + 1))})

    async def discord(path, *, method='GET', payload=None):
        nonlocal blocked_until
        if blocked_until > time.monotonic():
            raise HTTPException(429, 'Discord is rate limiting chat.', headers={'Retry-After': str(int(blocked_until - time.monotonic()) + 1)})
        try:
            async with httpx.AsyncClient(timeout=10.0, transport=transport) as client:
                result = await client.request(method, 'https://discord.com/api/v10' + path,
                                              headers={'Authorization': f'Bot {bot_token}'}, json=payload)
        except httpx.HTTPError:
            raise HTTPException(502, 'Discord is temporarily unavailable.') from None
        if result.status_code == 429:
            retry = max(1.0, float(result.json().get('retry_after', 5)))
            blocked_until = time.monotonic() + retry
            raise HTTPException(429, 'Discord is rate limiting chat.', headers={'Retry-After': str(int(retry) + 1)})
        if result.status_code == 404 and '/members/' in path:
            raise HTTPException(403, 'You must be a current member of this Discord server.')
        if not result.is_success:
            raise HTTPException(503, 'The Discord bot cannot access this resource. Check its channel permissions.')
        return result.json()

    async def cached(path):
        # Small shared cache with no stale fallback after a Discord error.
        async with cache_lock:
            entry = cache.get(path)
            if entry and entry[0] > time.monotonic():
                return entry[1]
            value = await discord(path)
            cache[path] = (time.monotonic() + 5, value)
            return value

    def csrf(request):
        raw = request.cookies.get(cookie_name, '')
        return hmac.new(secret, ('arcade-csrf:' + raw).encode(), hashlib.sha256).hexdigest()

    async def require_user(request):
        require_config()
        uid = session_user(request)
        if not uid:
            raise HTTPException(401, 'Sign in with Discord to use live chat.')
        limit(('read', uid), 60, 60)
        # Membership and roles are refreshed on every request, including sends.
        member = await discord(f'/guilds/{guild_id}/members/{uid}')
        if str(member.get('user', {}).get('id')) != str(uid):
            raise HTTPException(403, 'Your server membership could not be verified.')
        # Preserve the archive site's existing booster / paid-member access rule.
        if not await is_booster(member) and not subscription_active(await sub_status(uid)):
            raise HTTPException(403, 'Use your existing DiscordChats booster or subscription access to enter live chat.')
        return member

    async def info(member, channel_id):
        if channel_id not in allowed:
            raise HTTPException(403, 'This channel is not enabled for the arcade.')
        guild = await cached(f'/guilds/{guild_id}')
        roles = await cached(f'/guilds/{guild_id}/roles')
        channel = await cached(f'/channels/{channel_id}')
        if channel.get('guild_id') != guild_id or guild.get('id') != guild_id or channel.get('type') != 0 or channel.get('nsfw'):
            raise HTTPException(403, 'Only enabled, non-age-restricted text channels in this server are available.')
        bits = permission_bits(member, guild, roles, channel)
        can_read = bits & (VIEW | HISTORY) == VIEW | HISTORY
        can_send = can_read and bool(bits & SEND) and not member.get('pending')
        return channel, bits, can_read, can_send

    def safe_message(message):
        author = message.get('author') or {'id': '', 'username': 'Unknown'}
        attachments = []
        for item in message.get('attachments', []):
            url = item.get('url', '')
            parsed = urlparse(url)
            if parsed.scheme == 'https' and parsed.hostname in ('cdn.discordapp.com', 'media.discordapp.net'):
                attachments.append({'url': url, 'name': item.get('filename') or 'Attachment'})
        return {'id': message['id'], 'content': message.get('content', ''), 'timestamp': message['timestamp'],
                'author': {**public_user({'user': author}), 'bot': bool(author.get('bot'))}, 'attachments': attachments}

    @router.get('/config')
    async def config():
        return JSONResponse({'configured': configured, 'serverName': 'Wolfpac'}, headers={'Cache-Control': 'no-store'})

    @router.get('/me')
    async def me(request: Request):
        member = await require_user(request)
        return JSONResponse({'user': public_user(member), 'csrf': csrf(request)}, headers={'Cache-Control': 'no-store'})

    @router.get('/channels')
    async def channels(request: Request):
        member = await require_user(request)
        available = []
        for channel_id in channel_ids:
            try:
                channel, _, can_read, can_send = await info(member, channel_id)
                if can_read:
                    available.append({'id': channel_id, 'name': channel['name'], 'canSend': can_send})
            except HTTPException as exc:
                if exc.status_code != 403:
                    raise
        return JSONResponse({'channels': available}, headers={'Cache-Control': 'no-store'})

    @router.get('/messages')
    async def messages(request: Request, channel: str):
        member = await require_user(request)
        _, _, can_read, can_send = await info(member, channel)
        if not can_read:
            raise HTTPException(403, 'Your Discord account cannot read this channel.')
        values = await cached(f'/channels/{channel}/messages?limit=50')
        return JSONResponse({'messages': [safe_message(m) for m in reversed(values)], 'canSend': can_send}, headers={'Cache-Control': 'no-store'})

    @router.post('/messages')
    async def send(request: Request):
        if request.headers.get('origin') not in origins or not hmac.compare_digest(request.headers.get('x-csrf-token', ''), csrf(request)):
            raise HTTPException(403, 'Request could not be verified. Reload the arcade.')
        if not request.headers.get('content-type', '').startswith('application/json'):
            raise HTTPException(415, 'Expected JSON.')
        raw = bytearray()
        async for chunk in request.stream():
            raw.extend(chunk)
            if len(raw) > 10000:
                raise HTTPException(413, 'Message is too large.')
        try:
            payload = json.loads(raw)
        except (ValueError, UnicodeError):
            raise HTTPException(400, 'Invalid JSON.') from None
        if not isinstance(payload, dict):
            raise HTTPException(400, 'Invalid message.')
        channel_id = payload.get('channel')
        if not isinstance(channel_id, str) or channel_id not in allowed:
            raise HTTPException(403, 'This channel is not enabled for the arcade.')
        member = await require_user(request)
        uid = str(member['user']['id'])
        limit(('send', uid), 15, 60)
        channel, bits, _, can_send = await info(member, channel_id)
        if not can_send:
            raise HTTPException(403, 'Your Discord account cannot send in this channel.')
        content = payload.get('content')
        if not isinstance(content, str) or not content.strip() or len(content) > 1800:
            raise HTTPException(400, 'Write a message between 1 and 1800 characters.')
        key = (uid, channel_id)
        if key in sending or cooldowns.get(key, 0) > time.monotonic():
            wait = max(2, int(cooldowns.get(key, 0) - time.monotonic()) + 1)
            raise HTTPException(429, 'Please wait for this channel’s slow mode.', headers={'Retry-After': str(wait)})
        bypass = bits & (ADMIN | MANAGE_CHANNELS | MANAGE_MESSAGES | BYPASS_SLOWMODE)
        wait = max(2, 0 if bypass else channel.get('rate_limit_per_user', 0))
        name = re.sub(r'[\r\n\x00-\x1f\\*_~`|<>@]', '', public_user(member)['name'])[:60] or 'Member'
        sending.add(key)
        try:
            message = await discord(f'/channels/{channel_id}/messages', method='POST', payload={
                'content': f'**{name} · via website** (<@{uid}>)\n{content.strip()}',
                'allowed_mentions': {'parse': [], 'replied_user': False},
            })
            cooldowns[key] = time.monotonic() + wait
            if len(cooldowns) > 1000:
                for old_key, until in list(cooldowns.items()):
                    if until < time.monotonic():
                        cooldowns.pop(old_key, None)
            cache.pop(f'/channels/{channel_id}/messages?limit=50', None)
            return JSONResponse({'message': safe_message(message)}, status_code=201, headers={'Cache-Control': 'no-store'})
        finally:
            sending.discard(key)

    return router
