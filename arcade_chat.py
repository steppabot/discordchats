"""Wolfpac arcade: permission-checked live chat using the existing site login.

Only ARCADE_CHANNEL_IDS in DISCORD_GUILD_ID are exposed. Browser sends use
webhook identity overrides derived from the authenticated server member. No archive writes.
"""
import asyncio
import hashlib
import hmac
import json
import logging
import re
import time
from datetime import datetime, timezone, date
from urllib.parse import urlparse

import httpx
from fastapi import APIRouter, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse

# Webhook tokens live in URL paths: do not emit HTTP client request URLs.
logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('httpcore').setLevel(logging.WARNING)

ATTACH_FILES = 32768
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
    origins = {'https://www.discordarcade.com', 'https://discordarcade.com'}
    site = settings.get('site_url') or ''
    parsed_site = urlparse(site)
    if parsed_site.scheme == 'https' and parsed_site.netloc:
        origins.add(f'{parsed_site.scheme}://{parsed_site.netloc}')
    configured = bool(bot_token and guild_id.isdigit() and int(guild_id) > 0 and allowed and len(allowed) <= 20
                      and all(x.isdigit() for x in allowed) and len(secret) >= 32 and secret != b'dev_dev_dev')
    cache, rates, cooldowns, sending = {}, {}, {}, set()
    subscribers = {}
    relay_secret = settings.get('relay_secret', '')
    blocked_until = 0.0
    cache_lock = asyncio.Lock()
    hooks = {}
    hook_lock = asyncio.Lock()

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

    async def discord(path, *, method='GET', payload=None, webhook=False, files=None):
        nonlocal blocked_until
        if blocked_until > time.monotonic():
            raise HTTPException(429, 'Discord is rate limiting chat.', headers={'Retry-After': str(int(blocked_until - time.monotonic()) + 1)})
        try:
            async with httpx.AsyncClient(timeout=25.0 if files else 10.0, transport=transport) as client:
                result = await client.request(method, 'https://discord.com/api/v10' + path,
                                              headers={} if webhook else {'Authorization': f'Bot {bot_token}'},
                                              **({'data': {'payload_json': json.dumps(payload)}, 'files': files} if files else {'json': payload}))
        except httpx.HTTPError:
            raise HTTPException(502, 'Discord is temporarily unavailable.') from None
        if result.status_code == 429:
            retry = max(1.0, float(result.json().get('retry_after', 5)))
            blocked_until = time.monotonic() + retry
            raise HTTPException(429, 'Discord is rate limiting chat.', headers={'Retry-After': str(int(retry) + 1)})
        if webhook and result.status_code == 404:
            raise HTTPException(410, 'Arcade webhook was deleted.')
        if result.status_code == 403 and '/webhooks' in path:
            raise HTTPException(503, 'The server bot needs Manage Webhooks in this channel.')
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

    async def channel_hook(channel_id):
        async with hook_lock:
            if channel_id in hooks:
                return hooks[channel_id]
            bot = await cached('/users/@me')
            existing = await discord(f'/channels/{channel_id}/webhooks')
            hook = next((h for h in existing if h.get('type') == 1
                         and h.get('name') == 'Arcade Chat'
                         and str((h.get('user') or {}).get('id')) == str(bot['id'])
                         and h.get('token')), None)
            if hook is None:
                hook = await discord(f'/channels/{channel_id}/webhooks', method='POST', payload={'name': 'Arcade Chat'})
            if not hook.get('token') or not str(hook.get('id', '')).isdigit():
                raise HTTPException(503, 'Unable to initialize arcade webhook.')
            hooks[channel_id] = hook
            return hook

    async def webhook_send(channel_id, member, content, uploads=None):
        user = member['user']
        name = public_user(member)['name']
        name = re.sub(r'[\x00-\x1f\x7f]', '', name).strip()[:80] or 'Member'
        name = re.sub('discord', 'disc\u200bord', name, flags=re.I)
        name = re.sub('clyde', 'cly\u200bde', name, flags=re.I)[:80]
        avatar = public_user(member)['avatar']
        if member.get('avatar'):
            avatar = f"https://cdn.discordapp.com/guilds/{guild_id}/users/{user['id']}/avatars/{member['avatar']}.png?size=128"
        if not avatar:
            discriminator = int(user.get('discriminator') or '0')
            index = discriminator % 5 if discriminator else (int(user['id']) >> 22) % 6
            avatar = f'https://cdn.discordapp.com/embed/avatars/{index}.png'
        payload = {'content': content, 'username': name, 'avatar_url': avatar,
                   'allowed_mentions': {'parse': [], 'replied_user': False}}
        files = [(f'files[{i}]', item) for i, item in enumerate(uploads or [])]
        if files:
            payload['attachments'] = [{'id': i, 'filename': item[0]} for i, item in enumerate(uploads)]
        for attempt in range(2):
            hook = await channel_hook(channel_id)
            try:
                return await discord(f"/webhooks/{hook['id']}/{hook['token']}?wait=true",
                                     method='POST', payload=payload, webhook=True, files=files)
            except HTTPException as exc:
                if exc.status_code != 410 or attempt:
                    raise
                hooks.pop(channel_id, None)

    def csrf(request):
        raw = request.cookies.get(cookie_name, '')
        return hmac.new(secret, ('arcade-csrf:' + raw).encode(), hashlib.sha256).hexdigest()

    async def require_user(request):
        require_config()
        uid = session_user(request)
        if not uid:
            raise HTTPException(401, 'Sign in with Discord to use live chat.')
        limit(('read', uid), 180, 60)
        # Membership and roles are refreshed on every request, including sends.
        member = await cached(f'/guilds/{guild_id}/members/{uid}')
        if str(member.get('user', {}).get('id')) != str(uid):
            raise HTTPException(403, 'Your server membership could not be verified.')
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
                attachments.append({'url': url, 'name': item.get('filename') or 'Attachment', 'type': item.get('content_type', '')})
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
        _, bits, can_read, can_send = await info(member, channel)
        if not can_read:
            raise HTTPException(403, 'Your Discord account cannot read this channel.')
        values = await cached(f'/channels/{channel}/messages?limit=50')
        roles = await cached(f'/guilds/{guild_id}/roles')
        profiles = {}
        semaphore = asyncio.Semaphore(4)
        async def profile(uid):
            async with semaphore:
                try:
                    member = await cached(f'/guilds/{guild_id}/members/{uid}')
                    user = public_user(member)
                    if member.get('avatar'):
                        user['avatar'] = f"https://cdn.discordapp.com/guilds/{guild_id}/users/{uid}/avatars/{member['avatar']}.png?size=64"
                    colored = [x for x in roles if x['id'] in member.get('roles', []) and x.get('color')]
                    if colored:
                        user['color'] = '#%06x' % max(colored, key=lambda x: x.get('position', 0))['color']
                    profiles[uid] = user
                except HTTPException:
                    pass
        await asyncio.gather(*(profile(uid) for uid in {m['author']['id'] for m in values if not m.get('webhook_id')}))
        result = []
        for m in reversed(values):
            item = safe_message(m)
            if not m.get('webhook_id'):
                item['author'].update(profiles.get(m['author']['id'], {}))
            result.append(item)
        return JSONResponse({'messages': result, 'canSend': can_send, 'canAttach': can_send and bool(bits & ATTACH_FILES)}, headers={'Cache-Control': 'no-store'})

    @router.post('/messages')
    async def send(request: Request):
        if request.headers.get('origin') not in origins or not hmac.compare_digest(request.headers.get('x-csrf-token', ''), csrf(request)):
            raise HTTPException(403, 'Request could not be verified. Reload the arcade.')
        member = await require_user(request)
        uid = str(member['user']['id'])
        limit(('send', uid), 15, 60)
        content_type = request.headers.get('content-type', '')
        multipart = content_type.startswith('multipart/form-data')
        if not multipart and not content_type.startswith('application/json'):
            raise HTTPException(415, 'Expected JSON or file upload.')
        raw = bytearray()
        async for chunk in request.stream():
            raw.extend(chunk)
            if len(raw) > (20 * 1024 * 1024 + 65536 if multipart else 10000):
                raise HTTPException(413, 'Attachments must total 20 MB or less.')
        uploads = []
        if multipart:
            request._body = bytes(raw)
            async with request.form(max_files=4, max_fields=2) as form:
                payload = {'channel': form.get('channel'), 'content': form.get('content', '')}
                for field, item in form.multi_items():
                    if not hasattr(item, 'filename'):
                        continue
                    if field != 'files':
                        raise HTTPException(400, 'Invalid file field.')
                    data = await item.read(8 * 1024 * 1024 + 1)
                    if not data or len(data) > 8 * 1024 * 1024:
                        raise HTTPException(413, 'Each attachment must be between 1 byte and 8 MB.')
                    mime = item.content_type or ''
                    valid = ((mime == 'image/png' and data.startswith(b'\x89PNG\r\n\x1a\n'))
                        or (mime == 'image/jpeg' and data.startswith(b'\xff\xd8\xff'))
                        or (mime == 'image/gif' and data[:6] in (b'GIF87a', b'GIF89a'))
                        or (mime == 'image/webp' and data[:4] == b'RIFF' and data[8:12] == b'WEBP')
                        or (mime == 'video/mp4' and data[4:8] == b'ftyp')
                        or (mime == 'video/webm' and data.startswith(b'\x1a\x45\xdf\xa3')))
                    if not valid:
                        raise HTTPException(415, 'Choose a PNG, JPG, GIF, WebP, MP4 or WebM file.')
                    extension = {'image/png':'.png','image/jpeg':'.jpg','image/gif':'.gif','image/webp':'.webp','video/mp4':'.mp4','video/webm':'.webm'}[mime]
                    stem = re.sub(r'[^a-zA-Z0-9_-]', '_', (item.filename or 'attachment').rsplit('.', 1)[0])[:70] or 'attachment'
                    uploads.append((f'{len(uploads)+1}-{stem}{extension}', data, mime))
                if sum(len(item[1]) for item in uploads) > 20 * 1024 * 1024:
                    raise HTTPException(413, 'Attachments must total 20 MB or less.')
        else:
            try:
                payload = json.loads(raw)
            except (ValueError, UnicodeError):
                raise HTTPException(400, 'Invalid JSON.') from None
        if not isinstance(payload, dict):
            raise HTTPException(400, 'Invalid message.')
        channel_id = payload.get('channel')
        if not isinstance(channel_id, str) or channel_id not in allowed:
            raise HTTPException(403, 'This channel is not enabled for the arcade.')
        channel, bits, _, can_send = await info(member, channel_id)
        if not can_send:
            raise HTTPException(403, 'Your Discord account cannot send in this channel.')
        if uploads and not bits & ATTACH_FILES:
            raise HTTPException(403, 'Your Discord account cannot attach files in this channel.')
        content = payload.get('content', '')
        if not isinstance(content, str) or (not content.strip() and not uploads) or len(content) > 1800:
            raise HTTPException(400, 'Add a message or attachment; text is limited to 1800 characters.')
        key = (uid, channel_id)
        if key in sending or cooldowns.get(key, 0) > time.monotonic():
            wait = max(2, int(cooldowns.get(key, 0) - time.monotonic()) + 1)
            raise HTTPException(429, 'Please wait for this channel’s slow mode.', headers={'Retry-After': str(wait)})
        bypass = bits & (ADMIN | MANAGE_CHANNELS | MANAGE_MESSAGES | BYPASS_SLOWMODE)
        wait = max(2, 0 if bypass else channel.get('rate_limit_per_user', 0))
        sending.add(key)
        try:
            message = await webhook_send(channel_id, member, content.strip(), uploads)
            cooldowns[key] = time.monotonic() + wait
            if len(cooldowns) > 1000:
                for old_key, until in list(cooldowns.items()):
                    if until < time.monotonic():
                        cooldowns.pop(old_key, None)
            cache.pop(f'/channels/{channel_id}/messages?limit=50', None)
            return JSONResponse({'message': safe_message(message)}, status_code=201, headers={'Cache-Control': 'no-store'})
        finally:
            sending.discard(key)

    @router.post('/relay')
    async def relay(request: Request):
        # Only the bot knows this key. Event bodies contain IDs, never message content.
        if len(relay_secret) < 32 or not hmac.compare_digest(request.headers.get('authorization', ''), 'Bearer ' + relay_secret):
            raise HTTPException(403, 'Invalid relay credentials.')
        raw = bytearray()
        async for chunk in request.stream():
            raw.extend(chunk)
            if len(raw) > 2048:
                raise HTTPException(413, 'Event too large.')
        try:
            event = json.loads(raw)
            channel = str(event['channel_id'])
            if str(event['guild_id']) != guild_id or channel not in allowed:
                raise ValueError()
        except (ValueError, KeyError, TypeError):
            raise HTTPException(400, 'Invalid event.') from None
        async with cache_lock:
            cache.clear()
        for queue, selected in list(subscribers.items()):
            if selected == channel and queue.empty():
                queue.put_nowait(True)
        return {'ok': True}

    @router.websocket('/live')
    async def live(ws: WebSocket):
        queue = asyncio.Queue(maxsize=1)
        try:
            if ws.headers.get('origin') not in origins or not hmac.compare_digest(ws.query_params.get('csrf', ''), csrf(ws)):
                await ws.close(code=4403)
                return
            channel = ws.query_params.get('channel', '')
            member = await require_user(ws)
            if not (await info(member, channel))[2]:
                await ws.close(code=4403)
                return
            if len(subscribers) >= 200:
                await ws.close(code=1013)
                return
            await ws.accept()
            subscribers[queue] = channel
            await ws.send_json({'type': 'refresh'})
            while True:
                try:
                    await asyncio.wait_for(queue.get(), timeout=20)
                    kind = 'refresh'
                    await asyncio.sleep(0.3)  # coalesce rapid bursts
                    while not queue.empty():
                        queue.get_nowait()
                except asyncio.TimeoutError:
                    kind = 'heartbeat'
                # Revalidate even idle clients; no message payload crosses this socket.
                member = await require_user(ws)
                if not (await info(member, channel))[2]:
                    await ws.close(code=4403)
                    return
                await ws.send_json({'type': kind})
        except HTTPException:
            await ws.close(code=4403)
        except (WebSocketDisconnect, RuntimeError, OSError):
            pass
        finally:
            subscribers.pop(queue, None)

    return router
