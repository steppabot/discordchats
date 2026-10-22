"""Multiplayer groups for retro games (2-4 Premium players, one host).

The host's browser runs the emulator and streams it to the other players
over WebRTC. This module only keeps the lobby and relays WebRTC signaling;
no game video or audio passes through the server.

State lives in this process's memory, which matches the single gunicorn
worker in the Procfile. Running several workers would split the groups.
"""
import asyncio
import hmac
import json
import os
import re
import secrets
import time

import httpx
from fastapi import APIRouter, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse

GAMES = {'goldeneye007': {'title': 'GoldenEye 007', 'max': 4}}
MIN_PLAYERS = 2
LOBBY_TTL = 2 * 60 * 60      # idle lobby lifetime
PLAYING_TTL = 6 * 60 * 60    # a running game, with nobody connected
ENDED_TTL = 10 * 60          # keep ended groups briefly so late clicks get a clear answer
SIGNAL_LIMIT = 64 * 1024     # one SDP offer or ICE candidate
NO_STORE = {'Cache-Control': 'no-store'}


def clean_name(value):
    value = re.sub(r'[\x00-\x1f\x7f]', '', str(value or '')).strip()
    return value[:80] or 'Player'


def clean_avatar(value):
    value = str(value or '')
    return value if value.startswith('https://cdn.discordapp.com/') and len(value) < 300 else None


def create_group_router(*, session_user, is_premium, profile, csrf, origins, relay_secret,
                        bot_token, transport=None, turn_key_id=None, turn_api_token=None):
    router = APIRouter(prefix='/api/arcade')
    groups = {}            # id -> group
    sockets = {}           # id -> {uid: WebSocket}
    member_of = {}         # uid -> group id (one active group per player)
    lock = asyncio.Lock()
    guild_cache = {}       # (guild, uid) -> (expires, is_member)
    turn_key_id = turn_key_id or os.getenv('CLOUDFLARE_TURN_KEY_ID', '')
    turn_api_token = turn_api_token or os.getenv('CLOUDFLARE_TURN_KEY_API_TOKEN', '')

    # ---------------- helpers ----------------
    def bot_auth(request):
        if len(relay_secret) < 32 or not hmac.compare_digest(
                request.headers.get('authorization', ''), 'Bearer ' + relay_secret):
            raise HTTPException(403, 'Invalid bot credentials.')

    def web_auth(request):
        if request.headers.get('origin') not in origins or not hmac.compare_digest(
                request.headers.get('x-csrf-token', ''), csrf(request)):
            raise HTTPException(403, 'Request could not be verified. Reload the arcade.')

    async def body(request, limit=4096):
        raw = await request.body()
        if len(raw) > limit:
            raise HTTPException(413, 'Request too large.')
        try:
            value = json.loads(raw or b'{}')
        except ValueError:
            raise HTTPException(400, 'Invalid JSON.') from None
        if not isinstance(value, dict):
            raise HTTPException(400, 'Invalid request.')
        return value

    def user_from(value):
        user = value.get('user') if isinstance(value.get('user'), dict) else {}
        uid = str(user.get('id', ''))
        if not uid.isdigit():
            raise HTTPException(400, 'Invalid user.')
        return {'id': uid, 'name': clean_name(user.get('name')), 'avatar': clean_avatar(user.get('avatar'))}

    def prune():
        now = time.time()
        for gid, group in list(groups.items()):
            idle = now - group['updated']
            online = bool(sockets.get(gid))
            if (group['status'] == 'ended' and idle > ENDED_TTL) or \
               (group['status'] == 'lobby' and not online and idle > LOBBY_TTL) or \
               (group['status'] == 'playing' and not online and idle > PLAYING_TTL):
                end_locked(group, 'expired')
                groups.pop(gid, None)
                sockets.pop(gid, None)

    def public(group):
        online = sockets.get(group['id'], {})
        return {
            'id': group['id'], 'game': group['game'], 'title': GAMES[group['game']]['title'],
            'status': group['status'], 'host': group['host'], 'max': GAMES[group['game']]['max'],
            'guild_id': group['guild_id'], 'channel_id': group['channel_id'],
            'players': [{**p, 'online': p['id'] in online} for p in sorted(group['players'], key=lambda p: p['slot'])],
        }

    def find(gid):
        prune()
        group = groups.get(gid)
        if not group:
            raise HTTPException(404, 'This group has ended.')
        return group

    def remove_locked(group, uid):
        group['players'] = [p for p in group['players'] if p['id'] != uid]
        if member_of.get(uid) == group['id']:
            member_of.pop(uid, None)
        group['updated'] = time.time()
        if uid == group['host'] or not group['players']:
            end_locked(group, 'host-left' if uid == group['host'] else 'empty')

    def end_locked(group, reason):
        if group['status'] == 'ended':
            return
        group['status'] = 'ended'
        group['reason'] = reason
        group['updated'] = time.time()
        for p in group['players']:
            if member_of.get(p['id']) == group['id']:
                member_of.pop(p['id'], None)

    def leave_other_groups_locked(uid, keep=None):
        current = member_of.get(uid)
        if current and current != keep and current in groups:
            other = groups[current]
            remove_locked(other, uid)
            asyncio.get_running_loop().create_task(broadcast(other))

    def add_locked(group, user):
        if group['status'] == 'ended':
            raise HTTPException(410, 'This group has ended.')
        existing = next((p for p in group['players'] if p['id'] == user['id']), None)
        if existing:
            existing.update(name=user['name'], avatar=user['avatar'] or existing.get('avatar'))
            return existing
        taken = {p['slot'] for p in group['players']}
        free = [s for s in range(1, GAMES[group['game']]['max'] + 1) if s not in taken]
        if not free:
            raise HTTPException(409, 'This group is full.')
        leave_other_groups_locked(user['id'], keep=group['id'])
        player = {**user, 'slot': free[0]}
        group['players'].append(player)
        member_of[user['id']] = group['id']
        group['updated'] = time.time()
        return player

    async def require_premium(uid):
        if not await is_premium(uid):
            raise HTTPException(403, 'Premium is required to play in a group.')

    async def in_guild(guild_id, uid):
        key = (guild_id, uid)
        cached = guild_cache.get(key)
        if cached and cached[0] > time.monotonic():
            return cached[1]
        try:
            async with httpx.AsyncClient(timeout=10, transport=transport) as client:
                result = await client.get(f'https://discord.com/api/v10/guilds/{guild_id}/members/{uid}',
                                          headers={'Authorization': 'Bot ' + bot_token})
        except httpx.HTTPError:
            raise HTTPException(502, 'Discord is temporarily unavailable.') from None
        if result.status_code not in (200, 404):
            raise HTTPException(502, 'Could not check your server membership.')
        member = result.status_code == 200
        if len(guild_cache) > 2000:
            guild_cache.clear()
        guild_cache[key] = (time.monotonic() + 60, member)
        return member

    async def broadcast(group, extra=None):
        state = public(group)
        room = sockets.get(group['id'], {})
        for uid, ws in list(room.items()):
            message = {'type': 'state', 'group': state, 'you': you(group, uid)}
            if extra:
                message.update(extra)
            try:
                await ws.send_json(message)
            except Exception:
                pass
        if group['status'] == 'ended':
            for uid, ws in list(room.items()):
                room.pop(uid, None)
                try:
                    await ws.close(code=4410)
                except Exception:
                    pass

    def you(group, uid):
        player = next((p for p in group['players'] if p['id'] == uid), None)
        return {'id': uid, 'slot': player['slot'] if player else None, 'host': uid == group['host']}

    # ---------------- bot endpoints ----------------
    @router.post('/bot/groups')
    async def bot_create(request: Request):
        bot_auth(request)
        value = await body(request)
        game = str(value.get('game', ''))
        guild_id, channel_id = str(value.get('guild_id', '')), str(value.get('channel_id', ''))
        if game not in GAMES or not guild_id.isdigit() or not channel_id.isdigit():
            raise HTTPException(400, 'Invalid group.')
        host = user_from(value)
        await require_premium(host['id'])
        async with lock:
            prune()
            leave_other_groups_locked(host['id'])
            gid = secrets.token_urlsafe(9)
            group = {'id': gid, 'game': game, 'guild_id': guild_id, 'channel_id': channel_id,
                     'host': host['id'], 'players': [], 'status': 'lobby',
                     'created': time.time(), 'updated': time.time()}
            groups[gid] = group
            add_locked(group, host)
        return JSONResponse(public(group), status_code=201, headers=NO_STORE)

    @router.post('/bot/groups/{gid}/join')
    async def bot_join(gid: str, request: Request):
        bot_auth(request)
        user = user_from(await body(request))
        await require_premium(user['id'])
        async with lock:
            group = find(gid)
            add_locked(group, user)
        await broadcast(group)
        return JSONResponse(public(group), headers=NO_STORE)

    @router.post('/bot/groups/{gid}/leave')
    async def bot_leave(gid: str, request: Request):
        bot_auth(request)
        user = user_from(await body(request))
        async with lock:
            group = find(gid)
            remove_locked(group, user['id'])
        await broadcast(group)
        return JSONResponse(public(group), headers=NO_STORE)

    @router.get('/bot/groups/{gid}')
    async def bot_get(gid: str, request: Request):
        bot_auth(request)
        async with lock:
            group = find(gid)
        return JSONResponse(public(group), headers=NO_STORE)

    # ---------------- website endpoints ----------------
    async def web_user(request):
        uid = await session_user(request)
        if not uid:
            raise HTTPException(401, 'Sign in with Discord to join this group.')
        return str(uid)

    @router.get('/groups/ice')
    async def ice(request: Request):
        await web_user(request)
        servers = [{'urls': ['stun:stun.cloudflare.com:3478', 'stun:stun.l.google.com:19302']}]
        if turn_key_id and turn_api_token:
            try:
                async with httpx.AsyncClient(timeout=10, transport=transport) as client:
                    result = await client.post(
                        f'https://rtc.live.cloudflare.com/v1/turn/keys/{turn_key_id}/credentials/generate-ice-servers',
                        headers={'Authorization': 'Bearer ' + turn_api_token}, json={'ttl': 6 * 60 * 60})
                if result.is_success:
                    servers = []
                    for item in result.json().get('iceServers', []):
                        urls = item.get('urls')
                        urls = [urls] if isinstance(urls, str) else list(urls or [])
                        # Browsers block port 53; those URLs only add a timeout.
                        urls = [u for u in urls if ':53?' not in u and not u.endswith(':53')]
                        if urls:
                            servers.append({**item, 'urls': urls})
            except (httpx.HTTPError, ValueError):
                pass  # STUN-only still works for most home networks
        return JSONResponse({'iceServers': servers}, headers=NO_STORE)

    @router.get('/groups/{gid}')
    async def web_get(gid: str, request: Request):
        uid = await web_user(request)
        async with lock:
            group = find(gid)
            return JSONResponse({'group': public(group), 'you': you(group, uid)}, headers=NO_STORE)

    @router.post('/groups/{gid}/join')
    async def web_join(gid: str, request: Request):
        web_auth(request)
        uid = await web_user(request)
        async with lock:
            group = find(gid)
            guild_id = group['guild_id']
        if not await in_guild(guild_id, uid):
            raise HTTPException(403, 'Join the Discord server this group was made in first.')
        await require_premium(uid)
        user = await profile(request)
        async with lock:
            group = find(gid)
            add_locked(group, {'id': uid, 'name': clean_name(user.get('name')), 'avatar': clean_avatar(user.get('avatar'))})
        await broadcast(group)
        return JSONResponse({'group': public(group), 'you': you(group, uid)}, headers=NO_STORE)

    @router.post('/groups/{gid}/leave')
    async def web_leave(gid: str, request: Request):
        web_auth(request)
        uid = await web_user(request)
        async with lock:
            group = find(gid)
            remove_locked(group, uid)
        await broadcast(group)
        return JSONResponse({'ok': True}, headers=NO_STORE)

    @router.websocket('/groups/{gid}/live')
    async def live(ws: WebSocket, gid: str):
        if ws.headers.get('origin') not in origins or not hmac.compare_digest(
                ws.query_params.get('csrf', ''), csrf(ws)):
            await ws.close(code=4403)
            return
        uid = await session_user(ws)
        group = groups.get(gid)
        if not uid or not group or group['status'] == 'ended' or \
                not any(p['id'] == str(uid) for p in group['players']):
            await ws.close(code=4404)
            return
        uid = str(uid)
        if not await is_premium(uid):
            await ws.close(code=4403)
            return
        await ws.accept()
        room = sockets.setdefault(gid, {})
        old = room.get(uid)
        room[uid] = ws
        if old:
            try:
                await old.close(code=4409)  # replaced by a newer tab
            except Exception:
                pass
        group['updated'] = time.time()
        await broadcast(group)
        try:
            while True:
                try:
                    raw = await asyncio.wait_for(ws.receive_text(), timeout=25)
                except asyncio.TimeoutError:
                    await ws.send_json({'type': 'ping'})
                    continue
                if len(raw) > SIGNAL_LIMIT:
                    continue
                try:
                    message = json.loads(raw)
                except ValueError:
                    continue
                if not isinstance(message, dict):
                    continue
                kind = message.get('type')
                group = groups.get(gid)
                if not group or group['status'] == 'ended':
                    break
                is_host = uid == group['host']
                if kind == 'signal':
                    to = str(message.get('to', ''))
                    # Signaling only flows between the host and each guest.
                    if to == uid or (not is_host and to != group['host']):
                        continue
                    target = sockets.get(gid, {}).get(to)
                    if target and any(p['id'] == to for p in group['players']):
                        try:
                            await target.send_json({'type': 'signal', 'from': uid, 'data': message.get('data')})
                        except Exception:
                            pass
                elif kind == 'start' and is_host:
                    if len([p for p in group['players'] if p['id'] in sockets.get(gid, {})]) < MIN_PLAYERS:
                        await ws.send_json({'type': 'error', 'detail': 'Wait for at least one more player.'})
                        continue
                    group['status'] = 'playing'
                    group['updated'] = time.time()
                    await broadcast(group, {'event': 'start'})
                elif kind == 'leave':
                    async with lock:
                        remove_locked(group, uid)
                    await broadcast(group)
                    break
                group['updated'] = time.time()
        except (WebSocketDisconnect, RuntimeError, OSError):
            pass
        finally:
            room = sockets.get(gid, {})
            if room.get(uid) is ws:
                room.pop(uid, None)
            group = groups.get(gid)
            if group:
                group['updated'] = time.time()
                await broadcast(group)

    return router
