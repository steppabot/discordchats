import unittest

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

from arcade_groups import create_group_router

SECRET = 'r' * 32
BOT = {'Authorization': 'Bearer ' + SECRET}
ORIGIN = 'https://discordarcade.com'


def user(uid, name=None):
    return {'user': {'id': uid, 'name': name or 'P' + uid, 'avatar': 'https://cdn.discordapp.com/avatars/x.png'}}


class GroupTests(unittest.TestCase):
    def setUp(self):
        self.premium = {'1', '2', '3', '4', '5'}
        self.guild_members = {'1', '2', '3', '4', '5', '6'}

        async def session_user(request):
            return request.cookies.get('sid') or None

        async def is_premium(uid):
            return str(uid) in self.premium

        async def profile(request):
            uid = request.cookies.get('sid')
            if not uid:
                raise HTTPException(401, 'Sign in')
            return {'id': uid, 'name': 'Web' + uid, 'avatar': None}

        def csrf(request):
            return 'csrf-' + request.cookies.get('sid', '')

        def discord(req):
            uid = req.url.path.rsplit('/', 1)[-1]
            return httpx.Response(200 if uid in self.guild_members else 404, json={})

        app = FastAPI()
        app.include_router(create_group_router(
            session_user=session_user, is_premium=is_premium, profile=profile, csrf=csrf,
            origins={ORIGIN}, relay_secret=SECRET, bot_token='bot', transport=httpx.MockTransport(discord)))
        self.app = app

    def client(self, uid=None):
        c = TestClient(self.app, base_url='https://api.discordarcade.com')
        if uid:
            c.cookies.set('sid', uid)
        return c

    def web(self, uid):
        return {'Origin': ORIGIN, 'X-CSRF-Token': 'csrf-' + uid}

    def create(self, host='1'):
        r = self.client().post('/api/arcade/bot/groups', headers=BOT,
                               json={'game': 'goldeneye007', 'guild_id': '100', 'channel_id': '200', **user(host)})
        self.assertEqual(r.status_code, 201, r.text)
        return r.json()

    def test_bot_endpoints_need_secret(self):
        r = self.client().post('/api/arcade/bot/groups', json={'game': 'goldeneye007', 'guild_id': '1', 'channel_id': '2', **user('1')})
        self.assertEqual(r.status_code, 403)

    def test_create_requires_premium_and_known_game(self):
        r = self.client().post('/api/arcade/bot/groups', headers=BOT,
                               json={'game': 'goldeneye007', 'guild_id': '100', 'channel_id': '200', **user('9')})
        self.assertEqual(r.status_code, 403)
        r = self.client().post('/api/arcade/bot/groups', headers=BOT,
                               json={'game': 'mario', 'guild_id': '100', 'channel_id': '200', **user('1')})
        self.assertEqual(r.status_code, 400)

    def test_join_assigns_slots_and_caps_at_four(self):
        g = self.create('1')
        self.assertEqual([p['slot'] for p in g['players']], [1])
        for uid, slot in (('2', 2), ('3', 3), ('4', 4)):
            r = self.client().post(f"/api/arcade/bot/groups/{g['id']}/join", headers=BOT, json=user(uid))
            self.assertEqual(r.status_code, 200)
            self.assertEqual(next(p['slot'] for p in r.json()['players'] if p['id'] == uid), slot)
        r = self.client().post(f"/api/arcade/bot/groups/{g['id']}/join", headers=BOT, json=user('5'))
        self.assertEqual(r.status_code, 409)
        # Joining twice keeps the same slot.
        r = self.client().post(f"/api/arcade/bot/groups/{g['id']}/join", headers=BOT, json=user('3'))
        self.assertEqual(next(p['slot'] for p in r.json()['players'] if p['id'] == '3'), 3)

    def test_slot_reused_after_leave(self):
        g = self.create('1')
        for uid in ('2', '3'):
            self.client().post(f"/api/arcade/bot/groups/{g['id']}/join", headers=BOT, json=user(uid))
        self.client().post(f"/api/arcade/bot/groups/{g['id']}/leave", headers=BOT, json=user('2'))
        r = self.client().post(f"/api/arcade/bot/groups/{g['id']}/join", headers=BOT, json=user('4'))
        self.assertEqual(next(p['slot'] for p in r.json()['players'] if p['id'] == '4'), 2)

    def test_join_requires_premium(self):
        g = self.create('1')
        r = self.client().post(f"/api/arcade/bot/groups/{g['id']}/join", headers=BOT, json=user('9'))
        self.assertEqual(r.status_code, 403)

    def test_host_leaving_ends_group(self):
        g = self.create('1')
        self.client().post(f"/api/arcade/bot/groups/{g['id']}/join", headers=BOT, json=user('2'))
        r = self.client().post(f"/api/arcade/bot/groups/{g['id']}/leave", headers=BOT, json=user('1'))
        self.assertEqual(r.json()['status'], 'ended')
        r = self.client().post(f"/api/arcade/bot/groups/{g['id']}/join", headers=BOT, json=user('3'))
        self.assertEqual(r.status_code, 410)

    def test_player_is_in_one_group_at_a_time(self):
        a = self.create('1')
        self.client().post(f"/api/arcade/bot/groups/{a['id']}/join", headers=BOT, json=user('2'))
        b = self.create('3')
        r = self.client().post(f"/api/arcade/bot/groups/{b['id']}/join", headers=BOT, json=user('2'))
        self.assertEqual(len(r.json()['players']), 2)
        r = self.client().get(f"/api/arcade/bot/groups/{a['id']}", headers=BOT)
        self.assertEqual([p['id'] for p in r.json()['players']], ['1'])
        # A host who starts a new group ends their old one.
        c = self.create('1')
        self.assertEqual(self.client().get(f"/api/arcade/bot/groups/{a['id']}", headers=BOT).json()['status'], 'ended')
        self.assertEqual(c['players'][0]['id'], '1')

    def test_web_join_checks_csrf_guild_and_premium(self):
        g = self.create('1')
        c = self.client('2')
        self.assertEqual(c.post(f"/api/arcade/groups/{g['id']}/join").status_code, 403)
        r = c.post(f"/api/arcade/groups/{g['id']}/join", headers=self.web('2'))
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()['you'], {'id': '2', 'slot': 2, 'host': False})
        self.assertEqual(r.json()['group']['players'][1]['name'], 'Web2')
        # Premium but not in the group's server.
        self.guild_members.discard('3')
        self.assertEqual(self.client('3').post(f"/api/arcade/groups/{g['id']}/join", headers=self.web('3')).status_code, 403)
        # In the server but not Premium.
        self.assertEqual(self.client('6').post(f"/api/arcade/groups/{g['id']}/join", headers=self.web('6')).status_code, 403)

    def test_get_requires_login(self):
        g = self.create('1')
        self.assertEqual(self.client().get(f"/api/arcade/groups/{g['id']}").status_code, 401)
        r = self.client('1').get(f"/api/arcade/groups/{g['id']}")
        self.assertEqual(r.json()['you']['host'], True)
        self.assertEqual(self.client('1').get('/api/arcade/groups/nope').status_code, 404)

    def test_ice_defaults_to_stun(self):
        r = self.client('1').get('/api/arcade/groups/ice')
        self.assertTrue(r.json()['iceServers'][0]['urls'][0].startswith('stun:'))
        self.assertEqual(self.client().get('/api/arcade/groups/ice').status_code, 401)

    def socket(self, client, gid, uid):
        return client.websocket_connect(f'/api/arcade/groups/{gid}/live?csrf=csrf-{uid}', headers={'Origin': ORIGIN})

    def test_socket_lobby_signaling_and_start(self):
        g = self.create('1')
        self.client().post(f"/api/arcade/bot/groups/{g['id']}/join", headers=BOT, json=user('2'))
        self.client().post(f"/api/arcade/bot/groups/{g['id']}/join", headers=BOT, json=user('3'))
        host_c, guest_c, other_c = self.client('1'), self.client('2'), self.client('3')
        with self.socket(host_c, g['id'], '1') as host:
            first = host.receive_json()
            self.assertEqual(first['you'], {'id': '1', 'slot': 1, 'host': True})
            # Host alone cannot start.
            host.send_json({'type': 'start'})
            self.assertEqual(host.receive_json()['type'], 'error')
            with self.socket(guest_c, g['id'], '2') as guest:
                self.assertEqual(guest.receive_json()['you']['slot'], 2)
                state = host.receive_json()
                self.assertEqual([p['online'] for p in state['group']['players']], [True, True, False])
                # Guest -> host signal is relayed with the sender id.
                guest.send_json({'type': 'signal', 'to': '1', 'data': {'sdp': 'offer'}})
                self.assertEqual(host.receive_json(), {'type': 'signal', 'from': '2', 'data': {'sdp': 'offer'}})
                # Guests cannot signal each other (only via the host).
                with self.socket(other_c, g['id'], '3') as other:
                    other.receive_json(); host.receive_json(); guest.receive_json()
                    guest.send_json({'type': 'signal', 'to': '3', 'data': 'x'})
                    host.send_json({'type': 'signal', 'to': '3', 'data': {'sdp': 'answer'}})
                    self.assertEqual(other.receive_json(), {'type': 'signal', 'from': '1', 'data': {'sdp': 'answer'}})
                    # Only the host starts the game.
                    guest.send_json({'type': 'start'})
                    host.send_json({'type': 'start'})
                    started = guest.receive_json()
                    self.assertEqual(started['event'], 'start')
                    self.assertEqual(started['group']['status'], 'playing')

    def test_socket_rejects_non_members_and_bad_csrf(self):
        g = self.create('1')
        with self.assertRaises(WebSocketDisconnect):
            with self.socket(self.client('2'), g['id'], '2') as ws:
                ws.receive_json()
        with self.assertRaises(WebSocketDisconnect):
            with self.client('1').websocket_connect(f"/api/arcade/groups/{g['id']}/live?csrf=wrong",
                                                    headers={'Origin': ORIGIN}) as ws:
                ws.receive_json()

    def test_host_leave_over_socket_ends_for_everyone(self):
        g = self.create('1')
        self.client().post(f"/api/arcade/bot/groups/{g['id']}/join", headers=BOT, json=user('2'))
        with self.socket(self.client('1'), g['id'], '1') as host:
            host.receive_json()
            with self.socket(self.client('2'), g['id'], '2') as guest:
                guest.receive_json(); host.receive_json()
                host.send_json({'type': 'leave'})
                state = guest.receive_json()
                self.assertEqual(state['group']['status'], 'ended')
                with self.assertRaises(WebSocketDisconnect):
                    guest.receive_json()


if __name__ == '__main__':
    unittest.main()
