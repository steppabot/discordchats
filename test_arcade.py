import ast
import asyncio
import copy
import hashlib
import hmac
import json
import secrets
import time
import unittest
from datetime import datetime, timedelta, timezone
from typing import Optional
from urllib.parse import urlparse, parse_qs

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from starlette.responses import RedirectResponse
from live_chat import create_arcade_router, permission_bits, subscription_active, VIEW, SEND, HISTORY

GUILD={'id':'100','owner_id':'999'}
ROLES=[{'id':'100','permissions':str(VIEW|SEND|HISTORY)}]
MEMBER={'user':{'id':'123','username':'Greg','avatar':None},'roles':[]}
CHANNEL={'id':'200','guild_id':'100','name':'general','type':0,'nsfw':False,'permission_overwrites':[],'rate_limit_per_user':0}


class Permissions(unittest.TestCase):
    def test_member_deny_wins_over_role_allow(self):
        member={**MEMBER,'roles':['300']}
        channel={**CHANNEL,'permission_overwrites':[{'id':'100','type':0,'deny':str(SEND),'allow':'0'}, {'id':'300','type':0,'deny':'0','allow':str(SEND)}, {'id':'123','type':1,'deny':str(SEND),'allow':'0'}]}
        self.assertFalse(permission_bits(member,GUILD,ROLES,channel)&SEND)

    def test_timeout_keeps_reading_and_removes_sending(self):
        member={**MEMBER,'communication_disabled_until':(datetime.now(timezone.utc)+timedelta(hours=1)).isoformat()}
        self.assertEqual(permission_bits(member,GUILD,ROLES,CHANNEL),VIEW|HISTORY)

    def test_owner_bypasses_overwrites(self):
        member={**MEMBER,'user':{**MEMBER['user'],'id':'999'}}
        channel={**CHANNEL,'permission_overwrites':[{'id':'100','type':0,'deny':str(VIEW|SEND|HISTORY),'allow':'0'}]}
        self.assertTrue(permission_bits(member,GUILD,ROLES,channel)&SEND)

    def test_subscription_expiry(self):
        now=datetime.now(timezone.utc)
        self.assertTrue(subscription_active({'status':'active','current_period_end':now+timedelta(hours=1)}))
        self.assertFalse(subscription_active({'status':'active','current_period_end':now-timedelta(seconds=1)}))
        self.assertFalse(subscription_active({'status':'canceled','current_period_end':None}))


class LiveChat(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.member=copy.deepcopy(MEMBER);self.channel=copy.deepcopy(CHANNEL);self.booster=True;self.posted=[];self.member_status=200;self.rate_limited=False
        async def mock_discord(request):
            path=request.url.path
            if self.rate_limited:return httpx.Response(429,json={'retry_after':5})
            if path.endswith('/members/123'):return httpx.Response(self.member_status,json=self.member if self.member_status==200 else {})
            if path.endswith('/guilds/100'):return httpx.Response(200,json=GUILD)
            if path.endswith('/roles'):return httpx.Response(200,json=ROLES)
            if path.endswith('/channels/200'):return httpx.Response(200,json=self.channel)
            if path.endswith('/messages'):
                if request.method=='POST':
                    self.posted.append(json.loads(request.content))
                    return httpx.Response(200,json={'id':'501','author':{'id':'50','username':'Arcade','bot':True},'content':self.posted[-1]['content'],'timestamp':'2026-10-17T08:00:00Z'})
                return httpx.Response(200,json=[{'id':'500','author':MEMBER['user'],'content':'Hello','timestamp':'2026-10-17T08:00:00Z'}])
            raise AssertionError(path)
        async def booster(member):return self.booster
        async def sub_status(uid):return None
        app=FastAPI()
        app.include_router(create_arcade_router(session_user=lambda r:123 if r.cookies.get('dc_sid')=='valid' else None,
            is_booster=booster,sub_status=sub_status,transport=httpx.MockTransport(mock_discord),settings={
            'guild_id':'100','bot_token':'test-bot','session_secret':b'x'*32,'cookie_name':'dc_sid','channel_ids':'200'}))
        self.client=httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='https://api.discordchats.com',cookies={'dc_sid':'valid'})

    async def asyncTearDown(self):await self.client.aclose()
    async def headers(self):
        response=await self.client.get('/api/arcade/me');self.assertEqual(response.status_code,200)
        return {'Origin':'https://www.discordchats.com','X-CSRF-Token':response.json()['csrf']}

    async def test_read_and_send_real_permission_path(self):
        headers=await self.headers()
        result=await self.client.get('/api/arcade/messages?channel=200')
        self.assertEqual(result.json()['messages'][0]['content'],'Hello')
        sent=await self.client.post('/api/arcade/messages',headers=headers,json={'channel':'200','content':'@everyone hi'})
        self.assertEqual(sent.status_code,201)
        self.assertEqual(self.posted[0]['allowed_mentions']['parse'],[])
        self.assertIn('Greg · via website',self.posted[0]['content'])
        self.assertIn('<@123>',self.posted[0]['content'])

    async def test_signed_out_user_cannot_read(self):
        self.client.cookies.clear()
        self.assertEqual((await self.client.get('/api/arcade/messages?channel=200')).status_code,401)

    async def test_removed_member_cannot_read(self):
        self.member_status=404
        self.assertEqual((await self.client.get('/api/arcade/messages?channel=200')).status_code,403)

    async def test_existing_paid_booster_gate_remains(self):
        self.booster=False
        self.assertEqual((await self.client.get('/api/arcade/me')).status_code,403)

    async def test_disallowed_channel_is_rejected(self):
        self.assertEqual((await self.client.get('/api/arcade/messages?channel=999')).status_code,403)

    async def test_channel_from_another_server_is_rejected(self):
        self.channel['guild_id']='101'
        self.assertEqual((await self.client.get('/api/arcade/messages?channel=200')).status_code,403)

    async def test_hidden_channel_is_not_listed_or_read(self):
        self.channel['permission_overwrites']=[{'id':'123','type':1,'deny':str(VIEW),'allow':'0'}]
        self.assertEqual((await self.client.get('/api/arcade/channels')).json()['channels'],[])
        self.assertEqual((await self.client.get('/api/arcade/messages?channel=200')).status_code,403)

    async def test_bad_origin_and_csrf_are_rejected(self):
        headers=await self.headers();headers['Origin']='https://evil.example'
        self.assertEqual((await self.client.post('/api/arcade/messages',headers=headers,json={'channel':'200','content':'hi'})).status_code,403)
        headers['Origin']='https://www.discordchats.com';headers['X-CSRF-Token']='wrong'
        self.assertEqual((await self.client.post('/api/arcade/messages',headers=headers,json={'channel':'200','content':'hi'})).status_code,403)
        self.assertEqual(self.posted,[])

    async def test_timeout_and_membership_screening_block_send(self):
        headers=await self.headers()
        self.member['communication_disabled_until']=(datetime.now(timezone.utc)+timedelta(hours=1)).isoformat()
        self.assertEqual((await self.client.post('/api/arcade/messages',headers=headers,json={'channel':'200','content':'hi'})).status_code,403)
        self.member.pop('communication_disabled_until');self.member['pending']=True
        self.assertEqual((await self.client.post('/api/arcade/messages',headers=headers,json={'channel':'200','content':'hi'})).status_code,403)

    async def test_slowmode_cannot_be_bypassed(self):
        self.channel['rate_limit_per_user']=30;headers=await self.headers()
        self.assertEqual((await self.client.post('/api/arcade/messages',headers=headers,json={'channel':'200','content':'first'})).status_code,201)
        second=await self.client.post('/api/arcade/messages',headers=headers,json={'channel':'200','content':'second'})
        self.assertEqual(second.status_code,429);self.assertEqual(len(self.posted),1)

    async def test_discord_rate_limits_are_returned(self):
        self.rate_limited=True
        response=await self.client.get('/api/arcade/messages?channel=200')
        self.assertEqual(response.status_code,429);self.assertIn('retry-after',response.headers)


class ExistingAuth(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        # Load only changed auth functions; no database, S3, or production credentials.
        from pathlib import Path
        tree=ast.parse(Path('app.py').read_text())
        keep={'_sign','_verify','_session_user','login','callback'}
        selected=[n for n in tree.body if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef)) and n.name in keep]
        for node in selected:node.decorator_list=[]
        self.ns={'hmac':hmac,'hashlib':hashlib,'time':time,'secrets':secrets,'Optional':Optional,'Request':Request,
                 'JSONResponse':JSONResponse,'HTTPException':HTTPException,'SESSION_SECRET':b'x'*32,
                 'SESSION_COOKIE_NAME':'dc_sid','CLIENT_ID':'1','CLIENT_SECRET':'secret','DISCORD_OAUTH':'https://discord.com/api/oauth2',
                 'REDIRECT_URI':'https://api.discordchats.com/api/auth/discord/callback'}
        exec(compile(ast.Module(body=selected,type_ignores=[]),'auth-test','exec'),self.ns)

    def test_session_expiry_enforced(self):
        def request(value):return Request({'type':'http','headers':[(b'cookie',f'dc_sid={self.ns["_sign"](value)}'.encode())]})
        self.assertEqual(self.ns['_session_user'](request(f'123:{int(time.time())}')),123)
        self.assertIsNone(self.ns['_session_user'](request(f'123:{int(time.time())-31*86400}')))
        self.assertIsNone(self.ns['_session_user'](request(f'123:{int(time.time())+3600}')))

    def test_login_state_and_redirect_allowlist(self):
        response=self.ns['login']('/arcade.html')
        values=parse_qs(urlparse(json.loads(response.body)['url']).query)
        self.assertGreater(len(values['state'][0]),30)
        self.assertIn('HttpOnly',response.headers['set-cookie'])
        response=self.ns['login']('https://evil.example')
        self.assertNotIn('evil.example',response.headers['set-cookie'])

    async def test_invalid_oauth_state_rejected_before_network(self):
        request=Request({'type':'http','headers':[]})
        with self.assertRaises(HTTPException) as error:await self.ns['callback']('code',request,'bad-state')
        self.assertEqual(error.exception.status_code,400)


if __name__=='__main__':unittest.main()
