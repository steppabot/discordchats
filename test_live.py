import os, unittest, time, hmac, hashlib, json
from unittest.mock import patch
from fastapi import FastAPI
from fastapi.testclient import TestClient
import httpx
from arcade_auth import install_arcade, COOKIE
from arcade_chat import permission_bits, VIEW, SEND, HISTORY

class LiveTests(unittest.TestCase):
    def setUp(self):
        self.member={'user':{'id':'123','username':'greg'},'nick':'Greg','roles':['300']}
        self.channel={'id':'200','guild_id':'100','type':0,'name':'general','permission_overwrites':[]}
        self.posted=[]
        self.env=patch.dict(os.environ,{'ARCADE_SESSION_SECRET':'s'*32,'ARCADE_RELAY_SECRET':'r'*32,'DISCORD_GUILD_ID':'100','BOT_TOKEN':'bot','DISCORD_CLIENT_ID':'client','DISCORD_CLIENT_SECRET':'clientsecret','ARCADE_CHANNEL_IDS':'200'})
        self.env.start()
        def discord(req):
            path=req.url.path
            if path.endswith('/members/123'):return httpx.Response(200,json=self.member)
            if path.endswith('/guilds/100'):return httpx.Response(200,json={'id':'100','owner_id':'999'})
            if path.endswith('/roles'):return httpx.Response(200,json=[{'id':'100','permissions':str(VIEW|SEND|HISTORY)},{'id':'300','permissions':'0','position':5,'color':16711680}])
            if path.endswith('/channels/200'):return httpx.Response(200,json=self.channel)
            if path.endswith('/messages'):
                if req.method=='POST':self.posted.append(json.loads(req.content))
                msg={'id':'500','author':{'id':'123','username':'greg'},'content':'hello','timestamp':'2026-10-17T08:00:00Z'}
                return httpx.Response(200,json=msg if req.method=='POST' else [msg])
            if path.endswith('/oauth2/token'):return httpx.Response(200,json={'access_token':'oauth-token'})
            if path.endswith('/users/@me'):return httpx.Response(200,json={'id':'123'})
            return httpx.Response(404,json={})
        self.app=FastAPI();install_arcade(self.app,httpx.MockTransport(discord))
        self.client=TestClient(self.app,base_url='https://api.discordarcade.com')
        self.client.__enter__()
        self.login_cookie()
    def tearDown(self):self.client.__exit__(None,None,None);self.env.stop()
    def login_cookie(self, age=0):
        value='123:'+str(int(time.time())-age)
        self.client.cookies.set(COOKIE,value+'.'+hmac.new(b's'*32,value.encode(),hashlib.sha256).hexdigest())
    def headers(self):
        return {'Origin':'https://discordarcade.com','X-CSRF-Token':self.client.get('/api/arcade/me').json()['csrf']}
    def test_member_without_subscription_gets_names_colors(self):
        r=self.client.get('/api/arcade/messages?channel=200');self.assertEqual(r.status_code,200)
        self.assertEqual(r.json()['messages'][0]['author']['name'],'Greg')
        self.assertEqual(r.json()['messages'][0]['author']['color'],'#ff0000')
    def test_channel_allowlist(self):self.assertEqual(self.client.get('/api/arcade/messages?channel=999').status_code,403)
    def test_private_channel_denied(self):
        self.channel['permission_overwrites']=[{'id':'123','type':1,'deny':str(VIEW),'allow':'0'}]
        self.assertEqual(self.client.get('/api/arcade/messages?channel=200').status_code,403)
    def test_send_csrf_attribution_and_mentions(self):
        self.assertEqual(self.client.post('/api/arcade/messages',json={'channel':'200','content':'Hi'}).status_code,403)
        r=self.client.post('/api/arcade/messages',headers=self.headers(),json={'channel':'200','content':'@everyone Hi'})
        self.assertEqual(r.status_code,201);self.assertEqual(self.posted[0]['allowed_mentions']['parse'],[])
        self.assertIn('Greg · via website',self.posted[0]['content'])
    def test_expired_session(self):
        self.login_cookie(8*86400);self.assertEqual(self.client.get('/api/arcade/me').status_code,401)
    def test_oauth_rejects_forged_state(self):
        self.assertEqual(self.client.get('/api/arcade/auth/callback?code=x&state=bad').status_code,400)
    def test_oauth_roundtrip(self):
        from urllib.parse import urlparse,parse_qs
        r=self.client.get('/api/arcade/auth/login');state=parse_qs(urlparse(r.json()['url']).query)['state'][0]
        r=self.client.get('/api/arcade/auth/callback',params={'code':'valid','state':state},follow_redirects=False)
        self.assertEqual(r.status_code,303);self.assertEqual(r.headers['location'],'https://discordarcade.com/')
        self.assertIn(COOKIE,r.headers['set-cookie'])
    def test_relay_requires_secret_and_allowlist(self):
        body={'guild_id':'100','channel_id':'200'}
        self.assertEqual(self.client.post('/api/arcade/relay',json=body).status_code,403)
        self.assertEqual(self.client.post('/api/arcade/relay',json={**body,'channel_id':'999'},headers={'Authorization':'Bearer '+'r'*32}).status_code,400)
    def test_websocket_receives_relay_refresh(self):
        csrf=self.headers()['X-CSRF-Token']
        with self.client.websocket_connect('/api/arcade/live?channel=200&csrf='+csrf,headers={'Origin':'https://discordarcade.com'}) as ws:
            self.assertEqual(ws.receive_json()['type'],'refresh')
            r=self.client.post('/api/arcade/relay',json={'guild_id':'100','channel_id':'200'},headers={'Authorization':'Bearer '+'r'*32})
            self.assertEqual(r.status_code,200);self.assertEqual(ws.receive_json()['type'],'refresh')
    def test_wrong_origin_socket_rejected(self):
        from starlette.websockets import WebSocketDisconnect
        with self.assertRaises(WebSocketDisconnect):
            with self.client.websocket_connect('/api/arcade/live?channel=200&csrf='+self.headers()['X-CSRF-Token'],headers={'Origin':'https://evil.example'}):pass

if __name__=='__main__':unittest.main()
