"""Dedicated Token sessions in the API's existing PostgreSQL database."""
import asyncio
import hashlib
import json
import time
import asyncpg
from fastapi import HTTPException

class SessionStore:
    def __init__(self, url):
        self.url = url
        self.pool = None
        self.lock = asyncio.Lock()

    async def db(self):
        async with self.lock:
            if self.pool is None:
                if not self.url:
                    raise HTTPException(503, 'Token login requires DATABASE_URL on the API.')
                self.pool = await asyncpg.create_pool(self.url, min_size=1, max_size=4, command_timeout=15)
                try:
                    await self.pool.execute('''CREATE TABLE IF NOT EXISTS token_arcade_sessions (
                        sid_hash TEXT PRIMARY KEY, uid TEXT NOT NULL, profile TEXT NOT NULL,
                        access_token TEXT NOT NULL, refresh_token TEXT NOT NULL,
                        token_expires DOUBLE PRECISION NOT NULL, expires DOUBLE PRECISION NOT NULL
                    )''')
                except Exception:
                    await self.pool.close()
                    self.pool = None
                    raise
        return self.pool

    @staticmethod
    def key(sid):
        return hashlib.sha256(sid.encode()).hexdigest()

    async def create(self, sid, user, tokens, ttl):
        db = await self.db()
        now = time.time()
        await db.execute('DELETE FROM token_arcade_sessions WHERE expires < $1', now)
        await db.execute('''INSERT INTO token_arcade_sessions
            VALUES ($1,$2,$3,$4,$5,$6,$7)''', self.key(sid), str(user['id']), json.dumps(user),
            tokens['access_token'], tokens['refresh_token'], now + float(tokens['expires_in']), now + ttl)

    async def get(self, sid):
        if not sid or len(sid) > 100:
            return None
        db = await self.db()
        row = await db.fetchrow('SELECT * FROM token_arcade_sessions WHERE sid_hash=$1 AND expires>$2',
                                self.key(sid), time.time())
        return dict(row) if row else None

    async def delete(self, sid):
        if sid:
            db = await self.db()
            await db.execute('DELETE FROM token_arcade_sessions WHERE sid_hash=$1', self.key(sid))

    async def access(self, sid, refresh):
        db = await self.db()
        # Serialize token rotation across requests and API processes.
        async with db.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow('SELECT * FROM token_arcade_sessions WHERE sid_hash=$1 FOR UPDATE', self.key(sid))
                if not row or row['expires'] <= time.time():
                    raise HTTPException(401, 'Sign in with Discord again.')
                if row['token_expires'] > time.time() + 60:
                    return row['access_token']
                tokens = await refresh(row['refresh_token'])
                await conn.execute('''UPDATE token_arcade_sessions SET access_token=$2,
                    refresh_token=$3, token_expires=$4 WHERE sid_hash=$1''', self.key(sid),
                    tokens['access_token'], tokens.get('refresh_token', row['refresh_token']),
                    time.time() + float(tokens['expires_in']))
                return tokens['access_token']
