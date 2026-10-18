"""Helicopter rankings using Token's existing sessions and database."""
import asyncio
import hashlib
import hmac
import secrets
import time
from typing import Literal

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, conint, constr


class ScoreSubmission(BaseModel):
    run_id: constr(strict=True, min_length=20, max_length=100)
    score: conint(strict=True, ge=0, le=104400)


def create_leaderboard_router(
    *, store, session_user, profile, secret, cookie_name, origins
):
    router = APIRouter(prefix='/api/arcade')
    schema_lock = asyncio.Lock()
    schema_ready = False

    async def database():
        nonlocal schema_ready
        db = await store.db()

        async with schema_lock:
            if not schema_ready:
                async with db.acquire() as connection:
                    async with connection.transaction():
                        await connection.execute("""
                            CREATE TABLE IF NOT EXISTS token_helicopter_runs (
                                uid TEXT PRIMARY KEY,
                                run_id TEXT NOT NULL,
                                started DOUBLE PRECISION NOT NULL,
                                submitted_score INTEGER
                            )
                        """)
                        await connection.execute("""
                            CREATE TABLE IF NOT EXISTS token_helicopter_bests (
                                uid TEXT NOT NULL,
                                day DATE NOT NULL,
                                name TEXT NOT NULL,
                                avatar TEXT,
                                score INTEGER NOT NULL,
                                achieved_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                                PRIMARY KEY (uid, day)
                            )
                        """)
                        await connection.execute("""
                            CREATE INDEX IF NOT EXISTS
                                token_helicopter_ranking_idx
                            ON token_helicopter_bests
                                (day, score DESC, achieved_at, uid)
                        """)
                schema_ready = True

        return db

    def response(value):
        return JSONResponse(
            value,
            headers={'Cache-Control': 'no-store'}
        )

    async def require_player(request):
        raw = request.cookies.get(cookie_name, '')
        expected = hmac.new(
            secret,
            ('arcade-csrf:' + raw).encode(),
            hashlib.sha256
        ).hexdigest()

        if (
            request.headers.get('origin') not in origins
            or not hmac.compare_digest(
                request.headers.get('x-csrf-token', ''),
                expected
            )
        ):
            raise HTTPException(403, 'Invalid request.')

        uid = await session_user(request)
        if not uid:
            raise HTTPException(401, 'Sign in to save your score.')

        return uid

    @router.post('/leaderboards/helicopter/start')
    async def start_run(request: Request):
        uid = await require_player(request)
        db = await database()
        now = time.time()
        run_id = secrets.token_urlsafe(32)

        # One current run per account. Starting again invalidates the old run.
        # This also limits rapid restart requests.
        row = await db.fetchrow("""
            INSERT INTO token_helicopter_runs
                (uid, run_id, started, submitted_score)
            VALUES ($1, $2, $3, NULL)
            ON CONFLICT (uid) DO UPDATE
            SET run_id = EXCLUDED.run_id,
                started = EXCLUDED.started,
                submitted_score = NULL
            WHERE token_helicopter_runs.started <= EXCLUDED.started - 2
            RETURNING run_id
        """, uid, run_id, now)

        if not row:
            raise HTTPException(429, 'Wait a moment before restarting.')

        return response({'run_id': run_id})

    @router.post('/leaderboards/helicopter/finish')
    async def finish_run(request: Request, data: ScoreSubmission):
        uid = await require_player(request)
        user = await profile(request)
        db = await database()

        async with db.acquire() as connection:
            async with connection.transaction():
                run = await connection.fetchrow("""
                    SELECT *
                    FROM token_helicopter_runs
                    WHERE uid = $1
                    FOR UPDATE
                """, uid)

                if not run or not hmac.compare_digest(
                    run['run_id'], data.run_id
                ):
                    raise HTTPException(
                        409, 'This run is no longer active.'
                    )

                # Retrying the same submission cannot count it twice.
                if run['submitted_score'] is not None:
                    if run['submitted_score'] == data.score:
                        return response({
                            'ok': True,
                            'score': data.score
                        })
                    raise HTTPException(409, 'Run already submitted.')

                elapsed = time.time() - run['started']

                if elapsed < 0 or elapsed > 3600:
                    raise HTTPException(
                        400, 'Run expired. Start a new game.'
                    )

                # Current Helicopter speed never exceeds 28.5 meters/sec.
                # This rejects impossible distances, not all cheating.
                if data.score > int(elapsed * 29) + 2:
                    raise HTTPException(
                        400, 'Score exceeds the distance possible for this run.'
                    )

                await connection.execute("""
                    UPDATE token_helicopter_runs
                    SET submitted_score = $2
                    WHERE uid = $1
                """, uid, data.score)

                # 1970-01-01 is the all-time bucket.
                # The other bucket is the UTC date the run finishes.
                await connection.execute("""
                    INSERT INTO token_helicopter_bests
                        (uid, day, name, avatar, score)
                    SELECT $1, bucket, $2, $3, $4
                    FROM (
                        VALUES
                            (DATE '1970-01-01'),
                            ((now() AT TIME ZONE 'UTC')::date)
                    ) AS buckets(bucket)
                    ON CONFLICT (uid, day) DO UPDATE
                    SET name = EXCLUDED.name,
                        avatar = EXCLUDED.avatar,
                        score = EXCLUDED.score,
                        achieved_at = now()
                    WHERE EXCLUDED.score > token_helicopter_bests.score
                """,
                    uid,
                    str(user.get('name') or 'Player')[:100],
                    user.get('avatar'),
                    data.score
                )

        return response({'ok': True, 'score': data.score})

    @router.get('/leaderboards/helicopter')
    async def leaderboard(
        period: Literal['daily', 'all'] = 'all'
    ):
        db = await database()

        rows = await db.fetch("""
            SELECT uid, name, avatar, score
            FROM token_helicopter_bests
            WHERE day = CASE
                WHEN $1 = 'daily'
                    THEN (now() AT TIME ZONE 'UTC')::date
                ELSE DATE '1970-01-01'
            END
            ORDER BY score DESC, achieved_at ASC, uid ASC
            LIMIT 50
        """, period)

        return response({
            'period': period,
            'entries': [
                {
                    'rank': index,
                    'name': row['name'],
                    'avatar': row['avatar'],
                    'score': row['score']
                }
                for index, row in enumerate(rows, start=1)
            ]
        })

    return router
