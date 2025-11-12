import os
import logging
from typing import List, Optional, Dict, Any
import asyncpg
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL missing")

log = logging.getLogger("uvicorn.error")

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://www.discordchats.com",   # your GitHub Pages domain
        "https://discordchats.com"        # if you also use root domain
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

_pool: Optional[asyncpg.Pool] = None


@app.on_event("startup")
async def _startup():
    global _pool
    # Heroku PG often needs sslmode=require; if your URL doesn't include it,
    # asyncpg still negotiates TLS, but we log the URL (without secrets) to be sure.
    safe_url = DATABASE_URL.split("@")[-1]
    log.info("DB: creating pool to %s", safe_url)
    _pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    async with _pool.acquire() as conn:
        # sanity query
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS archived_messages (
            message_id      BIGINT PRIMARY KEY,
            guild_id        BIGINT NOT NULL,
            channel_id      BIGINT NOT NULL,
            user_id         BIGINT NOT NULL,
            display_name    TEXT   NOT NULL,
            ts_utc          TIMESTAMPTZ NOT NULL,
            ts_local_date   DATE   NOT NULL,
            ts_local_time   TEXT   NOT NULL,
            role_color_1    TEXT   NULL,
            role_color_2    TEXT   NULL,
            avatar_url      TEXT   NULL,
            content         TEXT   NULL
        );
        """)


@app.get("/api/health")
async def health():
    return {"ok": True, "msg": "discordchats backend up"}
    try:
        async with _pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT COUNT(*) AS n,
                       MIN(ts_local_date) AS min_date,
                       MAX(ts_local_date) AS max_date
                FROM archived_messages
            """)
        return dict(ok=True, count=row["n"], min_date=str(row["min_date"]) if row["min_date"] else None,
                    max_date=str(row["max_date"]) if row["max_date"] else None)
    except Exception as e:
        log.exception("/api/health failed: %s", e)
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.get("/api/dates")
async def dates():
    """All dates with counts (ascending). Used to populate the calendar."""
    try:
        async with _pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT ts_local_date AS d, COUNT(*) AS n
                FROM archived_messages
                GROUP BY ts_local_date
                ORDER BY ts_local_date ASC
            """)
        return [{"date": str(r["d"]), "count": r["n"]} for r in rows]
    except Exception as e:
        log.exception("/api/dates failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/messages")
async def messages(
    date: str = Query(..., description="YYYY-MM-DD (local day)"),
    limit: int = Query(2000, ge=1, le=10000),
):
    try:
        async with _pool.acquire() as conn:
            # First try: include attachments via LEFT JOIN + JSON aggregation
            try:
                rows = await conn.fetch(
                    """
                    SELECT
                        m.message_id,
                        m.display_name,
                        m.avatar_url,
                        m.role_color_1,
                        m.role_color_2,
                        m.ts_local_time,
                        m.content,
                        COALESCE(
                          json_agg(
                            json_build_object(
                              'url', COALESCE(a.s3_url, a.url),
                              'filename', a.filename,
                              'type', a.content_type,
                              'size', a.size_bytes
                            )
                          ) FILTER (WHERE a.attachment_id IS NOT NULL),
                          '[]'::json
                        ) AS attachments
                    FROM archived_messages m
                    LEFT JOIN archived_attachments a
                      ON a.message_id = m.message_id
                    WHERE m.ts_local_date = $1::date
                    GROUP BY m.message_id
                    ORDER BY m.ts_local_time ASC, m.message_id ASC
                    LIMIT $2
                    """,
                    date, limit
                )
            except Exception as join_err:
                # Table missing or schema mismatch: fall back to messages only.
                log.warning("Attachments join skipped: %s", join_err)
                rows = await conn.fetch(
                    """
                    SELECT
                        m.message_id,
                        m.display_name,
                        m.avatar_url,
                        m.role_color_1,
                        m.role_color_2,
                        m.ts_local_time,
                        m.content
                    FROM archived_messages m
                    WHERE m.ts_local_date = $1::date
                    ORDER BY m.ts_local_time ASC, m.message_id ASC
                    LIMIT $2
                    """,
                    date, limit
                )
                # Normalize to include empty attachments list so the UI code stays simple.
                rows = [
                    dict(r) | {"attachments": []}
                    for r in rows
                ]

        # asyncpg Record -> plain dicts
        out = []
        for r in rows:
            out.append({
                "message_id": str(r["message_id"]),
                "display_name": r["display_name"],
                "avatar_url": r.get("avatar_url"),
                "role_color_1": r.get("role_color_1"),
                "role_color_2": r.get("role_color_2"),
                "time": r.get("ts_local_time"),
                "content": r.get("content"),
                "attachments": r.get("attachments") or [],
            })
        return out

    except Exception as e:
        log.exception("/api/messages failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))

