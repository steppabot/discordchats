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
    allow_origins=["*"],  # your site is same-origin, but this avoids CORS surprises
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
    """Quick visibility: how many rows and earliest/latest date."""
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
    """Messages for a given local date, ordered like Discord."""
    try:
        async with _pool.acquire() as conn:
            msgs = await conn.fetch(
                """
                SELECT message_id, guild_id, channel_id, user_id, display_name,
                       ts_utc, ts_local_date, ts_local_time,
                       role_color_1, role_color_2, avatar_url, content
                FROM archived_messages
                WHERE ts_local_date = $1::date
                ORDER BY ts_local_date ASC, ts_local_time ASC, message_id ASC
                LIMIT $2
                """,
                date, limit
            )

            # attachments (only ones we mirrored or original CDN fallback)
            atts = await conn.fetch(
                """
                SELECT message_id,
                       COALESCE(s3_url, url) AS href,
                       filename, content_type, size_bytes
                FROM archived_attachments
                WHERE message_id = ANY($1::bigint[])
                ORDER BY message_id, attachment_id
                """,
                [m["message_id"] for m in msgs] or [0],
            )
        by_mid: Dict[int, List[Dict[str, Any]]] = {}
        for a in atts:
            if not a["href"]:
                continue
            by_mid.setdefault(a["message_id"], []).append(
                {"url": a["href"], "filename": a["filename"], "type": a["content_type"], "size": a["size_bytes"]}
            )

        out = []
        for m in msgs:
            out.append({
                "message_id": str(m["message_id"]),
                "display_name": m["display_name"],
                "avatar_url": m["avatar_url"],
                "role_color_1": m["role_color_1"],
                "role_color_2": m["role_color_2"],
                "time": m["ts_local_time"],
                "content": m["content"],
                "attachments": by_mid.get(m["message_id"], []),
            })
        return out
    except Exception as e:
        log.exception("/api/messages failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))
