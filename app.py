import os
import logging
from typing import Optional, List
from datetime import date

import boto3
import asyncpg
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel

log = logging.getLogger("discordchats")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

app = FastAPI(title="discordchats.com")

DB_URL = os.getenv("DATABASE_URL")
ARCHIVE_TZ = os.getenv("ARCHIVE_TZ", "America/Chicago")

# S3/MinIO (read-only is fine for website)
STACKHERO_ENDPOINT = os.getenv("STACKHERO_S3_ENDPOINT") or os.getenv("STACKHERO_MINIO_HOST")
if STACKHERO_ENDPOINT and not STACKHERO_ENDPOINT.startswith("http"):
    STACKHERO_ENDPOINT = f"https://{STACKHERO_ENDPOINT}"
STACKHERO_ACCESS_KEY = os.getenv("STACKHERO_S3_ACCESS_KEY") or os.getenv("STACKHERO_MINIO_ROOT_ACCESS_KEY")
STACKHERO_SECRET_KEY = os.getenv("STACKHERO_S3_SECRET_KEY") or os.getenv("STACKHERO_MINIO_ROOT_SECRET_KEY")
STACKHERO_BUCKET     = os.getenv("STACKHERO_S3_BUCKET") or os.getenv("STACKHERO_MINIO_BUCKET")
S3_PREFIX = os.getenv("S3_PREFIX", "discord-archive/").strip()
S3_PUBLIC_URL_BASE = os.getenv("S3_PUBLIC_URL_BASE", "").rstrip("/")

_pool: Optional[asyncpg.Pool] = None

def _s3_client():
    if STACKHERO_ENDPOINT and STACKHERO_ACCESS_KEY and STACKHERO_SECRET_KEY and STACKHERO_BUCKET:
        return boto3.client(
            "s3",
            endpoint_url=STACKHERO_ENDPOINT,
            aws_access_key_id=STACKHERO_ACCESS_KEY,
            aws_secret_access_key=STACKHERO_SECRET_KEY,
        )
    return None

@app.on_event("startup")
async def startup():
    global _pool
    # Boot even if DB is missing (so we can see health endpoint)
    if not DB_URL:
        log.warning("DATABASE_URL not set; API will run but data queries will 503")
        return
    try:
        _pool = await asyncpg.create_pool(DB_URL, min_size=1, max_size=5)
        async with _pool.acquire() as conn:
            # Sanity check a lightweight query
            await conn.execute("SELECT 1;")
        log.info("DB pool ready.")
    except Exception as e:
        log.exception("DB pool init failed: %s", e)
        # Don’t crash; allow /health to show error
        _pool = None

@app.on_event("shutdown")
async def shutdown():
    global _pool
    if _pool:
        await _pool.close()
        _pool = None

class MessageRow(BaseModel):
    message_id: int
    guild_id: int
    channel_id: int
    user_id: int
    display_name: str
    ts_local_date: date
    ts_local_time: str
    role_color_1: Optional[str]
    role_color_2: Optional[str]
    avatar_url: Optional[str]
    content: Optional[str]

@app.get("/health")
async def health():
    ok = True
    details = {}
    if _pool is None:
        ok = False
        details["db"] = "unavailable"
    else:
        details["db"] = "ok"
    details["s3_configured"] = bool(_s3_client() and STACKHERO_BUCKET)
    return {"ok": ok, **details}

@app.get("/dates")
async def list_dates():
    """Return all local dates that have messages (for calendar)."""
    if _pool is None:
        raise HTTPException(status_code=503, detail="DB unavailable")
    sql = """
        SELECT DISTINCT ts_local_date
        FROM archived_messages
        ORDER BY ts_local_date ASC
    """
    async with _pool.acquire() as conn:
        rows = await conn.fetch(sql)
    return [r["ts_local_date"].isoformat() for r in rows]

@app.get("/messages")
async def messages_for_date(
    date_str: str = Query(..., description="YYYY-MM-DD in ARCHIVE_TZ"),
    channel_id: Optional[int] = Query(None),
    limit: int = Query(2000, ge=1, le=10000),
):
    """Fetch messages for the chosen local date (optionally a single channel)."""
    if _pool is None:
        raise HTTPException(status_code=503, detail="DB unavailable")

    try:
        d = date.fromisoformat(date_str)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid date format")

    base_sql = """
        SELECT message_id, guild_id, channel_id, user_id, display_name,
               ts_local_date, ts_local_time, role_color_1, role_color_2,
               avatar_url, content
        FROM archived_messages
        WHERE ts_local_date = $1
    """
    params: List = [d]
    if channel_id:
        base_sql += " AND channel_id = $2"
        params.append(channel_id)
    base_sql += " ORDER BY ts_local_time ASC, message_id ASC LIMIT $%d" % (len(params)+1)
    params.append(limit)

    async with _pool.acquire() as conn:
        rows = await conn.fetch(base_sql, *params)
    return [MessageRow(**dict(r)).model_dump() for r in rows]

@app.get("/attachments")
async def attachments_for_message(
    message_id: int,
):
    """Fetch mirrored attachment URLs (prefer public s3_url if set)."""
    if _pool is None:
        raise HTTPException(status_code=503, detail="DB unavailable")
    sql = """
        SELECT attachment_id, filename, content_type, size_bytes,
               url, proxy_url, s3_key, s3_url
        FROM archived_attachments
        WHERE message_id = $1
        ORDER BY attachment_id ASC
    """
    async with _pool.acquire() as conn:
        rows = await conn.fetch(sql, message_id)

    out = []
    for r in rows:
        public = r["s3_url"]
        if not public and r["s3_key"] and S3_PUBLIC_URL_BASE:
            public = f"{S3_PUBLIC_URL_BASE}/{r['s3_key']}"
        out.append({
            "attachment_id": r["attachment_id"],
            "filename": r["filename"],
            "content_type": r["content_type"],
            "size_bytes": r["size_bytes"],
            "public_url": public,
            "fallback_url": r["url"] or r["proxy_url"],
        })
    return out

@app.get("/")
async def root():
    # Serve a tiny landing so Gunicorn boot proves out
    return JSONResponse({"ok": True, "msg": "discordchats backend up", "health": "/health", "dates": "/dates", "messages": "/messages?date_str=YYYY-MM-DD"})
