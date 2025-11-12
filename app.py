import os
import logging
import boto3
from typing import List, Optional, Dict, Any
import asyncpg
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from datetime import date as Date
from urllib.parse import urlparse

S3_ACCESS_KEY = os.getenv("STACKHERO_S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("STACKHERO_S3_SECRET_KEY")
S3_BUCKET     = os.getenv("STACKHERO_S3_BUCKET")
S3_ENDPOINT   = os.getenv("STACKHERO_S3_ENDPOINT")
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL missing")

log = logging.getLogger("uvicorn.error")

s3 = boto3.client(
    "s3",
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
    endpoint_url=f"https://{S3_ENDPOINT}",
    region_name="us-east-1"  # or whatever Stackhero gives you
)

def presign_stackhero(key: str, expires_in: int = 86400) -> str:
    """Return a temporary public URL to a private Stackhero S3 object."""
    return s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": S3_BUCKET, "Key": key},
        ExpiresIn=expires_in
    )

STACKHERO_HINTS = ("stackhero", ".s3.", ".object.", "amazonaws.com")  # loose check

def _extract_s3_key(u: str) -> Optional[str]:
    """
    Given a full S3/Stackhero URL, return the object key (path without leading '/').
    """
    try:
        p = urlparse(u)
        key = p.path.lstrip("/")
        return key or None
    except Exception:
        return None

def _presign_if_stackhero(u: Optional[str]) -> Optional[str]:
    """
    If the URL points at your Stackhero/S3 bucket (or you just want to treat it like S3),
    return a presigned URL. Otherwise return the original.
    """
    if not u:
        return None
    # Heuristic: presign if it looks like S3/Stackhero OR if it’s a bare key (no scheme).
    if "://" not in u:
        # looks like a key already
        try:
            return presign_stackhero(u)
        except Exception:
            return u

    low = u.lower()
    if any(h in low for h in STACKHERO_HINTS):
        key = _extract_s3_key(u)
        if key:
            try:
                return presign_stackhero(key)
            except Exception:
                return u
    return u

async def _table_exists(conn: asyncpg.Connection, name: str) -> bool:
    row = await conn.fetchrow(
        "SELECT to_regclass($1) IS NOT NULL AS exists", name
    )
    return bool(row["exists"])

def _row_to_public(m: dict) -> dict:
    # Normalize attachments shape (list of {url, filename, type, size})
    atts = m.get("attachments") or []
    # If you later store s3_key instead of url, presign here:
    # for a in atts:
    #     if a.get("s3_key") and not a.get("url"):
    #         a["url"] = presign_stackhero(a["s3_key"])
    return {
        "message_id": str(m["message_id"]),
        "display_name": m["display_name"],
        "avatar_url": m.get("avatar_url"),
        "role_color_1": m.get("role_color_1"),
        "time": m.get("ts_local_time"),
        "content": m.get("content"),
        "attachments": atts,
    }


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
    # 1) Validate & coerce incoming date string to a Python date
    try:
        d = Date.fromisoformat(date)  # raises ValueError if bad
    except ValueError:
        raise HTTPException(status_code=400, detail="Bad date; use YYYY-MM-DD")

    try:
        async with _pool.acquire() as conn:
            # 2) Try with attachments (LEFT JOIN + JSON agg)
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
                              -- include BOTH fields so we can decide at the API layer
                              's3_url', a.s3_url,
                              'url',     a.url,
                              'filename', a.filename,
                              'type',     a.content_type,
                              'size',     a.size_bytes
                            )
                          ) FILTER (WHERE a.attachment_id IS NOT NULL),
                          '[]'::json
                        ) AS attachments
                    FROM archived_messages m
                    LEFT JOIN archived_attachments a
                      ON a.message_id = m.message_id
                    WHERE m.ts_local_date = $1
                    GROUP BY m.message_id
                    ORDER BY m.ts_utc ASC, m.channel_id ASC, m.message_id ASC
                    LIMIT $2
                    """,
                    d, limit
                )
            except Exception as join_err:
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
                    WHERE m.ts_local_date = $1
                    ORDER BY m.ts_utc ASC, m.channel_id ASC, m.message_id ASC
                    LIMIT $2
                    """,
                    d, limit
                )
                # normalize to same shape
                rows = [dict(r) | {"attachments": []} for r in rows]

        out = []
        for r in rows:
            atts_in = r.get("attachments") or []
            atts_out = []
            for a in atts_in:
                # Prefer s3_url if present; else url.
                raw = a.get("s3_url") or a.get("url")
                signed = _presign_if_stackhero(raw)

                atts_out.append({
                    "url": signed,
                    "filename": a.get("filename"),
                    "type": a.get("type"),
                    "size": a.get("size"),
                })

            out.append({
                "message_id": str(r["message_id"]),
                "display_name": r["display_name"],
                "avatar_url": r.get("avatar_url"),
                "role_color_1": r.get("role_color_1"),
                "role_color_2": r.get("role_color_2"),
                "time": r.get("ts_local_time"),
                "content": r.get("content"),
                "attachments": atts_out,
            })

        return out

    except Exception as e:
        log.exception("/api/messages failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))

