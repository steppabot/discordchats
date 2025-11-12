import os
import json
import logging
from typing import Optional, Dict, Any, List

import asyncpg
import boto3
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from datetime import date as Date
from urllib.parse import urlparse

# ----------------------------
# Config / Env
# ----------------------------
S3_ACCESS_KEY = os.getenv("STACKHERO_S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("STACKHERO_S3_SECRET_KEY")
S3_BUCKET     = os.getenv("STACKHERO_S3_BUCKET")
S3_ENDPOINT   = os.getenv("STACKHERO_S3_ENDPOINT")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL missing")

log = logging.getLogger("uvicorn.error")

# S3 client is optional—only if creds/endpoint provided
_s3_client = None
if S3_ACCESS_KEY and S3_SECRET_KEY and S3_BUCKET and S3_ENDPOINT:
    _s3_client = boto3.client(
        "s3",
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        endpoint_url=f"https://{S3_ENDPOINT}",
        region_name="us-east-1",
    )

def presign_stackhero(key: str, expires_in: int = 86400) -> Optional[str]:
    """Return a temporary public URL to a private Stackhero S3 object."""
    if not _s3_client:
        return None
    try:
        return _s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": S3_BUCKET, "Key": key},
            ExpiresIn=expires_in,
        )
    except Exception as e:
        log.warning("presign failed for key %s: %s", key, e)
        return None

STACKHERO_HINTS = ("stackhero", ".s3.", ".object.", "amazonaws.com")

def _extract_s3_key(u: str) -> Optional[str]:
    """Given a full S3/Stackhero URL, return the object key (no leading '/')."""
    try:
        p = urlparse(u)
        key = p.path.lstrip("/")
        return key or None
    except Exception:
        return None

def _maybe_presign_url(raw_url: Optional[str]) -> Optional[str]:
    """
    If a URL (or key) looks like S3/Stackhero (or is a bare key), return a presigned URL.
    Else return the original value.
    """
    if not raw_url:
        return None

    # Bare key (no scheme) → presign as key
    if "://" not in raw_url:
        return presign_stackhero(raw_url) or raw_url

    low = raw_url.lower()
    if any(h in low for h in STACKHERO_HINTS):
        key = _extract_s3_key(raw_url)
        if key:
            return presign_stackhero(key) or raw_url

    return raw_url

def _normalize_atts(atts: Any) -> List[Dict[str, Any]]:
    """
    Normalize attachments into: [{url, filename, type, size}]
    Accepts:
      - list of dicts       (our normal join)
      - list of strings     (raw URLs)
      - JSON string         (either of the above)
      - None / empty
    Also presigns Stackhero/S3 links/keys.
    """
    if not atts:
        return []

    # If DB returned a JSON string
    if isinstance(atts, str):
        try:
            atts = json.loads(atts)
        except Exception:
            # Treat as a single URL string
            return [{"url": _maybe_presign_url(atts)}]

    out: List[Dict[str, Any]] = []
    if isinstance(atts, list):
        for a in atts:
            if isinstance(a, str):
                out.append({"url": _maybe_presign_url(a)})
                continue
            if isinstance(a, dict):
                raw = a.get("s3_url") or a.get("url") or a.get("s3_key")
                url = _maybe_presign_url(raw)
                out.append({
                    "url": url,
                    "filename": a.get("filename"),
                    "type": a.get("type") or a.get("content_type"),
                    "size": a.get("size") or a.get("size_bytes"),
                })
    return out

# ----------------------------
# FastAPI app
# ----------------------------
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://www.discordchats.com",
        "https://discordchats.com",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

_pool: Optional[asyncpg.Pool] = None

@app.on_event("startup")
async def _startup():
    global _pool
    safe_url = DATABASE_URL.split("@")[-1]
    log.info("DB: creating pool to %s", safe_url)
    _pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)

    async with _pool.acquire() as conn:
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
    try:
        async with _pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT COUNT(*) AS n,
                       MIN(ts_local_date) AS min_date,
                       MAX(ts_local_date) AS max_date
                FROM archived_messages
            """)
        return {
            "ok": True,
            "count": row["n"],
            "min_date": str(row["min_date"]) if row["min_date"] else None,
            "max_date": str(row["max_date"]) if row["max_date"] else None,
        }
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
    # Validate date
    try:
        d = Date.fromisoformat(date)
    except ValueError:
        raise HTTPException(status_code=400, detail="Bad date; use YYYY-MM-DD")

    try:
        async with _pool.acquire() as conn:
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
                        m.ts_utc,
                        m.channel_id,
                        m.content,
                        COALESCE(
                          json_agg(
                            json_build_object(
                              's3_url',   a.s3_url,
                              'url',      a.url,
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
                        m.ts_utc,
                        m.channel_id,
                        m.content
                    FROM archived_messages m
                    WHERE m.ts_local_date = $1
                    ORDER BY m.ts_utc ASC, m.channel_id ASC, m.message_id ASC
                    LIMIT $2
                    """,
                    d, limit
                )

        # Convert to plain dicts and normalize attachments
        out = []
        for rec in rows:
            r = dict(rec)
            atts = _normalize_atts(r.get("attachments"))
            out.append({
                "message_id": str(r["message_id"]),
                "display_name": r["display_name"],
                "avatar_url": r.get("avatar_url"),
                "role_color_1": r.get("role_color_1"),
                "role_color_2": r.get("role_color_2"),
                "time": r.get("ts_local_time"),
                "content": r.get("content"),
                "attachments": atts,
            })
        return out

    except Exception as e:
        log.exception("/api/messages failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))
