import os
import datetime as dt
from zoneinfo import ZoneInfo
from typing import Optional

import asyncpg
from fastapi import FastAPI, HTTPException, Depends
from fastapi.responses import FileResponse, RedirectResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.staticfiles import StaticFiles

import boto3
from botocore.config import Config as BotoConfig

# --- Env ---
DB_URL = os.getenv("DATABASE_URL")
ARCHIVE_TZ = os.getenv("ARCHIVE_TZ", "America/Chicago")
TZ = ZoneInfo(ARCHIVE_TZ)

S3_PUBLIC_URL_BASE = (os.getenv("S3_PUBLIC_URL_BASE") or "").rstrip("/")
STACKHERO_ENDPOINT = os.getenv("STACKHERO_S3_ENDPOINT") or os.getenv("STACKHERO_MINIO_HOST")
STACKHERO_ACCESS = os.getenv("STACKHERO_S3_ACCESS_KEY") or os.getenv("STACKHERO_MINIO_ROOT_ACCESS_KEY")
STACKHERO_SECRET = os.getenv("STACKHERO_S3_SECRET_KEY") or os.getenv("STACKHERO_MINIO_ROOT_SECRET_KEY")
STACKHERO_BUCKET = os.getenv("STACKHERO_S3_BUCKET") or os.getenv("STACKHERO_MINIO_BUCKET")
if STACKHERO_ENDPOINT and not STACKHERO_ENDPOINT.startswith("http"):
    STACKHERO_ENDPOINT = f"https://{STACKHERO_ENDPOINT}"

if not DB_URL:
    raise RuntimeError("DATABASE_URL is required")

# --- App ---
app = FastAPI(title="DiscordChats")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
)

# Serve index.html from root
@app.get("/")
async def root_page():
    return FileResponse("index.html", media_type="text/html")

# (Optional) serve /wolfpac.gif etc. if you put assets in ./public
if os.path.isdir("public"):
    app.mount("/public", StaticFiles(directory="public"), name="public")

# --- DB Pool ---
_pool: Optional[asyncpg.Pool] = None

@app.on_event("startup")
async def _startup():
    global _pool
    _pool = await asyncpg.create_pool(DB_URL, max_inactive_connection_lifetime=30.0)

@app.on_event("shutdown")
async def _shutdown():
    if _pool:
        await _pool.close()

async def db() -> asyncpg.Connection:
    assert _pool is not None
    async with _pool.acquire() as conn:
        yield conn

# --- S3 client (for presign if needed) ---
def _s3_client():
    if STACKHERO_ENDPOINT and STACKHERO_ACCESS and STACKHERO_SECRET and STACKHERO_BUCKET:
        return boto3.client(
            "s3",
            endpoint_url=STACKHERO_ENDPOINT,
            aws_access_key_id=STACKHERO_ACCESS,
            aws_secret_access_key=STACKHERO_SECRET,
            config=BotoConfig(s3={"addressing_style": "path"}),
        )
    return None

def public_or_presigned_url(s3_key: Optional[str]) -> Optional[str]:
    if not s3_key:
        return None
    # Prefer public base
    if S3_PUBLIC_URL_BASE:
        return f"{S3_PUBLIC_URL_BASE}/{s3_key}"
    # Else presign
    s3 = _s3_client()
    if not s3:
        return None
    try:
        return s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": STACKHERO_BUCKET, "Key": s3_key},
            ExpiresIn=3600,
        )
    except Exception:
        return None

# ---------------- API ----------------

@app.get("/api/dates")
async def list_dates(conn: asyncpg.Connection = Depends(db)):
    """
    Return all dates we have messages for (oldest → newest) with counts.
    """
    rows = await conn.fetch(
        "SELECT ts_local_date::date AS d, COUNT(*) AS c "
        "FROM archived_messages GROUP BY 1 ORDER BY 1 ASC"
    )
    return [{"date": r["d"].isoformat(), "count": r["c"]} for r in rows]

@app.get("/api/messages")
async def messages_by_date(
    date: str,
    channel_id: Optional[int] = None,
    offset: int = 0,
    limit: int = 500,   # tune as needed
    conn: asyncpg.Connection = Depends(db),
):
    """
    Return messages for a local date (ARCHIVE_TZ). Optional channel filter.
    Includes attachments as direct/public/presigned URLs.
    """
    try:
        d = dt.date.fromisoformat(date)
    except Exception:
        raise HTTPException(400, "Invalid date. Use YYYY-MM-DD")

    base_sql = (
        "SELECT m.message_id, m.guild_id, m.channel_id, m.user_id, m.display_name, "
        "m.ts_local_time, m.role_color_1, m.role_color_2, m.avatar_url, m.content "
        "FROM archived_messages m WHERE m.ts_local_date = $1"
    )
    args = [d]
    if channel_id:
        base_sql += " AND m.channel_id = $2"
        args.append(channel_id)
    base_sql += " ORDER BY m.ts_utc ASC OFFSET $%d LIMIT $%d" % (len(args)+1, len(args)+2)
    args.extend([offset, limit])

    msgs = await conn.fetch(base_sql, *args)
    ids = [r["message_id"] for r in msgs]
    att_map = {}
    if ids:
        atts = await conn.fetch(
            "SELECT message_id, attachment_id, filename, content_type, size_bytes, url, proxy_url, s3_key, s3_url "
            "FROM archived_attachments WHERE message_id = ANY($1::bigint[]) ORDER BY attachment_id",
            ids,
        )
        for a in atts:
            att_map.setdefault(a["message_id"], []).append({
                "attachment_id": a["attachment_id"],
                "filename": a["filename"],
                "content_type": a["content_type"],
                "size_bytes": a["size_bytes"],
                # Prefer public/presigned over raw discord url
                "url": public_or_presigned_url(a["s3_key"]) or a["s3_url"] or a["url"] or a["proxy_url"],
            })

    out = []
    for r in msgs:
        out.append({
            "message_id": r["message_id"],
            "channel_id": r["channel_id"],
            "display_name": r["display_name"],
            "time": r["ts_local_time"],
            "role_color_1": r["role_color_1"],
            "role_color_2": r["role_color_2"],
            "avatar_url": r["avatar_url"],
            "content": r["content"],
            "attachments": att_map.get(r["message_id"], []),
        })
    return {"items": out, "next_offset": offset + len(out)}
