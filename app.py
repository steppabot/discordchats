import os
import datetime as dt
from zoneinfo import ZoneInfo
from typing import Optional

import asyncpg
from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from jinja2 import Environment, FileSystemLoader, select_autoescape

import boto3
from botocore.config import Config as BotoConfig

# ------------------ Env ------------------
DB_URL = os.getenv("DATABASE_URL")
ARCHIVE_TZ = ZoneInfo(os.getenv("ARCHIVE_TZ", "America/Chicago"))

# Prefer public URL base if you made the bucket public
S3_PUBLIC_URL_BASE = (os.getenv("S3_PUBLIC_URL_BASE", "").rstrip("/") or None)

STACKHERO_ENDPOINT = os.getenv("STACKHERO_S3_ENDPOINT") or os.getenv("STACKHERO_MINIO_HOST")
STACKHERO_ACCESS_KEY = os.getenv("STACKHERO_S3_ACCESS_KEY") or os.getenv("STACKHERO_MINIO_ROOT_ACCESS_KEY")
STACKHERO_SECRET_KEY = os.getenv("STACKHERO_S3_SECRET_KEY") or os.getenv("STACKHERO_MINIO_ROOT_SECRET_KEY")
STACKHERO_BUCKET     = os.getenv("STACKHERO_S3_BUCKET") or os.getenv("STACKHERO_MINIO_BUCKET")
if STACKHERO_ENDPOINT and not STACKHERO_ENDPOINT.startswith("http"):
    STACKHERO_ENDPOINT = f"https://{STACKHERO_ENDPOINT}"

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET = os.getenv("S3_BUCKET")

def _s3_client():
    if STACKHERO_ENDPOINT and STACKHERO_ACCESS_KEY and STACKHERO_SECRET_KEY and (STACKHERO_BUCKET or S3_BUCKET):
        return boto3.client(
            "s3",
            endpoint_url=STACKHERO_ENDPOINT,
            aws_access_key_id=STACKHERO_ACCESS_KEY,
            aws_secret_access_key=STACKHERO_SECRET_KEY,
            config=BotoConfig(s3={"addressing_style": "path"}),
        )
    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY and S3_BUCKET:
        return boto3.client(
            "s3",
            region_name=AWS_REGION,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            config=BotoConfig(s3={"addressing_style": "virtual"}),
        )
    return None

_BUCKET = (STACKHERO_BUCKET or S3_BUCKET)
_s3 = _s3_client()

# ------------------ App ------------------
app = FastAPI(title="Wolfpac Archive")

# Static files (put wolfpac.gif and fonts in ./static)
app.mount("/static", StaticFiles(directory="static"), name="static")

# Jinja templates
env = Environment(
    loader=FileSystemLoader("templates"),
    autoescape=select_autoescape(["html"])
)

_pool: Optional[asyncpg.Pool] = None

@app.on_event("startup")
async def startup():
    global _pool
    if not DB_URL:
        raise RuntimeError("DATABASE_URL not set")
    _pool = await asyncpg.create_pool(DB_URL)

def _presign_if_needed(s3_key: Optional[str], s3_url: Optional[str]) -> Optional[str]:
    """
    Return a public URL for an attachment:
    - If you stored s3_url and bucket is public: use it.
    - Else, when s3_key exists and we have an S3 client: return a presigned GET.
    """
    if s3_url:
        return s3_url
    if s3_key and _s3 and _BUCKET:
        try:
            return _s3.generate_presigned_url(
                "get_object",
                Params={"Bucket": _BUCKET, "Key": s3_key},
                ExpiresIn=3600  # 1 hour
            )
        except Exception:
            return None
    return None

# ------------------ Pages ------------------
@app.get("/", response_class=HTMLResponse)
async def index():
    # get earliest and latest local date present
    async with _pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT MIN(ts_local_date) AS min_d, MAX(ts_local_date) AS max_d
            FROM archived_messages
        """)
    min_d = row["min_d"] or dt.date.today()
    max_d = row["max_d"] or dt.date.today()
    tmpl = env.get_template("index.html")
    return tmpl.render(min_date=min_d.isoformat(), max_date=max_d.isoformat())

# ------------------ API ------------------
@app.get("/api/messages")
async def api_messages(date: str = Query(..., description="YYYY-MM-DD in ARCHIVE_TZ")):
    try:
        d = dt.date.fromisoformat(date)
    except Exception:
        raise HTTPException(400, "Invalid date format. Use YYYY-MM-DD")

    async with _pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT m.message_id, m.channel_id, m.user_id, m.display_name,
                   m.ts_local_time, m.role_color_1, m.role_color_2,
                   m.avatar_url, m.content,
                   COALESCE(a.list, '[]') AS attachments
            FROM archived_messages m
            LEFT JOIN LATERAL (
                SELECT json_agg(json_build_object(
                    'filename', aa.filename,
                    'content_type', aa.content_type,
                    'size_bytes', aa.size_bytes,
                    'url', aa.url,
                    's3_key', aa.s3_key,
                    's3_url', aa.s3_url
                ) ORDER BY aa.attachment_id) AS list
                FROM archived_attachments aa
                WHERE aa.message_id = m.message_id
            ) a ON TRUE
            WHERE m.ts_local_date = $1
            ORDER BY m.ts_local_date ASC, m.ts_local_time ASC, m.message_id ASC
        """, d)

    # decorate attachment URLs (presign if needed)
    out = []
    for r in rows:
        att_list = []
        try:
            import json as _json
            att_list = _json.loads(r["attachments"])
        except Exception:
            att_list = []

        for a in att_list:
            a["display_url"] = _presign_if_needed(a.get("s3_key"), a.get("s3_url")) or a.get("url")

        out.append({
            "message_id": str(r["message_id"]),
            "display_name": r["display_name"],
            "ts_local_time": r["ts_local_time"],
            "role_color_1": r["role_color_1"],
            "role_color_2": r["role_color_2"],
            "avatar_url": r["avatar_url"],
            "content": r["content"],
            "attachments": att_list,
        })

    return JSONResponse(out)
