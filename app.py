import os
import json
import logging
from typing import Optional, Dict, Any, List
import asyncpg
import boto3
import httpx, hmac, hashlib, time, base64
from fastapi import FastAPI, HTTPException, Query, Request, Response, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.routing import APIRouter
from datetime import date as Date
from urllib.parse import urlparse
from starlette.responses import RedirectResponse

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

# Discord / Stripe / Site env
DISCORD_OAUTH = "https://discord.com/api/oauth2"
DISCORD_API   = "https://discord.com/api"

CLIENT_ID     = os.getenv("DISCORD_CLIENT_ID")
CLIENT_SECRET = os.getenv("DISCORD_CLIENT_SECRET")
REDIRECT_URI  = os.getenv("DISCORD_REDIRECT_URI")
GUILD_ID      = int(os.getenv("DISCORD_GUILD_ID", "0") or 0)
BOT_TOKEN = os.getenv("BOT_TOKEN") or os.getenv("DISCORD_TOKEN")
BOOSTER_ROLE_IDS = { int(x) for x in (os.getenv("BOOSTER_ROLE_IDS","").split(",")) if x.strip() }

STRIPE_SECRET         = os.getenv("STRIPE_SECRET")
STRIPE_PRICE_B5       = os.getenv("STRIPE_PRICE_B5")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET")
SITE_BASE_URL         = os.getenv("SITE_BASE_URL")

SESSION_COOKIE_NAME = os.getenv("SESSION_COOKIE_NAME", "dc_sid")
SESSION_SECRET      = (os.getenv("SESSION_SECRET") or "dev_dev_dev").encode("utf-8")

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

    # Bare key (no scheme) → treat as s3 key
    if "://" not in raw_url and not raw_url.startswith("http"):
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
                # PREFER s3_url or s3_key, and only fall back to Discord URL
                raw = a.get("s3_url") or a.get("s3_key") or a.get("url")
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
        # --- archive table ---
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

        # --- Auth/Billing tables ---
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS web_users (
          discord_user_id BIGINT PRIMARY KEY,
          username        TEXT,
          global_name     TEXT,
          avatar_url      TEXT,
          last_login_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """)

        await conn.execute("""
        CREATE TABLE IF NOT EXISTS subscriptions (
          discord_user_id    BIGINT PRIMARY KEY,
          stripe_customer_id TEXT UNIQUE,
          stripe_sub_id      TEXT UNIQUE,
          status             TEXT NOT NULL,                  -- 'active' | 'trialing' | 'past_due' | 'canceled'...
          current_period_end TIMESTAMPTZ,
          updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """)

        await conn.execute("CREATE INDEX IF NOT EXISTS idx_sub_status ON subscriptions(status);")

# ------------------------------
# Health / data APIs
# ------------------------------
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
                              's3_key',   a.s3_key,
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

# ------------------------------
# Full-archive search (earliest → latest)
# ------------------------------
@app.get("/api/search")
async def search_messages(
    q: str = Query("", max_length=100, description="Search term (can be empty if user_id/mentioned_id set)"),
    limit: int = Query(500, ge=1, le=5000),
    offset: int = Query(0, ge=0, le=100000),
    user_id: Optional[int] = Query(None, description="Filter by User"),
    mentioned_id: Optional[int] = Query(None, description="Filter by Mentions"),
):
    term = (q or "").strip()

    if not term and user_id is None and mentioned_id is None:
        raise HTTPException(status_code=400, detail="Need q, user_id, or mentioned_id")

    try:
        async with _pool.acquire() as conn:
            params = []
            where_clauses = []

            # text search
            if term:
                idx = len(params) + 1
                where_clauses.append(f"m.content ILIKE '%' || ${idx} || '%'")
                params.append(term)

            # from: user filter
            if user_id is not None:
                idx = len(params) + 1
                where_clauses.append(f"m.user_id = ${idx}")
                params.append(user_id)

            # mentions: user filter
            if mentioned_id is not None:
                p1 = len(params) + 1
                p2 = len(params) + 2
                where_clauses.append(
                    f"(m.content ILIKE '%' || ${p1} || '%' "
                    f"OR m.content ILIKE '%' || ${p2} || '%')"
                )
                params.append(f"<@{mentioned_id}>")
                params.append(f"<@!{mentioned_id}>")

            where_sql = " AND ".join(where_clauses) or "TRUE"

            sql = f"""
                SELECT
                    m.message_id,
                    m.display_name,
                    m.avatar_url,
                    m.role_color_1,
                    m.role_color_2,
                    m.ts_local_time,
                    m.ts_local_date,
                    m.ts_utc,
                    m.channel_id,
                    m.content,
                    COALESCE(
                      json_agg(
                        json_build_object(
                          's3_url',   a.s3_url,
                          's3_key',   a.s3_key,      -- 🔹 add this
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
                WHERE {where_sql}
                GROUP BY m.message_id
                ORDER BY m.ts_utc DESC, m.channel_id DESC, m.message_id DESC
                LIMIT ${len(params)+1} OFFSET ${len(params)+2}
            """
            params.extend([limit, offset])
            rows = await conn.fetch(sql, *params)
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
                "date": str(r.get("ts_local_date")) if r.get("ts_local_date") else None,
                "content": r.get("content"),
                "attachments": atts,
            })

        return {"ok": True, "q": term, "rows": out}

    except Exception as e:
        log.exception("/api/search failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/users")
async def user_search(
    term: str = Query(..., min_length=1, max_length=50),
    limit: int = Query(20, ge=1, le=50),
):
    """
    Fuzzy search for users by display_name (and optionally ID).
    Used for the `from:` autocomplete.
    """
    try:
        async with _pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    user_id,
                    MAX(display_name) AS display_name,
                    MAX(avatar_url)   AS avatar_url,
                    COUNT(*)          AS messages
                FROM archived_messages
                WHERE display_name ILIKE '%' || $1 || '%'
                   OR CAST(user_id AS TEXT) ILIKE '%' || $1 || '%'
                GROUP BY user_id
                ORDER BY messages DESC
                LIMIT $2
                """,
                term, limit
            )
        return [
            {
                "user_id": str(r["user_id"]),
                "display_name": r["display_name"],
                "avatar_url": r["avatar_url"],
                "messages": r["messages"],
            }
            for r in rows
        ]
    except Exception as e:
        log.exception("/api/users failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))

# ============================
# Auth / Gate / Billing
# ============================
auth_router = APIRouter(prefix="/api/auth", tags=["auth"])
gate_router = APIRouter(prefix="/api/gate", tags=["gate"])
pay_router  = APIRouter(prefix="/api/pay", tags=["pay"])
me_router   = APIRouter(prefix="/api/me", tags=["me"])
mentions_router = APIRouter(prefix="/api/mentions", tags=["mentions"])

# --- Simple signed cookie session (HMAC) ---
def _sign(v: str) -> str:
    mac = hmac.new(SESSION_SECRET, v.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"{v}.{mac}"

def _verify(sig: str) -> Optional[str]:
    if not sig or "." not in sig: return None
    v, mac = sig.rsplit(".", 1)
    good = hmac.new(SESSION_SECRET, v.encode("utf-8"), hashlib.sha256).hexdigest()
    if hmac.compare_digest(mac, good): return v
    return None

def _set_session(resp: Response, discord_user_id: int):
    payload = f"{discord_user_id}:{int(time.time())}"
    resp.set_cookie(
        SESSION_COOKIE_NAME, _sign(payload),
        httponly=True, secure=True, samesite="Lax", max_age=60*60*24*30
    )

def _clear_session(resp: Response):
    resp.delete_cookie(SESSION_COOKIE_NAME)

async def _db_execute(sql: str, *params):
    async with _pool.acquire() as conn:
        return await conn.execute(sql, *params)

async def _db_fetchrow(sql: str, *params):
    async with _pool.acquire() as conn:
        return await conn.fetchrow(sql, *params)

async def _db_fetchval(sql: str, *params):
    async with _pool.acquire() as conn:
        return await conn.fetchval(sql, *params)

def _avatar_url(u: dict) -> Optional[str]:
    # Discord CDN pattern (covers default avatar as well)
    if u.get("avatar"):
        return f"https://cdn.discordapp.com/avatars/{u['id']}/{u['avatar']}.png?size=128"
    idx = int(u["discriminator"]) % 5 if "discriminator" in u else 0
    return f"https://cdn.discordapp.com/embed/avatars/{idx}.png"

async def _upsert_web_user(u: dict):
    await _db_execute("""
        INSERT INTO web_users (discord_user_id, username, global_name, avatar_url, last_login_at)
        VALUES ($1, $2, $3, $4, NOW())
        ON CONFLICT (discord_user_id) DO UPDATE SET
          username=EXCLUDED.username,
          global_name=EXCLUDED.global_name,
          avatar_url=EXCLUDED.avatar_url,
          last_login_at=NOW()
    """, int(u["id"]), u.get("username"), u.get("global_name"), _avatar_url(u))

async def _fetch_member(duid: int) -> Optional[dict]:
    if not BOT_TOKEN or not GUILD_ID:
        return None
    url = f"{DISCORD_API}/guilds/{GUILD_ID}/members/{duid}"
    headers = {"Authorization": f"Bot {BOT_TOKEN}"}
    async with httpx.AsyncClient(timeout=8.0) as client:
        r = await client.get(url, headers=headers)
        if r.status_code == 404:
            log.info("Member %s not found in guild %s", duid, GUILD_ID)
            return None
        r.raise_for_status()
        m = r.json()
        # TEMP: peek at roles/premium_since once to confirm
        log.info("Member %s roles=%s premium_since=%s", duid, m.get("roles"), m.get("premium_since"))
        return m

# ---- Booster role resolver (cache) ----
_booster_role_ids: set[int] | None = None

async def _fetch_booster_role_ids() -> set[int]:
    """Return role IDs that represent the Nitro Booster role(s) for the guild."""
    global _booster_role_ids
    if _booster_role_ids is not None:
        return _booster_role_ids

    if not BOT_TOKEN or not GUILD_ID:
        return set()

    url = f"{DISCORD_API}/guilds/{GUILD_ID}/roles"
    headers = {"Authorization": f"Bot {BOT_TOKEN}"}
    async with httpx.AsyncClient(timeout=8.0) as client:
        r = await client.get(url, headers=headers)
        r.raise_for_status()
        roles = r.json()  # list of role objects

    ids: set[int] = set()
    for role in roles:
        tags = role.get("tags") or {}
        # Discord marks the booster role with {"premium_subscriber": None}
        if "premium_subscriber" in tags:
            try:
                ids.add(int(role["id"]))
            except Exception:
                pass

    # allow env override / additions too
    ids |= BOOSTER_ROLE_IDS

    _booster_role_ids = ids
    log.info("Booster role IDs resolved: %s", sorted(ids))
    return ids


async def _debug_booster_view(duid: int) -> dict:
    member = await _fetch_member(duid)
    try:
        m_roles = {int(r) for r in (member or {}).get("roles", [])}
    except Exception:
        m_roles = set()
    booster_ids = await _fetch_booster_role_ids()
    return {
        "duid": duid,
        "member_found": bool(member),
        "premium_since": (member or {}).get("premium_since"),
        "member_roles": sorted(m_roles),
        "booster_role_ids": sorted(booster_ids),
        "intersection": sorted(m_roles & booster_ids),
    }

async def _is_booster_async(member: dict | None) -> bool:
    if not member:
        return False

    # If present, trust it (may be absent on Bot-token fetches)
    if member.get("premium_since"):
        return True

    try:
        member_roles = {int(r) for r in member.get("roles", [])}
    except Exception:
        member_roles = set()

    booster_ids = await _fetch_booster_role_ids()
    return bool(member_roles & booster_ids)

async def _sub_status(duid: int) -> Optional[dict]:
    row = await _db_fetchrow("""
      SELECT status, current_period_end, stripe_sub_id, stripe_customer_id
      FROM subscriptions WHERE discord_user_id=$1
    """, duid)
    if not row: return None
    return dict(row)

def _allowed_from_status(st: Optional[dict]) -> bool:
    if not st: return False
    return st["status"] in ("active","trialing") and (st["current_period_end"] is None or st["current_period_end"] > Date.today())

def _session_user(request: Request) -> Optional[int]:
    raw = request.cookies.get(SESSION_COOKIE_NAME)
    ver = _verify(raw) if raw else None
    if not ver: return None
    try:
        duid = int(ver.split(":")[0]); return duid
    except Exception:
        return None

@mentions_router.get("")
async def resolve_mentions(ids: str, request: Request):
    """
    Resolve a comma-separated list of user IDs into display names.

    Uses archived_messages + web_users only.
    Returns: { "123": "VixesDog", "456": "MODA" }
    """
    duid = _session_user(request)
    if not duid:
        raise HTTPException(status_code=401, detail="Not logged in")

    # Parse & dedupe IDs
    raw_ids = [s.strip() for s in ids.split(",") if s.strip().isdigit()]
    seen: set[int] = set()
    uids: list[int] = []
    for s in raw_ids:
        uid = int(s)
        if uid not in seen:
            seen.add(uid)
            uids.append(uid)

    if not uids:
        return {}

    # Safety cap
    if len(uids) > 100:
        uids = uids[:100]

    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            """
            WITH wanted AS (
              SELECT unnest($1::bigint[]) AS uid
            ),
            latest AS (
              SELECT
                m.user_id,
                m.display_name,
                m.ts_utc,
                ROW_NUMBER() OVER (
                  PARTITION BY m.user_id
                  ORDER BY m.ts_utc DESC
                ) AS rn
              FROM archived_messages m
              JOIN wanted w ON w.uid = m.user_id
            )
            SELECT
              l.user_id,
              COALESCE(wu.global_name, wu.username, l.display_name) AS name
            FROM latest l
            LEFT JOIN web_users wu
              ON wu.discord_user_id = l.user_id
            WHERE l.rn = 1;
            """,
            uids,
        )

    out: dict[str, str] = {}
    for r in rows:
        name = r["name"]
        if name:
            out[str(r["user_id"])] = name

    return out

# --------- OAuth ----------
@auth_router.get("/login")
def login():
    from urllib.parse import urlencode
    params = {
        "client_id": CLIENT_ID,
        "redirect_uri": REDIRECT_URI,
        "response_type": "code",
        "scope": "identify guilds.members.read",
        "prompt": "none"
    }
    return JSONResponse({"url": f"{DISCORD_OAUTH}/authorize?{urlencode(params)}"})

@auth_router.get("/discord/callback")
async def callback(code: str, request: Request):
    async with httpx.AsyncClient() as client:
        tok = await client.post(f"{DISCORD_OAUTH}/token",
            data={
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": REDIRECT_URI,
            },
            headers={"Content-Type":"application/x-www-form-urlencoded"}
        )
        tok.raise_for_status()
        at = tok.json()["access_token"]

        me = await client.get(f"{DISCORD_API}/users/@me",
                              headers={"Authorization": f"Bearer {at}"})
        me.raise_for_status()
        u = me.json()

    await _upsert_web_user(u)
    resp = RedirectResponse(url=f"{SITE_BASE_URL}/?logged=1", status_code=302)
    _set_session(resp, int(u["id"]))
    return resp

@auth_router.post("/logout")
def logout():
    resp = Response(status_code=204)
    _clear_session(resp)
    return resp

# --------- Gate status ----------
@gate_router.get("/status")
async def gate_status(request: Request, debug: bool = Query(False)):
    duid = _session_user(request)
    if not duid:
        return {"authenticated": False, "allowed": False, "reason": "not_logged_in"}

    member = await _fetch_member(duid)
    allowed_booster = await _is_booster_async(member)

    if allowed_booster:
        payload = {"authenticated": True, "allowed": True, "reason": "booster"}
    else:
        st = await _sub_status(duid)
        if _allowed_from_status(st):
            payload = {"authenticated": True, "allowed": True, "reason": "paid"}
        else:
            payload = {"authenticated": True, "allowed": False, "reason": "paywall"}

    if debug:
        payload["debug"] = await _debug_booster_view(duid)

    return payload

# --------- Me / Profile ----------
@me_router.get("")
async def me(request: Request):
    duid = _session_user(request)
    if not duid: raise HTTPException(401, "Not logged in")
    user = await _db_fetchrow("SELECT * FROM web_users WHERE discord_user_id=$1", duid)
    st   = await _sub_status(duid)
    member = await _fetch_member(duid)
    return {
        "discord_user_id": duid,
        "display_name": (user["global_name"] or user["username"]) if user else None,
        "avatar_url": user["avatar_url"] if user else None,
        "is_booster": await _is_booster_async(member),
        "subscription": st or None
    }

# --------- Stripe: create checkout session ----------
@pay_router.post("/checkout")
async def checkout(request: Request):
    duid = _session_user(request)
    if not duid: raise HTTPException(401, "Not logged in")
    import stripe
    stripe.api_key = STRIPE_SECRET

    sess = stripe.checkout.Session.create(
        mode="subscription",
        line_items=[{"price": STRIPE_PRICE_B5, "quantity": 1}],
        success_url=f"{SITE_BASE_URL}/?paid=1",
        cancel_url=f"{SITE_BASE_URL}/?cancel=1",
        client_reference_id=str(duid),
        metadata={"discord_user_id": str(duid)},
    )
    return {"url": sess.url}

# --------- Stripe webhook (keep URL secret) ----------
@pay_router.post("/webhook")
async def stripe_webhook(request: Request, stripe_signature: str = Header(None, convert_underscores=False)):
    import stripe
    stripe.api_key = STRIPE_SECRET
    payload = await request.body()
    try:
        event = stripe.Webhook.construct_event(
            payload, stripe_signature, STRIPE_WEBHOOK_SECRET
        )
    except Exception as e:
        raise HTTPException(400, f"Webhook signature verification failed: {e}")

    typ = event["type"]
    obj = event["data"]["object"]

    # Link discord_user_id: prefer client_reference_id, else metadata
    duid = None
    if "client_reference_id" in obj and obj["client_reference_id"]:
        try:
            duid = int(obj["client_reference_id"])
        except Exception:
            duid = None
    if duid is None and obj.get("metadata", {}).get("discord_user_id"):
        try:
            duid = int(obj["metadata"]["discord_user_id"])
        except Exception:
            duid = None

    async def upsert(duid_val: int, sub: dict):
        return await _db_execute("""
          INSERT INTO subscriptions (discord_user_id, stripe_customer_id, stripe_sub_id, status, current_period_end, updated_at)
          VALUES ($1,$2,$3,$4, to_timestamp($5), NOW())
          ON CONFLICT (discord_user_id) DO UPDATE SET
            stripe_customer_id=EXCLUDED.stripe_customer_id,
            stripe_sub_id=EXCLUDED.stripe_sub_id,
            status=EXCLUDED.status,
            current_period_end=EXCLUDED.current_period_end,
            updated_at=NOW()
        """, duid_val, sub["customer"], sub["id"], sub["status"], sub["current_period_end"])

    if typ == "checkout.session.completed":
        if duid and obj.get("subscription"):
            sub = stripe.Subscription.retrieve(obj["subscription"])
            await upsert(duid, sub)

    elif typ in ("customer.subscription.created", "customer.subscription.updated"):
        if duid:
            await upsert(duid, obj)

    elif typ == "customer.subscription.deleted":
        if duid:
            await _db_execute("""
              UPDATE subscriptions SET status='canceled', updated_at=NOW() WHERE discord_user_id=$1
            """, duid)

    return {"received": True}

# --------- Cancel subscription (Profile page button) ----------
@pay_router.post("/cancel")
async def cancel_subscription(request: Request):
    duid = _session_user(request)
    if not duid: raise HTTPException(401, "Not logged in")
    row = await _db_fetchrow("SELECT stripe_sub_id FROM subscriptions WHERE discord_user_id=$1", duid)
    if not row or not row["stripe_sub_id"]:
        raise HTTPException(404, "No active subscription")

    import stripe
    stripe.api_key = STRIPE_SECRET
    stripe.Subscription.delete(row["stripe_sub_id"])

    await _db_execute("""
      UPDATE subscriptions SET status='canceled', updated_at=NOW() WHERE discord_user_id=$1
    """, duid)

    # immediately gate and log them out
    resp = Response(status_code=200, media_type="application/json")
    _clear_session(resp)
    resp.body = b'{"ok":true,"canceled":true}'
    return resp

# mount routers (after app is created)
app.include_router(auth_router)
app.include_router(gate_router)
app.include_router(pay_router)
app.include_router(me_router)
app.include_router(mentions_router)   # ← add this line
