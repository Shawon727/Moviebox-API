import os
import re
import json
import time
import hashlib
import hmac
import base64
import random
import string
from urllib.parse import urlparse, parse_qsl, urlencode
from typing import Optional

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse

app = FastAPI(
    title="MovieBox API Pro (Mobile)",
    description="Updated with MovieBox-TUI mobile API + HMAC signing",
    version="3.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Host pool (from MovieBox-TUI) ──────────────────────────────────────────
HOST_POOL = [
    "https://api6.aoneroom.com",
    "https://api5.aoneroom.com",
    "https://api4.aoneroom.com",
    "https://api4sg.aoneroom.com",
    "https://api3.aoneroom.com",
    "https://api6sg.aoneroom.com",
    "https://api.inmoviebox.com",
]

SECRET_KEY = "76iRl07s0xSN9jqmEWAt79EBJZulIQIsV64FZr2O"  # base64 secret from TUI
RETRY_STATUS = {403, 406, 407, 429, 500, 502, 503, 504}

# Global session
_bearer_token: Optional[str] = None
_active_host_idx = 0
_user_agent = ""
_client_info = ""
_spoofed_ip = ""


def _b64_decode(val: str) -> bytes:
    pad = (4 - len(val) % 4) % 4
    return base64.b64decode(val + "=" * pad)


def _md5_hex(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


def _generate_x_client_token(ts: int) -> str:
    rev = str(ts)[::-1]
    return f"{ts},{_md5_hex(rev.encode())}"


def _sorted_query(url: str) -> str:
    parsed = urlparse(url)
    params = parse_qsl(parsed.query, keep_blank_values=True)
    if not params:
        return ""
    # sort by key
    params = sorted(params, key=lambda x: x[0])
    return urlencode(params, doseq=True)


def _build_canonical(method: str, accept: str, content_type: str, url: str, body: Optional[str], ts: int) -> str:
    parsed = urlparse(url)
    path = parsed.path or "/"
    query = _sorted_query(url)
    canonical_url = f"{path}?{query}" if query else path

    body_hash = ""
    body_len = ""
    if body is not None:
        b = body.encode()
        truncated = b[:102400]
        body_hash = _md5_hex(truncated)
        body_len = str(len(b))

    return "\n".join([
        method.upper(),
        accept or "",
        content_type or "",
        body_len,
        str(ts),
        body_hash,
        canonical_url,
    ])


def _generate_signature(method: str, url: str, body: Optional[str], ts: int) -> str:
    accept = "application/json"
    content_type = "application/json"
    canonical = _build_canonical(method, accept, content_type, url, body, ts)
    key = _b64_decode(SECRET_KEY)
    sig = hmac.new(key, canonical.encode(), hashlib.md5).digest()
    sig_b64 = base64.b64encode(sig).decode()
    return f"{ts}|2|{sig_b64}"


def _random_hex(n: int) -> str:
    return "".join(random.choices("0123456789abcdef", k=n))


def _random_uuid() -> str:
    return f"{_random_hex(8)}-{_random_hex(4)}-{_random_hex(4)}-{_random_hex(4)}-{_random_hex(12)}"


def _generate_client_info_and_ua() -> tuple[str, str]:
    android_versions = [
        ("9", "PQ3A.190605.03081104"),
        ("10", "QP1A.191005.007.A3"),
        ("11", "RP1A.200720.011"),
        ("12", "S1B.220414.015"),
        ("13", "TQ2A.230405.003"),
    ]
    redmi = [
        ("23078RKD5C", "Redmi"),
        ("2201117TY", "Redmi"),
        ("22101316G", "Redmi"),
        ("M2012K11AG", "Redmi"),
    ]
    version_codes = [50020117, 50020118, 50020119, 50020120, 50020121]
    networks = ["NETWORK_WIFI", "NETWORK_MOBILE"]
    timezones = ["Asia/Dhaka", "Asia/Kolkata", "Asia/Shanghai", "America/New_York"]

    android = random.choice(android_versions)
    device = random.choice(redmi)
    vcode = random.choice(version_codes)
    network = random.choice(networks)
    tz = random.choice(timezones)
    gaid = _random_uuid()
    device_id = _random_hex(32)

    ua = (
        f"com.community.oneroom/{vcode} "
        f"(Linux; U; Android {android[0]}; en_US; {device[0]}; Build/{android[1]}; Cronet/135.0.7012.3)"
    )
    client_info = json.dumps({
        "package_name": "com.community.oneroom",
        "version_name": "4.0.01.0813.03",
        "version_code": vcode,
        "os": "android",
        "os_version": android[0],
        "install_ch": "ps",
        "device_id": device_id,
        "install_store": "ps",
        "gaid": gaid,
        "brand": device[1],
        "model": device[0],
        "system_language": "en",
        "net": network,
        "region": "US",
        "timezone": tz,
        "sp_code": "40401",
        "X-Play-Mode": "2",
    }, separators=(",", ":"))
    return ua, client_info


def _random_spoofed_ip() -> str:
    prefixes = ["103.241", "49.36", "117.195", "106.198", "122.162", "157.32", "182.70"]
    return f"{random.choice(prefixes)}.{random.randint(1,253)}.{random.randint(1,253)}"


def _init_identity():
    global _user_agent, _client_info, _spoofed_ip
    if not _user_agent:
        _user_agent, _client_info = _generate_client_info_and_ua()
        _spoofed_ip = _random_spoofed_ip()


def _build_headers(method: str, url: str, body: Optional[str] = None, token: Optional[str] = None) -> dict:
    _init_identity()
    ts = int(time.time() * 1000)
    headers = {
        "User-Agent": _user_agent,
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Connection": "keep-alive",
        "x-client-token": _generate_x_client_token(ts),
        "x-tr-signature": _generate_signature(method, url, body, ts),
        "x-client-info": _client_info,
        "x-client-status": "0",
        "x-forwarded-for": _spoofed_ip,
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


async def _visitor_login(client: httpx.AsyncClient) -> str:
    global _bearer_token, _active_host_idx
    path = "/wefeed-mobile-bff/user-api/visitor-login"
    body = "{}"

    for i in range(len(HOST_POOL)):
        idx = (_active_host_idx + i) % len(HOST_POOL)
        base = HOST_POOL[idx]
        url = base + path
        headers = _build_headers("POST", url, body, None)
        try:
            resp = await client.post(url, headers=headers, content=body, timeout=12)
            if resp.status_code in RETRY_STATUS:
                continue
            data = resp.json()
            # data may be wrapped or direct
            token = None
            if isinstance(data, dict):
                token = data.get("token") or (data.get("data") or {}).get("token")
            if token:
                _bearer_token = token
                _active_host_idx = idx
                # absorb x-user if present
                x_user = resp.headers.get("x-user")
                if x_user:
                    try:
                        xu = json.loads(x_user)
                        if xu.get("token"):
                            _bearer_token = xu["token"]
                    except Exception:
                        pass
                return _bearer_token
        except Exception:
            continue
    raise HTTPException(502, "Failed to obtain visitor token (all hosts exhausted)")


async def _get_token(client: httpx.AsyncClient) -> str:
    global _bearer_token
    if _bearer_token:
        return _bearer_token
    return await _visitor_login(client)


async def _make_request(method: str, path: str, body: Optional[dict] = None) -> dict:
    global _bearer_token, _active_host_idx
    body_str = json.dumps(body, separators=(",", ":")) if body is not None else None

    async with httpx.AsyncClient(follow_redirects=True, timeout=15) as client:
        token = await _get_token(client)

        for attempt in range(2):  # retry once on 401/403
            start = _active_host_idx
            for i in range(len(HOST_POOL)):
                idx = (start + i) % len(HOST_POOL)
                base = HOST_POOL[idx]
                url = base + path
                headers = _build_headers(method, url, body_str, token)

                try:
                    if method.upper() == "POST":
                        resp = await client.post(url, headers=headers, content=body_str or "{}")
                    else:
                        resp = await client.get(url, headers=headers)

                    # refresh token from x-user
                    x_user = resp.headers.get("x-user")
                    if x_user:
                        try:
                            xu = json.loads(x_user)
                            if xu.get("token"):
                                _bearer_token = xu["token"]
                                token = _bearer_token
                        except Exception:
                            pass

                    if resp.status_code in (401, 403) and attempt == 0:
                        _bearer_token = None
                        token = await _visitor_login(client)
                        break  # retry whole loop with new token

                    if resp.status_code in RETRY_STATUS:
                        continue

                    if resp.status_code != 200:
                        continue

                    data = resp.json()
                    _active_host_idx = idx
                    # return inner data if present
                    if isinstance(data, dict) and "data" in data:
                        return data["data"]
                    return data
                except Exception:
                    continue
            else:
                continue
            break

    raise HTTPException(502, f"Request failed for {path} (all hosts exhausted)")


# ── Endpoints ──────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok", "version": "3.0.0", "engine": "mobile-api + hmac"}


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return HTMLResponse("""
    <h1>MovieBox API v3 (Mobile)</h1>
    <p>Updated from MovieBox-TUI logic</p>
    <ul>
      <li><a href="/home">/home</a></li>
      <li><a href="/search?q=avatar">/search?q=avatar</a></li>
      <li>/detail/{subject_id}</li>
      <li>/api/stream/{subject_id}?se=1&ep=1</li>
    </ul>
    """)


@app.get("/home")
async def get_home(page: int = 1):
    # tabId=1 usually home
    data = await _make_request("GET", f"/wefeed-mobile-bff/tab-operating?page={page}&tabId=1&version=")
    return {"status": "success", "data": data}


@app.get("/search")
async def search(q: str = Query(..., min_length=1), page: int = 1):
    payload = {
        "keyword": q,
        "page": page,
        "perPage": 20,
        "subjectType": 0
    }
    data = await _make_request("POST", "/wefeed-mobile-bff/subject-api/search/v2", payload)
    items = []
    raw = data.get("items") or data.get("list") or data.get("subjects") or []
    for sub in raw:
        if isinstance(sub, dict) and "subject" in sub:
            sub = sub["subject"]
        items.append({
            "name": sub.get("title") or sub.get("name"),
            "poster_url": (sub.get("cover") or {}).get("url") if isinstance(sub.get("cover"), dict) else sub.get("cover"),
            "slug": sub.get("detailPath"),
            "subject_id": str(sub.get("subjectId") or sub.get("id") or ""),
            "year": (sub.get("releaseDate") or "")[:4] or None,
            "rating": sub.get("imdbRatingValue"),
        })
    return {"query": q, "page": page, "total": data.get("total") or len(items), "items": items}


@app.get("/detail/{subject_id}")
async def get_detail(subject_id: str):
    data = await _make_request("GET", f"/wefeed-mobile-bff/subject-api/get?subjectId={subject_id}")
    # attach seasons if series
    stype = data.get("subjectType") or data.get("stype") or 1
    if stype == 2:
        try:
            seasons = await _make_request("GET", f"/wefeed-mobile-bff/subject-api/season-info?subjectId={subject_id}")
            data["seasons"] = seasons
        except Exception:
            pass
    return data


@app.get("/api/stream/{subject_id}")
async def get_stream(
    subject_id: str,
    se: int = 0,
    ep: int = 0,
    detail_path: Optional[str] = None,  # kept for compatibility, not required
):
    """
    Returns playable streams using mobile play-info/v2
    se=0 & ep=0 → movie
    """
    if se == 0 and ep == 0:
        path = f"/wefeed-mobile-bff/subject-api/play-info/v2?subjectId={subject_id}"
    else:
        path = f"/wefeed-mobile-bff/subject-api/play-info/v2?subjectId={subject_id}&se={se}&ep={ep}"

    data = await _make_request("GET", path)

    streams = []
    # streams list
    for s in data.get("streams") or data.get("streamList") or []:
        url = s.get("url")
        if not url:
            continue
        streams.append({
            "resolution": f"{s.get('resolutions') or s.get('resolution') or s.get('quality') or '?'}p",
            "format": s.get("format") or ("DASH" if ".mpd" in url else "MP4" if ".mp4" in url else "HLS"),
            "url": url,
            "size": s.get("size"),
            "duration": s.get("duration"),
            "codec": s.get("codecName"),
            "id": s.get("id"),
        })

    # also collect dash / hls if present
    dash = data.get("dash") or []
    hls = data.get("hls") or []

    has_resource = bool(streams or dash or hls) or data.get("hasResource", False)

    return {
        "subject_id": subject_id,
        "se": se,
        "ep": ep,
        "has_resource": has_resource,
        "sources": streams,
        "hls": hls,
        "dash": dash,
        "free_episodes": data.get("freeNum"),
        "limited": data.get("limited", False),
        "note": None if has_resource else "No stream found (may require paid / region locked)",
        "raw_keys": list(data.keys()) if isinstance(data, dict) else [],
    }


@app.get("/api/stream/{subject_id}/captions")
async def get_captions(subject_id: str, resource_id: str = "", se: int = 0, ep: int = 0):
    if not resource_id:
        # try to get first stream id
        play = await get_stream(subject_id, se, ep)
        sources = play.get("sources") or []
        if sources:
            resource_id = str(sources[0].get("id") or "")
    if not resource_id:
        return {"subject_id": subject_id, "count": 0, "captions": []}

    data = await _make_request(
        "GET",
        f"/wefeed-mobile-bff/subject-api/get-ext-captions?subjectId={subject_id}&resourceId={resource_id}"
    )
    captions = data.get("captions") or data.get("list") or []
    return {
        "subject_id": subject_id,
        "resource_id": resource_id,
        "count": len(captions),
        "captions": captions,
    }


@app.get("/movies")
async def movies(page: int = 1):
    # tabId=2 movies (approximate)
    data = await _make_request("GET", f"/wefeed-mobile-bff/tab-operating?page={page}&tabId=2&version=")
    return data


@app.get("/tv-series")
async def tv_series(page: int = 1):
    data = await _make_request("GET", f"/wefeed-mobile-bff/tab-operating?page={page}&tabId=5&version=")
    return data


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("api:app", host="0.0.0.0", port=port, reload=True)
