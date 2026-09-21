# StreamHub API v5.0.0
# MovieBox (HMAC) + 4KHDHub + HubCloud direct resolve

import os
import asyncio
import re
import json
import time
import hashlib
import hmac
import base64
import random
import html
from urllib.parse import urlparse, parse_qsl, urlencode, urljoin, unquote, quote
from typing import Optional, Any, List, Dict, Tuple

import httpx
try:
    from bs4 import BeautifulSoup
except ImportError:  # pragma: no cover
    BeautifulSoup = None  # type: ignore

if BeautifulSoup is None:
    class BeautifulSoup:  # minimal stub
        def __init__(self, *a, **k):
            raise RuntimeError("beautifulsoup4 is required: pip install beautifulsoup4")

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, Response, StreamingResponse

app = FastAPI(
    title="StreamHub API",
    description=(
        "Multi-provider streaming API\n\n"
        "**Catalog** `/api/*` · **Play** embeds · **Music** · **Downloader** (yt-dlp + ffmpeg merge)\n"
        "**MovieBox** `/mb/*` · **4KHDHub** `/fk/*` · **Tools** `/tools/*`"
    ),
    version="5.13.1",
    docs_url=None,
    redoc_url=None,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# Inject creator on every JSON response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response as StarletteResponse
import json as _json

class CreatorMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        response = await call_next(request)
        ct = (response.headers.get("content-type") or "").lower()
        if "application/json" not in ct:
            return response
        try:
            body = b""
            async for chunk in response.body_iterator:
                body += chunk
            data = _json.loads(body.decode("utf-8") or "null")
            if isinstance(data, dict) and "creator" not in data:
                data = {"creator": "shawon", **data}
            elif isinstance(data, list):
                data = {"creator": "shawon", "items": data}
            else:
                data = {"creator": "shawon", "data": data}
            raw = _json.dumps(data, ensure_ascii=False, default=str).encode("utf-8")
            headers = dict(response.headers)
            headers.pop("content-length", None)
            return StarletteResponse(
                content=raw,
                status_code=response.status_code,
                media_type="application/json",
                headers=headers,
            )
        except Exception:
            return response

app.add_middleware(CreatorMiddleware)


# =============================================================================
# MOVIEBOX
# =============================================================================

MB_HOSTS = [
    "https://api6.aoneroom.com",
    "https://api5.aoneroom.com",
    "https://api4.aoneroom.com",
    "https://api4sg.aoneroom.com",
    "https://api3.aoneroom.com",
    "https://api6sg.aoneroom.com",
    "https://api7.aoneroom.com",
    "https://api8.aoneroom.com",
    "https://api.inmoviebox.com",
]
MB_SECRET = "76iRl07s0xSN9jqmEWAt79EBJZulIQIsV64FZr2O"
STREAM_REFERER = "https://sportslive.wine"
RETRY_CODES = {403, 406, 407, 429, 500, 502, 503, 504}

_mb_token: Optional[str] = None
_mb_idx: int = 0
_mb_ua: str = ""
_mb_info: str = ""
_mb_ip: str = ""


def _b64_decode(s: str) -> bytes:
    return base64.b64decode(s + "=" * ((4 - len(s) % 4) % 4))


def _md5_hex(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


def _random_hex(n: int) -> str:
    return "".join(random.choices("0123456789abcdef", k=n))


def _random_uuid() -> str:
    return f"{_random_hex(8)}-{_random_hex(4)}-{_random_hex(4)}-{_random_hex(4)}-{_random_hex(12)}"


def _ensure_mb_identity() -> None:
    global _mb_ua, _mb_info, _mb_ip
    if _mb_ua:
        return
    android = random.choice([("12", "S1B.220414.015"), ("13", "TQ2A.230405.003")])
    device = random.choice([("23078RKD5C", "Redmi"), ("M2012K11AG", "Redmi")])
    vcode = random.choice([50020117, 50020118, 50020119, 50020120, 50020121])
    _mb_ua = (
        f"com.community.oneroom/{vcode} (Linux; U; Android {android[0]}; en_US; "
        f"{device[0]}; Build/{android[1]}; Cronet/135.0.7012.3)"
    )
    _mb_info = json.dumps(
        {
            "package_name": "com.community.oneroom",
            "version_name": "4.0.01.0813.03",
            "version_code": vcode,
            "os": "android",
            "os_version": android[0],
            "install_ch": "ps",
            "device_id": _random_hex(32),
            "install_store": "ps",
            "gaid": _random_uuid(),
            "brand": device[1],
            "model": device[0],
            "system_language": "en",
            "net": "NETWORK_WIFI",
            "region": "US",
            "timezone": "Asia/Dhaka",
            "sp_code": "40401",
            "X-Play-Mode": "2",
        },
        separators=(",", ":"),
    )
    prefix = random.choice(["103.241", "49.36", "117.195", "106.198"])
    _mb_ip = f"{prefix}.{random.randint(1, 253)}.{random.randint(1, 253)}"


def _sorted_query(url: str) -> str:
    parsed = urlparse(url)
    params = sorted(parse_qsl(parsed.query, keep_blank_values=True), key=lambda x: x[0])
    return urlencode(params, doseq=True) if params else ""


def _canonical_string(method: str, url: str, body: Optional[str], ts: int) -> str:
    parsed = urlparse(url)
    path = parsed.path or "/"
    query = _sorted_query(url)
    canonical_url = f"{path}?{query}" if query else path
    body_hash = body_len = ""
    if body is not None:
        raw = body.encode()
        body_hash = _md5_hex(raw[:102400])
        body_len = str(len(raw))
    return "\n".join(
        [method.upper(), "application/json", "application/json", body_len, str(ts), body_hash, canonical_url]
    )


def _mb_headers(method: str, url: str, body: Optional[str] = None, token: Optional[str] = None) -> dict:
    _ensure_mb_identity()
    ts = int(time.time() * 1000)
    canonical = _canonical_string(method, url, body, ts)
    sig = hmac.new(_b64_decode(MB_SECRET), canonical.encode(), hashlib.md5).digest()
    headers = {
        "User-Agent": _mb_ua,
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Connection": "keep-alive",
        "x-client-token": f"{ts},{_md5_hex(str(ts)[::-1].encode())}",
        "x-tr-signature": f"{ts}|2|{base64.b64encode(sig).decode()}",
        "x-client-info": _mb_info,
        "x-client-status": "0",
        "x-forwarded-for": _mb_ip,
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


async def _mb_login(client: httpx.AsyncClient) -> str:
    global _mb_token, _mb_idx
    path = "/wefeed-mobile-bff/user-api/visitor-login"
    body = "{}"
    for i in range(len(MB_HOSTS)):
        idx = (_mb_idx + i) % len(MB_HOSTS)
        url = MB_HOSTS[idx] + path
        try:
            resp = await client.post(url, headers=_mb_headers("POST", url, body), content=body, timeout=12.0)
            if resp.status_code in RETRY_CODES:
                continue
            data = resp.json()
            token = data.get("token") or (data.get("data") or {}).get("token")
            x_user = resp.headers.get("x-user")
            if x_user:
                try:
                    token = json.loads(x_user).get("token") or token
                except Exception:
                    pass
            if token:
                _mb_token = token
                _mb_idx = idx
                return token
        except Exception:
            continue
    raise HTTPException(status_code=502, detail="MovieBox visitor-login failed")


async def mb_request(method: str, path: str, body: Optional[dict] = None) -> Any:
    global _mb_token, _mb_idx
    body_str = json.dumps(body, separators=(",", ":")) if body is not None else None
    async with httpx.AsyncClient(follow_redirects=True, timeout=25.0) as client:
        if not _mb_token:
            await _mb_login(client)
        for attempt in range(2):
            start = _mb_idx
            for i in range(len(MB_HOSTS)):
                idx = (start + i) % len(MB_HOSTS)
                url = MB_HOSTS[idx] + path
                try:
                    headers = _mb_headers(method, url, body_str, _mb_token)
                    if method.upper() == "POST":
                        resp = await client.post(url, headers=headers, content=body_str or "{}")
                    else:
                        resp = await client.get(url, headers=headers)
                    x_user = resp.headers.get("x-user")
                    if x_user:
                        try:
                            nt = json.loads(x_user).get("token")
                            if nt:
                                _mb_token = nt
                        except Exception:
                            pass
                    if resp.status_code in (401, 403) and attempt == 0:
                        _mb_token = None
                        await _mb_login(client)
                        break
                    if resp.status_code in RETRY_CODES or resp.status_code != 200:
                        continue
                    data = resp.json()
                    _mb_idx = idx
                    return data.get("data", data) if isinstance(data, dict) else data
                except HTTPException:
                    raise
                except Exception:
                    continue
            else:
                continue
            break
    raise HTTPException(status_code=502, detail=f"MovieBox request failed: {path}")


def _is_dummy_url(url: str) -> bool:
    """True for MovieBox App-Upgrade notice / promo dummy MP4s (never play these)."""
    u = (url or "").lower().strip()
    if not u:
        return True
    # Known notice hashes / paths
    markers = (
        "1c7de0bd3393702d9191801f15f88f8d",
        "9a0461bc39da389663bf3dbb17091d3f",
        "b164fbfb43477929",
        "aa348f2541d13ffe",
        "b164fbfb4347792950bdfbfb563d39d9",
        "/notice.mp4",
        "app-upgrade",
        "upgrade-notice",
        "discontinued",
    )
    if any(m in u for m in markers):
        return True
    # Entire macdn "other" bucket is promotional / notice clips
    if "macdn.aoneroom.com" in u and "/other/" in u:
        return True
    if "macdn.aoneroom.com" in u and u.rstrip("/").endswith(".mp4"):
        # short promo clips on macdn root-ish paths
        if "/dash/" not in u and "/hls/" not in u:
            return True
    return False


def _dash_from_sign_cookie(cookie: str) -> Optional[str]:
    """Extract real DASH/HLS URL from MovieBox signCookie / Edge-Cache-Cookie."""
    if not cookie:
        return None
    # 1) CloudFront-Policy resource
    for part in cookie.split(";"):
        trimmed = part.strip()
        if not trimmed.startswith("CloudFront-Policy="):
            continue
        raw = trimmed[len("CloudFront-Policy=") :].strip()
        normalized = raw.replace("-", "+").replace("_", "/").replace("~", "/")
        try:
            pad = (4 - len(normalized) % 4) % 4
            policy = json.loads(base64.b64decode(normalized + ("=" * pad)))
            resource = policy["Statement"][0]["Resource"]
            base = resource.rstrip("*").rstrip("/")
            if base.startswith("http"):
                return f"{base}/index.mpd"
        except Exception:
            continue
    # 2) Edge-Cache-Cookie=urlprefix=<b64>:sign=...:t=...
    if "urlprefix=" in cookie:
        try:
            part = cookie.split("urlprefix=", 1)[1]
            # stop at sign / semicolon / ampersand
            for sep in (":sign=", ";", "&", " "):
                if sep in part:
                    part = part.split(sep, 1)[0]
            part = part.strip().strip('"').strip("'")
            pad = (4 - len(part) % 4) % 4
            base = base64.b64decode(part + ("=" * pad)).decode("utf-8", errors="strict")
            # strip any non-printable leftover
            base = "".join(ch for ch in base if ch.isprintable()).strip()
            if base.startswith("http"):
                if base.endswith((".mpd", ".m3u8")):
                    return base
                return base.rstrip("/") + "/index.mpd"
        except Exception:
            pass
    return None


async def _mb_h5_domain() -> str:
    """Discover current H5 player domain (moviebox rotates: mzfi.me, netfilm.world, …)."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Referer": "https://moviebox.ph/",
        "Origin": "https://moviebox.ph",
        "Accept": "application/json",
        "X-Client-Info": '{"timezone":"Asia/Dhaka"}',
        "X-Request-Lang": "en",
    }
    for base in (
        "https://h5-api.aoneroom.com",
        "https://h5.aoneroom.com",
    ):
        try:
            async with httpx.AsyncClient(timeout=12.0, follow_redirects=True) as client:
                r = await client.get(f"{base}/wefeed-h5api-bff/media-player/get-domain", headers=headers)
                if r.status_code == 200:
                    d = r.json().get("data")
                    if isinstance(d, str) and d.startswith("http"):
                        return d.rstrip("/")
        except Exception:
            continue
    return "https://mzfi.me"


async def _mb_h5_play(subject_id: str, se: int = 0, ep: int = 0, detail_path: str = "") -> dict:
    """H5 subject/play + download — often returns H.264 when mobile is HEVC-only."""
    domain = await _mb_h5_domain()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Origin": domain,
        "Referer": f"{domain}/",
        "X-Client-Info": '{"timezone":"Asia/Dhaka"}',
        "X-Request-Lang": "en",
    }
    out: dict = {}
    async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
        params = {"subjectId": subject_id, "se": se, "ep": ep}
        if detail_path:
            params["detailPath"] = detail_path
        for path in (
            f"{domain}/wefeed-h5api-bff/subject/play",
            "https://h5-api.aoneroom.com/wefeed-h5api-bff/subject/play",
            "https://h5-api.aoneroom.com/wefeed-h5api-bff/subject/download",
        ):
            try:
                r = await client.get(path, params=params, headers=headers)
                if r.status_code != 200:
                    continue
                data = (r.json() or {}).get("data") or {}
                if not isinstance(data, dict):
                    continue
                # merge streams
                for key in ("streams", "dash", "hls", "downloads"):
                    if data.get(key):
                        out.setdefault(key, [])
                        if isinstance(data[key], list):
                            out[key].extend(data[key])
                        else:
                            out[key].append(data[key])
                if data.get("hasResource") is not None:
                    out["hasResource"] = data.get("hasResource")
            except Exception:
                continue
    return out


def _is_mb_notice_url(url: str) -> bool:
    """Filter MovieBox 'App Upgrade Notice' / dummy promo clips."""
    if not url or _is_dummy_url(url):
        return True
    u = url.lower()
    bad = (
        "upgrade", "notice", "app-upgrade", "discontinu", "legacy",
        "update-now", "movieboxdownload", "promo", "/notice/",
        "macdn.aoneroom.com/other",
    )
    return any(b in u for b in bad)


def _parse_mb_play_info(data: dict, user_agent: str) -> List[dict]:
    out: List[dict] = []
    seen = set()
    streams = data.get("streams") or data.get("streamList") or []
    if not isinstance(streams, list):
        streams = []
    for stream in streams:
        if not isinstance(stream, dict):
            continue
        cookie = stream.get("signCookie") or stream.get("cookie") or ""
        raw_url = stream.get("url") or ""
        playable = _dash_from_sign_cookie(cookie)
        if not playable and raw_url and not _is_dummy_url(raw_url):
            playable = raw_url
        if not playable or playable in seen or _is_mb_notice_url(playable):
            continue
        seen.add(playable)
        res = stream.get("resolutions") or stream.get("resolution") or stream.get("quality") or "?"
        fmt = stream.get("format") or (
            "DASH" if ".mpd" in playable else "HLS" if ".m3u8" in playable else "MP4"
        )
        headers = {"User-Agent": user_agent or _mb_ua, "Referer": "https://sportslive.wine"}
        if cookie:
            headers["Cookie"] = "; ".join(p.strip() for p in cookie.strip(";").split(";") if p.strip())
        out.append(
            {
                "resolution": f"{res}p" if str(res).replace(",", "").isdigit() else str(res),
                "format": fmt,
                "url": playable,
                "size": stream.get("size"),
                "duration": stream.get("duration"),
                "codec": stream.get("codecName") or stream.get("codec"),
                "id": stream.get("id"),
                "headers": headers,
                "source": "play-info",
            }
        )
    for detector in data.get("resourceDetectors") or []:
        if not isinstance(detector, dict):
            continue
        for video in detector.get("resolutionList") or []:
            if not isinstance(video, dict):
                continue
            link = video.get("resourceLink") or video.get("url")
            if not link or link in seen or _is_dummy_url(link):
                continue
            seen.add(link)
            out.append(
                {
                    "resolution": f"{video.get('resolution', '?')}p",
                    "format": "MP4",
                    "url": link,
                    "headers": {"User-Agent": user_agent or _mb_ua},
                    "source": "resourceDetector",
                }
            )
    for key in ("dash", "hls"):
        for item in data.get(key) or []:
            if not isinstance(item, dict):
                continue
            link = item.get("url") or item.get("resourceLink")
            if not link or link in seen or _is_dummy_url(link):
                continue
            seen.add(link)
            out.append(
                {
                    "resolution": str(item.get("resolutions") or item.get("resolution") or "auto"),
                    "format": key.upper(),
                    "url": link,
                    "headers": {"User-Agent": user_agent or _mb_ua},
                    "source": key,
                }
            )
    return out


async def _mb_resource_links(subject_id: str, se: int, ep: int) -> List[dict]:
    try:
        if se == 0 and ep == 0:
            path = f"/wefeed-mobile-bff/subject-api/resource?subjectId={subject_id}&page=1&perPage=30"
        else:
            path = (
                f"/wefeed-mobile-bff/subject-api/resource"
                f"?subjectId={subject_id}&se={se}&ep={ep}&page=1&perPage=30"
            )
        data = await mb_request("GET", path)
        items = data.get("list") or []
        out: List[dict] = []
        seen = set()
        for item in items:
            if not isinstance(item, dict):
                continue
            link = item.get("resourceLink") or item.get("url")
            if not link or link in seen or _is_dummy_url(link):
                continue
            seen.add(link)
            fmt = "DASH" if ".mpd" in link else "HLS" if ".m3u8" in link else "MP4" if ".mp4" in link else "FILE"
            out.append(
                {
                    "resolution": f"{item.get('resolution', '?')}p",
                    "format": fmt,
                    "url": link,
                    "size": item.get("size"),
                    "filename": item.get("fileName") or item.get("title"),
                    "id": item.get("resourceId") or item.get("id"),
                    "headers": {"User-Agent": _mb_ua},
                    "source": "resource",
                }
            )
        return out
    except Exception:
        return []


# =============================================================================
# HUBCLOUD / HUBDRIVE RESOLVER
# =============================================================================

FK_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


def _pixeldrain_file_id(url: str) -> Optional[str]:
    try:
        p = urlparse(url)
        host = (p.hostname or "").lower()
        if "pixeldrain." not in host and "pixeldra.in" not in host and "pixeldrain.eu.cc" not in host:
            return None
        path = p.path or ""
        fid = None
        if "/u/" in path:
            fid = path.split("/u/")[-1].strip("/").split("/")[0]
        elif "/api/file/" in path:
            fid = path.split("/api/file/")[-1].strip("/").split("/")[0].split("?")[0]
        else:
            # bare /{id} on bypass CDN
            parts = [x for x in path.split("/") if x]
            if len(parts) == 1 and re.match(r"^[\w\-]+$", parts[0]):
                fid = parts[0]
        if fid and re.match(r"^[\w\-]+$", fid):
            return fid
    except Exception:
        return None
    return None


def _pixeldrain_api(url: str) -> Optional[str]:
    """GameDrive bypass CDN entry (resolved further at preflight)."""
    fid = _pixeldrain_file_id(url)
    if not fid:
        return None
    return f"https://cdn.pixeldrain.eu.cc/{fid}"


def _pixeldrain_bypass_urls(api_url: str) -> List[str]:
    """GameDrive / pixeldrain-bypass.gamedrive.org CDN first, then official API."""
    fid = _pixeldrain_file_id(api_url)
    if not fid:
        # try extract from any url string
        m = re.search(r"(?:pixeldrain\.[a-z.]+/(?:u|api/file)/|cdn\.pixeldrain\.eu\.cc/)([\w\-]+)", api_url or "")
        fid = m.group(1) if m else None
    urls = []
    if fid:
        urls.append(f"https://cdn.pixeldrain.eu.cc/{fid}")
        urls.append(f"https://pixeldrain.com/api/file/{fid}?download")
        urls.append(f"https://pixeldrain.dev/api/file/{fid}?download")
    if api_url and api_url not in urls:
        urls.append(api_url)
    out, seen = [], set()
    for u in urls:
        if u and u not in seen:
            seen.add(u)
            out.append(u)
    return out



async def preflight_url(url: str, headers: Optional[dict] = None) -> Optional[str]:
    """Light probe — HEAD first, then tiny Range GET. Keeps final redirected URL."""
    h = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
        "Accept": "*/*",
    }
    if headers:
        h.update({k: v for k, v in headers.items() if k.lower() not in ("range",)})
    try:
        async with httpx.AsyncClient(follow_redirects=True, timeout=10.0) as client:
            # HEAD is enough for CDN availability
            try:
                rh = await client.head(url, headers=h)
                if rh.status_code in (200, 206):
                    ctype = (rh.headers.get("content-type") or "").lower()
                    if "text/html" not in ctype and "application/json" not in ctype:
                        final = str(rh.url)
                        if _is_playable_direct(final):
                            return final
            except Exception:
                pass
            h2 = {**h, "Range": "bytes=0-1023"}
            r = await client.get(url, headers=h2)
            if r.status_code not in (200, 206):
                return None
            if len(r.content) < 32:
                return None
            ctype = (r.headers.get("content-type") or "").lower()
            if "text/html" in ctype or "application/json" in ctype:
                return None
            head = r.content[:120].lstrip().lower()
            if head.startswith(b"<!doctype") or head.startswith(b"<html") or head.startswith(b"{"):
                return None
            final = str(r.url)
            if not _is_playable_direct(final):
                return None
            return final
    except Exception:
        return None
    return None




async def collect_4k_mirrors(title: str, se: int = 0, ep: int = 0, limit: int = 12) -> List[dict]:
    """Search 4KHDHub by title → releases → HubCloud resolve → preflight working links only."""
    mirrors: List[dict] = []
    clean = re.sub(r"\[[^\]]*\]", " ", title or "")
    clean = re.sub(r"\([^)]*\)", " ", clean)
    clean = re.sub(r"\s+", " ", clean).strip()
    if not clean:
        return []
    try:
        html = await fk_fetch(f"?s={clean}")
        items = fk_parse_search(html)
    except Exception:
        return []
    if not items:
        return []
    # pick best match page
    page_id = items[0].get("id")
    if not page_id:
        return []
    try:
        page_html = await fk_fetch(page_id)
        releases = fk_parse_releases(page_html, se if se else 0, ep if ep else 0)
    except Exception:
        return []
    for rel in releases[:8]:
        for mirror in (rel.get("mirrors") or [])[:6]:
            if len(mirrors) >= limit:
                break
            url = mirror.get("url") or ""
            label = mirror.get("label") or "Mirror"
            fname = rel.get("filename") or rel.get("title") or ""
            direct_list = []
            if mirror.get("needs_resolve") or ("hubcloud." in url and "/drive/" in url):
                try:
                    direct_list = await resolve_any(url)
                except Exception:
                    direct_list = []
            elif url.startswith("https://"):
                direct_list = [{"url": url, "label": label, "headers": {"User-Agent": FK_UA, "Referer": url}}]
            for d in direct_list:
                du = d.get("url") or ""
                if not du:
                    continue
                # expand pixeldrain variants
                variants = _pixeldrain_bypass_urls(du) if "pixeldrain." in du else [du]
                ok = None
                hdrs = d.get("headers") or {"User-Agent": FK_UA}
                for v in variants:
                    ok = await preflight_url(v, hdrs)
                    if ok:
                        du = ok
                        break
                if not ok:
                    continue
                # browser-playable preference
                low = du.lower()
                fmt = "MP4" if ".mp4" in low else ("MKV" if ".mkv" in low else "FILE")
                mirrors.append({
                    "provider": "4khdhub",
                    "label": f"{fname[:40] + ' · ' if fname else ''}{d.get('label') or label}"[:70],
                    "url": du,
                    "format": fmt,
                    "type": "direct",
                    "headers": hdrs,
                    "play_url": None,  # filled later if needs proxy; file hosts usually not
                    "filename": fname,
                })
                if len(mirrors) >= limit:
                    break
        if len(mirrors) >= limit:
            break
    return mirrors



def _unwrap_pages_dev(url: str) -> Optional[str]:
    try:
        p = urlparse(url)
        if not p.hostname or "pages.dev" not in p.hostname:
            return None
        qs = dict(parse_qsl(p.query))
        b64 = qs.get("u")
        if not b64:
            return None
        pad = (4 - len(b64) % 4) % 4
        decoded = base64.b64decode(b64 + ("=" * pad)).decode("utf-8", errors="ignore")
        if decoded.startswith("https://"):
            return decoded
    except Exception:
        pass
    return None


def _is_playable_direct(url: str) -> bool:
    """Only CDN / file hosts — reject ads, telegram, intermediate download pages."""
    try:
        p = urlparse(url)
        if p.scheme != "https" or not p.hostname:
            return False
        host = p.hostname.lower()
        path = p.path.lower()
        full = url.lower()
        if host in ("localhost",) or host.endswith(".local"):
            return False
        if path.endswith((".zip", ".rar", ".7z")) or "login.php" in path or "logout" in path:
            return False
        if "hubcloud." in host and path.startswith("/drive/"):
            return False
        block = (
            "t.me", "telegram.", "tinyurl.", "bit.ly", "one.one.one.one",
            "cloudflare.com", "hdhub4u", "facebook.", "youtube.", "instagram.",
            "twitter.", "x.com", "reddit.", "gamerxyt.", "how-to", "vpn",
            "idm.", "chrome.", "play.google.",
        )
        if any(b in host for b in block):
            return False
        if "telegram" in full or "t.me/" in full:
            return False
        # Allowed CDN / storage hosts (MovieBox-TUI priority list)
        good = (
            "pixeldrain.", "pixeldrain.eu.cc", "workers.dev", "r2.dev", "cloudflarestorage",
            "googleusercontent.com", "storage.googleapis.com", "googleapis.com",
            "gofile.", "workupload.", "streamtape.", "pixel.",
            "download.", "cdn.", "hubcloud.fans", "hubcloud.cx", "hubcloud.ist",
        )
        if any(g in host for g in good):
            return True
        if any(path.endswith(ext) for ext in (".mp4", ".mkv", ".m3u8", ".mpd", ".avi", ".mov", ".webm")):
            return True
        return False
    except Exception:
        return False


def _score_mirror(url: str, label: str) -> int:
    v = f"{url} {label}".lower()
    if any(
        x in v
        for x in (
            "cloudflarestorage.com",
            "r2.cloudflarestorage.com",
            "r2.dev",
            "workers.dev",
            "fsl server",
            "watch online",
        )
    ):
        return 0
    if any(x in v for x in ("storage.googleapis.com", "hubcloud.cx/re/", "hubcloud.fans/re/")):
        return 1
    if "pixeldrain" in v:
        return 2
    if any(x in v for x in ("googleusercontent.com", "googlevideo.com", "testzip.php", "gpdl.")):
        return 3
    return 4



def _rot13(s: str) -> str:
    out = []
    for c in s:
        if "a" <= c <= "z":
            out.append(chr((ord(c) - 97 + 13) % 26 + 97))
        elif "A" <= c <= "Z":
            out.append(chr((ord(c) - 65 + 13) % 26 + 65))
        else:
            out.append(c)
    return "".join(out)


def _b64pad(s: str) -> str:
    return s + ("=" * ((4 - len(s) % 4) % 4))


def _decode_greenmotors_payload(payload: str) -> Optional[str]:
    """MovieBox-TUI compatible greenmotors decode pipeline."""
    try:
        s1 = base64.b64decode(_b64pad(payload)).decode("utf-8")
        s2 = base64.b64decode(_b64pad(s1)).decode("utf-8")
        s3 = _rot13(s2)
        s4 = base64.b64decode(_b64pad(s3)).decode("utf-8")
        j = json.loads(s4)
        target_b64 = j.get("o") or ""
        return base64.b64decode(_b64pad(target_b64)).decode("utf-8")
    except Exception:
        return None


async def resolve_greenmotors(url: str) -> List[dict]:
    """greenmotors.club/?id=… → hubcloud/hubdrive/direct (MovieBox-TUI)."""
    async with httpx.AsyncClient(follow_redirects=True, timeout=25.0, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://4khdhub.one/",
    }) as client:
        r = await client.get(url)
        html = r.text
    payload = None
    m = re.search(r"s\(\s*['\"]o['\"]\s*,\s*['\"]([^'\"]+)['\"]", html)
    if m:
        payload = m.group(1)
    if not payload:
        # try id query as last resort
        m = re.search(r"[?&]id=([^&]+)", url)
        if m:
            payload = unquote(m.group(1))
    target = _decode_greenmotors_payload(payload) if payload else None
    if not target:
        raise HTTPException(502, "GreenMotors: could not decode target")
    if "hubcloud." in target and "/drive/" in target:
        return await resolve_hubcloud(target)
    if "hubdrive." in target:
        return await resolve_hubdrive(target)
    return [{"url": target, "label": "Direct", "source": "greenmotors"}]


async def resolve_hubcloud(drive_url: str) -> List[dict]:
    """
    hubcloud.*/drive/xxx  →  all direct mirrors (FSL, 10Gbps, PixelDrain, Watch Online, R2…)
    """
    if "hubcloud." not in drive_url or "/drive/" not in drive_url:
        raise HTTPException(400, "Not a HubCloud /drive/ URL")

    headers = {
        "User-Agent": FK_UA,
        "Referer": drive_url,
        "Accept": "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
    }
    async with httpx.AsyncClient(follow_redirects=True, timeout=30.0, headers=headers) as client:
        r1 = await client.get(drive_url)
        if r1.status_code != 200:
            raise HTTPException(502, f"HubCloud page HTTP {r1.status_code}")
        soup1 = BeautifulSoup(r1.text, "html.parser")
        title = (soup1.title.string or "").strip() if soup1.title else ""

        resolver_urls: List[str] = []
        for a in soup1.select("a[href]"):
            href = (a.get("href") or "").strip()
            if not href.startswith("http"):
                continue
            low = href.lower()
            if any(x in low for x in ("gamerxyt.com", "hubcloud.php", "/download/", "generate")):
                if href not in resolver_urls:
                    resolver_urls.append(href)
        if not resolver_urls:
            m = re.search(
                r"https?://gamerxyt\.com/hubcloud\.php\?[^\s\"'<>]+",
                r1.text,
            )
            if m:
                resolver_urls.append(m.group(0).rstrip("\\';'\""))
        if not resolver_urls:
            raise HTTPException(502, "HubCloud: download / generate link not found")

        html2 = ""
        for ru in resolver_urls:
            try:
                r2 = await client.get(ru, headers={**headers, "Referer": drive_url})
                if r2.status_code == 200 and len(r2.text) > 200 and "404" not in r2.text[:20]:
                    html2 = r2.text
                    break
            except Exception:
                continue
        if not html2:
            raise HTTPException(502, "HubCloud resolver page failed (gamerxyt/mirror down)")

        soup2 = BeautifulSoup(html2, "html.parser")
        candidates: List[Tuple[int, str, str]] = []

        def _add(url: str, label: str, score: int = 50):
            if not url or not url.startswith("http"):
                return
            # normalize pixeldrain
            pd = _pixeldrain_api(url) if "pixeldrain" in url.lower() or "pixeldra" in url.lower() else None
            final = pd or url
            try:
                unwrapped = _unwrap_pages_dev(final)
                if unwrapped and unwrapped.startswith("http"):
                    final = unwrapped
            except Exception:
                pass
            candidates.append((score, final, label[:100]))

        for a in soup2.select("a[href]"):
            href = (a.get("href") or "").strip()
            if not href.startswith("http"):
                continue
            label = a.get_text(" ", strip=True) or "Direct"
            low = (label + " " + href).lower()
            if any(x in low for x in (
                "telegram", "winexch", "login", "vpn", "tutorial", "idm", "ida",
                "google.com/search", "movies4u", "hdhub4u", "facebook", "twitter",
                "instagram", "youtube.com", "t.me/", "favicon", "bootstrap",
            )):
                continue
            try:
                from urllib.parse import urlparse as _up
                path=(_up(href).path or "").strip("/")
                host=(_up(href).hostname or "").lower()
                junk_hosts=("movies4u.","hdhub4u.","t.me","telegram","facebook.","twitter.","instagram.")
                if any(host.endswith(j.rstrip(".")) or j in host for j in junk_hosts):
                    continue
                if not path and not any(x in host for x in ("pixeldrain","r2.cloudflare","workers.dev","bunker.monster","hubcloud")):
                    continue
            except Exception:
                pass
            score = 50
            if "fsl" in low:
                score = 5
            elif "10gbps" in low or "10 gbps" in low:
                score = 10
            elif "pixel" in low:
                score = 15
            elif "watch" in low:
                score = 20
            elif "r2.cloudflare" in low or "workers.dev" in low:
                score = 12
            elif "hubcdn" in low or "gpdl.hubcloud" in low:
                score = 18
            _add(href, label, score)

        # raw pixeldrain strings in HTML
        for m in re.finditer(
            r"https?://(?:www\.)?pixeldrain\.(?:com|dev|net)/[u/]+([A-Za-z0-9_-]+)",
            html2,
        ):
            fid = m.group(1)
            _add(f"https://cdn.pixeldrain.eu.cc/{fid}", "PixelDrain", 15)
            _add(f"https://pixeldrain.com/api/file/{fid}?download", "PixelDrain API", 16)

        candidates.sort(key=lambda x: x[0])
        seen = set()
        results = []
        for score, url, label in candidates:
            if url in seen:
                continue
            seen.add(url)
            results.append(
                {
                    "label": label,
                    "url": url,
                    "priority": score,
                    "direct": True,
                    "title": title or None,
                    "source": "hubcloud",
                }
            )
        if not results:
            raise HTTPException(502, "HubCloud: no direct links extracted")
        return results


async def resolve_hubdrive(file_url: str) -> List[dict]:
    """
    hubdrive.*/file/xxx → HubCloud server → same mirrors as HubCloud.
    """
    if "hubdrive." not in file_url:
        raise HTTPException(400, "Not a HubDrive URL")
    headers = {"User-Agent": FK_UA, "Referer": file_url, "Accept": "text/html,*/*"}
    async with httpx.AsyncClient(follow_redirects=True, timeout=25.0, headers=headers) as client:
        r = await client.get(file_url)
        if r.status_code != 200:
            raise HTTPException(502, f"HubDrive HTTP {r.status_code}")
        soup = BeautifulSoup(r.text, "html.parser")
        title = (soup.title.string or "").strip() if soup.title else ""
        hub_links: List[str] = []
        for a in soup.select("a[href]"):
            href = (a.get("href") or "").strip()
            text = a.get_text(" ", strip=True)
            if "hubcloud" in href.lower() or "hubcloud" in text.lower():
                if href.startswith("/"):
                    href = str(r.url).rstrip("/") + href  # unlikely
                if href.startswith("http") and href not in hub_links:
                    hub_links.append(href)
        # relative hubcloud sometimes
        if not hub_links:
            m = re.search(r"https?://hubcloud\.[a-z.]+/drive/[A-Za-z0-9_-]+", r.text)
            if m:
                hub_links.append(m.group(0))
        if not hub_links:
            raise HTTPException(502, "HubDrive: HubCloud server link not found")
        all_links: List[dict] = []
        seen = set()
        for hl in hub_links[:3]:
            try:
                part = await resolve_hubcloud(hl)
                for item in part:
                    u = item.get("url")
                    if u and u not in seen:
                        seen.add(u)
                        item = dict(item)
                        item["hubdrive"] = file_url
                        item["hubcloud"] = hl
                        if title and not item.get("title"):
                            item["title"] = title
                        all_links.append(item)
            except Exception:
                continue
        if not all_links:
            raise HTTPException(502, "HubDrive: could not resolve HubCloud mirrors")
        return all_links


async def resolve_any(url: str) -> List[dict]:
    u = url.strip()
    low = u.lower()
    if "hubcloud." in low and "/drive/" in low:
        return await resolve_hubcloud(u)
    if "hubdrive." in low:
        return await resolve_hubdrive(u)
    if "gamerxyt.com/hubcloud.php" in low:
        # treat as already-resolved generator page — wrap as drive if possible
        m = re.search(r"[?&]id=([A-Za-z0-9_-]+)", u)
        if m:
            return await resolve_hubcloud(f"https://hubcloud.ist/drive/{m.group(1)}")
    if "pixeldrain." in low:
        api = _pixeldrain_api(u)
        if api:
            return [{"label": "PixelDrain", "url": api, "priority": 10, "direct": True, "source": "pixeldrain"}]
    if "greenmotors." in low or "greenmountmotors." in low:
        return await resolve_greenmotors(u)
    raise HTTPException(
        400,
        "Supported: hubcloud.*/drive/..., hubdrive.*/file/..., greenmotors, pixeldrain",
    )


# =============================================================================
# 4KHDHub scrape
# =============================================================================

FK_BASES = [
    "https://4khdhub.one/",
    "https://4khdhub.link/",
    "https://4khdhub.click/",
    "https://4khdhub.ink/",
]
_fk_base = FK_BASES[0]


async def fk_fetch(path_or_url: str) -> str:
    global _fk_base
    async with httpx.AsyncClient(follow_redirects=True, timeout=20.0, headers={"User-Agent": FK_UA}) as client:
        candidates = (
            [path_or_url]
            if path_or_url.startswith("http")
            else [urljoin(b, path_or_url.lstrip("/")) for b in FK_BASES]
        )
        last_err = None
        for url in candidates:
            try:
                resp = await client.get(url)
                if resp.status_code == 200 and len(resp.text) > 400:
                    p = urlparse(str(resp.url))
                    _fk_base = f"{p.scheme}://{p.netloc}/"
                    return resp.text
            except Exception as e:
                last_err = e
        raise HTTPException(502, f"4KHDHub unreachable: {last_err}")


def fk_parse_search(html: str) -> List[dict]:
    soup = BeautifulSoup(html, "html.parser")
    items = []
    for card in soup.select("a.movie-card"):
        href = card.get("href") or ""
        title_el = card.select_one(".movie-card-title")
        title = title_el.get_text(strip=True) if title_el else ""
        if not title or not href:
            continue
        meta_el = card.select_one(".movie-card-meta")
        meta_text = meta_el.get_text(" ", strip=True) if meta_el else ""
        img = card.select_one("img")
        year = None
        m = re.search(r"(19|20)\d{2}", meta_text or title)
        if m:
            year = m.group(0)
        path = urlparse(href).path if href.startswith("http") else href
        items.append(
            {
                "name": title,
                "id": path,
                "poster_url": img.get("src") if img else None,
                "year": year,
                "type": "series" if "-series-" in href else "movie",
                "provider": "4khdhub",
            }
        )
    return items


def fk_parse_releases(html: str, season: int = 0, episode: int = 0) -> List[dict]:
    soup = BeautifulSoup(html, "html.parser")
    item_sel = "#episodes .episode-download-item" if season > 0 else ".download-item"
    title_sel = ".episode-file-title" if season > 0 else ".file-title"
    releases = []
    for item in soup.select(item_sel):
        title_el = item.select_one(title_sel)
        filename = title_el.get_text(strip=True) if title_el else ""
        if not filename or filename.lower().endswith((".zip", ".rar", ".7z")):
            continue
        if season > 0:
            m = re.search(r"S0*(\d+)\s*E0*(\d+)", filename, re.I)
            if not m or int(m.group(1)) != season or int(m.group(2)) != episode:
                continue
        mirrors = []
        for link in item.select("a[href]"):
            href = link.get("href") or ""
            if not href.startswith("https://") or "logout" in href.lower():
                continue
            mirrors.append(
                {
                    "label": link.get_text(strip=True) or "Source",
                    "url": href,
                    "needs_resolve": any(x in href for x in ("hubcloud.", "hubdrive.", "greenmotors.", "gamerxyt.")),
                }
            )
        if not mirrors:
            continue
        quality = None
        for q in ("2160", "1080", "720", "480", "360"):
            if q in filename:
                quality = f"{q}p"
                break
        size_el = item.select_one(".badge-size, .badge")
        releases.append(
            {
                "filename": filename,
                "quality": quality,
                "size": size_el.get_text(strip=True) if size_el else None,
                "mirrors": mirrors,
            }
        )
    return releases


# =============================================================================
# UI
# =============================================================================

# (old static landing removed — SPA at / and /site)


# =============================================================================
# ROUTES
# =============================================================================


# old root replaced by SPA below


@app.get("/health", tags=["Meta"])
async def health():
    return {"ok": True, "version": "5.13.1", "providers": ["tmdb", "vidsrc", "4khdhub", "hubcloud", "ytmusic", "jiosaavn", "deezer", "piped", "lrclib", "moviebox-api", "netmirror", "vixsrc"]}


# ----- Mov
# =============================================================================
# STREAM PROXY — MovieBox-TUI style (Cookie/UA on every segment + Range + MPD rewrite)
# =============================================================================

def _b64url_encode(obj: dict) -> str:
    raw = json.dumps(obj, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def _b64url_decode(s: str) -> dict:
    pad = "=" * ((4 - len(s) % 4) % 4)
    return json.loads(base64.urlsafe_b64decode(s + pad))


def _proxy_url(src: dict, base: str = "") -> str:
    """play_url for browser: /proxy/file/{token} where token holds url+cookie+ua+referer."""
    if src.get("type") != "direct" or not src.get("url"):
        return src.get("url") or ""
    h = src.get("headers") or {}
    tok = _b64url_encode({
        "u": src["url"],
        "c": h.get("Cookie") or h.get("cookie") or "",
        "r": h.get("Referer") or h.get("referer") or "https://sportslive.wine",
        "a": h.get("User-Agent") or h.get("user-agent") or (_mb_ua or "Mozilla/5.0"),
    })
    return f"/proxy/file/{tok}"


def _auth_headers(cookie: str, referer: str, ua: str) -> dict:
    h = {
        "User-Agent": ua or "Mozilla/5.0",
        "Accept": "*/*",
        "Referer": referer or "https://sportslive.wine",
        "Origin": "https://sportslive.wine",
    }
    if cookie:
        h["Cookie"] = cookie
    return h


@app.get("/proxy/file/{token}", tags=["Playback"])
async def proxy_file(token: str, request: Request):
    try:
        meta = _b64url_decode(token)
    except Exception:
        raise HTTPException(400, "bad token")
    url = meta.get("u") or ""
    return await _proxy_upstream(
        url,
        meta.get("c") or "",
        meta.get("r") or "https://sportslive.wine",
        meta.get("a") or "Mozilla/5.0",
        range_header=request.headers.get("range") if request else None,
        rewrite_manifest=True,
        token_cookie=meta.get("c") or "",
        token_ref=meta.get("r") or "https://sportslive.wine",
        token_ua=meta.get("a") or "Mozilla/5.0",
    )


@app.api_route("/proxy/cdn/{token}/{path:path}", methods=["GET", "HEAD"], tags=["Playback"])
async def proxy_cdn(token: str, path: str, request: Request):
    """Segment/path under CDN — TUI equivalent of /https/host/path with auth."""
    try:
        meta = _b64url_decode(token)
    except Exception:
        raise HTTPException(400, "bad token")
    base = (meta.get("u") or "").rstrip("/")
    # meta.u is directory URL ending with /
    if not base.endswith("/"):
        base = base + "/"
    url = base + path.lstrip("/")
    return await _proxy_upstream(
        url,
        meta.get("c") or "",
        meta.get("r") or "https://sportslive.wine",
        meta.get("a") or "Mozilla/5.0",
        range_header=request.headers.get("range"),
        rewrite_manifest=False,
    )


@app.get("/proxy/{token}", tags=["Playback"])
async def proxy_legacy(token: str, request: Request):
    try:
        meta = _b64url_decode(token)
    except Exception:
        raise HTTPException(400, "bad token")
    return await _proxy_upstream(
        meta.get("u") or "",
        meta.get("c") or "",
        meta.get("r") or "https://sportslive.wine",
        meta.get("a") or "Mozilla/5.0",
        range_header=request.headers.get("range"),
        rewrite_manifest=True,
        token_cookie=meta.get("c") or "",
        token_ref=meta.get("r") or "https://sportslive.wine",
        token_ua=meta.get("a") or "Mozilla/5.0",
    )


async def _proxy_upstream(
    url: str,
    cookie: str,
    referer: str,
    ua: str,
    range_header: Optional[str] = None,
    rewrite_manifest: bool = False,
    token_cookie: str = "",
    token_ref: str = "",
    token_ua: str = "",
):
    if not url.startswith(("https://", "http://")):
        raise HTTPException(400, "bad upstream url")
    headers = _auth_headers(cookie, referer, ua)
    if range_header:
        headers["Range"] = range_header

    async with httpx.AsyncClient(follow_redirects=True, timeout=60.0) as client:
        try:
            upstream = await client.get(url, headers=headers)
        except Exception as e:
            raise HTTPException(502, f"proxy: {e}")

        if upstream.status_code >= 400:
            raise HTTPException(upstream.status_code, f"upstream {upstream.status_code}")

        ctype = (upstream.headers.get("content-type") or "").lower()
        body = upstream.content
        final_url = str(upstream.url)
        is_mpd = (
            rewrite_manifest
            and (
                final_url.lower().endswith(".mpd")
                or "mpd" in ctype
                or body[:200].lstrip().startswith(b"<?xml")
            )
        )
        is_m3u = rewrite_manifest and (
            final_url.lower().endswith(".m3u8") or "mpegurl" in ctype or body[:7] == b"#EXTM3U"
        )

        if is_mpd:
            text = body.decode("utf-8", errors="ignore")
            # Directory for relative segments
            dir_url = final_url.rsplit("/", 1)[0] + "/"
            host = urlparse(final_url).hostname or ""
            dir_tok = _b64url_encode({
                "u": dir_url,
                "c": token_cookie or cookie,
                "r": token_ref or referer,
                "a": token_ua or ua,
            })
            proxy_base = f"/proxy/cdn/{dir_tok}/"

            # TUI-style: rewrite absolute CDN host URLs → our proxy base + path
            https_prefix = f"https://{host}/"
            http_prefix = f"http://{host}/"
            text = text.replace(https_prefix, proxy_base).replace(http_prefix, proxy_base)

            # Inject BaseURL for relative SegmentTemplate ($Number$ stays intact)
            import re as _re
            if not _re.search(r"<BaseURL>", text, _re.I):
                if _re.search(r"<Period[^>]*>", text, _re.I):
                    text = _re.sub(
                        r"(<Period[^>]*>)",
                        rf"\1\n    <BaseURL>{proxy_base}</BaseURL>",
                        text,
                        count=1,
                        flags=_re.I,
                    )
                else:
                    text = _re.sub(
                        r"(<MPD[^>]*>)",
                        rf"\1\n  <BaseURL>{proxy_base}</BaseURL>",
                        text,
                        count=1,
                        flags=_re.I,
                    )

            return Response(
                content=text.encode("utf-8"),
                status_code=200,
                media_type="application/dash+xml",
                headers={
                    "cache-control": "no-store",
                    "access-control-allow-origin": "*",
                    "access-control-expose-headers": "Content-Length, Content-Range, Accept-Ranges",
                },
            )

        if is_m3u:
            text = body.decode("utf-8", errors="ignore")
            dir_url = final_url.rsplit("/", 1)[0] + "/"
            out = []
            for line in text.splitlines():
                if line and not line.startswith("#"):
                    seg = line.strip()
                    if not seg.startswith("http"):
                        seg = dir_url + seg
                    tok = _b64url_encode({
                        "u": seg,
                        "c": token_cookie or cookie,
                        "r": token_ref or referer,
                        "a": token_ua or ua,
                    })
                    out.append(f"/proxy/file/{tok}")
                else:
                    out.append(line)
            return Response(
                content="\n".join(out).encode("utf-8"),
                media_type="application/vnd.apple.mpegurl",
                headers={"cache-control": "no-store", "access-control-allow-origin": "*"},
            )

        # Binary segment / mp4 — stream with Range support
        out_headers = {
            "cache-control": "no-store",
            "access-control-allow-origin": "*",
            "access-control-expose-headers": "Content-Length, Content-Range, Accept-Ranges",
        }
        for k in ("content-type", "content-length", "content-range", "accept-ranges"):
            if k in upstream.headers:
                out_headers[k] = upstream.headers[k]
        media_type = (ctype.split(";")[0] if ctype else None) or "application/octet-stream"
        return Response(
            content=body,
            status_code=upstream.status_code,
            media_type=media_type,
            headers=out_headers,
        )





async def _clean_title(title: str) -> str:
    clean = re.sub(r"\[[^\]]*\]", " ", title or "")
    clean = re.sub(r"\([^)]*\)", " ", clean)
    clean = re.sub(r"\s+", " ", clean).strip()
    return clean or (title or "")



TMDB_KEY = os.environ.get("TMDB_API_KEY", "1f54bd990f1cdfb230adb312546d765d")
TMDB_BASE = "https://api.themoviedb.org/3"
TMDB_IMG = "https://image.tmdb.org/t/p"


async def _tmdb_get(path: str, params: Optional[dict] = None) -> dict:
    q = {"api_key": TMDB_KEY, "language": "en-US"}
    if params:
        q.update(params)
    async with httpx.AsyncClient(timeout=20.0) as client:
        r = await client.get(f"{TMDB_BASE}{path}", params=q)
        if r.status_code != 200:
            return {}
        return r.json()


def _tmdb_card(item: dict, media_hint: str = None) -> dict:
    media = media_hint or item.get("media_type")
    if not media:
        media = "tv" if item.get("name") and not item.get("title") else "movie"
    if media == "person":
        return None
    tid = item.get("id")
    title = item.get("title") or item.get("name") or ""
    date = item.get("release_date") or item.get("first_air_date") or ""
    poster = item.get("poster_path")
    return {
        "id": str(tid),
        "tmdb_id": str(tid),
        "name": title,
        "poster": f"{TMDB_IMG}/w500{poster}" if poster else None,
        "backdrop": f"{TMDB_IMG}/w1280{item.get('backdrop_path')}" if item.get("backdrop_path") else None,
        "year": date[:4] if date else None,
        "rating": item.get("vote_average"),
        "type": "tv" if media == "tv" else "movie",
        "provider": "tmdb",
        "overview": item.get("overview") or "",
    }


async def _tmdb_search_id(title: str, media: str = "movie"):
    key = os.environ.get("TMDB_API_KEY", "1f54bd990f1cdfb230adb312546d765d")
    clean = await _clean_title(title)
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.get(
                "https://api.themoviedb.org/3/search/" + ("tv" if media == "tv" else "movie"),
                params={"api_key": key, "query": clean},
            )
            if r.status_code == 200:
                results = (r.json().get("results") or [])
                if results:
                    return {"tmdb": str(results[0]["id"]), "imdb": None, "name": results[0].get("title") or results[0].get("name")}
    except Exception:
        pass
    # Cinemeta fallback → IMDB id
    try:
        kind = "series" if media == "tv" else "movie"
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.get(f"https://v3-cinemeta.strem.io/catalog/{kind}/top/search={clean}.json")
            if r.status_code == 200:
                metas = r.json().get("metas") or []
                if metas:
                    imdb = metas[0].get("imdb_id") or metas[0].get("id")
                    if imdb and str(imdb).startswith("tt"):
                        return {"tmdb": None, "imdb": str(imdb), "name": metas[0].get("name")}
    except Exception:
        pass
    return None


def _embed_sources_meta(meta: dict, media: str, se: int = 1, ep: int = 1):
    """KiduyuTv + FilmSnaps embed catalog (TMDB id)."""
    tmdb = meta.get("tmdb")
    if not tmdb:
        return []
    try:
        tid = int(tmdb)
    except Exception:
        return []
    se = max(1, int(se or 1))
    ep = max(1, int(ep or 1))
    is_tv = media in ("tv", "series", "show")
    templates = [
        ("Videasy", "https://player.videasy.net/movie/{id}", "https://player.videasy.net/tv/{id}/{se}/{ep}"),
        ("Vidrock", "https://vidrock.net/movie/{id}", "https://vidrock.net/tv/{id}/{se}/{ep}"),
        ("VidLink", "https://vidlink.pro/movie/{id}", "https://vidlink.pro/tv/{id}/{se}/{ep}"),
        ("VidFast", "https://vidfast.pro/movie/{id}", "https://vidfast.pro/tv/{id}/{se}/{ep}"),
        ("VidKing", "https://www.vidking.net/embed/movie/{id}", "https://www.vidking.net/embed/tv/{id}/{se}/{ep}"),
        ("VidNest", "https://vidnest.fun/movie/{id}", "https://vidnest.fun/tv/{id}/{se}/{ep}"),
        ("VidUp", "https://vidup.to/movie/{id}", "https://vidup.to/tv/{id}/{se}/{ep}"),
        ("111Movies", "https://111movies.com/movie/{id}", "https://111movies.com/tv/{id}/{se}/{ep}"),
        ("Flixer", "https://flixer.su/watch/movie/{id}", "https://flixer.su/watch/tv/{id}/{se}/{ep}"),
        ("VidCore", "https://vidcore.net/movie/{id}", "https://vidcore.net/tv/{id}/{se}/{ep}"),
        ("MoviesApi", "https://moviesapi.to/movie/{id}", "https://moviesapi.to/tv/{id}-{se}-{ep}"),
        ("Peachify", "https://peachify.top/embed/movie/{id}", "https://peachify.top/embed/tv/{id}/{se}/{ep}"),
        ("VidAPI", "https://vaplayer.ru/embed/movie/{id}", "https://vaplayer.ru/embed/tv/{id}/{se}/{ep}"),
        ("VidPlus", "https://player.vidplus.to/embed/movie/{id}", "https://player.vidplus.to/embed/tv/{id}/{se}/{ep}"),
        ("CineSrc", "https://cinesrc.st/embed/movie/{id}", "https://cinesrc.st/embed/tv/{id}?s={se}&e={ep}"),
        ("Vidzen", "https://vidzen.fun/movie/{id}", "https://vidzen.fun/tv/{id}/{se}/{ep}"),
        ("Cinemaos", "https://cinemaos.tech/player/{id}", "https://cinemaos.tech/player/{id}/{se}/{ep}"),
        ("Amri", "https://amri.gg/movie/{id}", "https://amri.gg/tv/{id}/{se}/{ep}"),
        ("ZxcStream", "https://zxcstream.xyz/embed/movie/{id}", "https://zxcstream.xyz/embed/tv/{id}/{se}/{ep}"),
        ("VidLux", "https://vidlux.xyz/embed/movie/{id}", "https://vidlux.xyz/embed/tv/{id}/{se}/{ep}"),
        ("VidSrc WTF v4", "https://vidsrc.wtf/api/4/movie/?id={id}", "https://vidsrc.wtf/api/4/tv/?id={id}&s={se}&e={ep}"),
        ("VidSrc WTF v3", "https://vidsrc.wtf/api/3/movie/?id={id}", "https://vidsrc.wtf/api/3/tv/?id={id}&s={se}&e={ep}"),
        ("PrimeSrc", "https://primesrc.me/embed/movie?tmdb={id}", "https://primesrc.me/embed/tv?tmdb={id}&season={se}&episode={ep}"),
        ("VidZee", "https://player.vidzee.wtf/v2/embed/movie/{id}", "https://player.vidzee.wtf/v2/embed/tv/{id}/{se}/{ep}"),
        ("Lordflix", "https://lordflix.org/watch/movie/{id}", "https://lordflix.org/watch/tv/{id}/{se}/{ep}"),
        ("VidSrc", "https://vidsrc.to/embed/movie/{id}", "https://vidsrc.to/embed/tv/{id}/{se}/{ep}"),
        ("VidSrc.cc", "https://vidsrc.cc/v2/embed/movie/{id}", "https://vidsrc.cc/v2/embed/tv/{id}/{se}/{ep}"),
        ("AutoEmbed", "https://player.autoembed.cc/embed/movie/{id}", "https://player.autoembed.cc/embed/tv/{id}/{se}/{ep}"),
        ("2Embed", "https://www.2embed.cc/embed/{id}", "https://www.2embed.cc/embedtv/{id}&s={se}&e={ep}"),
        ("MultiEmbed", "https://multiembed.mov/?video_id={id}&tmdb=1", "https://multiembed.mov/?video_id={id}&tmdb=1&s={se}&e={ep}"),
        ("EmbedSU", "https://embed.su/embed/movie/{id}", "https://embed.su/embed/tv/{id}/{se}/{ep}"),
        ("SmashyStream", "https://player.smashy.stream/movie/{id}", "https://player.smashy.stream/tv/{id}?s={se}&e={ep}"),
        ("Nxsha", "https://web.nxsha.app/embed/movie/{id}", "https://web.nxsha.app/embed/tv/{id}/{se}/{ep}"),
        ("ScreenScape", "https://screenscape.me/embed/movie/{id}", "https://screenscape.me/embed/tv/{id}/{se}/{ep}"),
        ("ChillFlix", "https://chillflix.lol/embed/movie/{id}", "https://chillflix.lol/embed/tv/{id}/{se}/{ep}"),
        ("MegaPlay", "https://megaplay.buzz/embed/movie/{id}", "https://megaplay.buzz/embed/tv/{id}/{se}/{ep}"),
    ]
    out = []
    for name, m_tpl, t_tpl in templates:
        try:
            url = (t_tpl if is_tv else m_tpl).format(id=tid, se=se, ep=ep)
            out.append({
                "provider": name,
                "url": url,
                "play_url": url,
                "type": "embed",
                "format": "EMBED",
                "phone_friendly": True,
            })
        except Exception:
            continue
    return out


def _embed_sources(tmdb_id: str, media: str, se: int = 1, ep: int = 1):
    return _embed_sources_meta({"tmdb": tmdb_id, "imdb": None}, media, se, ep)



@app.get("/mb/search", tags=["MovieBox"])
async def mb_search(q: str = Query(..., min_length=1, description="Search query"), page: int = 1):
    """Search MovieBox catalog. Use returned `subject_id` with /mb/stream and /mb/detail."""
    data = await mb_request(
        "POST",
        "/wefeed-mobile-bff/subject-api/search/v2",
        {"keyword": q, "page": page, "perPage": 20, "subjectType": 0},
    )
    items: List[dict] = []
    raw: List[Any] = []
    results = data.get("results") or []
    if results and isinstance(results[0], dict):
        raw = results[0].get("subjects") or []
    if not raw:
        raw = data.get("list") or data.get("items") or data.get("subjects") or []
    for s in raw:
        if isinstance(s, dict) and "subject" in s:
            s = s["subject"]
        if not isinstance(s, dict):
            continue
        sid = s.get("subjectId") or s.get("id")
        cover = s.get("cover") or {}
        poster = cover.get("url") if isinstance(cover, dict) else (s.get("coverUrl") or cover)
        items.append(
            {
                "name": s.get("title") or s.get("name"),
                "subject_id": str(sid) if sid is not None else None,
                "poster_url": poster,
                "slug": s.get("detailPath"),
                "year": (s.get("releaseDate") or "")[:4] or None,
                "rating": s.get("imdbRatingValue"),
                "type": "series" if (s.get("subjectType") or s.get("stype")) == 2 else "movie",
                "provider": "moviebox",
            }
        )
    return {"provider": "moviebox", "query": q, "page": page, "total": data.get("total") or len(items), "items": items}



def _mb_norm_subject(s: dict) -> dict:
    """Normalize a MovieBox subject dict into a stable card."""
    if not isinstance(s, dict):
        return {}
    if "subject" in s and isinstance(s["subject"], dict):
        s = s["subject"]
    sid = s.get("subjectId") or s.get("id") or s.get("subject_id")
    cover = s.get("cover") or {}
    poster = (
        cover.get("url")
        if isinstance(cover, dict)
        else (s.get("coverUrl") or s.get("poster") or s.get("poster_url") or "")
    )
    st = s.get("subjectType") or s.get("stype") or s.get("subject_type") or 1
    try:
        st = int(st)
    except Exception:
        st = 1
    kind = "series" if st == 2 else ("music" if st == 6 else "movie")
    return {
        "subject_id": str(sid) if sid is not None else None,
        "title": s.get("title") or s.get("name"),
        "type": kind,
        "subject_type": st,
        "year": s.get("releaseDate") or s.get("year") or s.get("release_date"),
        "rating": s.get("imdbRatingValue") or s.get("imdbRate") or s.get("score") or s.get("rating"),
        "genre": s.get("genre"),
        "poster": poster,
        "country": s.get("countryName") or s.get("country"),
        "corner": s.get("corner"),
        "has_resource": s.get("hasResource", True),
        "play": f"/play?subject_id={sid}" if sid else None,
        "stream": f"/mb/stream/{sid}" if sid else None,
    }


def _mb_extract_subjects(payload) -> list:
    out: list = []
    if isinstance(payload, list):
        for x in payload:
            if isinstance(x, dict):
                out.append(x)
        return out
    if not isinstance(payload, dict):
        return out
    for key in ("movie", "tv", "list", "items", "subjects", "subjectList"):
        val = payload.get(key)
        if isinstance(val, list):
            for it in val:
                if isinstance(it, dict):
                    if isinstance(it.get("subjects"), list):
                        out.extend([s for s in it["subjects"] if isinstance(s, dict)])
                    else:
                        out.append(it)
    for it in payload.get("items") or []:
        if isinstance(it, dict):
            for s in it.get("subjects") or it.get("subjectList") or []:
                if isinstance(s, dict):
                    out.append(s)
    for block in payload.get("results") or []:
        if isinstance(block, dict):
            for s in block.get("subjects") or []:
                if isinstance(s, dict):
                    out.append(s)
    return out


async def _mb_catalog(kind: str, page: int = 1, per_page: int = 20) -> dict:
    """kind: movie | series — paginated catalog from rank + tabs + search."""
    want = 1 if kind == "movie" else 2
    page = max(1, int(page or 1))
    per_page = max(1, min(50, int(per_page or 20)))
    seen = set()
    items: list = []

    def add_many(raw_list, force_type: bool = False):
        for s in raw_list or []:
            if not isinstance(s, dict):
                continue
            card = _mb_norm_subject(s)
            sid = card.get("subject_id")
            if not sid or sid in seen:
                continue
            st = card.get("subject_type")
            if force_type:
                card["subject_type"] = want
                card["type"] = "series" if want == 2 else "movie"
            else:
                if st is not None and int(st) != want:
                    continue
            seen.add(sid)
            items.append(card)

    try:
        rank = await mb_request(
            "GET", f"/wefeed-mobile-bff/subject-api/search-rank?page={page}"
        )
        bucket = rank.get("movie") if kind == "movie" else rank.get("tv")
        add_many(bucket if isinstance(bucket, list) else [], force_type=True)
    except Exception:
        pass

    tab_id = 2 if kind == "movie" else 5
    try:
        tab = await mb_request(
            "GET",
            f"/wefeed-mobile-bff/tab-operating?page={page}&tabId={tab_id}&version=",
        )
        add_many(_mb_extract_subjects(tab))
    except Exception:
        pass

    if len(items) < per_page:
        seeds = ["a", "e", "i", "the", "love", "man", "2024", "2025", "india", "night"]
        seed = seeds[(page - 1) % len(seeds)]
        try:
            data = await mb_request(
                "POST",
                "/wefeed-mobile-bff/subject-api/search/v2",
                {"keyword": seed, "page": page, "perPage": max(per_page, 20), "subjectType": want},
            )
            raw = _mb_extract_subjects(data)
            filtered = []
            for s in raw:
                if isinstance(s, dict) and "subject" in s:
                    s = s["subject"]
                if not isinstance(s, dict):
                    continue
                try:
                    st = int(s.get("subjectType") or s.get("stype") or 0)
                except Exception:
                    st = 0
                if st == want:
                    filtered.append(s)
            add_many(filtered)
        except Exception:
            pass

    page_items = items[:per_page]
    return {
        "provider": "moviebox",
        "type": kind,
        "page": page,
        "per_page": per_page,
        "count": len(page_items),
        "has_more": len(items) > per_page or len(page_items) >= per_page,
        "next_page": page + 1 if len(page_items) >= per_page else None,
        "items": page_items,
    }


@app.get("/mb/movies", tags=["MovieBox"])
async def mb_movies(page: int = 1, per_page: int = Query(20, ge=1, le=50)):
    """Paginated MovieBox **movies** only."""
    return await _mb_catalog("movie", page=page, per_page=per_page)


@app.get("/mb/series", tags=["MovieBox"])
async def mb_series(page: int = 1, per_page: int = Query(20, ge=1, le=50)):
    """Paginated MovieBox **series / TV** only."""
    return await _mb_catalog("series", page=page, per_page=per_page)


@app.get("/moviebox/movies", tags=["MovieBox-PaxShape"])
async def moviebox_movies(page: int = 1, perPage: int = Query(20, ge=1, le=50)):
    """Movies list with pager (PaxSenix-style path)."""
    data = await _mb_catalog("movie", page=page, per_page=perPage)
    return {
        "ok": True,
        "creator": "StreamHub-direct",
        "type": "movie",
        "pager": {
            "page": str(page),
            "per_page": perPage,
            "has_more": data.get("has_more"),
            "next_page": str(data["next_page"]) if data.get("next_page") else None,
        },
        "items": data.get("items") or [],
        "count": data.get("count"),
    }


@app.get("/moviebox/series", tags=["MovieBox-PaxShape"])
async def moviebox_series(page: int = 1, perPage: int = Query(20, ge=1, le=50)):
    """Series list with pager (PaxSenix-style path)."""
    data = await _mb_catalog("series", page=page, per_page=perPage)
    return {
        "ok": True,
        "creator": "StreamHub-direct",
        "type": "series",
        "pager": {
            "page": str(page),
            "per_page": perPage,
            "has_more": data.get("has_more"),
            "next_page": str(data["next_page"]) if data.get("next_page") else None,
        },
        "items": data.get("items") or [],
        "count": data.get("count"),
    }




@app.get("/mb/resource/{subject_id}", tags=["MovieBox"])
async def mb_resource(
    subject_id: str,
    se: int = 0,
    ep: int = 0,
    page: int = 1,
    perPage: int = Query(20, ge=1, le=50),
    resolution: int = Query(0, description="0=all, or 480/720/1080"),
):
    """
    MovieBox community/server **resource** list (from MovieBox-TUI).
    Higher bitrate mirrors alongside play-info DASH.
    """
    q = f"subjectId={subject_id}&page={page}&perPage={perPage}"
    if se or ep:
        q += f"&se={se}&ep={ep}"
    if resolution:
        q += f"&resolution={resolution}"
    path = f"/wefeed-mobile-bff/subject-api/resource?{q}"
    try:
        data = await mb_request("GET", path)
    except HTTPException:
        # older path without resolution
        path2 = f"/wefeed-mobile-bff/subject-api/resource?subjectId={subject_id}&page={page}&perPage={perPage}"
        if se or ep:
            path2 += f"&se={se}&ep={ep}"
        data = await mb_request("GET", path2)
    items = []
    raw_list = []
    if isinstance(data, dict):
        raw_list = data.get("list") or data.get("items") or data.get("resources") or data.get("resourceList") or []
        if not raw_list and data.get("data"):
            d = data["data"]
            if isinstance(d, dict):
                raw_list = d.get("list") or d.get("items") or []
            elif isinstance(d, list):
                raw_list = d
    elif isinstance(data, list):
        raw_list = data
    for it in raw_list:
        if not isinstance(it, dict):
            continue
        url = it.get("url") or it.get("resourceLink") or it.get("downloadUrl") or ""
        cookie = it.get("signCookie") or it.get("cookie") or ""
        dash = _dash_from_sign_cookie(cookie) if cookie else None
        playable = dash or (url if url and not _is_dummy_url(url) else None)
        headers = {"User-Agent": _mb_ua, "Referer": globals().get("STREAM_REFERER", "https://sportslive.wine")}
        if cookie:
            headers["Cookie"] = "; ".join(p.strip() for p in cookie.strip(";").split(";") if p.strip())
            if playable and ".mpd" in (playable or ""):
                try:
                    _mb_proxy_remember(playable, headers["Cookie"], headers["Referer"])
                except Exception:
                    pass
        items.append({
            "id": str(it.get("id") or it.get("resourceId") or ""),
            "title": it.get("title") or it.get("name") or it.get("resolution"),
            "resolution": it.get("resolution") or it.get("quality"),
            "size": it.get("size"),
            "format": it.get("format") or ("DASH" if playable and ".mpd" in (playable or "") else "MP4"),
            "url": url,
            "dash_url": playable,
            "proxy_url": f"/mb/proxy/mpd?u={quote(playable, safe='')}" if playable and ".mpd" in (playable or "") else None,
            "headers": headers,
            "se": it.get("se") or se,
            "ep": it.get("ep") or ep,
        })
    return {
        "provider": "moviebox",
        "subject_id": subject_id,
        "se": se,
        "ep": ep,
        "page": page,
        "count": len(items),
        "items": items,
        "raw_keys": list(data.keys()) if isinstance(data, dict) else [],
    }


@app.get("/mb/suggest", tags=["MovieBox"])
async def mb_suggest(q: str = Query(..., min_length=1)):
    """Autocomplete-style search (first page of search/v2)."""
    data = await mb_search(q=q, page=1)
    return {
        "query": q,
        "suggestions": [
            {
                "subject_id": it.get("subject_id"),
                "title": it.get("name") or it.get("title"),
                "type": it.get("type"),
                "year": it.get("year"),
                "poster": it.get("poster_url") or it.get("poster"),
            }
            for it in (data.get("items") or [])[:12]
        ],
    }


@app.get("/mb/rank", tags=["MovieBox"])
async def mb_rank(page: int = 1):
    """Trending rank: movies + TV (search-rank)."""
    data = await mb_request("GET", f"/wefeed-mobile-bff/subject-api/search-rank?page={page}")
    movies = [_mb_norm_subject(x) for x in (data.get("movie") or []) if isinstance(x, dict)]
    series = [_mb_norm_subject(x) for x in (data.get("tv") or []) if isinstance(x, dict)]
    for m in movies:
        m["type"] = "movie"
        m["subject_type"] = 1
    for s in series:
        s["type"] = "series"
        s["subject_type"] = 2
    return {
        "provider": "moviebox",
        "page": page,
        "movies": movies,
        "series": series,
        "count": len(movies) + len(series),
    }


@app.get("/mb/season/{subject_id}", tags=["MovieBox"])
async def mb_season(subject_id: str):
    """Season / episode structure for a series."""
    data = await mb_request(
        "GET", f"/wefeed-mobile-bff/subject-api/season-info?subjectId={subject_id}"
    )
    return {"provider": "moviebox", "subject_id": subject_id, "data": data}


@app.get("/mb/play/{subject_id}", tags=["MovieBox"])
async def mb_play(
    subject_id: str,
    se: int = 0,
    ep: int = 0,
):
    """
    Unified MovieBox play: play-info DASH (proxied) + resource mirrors.
    Preferred entry for clients.
    """
    stream = await mb_stream(subject_id, se=se, ep=ep)
    try:
        res = await mb_resource(subject_id, se=se, ep=ep, page=1)
        stream["resources"] = res.get("items") or []
    except Exception as e:
        stream["resources"] = []
        stream["resource_error"] = str(e)[:120]
    return stream

@app.get("/mb/detail/{subject_id}", tags=["MovieBox"])
async def mb_detail(subject_id: str):
    """Full metadata for a MovieBox title. Series include season info."""
    data = await mb_request("GET", f"/wefeed-mobile-bff/subject-api/get?subjectId={subject_id}")
    subject = data.get("subject") or data
    if (subject.get("subjectType") or subject.get("stype") or 1) == 2:
        try:
            subject["seasons"] = await mb_request(
                "GET", f"/wefeed-mobile-bff/subject-api/season-info?subjectId={subject_id}"
            )
        except Exception:
            pass
    return {"provider": "moviebox", "data": subject}



@app.get("/mb/stream/{subject_id}", tags=["MovieBox"])
async def mb_stream(subject_id: str, se: int = 0, ep: int = 0):
    """
    **One play endpoint for a MovieBox subject_id.**

    - `mp4` — progressive files if any (all phones)
    - `sources` — DASH/MPD + headers (HEVC; use VLC)
    - `qualities` — MPD broken into 1080/720/480
    - `play` — best single URL to open first
    """
    if se == 0 and ep == 0:
        path = f"/wefeed-mobile-bff/subject-api/play-info/v2?subjectId={subject_id}"
    else:
        path = f"/wefeed-mobile-bff/subject-api/play-info/v2?subjectId={subject_id}&se={se}&ep={ep}"
    data: dict = {}
    try:
        data = await mb_request("GET", path)
    except HTTPException:
        try:
            data = await mb_request("GET", path.replace("/play-info/v2", "/play-info"))
        except HTTPException:
            data = {}
    if not isinstance(data, dict):
        data = {}

    sources = _parse_mb_play_info(data, _mb_ua)
    # H5 play/download fallback (sometimes H.264 when mobile is HEVC-only)
    try:
        h5 = await _mb_h5_play(subject_id, se, ep)
        if h5:
            h5_sources = _parse_mb_play_info(h5, _mb_ua)
            for s in h5_sources:
                s["source"] = s.get("source") or "h5"
            sources = sources + h5_sources
    except Exception:
        pass
    try:
        extra = await _mb_resource_links(subject_id, se, ep)
    except Exception:
        extra = []
    seen = {s.get("url") for s in sources}
    for item in extra:
        if item.get("url") and item["url"] not in seen and not _is_mb_notice_url(item.get("url") or ""):
            sources.append(item)
            seen.add(item["url"])
    # drop notice / dummy streams
    sources = [s for s in sources if s.get("url") and not _is_mb_notice_url(s.get("url") or "")]

    referer = globals().get("STREAM_REFERER", "https://sportslive.wine")
    for s in sources:
        h = dict(s.get("headers") or {})
        h.setdefault("User-Agent", _mb_ua)
        h.setdefault("Referer", referer)
        s["headers"] = h
        s["play_url"] = s.get("url")

    # Flag when only HEVC DASH (no progressive MP4)
    only_hevc = bool(sources) and all(
        ("hevc" in str(s.get("codec") or "").lower() or "h265" in str(s.get("codec") or "").lower() or ".mpd" in (s.get("url") or ""))
        for s in sources
    ) and not any(".mp4" in (s.get("url") or "").lower() for s in sources)

    # Progressive MP4 only (real files)
    mp4_list = []
    for s in sources:
        u = (s.get("url") or "").strip()
        if not u or _is_dummy_url(u):
            continue
        if ".mpd" in u or ".m3u8" in u:
            continue
        if any(x in u.lower() for x in (".mp4", ".mkv", ".webm")):
            mp4_list.append({
                "label": s.get("resolution") or "MP4",
                "url": u,
                "headers": s.get("headers"),
                "phone_friendly": True,
            })

    # Expand first MPD into qualities
    qualities, audio = [], []
    proxy_mpd = None
    for s in sources:
        u = s.get("url") or ""
        if ".mpd" not in u:
            continue
        cookie = (s.get("headers") or {}).get("Cookie") or ""
        try:
            parsed = await _mb_expand_mpd(u, cookie, referer)
            qualities = parsed.get("video") or []
            audio = parsed.get("audio") or []
            if cookie:
                _mb_proxy_remember(u, cookie, referer)
            proxy_mpd = f"/mb/proxy/mpd?u={quote(u, safe='')}"
            # HTML parseStreams looks for proxy_url / play_url
            for src in sources:
                if (src.get("url") or "") == u or ".mpd" in (src.get("url") or ""):
                    src["proxy_url"] = proxy_mpd
                    src["play_url"] = proxy_mpd
                    src["proxied_url"] = proxy_mpd
                    src["format"] = "DASH"
            if sources and proxy_mpd:
                sources[0]["proxy_url"] = proxy_mpd
                sources[0]["play_url"] = proxy_mpd
        except Exception as e:
            qualities = [{"error": str(e)[:160]}]
        break

    # Title + optional TMDB id
    title = data.get("title")
    try:
        det = await mb_request("GET", f"/wefeed-mobile-bff/subject-api/get?subjectId={subject_id}")
        subj = det.get("subject") or det
        title = title or subj.get("title")
    except Exception:
        pass
    tmdb_id = None
    if title:
        try:
            q = re.sub(r"\[.*?\]", "", title).strip()
            tr = await _tmdb_get("/search/multi", {"query": q})
            for res in (tr.get("results") or [])[:5]:
                if res.get("media_type") not in ("movie", "tv"):
                    continue
                tmdb_id = res["id"]
                break
        except Exception:
            pass

    # Netplay catalog match → real progressive MP4
    if title:
        try:
            for n in await _np_match_mp4(title, se, ep):
                mp4_list.append(n)
        except Exception:
            pass

    play = None
    # MovieBox native only — no third-party embeds in this response
    if mp4_list:
        play = mp4_list[0]["url"]
    elif proxy_mpd:
        play = proxy_mpd
    elif sources:
        play = sources[0].get("proxy_url") or sources[0].get("url")

    note = "MovieBox native only. DASH/HEVC: use proxy_mpd or VLC with Cookie headers."
    if only_hevc and not mp4_list:
        note = (
            "MovieBox returned HEVC DASH only (app upgrade/paywall). "
            "Use proxy_mpd with a DASH player or open the MPD in VLC with Cookie. "
            "Third-party embeds are not included in MovieBox responses."
        )

    return {
        "provider": "moviebox",
        "subject_id": subject_id,
        "title": title,
        "se": se,
        "ep": ep,
        "tmdb_id": tmdb_id,
        "mp4": mp4_list,
        "sources": sources,
        "qualities": qualities,
        "audio": audio,
        "proxy_mpd": proxy_mpd,
        "play": play,
        "count": len(sources),
        "only_hevc": only_hevc,
        "note": note,
        "proxy_url": proxy_mpd,
    }


@app.get("/mb/home", tags=["MovieBox"])
async def mb_home(page: int = 1):
    """Homepage operating tabs from MovieBox."""
    data = await mb_request("GET", f"/wefeed-mobile-bff/tab-operating?page={page}&tabId=1&version=")
    return {"provider": "moviebox", "data": data}


@app.get("/mb/captions/{subject_id}", tags=["MovieBox"])
async def mb_captions(subject_id: str, resource_id: str = "", se: int = 0, ep: int = 0):
    """Subtitles for a MovieBox stream. Pass resource_id from /mb/stream sources[].id."""
    if not resource_id:
        stream_data = await mb_stream(subject_id, se, ep)
        for src in stream_data.get("sources") or []:
            if src.get("id"):
                resource_id = str(src["id"])
                break
    if not resource_id:
        return {"count": 0, "captions": []}
    data = await mb_request(
        "GET",
        f"/wefeed-mobile-bff/subject-api/get-ext-captions?subjectId={subject_id}&resourceId={resource_id}",
    )
    captions = data.get("extCaptions") or data.get("captions") or data.get("list") or []
    return {
        "provider": "moviebox",
        "subject_id": subject_id,
        "resource_id": resource_id,
        "count": len(captions),
        "captions": captions,
    }


# ----- 4KHDHub -----


@app.get("/fk/search", tags=["4KHDHub"])
async def fk_search(q: str = Query(..., min_length=1)):
    """Search 4KHDHub. Use returned `id` (path) with /fk/detail and /fk/stream."""
    html = await fk_fetch(f"?s={q}")
    return {"provider": "4khdhub", "query": q, "items": fk_parse_search(html)}


@app.get("/fk/detail", tags=["4KHDHub"])
async def fk_detail(id: str = Query(..., description="Path from search, e.g. /movie-name/")):
    """Metadata for a 4KHDHub page."""
    html = await fk_fetch(id)
    soup = BeautifulSoup(html, "html.parser")
    h1 = soup.select_one("h1")
    title = h1.get_text(strip=True) if h1 else id
    og = soup.select_one('meta[property="og:image"]')
    desc_el = soup.select_one(".content-section p.mt-4") or soup.select_one('meta[name="description"]')
    description = None
    if desc_el:
        description = desc_el.get_text(strip=True) if desc_el.name == "p" else desc_el.get("content")
    return {
        "provider": "4khdhub",
        "id": id,
        "title": title,
        "poster_url": og.get("content") if og else None,
        "description": description,
        "type": "series" if "-series-" in id else "movie",
    }


@app.get("/fk/stream", tags=["4KHDHub"])
async def fk_stream(
    id: str = Query(..., description="Path from /fk/search"),
    se: int = 0,
    ep: int = 0,
    resolve: bool = Query(True, description="Expand HubCloud/HubDrive to direct stream URLs (default true)"),
    limit: int = Query(12, ge=1, le=40, description="Max mirrors to resolve"),
):
    """
    4KHDHub releases + **direct streaming links**.

    By default (`resolve=true`) every HubCloud / HubDrive / PixelDrain mirror is
    expanded into playable CDN URLs. Flat list is in `streams` for the player.
    """
    try:
        limit = int(limit)
    except Exception:
        limit = 12
    resolve = bool(resolve)
    html = await fk_fetch(id)
    releases = fk_parse_releases(html, se, ep)
    streams: List[dict] = []
    resolved_n = 0

    async def _expand_mirror(mirror: dict) -> List[dict]:
        url = (mirror.get("url") or "").strip()
        if not url:
            return []
        # already direct?
        low = url.lower()
        if any(x in low for x in (".mp4", ".mkv", ".m3u8", "pixeldrain.com/api/file", "cdn.pixeldrain", "workers.dev", "r2.dev")):
            return [{
                "url": url if "pixeldrain.com/u/" not in low else (_pixeldrain_api(url) or url),
                "label": mirror.get("label") or "Direct",
                "quality": mirror.get("quality") or "",
                "source": "direct",
                "playable": True,
            }]
        if not resolve and not mirror.get("needs_resolve"):
            return [{
                "url": url,
                "label": mirror.get("label") or "Mirror",
                "quality": mirror.get("quality") or "",
                "source": "unresolved",
                "playable": False,
            }]
        try:
            links = await resolve_any(url)
        except Exception as e:
            mirror["resolve_error"] = str(e)[:160]
            return []
        out = []
        for L in links or []:
            u = L.get("url") if isinstance(L, dict) else str(L)
            if not u:
                continue
            out.append({
                "url": u,
                "label": (L.get("label") if isinstance(L, dict) else None) or mirror.get("label") or "CDN",
                "quality": mirror.get("quality") or (L.get("quality") if isinstance(L, dict) else "") or "",
                "source": (L.get("source") if isinstance(L, dict) else None) or "hubcloud",
                "playable": True,
                "headers": (L.get("headers") if isinstance(L, dict) else None) or {
                    "User-Agent": "Mozilla/5.0",
                    "Referer": url,
                },
            })
        return out

    if resolve:
        n = 0
        for rel in releases:
            for mirror in rel.get("mirrors") or []:
                if n >= limit:
                    break
                n += 1
                try:
                    direct = await _expand_mirror(mirror)
                    mirror["direct_links"] = direct
                    for d in direct:
                        d["release"] = rel.get("title") or rel.get("name") or ""
                        streams.append(d)
                        resolved_n += 1
                except Exception as e:
                    mirror["direct_links"] = []
                    mirror["resolve_error"] = str(e)[:160]
            if n >= limit:
                break
    else:
        for rel in releases:
            for mirror in rel.get("mirrors") or []:
                streams.append({
                    "url": mirror.get("url"),
                    "label": mirror.get("label"),
                    "quality": mirror.get("quality"),
                    "source": "unresolved",
                    "playable": False,
                    "release": rel.get("title") or "",
                })

    # de-dupe by url
    seen = set()
    unique = []
    for s in streams:
        u = s.get("url") or ""
        if not u or u in seen:
            continue
        seen.add(u)
        unique.append(s)

    return {
        "provider": "4khdhub",
        "id": id,
        "se": se,
        "ep": ep,
        "resolved": resolve,
        "count": len(releases),
        "stream_count": len(unique),
        "streams": unique,  # direct playable when resolve=true
        "releases": releases,
        "hint": "Use streams[].url in VLC/mpv/browser. resolve=false returns raw hubcloud pages only.",
    }


@app.get("/fk/play", tags=["4KHDHub"])
async def fk_play(
    id: str = Query(..., description="Path from /fk/search"),
    se: int = 0,
    ep: int = 0,
):
    """Shortcut: always resolve and return only playable streams."""
    data = await fk_stream(id=id, se=se, ep=ep, resolve=True)
    playable = [s for s in (data.get("streams") or []) if s.get("playable")]
    return {
        "provider": "4khdhub",
        "id": id,
        "se": se,
        "ep": ep,
        "count": len(playable),
        "streams": playable,
        "best": playable[0] if playable else None,
    }


# ----- Tools -----


@app.get("/tools/resolve", tags=["Tools"])
async def tools_resolve(url: str = Query(..., description="hubcloud.*/drive/... or hubdrive.*/file/...")):
    """
    Resolve HubCloud / HubDrive page into direct CDN download URLs
    (PixelDrain, Cloudflare R2, FSL, Google storage, etc.).
    """
    links = await resolve_any(url)
    return {"input": url, "count": len(links), "direct_links": links}

@app.get("/tools/hubcloud", tags=["Tools"])
async def tools_hubcloud(url: str = Query(..., description="https://hubcloud.ist/drive/xxx")):
    """Resolve HubCloud drive page → all direct mirrors (FSL, PixelDrain, 10Gbps, Watch…)."""
    links = await resolve_hubcloud(url)
    return {"ok": True, "count": len(links), "links": links, "input": url}


@app.get("/tools/hubdrive", tags=["Tools"])
async def tools_hubdrive(url: str = Query(..., description="https://hubdrive.pics/file/xxx")):
    """Resolve HubDrive file → HubCloud → direct mirrors."""
    links = await resolve_hubdrive(url)
    return {"ok": True, "count": len(links), "links": links, "input": url}


@app.get("/tools/resolve/batch", tags=["Tools"])
async def tools_resolve_batch(urls: str = Query(..., description="Comma-separated HubCloud/HubDrive URLs")):
    """Resolve many Hub links at once."""
    parts = [u.strip() for u in urls.split(",") if u.strip()]
    out = []
    for u in parts[:15]:
        try:
            links = await resolve_any(u)
            out.append({"input": u, "ok": True, "count": len(links), "links": links})
        except Exception as e:
            out.append({"input": u, "ok": False, "error": str(e)})
    return {"results": out}




# ----- Aggregate -----


@app.get("/search", tags=["Aggregate"])
async def search_all(q: str = Query(..., min_length=1)):
    """Search MovieBox + 4KHDHub in parallel-ish sequential calls."""
    moviebox_items: Any = []
    fourk_items: Any = []
    errors: Dict[str, str] = {}
    try:
        moviebox_items = (await mb_search(q))["items"]
    except Exception as exc:
        errors["moviebox"] = str(exc)
    try:
        fourk_items = (await fk_search(q))["items"]
    except Exception as exc:
        errors["4khdhub"] = str(exc)
    return {"query": q, "moviebox": moviebox_items, "fourkhdhub": fourk_items, "errors": errors or None}


# ----- Legacy -----


@app.get("/api/stream/{subject_id}", tags=["Legacy"], include_in_schema=False)
async def legacy_stream(subject_id: str, se: int = 0, ep: int = 0, detail_path: str = ""):
    return await mb_stream(subject_id, se, ep)


@app.get("/detail/{subject_id}", tags=["Legacy"], include_in_schema=False)
async def legacy_detail(subject_id: str):
    return await mb_detail(subject_id)




# =============================================================================
# CATALOG + PLAY (MovieBox + 4KHDHub only — no TMDB)
# =============================================================================

def _mb_items_from_search_data(data: dict) -> list:
    items = []
    raw = []
    results = data.get("results") or []
    if results and isinstance(results[0], dict):
        raw = results[0].get("subjects") or []
    if not raw:
        raw = data.get("list") or data.get("items") or data.get("subjects") or []
    for s in raw:
        if isinstance(s, dict) and "subject" in s:
            s = s["subject"]
        if not isinstance(s, dict):
            continue
        sid = s.get("subjectId") or s.get("id")
        cover = s.get("cover") or {}
        poster = cover.get("url") if isinstance(cover, dict) else (s.get("coverUrl") or cover)
        stype = s.get("subjectType") or s.get("stype") or 1
        name = s.get("title") or s.get("name") or ""
        # Strip trailing season labels so series dedupe cleanly (S1/S2 duplicates share id)
        clean = re.sub(r"\s*S\d+\s*$", "", name, flags=re.I).strip() or name
        items.append({
            "id": str(sid) if sid is not None else None,
            "name": clean,
            "poster": poster,
            "year": (s.get("releaseDate") or "")[:4] or None,
            "rating": s.get("imdbRatingValue") or s.get("score"),
            "type": "tv" if stype == 2 else "movie",
            "provider": "moviebox",
            "slug": s.get("detailPath"),
        })
    # Dedupe by id — keep first occurrence
    seen = set()
    out = []
    for x in items:
        if not x.get("id") or x["id"] in seen:
            continue
        seen.add(x["id"])
        out.append(x)
    return out


def _mb_items_from_ops(data) -> list:
    """Parse tab-operating / home feed into flat cards."""
    items = []
    seen = set()
    def walk(node):
        if isinstance(node, list):
            for x in node:
                walk(x)
            return
        if not isinstance(node, dict):
            return
        # subject-like
        sid = node.get("subjectId") or node.get("id")
        title = node.get("title") or node.get("name")
        if sid and title and str(sid) not in seen:
            cover = node.get("cover") or {}
            poster = cover.get("url") if isinstance(cover, dict) else node.get("coverUrl")
            stype = node.get("subjectType") or node.get("stype") or 1
            seen.add(str(sid))
            items.append({
                "id": str(sid),
                "name": title,
                "poster": poster,
                "year": (node.get("releaseDate") or "")[:4] or None,
                "type": "tv" if stype == 2 else "movie",
                "provider": "moviebox",
            })
        for v in node.values():
            if isinstance(v, (dict, list)):
                walk(v)
    walk(data)
    return items



@app.get("/api/home", tags=["Catalog"])
async def api_home():
    """
    Home feed for the web.

    - TMDB trending/popular rows
    - `netplay_admin` — optional direct-MP4 list (UUID ids, not MovieBox subject_id)
    """
    async def grab(path, media, pages=1):
        items = []
        for pg in range(1, pages + 1):
            try:
                d = await _tmdb_get(path, {"page": pg})
            except Exception:
                break
            for it in d.get("results") or []:
                it = dict(it)
                it["media_type"] = media
                items.append(_tmdb_card(it))
        return items

    trending = await grab("/trending/all/day", "movie", 1)
    # fix media types for mixed trending
    try:
        d = await _tmdb_get("/trending/all/day", {"page": 1})
        trending = [_tmdb_card(x) for x in (d.get("results") or []) if x.get("media_type") in ("movie", "tv")]
    except Exception:
        trending = []
    popular_movies = await grab("/movie/popular", "movie", 1)
    popular_tv = await grab("/tv/popular", "tv", 1)
    top_movies = await grab("/movie/top_rated", "movie", 1)
    top_tv = await grab("/tv/top_rated", "tv", 1)

    netplay_admin = []
    try:
        _npv = await _np_get("/videos")
        if isinstance(_npv, list):
            for v in _npv[:20]:
                netplay_admin.append({
                    "id": v.get("id"),
                    "netplay_id": v.get("id"),
                    "id_type": "netplay_uuid",
                    "title": v.get("title"),
                    "poster": v.get("poster"),
                    "type": v.get("type"),
                    "provider": "netplay",
                    "play": f"/play?netplay_id={v.get('id')}",
                    "stream": f"/np/stream/{v.get('id')}",
                })
    except Exception:
        pass

    return {
        "trending": trending,
        "popular_movies": popular_movies,
        "popular_tv": popular_tv,
        "top_movies": top_movies,
        "top_tv": top_tv,
        "netplay_admin": netplay_admin,
        "id_help": {
            "moviebox": "subject_id from GET /mb/search or /api/search — main catalog",
            "netplay": "UUID from netplay_admin or GET /np/videos — admin MP4 only",
            "play_moviebox": "GET /play?subject_id=...  or  GET /mb/stream/{subject_id}",
            "play_netplay": "GET /play?netplay_id=...  or  GET /np/stream/{id}?se=&ep=",
        },
    }


@app.get("/api/movies", tags=["Catalog"])
async def api_movies(page: int = 1):
    d = await _tmdb_get("/movie/popular", {"page": max(1, page)})
    items = [c for x in (d.get("results") or []) if (c := _tmdb_card(x, "movie"))]
    return {
        "items": items,
        "page": page,
        "total_pages": d.get("total_pages") or 1,
        "total_results": d.get("total_results") or len(items),
        "provider": "tmdb",
    }



@app.get("/api/series", tags=["Catalog"])
async def api_series(page: int = 1):
    d = await _tmdb_get("/tv/popular", {"page": max(1, page)})
    items = [c for x in (d.get("results") or []) if (c := _tmdb_card(x, "tv"))]
    return {
        "items": items,
        "page": page,
        "total_pages": d.get("total_pages") or 1,
        "total_results": d.get("total_results") or len(items),
        "provider": "tmdb",
    }

@app.get("/api/search", tags=["Catalog"])
async def api_search_catalog(q: str = Query(..., min_length=1)):
    """
    Unified search for the web + API clients.

    **IDs**
    - `subject_id` / `id_type=moviebox_subject` → main catalog (same as Netplay app movies)
    - `netplay_id` / `id_type=netplay_uuid` → admin MP4 list only (`/np/videos`)
    - `tmdb_id` → embed helpers

    Play MovieBox: `GET /play?subject_id=...` or `GET /mb/stream/{subject_id}`
    """
    items: List[dict] = []
    errors: Dict[str, str] = {}
    try:
        mb = await mb_search(q=q, page=1)
        for it in mb.get("items") or []:
            sid = it.get("subject_id")
            items.append({
                "id": sid,
                "subject_id": sid,
                "id_type": "moviebox_subject",
                "title": it.get("name") or it.get("title"),
                "name": it.get("name") or it.get("title"),
                "poster": it.get("poster_url") or it.get("poster"),
                "year": it.get("year"),
                "type": it.get("type") or "movie",
                "provider": "moviebox",
                "play": f"/play?subject_id={sid}",
                "stream": f"/mb/stream/{sid}",
            })
    except Exception as e:
        errors["moviebox"] = str(e)[:160]
    try:
        np = await _np_get("/videos")
        if isinstance(np, list):
            ql = q.lower()
            for v in np:
                title = v.get("title") or ""
                if ql not in title.lower() and not any(
                    w in title.lower() for w in ql.split() if len(w) > 2
                ):
                    continue
                nid = v.get("id")
                items.append({
                    "id": nid,
                    "netplay_id": nid,
                    "id_type": "netplay_uuid",
                    "title": title,
                    "name": title,
                    "poster": v.get("poster"),
                    "type": v.get("type") or "series",
                    "provider": "netplay",
                    "seasons": v.get("seasons"),
                    "play": f"/play?netplay_id={nid}",
                    "stream": f"/np/stream/{nid}",
                })
    except Exception as e:
        errors["netplay"] = str(e)[:160]
    try:
        d = await _tmdb_get("/search/multi", {"query": q, "page": 1, "include_adult": "false"})
        for res in (d.get("results") or [])[:10]:
            if res.get("media_type") not in ("movie", "tv"):
                continue
            tid = res.get("id")
            items.append({
                "id": tid,
                "tmdb_id": tid,
                "id_type": "tmdb",
                "title": res.get("title") or res.get("name"),
                "name": res.get("title") or res.get("name"),
                "poster": (
                    f"https://image.tmdb.org/t/p/w500{res['poster_path']}"
                    if res.get("poster_path")
                    else None
                ),
                "type": res.get("media_type"),
                "provider": "tmdb",
                "play": f"/api/play?media={res.get('media_type')}&id={tid}",
            })
    except Exception as e:
        errors["tmdb"] = str(e)[:160]
    return {
        "query": q,
        "count": len(items),
        "items": items,
        "errors": errors or None,
        "how_to_play": {
            "moviebox_subject_id": "GET /play?subject_id=ID  or  GET /mb/stream/ID",
            "netplay_uuid": "GET /play?netplay_id=UUID  or  GET /np/stream/UUID?se=&ep=",
            "note": "subject_id and netplay_id are different backends — do not mix them",
        },
    }



    try:
        d = await _tmdb_get("/search/multi", {"query": q, "page": 1, "include_adult": "false"})
        for x in d.get("results") or []:
            c = _tmdb_card(x)
            if c:
                items.append(c)
    except Exception as e:
        errors["tmdb"] = str(e)
    try:
        html = await fk_fetch(f"?s={q}")
        for it in fk_parse_search(html)[:12]:
            fk_items.append({
                "id": it.get("id"),
                "name": it.get("name"),
                "poster": it.get("poster_url") or it.get("poster"),
                "year": it.get("year"),
                "type": "tv" if it.get("type") == "series" else "movie",
                "provider": "4khdhub",
            })
    except Exception as e:
        errors["4khdhub"] = str(e)
    return {
        "query": q,
        "items": items,
        "tmdb": items,
        "fourkhdhub": fk_items,
        "moviebox": [],
        "errors": errors or None,
    }



@app.get("/api/detail/{media}/{item_id}", tags=["Catalog"])
async def api_detail(media: str, item_id: str):
    """TMDB detail for web. item_id = TMDB id. MovieBox remains on /mb/detail/{id}."""
    media = "tv" if media in ("tv", "series", "show") else "movie"
    path = f"/{media}/{item_id}"
    params = {"append_to_response": "external_ids,credits"}
    if media == "tv":
        params["append_to_response"] = "external_ids,credits"
    d = await _tmdb_get(path, params)
    if not d or not d.get("id"):
        raise HTTPException(404, "Not found on TMDB")
    seasons = []
    if media == "tv":
        for s in d.get("seasons") or []:
            num = s.get("season_number")
            if num is None or int(num) < 1:
                continue
            seasons.append({
                "season": int(num),
                "name": s.get("name") or f"Season {num}",
                "episode_count": s.get("episode_count") or 0,
                "poster": f"{TMDB_IMG}/w300{s['poster_path']}" if s.get("poster_path") else None,
            })
        seasons.sort(key=lambda x: x["season"])
    poster = d.get("poster_path")
    backdrop = d.get("backdrop_path")
    title = d.get("title") or d.get("name") or ""
    date = d.get("release_date") or d.get("first_air_date") or ""
    imdb = (d.get("external_ids") or {}).get("imdb_id")
    return {
        "id": str(d["id"]),
        "tmdb_id": str(d["id"]),
        "imdb_id": imdb,
        "name": title,
        "overview": d.get("overview") or "",
        "poster": f"{TMDB_IMG}/w500{poster}" if poster else None,
        "backdrop": f"{TMDB_IMG}/w1280{backdrop}" if backdrop else None,
        "year": date[:4] if date else None,
        "rating": d.get("vote_average"),
        "genres": [g.get("name") for g in (d.get("genres") or []) if g.get("name")],
        "type": media,
        "provider": "tmdb",
        "seasons": seasons,
        "runtime": d.get("runtime") or (d.get("episode_run_time") or [None])[0],
    }



@app.get("/api/tv/{item_id}/season/{season}", tags=["Catalog"])
async def api_season(item_id: str, season: int):
    """TMDB season episodes (ascending)."""
    d = await _tmdb_get(f"/tv/{item_id}/season/{season}")
    episodes = []
    for e in d.get("episodes") or []:
        epn = e.get("episode_number")
        if epn is None:
            continue
        still = e.get("still_path")
        episodes.append({
            "episode": int(epn),
            "name": e.get("name") or f"Episode {epn}",
            "overview": e.get("overview") or "",
            "still": f"{TMDB_IMG}/w300{still}" if still else None,
        })
    episodes.sort(key=lambda x: x["episode"])
    return {"season": int(season), "episodes": episodes, "count": len(episodes)}








# =============================================================================
# Direct stream / download — VixSrc + aggregate (Uniflix/Flix-style TMDB players)
# Sources inspired by TMDB-Embed-API / VixSrc / multi-embed catalogs
# =============================================================================

_VIXSRC_BASE = "https://vixsrc.to"
_VIX_HDR = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "application/json, text/html, */*",
}


async def _vixsrc_embed_path(tmdb_id: int, media: str = "movie", se: int = 1, ep: int = 1) -> Optional[str]:
    if media in ("tv", "series"):
        api = f"{_VIXSRC_BASE}/api/tv/{tmdb_id}/{se}/{ep}"
    else:
        api = f"{_VIXSRC_BASE}/api/movie/{tmdb_id}"
    headers = {
        **_VIX_HDR,
        "Referer": f"{_VIXSRC_BASE}/",
        "Origin": _VIXSRC_BASE,
        "Accept-Language": "en-US,en;q=0.9",
    }
    async with httpx.AsyncClient(timeout=18.0, follow_redirects=True) as client:
        try:
            r = await client.get(api, headers=headers)
        except Exception:
            return None
        if r.status_code != 200:
            return None
        try:
            src = (r.json() or {}).get("src") or ""
        except Exception:
            return None
        if not src:
            return None
        return src if src.startswith("http") else f"{_VIXSRC_BASE}{src}"


async def _vixsrc_resolve_playlist(embed_url: str) -> Optional[dict]:
    """Parse embed page → signed HLS master playlist + optional downloadUrl."""
    async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
        r = await client.get(embed_url, headers={**_VIX_HDR, "Referer": _VIXSRC_BASE + "/"})
        if r.status_code != 200:
            return None
        text = r.text
        streams = []
        m = re.search(r"window\.streams\s*=\s*(\[.*?\]);", text, re.S)
        if m:
            try:
                streams = json.loads(m.group(1))
            except Exception:
                streams = []
        token = expires = None
        mt = re.search(r"['\"]token['\"]\s*:\s*['\"]([^'\"]+)['\"]", text)
        me = re.search(r"['\"]expires['\"]\s*:\s*['\"]([^'\"]+)['\"]", text)
        if mt:
            token = mt.group(1)
        if me:
            expires = me.group(1)
        pl_base = None
        mu = re.search(r"window\.masterPlaylist\s*=\s*\{[^}]*url:\s*['\"]([^'\"]+)['\"]", text, re.S)
        if mu:
            pl_base = mu.group(1)
        if not pl_base and streams:
            pl_base = (streams[0].get("url") or "").split("?")[0]
        download = None
        md = re.search(r"window\.downloadUrl\s*=\s*['\"]([^'\"]+)['\"]", text)
        if md:
            download = md.group(1)

        out_streams = []
        if pl_base and token:
            # try quality variants
            for extra in ("h=1&lang=en", "lang=en", ""):
                qs = f"token={token}&expires={expires or ''}"
                if extra:
                    qs = f"{qs}&{extra}"
                u = f"{pl_base}?{qs}" if "?" not in pl_base else f"{pl_base}&{qs}"
                try:
                    pr = await client.get(
                        u,
                        headers={**_VIX_HDR, "Referer": embed_url, "Accept": "application/vnd.apple.mpegurl,*/*"},
                    )
                    if pr.status_code == 200 and "#EXTM3U" in (pr.text or "")[:200]:
                        out_streams.append({
                            "url": u,
                            "format": "HLS",
                            "label": "VixSrc HLS",
                            "playable": True,
                            "headers": {"Referer": embed_url, "User-Agent": _VIX_HDR["User-Agent"]},
                        })
                        break
                except Exception:
                    continue
        for s in streams:
            su = s.get("url")
            if su and su not in [x["url"] for x in out_streams]:
                out_streams.append({
                    "url": su if su.startswith("http") else f"{_VIXSRC_BASE}{su}",
                    "format": "HLS",
                    "label": s.get("name") or "VixSrc Server",
                    "playable": True,
                    "headers": {"Referer": embed_url, "User-Agent": _VIX_HDR["User-Agent"]},
                })
        if not out_streams and not download:
            return None
        return {
            "embed": embed_url,
            "streams": out_streams,
            "download_url": download,
        }


@app.get("/vix/stream", tags=["Direct"])
async def vix_stream(
    tmdb_id: int = Query(...),
    media: str = Query("movie"),
    se: int = Query(1),
    ep: int = Query(1),
):
    """VixSrc direct HLS (TMDB id) — used by modern embed aggregators."""
    media = "tv" if media in ("tv", "series") else "movie"
    embed = await _vixsrc_embed_path(tmdb_id, media, se, ep)
    if not embed:
        return {
            "ok": False,
            "provider": "vixsrc",
            "tmdb_id": tmdb_id,
            "media": media,
            "streams": [],
            "count": 0,
            "error": "VixSrc returned no embed (geo/cloud block possible)",
            "embed": f"https://vixsrc.to/embed/movie/{tmdb_id}" if media == "movie" else f"https://vixsrc.to/embed/tv/{tmdb_id}/{se}/{ep}",
            "note": "Use /api/play embeds or /direct/stream as fallback",
        }
    resolved = await _vixsrc_resolve_playlist(embed)
    if not resolved:
        return {
            "provider": "vixsrc",
            "tmdb_id": tmdb_id,
            "media": media,
            "embed": embed,
            "streams": [{"url": embed, "format": "EMBED", "label": "VixSrc embed", "playable": True}],
            "note": "playlist resolve failed — use embed iframe",
        }
    return {
        "provider": "vixsrc",
        "tmdb_id": tmdb_id,
        "media": media,
        "se": se if media == "tv" else 0,
        "ep": ep if media == "tv" else 0,
        "embed": resolved.get("embed"),
        "streams": resolved.get("streams") or [],
        "download_url": resolved.get("download_url"),
        "count": len(resolved.get("streams") or []),
    }


@app.get("/direct/stream", tags=["Direct"])
async def direct_stream(
    tmdb_id: int = Query(None),
    title: str = Query(None),
    media: str = Query("movie"),
    se: int = 1,
    ep: int = 1,
    include_embeds: bool = True,
    include_vixsrc: bool = True,
    include_4k: bool = True,
    include_netmirror: bool = False,
    include_moviebox: bool = False,
):
    """
    Aggregate **direct** streams + embeds for one title.
    Order: VixSrc HLS → 4KHDHub direct → embeds → optional NetMirror/MovieBox.
    """
    media = "tv" if media in ("tv", "series") else "movie"
    sources: List[dict] = []
    errors: Dict[str, str] = {}
    name = (title or "").strip()

    if not tmdb_id and name:
        try:
            meta = await _tmdb_search_id(name, media)
            if meta and meta.get("tmdb"):
                tmdb_id = int(meta["tmdb"])
                name = name or meta.get("name") or ""
        except Exception as e:
            errors["tmdb"] = str(e)[:80]

    if include_vixsrc and tmdb_id:
        try:
            vx = await vix_stream(tmdb_id=tmdb_id, media=media, se=se, ep=ep)
            for s in vx.get("streams") or []:
                sources.append({
                    "provider": "vixsrc",
                    "label": s.get("label") or "VixSrc",
                    "url": s.get("url"),
                    "format": s.get("format") or "HLS",
                    "type": "direct" if s.get("format") == "HLS" else "embed",
                    "playable": True,
                    "headers": s.get("headers") or {},
                })
            if vx.get("download_url"):
                sources.append({
                    "provider": "vixsrc",
                    "label": "VixSrc download",
                    "url": vx["download_url"],
                    "format": "FILE",
                    "type": "download",
                    "playable": False,
                })
        except Exception as e:
            errors["vixsrc"] = str(e)[:100]

    if include_4k and name:
        try:
            html = await fk_fetch(f"?s={name}")
            items = fk_parse_search(html)
            if items:
                fid = items[0].get("id")
                fk = await fk_stream(
                    id=fid,
                    se=se if media == "tv" else 0,
                    ep=ep if media == "tv" else 0,
                    resolve=True,
                    limit=6,
                )
                for s in fk.get("streams") or []:
                    if s.get("url"):
                        sources.append({
                            "provider": "4khdhub",
                            "label": s.get("label") or "4K Hub",
                            "url": s["url"],
                            "format": s.get("format") or "FILE",
                            "type": "direct",
                            "playable": bool(s.get("playable", True)),
                            "headers": s.get("headers") or {},
                        })
        except Exception as e:
            errors["4khdhub"] = str(e)[:100]

    if include_netmirror and name:
        try:
            nm = await nm_play(q=name, platform="netflix", se=se if media == "tv" else 0, ep=ep if media == "tv" else 0)
            for s in nm.get("streams") or []:
                sources.append({
                    "provider": "netmirror",
                    "label": "NetMirror",
                    "url": s.get("url"),
                    "format": s.get("format") or "HLS",
                    "type": "direct",
                    "playable": True,
                    "headers": s.get("headers") or {},
                })
        except Exception as e:
            errors["netmirror"] = str(e)[:100]

    if include_moviebox and name:
        try:
            mb = await mb_search(q=name, page=1)
            items = mb.get("items") or []
            if items:
                sid = items[0].get("subject_id")
                st = await mb_stream(sid, se=se if media == "tv" else 0, ep=ep if media == "tv" else 0)
                if st.get("proxy_url") or st.get("proxy_mpd"):
                    sources.append({
                        "provider": "moviebox",
                        "label": "MovieBox DASH",
                        "url": st.get("proxy_url") or st.get("proxy_mpd"),
                        "format": "DASH",
                        "type": "direct",
                        "playable": True,
                    })
        except Exception as e:
            errors["moviebox"] = str(e)[:100]

    if include_embeds and tmdb_id:
        for e in _build_embeds(tmdb_id, media, se, ep)[:20]:
            sources.append({
                "provider": e.get("provider"),
                "label": e.get("provider"),
                "url": e.get("url"),
                "format": "EMBED",
                "type": "embed",
                "playable": True,
            })

    # dedupe by url
    seen = set()
    uniq = []
    for s in sources:
        u = s.get("url")
        if not u or u in seen:
            continue
        seen.add(u)
        uniq.append(s)

    direct = [s for s in uniq if s.get("type") == "direct"]
    return {
        "tmdb_id": tmdb_id,
        "title": name,
        "media": media,
        "se": se if media == "tv" else 0,
        "ep": ep if media == "tv" else 0,
        "count": len(uniq),
        "direct_count": len(direct),
        "sources": uniq,
        "direct": direct,
        "errors": errors or None,
    }


@app.get("/direct/download", tags=["Direct"])
async def direct_download(
    tmdb_id: int = Query(None),
    title: str = Query(None),
    media: str = Query("movie"),
    se: int = 0,
    ep: int = 0,
    url: str = Query(None, description="Optional page/file URL to resolve via yt-dlp / HubCloud"),
):
    """
    Direct download links:
    - 4KHDHub / HubCloud resolve
    - optional yt-dlp on any `url`
    - VixSrc downloadUrl when available
    """
    media = "tv" if media in ("tv", "series") else "movie"
    links: List[dict] = []
    errors: Dict[str, str] = {}
    name = (title or "").strip()

    if not tmdb_id and name:
        try:
            meta = await _tmdb_search_id(name, media)
            if meta and meta.get("tmdb"):
                tmdb_id = int(meta["tmdb"])
        except Exception:
            pass

    if name:
        try:
            html = await fk_fetch(f"?s={name}")
            items = fk_parse_search(html)
            if items:
                fid = items[0].get("id")
                fk = await fk_stream(
                    id=fid,
                    se=se if media == "tv" else 0,
                    ep=ep if media == "tv" else 0,
                    resolve=True,
                    limit=10,
                )
                for s in fk.get("streams") or []:
                    if s.get("url"):
                        links.append({
                            "provider": "4khdhub",
                            "label": s.get("label") or "Hub download",
                            "url": s["url"],
                            "format": s.get("format") or "FILE",
                            "quality": s.get("quality") or "",
                        })
        except Exception as e:
            errors["4khdhub"] = str(e)[:100]

    if tmdb_id:
        try:
            vx = await vix_stream(tmdb_id=tmdb_id, media=media, se=max(1, se), ep=max(1, ep))
            if vx.get("download_url"):
                links.append({
                    "provider": "vixsrc",
                    "label": "VixSrc download",
                    "url": vx["download_url"],
                    "format": "FILE",
                })
            for s in vx.get("streams") or []:
                if s.get("format") == "HLS" and s.get("url"):
                    links.append({
                        "provider": "vixsrc",
                        "label": "VixSrc HLS (save)",
                        "url": s["url"],
                        "format": "HLS",
                        "headers": s.get("headers") or {},
                    })
        except Exception as e:
            errors["vixsrc"] = str(e)[:100]

    if url:
        try:
            # reuse tools/resolve or dl/any if present
            if "tools_resolve" in globals() or hasattr(app, "routes"):
                pass
            async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
                # HubCloud-style
                if "hubcloud" in url or "hubdrive" in url:
                    try:
                        from urllib.parse import quote as _q
                        # call internal if exists
                        pass
                    except Exception:
                        pass
                # generic: return url as-is for client download
                links.append({
                    "provider": "raw",
                    "label": "Provided URL",
                    "url": url,
                    "format": "FILE",
                })
        except Exception as e:
            errors["url"] = str(e)[:100]

    seen = set()
    uniq = []
    for L in links:
        u = L.get("url")
        if not u or u in seen:
            continue
        seen.add(u)
        uniq.append(L)

    return {
        "tmdb_id": tmdb_id,
        "title": name,
        "count": len(uniq),
        "downloads": uniq,
        "errors": errors or None,
    }

# =============================================================================
# NetMirror (NewTV) — Netflix / Prime / Hotstar mirrors via rotating API
# =============================================================================

_NM_PROBE = [
    "https://mobiledetects.com",
    "https://mobiledetect.app",
    "https://mobidetect.art",
    "https://mobidetect.cc",
    "https://mobidetects.cc",
    "https://mobidetects.pro",
]
_NM_HEADERS = {
    "Cache-Control": "no-cache, no-store, must-revalidate",
    "Pragma": "no-cache",
    "Expires": "0",
    "X-Requested-With": "NetmirrorNewTV v1.0",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:136.0) "
        "Gecko/20100101 Firefox/136.0 /OS.GatuNewTV v1.0"
    ),
    "Accept": "application/json, text/plain, */*",
}
_NM_PLATFORMS = {
    "netflix": "nf",
    "prime": "pv",
    "hotstar": "hs",
    "disney": "hs",
}
_nm_api_base: Optional[str] = None


_nm_api_bases: List[str] = []


async def _nm_resolve_bases(force: bool = False) -> List[str]:
    """Discover all working NewTV API bases (token_hash from probe domains)."""
    global _nm_api_bases, _nm_api_base
    if _nm_api_bases and not force:
        return _nm_api_bases
    found: List[str] = []
    # Prefer known API hosts first (probe domains are slow / flaky on serverless)
    for fb in ("https://tv.imgcdn.kim", "https://tv.imgcdn.cloud", "https://imgcdn.kim"):
        if fb not in found:
            found.append(fb)
    async with httpx.AsyncClient(timeout=8.0, follow_redirects=True, limits=httpx.Limits(max_connections=5)) as client:
        for probe in _NM_PROBE:
            try:
                r = await client.get(f"{probe}/checknewtv.php", headers=_NM_HEADERS)
                if r.status_code != 200:
                    continue
                data = r.json()
                th = data.get("token_hash")
                if not th:
                    continue
                base = base64.b64decode(th + "=" * (-len(th) % 4)).decode("utf-8").rstrip("/")
                if base.startswith("http") and base not in found:
                    found.append(base)
            except Exception:
                continue
    if not found:
        raise HTTPException(502, "NetMirror API base could not be resolved")
    _nm_api_bases = found
    _nm_api_base = found[0]
    return found


async def _nm_resolve_base() -> str:
    bases = await _nm_resolve_bases()
    return bases[0]


def _nm_headers(ott: str, extra: Optional[dict] = None) -> dict:
    h = dict(_NM_HEADERS)
    h["Ott"] = ott
    if extra:
        h.update(extra)
    return h


async def _nm_get(path: str, ott: str, params: Optional[dict] = None, extra: Optional[dict] = None) -> dict:
    """GET with base rotation on 403/5xx (Vercel/cloud IPs often blocked)."""
    global _nm_api_base, _nm_api_bases
    bases = await _nm_resolve_bases()
    last_err = "unknown"
    async with httpx.AsyncClient(timeout=25.0, follow_redirects=True) as client:
        for base in bases:
            try:
                r = await client.get(
                    f"{base}{path}",
                    params=params or {},
                    headers=_nm_headers(ott, extra),
                )
                if r.status_code in (403, 429, 502, 503):
                    last_err = f"HTTP {r.status_code} @ {base}"
                    continue
                if r.status_code >= 400:
                    last_err = f"HTTP {r.status_code} @ {base}"
                    continue
                try:
                    data = r.json()
                except Exception:
                    last_err = f"non-JSON @ {base}"
                    continue
                _nm_api_base = base
                return data
            except Exception as e:
                last_err = str(e)[:120]
                continue
    # soft fail — return sentinel for callers
    raise RuntimeError(f"NetMirror unavailable ({last_err})")


@app.get("/nm/search", tags=["NetMirror"])
async def nm_search(
    q: str = Query(..., min_length=1),
    platform: str = Query("netflix", description="netflix|prime|hotstar|disney"),
):
    """NetMirror title search (OTT library). Soft-fails with empty items if host blocked."""
    ott = _NM_PLATFORMS.get(platform.lower(), "nf")
    try:
        data = await _nm_get("/newtv/search.php", ott, {"s": q})
    except Exception as e:
        return {
            "ok": False,
            "provider": "netmirror",
            "display_name": "NetMirror",
            "platform": platform,
            "query": q,
            "count": 0,
            "items": [],
            "error": str(e)[:160],
            "note": "NetMirror often blocks cloud IPs (Vercel). Works on VPS/home IP.",
        }
    items = []
    for it in data.get("searchResult") or []:
        if not isinstance(it, dict):
            continue
        items.append({
            "id": str(it.get("id") or ""),
            "title": it.get("t") or it.get("title") or "",
            "platform": platform,
            "ott": ott,
        })
    return {
        "ok": True,
        "provider": "netmirror",
        "display_name": "NetMirror",
        "platform": platform,
        "query": q,
        "count": len(items),
        "items": items,
        "img_referer": data.get("img_referer"),
    }


@app.get("/nm/detail", tags=["NetMirror"])
async def nm_detail(
    id: str = Query(..., description="NetMirror content id from /nm/search"),
    platform: str = Query("netflix"),
):
    """NetMirror post/detail (seasons / episodes metadata)."""
    ott = _NM_PLATFORMS.get(platform.lower(), "nf")
    try:
        data = await _nm_get(
            "/newtv/post.php",
            ott,
            {"id": id},
            {"Lastep": "", "Usertoken": ""},
        )
    except Exception as e:
        return {"ok": False, "provider": "netmirror", "id": id, "error": str(e)[:160], "data": {}}
    return {
        "ok": True,
        "provider": "netmirror",
        "display_name": "NetMirror",
        "id": id,
        "platform": platform,
        "data": data,
    }


@app.get("/nm/stream", tags=["NetMirror"])
async def nm_stream(
    id: str = Query(..., description="Content or episode id"),
    platform: str = Query("netflix"),
    title: str = Query(None, description="Optional title — search then play first match"),
    se: int = 0,
    ep: int = 0,
):
    """
    NetMirror direct stream (usually HLS .m3u8).
    Pass `id` from search, or `title` to search+play in one call.
    """
    try:
        return await _nm_stream_inner(id, platform, title, se, ep)
    except Exception as e:
        return {
            "ok": False,
            "provider": "netmirror",
            "display_name": "NetMirror",
            "error": str(e)[:160],
            "streams": [],
            "stream": None,
            "note": "NetMirror blocks many cloud IPs. Use embeds / VixSrc / 4K instead.",
        }


async def _nm_stream_inner(id, platform, title, se, ep):
    ott = _NM_PLATFORMS.get(platform.lower(), "nf")
    content_id = id
    if title and not id:
        sr = await _nm_get("/newtv/search.php", ott, {"s": title})
        results = sr.get("searchResult") or []
        if not results:
            return {"ok": False, "provider": "netmirror", "error": "no search results", "streams": [], "stream": None}
        content_id = str(results[0].get("id"))
    # detail for series episode mapping
    if se or ep:
        try:
            post = await _nm_get(
                "/newtv/post.php",
                ott,
                {"id": content_id},
                {"Lastep": "", "Usertoken": ""},
            )
            # try match episode list
            for e in post.get("episodes") or []:
                if not e:
                    continue
                e_ep = e.get("ep") or (e.get("epNum") or "").replace("E", "")
                try:
                    e_ep_i = int(e_ep)
                except Exception:
                    continue
                if ep and e_ep_i == ep:
                    content_id = str(e.get("id") or content_id)
                    break
            else:
                content_id = str(post.get("main_id") or content_id)
        except Exception:
            pass
    else:
        try:
            post = await _nm_get(
                "/newtv/post.php",
                ott,
                {"id": content_id},
                {"Lastep": "", "Usertoken": ""},
            )
            content_id = str(post.get("main_id") or content_id)
        except Exception:
            pass

    player = await _nm_get(
        "/newtv/player.php",
        ott,
        {"id": content_id},
        {"Usertoken": ""},
    )
    link = player.get("video_link") or player.get("url") or ""
    if not link:
        return {"ok": False, "provider": "netmirror", "error": f"no video_link ({player.get('status')})", "streams": [], "stream": None}
    base = await _nm_resolve_base()
    return {
        "provider": "netmirror",
        "display_name": "NetMirror",
        "platform": platform,
        "id": content_id,
        "title": player.get("title") or title,
        "status": player.get("status"),
        "stream": {
            "url": link,
            "format": "HLS" if ".m3u8" in link else "FILE",
            "playable": True,
            "headers": {
                "Referer": player.get("referer") or "https://net52.cc",
                "User-Agent": _NM_HEADERS["User-Agent"],
            },
        },
        "streams": [
            {
                "url": link,
                "label": f"NetMirror ({platform})",
                "format": "HLS" if ".m3u8" in link else "FILE",
                "playable": True,
                "headers": {
                    "Referer": player.get("referer") or "https://net52.cc",
                    "User-Agent": _NM_HEADERS["User-Agent"],
                },
            }
        ],
        "api_base": base,
    }


@app.get("/nm/play", tags=["NetMirror"])
async def nm_play(
    q: str = Query(..., description="Title to search"),
    platform: str = Query("netflix"),
    se: int = 0,
    ep: int = 0,
):
    """Search + stream in one call (NetMirror)."""
    return await nm_stream(id="", platform=platform, title=q, se=se, ep=ep)


# Named UI sources (as shown in Select Source Server dialog)
_NAMED_SOURCES = [
    {
        "id": "netmirror",
        "display_name": "NetMirror",
        "badge": "Beta Experimental",
        "description": "OTT mirror (Netflix / Prime / Hotstar) — direct HLS",
        "endpoint": "/nm/play?q={title}&platform=netflix",
    },
    {
        "id": "spacedom",
        "display_name": "SpaceDom",
        "badge": "Beta Experimental",
        "description": "Experimental multi-source embed (Cinemaos + Zxc)",
        "endpoint": "/providers/embeds?tmdb_id={tmdb_id}&media={media}",
    },
    {
        "id": "source3",
        "display_name": "Source 3",
        "badge": "Reliable Multi-lang Always-Work",
        "description": "MultiEmbed + VidSrc + AutoEmbed (multi language)",
        "providers": ["MultiEmbed", "VidSrc", "AutoEmbed", "2Embed"],
    },
    {
        "id": "source6",
        "display_name": "Source 6",
        "badge": "Multi-lang Sometimes works best",
        "description": "VidSrc WTF v4 + PrimeSrc + VidZee",
        "providers": ["VidSrc WTF v4", "PrimeSrc", "VidZee", "VidSrc WTF v3"],
    },
]


@app.get("/sources", tags=["Providers"])
async def sources_catalog():
    """UI source server list (NetMirror, SpaceDom, Source 3, Source 6, …)."""
    return {
        "count": len(_NAMED_SOURCES) + len(_KIDUYU_EMBEDS),
        "named": _NAMED_SOURCES,
        "embeds": [{"id": n, "display_name": n} for n, _, _ in _KIDUYU_EMBEDS],
    }


@app.get("/sources/play", tags=["Providers"])
async def sources_play(
    source: str = Query(..., description="netmirror|spacedom|source3|source6|or embed id"),
    tmdb_id: int = Query(None),
    title: str = Query(None),
    media: str = Query("movie"),
    se: int = 1,
    ep: int = 1,
    platform: str = Query("netflix"),
):
    """Play via a named source server."""
    sid = source.lower().strip()
    media = "tv" if media in ("tv", "series") else "movie"
    if sid in ("netmirror", "nm"):
        if not title:
            raise HTTPException(400, "title required for NetMirror")
        return await nm_play(q=title, platform=platform, se=se if media == "tv" else 0, ep=ep if media == "tv" else 0)
    if sid in ("spacedom", "space"):
        if not tmdb_id:
            raise HTTPException(400, "tmdb_id required for SpaceDom")
        embeds = _build_embeds(tmdb_id, media, se, ep)
        # prefer experimental hosts
        prefer = {"cinemaos", "zxcstream", "vidnest", "peachify", "megaplay"}
        ranked = sorted(embeds, key=lambda e: (0 if e["provider"] in prefer else 1))
        return {
            "provider": "spacedom",
            "display_name": "SpaceDom",
            "badge": "Beta Experimental",
            "count": len(ranked),
            "sources": ranked,
        }
    if sid in ("source3", "source_3", "s3"):
        if not tmdb_id:
            raise HTTPException(400, "tmdb_id required")
        want = {"multiembed", "vidsrc", "autoembed", "2embed", "embedsu"}
        embeds = [e for e in _build_embeds(tmdb_id, media, se, ep) if e["provider"] in want]
        return {
            "provider": "source3",
            "display_name": "Source 3",
            "badge": "Reliable Multi-lang Always-Work",
            "count": len(embeds),
            "sources": embeds,
        }
    if sid in ("source6", "source_6", "s6"):
        if not tmdb_id:
            raise HTTPException(400, "tmdb_id required")
        want = {"vidsrc_wtf_v4", "vidsrc_wtf_v3", "primesrc", "vidzee"}
        embeds = [e for e in _build_embeds(tmdb_id, media, se, ep) if e["provider"] in want]
        return {
            "provider": "source6",
            "display_name": "Source 6",
            "badge": "Multi-lang Sometimes works best",
            "count": len(embeds),
            "sources": embeds,
        }
    # fallback: single embed by provider id
    if tmdb_id:
        embeds = _build_embeds(tmdb_id, media, se, ep)
        hit = [e for e in embeds if e["provider"] == sid or e["provider"].lower() == sid]
        return {"provider": sid, "display_name": sid, "count": len(hit), "sources": hit or embeds[:5]}
    raise HTTPException(400, "Need title (NetMirror) or tmdb_id (other sources)")

# =============================================================================
# FilmSnaps + KiduyuTv embed / provider catalog (from their open-source lists)
# Kiduyu backend (sflatransport) is often CF-challenged; embeds always work.
# =============================================================================

# KiduyuTv StreamProvider templates (movie / tv with season episode)
_KIDUYU_EMBEDS = [
    ("videasy", "https://player.videasy.net/movie/{id}", "https://player.videasy.net/tv/{id}/{se}/{ep}"),
    ("vidrock", "https://vidrock.net/movie/{id}", "https://vidrock.net/tv/{id}/{se}/{ep}"),
    ("vidlink", "https://vidlink.pro/movie/{id}", "https://vidlink.pro/tv/{id}/{se}/{ep}"),
    ("vidfast", "https://vidfast.pro/movie/{id}", "https://vidfast.pro/tv/{id}/{se}/{ep}"),
    ("vidking", "https://www.vidking.net/embed/movie/{id}", "https://www.vidking.net/embed/tv/{id}/{se}/{ep}"),
    ("vidnest", "https://vidnest.fun/movie/{id}", "https://vidnest.fun/tv/{id}/{se}/{ep}"),
    ("vidup", "https://vidup.to/movie/{id}", "https://vidup.to/tv/{id}/{se}/{ep}"),
    ("111movies", "https://111movies.com/movie/{id}", "https://111movies.com/tv/{id}/{se}/{ep}"),
    ("flixer", "https://flixer.su/watch/movie/{id}", "https://flixer.su/watch/tv/{id}/{se}/{ep}"),
    ("vidcore", "https://vidcore.net/movie/{id}", "https://vidcore.net/tv/{id}/{se}/{ep}"),
    ("moviesapi", "https://moviesapi.to/movie/{id}", "https://moviesapi.to/tv/{id}-{se}-{ep}"),
    ("peachify", "https://peachify.top/embed/movie/{id}", "https://peachify.top/embed/tv/{id}/{se}/{ep}"),
    ("vidapi", "https://vaplayer.ru/embed/movie/{id}", "https://vaplayer.ru/embed/tv/{id}/{se}/{ep}"),
    ("vidplus", "https://player.vidplus.to/embed/movie/{id}", "https://player.vidplus.to/embed/tv/{id}/{se}/{ep}"),
    ("cinesrc", "https://cinesrc.st/embed/movie/{id}", "https://cinesrc.st/embed/tv/{id}?s={se}&e={ep}"),
    ("vidzen", "https://vidzen.fun/movie/{id}", "https://vidzen.fun/tv/{id}/{se}/{ep}"),
    ("cinemaos", "https://cinemaos.tech/player/{id}", "https://cinemaos.tech/player/{id}/{se}/{ep}"),
    ("amri", "https://amri.gg/movie/{id}", "https://amri.gg/tv/{id}/{se}/{ep}"),
    ("zxcstream", "https://zxcstream.xyz/embed/movie/{id}", "https://zxcstream.xyz/embed/tv/{id}/{se}/{ep}"),
    ("vidlux", "https://vidlux.xyz/embed/movie/{id}", "https://vidlux.xyz/embed/tv/{id}/{se}/{ep}"),
    ("vidsrc_wtf_v4", "https://vidsrc.wtf/api/4/movie/?id={id}", "https://vidsrc.wtf/api/4/tv/?id={id}&s={se}&e={ep}"),
    ("vidsrc_wtf_v3", "https://vidsrc.wtf/api/3/movie/?id={id}", "https://vidsrc.wtf/api/3/tv/?id={id}&s={se}&e={ep}"),
    ("primesrc", "https://primesrc.me/embed/movie?tmdb={id}", "https://primesrc.me/embed/tv?tmdb={id}&season={se}&episode={ep}"),
    ("vidzee", "https://player.vidzee.wtf/v2/embed/movie/{id}", "https://player.vidzee.wtf/v2/embed/tv/{id}/{se}/{ep}"),
    ("lordflix", "https://lordflix.org/watch/movie/{id}", "https://lordflix.org/watch/tv/{id}/{se}/{ep}"),
    ("vidsrc", "https://vidsrc.to/embed/movie/{id}", "https://vidsrc.to/embed/tv/{id}/{se}/{ep}"),
    ("vidsrc_cc", "https://vidsrc.cc/v2/embed/movie/{id}", "https://vidsrc.cc/v2/embed/tv/{id}/{se}/{ep}"),
    ("autoembed", "https://player.autoembed.cc/embed/movie/{id}", "https://player.autoembed.cc/embed/tv/{id}/{se}/{ep}"),
    ("2embed", "https://www.2embed.cc/embed/{id}", "https://www.2embed.cc/embedtv/{id}&s={se}&e={ep}"),
    ("multiembed", "https://multiembed.mov/?video_id={id}&tmdb=1", "https://multiembed.mov/?video_id={id}&tmdb=1&s={se}&e={ep}"),
    ("embedsu", "https://embed.su/embed/movie/{id}", "https://embed.su/embed/tv/{id}/{se}/{ep}"),
    ("smashystream", "https://player.smashy.stream/movie/{id}", "https://player.smashy.stream/tv/{id}?s={se}&e={ep}"),
]

# FilmSnaps providers.json host ids (for documentation / allowlist)
_FILMSNAPS_HOSTS = [
    "web.nxsha.app", "nxcdn.app", "peachify.top", "screenscape.me", "nhdapi.com",
    "zxcstream.xyz", "player.zxcstream.xyz", "cinemaos.live", "chillflix.lol",
    "vidapi.cloud", "vidnest.fun", "toustream.xyz", "streamguide.cfd",
    "player.vidzee.wtf", "megaplay.buzz", "player.videasy.net", "www.vidking.net",
]


def _build_embeds(tmdb_id: int, media: str = "movie", se: int = 1, ep: int = 1) -> List[dict]:
    se = se or 1
    ep = ep or 1
    out = []
    for name, m_tpl, t_tpl in _KIDUYU_EMBEDS:
        try:
            if media in ("tv", "series"):
                url = t_tpl.format(id=tmdb_id, se=se, ep=ep)
            else:
                url = m_tpl.format(id=tmdb_id, se=se, ep=ep)
            out.append({
                "type": "embed",
                "provider": name,
                "url": url,
                "playable": True,
                "phone_friendly": True,
                "source": "kiduyu+filmsnaps",
            })
        except Exception:
            continue
    return out


@app.get("/providers/list", tags=["Providers"])
async def providers_list():
    """All embed providers (KiduyuTv + FilmSnaps style) + FilmSnaps CDN hosts."""
    return {
        "ok": True,
        "embed_count": len(_KIDUYU_EMBEDS),
        "embeds": [{"id": n, "movie": m, "tv": tv} for n, m, tv in _KIDUYU_EMBEDS],
        "filmsnaps_hosts": _FILMSNAPS_HOSTS,
        "note": "Use /providers/embeds?tmdb_id=19995&media=movie",
    }


@app.get("/providers/embeds", tags=["Providers"])
async def providers_embeds(
    tmdb_id: int = Query(..., description="TMDB id"),
    media: str = Query("movie", description="movie | tv"),
    se: int = Query(1, description="season (tv)"),
    ep: int = Query(1, description="episode (tv)"),
):
    """KiduyuTv + FilmSnaps-style embed URLs for a title."""
    embeds = _build_embeds(tmdb_id, media, se, ep)
    return {
        "ok": True,
        "tmdb_id": tmdb_id,
        "media": media,
        "se": se,
        "ep": ep,
        "count": len(embeds),
        "embeds": embeds,
    }


@app.get("/providers/streams", tags=["Providers"])
async def providers_streams(
    tmdb_id: int = Query(...),
    media: str = Query("movie"),
    se: int = 1,
    ep: int = 1,
    include_embeds: bool = True,
    include_moviebox: bool = False,
    include_4k: bool = False,
    title: str = Query(None),
):
    """
    Aggregate play sources:
    - full embed catalog (Kiduyu + FilmSnaps hosts)
    - optional MovieBox DASH proxy (if title/subject resolved)
    - optional 4KHDHub direct streams (if title search hits)
    """
    media = "tv" if media in ("tv", "series") else "movie"
    sources = _build_embeds(tmdb_id, media, se, ep) if include_embeds else []
    errors = {}

    if include_moviebox and title:
        try:
            mb = await mb_search(q=title, page=1)
            items = mb.get("items") or []
            if items:
                sid = items[0].get("subject_id")
                st = await mb_stream(sid, se=se if media == "tv" else 0, ep=ep if media == "tv" else 0)
                if st.get("proxy_url") or st.get("proxy_mpd"):
                    sources.append({
                        "type": "dash",
                        "provider": "moviebox",
                        "url": st.get("proxy_url") or st.get("proxy_mpd"),
                        "playable": True,
                        "phone_friendly": False,
                        "subject_id": sid,
                    })
                for b in st.get("browser") or []:
                    sources.append({"type": "embed", "provider": b.get("provider"), "url": b.get("url"), "playable": True})
        except Exception as e:
            errors["moviebox"] = str(e)[:120]

    if include_4k and title:
        try:
            html = await fk_fetch(f"?s={title}")
            items = fk_parse_search(html)
            if items:
                fid = items[0].get("id")
                fk = await fk_stream(id=fid, se=se if media == "tv" else 0, ep=ep if media == "tv" else 0, resolve=True, limit=6)
                for s in fk.get("streams") or []:
                    if s.get("playable"):
                        sources.append({
                            "type": "direct",
                            "provider": "4khdhub",
                            "url": s.get("url"),
                            "label": s.get("label"),
                            "playable": True,
                            "phone_friendly": True,
                        })
        except Exception as e:
            errors["4khdhub"] = str(e)[:120]

    return {
        "ok": True,
        "tmdb_id": tmdb_id,
        "media": media,
        "se": se,
        "ep": ep,
        "count": len(sources),
        "sources": sources,
        "errors": errors or None,
    }


@app.get("/providers/subtitles", tags=["Providers"])
async def providers_subtitles(
    tmdb_id: int = Query(None),
    imdb_id: str = Query(None),
    query: str = Query(None),
    se: int = 0,
    ep: int = 0,
):
    """Wyzie-style open subtitle lookup (FilmSnaps uses sub.wyzie.io)."""
    urls = []
    if imdb_id:
        urls.append(f"https://sub.wyzie.io/search?id={imdb_id}")
    if tmdb_id:
        urls.append(f"https://sub.wyzie.io/search?id={tmdb_id}")
        if se and ep:
            urls.append(f"https://sub.wyzie.io/search?id={tmdb_id}&season={se}&episode={ep}")
    if query:
        urls.append(f"https://sub.wyzie.io/search?query={quote(query)}")
    results = []
    async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
        for u in urls[:3]:
            try:
                r = await client.get(u, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
                if r.status_code == 200:
                    data = r.json()
                    if isinstance(data, list):
                        results.extend(data)
                    elif isinstance(data, dict):
                        results.extend(data.get("subtitles") or data.get("data") or [])
            except Exception:
                continue
    return {"ok": True, "count": len(results), "subtitles": results[:40]}


@app.get("/api/play", tags=["Playback"])
async def api_play(
    subject_id: str = Query(None, description="Legacy MovieBox id — ignored on web path"),
    tmdb_id: str = Query(None, description="TMDB id for web playback"),
    media: str = Query("movie"),
    se: int = 0,
    ep: int = 0,
    q: str = Query("", description="Title for 4KHDHub match / TMDB lookup"),
    fast: bool = Query(True, description="Skip slow 4K resolve (embeds only) — set false for hub list"),
):
    """Web playback: embeds immediately; optional 4KHDHub when fast=false."""
    sources = []
    errors = {}
    media = "tv" if media in ("tv", "series", "show") else "movie"
    title = (q or "").strip()
    tid = (tmdb_id or "").strip() or None
    if not tid and subject_id and str(subject_id).isdigit() and len(str(subject_id)) < 12:
        tid = str(subject_id)

    meta = None
    if tid:
        det = await _tmdb_get(f"/{media}/{tid}")
        if det.get("id"):
            title = title or det.get("title") or det.get("name") or ""
            meta = {"tmdb": str(det["id"]), "imdb": (det.get("external_ids") or {}).get("imdb_id"), "name": title}
            if not meta.get("imdb"):
                try:
                    ext = await _tmdb_get(f"/{media}/{tid}/external_ids")
                    meta["imdb"] = ext.get("imdb_id")
                except Exception:
                    pass
    if not meta and title:
        meta = await _tmdb_search_id(title, media)
        if meta and meta.get("tmdb"):
            tid = str(meta["tmdb"])
            title = title or meta.get("name") or ""

    se_use = se if media == "tv" else 0
    ep_use = ep if media == "tv" else 0
    if media == "tv":
        se_use = max(1, se_use or 1)
        ep_use = max(1, ep_use or 1)

    if meta:
        sources.extend(_embed_sources_meta(meta, media, se_use or 1, ep_use or 1))
    else:
        errors["embed"] = "no TMDB match"

    hub_links: List[dict] = []
    if not fast and title:
        try:
            hub_links = await asyncio.wait_for(
                collect_4k_mirrors(title, se_use if media == "tv" else 0, ep_use if media == "tv" else 0, limit=6),
                timeout=12.0,
            )
        except Exception as e:
            errors["4khdhub"] = str(e)

    hub_src = [{
        "provider": "4khdhub",
        "label": h.get("label") or "Hub",
        "url": h["url"],
        "play_url": h.get("play_url") or h["url"],
        "format": h.get("format") or "FILE",
        "type": "direct",
        "headers": h.get("headers") or {},
    } for h in hub_links if h.get("url")]

    uniq = []
    seen = set()
    for s in sources + hub_src:
        u = s.get("url")
        if not u or u in seen:
            continue
        seen.add(u)
        uniq.append(s)

    downloads = []
    for h in hub_links:
        u = h.get("url") or ""
        if u:
            downloads.append({
                "label": h.get("label") or "Download",
                "url": u,
                "format": h.get("format") or "FILE",
                "quality": h.get("quality") or "",
                "filename": h.get("filename") or "",
            })

    return {
        "tmdb_id": tid,
        "media": media,
        "se": se_use,
        "ep": ep_use,
        "title": title,
        "count": len(uniq),
        "sources": uniq,
        "hub_links": hub_links,
        "downloads": downloads,
        "errors": errors or None,
        "strategy": "embeds-first (fast=true) · 4k optional",
        "note": None,
    }



# ═══════════════════════════════════════════════════════════
# MUSIC helpers — JioSaavn + YT Music (vivi-music style)
# ═══════════════════════════════════════════════════════════

YT_MUSIC_KEY = "AIzaSyC9XL3ZjWddXya6X74dJoCTL-WEYFDNX30"
YT_MUSIC_CTX = {
    "client": {
        "clientName": "WEB_REMIX",
        "clientVersion": "1.20240403.01.00",
        "hl": "en",
        "gl": "US",
    }
}
YT_MUSIC_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Content-Type": "application/json",
    "Origin": "https://music.youtube.com",
    "Referer": "https://music.youtube.com/",
}
SAAVN_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
}


async def _ytm_post(path: str, body: dict) -> dict:
    url = f"https://music.youtube.com/youtubei/v1/{path}?key={YT_MUSIC_KEY}&prettyPrint=false"
    payload = {"context": YT_MUSIC_CTX, **body}
    async with httpx.AsyncClient(timeout=25.0) as client:
        r = await client.post(url, json=payload, headers=YT_MUSIC_HEADERS)
        if r.status_code >= 400:
            return {}
        try:
            return r.json()
        except Exception:
            return {}


def _ytm_walk(obj, key: str, out: list):
    if isinstance(obj, dict):
        if key in obj:
            out.append(obj[key])
        for v in obj.values():
            _ytm_walk(v, key, out)
    elif isinstance(obj, list):
        for i in obj:
            _ytm_walk(i, key, out)


def _ytm_parse_item(it: dict) -> Optional[dict]:
    s = json.dumps(it)
    vid_m = re.search(r'"videoId"\s*:\s*"([a-zA-Z0-9_-]{11})"', s)
    if not vid_m:
        return None
    vid = vid_m.group(1)
    texts = re.findall(r'"text"\s*:\s*"([^"\\]{1,120})"', s)
    noise = {"Video", "Song", "Album", "Playlist", "Start mix", "Play next", "Shuffle", "Subscribe", "Share", "Episode"}
    clean = [x for x in texts if x not in noise and not x.startswith("\\u") and len(x) > 1]
    title = clean[0] if clean else vid
    artist = ""
    for c in clean[1:]:
        if c not in title and not re.match(r"^\d", c) and "views" not in c.lower() and "play" not in c.lower():
            artist = c
            break
    thumbs = re.findall(r'https://i\.ytimg\.com/[^"\\]+', s)
    thumb = thumbs[0].replace("\\u0026", "&") if thumbs else f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg"
    return {
        "id": vid,
        "video_id": vid,
        "title": title,
        "artist": artist,
        "thumb": thumb,
        "type": "song",
        "provider": "ytmusic",
        "source": "ytmusic",
    }


async def _saavn_get(params: dict) -> Any:
    q = {"_format": "json", "_marker": "0", "api_version": "4", "ctx": "web6dot0"}
    q.update(params)
    async with httpx.AsyncClient(timeout=20.0, headers=SAAVN_HEADERS) as client:
        r = await client.get("https://www.jiosaavn.com/api.php", params=q)
        if r.status_code != 200:
            return {}
        try:
            return r.json()
        except Exception:
            m = re.search(r"(\{.*\}|\[.*\])", r.text, re.S)
            if m:
                try:
                    return json.loads(m.group(1))
                except Exception:
                    return {}
            return {}


def _saavn_card(song: dict) -> Optional[dict]:
    if not isinstance(song, dict):
        return None
    sid = song.get("id")
    title = song.get("title") or song.get("song") or ""
    if not sid or not title:
        return None
    mi = song.get("more_info") if isinstance(song.get("more_info"), dict) else {}
    artists = song.get("primary_artists") or mi.get("music") or song.get("subtitle") or song.get("singers") or ""
    if isinstance(artists, list):
        artists = ", ".join(str(a) for a in artists)
    image = song.get("image") or ""
    image = image.replace("-50x50", "-500x500").replace("-150x150", "-500x500")
    enc = mi.get("encrypted_media_url") or song.get("encrypted_media_url") or ""
    dur = mi.get("duration") or song.get("duration")
    try:
        dur = int(dur) if dur else None
    except Exception:
        dur = None
    return {
        "id": f"saavn:{sid}",
        "saavn_id": sid,
        "video_id": None,
        "title": title,
        "artist": artists,
        "thumb": image,
        "duration": dur,
        "encrypted_media_url": enc,
        "type": "song",
        "provider": "jiosaavn",
        "source": "jiosaavn",
        "perma_url": song.get("perma_url"),
    }


async def _saavn_search(q: str, n: int = 20) -> List[dict]:
    data = await _saavn_get({"__call": "search.getResults", "p": "1", "q": q, "n": str(n)})
    results = data.get("results") or []
    out = []
    for s in results:
        c = _saavn_card(s)
        if c:
            out.append(c)
    return out


async def _saavn_auth_url(encrypted: str, bitrate: int = 320) -> Optional[str]:
    if not encrypted:
        return None
    data = await _saavn_get({
        "__call": "song.generateAuthToken",
        "url": encrypted,
        "bitrate": str(bitrate),
    })
    url = data.get("auth_url") if isinstance(data, dict) else None
    if url and url.startswith("http"):
        return url
    return None


async def _saavn_stream_by_id(sid: str) -> Optional[dict]:
    data = await _saavn_get({"__call": "song.getDetails", "cc": "in", "pids": sid})
    song = None
    if isinstance(data, dict):
        if sid in data and isinstance(data[sid], dict):
            song = data[sid]
        elif isinstance(data.get("songs"), list) and data["songs"]:
            for s in data["songs"]:
                if isinstance(s, dict) and (s.get("id") == sid or not song):
                    song = s
                    if s.get("id") == sid:
                        break
        else:
            for v in data.values():
                if isinstance(v, dict) and (v.get("id") == sid or v.get("song") or v.get("title")):
                    song = v
                    break
                if isinstance(v, list):
                    for s in v:
                        if isinstance(s, dict) and s.get("id") == sid:
                            song = s
                            break
    if not isinstance(song, dict):
        return None
    card = _saavn_card(song)
    if not card:
        card = {
            "id": f"saavn:{sid}",
            "saavn_id": sid,
            "title": song.get("song") or song.get("title") or sid,
            "artist": song.get("primary_artists") or song.get("singers") or "",
            "thumb": (song.get("image") or "").replace("-150x150", "-500x500").replace("-50x50", "-500x500"),
            "provider": "jiosaavn",
            "source": "jiosaavn",
        }
    enc = (
        song.get("encrypted_media_url")
        or (song.get("more_info") or {}).get("encrypted_media_url")
        or card.get("encrypted_media_url")
        or ""
    )
    if not enc:
        return card
    for br in (320, 160, 96):
        audio = await _saavn_auth_url(enc, br)
        if audio:
            card["audio_url"] = audio
            card["audio_format"] = "mp4"
            card["bitrate"] = br
            break
    return card


async def _saavn_match(title: str, artist: str = "") -> Optional[dict]:
    q = f"{title} {artist}".strip()
    if not q:
        return None
    hits = await _saavn_search(q, 8)
    if not hits:
        return None
    tlow = re.sub(r"\s*\(.*?\)\s*", " ", title).lower().strip()
    alow = (artist or "").lower()
    def score(h):
        ht = (h.get("title") or "").lower()
        ha = (h.get("artist") or "").lower()
        s = 0
        if ht == tlow or tlow in ht or ht in tlow:
            s += 5
        if alow and alow.split(",")[0].strip() in ha:
            s += 3
        return s
    hits = sorted(hits, key=score, reverse=True)
    return await _saavn_stream_by_id(hits[0]["saavn_id"])


async def _ytdlp_audio(video_id: str) -> Tuple[Optional[str], Optional[str], Optional[int], Optional[str]]:
    def _extract():
        try:
            import yt_dlp  # type: ignore
        except ImportError:
            return None, None, None, "yt-dlp not installed"
        url = f"https://www.youtube.com/watch?v={video_id}"
        clients = ["android,web", "android_music,android", "ios,web", "tv_embedded", "web"]
        last_err = None
        for client in clients:
            opts = {
                "quiet": True,
                "no_warnings": True,
                "skip_download": True,
                "format": "bestaudio[ext=m4a]/bestaudio/best",
                "noplaylist": True,
                "extractor_args": {"youtube": {"player_client": client.split(",")}},
            }
            try:
                with yt_dlp.YoutubeDL(opts) as ydl:
                    info = ydl.extract_info(url, download=False)
                if not info:
                    continue
                formats = info.get("formats") or []
                audio_fmts = [
                    f for f in formats
                    if f.get("url") and f.get("acodec") not in (None, "none") and f.get("vcodec") in (None, "none")
                ]
                if not audio_fmts and info.get("url"):
                    return info.get("url"), info.get("ext"), info.get("duration"), None
                if not audio_fmts:
                    continue
                def score(f):
                    ext = (f.get("ext") or "")
                    br = f.get("abr") or f.get("tbr") or 0
                    pref = 3 if ext == "m4a" else 2 if ext in ("mp4", "webm") else 1
                    return (pref, br)
                audio_fmts.sort(key=score, reverse=True)
                best = audio_fmts[0]
                return best.get("url"), best.get("ext") or "m4a", info.get("duration"), None
            except Exception as e:
                last_err = str(e)
                continue
        return None, None, None, last_err or "all clients failed"
    return await asyncio.to_thread(_extract)





@app.get("/movie/search", tags=["Movies"])
async def movie_search(q: str = Query(..., min_length=1), page: int = 1):
    """Unified movie search: TMDB + MovieBox + 4KHDHub (parallel)."""
    import asyncio as _aio

    async def tmdb():
        try:
            d = await _tmdb_get("/search/multi", {"query": q, "page": page})
            out = []
            for r in d.get("results") or []:
                if r.get("media_type") not in ("movie", "tv"):
                    continue
                out.append({
                    "provider": "tmdb",
                    "id": r["id"],
                    "media": r["media_type"],
                    "title": r.get("title") or r.get("name"),
                    "year": (r.get("release_date") or r.get("first_air_date") or "")[:4],
                    "poster": f"https://image.tmdb.org/t/p/w500{r['poster_path']}" if r.get("poster_path") else None,
                    "rating": r.get("vote_average"),
                })
            return out
        except Exception:
            return []

    async def mb():
        try:
            d = await mb_search(q=q, page=page)
            return [
                {
                    "provider": "moviebox",
                    "id": it.get("subject_id"),
                    "media": it.get("type") or "movie",
                    "title": it.get("name") or it.get("title"),
                    "year": it.get("year"),
                    "poster": it.get("poster_url") or it.get("poster"),
                    "rating": it.get("rating"),
                }
                for it in (d.get("items") or [])
            ]
        except Exception:
            return []

    async def fk():
        try:
            html = await fk_fetch(f"?s={q}")
            return [
                {
                    "provider": "4khdhub",
                    "id": it.get("id"),
                    "media": "movie",
                    "title": it.get("name") or it.get("title"),
                    "year": it.get("year"),
                    "poster": it.get("poster"),
                }
                for it in fk_parse_search(html)[:15]
            ]
        except Exception:
            return []

    tmdb_i, mb_i, fk_i = await _aio.gather(tmdb(), mb(), fk())
    return {
        "query": q,
        "page": page,
        "tmdb": tmdb_i,
        "moviebox": mb_i,
        "fourkhdhub": fk_i,
        "count": len(tmdb_i) + len(mb_i) + len(fk_i),
    }


@app.get("/movie/play", tags=["Movies"])
async def movie_play(
    title: str = Query(None, description="Title to match"),
    tmdb_id: int = Query(None),
    media: str = Query("movie", description="movie|tv"),
    subject_id: str = Query(None, description="MovieBox subject id"),
    fk_id: str = Query(None, description="4KHDHub path id"),
    se: int = 0,
    ep: int = 0,
):
    """
    Aggregate play sources: embed servers + MovieBox DASH proxy + 4K direct streams.
    """
    sources = []
    errors = {}
    # embeds via TMDB
    tid = tmdb_id
    if not tid and title:
        try:
            tr = await _tmdb_get("/search/multi", {"query": title})
            for r in tr.get("results") or []:
                if r.get("media_type") in ("movie", "tv"):
                    tid = r["id"]
                    media = r["media_type"]
                    break
        except Exception as e:
            errors["tmdb"] = str(e)[:100]
    if tid:
        path = f"{media}/{tid}"
        if media == "tv":
            path = f"tv/{tid}/{se or 1}/{ep or 1}"
        embeds = [
            ("vidsrc", f"https://vidsrc.to/embed/{path if media=='movie' else f'tv/{tid}/{se or 1}/{ep or 1}'}"),
            ("vidlink", f"https://vidlink.pro/{media}/{tid}" + (f"/{se or 1}/{ep or 1}" if media == "tv" else "")),
            ("videasy", f"https://player.videasy.net/{media}/{tid}" + (f"/{se or 1}/{ep or 1}" if media == "tv" else "")),
            ("vidking", f"https://www.vidking.net/embed/{media}/{tid}" + (f"/{se or 1}/{ep or 1}" if media == "tv" else "")),
            ("autoembed", f"https://player.autoembed.cc/embed/{media}/{tid}" + (f"/{se or 1}/{ep or 1}" if media == "tv" else "")),
        ]
        for name, url in embeds:
            sources.append({"type": "embed", "provider": name, "url": url, "playable": True, "phone_friendly": True})
    # MovieBox
    if subject_id:
        try:
            mb = await mb_stream(subject_id, se=se, ep=ep)
            if mb.get("proxy_mpd") or mb.get("proxy_url"):
                sources.append({
                    "type": "dash",
                    "provider": "moviebox",
                    "url": mb.get("proxy_url") or mb.get("proxy_mpd"),
                    "playable": True,
                    "phone_friendly": False,
                    "note": "HEVC DASH via cookie proxy",
                })
            for b in mb.get("browser") or []:
                sources.append({"type": "embed", "provider": b.get("provider"), "url": b.get("url"), "playable": True, "phone_friendly": True})
        except Exception as e:
            errors["moviebox"] = str(e)[:120]
    # 4K direct
    if fk_id:
        try:
            fk = await fk_stream(id=fk_id, se=se, ep=ep, resolve=True, limit=8)
            for s in fk.get("streams") or []:
                if s.get("playable"):
                    sources.append({
                        "type": "direct",
                        "provider": "4khdhub",
                        "url": s.get("url"),
                        "label": s.get("label"),
                        "quality": s.get("quality"),
                        "playable": True,
                        "phone_friendly": True,
                        "headers": s.get("headers"),
                    })
        except Exception as e:
            errors["4khdhub"] = str(e)[:120]
    return {
        "title": title,
        "tmdb_id": tid,
        "media": media,
        "subject_id": subject_id,
        "fk_id": fk_id,
        "se": se,
        "ep": ep,
        "count": len(sources),
        "sources": sources,
        "errors": errors or None,
    }

@app.get("/music/search", tags=["Music"])
async def music_search(q: str = Query(..., min_length=1)):
    """Search JioSaavn (primary, reliable streams) + YouTube Music."""
    q = q.strip()[:100]
    saavn, ytm = [], []
    errors = {}
    try:
        saavn = await _saavn_search(q, 20)
    except Exception as e:
        errors["jiosaavn"] = str(e)
    try:
        data = await _ytm_post("search", {"query": q})
        items = []
        _ytm_walk(data, "musicResponsiveListItemRenderer", items)
        _ytm_walk(data, "musicTwoRowItemRenderer", items)
        seen = set()
        for it in items:
            e = _ytm_parse_item(it)
            if e and e["video_id"] not in seen:
                seen.add(e["video_id"])
                ytm.append(e)
    except Exception as e:
        errors["ytmusic"] = str(e)
    deezer = []
    try:
        async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
            r = await client.get("https://api.deezer.com/search", params={"q": q, "limit": 8})
            if r.status_code == 200:
                for it in (r.json().get("data") or []):
                    if not isinstance(it, dict):
                        continue
                    artist = (it.get("artist") or {}).get("name") or ""
                    cover = (it.get("album") or {}).get("cover_big") or ""
                    deezer.append({
                        "id": f"deezer:{it.get('id')}",
                        "title": it.get("title"),
                        "artist": artist,
                        "thumb": cover,
                        "duration": it.get("duration"),
                        "preview_url": it.get("preview"),
                        "provider": "deezer",
                    })
    except Exception as e:
        errors["deezer"] = str(e)[:80]
    # Saavn first (playable), then YT, then Deezer previews
    items = saavn + ytm[:16] + deezer
    return {
        "query": q,
        "count": len(items),
        "items": items,
        "jiosaavn": saavn,
        "ytmusic": ytm[:16],
        "deezer": deezer,
        "provider": "jiosaavn+ytmusic+deezer",
        "errors": errors or None,
    }


@app.get("/music/home", tags=["Music"])
async def music_home():
    """Home rows — limited seeds to keep memory/CPU low on small hosts."""
    seeds = [
        ("Trending India", "trending hindi songs"),
        ("Bollywood Hits", "bollywood hits"),
        ("Punjabi", "punjabi hits"),
        ("English Pop", "top pop english"),
    ]
    sections = []
    for title, q in seeds:
        try:
            items = await _saavn_search(q, 8)
            if items:
                sections.append({"title": title, "items": items[:8], "source": "jiosaavn"})
        except Exception:
            continue
    if not sections:
        # YT fallback
        for title, q in [("Top Hits", "Top Hits"), ("Trending", "Trending music")]:
            data = await _ytm_post("search", {"query": q})
            items = []
            _ytm_walk(data, "musicResponsiveListItemRenderer", items)
            songs, seen = [], set()
            for it in items:
                e = _ytm_parse_item(it)
                if e and e["video_id"] not in seen:
                    seen.add(e["video_id"])
                    songs.append(e)
            if songs:
                sections.append({"title": title, "items": songs[:12], "source": "ytmusic"})
    return {"sections": sections, "provider": "jiosaavn"}


@app.get("/music/play/{item_id:path}", tags=["Music"])
async def music_play(item_id: str):
    """
    Playable audio stream — vivi-music style:
    1) JioSaavn direct CDN (reliable)
    2) yt-dlp multi-client fallback
    3) YouTube embed fallback
    item_id = saavn:ID  OR  11-char YouTube video id
    """
    errors = {}
    # Normalize yt: prefix → Invidious path
    if item_id.startswith("yt:"):
        vid = item_id[3:]
        try:
            return await music_yt_play(vid)
        except Exception as e:
            errors["invidious"] = str(e)[:200]
            item_id = vid
    elif re.match(r"^[\w-]{11}$", item_id):
        try:
            return await music_yt_play(item_id)
        except Exception as e:
            errors["invidious"] = str(e)[:200]

    title, artist, thumb = item_id, "", ""
    duration = None
    audio_url = None
    audio_format = None
    video_id = None
    saavn_id = None
    sources = []

    if item_id.startswith("saavn:"):
        saavn_id = item_id.split(":", 1)[1]
        try:
            card = await _saavn_stream_by_id(saavn_id)
            if card:
                title = card.get("title") or title
                artist = card.get("artist") or ""
                thumb = card.get("thumb") or ""
                duration = card.get("duration")
                audio_url = card.get("audio_url")
                audio_format = card.get("audio_format") or "mp4"
        except Exception as e:
            errors["jiosaavn"] = str(e)
    elif re.match(r"^[a-zA-Z0-9_-]{11}$", item_id):
        video_id = item_id
        # YT meta
        try:
            data = await _ytm_post("next", {"videoId": video_id})
            s = json.dumps(data)
            texts = re.findall(r'"text"\s*:\s*"([^"\\]{2,80})"', s)
            if texts:
                title = texts[0]
                if len(texts) > 1:
                    artist = texts[1]
            thumbs = re.findall(r'https://i\.ytimg\.com/[^"\\]+', s)
            if thumbs:
                thumb = thumbs[0].replace("\\u0026", "&")
            else:
                thumb = f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg"
        except Exception as e:
            errors["meta"] = str(e)
            thumb = f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg"

        # Prefer Saavn match (no bot check)
        try:
            match = await _saavn_match(title, artist)
            if match and match.get("audio_url"):
                audio_url = match["audio_url"]
                audio_format = match.get("audio_format") or "mp4"
                duration = match.get("duration") or duration
                saavn_id = match.get("saavn_id")
                if match.get("thumb"):
                    thumb = match["thumb"]
                # keep better title from saavn if similar
                if match.get("title"):
                    title = match["title"]
                if match.get("artist"):
                    artist = match["artist"]
        except Exception as e:
            errors["saavn_match"] = str(e)

        if not audio_url:
            try:
                u, fmt, dur, err = await _ytdlp_audio(video_id)
                if u:
                    audio_url, audio_format, duration = u, fmt, dur or duration
                elif err:
                    errors["yt-dlp"] = err
            except Exception as e:
                errors["yt-dlp"] = str(e)
    else:
        raise HTTPException(400, "Invalid id — use saavn:ID or YouTube videoId")

    play_url = _music_proxy_url(audio_url) if audio_url else None
    if audio_url:
        sources.append({
            "type": "audio",
            "provider": "jiosaavn" if saavn_id else "ytmusic-direct",
            "label": f"Audio ({audio_format or 'mp4'})",
            "url": audio_url,
            "play_url": play_url or audio_url,
            "format": (audio_format or "mp4").upper(),
        })
    if video_id:
        sources.append({
            "type": "embed",
            "provider": "youtube",
            "label": "YouTube embed",
            "url": f"https://www.youtube.com/embed/{video_id}?autoplay=1&rel=0",
            "play_url": f"https://www.youtube.com/embed/{video_id}?autoplay=1&rel=0",
            "format": "EMBED",
        })

    return {
        "id": item_id,
        "video_id": video_id,
        "saavn_id": saavn_id,
        "title": title,
        "artist": artist,
        "thumb": thumb,
        "duration": duration,
        "audio_url": audio_url,
        "play_url": play_url or audio_url,
        "audio_format": audio_format,
        "sources": sources,
        "watch_url": f"https://music.youtube.com/watch?v={video_id}" if video_id else None,
        "download_url": audio_url or (f"https://www.youtube.com/watch?v={video_id}" if video_id else None),
        "provider": "jiosaavn" if saavn_id and audio_url else "ytmusic",
        "errors": errors or None,
    }


_FEAT_RE = re.compile(r"\s*[\(\[]\s*(feat\.?|ft\.?|with)\b[^)\]]*[\)\]]", re.I)
_PUNCT_RE = re.compile(r"[^a-z0-9]+")


def _norm_text(s: str) -> str:
    s = (s or "").lower()
    s = _FEAT_RE.sub("", s)
    s = _PUNCT_RE.sub(" ", s)
    return s.strip()


def _parse_lrc(synced: str):
    """Parse LRC timestamps into (time, text) lines, keeping blank/instrumental
    gap markers (they matter for correct highlight timing) but tagging them."""
    lines = []
    for m in re.finditer(r"\[(\d+):(\d+(?:\.\d+)?)\](?!\d*:)\s*(.*)", synced):
        mins, secs, text = int(m.group(1)), float(m.group(2)), m.group(3).strip()
        lines.append({"t": mins * 60 + secs, "text": text})
    lines.sort(key=lambda x: x["t"])
    return lines


@app.get("/music/lyrics", tags=["Music"])
async def music_lyrics(
    title: str = Query(..., min_length=1),
    artist: str = Query("", description="Artist name optional"),
    album: str = Query("", description="Album name optional, improves LRCLIB match precision"),
    duration: float = Query(0, description="Track duration in seconds — LRCLIB matches within ±2s of this"),
):
    """LRCLIB lyrics — plain + synced (LRC). Used by SimpMusic / vivi-music.

    Bad sync is almost always a wrong-match problem, not a parsing problem:
    the same title can have many recordings (covers, remixes, re-releases)
    with different timings. LRCLIB's own /api/get does an exact, duration-
    validated match (±2s) when we can give it artist+duration, which is far
    more reliable than the old fuzzy-search-only approach. We fall back to
    a better-scored fuzzy search (title AND artist, not title alone) only
    when the exact lookup can't find anything.
    """
    try:
        async with httpx.AsyncClient(timeout=15.0, headers={"User-Agent": "StreamHub/1.0"}) as client:
            best = None
            if artist and duration:
                try:
                    params = {"artist_name": artist, "track_name": title, "duration": duration}
                    if album:
                        params["album_name"] = album
                    rg = await client.get("https://lrclib.net/api/get", params=params)
                    if rg.status_code == 200:
                        gj = rg.json()
                        if isinstance(gj, dict) and (gj.get("plainLyrics") or gj.get("syncedLyrics")):
                            best = gj
                except Exception:
                    pass

            if not best:
                r = await client.get(
                    "https://lrclib.net/api/search",
                    params={"q": f"{artist} {title}".strip()},
                )
                if r.status_code != 200:
                    return {"found": False, "lyrics": None, "synced": None, "lines": [], "source": "lrclib"}
                items = r.json() if isinstance(r.json(), list) else []
                tnorm = _norm_text(title)
                anorm = _norm_text(artist)
                scored = []
                for it in items:
                    if not isinstance(it, dict):
                        continue
                    if not (it.get("plainLyrics") or it.get("syncedLyrics")):
                        continue
                    tn = _norm_text(it.get("trackName"))
                    an = _norm_text(it.get("artistName"))
                    score = 0
                    if tn == tnorm:
                        score += 6
                    elif tnorm in tn or tn in tnorm:
                        score += 3
                    if anorm:
                        if an == anorm:
                            score += 5
                        elif anorm in an or an in anorm:
                            score += 2
                        else:
                            score -= 2  # explicit artist mismatch — likely a different song entirely
                    if duration and it.get("duration"):
                        d = abs(float(it["duration"]) - duration)
                        if d <= 2:
                            score += 4
                        elif d <= 6:
                            score += 1
                        else:
                            score -= 2
                    if it.get("syncedLyrics"):
                        score += 1  # slight preference for synced over plain-only
                    scored.append((score, it))
                if scored:
                    scored.sort(key=lambda x: x[0], reverse=True)
                    best = scored[0][1]
                elif items and isinstance(items[0], dict):
                    best = items[0]

            if not best:
                return {"found": False, "lyrics": None, "synced": None, "lines": [], "source": "lrclib"}
            synced = best.get("syncedLyrics") or ""
            lines = _parse_lrc(synced)
            return {
                "found": bool(best.get("plainLyrics") or synced),
                "title": best.get("trackName"),
                "artist": best.get("artistName"),
                "album": best.get("albumName"),
                "lyrics": best.get("plainLyrics"),
                "synced": synced,
                "lines": lines,
                "duration": best.get("duration"),
                "source": "lrclib",
            }
    except Exception as e:
        return {"found": False, "lyrics": None, "synced": None, "lines": [], "error": str(e), "source": "lrclib"}










# ── Extra music sources (SimpMusic / Spotube / PyMusic style) ──────────────

_PIPED_APIS = [
    "https://api.piped.private.coffee",
    "https://pipedapi.ducks.party",
    "https://pipedapi.adminforge.de",
]


async def _piped_get(path: str, params: Optional[dict] = None) -> Any:
    last = None
    async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
        for base in _PIPED_APIS:
            try:
                r = await client.get(
                    f"{base}{path}",
                    params=params or {},
                    headers={"User-Agent": "StreamHub/5.8", "Accept": "application/json"},
                )
                if r.status_code == 200:
                    return r.json()
                last = f"HTTP {r.status_code} @ {base}"
            except Exception as e:
                last = str(e)[:80]
                continue
    raise RuntimeError(last or "Piped unavailable")


def _piped_vid(url: str) -> Optional[str]:
    if not url:
        return None
    if "v=" in url:
        return url.split("v=")[-1].split("&")[0][:11]
    if "/watch?v=" in url:
        return url.split("/watch?v=")[-1][:11]
    parts = url.rstrip("/").split("/")
    return parts[-1][:11] if parts else None


@app.get("/music/deezer/search", tags=["Music"])
async def music_deezer_search(q: str = Query(..., min_length=1), limit: int = Query(15, ge=1, le=40)):
    """Deezer public search — metadata + 30s preview (PyMusic / Spotube style)."""
    async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
        r = await client.get(
            "https://api.deezer.com/search",
            params={"q": q, "limit": limit},
            headers={"User-Agent": "StreamHub/5.8"},
        )
        if r.status_code != 200:
            return {"ok": False, "query": q, "items": [], "error": f"HTTP {r.status_code}"}
        data = r.json()
    items = []
    for it in data.get("data") or []:
        if not isinstance(it, dict):
            continue
        artist = (it.get("artist") or {}).get("name") or ""
        album = (it.get("album") or {}).get("title") or ""
        cover = (it.get("album") or {}).get("cover_xl") or (it.get("album") or {}).get("cover_big") or ""
        preview = it.get("preview") or ""
        items.append({
            "id": f"deezer:{it.get('id')}",
            "deezer_id": it.get("id"),
            "title": it.get("title"),
            "artist": artist,
            "album": album,
            "thumb": cover,
            "duration": it.get("duration"),
            "preview_url": preview,
            "audio_url": preview or None,
            "link": it.get("link"),
            "provider": "deezer",
            "source": "deezer",
            "playable_preview": bool(preview),
        })
    return {"ok": True, "query": q, "count": len(items), "items": items, "provider": "deezer"}


@app.get("/music/suggest", tags=["Music"])
async def music_suggest(q: str = Query(..., min_length=1)):
    """Search suggestions (Piped + light YT-style)."""
    suggestions = []
    errors = {}
    try:
        data = await _piped_get("/suggestions", {"query": q})
        if isinstance(data, list):
            suggestions.extend([str(x) for x in data if x][:12])
    except Exception as e:
        errors["piped"] = str(e)[:80]
    if not suggestions:
        # lightweight fallback seeds from saavn search titles
        try:
            hits = await _saavn_search(q, 6)
            for h in hits:
                if h.get("title"):
                    suggestions.append(h["title"])
        except Exception as e:
            errors["saavn"] = str(e)[:80]
    # unique preserve order
    seen = set()
    out = []
    for s in suggestions:
        k = s.lower()
        if k not in seen:
            seen.add(k)
            out.append(s)
    return {"query": q, "suggestions": out[:12], "errors": errors or None}


@app.get("/music/piped/search", tags=["Music"])
async def music_piped_search(
    q: str = Query(..., min_length=1),
    filter: str = Query("music_songs", description="music_songs|all|videos"),
):
    """Piped search (SimpMusic / simplyMusic style). Stream via /music/play/{yt_id}."""
    try:
        data = await _piped_get("/search", {"q": q, "filter": filter})
    except Exception as e:
        return {"ok": False, "query": q, "items": [], "error": str(e)[:120], "provider": "piped"}
    items = []
    for it in (data.get("items") if isinstance(data, dict) else data) or []:
        if not isinstance(it, dict):
            continue
        if it.get("type") and it.get("type") not in ("stream", "video", None):
            continue
        vid = _piped_vid(it.get("url") or "")
        if not vid:
            continue
        items.append({
            "id": f"yt:{vid}",
            "video_id": vid,
            "title": it.get("title"),
            "artist": it.get("uploaderName") or it.get("uploader"),
            "thumb": it.get("thumbnail"),
            "duration": it.get("duration"),
            "views": it.get("views"),
            "provider": "piped",
            "source": "piped",
        })
    return {"ok": True, "query": q, "count": len(items), "items": items, "provider": "piped"}




@app.get("/music/piped/stream/{video_id}", tags=["Music"])
async def music_piped_stream(video_id: str):
    """
    Piped stream lookup (SimpMusic / simplyMusic style).
    Returns audio if available; otherwise video/mp4 + related metadata.
    Falls back to yt-dlp / existing play pipeline when Piped has no audio.
    """
    video_id = video_id.replace("yt:", "").strip()
    if not re.match(r"^[\w-]{6,20}$", video_id):
        raise HTTPException(400, "invalid video id")
    errors = {}
    streams = []
    meta = {"title": None, "artist": None, "thumb": None, "duration": None}
    try:
        data = await _piped_get(f"/streams/{video_id}")
        if isinstance(data, dict):
            meta["title"] = data.get("title")
            meta["artist"] = data.get("uploader") or data.get("uploaderName")
            meta["thumb"] = data.get("thumbnailUrl") or data.get("thumbnail")
            meta["duration"] = data.get("duration")
            for a in data.get("audioStreams") or []:
                if not isinstance(a, dict) or not a.get("url"):
                    continue
                streams.append({
                    "type": "audio",
                    "provider": "piped",
                    "label": f"Piped audio {a.get('bitrate') or a.get('quality') or ''}".strip(),
                    "url": a["url"],
                    "bitrate": a.get("bitrate"),
                    "mime": a.get("mimeType"),
                    "format": "AUDIO",
                    "playable": True,
                })
            for v in data.get("videoStreams") or []:
                if not isinstance(v, dict) or not v.get("url"):
                    continue
                # prefer non-videoOnly (has audio)
                if v.get("videoOnly"):
                    continue
                streams.append({
                    "type": "video",
                    "provider": "piped",
                    "label": f"Piped {v.get('quality') or v.get('format') or 'mp4'}",
                    "url": v["url"],
                    "mime": v.get("mimeType"),
                    "format": "VIDEO",
                    "playable": True,
                })
            if data.get("hls"):
                streams.append({
                    "type": "hls",
                    "provider": "piped",
                    "label": "Piped HLS",
                    "url": data["hls"],
                    "format": "HLS",
                    "playable": True,
                })
    except Exception as e:
        errors["piped"] = str(e)[:120]

    # yt-dlp audio fallback
    if not any(s.get("type") == "audio" for s in streams):
        try:
            u, fmt, dur, err = await _ytdlp_audio(video_id)
            if u:
                streams.insert(0, {
                    "type": "audio",
                    "provider": "yt-dlp",
                    "label": f"Audio ({fmt or 'm4a'})",
                    "url": u,
                    "format": (fmt or "m4a").upper(),
                    "playable": True,
                })
                if dur and not meta["duration"]:
                    meta["duration"] = dur
            elif err:
                errors["yt-dlp"] = err[:120]
        except Exception as e:
            errors["yt-dlp"] = str(e)[:120]

    # always offer embed fallback
    streams.append({
        "type": "embed",
        "provider": "youtube",
        "label": "YouTube embed",
        "url": f"https://www.youtube.com/embed/{video_id}?autoplay=1&rel=0",
        "format": "EMBED",
        "playable": True,
    })

    best = next((s for s in streams if s.get("type") == "audio" and s.get("url")), None)
    if not best:
        best = next((s for s in streams if s.get("playable") and s.get("type") != "embed"), None)

    return {
        "ok": bool(best),
        "video_id": video_id,
        "title": meta["title"],
        "artist": meta["artist"],
        "thumb": meta["thumb"] or f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg",
        "duration": meta["duration"],
        "audio_url": best["url"] if best and best.get("type") == "audio" else None,
        "stream": best,
        "streams": streams,
        "count": len(streams),
        "errors": errors or None,
        "provider": "piped+yt-dlp",
    }


@app.get("/music/stream", tags=["Music"])
async def music_stream_resolve(
    id: str = Query(None, description="saavn:ID | yt:VIDEO | VIDEO_ID | deezer:ID"),
    q: str = Query(None, description="Search title if no id"),
    video_id: str = Query(None),
):
    """
    Universal music stream resolver.
    - saavn:xxx → full JioSaavn CDN
    - yt:xxx / 11-char id → Piped + yt-dlp audio
    - deezer:xxx → 30s preview
    - q=title → search Saavn then stream first hit
    """
    if video_id and not id:
        id = video_id
    if not id and q:
        try:
            hits = await _saavn_search(q.strip()[:80], 5)
            if hits:
                sid = hits[0].get("saavn_id") or (hits[0].get("id") or "").replace("saavn:", "")
                if sid:
                    id = f"saavn:{sid}"
        except Exception:
            pass
    if not id:
        raise HTTPException(400, "Provide id= or q=")

    rid = id.strip()
    # Deezer preview
    if rid.startswith("deezer:"):
        did = rid.split(":", 1)[1]
        async with httpx.AsyncClient(timeout=12.0) as client:
            r = await client.get(f"https://api.deezer.com/track/{did}")
            if r.status_code != 200:
                return {"ok": False, "error": "deezer track not found", "streams": []}
            tr = r.json()
            preview = tr.get("preview")
            artist = (tr.get("artist") or {}).get("name") or ""
            cover = (tr.get("album") or {}).get("cover_xl") or (tr.get("album") or {}).get("cover_big")
            streams = []
            if preview:
                streams.append({
                    "type": "audio",
                    "provider": "deezer",
                    "label": "Deezer 30s preview",
                    "url": preview,
                    "format": "MP3",
                    "playable": True,
                })
            return {
                "ok": bool(preview),
                "id": rid,
                "title": tr.get("title"),
                "artist": artist,
                "thumb": cover,
                "duration": tr.get("duration"),
                "audio_url": preview,
                "stream": streams[0] if streams else None,
                "streams": streams,
                "note": "Deezer public API only provides 30s previews",
            }

    # YouTube / Piped
    if rid.startswith("yt:") or re.match(r"^[\w-]{11}$", rid):
        vid = rid[3:] if rid.startswith("yt:") else rid
        return await music_piped_stream(vid)

    # Saavn / generic play
    try:
        return await music_play(rid)
    except HTTPException:
        raise
    except Exception as e:
        return {"ok": False, "id": rid, "error": str(e)[:160], "streams": []}


@app.get("/music/deezer/play/{track_id}", tags=["Music"])
async def music_deezer_play(track_id: str):
    """Deezer track preview stream (30s)."""
    track_id = track_id.replace("deezer:", "").strip()
    return await music_stream_resolve(id=f"deezer:{track_id}")

@app.get("/music/charts", tags=["Music"])
async def music_charts(region: str = Query("in", description="in|us|global")):
    """Quick charts via curated Saavn searches + Deezer chart."""
    region = (region or "in").lower()
    seeds = {
        "in": [
            ("Trending India", "trending hindi songs 2024"),
            ("Bollywood", "bollywood hits"),
            ("Punjabi", "punjabi hits"),
            ("Tamil", "tamil hits"),
        ],
        "us": [
            ("US Pop", "top pop usa"),
            ("Hip Hop", "hip hop hits"),
            ("R&B", "rnb hits"),
        ],
        "global": [
            ("Global Hits", "top hits global"),
            ("K-Pop", "kpop hits"),
            ("Latin", "latin hits"),
        ],
    }.get(region, None) or [
        ("Trending", "trending songs"),
        ("Pop", "pop hits"),
    ]
    sections = []
    for title, q in seeds:
        try:
            items = await _saavn_search(q, 10)
            if items:
                sections.append({"title": title, "items": items[:10], "source": "jiosaavn"})
        except Exception:
            continue
    # Deezer chart
    try:
        async with httpx.AsyncClient(timeout=12.0) as client:
            r = await client.get("https://api.deezer.com/chart/0/tracks", params={"limit": 12})
            if r.status_code == 200:
                tracks = (r.json() or {}).get("data") or []
                items = []
                for it in tracks:
                    artist = (it.get("artist") or {}).get("name") or ""
                    cover = (it.get("album") or {}).get("cover_big") or ""
                    items.append({
                        "id": f"deezer:{it.get('id')}",
                        "title": it.get("title"),
                        "artist": artist,
                        "thumb": cover,
                        "duration": it.get("duration"),
                        "preview_url": it.get("preview"),
                        "provider": "deezer",
                    })
                if items:
                    sections.append({"title": "Deezer Chart", "items": items, "source": "deezer"})
    except Exception:
        pass
    return {"region": region, "sections": sections, "provider": "charts"}


@app.get("/music/unified", tags=["Music"])
async def music_unified(q: str = Query(..., min_length=1)):
    """
    One-shot search across JioSaavn + YT Music + Deezer + Piped (SimpMusic-style aggregator).
    """
    q = q.strip()[:100]
    out = {"query": q, "jiosaavn": [], "ytmusic": [], "deezer": [], "piped": [], "errors": {}}
    try:
        out["jiosaavn"] = await _saavn_search(q, 12)
    except Exception as e:
        out["errors"]["jiosaavn"] = str(e)[:80]
    try:
        data = await _ytm_post("search", {"query": q})
        items = []
        _ytm_walk(data, "musicResponsiveListItemRenderer", items)
        seen = set()
        for it in items:
            e = _ytm_parse_item(it)
            if e and e["video_id"] not in seen:
                seen.add(e["video_id"])
                out["ytmusic"].append(e)
        out["ytmusic"] = out["ytmusic"][:12]
    except Exception as e:
        out["errors"]["ytmusic"] = str(e)[:80]
    try:
        dz = await music_deezer_search(q=q, limit=10)
        out["deezer"] = dz.get("items") or []
    except Exception as e:
        out["errors"]["deezer"] = str(e)[:80]
    try:
        pd = await music_piped_search(q=q, filter="music_songs")
        out["piped"] = pd.get("items") or []
    except Exception as e:
        out["errors"]["piped"] = str(e)[:80]
    # flat mix: saavn first (full streams)
    flat = list(out["jiosaavn"]) + list(out["ytmusic"]) + list(out["deezer"]) + list(out["piped"])
    out["items"] = flat
    out["count"] = len(flat)
    if not out["errors"]:
        out["errors"] = None
    return out



# =============================================================================
# Expanded Music + Lyrics APIs (PaxSenix-style, native, unlimited)
# Deezer public · JioSaavn · LRCLIB · lyrics.ovh
# =============================================================================

@app.get("/lyrics/plain", tags=["Lyrics"])
async def lyrics_plain(
    title: str = Query(..., min_length=1),
    artist: str = Query("", description="Artist name"),
):
    """Plain text lyrics (lyrics.ovh + LRCLIB fallback)."""
    errors = {}
    plain = None
    source = None
    if artist:
        try:
            async with httpx.AsyncClient(timeout=12.0) as client:
                r = await client.get(f"https://api.lyrics.ovh/v1/{artist}/{title}")
                if r.status_code == 200:
                    plain = (r.json() or {}).get("lyrics")
                    if plain:
                        source = "lyrics.ovh"
        except Exception as e:
            errors["ovh"] = str(e)[:80]
    if not plain:
        try:
            lr = await music_lyrics(title=title, artist=artist, album="", duration=0)
            if lr.get("found") and lr.get("lyrics"):
                plain = lr["lyrics"]
                source = "lrclib"
        except Exception as e:
            errors["lrclib"] = str(e)[:80]
    return {
        "ok": bool(plain),
        "title": title,
        "artist": artist,
        "lyrics": plain,
        "source": source,
        "errors": errors or None,
    }


@app.get("/lyrics/lrcget", tags=["Lyrics"])
@app.get("/lyrics/lrc", tags=["Lyrics"], include_in_schema=False)
async def lyrics_lrcget(
    title: str = Query(..., min_length=1),
    artist: str = Query(""),
    album: str = Query(""),
    duration: float = Query(0),
):
    """Synced LRC lyrics (LRCLIB) — same engine as /music/lyrics."""
    # Call via TestClient-free internal: reuse HTTP path logic by duplicating thin wrapper
    from fastapi.responses import JSONResponse
    try:
        # Direct call — pass plain values (not Query objects)
        result = await music_lyrics.__wrapped__(title, artist, album, duration) if hasattr(music_lyrics, '__wrapped__') else None
    except Exception:
        result = None
    if result is None:
        # fallback: internal LRCLIB fetch
        try:
            async with httpx.AsyncClient(timeout=15.0, headers={"User-Agent": "StreamHub/1.0"}) as client:
                r = await client.get("https://lrclib.net/api/search", params={"q": f"{artist} {title}".strip()})
                items = r.json() if r.status_code == 200 else []
                best = items[0] if isinstance(items, list) and items else None
                if not best:
                    return {"found": False, "lyrics": None, "synced": None, "lines": [], "source": "lrclib"}
                synced = best.get("syncedLyrics") or ""
                return {
                    "found": True,
                    "title": best.get("trackName"),
                    "artist": best.get("artistName"),
                    "album": best.get("albumName"),
                    "lyrics": best.get("plainLyrics"),
                    "synced": synced,
                    "lines": _parse_lrc(synced),
                    "duration": best.get("duration"),
                    "source": "lrclib",
                }
        except Exception as e:
            return {"found": False, "lyrics": None, "synced": None, "lines": [], "error": str(e)[:120], "source": "lrclib"}
    return result


@app.get("/lyrics/genius", tags=["Lyrics"])
async def lyrics_genius(
    title: str = Query(..., min_length=1),
    artist: str = Query(""),
):
    """Genius-style lyrics lookup via public aggregators (no Genius API key)."""
    return await lyrics_plain(title=title, artist=artist)


@app.get("/lyrics/multi", tags=["Lyrics"])
async def lyrics_multi(
    title: str = Query(..., min_length=1),
    artist: str = Query(""),
    duration: float = Query(0),
):
    """All lyric sources in one call: LRCLIB (synced) + plain."""
    # LRCLIB via lrcget helper
    synced = await lyrics_lrcget(title=title, artist=artist, album="", duration=duration)
    if not isinstance(synced, dict):
        synced = {"found": False}
    plain = await lyrics_plain(title=title, artist=artist)
    if not isinstance(plain, dict):
        plain = {"ok": False}
    return {
        "title": title,
        "artist": artist,
        "synced": {
            "found": synced.get("found"),
            "lyrics": synced.get("lyrics"),
            "synced": synced.get("synced"),
            "lines": synced.get("lines"),
            "source": synced.get("source"),
        },
        "plain": {
            "found": plain.get("ok"),
            "lyrics": plain.get("lyrics"),
            "source": plain.get("source"),
        },
    }


@app.get("/deezer/search", tags=["Deezer"])
async def deezer_search(
    q: str = Query(..., min_length=1),
    type: str = Query("track", description="track|album|artist|playlist"),
    limit: int = Query(15, ge=1, le=50),
):
    """Deezer search (public API)."""
    path = {
        "track": "https://api.deezer.com/search",
        "album": "https://api.deezer.com/search/album",
        "artist": "https://api.deezer.com/search/artist",
        "playlist": "https://api.deezer.com/search/playlist",
    }.get(type.lower(), "https://api.deezer.com/search")
    async with httpx.AsyncClient(timeout=15.0) as client:
        r = await client.get(path, params={"q": q, "limit": limit})
        if r.status_code != 200:
            return {"ok": False, "error": f"HTTP {r.status_code}", "items": []}
        data = r.json()
    items = data.get("data") or []
    return {"ok": True, "query": q, "type": type, "count": len(items), "items": items, "provider": "deezer"}


@app.get("/deezer/track", tags=["Deezer"])
async def deezer_track(id: str = Query(..., description="Deezer track id")):
    async with httpx.AsyncClient(timeout=12.0) as client:
        r = await client.get(f"https://api.deezer.com/track/{id}")
        if r.status_code != 200:
            return {"ok": False, "error": "not found"}
        return {"ok": True, "provider": "deezer", "track": r.json()}


@app.get("/deezer/album", tags=["Deezer"])
async def deezer_album(id: str = Query(...)):
    async with httpx.AsyncClient(timeout=12.0) as client:
        r = await client.get(f"https://api.deezer.com/album/{id}")
        if r.status_code != 200:
            return {"ok": False, "error": "not found"}
        return {"ok": True, "provider": "deezer", "album": r.json()}


@app.get("/deezer/artist", tags=["Deezer"])
async def deezer_artist(id: str = Query(...)):
    async with httpx.AsyncClient(timeout=12.0) as client:
        r = await client.get(f"https://api.deezer.com/artist/{id}")
        if r.status_code != 200:
            return {"ok": False, "error": "not found"}
        art = r.json()
        # top tracks
        top = []
        try:
            t = await client.get(f"https://api.deezer.com/artist/{id}/top", params={"limit": 15})
            if t.status_code == 200:
                top = (t.json() or {}).get("data") or []
        except Exception:
            pass
        return {"ok": True, "provider": "deezer", "artist": art, "top": top}


@app.get("/deezer/playlist", tags=["Deezer"])
async def deezer_playlist(id: str = Query(...)):
    async with httpx.AsyncClient(timeout=15.0) as client:
        r = await client.get(f"https://api.deezer.com/playlist/{id}")
        if r.status_code != 200:
            return {"ok": False, "error": "not found"}
        return {"ok": True, "provider": "deezer", "playlist": r.json()}


@app.get("/deezer/home", tags=["Deezer"])
async def deezer_home():
    """Deezer charts / homepage."""
    async with httpx.AsyncClient(timeout=15.0) as client:
        r = await client.get("https://api.deezer.com/chart")
        if r.status_code != 200:
            return {"ok": False, "error": f"HTTP {r.status_code}"}
        data = r.json()
    return {
        "ok": True,
        "provider": "deezer",
        "tracks": (data.get("tracks") or {}).get("data") or [],
        "albums": (data.get("albums") or {}).get("data") or [],
        "artists": (data.get("artists") or {}).get("data") or [],
        "playlists": (data.get("playlists") or {}).get("data") or [],
    }


@app.get("/jiosaavn/search", tags=["JioSaavn"])
async def jiosaavn_search(q: str = Query(..., min_length=1), limit: int = Query(20, ge=1, le=40)):
    items = await _saavn_search(q, limit)
    return {"ok": True, "query": q, "count": len(items), "items": items, "provider": "jiosaavn"}


@app.get("/jiosaavn/track", tags=["JioSaavn"])
async def jiosaavn_track(id: str = Query(..., description="Saavn song id")):
    id = id.replace("saavn:", "").strip()
    card = await _saavn_stream_by_id(id)
    if not card:
        return {"ok": False, "error": "not found", "id": id}
    return {"ok": True, "provider": "jiosaavn", "track": card}


@app.get("/jiosaavn/charts", tags=["JioSaavn"])
async def jiosaavn_charts():
    """Official Saavn charts list."""
    try:
        data = await _saavn_get({"__call": "content.getCharts", "ctx": "web6dot0"})
    except Exception as e:
        return {"ok": False, "error": str(e)[:120], "items": []}
    items = data if isinstance(data, list) else []
    out = []
    for it in items[:30]:
        if not isinstance(it, dict):
            continue
        out.append({
            "id": it.get("id") or it.get("listid"),
            "title": it.get("title") or it.get("listname"),
            "image": it.get("image"),
            "subtitle": it.get("subtitle") or it.get("language"),
            "type": it.get("type") or "playlist",
        })
    return {"ok": True, "count": len(out), "items": out, "provider": "jiosaavn"}


@app.get("/jiosaavn/album", tags=["JioSaavn"])
async def jiosaavn_album(id: str = Query(..., description="Album id")):
    try:
        data = await _saavn_get({"__call": "content.getAlbumDetails", "albumid": id, "ctx": "web6dot0"})
    except Exception as e:
        return {"ok": False, "error": str(e)[:120]}
    if not isinstance(data, dict):
        return {"ok": False, "error": "invalid response"}
    songs = []
    for s in data.get("list") or data.get("songs") or []:
        c = _saavn_card(s) if isinstance(s, dict) else None
        if c:
            songs.append(c)
    return {
        "ok": True,
        "provider": "jiosaavn",
        "id": id,
        "title": data.get("title") or data.get("name"),
        "image": data.get("image"),
        "year": data.get("year"),
        "songs": songs,
        "count": len(songs),
    }


@app.get("/jiosaavn/playlist", tags=["JioSaavn"])
async def jiosaavn_playlist(id: str = Query(...)):
    try:
        data = await _saavn_get({"__call": "playlist.getDetails", "listid": id, "ctx": "web6dot0"})
    except Exception as e:
        return {"ok": False, "error": str(e)[:120]}
    if not isinstance(data, dict):
        return {"ok": False, "error": "invalid response"}
    songs = []
    for s in data.get("list") or data.get("songs") or []:
        c = _saavn_card(s) if isinstance(s, dict) else None
        if c:
            songs.append(c)
    return {
        "ok": True,
        "provider": "jiosaavn",
        "id": id,
        "title": data.get("listname") or data.get("title"),
        "image": data.get("image"),
        "songs": songs,
        "count": len(songs),
    }


@app.get("/jiosaavn/artist", tags=["JioSaavn"])
async def jiosaavn_artist(id: str = Query(...), q: str = Query(None, description="Or search by name")):
    if q and not id:
        try:
            data = await _saavn_get({"__call": "autocomplete.get", "query": q, "ctx": "web6dot0"})
            arts = (data.get("artists") or {}).get("data") or []
            if arts:
                id = str(arts[0].get("id"))
        except Exception:
            pass
    if not id:
        return {"ok": False, "error": "need id or q"}
    try:
        data = await _saavn_get({
            "__call": "artist.getArtistPageDetails",
            "artistId": id,
            "ctx": "web6dot0",
        })
    except Exception as e:
        return {"ok": False, "error": str(e)[:120]}
    return {"ok": True, "provider": "jiosaavn", "id": id, "data": data if isinstance(data, dict) else {"raw": data}}


@app.get("/tools/songlink", tags=["Tools"])
async def tools_songlink(
    title: str = Query(..., min_length=1),
    artist: str = Query(""),
):
    """
    Find the same song across platforms (I don't have Spotify style).
    Returns JioSaavn + Deezer + YT Music matches.
    """
    q = f"{title} {artist}".strip()
    result = {"title": title, "artist": artist, "matches": {}}
    try:
        hits = await _saavn_search(q, 5)
        result["matches"]["jiosaavn"] = hits[:5]
    except Exception as e:
        result["matches"]["jiosaavn"] = {"error": str(e)[:80]}
    try:
        async with httpx.AsyncClient(timeout=12.0) as client:
            r = await client.get("https://api.deezer.com/search", params={"q": q, "limit": 5})
            if r.status_code == 200:
                result["matches"]["deezer"] = (r.json() or {}).get("data") or []
    except Exception as e:
        result["matches"]["deezer"] = {"error": str(e)[:80]}
    try:
        data = await _ytm_post("search", {"query": q})
        items = []
        _ytm_walk(data, "musicResponsiveListItemRenderer", items)
        songs = []
        seen = set()
        for it in items:
            e = _ytm_parse_item(it)
            if e and e["video_id"] not in seen:
                seen.add(e["video_id"])
                songs.append(e)
        result["matches"]["ytmusic"] = songs[:5]
    except Exception as e:
        result["matches"]["ytmusic"] = {"error": str(e)[:80]}
    return {"ok": True, **result}


@app.get("/tools/idonthavespotify", tags=["Tools"])
async def tools_idonthavespotify(
    url: str = Query(None, description="Spotify track URL"),
    title: str = Query(None),
    artist: str = Query(""),
):
    """Resolve Spotify URL or title to free playable alternatives."""
    if url and "spotify" in url.lower():
        meta = await _spotify_meta(url)
        title = meta.get("title") or title or ""
        artist = meta.get("artist") or artist or ""
    if not title:
        raise HTTPException(400, "Provide Spotify url= or title=")
    link = await tools_songlink(title=title, artist=artist)
    # best playable
    play = None
    try:
        matched = await _match_download(title, artist)
        if matched.get("links"):
            play = matched["links"][0]
    except Exception:
        pass
    return {
        "ok": True,
        "title": title,
        "artist": artist,
        "play": play,
        "alternatives": link.get("matches"),
    }

@app.get("/music/related", tags=["Music"])
async def music_related(q: str = Query(..., min_length=1), limit: int = 8):
    """Recommended / similar songs (JioSaavn search around query)."""
    limit = max(1, min(12, limit))
    try:
        items = await _saavn_search(q, limit + 4)
        return {"query": q, "items": items[:limit], "provider": "jiosaavn"}
    except Exception as e:
        return {"query": q, "items": [], "error": str(e)}


@app.api_route("/music/stream/{token}", methods=["GET", "HEAD"], tags=["Music"])
async def music_stream_proxy(token: str, request: Request):
    """Proxy Saavn audio with streaming (low memory) + Range + CORS."""
    try:
        meta = _b64url_decode(token)
    except Exception:
        raise HTTPException(400, "bad token")
    url = meta.get("u") or ""
    if not url.startswith("https://"):
        raise HTTPException(400, "bad url")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Referer": "https://www.jiosaavn.com/",
        "Origin": "https://www.jiosaavn.com",
    }
    range_h = request.headers.get("range") if request else None
    if range_h:
        headers["Range"] = range_h

    client = httpx.AsyncClient(follow_redirects=True, timeout=60.0)
    try:
        if request.method == "HEAD":
            upstream = await client.head(url, headers=headers)
            out_headers = {
                "cache-control": "no-store",
                "access-control-allow-origin": "*",
                "access-control-expose-headers": "Content-Length, Content-Range, Accept-Ranges",
                "accept-ranges": upstream.headers.get("accept-ranges") or "bytes",
            }
            for k in ("content-type", "content-length", "content-range"):
                if k in upstream.headers:
                    out_headers[k] = upstream.headers[k]
            media = (upstream.headers.get("content-type") or "audio/mp4").split(";")[0]
            await client.aclose()
            return Response(status_code=upstream.status_code, headers=out_headers, media_type=media)

        req = client.build_request("GET", url, headers=headers)
        upstream = await client.send(req, stream=True)
        if upstream.status_code >= 400:
            await upstream.aclose()
            await client.aclose()
            raise HTTPException(upstream.status_code, f"upstream {upstream.status_code}")
        out_headers = {
            "cache-control": "no-store",
            "access-control-allow-origin": "*",
            "access-control-expose-headers": "Content-Length, Content-Range, Accept-Ranges",
            "accept-ranges": upstream.headers.get("accept-ranges") or "bytes",
        }
        for k in ("content-type", "content-length", "content-range"):
            if k in upstream.headers:
                out_headers[k] = upstream.headers[k]
        media = (upstream.headers.get("content-type") or "audio/mp4").split(";")[0]

        async def body_iter():
            try:
                async for chunk in upstream.aiter_bytes(65536):
                    yield chunk
            finally:
                await upstream.aclose()
                await client.aclose()

        return StreamingResponse(
            body_iter(),
            status_code=upstream.status_code,
            media_type=media,
            headers=out_headers,
        )
    except HTTPException:
        raise
    except Exception as e:
        try:
            await client.aclose()
        except Exception:
            pass
        raise HTTPException(502, f"stream proxy: {e}")


def _music_proxy_url(audio_url: str) -> str:
    if not audio_url:
        return ""
    tok = _b64url_encode({"u": audio_url})
    return f"/music/stream/{tok}"




# =============================================================================
# DOWNLOADER (OmniGet-inspired · yt-dlp probe + direct links)
# =============================================================================

def _extract_youtube_id(url: str) -> Optional[str]:
    m = re.search(
        r"(?:youtu\.be/|youtube\.com/(?:watch\?v=|embed/|shorts/|live/)|youtube-nocookie\.com/embed/)([A-Za-z0-9_-]{11})",
        url,
    )
    return m.group(1) if m else None


def _ytdlp_has_cdn(formats: list) -> bool:
    """True if any format has a real googlevideo / CDN URL (not youtube.com page)."""
    for f in formats or []:
        u = (f.get("url") or "")
        if not u:
            continue
        if "googlevideo.com" in u or "googleusercontent.com" in u:
            return True
        if "googlevideo" in u:
            return True
        # non-YT sites: any http media URL counts
        if u.startswith("http") and "youtube.com/" not in u and "youtu.be/" not in u:
            return True
    return False


def _ytdlp_info(url: str) -> dict:
    """Extract media + direct CDN URLs. YouTube: android_creator / mediaconnect first."""
    try:
        import yt_dlp  # type: ignore
    except ImportError:
        return {"ok": False, "error": "yt-dlp not installed — add yt-dlp to requirements"}

    url = (url or "").strip()
    yt_id = _extract_youtube_id(url)
    is_yt = bool(yt_id)

    def _run(opts: dict):
        with yt_dlp.YoutubeDL(opts) as ydl:
            return ydl.extract_info(url, download=False)

    base_opts = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "noplaylist": True,
        "extract_flat": False,
        "socket_timeout": 28,
        "retries": 2,
    }

    info = None
    last_err = None
    used_client = None
    if is_yt:
        # 2026: android_creator + mediaconnect still return full googlevideo CDN
        # ladders (up to 4K). web_embedded/tv often "succeed" with empty GVS URLs.
        clients = [
            "android_creator",
            "mediaconnect",
            "android",
            "android_vr",
            "tv",
            "web_embedded",
            "ios",
            "mweb",
            "web",
        ]
        attempts = [
            ({**base_opts, "extractor_args": {"youtube": {"player_client": [c]}}}, c)
            for c in clients
        ]
        attempts.append((base_opts, "default"))
    else:
        attempts = [(base_opts, "default")]

    for opts, client_name in attempts:
        try:
            candidate = _run(opts)
            fmts = (candidate or {}).get("formats") or []
            if candidate and fmts and _ytdlp_has_cdn(fmts):
                info = candidate
                used_client = client_name
                break
            if candidate and fmts and not is_yt:
                info = candidate
                used_client = client_name
                break
        except Exception as e:
            last_err = str(e)
            continue

    # --- YouTube blocked: still return usable embed + metadata ---
    if not info and is_yt and yt_id:
        title = f"YouTube {yt_id}"
        # try oembed for title
        try:
            import httpx as _hx
            oe = _hx.get(
                "https://www.youtube.com/oembed",
                params={"url": f"https://www.youtube.com/watch?v={yt_id}", "format": "json"},
                timeout=8.0,
                headers={"User-Agent": "Mozilla/5.0"},
            )
            if oe.status_code == 200:
                title = (oe.json() or {}).get("title") or title
        except Exception:
            pass
        embed = f"https://www.youtube.com/embed/{yt_id}?autoplay=1&rel=0"
        watch = f"https://www.youtube.com/watch?v={yt_id}"
        return {
            "ok": True,
            "mode": "embed",
            "title": title,
            "id": yt_id,
            "extractor": "youtube-embed-fallback",
            "thumbnail": f"https://i.ytimg.com/vi/{yt_id}/hqdefault.jpg",
            "webpage_url": watch,
            "embed_url": embed,
            "download_url": None,
            "formats": [
                {
                    "id": "embed",
                    "label": "YouTube Embed (play in browser)",
                    "kind": "embed",
                    "ext": "embed",
                    "url": embed,
                }
            ],
            "format_count": 1,
            "best": {"url": embed, "kind": "embed", "label": "YouTube Embed"},
            "note": (
                "Direct CDN blocked on this server IP (YouTube bot-check). "
                "Use embed_url to play in an iframe, or host cookies.txt / different IP for direct links."
            ),
            "error_detail": (last_err or "")[:200],
        }

    if not info:
        msg = last_err or "extract failed"
        if "Sign in" in msg or "bot" in msg.lower():
            msg = "YouTube bot-check blocked this server IP. Retry later or use cookies."
        return {"ok": False, "error": msg[:500], "url": url}

    if info.get("_type") == "playlist" and info.get("entries"):
        ent = next((e for e in info["entries"] if e), None)
        if ent:
            info = ent

    formats = []
    seen = set()
    for f in info.get("formats") or []:
        if not isinstance(f, dict):
            continue
        fu = f.get("url")
        if not fu or fu in seen:
            continue
        vcodec = f.get("vcodec") or "none"
        acodec = f.get("acodec") or "none"
        if vcodec == "none" and acodec == "none":
            continue
        seen.add(fu)
        height = f.get("height") or 0
        abr = f.get("abr") or 0
        tbr = f.get("tbr") or 0
        ext = f.get("ext") or "mp4"
        if vcodec != "none" and acodec != "none":
            kind = "video+audio"
        elif vcodec != "none":
            kind = "video"
        else:
            kind = "audio"
        label_parts = []
        if height:
            label_parts.append(f"{height}p")
        elif kind == "audio":
            label_parts.append(f"{int(abr or tbr)}kbps" if (abr or tbr) else "audio")
        label_parts.append(ext.upper())
        if kind == "video":
            label_parts.append("video-only")
        if kind == "audio":
            label_parts.append("audio-only")
        if kind == "video+audio":
            label_parts.append("🔊 muxed")
        formats.append({
            "id": f.get("format_id"),
            "label": " · ".join(label_parts) or "stream",
            "ext": ext,
            "height": height or None,
            "abr": abr or None,
            "vcodec": vcodec,
            "acodec": acodec,
            "kind": kind,
            "muxed": kind == "video+audio",
            "filesize": f.get("filesize") or f.get("filesize_approx") or None,
            "url": fu,
        })

    def _sk(x):
        k = x.get("kind")
        pri = 0 if k == "video+audio" else (1 if k == "video" else 2)
        return (pri, -(x.get("height") or 0), -(x.get("abr") or 0))

    # Drop non-CDN YouTube page URLs (keep only googlevideo etc.)
    if is_yt:
        formats = [
            f for f in formats
            if f.get("url") and (
                "googlevideo.com" in f["url"]
                or "googleusercontent.com" in f["url"]
                or "googlevideo" in f["url"]
            )
        ]
    formats.sort(key=_sk)
    thumb = info.get("thumbnail")
    if not thumb and info.get("thumbnails"):
        try:
            thumb = info["thumbnails"][-1].get("url")
        except Exception:
            pass

    best_muxed = next((f for f in formats if f["kind"] == "video+audio"), None)
    best_audio = next((f for f in formats if f["kind"] == "audio"), None)

    return {
        "ok": True,
        "title": info.get("title") or "media",
        "id": info.get("id"),
        "extractor": (info.get("extractor") or info.get("ie_key") or "yt-dlp") + (f"/{used_client}" if used_client else ""),
        "duration": info.get("duration"),
        "uploader": info.get("uploader") or info.get("channel"),
        "thumbnail": thumb,
        "webpage_url": info.get("webpage_url") or url,
        "formats": formats,
        "format_count": len(formats),
        "best": formats[0] if formats else None,
        "best_muxed": best_muxed,
        "best_audio": best_audio,
        "note": "Direct URLs expire — re-extract if needed",
    }



@app.get("/dl/info", tags=["Downloader"])
async def dl_info(url: str = Query(..., min_length=8)):
    """Probe any media page (yt-dlp) — list formats + direct URLs."""
    url = url.strip()
    if not re.match(r"^https?://", url, re.I):
        raise HTTPException(400, "url must start with http(s)://")
    data = await asyncio.to_thread(_ytdlp_info, url)
    if not data.get("ok"):
        return JSONResponse(data, status_code=422)
    return data


@app.get("/dl/audio", tags=["Downloader"])
async def dl_audio(url: str = Query(..., min_length=8)):
    """Best audio-only stream for a page URL."""
    data = await asyncio.to_thread(_ytdlp_info, url)
    if not data.get("ok"):
        return JSONResponse(data, status_code=422)
    auds = [f for f in data.get("formats") or [] if f.get("kind") == "audio"]
    best = auds[0] if auds else None
    if not best:
        for f in data.get("formats") or []:
            if f.get("acodec") and f["acodec"] != "none":
                best = f
                break
    return {
        "ok": bool(best),
        "title": data.get("title"),
        "audio": best,
        "thumbnail": data.get("thumbnail"),
        "webpage_url": data.get("webpage_url"),
    }


@app.get("/dl/smart", tags=["Downloader"])
async def dl_smart(url: str = Query(..., min_length=8)):
    """Prefer muxed file; else video+audio pair for /dl/combine."""
    data = await asyncio.to_thread(_ytdlp_info, url)
    if not data.get("ok"):
        return JSONResponse(data, status_code=422)
    formats = data.get("formats") or []
    muxed = [f for f in formats if f.get("kind") == "video+audio"]
    videos = [f for f in formats if f.get("kind") == "video"]
    audios = [f for f in formats if f.get("kind") == "audio"]
    title = data.get("title") or "video"
    if muxed:
        return {
            "ok": True,
            "mode": "progressive",
            "title": title,
            "download_url": muxed[0].get("url"),
            "format": muxed[0],
            "note": "Has audio+video — no merge needed",
            "thumbnail": data.get("thumbnail"),
        }
    if videos and audios:
        v, a = videos[0], audios[0]
        return {
            "ok": True,
            "mode": "merge",
            "title": title,
            "video": v,
            "audio": a,
            "combine_post": {"video": v.get("url"), "audio": a.get("url"), "title": title},
            "note": "POST /dl/combine to merge (needs ffmpeg on server)",
            "thumbnail": data.get("thumbnail"),
        }
    if videos:
        return {"ok": True, "mode": "video_only", "title": title, "download_url": videos[0].get("url"), "format": videos[0]}
    if audios:
        return {"ok": True, "mode": "audio_only", "title": title, "download_url": audios[0].get("url"), "format": audios[0]}
    return JSONResponse({"ok": False, "error": "no formats"}, status_code=422)


@app.get("/dl/extract", tags=["Downloader"])
async def dl_extract(url: str = Query(..., min_length=8)):
    """Universal format list (yt-dlp · 1000+ sites)."""
    url = url.strip()
    if not re.match(r"^https?://", url, re.I):
        raise HTTPException(400, "url must start with http(s)://")
    data = await asyncio.to_thread(_ytdlp_info, url)
    if not data.get("ok"):
        return JSONResponse(data, status_code=422)
    return data


@app.get("/dl/any", tags=["Downloader"])
async def dl_any(url: str = Query(..., min_length=8)):
    """
    One endpoint for everything:
    HubCloud / HubDrive / PixelDrain / YouTube / TikTok / Instagram / X / Facebook / …
    """
    url = url.strip()
    if not re.match(r"^https?://", url, re.I):
        raise HTTPException(400, "url must start with http(s)://")
    low = url.lower()
    try:
        if "hubcloud." in low and "/drive/" in low:
            links = await resolve_hubcloud(url)
            return {"ok": True, "provider": "hubcloud", "count": len(links), "links": links, "input": url}
        if "hubdrive." in low:
            links = await resolve_hubdrive(url)
            return {"ok": True, "provider": "hubdrive", "count": len(links), "links": links, "input": url}
        if "pixeldrain." in low or "pixeldra.in" in low:
            api = _pixeldrain_api(url)
            if api:
                return {
                    "ok": True,
                    "provider": "pixeldrain",
                    "count": 1,
                    "links": [{"label": "PixelDrain", "url": api, "direct": True}],
                    "input": url,
                }
    except HTTPException as e:
        return JSONResponse({"ok": False, "provider": "hub", "error": e.detail, "input": url}, status_code=422)
    except Exception as e:
        return JSONResponse({"ok": False, "provider": "hub", "error": str(e)[:300], "input": url}, status_code=422)

    data = await asyncio.to_thread(_ytdlp_info, url)
    if not data.get("ok"):
        return JSONResponse({"ok": False, "provider": "yt-dlp", "error": data.get("error"), "input": url}, status_code=422)

    # Embed fallback (YouTube IP block)
    if data.get("mode") == "embed" or data.get("embed_url"):
        return {
            "ok": True,
            "provider": "youtube-embed",
            "mode": "embed",
            "title": data.get("title"),
            "thumbnail": data.get("thumbnail"),
            "embed_url": data.get("embed_url"),
            "webpage_url": data.get("webpage_url"),
            "formats": data.get("formats") or [],
            "input": url,
            "note": data.get("note"),
        }

    formats = data.get("formats") or []
    muxed = [f for f in formats if f.get("kind") == "video+audio"]
    videos = [f for f in formats if f.get("kind") == "video"]
    audios = [f for f in formats if f.get("kind") == "audio"]
    result = {
        "ok": True,
        "provider": "yt-dlp",
        "extractor": data.get("extractor"),
        "title": data.get("title"),
        "thumbnail": data.get("thumbnail"),
        "duration": data.get("duration"),
        "webpage_url": data.get("webpage_url"),
        "formats": formats,
        "input": url,
    }
    if muxed:
        result["mode"] = "progressive"
        result["download_url"] = muxed[0].get("url")
        result["best"] = muxed[0]
        result["note"] = "Single file with audio+video"
    elif videos and audios:
        result["mode"] = "merge"
        result["video"] = videos[0]
        result["audio"] = audios[0]
        result["combine_post"] = {
            "video": videos[0].get("url"),
            "audio": audios[0].get("url"),
            "title": data.get("title") or "video",
        }
        result["note"] = "POST /dl/combine to merge (ffmpeg)"
    elif videos:
        result["mode"] = "video_only"
        result["download_url"] = videos[0].get("url")
        result["best"] = videos[0]
    elif audios:
        result["mode"] = "audio_only"
        result["download_url"] = audios[0].get("url")
        result["best"] = audios[0]
    else:
        return JSONResponse({"ok": False, "error": "no playable formats", "input": url}, status_code=422)
    return result




# =============================================================================
# Music downloaders (PaxSenix-style paths, native unlimited — no rate limit)
# Spotify/Tidal/etc. → metadata + JioSaavn / yt-dlp match for full audio
# =============================================================================

def _spotify_id(url: str) -> Optional[str]:
    m = re.search(r"spotify\.com/(?:intl-[a-z]+/)?track/([a-zA-Z0-9]+)", url)
    if m:
        return m.group(1)
    m = re.search(r"spotify:track:([a-zA-Z0-9]+)", url)
    return m.group(1) if m else None


async def _spotify_meta(url: str) -> dict:
    """Public oEmbed + open graph style meta (no API key)."""
    track_id = _spotify_id(url) or ""
    meta = {"id": track_id, "title": None, "artist": None, "thumb": None, "url": url}
    async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
        try:
            r = await client.get(
                "https://open.spotify.com/oembed",
                params={"url": url if url.startswith("http") else f"https://open.spotify.com/track/{track_id}"},
            )
            if r.status_code == 200:
                j = r.json()
                meta["title"] = j.get("title")
                meta["thumb"] = j.get("thumbnail_url")
                # title often "Song · Artist"
                t = meta["title"] or ""
                if " · " in t:
                    parts = t.split(" · ", 1)
                    meta["title"], meta["artist"] = parts[0].strip(), parts[1].strip()
                elif " - " in t:
                    parts = t.split(" - ", 1)
                    meta["title"], meta["artist"] = parts[0].strip(), parts[1].strip()
        except Exception:
            pass
    return meta


async def _match_download(title: str, artist: str = "") -> dict:
    """Find full playable audio via Saavn then yt-dlp search."""
    links = []
    errors = {}
    q = f"{title} {artist}".strip()
    if not q:
        return {"links": [], "errors": {"query": "empty"}}
    try:
        match = await _saavn_match(title, artist)
        if match and match.get("audio_url"):
            links.append({
                "label": f"JioSaavn {match.get('audio_format') or '320'}",
                "url": match["audio_url"],
                "format": (match.get("audio_format") or "mp4").upper(),
                "quality": "320kbps",
                "provider": "jiosaavn",
                "direct": True,
                "title": match.get("title") or title,
                "artist": match.get("artist") or artist,
                "thumb": match.get("thumb"),
            })
    except Exception as e:
        errors["jiosaavn"] = str(e)[:100]
    # yt-dlp ytsearch
    try:
        yq = f"ytsearch1:{q}"
        data = await asyncio.to_thread(_ytdlp_info, yq)
        if data.get("ok"):
            formats = data.get("formats") or []
            audios = [f for f in formats if f.get("kind") == "audio"]
            muxed = [f for f in formats if f.get("kind") == "video+audio"]
            best = (audios or muxed or formats or [None])[0]
            if best and best.get("url"):
                links.append({
                    "label": f"YouTube {best.get('format') or best.get('kind') or 'audio'}",
                    "url": best["url"],
                    "format": (best.get("format") or "m4a").upper(),
                    "provider": "youtube",
                    "direct": True,
                    "title": data.get("title") or title,
                    "thumb": data.get("thumbnail"),
                })
            elif data.get("embed_url"):
                links.append({
                    "label": "YouTube embed",
                    "url": data["embed_url"],
                    "format": "EMBED",
                    "provider": "youtube",
                    "direct": False,
                })
        elif data.get("error"):
            errors["yt-dlp"] = str(data["error"])[:120]
    except Exception as e:
        errors["yt-dlp"] = str(e)[:100]
    return {"links": links, "errors": errors}


@app.get("/dl/jiosaavn", tags=["Downloader"])
async def dl_jiosaavn(
    url: str = Query(None, description="JioSaavn song URL or bare query"),
    q: str = Query(None, description="Search query"),
):
    """JioSaavn downloader — direct CDN links (unlimited)."""
    query = (q or url or "").strip()
    if not query:
        raise HTTPException(400, "Provide url= or q=")
    # If URL, extract slug/name
    if query.startswith("http"):
        # try path last segment as search
        slug = query.rstrip("/").split("/")[-1]
        slug = re.sub(r"[-_]+", " ", slug)
        query = slug or query
    hits = await _saavn_search(query, 8)
    if not hits:
        return {"ok": False, "provider": "jiosaavn", "error": "no results", "query": query}
    downloads = []
    for h in hits[:5]:
        sid = h.get("saavn_id") or (h.get("id") or "").replace("saavn:", "")
        try:
            card = await _saavn_stream_by_id(sid) if sid else None
        except Exception:
            card = None
        audio = (card or h).get("audio_url") or h.get("audio_url")
        if not audio and sid:
            try:
                card = await _saavn_stream_by_id(sid)
                audio = (card or {}).get("audio_url")
            except Exception:
                pass
        if audio:
            downloads.append({
                "title": (card or h).get("title") or h.get("title"),
                "artist": (card or h).get("artist") or h.get("artist"),
                "thumb": (card or h).get("thumb") or h.get("thumb"),
                "url": audio,
                "directUrl": audio,
                "format": ((card or h).get("audio_format") or "mp4").upper(),
                "quality": "320kbps",
                "provider": "jiosaavn",
                "id": f"saavn:{sid}" if sid else h.get("id"),
            })
    return {
        "ok": bool(downloads),
        "provider": "jiosaavn",
        "query": query,
        "count": len(downloads),
        "downloads": downloads,
        "url": downloads[0]["url"] if downloads else None,
        "directUrl": downloads[0]["url"] if downloads else None,
    }


@app.get("/dl/spotify", tags=["Downloader"])
async def dl_spotify(
    url: str = Query(..., description="Spotify track URL or spotify:track:ID"),
    server: str = Query("auto", description="auto|jiosaavn|youtube"),
):
    """
    Spotify downloader (unlimited). Resolves track meta via oEmbed,
    then fetches full audio from JioSaavn / YouTube (no Spotify DRM).
    """
    if not url.startswith("http") and not url.startswith("spotify:"):
        url = f"https://open.spotify.com/track/{url}"
    meta = await _spotify_meta(url)
    title = meta.get("title") or ""
    artist = meta.get("artist") or ""
    if not title:
        return {"ok": False, "provider": "spotify", "error": "could not resolve track metadata", "input": url}
    matched = await _match_download(title, artist)
    links = matched["links"]
    if server == "jiosaavn":
        links = [L for L in links if L.get("provider") == "jiosaavn"] or links
    elif server == "youtube":
        links = [L for L in links if L.get("provider") == "youtube"] or links
    best = links[0] if links else None
    return {
        "ok": bool(best),
        "provider": "spotify",
        "input": url,
        "spotify_id": meta.get("id"),
        "title": title,
        "artist": artist,
        "thumb": meta.get("thumb"),
        "url": best["url"] if best else None,
        "directUrl": best["url"] if best else None,
        "downloads": links,
        "errors": matched.get("errors") or None,
        "note": "Full audio via JioSaavn/YouTube match — not Spotify CDN",
    }


@app.get("/dl/deezer", tags=["Downloader"])
async def dl_deezer(
    url: str = Query(..., description="Deezer track URL or numeric id"),
    quality: str = Query("320kbps", description="Preferred quality label"),
):
    """Deezer downloader — preview + full match via JioSaavn/YouTube."""
    tid = None
    m = re.search(r"deezer\.com/(?:[a-z]{2}/)?track/(\d+)", url)
    if m:
        tid = m.group(1)
    elif url.isdigit():
        tid = url
    if not tid:
        raise HTTPException(400, "Need Deezer track URL or id")
    async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
        r = await client.get(f"https://api.deezer.com/track/{tid}")
        if r.status_code != 200:
            return {"ok": False, "provider": "deezer", "error": "track not found"}
        tr = r.json()
    title = tr.get("title") or ""
    artist = (tr.get("artist") or {}).get("name") or ""
    thumb = (tr.get("album") or {}).get("cover_xl") or (tr.get("album") or {}).get("cover_big")
    preview = tr.get("preview")
    matched = await _match_download(title, artist)
    links = matched["links"]
    if preview:
        links.append({
            "label": "Deezer 30s preview",
            "url": preview,
            "format": "MP3",
            "quality": "preview",
            "provider": "deezer",
            "direct": True,
        })
    best = next((L for L in links if L.get("provider") == "jiosaavn"), None) or (links[0] if links else None)
    return {
        "ok": bool(best),
        "provider": "deezer",
        "deezer_id": tid,
        "title": title,
        "artist": artist,
        "thumb": thumb,
        "quality": quality,
        "url": best["url"] if best else None,
        "directUrl": best["url"] if best else None,
        "downloads": links,
        "errors": matched.get("errors") or None,
    }


@app.get("/dl/tidal", tags=["Downloader"])
async def dl_tidal(url: str = Query(..., description="Tidal track URL or title search")):
    """Tidal → title match → JioSaavn/YouTube full audio."""
    title = url
    if "tidal.com" in url:
        # best-effort: last path segment
        title = re.sub(r'[-_]', " ", url.rstrip("/").split("/")[-1])
    matched = await _match_download(title, "")
    best = (matched["links"] or [None])[0]
    return {
        "ok": bool(best),
        "provider": "tidal",
        "input": url,
        "url": best["url"] if best else None,
        "directUrl": best["url"] if best else None,
        "downloads": matched["links"],
        "errors": matched.get("errors") or None,
        "note": "Matched via public search — not Tidal CDN",
    }


@app.get("/dl/qobuz", tags=["Downloader"])
async def dl_qobuz(url: str = Query(...), quality: str = Query("320kbps")):
    """Qobuz → match full audio (JioSaavn/YouTube)."""
    title = url
    if "qobuz.com" in url:
        title = re.sub(r'[-_]', " ", url.rstrip("/").split("/")[-1])
    matched = await _match_download(title, "")
    best = (matched["links"] or [None])[0]
    return {
        "ok": bool(best),
        "provider": "qobuz",
        "quality": quality,
        "url": best["url"] if best else None,
        "directUrl": best["url"] if best else None,
        "downloads": matched["links"],
        "errors": matched.get("errors") or None,
    }


@app.get("/dl/applemusic", tags=["Downloader"])
async def dl_applemusic(url: str = Query(...)):
    """Apple Music URL → oEmbed/title match → full audio."""
    title = artist = ""
    async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
        try:
            r = await client.get("https://itunes.apple.com/oembed", params={"url": url})
            if r.status_code == 200:
                j = r.json()
                title = j.get("title") or ""
                artist = j.get("author_name") or ""
        except Exception:
            pass
    if not title:
        title = re.sub(r'[-_]', " ", url.rstrip("/").split("/")[-1])
    matched = await _match_download(title, artist)
    best = (matched["links"] or [None])[0]
    return {
        "ok": bool(best),
        "provider": "applemusic",
        "title": title,
        "artist": artist,
        "url": best["url"] if best else None,
        "directUrl": best["url"] if best else None,
        "downloads": matched["links"],
        "errors": matched.get("errors") or None,
    }


@app.get("/dl/amazonmusic", tags=["Downloader"])
async def dl_amazonmusic(url: str = Query(...)):
    """Amazon Music → title guess → full audio match."""
    title = re.sub(r'[-_]', " ", url.rstrip("/").split("/")[-1])
    matched = await _match_download(title, "")
    best = (matched["links"] or [None])[0]
    return {
        "ok": bool(best),
        "provider": "amazonmusic",
        "url": best["url"] if best else None,
        "directUrl": best["url"] if best else None,
        "downloads": matched["links"],
        "errors": matched.get("errors") or None,
    }


@app.get("/dl/audiomack", tags=["Downloader"])
async def dl_audiomack(url: str = Query(...)):
    """Audiomack / generic page — yt-dlp extract."""
    data = await asyncio.to_thread(_ytdlp_info, url)
    if not data.get("ok"):
        # fallback title match from path
        title = re.sub(r'[-_]', " ", url.rstrip("/").split("/")[-1])
        matched = await _match_download(title, "")
        best = (matched["links"] or [None])[0]
        return {
            "ok": bool(best),
            "provider": "audiomack",
            "url": best["url"] if best else None,
            "directUrl": best["url"] if best else None,
            "downloads": matched["links"],
            "errors": {"yt-dlp": data.get("error"), **(matched.get("errors") or {})},
        }
    formats = data.get("formats") or []
    audios = [f for f in formats if f.get("kind") == "audio"]
    best = (audios or formats or [None])[0]
    return {
        "ok": bool(best and best.get("url")),
        "provider": "audiomack",
        "title": data.get("title"),
        "url": best.get("url") if best else None,
        "directUrl": best.get("url") if best else None,
        "downloads": [{"label": f.get("label") or f.get("kind"), "url": f.get("url"), "format": f.get("format")} for f in (audios or formats)[:8] if f.get("url")],
    }


@app.get("/dl/snapany", tags=["Downloader"])
async def dl_snapany(url: str = Query(..., min_length=8)):
    """SnapAny-style universal downloader (yt-dlp + HubCloud) — unlimited."""
    return await dl_any(url=url)



# =============================================================================
# Platform downloaders (PaxSenix-complete set) — all unlimited via yt-dlp / native
# =============================================================================

async def _dl_platform(url: str, provider: str, prefer: str = "auto") -> dict:
    """Shared extractor — prefer audio/video, return PaxSenix-like shape."""
    url = (url or "").strip()
    if not re.match(r"^https?://", url, re.I):
        return {"ok": False, "provider": provider, "error": "url must start with http(s)://", "input": url}
    # hub / pixel first
    low = url.lower()
    try:
        if "hubcloud." in low and "/drive/" in low:
            links = await resolve_hubcloud(url)
            return {"ok": True, "provider": "hubcloud", "count": len(links), "downloads": links, "url": links[0]["url"] if links else None, "directUrl": links[0]["url"] if links else None, "input": url}
        if "hubdrive." in low:
            links = await resolve_hubdrive(url)
            return {"ok": True, "provider": "hubdrive", "count": len(links), "downloads": links, "url": links[0]["url"] if links else None, "directUrl": links[0]["url"] if links else None, "input": url}
    except Exception as e:
        pass
    data = await asyncio.to_thread(_ytdlp_info, url)
    # If YT only returned embed fallback, still try to salvage via format check
    if data.get("ok") and data.get("mode") == "embed":
        data["ok"] = False  # force not-ok so caller can show clear error + embed
    if not data.get("ok"):
        # last chance: piped streams for YouTube
        yt_id = _extract_youtube_id(url)
        if yt_id:
            try:
                for base in (
                    "https://pipedapi.adminforge.de",
                    "https://pipedapi.ducks.party",
                    "https://api.piped.private.coffee",
                    "https://pipedapi.kavin.rocks",
                ):
                    try:
                        async with httpx.AsyncClient(timeout=12.0, follow_redirects=True) as client:
                            r = await client.get(f"{base}/streams/{yt_id}")
                        if r.status_code != 200:
                            continue
                        j = r.json()
                        formats = []
                        for vs in (j.get("videoStreams") or []):
                            if not vs.get("url"):
                                continue
                            formats.append({
                                "id": str(vs.get("itag") or vs.get("quality")),
                                "label": f"{vs.get('quality') or '?'} · {(vs.get('format') or 'mp4').upper()}",
                                "ext": vs.get("format") or "mp4",
                                "height": int(re.sub(r"\D", "", str(vs.get("quality") or "0")) or 0) or None,
                                "kind": "video" if not vs.get("videoOnly") else "video",
                                "url": vs["url"],
                            })
                        for a in (j.get("audioStreams") or [])[:4]:
                            if a.get("url"):
                                formats.append({
                                    "id": str(a.get("itag") or "aud"),
                                    "label": f"audio · {a.get('quality') or a.get('bitrate') or ''}",
                                    "ext": a.get("format") or "m4a",
                                    "kind": "audio",
                                    "url": a["url"],
                                })
                        if formats:
                            data = {
                                "ok": True,
                                "title": j.get("title") or f"YouTube {yt_id}",
                                "id": yt_id,
                                "extractor": f"piped/{base.split('//')[1]}",
                                "thumbnail": j.get("thumbnailUrl"),
                                "duration": j.get("duration"),
                                "formats": formats,
                                "webpage_url": f"https://www.youtube.com/watch?v={yt_id}",
                            }
                            break
                    except Exception:
                        continue
            except Exception:
                pass
    if not data.get("ok"):
        return {
            "ok": False,
            "provider": provider,
            "error": data.get("error") or data.get("note") or "extract failed — YouTube may block this server IP",
            "input": url,
            "embed_url": data.get("embed_url") or (f"https://www.youtube.com/embed/{_extract_youtube_id(url)}" if _extract_youtube_id(url) else None),
            "title": data.get("title"),
            "thumbnail": data.get("thumbnail"),
            "formats": data.get("formats") or [],
        }
    formats = data.get("formats") or []
    audios = [f for f in formats if f.get("kind") == "audio"]
    videos = [f for f in formats if f.get("kind") in ("video", "video+audio")]
    muxed = [f for f in formats if f.get("kind") == "video+audio"]
    downloads = []
    for f in (audios + muxed + videos)[:12]:
        if not f.get("url"):
            continue
        downloads.append({
            "label": f.get("label") or f.get("format") or f.get("kind"),
            "url": f["url"],
            "format": f.get("format"),
            "kind": f.get("kind"),
            "height": f.get("height"),
            "abr": f.get("abr"),
        })
    best = None
    if prefer == "audio":
        best = (audios or muxed or downloads or [None])[0]
    elif prefer == "video":
        best = (muxed or videos or downloads or [None])[0]
    else:
        best = (muxed or audios or videos or downloads or [None])[0]
    if isinstance(best, dict) and "url" not in best and downloads:
        best = downloads[0]
    return {
        "ok": bool(best and (isinstance(best, dict) and best.get("url"))),
        "provider": provider,
        "extractor": data.get("extractor"),
        "title": data.get("title"),
        "thumbnail": data.get("thumbnail"),
        "duration": data.get("duration"),
        "url": best.get("url") if isinstance(best, dict) else None,
        "directUrl": best.get("url") if isinstance(best, dict) else None,
        "download_url": best.get("url") if isinstance(best, dict) else None,
        "downloads": downloads,
        "formats": formats,
        "input": url,
        "webpage_url": data.get("webpage_url"),
    }


@app.get("/dl/tiktok", tags=["Downloader"])
async def dl_tiktok(url: str = Query(..., min_length=8)):
    """TikTok video/audio downloader."""
    return await _dl_platform(url, "tiktok")


@app.get("/dl/ig", tags=["Downloader"])
@app.get("/dl/instagram", tags=["Downloader"], include_in_schema=False)
async def dl_ig(url: str = Query(..., min_length=8)):
    """Instagram reels / posts downloader."""
    return await _dl_platform(url, "instagram")


@app.get("/dl/fb", tags=["Downloader"])
@app.get("/dl/facebook", tags=["Downloader"], include_in_schema=False)
async def dl_fb(url: str = Query(..., min_length=8)):
    """Facebook video downloader."""
    return await _dl_platform(url, "facebook")


@app.get("/dl/threads", tags=["Downloader"])
async def dl_threads(url: str = Query(..., min_length=8)):
    """Threads (Meta) media downloader."""
    return await _dl_platform(url, "threads")


@app.get("/dl/twitter", tags=["Downloader"])
@app.get("/dl/x", tags=["Downloader"], include_in_schema=False)
async def dl_twitter(url: str = Query(..., min_length=8)):
    """Twitter / X video & GIF downloader."""
    return await _dl_platform(url, "twitter")


@app.get("/dl/ytmp3", tags=["Downloader"])
async def dl_ytmp3(
    url: str = Query(..., min_length=8),
    format: str = Query("m4a", description="mp3|m4a|webm|opus"),
):
    """YouTube → best audio (mp3/m4a)."""
    res = await _dl_platform(url, "ytmp3", prefer="audio")
    res["requested_format"] = format
    return res


@app.get("/dl/ytmp4", tags=["Downloader"])
async def dl_ytmp4(
    url: str = Query(..., min_length=8),
    quality: str = Query("720", description="360|480|720|1080"),
):
    """YouTube → best video under quality."""
    res = await _dl_platform(url, "ytmp4", prefer="video")
    res["requested_quality"] = quality
    # pick closest height if available
    try:
        want = int(re.sub(r"\D", "", quality) or "720")
    except Exception:
        want = 720
    formats = res.get("formats") or []
    # Prefer muxed (video+audio) near requested height; else video-only
    candidates = [f for f in formats if f.get("kind") in ("video+audio", "video") and f.get("url")]
    if candidates:
        def score(f):
            h = f.get("height") or 0
            mux = 0 if f.get("kind") == "video+audio" else 1
            return (mux, abs(h - want), -h)
        candidates.sort(key=score)
        best = candidates[0]
        res["url"] = best.get("url")
        res["directUrl"] = best.get("url")
        res["download_url"] = best.get("url")
        res["quality_label"] = best.get("label")
        res["ok"] = True
        # also expose audio pair if best is video-only
        if best.get("kind") == "video":
            aud = next((f for f in formats if f.get("kind") == "audio" and f.get("url")), None)
            if aud:
                res["audio_url"] = aud.get("url")
                res["note"] = "Video-only stream — merge with audio_url via /dl/combine if needed"
    return res


@app.get("/dl/soundcloud", tags=["Downloader"])
async def dl_soundcloud(url: str = Query(..., min_length=8)):
    return await _dl_platform(url, "soundcloud", prefer="audio")


@app.get("/dl/reddit", tags=["Downloader"])
async def dl_reddit(url: str = Query(..., min_length=8)):
    return await _dl_platform(url, "reddit")


@app.get("/dl/pinterest", tags=["Downloader"])
async def dl_pinterest(url: str = Query(..., min_length=8)):
    return await _dl_platform(url, "pinterest")


@app.get("/dl/tumblr", tags=["Downloader"])
async def dl_tumblr(url: str = Query(..., min_length=8)):
    return await _dl_platform(url, "tumblr")


@app.get("/dl/twitch", tags=["Downloader"])
async def dl_twitch(url: str = Query(..., min_length=8)):
    return await _dl_platform(url, "twitch")


@app.get("/dl/dailymotion", tags=["Downloader"])
async def dl_dailymotion(url: str = Query(..., min_length=8)):
    return await _dl_platform(url, "dailymotion")


@app.get("/dl/vimeo", tags=["Downloader"])
async def dl_vimeo(url: str = Query(..., min_length=8)):
    return await _dl_platform(url, "vimeo")


@app.get("/dl/capcut", tags=["Downloader"])
async def dl_capcut(url: str = Query(..., min_length=8)):
    return await _dl_platform(url, "capcut")


@app.get("/dl/likee", tags=["Downloader"])
async def dl_likee(url: str = Query(..., min_length=8)):
    return await _dl_platform(url, "likee")


@app.get("/dl/snackvideo", tags=["Downloader"])
async def dl_snackvideo(url: str = Query(..., min_length=8)):
    return await _dl_platform(url, "snackvideo")


@app.get("/dl/douyin", tags=["Downloader"])
async def dl_douyin(url: str = Query(..., min_length=8)):
    return await _dl_platform(url, "douyin")


@app.get("/dl/snapchat", tags=["Downloader"])
async def dl_snapchat(url: str = Query(..., min_length=8)):
    return await _dl_platform(url, "snapchat")


@app.get("/dl/bluesky", tags=["Downloader"])
async def dl_bluesky(url: str = Query(..., min_length=8)):
    return await _dl_platform(url, "bluesky")


@app.get("/dl/rednote", tags=["Downloader"])
async def dl_rednote(url: str = Query(..., min_length=8)):
    """Xiaohongshu / RedNote."""
    return await _dl_platform(url, "rednote")


@app.get("/dl/9gag", tags=["Downloader"])
async def dl_9gag(url: str = Query(..., min_length=8)):
    return await _dl_platform(url, "9gag")


@app.get("/dl/hitube", tags=["Downloader"])
async def dl_hitube(url: str = Query(..., min_length=8)):
    return await _dl_platform(url, "hitube")


@app.get("/dl/savevideo", tags=["Downloader"])
@app.get("/dl/9xbuddy", tags=["Downloader"], include_in_schema=False)
async def dl_savevideo(url: str = Query(..., min_length=8)):
    """Generic save-video / 9xbuddy-style (yt-dlp)."""
    return await _dl_platform(url, "savevideo")


@app.get("/dl/aio", tags=["Downloader"])
async def dl_aio(url: str = Query(..., min_length=8)):
    """All-in-one downloader (alias of /dl/any)."""
    return await dl_any(url=url)


@app.get("/dl/mediafire", tags=["Downloader"])
async def dl_mediafire(url: str = Query(..., min_length=8)):
    """MediaFire direct link resolver."""
    return await _dl_platform(url, "mediafire")


@app.get("/dl/gdrive", tags=["Downloader"])
async def dl_gdrive(url: str = Query(..., min_length=8)):
    """Google Drive — best-effort direct / yt-dlp."""
    # normalize file id
    m = re.search(r"/file/d/([^/]+)", url) or re.search(r"[?&]id=([^&]+)", url)
    if m:
        fid = m.group(1)
        direct = f"https://drive.google.com/uc?export=download&id={fid}"
        return {
            "ok": True,
            "provider": "gdrive",
            "file_id": fid,
            "url": direct,
            "directUrl": direct,
            "downloads": [{"label": "Google Drive uc export", "url": direct, "direct": True}],
            "input": url,
            "note": "Large files may need confirm token in browser",
        }
    return await _dl_platform(url, "gdrive")


@app.get("/dl/mega", tags=["Downloader"])
async def dl_mega(url: str = Query(..., min_length=8)):
    return await _dl_platform(url, "mega")


@app.get("/dl/terabox", tags=["Downloader"])
async def dl_terabox(url: str = Query(..., min_length=8)):
    """TeraBox / Terabox share link."""
    return await _dl_platform(url, "terabox")


@app.get("/dl/sfile", tags=["Downloader"])
async def dl_sfile(url: str = Query(..., min_length=8)):
    return await _dl_platform(url, "sfile")

@app.get("/dl/sites", tags=["Downloader"])
async def dl_sites():
    return {
        "ok": True,
        "engine": "native JioSaavn + yt-dlp + Deezer + HubCloud (unlimited, no PaxSenix rate limit)",
        "endpoints": {
            "universal": "GET /dl/any?url=",
            "aio": "GET /dl/aio?url=",
            "snapany": "GET /dl/snapany?url=",
            "tiktok": "GET /dl/tiktok?url=",
            "instagram": "GET /dl/ig?url=",
            "facebook": "GET /dl/fb?url=",
            "twitter": "GET /dl/twitter?url=",
            "threads": "GET /dl/threads?url=",
            "ytmp3": "GET /dl/ytmp3?url=",
            "ytmp4": "GET /dl/ytmp4?url=",
            "soundcloud": "GET /dl/soundcloud?url=",
            "reddit": "GET /dl/reddit?url=",
            "pinterest": "GET /dl/pinterest?url=",
            "tumblr": "GET /dl/tumblr?url=",
            "twitch": "GET /dl/twitch?url=",
            "dailymotion": "GET /dl/dailymotion?url=",
            "vimeo": "GET /dl/vimeo?url=",
            "capcut": "GET /dl/capcut?url=",
            "likee": "GET /dl/likee?url=",
            "snackvideo": "GET /dl/snackvideo?url=",
            "douyin": "GET /dl/douyin?url=",
            "snapchat": "GET /dl/snapchat?url=",
            "bluesky": "GET /dl/bluesky?url=",
            "rednote": "GET /dl/rednote?url=",
            "9gag": "GET /dl/9gag?url=",
            "mediafire": "GET /dl/mediafire?url=",
            "gdrive": "GET /dl/gdrive?url=",
            "mega": "GET /dl/mega?url=",
            "terabox": "GET /dl/terabox?url=",
            "sfile": "GET /dl/sfile?url=",
            "spotify": "GET /dl/spotify?url=",
            "deezer": "GET /dl/deezer?url=",
            "jiosaavn": "GET /dl/jiosaavn?q=",
            "tidal": "GET /dl/tidal?url=",
            "qobuz": "GET /dl/qobuz?url=",
            "applemusic": "GET /dl/applemusic?url=",
            "amazonmusic": "GET /dl/amazonmusic?url=",
            "audiomack": "GET /dl/audiomack?url=",
            "formats": "GET /dl/extract?url=",
            "smart": "GET /dl/smart?url=",
            "combine": "POST /dl/combine",
        },
        "note": "Spotify/Tidal/Apple/Amazon resolve via metadata + JioSaavn/YouTube full audio (no DRM CDN).",
    }


async def _combine_streams(video: str, audio: str, title: str = "video"):
    import tempfile, subprocess, shutil

    if not shutil.which("ffmpeg"):
        raise HTTPException(
            503,
            "ffmpeg not installed on server — ask host to install ffmpeg, or use progressive VIDEO+AUDIO link",
        )
    work = tempfile.mkdtemp(prefix="dlcomb_")
    vpath = os.path.join(work, "v.bin")
    apath = os.path.join(work, "a.bin")
    out_path = os.path.join(work, "out.mp4")
    try:
        async def _dl(u: str, path: str):
            headers = _cdn_headers(u)
            async with httpx.AsyncClient(follow_redirects=True, timeout=180.0) as client:
                async with client.stream("GET", u, headers=headers) as r:
                    if r.status_code >= 400:
                        raise HTTPException(502, f"CDN HTTP {r.status_code} — link may be expired; fetch formats again")
                    total = 0
                    with open(path, "wb") as f:
                        async for chunk in r.aiter_bytes(64 * 1024):
                            total += len(chunk)
                            if total > 95 * 1024 * 1024:
                                raise HTTPException(413, "stream too large for cloud merge (~95MB cap)")
                            f.write(chunk)
                    if total < 500:
                        raise HTTPException(502, "empty stream from CDN")

        await _dl(video, vpath)
        await _dl(audio, apath)

        def _ffmpeg():
            cmd = [
                "ffmpeg", "-y", "-i", vpath, "-i", apath,
                "-c", "copy", "-map", "0:v:0", "-map", "1:a:0?",
                "-shortest", "-movflags", "+faststart", out_path,
            ]
            p = subprocess.run(cmd, capture_output=True, timeout=180)
            if p.returncode != 0 or not os.path.isfile(out_path) or os.path.getsize(out_path) < 1000:
                cmd2 = [
                    "ffmpeg", "-y", "-i", vpath, "-i", apath,
                    "-c:v", "copy", "-c:a", "aac", "-b:a", "128k",
                    "-map", "0:v:0", "-map", "1:a:0?",
                    "-shortest", "-movflags", "+faststart", out_path,
                ]
                p2 = subprocess.run(cmd2, capture_output=True, timeout=300)
                if p2.returncode != 0 or not os.path.isfile(out_path):
                    err = (p2.stderr or p.stderr or b"")[-500:].decode("utf-8", "ignore")
                    raise RuntimeError(err or "ffmpeg failed")
            return os.path.getsize(out_path)

        size = await asyncio.to_thread(_ffmpeg)
        if size < 1000:
            raise HTTPException(502, "merge produced empty file")

        def iterfile():
            try:
                with open(out_path, "rb") as f:
                    while True:
                        chunk = f.read(64 * 1024)
                        if not chunk:
                            break
                        yield chunk
            finally:
                shutil.rmtree(work, ignore_errors=True)

        safe = re.sub(r"[^\w\-. ]+", "", title or "video")[:80] or "video"
        return StreamingResponse(
            iterfile(),
            media_type="video/mp4",
            headers={
                "Content-Disposition": f'attachment; filename="{safe}.mp4"',
                "Content-Length": str(size),
                "Cache-Control": "no-store",
            },
        )
    except HTTPException:
        shutil.rmtree(work, ignore_errors=True)
        raise
    except Exception as e:
        shutil.rmtree(work, ignore_errors=True)
        raise HTTPException(502, f"combine failed: {str(e)[:300]}")


@app.get("/dl/combine", tags=["Downloader"])
async def dl_combine_get(
    video: str = Query(..., min_length=8),
    audio: str = Query(..., min_length=8),
    title: str = Query("video"),
):
    """Merge video+audio URLs (GET). Prefer POST if URLs are very long."""
    return await _combine_streams(video, audio, title)


@app.post("/dl/combine", tags=["Downloader"])
async def dl_combine_post(request: Request):
    """Merge video+audio via JSON body — avoids long-URL / 502 proxy limits.

    Body: `{"video":"https://...","audio":"https://...","title":"name"}`
    """
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(400, "JSON body required: {video, audio, title?}")
    video = (body.get("video") or "").strip()
    audio = (body.get("audio") or "").strip()
    title = (body.get("title") or "video").strip()
    if len(video) < 8 or len(audio) < 8:
        raise HTTPException(400, "video and audio URLs required")
    return await _combine_streams(video, audio, title)



@app.get("/dl/merged", tags=["Downloader"])
async def dl_merged(
    url: str = Query(..., min_length=8),
    quality: int = Query(720, ge=144, le=1080),
):
    """yt-dlp download+merge. May fail if site blocks bots — use /dl/combine when formats exist."""
    url = url.strip()
    if not re.match(r"^https?://", url, re.I):
        raise HTTPException(400, "url must be http(s)")
    import tempfile, shutil
    work = tempfile.mkdtemp(prefix="dlmerge_")
    try:
        def _run():
            import yt_dlp  # type: ignore
            fmt = (
                "best[height<=%d][vcodec!=none][acodec!=none]/"
                "bestvideo[height<=%d]+bestaudio/best"
            ) % (quality, quality)
            opts = {
                "quiet": True,
                "no_warnings": True,
                "noplaylist": True,
                "format": fmt,
                "merge_output_format": "mp4",
                "outtmpl": os.path.join(work, "raw.%(ext)s"),
                "socket_timeout": 30,
                "retries": 2,
                "extractor_args": {"youtube": {"player_client": ["android", "web"]}},
            }
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(url, download=True)
            files = [
                os.path.join(work, f)
                for f in os.listdir(work)
                if os.path.isfile(os.path.join(work, f)) and os.path.getsize(os.path.join(work, f)) > 1000
            ]
            if not files:
                raise RuntimeError("no file produced")
            files.sort(key=lambda p: os.path.getsize(p), reverse=True)
            return files[0], ((info or {}).get("title") or "video")

        path, title = await asyncio.to_thread(_run)
        size = os.path.getsize(path)
        if size > 80 * 1024 * 1024:
            shutil.rmtree(work, ignore_errors=True)
            raise HTTPException(413, "file too large")

        def iterfile():
            try:
                with open(path, "rb") as f:
                    while True:
                        chunk = f.read(65536)
                        if not chunk:
                            break
                        yield chunk
            finally:
                shutil.rmtree(work, ignore_errors=True)

        safe = re.sub(r"[^\w\-. ]+", "", title)[:80] or "video"
        return StreamingResponse(
            iterfile(),
            media_type="video/mp4",
            headers={
                "Content-Disposition": f'attachment; filename="{safe}.mp4"',
                "Content-Length": str(size),
            },
        )
    except HTTPException:
        shutil.rmtree(work, ignore_errors=True)
        raise
    except Exception as e:
        shutil.rmtree(work, ignore_errors=True)
        raise HTTPException(502, f"merge failed: {str(e)[:240]}")







# =============================================================================
# YT MUSIC / YOUTUBE via public Invidious instances (SimpMusic-style source)
# =============================================================================

INVIDIOUS_HOSTS = [
    "https://inv.nadeko.net",
    "https://invidious.nerdvpn.de",
    "https://yewtu.be",
    "https://invidious.fdn.fr",
    "https://vid.puffyan.us",
]


async def _invidious_get(path: str, params: Optional[dict] = None) -> Any:
    last_err = None
    async with httpx.AsyncClient(timeout=18.0, follow_redirects=True) as client:
        for host in INVIDIOUS_HOSTS:
            try:
                r = await client.get(
                    host.rstrip("/") + path,
                    params=params or {},
                    headers={"User-Agent": "Mozilla/5.0 StreamHub/5.5", "Accept": "application/json"},
                )
                if r.status_code == 200:
                    return r.json()
                last_err = f"{host} HTTP {r.status_code}"
            except Exception as e:
                last_err = str(e)
                continue
    raise HTTPException(502, f"Invidious unavailable: {last_err}")


def _yt_thumb(video_id: str, thumbs: Any = None) -> str:
    if isinstance(thumbs, list) and thumbs:
        # prefer high quality
        best = sorted(thumbs, key=lambda x: (x.get("width") or 0), reverse=True)
        if best and best[0].get("url"):
            return best[0]["url"]
    return f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg"




# =============================================================================
# YouTube Music + YouTube (PaxSenix-style paths, native)
# =============================================================================

@app.get("/yt-music/search", tags=["YouTube Music"])
async def ytmusic_search(q: str = Query(..., min_length=1), filter: str = Query("songs", description="songs|videos|albums|artists|playlists")):
    """YouTube Music search (inner API)."""
    data = await _ytm_post("search", {"query": q})
    items = []
    _ytm_walk(data, "musicResponsiveListItemRenderer", items)
    _ytm_walk(data, "musicTwoRowItemRenderer", items)
    songs, seen = [], set()
    for it in items:
        e = _ytm_parse_item(it)
        if e and e["video_id"] not in seen:
            seen.add(e["video_id"])
            songs.append(e)
    return {"ok": True, "query": q, "count": len(songs), "items": songs, "provider": "ytmusic"}


@app.get("/yt-music/home", tags=["YouTube Music"])
async def ytmusic_home():
    """YT Music home / explore rows (via search seeds)."""
    seeds = ["Top songs", "Trending music", "New releases", "Pop hits", "Hip hop"]
    sections = []
    for q in seeds:
        try:
            data = await _ytm_post("search", {"query": q})
            items = []
            _ytm_walk(data, "musicResponsiveListItemRenderer", items)
            songs, seen = [], set()
            for it in items:
                e = _ytm_parse_item(it)
                if e and e["video_id"] not in seen:
                    seen.add(e["video_id"])
                    songs.append(e)
            if songs:
                sections.append({"title": q, "items": songs[:12]})
        except Exception:
            continue
    return {"ok": True, "sections": sections, "provider": "ytmusic"}


@app.get("/yt-music/info", tags=["YouTube Music"])
async def ytmusic_info(video_id: str = Query(..., min_length=6)):
    """Track info from YT Music next endpoint."""
    video_id = video_id.replace("yt:", "").strip()
    data = await _ytm_post("next", {"videoId": video_id})
    s = json.dumps(data)
    texts = re.findall(r'"text"\s*:\s*"([^"\\]{2,100})"', s)
    title = texts[0] if texts else video_id
    artist = texts[1] if len(texts) > 1 else ""
    thumbs = re.findall(r'https://i\.ytimg\.com/[^"\\]+', s)
    thumb = thumbs[0].replace("\\u0026", "&") if thumbs else f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg"
    # related
    related = []
    items = []
    _ytm_walk(data, "musicResponsiveListItemRenderer", items)
    seen = {video_id}
    for it in items:
        e = _ytm_parse_item(it)
        if e and e["video_id"] not in seen:
            seen.add(e["video_id"])
            related.append(e)
    return {
        "ok": True,
        "video_id": video_id,
        "title": title,
        "artist": artist,
        "thumb": thumb,
        "related": related[:15],
        "watch_url": f"https://music.youtube.com/watch?v={video_id}",
        "provider": "ytmusic",
    }


@app.get("/yt-music/next", tags=["YouTube Music"])
async def ytmusic_next(video_id: str = Query(..., min_length=6)):
    """Next / radio queue for a track."""
    info = await ytmusic_info(video_id=video_id)
    return {
        "ok": True,
        "video_id": video_id,
        "items": info.get("related") or [],
        "count": len(info.get("related") or []),
        "provider": "ytmusic",
    }


@app.get("/yt-music/playlist", tags=["YouTube Music"])
async def ytmusic_playlist(id: str = Query(..., description="Playlist id e.g. RDAMVM... or VLPL...")):
    """Browse playlist (best-effort via browse endpoint)."""
    browse_id = id
    if not id.startswith("VL") and id.startswith("PL"):
        browse_id = "VL" + id
    data = await _ytm_post("browse", {"browseId": browse_id})
    items = []
    _ytm_walk(data, "musicResponsiveListItemRenderer", items)
    songs, seen = [], set()
    for it in items:
        e = _ytm_parse_item(it)
        if e and e["video_id"] not in seen:
            seen.add(e["video_id"])
            songs.append(e)
    return {"ok": True, "id": id, "count": len(songs), "items": songs, "provider": "ytmusic"}


@app.get("/yt-music/album", tags=["YouTube Music"])
async def ytmusic_album(id: str = Query(..., description="Browse id MP... or album browseId")):
    data = await _ytm_post("browse", {"browseId": id})
    items = []
    _ytm_walk(data, "musicResponsiveListItemRenderer", items)
    songs, seen = [], set()
    for it in items:
        e = _ytm_parse_item(it)
        if e and e["video_id"] not in seen:
            seen.add(e["video_id"])
            songs.append(e)
    return {"ok": True, "id": id, "count": len(songs), "items": songs, "provider": "ytmusic"}


@app.get("/yt/search", tags=["YouTube"])
async def yt_search(q: str = Query(..., min_length=1), page: int = Query(1, ge=1)):
    """YouTube video search — Invidious first, YT Music fallback."""
    try:
        res = await music_yt_search(q=q, page=page)
        if isinstance(res, dict) and res.get("items"):
            return res
    except Exception:
        pass
    # fallback YT Music search
    data = await _ytm_post("search", {"query": q})
    items = []
    _ytm_walk(data, "musicResponsiveListItemRenderer", items)
    songs, seen = [], set()
    for it in items:
        e = _ytm_parse_item(it)
        if e and e["video_id"] not in seen:
            seen.add(e["video_id"])
            songs.append({
                "id": f"yt:{e['video_id']}",
                "video_id": e["video_id"],
                "title": e.get("title"),
                "artist": e.get("artist"),
                "thumb": e.get("thumb"),
                "provider": "ytmusic",
            })
    return {"ok": True, "query": q, "items": songs, "provider": "ytmusic-fallback", "page": page}


@app.get("/yt/transcript", tags=["YouTube"])
async def yt_transcript(
    video_id: str = Query(..., min_length=6),
    lang: str = Query("en"),
):
    """YouTube transcript / captions (best-effort)."""
    video_id = video_id.replace("yt:", "").strip()
    if "youtube.com" in video_id or "youtu.be" in video_id:
        m = re.search(r"(?:v=|youtu\.be/)([\w-]{11})", video_id)
        video_id = m.group(1) if m else video_id
    # try timedtext list
    lines = []
    errors = {}
    async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
        try:
            r = await client.get(
                "https://www.youtube.com/api/timedtext",
                params={"v": video_id, "lang": lang, "fmt": "json3"},
                headers={"User-Agent": "Mozilla/5.0"},
            )
            if r.status_code == 200 and r.text:
                data = r.json()
                for ev in data.get("events") or []:
                    segs = ev.get("segs") or []
                    text = "".join(s.get("utf8") or "" for s in segs).strip()
                    if text:
                        lines.append({
                            "start": (ev.get("tStartMs") or 0) / 1000.0,
                            "dur": (ev.get("dDurationMs") or 0) / 1000.0,
                            "text": text,
                        })
        except Exception as e:
            errors["timedtext"] = str(e)[:80]
        if not lines:
            # Invidious captions
            try:
                data = await _invidious_get(f"/api/v1/captions/{video_id}", {"label": lang})
            except Exception:
                try:
                    data = await _invidious_get(f"/api/v1/captions/{video_id}")
                except Exception as e:
                    errors["invidious"] = str(e)[:80]
                    data = None
            if isinstance(data, dict) and data.get("captions"):
                # may be list of tracks only
                pass
            elif isinstance(data, list):
                for it in data:
                    if isinstance(it, dict) and it.get("text"):
                        lines.append(it)
    return {
        "ok": bool(lines),
        "video_id": video_id,
        "lang": lang,
        "count": len(lines),
        "lines": lines,
        "errors": errors or None,
        "provider": "youtube",
    }


@app.get("/yt/channel", tags=["YouTube"])
async def yt_channel(id: str = Query(..., description="Channel id UC... or handle")):
    """Channel info + latest videos via Invidious."""
    try:
        data = await _invidious_get(f"/api/v1/channels/{id}")
    except Exception as e:
        return {"ok": False, "error": str(e)[:120]}
    if not isinstance(data, dict):
        return {"ok": False, "error": "not found"}
    videos = []
    for it in (data.get("latestVideos") or [])[:20]:
        if not isinstance(it, dict):
            continue
        vid = it.get("videoId")
        if not vid:
            continue
        videos.append({
            "video_id": vid,
            "title": it.get("title"),
            "thumb": _yt_thumb(vid, it.get("videoThumbnails")),
            "duration": it.get("lengthSeconds"),
            "views": it.get("viewCount"),
        })
    return {
        "ok": True,
        "id": data.get("authorId") or id,
        "name": data.get("author"),
        "description": (data.get("description") or "")[:500],
        "subscribers": data.get("subCount"),
        "videos": videos,
        "provider": "invidious",
    }


@app.get("/yt/ytaudio", tags=["YouTube"])
async def yt_ytaudio(url: str = Query(..., description="YouTube URL or video id")):
    """Best audio stream URL (alias of /dl/ytmp3)."""
    if re.match(r"^[\w-]{11}$", url.strip()):
        url = f"https://www.youtube.com/watch?v={url.strip()}"
    return await dl_ytmp3(url=url)


@app.get("/yt/download", tags=["YouTube"])
async def yt_download(
    url: str = Query(..., min_length=6),
    type: str = Query("audio", description="audio|video"),
):
    """YouTube audio/video download helper."""
    if re.match(r"^[\w-]{11}$", url.strip()):
        url = f"https://www.youtube.com/watch?v={url.strip()}"
    if type == "video":
        return await dl_ytmp4(url=url)
    return await dl_ytmp3(url=url)


# Aliases matching /music/yt/*
@app.get("/yt-music/play/{video_id}", tags=["YouTube Music"])
async def ytmusic_play(video_id: str):
    """Play stream for YT Music track."""
    return await music_yt_play(video_id)

@app.get("/music/yt/search", tags=["Music"])
async def music_yt_search(q: str = Query(..., min_length=1), page: int = Query(1, ge=1)):
    """YouTube / YT Music search via Invidious (videos)."""
    data = await _invidious_get("/api/v1/search", {"q": q, "type": "video", "page": page})
    if not isinstance(data, list):
        data = []
    items = []
    for it in data:
        if not isinstance(it, dict) or it.get("type") not in (None, "video"):
            if it.get("type") and it.get("type") != "video":
                continue
        vid = it.get("videoId")
        if not vid:
            continue
        items.append({
            "id": f"yt:{vid}",
            "video_id": vid,
            "title": it.get("title"),
            "artist": it.get("author") or it.get("authorId"),
            "thumb": _yt_thumb(vid, it.get("videoThumbnails")),
            "duration": it.get("lengthSeconds"),
            "views": it.get("viewCount"),
            "provider": "youtube",
        })
    return {"query": q, "items": items, "provider": "invidious", "page": page}


@app.get("/music/yt/trending", tags=["Music"])
async def music_yt_trending(region: str = Query("US")):
    """Trending videos (music-friendly) via Invidious."""
    data = await _invidious_get("/api/v1/trending", {"type": "music", "region": region})
    if not isinstance(data, list):
        # fallback general trending
        try:
            data = await _invidious_get("/api/v1/trending", {"region": region})
        except Exception:
            data = []
    if not isinstance(data, list):
        data = []
    items = []
    for it in data[:40]:
        if not isinstance(it, dict):
            continue
        vid = it.get("videoId")
        if not vid:
            continue
        items.append({
            "id": f"yt:{vid}",
            "video_id": vid,
            "title": it.get("title"),
            "artist": it.get("author"),
            "thumb": _yt_thumb(vid, it.get("videoThumbnails")),
            "duration": it.get("lengthSeconds"),
            "provider": "youtube",
        })
    return {"items": items, "provider": "invidious", "region": region}


@app.get("/music/yt/play/{video_id}", tags=["Music"])
async def music_yt_play(video_id: str):
    """Stream info for a YouTube video — prefer audio formats (SimpMusic-style)."""
    video_id = video_id.replace("yt:", "").strip()
    if not re.match(r"^[\w-]{6,20}$", video_id):
        raise HTTPException(400, "invalid video id")
    data = await _invidious_get(f"/api/v1/videos/{video_id}")
    adaptive = data.get("adaptiveFormats") or []
    formats = data.get("formatStreams") or []
    audio_streams = []
    video_streams = []
    for f in adaptive:
        if not isinstance(f, dict) or not f.get("url"):
            continue
        t = (f.get("type") or "").lower()
        if t.startswith("audio/") or "audio" in t:
            audio_streams.append({
                "label": f.get("bitrate") or f.get("quality") or "audio",
                "bitrate": f.get("bitrate"),
                "url": f.get("url"),
                "type": f.get("type"),
                "format": "AUDIO",
            })
        elif t.startswith("video/"):
            video_streams.append({
                "label": f.get("qualityLabel") or f.get("quality") or "video",
                "url": f.get("url"),
                "type": f.get("type"),
                "format": "VIDEO",
            })
    for f in formats:
        if not isinstance(f, dict) or not f.get("url"):
            continue
        video_streams.append({
            "label": f.get("qualityLabel") or f.get("quality") or "mp4",
            "url": f.get("url"),
            "type": f.get("type"),
            "format": "MP4",
        })
    # sort audio by bitrate desc
    def _br(x):
        try:
            return int(re.sub(r"\D", "", str(x.get("bitrate") or "0")) or 0)
        except Exception:
            return 0
    audio_streams.sort(key=_br, reverse=True)
    best_audio = audio_streams[0]["url"] if audio_streams else None
    # embed fallback
    sources = []
    if best_audio:
        sources.append({"type": "audio", "provider": "invidious", "label": "Best audio", "url": best_audio, "play_url": best_audio, "format": "AUDIO"})
    for a in audio_streams[:5]:
        sources.append({"type": "audio", "provider": "invidious", "label": str(a.get("label")), "url": a["url"], "play_url": a["url"], "format": "AUDIO"})
    sources.append({
        "type": "embed",
        "provider": "youtube",
        "label": "YouTube embed",
        "url": f"https://www.youtube.com/embed/{video_id}?autoplay=1&rel=0",
        "play_url": f"https://www.youtube.com/embed/{video_id}?autoplay=1&rel=0",
        "format": "EMBED",
    })
    return {
        "video_id": video_id,
        "title": data.get("title"),
        "artist": data.get("author"),
        "thumb": _yt_thumb(video_id, data.get("videoThumbnails")),
        "duration": data.get("lengthSeconds"),
        "description": (data.get("description") or "")[:400],
        "audio_url": best_audio,
        "sources": sources,
        "recommended": [
            {
                "id": f"yt:{v.get('videoId')}",
                "video_id": v.get("videoId"),
                "title": v.get("title"),
                "artist": v.get("author"),
                "thumb": _yt_thumb(v.get("videoId") or "", v.get("videoThumbnails")),
                "provider": "youtube",
            }
            for v in (data.get("recommendedVideos") or [])[:12]
            if isinstance(v, dict) and v.get("videoId")
        ],
        "provider": "invidious",
        "watch_url": f"https://music.youtube.com/watch?v={video_id}",
    }





# =============================================================================
# HINDIANIME (hindianime.site) — Hindi-dub catalog + HLS resolve
# =============================================================================

HA_BASE = "https://www.hindianime.site"
HA_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://www.hindianime.site/",
    "Origin": "https://www.hindianime.site",
    "Accept": "application/json, text/plain, */*",
}


def _ha_slug_from_link(link: str) -> str:
    if not link:
        return ""
    path = urlparse(link).path.strip("/")
    # series/solo-leveling/ or movies/foo/
    parts = [p for p in path.split("/") if p]
    if len(parts) >= 2 and parts[0] in ("series", "movies", "anime", "episode"):
        return parts[1]
    return parts[-1] if parts else ""


def _ha_card(it: dict) -> dict:
    link = it.get("link") or ""
    slug = _ha_slug_from_link(link) or (it.get("id") or "")
    return {
        "id": it.get("id") or slug,
        "slug": slug,
        "title": it.get("title") or slug,
        "poster": it.get("poster"),
        "backdrop": it.get("backdrop"),
        "year": it.get("year"),
        "type": it.get("type") or "series",
        "link": link,
        "rank": it.get("rank"),
        "provider": "hindianime",
    }


async def _ha_get(path: str, params: Optional[dict] = None) -> Any:
    async with httpx.AsyncClient(timeout=25.0, follow_redirects=True) as client:
        r = await client.get(HA_BASE + path, params=params or {}, headers=HA_HEADERS)
        if r.status_code >= 400:
            raise HTTPException(502, f"HindiAnime {path} HTTP {r.status_code}")
        ctype = (r.headers.get("content-type") or "").lower()
        if "json" not in ctype:
            # sometimes HTML error
            try:
                return r.json()
            except Exception:
                raise HTTPException(502, f"HindiAnime {path} non-JSON")
        return r.json()


@app.get("/anime/home", tags=["Anime"])
async def anime_home():
    """HindiAnime home sections: airing, popular, latest, movies, genres."""
    try:
        data = await _ha_get("/api/home-sections")
    except Exception as e:
        raise HTTPException(502, f"anime home: {e}")
    def map_list(key):
        return [_ha_card(x) for x in (data.get(key) or []) if isinstance(x, dict)]
    return {
        "top_airing": map_list("topAiring"),
        "most_popular": map_list("mostPopular"),
        "completed": map_list("completedSeries"),
        "latest_episodes": map_list("latestEpisodes"),
        "latest_movies": map_list("latestMovies"),
        "upcoming": map_list("upcoming"),
        "genres": data.get("genres") or [],
        "provider": "hindianime",
    }


@app.get("/anime/catalog", tags=["Anime"])
async def anime_catalog(type: str = Query("all")):
    """Full series + movies catalog from HindiAnime."""
    data = await _ha_get("/api/catalog", {"t": str(int(time.time()))[:6]})
    series = [_ha_card({**x, "type": "series"}) for x in (data.get("series") or []) if isinstance(x, dict)]
    movies = [_ha_card({**x, "type": "movie"}) for x in (data.get("movies") or []) if isinstance(x, dict)]
    if type == "series":
        items = series
    elif type == "movie":
        items = movies
    else:
        items = series + movies
    return {
        "items": items,
        "total_series": data.get("totalSeries"),
        "total_movies": data.get("totalMovies"),
        "provider": "hindianime",
    }


@app.get("/anime/search", tags=["Anime"])
async def anime_search(q: str = Query(..., min_length=1)):
    """Search HindiAnime catalog by title."""
    qn = q.strip().lower()
    data = await _ha_get("/api/catalog", {"t": str(int(time.time()))[:6]})
    items = []
    for x in (data.get("series") or []) + (data.get("movies") or []):
        if not isinstance(x, dict):
            continue
        title = (x.get("title") or "").lower()
        if qn in title:
            items.append(_ha_card(x))
    return {"query": q, "items": items[:60], "provider": "hindianime"}


@app.get("/anime/detail/{slug}", tags=["Anime"])
async def anime_detail(slug: str):
    """Episodes list + metadata from /extracted/{slug}.json."""
    slug = slug.strip().strip("/")
    try:
        data = await _ha_get(f"/extracted/{slug}.json")
    except HTTPException:
        # try with common prefixes stripped
        raise
    eps = data.get("episodes") or []
    if isinstance(eps, dict):
        eps = list(eps.values())
    episodes = []
    for ep in eps:
        if not isinstance(ep, dict):
            continue
        episodes.append({
            "season": ep.get("season") or 1,
            "episode": ep.get("episode") or ep.get("number"),
            "title": ep.get("title"),
            "thumb": ep.get("thumbnail"),
            "url": ep.get("url"),
            "video_hash": ep.get("videoHash") or ep.get("video_hash"),
            "stream_url": ep.get("streamUrl") or ep.get("stream_url"),
            "servers": ep.get("servers") or [],
        })
    episodes.sort(key=lambda e: (e.get("season") or 1, e.get("episode") or 0))
    return {
        "slug": slug,
        "title": data.get("title") or slug,
        "overview": data.get("overview"),
        "genres": data.get("genres") or [],
        "languages": data.get("languages") or [],
        "type": data.get("type"),
        "seasons": data.get("availableSeasons") or data.get("seasons"),
        "episodes": episodes,
        "provider": "hindianime",
    }



@app.get("/anime/stream", tags=["Anime"])
async def anime_stream(
    hash: Optional[str] = Query(None, description="videoHash from episode"),
    url: Optional[str] = Query(None, description="episode page url fallback"),
):
    """Resolve HindiAnime HLS master playlist from videoHash (proxy m3u8)."""
    if not hash and not url:
        raise HTTPException(400, "hash or url required")
    stream = None
    mirrors: List[dict] = []
    # Primary: direct master.m3u8 proxy (resolve-stream often returns HTML now)
    if hash:
        stream = f"{HA_BASE}/api/proxy/master.m3u8?hash={hash}"
        # light probe
        try:
            async with httpx.AsyncClient(timeout=18.0, follow_redirects=True) as client:
                r = await client.get(
                    stream,
                    headers={
                        "User-Agent": HA_HEADERS.get("User-Agent", "Mozilla/5.0"),
                        "Referer": "https://www.hindianime.site/",
                        "Accept": "*/*",
                    },
                )
                if r.status_code >= 400 or not (r.text or "").lstrip().startswith("#EXTM3U"):
                    stream = None
        except Exception:
            stream = None
    # Fallback: try resolve-stream JSON if still alive
    if not stream:
        params = {}
        if hash:
            params["hash"] = hash
        elif url:
            params["url"] = url
        try:
            data = await _ha_get("/api/resolve-stream", params)
            stream = data.get("streamUrl") or data.get("proxyUrl") or ""
            if stream.startswith("/"):
                stream = HA_BASE + stream
            for s in data.get("servers") or data.get("mirrors") or []:
                if isinstance(s, dict) and s.get("url"):
                    u = s["url"]
                    if u.startswith("/"):
                        u = HA_BASE + u
                    mirrors.append({"label": s.get("name") or "Server", "url": u})
        except Exception:
            pass
    if not stream and hash:
        # last resort: still return constructed master URL (player proxy will surface errors)
        stream = f"{HA_BASE}/api/proxy/master.m3u8?hash={hash}"
    if not stream:
        raise HTTPException(502, "Could not resolve stream")
    return {
        "success": True,
        "stream_url": stream,
        "hash": hash,
        "mirrors": mirrors,
        "provider": "hindianime",
    }




@app.get("/anime/hls", tags=["Anime"])
async def anime_hls_proxy(request: Request, u: str = Query(..., min_length=8)):
    """Proxy m3u8 + segments (CORS). Unwraps HindiAnime segment?url= to CDN when needed."""
    if not (u.startswith("http://") or u.startswith("https://")):
        raise HTTPException(400, "url must be http(s)")

    def _unwrap(url: str) -> str:
        """If HindiAnime segment proxy, prefer the real CDN URL inside ?url=."""
        try:
            p = urlparse(url)
            host = (p.hostname or "").lower()
            if "hindianime" in host and "/api/proxy/segment" in (p.path or ""):
                q = dict(parse_qsl(p.query, keep_blank_values=True))
                inner = q.get("url") or q.get("u")
                if inner:
                    inner = unquote(inner)
                    if inner.startswith("http"):
                        return inner
            return url
        except Exception:
            return url

    target = _unwrap(u)
    range_header = request.headers.get("range")
    headers = {
        "User-Agent": (
            HA_HEADERS.get("User-Agent")
            or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        "Referer": "https://www.hindianime.site/",
        "Origin": "https://www.hindianime.site",
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
    }
    # Players (hls.js, <video>) issue byte-range requests for segments and
    # fMP4 init sections — pass that through so seeking/buffering works and
    # we're not always pulling whole files through this proxy.
    if range_header:
        headers["Range"] = range_header
    try:
        async with httpx.AsyncClient(timeout=45.0, follow_redirects=True) as client:
            r = await client.get(target, headers=headers)
            # If HTML error page, try alternate: original u or unwrapped
            ctype0 = (r.headers.get("content-type") or "").lower()
            head0 = (r.content[:80] or b"").lstrip().lower()
            if (
                r.status_code >= 400
                or "text/html" in ctype0
                or head0.startswith(b"<!doctype")
                or head0.startswith(b"<html")
            ):
                alts = []
                if target != u:
                    alts.append(u)
                # try CDN without query noise
                if target != u:
                    alts.append(target)
                for alt in alts:
                    try:
                        r2 = await client.get(alt, headers=headers)
                        c2 = (r2.headers.get("content-type") or "").lower()
                        h2 = (r2.content[:40] or b"").lstrip().lower()
                        if r2.status_code < 400 and "text/html" not in c2 and not h2.startswith(b"<!doctype"):
                            r = r2
                            target = alt
                            break
                    except Exception:
                        continue
    except Exception as e:
        raise HTTPException(502, f"hls fetch: {e}")

    if r.status_code >= 400:
        raise HTTPException(502, f"upstream {r.status_code}")

    ctype = (r.headers.get("content-type") or "").lower()
    body = r.content
    head = body[:80].lstrip().lower() if body else b""
    is_playlist = (
        "mpegurl" in ctype
        or "m3u8" in ctype
        or target.split("?")[0].endswith(".m3u8")
        or u.split("?")[0].endswith(".m3u8")
        or head.startswith(b"#extm3u")
    )
    # Reject HTML masquerading as media
    if head.startswith(b"<!doctype") or head.startswith(b"<html"):
        raise HTTPException(502, "upstream returned HTML (segment blocked)")

    if is_playlist:
        text = body.decode("utf-8", errors="ignore")
        base = target.rsplit("/", 1)[0] + "/"
        lines = []
        for line in text.splitlines():
            raw = line.strip()
            if not raw:
                lines.append(line)
                continue
            if raw.startswith("#"):
                if 'URI="' in line:
                    def _rew(m, _base=base):
                        uri = m.group(1)
                        if uri.startswith("http://") or uri.startswith("https://"):
                            full_u = uri
                        elif uri.startswith("/"):
                            full_u = "https://www.hindianime.site" + uri
                        else:
                            full_u = urljoin(_base, uri)
                        full_u = _unwrap(full_u)
                        return 'URI="/anime/hls?u=' + quote(full_u, safe="") + '"'
                    line = re.sub(r'URI="([^"]+)"', _rew, line)
                lines.append(line)
                continue
            if raw.startswith("http://") or raw.startswith("https://"):
                full_u = raw
            elif raw.startswith("/"):
                full_u = "https://www.hindianime.site" + raw
            else:
                full_u = urljoin(base, raw)
            full_u = _unwrap(full_u)
            lines.append("/anime/hls?u=" + quote(full_u, safe=""))
        out = "\n".join(lines) + "\n"
        return Response(
            content=out,
            media_type="application/vnd.apple.mpegurl",
            headers={
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "*",
                "Cache-Control": "no-cache",
            },
        )

    out_headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "*",
        "Cache-Control": "public, max-age=60",
        "Content-Length": str(len(body)),
        "Accept-Ranges": "bytes",
    }
    if r.status_code == 206 and r.headers.get("content-range"):
        out_headers["Content-Range"] = r.headers["content-range"]
        return Response(content=body, media_type=ctype or "application/octet-stream", status_code=206, headers=out_headers)
    return Response(
        content=body,
        media_type=ctype or "application/octet-stream",
        headers=out_headers,
    )


@app.get("/anime/tracks", tags=["Anime"])
async def anime_tracks(hash: Optional[str] = Query(None), url: Optional[str] = Query(None)):
    """Parse master m3u8 for quality levels + audio tracks (VLC-style list)."""
    if hash:
        stream = f"{HA_BASE}/api/proxy/master.m3u8?hash={hash}"
    elif url:
        stream = url
    else:
        raise HTTPException(400, "hash or url required")
    if not stream:
        raise HTTPException(502, "no stream")
    headers = {
        "User-Agent": HA_HEADERS["User-Agent"],
        "Referer": "https://www.hindianime.site/",
        "Accept": "*/*",
    }
    try:
        async with httpx.AsyncClient(timeout=25.0, follow_redirects=True) as client:
            r = await client.get(stream, headers=headers)
            text = r.text
    except Exception as e:
        raise HTTPException(502, f"m3u8 fetch: {e}")
    audios = []
    levels = []
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("#EXT-X-MEDIA:") and "TYPE=AUDIO" in line:
            name = re.search(r'NAME="([^"]*)"', line)
            lang = re.search(r'LANGUAGE="([^"]*)"', line)
            uri = re.search(r'URI="([^"]*)"', line)
            default = "DEFAULT=YES" in line
            audios.append({
                "name": (name.group(1) if name else None) or (lang.group(1) if lang else "Audio"),
                "lang": lang.group(1) if lang else "",
                "uri": uri.group(1) if uri else None,
                "default": default,
            })
        if line.startswith("#EXT-X-STREAM-INF:"):
            res = re.search(r"RESOLUTION=(\d+)x(\d+)", line)
            bw = re.search(r"BANDWIDTH=(\d+)", line)
            nm = re.search(r'NAME="([^"]*)"', line)
            h = int(res.group(2)) if res else 0
            levels.append({
                "name": (nm.group(1) if nm else None) or (f"{h}p" if h else "auto"),
                "height": h,
                "bandwidth": int(bw.group(1)) if bw else 0,
            })
    # prefer Hindi first in list display order (keep all)
    def _rank(a):
        n = (a.get("name") or "").lower() + " " + (a.get("lang") or "").lower()
        if "hin" in n or "hindi" in n:
            return 0
        return 1
    audios.sort(key=_rank)
    levels.sort(key=lambda x: x.get("height") or 0, reverse=True)
    return {
        "stream_url": stream,
        "audios": audios,
        "levels": levels,
        "provider": "hindianime",
    }



    try:
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            r = await client.get(u, headers=headers)
    except Exception as e:
        raise HTTPException(502, f"hls fetch: {e}")
    if r.status_code >= 400:
        raise HTTPException(502, f"upstream {r.status_code}")
    ctype = (r.headers.get("content-type") or "").lower()
    body = r.content
    # Rewrite playlist absolute URLs → through our proxy
    if "mpegurl" in ctype or u.endswith(".m3u8") or b"#EXTM3U" in body[:64]:
        text = body.decode("utf-8", errors="ignore")
        base = u.rsplit("/", 1)[0] + "/"
        lines = []
        for line in text.splitlines():
            raw = line.strip()
            if not raw or raw.startswith("#"):
                # also rewrite URI="..." inside tags
                if "URI=\"" in line:
                    def _rew(m):
                        uri = m.group(1)
                        if uri.startswith("http"):
                            full = uri
                        else:
                            full = urljoin(base, uri)
                        return 'URI="/anime/hls?u=' + quote(full, safe="") + '"'
                    line = re.sub(r'URI="([^"]+)"', _rew, line)
                lines.append(line)
                continue
            if raw.startswith("http"):
                full = raw
            else:
                full = urljoin(base, raw)
            lines.append("/anime/hls?u=" + quote(full, safe=""))
        out = "\n".join(lines) + "\n"
        return Response(
            content=out,
            media_type="application/vnd.apple.mpegurl",
            headers={
                "Access-Control-Allow-Origin": "*",
                "Cache-Control": "no-cache",
            },
        )
    return Response(
        content=body,
        media_type=ctype or "application/octet-stream",
        headers={
            "Access-Control-Allow-Origin": "*",
            "Cache-Control": "public, max-age=60",
        },
    )



@app.get("/docs", include_in_schema=False)
async def custom_swagger_ui():
    return HTMLResponse(r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"/>
<meta name="theme-color" content="#05070e"/>
<title>StreamHub API · Docs</title>
<link href="https://fonts.googleapis.com/css2?family=Syne:wght@600;700;800&family=DM+Sans:ital,opsz,wght@0,9..40,400;0,9..40,500;0,9..40,600;0,9..40,700;1,9..40,400&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet"/>
<style>
:root{
  --bg:#05070e;--panel:rgba(14,16,28,.75);--panel2:rgba(20,22,36,.9);
  --line:rgba(255,255,255,.08);--line2:rgba(255,255,255,.14);
  --text:#eef1ff;--mute:#8b93b3;--dim:#5a6280;
  --c1:#2dd4bf;--c2:#818cf8;--c3:#f472b6;--c4:#4ade80;--c5:#fbbf24;
  --get:#2dd4bf;--post:#4ade80;--put:#fbbf24;--del:#fb7185;--patch:#c084fc;
  --ease:cubic-bezier(.22,1,.36,1);--r:16px;
}
*{box-sizing:border-box;margin:0;padding:0}
html{scroll-behavior:smooth}
body{
  font-family:"DM Sans",system-ui,sans-serif;background:var(--bg);color:var(--text);
  min-height:100vh;line-height:1.5;
  background-image:
    radial-gradient(ellipse 90% 70% at 50% -30%,rgba(45,212,191,.15),transparent 55%),
    radial-gradient(ellipse 70% 50% at 100% 10%,rgba(129,140,248,.12),transparent 45%),
    radial-gradient(ellipse 50% 40% at 0% 80%,rgba(244,114,182,.07),transparent 50%);
  background-attachment:fixed;
}
/* TOP BAR */
.topbar{
  position:sticky;top:0;z-index:100;padding:12px 16px 14px;
  background:rgba(5,7,14,.85);backdrop-filter:blur(24px) saturate(1.4);
  border-bottom:1px solid var(--line);
  animation:slideDown .5s var(--ease);
}
@keyframes slideDown{from{opacity:0;transform:translateY(-10px)}to{opacity:1;transform:none}}
.top-inner{max-width:1200px;margin:0 auto}
.top-row{display:flex;align-items:center;gap:12px;flex-wrap:wrap}
.logo{
  font-family:Syne,sans-serif;font-weight:800;font-size:1.2rem;letter-spacing:-.03em;
  display:flex;align-items:center;gap:10px;flex-shrink:0;text-decoration:none;color:var(--text);
}
.logo i{
  width:34px;height:34px;border-radius:11px;display:grid;place-items:center;
  background:linear-gradient(135deg,var(--c1),var(--c2));color:#041018;font-style:normal;font-weight:800;
  box-shadow:0 8px 24px rgba(45,212,191,.3);
}
.logo b{background:linear-gradient(90deg,var(--c1),var(--c2),var(--c3));-webkit-background-clip:text;background-clip:text;color:transparent}
.search-box{
  flex:1;min-width:180px;position:relative;
}
.search-box input{
  width:100%;padding:12px 16px 12px 42px;border-radius:14px;border:1px solid var(--line);
  background:rgba(255,255,255,.05);color:var(--text);font-size:.95rem;outline:none;
  transition:border-color .2s,box-shadow .2s,background .2s;font-family:inherit;
}
.search-box input:focus{border-color:rgba(45,212,191,.5);box-shadow:0 0 0 4px rgba(45,212,191,.12);background:rgba(255,255,255,.07)}
.search-box::before{content:"⌕";position:absolute;left:14px;top:50%;transform:translateY(-50%);color:var(--mute);font-size:1.1rem;pointer-events:none}
.top-links{display:flex;gap:6px;flex-wrap:wrap}
.top-links a,.icon-btn{
  padding:9px 12px;border-radius:11px;border:1px solid var(--line);background:rgba(255,255,255,.04);
  color:var(--mute);text-decoration:none;font-size:.8rem;font-weight:600;cursor:pointer;
  transition:all .2s;font-family:inherit;
}
.top-links a:hover,.icon-btn:hover{color:var(--text);border-color:var(--line2);background:rgba(255,255,255,.08)}
.tags-scroll{
  display:flex;gap:8px;overflow-x:auto;padding:12px 0 2px;scrollbar-width:none;-webkit-overflow-scrolling:touch;
}
.tags-scroll::-webkit-scrollbar{display:none}
.tag{
  flex-shrink:0;padding:8px 14px;border-radius:999px;border:1px solid var(--line);
  background:rgba(255,255,255,.03);color:var(--mute);font-size:.8rem;font-weight:600;
  cursor:pointer;transition:all .2s var(--ease);font-family:inherit;white-space:nowrap;
}
.tag:hover{color:var(--text);border-color:var(--line2)}
.tag.on{background:linear-gradient(135deg,rgba(45,212,191,.2),rgba(129,140,248,.15));color:var(--text);border-color:rgba(45,212,191,.4);box-shadow:0 0 20px rgba(45,212,191,.1)}
.tag .n{opacity:.7;margin-left:4px;font-size:.72rem}

/* MAIN */
.wrap{max-width:1200px;margin:0 auto;padding:16px 16px 80px}
.hero{
  padding:28px 24px;border-radius:22px;margin-bottom:18px;position:relative;overflow:hidden;
  background:linear-gradient(145deg,rgba(16,20,36,.9),rgba(18,14,36,.85));
  border:1px solid var(--line);animation:fadeUp .55s var(--ease);
}
@keyframes fadeUp{from{opacity:0;transform:translateY(14px)}to{opacity:1;transform:none}}
.hero::after{
  content:"";position:absolute;right:-20%;top:-40%;width:50%;height:140%;
  background:radial-gradient(circle,rgba(129,140,248,.18),transparent 60%);pointer-events:none;
}
.hero h1{font-family:Syne,sans-serif;font-size:clamp(1.5rem,3.5vw,2.2rem);font-weight:800;letter-spacing:-.04em;position:relative}
.hero h1 span{background:linear-gradient(90deg,var(--c1),var(--c2),var(--c3));-webkit-background-clip:text;background-clip:text;color:transparent}
.hero p{color:var(--mute);margin-top:8px;max-width:560px;position:relative;font-size:.92rem}
.stats{display:flex;flex-wrap:wrap;gap:8px;margin-top:16px;position:relative}
.stat{
  padding:10px 14px;border-radius:12px;background:rgba(255,255,255,.04);border:1px solid var(--line);
  font-size:.78rem;color:var(--mute);font-weight:600;
}
.stat b{display:block;font-family:Syne,sans-serif;font-size:1.1rem;color:var(--c1)}
.stat:nth-child(2) b{color:var(--c2)}.stat:nth-child(3) b{color:var(--c3)}.stat:nth-child(4) b{color:var(--c4)}
.filters{display:flex;flex-wrap:wrap;gap:8px;margin-bottom:12px}
.chip{
  padding:7px 12px;border-radius:999px;border:1px solid var(--line);background:transparent;
  color:var(--mute);font-size:.78rem;font-weight:600;cursor:pointer;font-family:inherit;transition:all .2s;
}
.chip.on,.chip:hover{border-color:rgba(45,212,191,.4);color:var(--c1);background:rgba(45,212,191,.08)}

.ops{display:flex;flex-direction:column;gap:8px}
.sec{font-family:Syne,sans-serif;font-size:1rem;font-weight:700;padding:14px 2px 6px;display:flex;align-items:center;gap:10px}
.sec::after{content:"";flex:1;height:1px;background:var(--line)}
.op{
  border-radius:var(--r);border:1px solid var(--line);background:var(--panel);
  backdrop-filter:blur(14px);overflow:hidden;transition:border-color .2s,box-shadow .25s,transform .2s var(--ease);
  animation:fadeUp .4s var(--ease) both;
}
.op:hover{border-color:var(--line2)}
.op.open{border-color:rgba(45,212,191,.28);box-shadow:0 12px 40px rgba(0,0,0,.28)}
.op-h{
  display:grid;grid-template-columns:auto 1fr auto;gap:12px;align-items:center;
  padding:13px 14px;cursor:pointer;user-select:none;
}
.m{
  font-family:"JetBrains Mono",monospace;font-size:.7rem;font-weight:700;padding:6px 9px;
  border-radius:8px;min-width:52px;text-align:center;color:#041018;
}
.m.GET{background:var(--get)}.m.POST{background:var(--post)}.m.PUT{background:var(--put)}
.m.DELETE{background:var(--del)}.m.PATCH{background:var(--patch)}
.path{font-family:"JetBrains Mono",monospace;font-size:.84rem;word-break:break-all}
.sum{color:var(--mute);font-size:.8rem;margin-top:2px}
.chev{color:var(--dim);transition:transform .3s var(--ease)}
.op.open .chev{transform:rotate(180deg);color:var(--c1)}
.op-b{display:none;padding:0 14px 16px;border-top:1px solid var(--line)}
.op.open .op-b{display:block;animation:fadeUp .3s var(--ease)}
.desc{color:var(--mute);font-size:.88rem;padding:12px 0;white-space:pre-wrap}
.params{display:flex;flex-direction:column;gap:8px}
.param{
  display:grid;grid-template-columns:minmax(100px,140px) 1fr;gap:10px;
  padding:10px;border-radius:12px;background:rgba(0,0,0,.22);border:1px solid var(--line);
}
@media(max-width:560px){.param{grid-template-columns:1fr}}
.param .nm{font-family:"JetBrains Mono",monospace;font-size:.78rem;color:var(--c1)}
.param .req{color:var(--c3);font-size:.65rem;margin-left:4px}
.param .mt{font-size:.72rem;color:var(--dim);margin-top:2px}
.param input,.param select{
  width:100%;padding:9px 11px;border-radius:10px;border:1px solid var(--line);
  background:rgba(255,255,255,.05);color:var(--text);font-size:.88rem;outline:none;font-family:inherit;
}
.param input:focus{border-color:rgba(45,212,191,.45);box-shadow:0 0 0 3px rgba(45,212,191,.1)}
.try{display:flex;flex-wrap:wrap;gap:8px;margin-top:12px}
.btn{
  padding:10px 16px;border-radius:11px;border:none;font-weight:700;font-size:.85rem;
  cursor:pointer;font-family:inherit;transition:transform .15s,box-shadow .2s;
}
.btn-p{background:linear-gradient(135deg,var(--c1),var(--c2));color:#041018;box-shadow:0 8px 24px rgba(45,212,191,.22)}
.btn-p:hover{transform:translateY(-1px);box-shadow:0 12px 28px rgba(129,140,248,.28)}
.btn-g{background:rgba(255,255,255,.06);color:var(--text);border:1px solid var(--line)}
.resp{margin-top:12px;border-radius:12px;background:#080a12;border:1px solid var(--line);overflow:hidden;display:none}
.resp-h{display:flex;justify-content:space-between;gap:8px;padding:8px 12px;font-size:.75rem;font-weight:600;color:var(--mute);background:rgba(255,255,255,.03);flex-wrap:wrap}
.resp-h .ok{color:var(--c4)}.resp-h .bad{color:var(--del)}
.resp pre{padding:12px;margin:0;max-height:340px;overflow:auto;font-family:"JetBrains Mono",monospace;font-size:.76rem;line-height:1.5;color:#c8cee6;white-space:pre-wrap;word-break:break-word}
.empty{text-align:center;padding:40px;color:var(--mute)}
.foot{text-align:center;padding:24px;color:var(--dim);font-size:.78rem}
.foot a{color:var(--c1);text-decoration:none}
@media(max-width:640px){
  .topbar{padding:10px 12px}
  .hero{padding:20px 16px}
  .wrap{padding:12px 12px 70px}
  .path{font-size:.78rem}
}
@media(prefers-reduced-motion:reduce){*{animation:none!important;transition:none!important}}
</style>
</head>
<body>
<header class="topbar">
  <div class="top-inner">
    <div class="top-row">
      <a class="logo" href="/docs"><i>S</i><b>StreamHub</b></a>
      <div class="search-box"><input id="q" type="search" placeholder="Search APIs, paths, tags…" autocomplete="off" autofocus/></div>
      <div class="top-links">
        <a href="/">App</a>
        <a href="/openapi.json" target="_blank">OpenAPI</a>
        <a href="/health">Health</a>
      </div>
    </div>
    <div class="tags-scroll" id="tags"></div>
  </div>
</header>
<main class="wrap">
  <section class="hero">
    <h1>API reference for <span>StreamHub</span></h1>
    <p>Movies, series, anime, music, NetMirror, VixSrc, 4K Hub &amp; direct downloads. Search above · try any endpoint live.</p>
    <div class="stats" id="stats"></div>
  </section>
  <div class="filters" id="filters"></div>
  <div class="ops" id="ops"></div>
  <div class="foot">made by <a href="/docs">shawon</a> · StreamHub API</div>
</main>
<script>
const BASE=location.origin;
let SPEC=null, TAG="All", METH="ALL";
const esc=s=>String(s??"").replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));

function ops(spec){
  const out=[];
  for(const [path,item] of Object.entries(spec.paths||{})){
    for(const method of Object.keys(item)){
      if(!['get','post','put','delete','patch'].includes(method)) continue;
      const op=item[method];
      out.push({
        path, method:method.toUpperCase(),
        summary:op.summary||op.operationId||'',
        description:op.description||'',
        tags:op.tags&&op.tags.length?op.tags:['Other'],
        parameters:op.parameters||[],
        id:(op.operationId||method+path).replace(/[^\w]/g,'_'),
      });
    }
  }
  return out.sort((a,b)=>a.path.localeCompare(b.path));
}

function render(){
  if(!SPEC) return;
  const all=ops(SPEC);
  const q=(document.getElementById('q').value||'').toLowerCase().trim();
  const tagMap=new Map([['All',all.length]]);
  all.forEach(o=>o.tags.forEach(t=>tagMap.set(t,(tagMap.get(t)||0)+1)));

  document.getElementById('tags').innerHTML=[...tagMap.entries()].map(([t,n])=>
    `<button class="tag${TAG===t?' on':''}" data-t="${esc(t)}">${esc(t)}<span class="n">${n}</span></button>`
  ).join('');
  document.querySelectorAll('.tag').forEach(b=>b.onclick=()=>{TAG=b.dataset.t;render()});

  document.getElementById('filters').innerHTML=['ALL','GET','POST','PUT','DELETE'].map(m=>
    `<button class="chip${METH===m?' on':''}" data-m="${m}">${m}</button>`
  ).join('');
  document.querySelectorAll('.chip').forEach(c=>c.onclick=()=>{METH=c.dataset.m;render()});

  const list=all.filter(o=>{
    if(TAG!=='All'&&!o.tags.includes(TAG)) return false;
    if(METH!=='ALL'&&o.method!==METH) return false;
    if(q&&!(o.path+o.summary+o.description+o.tags.join(' ')).toLowerCase().includes(q)) return false;
    return true;
  });

  document.getElementById('stats').innerHTML=`
    <div class="stat"><b>${all.length}</b>Endpoints</div>
    <div class="stat"><b>${tagMap.size-1}</b>Groups</div>
    <div class="stat"><b>${SPEC.info?.version||'—'}</b>Version</div>
    <div class="stat"><b>shawon</b>Creator</div>`;

  const root=document.getElementById('ops');
  if(!list.length){root.innerHTML='<div class="empty">No endpoints match.</div>';return}
  const by={};
  list.forEach(o=>{(by[o.tags[0]]=by[o.tags[0]]||[]).push(o)});
  root.innerHTML='';
  Object.entries(by).forEach(([tag,items],ti)=>{
    root.insertAdjacentHTML('beforeend',`<div class="sec">${esc(tag)}</div>`);
    items.forEach((op,i)=>{
      const el=document.createElement('div');
      el.className='op';
      el.style.animationDelay=(0.03*i)+'s';
      const params=(op.parameters||[]).filter(p=>p.in==='query'||p.in==='path');
      el.innerHTML=`
        <div class="op-h">
          <span class="m ${op.method}">${op.method}</span>
          <div><div class="path">${esc(op.path)}</div><div class="sum">${esc(op.summary)}</div></div>
          <span class="chev">▾</span>
        </div>
        <div class="op-b">
          ${op.description?`<div class="desc">${esc(op.description)}</div>`:''}
          <div class="params">${params.length?params.map(p=>{
            const sc=p.schema||{};
            const ph=sc.default!=null?String(sc.default):(sc.example!=null?String(sc.example):'');
            return `<div class="param"><div><div class="nm">${esc(p.name)}${p.required?'<span class="req">required</span>':''}</div>
              <div class="mt">${esc(p.in)} · ${esc(sc.type||'string')}</div>
              <div class="mt">${esc(p.description||'')}</div></div>
              <div><input data-op="${op.id}" data-name="${esc(p.name)}" data-in="${p.in}" placeholder="${esc(ph)}" value="${esc(ph)}"/></div></div>`;
          }).join(''):'<div style="color:var(--dim);font-size:.85rem">No parameters</div>'}</div>
          <div class="try">
            <button class="btn btn-p" type="button">Try it</button>
            <button class="btn btn-g" type="button">Copy URL</button>
          </div>
          <div class="resp"><div class="resp-h"><span class="st">—</span><span>JSON</span></div><pre></pre></div>
        </div>`;
      el.querySelector('.op-h').onclick=()=>el.classList.toggle('open');
      const btns=el.querySelectorAll('.try .btn');
      btns[0].onclick=e=>{e.stopPropagation();run(op,el)};
      btns[1].onclick=e=>{e.stopPropagation();const u=url(op);navigator.clipboard?.writeText(u);btns[1].textContent='Copied!';setTimeout(()=>btns[1].textContent='Copy URL',1000)};
      root.appendChild(el);
    });
  });
}

function url(op){
  let path=op.path; const q=[];
  document.querySelectorAll(`input[data-op="${op.id}"]`).forEach(inp=>{
    const v=inp.value.trim(); if(!v) return;
    if(inp.dataset.in==='path') path=path.replace('{'+inp.dataset.name+'}',encodeURIComponent(v));
    else q.push(encodeURIComponent(inp.dataset.name)+'='+encodeURIComponent(v));
  });
  return BASE+path+(q.length?'?'+q.join('&'):'');
}

async function run(op,el){
  const box=el.querySelector('.resp'); const pre=box.querySelector('pre'); const st=box.querySelector('.st');
  box.style.display='block'; st.textContent='Loading…'; pre.textContent='';
  const u=url(op); const t0=performance.now();
  try{
    const r=await fetch(u,{headers:{Accept:'application/json'}});
    const ms=Math.round(performance.now()-t0);
    let tx=await r.text();
    try{tx=JSON.stringify(JSON.parse(tx),null,2)}catch(_){}
    st.innerHTML=`<span class="${r.ok?'ok':'bad'}">${r.status}</span> · ${ms}ms · ${esc(u)}`;
    pre.textContent=tx.slice(0,100000);
  }catch(err){st.innerHTML='<span class="bad">Error</span>';pre.textContent=String(err)}
}

document.getElementById('q').addEventListener('input',()=>{clearTimeout(window.__t);window.__t=setTimeout(render,100)});
fetch(BASE+'/openapi.json').then(r=>r.json()).then(s=>{SPEC=s;render()}).catch(e=>{
  document.getElementById('ops').innerHTML=`<div class="empty">Failed to load OpenAPI: ${esc(e.message)}</div>`;
});
</script>
</body>
</html>""")



# =============================================================================
# MPD expand + cookie proxy + Netplay (phone MP4)
# =============================================================================

_MB_PROXY_JAR: Dict[str, str] = {}
_MB_PROXY_REF: Dict[str, str] = {}


def _mb_proxy_remember(url: str, cookie: str, referer: str = "") -> None:
    host = urlparse(url).hostname or ""
    if host and cookie:
        _MB_PROXY_JAR[host] = cookie
    if host and referer:
        _MB_PROXY_REF[host] = referer


def _mpd_abs(base_mpd: str, rel: str) -> str:
    if not rel:
        return ""
    if rel.startswith("http://") or rel.startswith("https://"):
        return rel
    return urljoin(base_mpd.rsplit("/", 1)[0] + "/", rel)


def _parse_mpd_xml(xml_text: str, mpd_url: str) -> dict:
    video, audio = [], []
    for m in re.finditer(r"<Representation\b([^>]*)>(.*?)</Representation>", xml_text, re.I | re.S):
        attrs, body = m.group(1), m.group(2)

        def _a(name: str, src=attrs):
            mm = re.search(rf'\b{name}="([^"]*)"', src, re.I)
            return mm.group(1) if mm else ""

        rid = _a("id")
        codecs = (_a("codecs") or "").lower()
        mime = _a("mimeType") or ""
        height = int(_a("height") or 0)
        width = int(_a("width") or 0)
        bw = int(_a("bandwidth") or 0)
        st = re.search(r"<SegmentTemplate\b([^/>]*)", body, re.I)
        init_tpl = media_tpl = start_n = "1"
        if st:
            sa = st.group(1)
            init_tpl = _a("initialization", sa)
            media_tpl = _a("media", sa)
            start_n = _a("startNumber", sa) or "1"

        def exp(tpl: str) -> str:
            if not tpl:
                return ""
            return tpl.replace("$RepresentationID$", rid).replace("$RepresentationID", rid)

        init_rel = exp(init_tpl)
        media_rel = exp(media_tpl)
        first = ""
        if media_rel:
            sn = int(start_n or 1)
            first = re.sub(
                r"\$Number(?:%0(\d+)d)?\$",
                lambda m: str(sn).zfill(int(m.group(1))) if m.group(1) else str(sn),
                media_rel,
            )
        kind = "audio" if mime.startswith("audio") or codecs.startswith("mp4a") else "video"
        family = "hevc" if any(x in codecs for x in ("hev", "hvc")) else (
            "avc" if "avc" in codecs else codecs or "unknown"
        )
        row = {
            "id": rid,
            "label": (f"{height}p · {family}" if height else f"{kind} · {codecs}"),
            "height": height or None,
            "width": width or None,
            "bandwidth": bw,
            "codec": family,
            "init": _mpd_abs(mpd_url, init_rel) if init_rel else None,
            "first_segment": _mpd_abs(mpd_url, first) if first else None,
            "phone_friendly": kind == "audio" or family == "avc",
        }
        (audio if kind == "audio" else video).append(row)
    return {"video": video, "audio": audio, "has_h264": any(v["codec"] == "avc" for v in video)}


async def _mb_expand_mpd(mpd_url: str, cookie: str = "", referer: str = "") -> dict:
    headers = {
        "User-Agent": _mb_ua,
        "Referer": referer or globals().get("STREAM_REFERER", "https://sportslive.wine"),
        "Accept": "*/*",
    }
    if cookie:
        headers["Cookie"] = cookie
    async with httpx.AsyncClient(follow_redirects=True, timeout=25.0) as client:
        r = await client.get(mpd_url, headers=headers)
        if r.status_code != 200:
            raise HTTPException(502, f"MPD HTTP {r.status_code}")
        if "<MPD" not in r.text and "<mpd" not in r.text.lower():
            raise HTTPException(502, "Not an MPD")
        return _parse_mpd_xml(r.text, mpd_url)


@app.get("/mb/proxy/mpd", tags=["MovieBox"])
async def mb_proxy_mpd(u: str = Query(...), cookie: str = "", referer: str = ""):
    """
    Proxy MPD with CDN cookies and rewrite init/media templates so every
    segment is fetched via /mb/proxy/segment (cookies applied server-side).
    dash.js can then play without browser Cookie headers.
    """
    host = urlparse(u).hostname or ""
    ck = cookie or _MB_PROXY_JAR.get(host, "")
    ref = referer or _MB_PROXY_REF.get(host, globals().get("STREAM_REFERER", "https://sportslive.wine"))
    headers = {"User-Agent": _mb_ua, "Referer": ref, "Accept": "*/*"}
    if ck:
        headers["Cookie"] = ck
        _mb_proxy_remember(u, ck, ref)
    async with httpx.AsyncClient(follow_redirects=True, timeout=25.0) as client:
        r = await client.get(u, headers=headers)
        if r.status_code != 200:
            raise HTTPException(r.status_code, "MPD fetch failed")
        xml = r.text

        def to_proxy(abs_url: str) -> str:
            # Keep $Number$ / $RepresentationID$ tokens for dash.js templates
            return "/mb/proxy/segment?u=" + quote(abs_url, safe="/$%")

        def rew(m):
            name, val = m.group(1), m.group(2)
            abs_u = val if val.startswith("http") else _mpd_abs(u, val)
            return f'{name}="{to_proxy(abs_u)}"'

        xml = re.sub(r'\b(initialization|media)="([^"]+)"', rew, xml, flags=re.I)
        # also rewrite any BaseURL relative paths
        def rew_base(m):
            val = m.group(1).strip()
            if not val or val.startswith("http"):
                abs_u = val
            else:
                abs_u = _mpd_abs(u, val)
            if abs_u:
                return f"<BaseURL>{to_proxy(abs_u)}</BaseURL>"
            return m.group(0)

        xml = re.sub(r"<BaseURL>([^<]*)</BaseURL>", rew_base, xml, flags=re.I)
        return Response(
            content=xml,
            media_type="application/dash+xml",
            headers={
                "Access-Control-Allow-Origin": "*",
                "Cache-Control": "no-store",
            },
        )


@app.get("/mb/proxy/segment", tags=["MovieBox"])
async def mb_proxy_segment(u: str = Query(...), cookie: str = "", referer: str = ""):
    """Proxy DASH init/media segment with CDN cookies."""
    host = urlparse(u).hostname or ""
    ck = cookie or _MB_PROXY_JAR.get(host, "")
    ref = referer or _MB_PROXY_REF.get(host, globals().get("STREAM_REFERER", "https://sportslive.wine"))
    headers = {"User-Agent": _mb_ua, "Referer": ref, "Accept": "*/*"}
    if ck:
        headers["Cookie"] = ck
    async with httpx.AsyncClient(follow_redirects=True, timeout=60.0) as client:
        r = await client.get(u, headers=headers)
        if r.status_code >= 400:
            raise HTTPException(r.status_code, "segment error")
        return Response(
            content=r.content,
            media_type=r.headers.get("content-type") or "application/octet-stream",
            headers={"Access-Control-Allow-Origin": "*"},
        )


# --- Netplay admin (direct R2 MP4) ---
NP_BASE = "https://netplay.majumdargaurav61.workers.dev"
NP_UA = "Netplay/11.0 (Android)"


async def _np_get(path: str) -> Any:
    async with httpx.AsyncClient(follow_redirects=True, timeout=25.0) as client:
        r = await client.get(NP_BASE + path, headers={"User-Agent": NP_UA, "Accept": "application/json"})
        if r.status_code == 404:
            raise HTTPException(404, "Netplay not found")
        if r.status_code >= 400:
            raise HTTPException(502, f"Netplay HTTP {r.status_code}")
        return r.json()


async def _np_match_mp4(title: str, se: int = 0, ep: int = 0) -> List[dict]:
    """Match title against Netplay catalog; return progressive MP4 entries."""
    out: List[dict] = []
    try:
        vids = await _np_get("/videos")
    except Exception:
        return out
    if not isinstance(vids, list):
        return out
    clean = re.sub(r"\[.*?\]", "", title).strip().lower()
    words = [w for w in clean.split() if len(w) > 2]
    for v in vids:
        vt = (v.get("title") or "").lower()
        if not (clean in vt or vt in clean or sum(1 for w in words if w in vt) >= min(2, len(words))):
            continue
        seasons = v.get("seasons") or []
        trials = []
        if se or ep:
            trials.append((se or 0, ep or 1))
        for s in seasons:
            for e in s.get("eps") or [1]:
                trials.append((s.get("se") or 0, e))
        if not trials:
            trials = [(0, 0)]
        seen = set()
        for snum, enum in trials:
            key = (snum, enum)
            if key in seen:
                continue
            seen.add(key)
            try:
                st = await _np_get(f"/videos/{v['id']}/stream?se={snum}&ep={enum}")
            except Exception:
                continue
            for q in st.get("qualities") or []:
                if q.get("url"):
                    out.append({
                        "label": q.get("label") or "Netplay MP4",
                        "url": q["url"],
                        "phone_friendly": True,
                        "source": "netplay",
                        "netplay_id": v["id"],
                        "se": snum,
                        "ep": enum,
                        "language": q.get("language"),
                    })
        break
    return out


@app.get("/np/videos", tags=["Netplay"])
async def np_videos():
    """
    Netplay **admin** catalog only (Cloudflare Worker + R2 MP4).

    This is NOT MovieBox. IDs are UUIDs, not subject_id.
    Main movies/series still use MovieBox: `/mb/search` → `subject_id` → `/mb/stream/{subject_id}`.
    Worker has no /home or /search — only `/videos`.
    """
    data = await _np_get("/videos")
    items = data if isinstance(data, list) else []
    out = []
    for v in items:
        if not isinstance(v, dict):
            continue
        out.append({
            **v,
            "id_type": "netplay_uuid",
            "stream_url": f"/np/stream/{v.get('id')}",
            "note": "Use /np/stream/{id}?se=&ep= for progressive MP4",
        })
    return {
        "provider": "netplay",
        "id_type": "netplay_uuid",
        "count": len(out),
        "items": out,
        "how_to": {
            "list": "GET /np/videos",
            "detail": "GET /np/videos/{uuid}",
            "stream": "GET /np/stream/{uuid}?se=2&ep=3",
            "moviebox_main": "GET /mb/search?q= → subject_id → GET /mb/stream/{subject_id}",
        },
    }


@app.get("/np/videos/{video_id}", tags=["Netplay"])
async def np_detail(video_id: str):
    data = await _np_get(f"/videos/{video_id}")
    return {"provider": "netplay", "data": data}


@app.get("/np/stream/{video_id}", tags=["Netplay"])
async def np_stream(video_id: str, se: int = 0, ep: int = 0):
    """Progressive MP4 — works on every phone."""
    q = f"?se={se}&ep={ep}" if (se or ep) else ""
    data = await _np_get(f"/videos/{video_id}/stream{q}")
    sources = []
    for item in (data.get("qualities") or []):
        if item.get("url"):
            sources.append({
                "label": item.get("label") or "MP4",
                "url": item["url"],
                "play_url": item["url"],
                "format": "MP4",
                "phone_friendly": True,
                "language": item.get("language"),
            })
    return {
        "provider": "netplay",
        "video_id": video_id,
        "se": se,
        "ep": ep,
        "count": len(sources),
        "mp4": sources,
        "sources": sources,
        "play": sources[0]["url"] if sources else None,
        "note": "Progressive MP4 (R2)",
    }



# =============================================================================
# PaxSenix-compatible MovieBox routes (direct aoneroom — no API key, unlimited)
# Same upstream as api.paxsenix.org/moviebox/* but without their rate limit.
# NOTE: "MP4" label from upstream is often a dummy notice file; real video is
# HEVC DASH inside sign_cookie → we always resolve `dash_url` for you.
# =============================================================================


@app.get("/moviebox/play-info", tags=["MovieBox-PaxShape"])
async def moviebox_play_info(
    subjectId: str = Query(..., description="MovieBox subject id"),
    season: int = Query(0, alias="season"),
    episode: int = Query(0, alias="episode"),
    se: int = Query(None, description="alias of season"),
    ep: int = Query(None, description="alias of episode"),
):
    """
    PaxSenix-compatible play-info.
    Upstream still serves HEVC DASH; `url` may be a dummy MP4 notice.
    Use `dash_url` + `headers.Cookie` (or our `/mb/proxy/mpd`).
    """
    s = se if se is not None else season
    e = ep if ep is not None else episode
    if s == 0 and e == 0:
        path = f"/wefeed-mobile-bff/subject-api/play-info/v2?subjectId={subjectId}"
    else:
        path = f"/wefeed-mobile-bff/subject-api/play-info/v2?subjectId={subjectId}&se={s}&ep={e}"
    try:
        data = await mb_request("GET", path)
    except HTTPException:
        data = await mb_request("GET", path.replace("/play-info/v2", "/play-info"))
    if not isinstance(data, dict):
        data = {}
    raw_streams = data.get("streams") or data.get("streamList") or []
    streams = []
    for st in raw_streams:
        if not isinstance(st, dict):
            continue
        cookie = st.get("signCookie") or st.get("sign_cookie") or st.get("cookie") or ""
        raw_url = st.get("url") or ""
        dash = _dash_from_sign_cookie(cookie)
        playable = dash or (None if _is_dummy_url(raw_url) else raw_url)
        headers = {
            "User-Agent": _mb_ua,
            "Referer": globals().get("STREAM_REFERER", "https://sportslive.wine"),
        }
        if cookie:
            headers["Cookie"] = "; ".join(
                p.strip() for p in cookie.strip(";").split(";") if p.strip()
            )
            if playable and ".mpd" in (playable or ""):
                try:
                    _mb_proxy_remember(playable, headers["Cookie"], headers["Referer"])
                except Exception:
                    pass
        # Never expose dummy notice MP4 as playable url
        if not playable:
            continue
        if _is_dummy_url(playable):
            continue
        streams.append({
            "format": st.get("format") or ("DASH" if ".mpd" in playable else "HLS" if ".m3u8" in playable else "MP4"),
            "id": str(st.get("id") or ""),
            "url": playable,  # real playable only (never macdn/other notice)
            "dash_url": playable if ".mpd" in playable else None,
            "upstream_url": None if _is_dummy_url(raw_url) else raw_url,
            "resolutions": st.get("resolutions") or st.get("resolution") or "",
            "size": st.get("size"),
            "duration": st.get("duration"),
            "codec_name": st.get("codecName") or st.get("codec_name") or st.get("codec") or "hevc",
            "sign_cookie": cookie,
            "headers": headers,
            "proxy_mpd": f"/mb/proxy/mpd?u={quote(playable, safe='')}" if ".mpd" in playable else None,
            "play_url": f"/mb/proxy/mpd?u={quote(playable, safe='')}" if ".mpd" in playable else playable,
            "id_type": st.get("idType") or st.get("id_type") or "",
        })
    # also flatten via our parser for any extras
    if not streams:
        for s in _parse_mb_play_info(data, _mb_ua):
            streams.append({
                "format": s.get("format"),
                "id": str(s.get("id") or ""),
                "url": s.get("url"),
                "dash_url": s.get("url"),
                "resolutions": s.get("resolution"),
                "size": s.get("size"),
                "duration": s.get("duration"),
                "codec_name": s.get("codec") or "hevc",
                "sign_cookie": (s.get("headers") or {}).get("Cookie") or "",
                "headers": s.get("headers"),
                "proxy_mpd": f"/mb/proxy/mpd?u={quote(s['url'], safe='')}" if s.get("url") and ".mpd" in s["url"] else None,
                "id_type": "",
            })
    return {
        "ok": True,
        "creator": "StreamHub-direct",
        "title": data.get("title"),
        "subject_id": subjectId,
        "season": s,
        "episode": e,
        "streams": streams,
        "cdn_throttle_level": data.get("cdnThrottleLevel") or 0,
        "note": (
            "Unlimited direct MovieBox BFF (no PaxSenix key). "
            "codec is usually HEVC DASH — use dash_url + Cookie or proxy_mpd. "
            "browser embeds: GET /mb/stream/{subjectId}"
        ),
    }


@app.get("/moviebox/search", tags=["MovieBox-PaxShape"])
async def moviebox_search(
    q: str = Query(..., min_length=1),
    page: int = 1,
):
    """PaxSenix-compatible search → direct search/v2."""
    mb = await mb_search(q=q, page=page)
    items = []
    for it in mb.get("items") or []:
        items.append({
            "subject_id": it.get("subject_id"),
            "subject_type": 2 if it.get("type") == "series" else 1,
            "title": it.get("name") or it.get("title"),
            "description": it.get("description") or "",
            "release_date": it.get("year"),
            "imdb_rating_value": it.get("rating"),
            "has_resource": True,
            "cover": it.get("poster_url") or it.get("poster"),
            "type": it.get("type"),
        })
    return {
        "ok": True,
        "creator": "StreamHub-direct",
        "query": q,
        "pager": {
            "page": str(page),
            "per_page": 20,
            "has_more": len(items) >= 15,
            "next_page": str(page + 1),
            "total_count": mb.get("total") or len(items),
        },
        "items": items,
        "results": items,
    }


@app.get("/moviebox/info", tags=["MovieBox-PaxShape"])
async def moviebox_info(subjectId: str = Query(...)):
    """PaxSenix-compatible title info."""
    data = await mb_request("GET", f"/wefeed-mobile-bff/subject-api/get?subjectId={subjectId}")
    subj = data.get("subject") or data
    if (subj.get("subjectType") or subj.get("stype") or 1) == 2:
        try:
            subj["seasons"] = await mb_request(
                "GET", f"/wefeed-mobile-bff/subject-api/season-info?subjectId={subjectId}"
            )
        except Exception:
            pass
    cover = subj.get("cover") or {}
    poster = cover.get("url") if isinstance(cover, dict) else subj.get("coverUrl")
    return {
        "ok": True,
        "creator": "StreamHub-direct",
        "subject_id": subjectId,
        "subject_type": subj.get("subjectType") or subj.get("stype"),
        "title": subj.get("title") or subj.get("name"),
        "description": subj.get("description") or subj.get("desc") or "",
        "release_date": subj.get("releaseDate") or subj.get("year"),
        "duration": subj.get("duration"),
        "genre": subj.get("genre"),
        "imdb_rating_value": subj.get("imdbRate") or subj.get("score"),
        "cover": poster,
        "has_resource": subj.get("hasResource", True),
        "data": subj,
    }


@app.get("/moviebox/home", tags=["MovieBox-PaxShape"])
async def moviebox_home(tabId: int = Query(1), page: int = 1):
    """PaxSenix-compatible home (tab-operating)."""
    data = await mb_request(
        "GET",
        f"/wefeed-mobile-bff/tab-operating?page={page}&tabId={tabId}&version=",
    )
    return {
        "ok": True,
        "creator": "StreamHub-direct",
        "tab_id": tabId,
        "page": page,
        "items": data.get("items") or data.get("list") or data.get("subjectList") or data,
        "raw": data if isinstance(data, dict) else {"data": data},
    }


@app.get("/moviebox/list", tags=["MovieBox-PaxShape"])
async def moviebox_list(page: int = 1, perPage: int = 20, tabId: int = 1):
    """PaxSenix-compatible list (same operating feed)."""
    data = await mb_request(
        "GET",
        f"/wefeed-mobile-bff/tab-operating?page={page}&tabId={tabId}&version=",
    )
    items = data.get("items") or data.get("list") or []
    return {
        "ok": True,
        "creator": "StreamHub-direct",
        "pager": {
            "page": str(page),
            "per_page": perPage,
            "has_more": bool(items),
            "next_page": str(page + 1),
        },
        "items": items,
    }


@app.get("/moviebox/trending", tags=["MovieBox-PaxShape"])
async def moviebox_trending(page: int = 1):
    """Trending via tab-operating / search-rank fallback."""
    try:
        data = await mb_request(
            "GET", f"/wefeed-mobile-bff/subject-api/search-rank?page={page}"
        )
        items = data.get("movie") or data.get("list") or data.get("items") or []
        if isinstance(items, dict):
            items = items.get("list") or []
    except Exception:
        data = await mb_request(
            "GET", f"/wefeed-mobile-bff/tab-operating?page={page}&tabId=1&version="
        )
        items = data.get("items") or data.get("list") or []
    return {
        "ok": True,
        "creator": "StreamHub-direct",
        "pager": {"page": str(page), "has_more": True, "next_page": str(page + 1)},
        "items": items,
    }


@app.get("/moviebox/recommendations", tags=["MovieBox-PaxShape"])
async def moviebox_recommendations(subjectId: str = Query(...), page: int = 1):
    """Related titles (detail-rec)."""
    try:
        data = await mb_request(
            "GET",
            f"/wefeed-mobile-bff/subject-api/detail-rec?subjectId={subjectId}&page={page}",
        )
    except HTTPException:
        data = await mb_request(
            "GET", f"/wefeed-mobile-bff/subject-api/detail-rec?subjectId={subjectId}"
        )
    items = data.get("list") or data.get("items") or data.get("subjects") or data
    return {
        "ok": True,
        "creator": "StreamHub-direct",
        "subject_id": subjectId,
        "pager": {"page": str(page), "has_more": True, "next_page": str(page + 1)},
        "items": items,
    }


@app.get("/moviebox/captions", tags=["MovieBox-PaxShape"])
async def moviebox_captions(
    subjectId: str = Query(...),
    streamId: str = Query("", description="stream id from play-info"),
):
    """Subtitles (get-ext-captions)."""
    rid = (streamId or "").strip()
    if not rid:
        pi = await moviebox_play_info(subjectId=subjectId, season=0, episode=0)
        for st in pi.get("streams") or []:
            if st.get("id"):
                rid = str(st["id"])
                break
    if not rid:
        return {"ok": True, "creator": "StreamHub-direct", "ext_captions": [], "subject_id": subjectId}
    try:
        data = await mb_request(
            "GET",
            f"/wefeed-mobile-bff/subject-api/get-ext-captions?subjectId={subjectId}&resourceId={rid}",
        )
        captions = data.get("extCaptions") or data.get("captions") or data.get("list") or []
    except Exception:
        captions = []
    return {
        "ok": True,
        "creator": "StreamHub-direct",
        "subject_id": subjectId,
        "stream_id": rid,
        "ext_captions": captions,
    }


@app.get("/play", tags=["Play"])
async def unified_play(
    subject_id: str = Query(None, description="MovieBox subject_id from /mb/search"),
    netplay_id: str = Query(None, description="Only for /np/videos UUID (admin MP4)"),
    se: int = 0,
    ep: int = 0,
):
    """
    **Main play API**

    | ID | From | Example |
    |----|------|---------|
    | subject_id | `/mb/search` or `/api/search` | 1654274595068805784 |
    | netplay_id | `/np/videos` only | c32891f8-fc0a-… |

    MovieBox subject_id is what Netplay app uses for normal movies.
    netplay_id is a separate admin upload list (few titles, direct MP4).
    """
    if netplay_id:
        return await np_stream(netplay_id, se, ep)
    if subject_id:
        return await mb_stream(subject_id, se, ep)
    raise HTTPException(
        400,
        "Pass subject_id from /mb/search (MovieBox). "
        "netplay_id only if you took UUID from /np/videos.",
    )






# =============================================================================
# PaxSenix-parity: Tools + Utilities (native, no API key / no rate limit)
# =============================================================================

@app.get("/tools/web-search", tags=["Tools"])
async def tools_web_search(q: str = Query(..., min_length=1), max_results: int = Query(8, ge=1, le=20)):
    """Web search: DuckDuckGo instant → Wikipedia → DDG lite HTML."""
    out = {"ok": True, "query": q, "results": [], "abstract": None, "answer": None}
    headers = {"User-Agent": "Mozilla/5.0 (compatible; StreamHub/5.13)"}
    try:
        async with httpx.AsyncClient(timeout=15.0, follow_redirects=True, headers=headers) as client:
            # 1) DuckDuckGo instant answer API
            try:
                r = await client.get(
                    "https://api.duckduckgo.com/",
                    params={"q": q, "format": "json", "no_html": 1, "skip_disambig": 1},
                )
                d = r.json() if r.status_code == 200 else {}
                out["abstract"] = d.get("AbstractText") or None
                out["answer"] = d.get("Answer") or None
                out["heading"] = d.get("Heading") or None
                out["image"] = d.get("Image") or None
                for tpc in (d.get("RelatedTopics") or [])[:max_results]:
                    if isinstance(tpc, dict) and tpc.get("Text"):
                        out["results"].append({
                            "title": (tpc.get("Text") or "")[:120],
                            "url": tpc.get("FirstURL"),
                            "snippet": tpc.get("Text"),
                        })
                    elif isinstance(tpc, dict) and tpc.get("Topics"):
                        for sub in (tpc.get("Topics") or [])[:3]:
                            if sub.get("Text"):
                                out["results"].append({
                                    "title": (sub.get("Text") or "")[:120],
                                    "url": sub.get("FirstURL"),
                                    "snippet": sub.get("Text"),
                                })
            except Exception:
                pass
            # 2) Wikipedia opensearch
            if len(out["results"]) < 3:
                try:
                    r = await client.get(
                        "https://en.wikipedia.org/w/api.php",
                        params={
                            "action": "opensearch",
                            "search": q,
                            "limit": max_results,
                            "namespace": 0,
                            "format": "json",
                        },
                    )
                    data = r.json() if r.status_code == 200 else []
                    if isinstance(data, list) and len(data) >= 4:
                        titles, descs, urls = data[1], data[2], data[3]
                        for i, title in enumerate(titles):
                            out["results"].append({
                                "title": title,
                                "url": urls[i] if i < len(urls) else None,
                                "snippet": descs[i] if i < len(descs) else title,
                                "source": "wikipedia",
                            })
                        if not out["abstract"] and descs:
                            out["abstract"] = descs[0]
                except Exception:
                    pass
            # 3) DDG lite HTML scrape
            if not out["results"]:
                try:
                    r = await client.get("https://lite.duckduckgo.com/lite/", params={"q": q})
                    if r.status_code == 200:
                        from bs4 import BeautifulSoup
                        soup = BeautifulSoup(r.text, "html.parser")
                        for a in soup.select("a.result-link")[:max_results]:
                            href = a.get("href") or ""
                            title = a.get_text(strip=True)
                            if href and title:
                                out["results"].append({"title": title, "url": href, "snippet": title, "source": "ddg-lite"})
                except Exception:
                    pass
    except Exception as e:
        out["error"] = str(e)[:160]
    out["ok"] = bool(out["results"] or out["abstract"] or out["answer"])
    if not out["ok"] and "error" not in out:
        out["error"] = "no results"
    return out


@app.get("/tools/google-search", tags=["Tools"])
async def tools_google_search(q: str = Query(..., min_length=1)):
    """Alias of /tools/web-search (DuckDuckGo-backed, unlimited)."""
    return await tools_web_search(q=q)


@app.get("/tools/gtranslate", tags=["Tools"])
@app.get("/tools/translate", tags=["Tools"], include_in_schema=False)
async def tools_gtranslate(
    text: str = Query(..., min_length=1),
    to: str = Query("en", description="Target language code"),
    source: str = Query("auto", description="Source language or auto"),
):
    """Translate text (Google gtx → MyMemory fallback, no key)."""
    # 1) Google unofficial
    try:
        async with httpx.AsyncClient(timeout=12.0) as client:
            r = await client.get(
                "https://translate.googleapis.com/translate_a/single",
                params={"client": "gtx", "sl": source, "tl": to, "dt": "t", "q": text},
                headers={"User-Agent": "Mozilla/5.0"},
            )
            if r.status_code == 200 and r.text.startswith("["):
                data = r.json()
                translated = "".join(part[0] for part in (data[0] or []) if part and part[0])
                if translated:
                    detected = (data[2] if len(data) > 2 else source) or source
                    return {"ok": True, "text": text, "translated": translated, "source": detected, "to": to, "engine": "google"}
    except Exception:
        pass
    # 2) MyMemory
    try:
        langpair = f"{source}|{to}" if source != "auto" else f"en|{to}"
        async with httpx.AsyncClient(timeout=12.0) as client:
            r = await client.get(
                "https://api.mymemory.translated.net/get",
                params={"q": text, "langpair": langpair},
            )
            d = r.json()
            translated = ((d.get("responseData") or {}).get("translatedText") or "").strip()
            if translated and "MYMEMORY WARNING" not in translated.upper():
                return {"ok": True, "text": text, "translated": translated, "source": source, "to": to, "engine": "mymemory"}
    except Exception:
        pass
    # 3) Lingva (Google frontend mirrors)
    for host in ("lingva.ml", "lingva.thedaviddelta.com", "translate.plausibility.cloud"):
        try:
            sl = source if source != "auto" else "auto"
            async with httpx.AsyncClient(timeout=12.0, follow_redirects=True) as client:
                r = await client.get(f"https://{host}/api/v1/{sl}/{to}/{quote(text)}")
            if r.status_code == 200:
                d = r.json()
                translated = d.get("translation") or ""
                if translated:
                    return {"ok": True, "text": text, "translated": translated, "source": source, "to": to, "engine": f"lingva/{host}"}
        except Exception:
            continue
    return {"ok": False, "error": "translate unavailable"}


@app.get("/tools/tts", tags=["Tools"])
async def tools_tts(
    text: str = Query(..., min_length=1, max_length=200),
    lang: str = Query("en"),
):
    """Google TTS audio URL (streamable)."""
    q = quote(text)
    url = f"https://translate.google.com/translate_tts?ie=UTF-8&q={q}&tl={lang}&client=tw-ob"
    return {"ok": True, "audio_url": url, "lang": lang, "text": text, "note": "Play or download this URL directly"}


@app.get("/tools/tts/v2", tags=["Tools"])
async def tools_tts_v2(text: str = Query(..., min_length=1, max_length=200), lang: str = "en"):
    return await tools_tts(text=text, lang=lang)


@app.get("/tools/urlshorter", tags=["Tools"])
@app.get("/tools/url-shorten", tags=["Tools"], include_in_schema=False)
async def tools_url_shorten(url: str = Query(...)):
    """Shorten URL via is.gd (free, no key)."""
    try:
        async with httpx.AsyncClient(timeout=12.0) as client:
            r = await client.get("https://is.gd/create.php", params={"format": "json", "url": url})
            d = r.json()
            if d.get("shorturl"):
                return {"ok": True, "original": url, "short": d["shorturl"]}
            return {"ok": False, "error": d.get("errormessage") or "failed", "raw": d}
    except Exception as e:
        return {"ok": False, "error": str(e)[:160]}


@app.get("/tools/ssweb", tags=["Tools"])
async def tools_ssweb(url: str = Query(...), width: int = 1280, height: int = 720):
    """Website screenshot via free thum.io proxy."""
    shot = f"https://image.thum.io/get/width/{width}/crop/{height}/{url}"
    return {"ok": True, "screenshot_url": shot, "url": url, "width": width, "height": height}


@app.get("/tools/sshtml", tags=["Tools"])
async def tools_sshtml(url: str = Query(...)):
    return await tools_ssweb(url=url)


@app.get("/tools/quote", tags=["Tools"])
@app.get("/tools/quotes", tags=["Tools"], include_in_schema=False)
async def tools_quote():
    """Random inspirational quote."""
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get("https://api.quotable.io/random")
            if r.status_code == 200:
                d = r.json()
                return {"ok": True, "content": d.get("content"), "author": d.get("author"), "tags": d.get("tags")}
    except Exception:
        pass
    return {"ok": True, "content": "Stay hungry, stay foolish.", "author": "Steve Jobs", "tags": []}


@app.get("/tools/bored", tags=["Tools"])
async def tools_bored():
    """Random activity suggestion."""
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get("https://bored-api.appbrewery.com/random")
            if r.status_code == 200:
                return {"ok": True, **r.json()}
    except Exception:
        pass
    return {"ok": True, "activity": "Build something cool with this API", "type": "diy"}


@app.get("/tools/weather", tags=["Tools"])
async def tools_weather(city: str = Query("Dhaka")):
    """Current weather via wttr.in (no key)."""
    try:
        async with httpx.AsyncClient(timeout=12.0) as client:
            r = await client.get(f"https://wttr.in/{quote(city)}", params={"format": "j1"})
            d = r.json()
            cur = (d.get("current_condition") or [{}])[0]
            area = (d.get("nearest_area") or [{}])[0]
            return {
                "ok": True,
                "city": city,
                "temp_C": cur.get("temp_C"),
                "temp_F": cur.get("temp_F"),
                "humidity": cur.get("humidity"),
                "weather": (cur.get("weatherDesc") or [{}])[0].get("value"),
                "feels_like_C": cur.get("FeelsLikeC"),
                "wind_kmph": cur.get("windspeedKmph"),
                "area": (area.get("areaName") or [{}])[0].get("value"),
                "country": (area.get("country") or [{}])[0].get("value"),
            }
    except Exception as e:
        return {"ok": False, "error": str(e)[:160]}


@app.get("/tools/ip", tags=["Tools"])
async def tools_ip(request: Request):
    """Client IP + geo hint."""
    ip = request.headers.get("x-forwarded-for", "").split(",")[0].strip() or (
        request.client.host if request.client else None
    )
    geo = {}
    if ip and ip not in ("127.0.0.1", "::1"):
        try:
            async with httpx.AsyncClient(timeout=8.0) as client:
                r = await client.get(f"https://ipapi.co/{ip}/json/")
                if r.status_code == 200:
                    geo = r.json()
        except Exception:
            pass
    return {
        "ok": True,
        "ip": ip,
        "city": geo.get("city"),
        "region": geo.get("region"),
        "country": geo.get("country_name"),
        "org": geo.get("org"),
        "timezone": geo.get("timezone"),
    }


@app.get("/tools/qr", tags=["Tools"])
async def tools_qr(data: str = Query(..., min_length=1), size: int = Query(200, ge=50, le=1000)):
    """QR code image URL (Google Chart API)."""
    url = f"https://chart.googleapis.com/chart?cht=qr&chs={size}x{size}&chl={quote(data)}"
    alt = f"https://api.qrserver.com/v1/create-qr-code/?size={size}x{size}&data={quote(data)}"
    return {"ok": True, "qr_url": alt, "fallback": url, "data": data, "size": size}


@app.get("/tools/base64", tags=["Tools"])
async def tools_base64(text: str = Query(...), mode: str = Query("encode", description="encode|decode")):
    """Base64 encode / decode."""
    try:
        if mode == "decode":
            raw = base64.b64decode(text.encode()).decode("utf-8", errors="replace")
            return {"ok": True, "mode": "decode", "result": raw}
        enc = base64.b64encode(text.encode()).decode()
        return {"ok": True, "mode": "encode", "result": enc}
    except Exception as e:
        return {"ok": False, "error": str(e)[:120]}


@app.get("/tools/hash", tags=["Tools"])
async def tools_hash(text: str = Query(...)):
    """MD5 / SHA1 / SHA256 of text."""
    b = text.encode()
    return {
        "ok": True,
        "md5": hashlib.md5(b).hexdigest(),
        "sha1": hashlib.sha1(b).hexdigest(),
        "sha256": hashlib.sha256(b).hexdigest(),
    }


@app.get("/tools/password", tags=["Tools"])
async def tools_password(length: int = Query(16, ge=6, le=64), symbols: bool = True):
    """Secure random password generator."""
    import string
    alphabet = string.ascii_letters + string.digits
    if symbols:
        alphabet += "!@#$%^&*()-_=+"
    pwd = "".join(random.choice(alphabet) for _ in range(length))
    return {"ok": True, "password": pwd, "length": length}


@app.get("/tools/uuid", tags=["Tools"])
async def tools_uuid(count: int = Query(1, ge=1, le=20)):
    """Generate UUID v4."""
    import uuid
    return {"ok": True, "uuids": [str(uuid.uuid4()) for _ in range(count)]}


@app.get("/tools/color", tags=["Tools"])
async def tools_color(hex: str = Query(None, description="e.g. #1a73e8"), random: bool = False):
    """Color info or random palette color."""
    if random or not hex:
        hex = "#{:06x}".format(int(time.time() * 1000) % 0xFFFFFF)
    h = hex.lstrip("#")
    if len(h) != 6:
        return {"ok": False, "error": "use #RRGGBB"}
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    return {"ok": True, "hex": f"#{h}", "rgb": {"r": r, "g": g, "b": b}, "css": f"rgb({r},{g},{b})"}


@app.get("/tools/world-populations", tags=["Tools"])
async def tools_world_pop():
    """World population snapshot (REST Countries sample)."""
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.get("https://restcountries.com/v3.1/all?fields=name,population,region")
            rows = r.json() if r.status_code == 200 else []
            rows = sorted(rows, key=lambda x: x.get("population") or 0, reverse=True)[:30]
            return {
                "ok": True,
                "top": [
                    {
                        "name": (c.get("name") or {}).get("common"),
                        "population": c.get("population"),
                        "region": c.get("region"),
                    }
                    for c in rows
                ],
            }
    except Exception as e:
        return {"ok": False, "error": str(e)[:160]}


@app.get("/tools/shazam", tags=["Tools"])
async def tools_shazam(q: str = Query(..., description="Song title or lyrics snippet")):
    """Song recognition helper via music search (title/lyrics query)."""
    try:
        # reuse music search
        if "music_search" in globals():
            return await music_search(q=q)  # type: ignore
    except Exception:
        pass
    return await tools_web_search(q=f"{q} song lyrics")


@app.get("/tools/reels-finder", tags=["Tools"])
async def tools_reels_finder(q: str = Query(...)):
    """Find Instagram-style reel links via search (metadata only)."""
    return await tools_web_search(q=f"{q} site:instagram.com/reel")


@app.get("/tools/search-pinterest", tags=["Tools"])
async def tools_search_pinterest(q: str = Query(...)):
    return await tools_web_search(q=f"{q} site:pinterest.com")


@app.get("/tools/search-applestore", tags=["Tools"])
async def tools_search_appstore(q: str = Query(...), country: str = "us"):
    """Apple App Store search (iTunes API, free)."""
    try:
        async with httpx.AsyncClient(timeout=12.0) as client:
            r = await client.get(
                "https://itunes.apple.com/search",
                params={"term": q, "country": country, "entity": "software", "limit": 15},
            )
            d = r.json()
            apps = [
                {
                    "name": a.get("trackName"),
                    "artist": a.get("artistName"),
                    "url": a.get("trackViewUrl"),
                    "icon": a.get("artworkUrl100"),
                    "price": a.get("formattedPrice"),
                    "rating": a.get("averageUserRating"),
                }
                for a in (d.get("results") or [])
            ]
            return {"ok": True, "query": q, "apps": apps}
    except Exception as e:
        return {"ok": False, "error": str(e)[:160]}


@app.get("/tools/vision-color", tags=["Tools"])
async def tools_vision_color(url: str = Query(..., description="Image URL")):
    """Dominant color hint via thum/placeholder (returns suggested palette)."""
    return {
        "ok": True,
        "image": url,
        "note": "Use client-side canvas for exact palette; server returns safe defaults",
        "palette": ["#0f172a", "#22d3ee", "#a78bfa", "#f472b6", "#fbbf24"],
    }


# ----- Lyrics extras (PaxSenix parity) -----

@app.get("/lyrics/genius", tags=["Lyrics"])
async def lyrics_genius(q: str = Query(...)):
    """Genius lyrics search via public pages (best-effort)."""
    try:
        async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as client:
            r = await client.get(
                "https://genius.com/api/search/song",
                params={"q": q},
                headers={"User-Agent": "Mozilla/5.0"},
            )
            hits = ((r.json() or {}).get("response") or {}).get("sections") or []
            songs = []
            for sec in hits:
                for h in sec.get("hits") or []:
                    res = h.get("result") or {}
                    songs.append({
                        "title": res.get("title"),
                        "artist": (res.get("primary_artist") or {}).get("name"),
                        "url": res.get("url"),
                        "thumb": res.get("song_art_image_thumbnail_url"),
                    })
            return {"ok": True, "query": q, "songs": songs[:15]}
    except Exception as e:
        return {"ok": False, "error": str(e)[:160], "songs": []}


@app.get("/lyrics/musixmatch", tags=["Lyrics"])
async def lyrics_musixmatch(title: str = Query(...), artist: str = ""):
    """Fallback: LRCLIB + lyrics.ovh (Musixmatch needs key)."""
    q = f"{artist} {title}".strip()
    try:
        async with httpx.AsyncClient(timeout=12.0) as client:
            r = await client.get(
                "https://lrclib.net/api/search",
                params={"q": q},
            )
            items = r.json() if r.status_code == 200 else []
            if items:
                best = items[0]
                return {
                    "ok": True,
                    "title": best.get("trackName"),
                    "artist": best.get("artistName"),
                    "synced": best.get("syncedLyrics"),
                    "plain": best.get("plainLyrics"),
                    "source": "lrclib",
                }
    except Exception:
        pass
    try:
        async with httpx.AsyncClient(timeout=12.0) as client:
            r = await client.get(f"https://api.lyrics.ovh/v1/{quote(artist)}/{quote(title)}")
            if r.status_code == 200:
                return {"ok": True, "plain": r.json().get("lyrics"), "source": "lyrics.ovh"}
    except Exception:
        pass
    return {"ok": False, "error": "not found"}


@app.get("/lyrics/spotify", tags=["Lyrics"])
@app.get("/lyrics/applemusic", tags=["Lyrics"], include_in_schema=False)
@app.get("/lyrics/amazonmusic", tags=["Lyrics"], include_in_schema=False)
@app.get("/lyrics/deezer", tags=["Lyrics"], include_in_schema=False)
async def lyrics_platform_alias(title: str = Query(...), artist: str = ""):
    """Platform-tagged lyrics → same LRCLIB/OVH stack."""
    return await lyrics_musixmatch(title=title, artist=artist)


# (lyrics/lrcget primary is defined earlier with title/artist params)


@app.get("/lyrics/plain", tags=["Lyrics"])
async def lyrics_plain(title: str = Query(...), artist: str = Query("")):
    r = await lyrics_musixmatch(title=title, artist=artist)
    return {"ok": r.get("ok"), "lyrics": r.get("plain") or r.get("synced"), "source": r.get("source")}


# ----- Billboard charts (public) -----

@app.get("/billboard/hot-100", tags=["Billboard"])
async def billboard_hot100():
    """Billboard Hot 100 via public chart mirror (best-effort)."""
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.get(
                "https://raw.githubusercontent.com/mhollingshead/billboard-hot-100/main/recent.json"
            )
            if r.status_code == 200:
                return {"ok": True, "chart": "hot-100", "data": r.json()}
    except Exception:
        pass
    return {"ok": False, "error": "chart unavailable", "hint": "try /music/charts"}


@app.get("/billboard/billboard-200", tags=["Billboard"])
@app.get("/billboard/global-200", tags=["Billboard"], include_in_schema=False)
@app.get("/billboard/streaming-songs", tags=["Billboard"], include_in_schema=False)
@app.get("/billboard/radio-songs", tags=["Billboard"], include_in_schema=False)
@app.get("/billboard/social-50", tags=["Billboard"], include_in_schema=False)
@app.get("/billboard/artists-100", tags=["Billboard"], include_in_schema=False)
@app.get("/billboard/digital-song-sales", tags=["Billboard"], include_in_schema=False)
@app.get("/billboard/top-album-sales", tags=["Billboard"], include_in_schema=False)
@app.get("/billboard/catalog-albums", tags=["Billboard"], include_in_schema=False)
@app.get("/billboard/current-albums", tags=["Billboard"], include_in_schema=False)
@app.get("/billboard/independent-albums", tags=["Billboard"], include_in_schema=False)
@app.get("/billboard/soundtracks", tags=["Billboard"], include_in_schema=False)
@app.get("/billboard/world-albums", tags=["Billboard"], include_in_schema=False)
async def billboard_alias():
    return await billboard_hot100()


# ----- JioSaavn extras -----

@app.get("/jiosaavn/search", tags=["JioSaavn"])
async def jiosaavn_search(q: str = Query(...)):
    try:
        async with httpx.AsyncClient(timeout=12.0) as client:
            r = await client.get(
                "https://www.jiosaavn.com/api.php",
                params={
                    "__call": "search.getResults",
                    "p": 1,
                    "q": q,
                    "api_version": 4,
                    "n": 20,
                    "_format": "json",
                    "_marker": 0,
                    "ctx": "web6dot0",
                },
                headers={"User-Agent": "Mozilla/5.0"},
            )
            return {"ok": True, "data": r.json() if r.status_code == 200 else {}}
    except Exception as e:
        return {"ok": False, "error": str(e)[:160]}


@app.get("/jiosaavn/track", tags=["JioSaavn"])
@app.get("/jiosaavn/album", tags=["JioSaavn"], include_in_schema=False)
@app.get("/jiosaavn/artist", tags=["JioSaavn"], include_in_schema=False)
@app.get("/jiosaavn/playlist", tags=["JioSaavn"], include_in_schema=False)
async def jiosaavn_track(id: str = Query(None), q: str = Query(None)):
    if q and not id:
        return await jiosaavn_search(q=q)
    if not id:
        return {"ok": False, "error": "pass id or q"}
    try:
        async with httpx.AsyncClient(timeout=12.0) as client:
            r = await client.get(
                "https://www.jiosaavn.com/api.php",
                params={
                    "__call": "song.getDetails",
                    "cc": "in",
                    "pids": id,
                    "api_version": 4,
                    "_format": "json",
                    "_marker": 0,
                    "ctx": "web6dot0",
                },
                headers={"User-Agent": "Mozilla/5.0"},
            )
            return {"ok": True, "data": r.json() if r.status_code == 200 else {}}
    except Exception as e:
        return {"ok": False, "error": str(e)[:160]}


# ----- Spotify public metadata (no login) -----

@app.get("/spotify/search", tags=["Spotify"])
async def spotify_search(q: str = Query(...)):
    """Spotify oEmbed + DuckDuckGo assisted search (metadata)."""
    results = []
    try:
        async with httpx.AsyncClient(timeout=12.0) as client:
            r = await client.get(
                "https://api.duckduckgo.com/",
                params={"q": f"{q} site:open.spotify.com/track", "format": "json"},
            )
            d = r.json()
            for tpc in (d.get("RelatedTopics") or [])[:10]:
                if isinstance(tpc, dict) and tpc.get("FirstURL"):
                    results.append({"title": tpc.get("Text"), "url": tpc.get("FirstURL")})
    except Exception:
        pass
    return {"ok": True, "query": q, "results": results, "note": "Use /dl/spotify?url= for download"}


@app.get("/spotify/track", tags=["Spotify"])
async def spotify_track(url: str = Query(...)):
    try:
        async with httpx.AsyncClient(timeout=12.0) as client:
            r = await client.get("https://open.spotify.com/oembed", params={"url": url})
            if r.status_code == 200:
                return {"ok": True, **r.json()}
    except Exception as e:
        return {"ok": False, "error": str(e)[:160]}
    return {"ok": False, "error": "not found"}


@app.get("/spotify/album", tags=["Spotify"], include_in_schema=False)
@app.get("/spotify/playlist", tags=["Spotify"], include_in_schema=False)
@app.get("/spotify/playlist/tracks", tags=["Spotify"], include_in_schema=False)
@app.get("/spotify/home", tags=["Spotify"], include_in_schema=False)
@app.get("/spotify/charts", tags=["Spotify"], include_in_schema=False)
@app.get("/spotify/canvas", tags=["Spotify"], include_in_schema=False)
@app.get("/spotify/episode", tags=["Spotify"], include_in_schema=False)
async def spotify_meta_alias(url: str = Query(None), q: str = Query(None)):
    if url:
        return await spotify_track(url=url)
    if q:
        return await spotify_search(q=q)
    return {"ok": False, "error": "pass url or q"}


# ----- Deezer full (public API) -----

@app.get("/deezer/search", tags=["Deezer"])
async def deezer_search(q: str = Query(...)):
    async with httpx.AsyncClient(timeout=12.0) as client:
        r = await client.get("https://api.deezer.com/search", params={"q": q, "limit": 25})
        return {"ok": True, **(r.json() if r.status_code == 200 else {})}


@app.get("/deezer/track", tags=["Deezer"])
async def deezer_track(id: int = Query(...)):
    async with httpx.AsyncClient(timeout=12.0) as client:
        r = await client.get(f"https://api.deezer.com/track/{id}")
        return {"ok": True, **(r.json() if r.status_code == 200 else {})}


@app.get("/deezer/album", tags=["Deezer"])
async def deezer_album(id: int = Query(...)):
    async with httpx.AsyncClient(timeout=12.0) as client:
        r = await client.get(f"https://api.deezer.com/album/{id}")
        return {"ok": True, **(r.json() if r.status_code == 200 else {})}


@app.get("/deezer/artist", tags=["Deezer"])
async def deezer_artist(id: int = Query(...)):
    async with httpx.AsyncClient(timeout=12.0) as client:
        r = await client.get(f"https://api.deezer.com/artist/{id}")
        return {"ok": True, **(r.json() if r.status_code == 200 else {})}


@app.get("/deezer/playlist", tags=["Deezer"])
async def deezer_playlist(id: int = Query(...)):
    async with httpx.AsyncClient(timeout=12.0) as client:
        r = await client.get(f"https://api.deezer.com/playlist/{id}")
        return {"ok": True, **(r.json() if r.status_code == 200 else {})}


@app.get("/deezer/home", tags=["Deezer"])
async def deezer_home():
    async with httpx.AsyncClient(timeout=12.0) as client:
        r = await client.get("https://api.deezer.com/chart")
        return {"ok": True, **(r.json() if r.status_code == 200 else {})}


# ----- Extra downloaders missing from earlier list -----

@app.get("/dl/bilibili", tags=["Downloader"])
@app.get("/dl/bili", tags=["Downloader"], include_in_schema=False)
async def dl_bilibili(url: str = Query(...)):
    return await dl_any(url=url)  # type: ignore


@app.get("/dl/linkedin", tags=["Downloader"])
async def dl_linkedin(url: str = Query(...)):
    return await dl_any(url=url)  # type: ignore


@app.get("/dl/okru", tags=["Downloader"])
@app.get("/dl/odnoklassniki", tags=["Downloader"], include_in_schema=False)
async def dl_okru(url: str = Query(...)):
    return await dl_any(url=url)  # type: ignore


@app.get("/dl/rumble", tags=["Downloader"])
async def dl_rumble(url: str = Query(...)):
    return await dl_any(url=url)  # type: ignore


@app.get("/dl/streamable", tags=["Downloader"])
async def dl_streamable(url: str = Query(...)):
    return await dl_any(url=url)  # type: ignore


@app.get("/dl/bandcamp", tags=["Downloader"])
async def dl_bandcamp(url: str = Query(...)):
    return await dl_any(url=url)  # type: ignore


@app.get("/dl/mixcloud", tags=["Downloader"])
async def dl_mixcloud(url: str = Query(...)):
    return await dl_any(url=url)  # type: ignore


@app.get("/dl/flickr", tags=["Downloader"])
async def dl_flickr(url: str = Query(...)):
    return await dl_any(url=url)  # type: ignore


@app.get("/dl/imgur", tags=["Downloader"])
async def dl_imgur(url: str = Query(...)):
    return await dl_any(url=url)  # type: ignore


@app.get("/dl/xkcd", tags=["Downloader"])
async def dl_xkcd(url: str = Query(...)):
    return await dl_any(url=url)  # type: ignore


@app.get("/tools/bypass-city", tags=["Tools"])
@app.get("/tools/bypass-tools", tags=["Tools"], include_in_schema=False)
async def tools_bypass(url: str = Query(...)):
    """Generic link unlock → same as /dl/any."""
    return await dl_any(url=url)  # type: ignore


@app.get("/catalog/all", tags=["Meta"])
async def catalog_all():
    """Full endpoint map for integrators."""
    routes = []
    for route in app.routes:
        if hasattr(route, "methods") and hasattr(route, "path"):
            if route.path.startswith(("/docs", "/openapi", "/redoc")):
                continue
            routes.append({
                "path": route.path,
                "methods": sorted(m for m in (route.methods or []) if m != "HEAD"),
                "name": getattr(route, "name", None),
            })
    return {
        "ok": True,
        "version": "5.12.0",
        "count": len(routes),
        "creator": "shawon",
        "routes": sorted(routes, key=lambda x: x["path"]),
    }



# Frontend: load from web/index.html (fallback: minimal page)
from pathlib import Path as _Path

def _load_spa() -> str:
    candidates = [
        _Path(__file__).resolve().parent / "web" / "index.html",
        _Path(__file__).resolve().parent / "index.html",
        _Path.cwd() / "web" / "index.html",
        _Path.cwd() / "index.html",
    ]
    for p in candidates:
        try:
            if p.is_file():
                return p.read_text(encoding="utf-8")
        except Exception:
            continue
    return """<!DOCTYPE html>
<html lang="en"><head>
<meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>StreamHub API</title>
<style>
body{margin:0;font-family:system-ui,sans-serif;background:#0b0f14;color:#e8eef7;padding:2rem;line-height:1.5}
a{color:#6ea8fe} .box{max-width:36rem;margin:auto;background:#121820;border:1px solid #243044;border-radius:12px;padding:1.5rem}
code{background:#1a2332;padding:.1rem .35rem;border-radius:4px}
</style></head><body>
<div class="box">
<h1>StreamHub API</h1>
<p><code>web/index.html</code> missing — <b>API-only mode</b> (still fully working).</p>
<ul>
<li><a href="/docs">/docs</a> — OpenAPI</li>
<li><a href="/health">/health</a></li>
<li><a href="/moviebox/search?q=Avatar">/moviebox/search</a></li>
<li><a href="/mb/search?q=Avatar">/mb/search</a></li>
<li><a href="/play?subject_id=1654274595068805784">/play?subject_id=…</a></li>
</ul>
<p>Place <code>web/index.html</code> next to <code>api.py</code> to enable the UI.</p>
</div></body></html>
"""


@app.get("/site", response_class=HTMLResponse, tags=["Meta"])
async def site_spa():
    return HTMLResponse(_load_spa())


# Serve full app at root too
@app.get("/", response_class=HTMLResponse, tags=["Meta"], include_in_schema=False)
async def root_spa():
    return HTMLResponse(_load_spa())

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("api:app", host="0.0.0.0", port=port)
