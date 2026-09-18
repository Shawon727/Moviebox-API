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
from urllib.parse import urlparse, parse_qsl, urlencode, urljoin, unquote
from typing import Optional, Any, List, Dict, Tuple

import httpx
from bs4 import BeautifulSoup
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
    version="5.4.0",
    docs_url=None,
    redoc_url=None,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

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
    "https://api.inmoviebox.com",
]
MB_SECRET = "76iRl07s0xSN9jqmEWAt79EBJZulIQIsV64FZr2O"
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
    u = (url or "").lower()
    markers = [
        "1c7de0bd3393702d9191801f15f88f8d",
        "9a0461bc39da389663bf3dbb17091d3f",
        "/notice.mp4",
        "b164fbfb43477929",
        "aa348f2541d13ffe",
    ]
    if any(m in u for m in markers):
        return True
    return "macdn.aoneroom.com" in u and "/other/" in u


def _dash_from_sign_cookie(cookie: str) -> Optional[str]:
    if not cookie:
        return None
    for part in cookie.split(";"):
        trimmed = part.strip()
        if not trimmed.startswith("CloudFront-Policy="):
            continue
        raw = trimmed[len("CloudFront-Policy=") :].strip()
        normalized = raw.replace("-", "+").replace("_", "=").replace("~", "/")
        try:
            pad = (4 - len(normalized) % 4) % 4
            policy = json.loads(base64.b64decode(normalized + ("=" * pad)))
            resource = policy["Statement"][0]["Resource"]
            base = resource.rstrip("*").rstrip("/")
            if base.startswith("http"):
                return f"{base}/index.mpd"
        except Exception:
            continue
    if "urlprefix=" in cookie:
        try:
            part = cookie.split("urlprefix=")[1].split("&")[0].split(";")[0]
            pad = (4 - len(part) % 4) % 4
            base = base64.b64decode(part + ("=" * pad)).decode("utf-8", errors="ignore")
            if base.startswith("http"):
                return base if base.endswith((".mpd", ".m3u8")) else base.rstrip("/") + "/index.mpd"
        except Exception:
            pass
    return None


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
        if not playable or playable in seen:
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


async def resolve_hubcloud(drive_url: str) -> List[dict]:
    """
    hubcloud.*/drive/xxx  →  direct download / stream URLs
    """
    if "hubcloud." not in drive_url or "/drive/" not in drive_url:
        raise HTTPException(400, "Not a HubCloud /drive/ URL")

    headers = {"User-Agent": FK_UA, "Referer": drive_url}
    async with httpx.AsyncClient(follow_redirects=True, timeout=25.0, headers=headers) as client:
        r1 = await client.get(drive_url)
        if r1.status_code != 200:
            raise HTTPException(502, f"HubCloud page HTTP {r1.status_code}")
        soup1 = BeautifulSoup(r1.text, "html.parser")

        resolver_url = None
        for a in soup1.select(
            "a#download, a.btn-primary, a.btn-success, a.btn[href*='/download/'], "
            "a[href*='/download/'], a[href*='gamerxyt.com'], a[href*='hubcloud.php']"
        ):
            href = a.get("href") or ""
            if href.startswith("https://"):
                resolver_url = href
                break
        if not resolver_url:
            m = re.search(r"var\s+url\s*=\s*['\"](https://[^'\"]+)['\"]", r1.text)
            if m:
                resolver_url = m.group(1)
        if not resolver_url:
            raise HTTPException(502, "HubCloud: download button not found")

        r2 = await client.get(resolver_url, headers={**headers, "Referer": drive_url})
        if r2.status_code != 200:
            raise HTTPException(502, f"HubCloud resolver HTTP {r2.status_code}")
        html2 = r2.text
        soup2 = BeautifulSoup(html2, "html.parser")

        candidates: List[Tuple[int, str, str]] = []

        for prefix in (
            "https://pixeldrain.dev/u/",
            "https://pixeldrain.com/u/",
            "https://pixeldrain.dev/api/file/",
            "https://pixeldrain.com/api/file/",
        ):
            pos = 0
            while True:
                i = html2.find(prefix, pos)
                if i < 0:
                    break
                end = i
                while end < len(html2) and html2[end] not in "\"' \t\n\r<>\\":
                    end += 1
                chunk = html2[i:end]
                api = _pixeldrain_api(chunk)
                if api:
                    candidates.append((_score_mirror(api, "PixelDrain"), api, "PixelDrain"))
                pos = end

        for a in soup2.select("a[href]"):
            href = (a.get("href") or "").strip()
            if not href.startswith("https://"):
                continue
            label = a.get_text(" ", strip=True) or "Direct"
            unwrapped = _unwrap_pages_dev(href)
            if unwrapped and _is_playable_direct(unwrapped):
                candidates.append((_score_mirror(unwrapped, "Watch Online"), unwrapped, "Watch Online"))
                continue
            pd = _pixeldrain_api(href)
            if pd:
                candidates.append((_score_mirror(pd, label), pd, "PixelDrain"))
                continue
            if _is_playable_direct(href):
                candidates.append((_score_mirror(href, label), href, label[:80]))

        for a in soup2.select("a#fsl, a[id*=fsl]"):
            href = a.get("href") or ""
            if href.startswith("https://") and _is_playable_direct(href):
                candidates.append((0, href, "FSL Server"))

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
                }
            )
        if not results:
            raise HTTPException(502, "HubCloud: no direct links extracted")
        return results


async def resolve_hubdrive(file_url: str) -> List[dict]:
    if "hubdrive." not in file_url:
        raise HTTPException(400, "Not a HubDrive URL")
    headers = {"User-Agent": FK_UA, "Referer": file_url}
    async with httpx.AsyncClient(follow_redirects=True, timeout=20.0, headers=headers) as client:
        r = await client.get(file_url)
        if r.status_code != 200:
            raise HTTPException(502, f"HubDrive HTTP {r.status_code}")
        soup = BeautifulSoup(r.text, "html.parser")
        for a in soup.select("a[href]"):
            href = a.get("href") or ""
            if "hubcloud." in href and "/drive/" in href:
                return await resolve_hubcloud(href)
    raise HTTPException(502, "HubDrive: no HubCloud mirror found")




def _rot13(s: str) -> str:
    out = []
    for ch in s:
        if "a" <= ch <= "z":
            out.append(chr((ord(ch) - 97 + 13) % 26 + 97))
        elif "A" <= ch <= "Z":
            out.append(chr((ord(ch) - 65 + 13) % 26 + 65))
        else:
            out.append(ch)
    return "".join(out)


def _b64_str(s: str) -> str:
    pad = (-len(s)) % 4
    return base64.b64decode(s + ("=" * pad)).decode("utf-8", errors="strict")


def _decode_greenmotors_payload(payload: str) -> Optional[str]:
    """greenmotors localStorage `o`: atob → atob → rot13 → atob → JSON → o → atob → hubcloud URL."""
    try:
        s = payload.strip()
        s = _b64_str(s)
        s = _b64_str(s)
        s = _rot13(s)
        s = _b64_str(s)
        data = json.loads(s)
        inner = data.get("o") or data.get("url") or ""
        if not inner:
            return None
        url = _b64_str(inner) if not inner.startswith("http") else inner
        if url.startswith("https://") and ("hubcloud." in url or "hubdrive." in url):
            return url
    except Exception:
        return None
    return None


async def _resolve_masked_hub(url: str) -> List[dict]:
    """Follow greenmotors / similar gates → HubCloud /drive/ → direct mirrors."""
    headers = {
        "User-Agent": FK_UA,
        "Referer": "https://4khdhub.one/",
        "Accept": "text/html,application/xhtml+xml",
    }
    out: List[dict] = []
    async with httpx.AsyncClient(follow_redirects=True, timeout=25.0, headers=headers) as client:
        r = await client.get(url)
        text = r.text or ""
        final = str(r.url)
        candidates: List[str] = []

        # 1) Decode greenmotors s('o','...') payload
        for m in re.finditer(r"s\(\s*['\"]o['\"]\s*,\s*['\"]([^'\"]+)['\"]", text):
            decoded = _decode_greenmotors_payload(m.group(1))
            if decoded:
                candidates.append(decoded)

        # 2) Any hubcloud/hubdrive already in HTML
        for u in re.findall(r"https?://[^\s\"'<>]+", text + " " + final):
            u = u.rstrip(").,;'\"")
            if "hubcloud." in u and "/drive/" in u:
                candidates.append(u.split("&")[0])
            if "hubdrive." in u and "/file/" in u:
                candidates.append(u.split("&")[0])

        # 3) If still on greenmotors, fetch mediator with cookie and scan
        if "greenmotors." in final or "greenmotors." in url:
            try:
                client.cookies.set("xla", "s4t")
                r2 = await client.get(
                    "https://greenmotors.cc/homelander/",
                    headers={**headers, "Referer": final},
                )
                t2 = r2.text or ""
                for m in re.finditer(r"https://hubcloud\.[a-z.]+/drive/[a-zA-Z0-9]+", t2):
                    candidates.append(m.group(0))
                for m in re.finditer(r"s\(\s*['\"]o['\"]\s*,\s*['\"]([^'\"]+)['\"]", t2):
                    decoded = _decode_greenmotors_payload(m.group(1))
                    if decoded:
                        candidates.append(decoded)
            except Exception:
                pass

        seen = set()
        for c in candidates:
            if c in seen:
                continue
            seen.add(c)
            try:
                out.extend(await resolve_any(c))
            except Exception:
                continue
            if len(out) >= 8:
                break
    return out


async def resolve_any(url: str) -> List[dict]:
    u = url.strip()
    if "hubcloud." in u and "/drive/" in u:
        return await resolve_hubcloud(u)
    if "hubdrive." in u:
        return await resolve_hubdrive(u)
    raise HTTPException(400, "Supported: hubcloud.*/drive/... or hubdrive.*/file/...")


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
    return {"ok": True, "version": "5.3.0", "providers": ["tmdb", "vidsrc", "vidlink", "vidfast", "autoembed", "4khdhub", "hubcloud", "ytmusic", "moviebox-api"]}


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
    """Stream embeds inspired by streambert / Flick-style players (TMDB id).

    Note: Videasy and its mirror Vidking shut down their entire embed
    infrastructure on 2026-09-15 (see videasy.to), so they're intentionally
    left out here in favour of currently-live providers (VidSrc, VidLink,
    VidFast, AutoEmbed) that use the same "TMDB id -> iframe" shape.
    """
    sources = []
    tmdb = meta.get("tmdb")
    imdb = meta.get("imdb")
    se = max(1, int(se or 1))
    ep = max(1, int(ep or 1))
    pairs = []
    if media == "movie":
        if tmdb:
            pairs = [
                ("VidSrc", f"https://vidsrc.xyz/embed/movie/{tmdb}"),
                ("VidSrc 2", f"https://vsembed.su/embed/movie/{tmdb}"),
                ("VidLink", f"https://vidlink.pro/movie/{tmdb}"),
                ("VidFast", f"https://vidfast.pro/movie/{tmdb}"),
                ("AutoEmbed", f"https://player.autoembed.cc/embed/movie/{tmdb}"),
                ("EmbedSU", f"https://embed.su/embed/movie/{tmdb}"),
                ("2Embed", f"https://www.2embed.cc/embed/{tmdb}"),
            ]
        elif imdb:
            pairs = [
                ("VidSrc IMDB", f"https://vsembed.su/embed/movie/{imdb}"),
                ("AutoEmbed IMDB", f"https://player.autoembed.cc/embed/movie/{imdb}"),
            ]
    else:
        if tmdb:
            pairs = [
                ("VidSrc", f"https://vidsrc.xyz/embed/tv/{tmdb}/{se}/{ep}"),
                ("VidSrc 2", f"https://vsembed.su/embed/tv/{tmdb}/{se}/{ep}"),
                ("VidLink", f"https://vidlink.pro/tv/{tmdb}/{se}/{ep}"),
                ("VidFast", f"https://vidfast.pro/tv/{tmdb}/{se}/{ep}"),
                ("AutoEmbed", f"https://player.autoembed.cc/embed/tv/{tmdb}/{se}/{ep}"),
                ("EmbedSU", f"https://embed.su/embed/tv/{tmdb}/{se}/{ep}"),
            ]
        elif imdb:
            pairs = [
                ("VidSrc IMDB", f"https://vsembed.su/embed/tv/{imdb}/{se}/{ep}"),
                ("AutoEmbed IMDB", f"https://player.autoembed.cc/embed/tv/{imdb}/{se}/{ep}"),
            ]
    for label, url in pairs:
        sources.append({
            "provider": "embed",
            "label": label,
            "url": url,
            "play_url": url,
            "format": "EMBED",
            "type": "embed",
        })
    return sources



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
    Playable stream URLs (DASH / MP4 / HLS).
    - Movie: se=0 & ep=0
    - Series: se=1&ep=1 …
    Each source includes `headers` (Cookie, User-Agent) required by the CDN.
    """
    if se == 0 and ep == 0:
        path = f"/wefeed-mobile-bff/subject-api/play-info/v2?subjectId={subject_id}"
    else:
        path = f"/wefeed-mobile-bff/subject-api/play-info/v2?subjectId={subject_id}&se={se}&ep={ep}"
    try:
        data = await mb_request("GET", path)
    except HTTPException:
        data = await mb_request("GET", path.replace("/play-info/v2", "/play-info"))
    if not isinstance(data, dict):
        data = {}
    sources = _parse_mb_play_info(data, _mb_ua)
    extra = await _mb_resource_links(subject_id, se, ep)
    seen = {s["url"] for s in sources}
    for item in extra:
        if item["url"] not in seen:
            sources.append(item)
            seen.add(item["url"])
    return {
        "provider": "moviebox",
        "subject_id": subject_id,
        "se": se,
        "ep": ep,
        "count": len(sources),
        "has_resource": len(sources) > 0,
        "sources": sources,
        "note": None if sources else "No playable sources",
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
    resolve: bool = Query(False, description="If true, expand HubCloud mirrors to direct URLs"),
):
    """
    Release list with mirrors.
    Set resolve=true to call HubCloud resolver on each hubcloud/hubdrive link
    and attach `direct_links` under that mirror.
    """
    html = await fk_fetch(id)
    releases = fk_parse_releases(html, se, ep)

    if resolve:
        for rel in releases:
            for mirror in rel.get("mirrors") or []:
                if not mirror.get("needs_resolve"):
                    continue
                try:
                    mirror["direct_links"] = await resolve_any(mirror["url"])
                except Exception as e:
                    mirror["direct_links"] = []
                    mirror["resolve_error"] = str(e)

    return {
        "provider": "4khdhub",
        "id": id,
        "se": se,
        "ep": ep,
        "resolved": resolve,
        "count": len(releases),
        "releases": releases,
        "hint": "Use /tools/resolve?url= for a single HubCloud link, or resolve=true here.",
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
    """TMDB homepage — more rows for richer home."""
    async def grab(path, media, pages=1):
        items = []
        for pg in range(1, pages + 1):
            d = await _tmdb_get(path, {"page": pg})
            for x in d.get("results") or []:
                c = _tmdb_card(x, media)
                if c:
                    items.append(c)
        return items

    trending_m = await grab("/trending/movie/week", "movie", 2)
    trending_t = await grab("/trending/tv/week", "tv", 2)
    popular_m = await grab("/movie/popular", "movie", 2)
    popular_t = await grab("/tv/popular", "tv", 2)
    top_m = await grab("/movie/top_rated", "movie", 1)
    top_t = await grab("/tv/top_rated", "tv", 1)
    now_m = await grab("/movie/now_playing", "movie", 1)
    airing = await grab("/tv/on_the_air", "tv", 1)
    upcoming = await grab("/movie/upcoming", "movie", 1)
    return {
        "trending_movies": trending_m[:24],
        "trending_series": trending_t[:24],
        "popular_movies": popular_m[:24],
        "popular_series": popular_t[:24],
        "top_movies": top_m[:18],
        "top_series": top_t[:18],
        "now_playing": now_m[:18],
        "on_the_air": airing[:18],
        "upcoming": upcoming[:18],
        "provider": "tmdb",
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
    """TMDB multi-search for web + optional 4KHDHub hits."""
    items = []
    fk_items = []
    errors = {}
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
    # Saavn first (playable), then YT
    items = saavn + ytm[:20]
    return {
        "query": q,
        "count": len(items),
        "items": items,
        "jiosaavn": saavn,
        "ytmusic": ytm[:20],
        "provider": "jiosaavn+ytmusic",
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

def _ytdlp_info(url: str) -> dict:
    """Extract media metadata + formats without downloading (OmniGet-style probe)."""
    try:
        import yt_dlp  # type: ignore
    except ImportError:
        return {"ok": False, "error": "yt-dlp not installed — pip install yt-dlp"}
    opts = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "noplaylist": True,
        "extract_flat": False,
        "socket_timeout": 25,
        "extractor_args": {"youtube": {"player_client": ["android", "web"]}},
    }
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=False)
    except Exception as e:
        return {"ok": False, "error": str(e)[:400]}
    if not info:
        return {"ok": False, "error": "no info"}
    formats = []
    seen = set()
    for f in info.get("formats") or []:
        if not isinstance(f, dict):
            continue
        fu = f.get("url")
        if not fu or fu in seen:
            continue
        # skip storyboard / pure images
        if f.get("vcodec") == "none" and f.get("acodec") == "none":
            continue
        seen.add(fu)
        height = f.get("height") or 0
        abr = f.get("abr") or f.get("tbr") or 0
        vcodec = f.get("vcodec") or "none"
        acodec = f.get("acodec") or "none"
        ext = f.get("ext") or "mp4"
        kind = "video" if vcodec != "none" else "audio"
        label_parts = []
        if height:
            label_parts.append(f"{height}p")
        if kind == "audio":
            label_parts.append(f"{int(abr)}kbps" if abr else "audio")
        label_parts.append(ext.upper())
        if vcodec != "none" and acodec == "none":
            label_parts.append("video-only")
        if vcodec == "none" and acodec != "none":
            label_parts.append("audio-only")
        formats.append({
            "id": f.get("format_id"),
            "label": " · ".join(label_parts) or f.get("format") or "stream",
            "ext": ext,
            "height": height,
            "abr": abr,
            "vcodec": vcodec,
            "acodec": acodec,
            "filesize": f.get("filesize") or f.get("filesize_approx"),
            "url": fu,
            "kind": kind,
        })
    # Prefer progressive (video+audio muxed) — these play with sound when copied
    def score(x):
        prog = 2 if (x["vcodec"] != "none" and x["acodec"] != "none") else 0
        audio_only = 1 if (x["vcodec"] == "none" and x["acodec"] != "none") else 0
        return (prog, x.get("height") or 0, audio_only, x.get("abr") or 0)
    formats.sort(key=score, reverse=True)
    # Tag progressive clearly
    for x in formats:
        if x["vcodec"] != "none" and x["acodec"] != "none":
            if "VIDEO+AUDIO" not in x["label"]:
                x["label"] = "🔊 " + x["label"] + " · VIDEO+AUDIO"
            x["muxed"] = True
        else:
            x["muxed"] = False
    formats = formats[:40]
    best_muxed = next((f for f in formats if f.get("muxed")), None)
    best_audio = next((f for f in formats if f.get("kind") == "audio" or (f.get("acodec") and f["acodec"] != "none" and f.get("vcodec") == "none")), None)
    thumb = None
    ths = info.get("thumbnails") or []
    if ths:
        thumb = ths[-1].get("url")
    thumb = thumb or info.get("thumbnail")
    return {
        "ok": True,
        "id": info.get("id"),
        "title": info.get("title") or info.get("id"),
        "uploader": info.get("uploader") or info.get("channel"),
        "duration": info.get("duration"),
        "webpage_url": info.get("webpage_url") or url,
        "thumbnail": thumb,
        "description": (info.get("description") or "")[:400],
        "extractor": info.get("extractor") or info.get("ie_key"),
        "formats": formats,
        "count": len(formats),
        "best_muxed": best_muxed,
        "best_audio": best_audio,
        "note": "Prefer 🔊 VIDEO+AUDIO rows — single file with sound. Video-only needs separate audio.",
        "provider": "yt-dlp",
    }



@app.get("/dl/info", tags=["Downloader"])
async def dl_info(url: str = Query(..., min_length=8)):
    """Probe URL with yt-dlp — list formats (muxed preferred)."""
    url = url.strip()
    if not re.match(r"^https?://", url, re.I):
        raise HTTPException(400, "url must start with http(s)://")
    data = await asyncio.to_thread(_ytdlp_info, url)
    if not data.get("ok"):
        return JSONResponse(data, status_code=502)
    return data


@app.get("/dl/audio", tags=["Downloader"])
async def dl_audio(url: str = Query(..., min_length=8)):
    """Best audio-only stream for a page URL."""
    data = await asyncio.to_thread(_ytdlp_info, url)
    if not data.get("ok"):
        return JSONResponse(data, status_code=502)
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



def _cdn_headers(url: str) -> dict:
    """Headers so Instagram/FB/TikTok CDNs accept the request."""
    h = {
        "User-Agent": (
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
            "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1"
        ),
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.9",
    }
    u = (url or "").lower()
    if "instagram" in u or "cdninstagram" in u or "fbcdn" in u:
        h["Referer"] = "https://www.instagram.com/"
        h["Origin"] = "https://www.instagram.com"
    elif "tiktok" in u or "musical.ly" in u:
        h["Referer"] = "https://www.tiktok.com/"
    elif "youtube" in u or "googlevideo" in u:
        h["Referer"] = "https://www.youtube.com/"
    elif "twitter" in u or "twimg" in u or "x.com" in u:
        h["Referer"] = "https://x.com/"
    return h


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
    """Resolve HindiAnime HLS / proxy stream from videoHash."""
    if not hash and not url:
        raise HTTPException(400, "hash or url required")
    params = {}
    if hash:
        params["hash"] = hash
    else:
        params["url"] = url
    data = await _ha_get("/api/resolve-stream", params)
    if not data.get("success") and not data.get("streamUrl") and not data.get("proxyUrl"):
        raise HTTPException(502, f"resolve failed: {data}")
    stream = data.get("streamUrl") or data.get("proxyUrl") or ""
    if stream.startswith("/"):
        stream = HA_BASE + stream
    mirrors = []
    for s in data.get("servers") or data.get("mirrors") or []:
        if isinstance(s, dict) and s.get("url"):
            u = s["url"]
            if u.startswith("/"):
                u = HA_BASE + u
            mirrors.append({"label": s.get("name") or "Server", "url": u})
    return {
        "success": True,
        "stream_url": stream,
        "server": data.get("server"),
        "hash": hash or data.get("videoHash"),
        "mirrors": mirrors,
        "raw": {k: data[k] for k in data if k not in ("raw",)} if isinstance(data, dict) else {},
        "provider": "hindianime",
    }



@app.get("/docs", include_in_schema=False)
async def custom_swagger_ui():
    return HTMLResponse("""<!DOCTYPE html>
<html lang="en"><head>
<meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>StreamHub API Docs</title>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui.css"/>
<style>
  body{margin:0;background:#0b0d12}
  .swagger-ui .topbar{display:none}
  .swagger-ui{background:#0b0d12}
  .swagger-ui .info .title{color:#e8eaf0;font-family:system-ui}
  .swagger-ui .info p,.swagger-ui .info li,.swagger-ui .info table{color:#a0a6b8}
  .swagger-ui .scheme-container{background:#12151c;box-shadow:none;border-bottom:1px solid #222}
  .swagger-ui .opblock-tag{color:#e8eaf0;border-bottom:1px solid #222}
  .swagger-ui .opblock{background:#12151c;border-radius:12px;border:1px solid #2a2f3a;box-shadow:none;margin:0 0 12px}
  .swagger-ui .opblock .opblock-summary-description{color:#8b92a8}
  .swagger-ui .opblock.opblock-get{border-color:#1a6b8a;background:rgba(0,120,180,.08)}
  .swagger-ui .opblock.opblock-get .opblock-summary-method{background:#0a8ec0}
  .swagger-ui .opblock.opblock-post{border-color:#2a7a4a;background:rgba(0,140,80,.08)}
  .swagger-ui .btn.execute{background:linear-gradient(135deg,#00a4dc,#7c5cff);border:none;border-radius:8px}
  .swagger-ui input[type=text],.swagger-ui select,.swagger-ui textarea{background:#0b0d12;color:#e8eaf0;border:1px solid #333;border-radius:8px}
  .swagger-ui .parameter__name,.swagger-ui table thead td,.swagger-ui .response-col_status{color:#c8cdd8}
  .swagger-ui .model-box,.swagger-ui section.models{background:#12151c;border-color:#2a2f3a}
  .swagger-ui .model{color:#a0a6b8}
  .swagger-ui .response-col_description{color:#a0a6b8}
  .swagger-ui .highlight-code{background:#0b0d12}
  .swagger-ui .microlight{color:#d0d4e0}
  .sh-banner{padding:22px 28px;background:linear-gradient(135deg,#07080c,#0f172a 40%,#1e1b4b);border-bottom:1px solid rgba(255,255,255,.08);color:#f1f5f9;font-family:system-ui,sans-serif}
  .sh-banner h1{margin:0 0 8px;font-size:1.45rem;font-weight:800;background:linear-gradient(90deg,#5eead4,#a78bfa);-webkit-background-clip:text;background-clip:text;color:transparent}
  .sh-banner p{margin:0;color:#8b92a8;font-size:.9rem}
  .sh-banner a{color:#00d4ff;margin-right:14px}

/* Motion + glass 2026 */
:root{--a:#5eead4;--a2:#a78bfa;--bg:#07080c;--card:rgba(22,24,32,.72);--line:rgba(255,255,255,.08);--text:#f1f5f9;--mute:#94a3b8}
body{background:var(--bg);background-image:radial-gradient(ellipse 80% 50% at 20% -10%,rgba(94,234,212,.12),transparent),radial-gradient(ellipse 60% 40% at 100% 0%,rgba(167,139,250,.1),transparent);color:var(--text)}
.side{backdrop-filter:blur(20px);background:rgba(10,12,18,.85);border-right:1px solid var(--line)}
.top{backdrop-filter:blur(16px);background:rgba(10,12,18,.7);border-bottom:1px solid var(--line);position:sticky;top:0;z-index:40}
.card{background:var(--card);backdrop-filter:blur(12px);border:1px solid var(--line);border-radius:16px;overflow:hidden;transition:transform .25s cubic-bezier(.22,.61,.36,1),box-shadow .25s,border-color .2s}
.card:hover{transform:translateY(-6px) scale(1.01);box-shadow:0 20px 40px rgba(0,0,0,.4);border-color:rgba(94,234,212,.35)}
.btn{background:linear-gradient(135deg,#2dd4bf,#8b5cf6);border:none;color:#041018;font-weight:700;border-radius:12px;box-shadow:0 8px 24px rgba(45,212,191,.25);transition:transform .2s,box-shadow .2s}
.btn:hover{transform:translateY(-2px);box-shadow:0 12px 32px rgba(139,92,246,.35)}
.btn.ghost{background:transparent;color:var(--text);border:1px solid var(--line);box-shadow:none}
.hero{border-radius:24px;overflow:hidden;animation:fadeUp .6s cubic-bezier(.22,.61,.36,1)}
.sec-head h2{letter-spacing:-.02em}
.nav a{border-radius:10px;margin:2px 8px;transition:background .2s,transform .15s}
.nav a.on{background:linear-gradient(90deg,rgba(45,212,191,.2),rgba(139,92,246,.12));border-left:3px solid var(--a)}
.dl-hero{background:linear-gradient(135deg,rgba(45,212,191,.15),rgba(139,92,246,.12));border:1px solid var(--line);backdrop-filter:blur(12px)}
.dl-fmt{backdrop-filter:blur(8px);transition:transform .15s,border-color .15s}
.dl-fmt:hover{transform:translateX(4px)}
.dl-fmt.muxed{border-color:rgba(45,212,191,.5);box-shadow:0 0 20px rgba(45,212,191,.08)}
.now-bar{backdrop-filter:blur(20px);background:rgba(10,12,18,.9);border-top:1px solid var(--line)}
@keyframes fadeUp{from{opacity:0;transform:translateY(16px)}to{opacity:1;transform:none}}
@keyframes pulseGlow{0%,100%{box-shadow:0 0 0 0 rgba(45,212,191,.2)}50%{box-shadow:0 0 24px 4px rgba(45,212,191,.15)}}
.player-wrap{animation:pulseGlow 4s ease-in-out infinite;border-radius:16px}
.empty{animation:fadeUp .4s ease}
.api-card{transition:transform .2s,border-color .2s}
.api-card:hover{transform:translateY(-3px);border-color:rgba(45,212,191,.4)}
@media (prefers-reduced-motion:reduce){*{animation:none!important;transition:none!important}}

/* ===== StreamHub UI 2026 — liquid glass + motion (SimpMusic / vivi inspired) ===== */
:root{
  --bg:#05060a;--bg2:#0c0e14;--card:rgba(18,20,28,.65);--card-solid:#12141c;
  --line:rgba(255,255,255,.07);--line2:rgba(255,255,255,.12);
  --text:#f4f6fb;--mute:#8b93a7;--a:#2dd4bf;--a2:#a78bfa;--a3:#f472b6;
  --radius:18px;--ease:cubic-bezier(.22,.61,.36,1);
  --shadow:0 12px 40px rgba(0,0,0,.45);
}
*{box-sizing:border-box}
html{scroll-behavior:smooth}
body{
  margin:0;font-family:"Outfit",ui-sans-serif,system-ui,sans-serif;
  background:var(--bg);color:var(--text);min-height:100vh;line-height:1.45;
  background-image:
    radial-gradient(ellipse 90% 60% at 10% -20%,rgba(45,212,191,.14),transparent 55%),
    radial-gradient(ellipse 70% 50% at 100% 0%,rgba(167,139,250,.12),transparent 50%),
    radial-gradient(ellipse 50% 40% at 50% 100%,rgba(244,114,182,.06),transparent 50%);
  background-attachment:fixed;
}
.layout{display:flex;min-height:100vh}
.side{
  width:240px;flex-shrink:0;padding:18px 12px;display:flex;flex-direction:column;gap:6px;
  background:rgba(8,10,16,.82);backdrop-filter:blur(24px) saturate(1.4);
  border-right:1px solid var(--line);position:sticky;top:0;height:100vh;z-index:30;
}
.brand{font-size:1.25rem;font-weight:800;padding:8px 12px 18px;letter-spacing:-.03em}
.brand span{background:linear-gradient(120deg,var(--a),var(--a2),var(--a3));-webkit-background-clip:text;background-clip:text;color:transparent}
.nav a{
  display:flex;align-items:center;gap:10px;padding:11px 14px;border-radius:12px;
  color:var(--mute);text-decoration:none;font-weight:600;font-size:.92rem;
  transition:background .2s var(--ease),color .2s,transform .15s var(--ease);
}
.nav a:hover{background:rgba(255,255,255,.05);color:var(--text);transform:translateX(3px)}
.nav a.on{
  background:linear-gradient(105deg,rgba(45,212,191,.18),rgba(167,139,250,.1));
  color:var(--text);border-left:3px solid var(--a);box-shadow:inset 0 0 20px rgba(45,212,191,.05);
}
.main{flex:1;min-width:0;display:flex;flex-direction:column}
.top{
  display:flex;align-items:center;gap:10px;padding:12px 18px;
  background:rgba(8,10,16,.7);backdrop-filter:blur(20px);
  border-bottom:1px solid var(--line);position:sticky;top:0;z-index:25;
}
.top input,.search input{
  flex:1;background:rgba(255,255,255,.05);border:1px solid var(--line);border-radius:999px;
  padding:11px 18px;color:var(--text);font-size:.95rem;outline:none;
  transition:border-color .2s,box-shadow .2s;
}
.top input:focus{border-color:rgba(45,212,191,.45);box-shadow:0 0 0 3px rgba(45,212,191,.12)}
.content{padding:18px 20px 100px;animation:pageIn .45s var(--ease)}
@keyframes pageIn{from{opacity:0;transform:translateY(10px)}to{opacity:1;transform:none}}
@keyframes fadeUp{from{opacity:0;transform:translateY(16px)}to{opacity:1;transform:none}}
@keyframes scaleIn{from{opacity:0;transform:scale(.96)}to{opacity:1;transform:none}}
@keyframes shimmer{0%{background-position:200% 0}100%{background-position:-200% 0}}
.hero{
  position:relative;border-radius:24px;overflow:hidden;min-height:220px;margin-bottom:22px;
  animation:scaleIn .5s var(--ease);box-shadow:var(--shadow);
}
.hero img{position:absolute;inset:0;width:100%;height:100%;object-fit:cover;opacity:.45;filter:saturate(1.1)}
.hero .hbody{position:relative;padding:28px 24px;max-width:560px}
.hero h1{font-size:clamp(1.4rem,3vw,2rem);margin:0 0 8px;letter-spacing:-.03em}
.btn{
  display:inline-flex;align-items:center;gap:6px;padding:11px 18px;border:none;border-radius:12px;
  background:linear-gradient(135deg,var(--a),#0d9488 40%,var(--a2));color:#041016;font-weight:700;
  cursor:pointer;font-size:.9rem;box-shadow:0 8px 28px rgba(45,212,191,.22);
  transition:transform .2s var(--ease),box-shadow .2s;
}
.btn:hover{transform:translateY(-2px) scale(1.02);box-shadow:0 12px 36px rgba(167,139,250,.3)}
.btn.ghost{background:rgba(255,255,255,.06);color:var(--text);box-shadow:none;border:1px solid var(--line)}
.btn.ghost:hover{background:rgba(255,255,255,.1);border-color:var(--line2)}
.sec{margin:22px 0;animation:fadeUp .5s var(--ease) both}
.sec-head{display:flex;align-items:center;justify-content:space-between;margin-bottom:12px}
.sec-head h2{margin:0;font-size:1.15rem;letter-spacing:-.02em}
.row{display:flex;gap:12px;overflow-x:auto;padding-bottom:8px;scroll-snap-type:x mandatory;-webkit-overflow-scrolling:touch}
.row::-webkit-scrollbar{height:4px}
.row::-webkit-scrollbar-thumb{background:rgba(255,255,255,.15);border-radius:4px}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(140px,1fr));gap:14px}
.card{
  background:var(--card);backdrop-filter:blur(14px);border:1px solid var(--line);border-radius:var(--radius);
  overflow:hidden;cursor:pointer;scroll-snap-align:start;min-width:130px;flex-shrink:0;
  transition:transform .28s var(--ease),box-shadow .28s,border-color .2s;
}
.card:hover{transform:translateY(-6px) scale(1.02);box-shadow:0 16px 40px rgba(0,0,0,.4);border-color:rgba(45,212,191,.35)}
.card .p{aspect-ratio:2/3;background:var(--card-solid) center/cover no-repeat}
.card .t{padding:9px 10px;font-size:.8rem;font-weight:600;line-height:1.3;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden}
/* Music list rows — SimpMusic style */
.m-row{
  display:flex;align-items:center;gap:12px;padding:10px 12px;border-radius:14px;
  border:1px solid transparent;cursor:pointer;
  transition:background .2s,border-color .2s,transform .15s var(--ease);
}
.m-row:hover{background:rgba(255,255,255,.05);border-color:var(--line);transform:translateX(4px)}
.m-row img{width:52px;height:52px;border-radius:10px;object-fit:cover;background:#111;box-shadow:0 4px 12px rgba(0,0,0,.3)}
.m-row .meta{flex:1;min-width:0}
.m-row .meta .t{font-weight:650;font-size:.92rem;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.m-row .meta .a{color:var(--mute);font-size:.8rem}
.m-chips{display:flex;flex-wrap:wrap;gap:8px;margin:12px 0}
.m-chips button{
  border-radius:999px;padding:8px 14px;font-size:.8rem;font-weight:600;cursor:pointer;
  background:rgba(255,255,255,.05);border:1px solid var(--line);color:var(--text);
  transition:background .2s,border-color .2s,transform .15s;
}
.m-chips button:hover,.m-chips button.on{background:rgba(45,212,191,.15);border-color:rgba(45,212,191,.4);color:var(--a);transform:scale(1.04)}
/* Now playing bar — glass */
.now-bar{
  position:fixed;left:0;right:0;bottom:0;z-index:50;
  display:none;align-items:center;gap:12px;padding:10px 16px 12px;
  background:rgba(10,12,18,.88);backdrop-filter:blur(28px) saturate(1.5);
  border-top:1px solid var(--line);box-shadow:0 -8px 32px rgba(0,0,0,.35);
}
.now-bar.on{display:flex;animation:fadeUp .35s var(--ease)}
.now-bar img{width:48px;height:48px;border-radius:10px;object-fit:cover}
.now-meta{flex:1;min-width:0}
.now-meta .t{font-weight:700;font-size:.9rem;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.now-meta .a{color:var(--mute);font-size:.78rem}
.now-fill{position:absolute;left:0;top:0;height:2px;background:linear-gradient(90deg,var(--a),var(--a2));width:0;transition:width .2s linear}
/* Player page */
.mp-wrap{max-width:480px;margin:0 auto;text-align:center;animation:scaleIn .4s var(--ease)}
.mp-art{
  width:min(280px,70vw);aspect-ratio:1;border-radius:20px;margin:0 auto 18px;
  background:#111 center/cover;box-shadow:0 20px 60px rgba(0,0,0,.5),0 0 40px rgba(45,212,191,.1);
  animation:artFloat 6s ease-in-out infinite;
}
@keyframes artFloat{0%,100%{transform:translateY(0)}50%{transform:translateY(-8px)}}
.mp-title{font-size:1.35rem;font-weight:800;margin:0 0 4px;letter-spacing:-.02em}
.mp-artist{color:var(--mute);margin-bottom:16px}
.mp-controls{display:flex;justify-content:center;align-items:center;gap:14px;margin:16px 0}
.mp-controls button{
  width:48px;height:48px;border-radius:50%;border:1px solid var(--line);
  background:rgba(255,255,255,.06);color:var(--text);font-size:1.1rem;cursor:pointer;
  transition:transform .15s,background .2s;
}
.mp-controls button:hover{transform:scale(1.08);background:rgba(45,212,191,.2)}
.mp-controls button.play{
  width:60px;height:60px;background:linear-gradient(135deg,var(--a),var(--a2));color:#041016;border:none;
  box-shadow:0 8px 24px rgba(45,212,191,.3);
}
.lrc-box{
  height:min(42vh,320px);overflow:hidden;position:relative;margin:16px 0;
  mask-image:linear-gradient(transparent,black 12%,black 88%,transparent);
  -webkit-mask-image:linear-gradient(transparent,black 12%,black 88%,transparent);
}
.lrc-track{transition:transform .3s var(--ease);will-change:transform}
.lrc-line{padding:10px 8px;font-size:1rem;color:rgba(255,255,255,.22);transition:color .25s,font-size .25s,transform .25s}
.lrc-line.on{color:#fff;font-size:1.25rem;font-weight:700}
.lrc-line.near{color:rgba(255,255,255,.5)}
.player-wrap{border-radius:16px;overflow:hidden;box-shadow:var(--shadow);border:1px solid var(--line);background:#000;aspect-ratio:16/9}
.dl-hero,.api-card{
  background:var(--card);backdrop-filter:blur(16px);border:1px solid var(--line);border-radius:20px;
  animation:fadeUp .45s var(--ease);
}
#toast{
  position:fixed;bottom:80px;left:50%;transform:translateX(-50%) translateY(20px);
  background:rgba(20,22,30,.95);backdrop-filter:blur(12px);border:1px solid var(--line);
  padding:10px 18px;border-radius:12px;opacity:0;pointer-events:none;z-index:100;
  transition:opacity .25s,transform .25s var(--ease);font-size:.88rem;font-weight:600;
}
#toast.on{opacity:1;transform:translateX(-50%) translateY(0)}
@media(max-width:800px){
  .side{position:fixed;left:0;top:0;bottom:0;transform:translateX(-105%);transition:transform .3s var(--ease)}
  .side.open{transform:none;box-shadow:20px 0 40px rgba(0,0,0,.5)}
  .content{padding:14px 12px 110px}
  .grid{grid-template-columns:repeat(auto-fill,minmax(110px,1fr));gap:10px}
}
@media(prefers-reduced-motion:reduce){*{animation:none!important;transition:none!important}}

</style><script src="https://cdn.jsdelivr.net/npm/hls.js@1.5.8/dist/hls.min.js"></script>
<link href="https://fonts.googleapis.com/css2?family=Outfit:wght@400;500;600;700;800&display=swap" rel="stylesheet"/>
</head><body>
<div class="sh-banner">
  <h1>StreamHub API</h1>
  <p>Catalog · Play · Music · Downloader (yt-dlp + ffmpeg merge) · MovieBox · 4K</p>
  <p style="margin-top:8px">
    <a href="/">← Web App</a>
    <a href="/site">SPA</a>
    <a href="/openapi.json">OpenAPI JSON</a>
    <a href="/health">Health</a>
  </p>
</div>
<div id="swagger-ui"></div>
<script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
<script>
SwaggerUIBundle({
  url: '/openapi.json',
  dom_id: '#swagger-ui',
  deepLinking: true,
  docExpansion: 'list',
  defaultModelsExpandDepth: -1,
  tryItOutEnabled: true,
  persistAuthorization: true,
});
</script>
</body></html>""")



SPA_HTML = r"""<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8"/><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"/>
<title>StreamHub</title>
<link rel="preconnect" href="https://fonts.googleapis.com"/>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet"/>
<script src="https://cdn.dashjs.org/latest/dash.all.min.js"></script>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/plyr@3.7.8/dist/plyr.css"/>
<script src="https://cdn.jsdelivr.net/npm/plyr@3.7.8/dist/plyr.polyfilled.min.js"></script>
<style>
:root{
  --bg:#0b0b0e;--bg2:#15151a;--card:#1c1c22;--card2:#24242c;--line:#2a2a34;
  --text:#f0f0f3;--mute:#9a9aa6;--a:#00c2e6;--a2:#0b6e99;--danger:#ef5b5b;
  --grad:linear-gradient(135deg,#00c2e6 0%,#7b6bff 100%);
  --a-soft:rgba(0,194,230,.14);
  --rad:10px;--nav:64px;--side:220px;
}
*{box-sizing:border-box;margin:0;padding:0}
html,body{height:100%;background:var(--bg);color:var(--text);font-family:Inter,system-ui,sans-serif}
::-webkit-scrollbar{width:10px;height:8px}
::-webkit-scrollbar-track{background:transparent}
::-webkit-scrollbar-thumb{background:var(--line);border-radius:8px}
::-webkit-scrollbar-thumb:hover{background:var(--a2)}
*{scrollbar-color:var(--line) transparent;scrollbar-width:thin}
a{color:inherit;text-decoration:none}
button,input,select{font:inherit;color:inherit}
img{display:block;max-width:100%}
.app{display:flex;min-height:100%}
.side{width:var(--side);background:var(--bg2);border-right:1px solid var(--line);padding:18px 12px;position:sticky;top:0;height:100vh;flex-shrink:0;display:flex;flex-direction:column;gap:6px;z-index:20}
.brand{font-weight:800;font-size:1.2rem;padding:8px 12px 20px;letter-spacing:-.02em;display:flex;align-items:center;gap:8px}
.brand span{background:var(--grad);-webkit-background-clip:text;background-clip:text;color:transparent}
.nav a,.nav button{position:relative;display:flex;align-items:center;gap:10px;width:100%;padding:11px 12px;border-radius:8px;border:0;background:transparent;color:var(--mute);cursor:pointer;text-align:left;transition:background .15s,color .15s}
.nav a.on,.nav button.on,.nav a:hover,.nav button:hover{background:var(--a-soft);color:var(--text)}
.nav a.on::before{content:'';position:absolute;left:-12px;top:8px;bottom:8px;width:3px;border-radius:3px;background:var(--grad)}
.main{flex:1;min-width:0;display:flex;flex-direction:column}
.top{height:var(--nav);display:flex;align-items:center;gap:12px;padding:0 18px;border-bottom:1px solid var(--line);background:rgba(14,14,16,.85);backdrop-filter:blur(10px);position:sticky;top:0;z-index:15}
.search{flex:1;max-width:420px;background:var(--card);border:1px solid var(--line);border-radius:999px;padding:10px 16px;outline:none;transition:border-color .15s,box-shadow .15s}
.search:focus{border-color:var(--a);box-shadow:0 0 0 3px var(--a-soft)}
.content{padding:18px 18px 48px;flex:1}
.hero{position:relative;border-radius:14px;overflow:hidden;min-height:220px;margin-bottom:22px;background:var(--card)}
.hero img{width:100%;height:280px;object-fit:cover;opacity:.55}
.hero .hbody{position:absolute;inset:0;display:flex;flex-direction:column;justify-content:flex-end;padding:22px;background:linear-gradient(transparent 20%,rgba(0,0,0,.85))}
.hero h1{font-size:clamp(1.3rem,3vw,2rem);margin-bottom:6px}
.hero p{color:var(--mute);max-width:560px;font-size:.92rem;line-height:1.45;display:-webkit-box;-webkit-line-clamp:3;-webkit-box-orient:vertical;overflow:hidden}
.sec{margin:22px 0}
.sec h2{font-size:1.05rem;font-weight:600;margin-bottom:12px;padding-left:2px}
.row{display:flex;gap:12px;overflow-x:auto;padding-bottom:8px;scroll-snap-type:x mandatory;-webkit-overflow-scrolling:touch}
.row::-webkit-scrollbar{height:6px}
.row::-webkit-scrollbar-thumb{background:var(--line);border-radius:4px}
.card{flex:0 0 140px;scroll-snap-align:start;cursor:pointer;transition:transform .15s}
.card:hover{transform:translateY(-3px)}
.card .p{aspect-ratio:2/3;border-radius:var(--rad);background:var(--card) center/cover no-repeat;border:1px solid var(--line)}
.card .t{font-size:.8rem;margin-top:7px;line-height:1.25;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden}
.card .y{font-size:.72rem;color:var(--mute);margin-top:2px}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(120px,1fr));gap:14px}
.empty{padding:40px;text-align:center;color:var(--mute)}
.err{color:var(--danger)}
.toast{position:fixed;bottom:18px;left:50%;transform:translateX(-50%);background:var(--card);border:1px solid var(--line);padding:10px 16px;border-radius:999px;z-index:99;font-size:.85rem;box-shadow:0 8px 30px rgba(0,0,0,.4)}
.detail{display:grid;grid-template-columns:200px 1fr;gap:22px}
.detail .poster{border-radius:12px;aspect-ratio:2/3;background:var(--card) center/cover;border:1px solid var(--line)}
.detail h1{font-size:1.6rem;margin-bottom:8px}
.meta{color:var(--mute);font-size:.9rem;margin-bottom:12px}
.overview{line-height:1.55;color:#c8c8d0;margin-bottom:16px}
.btn{display:inline-flex;align-items:center;gap:8px;background:var(--a);color:#041018;border:0;padding:11px 18px;border-radius:8px;font-weight:600;cursor:pointer}
.btn.ghost{background:var(--card);color:var(--text);border:1px solid var(--line)}
.player-wrap{background:#000;border-radius:12px;overflow:hidden;border:1px solid var(--line);aspect-ratio:16/9;max-height:70vh}
#frame{width:100%;height:100%;background:#000}
.bar{display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin:12px 0;color:var(--mute);font-size:.85rem}
.src,.ep{background:var(--card);border:1px solid var(--line);padding:8px 12px;border-radius:8px;cursor:pointer}
.src.on,.ep.on{background:rgba(0,164,220,.2);border-color:var(--a);color:var(--a)}
.hubbox{margin-top:14px}
.hubbox h3{font-size:.85rem;color:var(--mute);margin-bottom:8px}
.hubbox .src{display:block;width:100%;text-align:left;margin:5px 0}
.se-select{background:var(--card);border:1px solid var(--line);padding:8px 10px;border-radius:8px}
.menu-btn{display:none;background:var(--card);border:1px solid var(--line);padding:8px 12px;border-radius:8px}
@media (max-width:860px){
  .side{position:fixed;left:0;top:0;transform:translateX(-105%);transition:transform .2s;box-shadow:8px 0 30px rgba(0,0,0,.5)}
  .side.open{transform:none}
  .menu-btn{display:inline-flex}
  .detail{grid-template-columns:1fr}
  .detail .poster{max-width:160px}
  .card{flex-basis:110px}
  .hero img{height:200px}
}

.badge{display:inline-block;background:var(--a);color:#041018;font-size:.7rem;font-weight:700;padding:3px 8px;border-radius:6px;margin-bottom:8px;letter-spacing:.04em}
.sec-head{display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap;margin-bottom:12px}
.sec-head h2{margin:0}
.pager{display:flex;align-items:center;gap:10px}
.pager .src:disabled{opacity:.35;cursor:not-allowed}
.page-n{font-size:.85rem;color:var(--mute)}
.card{position:relative}
.card::after{content:'';position:absolute;inset:0;border-radius:var(--rad);box-shadow:inset 0 0 0 1px rgba(255,255,255,.04);pointer-events:none}
.hero{box-shadow:0 12px 40px rgba(0,0,0,.35)}
.btn{transition:transform .12s,box-shadow .12s}
.btn:hover{transform:translateY(-1px);box-shadow:0 6px 20px rgba(0,164,220,.35)}
.src:hover,.ep:hover{border-color:var(--a)}
.player-wrap{box-shadow:0 16px 48px rgba(0,0,0,.45)}
@media (max-width:860px){
  .content{padding:12px 12px 40px}
  .pager{width:100%;justify-content:space-between}
  .brand{font-size:1.05rem}
}

.dlbox{margin-top:18px}
.dlbox h3,.hubbox h3{font-size:.8rem;color:var(--mute);margin-bottom:10px;text-transform:uppercase;letter-spacing:.06em}
.dl-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(240px,1fr));gap:10px}
.dl-card{display:flex;gap:12px;align-items:center;padding:12px 14px;background:linear-gradient(145deg,#1a1a22,#141418);border:1px solid var(--line);border-radius:12px;transition:border-color .15s,transform .15s}
.dl-card:hover{border-color:var(--a);transform:translateY(-2px)}
.dl-ico{width:40px;height:40px;border-radius:10px;background:rgba(0,164,220,.15);color:var(--a);display:flex;align-items:center;justify-content:center;font-size:1.1rem;flex-shrink:0}
.dl-title{font-size:.88rem;font-weight:600;line-height:1.25}
.dl-sub{font-size:.72rem;color:var(--mute);margin-top:3px}

.player-shell{background:#0a0a0c;border-radius:14px;overflow:hidden;border:1px solid var(--line);box-shadow:0 20px 60px rgba(0,0,0,.5)}
.player-wrap{aspect-ratio:16/9;max-height:72vh;background:#000;position:relative}
.plyr{height:100%}
.plyr__video-wrapper{background:#000}
.ext-panel{padding:16px 18px;background:linear-gradient(180deg,#14141a,#0e0e12);border-top:1px solid var(--line)}
.ext-panel h4{font-size:.95rem;margin-bottom:6px}
.ext-panel p{font-size:.82rem;color:var(--mute);line-height:1.45;margin-bottom:12px}
.ext-actions{display:flex;flex-wrap:wrap;gap:8px}
.ext-actions a,.ext-actions button{border-radius:10px;padding:10px 14px;font-weight:600;font-size:.85rem;border:1px solid var(--line);background:var(--card);cursor:pointer;color:var(--text)}
.ext-actions a.primary,.ext-actions button.primary{background:var(--a);color:#041018;border-color:var(--a)}
.fmt-tag{display:inline-block;font-size:.68rem;padding:2px 7px;border-radius:6px;background:rgba(0,164,220,.15);color:var(--a);margin-left:6px}

/* Music */
.music-layout{display:flex;flex-direction:column;gap:16px;padding-bottom:100px}
.m-search{display:flex;gap:10px;margin-bottom:8px}
.m-search input{flex:1;background:var(--card);border:1px solid var(--line);border-radius:12px;padding:12px 16px;color:var(--text);font-size:1rem}
.m-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:14px}
.m-card{background:var(--card);border:1px solid var(--line);border-radius:14px;overflow:hidden;cursor:pointer;transition:transform .15s,border-color .15s,box-shadow .15s}
.m-card:hover{transform:translateY(-3px);border-color:var(--a);box-shadow:0 14px 34px rgba(0,0,0,.4)}
.m-card .art{position:relative}
.m-card img{width:100%;aspect-ratio:1;object-fit:cover;display:block;background:#111}
.m-card .art::after{content:'▶';position:absolute;inset:0;display:flex;align-items:center;justify-content:center;font-size:1.4rem;color:#fff;background:rgba(0,0,0,.38);opacity:0;transition:opacity .15s}
.m-card:hover .art::after{opacity:1}
.m-card .mi{padding:10px 12px}
.m-card .mt{font-size:.88rem;font-weight:600;line-height:1.3;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden}
.m-card .ma{font-size:.75rem;color:var(--mute);margin-top:4px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.now-bar{position:fixed;left:0;right:0;bottom:0;z-index:50;background:linear-gradient(180deg,rgba(16,16,20,.92),#0c0c10);border-top:1px solid var(--line);backdrop-filter:blur(16px);padding:10px 16px;display:none;align-items:center;gap:14px}
.now-bar.on{display:flex}
.now-bar img{width:52px;height:52px;border-radius:8px;object-fit:cover}
.now-meta{flex:1;min-width:0}
.now-meta .t{font-weight:600;font-size:.9rem;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.now-meta .a{font-size:.75rem;color:var(--mute)}
.now-actions{display:flex;gap:8px;align-items:center}
.now-player{width:min(420px,40vw);height:52px;border-radius:8px;overflow:hidden;background:#000;flex-shrink:0}
.now-player iframe{width:100%;height:100%;border:0}
@media (max-width:860px){
  .now-player{width:120px;height:48px}
  .m-grid{grid-template-columns:repeat(auto-fill,minmax(130px,1fr))}
}

.api-table{display:flex;flex-direction:column;gap:8px}
.api-row{display:grid;grid-template-columns:100px 1fr 1.2fr;gap:10px;align-items:center;padding:12px 14px;background:var(--card);border:1px solid var(--line);border-radius:12px;font-size:.85rem}
.api-g{font-size:.7rem;font-weight:700;text-transform:uppercase;letter-spacing:.05em;color:var(--a)}
.api-e{font-size:.8rem;color:#e8e8f0;word-break:break-all}
.api-d{color:var(--mute);font-size:.8rem}
@media (max-width:860px){.api-row{grid-template-columns:1fr;gap:4px}}
.side a.on{background:rgba(0,164,220,.15);color:var(--a)}

.m-hero{margin-bottom:6px;padding:26px 22px;border-radius:18px;position:relative;overflow:hidden;background:radial-gradient(120% 160% at 0% 0%,rgba(0,194,230,.22),transparent 60%),radial-gradient(120% 160% at 100% 0%,rgba(123,107,255,.20),transparent 55%),var(--bg2);border:1px solid var(--line)}
.m-hero h1{font-size:1.9rem;margin:0 0 4px;letter-spacing:-.02em}
.m-hero p{color:var(--mute);font-size:.9rem;margin:0}
.m-search{margin-top:16px}
.m-chips{display:flex;flex-wrap:wrap;gap:8px;margin-bottom:4px}
.chip{border:1px solid var(--line);background:var(--card);color:var(--text);border-radius:999px;padding:7px 14px;font-size:.8rem;cursor:pointer;transition:border-color .15s,color .15s,background .15s,transform .1s}
.chip:hover{border-color:var(--a);color:var(--a);transform:translateY(-1px)}
.chip:active{transform:translateY(0)}
.chip.on{background:var(--grad);color:#04121a;border-color:transparent;font-weight:600}

/* Full music player */
.m-player-page{display:grid;grid-template-columns:1fr 1fr;gap:20px;padding-bottom:120px}
@media (max-width:900px){.m-player-page{grid-template-columns:1fr}}
.m-stage{background:var(--card);border:1px solid var(--line);border-radius:16px;overflow:hidden}
.m-stage .yt{aspect-ratio:16/9;background:#000}
.m-stage .yt iframe{width:100%;height:100%;border:0}
.m-info{padding:16px 18px}
.m-info h1{font-size:1.25rem;margin-bottom:6px}
.m-info .ar{color:var(--mute);margin-bottom:14px}
.m-actions{display:flex;flex-wrap:wrap;gap:8px}
.m-lyrics{background:linear-gradient(180deg,#14141a 0%,#0e0e14 100%);border:1px solid var(--line);border-radius:20px;padding:14px 12px 18px;display:flex;flex-direction:column;max-width:520px;margin:8px auto 0;width:100%}
.m-lyrics-head{display:flex;align-items:center;justify-content:space-between;padding:0 8px 10px}
.m-lyrics-head h3{font-size:.8rem;color:var(--mute);text-transform:uppercase;letter-spacing:.08em;margin:0}
.m-lyrics-head .tag{font-size:.68rem;color:var(--a);opacity:.9;background:var(--a-soft);padding:3px 9px;border-radius:999px}
.lrc-tools{display:flex;align-items:center;gap:8px}
.lrc-sync{display:inline-flex;align-items:center;gap:6px;background:var(--card2);border-radius:999px;padding:2px 4px;font-size:.72rem;color:var(--mute)}
.lrc-sync button{width:20px;height:20px;border-radius:50%;border:0;background:var(--line);color:var(--text);cursor:pointer;font-size:.85rem;line-height:1;display:inline-flex;align-items:center;justify-content:center}
.lrc-sync button:hover{background:var(--a);color:#041018}
.lrc-sync span{min-width:38px;text-align:center}
.lrc-box{height:min(42vh,320px);overflow-y:auto;overflow-x:hidden;scroll-behavior:smooth;padding:40% 12px;mask-image:linear-gradient(180deg,transparent 0%,#000 18%,#000 82%,transparent 100%);-webkit-mask-image:linear-gradient(180deg,transparent 0%,#000 18%,#000 82%,transparent 100%);-webkit-overflow-scrolling:touch}
.lrc-line{text-align:center;padding:10px 8px;font-size:1.05rem;line-height:1.45;color:rgba(255,255,255,.28);font-weight:500;transition:color .2s,transform .2s,font-size .2s;cursor:default}
.lrc-line.on{color:#fff;font-size:1.28rem;font-weight:700;transform:scale(1.02);text-shadow:0 0 18px var(--a-soft)}
.lrc-line.near{color:rgba(255,255,255,.55);font-size:1.08rem}
.lrc-line:empty{min-height:8px}
.lrc-line.gap{opacity:.35;font-size:.85rem}
.m-lyrics pre{white-space:pre-wrap;font-family:Inter,system-ui,sans-serif;font-size:.92rem;line-height:1.65;color:#d4d4dc;padding:8px}
.sm-rec-list{display:flex;flex-direction:column;gap:6px;max-width:520px;margin:0 auto;width:100%}
.sm-rec-row{display:flex;align-items:center;gap:12px;padding:8px 10px;border-radius:12px;background:var(--card);border:1px solid var(--line);cursor:pointer;transition:background .15s}
.sm-rec-row:hover,.sm-rec-row:active{background:var(--card2)}
.sm-rec-row img{width:48px;height:48px;border-radius:8px;object-fit:cover;background:#222;flex-shrink:0}
.sm-rec-row .t{font-size:.9rem;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.sm-rec-row .a{font-size:.75rem;color:var(--mute);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.sm-rec-row .meta{flex:1;min-width:0}
.sm-player .sec{max-width:520px;margin:18px auto 0;width:100%;padding:0 4px}
.sm-player .sec h2{font-size:1rem;margin-bottom:10px}

.now-bar{position:fixed;left:0;right:0;bottom:0;z-index:50;background:linear-gradient(180deg,rgba(16,16,20,.96),#0a0a0e);border-top:1px solid var(--line);backdrop-filter:blur(16px);padding:10px 14px;display:none;align-items:center;gap:12px}
.now-bar.on{display:flex}
.now-fill{position:absolute;left:0;top:-1px;height:2px;width:0;background:var(--grad);transition:width .2s linear}
.now-bar img{width:48px;height:48px;border-radius:8px;object-fit:cover;cursor:pointer}
#nb-toggle{font-size:.95rem;min-width:38px}
.now-meta{flex:1;min-width:0;cursor:pointer}
.now-meta .t{font-weight:600;font-size:.88rem;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.now-meta .a{font-size:.72rem;color:var(--mute)}
.now-actions{display:flex;gap:6px;align-items:center;flex-shrink:0}
.now-actions .src{padding:8px 10px;font-size:.8rem}
.top-nav-music{display:none}
@media (max-width:860px){
  .top-nav-music{display:inline-flex}
  .side{position:fixed;left:0;top:0;transform:translateX(-105%);transition:transform .2s;z-index:30;height:100vh;box-shadow:8px 0 30px rgba(0,0,0,.5)}
  .side.open{transform:none}
}

/* SimpMusic-style player */
.sm-player{max-width:480px;margin:0 auto;padding:12px 12px 120px;display:flex;flex-direction:column;align-items:center;gap:18px}
.sm-hero{position:relative;width:100%;border-radius:24px;overflow:hidden;padding:28px 20px 22px;display:flex;flex-direction:column;align-items:center;gap:14px;border:1px solid var(--line)}
.sm-hero-bg{position:absolute;inset:0;background-image:var(--art);background-size:cover;background-position:center;filter:blur(42px) saturate(1.6) brightness(.55);transform:scale(1.35);z-index:0}
.sm-hero-bg::after{content:'';position:absolute;inset:0;background:linear-gradient(180deg,rgba(10,10,14,.25) 0%,rgba(10,10,14,.82) 100%)}
.sm-hero>*{position:relative;z-index:1}
.sm-art-wrap{width:min(100%,300px)}
.sm-art{width:100%;aspect-ratio:1;object-fit:cover;border-radius:16px;box-shadow:0 20px 50px rgba(0,0,0,.5);background:var(--card)}
.sm-meta{text-align:center;width:100%}
.sm-title{font-size:1.3rem;font-weight:700;margin:0 0 4px;line-height:1.3}
.sm-artist{color:var(--mute);font-size:.92rem}
.sm-progress{width:100%;padding:0 4px}
.sm-time{display:flex;justify-content:space-between;font-size:.75rem;color:var(--mute);margin-top:4px}
.sm-controls{display:flex;align-items:center;justify-content:center;gap:16px;width:100%}
.sm-icons{display:flex;align-items:center;justify-content:center;gap:10px;width:100%;flex-wrap:wrap}
.sm-icons button{width:38px;height:38px;border-radius:50%;border:1px solid var(--line);background:var(--card);color:var(--mute);font-size:.95rem;cursor:pointer;display:inline-flex;align-items:center;justify-content:center;transition:border-color .15s,color .15s,background .15s}
.sm-icons button:hover{border-color:var(--a);color:var(--a)}
.sm-icons button.on{color:var(--a);border-color:var(--a);background:var(--a-soft)}
#sm-like.on{color:#ff5c8a;border-color:#ff5c8a;background:rgba(255,92,138,.12)}
.queue-backdrop{position:fixed;inset:0;background:rgba(0,0,0,.55);z-index:59;opacity:0;transition:opacity .2s}
.queue-sheet{position:fixed;left:0;right:0;bottom:0;max-height:72vh;background:var(--bg2);border-top:1px solid var(--line);border-radius:18px 18px 0 0;z-index:60;transform:translateY(100%);transition:transform .22s ease;display:flex;flex-direction:column;padding-bottom:env(safe-area-inset-bottom,0)}
.queue-backdrop.open,.queue-sheet.open{opacity:1;transform:translateY(0)}
.queue-sheet.open{transform:translateY(0)}
.queue-head{display:flex;align-items:center;justify-content:space-between;padding:14px 16px;border-bottom:1px solid var(--line)}
.queue-head h3{font-size:.95rem;margin:0}
.queue-head button{width:30px;height:30px;border-radius:50%;border:0;background:var(--card2);color:var(--text);cursor:pointer}
.queue-list{overflow-y:auto;padding:6px}
.queue-row{display:flex;align-items:center;gap:10px;padding:9px 10px;border-radius:10px;cursor:pointer}
.queue-row:hover{background:var(--card2)}
.queue-row.on{background:var(--a-soft)}
.queue-row img{width:42px;height:42px;border-radius:8px;object-fit:cover;background:#111}
.queue-row .meta{flex:1;min-width:0}
.queue-row .meta .t{font-size:.88rem;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.queue-row .meta .a{font-size:.76rem;color:var(--mute);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.queue-row .qrm{width:26px;height:26px;border-radius:50%;border:0;background:transparent;color:var(--mute);cursor:pointer}
.queue-row .qrm:hover{color:var(--danger)}
.queue-row .eq{margin-right:2px}
.sm-btn{width:48px;height:48px;border-radius:50%;border:1px solid var(--line);background:var(--card);color:var(--text);font-size:1.1rem;cursor:pointer;display:inline-flex;align-items:center;justify-content:center;text-decoration:none;transition:border-color .15s,transform .12s,box-shadow .15s}
.sm-btn.sm-play{width:64px;height:64px;background:var(--grad);color:#041018;border-color:transparent;font-size:1.4rem;box-shadow:0 10px 30px rgba(0,194,230,.35)}
.sm-btn:hover{border-color:var(--a);transform:translateY(-1px)}
.sm-btn.on{color:var(--a);border-color:var(--a)}
.sm-extra{display:flex;flex-wrap:wrap;gap:8px;justify-content:center}
.sm-player .m-lyrics{width:100%;max-height:40vh}
.sm-vol{display:flex;align-items:center;gap:10px;width:100%;padding:0 4px;color:var(--mute)}
.sm-vol input[type=range]{flex:1}

/* shared range slider look */
.sm-progress input[type=range],.sm-vol input[type=range]{-webkit-appearance:none;appearance:none;width:100%;height:5px;border-radius:6px;background:linear-gradient(90deg,var(--a) 0%,var(--a) var(--val,0%),var(--line) var(--val,0%),var(--line) 100%);outline:none;cursor:pointer}
.sm-progress input[type=range]::-webkit-slider-thumb,.sm-vol input[type=range]::-webkit-slider-thumb{-webkit-appearance:none;width:14px;height:14px;border-radius:50%;background:#fff;box-shadow:0 0 0 4px var(--a-soft);cursor:pointer}
.sm-progress input[type=range]::-moz-range-thumb,.sm-vol input[type=range]::-moz-range-thumb{width:14px;height:14px;border-radius:50%;background:#fff;border:0;box-shadow:0 0 0 4px var(--a-soft);cursor:pointer}

.sm-now{font-size:.7rem;letter-spacing:.12em;color:var(--mute);text-transform:uppercase;margin-bottom:2px;display:flex;align-items:center;justify-content:center;gap:7px}
.eq{display:none;align-items:flex-end;gap:2px;height:12px}
.eq.on{display:inline-flex}
.eq span{width:3px;background:var(--a);border-radius:2px;height:4px;animation:eqPulse 1s ease-in-out infinite}
.eq span:nth-child(2){animation-delay:.15s}
.eq span:nth-child(3){animation-delay:.3s}
.eq span:nth-child(4){animation-delay:.45s}
@keyframes eqPulse{0%,100%{height:3px}50%{height:12px}}
.yt-hidden{display:none}
.yt-box{width:100%;max-width:340px;margin:10px auto 0;border-radius:12px;overflow:hidden;aspect-ratio:16/9;background:#000}
.yt-box iframe{width:100%;height:100%;border:0}
.lyric-lines{max-height:42vh;overflow-y:auto;text-align:center;padding:8px 4px}
.lyric-lines .ll{padding:8px 6px;color:var(--mute);font-size:.95rem;line-height:1.45;transition:color .2s,transform .2s}
.lyric-lines .ll.on{color:#fff;font-weight:700;font-size:1.05rem;transform:scale(1.03)}
.lyric-lines .ll.past{color:#6a6a75}
.lyric-plain{white-space:pre-wrap;font-family:Inter,system-ui,sans-serif;font-size:.9rem;line-height:1.65;color:#d4d4dc}
.m-lyrics{border-radius:16px;background:linear-gradient(180deg,#1a1a22,#121218)}

/* Player responsive */
.player-shell{width:100%;max-width:1100px;margin:0 auto}
.player-wrap{aspect-ratio:16/9;max-height:min(72vh,620px);width:100%}
@media (max-width:860px){
  .player-wrap{max-height:56vw;border-radius:10px}
  .player-shell{border-radius:10px}
  .bar{gap:6px;font-size:.8rem}
  .src,.ep{padding:8px 10px;font-size:.8rem}
  .content{padding-bottom:88px}
  .top{padding:0 10px;gap:8px}
  .search{max-width:none;font-size:.9rem;padding:9px 12px}
  .btn{padding:9px 12px;font-size:.85rem}
  .m-hero h1{font-size:1.35rem}
  .sm-player{padding-bottom:100px}
  .now-bar{padding:8px 10px}
}
@media (min-width:861px){
  .content{padding:22px 28px 48px}
  .side{padding:20px 14px}
}
.player-wrap iframe{width:100%;height:100%;border:0;display:block}

.sm-vol{display:flex;align-items:center;gap:10px;width:100%;max-width:320px;margin:0 auto}
.sm-vol input{flex:1;accent-color:var(--a)}
.sm-btn.on{border-color:var(--a);color:var(--a);background:rgba(0,164,220,.12)}
.sm-extra .btn{min-width:120px;justify-content:center}

body.watching .now-bar{display:none!important}
body.watching .content{padding-bottom:24px}
.player-shell{max-width:1100px;margin:0 auto}
.player-wrap{border-radius:14px;overflow:hidden;box-shadow:0 12px 40px rgba(0,0,0,.45);border:1px solid var(--line);background:#000;aspect-ratio:16/9}
.player-wrap iframe{width:100%;height:100%;border:0;display:block}
.bar{display:flex;flex-wrap:wrap;align-items:center;gap:8px;margin:12px 0 8px;padding:0 2px}
.bar #st{font-size:.8rem;color:var(--mute);margin-right:auto}
#srcs{display:flex;flex-wrap:wrap;gap:8px;margin:8px 0 12px}
#srcs .src{border-radius:999px;padding:8px 14px;font-weight:600}
#srcs .src.on{background:var(--a);color:#041018;border-color:var(--a)}
.hubbox{margin:10px 0;padding:12px;border:1px dashed var(--line);border-radius:12px}
@media (max-width:700px){
  .player-wrap{border-radius:10px;max-height:56vw}
  .top .search{flex:1;min-width:0}
  .top-nav-music{display:none}
}

.lrc-box{height:min(48vh,360px)!important;overflow:hidden!important;position:relative;padding:0!important}
.lrc-track{position:absolute;left:0;right:0;top:0;padding:0 14px;will-change:transform;transition:transform .25s cubic-bezier(.22,.61,.36,1)}
.lrc-line{text-align:center;padding:12px 6px;font-size:1.02rem;line-height:1.4;color:rgba(255,255,255,.22);font-weight:500}
.lrc-line.on{color:#fff;font-size:1.35rem;font-weight:700}
.lrc-line.near{color:rgba(255,255,255,.5);font-size:1.08rem}
.lrc-line.far{opacity:.5}

/* Downloader — OmniGet inspired */
.dl-wrap{max-width:720px;margin:0 auto}
.dl-hero{background:linear-gradient(135deg,#0d1b2a 0%,#1b2838 50%,#0f3460 100%);border:1px solid var(--line);border-radius:20px;padding:28px 22px;margin-bottom:20px;text-align:center}
.dl-hero h1{font-size:1.6rem;margin:0 0 6px;letter-spacing:-.02em}
.dl-hero p{color:var(--mute);margin:0 0 18px;font-size:.9rem}
.dl-box{display:flex;gap:10px;flex-wrap:wrap}
.dl-box input{flex:1;min-width:200px;background:rgba(0,0,0,.35);border:1px solid var(--line);border-radius:12px;padding:14px 16px;color:var(--text);font-size:1rem}
.dl-box button{border-radius:12px;padding:14px 20px;font-weight:700}
.dl-preview{display:flex;gap:16px;background:var(--card);border:1px solid var(--line);border-radius:16px;padding:16px;margin-bottom:16px;align-items:flex-start}
.dl-preview img{width:160px;max-width:40%;border-radius:12px;aspect-ratio:16/9;object-fit:cover;background:#111}
.dl-preview .meta h2{font-size:1.1rem;margin:0 0 6px}
.dl-preview .meta .sub{color:var(--mute);font-size:.85rem;margin-bottom:8px}
.dl-formats{display:flex;flex-direction:column;gap:8px}
.dl-fmt{display:flex;align-items:center;gap:12px;padding:12px 14px;background:var(--card);border:1px solid var(--line);border-radius:12px;transition:border-color .15s}
.dl-fmt:hover{border-color:var(--a)}
.dl-fmt .lab{flex:1;font-weight:600;font-size:.9rem}
.dl-fmt .sz{color:var(--mute);font-size:.75rem}
.dl-fmt a.btn{padding:8px 14px;font-size:.85rem;text-decoration:none}
.dl-chips{display:flex;flex-wrap:wrap;gap:8px;margin:12px 0;justify-content:center}
.dl-chips button{border-radius:999px;padding:8px 14px;font-size:.8rem;background:rgba(255,255,255,.06);border:1px solid var(--line);color:var(--text);cursor:pointer}
.dl-chips button:hover{border-color:var(--a);color:var(--a)}
@media (max-width:600px){.dl-preview{flex-direction:column}.dl-preview img{width:100%;max-width:100%}}

/* Shine polish */
.brand span{background:linear-gradient(90deg,#00d4ff,#7c5cff);-webkit-background-clip:text;background-clip:text;color:transparent}
.nav a{transition:background .2s,transform .15s}
.nav a:hover{transform:translateX(4px)}
.nav a.on{background:linear-gradient(90deg,rgba(0,164,220,.25),rgba(124,92,255,.15));border-left:3px solid var(--a)}
.btn{background:linear-gradient(135deg,#00a4dc,#0066aa);box-shadow:0 4px 14px rgba(0,164,220,.25);transition:transform .15s,box-shadow .15s}
.btn:hover{transform:translateY(-1px);box-shadow:0 6px 20px rgba(0,164,220,.35)}
.card{transition:transform .2s,box-shadow .2s}
.card:hover{transform:translateY(-4px);box-shadow:0 12px 28px rgba(0,0,0,.35)}
.hero{animation:fadeUp .5s ease}
@keyframes fadeUp{from{opacity:0;transform:translateY(12px)}to{opacity:1;transform:none}}
.api-guide{max-width:900px;margin:0 auto}
.api-card{background:var(--card);border:1px solid var(--line);border-radius:16px;padding:18px 20px;margin-bottom:14px;animation:fadeUp .4s ease both}
.api-card h3{margin:0 0 8px;font-size:1.05rem;display:flex;align-items:center;gap:8px}
.api-card .ep{font-family:ui-monospace,monospace;font-size:.82rem;color:var(--a);background:rgba(0,164,220,.1);padding:4px 8px;border-radius:6px;display:inline-block;margin:4px 0}
.api-card p{color:var(--mute);font-size:.9rem;line-height:1.55;margin:8px 0 0}
.api-card .how{margin-top:10px;padding-top:10px;border-top:1px solid var(--line);font-size:.85rem;color:#c8c8d0}
.anime-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(140px,1fr));gap:14px}
.anime-card{background:var(--card);border:1px solid var(--line);border-radius:14px;overflow:hidden;cursor:pointer;transition:transform .2s}
.anime-card:hover{transform:translateY(-3px)}
.anime-card img{width:100%;aspect-ratio:2/3;object-fit:cover;background:#111}
.anime-card .t{padding:8px 10px;font-size:.82rem;font-weight:600;line-height:1.3}
.dl-fmt.muxed{border-color:rgba(0,200,120,.45);background:rgba(0,200,120,.06)}
.dl-note{background:rgba(0,164,220,.1);border:1px solid rgba(0,164,220,.3);border-radius:12px;padding:12px 14px;margin-bottom:14px;font-size:.88rem;line-height:1.5}

/* Motion + glass 2026 */
:root{--a:#5eead4;--a2:#a78bfa;--bg:#07080c;--card:rgba(22,24,32,.72);--line:rgba(255,255,255,.08);--text:#f1f5f9;--mute:#94a3b8}
body{background:var(--bg);background-image:radial-gradient(ellipse 80% 50% at 20% -10%,rgba(94,234,212,.12),transparent),radial-gradient(ellipse 60% 40% at 100% 0%,rgba(167,139,250,.1),transparent);color:var(--text)}
.side{backdrop-filter:blur(20px);background:rgba(10,12,18,.85);border-right:1px solid var(--line)}
.top{backdrop-filter:blur(16px);background:rgba(10,12,18,.7);border-bottom:1px solid var(--line);position:sticky;top:0;z-index:40}
.card{background:var(--card);backdrop-filter:blur(12px);border:1px solid var(--line);border-radius:16px;overflow:hidden;transition:transform .25s cubic-bezier(.22,.61,.36,1),box-shadow .25s,border-color .2s}
.card:hover{transform:translateY(-6px) scale(1.01);box-shadow:0 20px 40px rgba(0,0,0,.4);border-color:rgba(94,234,212,.35)}
.btn{background:linear-gradient(135deg,#2dd4bf,#8b5cf6);border:none;color:#041018;font-weight:700;border-radius:12px;box-shadow:0 8px 24px rgba(45,212,191,.25);transition:transform .2s,box-shadow .2s}
.btn:hover{transform:translateY(-2px);box-shadow:0 12px 32px rgba(139,92,246,.35)}
.btn.ghost{background:transparent;color:var(--text);border:1px solid var(--line);box-shadow:none}
.hero{border-radius:24px;overflow:hidden;animation:fadeUp .6s cubic-bezier(.22,.61,.36,1)}
.sec-head h2{letter-spacing:-.02em}
.nav a{border-radius:10px;margin:2px 8px;transition:background .2s,transform .15s}
.nav a.on{background:linear-gradient(90deg,rgba(45,212,191,.2),rgba(139,92,246,.12));border-left:3px solid var(--a)}
.dl-hero{background:linear-gradient(135deg,rgba(45,212,191,.15),rgba(139,92,246,.12));border:1px solid var(--line);backdrop-filter:blur(12px)}
.dl-fmt{backdrop-filter:blur(8px);transition:transform .15s,border-color .15s}
.dl-fmt:hover{transform:translateX(4px)}
.dl-fmt.muxed{border-color:rgba(45,212,191,.5);box-shadow:0 0 20px rgba(45,212,191,.08)}
.now-bar{backdrop-filter:blur(20px);background:rgba(10,12,18,.9);border-top:1px solid var(--line)}
@keyframes fadeUp{from{opacity:0;transform:translateY(16px)}to{opacity:1;transform:none}}
@keyframes pulseGlow{0%,100%{box-shadow:0 0 0 0 rgba(45,212,191,.2)}50%{box-shadow:0 0 24px 4px rgba(45,212,191,.15)}}
.player-wrap{animation:pulseGlow 4s ease-in-out infinite;border-radius:16px}
.empty{animation:fadeUp .4s ease}
.api-card{transition:transform .2s,border-color .2s}
.api-card:hover{transform:translateY(-3px);border-color:rgba(45,212,191,.4)}
@media (prefers-reduced-motion:reduce){*{animation:none!important;transition:none!important}}
</style></head>
<body>
<div class="app">
  <aside class="side" id="side">
    <div class="brand">🎬 Stream<span>Hub</span></div>
    <nav class="nav">
      <a href="#/" id="n-home">🏠 Home</a>
      <a href="#/movies" id="n-movies">🎞️ Movies</a>
      <a href="#/series" id="n-series">📺 Series</a>
      <a href="#/music" id="n-music">🎵 Music</a>
      <a href="#/anime" id="n-anime">🎌 Anime</a>
      <a href="#/dl" id="n-dl">⬇ Downloader</a>
      <a href="#/api" id="n-api">🧩 API Docs</a>
    </nav>
    <div style="flex:1"></div>
    <div style="font-size:.72rem;color:var(--mute);padding:8px 12px;line-height:1.4">TMDB · Music · 4K · Downloader<br/>/docs · Merge MP4</div>
  </aside>
  <div class="main">
    <div class="top">
      <button class="menu-btn" type="button" onclick="document.getElementById('side').classList.toggle('open')">☰</button>
      <button class="btn ghost top-nav-music" type="button" onclick="location.hash='#/music'">♪ Music</button>
      <input class="search" id="q" placeholder="Search movies, series, music…" onkeydown="if(event.key==='Enter')goSearch()"/>
      <button class="btn ghost" type="button" onclick="goSearch()">Search</button>
    </div>
    <div class="content" id="root"><div class="empty">Loading…</div></div>
  </div>
</div>
<script>
const root=document.getElementById('root');
const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({ '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
function toast(m,ms=2800){let t=document.querySelector('.toast');if(!t){t=document.createElement('div');t.className='toast';document.body.appendChild(t)}t.textContent=m;clearTimeout(t._x);t._x=setTimeout(()=>t.remove(),ms)}
async function api(path){const r=await fetch(path);if(!r.ok)throw new Error(path+' → '+r.status);return r.json()}
function setNav(k){
  ['home','movies','series','music','anime','dl','api'].forEach(id=>{const el=document.getElementById('n-'+id);if(el)el.classList.toggle('on',id===k)});
  const side=document.getElementById('side');if(side)side.classList.remove('open');
  const q=document.getElementById('q');
  if(q){
    q.placeholder=k==='music'?'Search songs, artists…':'Search movies, series…';
    if(k!=='music' && location.hash.indexOf('#/music')!==0){ /* keep typed query */ }
  }
  // Mini music bar only on music routes (don't cover movie player)
  const bar=document.getElementById('nowbar');
  if(bar){
    const onMusic = (location.hash||'').indexOf('#/music')===0;
    if(onMusic && MSTATE && MSTATE.id) bar.classList.add('on');
    else bar.classList.remove('on');
  }
  document.body.classList.toggle('watching', k==='' && (location.hash||'').indexOf('#/watch')===0);
  document.body.classList.toggle('on-music', (location.hash||'').indexOf('#/music')===0);
}
function card(it){
  const media=it.type==='tv'?'tv':'movie';
  const id=it.tmdb_id||it.id;
  const poster=it.poster?`background-image:url('${esc(it.poster)}')`:'';
  return `<div class="card" onclick="location.hash='#/title/${media}/${id}'"><div class="p" style="${poster}"></div><div class="t">${esc(it.name)}</div><div class="y">${esc(it.year||'')}${it.rating?(' · ★ '+Number(it.rating).toFixed(1)):''}</div></div>`;
}
function row(title,items){if(!items||!items.length)return'';return `<section class="sec"><h2>${esc(title)}</h2><div class="row">${items.map(card).join('')}</div></section>`}






let MSTATE={id:'',vid:'',title:'',artist:'',thumb:'',audioUrl:null,lines:[],queue:[],qIdx:0,loop:'off',shuffle:false,_lrcIdx:-1,_playToken:0,lyricsOffset:0};

function getAudio(){return document.getElementById('g-audio')}
function setPlayIcon(playing){
  const t=document.getElementById('sm-toggle'); if(t) t.textContent=playing?'⏸':'▶';
  const nb=document.getElementById('nb-toggle'); if(nb) nb.textContent=playing?'⏸':'▶';
  const eq=document.getElementById('sm-eq'); if(eq) eq.classList.toggle('on',playing);
}
function nbToggle(){
  const audio=getAudio();
  if(!audio||!audio.src){if(MSTATE.id)location.hash='#/music/play/'+encodeURIComponent(MSTATE.id);return}
  if(audio.paused){audio.play().catch(()=>{})}else{audio.pause()}
}
function copyStreamLink(){
  const u=MSTATE.audioUrl||'';
  if(!u){toast('No stream URL');return}
  const full=/^https?:\/\//i.test(u)?u:(location.origin+u);
  navigator.clipboard.writeText(full).then(()=>toast('Stream link copied')).catch(()=>toast('Copy failed'));
}
function getLiked(){try{return JSON.parse(localStorage.getItem('likedSongs')||'{}')}catch(e){return {}}}
function isLiked(id){return !!getLiked()[id]}
function toggleLike(){
  if(!MSTATE.id) return;
  const liked=getLiked();
  const btn=document.getElementById('sm-like');
  if(liked[MSTATE.id]){
    delete liked[MSTATE.id];
    if(btn){btn.classList.remove('on');btn.textContent='♡'}
    toast('Removed from Liked Songs');
  }else{
    liked[MSTATE.id]={id:MSTATE.id,title:MSTATE.title,artist:MSTATE.artist,thumb:MSTATE.thumb};
    if(btn){btn.classList.add('on');btn.textContent='♥'}
    toast('Added to Liked Songs');
  }
  try{localStorage.setItem('likedSongs',JSON.stringify(liked))}catch(e){}
}
function likedSongsList(){try{return Object.values(getLiked())}catch(e){return []}}
function toggleLyricsPanel(){
  const panel=document.getElementById('m-lyrics-panel');
  const btn=document.getElementById('sm-lyrics-toggle');
  if(!panel) return;
  const hide=panel.style.display!=='none';
  panel.style.display=hide?'none':'';
  if(btn) btn.classList.toggle('on',!hide);
}
function showTrackInfo(){
  const bits=[];
  if(MSTATE.audioUrl) bits.push('JioSaavn audio stream');
  else if(MSTATE.vid) bits.push('YouTube stream');
  bits.push('Lyrics via LRCLIB');
  toast((MSTATE.title||'Track')+' — '+bits.join(' · '));
}
function queueRowsHtml(){
  const q=MSTATE.queue||[];
  if(!q.length) return '<p class="empty" style="padding:20px">Queue is empty</p>';
  return q.map((it,i)=>{
    const rid=it.id||it.video_id||'';
    const active=i===MSTATE.qIdx;
    return `<div class="queue-row${active?' on':''}" onclick="location.hash='#/music/play/'+encodeURIComponent('${esc(rid)}')">
      <img src="${esc(it.thumb||'')}" alt="" loading="lazy"/>
      <div class="meta"><div class="t">${esc(it.title||'')}</div><div class="a">${esc(it.artist||'')}</div></div>
      ${active?'<span class="eq on"><span></span><span></span><span></span><span></span></span>':`<button type="button" class="qrm" onclick="event.stopPropagation();removeFromQueue(${i})" title="Remove">✕</button>`}
    </div>`;
  }).join('');
}
function openQueue(){
  if(document.getElementById('queue-sheet')) return;
  document.body.insertAdjacentHTML('beforeend',
    `<div class="queue-backdrop" id="queue-backdrop" onclick="closeQueue()"></div>
     <div class="queue-sheet" id="queue-sheet">
       <div class="queue-head"><h3>Queue · ${(MSTATE.queue||[]).length}</h3><button type="button" onclick="closeQueue()">✕</button></div>
       <div class="queue-list" id="queue-list">${queueRowsHtml()}</div>
     </div>`);
  requestAnimationFrame(()=>{
    const s=document.getElementById('queue-sheet'), b=document.getElementById('queue-backdrop');
    if(s) s.classList.add('open'); if(b) b.classList.add('open');
  });
}
function closeQueue(){
  const s=document.getElementById('queue-sheet'), b=document.getElementById('queue-backdrop');
  if(s) s.classList.remove('open'); if(b) b.classList.remove('open');
  setTimeout(()=>{if(s)s.remove();if(b)b.remove()},220);
}
function removeFromQueue(i){
  if(!MSTATE.queue) return;
  MSTATE.queue.splice(i,1);
  if(MSTATE.qIdx>i) MSTATE.qIdx--;
  const list=document.getElementById('queue-list');
  if(list) list.innerHTML=queueRowsHtml();
}
let _audioBound=false;
function bindGlobalAudio(){
  if(_audioBound) return; _audioBound=true;
  const audio=getAudio();
  if(!audio) return;
  // One set of listeners for the life of the app — the audio element itself
  // lives outside #root so it (and playback) survives hash navigation;
  // DOM lookups inside these handlers simply no-op on pages without them.
  audio.addEventListener('timeupdate',()=>{
    const dur=audio.duration;
    const fill=document.getElementById('nowfill');
    if(fill&&dur) fill.style.width=((audio.currentTime/dur)*100)+'%';
    const seek=document.getElementById('sm-seek');
    if(seek&&!seek._seeking&&dur){
      seek.value=Math.floor((audio.currentTime/dur)*1000);
      seek.style.setProperty('--val',(seek.value/10)+'%');
      const cur=document.getElementById('sm-cur'); if(cur) cur.textContent=fmtTime(audio.currentTime);
      const du=document.getElementById('sm-dur'); if(du) du.textContent=fmtTime(dur);
    }
    renderSyncLyrics(audio.currentTime);
  });
  audio.addEventListener('play',()=>setPlayIcon(true));
  audio.addEventListener('pause',()=>setPlayIcon(false));
  audio.addEventListener('ended',()=>{
    if(MSTATE.loop==='one'){audio.currentTime=0;audio.play();return}
    if(MSTATE.loop==='all'||(MSTATE.queue&&MSTATE.queue.length>1)){musicNext();return}
    setPlayIcon(false);
  });
}


async function animeHome(){
  setNav('anime');root.innerHTML='<div class="empty">Loading anime…</div>';
  try{
    const d=await api('/anime/home');
    const card=it=>`<div class="card" onclick="animeOpen('${esc(it.slug||'')}')">
      <div class="p" style="background-image:url('${esc(it.poster||'')}')"></div>
      <div class="t">${esc(it.title||'')}</div></div>`;
    const sec=(title,items)=>{
      if(!items||!items.length)return '';
      return `<section class="sec"><div class="sec-head"><h2>${esc(title)}</h2></div>
        <div class="row">${items.map(card).join('')}</div></section>`;
    };
    root.innerHTML=`<div class="dl-hero" style="margin-bottom:16px;padding:22px;background:linear-gradient(135deg,rgba(167,139,250,.12),rgba(45,212,191,.08))">
      <h1 style="margin:0 0 6px">🎌 Anime · Hindi Dub</h1>
      <p style="color:var(--mute);margin:0 0 12px">HindiAnime catalog · HD streams</p>
      <div class="dl-box">
        <input id="aq" placeholder="Search anime…" onkeydown="if(event.key==='Enter')animeSearchGo()"/>
        <button class="btn" type="button" onclick="animeSearchGo()">Search</button>
        <button class="btn ghost" type="button" onclick="animeCatalog()">Full catalog</button>
      </div>
    </div>
    ${sec('Top airing',d.top_airing)}
    ${sec('Most popular',d.most_popular)}
    ${sec('Latest episodes',d.latest_episodes)}
    ${sec('Latest movies',d.latest_movies)}
    ${sec('Completed',d.completed)}
    `;
  }catch(e){root.innerHTML=`<div class="empty err">${esc(e.message)}</div>`}
}
async function animeSearchGo(){
  const q=(document.getElementById('aq')||{}).value||'';
  if(!q.trim())return;
  root.innerHTML='<div class="empty">Searching…</div>';
  try{
    const d=await api('/anime/search?q='+encodeURIComponent(q.trim()));
    root.innerHTML=`<section class="sec"><div class="sec-head"><h2>Results · ${esc(q)}</h2>
      <button class="btn ghost" onclick="animeHome()">← Back</button></div>
      <div class="grid">${(d.items||[]).map(it=>`<div class="card" onclick="animeOpen('${esc(it.slug||'')}')">
        <div class="p" style="background-image:url('${esc(it.poster||'')}')"></div>
        <div class="t">${esc(it.title||'')}</div></div>`).join('')||'<p class="empty">No results</p>'}</div></section>`;
  }catch(e){root.innerHTML=`<div class="empty err">${esc(e.message)}</div>`}
}
async function animeCatalog(){
  root.innerHTML='<div class="empty">Loading catalog…</div>';
  try{
    const d=await api('/anime/catalog');
    root.innerHTML=`<section class="sec"><div class="sec-head"><h2>Catalog · ${(d.items||[]).length}</h2>
      <button class="btn ghost" onclick="animeHome()">← Back</button></div>
      <div class="grid">${(d.items||[]).slice(0,120).map(it=>`<div class="card" onclick="animeOpen('${esc(it.slug||'')}')">
        <div class="p" style="background-image:url('${esc(it.poster||'')}')"></div>
        <div class="t">${esc(it.title||'')}</div></div>`).join('')}</div></section>`;
  }catch(e){root.innerHTML=`<div class="empty err">${esc(e.message)}</div>`}
}
async function animeOpen(slug){
  if(!slug)return;
  root.innerHTML='<div class="empty">Loading episodes…</div>';
  try{
    const d=await api('/anime/detail/'+encodeURIComponent(slug));
    const eps=d.episodes||[];
    root.innerHTML=`<div class="dl-wrap" style="max-width:900px">
      <h1 style="margin:0 0 8px">${esc(d.title||slug)}</h1>
      <p style="color:var(--mute);font-size:.9rem;line-height:1.5">${esc((d.overview||'').slice(0,320))}</p>
      <p style="color:var(--mute);font-size:.8rem">${esc((d.genres||[]).join(' · '))}</p>
      <div style="margin:14px 0"><button class="btn ghost" onclick="animeHome()">← Back</button></div>
      <h3 style="margin:12px 0 8px">Episodes · ${eps.length}</h3>
      <div style="display:flex;flex-wrap:wrap;gap:8px">${eps.map((ep,i)=>
        `<button type="button" class="ep" onclick="animePlay('${esc(slug)}',${i})">S${ep.season||1}E${ep.episode||i+1}</button>`
      ).join('')}</div>
      <div id="anime-player" style="margin-top:16px"></div>
    </div>`;
    window.__animeEps=eps;
  }catch(e){root.innerHTML=`<div class="empty err">${esc(e.message)}</div>`}
}
async function animePlay(slug, idx){
  const eps=window.__animeEps||[];
  const ep=eps[idx];
  if(!ep)return;
  const box=document.getElementById('anime-player');
  if(box) box.innerHTML='<div class="empty">Resolving stream…</div>';
  try{
    let url='';
    if(ep.stream_url) url=ep.stream_url;
    else if(ep.video_hash){
      const s=await api('/anime/stream?hash='+encodeURIComponent(ep.video_hash));
      url=s.stream_url||'';
    }
    if(!url){if(box)box.innerHTML='<div class="empty err">No stream</div>';return}
    if(box){
      if(/\\.m3u8(\\?|$)/i.test(url) && window.Hls){
        box.innerHTML='<video id="av" controls autoplay playsinline style="width:100%;border-radius:14px;background:#000;max-height:70vh"></video>';
        const v=document.getElementById('av');
        if(Hls.isSupported()){const h=new Hls();h.loadSource(url);h.attachMedia(v);}
        else {v.src=url;}
      } else if(/embed|vidmoly|abyss|filesforever/i.test(url)){
        box.innerHTML=`<iframe src="${esc(url)}" allowfullscreen style="width:100%;aspect-ratio:16/9;border:0;border-radius:14px;background:#000"></iframe>`;
      } else {
        box.innerHTML=`<video src="${esc(url)}" controls autoplay playsinline style="width:100%;border-radius:14px;background:#000;max-height:70vh"></video>
          <p style="margin-top:8px"><a class="btn" href="${esc(url)}" target="_blank" rel="noopener">Open stream</a></p>`;
      }
    }
    toast('Playing S'+(ep.season||1)+'E'+(ep.episode||''));
  }catch(e){if(box)box.innerHTML=`<div class="empty err">${esc(e.message)}</div>`}
}


async function musicHome(){
  setNav('music');root.innerHTML='<div class="empty">Loading music…</div>';
  try{
    let trending=[], home={};
    try{const t=await api('/music/yt/trending');trending=t.items||[]}catch(e){}
    try{home=await api('/music/home')}catch(e){}
    const chips=['Arijit','Taylor Swift','Lo-fi','EDM','Punjabi','Anime OP','Bollywood','Hip Hop'];
    const row=(title,items,yt)=>{
      if(!items||!items.length)return '';
      const cards=items.slice(0,20).map(it=>{
        const id=it.id||it.video_id||'';
        const thumb=it.thumb||it.image||'';
        const name=it.title||it.name||'';
        const art=it.artist||it.subtitle||'';
        return `<div class="m-row" onclick="playMusic('${esc(String(id))}')">
          <img src="${esc(thumb)}" alt="" loading="lazy"/>
          <div class="meta"><div class="t">${esc(name)}</div><div class="a">${esc(art)}</div></div>
        </div>`;
      }).join('');
      return `<section class="sec"><div class="sec-head"><h2>${esc(title)}</h2></div><div style="display:flex;flex-direction:column;gap:4px">${cards}</div></section>`;
    };
    root.innerHTML=`<div class="dl-hero" style="margin-bottom:16px;padding:22px">
      <h1 style="margin:0 0 6px">🎵 Music</h1>
      <p style="color:var(--mute);margin:0 0 12px">Saavn · YouTube Music · Invidious streams</p>
      <div class="m-chips">${chips.map(c=>`<button type="button" onclick="document.getElementById('q').value='${c}';musicSearch('${c}')">${c}</button>`).join('')}</div>
    </div>
    ${row('Trending on YouTube',trending,true)}
    ${row('For you',home.trending||home.items||home.charts||[])}
    `;
  }catch(e){root.innerHTML=`<div class="empty err">${esc(e.message)}</div>`}
}
function musicCard(s){
  const id=s.id||s.video_id||s.saavn_id||'';
  return `<div class="m-card" role="button" tabindex="0" onclick="location.hash='#/music/play/${encodeURIComponent(id)}'">
    <div class="art"><img src="${esc(s.thumb||'')}" alt="" loading="lazy"/></div>
    <div class="mi"><div class="mt">${esc(s.title)}</div><div class="ma">${esc(s.artist||s.provider||'')}</div></div>
  </div>`;
}
async function musicSearch(q){
  q=q||((document.getElementById('q')||{}).value||'');
  q=String(q).trim();if(!q)return;
  setNav('music');root.innerHTML='<div class="empty">Searching…</div>';
  try{
    let items=[];
    try{const a=await api('/music/search?q='+encodeURIComponent(q));items=items.concat(a.items||a.results||[])}catch(e){}
    try{const y=await api('/music/yt/search?q='+encodeURIComponent(q));items=items.concat(y.items||[])}catch(e){}
    // dedupe by title
    const seen=new Set();const uniq=[];
    for(const it of items){
      const k=(it.title||it.name||'')+'|'+(it.artist||'');
      if(seen.has(k))continue;seen.add(k);uniq.push(it);
    }
    MSTATE.queue=uniq.slice(0,40).map(it=>({id:it.id||it.video_id,title:it.title||it.name,artist:it.artist,thumb:it.thumb||it.image}));
    root.innerHTML=`<section class="sec"><div class="sec-head"><h2>Results · ${esc(q)}</h2></div>
      <div style="display:flex;flex-direction:column;gap:4px">${uniq.map(it=>{
        const id=it.id||('yt:'+(it.video_id||''));
        return `<div class="m-row" onclick="playMusic('${esc(String(id))}')">
          <img src="${esc(it.thumb||it.image||'')}" alt="" loading="lazy"/>
          <div class="meta"><div class="t">${esc(it.title||it.name||'')}</div><div class="a">${esc(it.artist||it.subtitle||it.provider||'')}</div></div>
        </div>`;
      }).join('')||'<p class="empty">No results</p>'}</div></section>`;
  }catch(e){root.innerHTML=`<div class="empty err">${esc(e.message)}</div>`}
}
function fmtTime(s){s=Math.floor(s||0);return Math.floor(s/60)+':'+String(s%60).padStart(2,'0')}
function nudgeLyrics(delta){
  MSTATE.lyricsOffset=Math.round(((MSTATE.lyricsOffset||0)+delta)*10)/10;
  const el=document.getElementById('lrc-offset');
  if(el) el.textContent=(MSTATE.lyricsOffset>0?'+':'')+MSTATE.lyricsOffset.toFixed(1)+'s';
  const audio=getAudio();
  if(audio) renderSyncLyrics(audio.currentTime||0);
}
function renderSyncLyrics(t){
  const box=document.getElementById('mp-lyrics');
  if(!box||!MSTATE.lines||!MSTATE.lines.length) return;
  const et=t+(MSTATE.lyricsOffset||0);
  let idx=-1;
  for(let i=0;i<MSTATE.lines.length;i++){
    if(MSTATE.lines[i].t<=et) idx=i; else break;
  }
  if(idx<0) idx=0;
  let track=box.querySelector('.lrc-track');
  if(!track || MSTATE._lrcBuilt!==MSTATE.lines.length){
    MSTATE._lrcBuilt=MSTATE.lines.length;
    MSTATE._lrcIdx=-1;
    box.innerHTML='<div class="lrc-track">'+MSTATE.lines.map((ln,i)=>
      `<div class="lrc-line" data-i="${i}">${esc(ln.text||' ')}</div>`
    ).join('')+'</div>';
    track=box.querySelector('.lrc-track');
  }
  if(MSTATE._lrcIdx===idx) return;
  MSTATE._lrcIdx=idx;
  const lines=track.children;
  for(let i=0;i<lines.length;i++){
    const d=Math.abs(i-idx);
    lines[i].className='lrc-line'+(i===idx?' on':(d===1?' near':(d>3?' far':'')));
  }
  const active=lines[idx];
  if(active){
    const mid=box.clientHeight/2;
    const y=active.offsetTop+active.offsetHeight/2;
    track.style.transform='translateY('+(mid-y)+'px)';
  }
}
function cycleLoop(){
  MSTATE.loop=MSTATE.loop==='off'?'one':(MSTATE.loop==='one'?'all':'off');
  const b=document.getElementById('sm-loop');
  if(b){b.textContent=MSTATE.loop==='off'?'🔁':(MSTATE.loop==='one'?'🔂':'🔁');b.classList.toggle('on',MSTATE.loop!=='off');b.title='Loop: '+MSTATE.loop}
  toast('Loop: '+MSTATE.loop);
}
function toggleShuffle(){
  MSTATE.shuffle=!MSTATE.shuffle;
  const b=document.getElementById('sm-shuffle');
  if(b) b.classList.toggle('on',MSTATE.shuffle);
  toast(MSTATE.shuffle?'Shuffle on':'Shuffle off');
}
function musicNext(){
  if(!MSTATE.queue||!MSTATE.queue.length){toast('No queue');return}
  if(MSTATE.shuffle){
    MSTATE.qIdx=Math.floor(Math.random()*MSTATE.queue.length);
  }else{
    MSTATE.qIdx=(MSTATE.qIdx+1)%MSTATE.queue.length;
  }
  const it=MSTATE.queue[MSTATE.qIdx];
  if(it) location.hash='#/music/play/'+encodeURIComponent(it.id||it.video_id||'');
}
function musicPrev(){
  if(!MSTATE.queue||!MSTATE.queue.length){toast('No queue');return}
  MSTATE.qIdx=(MSTATE.qIdx-1+MSTATE.queue.length)%MSTATE.queue.length;
  const it=MSTATE.queue[MSTATE.qIdx];
  if(it) location.hash='#/music/play/'+encodeURIComponent(it.id||it.video_id||'');
}
async function musicPlayPage(rawId){
  const id=decodeURIComponent(rawId||'');
  bindGlobalAudio();
  // Every await below is guarded by this token: if the user navigates to
  // another track (or away) before a request resolves, the stale response
  // is dropped instead of overwriting the now-current track's state/UI —
  // this is what was causing lyrics/queue data from one song to bleed
  // into another when switching quickly.
  const myToken=(MSTATE._playToken=(MSTATE._playToken||0)+1);
  setNav('music');
  root.innerHTML='<div class="empty">Loading player…</div>';
  let title=id, artist='', thumb='', audioUrl=null, duration=null, videoId=null, downloadUrl=null;
  try{
    const d=await api('/music/play/'+encodeURIComponent(id));
    if(myToken!==MSTATE._playToken) return;
    title=d.title||title; artist=d.artist||''; thumb=d.thumb||'';
    audioUrl=d.play_url||d.audio_url||null; duration=d.duration||null; videoId=d.video_id||null;
    downloadUrl=d.audio_url||d.download_url||null;
    if(!audioUrl&&d.sources){const a=(d.sources||[]).find(s=>s.type==='audio');if(a)audioUrl=a.play_url||a.url}
  }catch(e){
    if(myToken!==MSTATE._playToken) return;
    toast('Stream failed: '+(e.message||e));
  }
  if(myToken!==MSTATE._playToken) return;
  // queue index
  if(MSTATE.queue&&MSTATE.queue.length){
    const ix=MSTATE.queue.findIndex(x=>(x.id||x.video_id)===id);
    if(ix>=0) MSTATE.qIdx=ix;
  }else{
    MSTATE.queue=[{id,title,artist,thumb}];
    MSTATE.qIdx=0;
  }
  MSTATE={...MSTATE,id,vid:videoId,title,artist,thumb,audioUrl,lines:[],_lrcIdx:-1,_playToken:myToken};

  root.innerHTML=`<div class="sm-player">
    <div class="sm-hero" style="--art:url('${esc(thumb)}')">
      <div class="sm-hero-bg"></div>
      <div class="sm-art-wrap">${thumb?`<img class="sm-art" src="${esc(thumb)}" alt=""/>`:'<div class="sm-art"></div>'}</div>
      <div class="sm-meta">
        <div class="sm-now"><span class="eq" id="sm-eq"><span></span><span></span><span></span><span></span></span>NOW PLAYING</div>
        <h1 class="sm-title" id="mp-title">${esc(title)}</h1>
        <div class="sm-artist" id="mp-artist">${esc(artist||'Unknown artist')}</div>
      </div>
    </div>
    <div class="sm-progress">
      <input type="range" id="sm-seek" min="0" max="1000" value="0"/>
      <div class="sm-time"><span id="sm-cur">0:00</span><span id="sm-dur">${duration?fmtTime(duration):'—:—'}</span></div>
    </div>
    <div class="sm-vol">
      <span>🔊</span>
      <input type="range" id="sm-vol" min="0" max="100" value="100"/>
    </div>
    <div class="sm-controls">
      <button type="button" class="sm-btn" id="sm-prev" onclick="musicPrev()" title="Previous">⏮</button>
      <button type="button" class="sm-btn sm-play" id="sm-toggle" title="Play">▶</button>
      <button type="button" class="sm-btn" id="sm-next" onclick="musicNext()" title="Next">⏭</button>
    </div>
    <div class="sm-icons">
      <button type="button" id="sm-info" onclick="showTrackInfo()" title="Track info">ℹ</button>
      <button type="button" id="sm-lyrics-toggle" class="on" onclick="toggleLyricsPanel()" title="Show/hide lyrics">📜</button>
      <button type="button" id="sm-shuffle" onclick="toggleShuffle()" title="Shuffle">🔀</button>
      <button type="button" id="sm-loop" onclick="cycleLoop()" title="Repeat">🔁</button>
      <button type="button" id="sm-like" onclick="toggleLike()" title="Like">♡</button>
      <button type="button" id="sm-queue-btn" onclick="openQueue()" title="Queue">☰</button>
    </div>
    <div class="sm-extra">
      <a class="btn" id="sm-dl" href="${esc(downloadUrl||audioUrl||'#')}" target="_blank" rel="noopener" download>⬇ Download</a>
      <button class="btn ghost" type="button" onclick="copyStreamLink()">Copy stream</button>
      <button class="btn ghost" type="button" onclick="location.hash='#/music'">Library</button>
    </div>
    <div class="m-lyrics" id="m-lyrics-panel">
      <div class="m-lyrics-head">
        <h3>Lyrics</h3>
        <div class="lrc-tools">
          <span class="tag" id="lrc-tag"></span>
          <div class="lrc-sync" id="lrc-sync" style="display:none">
            <button type="button" onclick="nudgeLyrics(-0.5)" title="Lyrics running late? Shift earlier">−</button>
            <span id="lrc-offset">0.0s</span>
            <button type="button" onclick="nudgeLyrics(0.5)" title="Lyrics running early? Shift later">+</button>
          </div>
        </div>
      </div>
      <div id="mp-lyrics" class="lrc-box">Loading lyrics…</div>
    </div>
    <section class="sec" id="sm-rec"><h2>More like this</h2><div class="empty">Loading…</div></section>
  </div>`;

  const bar=document.getElementById('nowbar');
  if(bar){
    document.getElementById('nowtitle').textContent=title;
    document.getElementById('nowartist').textContent=artist||'';
    document.getElementById('nowthumb').src=thumb||'';
    document.getElementById('nowplayer').innerHTML='';
    bar.classList.add('on');
  }

  // These buttons are recreated on every navigation, so re-apply the
  // persisted MSTATE flags (and liked status) to their fresh DOM nodes.
  const likeBtn=document.getElementById('sm-like');
  if(likeBtn){const on=isLiked(id);likeBtn.classList.toggle('on',on);likeBtn.textContent=on?'♥':'♡'}
  const shufBtn=document.getElementById('sm-shuffle');
  if(shufBtn) shufBtn.classList.toggle('on',!!MSTATE.shuffle);
  const loopBtn=document.getElementById('sm-loop');
  if(loopBtn){loopBtn.textContent=MSTATE.loop==='one'?'🔂':'🔁';loopBtn.classList.toggle('on',MSTATE.loop!=='off')}

  // One shared audio element lives outside #root (in the mini player bar)
  // so playback survives navigating to other pages — it is not recreated
  // here, only re-pointed when the track actually changed.
  const audio=getAudio();
  const toggle=document.getElementById('sm-toggle');
  const seek=document.getElementById('sm-seek');
  const vol=document.getElementById('sm-vol');

  if(audioUrl){
    if(audio.dataset.trackId!==id||!audio.src){
      audio.src=audioUrl; audio.dataset.trackId=id;
      audio.play().catch(()=>{toast('Tap ▶ to play')});
    }
    setPlayIcon(!audio.paused);
  }else if(videoId){
    document.querySelector('.sm-art-wrap').innerHTML=`<div class="yt" style="aspect-ratio:16/9;width:100%;border-radius:12px;overflow:hidden"><iframe src="https://www.youtube.com/embed/${esc(videoId)}?autoplay=1&rel=0" allow="autoplay;encrypted-media" allowfullscreen style="width:100%;height:100%;border:0"></iframe></div>`;
    toast('Using YouTube player');
  }else toast('No playable stream');

  toggle.onclick=()=>{
    if(!audio.src) return;
    if(audio.paused){audio.play().catch(()=>{})}else{audio.pause()}
  };
  if(vol){
    vol.value=Math.round((audio.volume==null?1:audio.volume)*100);
    vol.style.setProperty('--val',vol.value+'%');
    vol.oninput=()=>{audio.volume=(+vol.value)/100;vol.style.setProperty('--val',vol.value+'%')};
  }
  seek.addEventListener('input',()=>{seek._seeking=true;seek.style.setProperty('--val',(seek.value/10)+'%')});
  seek.addEventListener('change',()=>{
    if(audio.duration) audio.currentTime=(seek.value/1000)*audio.duration;
    seek._seeking=false;
  });
  if(audio.duration) seek.style.setProperty('--val',((audio.currentTime/audio.duration)*100)+'%');

  // lyrics
  MSTATE.lyricsOffset=0;
  const syncBox=document.getElementById('lrc-sync');
  const offEl=document.getElementById('lrc-offset');
  if(offEl) offEl.textContent='0.0s';
  try{
    let lq='/music/lyrics?title='+encodeURIComponent(title)+'&artist='+encodeURIComponent(artist||'');
    if(duration) lq+='&duration='+Math.round(duration);
    const L=await api(lq);
    if(myToken!==MSTATE._playToken) return;
    const el=document.getElementById('mp-lyrics');
    const tg=document.getElementById('lrc-tag');
    if(L.lines&&L.lines.length){
      MSTATE.lines=L.lines; MSTATE._lrcIdx=-1;
      renderSyncLyrics(audio.currentTime||0);
      if(tg) tg.textContent='Synced';
      if(syncBox) syncBox.style.display='inline-flex';
    }else if(L.found&&(L.lyrics||L.synced)){
      el.innerHTML='<pre style="white-space:pre-wrap;font:inherit;color:inherit;margin:0">'+esc(L.lyrics||L.synced)+'</pre>';
      if(tg) tg.textContent='Plain text';
    }else{
      el.innerHTML='<div class="empty" style="padding:26px 8px">No lyrics found</div>';
      if(tg) tg.textContent='';
    }
    if(L.title) document.getElementById('mp-title').textContent=L.title;
    if(L.artist) document.getElementById('mp-artist').textContent=L.artist;
  }catch(e){
    if(myToken!==MSTATE._playToken) return;
    const el=document.getElementById('mp-lyrics');
    if(el) el.textContent='Lyrics unavailable';
  }

  // recommended
  try{
    const rec=await api('/music/related?q='+encodeURIComponent((title+' '+(artist||'')).trim())+'&limit=8');
    if(myToken!==MSTATE._playToken) return;
    const recEl=document.getElementById('sm-rec');
    if(recEl){
      const items=(rec.items||[]).filter(x=>(x.id||'')!==id);
      if(items.length){
        const have=new Set((MSTATE.queue||[]).map(x=>x.id));
        items.forEach(it=>{if(it.id&&!have.has(it.id)){MSTATE.queue.push(it);have.add(it.id)}});
        recEl.innerHTML=`<h2>More like this</h2><div class="sm-rec-list">${items.map(it=>{
          const rid=it.id||it.video_id||'';
          return `<div class="sm-rec-row" onclick="location.hash='#/music/play/'+encodeURIComponent('${esc(rid)}')">
            <img src="${esc(it.thumb||'')}" alt="" loading="lazy"/>
            <div class="meta"><div class="t">${esc(it.title||'')}</div><div class="a">${esc(it.artist||'')}</div></div>
          </div>`;
        }).join('')}</div>`;
      }else recEl.innerHTML='<h2>More like this</h2><p class="empty">No suggestions</p>';
    }
  }catch(e){
    if(myToken!==MSTATE._playToken) return;
    const recEl=document.getElementById('sm-rec');
    if(recEl) recEl.innerHTML='';
  }
}
function playMusic(vid){location.hash='#/music/play/'+encodeURIComponent(vid)}
function closeMusic(){
  const a=getAudio();
  if(a){try{a.pause();a.removeAttribute('src');delete a.dataset.trackId;a.load()}catch(e){}}
  const np=document.getElementById('nowplayer');
  if(np) np.innerHTML='';
  const bar=document.getElementById('nowbar');
  if(bar) bar.classList.remove('on');
  const fill=document.getElementById('nowfill');
  if(fill) fill.style.width='0';
  closeQueue();
  MSTATE.id='';MSTATE.audioUrl=null;
  if(location.hash.indexOf('#/music/play')===0) location.hash='#/music';
}
















async function downloaderHome(){
  setNav('dl');
  root.innerHTML=`<div class="dl-wrap">
    <div class="dl-hero">
      <h1>⬇ Downloader</h1>
      <p>Paste any link — YouTube, TikTok, Instagram, X, Reddit, Vimeo &amp; 1000+ sites (yt-dlp · OmniGet-style)</p>
      <div class="dl-box">
        <input id="dl-url" type="url" placeholder="https://youtube.com/watch?v=…" onkeydown="if(event.key==='Enter')dlProbe()"/>
        <button class="btn" type="button" onclick="dlProbe()">Fetch formats</button>
      </div>
      <div class="dl-chips">
        <button type="button" onclick="document.getElementById('dl-url').value='https://www.youtube.com/watch?v=dQw4w9WgXcQ'">YouTube sample</button>
        <button type="button" onclick="dlMode='audio';dlProbe()">Audio only</button>
      </div>
    </div>
    <div id="dl-out"></div>
  </div>`;
}
let dlMode='all';
function fmtBytes(n){if(!n)return '';n=+n;if(n>1e9)return (n/1e9).toFixed(1)+' GB';if(n>1e6)return (n/1e6).toFixed(1)+' MB';if(n>1e3)return (n/1e3).toFixed(0)+' KB';return n+' B'}
async function dlProbe(){
  const url=(document.getElementById('dl-url')||{}).value||'';
  const u=url.trim(); if(!u){toast('Paste a URL');return}
  const out=document.getElementById('dl-out');
  if(out) out.innerHTML='<div class="empty">Probing with yt-dlp…</div>';
  try{
    const d=await api('/dl/info?url='+encodeURIComponent(u));
    if(!d.ok){out.innerHTML=`<div class="empty err">${esc(d.error||'Failed')}</div>`;return}
    let fmts=d.formats||[];
    if(dlMode==='audio') fmts=fmts.filter(f=>f.kind==='audio'||(f.acodec&&f.acodec!=='none'));
    dlMode='all';
    window.__dlFmts=fmts;
    window.__dlMux=d.best_muxed||fmts.find(f=>f.muxed)||null;
    window.__dlVid=fmts.find(f=>f.kind==='video'&&f.vcodec&&f.vcodec!=='none')||fmts.find(f=>f.height&&(!f.muxed));
    window.__dlAud=d.best_audio||fmts.find(f=>f.kind==='audio'||(f.acodec&&f.acodec!=='none'&&(!f.vcodec||f.vcodec==='none')));
    // prefer highest height video-only for combine
    const vids=fmts.filter(f=>f.vcodec&&f.vcodec!=='none'&&(!f.acodec||f.acodec==='none'||!f.muxed));
    if(vids.length){vids.sort((a,b)=>(b.height||0)-(a.height||0));window.__dlVid=vids[0]}
    const mux=window.__dlMux;
    let h='';
    if(mux&&mux.url){
      h+=`<div class="dl-note">✅ <b>Single file with sound:</b> ${esc(mux.label)}</div>`;
    }else if(window.__dlVid&&window.__dlAud){
      h+=`<div class="dl-note">🔊 Video &amp; audio are separate (normal for YT). Tap <b>⬇ Download with sound</b> — we merge them with ffmpeg into one MP4.</div>`;
    }else{
      h+=`<div class="dl-note">⚠️ No clear video+audio pair. Try another link or quality.</div>`;
    }
    h+=`<div class="dl-preview">
      ${d.thumbnail?`<img src="${esc(d.thumbnail)}" alt=""/>`:''}
      <div class="meta">
        <h2>${esc(d.title||'Untitled')}</h2>
        <div class="sub">${esc(d.uploader||'')} ${d.duration?('· '+fmtTime(d.duration)):''} · ${esc(d.extractor||'')}</div>
        <div style="display:flex;gap:8px;flex-wrap:wrap;margin-top:8px">
          <a class="btn ghost" href="${esc(d.webpage_url||u)}" target="_blank" rel="noopener">Open page</a>
          ${mux&&mux.url?'<button class="btn" type="button" onclick="dlCopyMux()">Copy VIDEO+AUDIO</button>':''}
          ${(window.__dlVid&&window.__dlAud)?'<button class="btn" type="button" onclick="dlCombine()">⬇ Download with sound</button>':''}
          <button class="btn ghost" type="button" onclick="dlMerge()">Merge via yt-dlp</button>
        </div>
      </div>
    </div>
    <h3 style="margin:0 0 10px;font-size:.95rem">Formats · ${fmts.length}</h3>
    <div class="dl-formats">`;
    if(!fmts.length) h+='<div class="empty">No direct formats</div>';
    fmts.forEach((f,i)=>{
      const isM=!!f.muxed;
      h+=`<div class="dl-fmt ${isM?'muxed':''}">
        <div class="lab">${esc(f.label)}</div>
        <div class="sz">${fmtBytes(f.filesize)}</div>
        <a class="btn" href="${esc(f.url||'#')}" target="_blank" rel="noopener" download>⬇ Save</a>
        <button class="btn ghost" type="button" onclick="dlCopyFmt(${i})">Copy</button>
      </div>`;
    });
    h+='</div>';
    out.innerHTML=h;
  }catch(e){
    if(out) out.innerHTML=`<div class="empty err">${esc(e.message||e)}</div>`;
  }
}
function dlCopyMux(){const m=window.__dlMux;if(m&&m.url)navigator.clipboard.writeText(m.url).then(()=>toast('Copied — VIDEO+AUDIO (has sound)'))}
function dlCopyFmt(i){const f=(window.__dlFmts||[])[i];if(f&&f.url)navigator.clipboard.writeText(f.url).then(()=>toast(f.muxed?'Copied (has audio)':'Copied'))}
async function dlCombine(){
  const v=window.__dlVid,a=window.__dlAud;
  if(!v||!a||!v.url||!a.url){toast('Need video + audio formats');return}
  const title=(document.querySelector('.dl-preview h2')||{}).textContent||'video';
  toast('Merging video+audio… keep this tab open (30–120s)');
  try{
    const r=await fetch('/dl/combine',{
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({video:v.url,audio:a.url,title:title})
    });
    if(!r.ok){
      let msg='Merge failed '+r.status;
      try{const j=await r.json();msg=j.detail||msg}catch(e){}
      toast(msg);return;
    }
    const blob=await r.blob();
    if(blob.size<1000){toast('Empty file');return}
    const url=URL.createObjectURL(blob);
    const link=document.createElement('a');
    link.href=url;link.download=(title||'video').replace(/[^\w\-. ]+/g,'').slice(0,80)+'.mp4';
    document.body.appendChild(link);link.click();link.remove();
    setTimeout(()=>URL.revokeObjectURL(url),5000);
    toast('Download started · '+(Math.round(blob.size/1e6*10)/10)+' MB');
  }catch(e){toast(e.message||'Merge error')}
}
function dlMerge(){
  const u=(document.getElementById('dl-url')||{}).value||'';
  if(!u.trim()){toast('Paste URL first');return}
  toast('Merging video+audio (may take 30–90s)…');
  const a=document.createElement('a');
  a.href='/dl/merged?url='+encodeURIComponent(u.trim())+'&quality=720';
  a.target='_blank';
  a.rel='noopener';
  document.body.appendChild(a);a.click();a.remove();
}



async function home(){
  setNav('home');root.innerHTML='<div class="empty">Loading…</div>';
  try{
    const d=await api('/api/home');
    const hero=(d.trending_movies&&d.trending_movies[0])||(d.trending_series&&d.trending_series[0]);
    let h='';
    if(hero){
      const media=hero.type==='tv'?'tv':'movie';
      const bg=hero.backdrop||hero.poster||'';
      h=`<div class="hero">${bg?`<img src="${esc(bg)}" alt=""/>`:''}<div class="hbody"><div class="badge">Featured</div><h1>${esc(hero.name)}</h1><p>${esc(hero.overview||'')}</p><div style="margin-top:12px"><button class="btn" onclick="location.hash='#/watch/${media}/${hero.tmdb_id||hero.id}'">▶ Play</button>
      <button class="btn ghost" style="margin-left:8px" onclick="location.hash='#/title/${media}/${hero.tmdb_id||hero.id}'">Details</button></div></div></div>`;
    }
    root.innerHTML=h
      +row('Trending Movies',d.trending_movies)
      +row('Trending Series',d.trending_series)
      +row('Popular Movies',d.popular_movies)
      +row('Popular Series',d.popular_series)
      +row('Now Playing',d.now_playing)
      +row('On The Air',d.on_the_air)
      +row('Top Movies',d.top_movies)
      +row('Top Series',d.top_series)
      +row('Upcoming',d.upcoming);
  }catch(e){root.innerHTML=`<div class="empty err">${esc(e.message)}</div>`}
}
async function grid(k,page){
  page=page||1;
  setNav(k);root.innerHTML='<div class="empty">Loading…</div>';
  try{
    const d=await api((k==='movies'?'/api/movies':'/api/series')+'?page='+page);
    const total=d.total_pages||1;
    const pager=`<div class="pager">
      <button class="src" type="button" ${page<=1?'disabled':''} onclick="grid('${k}',${page-1})">← Prev</button>
      <span class="page-n">Page ${page} / ${total}</span>
      <button class="src" type="button" ${page>=total?'disabled':''} onclick="grid('${k}',${page+1})">Next →</button>
    </div>`;
    root.innerHTML=`<section class="sec" style="padding-top:6px"><div class="sec-head"><h2>${k==='movies'?'Movies':'Series'}</h2>${pager}</div>
      <div class="grid">${(d.items||[]).map(card).join('')||'<p class="empty">Empty</p>'}</div>
      ${pager}</section>`;
  }catch(e){root.innerHTML=`<div class="empty err">${esc(e.message)}</div>`}
}
async function search(q){
  setNav('');root.innerHTML='<div class="empty">Searching…</div>';
  try{
    const d=await api('/api/search?q='+encodeURIComponent(q));
    const items=d.items||d.tmdb||[];
    root.innerHTML=`<section class="sec"><h2>Results for “${esc(q)}”</h2><div class="grid">${items.map(card).join('')||'<p class="empty">No results</p>'}</div></section>`;
  }catch(e){root.innerHTML=`<div class="empty err">${esc(e.message)}</div>`}
}
function goSearch(){const q=document.getElementById('q').value.trim();if(!q)return;if(location.hash.indexOf('#/music')===0){musicSearch(q);return}location.hash='#/search/'+encodeURIComponent(q)}
async function title(media,id){
  setNav('');root.innerHTML='<div class="empty">Loading…</div>';
  try{
    const d=await api(`/api/detail/${media}/${id}`);
    const poster=d.poster?`background-image:url('${esc(d.poster)}')`:'';
    root.innerHTML=`<div class="detail"><div class="poster" style="${poster}"></div><div>
      <h1>${esc(d.name)}</h1>
      <div class="meta">${esc(d.year||'')} · ${esc(d.type)} ${d.rating?('· ★ '+Number(d.rating).toFixed(1)):''} ${(d.genres||[]).slice(0,4).map(g=>'· '+esc(g)).join('')}</div>
      <p class="overview">${esc(d.overview||'')}</p>
      <button class="btn" onclick="location.hash='#/watch/${media}/${id}'">▶ Play</button>
      ${d.seasons&&d.seasons.length?`<section class="sec"><h2>Seasons</h2><div class="row">${d.seasons.map(s=>`<div class="card" onclick="location.hash='#/watch/tv/${id}?se=${s.season}&ep=1'"><div class="p" style="display:flex;align-items:center;justify-content:center;font-size:1.5rem;color:var(--a);aspect-ratio:1">S${s.season}</div><div class="t">${esc(s.name)} · ${s.episode_count||'?'} ep</div></div>`).join('')}</div></section>`:''}
    </div></div>`;
  }catch(e){root.innerHTML=`<div class="empty err">${esc(e.message)}</div>`}
}
let PS={sources:[],idx:0,se:1,ep:1};
let dashPlayer=null;
let plyrInst=null;
function destroyPlayer(){
  try{ if(dashPlayer){ dashPlayer.reset(); dashPlayer=null; } }catch(e){}
  try{ if(plyrInst){ plyrInst.destroy(); plyrInst=null; } }catch(e){}
}
function isBrowserPlayable(url, format){
  const u=(url||'').toLowerCase();
  const f=(format||'').toLowerCase();
  if(/\.mp4(\?|$)/i.test(u) || f==='mp4') return true;
  if(/\.webm(\?|$)/i.test(u) || f==='webm') return true;
  if(/\.m3u8(\?|$)/i.test(u) || f==='hls') return true;
  if(/\.mpd(\?|$)/i.test(u) || f==='dash') return true;
  // MKV/AVI often fail in HTML5 — still try, fallback panel if error
  return false;
}
function renderP(){
  const s=PS.sources[PS.idx]; const f=document.getElementById('frame');
  if(!s){f.innerHTML='<div class="empty">No sources</div>';return}
  destroyPlayer();
  const play=s.play_url||s.url;
  const st=document.getElementById('st');
  if(st) st.innerHTML=(PS.idx+1)+'/'+PS.sources.length+' · '+esc(s.label)+(s.format?('<span class="fmt-tag">'+esc(s.format)+'</span>'):'');
  document.querySelectorAll('.src[data-i]').forEach((b,i)=>b.classList.toggle('on',i===PS.idx));
  const qsel=document.getElementById('qsel');
  if(qsel){qsel.innerHTML='';qsel.style.display='none'}
  const ext=document.getElementById('extpanel');
  if(ext) ext.innerHTML='';

  if(s.type==='embed'){
    f.innerHTML=`<iframe src="${esc(play)}" allowfullscreen allow="autoplay;encrypted-media;picture-in-picture;fullscreen;clipboard-write" referrerpolicy="origin" style="width:100%;height:100%;border:0;background:#000" loading="eager"></iframe>`;
    return;
  }

  const isDash=/\\.mpd(\\?|$)/i.test(play)||/\\.mpd(\\?|$)/i.test(s.url||'')||(s.format||'').toUpperCase()==='DASH';
  const tryNative=isBrowserPlayable(play,s.format)||isDash||/\\.mkv/i.test(play)||(s.format||'').toUpperCase()==='MKV'||(s.format||'').toUpperCase()==='FILE';

  f.innerHTML='';
  const v=document.createElement('video');
  v.id='vmain'; v.playsInline=true; v.setAttribute('playsinline',''); v.setAttribute('crossorigin','anonymous');
  v.controls=true; v.style.cssText='width:100%;height:100%;background:#000';
  f.appendChild(v);

  const showExt=()=>{
    if(!ext) return;
    ext.innerHTML=`<div class="ext-panel">
      <h4>Advanced playback</h4>
      <p>This file may use codecs (MKV / HEVC / DTS) that browsers block. Use an external player like <b>VLC</b>, <b>mpv</b>, or <b>PotPlayer</b> for full quality — same approach as MovieBox-TUI.</p>
      <div class="ext-actions">
        <a class="primary" href="${esc(play)}" target="_blank" rel="noopener">Open / Download file</a>
        <button type="button" class="primary" onclick="navigator.clipboard.writeText('${esc(play).replace(/'/g,"\\'")}').then(()=>toast('Link copied — paste into VLC → Open Network Stream'))">Copy stream URL</button>
        <button type="button" onclick="window.__nx()">Try next source</button>
      </div>
    </div>`;
  };

  if(isDash && window.dashjs){
    try{
      dashPlayer=dashjs.MediaPlayer().create();
      dashPlayer.updateSettings({streaming:{abr:{autoSwitchBitrate:{video:true}},buffer:{fastSwitchEnabled:true}}});
      dashPlayer.initialize(v, play, true);
      if(window.Plyr){ try{ plyrInst=new Plyr(v,{controls:['play-large','play','progress','current-time','duration','mute','volume','settings','fullscreen'],settings:['quality','speed']}); }catch(e){} }
      dashPlayer.on(dashjs.MediaPlayer.events.ERROR,()=>{toast('DASH error');showExt();});
      dashPlayer.on(dashjs.MediaPlayer.events.STREAM_INITIALIZED,()=>{
        try{
          const bitrates=dashPlayer.getBitrateInfoListFor('video')||[];
          if(qsel&&bitrates.length){
            qsel.style.display='inline-block';
            qsel.innerHTML='<option value="auto">Auto</option>'+bitrates.map((b,i)=>`<option value="${i}">${b.height||'?'}p</option>`).join('');
            qsel.onchange=()=>{const val=qsel.value;if(val==='auto')dashPlayer.updateSettings({streaming:{abr:{autoSwitchBitrate:{video:true}}}});else{dashPlayer.updateSettings({streaming:{abr:{autoSwitchBitrate:{video:false}}}});dashPlayer.setQualityFor('video',parseInt(val,10))}};
          }
        }catch(e){}
      });
    }catch(e){showExt();}
  }else{
    v.src=play;
    if(window.Plyr){
      try{
        plyrInst=new Plyr(v,{
          controls:['play-large','play','progress','current-time','duration','mute','volume','settings','pip','fullscreen'],
          settings:['speed'],
          keyboard:{focused:true,global:true},
          tooltips:{controls:true,seek:true},
          autoplay:true
        });
      }catch(e){}
    }else{
      v.autoplay=true;
    }
    let erred=false;
    v.addEventListener('error',()=>{if(erred)return;erred=true;toast('Browser cannot decode this file');showExt();});
    // If MKV and no progress after 4s with readyState low
    if(/\\.mkv/i.test(play)||(s.format||'').toUpperCase()==='MKV'){
      setTimeout(()=>{
        try{
          if(v.readyState<2 && v.videoWidth===0){ showExt(); }
        }catch(e){}
      },4000);
    }
  }
}
window.__nx=
()=>{if(PS.idx<PS.sources.length-1){PS.idx++;toast('Next source…');renderP()}else toast('All sources failed')};

async function watch(media,id,se,ep){
  setNav('');
  document.body.classList.add('watching');
  const nbar=document.getElementById('nowbar'); if(nbar) nbar.classList.remove('on');
  PS={sources:[],idx:0,se:se||1,ep:ep||1};
  root.innerHTML=`<div class="player-shell"><div class="player-wrap"><div id="frame" class="empty">Loading stream…</div></div><div id="extpanel"></div></div>
    <div class="bar"><span id="st">Fetching sources…</span>
    <select id="qsel" class="se-select" style="display:none"></select>
    <button class="src" type="button" onclick="window.__nx()">Next source ↻</button>
    <a class="src" href="#/title/${media}/${id}">Details</a></div>
    <div id="srcs"></div><div id="hublist"></div><div id="eps"></div>`;
  try{
    let url=`/api/play?tmdb_id=${encodeURIComponent(id)}&media=${media}&fast=1`;
    if(media==='tv')url+=`&se=${PS.se}&ep=${PS.ep}`;
    const ctrl=new AbortController();
    const to=setTimeout(()=>ctrl.abort(),25000);
    let d;
    try{
      const r=await fetch(url,{signal:ctrl.signal});
      clearTimeout(to);
      if(!r.ok) throw new Error('Play API '+r.status);
      d=await r.json();
    }catch(e){
      clearTimeout(to);
      throw e;
    }
    PS.sources=d.sources||[];
    if(!PS.sources.length){
      document.getElementById('frame').innerHTML=`<div class="empty err">No sources found${d.errors?(' · '+esc(JSON.stringify(d.errors))):''}</div>`;
      return;
    }
    document.getElementById('srcs').innerHTML=
      '<div style="font-size:.75rem;color:var(--mute);margin:4px 0 6px">Sources</div>'+
      PS.sources.map((s,i)=>`<button type="button" class="src ${i===0?'on':''}" data-i="${i}" onclick="PS.idx=${i};renderP()">${esc(s.label)}</button>`).join('');
    const hubEl=document.getElementById('hublist');
    if(hubEl){
      hubEl.innerHTML=`<div class="hubbox"><button type="button" class="src" onclick="loadHubs('${esc(media)}','${esc(id)}',${PS.se},${PS.ep})">Load 4K / Download mirrors…</button></div>`;
    }
    renderP();
  }catch(e){
    const msg=e.name==='AbortError'?'Timed out — try again':(e.message||String(e));
    const fr=document.getElementById('frame');
    if(fr) fr.innerHTML=`<div class="empty err">${esc(msg)}</div>`;
    toast(msg);
  }
  if(media==='tv'){
    try{
      const sd=await api(`/api/tv/${id}/season/${PS.se}`);
      const epsList=(sd.episodes||[]).slice().sort((a,b)=>(a.episode||0)-(b.episode||0));
      const epEl=document.getElementById('eps');
      if(epEl) epEl.innerHTML=`<div style="margin-top:12px"><div style="font-size:.8rem;color:var(--mute);margin-bottom:6px">Episodes · S${PS.se}</div><div style="display:flex;flex-wrap:wrap;gap:6px">${epsList.map(e=>`<button type="button" class="ep ${e.episode==PS.ep?'on':''}" onclick="location.hash='#/watch/tv/${id}?se=${PS.se}&ep=${e.episode}'">E${e.episode}</button>`).join('')}</div></div>`;
    }catch(e){}
  }
}
async function loadHubs(media,id,se,ep){
  const hubEl=document.getElementById('hublist');
  if(hubEl) hubEl.innerHTML='<div class="empty">Loading 4K mirrors…</div>';
  try{
    let url=`/api/play?tmdb_id=${encodeURIComponent(id)}&media=${media}&fast=0`;
    if(media==='tv')url+=`&se=${se}&ep=${ep}`;
    const d=await api(url);
    const hubs=(d.sources||[]).map((s,i)=>({s,i})).filter(x=>x.s.provider==='4khdhub');
    // merge new hub sources
    const base=PS.sources.filter(s=>s.provider!=='4khdhub');
    const hubOnly=(d.sources||[]).filter(s=>s.provider==='4khdhub');
    PS.sources=base.concat(hubOnly);
    // refresh source buttons
    const srcs=document.getElementById('srcs');
    if(srcs) srcs.innerHTML='<div style="font-size:.75rem;color:var(--mute);margin:4px 0 6px">Sources</div>'+PS.sources.map((s,i)=>`<button type="button" class="src" data-i="${i}" onclick="PS.idx=${i};renderP()">${esc(s.label)}</button>`).join('');
    let html='';
    if(hubOnly.length){
      html+=`<div class="hubbox"><h3>4K / Hub</h3>${hubOnly.map((s)=>{
        const i=PS.sources.indexOf(s);
        return `<button type="button" class="src" onclick="PS.idx=${i};renderP()">▶ ${esc(s.label)}</button>`;
      }).join('')}</div>`;
    }
    const dls=d.downloads||[];
    if(dls.length){
      html+=`<div class="dlbox"><h3>Download</h3><div class="dl-grid">${dls.map(s=>{
        const u=s.url||'';
        return `<a class="dl-card" href="${esc(u)}" target="_blank" rel="noopener" download><div class="dl-ico">⬇</div><div class="dl-meta"><div class="dl-title">${esc(s.label||'File')}</div><div class="dl-sub">${esc(s.format||'')}</div></div></a>`;
      }).join('')}</div></div>`;
    }
    if(!html) html='<div class="empty">No 4K mirrors</div>';
    if(hubEl) hubEl.innerHTML=html;
  }catch(e){if(hubEl) hubEl.innerHTML=`<div class="empty err">${esc(e.message)}</div>`}
}

async function apiDocs(){
  setNav('api');
  const cards=[
    {g:'🌐 Web',e:'GET /',d:'This website (SPA). Hash routes: #/movies #/series #/music #/anime #/dl #/watch/...',h:'Open the site root. Use the sidebar to navigate.'},
    {g:'🏠 Catalog',e:'GET /api/home',d:'Trending & popular movies/series from TMDB.',h:'Browser calls this on Home. You can open /api/home in a new tab to see JSON.'},
    {g:'🔍 Search',e:'GET /api/search?q=avatar',d:'Search movies & TV on TMDB.',h:'Type in the top search box (not on Music) or call the URL with your query.'},
    {g:'▶️ Play',e:'GET /api/play?tmdb_id=19995&media=movie&fast=1',d:'Embed players (VidSrc, VidLink, …). fast=1 = quick embeds only.',h:'Click Play on any title. Use Next source if one embed fails. fast=0 also loads 4K mirrors.'},
    {g:'🎵 Music',e:'GET /music/search · /music/yt/search · /music/yt/trending · /music/yt/play/{id}',d:'Saavn + YouTube (Invidious) streams, trending, lyrics.',h:'Music tab: chips + search. YT ids play via Invidious audio.'},
    {g:'🎌 Anime',e:'GET /anime/home · /anime/search · /anime/detail/{slug} · /anime/stream?hash=',d:'HindiAnime Hindi-dub catalog + HLS resolve.',h:'Anime tab → pick series → episode → play.'},
    {g:'⬇ Downloader',e:'GET /dl/info?url=https://...',d:'yt-dlp probe (OmniGet-style). Lists formats with direct links.',h:'Paste a YouTube/TikTok/… link. Prefer rows marked 🔊 VIDEO+AUDIO so sound is included when you Copy/Save.'},
    {g:'🎧 Audio only',e:'GET /dl/audio?url=...',d:'Picks the best audio stream for a page URL.',h:'Use when you only need music/speech from a video page.'},
    {g:'📦 4K / Hub',e:'GET /fk/search?q= · /tools/resolve?url=',d:'4KHDHub search + HubCloud direct resolve.',h:'On a movie page, tap Load 4K mirrors, or call these APIs from /docs.'},
    {g:'🎬 MovieBox API',e:'GET /mb/search?q= · /mb/stream/{id}',d:'Mobile MovieBox BFF (HMAC). Streams may need cookies (use proxy).',h:'For API clients only — not used by the main web player. See /docs.'},
    {g:'❤️ Health',e:'GET /health',d:'Version + enabled providers.',h:'Check deploy is alive after update.'},
    {g:'📖 Swagger',e:'GET /docs',d:'Interactive OpenAPI — try every endpoint in the browser.',h:'Best for beginners to experiment with parameters.'},
  ];
  root.innerHTML=`<div class="api-guide">
    <div class="dl-hero"><h1>API guide</h1><p>Beginner-friendly map of StreamHub · click a path in Swagger to try live</p>
      <a class="btn" href="/docs" target="_blank">Open Swagger →</a>
    </div>
    ${cards.map(c=>`<div class="api-card"><h3><span>${c.g}</span></h3>
      <div class="ep">${esc(c.e)}</div>
      <p>${esc(c.d)}</p>
      <div class="how"><b>How to use:</b> ${esc(c.h)}</div>
    </div>`).join('')}
  </div>`;
}
async function router(){
  closeQueue();
  if((location.hash||'').indexOf('#/watch')!==0) document.body.classList.remove('watching');
  const h=location.hash.slice(1)||'/';const [path,qs]=h.split('?');
  const params=Object.fromEntries(new URLSearchParams(qs||''));
  const p=path.split('/').filter(Boolean);
  try{
    if(!p.length) return home();
    if(p[0]==='movies') return grid('movies');
    if(p[0]==='music'&&p[1]==='play'&&p[2]) return musicPlayPage(p.slice(2).join('/'));
    if(p[0]==='anime') return animeHome();
    if(p[0]==='music') return musicHome();
    if(p[0]==='dl') return downloaderHome();
    if(p[0]==='series') return grid('series');
    if(p[0]==='search'&&p[1]) return search(decodeURIComponent(p[1]));
    if(p[0]==='title'&&p[1]&&p[2]) return title(p[1],p[2]);
    if(p[0]==='watch'&&p[1]&&p[2]) return watch(p[1],p[2],parseInt(params.se||'1',10),parseInt(params.ep||'1',10));
    if(p[0]==='api') return apiDocs();
    root.innerHTML='<div class="empty">Not found</div>';
  }catch(e){root.innerHTML=`<div class="empty err">${esc(e.message)}</div>`}
}
window.addEventListener('hashchange',router);
router();
</script>
<div id="nowbar" class="now-bar">
  <div class="now-fill" id="nowfill"></div>
  <audio id="g-audio" preload="metadata"></audio>
  <img id="nowthumb" alt="" onclick="if(MSTATE.id)location.hash='#/music/play/'+encodeURIComponent(MSTATE.id)"/>
  <div class="now-meta" onclick="if(MSTATE.id)location.hash='#/music/play/'+encodeURIComponent(MSTATE.id)"><div class="t" id="nowtitle">—</div><div class="a" id="nowartist"></div></div>
  <div class="now-player" id="nowplayer"></div>
  <div class="now-actions">
    <button class="src" type="button" id="nb-toggle" onclick="nbToggle()" title="Play/Pause">▶</button>
    <button class="src" type="button" onclick="if(MSTATE.id)location.hash='#/music/play/'+encodeURIComponent(MSTATE.id)">Player</button>
    <button class="src" type="button" onclick="closeMusic()">✕</button>
  </div>
</div>
<div id="toast"></div>
</body></html>
"""



@app.get("/site", response_class=HTMLResponse, tags=["Meta"])
async def site_spa():
    return HTMLResponse(SPA_HTML)


# Serve full app at root too
@app.get("/", response_class=HTMLResponse, tags=["Meta"], include_in_schema=False)
async def root_spa():
    return HTMLResponse(SPA_HTML)

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("api:app", host="0.0.0.0", port=port)
