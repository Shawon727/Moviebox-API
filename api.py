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
        "**MovieBox** (`/mb/*`) — search, metadata, DASH/MP4 streams\n"
        "**4KHDHub** (`/fk/*`) — search, releases, HubCloud resolve\n"
        "**Tools** (`/tools/*`) — HubCloud/HubDrive direct-link resolver"
    ),
    version="5.0.0",
    docs_url="/docs",
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


def _pixeldrain_api(url: str) -> Optional[str]:
    try:
        p = urlparse(url)
        if "pixeldrain." not in (p.hostname or ""):
            return None
        path = p.path
        if path.startswith("/u/"):
            fid = path[3:].strip("/")
        elif path.startswith("/api/file/"):
            fid = path[len("/api/file/") :].strip("/")
        else:
            return None
        if not fid or not re.match(r"^[\w\-]+$", fid):
            return None
        host = p.hostname
        return f"https://{host}/api/file/{fid}?download"
    except Exception:
        return None

def _pixeldrain_bypass_urls(api_url: str) -> List[str]:
    """Alternate hosts that often work when pixeldrain rate-limits hotlinking."""
    urls = [api_url]
    try:
        p = urlparse(api_url)
        fid = p.path.split("/api/file/")[-1].split("?")[0].strip("/")
        if fid:
            # common community bypass / mirror patterns
            urls.append(f"https://pixeldrain.com/api/file/{fid}?download")
            urls.append(f"https://pixeldrain.dev/api/file/{fid}?download")
    except Exception:
        pass
    # dedupe
    out, seen = [], set()
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


async def preflight_url(url: str, headers: Optional[dict] = None) -> Optional[str]:
    """Range probe — only accept non-HTML binary responses (TUI-style)."""
    h = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
        "Range": "bytes=0-2047",
        "Accept": "*/*",
    }
    if headers:
        h.update({k: v for k, v in headers.items() if k.lower() != "range"})
    try:
        async with httpx.AsyncClient(follow_redirects=True, timeout=12.0) as client:
            r = await client.get(url, headers=h)
            if r.status_code not in (200, 206):
                return None
            if len(r.content) < 64:
                return None
            ctype = (r.headers.get("content-type") or "").lower()
            if "text/html" in ctype or "text/plain" in ctype and len(r.content) < 500:
                return None
            # reject if body looks like HTML
            head = r.content[:200].lstrip().lower()
            if head.startswith(b"<!doctype") or head.startswith(b"<html"):
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
            "pixeldrain.", "workers.dev", "r2.dev", "cloudflarestorage",
            "googleusercontent.com", "storage.googleapis.com", "googleapis.com",
            "gofile.", "workupload.", "streamtape.", "pixel.",
            "download.", "cdn.", "hubcloud.fans", "hubcloud.cx",
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



async def _resolve_masked_hub(url: str) -> List[dict]:
    """Follow intermediate download pages to HubCloud /drive/ then resolve."""
    headers = {"User-Agent": FK_UA, "Referer": url}
    async with httpx.AsyncClient(follow_redirects=True, timeout=20.0, headers=headers) as client:
        r = await client.get(url)
        text = r.text or ""
        final = str(r.url)
        candidates = []
        pattern = r"https?://[^\s\"'<>]+"
        for u in re.findall(pattern, text + " " + final):
            if "hubcloud." in u and "/drive/" in u:
                candidates.append(u.split("&")[0].rstrip(").,;"))
            if "hubdrive." in u:
                candidates.append(u.split("&")[0].rstrip(").,;"))
        out = []
        for c in candidates[:3]:
            try:
                out.extend(await resolve_any(c))
            except Exception:
                continue
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
    return {"ok": True, "version": "5.0.0", "providers": ["tmdb", "videasy", "vidsrc", "vidking", "4khdhub", "hubcloud", "moviebox-api"]}


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
    """Stream embeds inspired by streambert / Flick-style players (TMDB id)."""
    sources = []
    tmdb = meta.get("tmdb")
    imdb = meta.get("imdb")
    se = max(1, int(se or 1))
    ep = max(1, int(ep or 1))
    pairs = []
    if media == "movie":
        if tmdb:
            pairs = [
                ("Videasy", f"https://player.videasy.to/movie/{tmdb}?overlay=true"),
                ("VidSrc", f"https://vsembed.su/embed/movie/{tmdb}"),
                ("Vidking", f"https://www.vidking.net/embed/movie/{tmdb}?autoPlay=true"),
                ("VidLink", f"https://vidlink.pro/movie/{tmdb}"),
            ]
        elif imdb:
            pairs = [
                ("VidSrc IMDB", f"https://vsembed.su/embed/movie/{imdb}"),
                ("Vidking IMDB", f"https://www.vidking.net/embed/movie/{imdb}?autoPlay=true"),
            ]
    else:
        if tmdb:
            pairs = [
                ("Videasy", f"https://player.videasy.to/tv/{tmdb}/{se}/{ep}?overlay=true"),
                ("VidSrc", f"https://vsembed.su/embed/tv/{tmdb}/{se}/{ep}"),
                ("Vidking", f"https://www.vidking.net/embed/tv/{tmdb}/{se}/{ep}?autoPlay=true"),
                ("VidLink", f"https://vidlink.pro/tv/{tmdb}/{se}/{ep}"),
            ]
        elif imdb:
            pairs = [
                ("VidSrc IMDB", f"https://vsembed.su/embed/tv/{imdb}/{se}/{ep}"),
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




if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("api:app", host="0.0.0.0", port=port, reload=True)


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
    """TMDB homepage rows (Flick/streambert style catalog)."""
    trending_m = trending_t = popular_m = popular_t = top_m = []
    try:
        d = await _tmdb_get("/trending/movie/week")
        trending_m = [c for x in (d.get("results") or []) if (c := _tmdb_card(x, "movie"))]
    except Exception:
        pass
    try:
        d = await _tmdb_get("/trending/tv/week")
        trending_t = [c for x in (d.get("results") or []) if (c := _tmdb_card(x, "tv"))]
    except Exception:
        pass
    try:
        d = await _tmdb_get("/movie/popular")
        popular_m = [c for x in (d.get("results") or []) if (c := _tmdb_card(x, "movie"))]
    except Exception:
        pass
    try:
        d = await _tmdb_get("/tv/popular")
        popular_t = [c for x in (d.get("results") or []) if (c := _tmdb_card(x, "tv"))]
    except Exception:
        pass
    try:
        d = await _tmdb_get("/movie/top_rated")
        top_m = [c for x in (d.get("results") or []) if (c := _tmdb_card(x, "movie"))]
    except Exception:
        pass
    return {
        "trending_movies": trending_m[:18],
        "trending_series": trending_t[:18],
        "popular_movies": popular_m[:18],
        "popular_series": popular_t[:18],
        "top_movies": top_m[:18],
        "provider": "tmdb",
    }



@app.get("/api/movies", tags=["Catalog"])
async def api_movies(page: int = 1):
    d = await _tmdb_get("/movie/popular", {"page": page})
    items = [c for x in (d.get("results") or []) if (c := _tmdb_card(x, "movie"))]
    return {"items": items, "page": page, "provider": "tmdb"}



@app.get("/api/series", tags=["Catalog"])
async def api_series(page: int = 1):
    d = await _tmdb_get("/tv/popular", {"page": page})
    items = [c for x in (d.get("results") or []) if (c := _tmdb_card(x, "tv"))]
    return {"items": items, "page": page, "provider": "tmdb"}



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
):
    """Web playback: streambert-style embeds + 4KHDHub/HubCloud list. MovieBox is NOT used here — use /mb/stream."""
    sources = []
    errors = {}
    media = "tv" if media in ("tv", "series", "show") else "movie"
    title = (q or "").strip()
    tid = (tmdb_id or "").strip() or None
    # subject_id from old UI may actually be tmdb id when provider=tmdb
    if not tid and subject_id and str(subject_id).isdigit() and len(str(subject_id)) < 12:
        tid = str(subject_id)

    meta = None
    if tid:
        det = await _tmdb_get(f"/{media}/{tid}")
        if det.get("id"):
            title = title or det.get("title") or det.get("name") or ""
            meta = {"tmdb": str(det["id"]), "imdb": (det.get("external_ids") or {}).get("imdb_id"), "name": title}
            if not meta.get("imdb"):
                ext = await _tmdb_get(f"/{media}/{tid}/external_ids")
                meta["imdb"] = ext.get("imdb_id")
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

    # 4KHDHub / HubCloud direct mirrors (list under player)
    hub_links: List[dict] = []
    clean = re.sub(r"\[[^\]]*\]", " ", title or "")
    clean = re.sub(r"\([^)]*\)", " ", clean)
    clean = re.sub(r"\s+", " ", clean).strip()
    if clean:
        try:
            html = await fk_fetch(f"?s={clean}")
            hits = fk_parse_search(html)
            def tscore(name: str) -> int:
                n = (name or "").lower(); c = clean.lower()
                if n == c or n.startswith(c): return 0
                if c.split()[0] in n: return 1
                if c in n: return 2
                return 9
            hits = sorted(hits, key=lambda x: tscore(x.get("name") or ""))
            if hits and tscore(hits[0].get("name") or "") <= 2:
                path_id = hits[0].get("id")
                if path_id:
                    page = await fk_fetch(path_id)
                    releases = fk_parse_releases(page, se_use if media == "tv" else 0, ep_use if media == "tv" else 0)
                    for rel in releases[:10]:
                        if len(hub_links) >= 10:
                            break
                        for mir in rel.get("mirrors") or []:
                            if len(hub_links) >= 10:
                                break
                            murl = mir.get("url") or ""
                            # HubCloud often masked behind greenmotors / short redirects
                            if not any(x in murl for x in ("hubcloud.", "hubdrive.", "greenmotors.", "gamerxyt.", "/drive/")):
                                if not mir.get("needs_resolve"):
                                    continue
                            try:
                                if "hubcloud." in murl or "/drive/" in murl:
                                    links = await resolve_any(murl)
                                elif "hubdrive." in murl:
                                    links = await resolve_any(murl)
                                else:
                                    # follow redirect page to find hubcloud
                                    links = await _resolve_masked_hub(murl)
                            except Exception:
                                continue
                            for L in links:
                                u = L.get("url") or ""
                                if not u or not _is_playable_direct(u):
                                    continue
                                candidates = _pixeldrain_bypass_urls(u) if "pixeldrain." in u else [u]
                                ok = None
                                hdrs = L.get("headers") or {"User-Agent": FK_UA, "Referer": murl}
                                for cand in candidates:
                                    ok = await preflight_url(cand, hdrs)
                                    if ok and _is_playable_direct(ok):
                                        u = ok
                                        break
                                    ok = None
                                if not ok:
                                    continue
                                low = u.lower()
                                fmt = "MP4" if ".mp4" in low else ("MKV" if ".mkv" in low else "FILE")
                                hub_links.append({
                                    "label": f"{rel.get('quality') or '?'} · {(L.get('label') or 'CDN')[:40]} · {fmt}",
                                    "filename": (rel.get("filename") or "")[:120],
                                    "url": u,
                                    "play_url": u,
                                    "type": "direct",
                                    "format": fmt,
                                    "provider": "4khdhub",
                                    "headers": hdrs,
                                })
        except Exception as e:
            errors["4khdhub"] = str(e)

    def _hub_rank(s):
        u = (s.get("url") or "").lower()
        lab = (s.get("label") or "").lower()
        fmt = (s.get("format") or "").lower()
        if "pixeldrain" in u or "pixeldrain" in lab: return 0
        if ".mp4" in u or fmt == "mp4": return 1
        if "workers.dev" in u: return 2
        if ".mkv" in u or fmt == "mkv": return 4
        return 3
    hub_links.sort(key=_hub_rank)

    hub_src = [{
        "provider": "4khdhub",
        "label": h.get("label") or "Hub",
        "url": h["url"],
        "play_url": h["url"],
        "format": h.get("format") or "FILE",
        "type": "direct",
        "headers": h.get("headers") or {},
    } for h in hub_links]

    # Web order: embeds first (reliable A/V in browser), then hub files
    uniq = []
    seen = set()
    for s in sources + hub_src:
        u = s.get("url")
        if not u or u in seen:
            continue
        seen.add(u)
        uniq.append(s)

    return {
        "tmdb_id": tid,
        "media": media,
        "se": se_use,
        "ep": ep_use,
        "title": title,
        "count": len(uniq),
        "sources": uniq,
        "hub_links": hub_links,
        "errors": errors or None,
        "strategy": "videasy/vidsrc/vidking embeds → 4khdhub/hubcloud/pixeldrain",
        "note": "MovieBox streams are available via /mb/stream API only (not on web UI).",
    }





SPA_HTML = r"""<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8"/><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"/>
<title>StreamHub</title>
<link rel="preconnect" href="https://fonts.googleapis.com"/>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet"/>
<script src="https://cdn.dashjs.org/latest/dash.all.min.js"></script>
<style>
:root{
  --bg:#0e0e10;--bg2:#16161a;--card:#1c1c22;--line:#2a2a32;
  --text:#eeeef0;--mute:#9a9aa3;--a:#00a4dc;--a2:#0b6e99;--danger:#e25555;
  --rad:10px;--nav:64px;--side:220px;
}
*{box-sizing:border-box;margin:0;padding:0}
html,body{height:100%;background:var(--bg);color:var(--text);font-family:Inter,system-ui,sans-serif}
a{color:inherit;text-decoration:none}
button,input,select{font:inherit;color:inherit}
img{display:block;max-width:100%}
.app{display:flex;min-height:100%}
.side{width:var(--side);background:var(--bg2);border-right:1px solid var(--line);padding:18px 12px;position:sticky;top:0;height:100vh;flex-shrink:0;display:flex;flex-direction:column;gap:6px;z-index:20}
.brand{font-weight:700;font-size:1.15rem;padding:8px 12px 18px;letter-spacing:-.02em}
.brand span{color:var(--a)}
.nav a,.nav button{display:flex;align-items:center;gap:10px;width:100%;padding:11px 12px;border-radius:8px;border:0;background:transparent;color:var(--mute);cursor:pointer;text-align:left}
.nav a.on,.nav button.on,.nav a:hover,.nav button:hover{background:rgba(0,164,220,.12);color:var(--text)}
.main{flex:1;min-width:0;display:flex;flex-direction:column}
.top{height:var(--nav);display:flex;align-items:center;gap:12px;padding:0 18px;border-bottom:1px solid var(--line);background:rgba(14,14,16,.85);backdrop-filter:blur(10px);position:sticky;top:0;z-index:15}
.search{flex:1;max-width:420px;background:var(--card);border:1px solid var(--line);border-radius:999px;padding:10px 16px;outline:none}
.search:focus{border-color:var(--a)}
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
</style></head>
<body>
<div class="app">
  <aside class="side" id="side">
    <div class="brand">Stream<span>Hub</span></div>
    <nav class="nav">
      <a href="#/" id="n-home">Home</a>
      <a href="#/movies" id="n-movies">Movies</a>
      <a href="#/series" id="n-series">Series</a>
      <a href="#/api" id="n-api">API Docs</a>
    </nav>
    <div style="flex:1"></div>
    <div style="font-size:.72rem;color:var(--mute);padding:8px 12px;line-height:1.4">TMDB · Videasy · VidSrc · 4KHDHub<br/>MovieBox API → /mb/*</div>
  </aside>
  <div class="main">
    <div class="top">
      <button class="menu-btn" type="button" onclick="document.getElementById('side').classList.toggle('open')">☰</button>
      <input class="search" id="q" placeholder="Search movies & series…" onkeydown="if(event.key==='Enter')goSearch()"/>
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
function setNav(k){['home','movies','series','api'].forEach(id=>{const el=document.getElementById('n-'+id);if(el)el.classList.toggle('on',id===k)});document.getElementById('side').classList.remove('open')}
function card(it){
  const media=it.type==='tv'?'tv':'movie';
  const id=it.tmdb_id||it.id;
  const poster=it.poster?`background-image:url('${esc(it.poster)}')`:'';
  return `<div class="card" onclick="location.hash='#/title/${media}/${id}'"><div class="p" style="${poster}"></div><div class="t">${esc(it.name)}</div><div class="y">${esc(it.year||'')}${it.rating?(' · ★ '+Number(it.rating).toFixed(1)):''}</div></div>`;
}
function row(title,items){if(!items||!items.length)return'';return `<section class="sec"><h2>${esc(title)}</h2><div class="row">${items.map(card).join('')}</div></section>`}
async function home(){
  setNav('home');root.innerHTML='<div class="empty">Loading…</div>';
  try{
    const d=await api('/api/home');
    const hero=(d.trending_movies&&d.trending_movies[0])||(d.trending_series&&d.trending_series[0]);
    let h='';
    if(hero){
      const media=hero.type==='tv'?'tv':'movie';
      const bg=hero.backdrop||hero.poster||'';
      h=`<div class="hero">${bg?`<img src="${esc(bg)}" alt=""/>`:''}<div class="hbody"><h1>${esc(hero.name)}</h1><p>${esc(hero.overview||'')}</p><div style="margin-top:12px"><button class="btn" onclick="location.hash='#/watch/${media}/${hero.tmdb_id||hero.id}'">▶ Play</button>
      <button class="btn ghost" style="margin-left:8px" onclick="location.hash='#/title/${media}/${hero.tmdb_id||hero.id}'">Details</button></div></div></div>`;
    }
    root.innerHTML=h+row('Trending Movies',d.trending_movies)+row('Trending Series',d.trending_series)+row('Popular Movies',d.popular_movies)+row('Popular Series',d.popular_series)+row('Top Rated',d.top_movies);
  }catch(e){root.innerHTML=`<div class="empty err">${esc(e.message)}</div>`}
}
async function grid(k){
  setNav(k);root.innerHTML='<div class="empty">Loading…</div>';
  try{
    const d=await api(k==='movies'?'/api/movies':'/api/series');
    root.innerHTML=`<section class="sec" style="padding-top:6px"><h2>${k==='movies'?'Movies':'Series'}</h2><div class="grid">${(d.items||[]).map(card).join('')||'<p class="empty">Empty</p>'}</div></section>`;
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
function goSearch(){const q=document.getElementById('q').value.trim();if(q)location.hash='#/search/'+encodeURIComponent(q)}
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
function destroyPlayer(){try{if(dashPlayer){dashPlayer.reset();dashPlayer=null}}catch(e){}}
function renderP(){
  const s=PS.sources[PS.idx];const f=document.getElementById('frame');
  if(!s){f.innerHTML='<div class="empty">No sources</div>';return}
  destroyPlayer();
  const play=s.play_url||s.url;
  const st=document.getElementById('st');
  if(st) st.textContent=(PS.idx+1)+'/'+PS.sources.length+' · '+s.label;
  document.querySelectorAll('.src[data-i]').forEach((b,i)=>b.classList.toggle('on',i===PS.idx));
  const qsel=document.getElementById('qsel');
  if(qsel){qsel.innerHTML='';qsel.style.display='none'}
  if(s.type==='embed'){
    f.innerHTML=`<iframe src="${esc(play)}" allowfullscreen allow="autoplay;encrypted-media;picture-in-picture" style="width:100%;height:100%;border:0;background:#000"></iframe>`;
    return;
  }
  const isDash=/\\.mpd(\\?|$)/i.test(s.url||'')||/\\.mpd(\\?|$)/i.test(play)||(s.format||'').toUpperCase()==='DASH';
  f.innerHTML='';
  const v=document.createElement('video');
  v.controls=true;v.autoplay=true;v.playsInline=true;v.setAttribute('playsinline','');
  v.style.cssText='width:100%;height:100%;background:#000';
  f.appendChild(v);
  if(isDash&&window.dashjs){
    try{
      dashPlayer=dashjs.MediaPlayer().create();
      dashPlayer.initialize(v,play,true);
      dashPlayer.on(dashjs.MediaPlayer.events.ERROR,()=>{toast('Stream error — next');setTimeout(()=>window.__nx&&window.__nx(),800)});
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
    }catch(e){window.__nx&&window.__nx()}
  }else{
    v.src=play;
    v.onerror=()=>{window.__nx&&window.__nx()};
  }
}
window.__nx=()=>{if(PS.idx<PS.sources.length-1){PS.idx++;toast('Next source…');renderP()}else toast('All sources failed')};
async function watch(media,id,se,ep){
  setNav('');PS={sources:[],idx:0,se:se||1,ep:ep||1};
  root.innerHTML=`<div class="player-wrap"><div id="frame" class="empty">Loading stream…</div></div>
    <div class="bar"><span id="st">…</span>
    <select id="qsel" class="se-select" style="display:none"></select>
    <button class="src" type="button" onclick="window.__nx()">Next source ↻</button>
    <a class="src" href="#/title/${media}/${id}">Details</a></div>
    <div id="srcs"></div><div id="hublist"></div><div id="eps"></div>`;
  try{
    let url=`/api/play?tmdb_id=${encodeURIComponent(id)}&media=${media}`;
    if(media==='tv')url+=`&se=${PS.se}&ep=${PS.ep}`;
    const d=await api(url);
    PS.sources=d.sources||[];
    document.getElementById('srcs').innerHTML=
      (PS.sources.length?('<div style="font-size:.75rem;color:var(--mute);margin:4px 0 6px">Sources</div>'):'')+
      PS.sources.map((s,i)=>`<button type="button" class="src ${i===0?'on':''}" data-i="${i}" onclick="PS.idx=${i};renderP()">${esc(s.label)}</button>`).join('')
      ||'<span class="err">No streams</span>';
    const hubs=PS.sources.map((s,i)=>({s,i})).filter(x=>x.s.provider==='4khdhub');
    const hubEl=document.getElementById('hublist');
    if(hubEl){
      hubEl.innerHTML=hubs.length
        ? `<div class="hubbox"><h3>4K / HubCloud / Pixeldrain (working links)</h3>${hubs.map(({s,i})=>`<button type="button" class="src" onclick="PS.idx=${i};renderP()">▶ ${esc(s.label)}</button>`).join('')}</div>`
        : '';
    }
    if(d.note) toast(d.note,3500);
    renderP();
  }catch(e){document.getElementById('frame').innerHTML=`<div class="empty err">${esc(e.message)}</div>`}
  if(media==='tv'){
    try{
      const sd=await api(`/api/tv/${id}/season/${PS.se}`);
      const epsList=(sd.episodes||[]).slice().sort((a,b)=>(a.episode||0)-(b.episode||0));
      document.getElementById('eps').innerHTML=`<div style="margin-top:12px"><div style="font-size:.8rem;color:var(--mute);margin-bottom:6px">Episodes · S${PS.se}</div><div style="display:flex;flex-wrap:wrap;gap:6px">${epsList.map(e=>`<button type="button" class="ep ${e.episode==PS.ep?'on':''}" onclick="location.hash='#/watch/tv/${id}?se=${PS.se}&ep=${e.episode}'">E${e.episode}</button>`).join('')}</div></div>`;
    }catch{}
  }
}
async function apiDocs(){
  setNav('api');
  root.innerHTML=`<section class="sec"><h2>API reference</h2>
  <p style="color:var(--mute);margin-bottom:14px;line-height:1.5">Web UI uses <b>TMDB + Videasy/VidSrc/Vidking + 4KHDHub</b>. MovieBox remains available as a pure API under <code>/mb/*</code>.</p>
  <div class="grid" style="grid-template-columns:1fr;gap:10px;font-size:.88rem">
    <div class="src" style="cursor:default"><b>GET /api/home</b> — TMDB trending rows</div>
    <div class="src" style="cursor:default"><b>GET /api/search?q=</b> — TMDB multi-search</div>
    <div class="src" style="cursor:default"><b>GET /api/detail/{movie|tv}/{tmdb_id}</b> — details + seasons</div>
    <div class="src" style="cursor:default"><b>GET /api/play?tmdb_id=&media=&se=&ep=</b> — embeds + 4K list</div>
    <div class="src" style="cursor:default"><b>GET /mb/search?q=</b> — MovieBox search (API only)</div>
    <div class="src" style="cursor:default"><b>GET /mb/stream/{subject_id}?se=&ep=</b> — MovieBox DASH + cookies</div>
    <div class="src" style="cursor:default"><b>GET /fk/search?q=</b> · <b>/fk/stream</b> — 4KHDHub</div>
    <div class="src" style="cursor:default"><b>GET /tools/resolve?url=</b> — HubCloud → direct</div>
    <div class="src" style="cursor:default"><b>GET /docs</b> — OpenAPI Swagger</div>
  </div></section>`;
}
async function router(){
  const h=location.hash.slice(1)||'/';const [path,qs]=h.split('?');
  const params=Object.fromEntries(new URLSearchParams(qs||''));
  const p=path.split('/').filter(Boolean);
  try{
    if(!p.length) return home();
    if(p[0]==='movies') return grid('movies');
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
</script></body></html>
"""



@app.get("/site", response_class=HTMLResponse, tags=["Meta"])
async def site_spa():
    return HTMLResponse(SPA_HTML)


# Serve full app at root too
@app.get("/", response_class=HTMLResponse, tags=["Meta"], include_in_schema=False)
async def root_spa():
    return HTMLResponse(SPA_HTML)



