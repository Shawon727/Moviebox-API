# StreamHub API v5.0.0
# MovieBox (HMAC) + 4KHDHub + HubCloud direct resolve

import os
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
from fastapi import FastAPI, HTTPException, Query
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
    try:
        p = urlparse(url)
        if p.scheme != "https" or not p.hostname:
            return False
        host = p.hostname.lower()
        path = p.path.lower()
        if host in ("localhost",) or host.endswith(".local"):
            return False
        if path.endswith(".zip") or "login.php" in path or "logout" in path:
            return False
        if "hubcloud." in host and path.startswith("/drive/"):
            return False
        return True
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
                    "needs_resolve": ("hubcloud." in href or "hubdrive." in href),
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
    return {"ok": True, "version": "5.0.0", "providers": ["moviebox", "4khdhub", "hubcloud", "tmdb", "embeds"]}


# ----- MovieBo



# =============================================================================
# STREAM PROXY — stateless token (base64 payload, works on multi-worker hosts)
# =============================================================================

def _b64url_encode(obj: dict) -> str:
    raw = json.dumps(obj, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def _b64url_decode(s: str) -> dict:
    pad = "=" * ((4 - len(s) % 4) % 4)
    return json.loads(base64.urlsafe_b64decode(s + pad))


def _proxy_url(src: dict, base: str = "") -> str:
    if src.get("type") != "direct" or not src.get("url"):
        return src.get("url") or ""
    h = src.get("headers") or {}
    tok = _b64url_encode({
        "u": src["url"],
        "c": h.get("Cookie") or h.get("cookie") or "",
        "r": h.get("Referer") or h.get("referer") or "https://sportslive.wine",
        "a": h.get("User-Agent") or h.get("user-agent") or (_mb_ua or "Mozilla/5.0"),
    })
    return f"/proxy/{tok}"


@app.get("/proxy/{token}", tags=["Playback"])
async def proxy_stream_token(token: str):
    try:
        meta = _b64url_decode(token)
    except Exception:
        raise HTTPException(400, "bad proxy token")
    return await _proxy_fetch(
        meta.get("u") or "",
        meta.get("c") or "",
        meta.get("r") or "https://sportslive.wine",
        meta.get("a") or "Mozilla/5.0",
    )


@app.get("/proxy", tags=["Playback"])
async def proxy_stream_qs(
    url: str = Query(""),
    cookie: str = Query(""),
    referer: str = Query(""),
    ua: str = Query(""),
):
    if not url:
        raise HTTPException(400, "url required")
    return await _proxy_fetch(url, cookie, referer or "https://sportslive.wine", ua or _mb_ua or "Mozilla/5.0")


async def _proxy_fetch(url: str, cookie: str, referer: str, ua: str):
    if not url.startswith(("https://", "http://")):
        raise HTTPException(400, "Only http(s) upstream")
    headers = {
        "User-Agent": ua or "Mozilla/5.0",
        "Accept": "*/*",
        "Referer": referer or "https://sportslive.wine",
    }
    if cookie:
        headers["Cookie"] = cookie

    async with httpx.AsyncClient(follow_redirects=True, timeout=60.0) as client:
        try:
            upstream = await client.get(url, headers=headers)
        except Exception as e:
            raise HTTPException(502, f"proxy failed: {e}")
        if upstream.status_code >= 400:
            raise HTTPException(upstream.status_code, f"upstream {upstream.status_code}")

        ctype = (upstream.headers.get("content-type") or "").lower()
        body = upstream.content
        final_url = str(upstream.url)
        is_mpd = ".mpd" in url.lower() or "mpd" in ctype or body[:200].lstrip().startswith(b"<?xml")
        is_m3u = ".m3u8" in url.lower() or "mpegurl" in ctype or body[:7] == b"#EXTM3U"

        def make_prox(u: str) -> str:
            u = (u or "").strip().strip('"').strip("'")
            if not u or u.startswith("data:"):
                return u
            if u.startswith("//"):
                u = "https:" + u
            elif u.startswith("/"):
                pr = urlparse(final_url)
                u = f"{pr.scheme}://{pr.netloc}{u}"
            elif not u.startswith("http"):
                u = final_url.rsplit("/", 1)[0] + "/" + u
            tok = _b64url_encode({
                "u": u,
                "c": cookie or "",
                "r": referer or "https://sportslive.wine",
                "a": ua or "Mozilla/5.0",
            })
            return f"/proxy/{tok}"

        if is_mpd or is_m3u:
            try:
                text = body.decode("utf-8", errors="ignore")
                import re as _re
                if is_m3u:
                    out_lines = []
                    for line in text.splitlines():
                        if line and not line.startswith("#"):
                            out_lines.append(make_prox(line))
                        elif "URI=" in line:
                            out_lines.append(_re.sub(
                                r'URI="([^"]+)"',
                                lambda m: f'URI="{make_prox(m.group(1))}"',
                                line,
                            ))
                        else:
                            out_lines.append(line)
                    return Response(
                        content="\n".join(out_lines).encode("utf-8"),
                        media_type="application/vnd.apple.mpegurl",
                        headers={"cache-control": "no-store", "access-control-allow-origin": "*"},
                    )
                text = _re.sub(
                    r'\b(media|initialization|sourceURL|url)=["\']([^"\']+)["\']',
                    lambda m: f'{m.group(1)}="{make_prox(m.group(2))}"',
                    text,
                    flags=_re.I,
                )
                text = _re.sub(
                    r'(<BaseURL[^>]*>)([^<]+)(</BaseURL>)',
                    lambda m: m.group(1) + make_prox(m.group(2).strip()) + m.group(3),
                    text,
                    flags=_re.I,
                )
                return Response(
                    content=text.encode("utf-8"),
                    media_type="application/dash+xml",
                    headers={"cache-control": "no-store", "access-control-allow-origin": "*"},
                )
            except Exception:
                pass

        media_type = ctype.split(";")[0] if ctype else "application/octet-stream"
        return Response(
            content=body,
            media_type=media_type,
            headers={"cache-control": "no-store", "access-control-allow-origin": "*"},
        )


async def _tmdb_search_id(title: str, media: str = "movie"):
    key = os.environ.get("TMDB_API_KEY", "3fd2be6f0c70a2a598f084ddfb75487f")
    # strip [Hindi], (2024), etc for better match
    clean = re.sub(r"\[[^\]]*\]", " ", title or "")
    clean = re.sub(r"\([^)]*\)", " ", clean)
    clean = re.sub(r"\s+", " ", clean).strip() or (title or "")
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.get(
                "https://api.themoviedb.org/3/search/" + ("tv" if media == "tv" else "movie"),
                params={"api_key": key, "query": clean},
            )
            if r.status_code != 200:
                return None
            results = (r.json().get("results") or [])
            return str(results[0]["id"]) if results else None
    except Exception:
        return None


def _embed_sources(tmdb_id: str, media: str, se: int = 1, ep: int = 1):
    tid = tmdb_id
    if media == "movie":
        pairs = [
            ("VidSrc", f"https://vidsrc.xyz/embed/movie/{tid}"),
            ("VidSrc.to", f"https://vidsrc.to/embed/movie/{tid}"),
            ("2Embed", f"https://www.2embed.cc/embed/{tid}"),
            ("VidLink", f"https://vidlink.pro/movie/{tid}"),
            ("SuperEmbed", f"https://multiembed.mov/?video_id={tid}&tmdb=1"),
        ]
    else:
        pairs = [
            ("VidSrc", f"https://vidsrc.xyz/embed/tv/{tid}/{se}/{ep}"),
            ("VidSrc.to", f"https://vidsrc.to/embed/tv/{tid}/{se}/{ep}"),
            ("2Embed", f"https://www.2embed.cc/embedtv/{tid}&s={se}&e={ep}"),
            ("VidLink", f"https://vidlink.pro/tv/{tid}/{se}/{ep}"),
            ("SuperEmbed", f"https://multiembed.mov/?video_id={tid}&tmdb=1&s={se}&e={ep}"),
        ]
    return [
        {"provider": "embed", "label": n, "url": u, "format": "EMBED", "type": "embed", "headers": {}, "play_url": u}
        for n, u in pairs
    ]


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
        items.append({
            "id": str(sid) if sid is not None else None,
            "name": s.get("title") or s.get("name"),
            "poster": poster,
            "year": (s.get("releaseDate") or "")[:4] or None,
            "rating": s.get("imdbRatingValue") or s.get("score"),
            "type": "tv" if stype == 2 else "movie",
            "provider": "moviebox",
            "slug": s.get("detailPath"),
        })
    return [x for x in items if x.get("id")]


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
    """Homepage rows from MovieBox operating tabs + seeded searches."""
    trending = popular_m = popular_t = []
    try:
        data = await mb_request("GET", "/wefeed-mobile-bff/tab-operating?page=1&tabId=1&version=")
        all_items = _mb_items_from_ops(data)
        trending = all_items[:18]
        popular_m = [x for x in all_items if x["type"] == "movie"][:18]
        popular_t = [x for x in all_items if x["type"] == "tv"][:18]
    except Exception:
        pass
    # fallback seeded queries so UI never stays empty
    if len(trending) < 6:
        for q in ("Avengers", "Spider", "Batman"):
            try:
                d = await mb_request("POST", "/wefeed-mobile-bff/subject-api/search/v2",
                    {"keyword": q, "page": 1, "perPage": 10, "subjectType": 0})
                trending.extend(_mb_items_from_search_data(d))
            except Exception:
                continue
        # dedupe
        seen = set()
        uniq = []
        for x in trending:
            if x["id"] in seen:
                continue
            seen.add(x["id"])
            uniq.append(x)
        trending = uniq[:18]
        if not popular_m:
            popular_m = [x for x in trending if x["type"] == "movie"]
        if not popular_t:
            popular_t = [x for x in trending if x["type"] == "tv"]
    return {
        "trending_movies": [x for x in trending if x["type"] == "movie"][:18] or trending[:18],
        "trending_tv": [x for x in trending if x["type"] == "tv"][:18] or popular_t[:18],
        "popular_movies": popular_m[:18] or trending[:18],
        "popular_tv": popular_t[:18] or trending[:18],
    }


@app.get("/api/movies", tags=["Catalog"])
async def api_movies(page: int = 1):
    items = []
    errors = None
    for kw in ("action", "love", "2024", "the"):
        try:
            d = await mb_request("POST", "/wefeed-mobile-bff/subject-api/search/v2",
                {"keyword": kw, "page": page, "perPage": 24, "subjectType": 1})
            items = [x for x in _mb_items_from_search_data(d) if x.get("type") == "movie"] or _mb_items_from_search_data(d)
            if items:
                break
        except Exception as e:
            errors = str(e)
            continue
    return {"page": page, "items": items, "total_pages": 10, "error": errors}


@app.get("/api/series", tags=["Catalog"])
async def api_series(page: int = 1):
    items = []
    errors = None
    for kw in ("drama", "love", "2024", "the"):
        try:
            d = await mb_request("POST", "/wefeed-mobile-bff/subject-api/search/v2",
                {"keyword": kw, "page": page, "perPage": 24, "subjectType": 2})
            items = [x for x in _mb_items_from_search_data(d) if x.get("type") == "tv"] or _mb_items_from_search_data(d)
            if items:
                break
        except Exception as e:
            errors = str(e)
            continue
    return {"page": page, "items": items, "total_pages": 10, "error": errors}


@app.get("/api/search", tags=["Catalog"])
async def api_search_catalog(q: str = Query(..., min_length=1)):
    mb_items = []
    fk_items = []
    errors = {}
    try:
        d = await mb_request("POST", "/wefeed-mobile-bff/subject-api/search/v2",
            {"keyword": q, "page": 1, "perPage": 24, "subjectType": 0})
        mb_items = _mb_items_from_search_data(d)
    except Exception as e:
        errors["moviebox"] = str(e)
    try:
        html = await fk_fetch(f"?s={q}")
        for it in fk_parse_search(html):
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
    return {"query": q, "items": mb_items, "moviebox": mb_items, "fourkhdhub": fk_items, "tmdb": [], "errors": errors or None}


@app.get("/api/detail/{media}/{item_id}", tags=["Catalog"])
async def api_detail(media: str, item_id: str):
    """MovieBox subject detail. media is movie|tv (informational). item_id = subjectId."""
    data = await mb_request("GET", f"/wefeed-mobile-bff/subject-api/get?subjectId={item_id}")
    sub = data.get("subject") or data
    stype = sub.get("subjectType") or sub.get("stype") or (2 if media == "tv" else 1)
    seasons = []
    if stype == 2:
        try:
            sdata = await mb_request("GET", f"/wefeed-mobile-bff/subject-api/season-info?subjectId={item_id}")
            # normalize seasons list
            raw = sdata if isinstance(sdata, list) else (sdata.get("list") or sdata.get("seasons") or sdata.get("data") or [])
            if isinstance(raw, dict):
                raw = raw.get("list") or []
            for s in raw or []:
                if not isinstance(s, dict):
                    continue
                num = s.get("se") or s.get("season") or s.get("seasonNumber") or s.get("number")
                if num is None:
                    continue
                seasons.append({
                    "season": int(num),
                    "name": s.get("title") or s.get("name") or f"Season {num}",
                    "episode_count": s.get("episodeCount") or s.get("epCount") or s.get("maxEp") or len(s.get("episodes") or []),
                    "poster": None,
                })
        except Exception:
            pass
    cover = sub.get("cover") or {}
    poster = cover.get("url") if isinstance(cover, dict) else sub.get("coverUrl")
    return {
        "id": str(sub.get("subjectId") or item_id),
        "name": sub.get("title") or sub.get("name"),
        "overview": sub.get("description") or sub.get("intro") or sub.get("overview") or "",
        "poster": poster,
        "backdrop": poster,
        "year": (sub.get("releaseDate") or "")[:4],
        "rating": sub.get("imdbRatingValue") or sub.get("score"),
        "genres": sub.get("genreNames") or sub.get("genres") or [],
        "type": "tv" if stype == 2 else "movie",
        "seasons": seasons,
        "provider": "moviebox",
    }


@app.get("/api/tv/{item_id}/season/{season}", tags=["Catalog"])
async def api_season(item_id: str, season: int):
    """Build episode buttons 1..N from season-info or play probes."""
    episodes = []
    max_ep = 24
    try:
        sdata = await mb_request("GET", f"/wefeed-mobile-bff/subject-api/season-info?subjectId={item_id}")
        raw = sdata if isinstance(sdata, list) else (sdata.get("list") or sdata.get("seasons") or [])
        if isinstance(raw, dict):
            raw = raw.get("list") or []
        for s in raw or []:
            if not isinstance(s, dict):
                continue
            num = s.get("se") or s.get("season") or s.get("seasonNumber") or s.get("number")
            if num is not None and int(num) == season:
                max_ep = int(s.get("episodeCount") or s.get("epCount") or s.get("maxEp") or 24)
                eps = s.get("episodes") or []
                if eps:
                    for e in eps:
                        if isinstance(e, dict):
                            episodes.append({
                                "episode": e.get("ep") or e.get("episode") or e.get("number"),
                                "name": e.get("title") or e.get("name") or f"Episode {e.get('ep')}",
                                "overview": e.get("description") or "",
                                "still": None,
                            })
                break
    except Exception:
        pass
    if not episodes:
        episodes = [{"episode": i, "name": f"Episode {i}", "overview": "", "still": None} for i in range(1, max_ep + 1)]
    return {"season": season, "episodes": episodes}


@app.get("/api/play", tags=["Playback"])
async def api_play(
    subject_id: str = Query(None, description="MovieBox subject id"),
    tmdb_id: str = Query(None, description="Ignored — kept for old UI"),
    media: str = Query("movie"),
    se: int = 0,
    ep: int = 0,
    q: str = Query("", description="Title search if no subject_id"),
):
    """Failover: MovieBox streams only (direct)."""
    sources = []
    errors = {}
    sid = subject_id
    if not sid and q:
        try:
            d = await mb_request("POST", "/wefeed-mobile-bff/subject-api/search/v2",
                {"keyword": q, "page": 1, "perPage": 5, "subjectType": 0})
            items = _mb_items_from_search_data(d)
            if items:
                sid = items[0]["id"]
        except Exception as e:
            errors["search"] = str(e)
    # UI may pass item id as tmdb_id by mistake — treat as subject_id
    if not sid and tmdb_id:
        sid = tmdb_id
    if not sid:
        return {"count": 0, "sources": [], "errors": {"id": "subject_id required"}, "strategy": "moviebox"}

    se_use = se if media == "tv" else 0
    ep_use = ep if media == "tv" else 0
    try:
        if se_use == 0 and ep_use == 0:
            path = f"/wefeed-mobile-bff/subject-api/play-info/v2?subjectId={sid}"
        else:
            path = f"/wefeed-mobile-bff/subject-api/play-info/v2?subjectId={sid}&se={se_use}&ep={ep_use}"
        try:
            pdata = await mb_request("GET", path)
        except Exception:
            pdata = await mb_request("GET", path.replace("/play-info/v2", "/play-info"))
        for s in _parse_mb_play_info(pdata if isinstance(pdata, dict) else {}, _mb_ua):
            sources.append({
                "provider": "moviebox",
                "label": f"MovieBox {s.get('resolution') or s.get('format') or ''}".strip(),
                "url": s["url"],
                "format": s.get("format"),
                "type": "direct",
                "headers": s.get("headers") or {},
            })
        # resource links
        try:
            extra = await _mb_resource_links(sid, se_use, ep_use)
            seen = {x["url"] for x in sources}
            for e in extra:
                if e["url"] not in seen:
                    sources.append({
                        "provider": "moviebox",
                        "label": f"MovieBox file {e.get('resolution') or ''}".strip(),
                        "url": e["url"],
                        "format": e.get("format"),
                        "type": "direct",
                        "headers": e.get("headers") or {},
                    })
        except Exception:
            pass
    except Exception as e:
        errors["moviebox"] = str(e)

    
    # Attach proxy URLs for direct sources (Cookie/UA required by CDN)
    for s in sources:
        if s.get("type") == "direct":
            s["play_url"] = _proxy_url(s)
        else:
            s["play_url"] = s.get("url")

    # Always try embed backups (more reliable on cloud hosts)
    title = q or ""
    if not title and sid:
        try:
            det = await mb_request("GET", f"/wefeed-mobile-bff/subject-api/get?subjectId={sid}")
            sub = det.get("subject") or det
            title = sub.get("title") or sub.get("name") or ""
        except Exception:
            pass
    if title:
        tid = await _tmdb_search_id(title, media)
        if tid:
            sources.extend(_embed_sources(tid, media, se or 1, ep or 1))
        else:
            errors["embed"] = "no tmdb match for title: " + title[:60]
    else:
        errors["embed"] = "no title for embed lookup"

    seen = set()
    uniq = []
    for s in sources:
        u = s.get("url")
        if not u or u in seen:
            continue
        seen.add(u)
        if "play_url" not in s:
            s["play_url"] = _proxy_url(s) if s.get("type") == "direct" else u
        uniq.append(s)

    return {
        "subject_id": sid,
        "media": media,
        "se": se,
        "ep": ep,
        "title": title,
        "count": len(uniq),
        "sources": uniq,
        "errors": errors or None,
        "strategy": "moviebox-direct(+proxy) → embed failover",
    }




SPA_HTML = r"""<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>StreamHub</title>
<script src="https://cdn.dashjs.org/latest/dash.all.min.js"></script>
<link href="https://fonts.googleapis.com/css2?family=Outfit:wght@400;600;800&display=swap" rel="stylesheet"/>
<style>
:root{--bg:#05060a;--card:#141722;--line:#1e2333;--text:#f0f2f8;--mute:#8b93a7;--a:#6c5ce7;--a2:#00d2d3}
*{box-sizing:border-box;margin:0;padding:0}body{font-family:Outfit,system-ui,sans-serif;background:var(--bg);color:var(--text);min-height:100vh}
a{color:inherit;text-decoration:none}button{font:inherit;cursor:pointer;border:0;background:0;color:inherit}
.nav{position:sticky;top:0;z-index:40;display:flex;gap:10px;align-items:center;padding:12px 16px;background:rgba(5,6,10,.9);backdrop-filter:blur(14px);border-bottom:1px solid var(--line);flex-wrap:wrap}
.logo{font-weight:800;font-size:1.25rem}.logo span{background:linear-gradient(135deg,var(--a),var(--a2));-webkit-background-clip:text;-webkit-text-fill-color:transparent}
.nav a.n{padding:8px 12px;border-radius:10px;color:var(--mute);font-size:.9rem}.nav a.n.on,.nav a.n:hover{background:var(--card);color:var(--text)}
.nav .r{margin-left:auto;display:flex;gap:8px;align-items:center}
.nav input{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:8px 12px;color:var(--text);width:min(160px,30vw)}
.ib{width:38px;height:38px;border-radius:10px;background:var(--card);border:1px solid var(--line)}
.menu{position:absolute;right:12px;top:56px;width:260px;background:#0e1018;border:1px solid var(--line);border-radius:14px;padding:8px;display:none;z-index:50;box-shadow:0 16px 40px #0008}
.menu.open{display:block}.menu a{display:block;padding:10px 12px;border-radius:8px;font-size:.85rem}.menu a:hover{background:var(--card)}
.menu h4{font-size:.65rem;color:var(--mute);text-transform:uppercase;padding:8px 12px 2px}
main{padding-bottom:48px}.hero{position:relative;height:min(52vh,420px);overflow:hidden}
.hero img{width:100%;height:100%;object-fit:cover;filter:brightness(.45)}
.hero .g{position:absolute;inset:0;background:linear-gradient(0deg,var(--bg),transparent 55%)}
.hero .b{position:absolute;left:0;right:0;bottom:0;padding:20px;max-width:600px}
.hero h1{font-size:clamp(1.4rem,4vw,2.2rem);font-weight:800;margin-bottom:8px}
.hero p{color:var(--mute);font-size:.9rem;display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden;margin-bottom:12px}
.btn{display:inline-flex;padding:10px 16px;border-radius:12px;font-weight:600;background:linear-gradient(135deg,var(--a),#8b7cf7);color:#fff;font-size:.9rem}
.btn2{background:rgba(255,255,255,.08);border:1px solid var(--line);margin-left:8px}
.sec{padding:0 14px 22px}.sec h2{font-size:1.1rem;margin:6px 0 10px}
.row{display:grid;grid-auto-flow:column;grid-auto-columns:minmax(105px,130px);gap:10px;overflow-x:auto;padding-bottom:6px}
.card{cursor:pointer}.card .p{aspect-ratio:2/3;border-radius:12px;overflow:hidden;background:var(--card);border:1px solid var(--line)}
.card img{width:100%;height:100%;object-fit:cover}.card .t{font-size:.8rem;font-weight:600;margin-top:6px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.card .s{font-size:.7rem;color:var(--mute)}.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(110px,1fr));gap:12px}
.player{max-width:1000px;margin:0 auto;padding:14px}.frame{aspect-ratio:16/9;background:#000;border-radius:14px;overflow:hidden;border:1px solid var(--line)}
.frame video,.frame iframe{width:100%;height:100%;border:0;background:#000}.bar{display:flex;flex-wrap:wrap;gap:8px;align-items:center;padding:12px 0}
.src{padding:7px 11px;border-radius:9px;background:var(--card);border:1px solid var(--line);font-size:.78rem}
.src.on{border-color:var(--a);background:#6c5ce733}.ep{min-width:38px;padding:7px 9px;border-radius:9px;background:var(--card);border:1px solid var(--line)}
.ep.on{background:var(--a);border-color:var(--a)}.empty{text-align:center;padding:48px 16px;color:var(--mute)}
.detail{display:flex;gap:18px;flex-wrap:wrap;padding:24px 14px;max-width:1000px;margin:0 auto}
.detail .pos{width:140px;border-radius:12px;overflow:hidden;border:1px solid var(--line)}
.toast{position:fixed;bottom:20px;left:50%;transform:translateX(-50%);background:#0e1018;border:1px solid var(--line);padding:10px 16px;border-radius:10px;display:none;z-index:99}
.toast.show{display:block}
.err{color:#ff7675;font-size:.85rem;padding:8px}
</style></head><body>
<nav class="nav">
<a class="logo" href="#/">Stream<span>Hub</span></a>
<a class="n" href="#/" data-n="home">Home</a>
<a class="n" href="#/movies" data-n="movies">Movies</a>
<a class="n" href="#/series" data-n="series">Series</a>
<div class="r">
<input id="q" placeholder="Search…" enterkeyhint="search"/>
<button class="ib" id="mb" type="button">⋮</button>
</div>
</nav>
<div class="menu" id="menu">
<h4>API</h4>
<a href="/docs" target="_blank">Swagger</a>
<a href="/mb/search?q=Avatar" target="_blank">MovieBox /mb</a>
<a href="/fk/search?q=Dune" target="_blank">4KHDHub /fk</a>
<a href="/tools/resolve?url=https://hubcloud.ist/drive/x" target="_blank">HubCloud</a>
<a href="/api/home" target="_blank">Catalog JSON</a>
<a href="/health" target="_blank">Health</a>
</div>
<main id="root"><div class="empty">Loading…</div></main>
<div class="toast" id="toast"></div>
<script>
const root=document.getElementById('root');
const toast=m=>{const t=document.getElementById('toast');t.textContent=m;t.classList.add('show');setTimeout(()=>t.classList.remove('show'),2400)};
document.getElementById('mb').onclick=e=>{e.stopPropagation();document.getElementById('menu').classList.toggle('open')};
document.onclick=()=>document.getElementById('menu').classList.remove('open');
document.getElementById('q').onkeydown=e=>{if(e.key==='Enter'&&e.target.value.trim())location.hash='#/search/'+encodeURIComponent(e.target.value.trim())};
const esc=s=>String(s||'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
async function api(p){
  const r=await fetch(p);
  const tx=await r.text();
  let j=null;
  try{ j=JSON.parse(tx); }catch(e){
    throw new Error(r.ok ? 'Invalid JSON from API' : ('HTTP '+r.status));
  }
  if(!r.ok){
    const d=(j&&(j.detail||j.error||j.message))||('HTTP '+r.status);
    throw new Error(typeof d==='string'?d:JSON.stringify(d));
  }
  return j;
}
function card(i){
  const t=i.type||'movie';
  const id=i.id;
  return `<div class="card" onclick="location.hash='#/title/${t}/${id}'"><div class="p">${i.poster?`<img loading="lazy" src="${esc(i.poster)}" alt="">`:''}</div><div class="t">${esc(i.name)}</div><div class="s">${esc(i.year||'')} · ${t}</div></div>`;
}
function row(title,items){if(!items||!items.length)return '';return `<section class="sec"><h2>${esc(title)}</h2><div class="row">${items.map(card).join('')}</div></section>`}
function setNav(n){document.querySelectorAll('[data-n]').forEach(a=>a.classList.toggle('on',a.dataset.n===n))}
async function home(){
  setNav('home'); root.innerHTML='<div class="empty">Loading home…</div>';
  try{
    const d=await api('/api/home');
    const list=[...(d.trending_movies||[]),...(d.trending_tv||[])];
    const h=list[0];
    let html='';
    if(h) html+=`<div class="hero"><img src="${esc(h.backdrop||h.poster||'')}" alt=""><div class="g"></div><div class="b"><h1>${esc(h.name)}</h1><p>${esc(h.overview||'Stream from MovieBox')}</p>
      <a class="btn" href="#/watch/${h.type}/${h.id}">▶ Watch</a>
      <a class="btn btn2" href="#/title/${h.type}/${h.id}">Details</a></div></div>`;
    html+=row('Trending Movies',d.trending_movies)+row('Trending Series',d.trending_tv)+row('More Movies',d.popular_movies)+row('More Series',d.popular_tv);
    if(!list.length) html='<div class="empty">No catalog data. Check /api/home or MovieBox login.</div>';
    root.innerHTML=html;
  }catch(e){root.innerHTML=`<div class="empty err">${esc(e.message)}</div>`}
}
async function grid(k){
  setNav(k); root.innerHTML='<div class="empty">Loading…</div>';
  try{
    const d=await api(k==='movies'?'/api/movies':'/api/series');
    root.innerHTML=`<section class="sec" style="padding-top:18px"><h2>${k==='movies'?'Movies':'Series'}</h2><div class="grid">${(d.items||[]).map(card).join('')||'<p class="empty">Empty</p>'}</div></section>`;
  }catch(e){root.innerHTML=`<div class="empty err">${esc(e.message)}</div>`}
}
async function search(q){
  setNav(''); root.innerHTML='<div class="empty">Searching…</div>';
  try{
    const d=await api('/api/search?q='+encodeURIComponent(q));
    const items=d.items||d.moviebox||d.tmdb||[];
    let html=`<section class="sec" style="padding-top:18px"><h2>Results · ${esc(q)}</h2><div class="grid">${items.map(card).join('')||'<p class="empty">No results</p>'}</div>`;
    if(d.fourkhdhub&&d.fourkhdhub.length) html+=`<h2 style="margin-top:18px">4KHDHub</h2><div class="grid">${d.fourkhdhub.map(x=>card({...x,type:x.type||'movie'})).join('')}</div>`;
    html+='</section>'; root.innerHTML=html;
  }catch(e){root.innerHTML=`<div class="empty err">${esc(e.message)}</div>`}
}
async function title(media,id){
  setNav(''); root.innerHTML='<div class="empty">Loading…</div>';
  try{
    const d=await api(`/api/detail/${media}/${id}`);
    root.innerHTML=`<div class="detail"><div class="pos">${d.poster?`<img src="${esc(d.poster)}" style="width:100%" alt="">`:''}</div>
      <div style="flex:1;min-width:200px"><h1 style="font-size:1.6rem;font-weight:800;margin-bottom:8px">${esc(d.name)}</h1>
      <p style="color:var(--mute);margin-bottom:8px">${esc(d.year||'')} · ${d.rating||''}</p>
      <p style="color:var(--mute);line-height:1.5;margin-bottom:14px">${esc(d.overview||'')}</p>
      <a class="btn" href="#/watch/${d.type||media}/${id}${ (d.type||media)==='tv'?'?se=1&ep=1':'' }">▶ Play</a></div></div>
      ${(d.type||media)==='tv'&&d.seasons&&d.seasons.length?`<section class="sec"><h2>Seasons</h2><div class="row">${d.seasons.map(s=>`<div class="card" onclick="location.hash='#/watch/tv/${id}?se=${s.season}&ep=1'"><div class="p"></div><div class="t">${esc(s.name||('S'+s.season))}</div></div>`).join('')}</div></section>`:''}`;
  }catch(e){root.innerHTML=`<div class="empty err">${esc(e.message)}</div>`}
}
let PS={sources:[],idx:0};
function renderP(){
  const s=PS.sources[PS.idx]; const f=document.getElementById('frame');
  if(!s){f.innerHTML='<div class="empty">No sources</div>';return}
  const play=s.play_url||s.url;
  document.getElementById('st').textContent=(PS.idx+1)+'/'+PS.sources.length+' · '+s.label+(s.type==='direct'?' · proxy':'');
  document.querySelectorAll('.src[data-i]').forEach((b,i)=>b.classList.toggle('on',i===PS.idx));
  if(s.type==='embed'){
    f.innerHTML=`<iframe src="${esc(play)}" allowfullscreen allow="autoplay;encrypted-media;picture-in-picture" style="width:100%;height:100%;border:0"></iframe>`;
    return;
  }
  const isDash=/\.mpd(\?|$)/i.test(play)||/\.mpd(\?|$)/i.test(s.url||'')||(s.format||'').toUpperCase()==='DASH';
  const isHls=/\.m3u8(\?|$)/i.test(play)||(s.format||'').toUpperCase()==='HLS';
  f.innerHTML='';
  const v=document.createElement('video');
  v.controls=true; v.autoplay=true; v.playsInline=true;
  v.style.cssText='width:100%;height:100%;background:#000';
  v.onerror=()=>{window.__nx&&window.__nx()};
  f.appendChild(v);
  if(isDash&&window.dashjs){
    try{
      const player=dashjs.MediaPlayer().create();
      player.updateSettings({streaming:{abortDelay:{enabled:true}}});
      player.initialize(v, play, true);
      player.on(dashjs.MediaPlayer.events.ERROR,()=>{window.__nx&&window.__nx()});
    }catch(e){window.__nx&&window.__nx()}
  }else{
    v.src=play;
  }
}
window.__nx=()=>{if(PS.idx<PS.sources.length-1){PS.idx++;toast('Next source…');renderP()}else toast('All sources failed')};
async function watch(media,id,se,ep){
  setNav(''); PS={sources:[],idx:0,media,id,se:se||1,ep:ep||1};
  root.innerHTML=`<div class="player"><div class="frame" id="frame"><div class="empty">Resolving streams…</div></div>
    <div class="bar"><span id="st" style="color:var(--mute);font-size:.8rem">…</span>
    <button class="src" type="button" onclick="window.__nx()">Next source ↻</button>
    <a class="src" href="#/title/${media}/${id}">Details</a></div>
    <div id="srcs" style="display:flex;flex-wrap:wrap;gap:6px"></div><div id="eps"></div></div>`;
  try{
    const d=await api(`/api/play?subject_id=${encodeURIComponent(id)}&media=${media}&se=${PS.se}&ep=${PS.ep}`);
    PS.sources=d.sources||[];
    document.getElementById('srcs').innerHTML=PS.sources.map((s,i)=>`<button type="button" class="src ${i===0?'on':''}" data-i="${i}" onclick="PS.idx=${i};renderP()">${esc(s.label)}</button>`).join('')||'<span class="err">No streams</span>';
    if(d.errors) toast(JSON.stringify(d.errors));
    renderP();
  }catch(e){document.getElementById('frame').innerHTML=`<div class="empty err">${esc(e.message)}</div>`}
  if(media==='tv'){
    try{
      const sd=await api(`/api/tv/${id}/season/${PS.se}`);
      document.getElementById('eps').innerHTML=`<div style="margin-top:10px;display:flex;flex-wrap:wrap;gap:6px">${(sd.episodes||[]).map(e=>`<button type="button" class="ep ${e.episode==PS.ep?'on':''}" onclick="location.hash='#/watch/tv/${id}?se=${PS.se}&ep=${e.episode}'">${e.episode}</button>`).join('')}</div>`;
    }catch{}
  }
}
async function router(){
  const h=location.hash.slice(1)||'/'; const [path,qs]=h.split('?');
  const params=Object.fromEntries(new URLSearchParams(qs||''));
  const p=path.split('/').filter(Boolean);
  try{
    if(!p.length) return home();
    if(p[0]==='movies') return grid('movies');
    if(p[0]==='series') return grid('series');
    if(p[0]==='search'&&p[1]) return search(decodeURIComponent(p[1]));
    if(p[0]==='title'&&p[1]&&p[2]) return title(p[1],p[2]);
    if(p[0]==='watch'&&p[1]&&p[2]) return watch(p[1],p[2],+(params.se||1),+(params.ep||1));
    root.innerHTML='<div class="empty">Not found</div>';
  }catch(e){root.innerHTML=`<div class="empty err">${esc(e.message)}</div>`}
}
window.addEventListener('hashchange',router);
router();
</script>
</body></html>"""


@app.get("/site", response_class=HTMLResponse, tags=["Meta"])
async def site_spa():
    return HTMLResponse(SPA_HTML)


# Serve full app at root too
@app.get("/", response_class=HTMLResponse, tags=["Meta"], include_in_schema=False)
async def root_spa():
    return HTMLResponse(SPA_HTML)
