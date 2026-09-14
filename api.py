# StreamHub API v4.0.0
# MovieBox (mobile HMAC) + 4KHDHub (HTML scrape)
# Full single-file FastAPI application

import os
import re
import json
import time
import hashlib
import hmac
import base64
import random
from urllib.parse import urlparse, parse_qsl, urlencode, urljoin
from typing import Optional, Any, List, Dict

import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse

app = FastAPI(
    title="StreamHub API",
    description="MovieBox + 4KHDHub multi-provider streaming API",
    version="4.0.0",
    docs_url="/docs",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# =============================================================================
# MOVIEBOX — mobile API + HMAC-MD5 (from MovieBox-TUI)
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
    pad = (4 - len(s) % 4) % 4
    return base64.b64decode(s + ("=" * pad))


def _md5_hex(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


def _random_hex(n: int) -> str:
    return "".join(random.choices("0123456789abcdef", k=n))


def _random_uuid() -> str:
    return (
        f"{_random_hex(8)}-{_random_hex(4)}-{_random_hex(4)}-"
        f"{_random_hex(4)}-{_random_hex(12)}"
    )


def _ensure_mb_identity() -> None:
    global _mb_ua, _mb_info, _mb_ip
    if _mb_ua:
        return
    android = random.choice(
        [
            ("11", "RP1A.200720.011"),
            ("12", "S1B.220414.015"),
            ("13", "TQ2A.230405.003"),
        ]
    )
    device = random.choice(
        [
            ("23078RKD5C", "Redmi"),
            ("2201117TY", "Redmi"),
            ("M2012K11AG", "Redmi"),
        ]
    )
    vcode = random.choice([50020117, 50020118, 50020119, 50020120, 50020121])
    _mb_ua = (
        f"com.community.oneroom/{vcode} "
        f"(Linux; U; Android {android[0]}; en_US; {device[0]}; "
        f"Build/{android[1]}; Cronet/135.0.7012.3)"
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
    prefix = random.choice(["103.241", "49.36", "117.195", "106.198", "122.162"])
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
    body_hash = ""
    body_len = ""
    if body is not None:
        raw = body.encode()
        body_hash = _md5_hex(raw[:102400])
        body_len = str(len(raw))
    return "\n".join(
        [
            method.upper(),
            "application/json",
            "application/json",
            body_len,
            str(ts),
            body_hash,
            canonical_url,
        ]
    )


def _mb_headers(
    method: str,
    url: str,
    body: Optional[str] = None,
    token: Optional[str] = None,
) -> dict:
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
            resp = await client.post(
                url,
                headers=_mb_headers("POST", url, body),
                content=body,
                timeout=12.0,
            )
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
    raise HTTPException(status_code=502, detail="MovieBox visitor-login failed (all hosts)")


async def mb_request(method: str, path: str, body: Optional[dict] = None) -> Any:
    global _mb_token, _mb_idx
    body_str = json.dumps(body, separators=(",", ":")) if body is not None else None
    async with httpx.AsyncClient(follow_redirects=True, timeout=15.0) as client:
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
                        resp = await client.post(
                            url, headers=headers, content=body_str or "{}"
                        )
                    else:
                        resp = await client.get(url, headers=headers)

                    x_user = resp.headers.get("x-user")
                    if x_user:
                        try:
                            new_tok = json.loads(x_user).get("token")
                            if new_tok:
                                _mb_token = new_tok
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
                    if isinstance(data, dict) and "data" in data:
                        return data["data"]
                    return data
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
    if "macdn.aoneroom.com" in u and "/other/" in u:
        return True
    return False


def _dash_from_sign_cookie(cookie: str) -> Optional[str]:
    """Extract real DASH/HLS URL from CloudFront-Policy or urlprefix= (MovieBox-TUI)."""
    if not cookie:
        return None

    for part in cookie.split(";"):
        trimmed = part.strip()
        if not trimmed.startswith("CloudFront-Policy="):
            continue
        raw = trimmed[len("CloudFront-Policy=") :].strip()
        normalized = (
            raw.replace("-", "+").replace("_", "=").replace("~", "/")
        )
        try:
            pad = (4 - len(normalized) % 4) % 4
            decoded = base64.b64decode(normalized + ("=" * pad))
            policy = json.loads(decoded)
            resource = policy["Statement"][0]["Resource"]
            base = resource.rstrip("*").rstrip("/")
            if base.startswith("http://") or base.startswith("https://"):
                return f"{base}/index.mpd"
        except Exception:
            continue

    if "urlprefix=" in cookie:
        try:
            part = cookie.split("urlprefix=")[1].split("&")[0].split(";")[0]
            pad = (4 - len(part) % 4) % 4
            base = base64.b64decode(part + ("=" * pad)).decode("utf-8", errors="ignore")
            if base.startswith("http"):
                if base.endswith((".mpd", ".m3u8")):
                    return base
                return base.rstrip("/") + "/index.mpd"
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

        res = (
            stream.get("resolutions")
            or stream.get("resolution")
            or stream.get("quality")
            or "?"
        )
        fmt = stream.get("format")
        if not fmt:
            if ".mpd" in playable:
                fmt = "DASH"
            elif ".m3u8" in playable:
                fmt = "HLS"
            else:
                fmt = "MP4"

        headers = {
            "User-Agent": user_agent or _mb_ua,
            "Referer": "https://sportslive.wine",
        }
        if cookie:
            clean = "; ".join(
                p.strip() for p in cookie.strip(";").split(";") if p.strip()
            )
            headers["Cookie"] = clean

        out.append(
            {
                "resolution": f"{res}p"
                if str(res).replace(",", "").isdigit()
                else str(res),
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
            path = (
                f"/wefeed-mobile-bff/subject-api/resource"
                f"?subjectId={subject_id}&page=1&perPage=30"
            )
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
            if ".mpd" in link:
                fmt = "DASH"
            elif ".m3u8" in link:
                fmt = "HLS"
            elif ".mp4" in link:
                fmt = "MP4"
            else:
                fmt = "FILE"
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
# 4KHDHub — HTML scrape (from MovieBox-TUI selectors)
# =============================================================================

FK_BASES = [
    "https://4khdhub.one/",
    "https://4khdhub.link/",
    "https://4khdhub.click/",
    "https://4khdhub.ink/",
]
FK_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
_fk_base = FK_BASES[0]


async def fk_fetch(path_or_url: str) -> str:
    global _fk_base
    async with httpx.AsyncClient(
        follow_redirects=True,
        timeout=20.0,
        headers={"User-Agent": FK_UA},
    ) as client:
        if path_or_url.startswith("http"):
            candidates = [path_or_url]
        else:
            candidates = [urljoin(base, path_or_url.lstrip("/")) for base in FK_BASES]

        last_error = None
        for url in candidates:
            try:
                resp = await client.get(url)
                if resp.status_code == 200 and len(resp.text) > 400:
                    parsed = urlparse(str(resp.url))
                    _fk_base = f"{parsed.scheme}://{parsed.netloc}/"
                    return resp.text
            except Exception as exc:
                last_error = exc
                continue
    raise HTTPException(
        status_code=502,
        detail=f"4KHDHub unreachable: {last_error}",
    )


def fk_parse_search(html: str) -> List[dict]:
    soup = BeautifulSoup(html, "html.parser")
    items: List[dict] = []
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
        match = re.search(r"(19|20)\d{2}", meta_text or title)
        if match:
            year = match.group(0)
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
    releases: List[dict] = []

    for item in soup.select(item_sel):
        title_el = item.select_one(title_sel)
        filename = title_el.get_text(strip=True) if title_el else ""
        if not filename:
            continue
        lower = filename.lower()
        if lower.endswith((".zip", ".rar", ".7z")):
            continue

        if season > 0:
            m = re.search(r"S0*(\d+)\s*E0*(\d+)", filename, re.I)
            if not m:
                continue
            if int(m.group(1)) != season or int(m.group(2)) != episode:
                continue

        mirrors = []
        for link in item.select("a[href]"):
            href = link.get("href") or ""
            if not href.startswith("https://"):
                continue
            if "logout" in href.lower():
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
# UI — StreamHub (fresh, not Walter clone)
# =============================================================================

UI_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>StreamHub</title>
  <link href="https://fonts.googleapis.com/css2?family=Syne:wght@600;700;800&family=IBM+Plex+Mono:wght@400;500&display=swap" rel="stylesheet" />
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    :root {
      --bg: #0a0b0f;
      --panel: #12141c;
      --line: #1e2230;
      --text: #e8eaf0;
      --mute: #7a8194;
      --g: #39ff14;
      --c: #00e5ff;
      --m: #c77dff;
    }
    body {
      font-family: Syne, system-ui, sans-serif;
      background: var(--bg);
      color: var(--text);
      min-height: 100vh;
      background-image:
        radial-gradient(ellipse 80% 50% at 20% -10%, rgba(57,255,20,.08), transparent),
        radial-gradient(ellipse 60% 40% at 90% 10%, rgba(0,229,255,.07), transparent),
        radial-gradient(ellipse 50% 30% at 50% 100%, rgba(199,125,255,.06), transparent);
    }
    .wrap { max-width: 1080px; margin: 0 auto; padding: 48px 20px 80px; }
    .top {
      display: flex; align-items: center; justify-content: space-between;
      margin-bottom: 48px; flex-wrap: wrap; gap: 16px;
    }
    .logo { font-size: 1.75rem; font-weight: 800; letter-spacing: -0.04em; }
    .logo span {
      background: linear-gradient(135deg, var(--g), var(--c));
      -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    }
    .pill {
      font-family: "IBM Plex Mono", monospace; font-size: 0.7rem;
      padding: 6px 12px; border: 1px solid var(--line); border-radius: 999px;
      color: var(--mute); background: var(--panel);
    }
    .hero { margin-bottom: 40px; }
    .hero h1 {
      font-size: clamp(1.8rem, 5vw, 2.6rem); font-weight: 800;
      line-height: 1.15; margin-bottom: 12px; letter-spacing: -0.03em;
    }
    .hero p {
      color: var(--mute); font-size: 1.05rem; max-width: 520px;
      line-height: 1.5; font-weight: 500;
    }
    .providers {
      display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
      gap: 16px; margin-bottom: 36px;
    }
    .prov {
      background: var(--panel); border: 1px solid var(--line); border-radius: 16px;
      padding: 22px; position: relative; overflow: hidden; transition: 0.25s;
    }
    .prov:hover { border-color: #2a3145; transform: translateY(-3px); }
    .prov::before {
      content: ""; position: absolute; top: 0; left: 0; right: 0; height: 2px;
      background: var(--accent, var(--g));
    }
    .prov.mb { --accent: var(--g); }
    .prov.fk { --accent: var(--c); }
    .prov.ag { --accent: var(--m); }
    .prov h3 {
      font-size: 1.1rem; margin-bottom: 6px;
      display: flex; align-items: center; gap: 8px;
    }
    .prov .tag {
      font-family: "IBM Plex Mono", monospace; font-size: 0.65rem;
      color: var(--accent);
      border: 1px solid color-mix(in srgb, var(--accent) 40%, transparent);
      padding: 2px 8px; border-radius: 6px;
    }
    .prov p {
      color: var(--mute); font-size: 0.88rem; line-height: 1.45; margin-bottom: 14px;
    }
    .ep {
      font-family: "IBM Plex Mono", monospace; font-size: 0.72rem;
      background: #0d0f14; border: 1px solid var(--line); padding: 10px 12px;
      border-radius: 10px; color: var(--c); margin-bottom: 12px; word-break: break-all;
    }
    .btn {
      display: inline-flex; align-items: center; justify-content: center;
      padding: 11px 16px; border-radius: 10px; background: var(--text);
      color: var(--bg); font-weight: 700; font-size: 0.85rem;
      text-decoration: none; transition: 0.2s;
    }
    .btn:hover { opacity: 0.9; transform: scale(1.02); }
    .btn.ghost {
      background: transparent; color: var(--text); border: 1px solid var(--line);
    }
    .btn.ghost:hover { border-color: var(--mute); }
    .grid2 {
      display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
      gap: 12px;
    }
    .card {
      background: var(--panel); border: 1px solid var(--line);
      border-radius: 14px; padding: 18px;
    }
    .card h4 { font-size: 0.95rem; margin-bottom: 8px; }
    .foot {
      margin-top: 48px; text-align: center; color: var(--mute);
      font-size: 0.8rem; font-family: "IBM Plex Mono", monospace;
    }
  </style>
</head>
<body>
  <div class="wrap">
    <div class="top">
      <div class="logo">Stream<span>Hub</span></div>
      <div class="pill">v4.0 · multi-provider</div>
    </div>
    <div class="hero">
      <h1>One API.<br />Multiple sources.</h1>
      <p>MovieBox mobile streams + 4KHDHub releases. Clean endpoints with playback headers.</p>
    </div>
    <div class="providers">
      <div class="prov mb">
        <h3>MovieBox <span class="tag">HMAC</span></h3>
        <p>Mobile API · DASH + MP4 · signCookie bypass · resource links</p>
        <div class="ep">/mb/search?q= · /mb/stream/{id}</div>
        <a class="btn" href="/mb/search?q=Avatar" target="_blank">Try search</a>
      </div>
      <div class="prov fk">
        <h3>4KHDHub <span class="tag">SCRAPE</span></h3>
        <p>4K / BluRay releases · multi-mirror · series episodes</p>
        <div class="ep">/fk/search?q= · /fk/stream?id=</div>
        <a class="btn" href="/fk/search?q=Dune" target="_blank">Try search</a>
      </div>
      <div class="prov ag">
        <h3>Aggregate <span class="tag">ALL</span></h3>
        <p>Search both providers in one call</p>
        <div class="ep">/search?q=Avatar</div>
        <a class="btn" href="/search?q=Avatar" target="_blank">Search all</a>
      </div>
    </div>
    <div class="grid2">
      <div class="card">
        <h4>Docs</h4>
        <div class="ep">/docs</div>
        <a class="btn ghost" href="/docs">Swagger UI</a>
      </div>
      <div class="card">
        <h4>Health</h4>
        <div class="ep">/health</div>
        <a class="btn ghost" href="/health">Check</a>
      </div>
      <div class="card">
        <h4>Movie stream</h4>
        <div class="ep">/mb/stream/{id}?se=0&amp;ep=0</div>
      </div>
      <div class="card">
        <h4>Series stream</h4>
        <div class="ep">/mb/stream/{id}?se=1&amp;ep=1</div>
      </div>
    </div>
    <div class="foot">StreamHub · independent client · not affiliated</div>
  </div>
</body>
</html>
"""


# =============================================================================
# Routes
# =============================================================================


@app.get("/", response_class=HTMLResponse)
async def root():
    return HTMLResponse(UI_HTML)


@app.get("/health")
async def health():
    return {
        "ok": True,
        "version": "4.0.0",
        "providers": ["moviebox", "4khdhub"],
    }


# ----- MovieBox (/mb/...) -----


@app.get("/mb/search")
async def mb_search(q: str = Query(..., min_length=1), page: int = 1):
    data = await mb_request(
        "POST",
        "/wefeed-mobile-bff/subject-api/search/v2",
        {
            "keyword": q,
            "page": page,
            "perPage": 20,
            "subjectType": 0,
        },
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
        poster = None
        if isinstance(cover, dict):
            poster = cover.get("url")
        else:
            poster = s.get("coverUrl") or cover
        items.append(
            {
                "name": s.get("title") or s.get("name"),
                "subject_id": str(sid) if sid is not None else None,
                "poster_url": poster,
                "slug": s.get("detailPath"),
                "year": (s.get("releaseDate") or "")[:4] or None,
                "rating": s.get("imdbRatingValue"),
                "type": "series"
                if (s.get("subjectType") or s.get("stype")) == 2
                else "movie",
                "provider": "moviebox",
            }
        )
    return {
        "provider": "moviebox",
        "query": q,
        "page": page,
        "total": data.get("total") or len(items),
        "items": items,
    }


@app.get("/mb/detail/{subject_id}")
async def mb_detail(subject_id: str):
    data = await mb_request(
        "GET", f"/wefeed-mobile-bff/subject-api/get?subjectId={subject_id}"
    )
    subject = data.get("subject") or data
    stype = subject.get("subjectType") or subject.get("stype") or 1
    if stype == 2:
        try:
            seasons = await mb_request(
                "GET",
                f"/wefeed-mobile-bff/subject-api/season-info?subjectId={subject_id}",
            )
            subject["seasons"] = seasons
        except Exception:
            pass
    return {"provider": "moviebox", "data": subject}


@app.get("/mb/stream/{subject_id}")
async def mb_stream(subject_id: str, se: int = 0, ep: int = 0):
    if se == 0 and ep == 0:
        path = f"/wefeed-mobile-bff/subject-api/play-info/v2?subjectId={subject_id}"
    else:
        path = (
            f"/wefeed-mobile-bff/subject-api/play-info/v2"
            f"?subjectId={subject_id}&se={se}&ep={ep}"
        )
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
        "note": None
        if sources
        else "No playable sources (try another id / episode)",
    }


@app.get("/mb/home")
async def mb_home(page: int = 1):
    data = await mb_request(
        "GET", f"/wefeed-mobile-bff/tab-operating?page={page}&tabId=1&version="
    )
    return {"provider": "moviebox", "data": data}


@app.get("/mb/captions/{subject_id}")
async def mb_captions(
    subject_id: str,
    resource_id: str = "",
    se: int = 0,
    ep: int = 0,
):
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
        f"/wefeed-mobile-bff/subject-api/get-ext-captions"
        f"?subjectId={subject_id}&resourceId={resource_id}",
    )
    captions = (
        data.get("extCaptions")
        or data.get("captions")
        or data.get("list")
        or []
    )
    return {
        "provider": "moviebox",
        "subject_id": subject_id,
        "resource_id": resource_id,
        "count": len(captions),
        "captions": captions,
    }


# ----- 4KHDHub (/fk/...) -----


@app.get("/fk/search")
async def fk_search(q: str = Query(..., min_length=1)):
    html = await fk_fetch(f"?s={q}")
    items = fk_parse_search(html)
    return {"provider": "4khdhub", "query": q, "items": items}


@app.get("/fk/detail")
async def fk_detail(
    id: str = Query(..., description="Path id from search, e.g. /some-movie/"),
):
    html = await fk_fetch(id)
    soup = BeautifulSoup(html, "html.parser")
    h1 = soup.select_one("h1")
    title = h1.get_text(strip=True) if h1 else id
    og = soup.select_one('meta[property="og:image"]')
    desc_el = soup.select_one(".content-section p.mt-4")
    if not desc_el:
        desc_el = soup.select_one('meta[name="description"]')
    description = None
    if desc_el:
        if desc_el.name == "p":
            description = desc_el.get_text(strip=True)
        else:
            description = desc_el.get("content")
    return {
        "provider": "4khdhub",
        "id": id,
        "title": title,
        "poster_url": og.get("content") if og else None,
        "description": description,
        "type": "series" if "-series-" in id else "movie",
    }


@app.get("/fk/stream")
async def fk_stream(
    id: str = Query(..., description="Path id from search"),
    se: int = 0,
    ep: int = 0,
):
    html = await fk_fetch(id)
    releases = fk_parse_releases(html, se, ep)
    return {
        "provider": "4khdhub",
        "id": id,
        "se": se,
        "ep": ep,
        "count": len(releases),
        "releases": releases,
        "note": (
            "mirrors with needs_resolve=true need hubcloud/hubdrive "
            "resolution in your player/client"
        ),
    }


# ----- Aggregate -----


@app.get("/search")
async def search_all(q: str = Query(..., min_length=1)):
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
    return {
        "query": q,
        "moviebox": moviebox_items,
        "fourkhdhub": fourk_items,
        "errors": errors or None,
    }


# ----- Legacy aliases (old clients) -----


@app.get("/api/stream/{subject_id}")
async def legacy_stream(
    subject_id: str,
    se: int = 0,
    ep: int = 0,
    detail_path: str = "",
):
    return await mb_stream(subject_id, se, ep)


@app.get("/detail/{subject_id}")
async def legacy_detail(subject_id: str):
    return await mb_detail(subject_id)


if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("api:app", host="0.0.0.0", port=port, reload=True)
