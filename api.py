import os
import re
import json
import time
import hashlib
import hmac
import base64
import random
from urllib.parse import urlparse, parse_qsl, urlencode, unquote
from typing import Optional, Any

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse

app = FastAPI(title="MovieBox API Pro", version="3.1.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

HOST_POOL = [
    "https://api6.aoneroom.com",
    "https://api5.aoneroom.com",
    "https://api4.aoneroom.com",
    "https://api4sg.aoneroom.com",
    "https://api3.aoneroom.com",
    "https://api6sg.aoneroom.com",
    "https://api.inmoviebox.com",
]
SECRET_KEY = "76iRl07s0xSN9jqmEWAt79EBJZulIQIsV64FZr2O"
RETRY = {403, 406, 407, 429, 500, 502, 503, 504}

_token: Optional[str] = None
_host_idx = 0
_ua = ""
_cinfo = ""
_ip = ""


def _b64d(s: str) -> bytes:
    return base64.b64decode(s + "=" * ((4 - len(s) % 4) % 4))


def _md5(b: bytes) -> str:
    return hashlib.md5(b).hexdigest()


def _client_token(ts: int) -> str:
    return f"{ts},{_md5(str(ts)[::-1].encode())}"


def _sorted_q(url: str) -> str:
    p = urlparse(url)
    qs = sorted(parse_qsl(p.query, keep_blank_values=True), key=lambda x: x[0])
    return urlencode(qs, doseq=True) if qs else ""


def _canonical(method: str, url: str, body: Optional[str], ts: int) -> str:
    p = urlparse(url)
    path = p.path or "/"
    q = _sorted_q(url)
    curl = f"{path}?{q}" if q else path
    bh, bl = "", ""
    if body is not None:
        bb = body.encode()
        bh = _md5(bb[:102400])
        bl = str(len(bb))
    return "\n".join([method.upper(), "application/json", "application/json", bl, str(ts), bh, curl])


def _sign(method: str, url: str, body: Optional[str], ts: int) -> str:
    can = _canonical(method, url, body, ts)
    sig = hmac.new(_b64d(SECRET_KEY), can.encode(), hashlib.md5).digest()
    return f"{ts}|2|{base64.b64encode(sig).decode()}"


def _hex(n: int) -> str:
    return "".join(random.choices("0123456789abcdef", k=n))


def _uuid() -> str:
    return f"{_hex(8)}-{_hex(4)}-{_hex(4)}-{_hex(4)}-{_hex(12)}"


def _identity():
    global _ua, _cinfo, _ip
    if _ua:
        return
    androids = [("11", "RP1A.200720.011"), ("12", "S1B.220414.015"), ("13", "TQ2A.230405.003")]
    devices = [("23078RKD5C", "Redmi"), ("2201117TY", "Redmi"), ("M2012K11AG", "Redmi")]
    vcodes = [50020117, 50020118, 50020119, 50020120, 50020121]
    a = random.choice(androids)
    d = random.choice(devices)
    vc = random.choice(vcodes)
    _ua = f"com.community.oneroom/{vc} (Linux; U; Android {a[0]}; en_US; {d[0]}; Build/{a[1]}; Cronet/135.0.7012.3)"
    _cinfo = json.dumps({
        "package_name": "com.community.oneroom", "version_name": "4.0.01.0813.03",
        "version_code": vc, "os": "android", "os_version": a[0], "install_ch": "ps",
        "device_id": _hex(32), "install_store": "ps", "gaid": _uuid(),
        "brand": d[1], "model": d[0], "system_language": "en", "net": "NETWORK_WIFI",
        "region": "US", "timezone": "Asia/Dhaka", "sp_code": "40401", "X-Play-Mode": "2"
    }, separators=(",", ":"))
    prefs = ["103.241", "49.36", "117.195", "106.198", "122.162"]
    _ip = f"{random.choice(prefs)}.{random.randint(1,253)}.{random.randint(1,253)}"


def _headers(method: str, url: str, body: Optional[str] = None, token: Optional[str] = None) -> dict:
    _identity()
    ts = int(time.time() * 1000)
    h = {
        "User-Agent": _ua,
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Connection": "keep-alive",
        "x-client-token": _client_token(ts),
        "x-tr-signature": _sign(method, url, body, ts),
        "x-client-info": _cinfo,
        "x-client-status": "0",
        "x-forwarded-for": _ip,
    }
    if token:
        h["Authorization"] = f"Bearer {token}"
    return h


async def _login(client: httpx.AsyncClient) -> str:
    global _token, _host_idx
    path = "/wefeed-mobile-bff/user-api/visitor-login"
    body = "{}"
    for i in range(len(HOST_POOL)):
        idx = (_host_idx + i) % len(HOST_POOL)
        url = HOST_POOL[idx] + path
        try:
            r = await client.post(url, headers=_headers("POST", url, body), content=body, timeout=12)
            if r.status_code in RETRY:
                continue
            data = r.json()
            tok = data.get("token") or (data.get("data") or {}).get("token")
            xu = r.headers.get("x-user")
            if xu:
                try:
                    tok = json.loads(xu).get("token") or tok
                except Exception:
                    pass
            if tok:
                _token = tok
                _host_idx = idx
                return tok
        except Exception:
            continue
    raise HTTPException(502, "visitor-login failed (all hosts)")


async def _req(method: str, path: str, body: Optional[dict] = None) -> Any:
    global _token, _host_idx
    body_str = json.dumps(body, separators=(",", ":")) if body is not None else None
    async with httpx.AsyncClient(follow_redirects=True, timeout=15) as client:
        if not _token:
            await _login(client)
        for attempt in range(2):
            start = _host_idx
            for i in range(len(HOST_POOL)):
                idx = (start + i) % len(HOST_POOL)
                url = HOST_POOL[idx] + path
                try:
                    h = _headers(method, url, body_str, _token)
                    if method == "POST":
                        r = await client.post(url, headers=h, content=body_str or "{}")
                    else:
                        r = await client.get(url, headers=h)
                    xu = r.headers.get("x-user")
                    if xu:
                        try:
                            nt = json.loads(xu).get("token")
                            if nt:
                                _token = nt
                        except Exception:
                            pass
                    if r.status_code in (401, 403) and attempt == 0:
                        _token = None
                        await _login(client)
                        break
                    if r.status_code in RETRY or r.status_code != 200:
                        continue
                    data = r.json()
                    _host_idx = idx
                    return data.get("data", data) if isinstance(data, dict) else data
                except Exception:
                    continue
            else:
                continue
            break
    raise HTTPException(502, f"failed: {path}")


def _extract_real_url(stream: dict) -> Optional[str]:
    """Bypass dummy video: real URL often hidden in signCookie urlprefix="""
    cookie = stream.get("signCookie") or stream.get("cookie") or ""
    if "urlprefix=" in cookie:
        try:
            part = cookie.split("urlprefix=")[1].split("&")[0].split(";")[0]
            base = base64.b64decode(part + "=" * ((4 - len(part) % 4) % 4)).decode("utf-8", errors="ignore")
            if base.startswith("http"):
                if not base.endswith((".mpd", ".m3u8", "/")):
                    base = base.rstrip("/") + "/index.mpd"
                return base
        except Exception:
            pass
    url = stream.get("url") or stream.get("resourceLink")
    # skip known dummy notice videos
    if url and "b164fbfb43477929" not in url and "aa348f2541d13ffe" not in url:
        return url
    return url  # fallback even if dummy


def _parse_streams(data: dict) -> list:
    out = []
    lists = []
    for key in ("streams", "streamList", "dash", "hls"):
        v = data.get(key)
        if isinstance(v, list):
            lists.extend(v)
    # resourceDetectors fallback
    for det in data.get("resourceDetectors") or []:
        for v in det.get("resolutionList") or []:
            lists.append(v)

    seen = set()
    for s in lists:
        if not isinstance(s, dict):
            continue
        url = _extract_real_url(s)
        if not url or url in seen:
            continue
        seen.add(url)
        res = s.get("resolutions") or s.get("resolution") or s.get("quality") or "?"
        fmt = s.get("format") or ("DASH" if ".mpd" in url else "HLS" if ".m3u8" in url else "MP4")
        item = {
            "resolution": f"{res}p" if str(res).isdigit() else str(res),
            "format": fmt,
            "url": url,
            "size": s.get("size"),
            "duration": s.get("duration"),
            "codec": s.get("codecName"),
            "id": s.get("id"),
            "sign_cookie": s.get("signCookie") or s.get("cookie"),
        }
        out.append(item)
    return out


# ── UI ──────────────────────────────────────────────────────────────────────

DASHBOARD = """<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>MovieBox API Pro</title>
<link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;800&family=JetBrains+Mono:wght@400;700&display=swap" rel="stylesheet">
<style>
:root{--primary:#ff3d71;--secondary:#3366ff;--accent:#00f2ff;--bg:#07080c;--card:rgba(255,255,255,.03);--glass:rgba(255,255,255,.06);--text:#fff}
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:Outfit,sans-serif;background:var(--bg);color:var(--text);min-height:100vh;
background-image:radial-gradient(circle at 10% 10%,rgba(255,61,113,.12) 0%,transparent 40%),radial-gradient(circle at 90% 90%,rgba(51,102,255,.12) 0%,transparent 40%)}
.container{max-width:1100px;margin:0 auto;padding:50px 20px}
header{text-align:center;margin-bottom:50px}
.badge{background:linear-gradient(90deg,var(--primary),var(--secondary));padding:8px 18px;border-radius:40px;font-size:.8rem;font-weight:700;display:inline-block;margin-bottom:16px;text-transform:uppercase;letter-spacing:1px}
h1{font-size:clamp(2.2rem,7vw,3.5rem);font-weight:800;background:linear-gradient(135deg,#fff,#aaa);-webkit-background-clip:text;-webkit-text-fill-color:transparent;margin-bottom:8px}
.sub{color:#667;font-size:1.1rem;font-weight:300}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:22px}
.card{background:var(--card);border:1px solid var(--glass);border-radius:22px;padding:28px;backdrop-filter:blur(12px);display:flex;flex-direction:column;transition:.3s}
.card:hover{transform:translateY(-8px);border-color:rgba(255,255,255,.2);box-shadow:0 20px 40px rgba(0,0,0,.4)}
.card-title{font-size:1.25rem;font-weight:700;margin-bottom:10px;display:flex;align-items:center;gap:10px}
.card-desc{color:#9ea3ac;font-size:.95rem;line-height:1.5;margin-bottom:16px;flex-grow:1}
.endpoint{font-family:JetBrains Mono,monospace;background:rgba(0,0,0,.4);padding:12px;border-radius:12px;font-size:.8rem;color:var(--accent);border:1px solid rgba(0,242,255,.15);margin-bottom:16px;word-break:break-all}
.btn{display:block;text-align:center;padding:14px;background:#fff;color:#000;text-decoration:none;border-radius:14px;font-weight:700;transition:.25s}
.btn:hover{background:var(--primary);color:#fff}
footer{text-align:center;padding:50px 0 20px;color:#555;font-size:.85rem}
</style></head><body>
<div class="container">
<header>
<div class="badge">Mobile API · HMAC · v3.1</div>
<h1>MovieBox Pro</h1>
<p class="sub">Search · Detail · Direct Streams</p>
</header>
<div class="grid">
<div class="card"><div class="card-title">🏠 Home</div><p class="card-desc">Homepage sections & trending</p><div class="endpoint">GET /home</div><a class="btn" href="/home" target="_blank">Open</a></div>
<div class="card"><div class="card-title">🔍 Search</div><p class="card-desc">Search any movie / series</p><div class="endpoint">GET /search?q=Avatar</div><a class="btn" href="/search?q=Avatar" target="_blank">Test</a></div>
<div class="card"><div class="card-title">🆔 Detail</div><p class="card-desc">Full metadata by subject ID</p><div class="endpoint">GET /detail/{subject_id}</div><a class="btn" href="/docs" target="_blank">Docs</a></div>
<div class="card"><div class="card-title">▶️ Stream</div><p class="card-desc">Playable URLs (movie: se=0&ep=0)</p><div class="endpoint">GET /api/stream/{id}?se=0&ep=0</div><a class="btn" href="/docs" target="_blank">Docs</a></div>
<div class="card"><div class="card-title">💬 Captions</div><p class="card-desc">Subtitles for a stream</p><div class="endpoint">GET /api/stream/{id}/captions</div><a class="btn" href="/docs" target="_blank">Docs</a></div>
<div class="card"><div class="card-title">📖 Swagger</div><p class="card-desc">Interactive API documentation</p><div class="endpoint">GET /docs</div><a class="btn" href="/docs" target="_blank">Open Docs</a></div>
</div>
<footer>MovieBox API · Mobile engine · Not affiliated with MovieBox</footer>
</div></body></html>"""


@app.get("/", response_class=HTMLResponse)
async def root():
    return HTMLResponse(DASHBOARD)


@app.get("/health")
async def health():
    return {"status": "ok", "version": "3.1.0"}


@app.get("/home")
async def home(page: int = 1):
    data = await _req("GET", f"/wefeed-mobile-bff/tab-operating?page={page}&tabId=1&version=")
    sections = []
    groups = data.get("items") or data.get("operatingList") or (data if isinstance(data, list) else [])
    if not isinstance(groups, list):
        groups = []
    for g in groups:
        title = g.get("title") or g.get("name") or "Section"
        items = []
        for src in (
            (g.get("subjects") or []),
            [b.get("subject") for b in (g.get("banner") or {}).get("banners") or [] if isinstance(b, dict)],
            [c.get("subject") for c in (g.get("customData") or {}).get("items") or [] if isinstance(c, dict)],
        ):
            for s in src:
                if not isinstance(s, dict):
                    continue
                sid = s.get("subjectId") or s.get("id")
                if not sid:
                    continue
                cover = s.get("cover") or {}
                items.append({
                    "name": s.get("title") or s.get("name"),
                    "poster_url": cover.get("url") if isinstance(cover, dict) else cover,
                    "subject_id": str(sid),
                    "slug": s.get("detailPath"),
                    "rating": s.get("imdbRatingValue"),
                })
        if items:
            sections.append({"section": title, "count": len(items), "items": items})
    return {"status": "success", "sections": sections, "raw_keys": list(data.keys()) if isinstance(data, dict) else []}


@app.get("/search")
async def search(q: str = Query(..., min_length=1), page: int = 1):
    data = await _req("POST", "/wefeed-mobile-bff/subject-api/search/v2", {
        "keyword": q, "page": page, "perPage": 20, "subjectType": 0
    })
    items = []
    # TUI shape: results[0].subjects  or list
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
        items.append({
            "name": s.get("title") or s.get("name"),
            "poster_url": cover.get("url") if isinstance(cover, dict) else s.get("coverUrl"),
            "subject_id": str(sid) if sid else None,
            "slug": s.get("detailPath"),
            "year": (s.get("releaseDate") or "")[:4] or None,
            "rating": s.get("imdbRatingValue"),
            "type": "series" if (s.get("subjectType") or s.get("stype")) == 2 else "movie",
        })
    return {"query": q, "page": page, "total": data.get("total") or len(items), "items": items}


@app.get("/detail/{subject_id}")
async def detail(subject_id: str):
    data = await _req("GET", f"/wefeed-mobile-bff/subject-api/get?subjectId={subject_id}")
    subject = data.get("subject") or data
    stype = subject.get("subjectType") or subject.get("stype") or 1
    if stype == 2:
        try:
            seasons = await _req("GET", f"/wefeed-mobile-bff/subject-api/season-info?subjectId={subject_id}")
            subject["seasons"] = seasons
        except Exception:
            pass
    return subject


@app.get("/api/stream/{subject_id}")
async def stream(subject_id: str, se: int = 0, ep: int = 0):
    if se == 0 and ep == 0:
        path = f"/wefeed-mobile-bff/subject-api/play-info/v2?subjectId={subject_id}"
    else:
        path = f"/wefeed-mobile-bff/subject-api/play-info/v2?subjectId={subject_id}&se={se}&ep={ep}"
    # also try non-v2 as fallback
    try:
        data = await _req("GET", path)
    except HTTPException:
        path2 = path.replace("/play-info/v2", "/play-info")
        data = await _req("GET", path2)

    sources = _parse_streams(data if isinstance(data, dict) else {})
    return {
        "subject_id": subject_id,
        "se": se,
        "ep": ep,
        "has_resource": len(sources) > 0,
        "sources": sources,
        "count": len(sources),
        "note": None if sources else "No stream (paid/region/dummy trap). Try another title.",
        "headers_hint": {
            "User-Agent": _ua or "com.community.oneroom",
            "Cookie": (sources[0].get("sign_cookie") if sources else None),
        },
    }


@app.get("/api/stream/{subject_id}/captions")
async def captions(subject_id: str, resource_id: str = "", se: int = 0, ep: int = 0):
    if not resource_id:
        st = await stream(subject_id, se, ep)
        if st["sources"]:
            resource_id = str(st["sources"][0].get("id") or "")
    if not resource_id:
        return {"count": 0, "captions": []}
    data = await _req("GET", f"/wefeed-mobile-bff/subject-api/get-ext-captions?subjectId={subject_id}&resourceId={resource_id}")
    caps = data.get("extCaptions") or data.get("captions") or data.get("list") or []
    return {"subject_id": subject_id, "resource_id": resource_id, "count": len(caps), "captions": caps}


@app.get("/movies")
async def movies(page: int = 1):
    return await _req("GET", f"/wefeed-mobile-bff/tab-operating?page={page}&tabId=2&version=")


@app.get("/tv-series")
async def tv(page: int = 1):
    return await _req("GET", f"/wefeed-mobile-bff/tab-operating?page={page}&tabId=5&version=")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), reload=True)
