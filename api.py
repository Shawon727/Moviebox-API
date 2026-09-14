# api.py — MovieBox + 4KHDHub multi-provider API v4
import os, re, json, time, hashlib, hmac, base64, random
from urllib.parse import urlparse, parse_qsl, urlencode, urljoin, unquote
from typing import Optional, Any, List, Dict

import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse

app = FastAPI(title="StreamHub API", version="4.0.0", docs_url="/docs")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ═══════════════════════════════════════════════════════════
#  MOVIEBOX — mobile HMAC
# ═══════════════════════════════════════════════════════════
MB_HOSTS = [
    "https://api6.aoneroom.com", "https://api5.aoneroom.com",
    "https://api4.aoneroom.com", "https://api4sg.aoneroom.com",
    "https://api3.aoneroom.com", "https://api.inmoviebox.com",
]
MB_SECRET = "76iRl07s0xSN9jqmEWAt79EBJZulIQIsV64FZr2O"
RETRY = {403, 406, 407, 429, 500, 502, 503, 504}
_mb_token = None
_mb_idx = 0
_mb_ua = _mb_info = _mb_ip = ""

def _b64d(s): return base64.b64decode(s + "=" * ((4 - len(s) % 4) % 4))
def _md5(b): return hashlib.md5(b).hexdigest()
def _hex(n): return "".join(random.choices("0123456789abcdef", k=n))
def _uuid(): return f"{_hex(8)}-{_hex(4)}-{_hex(4)}-{_hex(4)}-{_hex(12)}"

def _mb_id():
    global _mb_ua, _mb_info, _mb_ip
    if _mb_ua: return
    a = random.choice([("12","S1B.220414.015"),("13","TQ2A.230405.003")])
    d = random.choice([("23078RKD5C","Redmi"),("M2012K11AG","Redmi")])
    vc = random.choice([50020117,50020118,50020119,50020120,50020121])
    _mb_ua = f"com.community.oneroom/{vc} (Linux; U; Android {a[0]}; en_US; {d[0]}; Build/{a[1]}; Cronet/135.0.7012.3)"
    _mb_info = json.dumps({"package_name":"com.community.oneroom","version_name":"4.0.01.0813.03","version_code":vc,"os":"android","os_version":a[0],"install_ch":"ps","device_id":_hex(32),"install_store":"ps","gaid":_uuid(),"brand":d[1],"model":d[0],"system_language":"en","net":"NETWORK_WIFI","region":"US","timezone":"Asia/Dhaka","sp_code":"40401","X-Play-Mode":"2"},separators=(",",":"))
    _mb_ip = f"{random.choice(['103.241','49.36','117.195'])}.{random.randint(1,253)}.{random.randint(1,253)}"

def _mb_canon(method, url, body, ts):
    p = urlparse(url); path = p.path or "/"
    qs = sorted(parse_qsl(p.query, keep_blank_values=True), key=lambda x: x[0])
    curl = f"{path}?{urlencode(qs, doseq=True)}" if qs else path
    bh = bl = ""
    if body is not None:
        bb = body.encode(); bh = _md5(bb[:102400]); bl = str(len(bb))
    return "\n".join([method.upper(),"application/json","application/json",bl,str(ts),bh,curl])

def _mb_headers(method, url, body=None, token=None):
    _mb_id(); ts = int(time.time()*1000)
    sig = hmac.new(_b64d(MB_SECRET), _mb_canon(method,url,body,ts).encode(), hashlib.md5).digest()
    h = {"User-Agent":_mb_ua,"Accept":"application/json","Content-Type":"application/json",
         "x-client-token":f"{ts},{_md5(str(ts)[::-1].encode())}",
         "x-tr-signature":f"{ts}|2|{base64.b64encode(sig).decode()}",
         "x-client-info":_mb_info,"x-client-status":"0","x-forwarded-for":_mb_ip}
    if token: h["Authorization"] = f"Bearer {token}"
    return h

async def _mb_login(c):
    global _mb_token, _mb_idx
    for i in range(len(MB_HOSTS)):
        idx = (_mb_idx+i)%len(MB_HOSTS)
        url = MB_HOSTS[idx]+"/wefeed-mobile-bff/user-api/visitor-login"
        try:
            r = await c.post(url, headers=_mb_headers("POST",url,"{}"), content="{}", timeout=12)
            if r.status_code in RETRY: continue
            d = r.json(); tok = d.get("token") or (d.get("data") or {}).get("token")
            xu = r.headers.get("x-user")
            if xu:
                try: tok = json.loads(xu).get("token") or tok
                except: pass
            if tok: _mb_token=tok; _mb_idx=idx; return tok
        except: continue
    raise HTTPException(502,"MovieBox login failed")

async def mb_req(method, path, body=None):
    global _mb_token, _mb_idx
    bs = json.dumps(body, separators=(",",":")) if body is not None else None
    async with httpx.AsyncClient(follow_redirects=True, timeout=15) as c:
        if not _mb_token: await _mb_login(c)
        for attempt in range(2):
            start = _mb_idx
            for i in range(len(MB_HOSTS)):
                idx = (start+i)%len(MB_HOSTS)
                url = MB_HOSTS[idx]+path
                try:
                    h = _mb_headers(method, url, bs, _mb_token)
                    r = await (c.post(url,headers=h,content=bs or "{}") if method=="POST" else c.get(url,headers=h))
                    xu = r.headers.get("x-user")
                    if xu:
                        try:
                            nt = json.loads(xu).get("token")
                            if nt: _mb_token = nt
                        except: pass
                    if r.status_code in (401,403) and attempt==0:
                        _mb_token=None; await _mb_login(c); break
                    if r.status_code in RETRY or r.status_code!=200: continue
                    data = r.json(); _mb_idx = idx
                    return data.get("data", data) if isinstance(data,dict) else data
                except: continue
            else: continue
            break
    raise HTTPException(502, f"MovieBox failed: {path}")

def _is_dummy(url: str) -> bool:
    u = (url or "").lower()
    return any(x in u for x in [
        "1c7de0bd3393702d9191801f15f88f8d","9a0461bc39da389663bf3dbb17091d3f",
        "/notice.mp4","b164fbfb43477929","aa348f2541d13ffe"
    ]) or ("macdn.aoneroom.com" in u and "/other/" in u)

def _dash_from_policy(cookie: str) -> Optional[str]:
    """CloudFront-Policy → real index.mpd (MovieBox-TUI logic)"""
    for part in (cookie or "").split(";"):
        t = part.strip()
        if not t.startswith("CloudFront-Policy="): continue
        raw = t[len("CloudFront-Policy="):].strip()
        norm = raw.replace("-","+").replace("_","=").replace("\~","/")
        try:
            dec = base64.b64decode(norm + "="*((4-len(norm)%4)%4))
            j = json.loads(dec)
            res = j["Statement"][0]["Resource"]
            base = res.rstrip("*").rstrip("/")
            if base.startswith("http"):
                return base + "/index.mpd"
        except Exception:
            continue
    # urlprefix= fallback
    if "urlprefix=" in (cookie or ""):
        try:
            p = cookie.split("urlprefix=")[1].split("&")[0].split(";")[0]
            base = base64.b64decode(p + "="*((4-len(p)%4)%4)).decode("utf-8","ignore")
            if base.startswith("http"):
                return base if base.endswith((".mpd",".m3u8")) else base.rstrip("/")+"/index.mpd"
        except: pass
    return None

def _parse_mb_streams(data: dict, ua: str) -> List[dict]:
    out, seen = [], set()
    streams = data.get("streams") or data.get("streamList") or []
    if not isinstance(streams, list): streams = []

    for s in streams:
        if not isinstance(s, dict): continue
        cookie = s.get("signCookie") or s.get("cookie") or ""
        raw_url = s.get("url") or ""
        play = _dash_from_policy(cookie)
        if not play and raw_url and not _is_dummy(raw_url):
            play = raw_url
        if not play or play in seen: continue
        seen.add(play)
        res = s.get("resolutions") or s.get("resolution") or s.get("quality") or "?"
        fmt = s.get("format") or ("DASH" if ".mpd" in play else "HLS" if ".m3u8" in play else "MP4")
        headers = {"User-Agent": ua, "Referer": "https://sportslive.wine"}
        if cookie:
            headers["Cookie"] = "; ".join(x.strip() for x in cookie.strip(";").split(";") if x.strip())
        out.append({
            "resolution": f"{res}p" if str(res).replace(",","").isdigit() else str(res),
            "format": fmt, "url": play, "size": s.get("size"),
            "duration": s.get("duration"), "codec": s.get("codecName"),
            "id": s.get("id"), "headers": headers, "source": "play-info"
        })

    # resourceDetectors
    for det in data.get("resourceDetectors") or []:
        for v in det.get("resolutionList") or []:
            if not isinstance(v, dict): continue
            u = v.get("resourceLink") or v.get("url")
            if not u or u in seen or _is_dummy(u): continue
            seen.add(u)
            out.append({
                "resolution": f"{v.get('resolution','?')}p", "format": "MP4",
                "url": u, "headers": {"User-Agent": ua}, "source": "resourceDetector"
            })
    return out

async def _mb_resource_links(subject_id: str, se: int, ep: int) -> List[dict]:
    """Extra direct links from subject-api/resource"""
    try:
        if se==0 and ep==0:
            path = f"/wefeed-mobile-bff/subject-api/resource?subjectId={subject_id}&page=1&perPage=30"
        else:
            path = f"/wefeed-mobile-bff/subject-api/resource?subjectId={subject_id}&se={se}&ep={ep}&page=1&perPage=30"
        data = await mb_req("GET", path)
        items = data.get("list") or []
        out, seen = [], set()
        for it in items:
            u = it.get("resourceLink") or it.get("url")
            if not u or u in seen or _is_dummy(u): continue
            seen.add(u)
            out.append({
                "resolution": f"{it.get('resolution','?')}p",
                "format": "MP4" if ".mp4" in u else ("DASH" if ".mpd" in u else "FILE"),
                "url": u, "size": it.get("size"), "filename": it.get("fileName") or it.get("title"),
                "headers": {"User-Agent": _mb_ua}, "source": "resource", "id": it.get("resourceId") or it.get("id")
            })
        return out
    except Exception:
        return []

# ═══════════════════════════════════════════════════════════
#  4KHDHub — HTML scrape
# ═══════════════════════════════════════════════════════════
FK_BASES = ["https://4khdhub.one/", "https://4khdhub.link/", "https://4khdhub.click/"]
FK_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
_fk_base = FK_BASES[0]

async def fk_get(path_or_url: str) -> str:
    global _fk_base
    async with httpx.AsyncClient(follow_redirects=True, timeout=20, headers={"User-Agent": FK_UA}) as c:
        urls = []
        if path_or_url.startswith("http"):
            urls = [path_or_url]
        else:
            urls = [urljoin(b, path_or_url.lstrip("/")) for b in FK_BASES]
        for u in urls:
            try:
                r = await c.get(u)
                if r.status_code == 200 and len(r.text) > 500:
                    _fk_base = str(r.url).rsplit("/", 1)[0] + "/"
                    return r.text
            except: continue
    raise HTTPException(502, "4KHDHub unreachable")

def fk_parse_search(html: str) -> List[dict]:
    soup = BeautifulSoup(html, "lxml")
    items = []
    for a in soup.select("a.movie-card"):
        href = a.get("href") or ""
        title_el = a.select_one(".movie-card-title")
        title = title_el.get_text(strip=True) if title_el else ""
        if not title or not href: continue
        meta = a.select_one(".movie-card-meta")
        meta_t = meta.get_text(" ", strip=True) if meta else ""
        img = a.select_one("img")
        year = None
        m = re.search(r"(19|20)\d{2}", meta_t or title)
        if m: year = m.group(0)
        path = urlparse(href).path if href.startswith("http") else href
        items.append({
            "name": title, "id": path, "poster_url": img.get("src") if img else None,
            "year": year, "type": "series" if "-series-" in href else "movie"
        })
    return items

def fk_parse_releases(html: str, se: int=0, ep: int=0) -> List[dict]:
    soup = BeautifulSoup(html, "lxml")
    sel = "#episodes .episode-download-item" if se > 0 else ".download-item"
    title_sel = ".episode-file-title" if se > 0 else ".file-title"
    releases = []
    for item in soup.select(sel):
        ft = item.select_one(title_sel)
        filename = ft.get_text(strip=True) if ft else ""
        if not filename or filename.lower().endswith((".zip",".rar")): continue
        if se > 0:
            m = re.search(r"S0*(\d+)\s*E0*(\d+)", filename, re.I)
            if not m or int(m.group(1)) != se or int(m.group(2)) != ep:
                continue
        mirrors = []
        for link in item.select("a[href]"):
            href = link.get("href") or ""
            if not href.startswith("https://") or "logout" in href: continue
            mirrors.append({
                "label": link.get_text(strip=True) or "Source",
                "url": href,
                "needs_resolve": "hubcloud." in href or "hubdrive." in href
            })
        if not mirrors: continue
        q = None
        for x in ("2160","1080","720","480","360"):
            if x in filename: q = f"{x}p"; break
        size_el = item.select_one(".badge-size, .badge")
        releases.append({
            "filename": filename, "quality": q,
            "size": size_el.get_text(strip=True) if size_el else None,
            "mirrors": mirrors
        })
    return releases

# ═══════════════════════════════════════════════════════════
#  UI — fresh cyber neon (not Walter clone)
# ═══════════════════════════════════════════════════════════
UI = r"""<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>StreamHub</title>
<link href="https://fonts.googleapis.com/css2?family=Syne:wght@600;700;800&family=IBM+Plex+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
*{margin:0;padding:0;box-sizing:border-box}
:root{--bg:#0a0b0f;--panel:#12141c;--line:#1e2230;--text:#e8eaf0;--mute:#7a8194;
--g:#39ff14;--c:#00e5ff;--m:#c77dff;--warn:#ff6b4a}
body{font-family:Syne,system-ui,sans-serif;background:var(--bg);color:var(--text);min-height:100vh;
background-image:
radial-gradient(ellipse 80% 50% at 20% -10%,rgba(57,255,20,.08),transparent),
radial-gradient(ellipse 60% 40% at 90% 10%,rgba(0,229,255,.07),transparent),
radial-gradient(ellipse 50% 30% at 50% 100%,rgba(199,125,255,.06),transparent)}
.wrap{max-width:1080px;margin:0 auto;padding:48px 20px 80px}
.top{display:flex;align-items:center;justify-content:space-between;margin-bottom:48px;flex-wrap:wrap;gap:16px}
.logo{font-size:1.75rem;font-weight:800;letter-spacing:-.04em}
.logo span{background:linear-gradient(135deg,var(--g),var(--c));-webkit-background-clip:text;-webkit-text-fill-color:transparent}
.pill{font-family:IBM Plex Mono,monospace;font-size:.7rem;padding:6px 12px;border:1px solid var(--line);
border-radius:999px;color:var(--mute);background:var(--panel)}
.hero{margin-bottom:40px}
.hero h1{font-size:clamp(1.8rem,5vw,2.6rem);font-weight:800;line-height:1.15;margin-bottom:12px;letter-spacing:-.03em}
.hero p{color:var(--mute);font-size:1.05rem;max-width:520px;line-height:1.5;font-weight:500}
.providers{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:16px;margin-bottom:36px}
.prov{background:var(--panel);border:1px solid var(--line);border-radius:16px;padding:22px;position:relative;overflow:hidden;transition:.25s}
.prov:hover{border-color:#2a3145;transform:translateY(-3px)}
.prov::before{content:"";position:absolute;top:0;left:0;right:0;height:2px;background:var(--accent,var(--g))}
.prov.mb{--accent:var(--g)}.prov.fk{--accent:var(--c)}.prov.ag{--accent:var(--m)}
.prov h3{font-size:1.1rem;margin-bottom:6px;display:flex;align-items:center;gap:8px}
.prov .tag{font-family:IBM Plex Mono,monospace;font-size:.65rem;color:var(--accent);border:1px solid color-mix(in srgb,var(--accent) 40%,transparent);padding:2px 8px;border-radius:6px}
.prov p{color:var(--mute);font-size:.88rem;line-height:1.45;margin-bottom:14px}
.ep{font-family:IBM Plex Mono,monospace;font-size:.72rem;background:#0d0f14;border:1px solid var(--line);padding:10px 12px;border-radius:10px;color:var(--c);margin-bottom:12px;word-break:break-all}
.btn{display:inline-flex;align-items:center;justify-content:center;gap:6px;padding:11px 16px;border-radius:10px;
background:var(--text);color:var(--bg);font-weight:700;font-size:.85rem;text-decoration:none;transition:.2s;border:none;cursor:pointer}
.btn:hover{opacity:.9;transform:scale(1.02)}
.btn.ghost{background:transparent;color:var(--text);border:1px solid var(--line)}
.btn.ghost:hover{border-color:var(--mute)}
.grid2{display:grid;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));gap:12px}
.card{background:var(--panel);border:1px solid var(--line);border-radius:14px;padding:18px}
.card h4{font-size:.95rem;margin-bottom:8px}
.card .ep{margin-bottom:10px}
.foot{margin-top:48px;text-align:center;color:var(--mute);font-size:.8rem;font-family:IBM Plex Mono,monospace}
</style></head><body>
<div class="wrap">
<div class="top">
<div class="logo">Stream<span>Hub</span></div>
<div class="pill">v4 · multi-provider</div>
</div>
<div class="hero">
<h1>One API.<br>Multiple sources.</h1>
<p>MovieBox mobile streams + 4KHDHub releases. Clean endpoints, playback headers included.</p>
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
<div class="card"><h4>Docs</h4><div class="ep">/docs</div><a class="btn ghost" href="/docs">Swagger UI</a></div>
<div class="card"><h4>Health</h4><div class="ep">/health</div><a class="btn ghost" href="/health">Check</a></div>
<div class="card"><h4>Movie stream</h4><div class="ep">/mb/stream/{id}?se=0&ep=0</div></div>
<div class="card"><h4>Series stream</h4><div class="ep">/mb/stream/{id}?se=1&ep=1</div></div>
</div>
<div class="foot">StreamHub · independent client · not affiliated</div>
</div></body></html>"""

@app.get("/", response_class=HTMLResponse)
async def root():
    return HTMLResponse(UI)

@app.get("/health")
async def health():
    return {"ok": True, "version": "4.0.0", "providers": ["moviebox", "4khdhub"]}

# ── MovieBox routes (/mb/...) ─────────────────────────────
@app.get("/mb/search")
async def mb_search(q: str = Query(..., min_length=1), page: int = 1):
    data = await mb_req("POST", "/wefeed-mobile-bff/subject-api/search/v2",
                        {"keyword": q, "page": page, "perPage": 20, "subjectType": 0})
    items, raw = [], []
    results = data.get("results") or []
    if results and isinstance(results[0], dict):
        raw = results[0].get("subjects") or []
    if not raw:
        raw = data.get("list") or data.get("items") or data.get("subjects") or []
    for s in raw:
        if isinstance(s, dict) and "subject" in s: s = s["subject"]
        if not isinstance(s, dict): continue
        sid = s.get("subjectId") or s.get("id")
        cover = s.get("cover") or {}
        items.append({
            "name": s.get("title") or s.get("name"),
            "subject_id": str(sid) if sid else None,
            "poster_url": cover.get("url") if isinstance(cover, dict) else s.get("coverUrl"),
            "year": (s.get("releaseDate") or "")[:4] or None,
            "type": "series" if (s.get("subjectType") or s.get("stype")) == 2 else "movie",
            "provider": "moviebox"
        })
    return {"provider": "moviebox", "query": q, "items": items}

@app.get("/mb/detail/{subject_id}")
async def mb_detail(subject_id: str):
    data = await mb_req("GET", f"/wefeed-mobile-bff/subject-api/get?subjectId={subject_id}")
    sub = data.get("subject") or data
    if (sub.get("subjectType") or sub.get("stype") or 1) == 2:
        try:
            sub["seasons"] = await mb_req("GET", f"/wefeed-mobile-bff/subject-api/season-info?subjectId={subject_id}")
        except: pass
    return {"provider": "moviebox", "data": sub}

@app.get("/mb/stream/{subject_id}")
async def mb_stream(subject_id: str, se: int = 0, ep: int = 0):
    if se == 0 and ep == 0:
        path = f"/wefeed-mobile-bff/subject-api/play-info/v2?subjectId={subject_id}"
    else:
        path = f"/wefeed-mobile-bff/subject-api/play-info/v2?subjectId={subject_id}&se={se}&ep={ep}"
    try:
        data = await mb_req("GET", path)
    except HTTPException:
        data = await mb_req("GET", path.replace("/play-info/v2", "/play-info"))

    sources = _parse_mb_streams(data if isinstance(data, dict) else {}, _mb_ua)
    # merge resource endpoint links (often MP4)
    extra = await _mb_resource_links(subject_id, se, ep)
    seen = {s["url"] for s in sources}
    for e in extra:
        if e["url"] not in seen:
            sources.append(e); seen.add(e["url"])

    return {
        "provider": "moviebox", "subject_id": subject_id, "se": se, "ep": ep,
        "count": len(sources), "sources": sources,
        "has_resource": len(sources) > 0,
        "note": None if sources else "No playable sources (try another id / episode)"
    }

@app.get("/mb/home")
async def mb_home(page: int = 1):
    data = await mb_req("GET", f"/wefeed-mobile-bff/tab-operating?page={page}&tabId=1&version=")
    return {"provider": "moviebox", "data": data}

# ── 4KHDHub routes (/fk/...) ──────────────────────────────
@app.get("/fk/search")
async def fk_search(q: str = Query(..., min_length=1)):
    html = await fk_get(f"?s={q}")
    items = fk_parse_search(html)
    for it in items: it["provider"] = "4khdhub"
    return {"provider": "4khdhub", "query": q, "items": items}

@app.get("/fk/detail")
async def fk_detail(id: str = Query(..., description="path id from search, e.g. /movie-name/")):
    html = await fk_get(id)
    soup = BeautifulSoup(html, "lxml")
    h1 = soup.select_one("h1")
    title = h1.get_text(strip=True) if h1 else id
    og = soup.select_one('meta[property="og:image"]')
    desc = soup.select_one(".content-section p.mt-4") or soup.select_one('meta[name="description"]')
    return {
        "provider": "4khdhub", "id": id, "title": title,
        "poster_url": og.get("content") if og else None,
        "description": desc.get_text(strip=True) if desc and desc.name == "p" else (desc.get("content") if desc else None),
        "type": "series" if "-series-" in id else "movie"
    }

@app.get("/fk/stream")
async def fk_stream(id: str = Query(...), se: int = 0, ep: int = 0):
    html = await fk_get(id)
    releases = fk_parse_releases(html, se, ep)
    return {
        "provider": "4khdhub", "id": id, "se": se, "ep": ep,
        "count": len(releases), "releases": releases,
        "note": "mirrors with needs_resolve=true require hubcloud/hubdrive resolve in player"
    }

# ── Aggregate ─────────────────────────────────────────────
@app.get("/search")
async def search_all(q: str = Query(..., min_length=1)):
    mb, fk = [], []
    try:
        mb = (await mb_search(q))["items"]
    except Exception as e:
        mb = [{"error": str(e)}]
    try:
        fk = (await fk_search(q))["items"]
    except Exception as e:
        fk = [{"error": str(e)}]
    return {"query": q, "moviebox": mb, "fourkhdhub": fk}

# legacy aliases so old clients still work
@app.get("/api/stream/{subject_id}")
async def legacy_stream(subject_id: str, se: int = 0, ep: int = 0, detail_path: str = ""):
    return await mb_stream(subject_id, se, ep)

@app.get("/search/legacy")
async def legacy_search(q: str = Query(...)):
    return await mb_search(q)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), reload=True)
