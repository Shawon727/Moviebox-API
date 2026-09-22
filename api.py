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
    from wasmtime import Store, Module, Instance, Engine
    _HAS_WASM = True
except Exception:
    _HAS_WASM = False
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
    version="5.28.0",
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
# HindiAnime (www.hindianime.site) — robust catalog + HLS stream
# Public JSON: /api/home-sections, /api/hero, /api/catalog, /api/browse-index.json
# Episodes: /extracted/{slug}.json  Stream: stream.hindianime.site/api/proxy/master.m3u8?hash=
# =============================================================================

HA_BASE = globals().get("HA_BASE") or "https://www.hindianime.site"
HA_STREAM = globals().get("HA_STREAM") or "https://stream.hindianime.site"
HA_HEADERS = globals().get("HA_HEADERS") or {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.hindianime.site/",
    "Origin": "https://www.hindianime.site",
}

_ha_catalog_cache: Dict[str, Any] = {"ts": 0, "data": None}
_ha_browse_cache: Dict[str, Any] = {"ts": 0, "data": None}


def _ha_slug_from_link(link: str) -> str:
    if not link:
        return ""
    return link.rstrip("/").split("/")[-1].strip()


def _ha_card(x: dict) -> dict:
    if not isinstance(x, dict):
        return {}
    link = x.get("link") or x.get("url") or x.get("perma_url") or ""
    title = x.get("title") or x.get("name") or ""
    poster = x.get("poster") or x.get("image") or x.get("thumb") or x.get("thumbnail") or ""
    slug = x.get("slug") or _ha_slug_from_link(link)
    return {
        "id": x.get("id") or slug or title,
        "title": title,
        "slug": slug,
        "link": link,
        "url": link,
        "poster": poster,
        "thumb": poster,
        "type": (x.get("type") or ("movie" if "/movie" in (link or "") else "series")).lower(),
        "episodes": x.get("episodes") or x.get("episode") or x.get("eps"),
        "rank": x.get("rank"),
        "genres": x.get("genres") or x.get("g") or [],
        "languages": x.get("languages") or x.get("lang") or [],
        "provider": "hindianime",
    }


async def _ha_get(path: str, params: Optional[dict] = None, timeout: float = 30.0) -> Any:
    base = globals().get("HA_BASE") or HA_BASE or "https://www.hindianime.site"
    headers = globals().get("HA_HEADERS") or HA_HEADERS
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        r = await client.get(base.rstrip("/") + path, params=params or {}, headers=headers)
        if r.status_code >= 400:
            raise HTTPException(502, f"HindiAnime {path} HTTP {r.status_code}")
        try:
            return r.json()
        except Exception:
            raise HTTPException(502, f"HindiAnime {path} non-JSON")


async def _ha_catalog(force: bool = False) -> dict:
    now = time.time()
    if not force and _ha_catalog_cache["data"] and now - _ha_catalog_cache["ts"] < 600:
        return _ha_catalog_cache["data"]
    data = await _ha_get("/api/catalog")
    _ha_catalog_cache["data"] = data
    _ha_catalog_cache["ts"] = now
    return data


async def _ha_browse(force: bool = False) -> dict:
    now = time.time()
    if not force and _ha_browse_cache["data"] and now - _ha_browse_cache["ts"] < 600:
        return _ha_browse_cache["data"]
    try:
        data = await _ha_get("/api/browse-index.json")
    except Exception:
        data = {"index": {}, "meta": {}}
    _ha_browse_cache["data"] = data
    _ha_browse_cache["ts"] = now
    return data


async def _ha_find_by_title(title: str) -> Optional[dict]:
    if not title:
        return None
    qn = title.lower().strip()
    cat = await _ha_catalog()
    best = None
    best_score = 0
    for x in list(cat.get("series") or []) + list(cat.get("movies") or []):
        if not isinstance(x, dict):
            continue
        xt = (x.get("title") or "").lower().strip()
        if not xt:
            continue
        if qn == xt:
            return x
        if qn in xt or xt in qn:
            score = 100 - abs(len(xt) - len(qn))
            if score > best_score:
                best_score = score
                best = x
    return best


async def _ha_extracted(slug: str) -> dict:
    slug = (slug or "").strip().strip("/")
    if not slug:
        raise HTTPException(400, "slug required")
    return await _ha_get(f"/extracted/{slug}.json")


def _ha_pick_episode(eps: list, season: int, episode: int) -> Optional[dict]:
    if not eps:
        return None
    if isinstance(eps, dict):
        eps = list(eps.values())
    pick = None
    for ep in eps:
        if not isinstance(ep, dict):
            continue
        try:
            if int(ep.get("season") or 1) == int(season) and int(ep.get("episode") or ep.get("number") or 1) == int(episode):
                return ep
        except Exception:
            continue
    for ep in eps:
        if isinstance(ep, dict):
            return ep
    return None



@app.get("/health", tags=["Meta"])
async def health():
    return {"ok": True, "version": "5.28.0", "creator": "shawon", "providers": ["hindianime", "ytmusic", "deezer", "jiosaavn", "tmdb"]}

@app.get("/anime/status", tags=["Anime"])
async def anime_status():
    """Health of HindiAnime catalog + stream tunnel."""
    out: Dict[str, Any] = {"provider": "hindianime", "ok": False}
    try:
        cat = await _ha_catalog()
        out["catalog"] = True
        out["series"] = cat.get("totalSeries") or len(cat.get("series") or [])
        out["movies"] = cat.get("totalMovies") or len(cat.get("movies") or [])
    except Exception as e:
        out["catalog"] = False
        out["catalog_error"] = str(e)[:100]
    # tunnel probe with known hash if possible
    try:
        async with httpx.AsyncClient(timeout=12.0, follow_redirects=True) as client:
            r = await client.get(
                f"{HA_STREAM}/api/proxy/master.m3u8?hash=36660e59856b4de58a219bcf4e27eba3",
                headers={"User-Agent": "Mozilla/5.0", "Referer": HA_BASE + "/", "Accept": "*/*"},
            )
            out["tunnel"] = r.status_code == 200 and (r.text or "").lstrip().startswith("#EXTM3U")
            out["tunnel_status"] = r.status_code
    except Exception as e:
        out["tunnel"] = False
        out["tunnel_error"] = str(e)[:80]
    out["ok"] = bool(out.get("catalog"))
    out["note"] = "Streaming via /anime/stream or /anime/hls"
    return out


@app.get("/anime/home", tags=["Anime"])
async def anime_home():
    """Home sections from HindiAnime."""
    data = await _ha_get("/api/home-sections")
    def ml(key):
        return [_ha_card(x) for x in (data.get(key) or []) if isinstance(x, dict)]
    posts = data.get("popularPosts") or {}
    return {
        "ok": True,
        "top_airing": ml("topAiring"),
        "most_popular": ml("mostPopular"),
        "completed": ml("completedSeries"),
        "latest_episodes": ml("latestEpisodes"),
        "latest_movies": ml("latestMovies"),
        "upcoming": ml("upcoming"),
        "popular_day": [_ha_card(x) for x in (posts.get("day") or []) if isinstance(x, dict)],
        "popular_week": [_ha_card(x) for x in (posts.get("week") or []) if isinstance(x, dict)],
        "genres": data.get("genres") or [],
        "provider": "hindianime",
    }


@app.get("/anime/hero", tags=["Anime"])
async def anime_hero():
    data = await _ha_get("/api/hero")
    items = [_ha_card(x) for x in (data.get("hero") or []) if isinstance(x, dict)]
    return {"ok": True, "items": items, "count": len(items), "provider": "hindianime"}


@app.get("/anime/spotlights", tags=["Anime"])
async def anime_spotlights():
    data = await _ha_get("/api/spotlights")
    items = [_ha_card(x) for x in (data.get("all") or data.get("spotlights") or []) if isinstance(x, dict)]
    return {
        "ok": True,
        "items": items,
        "is_custom": data.get("isCustom"),
        "rotation": data.get("rotationPeriodDays"),
        "provider": "hindianime",
    }


@app.get("/anime/top10", tags=["Anime"])
async def anime_top10():
    """Top airing as top10 (site /api/top10 often empty)."""
    data = await _ha_get("/api/home-sections")
    items = [_ha_card(x) for x in (data.get("topAiring") or data.get("mostPopular") or []) if isinstance(x, dict)]
    return {"ok": True, "items": items, "provider": "hindianime"}


@app.get("/anime/catalog", tags=["Anime"])
async def anime_catalog(
    type: Optional[str] = Query(None, description="series|movie|all"),
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=200),
):
    data = await _ha_catalog()
    series = [_ha_card(x) for x in (data.get("series") or []) if isinstance(x, dict)]
    movies = [_ha_card(x) for x in (data.get("movies") or []) if isinstance(x, dict)]
    t = (type or "all").lower()
    if t in ("series", "tv", "anime"):
        items = series
    elif t in ("movie", "movies", "film"):
        items = movies
    else:
        items = series + movies
    start = (page - 1) * limit
    chunk = items[start:start + limit]
    return {
        "ok": True,
        "type": t,
        "page": page,
        "limit": limit,
        "total": len(items),
        "total_series": data.get("totalSeries") or len(series),
        "total_movies": data.get("totalMovies") or len(movies),
        "items": chunk,
        "provider": "hindianime",
    }


@app.get("/anime/genres", tags=["Anime"])
async def anime_genres():
    """Genre list from home-sections + browse-index."""
    genres = []
    try:
        data = await _ha_get("/api/home-sections")
        genres = data.get("genres") or []
    except Exception:
        pass
    if not genres:
        browse = await _ha_browse()
        gset = set()
        for v in (browse.get("index") or {}).values():
            if isinstance(v, dict):
                for g in (v.get("g") or []):
                    gset.add(g)
        genres = sorted(gset)
    return {"ok": True, "genres": genres, "count": len(genres), "provider": "hindianime"}


@app.get("/anime/genre/{genre}", tags=["Anime"])
async def anime_by_genre(genre: str, limit: int = Query(60, ge=1, le=200)):
    """Filter catalog by genre using browse-index."""
    genre_l = genre.lower().strip()
    browse = await _ha_browse()
    cat = await _ha_catalog()
    # map link -> card
    by_link = {}
    for x in list(cat.get("series") or []) + list(cat.get("movies") or []):
        if isinstance(x, dict):
            link = x.get("link") or ""
            by_link[link] = x
            by_link[link.rstrip("/")] = x
    items = []
    for link, meta in (browse.get("index") or {}).items():
        if not isinstance(meta, dict):
            continue
        gs = [str(g).lower() for g in (meta.get("g") or [])]
        if genre_l in gs or any(genre_l in g for g in gs):
            src = by_link.get(link) or by_link.get(link.rstrip("/")) or {"title": _ha_slug_from_link(link).replace("-", " ").title(), "link": link, "type": meta.get("t")}
            card = _ha_card(src)
            card["genres"] = meta.get("g") or []
            card["languages"] = meta.get("lang") or []
            items.append(card)
            if len(items) >= limit:
                break
    return {"ok": True, "genre": genre, "count": len(items), "items": items, "provider": "hindianime"}


@app.get("/anime/search", tags=["Anime"])
async def anime_search(q: str = Query(..., min_length=1), limit: int = Query(60, ge=1, le=100)):
    """Search series + movies by title."""
    qn = q.strip().lower()
    data = await _ha_catalog()
    items = []
    seen = set()
    for x in list(data.get("series") or []) + list(data.get("movies") or []):
        if not isinstance(x, dict):
            continue
        title = (x.get("title") or "").lower()
        if qn in title or all(p in title for p in qn.split() if len(p) > 1):
            card = _ha_card(x)
            key = card.get("slug") or card.get("title")
            if key in seen:
                continue
            seen.add(key)
            items.append(card)
            if len(items) >= limit:
                break
    return {"ok": True, "query": q, "count": len(items), "items": items, "provider": "hindianime"}


@app.get("/anime/detail/{slug}", tags=["Anime"])
async def anime_detail(slug: str):
    """Full metadata + episode list with videoHash for each ep."""
    data = await _ha_extracted(slug)
    eps_raw = data.get("episodes") or []
    if isinstance(eps_raw, dict):
        eps_raw = list(eps_raw.values())
    episodes = []
    for ep in eps_raw:
        if not isinstance(ep, dict):
            continue
        episodes.append({
            "season": ep.get("season") or 1,
            "episode": ep.get("episode") or ep.get("number"),
            "title": ep.get("title"),
            "thumbnail": ep.get("thumbnail") or ep.get("thumb"),
            "url": ep.get("url"),
            "hash": ep.get("videoHash") or ep.get("hash"),
            "videoHash": ep.get("videoHash") or ep.get("hash"),
        })
    # group by season
    seasons: Dict[int, list] = {}
    for ep in episodes:
        try:
            s = int(ep.get("season") or 1)
        except Exception:
            s = 1
        seasons.setdefault(s, []).append(ep)
    return {
        "ok": True,
        "slug": slug,
        "title": data.get("title"),
        "type": data.get("type"),
        "overview": data.get("overview") or data.get("description"),
        "genres": data.get("genres") or [],
        "languages": data.get("languages") or [],
        "available_seasons": data.get("availableSeasons") or list(seasons.keys()),
        "seasons": data.get("seasons") or {str(k): len(v) for k, v in seasons.items()},
        "episodes": episodes,
        "episode_count": len(episodes),
        "provider": "hindianime",
    }


@app.get("/anime/episodes/{slug}", tags=["Anime"])
async def anime_episodes(
    slug: str,
    season: Optional[int] = Query(None),
):
    """Episode list only (optional season filter)."""
    detail = await anime_detail(slug)
    eps = detail.get("episodes") or []
    if season is not None:
        eps = [e for e in eps if int(e.get("season") or 1) == int(season)]
    return {"ok": True, "slug": slug, "title": detail.get("title"), "season": season, "count": len(eps), "episodes": eps, "provider": "hindianime"}


@app.get("/anime/trailer", tags=["Anime"])
async def anime_trailer(title: str = Query(..., min_length=1)):
    """YouTube trailer id for a title."""
    data = await _ha_get("/api/find-trailer", {"title": title})
    return {
        "ok": bool(data.get("id") or data.get("video")),
        "title": title,
        "youtube_id": data.get("id"),
        "embed": data.get("video") or (f"https://www.youtube.com/embed/{data['id']}" if data.get("id") else None),
        "provider": "hindianime",
    }


@app.get("/anime/stream", tags=["Anime"])
async def anime_stream(
    hash: Optional[str] = Query(None, description="videoHash"),
    url: Optional[str] = Query(None, description="master m3u8 url"),
    slug: Optional[str] = Query(None),
    title: Optional[str] = Query(None),
    tmdb_id: Optional[str] = Query(None),
    type: str = Query("series"),
    season: int = Query(1, ge=1),
    episode: int = Query(1, ge=1),
    request: Request = None,
):
    """Playable HLS for anime. Resolves hash from slug/title if needed.

    Returns play_url (proxied via /anime/hls) + direct stream_url + audio/quality list.
    """
    video_hash = hash
    resolved_title = title
    type_s = type if isinstance(type, str) else "series"
    is_movie = (type_s or "").lower() in ("movie", "movies", "film")

    if not video_hash and not url:
        if not slug and title:
            found = await _ha_find_by_title(title)
            if found:
                slug = _ha_slug_from_link(found.get("link") or "")
                resolved_title = found.get("title") or title
                is_movie = is_movie or ((found.get("type") or "").lower() == "movie")
        if slug:
            try:
                data = await _ha_extracted(slug)
                resolved_title = resolved_title or data.get("title")
                is_movie = is_movie or ((data.get("type") or "").lower() == "movie")
                pick = _ha_pick_episode(data.get("episodes") or [], season, episode)
                if pick:
                    video_hash = pick.get("videoHash") or pick.get("hash")
            except Exception as e:
                raise HTTPException(502, f"detail: {e}")
        if not video_hash and tmdb_id:
            # fallback to servers/cdn path
            try:
                srv = await anime_servers(
                    title=resolved_title, slug=slug, hash=None, tmdb_id=tmdb_id,
                    type="movie" if is_movie else "series", season=season, episode=episode, request=request,
                )
                if srv.get("hash"):
                    video_hash = srv["hash"]
                elif srv.get("cdn_streams"):
                    s0 = srv["cdn_streams"][0]
                    return {
                        "ok": True, "type": "cdn", "title": srv.get("title"),
                        "url": s0.get("url"), "play_url": s0.get("url"),
                        "stream_url": s0.get("url"), "servers": srv.get("servers"),
                        "provider": "vidsrc-cdn",
                    }
                elif srv.get("servers"):
                    return {
                        "ok": True, "type": "servers", "title": srv.get("title"),
                        "servers": srv["servers"],
                        "play_url": srv["servers"][0].get("play_url"),
                        "provider": "embeds",
                    }
            except Exception as e:
                raise HTTPException(502, f"tmdb resolve: {e}")
        if not video_hash and not url:
            raise HTTPException(404, "episode hash not found — try slug= or title=")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0",
        "Referer": HA_BASE + "/",
        "Origin": HA_BASE,
        "Accept": "*/*",
    }
    stream = url if url and ("m3u8" in url or "proxy" in url) else None
    master_body = ""
    host_used = None
    tunnel_ok = False
    errors: List[str] = []
    audio_tracks: List[dict] = []
    qualities: List[dict] = []

    if video_hash and not stream:
        for host in [HA_STREAM, HA_BASE]:
            candidate = f"{host.rstrip('/')}/api/proxy/master.m3u8?hash={video_hash}"
            try:
                async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
                    r = await client.get(candidate, headers=headers)
                    body = r.text or ""
                    if r.status_code == 200 and body.lstrip().startswith("#EXTM3U"):
                        stream = candidate
                        master_body = body
                        host_used = host
                        # parse audio + quality
                        lines = body.splitlines()
                        for i, line in enumerate(lines):
                            if line.startswith("#EXT-X-MEDIA:") and "TYPE=AUDIO" in line:
                                name = re.search(r'NAME="([^"]+)"', line)
                                lang = re.search(r'LANGUAGE="([^"]+)"', line)
                                uri = re.search(r'URI="([^"]+)"', line)
                                audio_tracks.append({
                                    "name": name.group(1) if name else "Audio",
                                    "language": lang.group(1) if lang else None,
                                    "uri": uri.group(1) if uri else None,
                                })
                            if line.startswith("#EXT-X-STREAM-INF"):
                                bw = re.search(r"BANDWIDTH=(\d+)", line)
                                res = re.search(r"RESOLUTION=(\d+x\d+)", line)
                                uri = lines[i + 1].strip() if i + 1 < len(lines) else ""
                                qualities.append({
                                    "bandwidth": int(bw.group(1)) if bw else None,
                                    "resolution": res.group(1) if res else None,
                                    "uri": uri,
                                })
                        # probe first sub-playlist
                        for q in qualities[:1]:
                            u = q.get("uri") or ""
                            full = host.rstrip("/") + u if u.startswith("/") else u
                            if full:
                                try:
                                    pr = await client.get(full, headers=headers)
                                    tunnel_ok = pr.status_code == 200 and (pr.text or "").lstrip().startswith("#EXTM3U")
                                except Exception as e:
                                    errors.append(str(e)[:80])
                        break
                    errors.append(f"{host} HTTP {r.status_code}")
            except Exception as e:
                errors.append(f"{host}: {type(e).__name__}")

    if not stream:
        raise HTTPException(502, {"error": "no playable stream", "hash": video_hash, "errors": errors})

    try:
        base = str(request.base_url).rstrip("/") if request is not None else ""
    except Exception:
        base = ""
    play_url = f"{base}/anime/hls?u={quote(stream, safe='')}" if base else f"/anime/hls?u={quote(stream, safe='')}"

    return {
        "ok": True,
        "hash": video_hash,
        "title": resolved_title,
        "slug": slug,
        "season": season,
        "episode": episode,
        "stream_url": stream,
        "play_url": play_url,
        "url": stream,
        "host": host_used,
        "tunnel_ok": tunnel_ok,
        "audio_tracks": audio_tracks,
        "qualities": qualities,
        "errors": errors or None,
        "provider": "hindianime",
        "note": "Play play_url with HLS.js / VLC. tunnel_ok=true means segments reachable.",
    }


@app.get("/anime/servers", tags=["Anime"])
async def anime_servers(
    title: Optional[str] = Query(None),
    slug: Optional[str] = Query(None),
    hash: Optional[str] = Query(None),
    tmdb_id: Optional[str] = Query(None),
    type: str = Query("series"),
    season: int = Query(1, ge=1),
    episode: int = Query(1, ge=1),
    request: Request = None,
):
    """Native HLS + embed servers for a title/episode."""
    is_movie = (type or "").lower() in ("movie", "movies", "film")
    resolved_title = title
    video_hash = hash
    tid = str(tmdb_id).strip() if tmdb_id else None

    if tid and not resolved_title:
        try:
            media = "movie" if is_movie else "tv"
            key = globals().get("TMDB_KEY") or globals().get("TMDB_API_KEY") or "b39176614e7ea6307888211ddd83549a"
            async with httpx.AsyncClient(timeout=12.0) as client:
                tr = await client.get(f"https://api.themoviedb.org/3/{media}/{tid}", params={"api_key": key})
                if tr.status_code == 200:
                    tj = tr.json()
                    resolved_title = tj.get("title") or tj.get("name")
        except Exception:
            pass

    if resolved_title and not slug and not video_hash:
        found = await _ha_find_by_title(resolved_title)
        if found:
            slug = _ha_slug_from_link(found.get("link") or "")
            resolved_title = found.get("title") or resolved_title

    if slug and not video_hash:
        try:
            data = await _ha_extracted(slug)
            resolved_title = resolved_title or data.get("title")
            pick = _ha_pick_episode(data.get("episodes") or [], season, episode)
            if pick:
                video_hash = pick.get("videoHash") or pick.get("hash")
        except Exception:
            pass

    servers: List[dict] = []
    tunnel_ok = False
    play_url = None
    stream_url = None

    if video_hash:
        try:
            st = await anime_stream(
                hash=video_hash, url=None, slug=None, title=None, tmdb_id=None,
                type="movie" if is_movie else "series",
                season=season, episode=episode, request=request,
            )
            tunnel_ok = bool(st.get("tunnel_ok"))
            stream_url = st.get("stream_url")
            play_url = st.get("play_url")
            if stream_url:
                servers.append({
                    "name": "HindiAnime HLS",
                    "type": "hls",
                    "url": stream_url,
                    "play_url": play_url or stream_url,
                    "hash": video_hash,
                    "working": tunnel_ok,
                    "audio_tracks": st.get("audio_tracks") or [],
                    "qualities": st.get("qualities") or [],
                    "provider": "hindianime",
                })
                servers.append({
                    "name": "HLS Proxy",
                    "type": "hls",
                    "url": play_url or stream_url,
                    "play_url": play_url or stream_url,
                    "working": True,
                    "provider": "proxy",
                })
        except Exception as e:
            # still expose direct master URL
            master = f"{HA_STREAM}/api/proxy/master.m3u8?hash={video_hash}"
            try:
                base = str(request.base_url).rstrip("/") if request is not None else ""
            except Exception:
                base = ""
            play_url = f"{base}/anime/hls?u={quote(master, safe='')}" if base else f"/anime/hls?u={quote(master, safe='')}"
            servers.append({
                "name": "HindiAnime HLS",
                "type": "hls",
                "url": master,
                "play_url": play_url,
                "hash": video_hash,
                "working": True,
                "error": str(e)[:100],
                "provider": "hindianime",
            })

    if tid:
        if is_movie:
            embeds = [
                ("VidSrc", f"https://vidsrc.xyz/embed/movie/{tid}"),
                ("VidSrc.to", f"https://vidsrc.to/embed/movie/{tid}"),
                ("VidLink", f"https://vidlink.pro/movie/{tid}"),
                ("2Embed", f"https://www.2embed.cc/embed/{tid}"),
                ("AutoEmbed", f"https://autoembed.co/movie/tmdb/{tid}"),
                ("VidKing", f"https://www.vidking.net/embed/movie/{tid}"),
            ]
        else:
            embeds = [
                ("VidSrc", f"https://vidsrc.xyz/embed/tv/{tid}/{season}/{episode}"),
                ("VidSrc.to", f"https://vidsrc.to/embed/tv/{tid}/{season}/{episode}"),
                ("VidLink", f"https://vidlink.pro/tv/{tid}/{season}/{episode}"),
                ("2Embed", f"https://www.2embed.cc/embedtv/{tid}&s={season}&e={episode}"),
                ("AutoEmbed", f"https://autoembed.co/tv/tmdb/{tid}-{season}-{episode}"),
                ("VidKing", f"https://www.vidking.net/embed/tv/{tid}/{season}/{episode}"),
            ]
        for name, u in embeds:
            servers.append({"name": name, "type": "embed", "url": u, "play_url": u, "working": True, "provider": name.lower()})

    cdn_streams = []
    if tid:
        try:
            cdn = await _vidsrc_resolve_cdn(str(tid), media_type="movie" if is_movie else "tv", season=season, episode=episode)
            for s in (cdn.get("streams") or [])[:6]:
                if s.get("url"):
                    cdn_streams.append(s)
                    servers.append({
                        "name": s.get("label") or "CDN",
                        "type": s.get("type") or "hls",
                        "url": s.get("url"),
                        "play_url": s.get("url"),
                        "working": True,
                        "provider": "vidsrc-cdn",
                    })
        except Exception:
            pass

    return {
        "ok": bool(servers),
        "title": resolved_title,
        "slug": slug,
        "hash": video_hash,
        "tmdb_id": tid,
        "type": "movie" if is_movie else "series",
        "season": season,
        "episode": episode,
        "tunnel_ok": tunnel_ok,
        "stream_url": stream_url,
        "play_url": play_url,
        "servers": servers,
        "cdn_streams": cdn_streams,
        "count": len(servers),
        "provider": "hindianime+embeds",
    }



@app.get("/anime/hls", tags=["Anime"])
async def anime_hls_proxy(request: Request, u: str = Query(..., min_length=8)):
    """Proxy m3u8 + segments (CORS). Unwraps HindiAnime segment?url= to CDN when needed."""
    if not (u.startswith("http://") or u.startswith("https://")):
        raise HTTPException(400, "url must be http(s)")

    def _ha_force_stream_host(url: str) -> str:
        """www returns SPA HTML for sub/segment; stream.* returns real media."""
        try:
            p = urlparse(url)
            host = (p.hostname or "").lower()
            if host in ("www.hindianime.site", "hindianime.site") and "/api/proxy/" in (p.path or ""):
                return "https://stream.hindianime.site" + (p.path or "") + (("?" + p.query) if p.query else "")
            return url
        except Exception:
            return url

    # Do NOT unwrap segment→CDN (zn-grid returns 403 without their edge cookies).
    # Always fetch via stream.hindianime.site proxy paths.
    target = _ha_force_stream_host(u)
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
                or (r.status_code == 410)
            ):
                alts = []
                # force stream host
                forced = _ha_force_stream_host(u)
                if forced != target:
                    alts.append(forced)
                if "www.hindianime.site" in target:
                    alts.append(target.replace("www.hindianime.site", "stream.hindianime.site"))
                if target != u:
                    alts.append(u)
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
        detail = f"upstream {r.status_code}"
        if r.status_code in (530, 1033, 502, 503):
            detail += " — HindiAnime stream tunnel may be offline (Cloudflare 1033). Retry later."
        raise HTTPException(502, detail)

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
                            full_u = "https://stream.hindianime.site" + uri
                        else:
                            full_u = urljoin(_base, uri)
                        full_u = _ha_force_stream_host(full_u)
                        return 'URI="/anime/hls?u=' + quote(full_u, safe="") + '"'
                    line = re.sub(r'URI="([^"]+)"', _rew, line)
                lines.append(line)
                continue
            if raw.startswith("http://") or raw.startswith("https://"):
                full_u = raw
            elif raw.startswith("/"):
                full_u = "https://stream.hindianime.site" + raw
            else:
                full_u = urljoin(base, raw)
            full_u = _ha_force_stream_host(full_u)
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



@app.get("/anime/cdn", tags=["Anime"])
async def anime_cdn(
    tmdb_id: Optional[str] = Query(None),
    title: Optional[str] = Query(None),
    type: str = Query("series", description="series|movie"),
    season: int = Query(1, ge=1),
    episode: int = Query(1, ge=1),
):
    """Direct CDN m3u8 links (VidSrc data API + WASM decrypt + host token).

    Prefer this over iframe embeds when you need real playable HLS URLs for VLC / native players.
    """
    is_movie = (type or "").lower() in ("movie", "movies", "film")
    tid = tmdb_id
    if not tid and title:
        tid = await _ha_tmdb_id(title, is_movie=is_movie)
    if not tid:
        raise HTTPException(400, "tmdb_id or title required")
    result = await _vidsrc_resolve_cdn(
        str(tid),
        media_type="movie" if is_movie else "tv",
        season=season,
        episode=episode,
    )
    result["creator"] = "shawon"
    return result


@app.get("/play/cdn", tags=["Play"])
async def play_cdn(
    tmdb_id: Optional[str] = Query(None),
    title: Optional[str] = Query(None),
    type: str = Query("movie"),
    season: int = Query(1, ge=1),
    episode: int = Query(1, ge=1),
):
    """Alias of /anime/cdn for movies & series (same resolver)."""
    return await anime_cdn(tmdb_id=tmdb_id, title=title, type=type, season=season, episode=episode)

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



async def _tikwm_extract(url: str) -> dict:
    """TikTok multi-source: TikWM → iosint → yt-dlp fallback. Unlimited, no API key."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept": "application/json",
    }
    formats = []
    meta: Dict[str, Any] = {}
    errors = []

    # resolve short links
    resolved = url
    try:
        async with httpx.AsyncClient(timeout=12.0, follow_redirects=True, headers=headers) as client:
            r = await client.head(url)
            resolved = str(r.url)
    except Exception:
        try:
            async with httpx.AsyncClient(timeout=12.0, follow_redirects=True, headers=headers) as client:
                r = await client.get(url)
                resolved = str(r.url)
        except Exception:
            pass

    # 1) TikWM
    for api_url, params in [
        ("https://www.tikwm.com/api/", {"url": resolved, "hd": 1}),
        ("https://tikwm.com/api/", {"url": resolved, "hd": 1}),
        ("https://www.tikwm.com/api/", {"url": url, "hd": 1}),
    ]:
        try:
            async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
                r = await client.get(
                    api_url,
                    params=params,
                    headers={**headers, "Referer": "https://www.tikwm.com/"},
                )
                if r.status_code != 200:
                    continue
                j = r.json()
                if j.get("code") != 0:
                    errors.append(j.get("msg") or "tikwm")
                    continue
                d = j.get("data") or {}
                meta = {
                    "title": d.get("title"),
                    "author": (d.get("author") or {}).get("nickname") if isinstance(d.get("author"), dict) else d.get("author"),
                    "thumbnail": d.get("cover") or d.get("origin_cover"),
                    "duration": d.get("duration"),
                }
                if d.get("hdplay"):
                    formats.append({"id": "hd", "label": "HD · no watermark", "ext": "mp4", "kind": "video+audio", "muxed": True, "url": d["hdplay"]})
                if d.get("play"):
                    formats.append({"id": "play", "label": "Standard · no watermark", "ext": "mp4", "kind": "video+audio", "muxed": True, "url": d["play"]})
                if d.get("wmplay"):
                    formats.append({"id": "wm", "label": "With watermark", "ext": "mp4", "kind": "video+audio", "muxed": True, "url": d["wmplay"]})
                if d.get("music"):
                    formats.append({"id": "music", "label": "Audio", "ext": "mp3", "kind": "audio", "muxed": False, "url": d["music"]})
                if formats:
                    break
        except Exception as e:
            errors.append(f"tikwm:{type(e).__name__}")

    # 2) tiktok oembed + third party mirror
    if not formats:
        try:
            async with httpx.AsyncClient(timeout=15.0, follow_redirects=True, headers=headers) as client:
                r = await client.get("https://www.tiktok.com/oembed", params={"url": resolved})
                if r.status_code == 200:
                    oj = r.json()
                    meta.setdefault("title", oj.get("title"))
                    meta.setdefault("author", oj.get("author_name"))
                    meta.setdefault("thumbnail", oj.get("thumbnail_url"))
        except Exception:
            pass
        # try public downloader API mirrors
        for mirror in [
            f"https://api.tikmate.app/api/lookup?url={resolved}",
            f"https://api.tikdown.org/api/?url={resolved}",
        ]:
            try:
                async with httpx.AsyncClient(timeout=12.0, verify=False) as client:
                    r = await client.get(mirror, headers=headers)
                    if r.status_code != 200:
                        continue
                    j = r.json()
                    for key in ("nwm_video_url", "video", "download", "play", "url", "hdplay"):
                        val = j.get(key) or (j.get("data") or {}).get(key)
                        if isinstance(val, str) and val.startswith("http"):
                            formats.append({"id": key, "label": key, "ext": "mp4", "kind": "video+audio", "muxed": True, "url": val})
                    if formats:
                        break
            except Exception as e:
                errors.append(str(e)[:40])

    if formats:
        best = formats[0]["url"]
        return {
            "ok": True,
            "provider": "tiktok",
            "title": meta.get("title"),
            "thumbnail": meta.get("thumbnail"),
            "duration": meta.get("duration"),
            "formats": formats,
            "url": best,
            "directUrl": best,
            "download_url": best,
            "webpage_url": resolved,
            "extractor": "tikwm-multi",
        }
    return {"ok": False, "error": "; ".join(errors) or "tiktok extract failed", "extractor": "tikwm-multi"}



async def _piped_yt_streams(video_id: str) -> dict:
    """Fetch YouTube streams from multiple Piped instances (CDN URLs)."""
    bases = [
        "https://pipedapi.kavin.rocks",
        "https://pipedapi.leptons.xyz",
        "https://pipedapi.adminforge.de",
        "https://pipedapi.ducks.party",
        "https://api.piped.private.coffee",
        "https://pipedapi.nosebs.ru",
        "https://pipedapi.drgns.space",
        "https://pipedapi.owo.si",
        "https://piped-api.codespace.cz",
        "https://pipedapi.reallyaweso.me",
        "https://api.piped.yt",
    ]
    errors = []
    async with httpx.AsyncClient(timeout=14.0, follow_redirects=True, headers={"User-Agent": "StreamHub/5.14"}) as client:
        for base in bases:
            try:
                r = await client.get(f"{base}/streams/{video_id}")
                if r.status_code != 200:
                    errors.append(f"{base}:{r.status_code}")
                    continue
                j = r.json()
                formats = []
                for vs in (j.get("videoStreams") or []):
                    u = vs.get("url")
                    if not u:
                        continue
                    q = str(vs.get("quality") or "")
                    h = int(re.sub(r"\D", "", q) or 0) or None
                    formats.append({
                        "id": str(vs.get("itag") or q),
                        "label": f"{q} · {(vs.get('format') or 'mp4').upper()}" + (" · video-only" if vs.get("videoOnly") else " · 🔊"),
                        "ext": (vs.get("format") or "mp4").lower(),
                        "height": h,
                        "kind": "video" if vs.get("videoOnly") else "video+audio",
                        "muxed": not vs.get("videoOnly"),
                        "url": u,
                    })
                for a in (j.get("audioStreams") or [])[:6]:
                    if a.get("url"):
                        formats.append({
                            "id": str(a.get("itag") or "a"),
                            "label": f"audio · {a.get('quality') or a.get('bitrate') or ''}",
                            "ext": (a.get("format") or "m4a").lower(),
                            "kind": "audio",
                            "url": a["url"],
                        })
                if not formats:
                    errors.append(f"{base}:empty")
                    continue
                # prefer muxed then height
                formats.sort(key=lambda f: (0 if f.get("muxed") else 1, -(f.get("height") or 0)))
                return {
                    "ok": True,
                    "title": j.get("title") or f"YouTube {video_id}",
                    "id": video_id,
                    "extractor": f"piped/{base.split('//')[1]}",
                    "duration": j.get("duration"),
                    "thumbnail": j.get("thumbnailUrl"),
                    "webpage_url": f"https://www.youtube.com/watch?v={video_id}",
                    "formats": formats,
                    "format_count": len(formats),
                    "best": formats[0],
                    "best_muxed": next((f for f in formats if f.get("muxed")), None),
                    "best_audio": next((f for f in formats if f.get("kind") == "audio"), None),
                    "download_url": formats[0]["url"],
                    "hls": j.get("hls"),
                    "dash": j.get("dash"),
                    "note": "Direct stream via Piped",
                }
            except Exception as e:
                errors.append(f"{base}:{type(e).__name__}")
                continue
    return {"ok": False, "error": "piped all failed", "detail": errors[:6]}


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



def _resolve_ytdlp_cookies() -> Optional[str]:
    """Return path to a Netscape cookies.txt usable by yt-dlp, or None.

    Search order:
      1. env YTDLP_COOKIES / YTDLP_COOKIES_FILE (file path)
      2. env YTDLP_COOKIES_B64 (base64 of cookies.txt contents) → temp file
      3. ./cookies.txt (cwd)
      4. next to this api.py
      5. /app/cookies.txt / /var/task/cookies.txt (Vercel/serverless common)
    """
    import tempfile
    candidates = []
    for key in ("YTDLP_COOKIES", "YTDLP_COOKIES_FILE"):
        v = (os.environ.get(key) or "").strip()
        if v:
            candidates.append(v)
    # relative + absolute common locations
    here = os.path.dirname(os.path.abspath(__file__)) if "__file__" in dir() else os.getcwd()
    candidates.extend([
        os.path.join(os.getcwd(), "cookies.txt"),
        os.path.join(here, "cookies.txt"),
        "/app/cookies.txt",
        "/var/task/cookies.txt",
        "/home/workdir/artifacts/cookies.txt",
    ])
    for path in candidates:
        try:
            if path and os.path.isfile(path) and os.path.getsize(path) > 50:
                # skip pure template (no real session cookies)
                with open(path, "r", encoding="utf-8", errors="ignore") as f:
                    body = f.read()
                if "# TEMPLATE" in body or "REPLACE_ME" in body:
                    continue
                real = 0
                for line in body.splitlines():
                    s = line.strip()
                    if not s or s.startswith("#"):
                        continue
                    parts = s.split("	")
                    if len(parts) >= 7 and "youtube" in parts[0].lower():
                        if parts[5] in ("LOGIN_INFO", "SID", "__Secure-1PSID", "__Secure-3PSID", "SAPISID", "APISID", "HSID", "SSID"):
                            real += 1
                if real >= 1:
                    return path
        except Exception:
            continue
    b64 = (os.environ.get("YTDLP_COOKIES_B64") or "").strip()
    if b64:
        try:
            raw = base64.b64decode(b64)
            if len(raw) > 50:
                tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".txt", prefix="ytcookies_")
                tmp.write(raw)
                tmp.close()
                return tmp.name
        except Exception:
            pass
    return None


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
        "socket_timeout": 22,
        "retries": 2,
        "fragment_retries": 3,
    }
    # Cookies (bot-check bypass on Vercel/datacenter IPs)
    # Priority: YTDLP_COOKIES path → YTDLP_COOKIES_FILE → YTDLP_COOKIES_B64 → ./cookies.txt → /app/cookies.txt
    cookie_file = _resolve_ytdlp_cookies()
    if cookie_file:
        base_opts["cookiefile"] = cookie_file

    info = None
    last_err = None
    used_client = None
    if is_yt:
        # Multi-client in one call often recovers more formats; then try singles
        # Keep list short — Vercel/serverless time limits; these clients return CDN most often
        clients_batches = [
            ["android_creator"],
            ["mediaconnect"],
            ["android_creator", "mediaconnect"],
            ["android"],
        ]
        attempts = []
        for batch in clients_batches:
            attempts.append((
                {
                    **base_opts,
                    "extractor_args": {
                        "youtube": {
                            "player_client": batch,
                            "player_skip": ["webpage"],
                        }
                    },
                },
                "+".join(batch),
            ))
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
                "YouTube blocked direct CDN on this server IP for this video. "
                "Set env YTDLP_COOKIES_FILE or YTDLP_COOKIES_B64 (Netscape cookies.txt) on the host, "
                "or use embed_url. Some videos still work without cookies."
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


async def _paxsenix_proxy(path: str, params: dict) -> Optional[dict]:
    """Optional fallback if PAXSENIX_KEY env is set — native extractors always tried first."""
    key = (os.environ.get("PAXSENIX_KEY") or "").strip()
    if not key:
        return None
    try:
        async with httpx.AsyncClient(timeout=45.0) as client:
            r = await client.get(
                f"https://api.paxsenix.org{path}",
                params=params,
                headers={"Authorization": f"Bearer {key}", "Accept": "application/json"},
            )
            if r.status_code == 200:
                j = r.json()
                if j.get("ok") or j.get("status") == "done":
                    return j
    except Exception:
        pass
    return None


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
    except Exception:
        pass

    # TikTok → TikWM first (reliable CDN)
    if any(x in low for x in ("tiktok.com", "vm.tiktok.com", "vt.tiktok.com", "tiktok.com/t/")):
        try:
            tw = await _tikwm_extract(url)
            if tw.get("ok") and tw.get("formats"):
                data = tw
                # jump to format assembly below via fall-through after setting data
            else:
                data = await asyncio.to_thread(_ytdlp_info, url)
        except Exception:
            data = await asyncio.to_thread(_ytdlp_info, url)
    else:
        data = await asyncio.to_thread(_ytdlp_info, url)
    # YouTube: if yt-dlp gave embed-only / failed → Piped multi-instance CDN
    if data.get("ok") and data.get("mode") == "embed":
        data["ok"] = False
    yt_id = _extract_youtube_id(url)
    if yt_id and (not data.get("ok") or not any(
        "googlevideo" in (f.get("url") or "") for f in (data.get("formats") or [])
    )):
        try:
            piped = await _piped_yt_streams(yt_id)
            if piped.get("ok") and piped.get("formats"):
                data = piped
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
    "https://yewtu.be",
    "https://invidious.nerdvpn.de",
    "https://invidious.privacyredirect.com",
    "https://inv.tux.pizza",
    "https://invidious.flokinet.to",
    "https://iv.ggtyler.dev",
    "https://invidious.protokolos.eu",
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



@app.get("/yt/savetube", tags=["YouTube"])
async def yt_savetube(
    url: str = Query(..., min_length=8),
    quality: str = Query("720", description="144|240|360|480|720|1080|mp3"),
):
    """YouTube download (native, unlimited) — PaxSenix /yt/savetube compatible shape."""
    q = (quality or "720").lower().strip()
    if q == "mp3":
        res = await _dl_platform(url, "ytmp3", prefer="audio")
        res["format"] = "mp3"
    else:
        res = await _dl_platform(url, "ytmp4", prefer="video")
        res["requested_quality"] = q
    res["provider"] = "savetube-native"
    return res


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
    """YouTube / YT Music search — Invidious when up, else YT Music inner API."""
    items = []
    provider = None
    # 1) Invidious
    try:
        data = await _invidious_get("/api/v1/search", {"q": q, "type": "video", "page": page})
        if isinstance(data, list):
            for it in data:
                if not isinstance(it, dict):
                    continue
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
            if items:
                provider = "invidious"
    except Exception:
        pass
    # 2) YT Music
    if not items:
        try:
            data = await _ytm_post("search", {"query": q})
            raw = []
            _ytm_walk(data, "musicResponsiveListItemRenderer", raw)
            seen = set()
            for it in raw:
                e = _ytm_parse_item(it)
                if e and e.get("video_id") and e["video_id"] not in seen:
                    seen.add(e["video_id"])
                    items.append({
                        "id": f"yt:{e['video_id']}",
                        "video_id": e["video_id"],
                        "title": e.get("title"),
                        "artist": e.get("artist"),
                        "thumb": e.get("thumb"),
                        "duration": e.get("duration"),
                        "provider": "ytmusic",
                    })
            provider = "ytmusic"
        except Exception:
            pass
    return {"ok": True, "query": q, "count": len(items), "items": items, "provider": provider or "none"}



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


async def _loader_to_youtube(url_or_id: str, fmt: str = "mp3") -> dict:
    """Public loader.to → savenow.to CDN (works when InnerTube blocked on datacenter IPs)."""
    vid = _extract_youtube_id(url_or_id) or (url_or_id if re.match(r"^[\w-]{6,20}$", (url_or_id or "").strip()) else None)
    if not vid:
        return {"ok": False, "error": "invalid youtube"}
    watch = f"https://www.youtube.com/watch?v={vid}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Referer": "https://loader.to/",
    }
    try:
        async with httpx.AsyncClient(timeout=45.0, follow_redirects=True) as client:
            r = await client.get(
                "https://loader.to/ajax/download.php",
                params={"format": fmt, "url": watch},
                headers=headers,
            )
            if r.status_code != 200:
                return {"ok": False, "error": f"loader start HTTP {r.status_code}"}
            j = r.json()
            prog = j.get("progress_url")
            title = (j.get("info") or {}).get("title") or j.get("title")
            thumb = (j.get("info") or {}).get("image") or j.get("thumbnail_url")
            if not prog:
                return {"ok": False, "error": "no progress_url", "raw": str(j)[:200]}
            download_url = None
            for _ in range(12):
                await asyncio.sleep(1.2)
                r2 = await client.get(prog, headers=headers)
                if r2.status_code != 200:
                    continue
                j2 = r2.json()
                download_url = j2.get("download_url") or j2.get("url")
                if download_url:
                    break
                # progress 1000 = done on some mirrors
                if int(j2.get("progress") or 0) >= 1000 and j2.get("text"):
                    # sometimes URL in text
                    m = re.search(r"https://[^\s\"']+", str(j2.get("text")))
                    if m:
                        download_url = m.group(0)
                        break
            if not download_url:
                return {"ok": False, "error": "loader timeout", "title": title}
            return {
                "ok": True,
                "video_id": vid,
                "title": title,
                "thumb": thumb or f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg",
                "url": download_url,
                "directUrl": download_url,
                "audio_url": download_url if fmt in ("mp3", "m4a", "audio") else None,
                "provider": "loader.to/savenow",
                "format": fmt,
            }
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}:{e}"}


async def _innertube_player(video_id: str) -> dict:
    """Native YouTube InnerTube player (ANDROID/IOS) — direct googlevideo CDN, no yt-dlp.

    Returns {ok, title, thumb, duration, audio_streams, video_streams, formats, provider}
    """
    video_id = (video_id or "").strip()
    if not re.match(r"^[\w-]{6,20}$", video_id):
        return {"ok": False, "error": "invalid video id"}

    clients = [
        {
            "name": "ANDROID",
            "clientName": "ANDROID",
            "clientVersion": "20.10.38",
            "api_key": "AIzaSyA8eiZmM1FaDVjRy-df2KTyQ_vz_yYM39w",
            "ua": "com.google.android.youtube/20.10.38 (Linux; U; Android 14)",
            "client_name_hdr": "3",
        },
        {
            "name": "IOS",
            "clientName": "IOS",
            "clientVersion": "20.10.4",
            "api_key": "AIzaSyB-63vPrdThhKuerbB2N_l7Kwwcxj6yUAc",
            "ua": "com.google.ios.youtube/20.10.4 (iPhone16,2; U; CPU iOS 17_5 like Mac OS X)",
            "client_name_hdr": "5",
        },
        {
            "name": "TVHTML5",
            "clientName": "TVHTML5_SIMPLY_EMBEDDED_PLAYER",
            "clientVersion": "2.0",
            "api_key": "AIzaSyAO_FJ2SlqU8Q4STEHLGCilw_Y9_11qcW8",
            "ua": "Mozilla/5.0 (ChromiumStylePlatform) Cobalt/Version",
            "client_name_hdr": "85",
        },
    ]

    last_err = None
    async with httpx.AsyncClient(timeout=28.0, follow_redirects=True) as client:
        for c in clients:
            body = {
                "context": {
                    "client": {
                        "clientName": c["clientName"],
                        "clientVersion": c["clientVersion"],
                        "hl": "en",
                        "gl": "US",
                        "androidSdkVersion": 34,
                    }
                },
                "videoId": video_id,
                "contentCheckOk": True,
                "racyCheckOk": True,
            }
            url = f"https://www.youtube.com/youtubei/v1/player?key={c['api_key']}"
            headers = {
                "Content-Type": "application/json",
                "User-Agent": c["ua"],
                "X-YouTube-Client-Name": c["client_name_hdr"],
                "X-YouTube-Client-Version": c["clientVersion"],
                "Origin": "https://www.youtube.com",
                "Referer": f"https://www.youtube.com/watch?v={video_id}",
            }
            try:
                r = await client.post(url, json=body, headers=headers)
                if r.status_code != 200:
                    last_err = f"{c['name']} HTTP {r.status_code}"
                    continue
                j = r.json()
                status = ((j.get("playabilityStatus") or {}).get("status") or "").upper()
                if status not in ("OK", "LIVE_STREAM"):
                    last_err = f"{c['name']} playability={status}"
                    continue
                sd = j.get("streamingData") or {}
                raw = list(sd.get("formats") or []) + list(sd.get("adaptiveFormats") or [])
                if not raw:
                    last_err = f"{c['name']} no formats"
                    continue
                details = j.get("videoDetails") or {}
                title = details.get("title")
                thumb = f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg"
                try:
                    thumbs = (details.get("thumbnail") or {}).get("thumbnails") or []
                    if thumbs:
                        thumb = thumbs[-1].get("url") or thumb
                except Exception:
                    pass
                duration = None
                try:
                    duration = int(details.get("lengthSeconds") or 0) or None
                except Exception:
                    pass

                audio_streams = []
                video_streams = []
                formats = []
                for f in raw:
                    u = f.get("url")
                    if not u:
                        # signatureCipher needs decipher — skip for pure-URL clients
                        continue
                    mime = (f.get("mimeType") or "").split(";")[0].strip()
                    itag = f.get("itag")
                    br = f.get("bitrate") or f.get("averageBitrate")
                    entry = {
                        "itag": itag,
                        "label": f.get("qualityLabel") or (f"{br}bps" if br else str(itag)),
                        "url": u,
                        "mime": mime,
                        "bitrate": br,
                        "height": f.get("height"),
                        "width": f.get("width"),
                        "fps": f.get("fps"),
                        "contentLength": f.get("contentLength"),
                    }
                    formats.append(entry)
                    if mime.startswith("audio/"):
                        audio_streams.append({
                            **entry,
                            "format": "AUDIO",
                            "type": mime,
                        })
                    elif mime.startswith("video/") or "video" in mime:
                        # progressive (audio+video) vs adaptive
                        codecs = (f.get("mimeType") or "")
                        has_audio = "mp4a" in codecs or "opus" in codecs or "vorbis" in codecs
                        video_streams.append({
                            **entry,
                            "format": "MP4" if has_audio and f.get("height") else "VIDEO",
                            "type": mime,
                            "progressive": bool(has_audio and f.get("height")),
                        })

                if not audio_streams and not video_streams:
                    last_err = f"{c['name']} urls empty (cipher only)"
                    continue

                audio_streams.sort(key=lambda x: int(x.get("bitrate") or 0), reverse=True)
                video_streams.sort(key=lambda x: int(x.get("height") or 0), reverse=True)
                return {
                    "ok": True,
                    "video_id": video_id,
                    "title": title,
                    "thumb": thumb,
                    "thumbnail": thumb,
                    "duration": duration,
                    "audio_streams": audio_streams,
                    "video_streams": video_streams,
                    "formats": formats,
                    "provider": f"innertube/{c['name'].lower()}",
                    "audio_url": audio_streams[0]["url"] if audio_streams else None,
                    "video_url": next((v["url"] for v in video_streams if v.get("progressive")), None)
                    or (video_streams[0]["url"] if video_streams else None),
                }
            except Exception as e:
                last_err = f"{c['name']}:{type(e).__name__}:{e}"
                continue
    return {"ok": False, "error": last_err or "innertube failed", "video_id": video_id}


async def music_yt_play(video_id: str):
    """Stream info for YouTube / YT Music — InnerTube CDN first (no yt-dlp)."""
    video_id = (video_id or "").replace("yt:", "").strip()
    if not re.match(r"^[\w-]{6,20}$", video_id):
        raise HTTPException(400, "invalid video id")
    watch = f"https://www.youtube.com/watch?v={video_id}"
    errors: List[str] = []

    # 1) Native InnerTube (ANDROID/IOS) — direct googlevideo
    data = await _innertube_player(video_id)
    if data.get("ok"):
        audio_streams = data.get("audio_streams") or []
        video_streams = data.get("video_streams") or []
        best_audio = data.get("audio_url")
        best_video = data.get("video_url")
        sources = []
        if best_audio:
            sources.append({"type": "audio", "provider": data["provider"], "label": "Best audio", "url": best_audio, "play_url": best_audio, "format": "AUDIO"})
        if best_video:
            sources.append({"type": "video", "provider": data["provider"], "label": "Best video", "url": best_video, "play_url": best_video, "format": "VIDEO"})
        return {
            "ok": True,
            "video_id": video_id,
            "title": data.get("title"),
            "thumb": data.get("thumb"),
            "thumbnail": data.get("thumb"),
            "duration": data.get("duration"),
            "audio_url": best_audio,
            "video_url": best_video,
            "url": best_audio or best_video,
            "directUrl": best_audio or best_video,
            "audio_streams": audio_streams[:12],
            "video_streams": video_streams[:12],
            "sources": sources,
            "watch_url": watch,
            "download_url": watch,
            "provider": data.get("provider"),
            "note": "Direct googlevideo CDN via InnerTube. Tokens expire ~6h — re-fetch when needed.",
        }
    errors.append(str(data.get("error") or "innertube"))

    # 2) loader.to → savenow CDN (works on many datacenter IPs)
    try:
        ld = await _loader_to_youtube(video_id, fmt="mp3")
        if ld.get("ok") and ld.get("url"):
            return {
                "ok": True,
                "video_id": video_id,
                "title": ld.get("title"),
                "thumb": ld.get("thumb"),
                "thumbnail": ld.get("thumb"),
                "duration": None,
                "audio_url": ld.get("url"),
                "video_url": None,
                "url": ld.get("url"),
                "directUrl": ld.get("url"),
                "audio_streams": [{"label": "mp3", "url": ld["url"], "format": "AUDIO", "type": "audio/mpeg"}],
                "video_streams": [],
                "sources": [{"type": "audio", "provider": ld.get("provider"), "label": "MP3 CDN", "url": ld["url"], "play_url": ld["url"], "format": "AUDIO"}],
                "watch_url": watch,
                "download_url": ld.get("url"),
                "provider": ld.get("provider"),
                "errors": errors or None,
                "note": "CDN via loader.to/savenow",
            }
        errors.append(str(ld.get("error") or "loader"))
    except Exception as e:
        errors.append(f"loader:{type(e).__name__}")

    # 3) Optional yt-dlp only if installed (not required)
    try:
        yd = await asyncio.to_thread(_ytdlp_info, watch)
        if yd.get("ok") and (yd.get("url") or yd.get("formats")):
            formats = yd.get("formats") or []
            audio_streams = [f for f in formats if "audio" in (f.get("kind") or "").lower() and f.get("url")]
            best = yd.get("url") or (audio_streams[0]["url"] if audio_streams else None)
            if best:
                return {
                    "ok": True,
                    "video_id": video_id,
                    "title": yd.get("title"),
                    "thumb": yd.get("thumbnail") or f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg",
                    "audio_url": best,
                    "url": best,
                    "directUrl": best,
                    "audio_streams": audio_streams[:8],
                    "video_streams": [],
                    "sources": [{"type": "audio", "provider": "yt-dlp", "label": "Best", "url": best, "play_url": best}],
                    "provider": yd.get("extractor") or "yt-dlp",
                    "errors": errors or None,
                }
    except Exception as e:
        errors.append(f"ytdlp:{type(e).__name__}")

    # 3) embed last resort
    embed = f"https://www.youtube.com/embed/{video_id}?autoplay=1&rel=0"
    return {
        "ok": False,
        "video_id": video_id,
        "title": None,
        "thumb": f"https://i.ytimg.com/vi/{video_id}/hqdefault.jpg",
        "audio_url": None,
        "video_url": embed,
        "url": embed,
        "directUrl": embed,
        "audio_streams": [],
        "video_streams": [],
        "sources": [{"type": "embed", "provider": "youtube", "label": "YouTube Embed", "url": embed, "play_url": embed, "format": "EMBED"}],
        "watch_url": watch,
        "provider": "embed",
        "errors": errors,
        "note": "CDN blocked on this IP. Embed works in browser; set residential IP or cookies for direct.",
    }





# (HA_BASE moved to top)



def _ha_card(x: dict) -> dict:
    """Normalize HindiAnime list item for frontend cards."""
    if not isinstance(x, dict):
        return {}
    link = x.get("link") or x.get("url") or ""
    title = x.get("title") or x.get("name") or ""
    poster = x.get("poster") or x.get("image") or x.get("thumb") or ""
    return {
        "id": x.get("id") or link or title,
        "title": title,
        "link": link,
        "url": link,
        "poster": poster,
        "thumb": poster,
        "type": x.get("type"),
        "episodes": x.get("episodes") or x.get("episode"),
        "episode": x.get("episode"),
        "duration": x.get("duration"),
        "rating": x.get("rating"),
        "status": x.get("status"),
        "overview": x.get("overview") or x.get("description"),
        "views": x.get("viewsFormatted") or x.get("rawViews"),
        "rank": x.get("rank"),
        "time_ago": x.get("timeAgo"),
        "provider": "hindianime",
        "raw": x,
    }


async def _ha_get(path: str, params: Optional[dict] = None) -> Any:
    base = globals().get("HA_BASE") or "https://www.hindianime.site"
    headers = globals().get("HA_HEADERS") or {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://www.hindianime.site/",
        "Origin": "https://www.hindianime.site",
    }
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        r = await client.get(base.rstrip("/") + path, params=params or {}, headers=headers)
        if r.status_code >= 400:
            raise HTTPException(502, f"HindiAnime {path} HTTP {r.status_code}")
        try:
            return r.json()
        except Exception:
            raise HTTPException(502, f"HindiAnime {path} non-JSON ({r.status_code})")



@app.get("/anime/status", tags=["Anime"])
async def anime_status():
    """Check if HindiAnime catalog + stream tunnel are reachable."""
    out = {"provider": "hindianime", "catalog": False, "master": False, "tunnel": False}
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.hindianime.site/",
        "Accept": "*/*",
    }
    async with httpx.AsyncClient(timeout=12.0, follow_redirects=True) as client:
        try:
            r = await client.get("https://www.hindianime.site/api/catalog", headers={**headers, "Accept": "application/json"})
            out["catalog"] = r.status_code == 200 and "series" in (r.text or "")
        except Exception as e:
            out["catalog_error"] = str(e)[:80]
        # use a known hash from catalog if possible — fixed probe hash
        probe = "8eb42db3c9772af6dba6f1b9f7095feb"
        try:
            r = await client.get(
                f"https://www.hindianime.site/api/proxy/master.m3u8?hash={probe}",
                headers=headers,
            )
            out["master"] = r.status_code == 200 and (r.text or "").lstrip().startswith("#EXTM3U")
        except Exception as e:
            out["master_error"] = str(e)[:80]
        try:
            r = await client.get(
                f"https://stream.hindianime.site/api/proxy/master.m3u8?hash={probe}",
                headers=headers,
            )
            master_ok = r.status_code == 200 and (r.text or "").lstrip().startswith("#EXTM3U")
            out["stream_host_master"] = master_ok
            if master_ok:
                # probe first variant
                lines = (r.text or "").splitlines()
                for i, line in enumerate(lines):
                    if line.startswith("#EXT-X-STREAM-INF") and i + 1 < len(lines):
                        u = lines[i + 1].strip()
                        full = "https://stream.hindianime.site" + u if u.startswith("/") else u
                        pr = await client.get(full, headers=headers)
                        out["tunnel"] = pr.status_code == 200 and (pr.text or "").lstrip().startswith("#EXTM3U")
                        out["tunnel_status"] = pr.status_code
                        break
        except Exception as e:
            out["tunnel_error"] = str(e)[:80]
    out["ok"] = out["catalog"] and out["tunnel"]
    out["note"] = (
        "Streaming fully available"
        if out["ok"]
        else "Catalog may work but stream tunnel is down — video playback unavailable until HindiAnime restores stream.hindianime.site"
    )
    return out

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
        "ok": True,
        "top_airing": map_list("topAiring"),
        "most_popular": map_list("mostPopular"),
        "completed": map_list("completedSeries"),
        "latest_episodes": map_list("latestEpisodes"),
        "latest_movies": map_list("latestMovies"),
        "upcoming": map_list("upcoming"),
        "genres": data.get("genres") or [],
        "provider": "hindianime",
    }


@app.get("/anime/hero", tags=["Anime"])
async def anime_hero():
    """HindiAnime hero banners."""
    data = await _ha_get("/api/hero")
    items = [_ha_card(x) for x in (data.get("hero") or []) if isinstance(x, dict)]
    return {"items": items, "provider": "hindianime"}


@app.get("/anime/spotlights", tags=["Anime"])
async def anime_spotlights():
    """HindiAnime spotlight rotation."""
    data = await _ha_get("/api/spotlights")
    items = [_ha_card(x) for x in (data.get("all") or data.get("spotlights") or []) if isinstance(x, dict)]
    return {
        "items": items,
        "is_custom": data.get("isCustom"),
        "rotation": data.get("rotationPeriodDays"),
        "provider": "hindianime",
    }


@app.get("/anime/top10", tags=["Anime"])
async def anime_top10():
    """Best-effort top10 from home sections (site top10 may be SPA-only)."""
    data = await _ha_get("/api/home-sections")
    items = [_ha_card(x) for x in (data.get("mostPopular") or data.get("topAiring") or [])[:10] if isinstance(x, dict)]
    return {"items": items, "provider": "hindianime"}


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
    """Search HindiAnime catalog by title (series + movies)."""
    qn = (q or "").strip().lower()
    if not qn:
        raise HTTPException(400, "q required")
    try:
        data = await _ha_get("/api/catalog")
    except Exception as e:
        raise HTTPException(502, f"catalog: {e}")
    items = []
    seen = set()
    for x in list(data.get("series") or []) + list(data.get("movies") or []):
        if not isinstance(x, dict):
            continue
        title = (x.get("title") or x.get("name") or "").lower()
        if qn in title or any(part and part in title for part in qn.split()):
            card = _ha_card(x)
            key = card.get("id") or card.get("title")
            if key in seen:
                continue
            seen.add(key)
            items.append(card)
    return {"ok": True, "query": q, "count": len(items), "items": items[:80], "provider": "hindianime"}



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




# --- HindiAnime multi-server embeds (from their watch page logic) ---
_HA_TMDB_KEY = "8265bd1679663a7ea12ac168da84d2e8"  # same key used by hindianime.site frontend
_HA_TMDB_MAP = {
    "demon slayer": "85937", "jujutsu kaisen": "95479", "naruto shippuden": "31910",
    "naruto": "46260", "one piece": "37854", "bleach": "30984", "solo leveling": "205120",
    "chainsaw man": "114410", "spy x family": "120089", "attack on titan": "1429",
    "tokyo ghoul": "61374", "classroom of the elite": "72636", "oshi no ko": "203737",
    "my hero academia": "65930", "death note": "13916", "hunter x hunter": "46298",
    "kaiju no 8": "207347", "dandadan": "240411", "blue lock": "136283",
    "frieren": "209867", "your name": "372058", "suzume": "916224",
    "weathering with you": "568160", "spirited away": "129",
    "trapped in a dating sim": "124361",
}


async def _ha_tmdb_id(title: str, is_movie: bool = False) -> Optional[str]:
    if not title:
        return None
    clean = re.sub(r"\(.*?\)", "", title)
    clean = re.sub(r"Season\s+\d+", "", clean, flags=re.I)
    clean = re.sub(r"Hindi|Dub|Sub|Multi", "", clean, flags=re.I)
    clean = re.sub(r"[-_]+", " ", clean).strip()
    norm = clean.lower()
    for k, v in _HA_TMDB_MAP.items():
        if norm == k or k in norm or norm in k:
            return v
    endpoint = "movie" if is_movie else "tv"
    try:
        async with httpx.AsyncClient(timeout=12.0) as client:
            r = await client.get(
                f"https://api.themoviedb.org/3/search/{endpoint}",
                params={"api_key": _HA_TMDB_KEY, "query": clean},
            )
            if r.status_code == 200:
                results = (r.json() or {}).get("results") or []
                if results:
                    return str(results[0].get("id"))
    except Exception:
        pass
    # try the other type
    try:
        endpoint = "tv" if is_movie else "movie"
        async with httpx.AsyncClient(timeout=12.0) as client:
            r = await client.get(
                f"https://api.themoviedb.org/3/search/{endpoint}",
                params={"api_key": _HA_TMDB_KEY, "query": clean},
            )
            if r.status_code == 200:
                results = (r.json() or {}).get("results") or []
                if results:
                    return str(results[0].get("id"))
    except Exception:
        pass
    return None


def _ha_embed_servers(tmdb_id: str, is_movie: bool, season: int = 1, episode: int = 1) -> List[dict]:
    """Mirror hindianime.site watch-page embed server list."""
    if not tmdb_id:
        return []
    sN, eN = int(season or 1), int(episode or 1)
    if is_movie:
        return [
            {"name": "Server 4 (VidLink Fast)", "type": "iframe", "url": f"https://vidlink.pro/movie/{tmdb_id}"},
            {"name": "Server 5 (VidSrc PM)", "type": "iframe", "url": f"https://vidsrc.pm/embed/movie/{tmdb_id}"},
            {"name": "Server 6 (VidSrc TO)", "type": "iframe", "url": f"https://vidsrc.to/embed/movie/{tmdb_id}"},
            {"name": "Server 7 (2Embed Stream)", "type": "iframe", "url": f"https://www.2embed.cc/embed/{tmdb_id}"},
            {"name": "Server 8 (AnimeDekho Multi)", "type": "iframe", "url": f"https://animedekho.app/embed/{tmdb_id}"},
            {"name": "Server 9 (AutoEmbed Stream)", "type": "iframe", "url": f"https://player.autoembed.cc/embed/movie/{tmdb_id}"},
            {"name": "Server 10 (MultiEmbed VIP)", "type": "iframe", "url": f"https://multiembed.mov/?video_id={tmdb_id}&tmdb=1"},
            {"name": "Server 11 (VidSrc Net)", "type": "iframe", "url": f"https://vidsrc.net/embed/movie/{tmdb_id}"},
            {"name": "Server 12 (MoviesAPI Club)", "type": "iframe", "url": f"https://moviesapi.club/movie/{tmdb_id}"},
            {"name": "Server 13 (SmashyStream)", "type": "iframe", "url": f"https://embed.smashystream.com/playere.php?tmdb={tmdb_id}"},
            {"name": "Server 14 (VidSrc In)", "type": "iframe", "url": f"https://vidsrc.in/embed/movie/{tmdb_id}"},
            {"name": "Server 15 (VidSrc XYZ)", "type": "iframe", "url": f"https://vidsrc.xyz/embed/movie/{tmdb_id}"},
            {"name": "Server 16 (VidSrc CC)", "type": "iframe", "url": f"https://vidsrc.cc/v2/embed/movie/{tmdb_id}"},
        ]
    return [
        {"name": "Server 4 (VidLink Fast)", "type": "iframe", "url": f"https://vidlink.pro/tv/{tmdb_id}/{sN}/{eN}"},
        {"name": "Server 5 (VidSrc PM)", "type": "iframe", "url": f"https://vidsrc.pm/embed/tv/{tmdb_id}/{sN}/{eN}"},
        {"name": "Server 6 (VidSrc TO)", "type": "iframe", "url": f"https://vidsrc.to/embed/tv/{tmdb_id}/{sN}/{eN}"},
        {"name": "Server 7 (2Embed Stream)", "type": "iframe", "url": f"https://www.2embed.cc/embedtv/{tmdb_id}&s={sN}&e={eN}"},
        {"name": "Server 8 (AnimeDekho Multi)", "type": "iframe", "url": f"https://animedekho.app/embed/{tmdb_id}/{sN}-{eN}"},
        {"name": "Server 9 (AutoEmbed Stream)", "type": "iframe", "url": f"https://player.autoembed.cc/embed/tv/{tmdb_id}/{sN}/{eN}"},
        {"name": "Server 10 (MultiEmbed VIP)", "type": "iframe", "url": f"https://multiembed.mov/?video_id={tmdb_id}&tmdb=1&s={sN}&e={eN}"},
        {"name": "Server 11 (VidSrc Net)", "type": "iframe", "url": f"https://vidsrc.net/embed/tv/{tmdb_id}/{sN}/{eN}"},
        {"name": "Server 12 (MoviesAPI Club)", "type": "iframe", "url": f"https://moviesapi.club/tv/{tmdb_id}-{sN}-{eN}"},
        {"name": "Server 13 (SmashyStream)", "type": "iframe", "url": f"https://embed.smashystream.com/playere.php?tmdb={tmdb_id}&season={sN}&episode={eN}"},
        {"name": "Server 14 (VidSrc In)", "type": "iframe", "url": f"https://vidsrc.in/embed/tv/{tmdb_id}/{sN}/{eN}"},
        {"name": "Server 15 (VidSrc XYZ)", "type": "iframe", "url": f"https://vidsrc.xyz/embed/tv/{tmdb_id}/{sN}/{eN}"},
        {"name": "Server 16 (VidSrc CC)", "type": "iframe", "url": f"https://vidsrc.cc/v2/embed/tv/{tmdb_id}/{sN}/{eN}"},
    ]



# =============================================================================
# VidSrc / CDN direct m3u8 resolver (WASM ChaCha20 decrypt + host token)
# =============================================================================

_VIDSRC_DATA = "https://data.vidsrc.sh/api.php"
_wasm_module_cache: Dict[str, Any] = {}


def _vidsrc_decrypt_stream_urls(enc_b64: str, wasm_url: str) -> List[str]:
    """Decrypt data.stream_urls using the rotating ChaCha20 WASM from data.vidsrc.sh."""
    if not _HAS_WASM:
        return []
    try:
        import base64 as _b64
        enc = _b64.b64decode(enc_b64)
        # fetch wasm
        with httpx.Client(timeout=20.0, verify=False) as client:
            wasm_bytes = client.get(wasm_url, headers={"User-Agent": "Mozilla/5.0"}).content
        engine = Engine()
        store = Store(engine)
        module = Module(engine, wasm_bytes)
        instance = Instance(store, module, [])
        ex = instance.exports(store)
        alloc = ex["alloc"]
        decrypt = ex["decrypt"]
        memory = ex["memory"]
        ptr = alloc(store, len(enc))
        memory.write(store, enc, ptr)
        out_len = decrypt(store, ptr, len(enc))
        raw = memory.read(store, ptr + 12, ptr + 12 + out_len)
        text = bytes(raw).decode("utf-8", errors="ignore")
        return [ln.strip() for ln in text.splitlines() if ln.strip().startswith("http")]
    except Exception as e:
        return []


async def _vidsrc_token_for(host: str) -> Optional[str]:
    """IP-bound JWT from {origin}/generate.php — required to play CDN m3u8."""
    from urllib.parse import urlparse
    try:
        if "://" in host:
            origin = f"{urlparse(host).scheme}://{urlparse(host).netloc}"
        else:
            origin = f"https://{host}"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "https://cloudorchestranova.com/",
            "Accept": "*/*",
        }
        async with httpx.AsyncClient(timeout=15.0, verify=False) as client:
            for attempt in range(3):
                r = await client.get(f"{origin}/generate.php", headers=headers)
                tok = (r.text or "").strip()
                if r.status_code == 200 and tok.startswith("eyJ"):
                    return tok
                if r.status_code == 429:
                    await asyncio.sleep(1.5 * (attempt + 1))
                    continue
                break
    except Exception:
        pass
    return None


async def _vidsrc_resolve_cdn(
    tmdb_id: str,
    media_type: str = "tv",
    season: int = 1,
    episode: int = 1,
) -> Dict[str, Any]:
    """Return direct CDN master.m3u8 URLs (with tokens) for a TMDB title."""
    out: Dict[str, Any] = {
        "ok": False,
        "tmdb_id": tmdb_id,
        "type": media_type,
        "season": season if media_type == "tv" else None,
        "episode": episode if media_type == "tv" else None,
        "streams": [],
        "title": None,
        "provider": "vidsrc-cdn",
    }
    if not tmdb_id:
        out["error"] = "tmdb_id required"
        return out

    if media_type == "movie":
        api = f"{_VIDSRC_DATA}?type=movie&tmdb={tmdb_id}&stream_urls"
    else:
        api = f"{_VIDSRC_DATA}?type=tv&tmdb={tmdb_id}&season={season}&episode={episode}&stream_urls"

    try:
        async with httpx.AsyncClient(timeout=25.0, verify=False) as client:
            r = await client.get(
                api,
                headers={
                    "User-Agent": "Mozilla/5.0",
                    "Accept": "application/json",
                    "Referer": "https://cloudorchestranova.com/",
                },
            )
            j = r.json()
    except Exception as e:
        out["error"] = f"api fetch failed: {e}"
        return out

    code = str(j.get("status_code") or j.get("status") or "")
    data = j.get("data") or {}
    if code not in ("200", "200.0") or not data:
        out["error"] = f"vidsrc status {code} (title may not be in catalog)"
        out["raw_status"] = code
        return out

    out["title"] = data.get("title")
    out["imdb_id"] = data.get("imdb_id")
    su = data.get("stream_urls")

    urls: List[str] = []
    if isinstance(su, list):
        urls = [u for u in su if isinstance(u, str) and u.startswith("http")]
    elif isinstance(su, str):
        vs = j.get("vs") or {}
        wasm_url = vs.get("wasm_url")
        if wasm_url:
            # run sync decrypt in thread to avoid blocking
            urls = await asyncio.to_thread(_vidsrc_decrypt_stream_urls, su, wasm_url)
        if not urls:
            out["error"] = "encrypted stream_urls but WASM decrypt failed (install wasmtime)"
            out["encrypted"] = True
            return out
    else:
        out["error"] = "no stream_urls"
        return out

    streams = []
    from urllib.parse import urlparse
    token_cache: Dict[str, Optional[str]] = {}
    for i, raw in enumerate(urls):
        host = urlparse(raw).netloc
        if host not in token_cache:
            token_cache[host] = await _vidsrc_token_for(host)
        tok = token_cache[host]
        play = raw
        if tok:
            sep = "&" if "?" in raw else "?"
            play = f"{raw}{sep}token={tok}"
        streams.append({
            "label": f"CDN {i + 1}",
            "type": "hls",
            "url": play,
            "direct": raw,
            "host": host,
            "token": bool(tok),
        })

    out["ok"] = bool(streams)
    out["streams"] = streams
    out["count"] = len(streams)
    out["note"] = "Direct CDN master.m3u8 with IP-bound token. Tokens expire ~4h; call again for fresh links."
    return out


@app.get("/anime/cdn", tags=["Anime"])
async def anime_cdn(
    tmdb_id: Optional[str] = Query(None),
    title: Optional[str] = Query(None),
    type: str = Query("series", description="series|movie"),
    season: int = Query(1, ge=1),
    episode: int = Query(1, ge=1),
):
    """Direct CDN m3u8 links (VidSrc data API + WASM decrypt + host token).

    Prefer this over iframe embeds when you need real playable HLS URLs for VLC / native players.
    """
    is_movie = (type or "").lower() in ("movie", "movies", "film")
    tid = tmdb_id
    if not tid and title:
        tid = await _ha_tmdb_id(title, is_movie=is_movie)
    if not tid:
        raise HTTPException(400, "tmdb_id or title required")
    result = await _vidsrc_resolve_cdn(
        str(tid),
        media_type="movie" if is_movie else "tv",
        season=season,
        episode=episode,
    )
    result["creator"] = "shawon"
    return result


@app.get("/play/cdn", tags=["Play"])
async def play_cdn(
    tmdb_id: Optional[str] = Query(None),
    title: Optional[str] = Query(None),
    type: str = Query("movie"),
    season: int = Query(1, ge=1),
    episode: int = Query(1, ge=1),
):
    """Alias of /anime/cdn for movies & series (same resolver)."""
    return await anime_cdn(tmdb_id=tmdb_id, title=title, type=type, season=season, episode=episode)

@app.get("/anime/servers", tags=["Anime"])
async def anime_servers(
    title: Optional[str] = Query(None, description="Anime/movie title"),
    slug: Optional[str] = Query(None, description="hindianime slug"),
    hash: Optional[str] = Query(None, description="videoHash for native HLS"),
    tmdb_id: Optional[str] = Query(None, description="TMDB id"),
    type: str = Query("series", description="series|movie"),
    season: int = Query(1, ge=1),
    episode: int = Query(1, ge=1),
    request: Request = None,
):
    """Playback servers: native HindiAnime HLS + embed fallbacks (VidSrc etc.)."""
    is_movie = (type or "").lower() in ("movie", "movies", "film")
    resolved_title = title
    video_hash = hash
    tid = str(tmdb_id).strip() if tmdb_id else None

    # Resolve title from TMDB if needed
    if tid and not resolved_title:
        try:
            media = "movie" if is_movie else "tv"
            key = globals().get("TMDB_KEY") or globals().get("TMDB_API_KEY") or "b39176614e7ea6307888211ddd83549a"
            async with httpx.AsyncClient(timeout=15.0) as client:
                tr = await client.get(
                    f"https://api.themoviedb.org/3/{media}/{tid}",
                    params={"api_key": key},
                )
                if tr.status_code == 200:
                    tj = tr.json()
                    resolved_title = tj.get("title") or tj.get("name")
        except Exception:
            pass

    # Resolve slug from title via catalog search
    if resolved_title and not slug and not video_hash:
        try:
            cat = await _ha_get("/api/catalog")
            qn = resolved_title.lower().strip()
            best = None
            for x in list(cat.get("series") or []) + list(cat.get("movies") or []):
                if not isinstance(x, dict):
                    continue
                xt = (x.get("title") or "").lower()
                if qn == xt or qn in xt or xt in qn:
                    best = x
                    if qn == xt:
                        break
            if best:
                link = best.get("link") or best.get("url") or ""
                slug = link.rstrip("/").split("/")[-1] if link else slug
                resolved_title = resolved_title or best.get("title")
                if (best.get("type") or "").lower() == "movie":
                    is_movie = True
        except Exception:
            pass

    if slug and not video_hash:
        try:
            data = await _ha_get(f"/extracted/{slug.strip().strip('/')}.json")
            resolved_title = resolved_title or data.get("title")
            is_movie = is_movie or ((data.get("type") or "").lower() == "movie")
            eps = data.get("episodes") or []
            if isinstance(eps, dict):
                eps = list(eps.values())
            pick = None
            for ep in eps:
                if not isinstance(ep, dict):
                    continue
                if int(ep.get("season") or 1) == int(season) and int(ep.get("episode") or ep.get("number") or 1) == int(episode):
                    pick = ep
                    break
            if not pick and eps:
                # first episode fallback
                for ep in eps:
                    if isinstance(ep, dict):
                        pick = ep
                        break
            if pick:
                video_hash = pick.get("videoHash") or pick.get("hash") or video_hash
                try:
                    season = int(pick.get("season") or season)
                    episode = int(pick.get("episode") or pick.get("number") or episode)
                except Exception:
                    pass
        except Exception:
            pass

    servers: List[dict] = []
    tunnel_ok = False
    play_url = None
    stream_url = None
    host = globals().get("HA_STREAM") or "https://stream.hindianime.site"

    if video_hash:
        master = f"{host}/api/proxy/master.m3u8?hash={video_hash}"
        try:
            async with httpx.AsyncClient(timeout=14.0, follow_redirects=True) as client:
                r = await client.get(
                    master,
                    headers={"User-Agent": "Mozilla/5.0", "Referer": "https://www.hindianime.site/", "Accept": "*/*"},
                )
                if r.status_code == 200 and (r.text or "").lstrip().startswith("#EXTM3U"):
                    tunnel_ok = True
                    lines = (r.text or "").splitlines()
                    for i, line in enumerate(lines):
                        if line.startswith("#EXT-X-STREAM-INF") and i + 1 < len(lines):
                            u = lines[i + 1].strip()
                            full = host + u if u.startswith("/") else u
                            try:
                                pr = await client.get(full, headers={"User-Agent": "Mozilla/5.0", "Referer": "https://www.hindianime.site/"})
                                tunnel_ok = pr.status_code == 200 and (pr.text or "").lstrip().startswith("#EXTM3U")
                            except Exception:
                                tunnel_ok = False
                            break
        except Exception:
            tunnel_ok = False

        stream_url = master
        try:
            base = str(request.base_url).rstrip("/") if request is not None else ""
        except Exception:
            base = ""
        play_url = f"{base}/anime/hls?u={quote(master, safe='')}" if base else f"/anime/hls?u={quote(master, safe='')}"

        servers.append({
            "name": "Server 1 (HindiAnime HLS)",
            "type": "hls",
            "url": master,
            "play_url": play_url,
            "hash": video_hash,
            "working": tunnel_ok,
            "provider": "hindianime",
        })
        servers.append({
            "name": "Server 2 (HLS via proxy)",
            "type": "hls",
            "url": play_url,
            "play_url": play_url,
            "hash": video_hash,
            "working": True,
            "provider": "proxy",
        })

    # Embed servers via TMDB
    if tid:
        media = "movie" if is_movie else "tv"
        embeds = []
        if is_movie:
            embeds = [
                ("VidSrc", f"https://vidsrc.xyz/embed/movie/{tid}"),
                ("VidSrc.to", f"https://vidsrc.to/embed/movie/{tid}"),
                ("VidLink", f"https://vidlink.pro/movie/{tid}"),
                ("2Embed", f"https://www.2embed.cc/embed/{tid}"),
                ("AutoEmbed", f"https://autoembed.co/movie/tmdb/{tid}"),
                ("VidKing", f"https://www.vidking.net/embed/movie/{tid}"),
            ]
        else:
            embeds = [
                ("VidSrc", f"https://vidsrc.xyz/embed/tv/{tid}/{season}/{episode}"),
                ("VidSrc.to", f"https://vidsrc.to/embed/tv/{tid}/{season}/{episode}"),
                ("VidLink", f"https://vidlink.pro/tv/{tid}/{season}/{episode}"),
                ("2Embed", f"https://www.2embed.cc/embedtv/{tid}&s={season}&e={episode}"),
                ("AutoEmbed", f"https://autoembed.co/tv/tmdb/{tid}-{season}-{episode}"),
                ("VidKing", f"https://www.vidking.net/embed/tv/{tid}/{season}/{episode}"),
            ]
        for name, url in embeds:
            servers.append({
                "name": name,
                "type": "embed",
                "url": url,
                "play_url": url,
                "working": True,
                "provider": name.lower().replace(" ", ""),
            })

    # CDN streams via vidsrc resolver
    cdn_streams = []
    if tid:
        try:
            cdn = await _vidsrc_resolve_cdn(
                str(tid),
                media_type="movie" if is_movie else "tv",
                season=season,
                episode=episode,
            )
            for s in (cdn.get("streams") or [])[:6]:
                if s.get("url"):
                    cdn_streams.append(s)
                    servers.append({
                        "name": s.get("label") or "CDN",
                        "type": s.get("type") or "hls",
                        "url": s.get("url"),
                        "play_url": s.get("url"),
                        "working": True,
                        "provider": "vidsrc-cdn",
                    })
        except Exception:
            pass

    return {
        "ok": bool(servers),
        "title": resolved_title,
        "slug": slug,
        "hash": video_hash,
        "tmdb_id": tid,
        "type": "movie" if is_movie else "series",
        "season": season,
        "episode": episode,
        "tunnel_ok": tunnel_ok,
        "stream_url": stream_url,
        "play_url": play_url,
        "servers": servers,
        "cdn_streams": cdn_streams,
        "count": len(servers),
        "provider": "hindianime+embeds",
    }



@app.get("/anime/stream", tags=["Anime"])
async def anime_stream(
    hash: Optional[str] = Query(None, description="videoHash from episode"),
    url: Optional[str] = Query(None, description="master m3u8 or episode page url"),
    slug: Optional[str] = Query(None),
    tmdb_id: Optional[str] = Query(None),
    title: Optional[str] = Query(None),
    type: str = Query("series"),
    season: int = Query(1, ge=1),
    episode: int = Query(1, ge=1),
    request: Request = None,
):
    """Resolve playable HLS for anime/series/movie.

    Accepts hash, direct master url, slug, or tmdb_id (auto-resolves hash via catalog).
    Always returns play_url through /anime/hls proxy when possible.
    """
    video_hash = hash
    is_movie = (type or "").lower() in ("movie", "movies", "film")

    # Resolve via servers helper when no hash/url
    if not video_hash and not url:
        if slug or tmdb_id or title:
            try:
                srv = await anime_servers(
                    title=title, slug=slug, hash=None, tmdb_id=tmdb_id,
                    type=type, season=season, episode=episode, request=request,
                )
                video_hash = srv.get("hash")
                if not video_hash and srv.get("cdn_streams"):
                    # return first CDN stream
                    s0 = srv["cdn_streams"][0]
                    return {
                        "ok": True,
                        "type": "cdn",
                        "url": s0.get("url"),
                        "play_url": s0.get("url"),
                        "stream_url": s0.get("url"),
                        "servers": srv.get("servers") or [],
                        "tmdb_id": tmdb_id,
                        "title": srv.get("title"),
                        "provider": "vidsrc-cdn",
                    }
                if not video_hash and srv.get("servers"):
                    # return embed list
                    return {
                        "ok": bool(srv.get("servers")),
                        "type": "servers",
                        "hash": None,
                        "servers": srv.get("servers"),
                        "play_url": (srv.get("servers") or [{}])[0].get("play_url"),
                        "title": srv.get("title"),
                        "tmdb_id": tmdb_id,
                        "provider": "embeds",
                        "note": "No native hash — use embed/CDN servers",
                    }
            except Exception as e:
                raise HTTPException(502, f"resolve failed: {e}")
        else:
            raise HTTPException(400, "hash, url, slug, tmdb_id or title required")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0",
        "Referer": "https://www.hindianime.site/",
        "Origin": "https://www.hindianime.site",
        "Accept": "*/*",
    }
    host_list = [
        globals().get("HA_STREAM") or "https://stream.hindianime.site",
        globals().get("HA_BASE") or "https://www.hindianime.site",
    ]
    stream = None
    master_body = ""
    host_used = None
    tunnel_ok = False
    errors: List[str] = []

    if url and (url.endswith(".m3u8") or "master.m3u8" in url or "proxy" in url):
        stream = url
    elif video_hash:
        async with httpx.AsyncClient(timeout=22.0, follow_redirects=True) as client:
            for host in host_list:
                candidate = f"{host.rstrip('/')}/api/proxy/master.m3u8?hash={video_hash}"
                try:
                    r = await client.get(candidate, headers=headers)
                    body = r.text or ""
                    if r.status_code == 200 and body.lstrip().startswith("#EXTM3U"):
                        stream = candidate
                        master_body = body
                        host_used = host
                        # probe sub
                        lines = body.splitlines()
                        for i, line in enumerate(lines):
                            if line.startswith("#EXT-X-STREAM-INF") and i + 1 < len(lines):
                                probe_uri = lines[i + 1].strip()
                                probe_url = host + probe_uri if probe_uri.startswith("/") else probe_uri
                                try:
                                    pr = await client.get(probe_url, headers=headers)
                                    tunnel_ok = pr.status_code == 200 and (pr.text or "").lstrip().startswith("#EXTM3U")
                                except Exception as e:
                                    errors.append(str(e)[:80])
                                break
                        break
                    errors.append(f"{host} HTTP {r.status_code}")
                except Exception as e:
                    errors.append(f"{host}: {type(e).__name__}")
    elif url:
        stream = url

    if not stream:
        raise HTTPException(502, {"error": "no playable stream", "errors": errors, "hash": video_hash})

    try:
        base = str(request.base_url).rstrip("/") if request is not None else ""
    except Exception:
        base = ""
    play_url = f"{base}/anime/hls?u={quote(stream, safe='')}" if base else f"/anime/hls?u={quote(stream, safe='')}"

    # parse qualities from master
    qualities = []
    if master_body:
        lines = master_body.splitlines()
        for i, line in enumerate(lines):
            if line.startswith("#EXT-X-STREAM-INF"):
                bw = re.search(r"BANDWIDTH=(\d+)", line)
                res = re.search(r"RESOLUTION=(\d+x\d+)", line)
                uri = lines[i + 1].strip() if i + 1 < len(lines) else ""
                qualities.append({
                    "bandwidth": int(bw.group(1)) if bw else None,
                    "resolution": res.group(1) if res else None,
                    "uri": uri,
                })

    return {
        "ok": True,
        "hash": video_hash,
        "stream_url": stream,
        "play_url": play_url,
        "url": stream,
        "host": host_used,
        "tunnel_ok": tunnel_ok,
        "qualities": qualities,
        "tmdb_id": tmdb_id,
        "title": title,
        "season": season,
        "episode": episode,
        "errors": errors or None,
        "provider": "hindianime",
        "note": "Use play_url in HLS.js / VLC. Native segments need tunnel_ok=true.",
    }



@app.get("/anime/hls", tags=["Anime"])
async def anime_hls_proxy(request: Request, u: str = Query(..., min_length=8)):
    """Proxy m3u8 + segments (CORS). Unwraps HindiAnime segment?url= to CDN when needed."""
    if not (u.startswith("http://") or u.startswith("https://")):
        raise HTTPException(400, "url must be http(s)")

    def _ha_force_stream_host(url: str) -> str:
        """www returns SPA HTML for sub/segment; stream.* returns real media."""
        try:
            p = urlparse(url)
            host = (p.hostname or "").lower()
            if host in ("www.hindianime.site", "hindianime.site") and "/api/proxy/" in (p.path or ""):
                return "https://stream.hindianime.site" + (p.path or "") + (("?" + p.query) if p.query else "")
            return url
        except Exception:
            return url

    # Do NOT unwrap segment→CDN (zn-grid returns 403 without their edge cookies).
    # Always fetch via stream.hindianime.site proxy paths.
    target = _ha_force_stream_host(u)
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
                or (r.status_code == 410)
            ):
                alts = []
                # force stream host
                forced = _ha_force_stream_host(u)
                if forced != target:
                    alts.append(forced)
                if "www.hindianime.site" in target:
                    alts.append(target.replace("www.hindianime.site", "stream.hindianime.site"))
                if target != u:
                    alts.append(u)
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
        detail = f"upstream {r.status_code}"
        if r.status_code in (530, 1033, 502, 503):
            detail += " — HindiAnime stream tunnel may be offline (Cloudflare 1033). Retry later."
        raise HTTPException(502, detail)

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
                            full_u = "https://stream.hindianime.site" + uri
                        else:
                            full_u = urljoin(_base, uri)
                        full_u = _ha_force_stream_host(full_u)
                        return 'URI="/anime/hls?u=' + quote(full_u, safe="") + '"'
                    line = re.sub(r'URI="([^"]+)"', _rew, line)
                lines.append(line)
                continue
            if raw.startswith("http://") or raw.startswith("https://"):
                full_u = raw
            elif raw.startswith("/"):
                full_u = "https://stream.hindianime.site" + raw
            else:
                full_u = urljoin(base, raw)
            full_u = _ha_force_stream_host(full_u)
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
        stream = f"{HA_STREAM}/api/proxy/master.m3u8?hash={hash}"
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



# =============================================================================
# AI · Chat · Image · TempMail  (native free backends — no PaxSenix key needed)
# Pollinations.ai (text + image) · optional GROQ/OPENROUTER/GEMINI/OPENAI keys
# =============================================================================

_AI_MODELS = [
    {"id": "openai", "name": "OpenAI (via Pollinations)", "owned_by": "pollinations", "tier": "free", "backend": "pollinations"},
    {"id": "openai-fast", "name": "GPT-OSS Fast", "owned_by": "pollinations", "tier": "free", "backend": "pollinations"},
    {"id": "deepseek", "name": "DeepSeek", "owned_by": "pollinations", "tier": "free", "backend": "pollinations"},
    {"id": "mistral", "name": "Mistral", "owned_by": "pollinations", "tier": "free", "backend": "pollinations"},
    {"id": "gemini", "name": "Gemini", "owned_by": "pollinations", "tier": "free", "backend": "pollinations"},
    {"id": "claude", "name": "Claude", "owned_by": "pollinations", "tier": "free", "backend": "pollinations"},
    {"id": "groq", "name": "Groq Llama (needs GROQ_API_KEY)", "owned_by": "groq", "tier": "free-key", "backend": "groq"},
    {"id": "openrouter", "name": "OpenRouter auto (needs OPENROUTER_API_KEY)", "owned_by": "openrouter", "tier": "free-key", "backend": "openrouter"},
]


async def _ai_chat_pollinations(messages: list, model: str = "openai", system: Optional[str] = None) -> dict:
    msgs = list(messages or [])
    if system:
        msgs = [{"role": "system", "content": system}] + msgs
    payload = {"model": model or "openai", "messages": msgs}
    async with httpx.AsyncClient(timeout=90.0) as client:
        r = await client.post(
            "https://text.pollinations.ai/openai",
            json=payload,
            headers={"Content-Type": "application/json", "User-Agent": "StreamHub/5.19", "Accept": "application/json"},
        )
        if r.status_code != 200:
            # fallback simple GET
            user = ""
            for m in reversed(msgs):
                if m.get("role") == "user":
                    user = m.get("content") or ""
                    break
            if user:
                r2 = await client.get(
                    f"https://text.pollinations.ai/{quote(user[:1500])}",
                    headers={"User-Agent": "StreamHub/5.19"},
                )
                if r2.status_code == 200 and r2.text:
                    return {
                        "id": f"poll_{int(time.time())}",
                        "object": "chat.completion",
                        "choices": [{"index": 0, "message": {"role": "assistant", "content": r2.text}, "finish_reason": "stop"}],
                        "model": model,
                        "provider": "pollinations-get",
                    }
            return {"ok": False, "error": f"pollinations HTTP {r.status_code}", "body": r.text[:300]}
        j = r.json()
        j["provider"] = "pollinations"
        return j


async def _ai_chat_groq(messages: list, model: str = "llama-3.3-70b-versatile") -> dict:
    key = (os.environ.get("GROQ_API_KEY") or "").strip()
    if not key:
        return {"ok": False, "error": "GROQ_API_KEY not set"}
    async with httpx.AsyncClient(timeout=60.0) as client:
        r = await client.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
            json={"model": model, "messages": messages},
        )
        if r.status_code != 200:
            return {"ok": False, "error": r.text[:300]}
        j = r.json()
        j["provider"] = "groq"
        return j


async def _ai_chat_openrouter(messages: list, model: str = "openrouter/auto") -> dict:
    key = (os.environ.get("OPENROUTER_API_KEY") or "").strip()
    if not key:
        return {"ok": False, "error": "OPENROUTER_API_KEY not set"}
    async with httpx.AsyncClient(timeout=90.0) as client:
        r = await client.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {key}",
                "Content-Type": "application/json",
                "HTTP-Referer": "https://streamhub.local",
                "X-Title": "StreamHub",
            },
            json={"model": model, "messages": messages},
        )
        if r.status_code != 200:
            return {"ok": False, "error": r.text[:300]}
        j = r.json()
        j["provider"] = "openrouter"
        return j


async def _ai_chat_gemini(messages: list, model: str = "gemini-2.0-flash") -> dict:
    key = (os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY") or "").strip()
    if not key:
        return {"ok": False, "error": "GEMINI_API_KEY not set"}
    # convert messages
    contents = []
    system = None
    for m in messages:
        role = m.get("role")
        if role == "system":
            system = m.get("content")
            continue
        contents.append({
            "role": "user" if role == "user" else "model",
            "parts": [{"text": m.get("content") or ""}],
        })
    body: Dict[str, Any] = {"contents": contents}
    if system:
        body["systemInstruction"] = {"parts": [{"text": system}]}
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={key}"
    async with httpx.AsyncClient(timeout=60.0) as client:
        r = await client.post(url, json=body)
        if r.status_code != 200:
            return {"ok": False, "error": r.text[:300]}
        j = r.json()
        text = ""
        try:
            text = j["candidates"][0]["content"]["parts"][0]["text"]
        except Exception:
            text = str(j)[:500]
        return {
            "id": f"gem_{int(time.time())}",
            "object": "chat.completion",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": text}, "finish_reason": "stop"}],
            "model": model,
            "provider": "gemini",
        }


async def _ai_chat_route(messages: list, model: str = "openai", system: Optional[str] = None) -> dict:
    m = (model or "openai").lower().strip()
    if m in ("groq", "llama", "llama3") or m.startswith("llama-"):
        return await _ai_chat_groq(messages, model if m.startswith("llama") else "llama-3.3-70b-versatile")
    if m in ("openrouter", "auto") or "/" in m and not m.startswith("openai"):
        if os.environ.get("OPENROUTER_API_KEY"):
            return await _ai_chat_openrouter(messages, model if "/" in m else "openrouter/auto")
    if m.startswith("gemini") and (os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY")):
        return await _ai_chat_gemini(messages, model if m.startswith("gemini-") else "gemini-2.0-flash")
    # default: pollinations free
    return await _ai_chat_pollinations(messages, model=model if model else "openai", system=system)



@app.get("/health/cookies", tags=["Meta"])
async def health_cookies():
    """Show whether YouTube cookies.txt is loaded (does not expose cookie values)."""
    path = _resolve_ytdlp_cookies()
    info = {"ok": bool(path), "loaded": bool(path), "path": None, "size": 0, "has_sid": False}
    if path:
        try:
            info["path"] = path
            info["size"] = os.path.getsize(path)
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                body = f.read(50000)
            real = 0
            for line in body.splitlines():
                if line.startswith("#") or not line.strip():
                    continue
                parts = line.split("	")
                if len(parts) >= 7 and parts[5] in ("LOGIN_INFO", "SID", "__Secure-1PSID", "SAPISID", "APISID", "HSID", "SSID"):
                    real += 1
            info["has_sid"] = real >= 1
            info["cookie_rows"] = real
            if "# TEMPLATE" in body or "REPLACE_ME" in body:
                info["ok"] = False
                info["loaded"] = False
                info["note"] = "Template cookies.txt only — export real YouTube cookies from browser"
        except Exception as e:
            info["error"] = str(e)[:80]
    else:
        info["note"] = (
            "No cookies loaded. Export from browser → cookies.txt next to api.py, "
            "or set YTDLP_COOKIES / YTDLP_COOKIES_B64 env. See cookies.txt template."
        )
    return info


@app.get("/yt/player/{video_id}", tags=["YouTube"])
@app.get("/youtube/player/{video_id}", tags=["YouTube"])
async def yt_player(video_id: str):
    """Direct CDN streams via InnerTube (ANDROID) — no yt-dlp, unlimited."""
    data = await _innertube_player(video_id.replace("yt:", "").strip())
    if not data.get("ok"):
        raise HTTPException(502, data.get("error") or "player failed")
    return data


@app.get("/dl/yt", tags=["Downloader"])
@app.get("/dl/youtube", tags=["Downloader"])
async def dl_youtube_native(
    url: str = Query(..., description="YouTube URL or video id"),
    quality: str = Query("720", description="360|480|720|1080|best|audio"),
):
    """YouTube direct CDN via InnerTube (no yt-dlp). quality=audio for audio-only."""
    vid = _extract_youtube_id(url) or url.strip()
    if not re.match(r"^[\w-]{6,20}$", vid or ""):
        raise HTTPException(400, "invalid youtube url/id")
    data = await _innertube_player(vid)
    if not data.get("ok"):
        raise HTTPException(502, data.get("error") or "extract failed")
    q = (quality or "720").lower()
    chosen = None
    if q in ("audio", "mp3", "m4a", "bestaudio"):
        chosen = data.get("audio_url")
        kind = "audio"
    else:
        target = {"360": 360, "480": 480, "720": 720, "1080": 1080, "best": 9999}.get(q, 720)
        # prefer progressive
        progressive = [v for v in (data.get("video_streams") or []) if v.get("progressive")]
        pool = progressive or (data.get("video_streams") or [])
        # closest height <= target
        pool_sorted = sorted(pool, key=lambda x: abs((x.get("height") or 0) - target))
        for v in pool_sorted:
            h = v.get("height") or 0
            if target >= 9999 or h <= target + 80:
                chosen = v.get("url")
                break
        if not chosen and pool:
            chosen = pool[0].get("url")
        if not chosen:
            chosen = data.get("video_url") or data.get("audio_url")
        kind = "video"
    return {
        "ok": bool(chosen),
        "provider": data.get("provider"),
        "title": data.get("title"),
        "thumbnail": data.get("thumb"),
        "duration": data.get("duration"),
        "url": chosen,
        "directUrl": chosen,
        "download_url": chosen,
        "quality": quality,
        "kind": kind,
        "audio_url": data.get("audio_url"),
        "video_url": data.get("video_url"),
        "formats": (data.get("formats") or [])[:20],
        "video_id": vid,
    }


@app.get("/lyrics/synced", tags=["Lyrics"])
async def lyrics_synced(
    title: str = Query(..., min_length=1),
    artist: str = Query(""),
):
    """Synced LRC lyrics via LRCLIB."""
    q = f"{title} {artist}".strip()
    async with httpx.AsyncClient(timeout=15.0) as client:
        r = await client.get("https://lrclib.net/api/search", params={"q": q})
        arr = r.json() if r.status_code == 200 else []
    if not arr:
        return {"ok": False, "title": title, "artist": artist, "synced": None, "plain": None}
    best = arr[0]
    return {
        "ok": True,
        "title": best.get("trackName") or title,
        "artist": best.get("artistName") or artist,
        "album": best.get("albumName"),
        "duration": best.get("duration"),
        "synced": best.get("syncedLyrics"),
        "plain": best.get("plainLyrics"),
        "provider": "lrclib",
        "id": best.get("id"),
    }


@app.get("/yt/stream/{video_id}", tags=["YouTube"])
@app.get("/youtube/stream/{video_id}", tags=["YouTube"])
async def yt_stream_cdn(video_id: str, audio: bool = Query(True)):
    """Best-effort direct CDN: InnerTube → loader.to."""
    vid = video_id.replace("yt:", "").strip()
    data = await _innertube_player(vid)
    if data.get("ok"):
        url = data.get("audio_url") if audio else (data.get("video_url") or data.get("audio_url"))
        return {
            "ok": True,
            "video_id": vid,
            "title": data.get("title"),
            "url": url,
            "directUrl": url,
            "audio_url": data.get("audio_url"),
            "video_url": data.get("video_url"),
            "provider": data.get("provider"),
        }
    ld = await _loader_to_youtube(vid, fmt="mp3" if audio else "360")
    if ld.get("ok"):
        return {**ld, "ok": True}
    raise HTTPException(502, {"error": "cdn unavailable", "innertube": data.get("error"), "loader": ld.get("error")})


@app.get("/music/home", tags=["Music"])
async def music_home_all():
    sections = []
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.get("https://api.deezer.com/chart/0/tracks")
            if r.status_code == 200:
                tracks = []
                for it in (r.json().get("data") or [])[:15]:
                    tracks.append({
                        "id": it.get("id"),
                        "title": it.get("title"),
                        "artist": (it.get("artist") or {}).get("name"),
                        "thumb": (it.get("album") or {}).get("cover_medium"),
                        "preview": it.get("preview"),
                        "audio_url": it.get("preview"),
                        "provider": "deezer",
                    })
                sections.append({"title": "Deezer Charts", "items": tracks})
    except Exception:
        pass
    return {"ok": True, "sections": sections, "provider": "aggregate"}

@app.get("/v1/models", tags=["AI"])
async def v1_models():
    """List available LLM models (free Pollinations + optional keyed providers)."""
    data = []
    for m in _AI_MODELS:
        data.append({
            "id": m["id"],
            "object": "model",
            "owned_by": m["owned_by"],
            "tier": m["tier"],
            "backend": m["backend"],
            "name": m["name"],
        })
    # try live pollinations list
    try:
        async with httpx.AsyncClient(timeout=12.0) as client:
            r = await client.get("https://text.pollinations.ai/models")
            if r.status_code == 200:
                live = r.json()
                if isinstance(live, list):
                    for item in live:
                        if isinstance(item, dict) and item.get("name"):
                            mid = item["name"]
                            if not any(x["id"] == mid for x in data):
                                data.append({
                                    "id": mid,
                                    "object": "model",
                                    "owned_by": "pollinations",
                                    "tier": item.get("tier") or "free",
                                    "backend": "pollinations",
                                    "name": item.get("description") or mid,
                                })
    except Exception:
        pass
    return {"object": "list", "data": data, "provider": "streamhub-native"}


@app.post("/v1/chat/completions", tags=["AI"])
async def v1_chat_completions(request: Request):
    """OpenAI-compatible chat. Free via Pollinations — no key required.

    Optional env for stronger models: GROQ_API_KEY, OPENROUTER_API_KEY, GEMINI_API_KEY
    Body: { model, messages: [{role, content}], system? }
    """
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(400, "JSON body required")
    messages = body.get("messages") or []
    if not messages and body.get("text"):
        messages = [{"role": "user", "content": body["text"]}]
    if not messages:
        raise HTTPException(400, "messages required")
    model = body.get("model") or "openai"
    system = body.get("system")
    result = await _ai_chat_route(messages, model=model, system=system)
    if result.get("ok") is False and "choices" not in result:
        raise HTTPException(502, result.get("error") or "chat failed")
    return result


@app.get("/v1/{model_id}/chat", tags=["AI"])
async def v1_model_chat(
    model_id: str,
    text: str = Query(..., min_length=1),
    system: Optional[str] = Query(None),
):
    """Simple GET chat with a model id."""
    messages = [{"role": "user", "content": text}]
    result = await _ai_chat_route(messages, model=model_id, system=system)
    if result.get("ok") is False and "choices" not in result:
        return {"ok": False, "error": result.get("error")}
    content = ""
    try:
        content = result["choices"][0]["message"]["content"]
    except Exception:
        content = str(result)[:1000]
    return {"ok": True, "model": model_id, "text": text, "response": content, "raw": result}


@app.get("/ai/chat", tags=["AI"])
@app.get("/ai/deepseek", tags=["AI"])
@app.get("/ai/metaai", tags=["AI"])
@app.get("/ai/gemini-realtime", tags=["AI"])
async def ai_chat_simple(
    text: str = Query(..., min_length=1, description="prompt"),
    model: str = Query("openai"),
    system: Optional[str] = None,
):
    """Unified simple AI chat (PaxSenix-style GET). Free, unlimited via Pollinations."""
    result = await _ai_chat_route([{"role": "user", "content": text}], model=model, system=system)
    content = ""
    try:
        content = result["choices"][0]["message"]["content"]
    except Exception:
        content = result.get("error") or str(result)[:800]
    return {
        "ok": "choices" in result,
        "model": model,
        "text": text,
        "response": content,
        "provider": result.get("provider") or "pollinations",
    }


@app.post("/ai/chat", tags=["AI"])
async def ai_chat_post(request: Request):
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(400, "JSON body required")
    text = body.get("text") or body.get("prompt") or body.get("message") or ""
    if not text and body.get("messages"):
        return await v1_chat_completions(request)
    if not text:
        raise HTTPException(400, "text required")
    model = body.get("model") or "openai"
    result = await _ai_chat_route([{"role": "user", "content": text}], model=model, system=body.get("system"))
    content = ""
    try:
        content = result["choices"][0]["message"]["content"]
    except Exception:
        content = result.get("error") or str(result)[:800]
    return {"ok": "choices" in result, "model": model, "response": content, "provider": result.get("provider")}


@app.get("/ai-image/generate", tags=["AI Images"])
@app.get("/ai-image/flux", tags=["AI Images"])
@app.get("/ai-image/pollinations", tags=["AI Images"])
@app.get("/ai-image/sdxl", tags=["AI Images"])
async def ai_image_generate(
    prompt: Optional[str] = Query(None),
    text: Optional[str] = Query(None),
    width: int = Query(1024, ge=256, le=2048),
    height: int = Query(1024, ge=256, le=2048),
    model: str = Query("flux"),
    nologo: bool = Query(True),
):
    """Free AI image generation via Pollinations (Flux etc). No API key."""
    p = (prompt or text or "").strip()
    if not p:
        raise HTTPException(400, "prompt or text required")
    # Pollinations image CDN — returns image directly; we return the URL
    img_url = (
        f"https://image.pollinations.ai/prompt/{quote(p)}"
        f"?width={width}&height={height}&model={quote(model)}&nologo={'true' if nologo else 'false'}&enhance=true"
    )
    return {
        "ok": True,
        "provider": "pollinations",
        "model": model,
        "prompt": p,
        "image_url": img_url,
        "url": img_url,
        "width": width,
        "height": height,
        "note": "Open image_url directly — PNG/JPEG from Pollinations CDN",
    }


@app.get("/ai-image/models", tags=["AI Images"])
async def ai_image_models():
    return {
        "ok": True,
        "models": [
            {"id": "flux", "name": "Flux"},
            {"id": "flux-realism", "name": "Flux Realism"},
            {"id": "turbo", "name": "Turbo"},
            {"id": "gptimage", "name": "GPT Image"},
        ],
        "provider": "pollinations",
    }


# ---- Temp Mail (Guerrilla Mail free API) ----
@app.get("/tempmail/create", tags=["TempMail"])
async def tempmail_create():
    """Create a temporary email address (Guerrilla Mail)."""
    async with httpx.AsyncClient(timeout=20.0) as client:
        r = await client.get(
            "https://api.guerrillamail.com/ajax.php",
            params={"f": "get_email_address", "lang": "en"},
        )
        if r.status_code != 200:
            raise HTTPException(502, "tempmail provider error")
        j = r.json()
        return {
            "ok": True,
            "email": j.get("email_addr"),
            "alias": j.get("alias"),
            "sid_token": j.get("sid_token"),
            "timestamp": j.get("email_timestamp"),
            "provider": "guerrillamail",
            "note": "Use sid_token with /tempmail/inbox to check mail",
        }


@app.get("/tempmail/inbox", tags=["TempMail"])
async def tempmail_inbox(
    sid_token: str = Query(..., description="from /tempmail/create"),
    email: Optional[str] = Query(None),
):
    async with httpx.AsyncClient(timeout=20.0) as client:
        r = await client.get(
            "https://api.guerrillamail.com/ajax.php",
            params={"f": "check_email", "sid_token": sid_token, "seq": 0},
        )
        if r.status_code != 200:
            raise HTTPException(502, "inbox fetch failed")
        j = r.json()
        return {"ok": True, "email": email, "count": j.get("count"), "list": j.get("list") or [], "provider": "guerrillamail"}


@app.get("/tempmail/body", tags=["TempMail"])
async def tempmail_body(
    sid_token: str = Query(...),
    message_id: str = Query(..., description="mail_id from inbox"),
):
    async with httpx.AsyncClient(timeout=20.0) as client:
        r = await client.get(
            "https://api.guerrillamail.com/ajax.php",
            params={"f": "fetch_email", "sid_token": sid_token, "email_id": message_id},
        )
        if r.status_code != 200:
            raise HTTPException(502, "body fetch failed")
        j = r.json()
        return {"ok": True, "mail": j, "provider": "guerrillamail"}


@app.get("/tools/urlshorten", tags=["Tools"])
@app.get("/tools/urlshorter", tags=["Tools"])
async def tools_urlshorten(url: str = Query(..., min_length=8)):
    """URL shortener via is.gd (free, no key)."""
    async with httpx.AsyncClient(timeout=15.0) as client:
        r = await client.get("https://is.gd/create.php", params={"format": "json", "url": url})
        if r.status_code != 200:
            raise HTTPException(502, "shorten failed")
        j = r.json()
        return {"ok": True, "url": url, "short": j.get("shorturl"), "provider": "is.gd"}


@app.get("/tools/whois", tags=["Tools"])
async def tools_whois(domain: str = Query(..., min_length=3)):
    """Simple DNS lookup."""
    import socket
    try:
        ips = socket.getaddrinfo(domain, None)
        addrs = sorted({x[4][0] for x in ips})
        return {"ok": True, "domain": domain, "addresses": addrs}
    except Exception as e:
        return {"ok": False, "domain": domain, "error": str(e)}


@app.get("/ai-tools/summarize", tags=["AI Tools"])
async def ai_summarize(
    input: str = Query(..., min_length=1),
    length: str = Query("medium"),
):
    prompt = f"Summarize the following text in a {length} summary. Reply with only the summary:\n\n{input[:6000]}"
    result = await _ai_chat_route([{"role": "user", "content": prompt}], model="openai")
    content = ""
    try:
        content = result["choices"][0]["message"]["content"]
    except Exception:
        content = result.get("error") or ""
    return {"ok": bool(content), "summary": content, "provider": result.get("provider")}


@app.get("/ai-tools/tone-rewrite", tags=["AI Tools"])
async def ai_tone_rewrite(
    text: str = Query(..., min_length=1),
    tone: str = Query("professional"),
):
    prompt = f"Rewrite the text in a {tone} tone. Reply with only the rewritten text:\n\n{text[:4000]}"
    result = await _ai_chat_route([{"role": "user", "content": prompt}], model="openai")
    content = ""
    try:
        content = result["choices"][0]["message"]["content"]
    except Exception:
        content = result.get("error") or ""
    return {"ok": bool(content), "text": content, "tone": tone, "provider": result.get("provider")}



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
