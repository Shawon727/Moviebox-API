# FastMusic + ClulesVibe — fast path (SimpMusic-style)
# Wired from api.py: from fastmusic import router as fastmusic_router; app.include_router(fastmusic_router)
#
# Speed rules:
#  1) Resolve stream once, cache 10 min
#  2) Default 302 → googlevideo CDN (instant ExoPlayer / VLC)
#  3) Only one InnerTube client first; loader only if blocked
#  4) Minimal public routes (ClulesVibe + short aliases)

from __future__ import annotations

import asyncio
import re
import time
from typing import Optional, Dict, Any

import httpx
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import RedirectResponse, Response

router = APIRouter(tags=["FastMusic"])

# ---- resolve cache: video_id -> {ts, data} ----
_STREAM_CACHE: Dict[str, Any] = {}
_STREAM_TTL = 600  # 10 minutes


def _H():
    import api as _api
    return _api


def _thumb(vid: str, hq: bool = True) -> str:
    return f"https://i.ytimg.com/vi/{vid}/{'hqdefault' if hq else 'mqdefault'}.jpg"


def _item(video_id: str, title: str = "", author: str = "", thumb: str = None, duration=None, **extra) -> dict:
    vid = (video_id or "").replace("yt:", "").strip()
    t1 = thumb or (_thumb(vid) if vid else None)
    out = {
        "videoId": vid,
        "title": title or vid,
        "author": author or "Unknown",
        "thumbnail": t1,
        "duration": duration,
        "subtitle": extra.get("subtitle") or author,
        "text": title or "",
        "browseId": extra.get("browseId"),
        "channelId": extra.get("channelId"),
    }
    return {k: v for k, v in out.items() if v is not None}


async def _resolve_fast(vid: str, type_: str = "audio") -> dict:
    """Fast resolve with cache. ANDROID first (~0.2s), loader only on miss."""
    now = time.time()
    hit = _STREAM_CACHE.get(vid)
    if hit and now - hit["ts"] < _STREAM_TTL and (hit.get("data") or {}).get("ok"):
        return hit["data"]

    api = _H()
    # prefer single fast client path via innertube (already parallel ANDROID+IOS inside)
    data = await api._innertube_player(vid, prefer="auto")
    stream_url = None
    mime = "audio/mp4"
    title = vid
    thumb = _thumb(vid)
    duration = None
    provider = "innertube"

    if data.get("ok"):
        title = data.get("title") or title
        thumb = data.get("thumb") or thumb
        duration = data.get("duration")
        provider = data.get("provider") or provider
        if (type_ or "audio").lower() in ("audio", "mp3", "m4a", "bestaudio"):
            stream_url = data.get("audio_url")
            if not stream_url:
                for a in (data.get("audio_streams") or []):
                    if a.get("url"):
                        stream_url = a["url"]
                        break
            if stream_url and "webm" in stream_url:
                mime = "audio/webm"
        else:
            stream_url = data.get("video_url") or data.get("audio_url")
            mime = "video/mp4"

    # blocked IP → loader (slow, only if needed)
    if not stream_url:
        try:
            ld = await asyncio.wait_for(api._loader_to_youtube(vid, fmt="mp3"), timeout=18.0)
            if ld.get("ok") and ld.get("url"):
                stream_url = ld["url"]
                title = ld.get("title") or title
                thumb = ld.get("thumb") or thumb
                mime = "audio/mpeg"
                provider = ld.get("provider") or "loader"
        except Exception:
            pass

    if not stream_url:
        out = {"ok": False, "error": (data.get("error") if isinstance(data, dict) else None) or "no stream"}
        _STREAM_CACHE[vid] = {"ts": now, "data": out}
        return out

    out = {
        "ok": True,
        "url": stream_url,
        "title": title,
        "thumb": thumb,
        "mime": mime,
        "duration": duration,
        "provider": provider,
    }
    _STREAM_CACHE[vid] = {"ts": now, "data": out}
    # also warm api-level cache
    try:
        if data.get("ok"):
            api._innertube_cache[vid] = {"ts": now, "data": data}
    except Exception:
        pass
    return out


# =============================================================================
# DOWNLOAD — instant 302 to CDN (SimpMusic-style)
# App: .../download?id=VIDEO_ID&type=audio&quality=best
# =============================================================================

@router.get("/download")
@router.get("/fm/download")
async def download(
    request: Request,
    id: str = Query(...),
    type: str = Query("audio"),
    quality: str = Query("best"),
    json: bool = Query(False, description="return JSON instead of redirect"),
    proxy: bool = Query(False, description="slow: pipe bytes via server"),
):
    vid = (id or "").replace("yt:", "").strip()
    if not re.match(r"^[\w-]{6,20}$", vid):
        raise HTTPException(400, "invalid video id")

    resolved = await _resolve_fast(vid, type_=type)
    if not resolved.get("ok"):
        raise HTTPException(502, resolved.get("error") or "stream unavailable")

    url = resolved["url"]
    if json:
        return {
            "ok": True,
            "videoId": vid,
            "title": resolved.get("title"),
            "thumbnail": resolved.get("thumb"),
            "url": url,
            "directUrl": url,
            "mime": resolved.get("mime"),
            "duration": resolved.get("duration"),
            "provider": resolved.get("provider"),
        }

    # proxy only if explicitly requested (slow on serverless)
    if proxy:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "*/*",
            "Referer": "https://www.youtube.com/" if "googlevideo" in url else "https://loader.to/",
        }
        if request.headers.get("range"):
            headers["Range"] = request.headers["range"]
        from fastapi.responses import StreamingResponse
        client = httpx.AsyncClient(timeout=60.0, follow_redirects=True)
        upstream = await client.send(client.build_request("GET", url, headers=headers), stream=True)
        if upstream.status_code >= 400:
            await upstream.aclose()
            await client.aclose()
            return RedirectResponse(url=url, status_code=302)

        async def body():
            try:
                async for chunk in upstream.aiter_bytes(65536):
                    yield chunk
            finally:
                await upstream.aclose()
                await client.aclose()

        ct = upstream.headers.get("content-type") or resolved.get("mime") or "audio/mp4"
        out_h = {
            "Content-Type": ct,
            "Accept-Ranges": "bytes",
            "Access-Control-Allow-Origin": "*",
            "Cache-Control": "no-cache",
        }
        if upstream.headers.get("content-length"):
            out_h["Content-Length"] = upstream.headers["content-length"]
        if upstream.headers.get("content-range"):
            out_h["Content-Range"] = upstream.headers["content-range"]
        return StreamingResponse(body(), status_code=upstream.status_code, headers=out_h, media_type=ct)

    # DEFAULT: instant 302 → CDN (click → play)
    return RedirectResponse(url=url, status_code=302)


# =============================================================================
# THUMBNAIL
# =============================================================================

@router.get("/thumbnailHD")
@router.get("/thumbnail")
@router.get("/fm/thumbnailHD")
async def thumbnail_hd(
    id: str = Query(...),
    proxy: bool = Query(False),
):
    vid = (id or "").replace("yt:", "").strip()
    if not re.match(r"^[\w-]{6,20}$", vid):
        raise HTTPException(400, "invalid video id")
    # hqdefault is always valid & fast (no HEAD probe)
    url = f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg"
    if proxy:
        async with httpx.AsyncClient(timeout=8.0, follow_redirects=True) as client:
            r = await client.get(url)
            return Response(
                content=r.content,
                media_type="image/jpeg",
                headers={"Cache-Control": "public, max-age=86400"},
            )
    return RedirectResponse(url=url, status_code=302)


# =============================================================================
# SEARCH / HOME / NEXT / LYRICS / ALBUM / ARTIST
# =============================================================================

@router.get("/search")
@router.get("/fm/search")
async def search(
    q: str = Query(..., min_length=1),
    type: str = Query("song"),
    limit: int = Query(20, ge=1, le=40),
):
    api = _H()
    songs = await api._ytm_search_songs(q, limit=limit)
    t = (type or "song").lower()
    if t in ("artist", "artists"):
        seen, items = set(), []
        for s in songs:
            a = (s.get("artist") or "").strip()
            if not a or a.lower() in seen:
                continue
            seen.add(a.lower())
            items.append(_item(s.get("video_id") or "", title=a, author=a, thumb=s.get("thumb"), subtitle="Artist"))
        return {"result": items, "results": items, "ok": True}
    items = [
        _item(s["video_id"], s.get("title") or "", s.get("artist") or "", s.get("thumb"), s.get("duration"))
        for s in songs
    ]
    return {"result": items, "results": items, "ok": True, "query": q, "type": type}


@router.get("/v2/home")
@router.get("/fm/home")
async def home():
    api = _H()
    sections = []
    try:
        for sec in await api._ytm_home_sections():
            items = [
                _item(s["video_id"], s.get("title") or "", s.get("artist") or "", s.get("thumb"))
                for s in (sec.get("items") or []) if s.get("video_id")
            ]
            if items:
                sections.append({"title": sec.get("title") or "Home", "contents": items, "items": items})
    except Exception:
        pass
    if not sections:
        for title, q in (("You might also like", "Top songs this week"), ("Trending", "Trending music")):
            try:
                songs = await api._ytm_search_songs(q, limit=12)
                items = [_item(s["video_id"], s.get("title") or "", s.get("artist") or "", s.get("thumb")) for s in songs]
                if items:
                    sections.append({"title": title, "contents": items, "items": items})
            except Exception:
                continue
    flat = [i for s in sections for i in (s.get("items") or [])]
    return {"result": sections, "results": sections, "items": flat, "ok": bool(sections)}


@router.get("/next")
@router.get("/fm/next")
async def next_songs(id: str = Query(...)):
    api = _H()
    vid = id.replace("yt:", "").strip()
    songs = await api._ytm_next_songs(vid, limit=25)
    items = [_item(s["video_id"], s.get("title") or "", s.get("artist") or "", s.get("thumb")) for s in songs]
    return {"result": items, "results": items, "items": items, "ok": bool(items), "videoId": vid}


@router.get("/music/lyrics/plain")
@router.get("/fm/lyrics")
async def lyrics_plain(id: str = Query(...)):
    api = _H()
    vid = id.replace("yt:", "").strip()
    title = vid
    # use cache if available
    hit = _STREAM_CACHE.get(vid)
    if hit and (hit.get("data") or {}).get("title"):
        title = hit["data"]["title"]
    else:
        try:
            pl = await api._innertube_player(vid)
            if pl.get("ok"):
                title = pl.get("title") or title
        except Exception:
            pass
    async with httpx.AsyncClient(timeout=12.0) as client:
        r = await client.get("https://lrclib.net/api/search", params={"q": title})
        arr = r.json() if r.status_code == 200 else []
    if not arr:
        return {"ok": False, "lyrics": None, "text": None, "result": None, "videoId": vid}
    best = arr[0]
    plain = best.get("plainLyrics") or best.get("syncedLyrics") or ""
    return {
        "ok": True,
        "videoId": vid,
        "title": best.get("trackName") or title,
        "author": best.get("artistName") or "",
        "lyrics": plain,
        "text": plain,
        "result": plain,
        "synced": best.get("syncedLyrics"),
    }


@router.get("/songs/lyrics")
async def songs_lyrics(
    id: Optional[str] = Query(None),
    q: Optional[str] = Query(None),
    title: Optional[str] = Query(None),
    artist: Optional[str] = Query(""),
    type: Optional[str] = Query(None),
):
    query = (q or title or "").strip()
    if id and not query:
        return await lyrics_plain(id=id)
    if not query:
        raise HTTPException(400, "q, title, or id required")
    async with httpx.AsyncClient(timeout=12.0) as client:
        r = await client.get("https://lrclib.net/api/search", params={"q": f"{query} {artist or ''}".strip()})
        arr = r.json() if r.status_code == 200 else []
    if not arr:
        return {"success": False, "message": {"lyrics": None, "text": None}}
    best = arr[0]
    plain = best.get("plainLyrics") or best.get("syncedLyrics") or ""
    return {
        "success": True,
        "ok": True,
        "message": {"lyrics": plain, "text": plain, "title": best.get("trackName"), "artist": best.get("artistName")},
        "lyrics": plain,
        "text": plain,
    }


@router.get("/getAlbum")
@router.get("/fm/album")
async def get_album(id: str = Query(...)):
    api = _H()
    try:
        data = await api._ytm_post("browse", {"browseId": id})
        raw = []
        api._ytm_walk(data, "musicResponsiveListItemRenderer", raw)
        songs, seen = [], set()
        for it in raw:
            e = api._ytm_parse_item(it)
            if e and e["video_id"] not in seen:
                seen.add(e["video_id"])
                songs.append(_item(e["video_id"], e.get("title") or "", e.get("artist") or "", e.get("thumb")))
        return {"result": songs, "results": songs, "items": songs, "ok": bool(songs)}
    except Exception as e:
        return {"ok": False, "result": [], "error": str(e)[:100]}


@router.get("/getArtists")
@router.get("/getArtist")
@router.get("/fm/artist")
async def get_artists(id: str = Query(...)):
    api = _H()
    try:
        data = await api._ytm_post("browse", {"browseId": id})
        raw = []
        api._ytm_walk(data, "musicResponsiveListItemRenderer", raw)
        songs, seen = [], set()
        for it in raw:
            e = api._ytm_parse_item(it)
            if e and e["video_id"] not in seen:
                seen.add(e["video_id"])
                songs.append(_item(e["video_id"], e.get("title") or "", e.get("artist") or "", e.get("thumb")))
        return {"result": songs, "results": songs, "items": songs[:40], "ok": bool(songs)}
    except Exception as e:
        return {"ok": False, "result": [], "error": str(e)[:100]}


@router.get("/fm/info")
async def info(id: str = Query(...)):
    resolved = await _resolve_fast(id.replace("yt:", "").strip())
    vid = id.replace("yt:", "").strip()
    return {
        "ok": True,
        "videoId": vid,
        "title": resolved.get("title") or vid,
        "author": "",
        "thumbnail": resolved.get("thumb") or _thumb(vid),
        "duration": resolved.get("duration"),
        "url": f"/download?id={vid}&type=audio&quality=best",
    }
