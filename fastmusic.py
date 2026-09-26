# FastMusic — Sketchware / ClulesVibe drop-in (fast)
# api.py: from fastmusic import router as fastmusic_router; app.include_router(fastmusic_router)

from __future__ import annotations

import asyncio
import re
import time
from typing import Optional, Dict, Any

import httpx
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import RedirectResponse, Response, StreamingResponse

router = APIRouter(tags=["FastMusic"])

_STREAM_CACHE: Dict[str, Any] = {}
_STREAM_TTL = 900  # 15 min


def _H():
    import api as _api
    return _api


def _thumb(vid: str) -> str:
    return f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg"


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
    }
    return {k: v for k, v in out.items() if v is not None}


def _pick_audio(data: dict) -> tuple:
    """Prefer AAC/mp4 (itag 140) for ExoPlayer, then any audio."""
    streams = data.get("audio_streams") or []
    # prefer itag 140, then 139, then mp4 mime, then anything
    def score(a):
        itag = str(a.get("itag") or "")
        mime = (a.get("mime") or a.get("mimeType") or "").lower()
        url = a.get("url") or ""
        if not url:
            return -1
        s = 0
        if itag == "140":
            s += 100
        elif itag == "139":
            s += 80
        elif "mp4" in mime or "mp4a" in mime:
            s += 60
        elif "webm" in mime or "opus" in mime:
            s += 20
        br = a.get("bitrate") or 0
        s += min(int(br) // 1000, 30)
        return s

    ranked = sorted(streams, key=score, reverse=True)
    for a in ranked:
        if a.get("url"):
            mime = a.get("mime") or a.get("mimeType") or "audio/mp4"
            if "webm" in mime:
                ct = "audio/webm"
            elif "mp4" in mime or "mp4a" in mime:
                ct = "audio/mp4"
            else:
                ct = "audio/mp4"
            return a["url"], ct
    url = data.get("audio_url")
    if url:
        ct = "audio/webm" if "webm" in url else "audio/mp4"
        return url, ct
    return None, "audio/mp4"


async def _resolve_fast(vid: str, type_: str = "audio") -> dict:
    now = time.time()
    hit = _STREAM_CACHE.get(vid)
    if hit and now - hit["ts"] < _STREAM_TTL and (hit.get("data") or {}).get("ok"):
        return hit["data"]

    api = _H()
    want_audio = (type_ or "audio").lower() in ("audio", "mp3", "m4a", "bestaudio", "best")

    async def do_innertube():
        try:
            return await api._innertube_player(vid, prefer="auto")
        except Exception as e:
            return {"ok": False, "error": str(e)[:120]}

    async def do_loader():
        try:
            return await asyncio.wait_for(api._loader_to_youtube(vid, fmt="mp3"), timeout=20.0)
        except Exception as e:
            return {"ok": False, "error": str(e)[:80]}

    # Race: innertube first 3s, if fail keep waiting loader
    it_task = asyncio.create_task(do_innertube())
    ld_task = asyncio.create_task(do_loader())

    stream_url = None
    mime = "audio/mp4"
    title = vid
    thumb = _thumb(vid)
    duration = None
    provider = None
    err = None

    try:
        done, pending = await asyncio.wait({it_task}, timeout=4.0)
        data = None
        if it_task in done:
            data = it_task.result()
        if data and data.get("ok"):
            title = data.get("title") or title
            thumb = data.get("thumb") or thumb
            duration = data.get("duration")
            provider = data.get("provider") or "innertube"
            if want_audio:
                stream_url, mime = _pick_audio(data)
            else:
                stream_url = data.get("video_url") or data.get("audio_url")
                mime = "video/mp4"
            if stream_url:
                ld_task.cancel()
        else:
            err = (data or {}).get("error")
    except Exception as e:
        err = str(e)[:80]

    if not stream_url:
        try:
            ld = await ld_task
            if ld.get("ok") and ld.get("url"):
                stream_url = ld["url"]
                title = ld.get("title") or title
                thumb = ld.get("thumb") or thumb
                mime = "audio/mpeg"
                provider = ld.get("provider") or "loader"
            else:
                err = (ld or {}).get("error") or err
        except Exception as e:
            err = str(e)[:80]
    else:
        if not ld_task.done():
            ld_task.cancel()

    if not stream_url:
        out = {"ok": False, "error": err or "no stream"}
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
    try:
        if data and data.get("ok"):
            api._innertube_cache[vid] = {"ts": now, "data": data}
    except Exception:
        pass
    return out


@router.get("/download")
@router.get("/fm/download")
@router.get("/api/download")
async def download(
    request: Request,
    id: str = Query(...),
    type: str = Query("audio"),
    quality: str = Query("best"),
    json: bool = Query(False),
    proxy: bool = Query(False),
):
    """youtube-data.deno.dev/download drop-in. Default 302 → CDN (instant)."""
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
    if proxy:
        headers = {
            "User-Agent": "Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36 Chrome/120.0.0.0 Mobile Safari/537.36",
            "Accept": "*/*",
            "Referer": "https://www.youtube.com/" if "googlevideo" in url else "https://loader.to/",
        }
        if request.headers.get("range"):
            headers["Range"] = request.headers["range"]
        client = httpx.AsyncClient(timeout=90.0, follow_redirects=True)
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

        ct = resolved.get("mime") or upstream.headers.get("content-type") or "audio/mp4"
        out_h = {
            "Content-Type": ct,
            "Accept-Ranges": "bytes",
            "Access-Control-Allow-Origin": "*",
            "Cache-Control": "public, max-age=300",
        }
        if upstream.headers.get("content-length"):
            out_h["Content-Length"] = upstream.headers["content-length"]
        if upstream.headers.get("content-range"):
            out_h["Content-Range"] = upstream.headers["content-range"]
        return StreamingResponse(body(), status_code=upstream.status_code, headers=out_h, media_type=ct)

    return RedirectResponse(url=url, status_code=302)


@router.get("/thumbnailHD")
@router.get("/thumbnail")
@router.get("/fm/thumbnailHD")
async def thumbnail_hd(id: str = Query(...), proxy: bool = Query(False)):
    vid = (id or "").replace("yt:", "").strip()
    if not re.match(r"^[\w-]{6,20}$", vid):
        raise HTTPException(400, "invalid video id")
    url = _thumb(vid)
    if proxy:
        async with httpx.AsyncClient(timeout=8.0, follow_redirects=True) as client:
            r = await client.get(url)
            return Response(content=r.content, media_type="image/jpeg",
                            headers={"Cache-Control": "public, max-age=86400"})
    return RedirectResponse(url=url, status_code=302)


@router.get("/search")
@router.get("/fm/search")
async def search(q: str = Query(..., min_length=1), type: str = Query("song"), limit: int = Query(20, ge=1, le=40)):
    api = _H()
    songs = await api._ytm_search_songs(q, limit=limit)
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
        "ok": True, "videoId": vid,
        "title": best.get("trackName") or title,
        "author": best.get("artistName") or "",
        "lyrics": plain, "text": plain, "result": plain,
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
    if id and not (q or title):
        return await lyrics_plain(id=id)
    query = (q or title or "").strip()
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
        "success": True, "ok": True,
        "message": {"lyrics": plain, "text": plain, "title": best.get("trackName"), "artist": best.get("artistName")},
        "lyrics": plain, "text": plain,
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
    vid = id.replace("yt:", "").strip()
    resolved = await _resolve_fast(vid)
    return {
        "ok": True, "videoId": vid,
        "title": resolved.get("title") or vid,
        "thumbnail": resolved.get("thumb") or _thumb(vid),
        "duration": resolved.get("duration"),
        "url": f"/download?id={vid}&type=audio&quality=best",
        "provider": resolved.get("provider"),
    }
