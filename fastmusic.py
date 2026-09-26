# FastMusic + ClulesVibe (PaxSenix Sketchware compatible)
# Import into api.py via: from fastmusic import router as fastmusic_router; app.include_router(fastmusic_router)
# Edit this file to change music download / search / home / next / lyrics / thumbnail.

from __future__ import annotations

import asyncio
import re
import time
import json
from typing import Optional, Any, List, Dict
from urllib.parse import quote

import httpx
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import RedirectResponse, Response, StreamingResponse

router = APIRouter(tags=["FastMusic"])

# ---- helpers imported lazily from api to avoid circular import ----
def _H():
    """Access shared helpers defined on api module after load."""
    import api as _api
    return _api


def _fm_thumb(vid: str, high: bool = True) -> str:
    q = "maxresdefault" if high else "hqdefault"
    return f"https://i.ytimg.com/vi/{vid}/{q}.jpg"


def _cv_item(video_id: str, title: str = "", author: str = "", thumb: str = None, duration=None, **extra) -> dict:
    vid = (video_id or "").replace("yt:", "").strip()
    t1 = thumb or (f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg" if vid else None)
    out = {
        "videoId": vid,
        "title": title or vid,
        "author": author or "Unknown",
        "thumbnail": t1,
        "duration": duration,
        "browseId": extra.get("browseId"),
        "channelId": extra.get("channelId"),
        "subtitle": extra.get("subtitle") or author,
        "subscriberText": extra.get("subscriberText"),
        "text": title or "",
    }
    return {k: v for k, v in out.items() if v is not None}


def _fm_item(video_id: str, title: str = "", author: str = "", thumb: str = None, **extra) -> dict:
    vid = (video_id or "").replace("yt:", "").strip()
    t1 = thumb or _fm_thumb(vid, True)
    t2 = _fm_thumb(vid, False)
    return {
        "videoId": vid,
        "video_id": vid,
        "id": vid,
        "title": title or vid,
        "name": title or vid,
        "author": author or "Unknown",
        "artist": author or "Unknown",
        "thumbnail": t1,
        "thumbnail2": t2,
        "thumb": t1,
        "image": t1,
        "thumbnails": [{"url": t1, "width": 1280}, {"url": t2, "width": 480}],
        "url": f"/download?id={vid}&type=audio&quality=best",
        "stream": f"/download?id={vid}&type=audio&quality=best",
        "download": f"/download?id={vid}&type=audio&quality=best",
        "provider": "fastmusic",
        **{k: v for k, v in extra.items() if v is not None},
    }


async def _resolve_stream(vid: str, type_: str = "audio", quality: str = "best") -> dict:
    """Resolve direct CDN URL: InnerTube first, then loader.to."""
    api = _H()
    prefer = "music" if (type_ or "audio").lower() in ("audio", "mp3", "m4a", "bestaudio") else "auto"
    data = await api._innertube_player(vid, prefer=prefer)
    try:
        if data.get("ok"):
            api._innertube_cache[vid] = {"ts": time.time(), "data": data}
    except Exception:
        pass
    stream_url = None
    mime = "audio/mp4"
    if data.get("ok"):
        t = (type_ or "audio").lower()
        q = (quality or "best").lower()
        if t in ("audio", "mp3", "m4a", "bestaudio") or q in ("audio", "mp3"):
            stream_url = data.get("audio_url")
            if not stream_url:
                for a in (data.get("audio_streams") or []):
                    if a.get("url"):
                        stream_url = a["url"]
                        break
            mime = "audio/webm" if stream_url and "webm" in (stream_url or "") else "audio/mp4"
        else:
            target = {"360": 360, "480": 480, "720": 720, "1080": 1080, "best": 9999}.get(q, 720)
            pool = data.get("video_streams") or []
            progressive = [v for v in pool if v.get("progressive")]
            use = progressive or pool
            use = sorted(use, key=lambda x: abs((x.get("height") or 0) - target))
            for v in use:
                if target >= 9999 or (v.get("height") or 0) <= target + 80:
                    stream_url = v.get("url")
                    break
            if not stream_url:
                stream_url = data.get("video_url") or data.get("audio_url")
            mime = "video/mp4"
    if not stream_url:
        try:
            ld = await asyncio.wait_for(
                api._loader_to_youtube(vid, fmt="mp3" if "audio" in (type_ or "audio").lower() else "360"),
                timeout=20.0,
            )
            if ld.get("ok") and ld.get("url"):
                return {
                    "ok": True,
                    "url": ld["url"],
                    "title": ld.get("title") or vid,
                    "thumb": ld.get("thumb") or _fm_thumb(vid),
                    "mime": "audio/mpeg",
                    "provider": ld.get("provider") or "loader",
                }
        except Exception:
            pass
        return {"ok": False, "error": (data.get("error") if isinstance(data, dict) else None) or "no stream"}
    return {
        "ok": True,
        "url": stream_url,
        "title": data.get("title") or vid,
        "thumb": data.get("thumb") or _fm_thumb(vid),
        "mime": mime,
        "duration": data.get("duration"),
        "provider": data.get("provider") or "innertube",
        "audio_url": data.get("audio_url"),
        "video_url": data.get("video_url"),
    }


async def _proxy_stream(url: str, mime: str, request: Request = None):
    """Pipe upstream media to client — ExoPlayer-friendly (no 302)."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "*/*",
    }
    # host-specific referer
    if "googlevideo.com" in url or "youtube.com" in url:
        headers["Referer"] = "https://www.youtube.com/"
        headers["Origin"] = "https://www.youtube.com"
    elif "savenow" in url or "loader" in url or "oceansaver" in url or "affadaffa" in url:
        headers["Referer"] = "https://loader.to/"
    else:
        headers["Referer"] = "https://www.youtube.com/"
    range_h = None
    if request is not None:
        range_h = request.headers.get("range") or request.headers.get("Range")
        if range_h:
            headers["Range"] = range_h

    client = httpx.AsyncClient(timeout=60.0, follow_redirects=True)
    req = client.build_request("GET", url, headers=headers)
    upstream = await client.send(req, stream=True)
    if upstream.status_code >= 400:
        await upstream.aclose()
        await client.aclose()
        raise HTTPException(upstream.status_code, f"upstream {upstream.status_code}")

    out_headers = {
        "Accept-Ranges": "bytes",
        "Cache-Control": "no-cache",
        "Access-Control-Allow-Origin": "*",
    }
    ct = upstream.headers.get("content-type") or mime or "audio/mp4"
    # normalize for ExoPlayer
    if "webm" in ct:
        ct = "audio/webm"
    elif "mp4" in ct or "m4a" in ct or "aac" in ct:
        ct = "audio/mp4"
    elif "mpeg" in ct or "mp3" in ct:
        ct = "audio/mpeg"
    out_headers["Content-Type"] = ct
    if upstream.headers.get("content-length"):
        out_headers["Content-Length"] = upstream.headers["content-length"]
    if upstream.headers.get("content-range"):
        out_headers["Content-Range"] = upstream.headers["content-range"]

    status = upstream.status_code  # 200 or 206

    async def body():
        try:
            async for chunk in upstream.aiter_bytes(64 * 1024):
                yield chunk
        finally:
            await upstream.aclose()
            await client.aclose()

    return StreamingResponse(body(), status_code=status, headers=out_headers, media_type=ct)


# ---- PLAYABLE DOWNLOAD (ExoPlayer MediaSource URI) ----
# App: Uri.parse(base + "/download?id=" + videoId + "&type=audio&quality=best")
# Default: PROXY stream (not 302) so ExoPlayer gets real audio bytes + duration

@router.get("/download")
@router.get("/fm/download")
@router.get("/api/download")
@router.get("/pax/download")
@router.get("/fastmusic/download")
async def fm_download_playable(
    request: Request,
    id: str = Query(..., description="YouTube video id"),
    type: str = Query("audio", description="audio|video"),
    quality: str = Query("best", description="best|360|480|720|1080|audio"),
    redirect: bool = Query(False, description="302 to CDN (VLC); default proxy for ExoPlayer"),
    as_json: bool = Query(False, alias="json", description="JSON meta + url"),
):
    """Drop-in for youtube-data.deno.dev/download?id=&type=audio&quality=best

    Default: streams audio bytes through this server (ExoPlayer plays + shows duration).
    ?redirect=1 → 302 to CDN (VLC-style).
    ?json=1 → metadata only.
    """
    vid = (id or "").replace("yt:", "").strip()
    if not re.match(r"^[\w-]{6,20}$", vid):
        raise HTTPException(400, "invalid video id")
    resolved = await _resolve_stream(vid, type_=type, quality=quality)
    if not resolved.get("ok"):
        raise HTTPException(502, resolved.get("error") or "stream unavailable")
    stream_url = resolved["url"]
    mime = resolved.get("mime") or "audio/mp4"
    if as_json:
        return {
            "ok": True,
            "videoId": vid,
            "title": resolved.get("title"),
            "author": "",
            "thumbnail": resolved.get("thumb"),
            "thumbnail2": _fm_thumb(vid, False),
            "url": stream_url,
            "directUrl": stream_url,
            "type": type,
            "quality": quality,
            "mime": mime,
            "duration": resolved.get("duration"),
            "provider": resolved.get("provider"),
        }
    if redirect:
        return RedirectResponse(url=stream_url, status_code=302)
    # Proxy stream — ExoPlayer friendly (duration + progressive play)
    try:
        return await _proxy_stream(stream_url, mime, request)
    except Exception as e:
        # last resort: redirect (VLC / some players follow)
        return RedirectResponse(url=stream_url, status_code=302)


@router.get("/fm/play")
@router.get("/pax/play")
@router.get("/fastmusic/play")
async def fm_play_json(
    id: str = Query(...),
    type: str = Query("audio"),
    quality: str = Query("best"),
):
    resolved = await _resolve_stream(id.replace("yt:", "").strip(), type_=type, quality=quality)
    if not resolved.get("ok"):
        raise HTTPException(502, resolved.get("error") or "stream unavailable")
    return {
        "ok": True,
        "videoId": id,
        "title": resolved.get("title"),
        "url": resolved.get("url"),
        "directUrl": resolved.get("url"),
        "mime": resolved.get("mime"),
        "duration": resolved.get("duration"),
        "provider": resolved.get("provider"),
        "play_url": f"/download?id={id}&type={type}&quality={quality}",
    }


# ---- Thumbnail HD ----
@router.get("/thumbnailHD")
@router.get("/fm/thumbnailHD")
@router.get("/pax/thumbnailHD")
@router.get("/api/thumbnailHD")
@router.get("/thumbnail")
@router.get("/fm/thumbnail")
async def fm_thumbnail_hd(
    id: str = Query(...),
    redirect: bool = Query(True),
    proxy: bool = Query(False),
):
    vid = (id or "").replace("yt:", "").strip()
    if not re.match(r"^[\w-]{6,20}$", vid):
        raise HTTPException(400, "invalid video id")
    candidates = [
        f"https://i.ytimg.com/vi/{vid}/maxresdefault.jpg",
        f"https://i.ytimg.com/vi/{vid}/sddefault.jpg",
        f"https://i.ytimg.com/vi/{vid}/hqdefault.jpg",
        f"https://i.ytimg.com/vi/{vid}/mqdefault.jpg",
    ]
    chosen = candidates[2]
    try:
        async with httpx.AsyncClient(timeout=4.0, follow_redirects=True) as client:
            for u in candidates:
                try:
                    r = await client.head(u)
                    cl = int(r.headers.get("content-length") or 0)
                    if r.status_code == 200 and cl > 8000:
                        chosen = u
                        break
                except Exception:
                    continue
    except Exception:
        pass
    if proxy:
        async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
            r = await client.get(chosen)
            if r.status_code != 200:
                r = await client.get(candidates[2])
            return Response(
                content=r.content,
                media_type=r.headers.get("content-type") or "image/jpeg",
                headers={"Cache-Control": "public, max-age=86400"},
            )
    if redirect:
        return RedirectResponse(url=chosen, status_code=302)
    return {
        "ok": True,
        "videoId": vid,
        "url": chosen,
        "thumbnail": chosen,
        "thumbnailHD": chosen,
        "thumbnail2": candidates[2],
        "provider": "ytimg",
    }


# ---- ClulesVibe RapidAPI path drop-ins ----
@router.get("/search")
async def cv_search(
    q: str = Query(..., min_length=1),
    type: str = Query("song"),
    limit: int = Query(20, ge=1, le=50),
):
    api = _H()
    t = (type or "song").lower()
    songs = await api._ytm_search_songs(q, limit=limit)
    if t in ("artist", "artists"):
        seen, items = set(), []
        for s in songs:
            a = (s.get("artist") or "").strip()
            if not a or a.lower() in seen:
                continue
            seen.add(a.lower())
            items.append(_cv_item(s.get("video_id") or "", title=a, author=a, thumb=s.get("thumb"), subtitle="Artist"))
        return {"result": items, "results": items, "ok": True}
    items = [
        _cv_item(s["video_id"], s.get("title") or "", s.get("artist") or "", s.get("thumb"), s.get("duration"))
        for s in songs
    ]
    return {"result": items, "results": items, "ok": True, "query": q, "type": type}


@router.get("/v2/home")
async def cv_v2_home():
    api = _H()
    sections = []
    try:
        home = await api._ytm_home_sections()
        for sec in home:
            items = [
                _cv_item(s["video_id"], s.get("title") or "", s.get("artist") or "", s.get("thumb"))
                for s in (sec.get("items") or []) if s.get("video_id")
            ]
            if items:
                sections.append({"title": sec.get("title") or "Home", "contents": items, "items": items})
    except Exception:
        pass
    if not sections:
        for title, q in [("You might also like", "Top songs this week"), ("Trending", "Trending music"), ("Quick picks", "pop hits")]:
            try:
                songs = await api._ytm_search_songs(q, limit=12)
                items = [_cv_item(s["video_id"], s.get("title") or "", s.get("artist") or "", s.get("thumb")) for s in songs]
                if items:
                    sections.append({"title": title, "contents": items, "items": items})
            except Exception:
                continue
    flat = []
    for s in sections:
        flat.extend(s.get("items") or [])
    return {"result": sections, "results": sections, "items": flat, "ok": bool(sections)}


@router.get("/next")
async def cv_next(id: str = Query(...)):
    api = _H()
    vid = id.replace("yt:", "").strip()
    songs = await api._ytm_next_songs(vid, limit=25)
    items = [_cv_item(s["video_id"], s.get("title") or "", s.get("artist") or "", s.get("thumb")) for s in songs]
    return {"result": items, "results": items, "items": items, "ok": bool(items), "videoId": vid}


@router.get("/music/lyrics/plain")
async def cv_lyrics_plain(id: str = Query(...)):
    api = _H()
    vid = id.replace("yt:", "").strip()
    title = vid
    try:
        pl = await api._innertube_player(vid)
        if pl.get("ok"):
            title = pl.get("title") or title
    except Exception:
        pass
    async with httpx.AsyncClient(timeout=15.0) as client:
        r = await client.get("https://lrclib.net/api/search", params={"q": title})
        arr = r.json() if r.status_code == 200 else []
    if not arr:
        return {"result": None, "lyrics": None, "text": None, "ok": False, "videoId": vid}
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
        "source": "lrclib",
    }


@router.get("/getAlbum")
async def cv_get_album(id: str = Query(...)):
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
                songs.append(_cv_item(e["video_id"], e.get("title") or "", e.get("artist") or "", e.get("thumb")))
        return {"result": songs, "results": songs, "items": songs, "ok": bool(songs), "albumId": id}
    except Exception as e:
        return {"ok": False, "result": [], "error": str(e)[:120]}


@router.get("/getArtists")
@router.get("/getArtist")
async def cv_get_artists(id: str = Query(...)):
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
                songs.append(_cv_item(e["video_id"], e.get("title") or "", e.get("artist") or "", e.get("thumb")))
        return {"result": songs, "results": songs, "items": songs[:40], "ok": bool(songs), "artistId": id}
    except Exception as e:
        return {"ok": False, "result": [], "error": str(e)[:120]}


@router.get("/songs/lyrics")
async def cv_musixmatch_lyrics(
    track_id: Optional[str] = Query(None),
    id: Optional[str] = Query(None),
    type: Optional[str] = Query(None),
    q: Optional[str] = Query(None),
    title: Optional[str] = Query(None),
    artist: Optional[str] = Query(""),
):
    api = _H()
    vid = (id or track_id or "").replace("yt:", "").strip() if (id or track_id) else ""
    query = (q or title or "").strip()
    if vid and not query:
        try:
            pl = await api._innertube_player(vid)
            query = (pl.get("title") if pl.get("ok") else None) or vid
        except Exception:
            query = vid
    if not query:
        raise HTTPException(400, "title, q, or id required")
    async with httpx.AsyncClient(timeout=15.0) as client:
        r = await client.get("https://lrclib.net/api/search", params={"q": f"{query} {artist or ''}".strip()})
        arr = r.json() if r.status_code == 200 else []
    if not arr:
        return {"success": False, "message": {"lyrics": None, "text": None}}
    best = arr[0]
    plain = best.get("plainLyrics") or best.get("syncedLyrics") or ""
    return {
        "success": True,
        "message": {
            "lyrics": plain,
            "text": plain,
            "synced": best.get("syncedLyrics"),
            "title": best.get("trackName"),
            "artist": best.get("artistName"),
        },
        "lyrics": plain,
        "text": plain,
        "ok": True,
    }


# ---- /fm/* aliases ----
@router.get("/fm/search")
@router.get("/pax/search")
@router.get("/fastmusic/search")
@router.get("/search/music")
async def fm_search(q: str = Query(..., min_length=1), type: str = Query("song"), limit: int = Query(20, ge=1, le=50)):
    return await cv_search(q=q, type=type, limit=limit)


@router.get("/fm/home")
@router.get("/pax/home")
@router.get("/fastmusic/home")
async def fm_home():
    return await cv_v2_home()


@router.get("/fm/next")
@router.get("/fm/next-v2")
@router.get("/pax/next")
@router.get("/fastmusic/next")
async def fm_next(
    id: Optional[str] = Query(None),
    videoId: Optional[str] = Query(None),
    video_id: Optional[str] = Query(None),
    limit: int = Query(25, ge=1, le=50),
):
    vid = str(videoId or video_id or id or "").replace("yt:", "").strip()
    if not vid:
        raise HTTPException(400, "id or videoId required")
    api = _H()
    songs = await api._ytm_next_songs(vid, limit=limit)
    items = [_fm_item(s["video_id"], s.get("title") or "", s.get("artist") or "", s.get("thumb")) for s in songs]
    return {
        "ok": bool(items),
        "videoId": vid,
        "sections": [{"title": "You might also like", "items": items}],
        "items": items,
        "result": items,
        "count": len(items),
        "provider": "fastmusic",
    }


@router.get("/fm/related")
@router.get("/pax/related")
async def fm_related(id: str = Query(...), limit: int = Query(20, ge=1, le=50)):
    return await fm_next(id=id, limit=limit)


@router.get("/fm/info")
@router.get("/fm/info-v2")
@router.get("/fm/get")
@router.get("/pax/info")
async def fm_info(id: str = Query(...)):
    api = _H()
    vid = id.replace("yt:", "").strip()
    title, author, thumb, duration = vid, "", _fm_thumb(vid), None
    try:
        pl = await api._innertube_player(vid)
        if pl.get("ok"):
            title = pl.get("title") or title
            duration = pl.get("duration")
            thumb = pl.get("thumb") or thumb
    except Exception:
        pass
    item = _fm_item(vid, title, author, thumb, duration=duration, lengthSeconds=duration)
    return {"ok": True, "result": item, **item, "provider": "fastmusic"}


@router.get("/fm/lyrics")
@router.get("/pax/lyrics")
async def fm_lyrics(id: Optional[str] = Query(None), title: Optional[str] = Query(None), artist: Optional[str] = Query("")):
    if id:
        return await cv_lyrics_plain(id=id)
    return await cv_musixmatch_lyrics(title=title, artist=artist, q=title)


@router.get("/fm/match")
@router.get("/pax/match")
async def fm_match(title: str = Query(...), artist: str = Query("")):
    api = _H()
    songs = await api._ytm_search_songs(f"{title} {artist}".strip(), limit=5)
    if not songs:
        return {"ok": False, "result": None}
    s = songs[0]
    return {"ok": True, "result": _fm_item(s["video_id"], s.get("title") or "", s.get("artist") or "", s.get("thumb")), "provider": "fastmusic"}


@router.get("/fm/trending")
@router.get("/pax/trending")
async def fm_trending(limit: int = Query(20, ge=1, le=40)):
    api = _H()
    songs = await api._ytm_search_songs("Top songs this week", limit=limit)
    items = [_fm_item(s["video_id"], s.get("title") or "", s.get("artist") or "", s.get("thumb")) for s in songs]
    return {"ok": bool(items), "sections": [{"title": "Trending", "items": items}], "items": items, "result": items, "count": len(items), "provider": "fastmusic"}


@router.get("/fm/playlist")
@router.get("/pax/playlist")
async def fm_playlist(id: str = Query(...)):
    return await cv_get_album(id=id if id.startswith("VL") or id.startswith("PL") else f"VL{id}")


@router.get("/fm/album")
@router.get("/pax/album")
async def fm_album(id: str = Query(...)):
    return await cv_get_album(id=id)


@router.get("/fm/artist")
@router.get("/pax/artist")
async def fm_artist(id: str = Query(...)):
    return await cv_get_artists(id=id)


@router.get("/fm/suggestions")
@router.get("/pax/suggestions")
async def fm_suggestions(q: str = Query(..., min_length=1)):
    api = _H()
    songs = await api._ytm_search_songs(q, limit=12)
    suggestions, seen = [], set()
    for s in songs:
        for text in (s.get("title"), f"{s.get('title')} - {s.get('artist')}"):
            t = (text or "").strip()
            if t and t.lower() not in seen:
                seen.add(t.lower())
                suggestions.append(t)
    return {"ok": True, "query": q, "suggestions": suggestions[:15], "provider": "fastmusic"}
