<p align="center">
  <img src="https://h5-static.aoneroom.com/ssrStatic/mbOfficial/public/_nuxt/web-logo.apJjVir2.svg" alt="StreamHub" width="180"/>
</p>

<p align="center">
  <strong>StreamHub API</strong> — multi-provider movies · series · anime · music · downloaders · tools
</p>

<p align="center">
  <a href="https://fastapi.tiangolo.com/"><img src="https://img.shields.io/badge/FastAPI-005571?style=for-the-badge&logo=fastapi" alt="FastAPI"/></a>
  <a href="https://www.python.org/"><img src="https://img.shields.io/badge/Python-3.9+-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python"/></a>
  <img src="https://img.shields.io/badge/Status-Active-4c1?style=for-the-badge" alt="Status"/>
  <img src="https://img.shields.io/badge/License-MIT-blue.svg?style=for-the-badge" alt="License"/>
  <img src="https://img.shields.io/badge/Version-5.x-orange?style=for-the-badge" alt="Version"/>
</p>

> **Made by Shawon** · Repo: [github.com/Shawon727/StreamHub-Api](https://github.com/Shawon727/StreamHub-Api)

---

## What this is

One FastAPI backend (`api.py`) that aggregates **many streaming & media providers** behind clean REST endpoints. Every JSON response includes:

```json
{ "creator": "shawon", ... }
```

| Area | Prefix | What you get |
|------|--------|----------------|
| **MovieBox** | `/mb/*` · `/moviebox/*` | HMAC mobile API — search, detail, DASH/MP4 streams |
| **MovieBox New** | `/mbn/*` | movieboxhd.net H5 + mobile BFF, real sbcdn DASH |
| **PirateBot** | `/pb/*` | TMDB proxy (no key) + multi embed servers |
| **CineStream** | `/cs/*` · `/cinestream/*` | ToonStream catalog + multi-source episode streams |
| **Anime** | `/anime/*` | HindiAnime home/search/detail/episodes + HLS proxy |
| **4K / Hub** | `/fk/*` · `/tools/*` | 4KHDHub scrape, HubCloud/HubDrive/Pixeldrain resolve |
| **Music** | `/music/*` · `/yt-music/*` | JioSaavn, YT Music (InnerTube CDN), lyrics, charts |
| **YouTube** | `/yt/*` · `/youtube/*` | Player + direct googlevideo CDN streams |
| **Downloader** | `/dl/*` | YouTube, TikTok, Instagram, Spotify, … smart extract |
| **Spotify / Deezer** | `/spotify/*` · `/deezer/*` | Search, track, album, playlist meta |
| **Billboard** | `/billboard/*` | Hot 100, Global 200, artists, albums charts |
| **AI** | `/ai/*` · `/ai-image/*` | Chat + image generation helpers |
| **Tools** | `/tools/*` | Translate, TTS, URL short, screenshot, web search |
| **Docs UI** | `/docs` | Interactive OpenAPI (Swagger) |
| **Web SPA** | `/` · `/site` | Optional embedded front-end (if present in build) |

---

## Features (how they work)

### 1. MovieBox (`/mb/*`)

Uses the **official mobile BFF** (`api*.aoneroom.com`) with **HMAC-signed** requests (same pattern as [MovieBox-TUI](https://github.com/mesamirh/MovieBox-TUI)).

| Endpoint | Role |
|----------|------|
| `GET /mb/search?q=` | Keyword search |
| `GET /mb/detail/{subject_id}` | Full metadata + seasons |
| `GET /mb/stream/{subject_id}?se=&ep=` | Play-info → DASH/MP4 + Cookie headers |
| `GET /mb/movies` · `/mb/series` | Movie / series shelves |
| `GET /mb/home` · `/mb/trending` | Operating tabs / trending |
| `GET /mb/captions/{subject_id}` | Subtitles |

**How streaming works**

1. Call `/mb/stream/{id}?se=1&ep=1` (movie: `se=0&ep=0`).
2. Response includes `streams[]` with `url` (often `.mpd` DASH) and `headers.Cookie`.
3. Player must send that Cookie (or use built-in `/proxy/file/{token}` if enabled).
4. Quality from MovieBox alone is often limited (~480p on free tier).

### 2. MovieBox New (`/mbn/*`)

Separate section for **movieboxhd.net**-style H5 + mobile `play-info/v2`.

| Endpoint | Role |
|----------|------|
| `GET /mbn/home` | Sections |
| `GET /mbn/search?q=` | Search |
| `GET /mbn/trending` | Trending |
| `GET /mbn/stream?subject_id=&season=&episode=` | **Real sbcdn / hakunaymatata DASH** via `signCookie` decode |

`signCookie` is base64-decoded to a `urlprefix=…` CDN base; MPD URLs are built from that — not empty free-tier web play.

### 3. PirateBot (`/pb/*`) — [theogpiratebot.online](https://theogpiratebot.online)

Multi-tier **Cloudflare edge** TMDB proxy (no personal TMDB key required) + stream server registry.

| Endpoint | Role |
|----------|------|
| `GET /pb/trending` · `/pb/popular` · `/pb/search` | Catalog |
| `GET /pb/movie/{tmdb_id}` · `/pb/tv/{tmdb_id}` | Detail |
| `GET /pb/tv/{id}/season/{n}` | Season episodes |
| `GET /pb/stream?tmdb_id=&type=movie\|tv&season=&episode=` | **Embed servers**: vidstuck, vidfast, roxy, bingr, nxsha + toon-stream when resolved |
| `GET /pb/anime/stream?anilist_id=&episode=` | MegaPlay / Zoko / 4animo |
| `GET /pb/tmdb/{path}` | Raw TMDB path proxy |
| `GET /pb/toon-stream` · `/pb/downloads` · `/pb/stream-check` | Edge resolvers |

**Example:** `/pb/stream?tmdb_id=276161&type=tv&season=1&episode=1`

### 4. CineStream (`/cs/*`) — [cinestream.watch](https://cinestream.watch)

MongoDB-backed **ToonStream** catalog exposed by their `/api/v1/*`.

| Endpoint | Role |
|----------|------|
| `GET /cs/home` | Trending / popular / top_rated / fresh-drop shelves |
| `GET /cs/browse?filter=&page=&type=` | Paginated browse |
| `GET /cs/search?q=` | Search |
| `GET /cs/detail?id=toon_…` | Metadata |
| `GET /cs/episodes?animeId=` | Episode list |
| `GET /cs/stream?animeId=&season=&episode=` | **Multi-source** list (Ruby, filesforever, cloudy, as-cdn, Vidstream, Gemma player…) |

Sources are mostly embeds; `cdn_sources` highlights as-cdn / m3u8 / mp4 when detected.

### 5. Anime (`/anime/*`)

Hindi-focused anime catalog (HindiAnime-style) with **server-side HLS proxy** so browsers can play without CORS / 410 errors.

| Endpoint | Role |
|----------|------|
| `GET /anime/home` · `/anime/search` | Browse |
| `GET /anime/detail/{slug}` · `/anime/episodes/{slug}` | Detail + EP list |
| `GET /anime/stream` · `/anime/servers` | Server list + stream URLs |
| `GET /anime/hls?u=` | **Proxy** master/sub playlists (rewrites segments) |

### 6. Music

#### YouTube Music (`/yt-music/*`) — SimpMusic-style InnerTube

| Endpoint | Role |
|----------|------|
| `GET /yt-music/search?q=` | WEB_REMIX search |
| `GET /yt-music/home` | Browse + chart seeds |
| `GET /yt-music/trending` | Top songs |
| `GET /yt-music/next?video_id=` | **Radio queue** (`RDAMVM{id}`) |
| `GET /yt-music/play/{video_id}` | **Direct googlevideo CDN** (ANDROID InnerTube) |
| `GET /yt-music/info` · `/playlist` · `/album` | Meta |

**CDN note:** `audio_url` / `directUrl` are real `*.googlevideo.com` links. They **expire ~6 hours** — call play again when needed.

**Optional cookie** (better success / Premium formats):

```bash
export YT_MUSIC_COOKIE="SID=...; HSID=...; SAPISID=..."
# or Netscape cookies.txt in project root / YTDLP_COOKIES path
```

#### JioSaavn & unified music (`/music/*`)

| Endpoint | Role |
|----------|------|
| `GET /music/home` · `/music/search?q=` | Charts + search |
| `GET /music/play/{id}` | `saavn:…` or YouTube id |
| `GET /music/stream/{token}` | **Proxied audio** (Range + CORS) for `<audio>` |
| `GET /music/lyrics?title=&artist=` | LRCLIB plain + **synced** `lines[]` |
| `GET /music/charts` · `/music/unified` | Aggregated |

#### Spotify / Deezer / Billboard

- `/spotify/search` · `/track` · `/album` · `/playlist` · `/charts`
- `/deezer/search` · `/track` · `/album` · `/artist` · `/home`
- `/billboard/hot-100` · `/global-200` · `/artists-100` · …

### 7. YouTube (`/yt/*`)

| Endpoint | Role |
|----------|------|
| `GET /yt/player/{video_id}` · `/yt/stream/{video_id}` | InnerTube CDN (same engine as YT Music play) |
| `GET /yt/search` · `/yt/transcript` · `/yt/channel` | Meta |
| `GET /yt/download` · `/yt/ytaudio` | Download helpers |

Primary path is **native InnerTube** (ANDROID / IOS / ANDROID_MUSIC). yt-dlp is optional fallback only.

### 8. Downloader (`/dl/*`)

Unified extractors for many sites (YouTube, TikTok, Instagram, Facebook, Spotify, Deezer, Apple Music, Amazon Music, Tidal, Qobuz, …).

| Endpoint | Role |
|----------|------|
| `GET /dl/smart?url=` · `/dl/any?url=` · `/dl/extract?url=` | Auto provider |
| `GET /dl/ytmp4?url=&quality=` | YouTube video |
| `GET /dl/audio?url=` | Best audio |
| `GET /dl/info?url=` | Formats list without downloading |

On datacenter IPs, YouTube may return **embed fallback** instead of CDN (bot-check). Use cookies or a residential IP when possible.

### 9. Tools (`/tools/*`)

| Endpoint | Role |
|----------|------|
| `GET /tools/resolve?url=` | HubCloud / HubDrive → direct links |
| `GET /tools/translate` · `/tools/gtranslate` | Translation |
| `GET /tools/tts` · `/tools/tts/v2` | Text-to-speech |
| `GET /tools/url-shorten` · `/tools/ssweb` | Short URL / screenshot |
| `GET /tools/web-search` · `/tools/google-search` | Search helpers |
| `GET /tools/songlink` | Song.link cross-platform |

### 10. AI helpers

| Endpoint | Role |
|----------|------|
| `GET/POST /ai/chat` · `/ai/deepseek` · `/ai/metaai` | Chat-style |
| `GET /ai-image/generate` · `/flux` · `/pollinations` · `/sdxl` | Image gen |
| `GET /v1/models` · `POST /v1/chat/completions` | OpenAI-compatible shape |

Availability depends on upstream; treat as best-effort.

---

## Quick start

### Requirements

- Python **3.9+**
- `pip`

### Install

```bash
git clone https://github.com/Shawon727/StreamHub-Api.git
cd Moviebox-API
pip install -r requirements.txt
```

**requirements.txt** (typical)

```text
fastapi[standard]==0.115.0
uvicorn[standard]==0.30.6
httpx==0.27.2
beautifulsoup4==4.12.3
yt-dlp>=2024.8.0
```

(`yt-dlp` is optional; InnerTube / JioSaavn work without it.)

### Run

```bash
uvicorn api:app --host 0.0.0.0 --port 8000
# or
python main.py
```

| URL | Purpose |
|-----|---------|
| http://127.0.0.1:8000/docs | **Swagger** — try every endpoint |
| http://127.0.0.1:8000/redoc | ReDoc |
| http://127.0.0.1:8000/health | Health / version |
| http://127.0.0.1:8000/ | SPA (if embedded in your build) |

### Termux

```bash
pkg update -y && pkg install -y python git
cd $HOME
git clone https://github.com/Shawon727/Moviebox-API.git
cd Moviebox-API
pip install -r requirements.txt
python main.py
```

### Deploy (Railway / Vercel / FastAPI Cloud)

```bash
uvicorn api:app --host 0.0.0.0 --port $PORT
```

Optional env:

```bash
PORT=8000
TMDB_API_KEY=...          # optional if using /pb TMDB proxy
YT_MUSIC_COOKIE=...       # optional YouTube Music cookie string
YTDLP_COOKIES=/path/to/cookies.txt
```

---

## Architecture (short)

```
Clients (app / web / curl)
    │
    ▼
FastAPI (api.py)  ── creator: shawon on every JSON response
    │
    ├── /mb/*     → MovieBox mobile BFF (HMAC)
    ├── /mbn/*    → MovieBox New (play-info + signCookie → sbcdn DASH)
    ├── /pb/*     → PirateBot CF workers (TMDB + embeds)
    ├── /cs/*     → CineStream / ToonStream catalog
    ├── /anime/*  → HindiAnime + HLS rewrite proxy
    ├── /fk/*     → 4KHDHub + HubCloud resolve
    ├── /yt-music/* / /yt/*  → InnerTube (googlevideo CDN)
    ├── /music/*  → JioSaavn + lyrics + stream proxy
    ├── /dl/*     → multi-site downloaders
    └── /tools/*  → resolve, translate, TTS, …
```

**Streaming strategy**

1. Prefer **native CDN** (InnerTube googlevideo, Saavn CDN, MovieBox DASH).
2. Else **edge resolvers** (toon-stream, HubCloud, loader.to).
3. Else **embed players** (vidstuck, vidfast, VidSrc-class, Gemma).
4. Browser-hostile links go through **proxy** routes when available.

---

## Project layout

```text
Moviebox-API/
├── api.py              # Full API (all providers)
├── main.py             # from api import app
├── requirements.txt
├── cookies.txt         # optional (YouTube / YT Music)
├── Procfile            # web: uvicorn api:app …
├── railway.json / vercel.json
└── readme.md
```

`main.py` example:

```python
from api import app

if __name__ == "__main__":
    import os, uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("api:app", host="0.0.0.0", port=port, reload=False)
```

---

## Example calls

```bash
# MovieBox search + stream
curl "http://127.0.0.1:8000/mb/search?q=avatar"
curl "http://127.0.0.1:8000/mb/stream/SUBJECT_ID?se=0&ep=0"

# PirateBot TV stream servers
curl "http://127.0.0.1:8000/pb/stream?tmdb_id=276161&type=tv&season=1&episode=1"

# CineStream episode sources
curl "http://127.0.0.1:8000/cs/stream?animeId=toon_jojos-bizarre-adventure&season=1&episode=1"

# YouTube Music — direct CDN
curl "http://127.0.0.1:8000/yt-music/play/dQw4w9WgXcQ"
curl "http://127.0.0.1:8000/yt-music/search?q=saiyaara"
curl "http://127.0.0.1:8000/yt-music/next?video_id=dQw4w9WgXcQ"

# Music lyrics
curl "http://127.0.0.1:8000/music/lyrics?title=One%20Love&artist=Shubh"

# Smart downloader
curl "http://127.0.0.1:8000/dl/smart?url=https://youtu.be/dQw4w9WgXcQ"
```

---

## Troubleshooting

| Symptom | What to do |
|---------|------------|
| YT Music / YouTube **no CDN** | Server IP bot-checked → set `YT_MUSIC_COOKIE` / `cookies.txt`, or retry later |
| MovieBox **empty streams** | Use `/mbn/stream` for series; ensure `season` & `episode` set; Cookie required for DASH |
| Anime **410 / Cloudflare** | Use `/anime/hls?u=` proxy, not raw upstream m3u8 |
| CineStream only iframes | Expected — pick `cdn_sources` when present |
| `/dl/ytmp4` embed only | Same as YT bot-check; use `/yt/stream/{id}` InnerTube first |
| Deploy **Import error api** | Ensure `api.py` is in app root; start `uvicorn api:app` |
| Music no sound in browser | Prefer `/music/stream/{token}` proxy over raw CDN |

---

## Disclaimer

Educational / personal use only. Respect terms of MovieBox, TMDB, YouTube, JioSaavn, CineStream, PirateBot edge hosts, and any CDN you hit. Unauthorized redistribution of copyrighted media may be illegal where you live. Authors are not responsible for misuse.

---

## License

MIT — see `LICENSE`.

---

<p align="center">
  <strong>Made by Shawon</strong><br/>
  <a href="https://github.com/Shawon727/Moviebox-API">github.com/Shawon727/StreamHub-Api</a>
</p>
