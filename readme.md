<p align="center">
  <img src="https://h5-static.aoneroom.com/ssrStatic/mbOfficial/public/_nuxt/web-logo.apJjVir2.svg" alt="StreamHub / MovieBox" width="180"/>
</p>

<p align="center">
  <strong>StreamHub API</strong> — multi-provider streaming backend + modern web UI
</p>

<p align="center">
  <a href="https://fastapi.tiangolo.com/"><img src="https://img.shields.io/badge/FastAPI-005571?style=for-the-badge&logo=fastapi" alt="FastAPI"/></a>
  <a href="https://www.python.org/"><img src="https://img.shields.io/badge/Python-3.9+-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python"/></a>
  <img src="https://img.shields.io/badge/Status-Active-4c1?style=for-the-badge" alt="Status"/>
  <img src="https://img.shields.io/badge/License-MIT-blue.svg?style=for-the-badge" alt="License"/>
</p>

> **Note:** MovieBox has moved toward a paid model; 1080p is often unavailable and ~480p may be the highest quality from MovieBox alone. This project keeps **MovieBox under `/mb/*` for API users**, and powers the **web UI** with **TMDB catalog + embed players + 4KHDHub mirrors + JioSaavn music**.

---

## What this project is

A single-file **FastAPI** app (`api.py`) that includes:

| Layer | What it does |
|--------|----------------|
| **Web UI** (`/` or `/site`) | Jellyfin-inspired SPA — Home, Movies, Series, Music, API Docs |
| **Catalog** (`/api/*`) | TMDB search, detail, seasons, play (embeds + optional 4K) |
| **MovieBox** (`/mb/*`) | HMAC-signed mobile API — search, detail, DASH/MP4 streams |
| **4KHDHub** (`/fk/*`) | Scrape releases + HubCloud / Pixeldrain resolve |
| **Music** (`/music/*`) | JioSaavn streams (vivi-music style) + YT Music search + synced lyrics |
| **Tools** | HubCloud resolver, DASH/audio proxies |

**Made by Shawon**

Repo: [github.com/Shawon727/Moviebox-API](https://github.com/Shawon727/Moviebox-API)

---

## Features

### Web (SPA)
- Home rows: trending / popular / now playing / on the air / top rated / upcoming
- Movies & Series grids with **pagination**
- Search (TMDB multi-search)
- Title detail + season / episode picker
- **Player**: Videasy · VidSrc · Vidking · VidLink embeds first (`fast=1`)
- Optional **4K / Hub / Pixeldrain** mirrors on demand
- **Music**: search, home charts, HTML5 player, **synced LRCLIB lyrics**, download link
- Responsive **phone + PC** layout, dark theme

### MovieBox API (`/mb/*`)
- Visitor login + host rotation (`api*.aoneroom.com`)
- Search, detail, season info
- Play-info streams (DASH / MP4) with **signCookie** headers
- Resource links + captions
- Stateless DASH proxy (`/proxy/file/{token}`) with MPD rewrite

### 4KHDHub + HubCloud
- Search & release lists
- HubCloud / HubDrive / greenmotors resolve
- Pixeldrain via CDN (`cdn.pixeldrain.eu.cc`) + preflight filter
- Only playable direct links kept when resolving

### Music (SimpMusic / vivi-music style)
- **JioSaavn** primary (stable `generateAuthToken` CDN)
- YouTube Music Innertube search / meta
- **`/music/stream/{token}`** proxy so browsers can play (Range + CORS)
- LRCLIB plain + **synced LRC** lyrics
- yt-dlp multi-client fallback when needed

---

## Tech stack

- **FastAPI** + **Uvicorn**
- **httpx** (async HTTP)
- **BeautifulSoup4** (4KHDHub / HubCloud HTML)
- **yt-dlp** (optional YouTube audio fallback)
- Front-end: single-page app embedded in `api.py` (Plyr / dash.js for video when needed)

---

## Quick start

### Requirements
- Python **3.9+**
- `pip`

### Install

```bash
git clone https://github.com/Shawon727/Moviebox-API.git
cd Moviebox-API
pip install -r requirements.txt
```

**requirements.txt**

```text
fastapi[standard]==0.115.0
uvicorn[standard]==0.30.6
httpx==0.27.2
beautifulsoup4==4.12.3
yt-dlp>=2024.8.0
```

### Run

```bash
# recommended
uvicorn api:app --host 0.0.0.0 --port 8000

# or
python main.py
```

Open:

| URL | Purpose |
|-----|---------|
| http://127.0.0.1:8000/ | Web UI |
| http://127.0.0.1:8000/docs | Swagger |
| http://127.0.0.1:8000/health | Health check |

### Termux

```bash
pkg update -y && pkg install -y python git
cd $HOME
git clone https://github.com/Shawon727/Moviebox-API.git
cd Moviebox-API
pip install -r requirements.txt
python main.py
```

### Deploy (Railway / FastAPI Cloud)

**Start command:**

```bash
uvicorn api:app --host 0.0.0.0 --port $PORT
```

Optional env:

```bash
TMDB_API_KEY=your_tmdb_key   # optional; a default key is embedded for demo
PORT=8000
```

---

## API reference

Interactive docs: **`/docs`** (Swagger) and **`/redoc`**.

### Meta

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/` | SPA web UI |
| GET | `/site` | Same SPA |
| GET | `/health` | `{ ok, version, providers }` |
| GET | `/docs` | OpenAPI UI |

### Catalog (web)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/home` | TMDB home rows |
| GET | `/api/movies?page=` | Popular movies + pagination |
| GET | `/api/series?page=` | Popular series + pagination |
| GET | `/api/search?q=` | TMDB multi-search (+ optional 4KHDHub hits) |
| GET | `/api/detail/{movie\|tv}/{id}` | Detail + seasons |
| GET | `/api/tv/{id}/season/{n}` | Episodes (ascending) |
| GET | `/api/play?tmdb_id=&media=&se=&ep=&fast=1` | **Play sources** |

**`/api/play` notes**

- `fast=1` (default in UI): **embeds only** — fast (~0.1s)
- `fast=0`: also resolve 4KHDHub / Hub / Pixeldrain (slower)
- Returns `sources[]` (`type`: `embed` \| `direct`), `downloads[]`, `errors`

### MovieBox

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/mb/search?q=&page=` | Search catalog |
| GET | `/mb/detail/{subject_id}` | Metadata (+ seasons for series) |
| GET | `/mb/stream/{subject_id}?se=&ep=` | Stream list (DASH/MP4 + headers/cookies) |
| GET | `/mb/home?page=` | Operating tabs |
| GET | `/mb/captions/{subject_id}?resource_id=` | Subtitles |

Movie: `se=0&ep=0` · Series: `se=1&ep=1` …

Each stream may include `headers` (`Cookie`, `User-Agent`) required by the CDN. Use **`/proxy/file/{token}`** for browser DASH.

### 4KHDHub

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/fk/search?q=` | Search |
| GET | `/fk/detail?id=` | Page meta (`id` = path from search) |
| GET | `/fk/stream?id=&se=&ep=&resolve=` | Releases + mirrors; `resolve=true` expands HubCloud |

### Tools & proxy

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/tools/resolve?url=` | HubCloud / HubDrive → direct CDN list |
| GET | `/proxy/file/{token}` | Authenticated file / MPD proxy |
| GET | `/proxy/cdn/{token}/{path}` | Segment proxy under rewritten BaseURL |
| GET | `/search?q=` | Aggregate MovieBox + 4KHDHub search |

### Music

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/music/home` | Curated sections (JioSaavn charts) |
| GET | `/music/search?q=` | JioSaavn + YouTube Music |
| GET | `/music/play/{id}` | Stream meta — `id` = `saavn:XXXX` or YouTube `videoId` |
| GET | `/music/stream/{token}` | **Proxied audio** (Range, CORS) for `<audio>` |
| GET | `/music/lyrics?title=&artist=` | LRCLIB plain + synced `lines[]` |

**Play response (example fields)**

```json
{
  "title": "One Love",
  "artist": "Shubh",
  "audio_url": "https://web.saavncdn.com/...",
  "play_url": "/music/stream/eyJ...",
  "sources": [{ "type": "audio", "play_url": "/music/stream/..." }],
  "provider": "jiosaavn"
}
```

Use **`play_url`** in the browser (proxy). Raw `audio_url` expires; do not hardcode CDN links.

### Legacy

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/stream/{subject_id}` | Alias → `/mb/stream` |
| GET | `/detail/{subject_id}` | Alias → `/mb/detail` |

---

## Architecture (short)

```
Browser SPA (/)
  ├─ Catalog  → TMDB (/api/home, /api/search, /api/detail, …)
  ├─ Watch    → /api/play?fast=1 → embeds; optional fast=0 → 4K list
  └─ Music    → /music/search → /music/play → /music/stream (proxy)
                /music/lyrics (LRCLIB synced)

API clients
  ├─ /mb/*    → MovieBox mobile BFF (HMAC signature)
  ├─ /fk/*    → 4KHDHub scrape + HubCloud resolve
  └─ /tools/* → single-link resolvers
```

---

## Project layout

```text
Moviebox-API/
├── api.py              # Full API + embedded SPA
├── main.py             # Entry: from api import app
├── requirements.txt
├── Procfile            # web: uvicorn …
├── railway.json        # optional Railway config
├── vercel.json         # optional
├── verify.py           # optional checks
└── readme.md
```

---

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| Web stuck on **Loading…** | Hard refresh; ensure only one `let MSTATE` in SPA (fixed in current tree) |
| Movie **Loading stream…** | Use current build (`fast=1`); play should return embeds in &lt;1s |
| Music no sound | Use `play_url` (`/music/stream/...`), not expired Saavn CDN links |
| yt-dlp bot errors | Expected on many VPS IPs — JioSaavn path is primary |
| MovieBox low quality | Provider limit (~480p); use embeds / 4K mirrors on web |
| Deploy timeout | Start: `uvicorn api:app --host 0.0.0.0 --port $PORT` (no `reload`) |

---

## Disclaimer

This project is for **educational and personal** use. Respect the terms of service of MovieBox, TMDB, JioSaavn, YouTube, and any host you resolve. Streaming copyrighted content without permission may be illegal in your country. The authors are not responsible for misuse.

---

## License

MIT — see `LICENSE`.

---

<p align="center">
  <strong>Made by Shawon</strong><br/>
  <a href="https://github.com/Shawon727/Moviebox-API">github.com/Shawon727/Moviebox-API</a>
</p>
