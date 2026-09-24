# Contributing to StreamHub / MovieBox API

Thanks for taking the time to contribute.

This project is maintained at **[Shawon727/StreamHub-Api](https://github.com/Shawon727/StreamHub-Api)**  

Guidelines below are recommendations, not hard rules — use good judgment, and feel free to improve this document in a PR.

---

## How can I contribute?

### Reporting bugs

- Open an issue on the repo’s **Issues** page
- Include:
  - Expected vs actual behavior
  - Steps to reproduce
  - Endpoint or UI path (e.g. `/mb/stream/...`, `/yt-music/play/...`, `/pb/stream?...`)
  - Environment: Python version, OS, deploy target (local / Termux / Railway / Vercel / FastAPI Cloud)
  - Relevant logs (no secrets or full cookies)

### Suggesting enhancements

- Search existing issues first
- Open an issue describing the feature, why it helps, and any API/UI surface you have in mind

### Pull requests

1. Fork the repo and create a branch from `main`
2. Keep changes focused (one concern per PR when possible)
3. If you change behavior, update `readme.md` / docs notes when relevant
4. Run a quick local check (see below)
5. Prefer **PEP 8**-style Python; keep helpers readable
6. Open a PR with a clear summary of *what* and *why*

---

## Development workflow

### 1. Clone & install

```bash
git clone https://github.com/Shawon727/StreamHub-Api.git
cd StreamHub-Api
pip install -r requirements.txt
```

Expected packages (see `requirements.txt`):

```text
fastapi[standard]==0.115.0
uvicorn[standard]==0.30.6
httpx==0.27.2
beautifulsoup4==4.12.3
yt-dlp>=2024.8.0
```

(`yt-dlp` is optional; InnerTube / JioSaavn work without it.)

### 2. Run the app

```bash
uvicorn api:app --host 0.0.0.0 --port 8000 --reload
```

Or:

```bash
python main.py
```

| URL | Purpose |
|-----|---------|
| http://127.0.0.1:8000/docs | Swagger |
| http://127.0.0.1:8000/health | Health |
| http://127.0.0.1:8000/ | Web UI (if embedded SPA is present) |

### 3. Smoke-test before PR

```bash
# health
curl -s http://127.0.0.1:8000/health

# MovieBox
curl -s "http://127.0.0.1:8000/mb/search?q=avatar" | head -c 200

# PirateBot
curl -s "http://127.0.0.1:8000/pb/stream?tmdb_id=550&type=movie" | head -c 300

# YouTube Music CDN
curl -s "http://127.0.0.1:8000/yt-music/play/dQw4w9WgXcQ" | head -c 300

# Music search
curl -s "http://127.0.0.1:8000/music/search?q=One%20Love" | head -c 200

# optional full verify script (if present)
python verify.py
```

### 4. Project map

```text
api.py              # Full API (all providers)
main.py             # entry: from api import app
requirements.txt
readme.md
CONTRIBUTING.md
LICENSE
cookies.txt         # optional (YouTube / YT Music)
```

**Areas of the codebase**

| Area | Prefix / location | Notes |
|------|-------------------|--------|
| MovieBox | `/mb/*` · `/moviebox/*` | HMAC + host rotation; Cookie for DASH |
| MovieBox New | `/mbn/*` | play-info + signCookie → sbcdn |
| PirateBot | `/pb/*` | TMDB edge proxy + embed servers |
| CineStream | `/cs/*` | ToonStream catalog + multi-source |
| Anime | `/anime/*` | HLS proxy for CORS / 410 issues |
| Music | `/music/*` · `/yt-music/*` | JioSaavn + InnerTube CDN + lyrics |
| YouTube | `/yt/*` | InnerTube googlevideo streams |
| Downloader | `/dl/*` | Multi-site extract |
| Tools | `/tools/*` | Hub resolve, translate, TTS, … |

---

## Coding notes

- **Single-file design**: most logic lives in `api.py` for easy deploy — keep helpers near their routes
- **Async I/O**: use `httpx.AsyncClient`; don’t block the event loop (heavy work → `asyncio.to_thread`)
- **Secrets**: no committed cookies, tokens, or private API keys; use env vars (`YT_MUSIC_COOKIE`, `YTDLP_COOKIES`, …)
- **Providers change**: scrapers and YouTube extraction fail often — return structured `errors` instead of crashing
- **Streaming**: prefer native CDN (InnerTube, Saavn, MovieBox DASH); then resolvers; then embeds
- **Music in browser**: use `/music/stream/{token}` (`play_url`), not expired raw CDN links
- **YT Music radio**: next queue uses `playlistId=RDAMVM{videoId}`

---

## Code of conduct

Be respectful and professional. No harassment, spam, or malicious PRs.  
This project is for learning and personal use — do not use contributions to promote abuse of third-party services.

---

## License

By contributing, you agree that your contributions are licensed under the same **MIT** license as the project.

---

Thank you for helping improve **StreamHub API**.

**Maintainer:** Shawon · [github.com/Shawon727/StreamHub-Api](https://github.com/Shawon727/StreamHub-Api)
