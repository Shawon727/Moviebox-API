# Contributing to StreamHub / MovieBox API

Thanks for taking the time to contribute.

This project is maintained at **[Shawon727/Moviebox-API](https://github.com/Shawon727/Moviebox-API)**.  
Guidelines below are recommendations, not hard rules — use good judgment, and feel free to improve this document in a PR.

---

## How can I contribute?

### Reporting bugs

- Open an issue: [github.com/Shawon727/Moviebox-API/issues](https://github.com/Shawon727/Moviebox-API/issues)
- Include:
  - Expected vs actual behavior
  - Steps to reproduce
  - Endpoint or UI path (e.g. `/api/play`, `/music/play/...`, `#/watch/...`)
  - Environment: Python version, OS, deploy target (local / Termux / Railway / FastAPI Cloud)
  - Relevant logs (no secrets or full cookies)

### Suggesting enhancements

- Search existing issues first
- Open an issue describing the feature, why it helps, and any API/UI surface you have in mind

### Pull requests

1. Fork the repo and create a branch from `main`
2. Keep changes focused (one concern per PR when possible)
3. If you change behavior, update `readme.md` / docs notes when relevant
4. Run a quick local check (see below)
5. Prefer **PEP 8**-style Python; keep the embedded SPA readable
6. Open a PR with a clear summary of *what* and *why*

---

## Development workflow

### 1. Clone & install

```bash
git clone https://github.com/Shawon727/Moviebox-API.git
cd Moviebox-API
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
| http://127.0.0.1:8000/ | Web UI |
| http://127.0.0.1:8000/docs | Swagger |
| http://127.0.0.1:8000/health | Health |

### 3. Smoke-test before PR

```bash
# health
curl -s http://127.0.0.1:8000/health

# catalog
curl -s "http://127.0.0.1:8000/api/home" | head -c 200

# fast play (embeds)
curl -s "http://127.0.0.1:8000/api/play?tmdb_id=19995&media=movie&fast=1" | head -c 300

# music
curl -s "http://127.0.0.1:8000/music/search?q=One%20Love" | head -c 200

# optional full verify script (if present)
python verify.py
```

### 4. Project map

```text
api.py          # API routes + embedded SPA (main surface)
main.py         # entry: from api import app
requirements.txt
readme.md
CONTRIBUTING.md
```

**Areas of the codebase**

| Area | Prefix / location | Notes |
|------|-------------------|--------|
| Web SPA | `SPA_HTML` in `api.py`, `/` | Avoid duplicate `let` bindings in JS |
| Catalog / play | `/api/*` | Prefer `fast=1` for embeds; 4K is optional |
| MovieBox | `/mb/*` | HMAC + host rotation; proxy for DASH |
| 4KHDHub | `/fk/*`, `/tools/resolve` | Scraping can break when sites change |
| Music | `/music/*` | JioSaavn + stream proxy; don’t hardcode CDN URLs |

---

## Coding notes

- **Single-file design**: most logic lives in `api.py` for easy deploy — keep helpers near their routes
- **Async I/O**: use `httpx.AsyncClient`; don’t block the event loop (heavy work → `asyncio.to_thread`)
- **Secrets**: no committed cookies, tokens, or private API keys; use env vars when adding new keys
- **Providers change**: scrapers (4KHDHub, HubCloud) and YouTube extraction fail often — handle errors in responses (`errors` field) instead of crashing
- **SPA**: after JS edits, ensure the script parses (no duplicate `let MSTATE`, etc.)
- **Music**: browser playback should use `/music/stream/{token}` (`play_url`), not expired Saavn CDN links

---

## Code of conduct

Be respectful and professional. No harassment, spam, or malicious PRs.  
This project is for learning and personal use — do not use contributions to promote abuse of third-party services.

---

## License

By contributing, you agree that your contributions are licensed under the same **MIT** license as the project.

---

Thank you for helping improve **StreamHub / MovieBox API**.

**Maintainer:** Shawon · [github.com/Shawon727/Moviebox-API](https://github.com/Shawon727/Moviebox-API)
