# Entry point — loads api.py from the same directory (FastAPI Cloud safe)
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import importlib.util

_api_path = _ROOT / "api.py"
if not _api_path.is_file():
    raise SystemExit(
        f"FATAL: api.py not found at {_api_path}. "
        "Upload api.py next to main.py, then redeploy."
    )

_spec = importlib.util.spec_from_file_location("api", _api_path)
if _spec is None or _spec.loader is None:
    raise SystemExit("FATAL: cannot load api.py")
_mod = importlib.util.module_from_spec(_spec)
sys.modules["api"] = _mod
_spec.loader.exec_module(_mod)

app = _mod.app

if __name__ == "__main__":
    import os
    import uvicorn

    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port, proxy_headers=True)
