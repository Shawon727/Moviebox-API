from fastapi import FastAPI
from datetime import datetime, timezone

app = FastAPI()

@app.get("/health")
async def health_check():
    return {
        "status": "ok",
        "message": "Healthy",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }
