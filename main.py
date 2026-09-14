from api import app
if __name__ == "__main__":
    import uvicorn, os
    uvicorn.run("api:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), reload=True)
