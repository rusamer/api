from __future__ import annotations
from fastapi import FastAPI, Depends
from fastapi.responses import JSONResponse
from pathlib import Path
import time

from auth import verify_api_key, load_api_keys
from db_loader import load_hash_file, lookup

DATA_FILE = Path("data/rockyou_pwned.txt")  # local dev sample
app = FastAPI(title="Pwned Check API")

@app.on_event("startup")
async def startup() -> None:
    t0 = time.time()
    load_api_keys()
    loaded = load_hash_file(DATA_FILE)
    print(f"🔐 API keys loaded, 🔎 {loaded:,} hashes in memory (startup {time.time()-t0:.2f}s)")

@app.get("/healthz")
async def healthz():
    return {"ok": True, "message": "alive"}

@app.get("/check/{sha1}")
async def check_password(sha1: str, _: bool = Depends(verify_api_key)):
    h = sha1.strip().upper()
    if len(h) != 40:
        return JSONResponse(status_code=400, content={"error": "sha1 must be 40 hex chars"})

    count = lookup(h)
    return JSONResponse({"found": count > 0, "count": count})
