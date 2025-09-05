# main.py
from __future__ import annotations
from fastapi import FastAPI, HTTPException, Header
from fastapi.responses import JSONResponse
import time

from auth import load_api_keys, api_key_ok, check_rate_limit
from db_loader import load_hash_file, lookup

app = FastAPI(title="Pwned Check (Bank Edition)")

@app.on_event("startup")
async def startup() -> None:
    t0 = time.time()
    load_api_keys()
    loaded = load_hash_file(None)  # now no-op
    print(f"🔐 API keys loaded (startup {time.time()-t0:.2f}s)")

@app.get("/healthz")
async def healthz():
    return {"ok": True, "message": "alive"}

@app.get("/check/{sha1}")
async def check_password(sha1: str, x_api_key: str = Header(None)):
    if not x_api_key or not api_key_ok(x_api_key):
        raise HTTPException(status_code=401, detail="Invalid or missing API key")

    if not check_rate_limit(x_api_key):
        raise HTTPException(status_code=429, detail="Rate limit exceeded")

    h = sha1.strip().upper()
    if len(h) != 40:
        raise HTTPException(status_code=400, detail="sha1 must be 40 hex chars")

    count = lookup(h)
    return JSONResponse({"found": count > 0, "count": count})
