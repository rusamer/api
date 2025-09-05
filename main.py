# main.py
from __future__ import annotations
from fastapi import FastAPI, HTTPException, Header
from fastapi.responses import JSONResponse
from pathlib import Path
import time
import hashlib

from auth import load_api_keys, api_key_ok, check_rate_limit
from db_loader import load_hash_file, lookup

DATA_FILE = Path("data/rockyou_pwned.txt")  # small local sample for dev
app = FastAPI(title="Pwned Check (Dev)")

@app.on_event("startup")
async def startup() -> None:
    t0 = time.time()
    load_api_keys()
    loaded = load_hash_file(DATA_FILE)
    print(f"🔐 API keys loaded, 🔎 {loaded:,} hashes in memory (startup {time.time()-t0:.2f}s)")

@app.get("/healthz")
async def healthz():
    return {"ok": True, "message": "alive"}

# 🔹 Old endpoint (hash-based)
@app.get("/check/{sha1}")
async def check_sha1(sha1: str, x_api_key: str = Header(None)):
    if not x_api_key or not api_key_ok(x_api_key):
        raise HTTPException(status_code=401, detail="Invalid or missing API key")

    if not check_rate_limit(x_api_key):
        raise HTTPException(status_code=429, detail="Rate limit exceeded")

    h = sha1.strip().upper()
    if len(h) != 40:
        raise HTTPException(status_code=400, detail="sha1 must be 40 hex chars")

    count = lookup(h)
    return JSONResponse({"found": count > 0, "count": count})

# 🔹 New endpoint (password-based)
@app.get("/check_password/{password}")
async def check_password(password: str, x_api_key: str = Header(None)):
    if not x_api_key or not api_key_ok(x_api_key):
        raise HTTPException(status_code=401, detail="Invalid or missing API key")

    if not check_rate_limit(x_api_key):
        raise HTTPException(status_code=429, detail="Rate limit exceeded")

    # hash password -> sha1
    h = hashlib.sha1(password.encode("utf-8")).hexdigest().upper()

    count = lookup(h)
    return JSONResponse({"found": count > 0, "count": count, "sha1": h})
