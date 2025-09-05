# main.py
from __future__ import annotations
from fastapi import FastAPI, HTTPException, Depends
from fastapi.responses import JSONResponse
import hashlib
import time

from auth import load_api_keys, verify_api_key
from db_loader import init_db, load_hash_file, lookup

app = FastAPI(title="Pwned Check (Bank API)")

@app.on_event("startup")
async def on_startup():
    t0 = time.time()
    load_api_keys()
    init_db()            # ensure table exists (no in-memory load)
    load_hash_file(None) # kept for compatibility; returns 0
    print(f"🔐 Startup complete (took {time.time()-t0:.2f}s)")

@app.get("/healthz")
async def healthz():
    return {"ok": True, "message": "alive"}

@app.get("/check/{sha1}")
async def check_sha1(sha1: str, _auth: bool = Depends(verify_api_key)):
    h = sha1.strip().upper()
    if len(h) != 40:
        raise HTTPException(status_code=400, detail="sha1 must be 40 hex chars")
    count = lookup(h)
    return JSONResponse({"found": count > 0, "count": count})

@app.get("/check_password/{password}")
async def check_password(password: str, _auth: bool = Depends(verify_api_key)):
    sha1 = hashlib.sha1(password.encode("utf-8")).hexdigest().upper()
    count = lookup(sha1)
    return JSONResponse({"password": password, "sha1": sha1, "found": count > 0, "count": count})
