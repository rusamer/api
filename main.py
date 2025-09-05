from fastapi import FastAPI, Header, HTTPException
import hashlib
import db_loader
import auth

app = FastAPI(title="Pwned Check", version="0.1.0")


# 🔹 Middleware for API Key authentication
@app.middleware("http")
async def verify_api_key(request, call_next):
    api_key = request.headers.get("x-api-key")
    if not api_key or not auth.verify_api_key(api_key):
        raise HTTPException(status_code=401, detail="Invalid or missing API key")
    return await call_next(request)


# 🔹 Healthcheck
@app.get("/healthz")
def healthz():
    return {"status": "ok"}


# 🔹 Check by SHA1 hash
@app.get("/check/{sha1}")
def check_hash(sha1: str):
    sha1 = sha1.strip().upper()
    if len(sha1) != 40:
        raise HTTPException(status_code=400, detail="Invalid SHA1 hash format")

    found, count = db_loader.check_hash(sha1)
    return {"hash": sha1, "found": found, "count": count}


# 🔹 Check by plain password
@app.get("/check_password/{password}")
def check_password(password: str):
    # Convert password → SHA1 uppercase (HIBP format)
    sha1_hash = hashlib.sha1(password.encode("utf-8")).hexdigest().upper()

    found, count = db_loader.check_hash(sha1_hash)
    return {"password": password, "sha1": sha1_hash, "found": found, "count": count}
