from fastapi import FastAPI, HTTPException, Security, Request
from fastapi.security import APIKeyHeader
from fastapi.middleware.cors import CORSMiddleware
import hashlib

# --------------------------
# CONFIG
# --------------------------
API_KEY_NAME = "x-api-key"
VALID_KEYS = {"mysecretapikey"}  # <-- change to your real API key
PWNED_FILE = "data/rockyou_pwned.txt"

# FastAPI app
app = FastAPI(
    title="Pwned Passwords API",
    description="Check if a password has been exposed in known breaches.",
    version="1.0.0"
)

# Enable CORS if frontend will call this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # change to your domain in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# API Key header
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)


# --------------------------
# Middleware (skip docs)
# --------------------------
@app.middleware("http")
async def verify_api_key(request: Request, call_next):
    if request.url.path in ["/docs", "/openapi.json", "/redoc"]:
        return await call_next(request)

    api_key = request.headers.get(API_KEY_NAME)
    if not api_key or api_key not in VALID_KEYS:
        return HTTPException(status_code=401, detail="Invalid or missing API key")

    return await call_next(request)


# --------------------------
# Helper function
# --------------------------
def is_password_pwned(password: str) -> int:
    """Check if password hash exists in pwnedpasswords file. Returns count if found."""
    sha1 = hashlib.sha1(password.encode("utf-8")).hexdigest().upper()
    with open(PWNED_FILE, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            hash_prefix, count = line.strip().split(":")
            if sha1 == hash_prefix:
                return int(count)
    return 0


# --------------------------
# Routes
# --------------------------
@app.get("/check_password/{password}")
async def check_password(password: str, api_key: str = Security(api_key_header)):
    if api_key not in VALID_KEYS:
        raise HTTPException(status_code=401, detail="Invalid API Key")

    count = is_password_pwned(password)
    return {
        "password": password,
        "pwned": count > 0,
        "times_seen": count
    }


@app.get("/")
async def root():
    return {"message": "Pwned Passwords API is running. Go to /docs for Swagger UI."}
