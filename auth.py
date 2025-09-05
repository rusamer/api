from __future__ import annotations
import json
import time
from pathlib import Path
from fastapi import Header, HTTPException

KEYS_PATH = Path("keys.json")

_api_keys = {}
_rate_limits = {}
RATE_LIMIT_WINDOW = 60  # 1 minute default

def load_api_keys():
    global _api_keys
    if KEYS_PATH.exists():
        _api_keys = json.loads(KEYS_PATH.read_text() or "{}")
    else:
        _api_keys = {}
    return _api_keys

def api_key_ok(key: str) -> bool:
    return key in _api_keys

def get_limit_for(key: str) -> int:
    return int(_api_keys.get(key, {}).get("limit", 60))

def check_rate_limit(key: str) -> bool:
    """Simple fixed-window limiter per API key."""
    now = int(time.time())
    window = now // RATE_LIMIT_WINDOW
    if key not in _rate_limits:
        _rate_limits[key] = {}
    if window not in _rate_limits[key]:
        _rate_limits[key][window] = 0
    if _rate_limits[key][window] >= get_limit_for(key):
        return False
    _rate_limits[key][window] += 1
    return True

def verify_api_key(x_api_key: str = Header(...)):
    if not api_key_ok(x_api_key):
        raise HTTPException(status_code=403, detail="Forbidden (Invalid API key)")
    if not check_rate_limit(x_api_key):
        raise HTTPException(status_code=429, detail="Rate limit exceeded")
    return True
