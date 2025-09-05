# auth.py
from __future__ import annotations
import json
import time
import threading
from pathlib import Path
from typing import Dict
from fastapi import Header, HTTPException

KEYS_PATH = Path("keys.json")

_lock = threading.Lock()
_api_keys: Dict[str, Dict] = {}
_rate_buckets: Dict[str, Dict[int, int]] = {}  # key -> {window:int -> count:int}
RATE_LIMIT_WINDOW = 60  # seconds

def load_api_keys() -> Dict[str, Dict]:
    """
    Loads keys.json into _api_keys. Keys file format:
    { "bankdev123": { "limit": 1000 }, "anotherkey": { "limit": 100 } }
    """
    global _api_keys
    if KEYS_PATH.exists():
        try:
            _api_keys = json.loads(KEYS_PATH.read_text() or "{}")
        except Exception:
            _api_keys = {}
    else:
        # create sample file
        KEYS_PATH.write_text(json.dumps({"bankdev123": {"limit": 1000}}, indent=2))
        _api_keys = json.loads(KEYS_PATH.read_text())
    return _api_keys

def api_key_ok(key: str) -> bool:
    return key in _api_keys

def get_limit_for(key: str) -> int:
    try:
        return int(_api_keys.get(key, {}).get("limit", 60))
    except Exception:
        return 60

def check_rate_limit(key: str) -> bool:
    """
    Fixed-window per-minute rate limiting.
    Returns True if allowed, False if limit exceeded.
    """
    limit = get_limit_for(key)
    if limit <= 0:
        return False  # disabled key

    now = int(time.time())
    window = now - (now % RATE_LIMIT_WINDOW)  # window start epoch

    with _lock:
        buckets = _rate_buckets.setdefault(key, {})
        count = buckets.get(window, 0)
        if count + 1 > limit:
            # keep state (do not advance window)
            buckets[window] = count
            return False
        buckets[window] = count + 1

        # prune old windows to keep memory small
        if len(buckets) > 5:
            keys_to_remove = [w for w in list(buckets.keys()) if w < window]
            for w in keys_to_remove:
                buckets.pop(w, None)

    return True

def verify_api_key(x_api_key: str = Header(...)):
    """
    FastAPI dependency. Raises HTTPException on invalid key / rate limit.
    """
    if not _api_keys:
        load_api_keys()

    if not x_api_key or not api_key_ok(x_api_key):
        raise HTTPException(status_code=401, detail="Invalid or missing API key")

    if not check_rate_limit(x_api_key):
        raise HTTPException(status_code=429, detail="Rate limit exceeded")

    return True
