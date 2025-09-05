# auth.py
from __future__ import annotations
import json
import time
from pathlib import Path
from typing import Dict, Tuple

KEYS_PATH = Path("keys.json")

_api_keys: Dict[str, Dict] = {}
_buckets: Dict[str, Tuple[int,int]] = {}  # key -> (window_start_epoch, count_in_window)
DEFAULT_LIMIT = 60

def load_api_keys() -> Dict[str, Dict]:
    global _api_keys
    if not KEYS_PATH.exists():
        KEYS_PATH.write_text(json.dumps({"bankdev123": {"limit": 1000}}, indent=2))
    _api_keys = json.loads(KEYS_PATH.read_text() or "{}")
    return _api_keys

def get_limit_for(key: str) -> int:
    meta = _api_keys.get(key)
    if not meta:
        return 0
    try:
        return int(meta.get("limit", DEFAULT_LIMIT))
    except Exception:
        return DEFAULT_LIMIT

def api_key_ok(key: str) -> bool:
    return key in _api_keys

def check_rate_limit(key: str) -> bool:
    """Simple fixed-window limit per minute."""
    limit = get_limit_for(key)
    if limit <= 0:
        return False
    now = int(time.time())
    window = now - (now % 60)  # minute boundary
    start, count = _buckets.get(key, (window, 0))
    if start != window:
        start, count = window, 0
    if count + 1 > limit:
        _buckets[key] = (start, count)  # unchanged
        return False
    _buckets[key] = (start, count + 1)
    return True

def force_reload() -> None:
    load_api_keys()
    _buckets.clear()
