# db_loader.py
from __future__ import annotations
from pathlib import Path
from typing import Dict

HASHES: Dict[str, int] = {}

def load_hash_file(path: Path) -> int:
    """Load 'SHA1:COUNT' lines into memory. Returns number of rows loaded."""
    global HASHES
    HASHES.clear()
    if not path.exists():
        print(f"❌ Hash file not found: {path}")
        return 0

    total = 0
    print(f"📂 Loading hashes from: {path}")
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        for i, line in enumerate(f, 1):
            line = line.strip()
            if not line or ":" not in line:
                continue
            sha1, count = line.split(":", 1)
            sha1 = sha1.strip().upper()
            try:
                cnt = int(count.strip())
            except Exception:
                cnt = 1
            HASHES[sha1] = cnt
            total += 1
            if i % 100000 == 0:
                print(f"   … loaded {i:,} lines")
    print(f"✅ Finished loading {total:,} hashes")
    return total

def lookup(sha1_hex_upper: str) -> int:
    """Return count for a given UPPERCASE SHA1 hex, or 0 if not found."""
    return HASHES.get(sha1_hex_upper, 0)
