# manage_api_keys.py
import json
import secrets
from pathlib import Path
from typing import Dict

KEYS_PATH = Path("keys.json")

def load_keys() -> Dict:
    if not KEYS_PATH.exists():
        KEYS_PATH.write_text(json.dumps({"bankdev123": {"limit": 1000}}, indent=2))
    return json.loads(KEYS_PATH.read_text() or "{}")

def save_keys(d: Dict):
    KEYS_PATH.write_text(json.dumps(d, indent=2))

def list_keys():
    keys = load_keys()
    print("🔑 Existing API Keys:")
    for k, v in keys.items():
        print(f"  • {k}  →  {v.get('limit', 60)} req/min")

def add_key(label: str | None = None, limit: int = 60):
    keys = load_keys()
    token = secrets.token_urlsafe(24)
    label = label or token[:8]
    keys[token] = {"limit": limit, "label": label}
    save_keys(keys)
    print("✅ New key created:")
    print(token)
    print(f"limit: {limit}")

def remove_key(key: str):
    keys = load_keys()
    if key in keys:
        del keys[key]
        save_keys(keys)
        print("✅ Removed", key)
    else:
        print("⚠ Key not found")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Manage API keys (keys.json)")
    parser.add_argument("--list", action="store_true")
    parser.add_argument("--add", action="store_true")
    parser.add_argument("--label", type=str, default=None)
    parser.add_argument("--limit", type=int, default=60)
    parser.add_argument("--remove", type=str, default=None)
    args = parser.parse_args()
    if args.list:
        list_keys()
    elif args.add:
        add_key(args.label, args.limit)
    elif args.remove:
        remove_key(args.remove)
    else:
        parser.print_help()
