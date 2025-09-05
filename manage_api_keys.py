# manage_api_keys.py
from __future__ import annotations
import json, os, random, string, time
from pathlib import Path
from sys import platform

KEYS_PATH = Path("keys.json")

# ----------------- visuals -----------------
def clear():
    os.system('cls' if platform == "win32" else 'clear')

def color(text, c="92"):  # green default
    return f"\033[{c}m{text}\033[0m"

BANNER = r"""
██████╗ ██╗   ██╗███████╗ █████╗ ███╗   ███╗███████╗██████╗ 
██╔══██╗██║   ██║██╔════╝██╔══██╗████╗ ████║██╔════╝██╔══██╗
██████╔╝██║   ██║███████╗███████║██╔████╔██║█████╗  ██████╔╝
██╔══██╗██║   ██║╚════██║██╔══██║██║╚██╔╝██║██╔══╝  ██╔══██╗
██║  ██║╚██████╔╝███████║██║  ██║██║ ╚═╝ ██║███████╗██║  ██║
╚═╝  ╚═╝ ╚═════╝ ╚══════╝╚═╝  ╚═╝╚═╝     ╚═╝╚══════╝╚═╝  ╚═╝
                  RUsamer API Key Vault
"""

def header():
    clear()
    print(color(BANNER, "92"))
    print(color("==[ Kirmina API Key Vault ]==\n", "96"))

# ----------------- storage -----------------
def load_keys():
    if not KEYS_PATH.exists():
        KEYS_PATH.write_text(json.dumps({"bankdev123": {"limit": 1000}}, indent=2))
    return json.loads(KEYS_PATH.read_text() or "{}")

def save_keys(keys: dict):
    KEYS_PATH.write_text(json.dumps(keys, indent=2))

# ----------------- actions -----------------
def list_keys(keys: dict):
    print(color("[+] Active API Keys:", "92"))
    for k, v in keys.items():
        lim = v.get("limit", 60)
        print(color(f"   • {k}  →  {lim} requests/min", "92"))

def gen_key(n=24):
    alphabet = string.ascii_letters + string.digits
    return ''.join(random.choices(alphabet, k=n))

def add_key(keys: dict):
    newk = gen_key()
    while True:
        try:
            limit = int(input(color("Set request limit per minute: ", "93")))
            break
        except ValueError:
            print(color("Enter a number.", "91"))
    keys[newk] = {"limit": limit}
    save_keys(keys)
    print(color(f"\n[+] Created key: {newk}", "92"))

def edit_key(keys: dict):
    k = input(color("Enter existing API key: ", "93")).strip()
    if k not in keys:
        print(color("Key not found.", "91"))
        return
    while True:
        try:
            limit = int(input(color("New request limit per minute: ", "93")))
            break
        except ValueError:
            print(color("Enter a number.", "91"))
    keys[k]["limit"] = limit
    save_keys(keys)
    print(color(f"[~] Updated key '{k}' → {limit}/min", "92"))

def delete_key(keys: dict):
    k = input(color("Enter API key to delete: ", "93")).strip()
    if k not in keys:
        print(color("Key not found.", "91"))
        return
    del keys[k]
    save_keys(keys)
    print(color(f"[-] Removed key '{k}'", "91"))

# ----------------- menu -----------------
def main():
    while True:
        header()
        keys = load_keys()
        list_keys(keys)
        print()
        print(color("1) List  2) Add  3) Edit  4) Delete  5) Exit", "96"))
        choice = input(color("\nSelect option: ", "92")).strip()
        if choice == "1":
            pass  # already listed
        elif choice == "2":
            add_key(keys)
        elif choice == "3":
            edit_key(keys)
        elif choice == "4":
            delete_key(keys)
        elif choice == "5":
            print(color("\nBye! RUsamer vault closed.\n", "92"))
            break
        else:
            print(color("Invalid option.", "91"))
        input(color("\nPress Enter to continue...", "90"))

if __name__ == "__main__":
    main()
