# db_loader.py
from __future__ import annotations
import psycopg2
import os

DB_NAME = os.getenv("DB_NAME", "pwned")
DB_USER = os.getenv("DB_USER", "pwned_user")
DB_PASS = os.getenv("DB_PASS", "CHANGE_ME_STRONG_PASS")
DB_HOST = os.getenv("DB_HOST", "localhost")

def get_connection():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST
    )

def load_hash_file(path=None) -> int:
    """No-op in DB mode (data preloaded via COPY)."""
    return 0

def lookup(sha1_hex_upper: str) -> int:
    """Query Postgres for a SHA1 hash count."""
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT count FROM pwned_passwords WHERE sha1 = %s;", (sha1_hex_upper,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    if row:
        return row[0]
    return 0
