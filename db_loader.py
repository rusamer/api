# db_loader.py
from __future__ import annotations
import os
import psycopg2
from psycopg2.extras import RealDictCursor

# Default DB credentials (change if you used something else)
DB_NAME = os.getenv("DB_NAME", "pwned")
DB_USER = os.getenv("DB_USER", "pwned_user")
DB_PASS = os.getenv("DB_PASS", "P@ssw0rd512")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")

def get_conn():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT
    )

def init_db():
    """
    Ensure the main table exists. Call at startup.
    """
    sql = """
    CREATE TABLE IF NOT EXISTS hashes (
        sha1 CHAR(40) PRIMARY KEY,
        count BIGINT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_hashes_sha1 ON hashes(sha1);
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            conn.commit()
    finally:
        conn.close()

def load_hash_file(path) -> int:
    """
    Legacy placeholder: no in-memory loading. Return 0.
    Kept for compatibility with older startup code.
    """
    return 0

def lookup(sha1_hex_upper: str) -> int:
    """
    Query Postgres for a SHA1 hash count. Returns integer count or 0.
    """
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT count FROM hashes WHERE sha1 = %s;", (sha1_hex_upper,))
            row = cur.fetchone()
            return int(row["count"]) if row else 0
    finally:
        conn.close()
