# load_to_postgres.py
"""
Stream-import a large 'SHA1:COUNT' file into Postgres.

Usage:
    python3 load_to_postgres.py /path/to/pwnedpasswords.txt

This script:
 - creates a temporary unlogged table pwned_tmp
 - COPY FROM STDIN the input file (delimiter ':')
 - INSERT ... SELECT into final 'hashes' table with ON CONFLICT using GREATEST
 - drops the temp table and VACUUM ANALYZE
"""
import sys
import os
import time
import psycopg2

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

def create_tmp_table(cur):
    cur.execute("DROP TABLE IF EXISTS pwned_tmp;")
    cur.execute("CREATE UNLOGGED TABLE pwned_tmp (sha1 CHAR(40), count BIGINT);")

def merge_tmp_to_hashes(cur):
    cur.execute("""
        INSERT INTO hashes (sha1, count)
        SELECT sha1, MAX(count) AS count FROM pwned_tmp GROUP BY sha1
        ON CONFLICT (sha1) DO UPDATE
          SET count = GREATEST(hashes.count, EXCLUDED.count);
    """)
    cur.connection.commit()

def vacuum_analyze(cur):
    cur.execute("VACUUM ANALYZE hashes;")
    cur.connection.commit()

def stream_copy_file(conn, file_path):
    cur = conn.cursor()
    create_tmp_table(cur)
    conn.commit()

    sql = "COPY pwned_tmp (sha1, count) FROM STDIN WITH (FORMAT text, DELIMITER ':' )"
    print(f"📥 Starting COPY from {file_path} into pwned_tmp (this may take a while)...")

    started = time.time()
    with open(file_path, "r", encoding="utf-8", errors="ignore") as fh:
        cur.copy_expert(sql, fh)  # this streams directly to postgres
    print(f"✅ COPY finished in {time.time()-started:.2f}s")

    cur.execute("SELECT COUNT(*) FROM pwned_tmp;")
    tmp_count = cur.fetchone()[0]
    print(f"Temporary rows loaded: {tmp_count:,}")

    print("🔁 Merging into final table (hashes)...")
    started = time.time()
    merge_tmp_to_hashes(cur)
    print(f"✅ Merge complete in {time.time()-started:.2f}s")

    print("🧹 Dropping temporary table...")
    cur.execute("DROP TABLE IF EXISTS pwned_tmp;")
    conn.commit()

    print("🔎 Running VACUUM ANALYZE...")
    vacuum_analyze(cur)
    print("✅ Done.")

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 load_to_postgres.py /path/to/pwnedpasswords.txt")
        sys.exit(1)
    file_path = sys.argv[1]
    if not os.path.exists(file_path):
        print("File not found:", file_path)
        sys.exit(1)

    conn = get_conn()
    try:
        # ensure final table exists
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS hashes (
                sha1 CHAR(40) PRIMARY KEY,
                count BIGINT NOT NULL
            );
            """)
            conn.commit()

        stream_copy_file(conn, file_path)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
