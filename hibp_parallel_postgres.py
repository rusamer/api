"""
Full HIBP SHA1:COUNT import into PostgreSQL using a local txt file.

Features:
- Reads HIBP txt file in parallel batches
- Live global progress counter
- PostgreSQL tuning for bulk import
- Temp tables merged safely into final table
- Prompts for custom PostgreSQL data path
"""

import os
import time
import psycopg2
from queue import Queue
from threading import Thread
from multiprocessing import Value, cpu_count

# =========================
# CONFIGURATION
# =========================
DB_NAME = os.getenv("DB_NAME", "pwned")
DB_USER = os.getenv("DB_USER", "pwned_user")
DB_PASS = os.getenv("DB_PASS", "P@ssw0rd512")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")

POSTGRES_DATA_PATH = "/home/samer/api/data"  # Custom PostgreSQL data path
HIBP_FILE = "/home/samer/api/data/pwned.txt"  # Local HIBP txt file
NUM_WORKERS = max(1, cpu_count() - 1)
BATCH_SIZE = 1_000_000  # Number of lines per batch

# =========================
# POSTGRES SETUP
# =========================
def configure_postgres_data_path():
    print(f"⚠️ Make sure PostgreSQL service is stopped before moving data!")
    input("Press Enter to continue...")
    os.makedirs(POSTGRES_DATA_PATH, exist_ok=True)
    print(f"✅ Directory {POSTGRES_DATA_PATH} ready for PostgreSQL data.")
    print("⚠️RUsamer : must manually configure postgresql.conf:")
    print(f"data_directory = '{POSTGRES_DATA_PATH}'")
    input("After editing postgresql.conf and restarting PostgreSQL, press Enter to continue...")

def get_conn():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT
    )

def tune_postgres(conn):
    cur = conn.cursor()
    print("⚡ Tuning PostgreSQL for bulk import...")
    cur.execute("SET synchronous_commit = OFF;")
    cur.execute("SET work_mem = '128MB';")
    cur.execute("SET maintenance_work_mem = '2GB';")
    cur.execute("SET max_parallel_workers_per_gather = 4;")
    cur.connection.commit()
    print("✅ PostgreSQL tuned.")

# =========================
# DATABASE FUNCTIONS
# =========================
def create_final_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS hashes (
            sha1 CHAR(40) PRIMARY KEY,
            count BIGINT NOT NULL
        );
    """)

def merge_temp_to_hashes(cur, temp_table):
    cur.execute(f"""
        INSERT INTO hashes (sha1, count)
        SELECT sha1, MAX(count) AS count FROM {temp_table} GROUP BY sha1
        ON CONFLICT (sha1) DO UPDATE
        SET count = GREATEST(hashes.count, EXCLUDED.count);
    """)
    cur.connection.commit()

def vacuum_hashes(cur):
    cur.execute("VACUUM ANALYZE hashes;")
    cur.connection.commit()

# =========================
# BATCH PROCESSING
# =========================
def process_batch(batch_lines, batch_id, total_counter):
    # Filter invalid lines just in case
    batch_lines = [line for line in batch_lines if ':' in line and line.split(':')[1].isdigit()]

    temp_table = f"pwned_tmp_{batch_id}"
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS {temp_table};")
    cur.execute(f"CREATE UNLOGGED TABLE {temp_table} (sha1 CHAR(40), count BIGINT);")
    conn.commit()

    from io import StringIO
    buffer = StringIO("\n".join(batch_lines))
    cur.copy_expert(
        f"COPY {temp_table} (sha1, count) FROM STDIN WITH (FORMAT text, DELIMITER ':' )",
        buffer
    )

    # Update global progress
    with total_counter.get_lock():
        total_counter.value += len(batch_lines)
        print(f"[Batch {batch_id}] Loaded {len(batch_lines):,} lines. Total inserted: {total_counter.value:,}")

    # Merge into final table
    merge_temp_to_hashes(cur, temp_table)
    cur.execute(f"DROP TABLE IF EXISTS {temp_table};")
    conn.commit()
    conn.close()
    print(f"[Batch {batch_id}] ✅ Merged and temp table dropped.")

def batch_worker(queue, total_counter):
    while True:
        item = queue.get()
        if item is None:
            break
        batch_id, batch_lines = item
        process_batch(batch_lines, batch_id, total_counter)
        queue.task_done()

# =========================
# MAIN FUNCTION
# =========================
def main():
    if not os.path.exists(HIBP_FILE):
        print(f"HIBP file not found: {HIBP_FILE}")
        return

    configure_postgres_data_path()

    # Connect and tune
    conn = get_conn()
    tune_postgres(conn)
    cur = conn.cursor()
    create_final_table(cur)
    conn.commit()
    conn.close()

    print("🚀 Starting import from HIBP txt file...")
    queue = Queue(maxsize=NUM_WORKERS*2)
    total_counter = Value('i', 0)
    threads = []

    # Start worker threads
    for _ in range(NUM_WORKERS):
        t = Thread(target=batch_worker, args=(queue, total_counter))
        t.start()
        threads.append(t)

    # Read file and queue batches
    batch_lines = []
    batch_id = 0
    start_time = time.time()
    with open(HIBP_FILE, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            batch_lines.append(line.strip())
            if len(batch_lines) >= BATCH_SIZE:
                batch_id += 1
                queue.put((batch_id, batch_lines))
                print(f"[Main] Queued batch {batch_id} with {len(batch_lines):,} lines...")
                batch_lines = []

        # Last batch
        if batch_lines:
            batch_id += 1
            queue.put((batch_id, batch_lines))
            print(f"[Main] Queued final batch {batch_id} with {len(batch_lines):,} lines...")

    # Stop workers
    queue.join()
    for _ in threads:
        queue.put(None)
    for t in threads:
        t.join()

    # Vacuum
    conn = get_conn()
    cur = conn.cursor()
    print("🔎 Running VACUUM ANALYZE on final hashes table...")
    vacuum_hashes(cur)
    conn.close()

    print(f"✅ All done in {time.time() - start_time:.2f}s!")
    print(f"📊 Total lines processed: {total_counter.value:,}")

if __name__ == "__main__":
    main()
