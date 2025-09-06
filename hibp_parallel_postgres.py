"""
All-in-one HIBP SHA1:COUNT import to Postgres
- Configure Postgres to use custom data path
- Parallel batch streaming
- PostgreSQL tuning
- Live global progress counter
"""
import os
import time
import psycopg2
import subprocess
from multiprocessing import Queue, Process, cpu_count, Manager
from threading import Thread

# =========================
# CONFIGURATION
# =========================
DB_NAME = os.getenv("DB_NAME", "pwned")
DB_USER = os.getenv("DB_USER", "pwned_user")
DB_PASS = os.getenv("DB_PASS", "P@ssw0rd512")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")

POSTGRES_DATA_PATH = "/home/samer/api/data"  # custom DB storage path
NUM_WORKERS = max(1, cpu_count() - 1)
BATCH_SIZE = 1_000_000
HIBP_COMMAND = ["haveibeenpwned-downloader", "--format", "sha1ordered", "--output", "-"]

# =========================
# POSTGRES SETUP
# =========================
def configure_postgres_data_path():
    print(f"⚠️ Make sure PostgreSQL service is stopped before moving data!")
    input("Press Enter to continue...")
    os.makedirs(POSTGRES_DATA_PATH, exist_ok=True)
    print(f"✅ Directory {POSTGRES_DATA_PATH} ready for PostgreSQL data.")
    print("⚠️ You must manually configure your postgresql.conf:")
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

# =========================
# POSTGRES TUNING
# =========================
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
# BATCH PROCESSING
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

def process_batch(batch_lines, batch_id, total_counter, total_lines_estimate):
    temp_table = f"pwned_tmp_{batch_id}"
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS {temp_table};")
    cur.execute(f"CREATE UNLOGGED TABLE {temp_table} (sha1 CHAR(40), count BIGINT);")
    conn.commit()

    from io import StringIO
    buffer = StringIO("\n".join(batch_lines))
    cur.copy_expert(f"COPY {temp_table} (sha1, count) FROM STDIN WITH (FORMAT text, DELIMITER ':' )", buffer)

    # Update global counter
    total_counter.value += len(batch_lines)
    percent = (total_counter.value / total_lines_estimate) * 100 if total_lines_estimate else 0
    print(f"[Batch {batch_id}] ✅ Loaded {len(batch_lines):,} lines. Global progress: {total_counter.value:,} lines (~{percent:.2f}%)")

    # Merge into final table
    merge_temp_to_hashes(cur, temp_table)
    cur.execute(f"DROP TABLE IF EXISTS {temp_table};")
    conn.commit()
    conn.close()

# =========================
# MAIN STREAM WORKER
# =========================
def batch_worker(queue, total_counter, total_lines_estimate):
    while True:
        item = queue.get()
        if item is None:
            break
        batch_id, batch_lines = item
        process_batch(batch_lines, batch_id, total_counter, total_lines_estimate)
        queue.task_done()

# =========================
# MAIN FUNCTION
# =========================
def main():
    configure_postgres_data_path()

    conn = get_conn()
    tune_postgres(conn)
    cur = conn.cursor()
    create_final_table(cur)
    conn.commit()
    conn.close()

    print("🚀 Starting dynamic streaming from HIBP...")
    manager = Manager()
    total_counter = manager.Value('i', 0)
    total_lines_estimate = 0  # unknown; will remain zero until finish

    queue = Queue(maxsize=NUM_WORKERS * 2)
    threads = []
    for _ in range(NUM_WORKERS):
        t = Thread(target=batch_worker, args=(queue, total_counter, total_lines_estimate))
        t.start()
        threads.append(t)

    batch_lines = []
    batch_id = 0
    start_time = time.time()

    process = subprocess.Popen(HIBP_COMMAND, stdout=subprocess.PIPE, bufsize=10**7, text=True)

    for line in process.stdout:
        batch_lines.append(line.strip())
        if len(batch_lines) >= BATCH_SIZE:
            batch_id += 1
            queue.put((batch_id, batch_lines))
            batch_lines = []

    # Last batch
    if batch_lines:
        batch_id += 1
        queue.put((batch_id, batch_lines))

    process.stdout.close()
    process.wait()

    # Stop workers
    queue.join()
    for _ in threads:
        queue.put(None)
    for t in threads:
        t.join()

    # Vacuum final table
    conn = get_conn()
    cur = conn.cursor()
    print("🔎 Running VACUUM ANALYZE on final hashes table...")
    vacuum_hashes(cur)
    conn.close()

    print(f"✅ All done in {time.time() - start_time:.2f}s!")
    print(f"📊 Total lines processed: {total_counter.value:,}")

if __name__ == "__main__":
    main()
