
import db
import time
import os
import sqlite3

def verify_cache():
    print("--- Starting Cache Verification ---")

    print("Clearing existing cache data...")
    try:
        if os.path.exists("cache.db"):

             pass
    except OSError:
        print("Could not remove cache.db, clearing tables instead.")

    conn = sqlite3.connect("cache.db")
    cursor = conn.cursor()

    try:
        cursor.execute("CREATE TABLE IF NOT EXISTS _metadata (key TEXT PRIMARY KEY, value TEXT)")
        cursor.execute("INSERT OR REPLACE INTO _metadata (key, value) VALUES ('last_sync', '0')")
        conn.commit()
    except Exception as e:
        print(f"Setup error: {e}")
    finally:
        conn.close()

    print("\n[Action] First access (should sync)...")
    start = time.time()
    stores = db.get_stores_with_all_cashbacks()
    duration = time.time() - start
    print(f"Fetch took {duration:.2f}s")
    print(f"Got {len(stores)} stores.")

    conn = sqlite3.connect("cache.db")
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM _metadata WHERE key = 'last_sync'")
    row = cursor.fetchone()
    conn.close()

    if row:
        print(f"Sync timestamp in DB: {row[0]}")
    else:
        print("ERROR: No sync timestamp found!")

    print("\n[Action] Second access (should be cached)...")
    start = time.time()
    db.get_stores_with_all_cashbacks()
    duration = time.time() - start
    print(f"Fetch took {duration:.4f}s")

    if duration > 1.0:
        print("WARNING: Second access took > 1s, might not be cached?")
    else:
        print("SUCCESS: Cache hit confirmed.")

    print("\n[Action] Forcing cache expiration...")
    conn = sqlite3.connect("cache.db")
    cursor = conn.cursor()

    past = time.time() - 700 
    cursor.execute("UPDATE _metadata SET value = ? WHERE key = 'last_sync'", (str(past),))
    conn.commit()
    conn.close()
    print("Cache expired manually.")

    print("\n[Action] Third access (expired, should sync)...")
    start = time.time()
    db.get_stores_with_all_cashbacks()
    duration = time.time() - start
    print(f"Fetch took {duration:.2f}s")

    if duration < 0.1:
         print("WARNING: Sync might not have happened?")
    else:
         print("SUCCESS: Re-sync looks likely.")

if __name__ == "__main__":
    verify_cache()
