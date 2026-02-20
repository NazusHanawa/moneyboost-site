
import os
import time
import sqlite3
import threading
from datetime import datetime
import libsql_client
from dotenv import load_dotenv

load_dotenv(override=True)

URL = os.getenv("TURSO_DATABASE_URL")
TOKEN = os.getenv("TURSO_AUTH_TOKEN")
LOCAL_DB = "cache.db"
CACHE_DURATION = 1800  

class CacheManager:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(CacheManager, cls).__new__(cls)
                    cls._instance._init_cache()
        return cls._instance

    def _init_cache(self):
        """Initializes the local cache database."""
        self.conn = sqlite3.connect(LOCAL_DB, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row  

        self.conn.execute("PRAGMA journal_mode=WAL;")

        self.cursor = self.conn.cursor()
        self.last_check_time = 0
        self._create_tables()

        self.sync_thread = threading.Thread(target=self._background_sync_loop, daemon=True)
        self.sync_thread.start()

    def _background_sync_loop(self):
        """Background loop to check for updates and sync."""
        print("DEBUG: Background sync thread started.")
        while True:
            try:
                with self._lock:
                    self.sync_from_turso()

                time.sleep(30)

            except Exception as e:
                print(f"ERROR: Background sync loop error: {e}")
                time.sleep(60) 

    def _create_tables(self):
        """Creates tables in the local cache if they don't exist."""

        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS _metadata (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)

        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS stores (
                id INTEGER PRIMARY KEY,
                name TEXT UNIQUE NOT NULL,
                url TEXT
            )
        """)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS platforms (
                id INTEGER PRIMARY KEY,
                name TEXT UNIQUE NOT NULL,
                url TEXT
            )
        """)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS partnerships (
                id INTEGER PRIMARY KEY,
                store_id INTEGER NOT NULL,
                platform_id INTEGER NOT NULL,
                url TEXT,
                UNIQUE (store_id, platform_id),
                FOREIGN KEY (store_id) REFERENCES stores (id) ON DELETE CASCADE,
                FOREIGN KEY (platform_id) REFERENCES platforms (id) ON DELETE CASCADE
            )
        """)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS cashbacks (
                id INTEGER PRIMARY KEY,
                partnership_id INTEGER NOT NULL,
                value_global REAL NOT NULL CHECK (value_global >= 0),
                value_specific REAL CHECK (value_specific >= value_global),
                description TEXT,
                date_start TEXT NOT NULL DEFAULT (datetime ('now', 'localtime')),
                date_end TEXT NOT NULL DEFAULT (datetime ('now', 'localtime')),
                FOREIGN KEY (partnership_id) REFERENCES partnerships (id) ON DELETE CASCADE
            )
        """)

        self.cursor.execute("DROP VIEW IF EXISTS vw_partnerships")
        self.cursor.execute("""
            CREATE VIEW vw_partnerships AS
            SELECT 
                p.id AS partnership_id,
                p.url AS partnership_url,
                s.id AS store_id,
                s.name AS store_name,
                pl.id AS platform_id,
                pl.name AS platform_name
            FROM partnerships p
            JOIN stores s ON p.store_id = s.id
            JOIN platforms pl ON p.platform_id = pl.id
        """)

        self.cursor.execute("DROP VIEW IF EXISTS vw_cashbacks")
        self.cursor.execute("""
            CREATE VIEW vw_cashbacks AS
            SELECT 
                c.id AS cashback_id,
                c.value_global AS global_value,
                c.value_specific AS max_value,
                c.description AS description,
                c.date_start AS date_start,
                c.date_end AS date_end,
                vp.partnership_id AS partnership_id,
                vp.partnership_url AS partnership_url,
                vp.store_id AS store_id,
                vp.store_name AS store_name,
                vp.platform_id AS platform_id,
                vp.platform_name AS platform_name
            FROM cashbacks c
            JOIN vw_partnerships vp ON c.partnership_id = vp.partnership_id
        """)

        self.cursor.execute("DROP VIEW IF EXISTS vw_latest_cashbacks")
        self.cursor.execute("""
            CREATE VIEW vw_latest_cashbacks AS
            SELECT *
            FROM (
                SELECT 
                    c.id AS cashback_id,
                    c.value_global AS global_value,
                    c.value_specific AS max_value,
                    c.description AS description,
                    c.date_start AS date_start,
                    c.date_end AS date_end,
                    vp.partnership_id AS partnership_id,
                    vp.partnership_url AS partnership_url,
                    vp.store_id AS store_id,
                    vp.store_name AS store_name,
                    vp.platform_id AS platform_id,
                    vp.platform_name AS platform_name,
                    ROW_NUMBER() OVER (
                        PARTITION BY c.partnership_id 
                        ORDER BY c.date_start DESC, c.id DESC
                    ) as rn
                FROM cashbacks c
                JOIN vw_partnerships vp ON c.partnership_id = vp.partnership_id
            ) 
            WHERE rn = 1
        """)

        self.conn.commit()

    def _should_sync(self):
        """Checks if the cache needs to be synced."""

        self.cursor.execute("SELECT value FROM _metadata WHERE key = 'last_check_time'")
        row = self.cursor.fetchone()
        last_check = float(row[0]) if row else 0

        if time.time() - last_check < CACHE_DURATION:
            return False

        print("DEBUG: Checking for remote updates...")
        self.cursor.execute("INSERT OR REPLACE INTO _metadata (key, value) VALUES ('last_check_time', ?)", (str(time.time()),))
        self.conn.commit()

        self.cursor.execute("SELECT value FROM _metadata WHERE key = 'last_sync'")
        row = self.cursor.fetchone()
        last_sync = float(row[0]) if row else 0

        try:
            remote_client = libsql_client.create_client_sync(url=URL, auth_token=TOKEN)
            rs = remote_client.execute("SELECT MAX(updated_at) FROM table_updates")
            remote_client.close()

            if not rs.rows or not rs.rows[0][0]:
                if last_sync == 0:
                    return True
                return False

            remote_updated_at_str = rs.rows[0][0] 

            dt = datetime.strptime(remote_updated_at_str, "%Y-%m-%d %H:%M:%S")
            from datetime import timezone
            dt = dt.replace(tzinfo=timezone.utc)
            remote_updated_ts = dt.timestamp()

            print(f"DEBUG: Remote Last Update: {remote_updated_ts} (Str: {remote_updated_at_str}) vs Local Sync: {last_sync}")

            if remote_updated_ts > last_sync:
                self.pending_sync_ts = remote_updated_ts
                return True

            return False

        except Exception as e:
            print(f"ERROR: Failed to check remote updates: {e}")
            return False

    def sync_from_turso(self):
        """Syncs data from Turso to local cache."""
        if not self._should_sync():
            return

        print("DEBUG: Syncing cache from Turso...")
        try:

            self.cursor.execute("SELECT MAX(id) FROM cashbacks")
            row = self.cursor.fetchone()
            max_local_id = row[0] if row and row[0] is not None else 0

            print(f"DEBUG: Fetching new cashbacks from ID > {max_local_id}")

            remote_client = libsql_client.create_client_sync(url=URL, auth_token=TOKEN)

            stores = remote_client.execute("SELECT * FROM stores").rows
            platforms = remote_client.execute("SELECT * FROM platforms").rows
            partnerships = remote_client.execute("SELECT * FROM partnerships").rows

            cashbacks = remote_client.execute("SELECT * FROM cashbacks WHERE id > ?", [max_local_id]).rows

            remote_client.close()

            self.cursor.execute("BEGIN TRANSACTION")

            self.cursor.execute("DELETE FROM partnerships")
            self.cursor.execute("DELETE FROM platforms")
            self.cursor.execute("DELETE FROM stores")

            self.cursor.executemany("INSERT INTO stores (id, name, url) VALUES (?, ?, ?)", stores)
            self.cursor.executemany("INSERT INTO platforms (id, name, url) VALUES (?, ?, ?)", platforms)
            self.cursor.executemany("INSERT INTO partnerships (id, store_id, platform_id, url) VALUES (?, ?, ?, ?)", partnerships)

            if cashbacks:
                self.cursor.executemany("INSERT INTO cashbacks (id, partnership_id, value_global, value_specific, description, date_start, date_end) VALUES (?, ?, ?, ?, ?, ?, ?)", cashbacks)
                print(f"DEBUG: Inserted {len(cashbacks)} new cashbacks.")
            else:
                print("DEBUG: No new cashbacks found.")

            sync_ts = getattr(self, 'pending_sync_ts', time.time())
            self.cursor.execute("INSERT OR REPLACE INTO _metadata (key, value) VALUES ('last_sync', ?)", (str(sync_ts),))

            self.conn.commit()
            print("DEBUG: Sync complete.")
        except Exception as e:
            print(f"ERROR: Failed to sync cache: {e}")
            self.conn.rollback()

    def get_connection(self):
        """Returns the local sqlite connection, syncing if necessary."""

        return self.conn

    def get_last_sync_time(self):
        """Returns the last check timestamp as a float or None."""
        self.cursor.execute("SELECT value FROM _metadata WHERE key = 'last_check_time'")
        row = self.cursor.fetchone()
        return float(row['value']) if row else None

cache_manager = CacheManager()

def get_client():
    """Returns a connection to the local cache database."""

    return cache_manager.get_connection()

def get_last_sync_time():
    return cache_manager.get_last_sync_time()

class LocalResultSet:
    def __init__(self, rows):
        self.rows = rows

class LocalClientWrapper:
    def __init__(self, connection):
        self.conn = connection

    def execute(self, query, params=()):
        cursor = self.conn.cursor()
        try:
            cursor.execute(query, params)
            rows = cursor.fetchall()
            return LocalResultSet(rows)
        except Exception as e:
            print(f"Query Error: {e}")
            raise

    def close(self):

        pass

def get_stores_with_all_cashbacks(search_query=None):

    raw_conn = get_client()
    client = LocalClientWrapper(raw_conn)

    try:

        base_query = """
            WITH LatestCashbacks AS (
                SELECT 
                    s.id as store_id, 
                    s.name as store_name, 
                    s.url as store_url, 
                    c.value_global as value,
                    c.value_specific,
                    p.id as platform_id,
                    p.name as platform_name,
                    ROW_NUMBER() OVER (PARTITION BY s.id, p.id ORDER BY c.date_start DESC) as rn
                FROM stores s
                JOIN partnerships pa ON s.id = pa.store_id
                JOIN cashbacks c ON pa.id = c.partnership_id
                JOIN platforms p ON pa.platform_id = p.id
        """

        params = []
        middle_query = ""
        if search_query:
            middle_query = " WHERE s.name LIKE ?"
            params.append(f"%{search_query}%")

        final_query = """
            )
            SELECT store_id, store_name, store_url, value, value_specific, platform_id, platform_name 
            FROM LatestCashbacks 
            WHERE rn = 1
        """

        query = base_query + middle_query + final_query

        rs = client.execute(query, params)

        stores_map = {}
        for row in rs.rows:
            store_id = row[0]
            if store_id not in stores_map:
                stores_map[store_id] = {
                    'id': row[0],
                    'name': row[1],
                    'url': row[2],
                    'offers': []
                }

            stores_map[store_id]['offers'].append({
                'platform_id': row[5],
                'platform_name': row[6],
                'value': row[3],
                'value_specific': row[4]
            })

        results = []
        for store in stores_map.values():
            if not store['offers']:
                continue

            store['offers'].sort(key=lambda x: x['value'], reverse=True)
            store['max_cashback'] = store['offers'][0]['value']
            store['platform_name'] = store['offers'][0]['platform_name'] 
            results.append(store)

        results.sort(key=lambda x: x['max_cashback'], reverse=True)
        return results
    finally:
        client.close()

def get_store_details(store_id):
    raw_conn = get_client()
    client = LocalClientWrapper(raw_conn)
    try:
        store_rs = client.execute("SELECT * FROM stores WHERE id = ?", [store_id])
        if not store_rs.rows:
            return None

        cashbacks_rs = client.execute("""
            SELECT 
                p.name as platform_name,
                c.value_global as value,
                c.value_specific,
                c.description,
                c.date_end,
                c.date_start,
                pa.url as partnership_url
            FROM partnerships pa
            JOIN platforms p ON pa.platform_id = p.id
            JOIN cashbacks c ON pa.id = c.partnership_id
            WHERE pa.store_id = ?
            ORDER BY c.date_start DESC
        """, [store_id])

        unique_cashbacks = {}
        for row in cashbacks_rs.rows:

            p_name = row['platform_name']
            if p_name not in unique_cashbacks:
                unique_cashbacks[p_name] = row

        final_cashbacks = list(unique_cashbacks.values())
        final_cashbacks.sort(key=lambda x: x['value'], reverse=True)

        return {
            "store": store_rs.rows[0],
            "cashbacks": final_cashbacks
        }
    finally:
        client.close()

def get_platforms():
    raw_conn = get_client()
    client = LocalClientWrapper(raw_conn)
    try:
        rs = client.execute("SELECT * FROM platforms ORDER BY name")
        return rs.rows
    finally:
        client.close()

def get_cashback_history(store_id, start_date=None, end_date=None, platform_ids=None):
    raw_conn = get_client() 
    client = LocalClientWrapper(raw_conn)

    try:
        query = """
            SELECT 
                c.value_global as value,
                c.value_specific,
                c.description,
                c.date_start,
                c.date_end,
                p.name as platform_name,
                p.id as platform_id
            FROM partnerships pa
            JOIN platforms p ON pa.platform_id = p.id
            JOIN cashbacks c ON pa.id = c.partnership_id
            WHERE pa.store_id = ?
        """
        params = [store_id]

        if start_date:

            query += " AND (c.date_end >= ? OR c.date_end IS NULL)"
            params.append(start_date)

        if end_date:
            query += " AND c.date_start <= ?"
            params.append(end_date)

        query += " ORDER BY c.date_start ASC"

        rs = client.execute(query, params)
        rows = rs.rows

        if platform_ids:
             rows = [r for r in rows if r['platform_id'] in platform_ids]

        return rows
    finally:
        client.close()
