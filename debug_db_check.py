import sqlite3

def check_db():
    try:
        conn = sqlite3.connect('cache.db')
        cursor = conn.cursor()

        tables = ['stores', 'platforms', 'partnerships', 'cashbacks', '_metadata']
        for table in tables:
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"{table}: {count}")
            except Exception as e:
                print(f"{table}: ERROR {e}")

        try:
            cursor.execute("PRAGMA table_info(cashbacks)")
            columns = [info[1] for info in cursor.fetchall()]
            print(f"Cashbacks columns: {columns}")
        except Exception as e:
            print(f"Schema check error: {e}")

    except Exception as e:
        print(f"DB Connection error: {e}")

if __name__ == "__main__":
    check_db()
