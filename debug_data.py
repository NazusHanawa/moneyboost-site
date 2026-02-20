
import db

def debug_data():
    client = db.get_client()
    try:
        print("--- Table Counts ---")
        tables = ["stores", "platforms", "partnerships", "cashbacks"]
        for t in tables:
            rs = client.execute(f"SELECT count(*) FROM {t}")
            print(f"{t}: {rs.rows[0][0]}")

        print("\n--- Recent Cashbacks (Limit 5) ---")

        rs = client.execute("SELECT id, value, date_end, datetime('now', 'localtime') as current_time FROM cashbacks ORDER BY date_end DESC LIMIT 5")
        for row in rs.rows:
            print(row)

        print("\n--- Partnerships (Limit 5) ---")
        rs = client.execute("SELECT * FROM partnerships LIMIT 5")
        for row in rs.rows:
            print(row)

    except Exception as e:
        print(f"Error: {e}")
    finally:
        client.close()

if __name__ == "__main__":
    debug_data()
