import db
import sqlite3

print("Testing get_stores_with_all_cashbacks...")
try:
    stores = db.get_stores_with_all_cashbacks()
    print(f"Found {len(stores)} stores.")
    for s in stores[:3]:
        print(s)
except Exception as e:
    print(f"Error: {e}")

print("\nTesting raw query...")
try:
    conn = db.get_client()
    cursor = conn.cursor()
    cursor.execute("SELECT count(*) FROM stores")
    print(f"Stores count: {cursor.fetchone()[0]}")

    cursor.execute("SELECT count(*) FROM partnerships")
    print(f"Partnerships count: {cursor.fetchone()[0]}")

    cursor.execute("SELECT count(*) FROM cashbacks")
    print(f"Cashbacks count: {cursor.fetchone()[0]}")

    query = """
        SELECT count(*)
        FROM stores s
        JOIN partnerships pa ON s.id = pa.store_id
        JOIN cashbacks c ON pa.id = c.partnership_id
        JOIN platforms p ON pa.platform_id = p.id
    """
    cursor.execute(query)
    print(f"Join count: {cursor.fetchone()[0]}")

except Exception as e:
    print(f"Raw query error: {e}")
