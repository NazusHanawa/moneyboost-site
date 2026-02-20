
import db
try:
    client = db.get_client()
    rs = client.execute("SELECT 1")
    print(f"Query successful! Result: {rs.rows}")
    client.close()
except Exception as e:
    print(f"Query failed: {e}")
