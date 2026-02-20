
import db
import sys

sys.path.append('.')

def inspect_schema():
    print("Connecting to DB...")
    client = db.get_client()
    try:

        rs = client.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
        tables = [row[0] for row in rs.rows]
        print(f"Tables found: {tables}")

        with open('schema.txt', 'w', encoding='utf-8') as f:
            for table in tables:
                f.write(f"\n--- Schema for {table} ---\n")
                rs_schema = client.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table}';")
                if rs_schema.rows:
                    f.write(rs_schema.rows[0][0] + ";\n")
            print("Schema written to schema.txt")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        client.close()

if __name__ == "__main__":
    inspect_schema()
