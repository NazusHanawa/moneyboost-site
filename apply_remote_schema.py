import os
import libsql_client
from dotenv import load_dotenv

load_dotenv(override=True)

URL = os.getenv("TURSO_DATABASE_URL")
TOKEN = os.getenv("TURSO_AUTH_TOKEN")

def apply_schema():
    if not URL or not TOKEN:
        print("Error: TURSO_DATABASE_URL or TURSO_AUTH_TOKEN not found in .env")
        return

    print(f"Connecting to {URL}...")
    client = libsql_client.create_client_sync(url=URL, auth_token=TOKEN)

    try:
        with open("schema.txt", "r", encoding="utf-8") as f:
            schema_sql = f.read()

        statements = [s.strip() for s in schema_sql.split(";") if s.strip()]

        print(f"Found {len(statements)} statements to execute.")

        for i, stmt in enumerate(statements):
            try:
                print(f"Executing statement {i+1}...")

                client.execute(stmt)
            except Exception as e:
                print(f"Error executing statement {i+1}: {e}")
                print(f"Statement: {stmt[:100]}...")

        print("Schema application complete.")

    except Exception as e:
        print(f"Critical Error: {e}")
    finally:
        client.close()

if __name__ == "__main__":
    apply_schema()
