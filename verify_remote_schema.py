import os
import libsql_client
from dotenv import load_dotenv

load_dotenv(override=True)

URL = os.getenv("TURSO_DATABASE_URL")
TOKEN = os.getenv("TURSO_AUTH_TOKEN")

def check_remote_schema():
    if not URL or not TOKEN:
        print("Error: Config missing")
        return

    print(f"Connecting to {URL}...")
    client = libsql_client.create_client_sync(url=URL, auth_token=TOKEN)

    try:

        print("Checking tables...")
        tables = client.execute("SELECT name FROM sqlite_master WHERE type='table'").rows
        table_names = [row[0] for row in tables]
        print(f"Tables found: {table_names}")

        if 'table_updates' in table_names:
            print("PASS: 'table_updates' table exists.")

            rows = client.execute("SELECT * FROM table_updates").rows
            print(f"table_updates rows: {len(rows)}")
            for r in rows:
                print(f" - {r}")
        else:
            print("FAIL: 'table_updates' table MISSING.")

        print("\nChecking triggers...")
        triggers = client.execute("SELECT name FROM sqlite_master WHERE type='trigger'").rows
        trigger_names = [row[0] for row in triggers]
        print(f"Triggers found: {trigger_names}")

        expected_triggers = [
            'tr_stores_ins', 'tr_stores_upd', 'tr_stores_del',
            'tr_platforms_ins', 'tr_platforms_upd', 'tr_platforms_del',
            'tr_partnerships_ins', 'tr_partnerships_upd', 'tr_partnerships_del',
            'tr_cashbacks_ins', 'tr_cashbacks_upd', 'tr_cashbacks_del'
        ]

        missing_triggers = [t for t in expected_triggers if t not in trigger_names]
        if missing_triggers:
            print(f"FAIL: Missing triggers: {missing_triggers}")
        else:
            print("PASS: All sync triggers exist.")

        print("\nChecking views...")
        views = client.execute("SELECT name FROM sqlite_master WHERE type='view'").rows
        view_names = [row[0] for row in views]
        print(f"Views found: {view_names}")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        client.close()

if __name__ == "__main__":
    check_remote_schema()
