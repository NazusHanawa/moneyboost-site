
import db
import sqlite3

def test_row_access():
    print("Testing row access...")
    platforms = db.get_platforms()

    if not platforms:
        print("No platforms found to test.")
        return

    p = platforms[0]
    print(f"Row type: {type(p)}")

    try:
        print(f"Index 0: {p[0]}")
        print("Index access OK.")
    except Exception as e:
        print(f"Index access FAILED: {e}")

    try:
        print(f"Name key: {p['name']}")
        print("Key access OK.")
    except Exception as e:
        print(f"Key access FAILED: {e}")

if __name__ == "__main__":
    test_row_access()
