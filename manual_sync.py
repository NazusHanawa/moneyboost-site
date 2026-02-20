import db
import traceback

print("Starting manual sync...")
try:
    db.cache_manager.sync_from_turso()
    print("Sync finished successfully.")
except Exception:
    traceback.print_exc()
