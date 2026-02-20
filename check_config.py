
import os
from dotenv import load_dotenv

load_dotenv(override=True)

url = os.getenv("TURSO_DATABASE_URL")
print(f"Current configured URL: {url}")

if url.startswith("https://"):
    print("SUCCESS: Configuration is correct.")
else:
    print("WARNING: URL does not start with https://. Check .env file.")
