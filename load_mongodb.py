# load_mongodb.py
import json
from pymongo import MongoClient
import os
import glob

# --- MongoDB Connection ---
client = MongoClient("mongodb://localhost:27017")
db = client["ecommerce"]

# --- Base directory paths ---
PROJECT_ROOT = os.path.join(os.getcwd(), "..")
GENERATED_DATA_DIR = os.path.join(PROJECT_ROOT, "generateddata")

# --- JSON files mapped to MongoDB collections ---
FILES_COLLECTIONS = {
    "users.json": "users",
    "products.json": "products",
    "categories.json": "categories",
    "transactions.json": "transactions"
}

# --- Batch size for large collections ---
BATCH_SIZE = 1000  # Safe for transactions & sessions

# -----------------------------
# Helper Functions
# -----------------------------

def load_json_file(file_path):
    """Load JSON data from a file."""
    with open(file_path, "r") as f:
        return json.load(f)

def load_collection(file_path, collection_name):
    """Load a single JSON file into a MongoDB collection using batching."""
    if not os.path.exists(file_path):
        print(f"[!] File not found: {file_path}, skipping.")
        return

    print(f"\nLoading {os.path.basename(file_path)} into '{collection_name}' collection...")
    data = load_json_file(file_path)

    # Clear existing data to avoid duplicates
    db[collection_name].delete_many({})

    total_docs = len(data)
    for i in range(0, total_docs, BATCH_SIZE):
        batch = data[i:i + BATCH_SIZE]
        db[collection_name].insert_many(batch)
        print(f"  Inserted {i + len(batch):,}/{total_docs:,} documents", end="\r")

    print(f"\n[✓] Loaded {total_docs:,} documents into '{collection_name}'.")

def load_sessions():
    """
    Load session data split across multiple files:
    sessions_0.json, sessions_1.json, ...
    """
    print("\nLoading session files into 'sessions' collection...")

    session_files = sorted(
        glob.glob(os.path.join(GENERATED_DATA_DIR, "sessions_*.json"))
    )

    if not session_files:
        print("[!] No session files found.")
        return

    # Clear existing sessions
    db["sessions"].delete_many({})

    total_inserted = 0

    for file_path in session_files:
        print(f"Processing {os.path.basename(file_path)}...")
        data = load_json_file(file_path)

        for i in range(0, len(data), BATCH_SIZE):
            batch = data[i:i + BATCH_SIZE]
            db["sessions"].insert_many(batch)
            total_inserted += len(batch)
            print(f"  Total sessions inserted: {total_inserted:,}", end="\r")

    print(f"\n[✓] Loaded {total_inserted:,} session documents into 'sessions'.")

# -----------------------------
# Main Execution
# -----------------------------

def main():
    print("Starting MongoDB data load...\n")

    # Load core collections
    for file_name, collection_name in FILES_COLLECTIONS.items():
        file_path = os.path.join(GENERATED_DATA_DIR, file_name)
        load_collection(file_path, collection_name)

    # Load sessions separately
    load_sessions()

    print("\n✅ All MongoDB data loaded successfully!")

if __name__ == "__main__":
    main()
