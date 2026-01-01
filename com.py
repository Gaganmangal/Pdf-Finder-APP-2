import os
import hashlib
import pymongo
from datetime import datetime, timezone
from pymongo import UpdateOne
from concurrent.futures import ProcessPoolExecutor

# ================= CONFIG =================
MONGO_URI = "mongodb+srv://Gaganfnr:ndLz9yHCsOmv9S3k@gagan.jhuti8y.mongodb.net/test?appName=Gagan&compressors=zlib&maxPoolSize=50"
ROOT_PATH = "/mnt/pdfs"
BATCH_SIZE = 5000  
MAX_WORKERS = 8    # Jitne aapke CPU cores hain (Ubuntu par 'nproc' se check karein)
# ==========================================

def sha1(val: str) -> str:
    return hashlib.sha1(val.encode("utf-8")).hexdigest()

def scan_branch(folder_path):
    """Har worker ek sub-folder ko scan karega"""
    client = pymongo.MongoClient(MONGO_URI)
    db = client.test
    latest = db.FileMetaLatest
    
    # 80TB scale par Index check yahan zaruri hai
    latest.create_index("fileId", unique=True)

    ops = []
    local_count = 0
    now = datetime.now(timezone.utc)
    stack = [folder_path]

    while stack:
        current = stack.pop()
        try:
            with os.scandir(current) as it:
                for entry in it:
                    if entry.is_dir(follow_symlinks=False):
                        stack.append(entry.path)
                        continue

                    if entry.is_file(follow_symlinks=False):
                        st = entry.stat()
                        path = entry.path
                        f_id = sha1(path)

                        # Compact document structure for high speed
                        doc = {
                            "fileId": f_id,
                            "fullPath": path,
                            "fileName": entry.name,
                            "sizeBytes": st.st_size,
                            "modifiedAt": datetime.fromtimestamp(st.st_mtime, tz=timezone.utc),
                            "updatedAt": now,
                            "accessCount": {"$inc": 1}, # Pattern tracking integrated
                            "lastAccessedAt": now
                        }

                        # Hum UpdateOne use kar rahe hain with $set and $inc
                        ops.append(UpdateOne(
                            {"fileId": f_id},
                            {
                                "$set": {
                                    "fullPath": path,
                                    "fileName": entry.name,
                                    "sizeBytes": st.st_size,
                                    "modifiedAt": doc["modifiedAt"],
                                    "updatedAt": now,
                                    "lastAccessedAt": now
                                },
                                "$inc": {"accessCount": 1},
                                "$setOnInsert": {"firstSeenAt": now}
                            },
                            upsert=True
                        ))

                        local_count += 1
                        if len(ops) >= BATCH_SIZE:
                            latest.bulk_write(ops, ordered=False)
                            ops.clear()

        except (PermissionError, OSError):
            continue

    if ops:
        latest.bulk_write(ops, ordered=False)
    
    client.close()
    return local_count

def main():
    print(f"🚀 Starting Multi-Core Turbo Scan...")
    
    # Root ke top-level folders ko list karein taaki workers ko kaam baant sakein
    try:
        branches = [f.path for f in os.scandir(ROOT_PATH) if f.is_dir()]
        if not branches: branches = [ROOT_PATH] # Agar koi subfolder nahi hai
    except Exception as e:
        print(f"Error: {e}"); return

    total_files = 0
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = list(executor.map(scan_branch, branches))
        total_files = sum(results)

    print(f"✅ Completed. Total: {total_files:,} files indexed.")

if __name__ == "__main__":
    main()