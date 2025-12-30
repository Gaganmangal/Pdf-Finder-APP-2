import os
import pymongo
import hashlib
from datetime import datetime, timezone

MONGO_URI = "mongodb+srv://Gaganfnr:ndLz9yHCsOmv9S3k@gagan.jhuti8y.mongodb.net/test?appName=Gagan"
FILE_ROOT = "D:/PDF"
FILEMETA_BATCH_SIZE = 1000
DUPLICATE_BATCH_SIZE = 500


def get_fingerprint(file_name, extension, size_bytes):
    return f"{file_name.lower()}|{extension}|{size_bytes}"

def sha1_hash(val):
    return hashlib.sha1(val.encode("utf-8")).hexdigest()

def classify_access(last_accessed, now):
    days = (now - last_accessed).days
    if days <= 30:
        return "HOT"
    elif days <= 180:
        return "WARM"
    return "COLD"

def strip_large_fields(doc):
    return {
        "fullPath": doc["fullPath"],
        "fileName": doc["fileName"],
        "extension": doc["extension"],
        "sizeBytes": doc["sizeBytes"],
        "modifiedAt": doc["modifiedAt"],
        "scannedAt": doc["scannedAt"],
        "fileAccessedAt": doc["fileAccessedAt"],
        "fileCreatedAt": doc["fileCreatedAt"],
    }

def scan_and_insert():
    client = pymongo.MongoClient(MONGO_URI)
    db = client["test"]
    filemeta = db["FileMeta"]
    duplicatefile = db["DuplicateFile"]
    filemetaaccess = db["FileMetaAccess"]

    # Clear duplicates and FileMetaAccess each scan (optional for FileMetaAccess)
    duplicatefile.delete_many({})
    # filemetaaccess.delete_many({})            # Uncomment if you want to reset each scan

    scan_time = datetime.now(timezone.utc)
    filemeta_batch = []
    # duplicate_map = {}
    duplicate_batch = []
    filemetaaccess_batch = []
    now = scan_time

    for dirpath, _, filenames in os.walk(FILE_ROOT):
        for fname in filenames:
            full_path = os.path.join(dirpath, fname)
            try:
                stat = os.stat(full_path)
            except Exception as e:
                print("Stat error:", full_path, e)
                continue

            extension = os.path.splitext(fname)[1]
            size_bytes = stat.st_size
            fingerprint = get_fingerprint(fname, extension, size_bytes)
            file_id = sha1_hash(full_path)
            last_accessed_at = datetime.fromtimestamp(stat.st_atime, tz=timezone.utc)
            first_accessed_at = last_accessed_at  # If no prior, this is the first

            meta_doc = {
                "fullPath": full_path,
                "fileName": fname,
                "extension": extension,
                "sizeBytes": size_bytes,
                "modifiedAt": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc),
                "fileAccessedAt": last_accessed_at,
                "fileCreatedAt": datetime.fromtimestamp(stat.st_ctime, tz=timezone.utc),
                "scannedAt": scan_time,
                "fingerprint": fingerprint,
            }

            filemeta_batch.append(meta_doc)

            # ---- DUPLICATE LOGIC ----
            if fingerprint not in duplicate_map:
                duplicate_map[fingerprint] = [meta_doc]
            else:
                group = duplicate_map[fingerprint]
                group.append(meta_doc)

                if len(group) == 2:
                    duplicate_batch.append({
                        "fingerprint": fingerprint,
                        "count": 2,
                        "files": [strip_large_fields(d) for d in group],
                        "detectedAt": scan_time,
                    })
                else:
                    duplicate_batch[-1]["count"] += 1
                    duplicate_batch[-1]["files"].append(strip_large_fields(meta_doc))

                if len(duplicate_batch) >= DUPLICATE_BATCH_SIZE:
                    duplicatefile.insert_many(duplicate_batch)
                    print(f"Inserted {len(duplicate_batch)} duplicate batches!")
                    duplicate_batch.clear()

            # ---- FILEMETA BATCH ----
            if len(filemeta_batch) >= FILEMETA_BATCH_SIZE:
                filemeta.insert_many(filemeta_batch)
                print(f"Inserted {len(filemeta_batch)} FileMeta docs!")
                filemeta_batch.clear()

            # ---- FILEMETAACCESS: UPSERT HOT/WARM/COLD ACCESS ----
            access_category = classify_access(last_accessed_at, now)
            update_result = filemetaaccess.update_one(
                {"fileId": file_id},
                {
                    "$inc": {"accessCount": 1},
                    "$setOnInsert": {
                        "firstAccessedAt": first_accessed_at,
                        "fullPath": full_path,
                        "fileName": fname,
                        "extension": extension,
                        "sizeBytes": size_bytes,
                    },
                    "$set": {
                        "lastAccessedAt": last_accessed_at,
                        "accessCategory": access_category,
                        "updatedAt": now,
                    },
                },
                upsert=True
            )

    if filemeta_batch:
        filemeta.insert_many(filemeta_batch)
        print(f"Inserted final {len(filemeta_batch)} FileMeta docs!")

    if duplicate_batch:
        duplicatefile.insert_many(duplicate_batch)
        print(f"Inserted final {len(duplicate_batch)} duplicate batches!")

    print("✅ Scan finished at", scan_time)
    print("👉 Database:", db.name)
    print("👉 Collections:", db.list_collection_names())
    client.close()

if __name__ == "__main__":
    scan_and_insert()
