# import os
# import pymongo
# import hashlib
# from datetime import datetime, timezone
# from pymongo import UpdateOne, InsertOne
# from pymongo.errors import DuplicateKeyError

# # ================= CONFIG =================
# MONGO_URI = "mongodb+srv://Gaganfnr:ndLz9yHCsOmv9S3k@gagan.jhuti8y.mongodb.net/test?appName=Gagan"
# ROOT_PATH = "/mnt/pdfs"   # change to /mnt/pdfs on EC2
# BATCH_SIZE = 10000
# CHECKPOINT_EVERY = 50000
# # ==========================================


# def sha1(val: str) -> str:
#     return hashlib.sha1(val.encode("utf-8")).hexdigest()


# def classify_access(last_accessed, now):
#     days = (now - last_accessed).days
#     if days <= 30:
#         return "HOT"
#     if days <= 180:
#         return "WARM"
#     return "COLD"


# def load_checkpoint(db):
#     state = db.ScanState.find_one({"_id": "current_scan"})
#     return state["lastPath"] if state else None


# def save_checkpoint(db, path, count):
#     db.ScanState.update_one(
#         {"_id": "current_scan"},
#         {"$set": {
#             "lastPath": path,
#             "filesScanned": count,
#             "updatedAt": datetime.now(timezone.utc)
#         }},
#         upsert=True
#     )


# def scan():
#     client = pymongo.MongoClient(MONGO_URI)
#     db = client.test

#     raw = db.FileMetaRaw
#     latest = db.FileMetaLatest
#     access = db.FileMetaAccess
#     trends = db.FileSystemTrends
#     dup_index = db.DuplicateIndex
#     dup_files = db.DuplicateFiles

#     # Required indexes (safe if already exist)
#     dup_index.create_index("fingerprint", unique=True)
#     latest.create_index("fileId", unique=True)
#     access.create_index("fileId", unique=True)

#     scan_id = sha1(str(datetime.now()))
#     now = datetime.now(timezone.utc)
#     today = now.strftime("%Y-%m-%d")

#     resume_from = load_checkpoint(db)

#     raw_ops = []
#     latest_ops = []
#     access_ops = []

#     counters = {
#         "totalFiles": 0,
#         "totalSizeGB": 0,
#         "HOT": 0,
#         "WARM": 0,
#         "COLD": 0
#     }

#     skipped = resume_from is not None
#     scanned = 0

#     for root, _, files in os.walk(ROOT_PATH):
#         if skipped:
#             if root == resume_from:
#                 skipped = False
#             else:
#                 continue

#         for name in files:
#             path = os.path.join(root, name)

#             try:
#                 st = os.stat(path)
#             except Exception:
#                 continue

#             file_id = sha1(path)
#             ext = os.path.splitext(name)[1]
#             size = st.st_size

#             accessed = datetime.fromtimestamp(st.st_atime, tz=timezone.utc)
#             modified = datetime.fromtimestamp(st.st_mtime, tz=timezone.utc)
#             created = datetime.fromtimestamp(st.st_ctime, tz=timezone.utc)

#             fingerprint = f"{name.lower()}|{ext}|{size}"

#             # ---------- RAW ----------
#             raw_ops.append(InsertOne({
#                 "scanId": scan_id,
#                 "fullPath": path,
#                 "fileName": name,
#                 "extension": ext,
#                 "sizeBytes": size,
#                 "modifiedAt": modified,
#                 "accessedAt": accessed,
#                 "createdAt": created,
#                 "fingerprint": fingerprint,
#                 "scannedAt": now
#             }))

#             # ---------- LATEST ----------
#             latest_ops.append(UpdateOne(
#                 {"fileId": file_id},
#                 {"$set": {
#                     "fullPath": path,
#                     "fileName": name,
#                     "extension": ext,
#                     "sizeBytes": size,
#                     "modifiedAt": modified,
#                     "accessedAt": accessed,
#                     "createdAt": created,
#                     "fingerprint": fingerprint,
#                     "updatedAt": now
#                 }},
#                 upsert=True
#             ))

#             # ---------- ACCESS ----------
#             category = classify_access(accessed, now)
#             access_ops.append(UpdateOne(
#                 {"fileId": file_id},
#                 {
#                     "$inc": {"accessCount": 1},
#                     "$setOnInsert": {
#                         "fullPath": path,
#                         "firstAccessedAt": accessed
#                     },
#                     "$set": {
#                         "lastAccessedAt": accessed,
#                         "accessCategory": category,
#                         "updatedAt": now
#                     }
#                 },
#                 upsert=True
#             ))

#             # ---------- DUPLICATE DETECTION (NO RAM) ----------
#     #         try:
#     #             dup_index.insert_one({
#     #                 "fingerprint": fingerprint,
#     #                 # "count": 1,
#     #                 # "firstFileId": file_id,
#     #                 "firstPath": path,
#     #                 "createdAt": now,
#     #                 "updatedAt": now
#     #             })
#     #         except DuplicateKeyError:
#     #             idx = dup_index.find_one(
#     #                 {"fingerprint": fingerprint}, 
#     #                 {"firstPath": 1}
#     #                 )

#     #             if not idx:
#     #                     continue

#     #             if idx["firstPath"] == path:
#     #                 dup_index.update_one(
#     #                     {
#     #                         "fingerprint": fingerprint}, 
#     #                         {"$set": {"updatedAt": now}
#     #                     }
#     #                 )
#     #                 continue
                    
#     #             dup_files.update_one(
#     #             {"fingerprint": fingerprint},
#     #             {
#     #                 "$setOnInsert": {
#     #                 "fingerprint": fingerprint,
#     #                 "createdAt": now,
#     #             },
#     #                 "$addToSet": {
#     #                     "files": {
#     #                         # # "fileId": file_id, 
#     #                         # "fullPath": path, 
#     #                         # "scannedAt": now
#     #                         "$each": [
#     #                             idx["firstPath"],
#     #                             path  
#     #                         ]
#     #                     }
#     #                 },
#     #             "$set": {
#     #                 "updatedAt": now
#     #                 }
#     #             },
#     #             upsert=True
#     #         )

#     #         dup_index.update_one(
#     #     {"fingerprint": fingerprint},
#     #     {"$set": {"updatedAt": now}}
#     # )

#             # ---------- TRENDS ----------
#             counters["totalFiles"] += 1
#             counters["totalSizeGB"] += size / (1024 ** 3)
#             counters[category] += 1

#             scanned += 1

#             # ---------- BULK FLUSH ----------
#             if scanned % BATCH_SIZE == 0:
#                 raw.bulk_write(raw_ops, ordered=False)
#                 latest.bulk_write(latest_ops, ordered=False)
#                 access.bulk_write(access_ops, ordered=False)
#                 raw_ops.clear()
#                 latest_ops.clear()
#                 access_ops.clear()

#             if scanned % CHECKPOINT_EVERY == 0:
#                 save_checkpoint(db, root, scanned)

#     # ---------- FINAL FLUSH ----------
#     if raw_ops:
#         raw.bulk_write(raw_ops, ordered=False)
#         latest.bulk_write(latest_ops, ordered=False)
#         access.bulk_write(access_ops, ordered=False)

#     # ---------- DAILY TREND ----------
#     trends.update_one(
#         {"date": today},
#         {"$set": {
#             "totalFiles": counters["totalFiles"],
#             "totalSizeGB": round(counters["totalSizeGB"], 2),
#             "hotFiles": counters["HOT"],
#             "warmFiles": counters["WARM"],
#             "coldFiles": counters["COLD"],
#             "updatedAt": now
#         }},
#         upsert=True
#     )

#     print(f"✅ Scan completed: {scanned} files")
#     client.close()


# if __name__ == "__main__":
#     scan()

# import os
# import pymongo
# import hashlib
# from datetime import datetime, timezone
# from pymongo import UpdateOne, InsertOne

# # ================= CONFIG =================
# MONGO_URI = "mongodb+srv://Gaganfnr:ndLz9yHCsOmv9S3k@gagan.jhuti8y.mongodb.net/test?appName=Gagan"
# ROOT_PATH = "C:/affice_Project"
# BATCH_SIZE = 1000
# CHECKPOINT_EVERY = 5000
# # ==========================================


# def sha1(val: str) -> str:
#     return hashlib.sha1(val.encode("utf-8")).hexdigest()


# def classify_access(last_accessed, now):
#     days = (now - last_accessed).days
#     if days <= 30:
#         return "HOT"
#     if days <= 180:
#         return "WARM"
#     return "COLD"


# def load_checkpoint(db):
#     state = db.ScanState.find_one({"_id": "current_scan"})
#     return state["lastPath"] if state else None


# def save_checkpoint(db, path, count):
#     db.ScanState.update_one(
#         {"_id": "current_scan"},
#         {"$set": {
#             "lastPath": path,
#             "filesScanned": count,
#             "updatedAt": datetime.now(timezone.utc)
#         }},
#         upsert=True
#     )


# # -------- FAST SCANDIR WALK ----------
# def scandir_walk(base_path):
#     stack = [base_path]
#     while stack:
#         current = stack.pop()
#         try:
#             with os.scandir(current) as it:
#                 for entry in it:
#                     if entry.is_dir(follow_symlinks=False):
#                         stack.append(entry.path)
#                     elif entry.is_file(follow_symlinks=False):
#                         yield entry.path, entry.stat()
#         except Exception:
#             continue


# def scan():
#     client = pymongo.MongoClient(MONGO_URI)
#     db = client.test

#     raw = db.FileMetaRaw
#     latest = db.FileMetaLatest
#     access = db.FileMetaAccess
#     trends = db.FileSystemTrends

#     # indexes
#     latest.create_index("fileId", unique=True)
#     access.create_index("fileId", unique=True)

#     scan_id = sha1(str(datetime.now()))
#     now = datetime.now(timezone.utc)
#     today = now.strftime("%Y-%m-%d")

#     resume_from = load_checkpoint(db)

#     raw_ops = []
#     latest_ops = []
#     access_ops = []

#     counters = {
#         "totalFiles": 0,
#         "totalSizeGB": 0,
#         "HOT": 0,
#         "WARM": 0,
#         "COLD": 0
#     }

#     scanned = 0
#     resume_passed = resume_from is None

#     for path, st in scandir_walk(ROOT_PATH):

#         if not resume_passed:
#             if path.startswith(resume_from):
#                 resume_passed = True
#             else:
#                 continue

#         file_id = sha1(path)
#         name = os.path.basename(path)
#         ext = os.path.splitext(name)[1]
#         size = st.st_size

#         modified = datetime.fromtimestamp(st.st_mtime, tz=timezone.utc)
#         created = datetime.fromtimestamp(st.st_ctime, tz=timezone.utc)

#         # 🔥 ACCESS TIME CACHE (FAST)
#         old = latest.find_one(
#             {"fileId": file_id},
#             {"accessedAt": 1}
#         )

#         if old and "accessedAt" in old:
#             accessed = old["accessedAt"]
#         else:
#             accessed = datetime.fromtimestamp(st.st_atime, tz=timezone.utc)

#         fingerprint = f"{name.lower()}|{ext}|{size}"

#         # ---------- RAW ----------
#         raw_ops.append(InsertOne({
#             "scanId": scan_id,
#             "fullPath": path,
#             "fileName": name,
#             "extension": ext,
#             "sizeBytes": size,
#             "modifiedAt": modified,
#             "accessedAt": accessed,
#             "createdAt": created,
#             "fingerprint": fingerprint,
#             "scannedAt": now
#         }))

#         # ---------- LATEST ----------
#         latest_ops.append(UpdateOne(
#             {"fileId": file_id},
#             {"$set": {
#                 "fullPath": path,
#                 "fileName": name,
#                 "extension": ext,
#                 "sizeBytes": size,
#                 "modifiedAt": modified,
#                 "accessedAt": accessed,
#                 "createdAt": created,
#                 "fingerprint": fingerprint,
#                 "updatedAt": now
#             }},
#             upsert=True
#         ))

#         # ---------- ACCESS ----------
#         category = classify_access(accessed, now)
#         access_ops.append(UpdateOne(
#             {"fileId": file_id},
#             {
#                 "$inc": {"accessCount": 1},
#                 "$setOnInsert": {
#                     "fullPath": path,
#                     "firstAccessedAt": accessed
#                 },
#                 "$set": {
#                     "lastAccessedAt": accessed,
#                     "accessCategory": category,
#                     "updatedAt": now
#                 }
#             },
#             upsert=True
#         ))

#         counters["totalFiles"] += 1
#         counters["totalSizeGB"] += size / (1024 ** 3)
#         counters[category] += 1
#         scanned += 1

#         # ---------- BULK FLUSH ----------
#         if scanned % BATCH_SIZE == 0:
#             raw.bulk_write(raw_ops, ordered=False)
#             latest.bulk_write(latest_ops, ordered=False)
#             access.bulk_write(access_ops, ordered=False)
#             raw_ops.clear()
#             latest_ops.clear()
#             access_ops.clear()

#         if scanned % CHECKPOINT_EVERY == 0:
#             save_checkpoint(db, path, scanned)

#     # ---------- FINAL FLUSH ----------
#     if raw_ops:
#         raw.bulk_write(raw_ops, ordered=False)
#         latest.bulk_write(latest_ops, ordered=False)
#         access.bulk_write(access_ops, ordered=False)

#     # ---------- DAILY TREND ----------
#     trends.update_one(
#         {"date": today},
#         {"$set": {
#             "totalFiles": counters["totalFiles"],
#             "totalSizeGB": round(counters["totalSizeGB"], 2),
#             "hotFiles": counters["HOT"],
#             "warmFiles": counters["WARM"],
#             "coldFiles": counters["COLD"],
#             "updatedAt": now
#         }},
#         upsert=True
#     )

#     print(f"✅ Scan completed: {scanned} files")
#     client.close()


# if __name__ == "__main__":
#     scan()


import os
import hashlib
import pymongo
from datetime import datetime, timezone
from pymongo import UpdateOne, InsertOne
from concurrent.futures import ProcessPoolExecutor

# ================= CONFIG =================
MONGO_URI = "mongodb+srv://Gaganfnr:ndLz9yHCsOmv9S3k@gagan.jhuti8y.mongodb.net/test?appName=Gagan"
ROOT_PATH = "/mnt/pdfs"
BATCH_SIZE = 2000
MAX_WORKERS = 4   # Windows + Disk safe
# ==========================================


def sha1(val: str) -> str:
    return hashlib.sha1(val.encode("utf-8")).hexdigest()


def scan_subfolder(folder_path: str, scan_id: str) -> int:
    """One worker scans one top-level folder (FAST, no logic)"""
    client = pymongo.MongoClient(MONGO_URI)
    db = client.test

    raw_ops, latest_ops = [], []
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

                    elif entry.is_file(follow_symlinks=False):
                        st = entry.stat()
                        path = entry.path

                        file_name = entry.name
                        extension = os.path.splitext(file_name)[1].lower()
                        parent_dir = os.path.dirname(path)

                        file_id = sha1(path)

                        # 🔑 Fingerprint for duplicate detection (post-scan)
                        fingerprint = f"{file_name.lower()}|{st.st_size}"

                        # created = datetime.fromtimestamp(st.st_ctime, tz=timezone.utc)
                        modified = datetime.fromtimestamp(st.st_mtime, tz=timezone.utc)

                        # -------- RAW (append only) --------
                        raw_ops.append(InsertOne({
                            "scanId": scan_id,
                            "fileId": file_id,
                            "fullPath": path,
                            "parentDir": parent_dir,
                            "fileName": file_name,
                            "extension": extension,
                            "sizeBytes": st.st_size,
                            "fingerprint": fingerprint,
                            # "createdAt": created,
                            "modifiedAt": modified,
                            "scannedAt": now
                        }))

                        # -------- LATEST (current state) --------
                        latest_ops.append(UpdateOne(
                            {"fileId": file_id},
                            {
                                "$set": {
                                    "fullPath": path,
                                    "parentDir": parent_dir,
                                    "fileName": file_name,
                                    "extension": extension,
                                    "sizeBytes": st.st_size,
                                    "fingerprint": fingerprint,
                                    # "createdAt": created,
                                    "modifiedAt": modified,
                                    "updatedAt": now
                                }
                            },
                            upsert=True
                        ))

                        local_count += 1

                        # -------- BULK FLUSH --------
                        if len(raw_ops) >= BATCH_SIZE:
                            db.FileMetaRaw.bulk_write(raw_ops, ordered=False)
                            db.FileMetaLatest.bulk_write(latest_ops, ordered=False)
                            raw_ops.clear()
                            latest_ops.clear()

        except (PermissionError, OSError):
            continue

    # -------- FINAL FLUSH --------
    if raw_ops:
        db.FileMetaRaw.bulk_write(raw_ops, ordered=False)
        db.FileMetaLatest.bulk_write(latest_ops, ordered=False)

    client.close()
    return local_count


def main():
    print("🚀 Starting FAST parallel scan (no logic)...")

    scan_id = sha1(str(datetime.now(timezone.utc)))

    try:
        top_folders = [
            f.path for f in os.scandir(ROOT_PATH) if f.is_dir()
        ]
    except Exception as e:
        print(f"❌ Cannot read root folder: {e}")
        return

    if not top_folders:
        print("⚠️ No subfolders found to scan.")
        return

    total_files = 0

    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = executor.map(
            scan_subfolder,
            top_folders,
            [scan_id] * len(top_folders)
        )
        total_files = sum(results)

    print("✅ Scan completed successfully")
    print(f"📦 Total files scanned: {total_files}")
    print(f"🆔 Scan ID: {scan_id}")


if __name__ == "__main__":
    main()
