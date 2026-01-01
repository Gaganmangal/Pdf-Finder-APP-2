################################## Working Code But Slow ##################################
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

################################## Working Code But Slow ##################################


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


# import os
# import hashlib
# import pymongo
# from datetime import datetime, timezone
# from pymongo import UpdateOne, InsertOne
# from concurrent.futures import ProcessPoolExecutor

# # ================= CONFIG =================
# MONGO_URI = "mongodb+srv://Gaganfnr:ndLz9yHCsOmv9S3k@gagan.jhuti8y.mongodb.net/test?appName=Gagan"
# ROOT_PATH = "/mnt/pdfs"
# BATCH_SIZE = 2000
# MAX_WORKERS = 4   # Windows + Disk safe
# # ==========================================


# def sha1(val: str) -> str:
#     return hashlib.sha1(val.encode("utf-8")).hexdigest()


# def scan_subfolder(folder_path: str, scan_id: str) -> int:
#     """One worker scans one top-level folder (FAST, no logic)"""
#     client = pymongo.MongoClient(MONGO_URI)
#     db = client.test

#     raw_ops, latest_ops = [], []
#     local_count = 0
#     now = datetime.now(timezone.utc)

#     stack = [folder_path]

#     while stack:
#         current = stack.pop()
#         try:
#             with os.scandir(current) as it:
#                 for entry in it:
#                     if entry.is_dir(follow_symlinks=False):
#                         stack.append(entry.path)

#                     elif entry.is_file(follow_symlinks=False):
#                         st = entry.stat()
#                         path = entry.path

#                         file_name = entry.name
#                         extension = os.path.splitext(file_name)[1].lower()
#                         parent_dir = os.path.dirname(path)

#                         file_id = sha1(path)

#                         # 🔑 Fingerprint for duplicate detection (post-scan)
#                         fingerprint = f"{file_name.lower()}|{st.st_size}"

#                         # created = datetime.fromtimestamp(st.st_ctime, tz=timezone.utc)
#                         modified = datetime.fromtimestamp(st.st_mtime, tz=timezone.utc)

#                         # -------- RAW (append only) --------
#                         raw_ops.append(InsertOne({
#                             "scanId": scan_id,
#                             "fileId": file_id,
#                             "fullPath": path,
#                             "parentDir": parent_dir,
#                             "fileName": file_name,
#                             "extension": extension,
#                             "sizeBytes": st.st_size,
#                             "fingerprint": fingerprint,
#                             # "createdAt": created,
#                             "modifiedAt": modified,
#                             "scannedAt": now
#                         }))

#                         # -------- LATEST (current state) --------
#                         latest_ops.append(UpdateOne(
#                             {"fileId": file_id},
#                             {
#                                 "$set": {
#                                     "fullPath": path,
#                                     "parentDir": parent_dir,
#                                     "fileName": file_name,
#                                     "extension": extension,
#                                     "sizeBytes": st.st_size,
#                                     "fingerprint": fingerprint,
#                                     # "createdAt": created,
#                                     "modifiedAt": modified,
#                                     "updatedAt": now
#                                 }
#                             },
#                             upsert=True
#                         ))

#                         local_count += 1

#                         # -------- BULK FLUSH --------
#                         if len(raw_ops) >= BATCH_SIZE:
#                             db.FileMetaRaw.bulk_write(raw_ops, ordered=False)
#                             db.FileMetaLatest.bulk_write(latest_ops, ordered=False)
#                             raw_ops.clear()
#                             latest_ops.clear()

#         except (PermissionError, OSError):
#             continue

#     # -------- FINAL FLUSH --------
#     if raw_ops:
#         db.FileMetaRaw.bulk_write(raw_ops, ordered=False)
#         db.FileMetaLatest.bulk_write(latest_ops, ordered=False)

#     client.close()
#     return local_count


# def main():
#     print("🚀 Starting FAST parallel scan (no logic)...")

#     scan_id = sha1(str(datetime.now(timezone.utc)))

#     try:
#         top_folders = [
#             f.path for f in os.scandir(ROOT_PATH) if f.is_dir()
#         ]
#     except Exception as e:
#         print(f"❌ Cannot read root folder: {e}")
#         return

#     if not top_folders:
#         print("⚠️ No subfolders found to scan.")
#         return

#     total_files = 0

#     with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
#         results = executor.map(
#             scan_subfolder,
#             top_folders,
#             [scan_id] * len(top_folders)
#         )
#         total_files = sum(results)

#     print("✅ Scan completed successfully")
#     print(f"📦 Total files scanned: {total_files}")
#     print(f"🆔 Scan ID: {scan_id}")


# if __name__ == "__main__":
#     main()


# import os
# import hashlib
# import pymongo
# from datetime import datetime, timezone

# # ================= CONFIG =================
# MONGO_URI = "mongodb+srv://Gaganfnr:ndLz9yHCsOmv9S3k@gagan.jhuti8y.mongodb.net/test?appName=Gagan"
# ROOT_PATH = "/mnt/pdfs"
# # ==========================================

# def sha1(val: str) -> str:
#     return hashlib.sha1(val.encode("utf-8")).hexdigest()

# def scan():
#     client = pymongo.MongoClient(MONGO_URI)
#     db = client.test
#     latest = db.FileMetaLatest

#     now = datetime.now(timezone.utc)
#     total = 0

#     stack = [ROOT_PATH]

#     while stack:
#         current = stack.pop()
#         try:
#             with os.scandir(current) as it:
#                 for entry in it:
#                     if entry.is_dir(follow_symlinks=False):
#                         stack.append(entry.path)

#                     elif entry.is_file(follow_symlinks=False):
#                         st = entry.stat()
#                         path = entry.path

#                         doc = {
#                             "fileId": sha1(path),
#                             "fullPath": path,
#                             "parentDir": os.path.dirname(path),
#                             "fileName": entry.name,
#                             "extension": os.path.splitext(entry.name)[1].lower(),
#                             "sizeBytes": st.st_size,
#                             "createdAt": datetime.fromtimestamp(st.st_ctime, tz=timezone.utc),
#                             "modifiedAt": datetime.fromtimestamp(st.st_mtime, tz=timezone.utc),
#                             "accessedAt": datetime.fromtimestamp(st.st_atime, tz=timezone.utc),
#                             "updatedAt": now
#                         }

#                         latest.update_one(
#                             {"fileId": doc["fileId"]},
#                             {"$set": doc},
#                             upsert=True
#                         )

#                         total += 1

#         except (PermissionError, OSError):
#             continue

#     client.close()
#     print(f"✅ Scan completed. Files indexed: {total}")

# if __name__ == "__main__":
#     scan()

################################## Working Code For Fast Scan But No logic ##################################

# import os
# import hashlib
# import pymongo
# from datetime import datetime, timezone
# from pymongo import UpdateOne

# # ================= CONFIG =================
# MONGO_URI = "mongodb+srv://Gaganfnr:ndLz9yHCsOmv9S3k@gagan.jhuti8y.mongodb.net/test?appName=Gagan"
# ROOT_PATH = "/mnt/pdfs"

# BATCH_SIZE = 5000        # sweet spot for MongoDB
# LOG_EVERY = 100_000      # progress log
# # ==========================================


# def sha1(val: str) -> str:
#     return hashlib.sha1(val.encode("utf-8")).hexdigest()


# def scan():
#     client = pymongo.MongoClient(
#         MONGO_URI,
#         maxPoolSize=20,
#         serverSelectionTimeoutMS=5000,
#         socketTimeoutMS=600000,
#     )

#     db = client.test
#     latest = db.FileMetaLatest

#     # MUST index (run once, safe if exists)
#     latest.create_index("fileId", unique=True)

#     now = datetime.now(timezone.utc)
#     ops = []
#     total = 0

#     stack = [ROOT_PATH]

#     print("🚀 Starting 80TB scan...")

#     while stack:
#         current = stack.pop()

#         try:
#             with os.scandir(current) as it:
#                 for entry in it:
#                     if entry.is_dir(follow_symlinks=False):
#                         stack.append(entry.path)
#                         continue

#                     if not entry.is_file(follow_symlinks=False):
#                         continue

#                     st = entry.stat()
#                     path = entry.path

#                     doc = {
#                         "fileId": sha1(path),
#                         "fullPath": path,
#                         "parentDir": os.path.dirname(path),
#                         "fileName": entry.name,
#                         "extension": os.path.splitext(entry.name)[1].lower(),
#                         "sizeBytes": st.st_size,
#                         "createdAt": datetime.fromtimestamp(st.st_ctime, tz=timezone.utc),
#                         "modifiedAt": datetime.fromtimestamp(st.st_mtime, tz=timezone.utc),

#                         # 🔑 ACCESS TIME = SCAN TIME (reliable)
#                         "accessedAt": now,

#                         "updatedAt": now,
#                     }

#                     ops.append(
#                         UpdateOne(
#                             {"fileId": doc["fileId"]},
#                             {"$set": doc},
#                             upsert=True,
#                         )
#                     )

#                     total += 1

#                     # ---------- BULK WRITE ----------
#                     if len(ops) >= BATCH_SIZE:
#                         latest.bulk_write(ops, ordered=False)
#                         ops.clear()

#                     if total % LOG_EVERY == 0:
#                         print(f"📂 Files scanned: {total:,}")

#         except (PermissionError, OSError):
#             continue

#     # ---------- FINAL FLUSH ----------
#     if ops:
#         latest.bulk_write(ops, ordered=False)

#     client.close()

#     print("✅ Scan completed")
#     print(f"📦 Total files indexed: {total:,}")


# if __name__ == "__main__":
#     scan()

################################## Working Code For Fast Scan But No logic ##################################





################################## Working Code For Very Fast Scan But No logic ##################################

# import os
# import hashlib
# import pymongo
# from datetime import datetime, timezone
# from pymongo import UpdateOne
# from concurrent.futures import ProcessPoolExecutor

# # ================= CONFIG =================
# MONGO_URI = "mongodb+srv://Gaganfnr:ndLz9yHCsOmv9S3k@gagan.jhuti8y.mongodb.net/test?appName=Gagan&compressors=zlib&maxPoolSize=50"
# ROOT_PATH = "/mnt/pdfs"
# BATCH_SIZE = 5000  
# MAX_WORKERS = 8    # Jitne aapke CPU cores hain (Ubuntu par 'nproc' se check karein)
# # ==========================================

# def sha1(val: str) -> str:
#     return hashlib.sha1(val.encode("utf-8")).hexdigest()

# def scan_branch(folder_path):
#     """Har worker ek sub-folder ko scan karega"""
#     client = pymongo.MongoClient(MONGO_URI)
#     db = client.test
#     latest = db.FileMetaLatest

#     # 80TB scale par Index check yahan zaruri hai
#     latest.create_index("fileId", unique=True)

#     ops = []
#     local_count = 0
#     now = datetime.now(timezone.utc)
#     stack = [folder_path]

#     while stack:
#         current = stack.pop()
#         try:
#             with os.scandir(current) as it:
#                 for entry in it:
#                     if entry.is_dir(follow_symlinks=False):
#                         stack.append(entry.path)
#                         continue

#                     if entry.is_file(follow_symlinks=False):
#                         st = entry.stat()
#                         path = entry.path
#                         f_id = sha1(path)

#                         # Compact document structure for high speed
#                         doc = {
#                             "fileId": f_id,
#                             "fullPath": path,
#                             "fileName": entry.name,
#                             "sizeBytes": st.st_size,
#                             "modifiedAt": datetime.fromtimestamp(st.st_mtime, tz=timezone.utc),
#                             "updatedAt": now,
#                             "accessCount": {"$inc": 1}, # Pattern tracking integrated
#                             "lastAccessedAt": now
#                         }

#                         # Hum UpdateOne use kar rahe hain with $set and $inc
#                         ops.append(UpdateOne(
#                             {"fileId": f_id},
#                             {
#                                 "$set": {
#                                     "fullPath": path,
#                                     "fileName": entry.name,
#                                     "sizeBytes": st.st_size,
#                                     "modifiedAt": doc["modifiedAt"],
#                                     "updatedAt": now,
#                                     "lastAccessedAt": now
#                                 },
#                                 "$inc": {"accessCount": 1},
#                                 "$setOnInsert": {"firstSeenAt": now}
#                             },
#                             upsert=True
#                         ))

#                         local_count += 1
#                         if len(ops) >= BATCH_SIZE:
#                             latest.bulk_write(ops, ordered=False)
#                             ops.clear()

#         except (PermissionError, OSError):
#             continue

#     if ops:
#         latest.bulk_write(ops, ordered=False)

#     client.close()
#     return local_count

# def main():
#     print(f"🚀 Starting Multi-Core Turbo Scan...")

#     # Root ke top-level folders ko list karein taaki workers ko kaam baant sakein
#     try:
#         branches = [f.path for f in os.scandir(ROOT_PATH) if f.is_dir()]
#         if not branches: branches = [ROOT_PATH] # Agar koi subfolder nahi hai
#     except Exception as e:
#         print(f"Error: {e}"); return

#     total_files = 0
#     with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
#         results = list(executor.map(scan_branch, branches))
#         total_files = sum(results)

#     print(f"✅ Completed. Total: {total_files:,} files indexed.")

# if __name__ == "__main__":
#     main()

################################## Working Code For Very Fast Scan But No logic ##################################

# import os
# import hashlib
# import pymongo
# from datetime import datetime, timezone
# from pymongo import UpdateOne
# from concurrent.futures import ProcessPoolExecutor

# # ================= CONFIG =================
# MONGO_URI = "mongodb+srv://Gaganfnr:ndLz9yHCsOmv9S3k@gagan.jhuti8y.mongodb.net/test?appName=Gagan&compressors=zlib&maxPoolSize=50"
# ROOT_PATH = "D:/PDF"
# BATCH_SIZE = 5000
# MAX_WORKERS = 8
# # ==========================================

# def sha1(val: str) -> str:
#     return hashlib.sha1(val.encode("utf-8")).hexdigest()

# # ---------- SCAN WORKER ----------
# def scan_branch(folder_path: str) -> int:
#     client = pymongo.MongoClient(MONGO_URI)
#     db = client.test
#     latest = db.FileMetaLatest

#     ops = []
#     count = 0
#     now = datetime.now(timezone.utc)
#     stack = [folder_path]

#     while stack:
#         current = stack.pop()
#         try:
#             with os.scandir(current) as it:
#                 for entry in it:
#                     if entry.is_dir(follow_symlinks=False):
#                         stack.append(entry.path)
#                         continue

#                     if entry.is_file(follow_symlinks=False):
#                         st = entry.stat()
#                         path = entry.path
#                         file_id = sha1(path)

#                         fingerprint = f"{entry.name.lower()}|{st.st_size}"

#                         ops.append(UpdateOne(
#                             {"fileId": file_id},
#                             {
#                                 "$set": {
#                                     "fileId": file_id,
#                                     "fullPath": path,
#                                     "fileName": entry.name,
#                                     "sizeBytes": st.st_size,
#                                     "fingerprint": fingerprint,
#                                     "modifiedAt": datetime.fromtimestamp(
#                                         st.st_mtime, tz=timezone.utc
#                                     ),
#                                     "updatedAt": now
#                                 }
#                             },
#                             upsert=True
#                         ))

#                         count += 1

#                         if len(ops) >= BATCH_SIZE:
#                             latest.bulk_write(ops, ordered=False)
#                             ops.clear()

#         except (PermissionError, OSError):
#             continue

#     if ops:
#         latest.bulk_write(ops, ordered=False)

#     client.close()
#     return count

# # ---------- DUPLICATE DETECTION ----------
# def run_duplicate_detection(db):
#     print("🔍 Running duplicate detection...")

#     pipeline = [
#         {
#             "$group": {
#                 "_id": "$fingerprint",
#                 "count": {"$sum": 1},
#                 "files": {
#                     "$push": {
#                         "fileId": "$fileId",
#                         "fullPath": "$fullPath",
#                         "sizeBytes": "$sizeBytes",
#                         "modifiedAt": "$modifiedAt"
#                     }
#                 }
#             }
#         },
#         {"$match": {"count": {"$gt": 1}}},
#         {
#             "$project": {
#                 "_id": 0,
#                 "fingerprint": "$_id",
#                 "count": 1,
#                 "files": 1,
#                 "detectedAt": datetime.now(timezone.utc)
#             }
#         },
#         {
#             "$merge": {
#                 "into": "DuplicateFiles",
#                 "whenMatched": "replace",
#                 "whenNotMatched": "insert"
#             }
#         }
#     ]

#     db.FileMetaLatest.aggregate(pipeline, allowDiskUse=True)
#     print("✅ Duplicate detection completed")

# # ---------- MAIN ----------
# def main():
#     start = datetime.now()
#     print(f"🚀 Scan started at {start.strftime('%H:%M:%S')}")

#     try:
#         branches = [f.path for f in os.scandir(ROOT_PATH) if f.is_dir()]
#         if not branches:
#             branches = [ROOT_PATH]
#     except Exception as e:
#         print(f"❌ Error reading root: {e}")
#         return

#     with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
#         total_files = sum(executor.map(scan_branch, branches))

#     print(f"✅ Scan finished. Files indexed: {total_files:,}")

#     # Run duplicate detection AFTER scan
#     client = pymongo.MongoClient(MONGO_URI)
#     db = client.test
#     run_duplicate_detection(db)
#     client.close()

#     duration = datetime.now() - start
#     print(f"⏱ Total time: {duration}")

# if __name__ == "__main__":
#     main()


# import os
# import hashlib
# import pymongo
# import time
# from datetime import datetime, timezone
# from pymongo import UpdateOne
# from pymongo.errors import DuplicateKeyError
# from concurrent.futures import ProcessPoolExecutor

# # ================= CONFIG =================
# MONGO_URI = "mongodb+srv://Gaganfnr:ndLz9yHCsOmv9S3k@gagan.jhuti8y.mongodb.net/test?appName=Gagan&compressors=zlib&maxPoolSize=100"
# ROOT_PATH = "/mnt/pdfs"
# BATCH_SIZE = 3000   # Optimized for network/disk balance
# MAX_WORKERS = 8     # Match with 'nproc' output
# # ==========================================

# def sha1(val: str) -> str:
#     return hashlib.sha1(val.encode("utf-8")).hexdigest()

# def scan_branch(folder_path):
#     client = pymongo.MongoClient(MONGO_URI)
#     db = client.test
#     col_latest = db.FileMetaLatest
#     col_dup_index = db.DuplicateIndex
#     col_dup_files = db.DuplicateFiles

#     # Ensure Indexes (Safe to call multiple times)
#     col_dup_index.create_index("fingerprint", unique=True)
#     col_dup_files.create_index("fingerprint", unique=True)

#     ops_latest = []
#     local_count = 0
#     now = datetime.now(timezone.utc)
#     stack = [folder_path]

#     while stack:
#         current = stack.pop()
#         try:
#             with os.scandir(current) as it:
#                 for entry in it:
#                     if entry.is_dir(follow_symlinks=False):
#                         stack.append(entry.path)
#                         continue

#                     if entry.is_file(follow_symlinks=False):
#                         st = entry.stat()
#                         path = entry.path
#                         f_name = entry.name
#                         f_size = st.st_size
#                         fingerprint = f"{f_name.lower()}|{f_size}"
#                         f_id = sha1(path)

#                         # --- STEP 1: REAL-TIME DUPLICATE DETECTION (RAM SAFE) ---
#                         try:
#                             # Insert fingerprint as a 'Master' record
#                             col_dup_index.insert_one({
#                                 "fingerprint": fingerprint,
#                                 "firstPath": path,
#                                 "createdAt": now
#                             })
#                         except DuplicateKeyError:
#                             # If already exists, find the first seen path
#                             idx = col_dup_index.find_one({"fingerprint": fingerprint}, {"firstPath": 1})

#                             if idx and idx["firstPath"] != path:
#                                 # Add to Duplicate collection only if paths differ
#                                 col_dup_files.update_one(
#                                     {"fingerprint": fingerprint},
#                                     {
#                                         "$setOnInsert": {"createdAt": now},
#                                         "$addToSet": {"files": {"$each": [idx["firstPath"], path]}},
#                                         "$set": {"updatedAt": now}
#                                     },
#                                     upsert=True
#                                 )

#                         # --- STEP 2: METADATA UPDATE ---
#                         ops_latest.append(UpdateOne(
#                             {"fileId": f_id},
#                             {"$set": {
#                                 "fullPath": path,
#                                 "fileName": f_name,
#                                 "sizeBytes": f_size,
#                                 "fingerprint": fingerprint,
#                                 "modifiedAt": datetime.fromtimestamp(st.st_mtime, tz=timezone.utc),
#                                 "updatedAt": now
#                             }},
#                             upsert=True
#                         ))

#                         local_count += 1
#                         if len(ops_latest) >= BATCH_SIZE:
#                             col_latest.bulk_write(ops_latest, ordered=False)
#                             ops_latest.clear()

#         except (PermissionError, OSError):
#             continue

#     if ops_latest:
#         col_latest.bulk_write(ops_latest, ordered=False)

#     client.close()
#     return local_count

# def main():
#     start_time = time.time()
#     print(f"🚀 Initializing Parallel Scan...")

#     try:
#         branches = [f.path for f in os.scandir(ROOT_PATH) if f.is_dir()]
#         if not branches: branches = [ROOT_PATH]
#     except Exception as e:
#         print(f"❌ Root path error: {e}"); return

#     print(f"📂 Found {len(branches)} branch points. Deploying {MAX_WORKERS} workers...")

#     total_files = 0
#     with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
#         # Use list() to wait for results and sum them up
#         results = list(executor.map(scan_branch, branches))
#         total_files = sum(results)

#     duration = time.time() - start_time
#     print("\n" + "="*40)
#     print("✅ SCAN COMPLETE")
#     print(f"⏱️ Total Time: {duration:.2f} seconds")
#     print(f"📦 Files Processed: {total_files:,}")
#     if duration > 0:
#         print(f"⚡ Average Speed: {int(total_files/duration):,} files/sec")
#     print("="*40)

# if __name__ == "__main__":
#     main()














































# import os
# import hashlib
# import pymongo
# from datetime import datetime, timezone
# from pymongo import UpdateOne
# from concurrent.futures import ProcessPoolExecutor

# # ================= CONFIG =================
# MONGO_URI = "mongodb+srv://Gaganfnr:ndLz9yHCsOmv9S3k@gagan.jhuti8y.mongodb.net/test?appName=Gagan&compressors=zlib&maxPoolSize=50"
# ROOT_PATH = "/mnt/pdfs"
# BATCH_SIZE = 5000
# MAX_WORKERS = 8
# # ==========================================

# def sha1(val: str) -> str:
#     return hashlib.sha1(val.encode("utf-8")).hexdigest()

# # ---------- SCAN WORKER ----------
# def scan_branch(folder_path):
#     client = pymongo.MongoClient(MONGO_URI)
#     db = client.test
#     latest = db.FileMetaLatest

#     latest.create_index("fileId", unique=True)
#     latest.create_index("fingerprint")

#     ops = []
#     local_count = 0
#     now = datetime.now(timezone.utc)
#     stack = [folder_path]

#     while stack:
#         current = stack.pop()
#         try:
#             with os.scandir(current) as it:
#                 for entry in it:
#                     if entry.is_dir(follow_symlinks=False):
#                         stack.append(entry.path)
#                         continue

#                     if entry.is_file(follow_symlinks=False):
#                         st = entry.stat()
#                         path = entry.path
#                         file_id = sha1(path)

#                         fingerprint = f"{entry.name.lower()}|{st.st_size}"

#                         ops.append(UpdateOne(
#                             {"fileId": file_id},
#                             {
#                                 "$set": {
#                                     "fileId": file_id,
#                                     "fullPath": path,
#                                     "fileName": entry.name,
#                                     "sizeBytes": st.st_size,
#                                     "fingerprint": fingerprint,
#                                     "filemodifiedAt": datetime.fromtimestamp(
#                                         st.st_mtime, tz=timezone.utc
#                                     ),
#                                     "fileaccessedAt": datetime.fromtimestamp(
#                                         st.st_atime, tz=timezone.utc
#                                     ),
#                                     "updatedAt": now
#                                 },
#                                 "$setOnInsert": {"firstSeenAt": now}
#                             },
#                             upsert=True
#                         ))

#                         local_count += 1

#                         if len(ops) >= BATCH_SIZE:
#                             latest.bulk_write(ops, ordered=False)
#                             ops.clear()

#         except (PermissionError, OSError):
#             continue

#     if ops:
#         latest.bulk_write(ops, ordered=False)

#     client.close()
#     return local_count


# # ---------- DUPLICATE DETECTION ----------
# def run_duplicate_detection(db):
#     print("🔍 Finding duplicates...")

#     pipeline = [
#         {
#             "$group": {
#                 "_id": "$fingerprint",
#                 "count": {"$sum": 1},
#                 "files": {
#                     "$push": {
#                         "fullPath": "$fullPath",
#                         "sizeBytes": "$sizeBytes"
#                     }
#                 }
#             }
#         },
#         {"$match": {"count": {"$gt": 1}}},
#         {
#             "$project": {
#                 "_id": "$_id",
#                 "fingerprint": "$_id",
#                 "count": 1,
#                 "files": 1,
#                 "detectedAt": datetime.now(timezone.utc)
#             }
#         },
#         {
#             "$merge": {
#                 "into": "DuplicateFiles",
#                 "whenMatched": "replace",
#                 "whenNotMatched": "insert"
#             }
#         }
#     ]

#     db.FileMetaLatest.aggregate(pipeline, allowDiskUse=True)
#     print("✅ Duplicate detection completed")


# # ---------- MAIN ----------
# def main():
#     print("🚀 Starting fast scan...")

#     try:
#         branches = [f.path for f in os.scandir(ROOT_PATH) if f.is_dir()]
#         if not branches:
#             branches = [ROOT_PATH]
#     except Exception as e:
#         print(f"❌ Error reading root: {e}")
#         return

#     with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
#         total_files = sum(executor.map(scan_branch, branches))

#     print(f"✅ Scan done. Files indexed: {total_files:,}")

#     # Run duplicate detection AFTER scan
#     client = pymongo.MongoClient(MONGO_URI)
#     db = client.test
#     run_duplicate_detection(db)
#     client.close()

#     print("🏁 All tasks completed.")


# if __name__ == "__main__":
#     main()





























# import os
# import hashlib
# import pymongo
# from datetime import datetime, timezone
# from pymongo import UpdateOne
# from concurrent.futures import ProcessPoolExecutor

# # ================= CONFIG =================
# MONGO_URI = "mongodb+srv://Gaganfnr:ndLz9yHCsOmv9S3k@gagan.jhuti8y.mongodb.net/test?appName=Gagan&compressors=zlib&maxPoolSize=50"
# ROOT_PATH = "/mnt/pdfs"
# BATCH_SIZE = 5000
# MAX_WORKERS = 8 
# # ==========================================

# def sha1(val: str) -> str:
#     return hashlib.sha1(val.encode("utf-8")).hexdigest()

# # ---------- SCAN WORKER ----------
# def scan_branch(folder_path):
#     client = pymongo.MongoClient(MONGO_URI)
#     db = client.test
#     latest = db.FileMetaLatest

#     ops = []
#     local_count = 0
#     now = datetime.now(timezone.utc)
#     stack = [folder_path]

#     while stack:
#         current = stack.pop()
#         try:
#             with os.scandir(current) as it:
#                 for entry in it:
#                     if entry.is_dir(follow_symlinks=False):
#                         stack.append(entry.path)
#                         continue

#                     if entry.is_file(follow_symlinks=False):
#                         st = entry.stat()
#                         path = entry.path
#                         file_id = sha1(path)
#                         fingerprint = f"{entry.name.lower()}|{st.st_size}"

#                         ops.append(UpdateOne(
#                             {"fileId": file_id},
#                             {
#                                 "$set": {
#                                     "fullPath": path,
#                                     "fileName": entry.name,
#                                     "sizeBytes": st.st_size,
#                                     "fingerprint": fingerprint,
#                                     "modifiedAt": datetime.fromtimestamp(st.st_mtime, tz=timezone.utc),
#                                     "lastAccessedAt": datetime.fromtimestamp(st.st_atime, tz=timezone.utc),
#                                     "updatedAt": now
#                                 },
#                                 "$setOnInsert": {"firstSeenAt": now}
#                             },
#                             upsert=True
#                         ))

#                         local_count += 1
#                         if len(ops) >= BATCH_SIZE:
#                             latest.bulk_write(ops, ordered=False)
#                             ops.clear()

#         except (PermissionError, OSError):
#             continue

#     if ops:
#         latest.bulk_write(ops, ordered=False)

#     client.close()
#     return local_count

# # ---------- DUPLICATE DETECTION ----------
# def run_duplicate_detection(db):
#     print("🔍 Aggregating duplicates (this may take time)...")

#     pipeline = [
#         # Optimize: Sirf zaruri data memory mein lein
#         {"$project": {"fingerprint": 1, "fullPath": 1, "sizeBytes": 1}},
#         {
#             "$group": {
#                 "_id": "$fingerprint",
#                 "count": {"$sum": 1},
#                 "files": {
#                     "$push": {
#                         "fullPath": "$fullPath",
#                         "sizeBytes": "$sizeBytes"
#                     }
#                 }
#             }
#         },
#         {"$match": {"count": {"$gt": 1}}},
#         {
#             "$project": {
#                 "_id": "$_id",
#                 "fingerprint": "$_id",
#                 "count": 1,
#                 "files": 1,
#                 "detectedAt": datetime.now(timezone.utc)
#             }
#         },
#         {
#             "$merge": {
#                 "into": "DuplicateFiles",
#                 "whenMatched": "replace",
#                 "whenNotMatched": "insert"
#             }
#         }
#     ]

#     db.FileMetaLatest.aggregate(pipeline, allowDiskUse=True)
#     print("✅ Duplicate detection completed. Data in 'DuplicateFiles' collection.")

# #---------- File Access Pattern ----------
# def run_file_access_pattern(db):
#     print("📊 Building FileMetaAccess (USER-centric access pattern)...")

#     now = datetime.now(timezone.utc)

#     pipeline = [
#         # 1️⃣ Pick required fields from FileMetaLatest
#         {
#             "$project": {
#                 "fileId": 1,
#                 "fullPath": 1,
#                 "firstSeenAt": 1,
#                 "lastScanAt": "$updatedAt",
#                 "osAccessedAt": "$lastAccessedAt"
#             }
#         },

#         # 2️⃣ Handle missing / null osAccessedAt
#         # If OS access not available, fallback to firstSeenAt
#         {
#             "$addFields": {
#                 "effectiveUserAccessAt": {
#                     "$cond": [
#                         {"$ifNull": ["$osAccessedAt", False]},
#                         "$osAccessedAt",
#                         "$firstSeenAt"
#                     ]
#                 }
#             }
#         },

#         # 3️⃣ Calculate days since USER last access
#         {
#             "$addFields": {
#                 "daysSinceUserAccess": {
#                     "$divide": [
#                         {"$subtract": [now, "$effectiveUserAccessAt"]},
#                         1000 * 60 * 60 * 24
#                     ]
#                 }
#             }
#         },

#         # 4️⃣ Classify based on USER freshness
#         {
#             "$addFields": {
#                 "accessClass": {
#                     "$switch": {
#                         "branches": [
#                             {
#                                 "case": {"$lte": ["$daysSinceUserAccess", 7]},
#                                 "then": "HOT"
#                             },
#                             {
#                                 "case": {"$lte": ["$daysSinceUserAccess", 30]},
#                                 "then": "WARM"
#                             }
#                         ],
#                         "default": "COLD"
#                     }
#                 }
#             }
#         },

#         # 5️⃣ Final shape for FileMetaAccess
#         {
#             "$project": {
#                 "_id": "$fileId",
#                 "fileId": 1,
#                 "fullPath": 1,

#                 "firstSeenAt": 1,
#                 "lastScanAt": 1,

#                 "osAccessedAt": 1,
#                 "effectiveUserAccessAt": 1,
#                 "daysSinceUserAccess": 1,

#                 "accessClass": 1,
#                 "updatedAt": now
#             }
#         },

#         # 6️⃣ Upsert into FileMetaAccess
#         {
#             "$merge": {
#                 "into": "FileMetaAccess",
#                 "whenMatched": "replace",
#                 "whenNotMatched": "insert"
#             }
#         }
#     ]

#     db.FileMetaLatest.aggregate(pipeline, allowDiskUse=True)
#     print("✅ FileMetaAccess (user-centric) updated successfully.")

# #---------- Global Cleanup ----------
# def run_global_cleanup(db, scan_start_time, dry_run=True):
#     """
#     Deletes records of files that were NOT seen in this scan
#     from ALL related collections.

#     dry_run=True  -> sirf counts show karega
#     dry_run=False -> actual delete karega (DANGEROUS)
#     """

#     print("🧹 Running GLOBAL cleanup across all collections...")

#     # 1️⃣ Find stale files from FileMetaLatest
#     stale_cursor = db.FileMetaLatest.find(
#         {"updatedAt": {"$lt": scan_start_time}},
#         {"fileId": 1, "fullPath": 1, "fingerprint": 1}
#     )

#     stale = list(stale_cursor)
#     if not stale:
#         print("✅ No stale files found. Cleanup skipped.")
#         return 0

#     file_ids = [d["fileId"] for d in stale]
#     paths = [d["fullPath"] for d in stale]
#     fingerprints = [d["fingerprint"] for d in stale if "fingerprint" in d]

#     print(f"⚠️ Found {len(file_ids):,} stale files.")

#     if dry_run:
#         print("🔎 DRY RUN counts:")
#         print(" FileMetaLatest   :", len(file_ids))
#         print(" FileMetaAccess   :", db.FileMetaAccess.count_documents({"fileId": {"$in": file_ids}}))
#         print(" DuplicateFiles   :", db.DuplicateFiles.count_documents({"files.fullPath": {"$in": paths}}))
#         return len(file_ids)

#     # 2️⃣ REAL DELETE (use carefully)
#     r1 = db.FileMetaLatest.delete_many({"fileId": {"$in": file_ids}})
#     r2 = db.FileMetaAccess.delete_many({"fileId": {"$in": file_ids}})

#     # DuplicateFiles cleanup (partial update)
#     db.DuplicateFiles.update_many(
#         {},
#         {"$pull": {"files": {"fullPath": {"$in": paths}}}},
#     )
#     # 2️⃣ Recalculate count = files.length (aggregation update)
#     db.DuplicateFiles.update_many(
#     {},
#     [
#         {
#             "$set": {
#                 "count": { "$size": "$files" }
#             }
#         }
#     ]
#     )

#     # 3️⃣ Remove invalid duplicate groups (count < 2)
#     db.DuplicateFiles.delete_many({ "count": { "$lt": 2 } })

#     print("✅ Cleanup completed:")
#     print(f" FileMetaLatest deleted : {r1.deleted_count}")
#     print(f" FileMetaAccess deleted : {r2.deleted_count}")
#     print(f" DuplicateFiles cleaned : paths removed")

#     return r1.deleted_count

# #---------- Trend Daily Summary ----------
# def build_trend_daily_summary(db, scan_start_time, scan_end_time):
#     now = datetime.now(timezone.utc)
#     date_key = now.strftime("%Y-%m-%d")

#     total_files = db.FileMetaLatest.count_documents({})
#     total_size = db.FileMetaLatest.aggregate([
#         {"$group": {"_id": None, "size": {"$sum": "$sizeBytes"}}}
#     ]).next()["size"]

#     access_stats = list(db.FileMetaAccess.aggregate([
#         {
#             "$group": {
#                 "_id": "$accessClass",
#                 "count": {"$sum": 1}
#             }
#         }
#     ]))

#     access_map = {x["_id"]: x["count"] for x in access_stats}

#     duplicate_groups = db.DuplicateFiles.count_documents({})
#     duplicate_files = db.DuplicateFiles.aggregate([
#         {"$unwind": "$files"},
#         {"$count": "count"}
#     ]).next()["count"] if duplicate_groups else 0

#     db.TrendDailySummary.update_one(
#         {"_id": date_key},
#         {"$set": {
#             "date": date_key,
#             "totalFiles": total_files,
#             "totalSizeBytes": total_size,

#             "hotFiles": access_map.get("HOT", 0),
#             "warmFiles": access_map.get("WARM", 0),
#             "coldFiles": access_map.get("COLD", 0),

#             "duplicateGroups": duplicate_groups,
#             "duplicateFiles": duplicate_files,

#             "scanDurationSec": int((scan_end_time - scan_start_time).total_seconds()),
#             "createdAt": now
#         }},
#         upsert=True
#     )

# #---------- Trend Folder Heatmap ----------
# def build_folder_heatmap(db):
#     pipeline = [
#         {
#             # Split fullPath into array by "/"
#             "$addFields": {
#                 "pathParts": { "$split": ["$fullPath", "/"] }
#             }
#         },
#         {
#             # Remove last element (file name)
#             "$addFields": {
#                 "folder": {
#                     "$reduce": {
#                         "input": {
#                             "$slice": [
#                                 "$pathParts",
#                                 0,
#                                 { "$subtract": [ { "$size": "$pathParts" }, 1 ] }
#                             ]
#                         },
#                         "initialValue": "",
#                         "in": {
#                             "$cond": [
#                                 { "$eq": ["$$value", ""] },
#                                 "$$this",
#                                 { "$concat": ["$$value", "/", "$$this"] }
#                             ]
#                         }
#                     }
#                 }
#             }
#         },
#         {
#             "$group": {
#                 "_id": "$folder",
#                 "totalFiles": { "$sum": 1 },
#                 "hotFiles": {
#                     "$sum": { "$cond": [{ "$eq": ["$accessClass", "HOT"] }, 1, 0] }
#                 },
#                 "warmFiles": {
#                     "$sum": { "$cond": [{ "$eq": ["$accessClass", "WARM"] }, 1, 0] }
#                 },
#                 "coldFiles": {
#                     "$sum": { "$cond": [{ "$eq": ["$accessClass", "COLD"] }, 1, 0] }
#                 }
#             }
#         },
#         {
#             "$project": {
#                 "_id": "$_id",
#                 "folder": "$_id",
#                 "totalFiles": 1,
#                 "hotFiles": 1,
#                 "warmFiles": 1,
#                 "coldFiles": 1,
#                 "updatedAt": datetime.now(timezone.utc)
#             }
#         },
#         {
#             "$merge": {
#                 "into": "TrendFolderHeatmap",
#                 "whenMatched": "replace",
#                 "whenNotMatched": "insert"
#             }
#         }
#     ]

#     db.FileMetaAccess.aggregate(pipeline, allowDiskUse=True)
#     print("✅ TrendFolderHeatmap updated successfully.")
# # ---------- MAIN ----------
# def main():
#     print(f"🚀 Starting fast scan on {ROOT_PATH}...")
#     start_time = datetime.now()
#     scan_start_time = datetime.now(timezone.utc)

#     # Setup Indices BEFORE starting workers
#     client = pymongo.MongoClient(MONGO_URI)
#     db = client.test
#     db.FileMetaLatest.create_index("fileId", unique=True)
#     db.FileMetaLatest.create_index("fingerprint")
#     client.close()

#     try:
#         branches = [f.path for f in os.scandir(ROOT_PATH) if f.is_dir()]
#         if not branches:
#             branches = [ROOT_PATH]
#     except Exception as e:
#         print(f"❌ Error reading root: {e}")
#         return

#     with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
#         total_files = sum(executor.map(scan_branch, branches))

#     client = pymongo.MongoClient(MONGO_URI)
#     # Duplicate detection process
#     run_duplicate_detection(client.test)
#     # NEW: Access pattern generation
#     run_file_access_pattern(client.test)
#     # Always DRY RUN first
#     # run_global_cleanup(client.test, scan_start_time, dry_run=True)
#     run_global_cleanup(client.test, scan_start_time, dry_run=False)
#     # 🔹 BUILD TRENDS (AFTER EVERYTHING)
#     scan_end_time = datetime.now(timezone.utc)
#     build_trend_daily_summary(client.test, scan_start_time, scan_end_time)
#     build_folder_heatmap(client.test)
#     client.close()

#     duration = datetime.now() - start_time
#     print(f"\n✅ All tasks completed in {duration}")
#     print(f"📦 Total files indexed: {total_files:,}")

# if __name__ == "__main__":
#     main()






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
MAX_WORKERS = 8 
# ==========================================

def sha1(val: str) -> str:
    return hashlib.sha1(val.encode("utf-8")).hexdigest()

# ---------- SCAN WORKER ----------
def scan_branch(folder_path):
    client = pymongo.MongoClient(MONGO_URI)
    db = client.test
    latest = db.FileMetaLatest

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
                        file_id = sha1(path)
                        fingerprint = f"{entry.name.lower()}|{st.st_size}"

                        ops.append(UpdateOne(
                            {"fileId": file_id},
                            {
                                "$set": {
                                    "fullPath": path,
                                    "fileName": entry.name,
                                    "sizeBytes": st.st_size,
                                    "fingerprint": fingerprint,
                                    "modifiedAt": datetime.fromtimestamp(st.st_mtime, tz=timezone.utc),
                                    "lastAccessedAt": datetime.fromtimestamp(st.st_atime, tz=timezone.utc),
                                    "updatedAt": now
                                },
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

# ---------- DUPLICATE DETECTION ----------
def run_duplicate_detection(db):
    print("🔍 Aggregating duplicates (Memory Efficient Mode)...")

    pipeline = [
        # Step 1: Filter only necessary fields immediately to reduce document size
        {"$project": {"fingerprint": 1, "fullPath": 1, "sizeBytes": 1}},
        
        # Step 2: Sort by fingerprint (Requires index for performance)
        {"$sort": {"fingerprint": 1}}, 
        
        # Step 3: Group duplicates
        {
            "$group": {
                "_id": "$fingerprint",
                "count": {"$sum": 1},
                "files": {
                    "$push": {
                        "fullPath": "$fullPath",
                        "sizeBytes": "$sizeBytes"
                    }
                }
            }
        },
        # Filter only actual duplicates
        {"$match": {"count": {"$gt": 1}}},
        
        # Step 4: Final formatting
        {
            "$project": {
                "_id": 0,
                "fingerprint": "$_id",
                "count": 1,
                "files": 1,
                "detectedAt": datetime.now(timezone.utc)
            }
        },
        # Step 5: Save results
        {
            "$merge": {
                "into": "DuplicateFiles",
                "whenMatched": "replace",
                "whenNotMatched": "insert"
            }
        }
    ]

    db.FileMetaLatest.aggregate(pipeline, allowDiskUse=True)
    print("✅ Duplicate detection completed.")

#---------- File Access Pattern ----------
def run_file_access_pattern(db):
    print("📊 Building FileMetaAccess (USER-centric access pattern)...")

    now = datetime.now(timezone.utc)

    pipeline = [
        # 1️⃣ Pick required fields from FileMetaLatest
        {
            "$project": {
                "fileId": 1,
                "fullPath": 1,
                "firstSeenAt": 1,
                "lastScanAt": "$updatedAt",
                "osAccessedAt": "$lastAccessedAt"
            }
        },

        # 2️⃣ Handle missing / null osAccessedAt
        # If OS access not available, fallback to firstSeenAt
        {
            "$addFields": {
                "effectiveUserAccessAt": {
                    "$cond": [
                        {"$ifNull": ["$osAccessedAt", False]},
                        "$osAccessedAt",
                        "$firstSeenAt"
                    ]
                }
            }
        },

        # 3️⃣ Calculate days since USER last access
        {
            "$addFields": {
                "daysSinceUserAccess": {
                    "$divide": [
                        {"$subtract": [now, "$effectiveUserAccessAt"]},
                        1000 * 60 * 60 * 24
                    ]
                }
            }
        },

        # 4️⃣ Classify based on USER freshness
        {
            "$addFields": {
                "accessClass": {
                    "$switch": {
                        "branches": [
                            {
                                "case": {"$lte": ["$daysSinceUserAccess", 7]},
                                "then": "HOT"
                            },
                            {
                                "case": {"$lte": ["$daysSinceUserAccess", 30]},
                                "then": "WARM"
                            }
                        ],
                        "default": "COLD"
                    }
                }
            }
        },

        # 5️⃣ Final shape for FileMetaAccess
        {
            "$project": {
                "_id": "$fileId",
                "fileId": 1,
                "fullPath": 1,

                "firstSeenAt": 1,
                "lastScanAt": 1,

                "osAccessedAt": 1,
                "effectiveUserAccessAt": 1,
                "daysSinceUserAccess": 1,

                "accessClass": 1,
                "updatedAt": now
            }
        },

        # 6️⃣ Upsert into FileMetaAccess
        {
            "$merge": {
                "into": "FileMetaAccess",
                "whenMatched": "replace",
                "whenNotMatched": "insert"
            }
        }
    ]

    db.FileMetaLatest.aggregate(pipeline, allowDiskUse=True)
    print("✅ FileMetaAccess (user-centric) updated successfully.")

#---------- Global Cleanup ----------
def run_global_cleanup(db, scan_start_time, dry_run=True):
    """
    Deletes records of files that were NOT seen in this scan
    from ALL related collections.

    dry_run=True  -> sirf counts show karega
    dry_run=False -> actual delete karega (DANGEROUS)
    """

    print("🧹 Running GLOBAL cleanup across all collections...")

    # 1️⃣ Find stale files from FileMetaLatest
    stale_cursor = db.FileMetaLatest.find(
        {"updatedAt": {"$lt": scan_start_time}},
        {"fileId": 1, "fullPath": 1, "fingerprint": 1}
    )

    stale = list(stale_cursor)
    if not stale:
        print("✅ No stale files found. Cleanup skipped.")
        return 0

    file_ids = [d["fileId"] for d in stale]
    paths = [d["fullPath"] for d in stale]
    fingerprints = [d["fingerprint"] for d in stale if "fingerprint" in d]

    print(f"⚠️ Found {len(file_ids):,} stale files.")

    if dry_run:
        print("🔎 DRY RUN counts:")
        print(" FileMetaLatest   :", len(file_ids))
        print(" FileMetaAccess   :", db.FileMetaAccess.count_documents({"fileId": {"$in": file_ids}}))
        print(" DuplicateFiles   :", db.DuplicateFiles.count_documents({"files.fullPath": {"$in": paths}}))
        return len(file_ids)

    # 2️⃣ REAL DELETE (use carefully)
    r1 = db.FileMetaLatest.delete_many({"fileId": {"$in": file_ids}})
    r2 = db.FileMetaAccess.delete_many({"fileId": {"$in": file_ids}})

    # DuplicateFiles cleanup (partial update)
    db.DuplicateFiles.update_many(
        {},
        {"$pull": {"files": {"fullPath": {"$in": paths}}}},
    )
    # 2️⃣ Recalculate count = files.length (aggregation update)
    db.DuplicateFiles.update_many(
    {},
    [
        {
            "$set": {
                "count": { "$size": "$files" }
            }
        }
    ]
    )

    # 3️⃣ Remove invalid duplicate groups (count < 2)
    db.DuplicateFiles.delete_many({ "count": { "$lt": 2 } })

    print("✅ Cleanup completed:")
    print(f" FileMetaLatest deleted : {r1.deleted_count}")
    print(f" FileMetaAccess deleted : {r2.deleted_count}")
    print(f" DuplicateFiles cleaned : paths removed")

    return r1.deleted_count

#---------- Trend Daily Summary ----------
def build_trend_daily_summary(db, scan_start_time, scan_end_time):
    now = datetime.now(timezone.utc)
    date_key = now.strftime("%Y-%m-%d")

    total_files = db.FileMetaLatest.count_documents({})
    total_size = db.FileMetaLatest.aggregate([
        {"$group": {"_id": None, "size": {"$sum": "$sizeBytes"}}}
    ]).next()["size"]

    access_stats = list(db.FileMetaAccess.aggregate([
        {
            "$group": {
                "_id": "$accessClass",
                "count": {"$sum": 1}
            }
        }
    ]))

    access_map = {x["_id"]: x["count"] for x in access_stats}

    duplicate_groups = db.DuplicateFiles.count_documents({})
    duplicate_files = db.DuplicateFiles.aggregate([
        {"$unwind": "$files"},
        {"$count": "count"}
    ]).next()["count"] if duplicate_groups else 0

    db.TrendDailySummary.update_one(
        {"_id": date_key},
        {"$set": {
            "date": date_key,
            "totalFiles": total_files,
            "totalSizeBytes": total_size,

            "hotFiles": access_map.get("HOT", 0),
            "warmFiles": access_map.get("WARM", 0),
            "coldFiles": access_map.get("COLD", 0),

            "duplicateGroups": duplicate_groups,
            "duplicateFiles": duplicate_files,

            "scanDurationSec": int((scan_end_time - scan_start_time).total_seconds()),
            "createdAt": now
        }},
        upsert=True
    )

#---------- Trend Folder Heatmap ----------
def build_folder_heatmap(db):
    print("🌡️ Building Folder Heatmap (Memory Efficient)...")
    
    pipeline = [
        # Extract folder using regex instead of split/reduce for better performance
        {
            "$project": {
                "accessClass": 1,






                "folder": {
                    "$trim": {
                        "input": {
                            "$replaceAll": {
                                "input": "$fullPath",
                                "find": r"[^/]+$", # Matches everything after the last slash
                                "replacement": ""
                            }
                        },
                        "chars": "/"







                    }
                }
            }
        },
        {
            "$group": {
                "_id": "$folder",
                "totalFiles": { "$sum": 1 },
                "hotFiles": { "$sum": { "$cond": [{ "$eq": ["$accessClass", "HOT"] }, 1, 0] } },
                "warmFiles": { "$sum": { "$cond": [{ "$eq": ["$accessClass", "WARM"] }, 1, 0] } },
                "coldFiles": { "$sum": { "$cond": [{ "$eq": ["$accessClass", "COLD"] }, 1, 0] } }






            }
        },
        { "$merge": { "into": "TrendFolderHeatmap", "whenMatched": "replace" } }

















    ]

    db.FileMetaAccess.aggregate(pipeline, allowDiskUse=True)

# ---------- MAIN ----------
def main():
    print(f"🚀 Starting fast scan on {ROOT_PATH}...")
    start_time = datetime.now()
    scan_start_time = datetime.now(timezone.utc)

    # Setup Indices BEFORE starting workers
    client = pymongo.MongoClient(MONGO_URI)
    db = client.test
    db.FileMetaLatest.create_index("fileId", unique=True)
    db.FileMetaLatest.create_index("fingerprint")
    db.FileMetaLatest.create_index("fullPath")
    client.close()

    try:
        branches = [f.path for f in os.scandir(ROOT_PATH) if f.is_dir()]
        if not branches:
            branches = [ROOT_PATH]
    except Exception as e:
        print(f"❌ Error reading root: {e}")
        return

    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        total_files = sum(executor.map(scan_branch, branches))

    client = pymongo.MongoClient(MONGO_URI)
    # Duplicate detection process
    run_duplicate_detection(client.test)
    # NEW: Access pattern generation
    run_file_access_pattern(client.test)
    # Always DRY RUN first
    # run_global_cleanup(client.test, scan_start_time, dry_run=True)
    run_global_cleanup(client.test, scan_start_time, dry_run=False)
    # 🔹 BUILD TRENDS (AFTER EVERYTHING)
    scan_end_time = datetime.now(timezone.utc)
    build_trend_daily_summary(client.test, scan_start_time, scan_end_time)
    build_folder_heatmap(client.test)
    client.close()

    duration = datetime.now() - start_time
    print(f"\n✅ All tasks completed in {duration}")
    print(f"📦 Total files indexed: {total_files:,}")

if __name__ == "__main__":
    main()