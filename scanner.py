import os
import hashlib
import pymongo
from datetime import datetime, timezone
from pymongo import UpdateOne
from concurrent.futures import ProcessPoolExecutor

# ================= CONFIG =================
MONGO_URI = "mongodb://scannerUser:StrongPassword123@localhost:27017/fileScanner?authSource=admin"
ROOT_PATH = "/mnt/pdfs"
BATCH_SIZE = 5000
MAX_WORKERS = 8 
# ==========================================

def sha1(val: str) -> str:
    return hashlib.sha1(val.encode("utf-8")).hexdigest()

# ---------- SCAN WORKER ----------
def scan_branch(folder_path):
    client = pymongo.MongoClient(MONGO_URI)
    db = client.fileScanner
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
    print("Aggregating duplicates (SAFE + SCALABLE)...")

    pipeline = [
        # 1️⃣ Project only required fields
        {
            "$project": {
                "fingerprint": 1,
                "fullPath": 1,
                "sizeBytes": 1
            }
        },

        # 2️⃣ Group by fingerprint (NO SORT)
        {
            "$group": {
                "_id": "$fingerprint",
                "count": { "$sum": 1 },
                "files": {
                    "$push": {
                        "fullPath": "$fullPath",
                        "sizeBytes": "$sizeBytes"
                    }
                }
            }
        },

        # 3️⃣ Keep only duplicates
        { "$match": { "count": { "$gt": 1 } } },

        # 4️⃣ Final shape
        {
            "$project": {
                "_id": "$_id",
                "fingerprint": "$_id",
                "count": 1,
                "files": 1,
                "detectedAt": datetime.now(timezone.utc)
            }
        },

        # 5️⃣ Save
        {
            "$merge": {
                "into": "DuplicateFiles",
                "whenMatched": "replace",
                "whenNotMatched": "insert"
            }
        }
    ]

    db.FileMetaLatest.aggregate(pipeline, allowDiskUse=True)
    print("Duplicate detection completed safely.")


#---------- File Access Pattern ----------
def run_file_access_pattern(db):
    print("Building FileMetaAccess (USER-centric access pattern)...")

    # now = datetime.now(timezone.utc)

    pipeline = [
        # 1️⃣ Pick required fields from FileMetaLatest
        {
            "$project": {
                "fileId": 1,
                "fullPath": 1,
                "firstSeenAt": 1,
                "fileName": 1,          # ✅ added
                "sizeBytes": 1,         # ✅ added
                "modifiedAt": 1,
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
                        {"$ne": ["$osAccessedAt", None]},
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
                    "$floor": {
                    "$divide": [
                        {"$subtract": ["$$NOW", "$effectiveUserAccessAt"]},
                        86400000
                    ]
                }
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
                                "case": {"$lte": ["$daysSinceUserAccess", 30]},
                                "then": "HOT"
                            },
                            {
                                "case": {
                                    "$and": [
                                        { "$gt": ["$daysSinceUserAccess", 30] },
                                        { "$lte": ["$daysSinceUserAccess", 90] }
                                    ]
                                },
                                "then": "WARM"
                            },
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
        
                "fullPath": 1,
                "fileName": 1,          # ✅ stored
                "sizeBytes": 1,         # ✅ stored
                "modifiedAt": 1, 

                "osAccessedAt": 1,
                "effectiveUserAccessAt": 1,
                "daysSinceUserAccess": 1,

                "accessClass": 1,
                "updatedAt": "$$NOW"
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
    print("FileMetaAccess (user-centric) updated successfully.")

#---------- Global Cleanup ----------
def run_global_cleanup(db, scan_start_time, dry_run=True):
    """
    Deletes records of files that were NOT seen in this scan
    from ALL related collections.

    dry_run=True  -> sirf counts show karega
    dry_run=False -> actual delete karega (DANGEROUS)
    """

    print("Running GLOBAL cleanup across all collections...")

    # 1️⃣ Find stale files from FileMetaLatest
    stale_cursor = db.FileMetaLatest.find(
        {"updatedAt": {"$lt": scan_start_time}},
        {"fileId": 1, "fullPath": 1, "fingerprint": 1}
    )

    stale = list(stale_cursor)
    if not stale:
        print("No stale files found. Cleanup skipped.")
        return 0

    file_ids = [d["fileId"] for d in stale]
    paths = [d["fullPath"] for d in stale]
    fingerprints = [d["fingerprint"] for d in stale if "fingerprint" in d]

    print(f"Found {len(file_ids):,} stale files.")

    if dry_run:
        print("DRY RUN counts:")
        print(" FileMetaLatest   :", len(file_ids))
        # print(" FileMetaAccess   :", db.FileMetaAccess.count_documents({"fileId": {"$in": file_ids}}))
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

    print("Cleanup completed:")
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

    # Total duplicate files count (sum of counts)
    dup_count_result = list(db.DuplicateFiles.aggregate([
        {
            "$group": {
                "_id": None,
                "total": {"$sum": "$count"}
            }
        }
    ]))

    duplicate_files = dup_count_result[0]["total"] if dup_count_result else 0

    # Wasted storage size
    wasted_result = list(db.DuplicateFiles.aggregate([
        {
            "$project": {
                "wastedBytes": {
                    "$multiply": [
                        {"$subtract": ["$count", 1]},
                        {"$arrayElemAt": ["$files.sizeBytes", 0]}
                    ]
                }
            }
        },
        {
            "$group": {
                "_id": None,
                "totalWasted": {"$sum": "$wastedBytes"}
            }
        }
    ]))

    duplicate_wasted_size = wasted_result[0]["totalWasted"] if wasted_result else 0

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
            "TotalDuplicateSize": duplicate_wasted_size,

            "scanDurationSec": int((scan_end_time - scan_start_time).total_seconds()),
            "createdAt": now
        }},
        upsert=True
    )

#---------- Trend Folder Heatmap ----------
# def build_folder_heatmap(db):
#     print("🌡️ Building Folder Heatmap (Memory Efficient)...")

#     pipeline = [
#         # Extract folder using regex instead of split/reduce for better performance
#         {
#             "$project": {
#                 "accessClass": 1,
#                 "folder": {
#                     "$trim": {
#                         "input": {
#                             "$replaceAll": {
#                                 "input": "$fullPath",
#                                 "find": r"[^/]+$", # Matches everything after the last slash
#                                 "replacement": ""
#                             }
#                         },
#                         "chars": "/"
#                     }
#                 }
#             }
#         },
#         {
#             "$group": {
#                 "_id": "$folder",
#                 "totalFiles": { "$sum": 1 },
#                 "hotFiles": { "$sum": { "$cond": [{ "$eq": ["$accessClass", "HOT"] }, 1, 0] } },
#                 "warmFiles": { "$sum": { "$cond": [{ "$eq": ["$accessClass", "WARM"] }, 1, 0] } },
#                 "coldFiles": { "$sum": { "$cond": [{ "$eq": ["$accessClass", "COLD"] }, 1, 0] } }
#             }
#         },
#         { "$merge": { "into": "TrendFolderHeatmap", "whenMatched": "replace" } }
#     ]
#     db.FileMetaAccess.aggregate(pipeline, allowDiskUse=True)
# ---------- MAIN ----------
def main():
    print(f"Starting fast scan on {ROOT_PATH}...")
    start_time = datetime.now()
    scan_start_time = datetime.now(timezone.utc)

    # Setup Indices BEFORE starting workers
    client = pymongo.MongoClient(MONGO_URI)
    db = client.fileScanner
    db.FileMetaLatest.create_index("fileId", unique=True)
    db.FileMetaLatest.create_index("fingerprint")
    db.FileMetaLatest.create_index("fullPath")
    db.FileMetaLatest.create_index({ "fingerprint": 1, "fullPath": 1 })
    client.close()

    try:
        branches = [f.path for f in os.scandir(ROOT_PATH) if f.is_dir()]
        if not branches:
            branches = [ROOT_PATH]
    except Exception as e:
        print(f"Error reading root: {e}")
        return

    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        total_files = sum(executor.map(scan_branch, branches))

    client = pymongo.MongoClient(MONGO_URI)
    # Duplicate detection process
    run_duplicate_detection(client.fileScanner)
    # NEW: Access pattern generation
    run_file_access_pattern(client.fileScanner)
    # Always DRY RUN first
    # run_global_cleanup(client.fileScanner, scan_start_time, dry_run=True)
    run_global_cleanup(client.fileScanner, scan_start_time, dry_run=True)
    # 🔹 BUILD TRENDS (AFTER EVERYTHING)
    scan_end_time = datetime.now(timezone.utc)
    build_trend_daily_summary(client.fileScanner, scan_start_time, scan_end_time)
    # build_folder_heatmap(client.fileScanner)
    client.close()

    duration = datetime.now() - start_time
    print(f"\nAll tasks completed in {duration}")
    print(f"Total files indexed: {total_files:,}")

if __name__ == "__main__":
    main()
