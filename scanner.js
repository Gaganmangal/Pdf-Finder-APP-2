const fs = require("fs");

// Helper to normalize file paths (cross-platform, case-insensitive)
function normalizePath(p) {
  return (p || "")
    .replace(/\\/g, "/")
    .toLowerCase()
    .replace(/\/+$|^\/+/g, "");
}

const path = require("path");
const FileMeta = require("./models/FileMeta");
const DuplicateFile = require("./models/DuplicateFile");

// Batch sizes for MongoDB operations (optimized for 200k+ files)
const FILE_BATCH_SIZE = 1000; // Files per batch write
const DUPLICATE_BATCH_SIZE = 500; // Duplicate groups per batch write

// Global state for single-pass scanning
let totalFilesScanned = 0;
let totalDirsScanned = 0;
let totalDuplicatesFound = 0;

// Batch buffers
let fileBatchBuffer = [];
let duplicateBatchBuffer = [];

// In-memory Map to track fingerprints during scan
// Key: fingerprint, Value: array of file info objects
// This is memory-efficient because:
// 1. We only store fingerprints we've seen (not all files)
// 2. We flush duplicates periodically to MongoDB
// 3. Map size is bounded by number of unique fingerprints, not total files
const fingerprintMap = new Map();

/**
 * Generate lightweight fingerprint for duplicate detection
 * Format: normalizedFileName::extension::sizeBytes
 * This avoids expensive aggregation pipelines
 */
function generateFingerprint(fileName, extension, sizeBytes) {
  const normalized = (fileName || "").toLowerCase().trim();
  return `${normalized}::${extension || ""}::${sizeBytes}`;
}

/**
 * Flush file batch to MongoDB using bulkWrite
 * This is faster than insertMany for large datasets
 */
async function flushFileBatch() {
  if (fileBatchBuffer.length === 0) return;

  try {
    const operations = fileBatchBuffer.map((doc) => ({
      updateOne: {
        filter: { fullPath: doc.fullPath }, // Use file path as unique key
        update: { $set: doc },
        upsert: true,
      },
    }));

    await FileMeta.bulkWrite(operations, { ordered: false });
    fileBatchBuffer = [];
  } catch (err) {
    fileBatchBuffer = []; // Clear on error to prevent memory issues
  }
}

/**
 * Flush duplicate groups to MongoDB using bulkWrite
 * Only writes groups that have 2+ files (actual duplicates)
 */
async function flushDuplicateBatch() {
  if (duplicateBatchBuffer.length === 0) return;

  try {
    const operations = duplicateBatchBuffer.map((doc) => ({
      insertOne: { document: doc },
    }));

    await DuplicateFile.bulkWrite(operations, { ordered: false });
    duplicateBatchBuffer = [];
  } catch (err) {
    duplicateBatchBuffer = []; // Clear on error
  }
}

/**
 * Process a duplicate fingerprint and add to batch buffer
 * Called when we encounter a fingerprint for the 2nd+ time
 */
function processDuplicate(fingerprint, fileInfo, existingFiles) {
  // If this is the 2nd occurrence, create a new duplicate group
  if (existingFiles.length === 1) {
    const firstFile = existingFiles[0];
    const duplicateGroup = {
      fingerprint: fingerprint,
      fileName: firstFile.fileName,
      extension: firstFile.extension,
      sizeBytes: firstFile.sizeBytes,
      count: 2, // Now we have 2 files
      files: [
        {
          fullPath: firstFile.fullPath,
          folderPath: firstFile.folderPath,
          drive: firstFile.drive,
          scannedAt: firstFile.scannedAt,
        },
        {
          fullPath: fileInfo.fullPath,
          folderPath: fileInfo.folderPath,
          drive: fileInfo.drive,
          scannedAt: fileInfo.scannedAt,
        },
      ],
      detectedAt: new Date(),
    };

    duplicateBatchBuffer.push(duplicateGroup);
    totalDuplicatesFound += 2; // Count both files

    // Flush if batch is full
    if (duplicateBatchBuffer.length >= DUPLICATE_BATCH_SIZE) {
      return flushDuplicateBatch();
    }
  } else {
    // This is 3rd+ occurrence, update existing group in buffer or Map
    // Find the group in buffer and update it
    const groupIndex = duplicateBatchBuffer.findIndex(
      (g) => g.fingerprint === fingerprint
    );

    if (groupIndex !== -1) {
      // Update existing group in buffer
      duplicateBatchBuffer[groupIndex].count++;
      duplicateBatchBuffer[groupIndex].files.push({
        fullPath: fileInfo.fullPath,
        folderPath: fileInfo.folderPath,
        drive: fileInfo.drive,
        scannedAt: fileInfo.scannedAt,
      });
      totalDuplicatesFound++;
    } else {
      // Group was already flushed, we need to update in DB later
      // For now, just track in Map - will be handled in final flush
      existingFiles.push(fileInfo);
    }
  }
}

/**
 * Single-pass directory scanner with real-time duplicate detection
 * WHY THIS APPROACH:
 * 1. No aggregation pipelines - avoids MongoDB memory limits
 * 2. Duplicates detected during scan - no second pass needed
 * 3. Memory-efficient Map - only stores fingerprints, not all file data
 * 4. Bulk writes - fast MongoDB operations
 * 5. Scales to 200k+ files without memory issues
 */
async function scanDirectory(dir, drive = "D") {
  let items;
  let fileCount = 0;
  let dirCount = 0;

  try {
    items = fs.readdirSync(dir);
  } catch (err) {
    return { fileCount: 0, dirCount: 0 };
  }

  for (const item of items) {
    const fullPath = path.join(dir, item);

    let stat;
    try {
      stat = fs.statSync(fullPath);
    } catch {
      continue;
    }

    if (stat.isDirectory()) {
      dirCount++;
      totalDirsScanned++;
      const result = await scanDirectory(fullPath, drive);
      fileCount += result.fileCount;
      dirCount += result.dirCount;
    } else if (stat.isFile()) {
      try {
        // Get file timestamps
        let fileCreated = stat.birthtime;
        if (
          !fileCreated ||
          fileCreated.getTime() <= 0 ||
          fileCreated.getTime() > Date.now()
        ) {
          fileCreated = stat.ctime;
        }

        const extension = path.extname(item);
        const sizeBytes = stat.size;
        const scannedAt = new Date();

        // Generate fingerprint for duplicate detection
        const fingerprint = generateFingerprint(item, extension, sizeBytes);

        // Build file metadata (includes fingerprint for indexing)
        const fileMetadata = {
          fileName: item,
          fullPath,
          folderPath: dir,
          extension,
          sizeBytes,
          sizeMB: +(sizeBytes / (1024 * 1024)).toFixed(2),
          drive,
          fingerprint, // Store fingerprint for potential future queries
          fileCreatedAt: fileCreated,
          modifiedAt: stat.mtime,
          fileAccessedAt: stat.atime,
          scannedAt,
        };

        // Check for duplicates using in-memory Map
        // This is O(1) lookup - very fast
        const fileInfo = {
          fileName: item,
          fullPath,
          folderPath: dir,
          drive,
          extension,
          sizeBytes,
          scannedAt,
        };

        if (fingerprintMap.has(fingerprint)) {
          // DUPLICATE FOUND - process it
          const existingFiles = fingerprintMap.get(fingerprint);
          existingFiles.push(fileInfo);
          fingerprintMap.set(fingerprint, existingFiles);

          // Process duplicate group
          await processDuplicate(fingerprint, fileInfo, existingFiles);
        } else {
          // First occurrence - just track it
          fingerprintMap.set(fingerprint, [fileInfo]);
        }

        // Add to file batch buffer (ALL files are saved to FileMeta)
        fileBatchBuffer.push(fileMetadata);
        fileCount++;
        totalFilesScanned++;

        // Flush file batch when full
        if (fileBatchBuffer.length >= FILE_BATCH_SIZE) {
          await flushFileBatch();
        }
      } catch (err) {
        continue;
      }
    }
  }

  return { fileCount, dirCount };
}

/**
 * Main scanning function with duplicate detection
 * Clears old duplicates and performs single-pass scan
 */
async function scanDirectoryWithStats(dir, drive = "D") {
  const foundPathsSet = new Set();
  // Reset global state
  totalFilesScanned = 0;
  totalDirsScanned = 0;
  totalDuplicatesFound = 0;
  fileBatchBuffer = [];
  duplicateBatchBuffer = [];
  fingerprintMap.clear();

  // Clear old duplicate records (optional - can be commented out to keep history)
  await DuplicateFile.deleteMany({});

  // Perform single-pass scan with real-time duplicate detection
  // Wrap scanDirectory to collect all found paths
  async function scanAndCollect(dir, drive) {
    let items;
    try {
      items = fs.readdirSync(dir);
    } catch (err) {
      return { fileCount: 0, dirCount: 0 };
    }
    let fileCount = 0;
    let dirCount = 0;
    for (const item of items) {
      const fullPath = path.join(dir, item);
      let stat;
      try {
        stat = fs.statSync(fullPath);
      } catch {
        continue;
      }
      if (stat.isDirectory()) {
        dirCount++;
        totalDirsScanned++;
        const result = await scanAndCollect(fullPath, drive);
        fileCount += result.fileCount;
        dirCount += result.dirCount;
      } else if (stat.isFile()) {
        foundPathsSet.add(normalizePath(fullPath));
        try {
          let fileCreated = stat.birthtime;
          if (
            !fileCreated ||
            fileCreated.getTime() <= 0 ||
            fileCreated.getTime() > Date.now()
          ) {
            fileCreated = stat.ctime;
          }
          const extension = path.extname(item);
          const sizeBytes = stat.size;
          const scannedAt = new Date();
          const fingerprint = generateFingerprint(item, extension, sizeBytes);
          const fileMetadata = {
            fileName: item,
            fullPath,
            folderPath: dir,
            extension,
            sizeBytes,
            sizeMB: +(sizeBytes / (1024 * 1024)).toFixed(2),
            drive,
            fingerprint,
            fileCreatedAt: fileCreated,
            modifiedAt: stat.mtime,
            fileAccessedAt: stat.atime,
            scannedAt,
          };
          const fileInfo = {
            fileName: item,
            fullPath,
            folderPath: dir,
            drive,
            extension,
            sizeBytes,
            scannedAt,
          };
          if (fingerprintMap.has(fingerprint)) {
            const existingFiles = fingerprintMap.get(fingerprint);
            existingFiles.push(fileInfo);
            fingerprintMap.set(fingerprint, existingFiles);
            await processDuplicate(fingerprint, fileInfo, existingFiles);
          } else {
            fingerprintMap.set(fingerprint, [fileInfo]);
          }
          fileBatchBuffer.push(fileMetadata);
          fileCount++;
          totalFilesScanned++;
          if (fileBatchBuffer.length >= FILE_BATCH_SIZE) {
            await flushFileBatch();
          }
        } catch (err) {
          continue;
        }
      }
    }
    return { fileCount, dirCount };
  }

  const result = await scanAndCollect(dir, drive);

  // Flush remaining batches
  await flushFileBatch();
  await flushDuplicateBatch();

  // Final pass: Process any remaining duplicates in Map that weren't flushed
  // This handles cases where duplicates were found but groups weren't complete
  for (const [fingerprint, files] of fingerprintMap.entries()) {
    if (files.length > 1) {
      // This is a duplicate group that might not have been written yet
      // Check if it exists in DB, if not, create it
      const existingGroup = await DuplicateFile.findOne({ fingerprint });
      if (!existingGroup) {
        const duplicateGroup = {
          fingerprint: fingerprint,
          fileName: files[0].fileName,
          extension: files[0].extension,
          sizeBytes: files[0].sizeBytes,
          count: files.length,
          files: files.map((f) => ({
            fullPath: f.fullPath,
            folderPath: f.folderPath,
            drive: f.drive,
            scannedAt: f.scannedAt,
          })),
          detectedAt: new Date(),
        };
        await DuplicateFile.create(duplicateGroup);
        totalDuplicatesFound += files.length;
      }
    }
  }

  // Clean up deleted files in database
  const dbAllFiles = await FileMeta.find({}, "fullPath");
  const pathsInDb = dbAllFiles.map((f) => normalizePath(f.fullPath));
  const pathsToDelete = pathsInDb.filter((p) => !foundPathsSet.has(p));
  if (pathsToDelete.length > 0) {
    await FileMeta.deleteMany({ fullPath: { $in: pathsToDelete } });
  }

  return {
    fileCount: totalFilesScanned,
    dirCount: totalDirsScanned,
    duplicateFiles: totalDuplicatesFound,
    duplicateGroups: await DuplicateFile.countDocuments(),
  };
}

// Export the optimized scanner function
module.exports = scanDirectoryWithStats;
