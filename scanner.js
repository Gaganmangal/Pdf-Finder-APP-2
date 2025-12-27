const fs = require("fs");
const path = require("path");
const FileMeta = require("./models/FileMeta");

// Batch size for MongoDB bulk inserts (optimized for performance)
const BATCH_SIZE = 1000;

// Global counters for progress tracking
let totalFilesScanned = 0;
let totalDirsScanned = 0;
let batchBuffer = [];

async function flushBatch() {
  if (batchBuffer.length === 0) return;

  try {
    // Use insertMany for better performance (faster than bulkWrite for pure inserts)
    await FileMeta.insertMany(batchBuffer, { ordered: false });
    batchBuffer = [];
  } catch (err) {
    batchBuffer = []; // Clear buffer on error to prevent memory issues
  }
}

async function scanDirectory(dir, drive = "D") {
  let items;
  let fileCount = 0;
  let dirCount = 0;

  try {
    items = fs.readdirSync(dir);
  } catch (err) {
    // Silently skip directories we can't read (permissions, etc.)
    return { fileCount: 0, dirCount: 0 };
  }

  for (const item of items) {
    const fullPath = path.join(dir, item);

    let stat;
    try {
      stat = fs.statSync(fullPath);
    } catch {
      continue; // Skip files we can't stat
    }

    if (stat.isDirectory()) {
      dirCount++;
      totalDirsScanned++;
      // Recursively scan subdirectories
      const result = await scanDirectory(fullPath, drive);
      fileCount += result.fileCount;
      dirCount += result.dirCount;
    } else if (stat.isFile()) {
      try {
        // Get file timestamps - optimized timestamp handling
        let fileCreated = stat.birthtime;
        if (
          !fileCreated ||
          fileCreated.getTime() <= 0 ||
          fileCreated.getTime() > Date.now()
        ) {
          fileCreated = stat.ctime;
        }

        // Build metadata object (no Date conversion here for performance)
        const fileMetadata = {
          fileName: item,
          fullPath,
          folderPath: dir,
          extension: path.extname(item),
          sizeBytes: stat.size,
          sizeMB: +(stat.size / (1024 * 1024)).toFixed(2),
          drive,
          fileCreatedAt: fileCreated,
          modifiedAt: stat.mtime,
          fileAccessedAt: stat.atime,
          scannedAt: new Date(),
        };

        // Add to batch buffer
        batchBuffer.push(fileMetadata);
        fileCount++;
        totalFilesScanned++;

        // Flush batch when it reaches BATCH_SIZE
        if (batchBuffer.length >= BATCH_SIZE) {
          await flushBatch();
        }
      } catch (err) {
        // Silently skip files that cause errors
        continue;
      }
    }
  }

  return { fileCount, dirCount };
}

async function scanDirectoryWithStats(dir, drive = "D") {
  // Reset global counters
  totalFilesScanned = 0;
  totalDirsScanned = 0;
  batchBuffer = [];

  // Start scanning
  const result = await scanDirectory(dir, drive);

  // Flush any remaining items in buffer
  await flushBatch();

  return {
    fileCount: totalFilesScanned,
    dirCount: totalDirsScanned,
  };
}

// Export the optimized scanner function
module.exports = scanDirectoryWithStats;
