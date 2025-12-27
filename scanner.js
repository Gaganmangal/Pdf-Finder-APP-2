const fs = require("fs");
const path = require("path");
const FileMeta = require("./models/FileMeta");

// Batch size for MongoDB bulk inserts (optimized for performance)
const BATCH_SIZE = 1000;
const LOG_INTERVAL = 100; // Log progress every N files

// Global counters for progress tracking
let totalFilesScanned = 0;
let totalDirsScanned = 0;
let batchBuffer = [];
let lastLogTime = Date.now();

async function flushBatch() {
  if (batchBuffer.length === 0) return;

  try {
    // Use bulkWrite for better performance
    const operations = batchBuffer.map((doc) => ({
      insertOne: { document: doc },
    }));

    await FileMeta.bulkWrite(operations, { ordered: false });
    batchBuffer = [];
  } catch (err) {
    console.error(`❌ Batch insert error:`, err.message);
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

        // Log progress periodically (not every file)
        if (totalFilesScanned % LOG_INTERVAL === 0) {
          const now = Date.now();
          const elapsed = (now - lastLogTime) / 1000;
          const rate = LOG_INTERVAL / elapsed;
          const totalMB = batchBuffer.reduce(
            (sum, f) => sum + (f.sizeMB || 0),
            0
          );

          console.log(
            `📊 Progress: ${totalFilesScanned} files scanned | ${totalDirsScanned} dirs | ${rate.toFixed(1)} files/sec | Rate: ${(totalMB / elapsed).toFixed(2)} MB/s`
          );
          lastLogTime = now;
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
  lastLogTime = Date.now();

  const startTime = Date.now();
  console.log(`\n🚀 Starting scan of: ${dir}`);
  console.log(`📦 Batch size: ${BATCH_SIZE} files per insert`);

  // Start scanning
  const result = await scanDirectory(dir, drive);

  // Flush any remaining items in buffer
  await flushBatch();

  const endTime = Date.now();
  const duration = ((endTime - startTime) / 1000).toFixed(2);
  const filesPerSec = (totalFilesScanned / (duration || 1)).toFixed(2);

  console.log(`\n✅ Scan completed!`);
  console.log(`📊 Final Stats:`);
  console.log(`   Files scanned: ${totalFilesScanned}`);
  console.log(`   Directories scanned: ${totalDirsScanned}`);
  console.log(`   Duration: ${duration}s`);
  console.log(`   Speed: ${filesPerSec} files/sec`);

  return {
    fileCount: totalFilesScanned,
    dirCount: totalDirsScanned,
    duration: parseFloat(duration),
  };
}

// Export the optimized scanner function
module.exports = scanDirectoryWithStats;
