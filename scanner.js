const fs = require("fs");
const path = require("path");
const FileMeta = require("./models/FileMeta");

async function scanDirectory(dir, drive = "D") {
  let items;
  let fileCount = 0;

  try {
    items = fs.readdirSync(dir);
  } catch {
    return;
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
      await scanDirectory(fullPath, drive);
    } else if (stat.isFile()) {
      try {
        // Get file timestamps - on Windows, stat.birthtime should work but verify it's valid
        // If birthtime is invalid (epoch 0 or future date), use ctime as fallback
        let fileCreated = stat.birthtime;
        if (
          !fileCreated ||
          fileCreated.getTime() <= 0 ||
          fileCreated.getTime() > Date.now()
        ) {
          fileCreated = stat.ctime; // Use ctime (status change time) as fallback on Windows
        }

        // Ensure all timestamps are valid Date objects
        const fileMetadata = {
          fileName: item,
          fullPath,
          folderPath: dir,
          extension: path.extname(item),
          sizeBytes: stat.size,
          sizeMB: +(stat.size / (1024 * 1024)).toFixed(2),
          drive,
          fileCreatedAt: new Date(fileCreated), // File creation time from D drive
          modifiedAt: new Date(stat.mtime), // File modification time from D drive
          fileAccessedAt: new Date(stat.atime), // File access time from D drive
          scannedAt: new Date(), // When document was created in MongoDB
        };

        // Console log to show metadata being read from file system
        console.log(`\n📄 File Metadata from ${drive}:`, {
          fileName: fileMetadata.fileName,
          fullPath: fileMetadata.fullPath,
          sizeMB: fileMetadata.sizeMB,
          fileCreatedAt: fileMetadata.fileCreatedAt.toISOString(),
          modifiedAt: fileMetadata.modifiedAt.toISOString(),
          fileAccessedAt: fileMetadata.fileAccessedAt.toISOString(),
        });

        // Create new document to allow duplicates
        const savedDoc = await FileMeta.create(fileMetadata);
        fileCount++;
        console.log(`✅ Saved to MongoDB (${fileCount}) - _id:`, savedDoc._id);
        console.log("   Saved Document Timestamps:", {
          fileCreatedAt: savedDoc.fileCreatedAt
            ? savedDoc.fileCreatedAt.toISOString()
            : "MISSING",
          modifiedAt: savedDoc.modifiedAt
            ? savedDoc.modifiedAt.toISOString()
            : "MISSING",
          fileAccessedAt: savedDoc.fileAccessedAt
            ? savedDoc.fileAccessedAt.toISOString()
            : "MISSING",
        });
      } catch (err) {
        console.error(`❌ Error saving file ${fullPath}:`, err.message);
      }
    }
  }

  if (fileCount > 0) {
    console.log(`\n📊 Total files saved from ${dir}: ${fileCount}`);
  }
}

module.exports = scanDirectory;
