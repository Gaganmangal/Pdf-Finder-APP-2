const fs = require("fs");
const path = require("path");
const File = require("../models/FileMeta");

async function indexFiles(rootDir) {
  async function walk(dir) {
    let items;
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
        await walk(fullPath);
      } else {
        const ext = path.extname(item).toLowerCase();

        // Get file timestamps - on Windows, use birthtime if valid, else ctime
        const fileCreated =
          stat.birthtime && stat.birthtime.getTime() > 0
            ? stat.birthtime
            : stat.ctime; // Fallback to ctime if birthtime is invalid

        const doc = {
          fileName: item,
          extension: ext,
          folderPath: dir,
          fullPath,
          sizeBytes: stat.size,
          sizeMB: +(stat.size / (1024 * 1024)).toFixed(2),
          fileCreatedAt: fileCreated, // File creation time from D drive
          modifiedAt: stat.mtime, // File modification time from D drive
          fileAccessedAt: stat.atime, // File access time from D drive
          scannedAt: new Date(), // When document was created in MongoDB
        };

        // Console log to show metadata
        console.log("\n📄 File Metadata:", {
          fileName: doc.fileName,
          fullPath: doc.fullPath,
          sizeMB: doc.sizeMB,
          fileCreatedAt: doc.fileCreatedAt,
          modifiedAt: doc.modifiedAt,
          fileAccessedAt: doc.fileAccessedAt,
        });

        // Create new document to allow duplicates
        const savedDoc = await File.create(doc);
        console.log("✅ Saved to MongoDB - _id:", savedDoc._id);
      }
    }
  }

  await walk(rootDir);
  console.log("Indexing completed");
}

module.exports = { indexFiles };
