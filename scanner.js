const fs = require("fs");
const path = require("path");
const FileMeta = require("./models/FileMeta");

// STRICT O(1) MEMORY FILE SCANNER FOR LARGE FILESYSTEMS
// See requirements in prompt

const FILE_BATCH_SIZE = 1000;

// Use only ONE drive string across all scans for consistency
const DRIVE = "NETWORK";

async function scanDirectoryAndMirror(dir) {
  const scanId = new Date();
  let batch = [];
  let totalFiles = 0;
  let totalDirs = 0;

  async function flush() {
    if (batch.length === 0) return;
    const ops = batch.map((doc) => ({
      updateOne: {
        filter: { fullPath: doc.fullPath },
        update: { $set: doc },
        upsert: true,
      },
    }));
    await FileMeta.bulkWrite(ops, { ordered: false });
    batch = [];
  }

  async function scanDir(current) {
    let items;
    try {
      items = fs.readdirSync(current);
    } catch {
      return;
    }
    for (const item of items) {
      const full = path.join(current, item);
      let stat;
      try {
        stat = fs.statSync(full);
      } catch {
        continue;
      }
      if (stat.isDirectory()) {
        totalDirs++;
        await scanDir(full);
      } else if (stat.isFile()) {
        totalFiles++;
        let created = stat.birthtime;
        if (
          !created ||
          created.getTime() <= 0 ||
          created.getTime() > Date.now()
        )
          created = stat.ctime;
        const doc = {
          fileName: item,
          fullPath: full,
          folderPath: current,
          extension: path.extname(item),
          sizeBytes: stat.size,
          sizeMB: +(stat.size / (1024 * 1024)).toFixed(2),
          drive: DRIVE,
          fileCreatedAt: created,
          modifiedAt: stat.mtime,
          fileAccessedAt: stat.atime,
          lastSeenAt: scanId,
        };
        batch.push(doc);
        if (batch.length >= FILE_BATCH_SIZE) await flush();
      }
    }
  }

  await scanDir(dir);
  await flush();
  await FileMeta.deleteMany({ drive: DRIVE, lastSeenAt: { $ne: scanId } });
  return { fileCount: totalFiles, dirCount: totalDirs };
}

module.exports = scanDirectoryAndMirror;
