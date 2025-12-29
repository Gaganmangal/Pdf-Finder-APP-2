const fs = require("fs/promises");
const path = require("path");
const { MongoClient } = require("mongodb");

const MONGO_URI =
  "mongodb+srv://Gaganfnr:ndLz9yHCsOmv9S3k@gagan.jhuti8y.mongodb.net/test?appName=Gagan";
const FILE_ROOT = "mnt/pdfs"; // Update as needed
const FILEMETA_BATCH_SIZE = 1000;
const DUPLICATE_BATCH_SIZE = 500;
// Each value in map is array of { file metadata }

// Given all required fields, build the fingerprint string
function getFingerprint({ fileName, extension, sizeBytes }) {
  return `${fileName.toLowerCase()}|${extension}|${sizeBytes}`;
}

async function* walk(dir) {
  for (const entry of await fs.readdir(dir, { withFileTypes: true })) {
    const abs = path.join(dir, entry.name);
    if (entry.isDirectory()) yield* walk(abs);
    else yield abs;
  }
}

async function scanAndInsert() {
  const client = new MongoClient(MONGO_URI);
  await client.connect();
  const db = client.db();
  const FileMeta = db.collection("FileMeta");
  const DuplicateFile = db.collection("DuplicateFile");

  // 1️⃣ Clear DuplicateFile collection at scan start
  await DuplicateFile.deleteMany({});

  const scanTime = new Date();

  let batch = [];
  // Note: minimal file info for Map value
  const duplicateMap = new Map();
  let duplicateBatch = [];

  for await (const filePath of walk(FILE_ROOT)) {
    let stat;
    try {
      stat = await fs.stat(filePath);
    } catch (err) {
      console.error(`Error statting file: ${filePath}:`, err);
      continue;
    }
    const fileName = path.basename(filePath);
    const extension = path.extname(filePath);
    const sizeBytes = stat.size;
    const modifiedAt = stat.mtime;
    const fileAccessedAt = stat.atime;
    const fileCreatedAt = stat.birthtime;
    const fingerprint = getFingerprint({ fileName, extension, sizeBytes });

    // Prepare FileMeta document
    const metaDoc = {
      fullPath: filePath,
      fileName,
      extension,
      sizeBytes,
      modifiedAt,
      scannedAt: scanTime,
      fingerprint,
      fileAccessedAt,
      fileCreatedAt,
    };
    batch.push(metaDoc);

    // Duplicate detection logic (in-memory, during scan)
    if (!duplicateMap.has(fingerprint)) {
      duplicateMap.set(fingerprint, [metaDoc]);
    } else {
      let group = duplicateMap.get(fingerprint);
      group.push(metaDoc);
      if (group.length === 2) {
        // First time a duplicate is found, create a group
        duplicateBatch.push({
          fingerprint,
          count: group.length,
          files: group.map((f) => ({
            fullPath: f.fullPath,
            fileName: f.fileName,
            extension: f.extension,
            sizeBytes: f.sizeBytes,
            modifiedAt: f.modifiedAt,
            scannedAt: f.scannedAt,
            fileAccessedAt: f.fileAccessedAt,
            fileCreatedAt: f.fileCreatedAt,
          })),
          detectedAt: new Date(),
        });
      } else if (group.length > 2) {
        // For 3rd+ occurrence, update last group in batch (not efficient for huge RAM, will work here)
        let lastGroup = duplicateBatch[duplicateBatch.length - 1];
        lastGroup.count = group.length;
        lastGroup.files.push({
          fullPath: metaDoc.fullPath,
          fileName: metaDoc.fileName,
          extension: metaDoc.extension,
          sizeBytes: metaDoc.sizeBytes,
          modifiedAt: metaDoc.modifiedAt,
          scannedAt: metaDoc.scannedAt,
          fileAccessedAt: metaDoc.fileAccessedAt,
          fileCreatedAt: metaDoc.fileCreatedAt,
        });
      }
      // Flush duplicates in batches
      if (duplicateBatch.length >= DUPLICATE_BATCH_SIZE) {
        await DuplicateFile.insertMany(duplicateBatch);
        duplicateBatch = [];
      }
    }

    // FileMeta batch insert
    if (batch.length >= FILEMETA_BATCH_SIZE) {
      await FileMeta.insertMany(batch);
      batch = [];
    }
  }

  // Final batch flush for FileMeta
  if (batch.length) await FileMeta.insertMany(batch);
  // Final batch flush for duplicates
  if (duplicateBatch.length) await DuplicateFile.insertMany(duplicateBatch);

  console.log("Scan finished at", new Date());
  await client.close();
}

scanAndInsert().catch(console.error);
