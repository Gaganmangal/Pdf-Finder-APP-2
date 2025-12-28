/**
 * MongoDB Index Creation Script
 * Run this once to create optimal indexes for duplicate detection
 *
 * Usage: node createIndexes.js
 */

const mongoose = require("mongoose");
const connectDB = require("./db");
const FileMeta = require("./models/FileMeta");
const DuplicateFile = require("./models/DuplicateFile");

async function createIndex(indexSpec, options = {}) {
  try {
    await indexSpec.collection.createIndex(indexSpec.keys, options);
    return true;
  } catch (err) {
    if (err.code === 85 || err.codeName === "IndexOptionsConflict") {
      // Index already exists with different name - skip
      return false;
    }
    if (err.code === 86 || err.codeName === "IndexKeySpecsConflict") {
      // Index already exists - skip
      return false;
    }
    throw err;
  }
}

async function createIndexes() {
  try {
    await connectDB();

    // Indexes for FileMeta collection (for duplicate detection aggregation)
    await createIndex(
      {
        collection: FileMeta,
        keys: { fileName: 1, extension: 1, sizeBytes: 1 },
      },
      { name: "idx_duplicate_detection" }
    );

    await createIndex(
      { collection: FileMeta, keys: { fileName: 1 } },
      { name: "idx_fileName" }
    );

    await createIndex(
      { collection: FileMeta, keys: { extension: 1 } },
      { name: "idx_extension" }
    );

    await createIndex(
      { collection: FileMeta, keys: { sizeBytes: 1 } },
      { name: "idx_sizeBytes" }
    );

    // Indexes for DuplicateFile collection
    // Check if fingerprint index exists with different name first
    const existingIndexes = await DuplicateFile.collection.getIndexes();
    const hasFingerprintIndex = Object.keys(existingIndexes).some(
      (name) =>
        existingIndexes[name].key && existingIndexes[name].key.fingerprint
    );

    if (!hasFingerprintIndex) {
      await createIndex(
        { collection: DuplicateFile, keys: { fingerprint: 1 } },
        { name: "idx_fingerprint", unique: false }
      );
    }

    await createIndex(
      { collection: DuplicateFile, keys: { count: -1, detectedAt: -1 } },
      { name: "idx_count_detectedAt" }
    );

    await createIndex(
      { collection: DuplicateFile, keys: { extension: 1, sizeBytes: -1 } },
      { name: "idx_extension_sizeBytes" }
    );

    await createIndex(
      { collection: DuplicateFile, keys: { detectedAt: -1 } },
      { name: "idx_detectedAt" }
    );

    // Index for drive filtering in files array
    await createIndex(
      { collection: DuplicateFile, keys: { "files.drive": 1 } },
      { name: "idx_files_drive" }
    );

    console.log("✅ All indexes created successfully!");

    // Show index information
    const fileMetaIndexes = await FileMeta.collection.getIndexes();
    const duplicateFileIndexes = await DuplicateFile.collection.getIndexes();

    console.log("\n📊 FileMeta indexes:");
    console.log(JSON.stringify(fileMetaIndexes, null, 2));

    console.log("\n📊 DuplicateFile indexes:");
    console.log(JSON.stringify(duplicateFileIndexes, null, 2));

    process.exit(0);
  } catch (err) {
    console.error("❌ Error creating indexes:", err);
    process.exit(1);
  }
}

createIndexes();
