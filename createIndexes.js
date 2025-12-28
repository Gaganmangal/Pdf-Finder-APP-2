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

async function createIndexes() {
  try {
    await connectDB();

    // Indexes for FileMeta collection (for duplicate detection aggregation)
    await FileMeta.collection.createIndex(
      { fileName: 1, extension: 1, sizeBytes: 1 },
      { name: "idx_duplicate_detection" }
    );

    await FileMeta.collection.createIndex(
      { fileName: 1 },
      { name: "idx_fileName" }
    );

    await FileMeta.collection.createIndex(
      { extension: 1 },
      { name: "idx_extension" }
    );

    await FileMeta.collection.createIndex(
      { sizeBytes: 1 },
      { name: "idx_sizeBytes" }
    );

    // Indexes for DuplicateFile collection (already defined in schema, but ensure they exist)
    await DuplicateFile.collection.createIndex(
      { fingerprint: 1 },
      { name: "idx_fingerprint", unique: false }
    );

    await DuplicateFile.collection.createIndex(
      { count: -1, detectedAt: -1 },
      { name: "idx_count_detectedAt" }
    );

    await DuplicateFile.collection.createIndex(
      { extension: 1, sizeBytes: -1 },
      { name: "idx_extension_sizeBytes" }
    );

    await DuplicateFile.collection.createIndex(
      { detectedAt: -1 },
      { name: "idx_detectedAt" }
    );

    // Index for drive filtering in files array
    await DuplicateFile.collection.createIndex(
      { "files.drive": 1 },
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

