const mongoose = require("mongoose");

const FileMetaSchema = new mongoose.Schema(
  {
    fileName: String,
    fullPath: String, // Removed unique constraint to allow duplicates
    folderPath: String,
    extension: String,
    sizeBytes: Number,
    sizeMB: Number,
    drive: String,
    fingerprint: String, // Lightweight fingerprint for duplicate detection: normalizedFileName::extension::sizeBytes
    fileCreatedAt: Date, // File creation time (from D drive)
    modifiedAt: Date, // File modification time (from D drive)
    fileAccessedAt: Date, // File access time (from D drive)
    scannedAt: Date, // When document was created in MongoDB
  },
  { timestamps: false } // Disabled to use file's actual timestamps
);

// Index for fingerprint (used for duplicate detection during scan)
FileMetaSchema.index({ fingerprint: 1 });

module.exports = mongoose.model("FileMeta", FileMetaSchema);

module.exports = mongoose.model("FileMeta", FileMetaSchema);
