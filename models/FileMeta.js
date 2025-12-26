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
    fileCreatedAt: Date, // File creation time (from D drive)
    modifiedAt: Date, // File modification time (from D drive)
    fileAccessedAt: Date, // File access time (from D drive)
    scannedAt: Date, // When document was created in MongoDB
  },
  { timestamps: false } // Disabled to use file's actual timestamps
);

module.exports = mongoose.model("FileMeta", FileMetaSchema);
