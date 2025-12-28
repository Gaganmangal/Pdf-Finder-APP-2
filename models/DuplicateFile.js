const mongoose = require("mongoose");

const DuplicateFileSchema = new mongoose.Schema(
  {
    fingerprint: {
      type: String,
      required: true,
      index: true, // Index for fast lookups
    },
    fileName: {
      type: String,
      required: true,
    },
    extension: {
      type: String,
      required: true,
    },
    sizeBytes: {
      type: Number,
      required: true,
    },
    count: {
      type: Number,
      required: true,
      default: 0,
    },
    files: [
      {
        fullPath: String,
        folderPath: String,
        drive: String,
        scannedAt: Date,
      },
    ],
    detectedAt: {
      type: Date,
      default: Date.now,
      index: true, // Index for sorting by detection time
    },
  },
  { timestamps: false }
);

// Indexes for efficient duplicate queries
DuplicateFileSchema.index({ fingerprint: 1 }); // Primary lookup index
DuplicateFileSchema.index({ count: -1 }); // Sort by most duplicates first

module.exports = mongoose.model("DuplicateFile", DuplicateFileSchema);
