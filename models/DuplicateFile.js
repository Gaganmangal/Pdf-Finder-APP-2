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
        fileKey: String,
        folderPath: String,
        drive: String,
        fullPath: String,
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

// Compound index for faster queries
DuplicateFileSchema.index({ count: -1, detectedAt: -1 });
DuplicateFileSchema.index({ extension: 1, sizeBytes: -1 });

module.exports = mongoose.model("DuplicateFile", DuplicateFileSchema);

