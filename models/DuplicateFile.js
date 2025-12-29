const mongoose = require("mongoose");

const DuplicateFileSchema = new mongoose.Schema(
  {
    fingerprint: { type: String, index: true },
    count: Number,

    files: [
      {
        fullPath: String,
        folderPath: String,
        drive: String,
        scannedAt: Date,
      },
    ],

    detectedAt: { type: Date, default: Date.now },
  },
  { timestamps: true }
);

module.exports = mongoose.model("DuplicateFile", DuplicateFileSchema);
