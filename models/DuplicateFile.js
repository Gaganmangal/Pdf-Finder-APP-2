// MongoDB DuplicateFile schema for reference only
module.exports = {
  fingerprint: String,
  count: Number,
  files: [  // Small batch only; projection limited to key fields
    {
      _id: 'ObjectId',
      path: String,
      name: String,
      ext: String,
      size: Number,
      modifiedAt: Date,
    }
  ],
  detectedAt: Date,
};
