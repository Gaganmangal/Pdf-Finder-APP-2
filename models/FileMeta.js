// MongoDB FileMeta schema for reference only (if you use Mongoose, otherwise not required)
// Not used by scanner.js directly, but helpful for clarity and validation
module.exports = {
  path: String,           // full path at scan time
  name: String,
  ext: String,
  size: Number,
  modifiedAt: Date,
  scanTime: Date,         // when this FileMeta was added
  fingerprint: String,    // name|ext|size
};
