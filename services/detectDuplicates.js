const FileMeta = require("../models/FileMeta");
const DuplicateFile = require("../models/DuplicateFile");

/**
 * Normalize fileName for comparison (lowercase, trim)
 */
function normalizeFileName(fileName) {
  if (!fileName) return "";
  return fileName.toLowerCase().trim();
}

/**
 * Generate fingerprint from fileName, extension, and sizeBytes
 */
function generateFingerprint(fileName, extension, sizeBytes) {
  const normalized = normalizeFileName(fileName);
  return `${normalized}::${extension}::${sizeBytes}`;
}

/**
 * Detect duplicates using MongoDB aggregation pipeline
 * This is highly optimized for large datasets
 */
async function detectDuplicates() {
  // Step 1: Aggregation pipeline to find duplicates
  const duplicates = await FileMeta.aggregate([
    // Match only files that have required fields
    {
      $match: {
        fileName: { $exists: true, $ne: null, $ne: "" },
        extension: { $exists: true, $ne: null },
        sizeBytes: { $exists: true, $type: "number", $gt: 0 },
      },
    },
    // Normalize fileName and create fingerprint
    {
      $addFields: {
        normalizedFileName: {
          $toLower: { $trim: { input: "$fileName" } },
        },
        fingerprint: {
          $concat: [
            { $toLower: { $trim: { input: "$fileName" } } },
            "::",
            { $ifNull: ["$extension", ""] },
            "::",
            { $toString: "$sizeBytes" },
          ],
        },
      },
    },
    // Group by fingerprint to find duplicates
    {
      $group: {
        _id: "$fingerprint",
        count: { $sum: 1 },
        files: {
          $push: {
            fileKey: { $ifNull: ["$fileKey", "$fullPath"] }, // Use fullPath as fileKey if fileKey doesn't exist
            folderPath: "$folderPath",
            drive: "$drive",
            fullPath: "$fullPath",
            scannedAt: "$scannedAt",
          },
        },
        fileName: { $first: "$fileName" },
        normalizedFileName: { $first: "$normalizedFileName" },
        extension: { $first: "$extension" },
        sizeBytes: { $first: "$sizeBytes" },
      },
    },
    // Only keep groups with count > 1 (duplicates)
    {
      $match: {
        count: { $gt: 1 },
      },
    },
    // Sort by count descending (most duplicates first)
    {
      $sort: { count: -1 },
    },
    // Project final structure
    {
      $project: {
        _id: 0,
        fingerprint: "$_id",
        fileName: 1,
        extension: 1,
        sizeBytes: 1,
        count: 1,
        files: 1,
      },
    },
  ]);

  return duplicates;
}

/**
 * Main function to detect and save duplicates
 */
async function detectAndSaveDuplicates() {
  // Step 1: Run aggregation to detect duplicates
  const duplicateGroups = await detectDuplicates();

  // Step 2: Clear old duplicate_files collection
  await DuplicateFile.deleteMany({});

  // Step 3: Insert new duplicate groups
  if (duplicateGroups.length > 0) {
    await DuplicateFile.insertMany(duplicateGroups);
  }

  return {
    totalGroups: duplicateGroups.length,
    totalDuplicateFiles: duplicateGroups.reduce(
      (sum, group) => sum + group.count,
      0
    ),
    groups: duplicateGroups,
  };
}

/**
 * Get duplicate groups with optional filters
 */
async function getDuplicates(options = {}) {
  const { limit = 100, skip = 0, minCount = 2, extension, drive } = options;

  const query = {
    count: { $gte: minCount },
  };

  if (extension) {
    query.extension = extension;
  }

  if (drive) {
    query["files.drive"] = drive;
  }

  const duplicates = await DuplicateFile.find(query)
    .sort({ count: -1, detectedAt: -1 })
    .limit(parseInt(limit))
    .skip(parseInt(skip));

  const total = await DuplicateFile.countDocuments(query);

  return {
    total,
    limit: parseInt(limit),
    skip: parseInt(skip),
    duplicates,
  };
}

module.exports = {
  detectAndSaveDuplicates,
  getDuplicates,
  detectDuplicates,
};

