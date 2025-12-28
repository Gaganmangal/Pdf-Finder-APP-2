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
 * Detect duplicates using optimized MongoDB aggregation pipeline
 * Optimized for 80TB+ datasets with cursor-based processing
 */
async function detectDuplicates() {
  const BATCH_SIZE = 5000; // Process in batches to avoid memory issues
  const duplicateGroups = [];
  let processedCount = 0;

  // Use cursor for memory-efficient processing
  const cursor = FileMeta.aggregate(
    [
      // Match only files that have required fields
      {
        $match: {
          fileName: { $exists: true, $ne: null, $ne: "" },
          extension: { $exists: true, $ne: null },
          sizeBytes: { $exists: true, $type: "number", $gt: 0 },
        },
      },
      // Create fingerprint directly (more efficient)
      {
        $addFields: {
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
      // Group by fingerprint - only count first (memory efficient)
      {
        $group: {
          _id: "$fingerprint",
          count: { $sum: 1 },
          // Store minimal data in group stage
          firstFile: { $first: "$$ROOT" },
        },
      },
      // Filter duplicates only (count > 1)
      {
        $match: {
          count: { $gt: 1 },
        },
      },
      // Sort by count (for processing priority)
      {
        $sort: { count: -1 },
      },
    ],
    {
      allowDiskUse: true,
      cursor: { batchSize: BATCH_SIZE },
    }
  );

  // Process cursor in batches
  for await (const group of cursor) {
    // For each duplicate group, fetch all files with this fingerprint
    const fingerprint = group._id;
    const files = await FileMeta.find(
      {
        fileName: { $exists: true, $ne: null, $ne: "" },
        extension: { $exists: true, $ne: null },
        sizeBytes: { $exists: true, $type: "number", $gt: 0 },
        $expr: {
          $eq: [
            {
              $concat: [
                { $toLower: { $trim: { input: "$fileName" } } },
                "::",
                { $ifNull: ["$extension", ""] },
                "::",
                { $toString: "$sizeBytes" },
              ],
            },
            fingerprint,
          ],
        },
      },
      {
        fileKey: 1,
        folderPath: 1,
        drive: 1,
        fullPath: 1,
        scannedAt: 1,
        _id: 0,
      }
    ).lean();

    // Build duplicate group document
    duplicateGroups.push({
      fingerprint: fingerprint,
      fileName: group.firstFile.fileName,
      extension: group.firstFile.extension,
      sizeBytes: group.firstFile.sizeBytes,
      count: group.count,
      files: files.map((f) => ({
        fileKey: f.fileKey || f.fullPath,
        folderPath: f.folderPath,
        drive: f.drive,
        fullPath: f.fullPath,
        scannedAt: f.scannedAt,
      })),
    });

    processedCount++;

    // Log progress every 100 groups
    if (processedCount % 100 === 0) {
      console.log(`Processed ${processedCount} duplicate groups...`);
    }
  }

  return duplicateGroups;
}

/**
 * Main function to detect and save duplicates
 * Optimized for 80TB+ datasets with batch inserts
 */
async function detectAndSaveDuplicates() {
  const INSERT_BATCH_SIZE = 1000; // Insert in batches to avoid memory issues

  // Step 1: Clear old duplicate_files collection
  await DuplicateFile.deleteMany({});

  // Step 2: Run aggregation to detect duplicates (cursor-based)
  const duplicateGroups = [];
  let totalDuplicateFiles = 0;
  let batchBuffer = [];

  // Process duplicates in streaming fashion
  const cursor = FileMeta.aggregate(
    [
      {
        $match: {
          fileName: { $exists: true, $ne: null, $ne: "" },
          extension: { $exists: true, $ne: null },
          sizeBytes: { $exists: true, $type: "number", $gt: 0 },
        },
      },
      {
        $addFields: {
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
      {
        $group: {
          _id: "$fingerprint",
          count: { $sum: 1 },
          firstFile: { $first: "$$ROOT" },
        },
      },
      {
        $match: {
          count: { $gt: 1 },
        },
      },
      {
        $sort: { count: -1 },
      },
    ],
    {
      allowDiskUse: true,
      cursor: { batchSize: 1000 },
    }
  );

  let processedGroups = 0;

  // Process each duplicate group
  for await (const group of cursor) {
    const fingerprint = group._id;
    const firstFile = group.firstFile;

    // Use exact values from first file for efficient indexed lookup
    // This avoids $expr and can use the compound index
    const files = await FileMeta.find(
      {
        extension: firstFile.extension,
        sizeBytes: firstFile.sizeBytes,
        $expr: {
          $eq: [
            { $toLower: { $trim: { input: "$fileName" } } },
            { $toLower: { $trim: { input: firstFile.fileName } } },
          ],
        },
      },
      {
        fileKey: 1,
        folderPath: 1,
        drive: 1,
        fullPath: 1,
        scannedAt: 1,
        _id: 0,
      }
    )
      .lean()
      .hint({ extension: 1, sizeBytes: 1 }); // Use compound index for faster lookup

    const duplicateGroup = {
      fingerprint: fingerprint,
      fileName: group.firstFile.fileName,
      extension: group.firstFile.extension,
      sizeBytes: group.firstFile.sizeBytes,
      count: group.count,
      files: files.map((f) => ({
        fileKey: f.fileKey || f.fullPath,
        folderPath: f.folderPath,
        drive: f.drive,
        fullPath: f.fullPath,
        scannedAt: f.scannedAt,
      })),
      detectedAt: new Date(),
    };

    batchBuffer.push(duplicateGroup);
    totalDuplicateFiles += group.count;
    processedGroups++;

    // Insert in batches to avoid memory issues
    if (batchBuffer.length >= INSERT_BATCH_SIZE) {
      await DuplicateFile.insertMany(batchBuffer, { ordered: false });
      batchBuffer = [];
    }

    // Log progress
    if (processedGroups % 500 === 0) {
      console.log(
        `Progress: ${processedGroups} groups processed, ${totalDuplicateFiles} duplicate files found`
      );
    }
  }

  // Insert remaining items
  if (batchBuffer.length > 0) {
    await DuplicateFile.insertMany(batchBuffer, { ordered: false });
  }

  return {
    totalGroups: processedGroups,
    totalDuplicateFiles: totalDuplicateFiles,
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
