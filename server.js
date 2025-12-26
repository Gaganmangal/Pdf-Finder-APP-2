const express = require("express");
const connectDB = require("./db");
const scanDrive = require("./scanner");

const app = express();
app.use(express.json());

connectDB();

app.post("/scan-d-drive", async (req, res) => {
  try {
    const fs = require("fs");
    const scanPath = "D:/";

    console.log("\n🚀 Starting D drive scan...");

    // Check if path exists (Windows only - won't work on EC2/Linux)
    if (!fs.existsSync(scanPath)) {
      const errorMsg = `Path "${scanPath}" does not exist. This route is for Windows D drive only. On EC2/Linux, use /scan-network-share instead.`;
      console.error(`❌ ${errorMsg}`);
      return res.status(404).json({
        error: errorMsg,
        suggestion:
          "Use POST /scan-network-share for EC2/Linux network share scanning",
      });
    }

    const FileMeta = require("./models/FileMeta");
    const countBefore = await FileMeta.countDocuments();

    await scanDrive(scanPath, "D");

    const countAfter = await FileMeta.countDocuments();
    const newDocs = countAfter - countBefore;

    console.log("\n✅ D drive scan completed!");
    console.log(
      `📊 Documents before: ${countBefore}, after: ${countAfter}, new: ${newDocs}`
    );

    // Get a sample document to show metadata in response
    const sampleDoc = await FileMeta.findOne().sort({ scannedAt: -1 });

    res.json({
      message: "D drive scanned & data saved",
      stats: {
        documentsBefore: countBefore,
        documentsAfter: countAfter,
        newDocuments: newDocs,
      },
      sampleMetadata: sampleDoc
        ? {
            fileName: sampleDoc.fileName,
            fileCreatedAt: sampleDoc.fileCreatedAt,
            modifiedAt: sampleDoc.modifiedAt,
            fileAccessedAt: sampleDoc.fileAccessedAt,
            scannedAt: sampleDoc.scannedAt,
          }
        : null,
    });
  } catch (err) {
    console.error("❌ Scan error:", err);
    res.status(500).json({ error: err.message });
  }
});

// Route to scan mounted network share
app.post("/scan-network-share", async (req, res) => {
  try {
    console.log("\n🚀 Starting network share scan...");
    const FileMeta = require("./models/FileMeta");
    const countBefore = await FileMeta.countDocuments();

    // Scan the mounted network share at /mnt/pdfs
    await scanDrive("/mnt/pdfs", "NETWORK");

    const countAfter = await FileMeta.countDocuments();
    const newDocs = countAfter - countBefore;

    console.log("\n✅ Network share scan completed!");
    console.log(
      `📊 Documents before: ${countBefore}, after: ${countAfter}, new: ${newDocs}`
    );

    // Get a sample document to show metadata in response
    const sampleDoc = await FileMeta.findOne().sort({ scannedAt: -1 });

    res.json({
      message: "Network share scanned & data saved",
      stats: {
        documentsBefore: countBefore,
        documentsAfter: countAfter,
        newDocuments: newDocs,
      },
      sampleMetadata: sampleDoc
        ? {
            fileName: sampleDoc.fileName,
            fileCreatedAt: sampleDoc.fileCreatedAt,
            modifiedAt: sampleDoc.modifiedAt,
            fileAccessedAt: sampleDoc.fileAccessedAt,
            scannedAt: sampleDoc.scannedAt,
          }
        : null,
    });
  } catch (err) {
    console.error("❌ Scan error:", err);
    res.status(500).json({ error: err.message });
  }
});

// Generic route to scan any path
app.post("/scan", async (req, res) => {
  try {
    const { path: scanPath, drive = "CUSTOM" } = req.body;

    if (!scanPath) {
      return res
        .status(400)
        .json({ error: "Path is required in request body" });
    }

    console.log(`\n🚀 Starting scan of: ${scanPath}`);
    const FileMeta = require("./models/FileMeta");
    const countBefore = await FileMeta.countDocuments();

    await scanDrive(scanPath, drive);

    const countAfter = await FileMeta.countDocuments();
    const newDocs = countAfter - countBefore;

    console.log(`\n✅ Scan of ${scanPath} completed!`);
    console.log(
      `📊 Documents before: ${countBefore}, after: ${countAfter}, new: ${newDocs}`
    );

    res.json({
      message: `Scan of ${scanPath} completed & data saved`,
      stats: {
        documentsBefore: countBefore,
        documentsAfter: countAfter,
        newDocuments: newDocs,
      },
    });
  } catch (err) {
    console.error("❌ Scan error:", err);
    res.status(500).json({ error: err.message });
  }
});

// Route to list files in a folder (without scanning)
app.get("/list-folder", async (req, res) => {
  try {
    const { path: folderPath } = req.query;
    const fs = require("fs");
    const path = require("path");

    if (!folderPath) {
      return res
        .status(400)
        .json({ error: "Path query parameter is required" });
    }

    console.log(`\n📋 Listing contents of: ${folderPath}`);

    if (!fs.existsSync(folderPath)) {
      return res.status(404).json({ error: `Path not found: ${folderPath}` });
    }

    const stats = fs.statSync(folderPath);
    if (!stats.isDirectory()) {
      return res
        .status(400)
        .json({ error: `Path is not a directory: ${folderPath}` });
    }

    const items = fs.readdirSync(folderPath);
    const result = {
      path: folderPath,
      totalItems: items.length,
      items: [],
    };

    for (const item of items) {
      const fullPath = path.join(folderPath, item);
      try {
        const stat = fs.statSync(fullPath);
        result.items.push({
          name: item,
          type: stat.isDirectory() ? "directory" : "file",
          size: stat.isFile() ? stat.size : null,
          sizeMB: stat.isFile()
            ? +(stat.size / (1024 * 1024)).toFixed(2)
            : null,
          modifiedAt: stat.mtime,
          createdAt: stat.birthtime || stat.ctime,
          accessedAt: stat.atime,
        });
      } catch (err) {
        result.items.push({
          name: item,
          type: "unknown",
          error: err.message,
        });
      }
    }

    console.log(`✅ Found ${result.items.length} items in ${folderPath}`);
    res.json(result);
  } catch (err) {
    console.error("❌ List folder error:", err);
    res.status(500).json({ error: err.message });
  }
});

// 🔥 THIS ROUTE WAS MISSING (404 fix)
app.get("/files", async (req, res) => {
  const FileMeta = require("./models/FileMeta");
  const { limit = 50, drive, folderPath } = req.query;

  let query = {};
  if (drive) query.drive = drive;
  if (folderPath) query.folderPath = { $regex: folderPath, $options: "i" };

  const files = await FileMeta.find(query)
    .limit(parseInt(limit))
    .sort({ scannedAt: -1 });
  res.json({
    total: files.length,
    files: files,
  });
});

app.listen(3001, () => {
  console.log("Backend running on port 3001");
});
