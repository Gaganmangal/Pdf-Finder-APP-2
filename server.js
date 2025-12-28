const express = require("express");
const fs = require("fs");
const path = require("path");

const connectDB = require("./db");
const scanDrive = require("./scanner");
const FileMeta = require("./models/FileMeta");

const app = express();
app.use(express.json());

// --------------------
// DB CONNECT
// --------------------
connectDB();

// =======================================================
// 1️⃣ SCAN NETWORK SHARE (MAIN ROUTE – EC2 LINUX)
// =======================================================
app.post("/scan-network-share", async (req, res) => {
  const scanPath = "/mnt/pdfs";

  if (!fs.existsSync(scanPath)) {
    return res.status(404).json({
      error: `Mount path not found: ${scanPath}`,
      hint: "Check CIFS mount on Linux EC2",
    });
  }

  // Return immediately - scan runs in background
  res.json({
    message: "Network share scan started in background",
    scanPath: scanPath,
  });

  // Background execution (non-blocking)
  setImmediate(async () => {
    try {
      const countBefore = await FileMeta.countDocuments();
      const result = await scanDrive(scanPath, "NETWORK");
      const countAfter = await FileMeta.countDocuments();

      // Log completion (can be removed if no console.log desired)
      console.log(
        `Scan completed: ${result.fileCount} files, ${result.duplicateGroups} duplicate groups`
      );
    } catch (err) {
      console.error("Scan error:", err.message);
    }
  });
});

// =======================================================
// 2️⃣ GENERIC SCAN (ANY PATH – OPTIONAL)
// =======================================================
app.post("/scan", async (req, res) => {
  try {
    const { scanPath, drive = "CUSTOM" } = req.body;

    if (!scanPath) {
      return res.status(400).json({
        error: "scanPath is required in request body",
      });
    }

    if (!fs.existsSync(scanPath)) {
      return res.status(404).json({
        error: `Path does not exist: ${scanPath}`,
      });
    }

    const countBefore = await FileMeta.countDocuments();

    await scanDrive(scanPath, drive);

    const countAfter = await FileMeta.countDocuments();
    const newDocs = countAfter - countBefore;

    res.json({
      message: "Scan completed",
      stats: {
        documentsBefore: countBefore,
        documentsAfter: countAfter,
        newDocuments: newDocs,
      },
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// =======================================================
// 3️⃣ LIST FILES FROM DB (FIXED 404)
// =======================================================
app.get("/files", async (req, res) => {
  try {
    const { limit = 50, drive, folderPath } = req.query;

    let query = {};
    if (drive) query.drive = drive;
    if (folderPath) query.folderPath = { $regex: folderPath, $options: "i" };

    const files = await FileMeta.find(query)
      .sort({ scannedAt: -1 })
      .limit(parseInt(limit));

    res.json({
      total: files.length,
      files,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// =======================================================
// 4️⃣ LIST FOLDER CONTENTS (NO DB, DIRECT FS)
// =======================================================
app.get("/list-folder", async (req, res) => {
  try {
    const folderPath = req.query.path;

    if (!folderPath) {
      return res.status(400).json({
        error: "Query param ?path= is required",
      });
    }

    if (!fs.existsSync(folderPath)) {
      return res.status(404).json({
        error: `Path not found: ${folderPath}`,
      });
    }

    const stats = fs.statSync(folderPath);
    if (!stats.isDirectory()) {
      return res.status(400).json({
        error: "Provided path is not a directory",
      });
    }

    const items = fs.readdirSync(folderPath);

    const result = items.map((item) => {
      const fullPath = path.join(folderPath, item);
      try {
        const stat = fs.statSync(fullPath);
        return {
          name: item,
          type: stat.isDirectory() ? "directory" : "file",
          sizeBytes: stat.isFile() ? stat.size : null,
          sizeMB: stat.isFile()
            ? +(stat.size / (1024 * 1024)).toFixed(2)
            : null,
          createdAt: stat.birthtime || stat.ctime,
          modifiedAt: stat.mtime,
          accessedAt: stat.atime,
        };
      } catch (e) {
        return {
          name: item,
          error: e.message,
        };
      }
    });

    res.json({
      path: folderPath,
      totalItems: result.length,
      items: result,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// =======================================================
// 5️⃣ DETECT DUPLICATES
// =======================================================
// app.post("/detect-duplicates", async (req, res) => {
//   try {
//     const { detectAndSaveDuplicates } = require("./services/detectDuplicates");

//     const startTime = Date.now();
//     const result = await detectAndSaveDuplicates();
//     const duration = ((Date.now() - startTime) / 1000).toFixed(2);

//     res.json({
//       message: "Duplicate detection completed",
//       stats: {
//         totalDuplicateGroups: result.totalGroups,
//         totalDuplicateFiles: result.totalDuplicateFiles,
//         duration: `${duration}s`,
//       },
//     });
//   } catch (err) {
//     res.status(500).json({ error: err.message });
//   }
// });

app.post("/detect-duplicates", async (req, res) => {
  const { detectAndSaveDuplicates } = require("./services/detectDuplicates");
  // 1️⃣ Turant response
  res.json({
    message: "Duplicate detection started in background",
  });

  // 2️⃣ Background execution
  setImmediate(async () => {
    try {
      console.log("🚀 Duplicate detection started...");
      await detectAndSaveDuplicates();
      console.log("✅ Duplicate detection finished");
    } catch (err) {
      console.error("❌ Duplicate detection failed:", err.message);
    }
  });
});

// =======================================================
// 6️⃣ GET DUPLICATES
// =======================================================
app.get("/duplicates", async (req, res) => {
  try {
    const { getDuplicates } = require("./services/detectDuplicates");

    const { limit = 100, skip = 0, minCount = 2, extension, drive } = req.query;

    const result = await getDuplicates({
      limit: parseInt(limit),
      skip: parseInt(skip),
      minCount: parseInt(minCount),
      extension,
      drive,
    });

    res.json(result);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// =======================================================
// SERVER START
// =======================================================
app.listen(3001, "0.0.0.0");
