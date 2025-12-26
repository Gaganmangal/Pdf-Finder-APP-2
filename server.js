const express = require("express");
const connectDB = require("./db");
const scanDrive = require("./scanner");

const app = express();
app.use(express.json());

connectDB();

app.post("/scan-d-drive", async (req, res) => {
  try {
    console.log("\n🚀 Starting D drive scan...");
    const FileMeta = require("./models/FileMeta");
    const countBefore = await FileMeta.countDocuments();

    await scanDrive("D:/", "D"); // Laptop test

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
      return res.status(400).json({ error: "Path is required in request body" });
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

// 🔥 THIS ROUTE WAS MISSING (404 fix)
app.get("/files", async (req, res) => {
  const FileMeta = require("./models/FileMeta");
  const files = await FileMeta.find().limit(50);
  res.json(files);
});

app.listen(3001, () => {
  console.log("Backend running on port 3001");
});
