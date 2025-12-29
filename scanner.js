const fs = require("fs/promises");
const path = require("path");
const { MongoClient } = require("mongodb");

const MONGO_URI = "YOUR_MONGO_URI";
const FILE_ROOT = "/mnt/windows_share"; // Update as needed
const BATCH_SIZE = 1000;

function getFingerprint({ name, ext, size }) {
  return `${name}|${ext}|${size}`;
}

async function* walk(dir) {
  for (const entry of await fs.readdir(dir, { withFileTypes: true })) {
    const abs = path.join(dir, entry.name);
    if (entry.isDirectory()) yield* walk(abs);
    else yield abs;
  }
}

async function scanAndInsert() {
  const client = new MongoClient(MONGO_URI);
  await client.connect();
  const db = client.db();
  const FileMeta = db.collection("FileMeta");
  const scanTime = new Date();

  let batch = [];
  for await (const filePath of walk(FILE_ROOT)) {
    const stat = await fs.stat(filePath);
    const ext = path.extname(filePath);
    const name = path.basename(filePath, ext);
    const size = stat.size;
    const modifiedAt = stat.mtime;
    const fingerprint = getFingerprint({ name, ext, size });

    batch.push({
      path: filePath,
      name,
      ext,
      size,
      modifiedAt,
      scanTime,
      fingerprint,
    });

    if (batch.length >= BATCH_SIZE) {
      await FileMeta.insertMany(batch);
      batch = [];
    }
  }

  if (batch.length) await FileMeta.insertMany(batch);

  console.log("Scan finished at", new Date());
  await client.close();
}

scanAndInsert().catch(console.error);
