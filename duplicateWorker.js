const { MongoClient } = require('mongodb');

const MONGO_URI = 'YOUR_MONGO_URI';
const BATCH_SIZE = 10000; // fingerprint scan window size

async function detectDuplicates() {
  const client = new MongoClient(MONGO_URI);
  await client.connect();
  const db = client.db();
  const FileMeta = db.collection('FileMeta');
  const DuplicateFile = db.collection('DuplicateFile');

  // Find all non-unique fingerprints in batches
  let lastFingerprint = null;
  while (true) {
    // Get next batch of fingerprints (sorted)
    const fingerprints = await FileMeta
      .aggregate([
        { $match: lastFingerprint ? { fingerprint: { $gt: lastFingerprint } } : {} },
        { $group: { _id: '$fingerprint', count: { $sum: 1 } } },
        { $match: { count: { $gt: 1 } } },
        { $sort: { _id: 1 } },
        { $limit: BATCH_SIZE }
      ], { allowDiskUse: true })
      .toArray();

    if (!fingerprints.length) break;

    for (const { _id: fingerprint, count } of fingerprints) {
      // Get only IDs/files for this fingerprint
      const dupFiles = await FileMeta
        .find({ fingerprint })
        .project({ _id: 1, path: 1, name: 1, ext: 1, size: 1, modifiedAt: 1 })
        .toArray();

      await DuplicateFile.insertOne({
        fingerprint,
        count,
        files: dupFiles,
        detectedAt: new Date(),
      });

      lastFingerprint = fingerprint;
    }
  }

  await client.close();
  console.log('Duplication analysis finished');
}

detectDuplicates().catch(console.error);

