const { MongoClient } = require("mongodb");

const MONGO_URI =
  "mongodb+srv://Gaganfnr:ndLz9yHCsOmv9S3k@gagan.jhuti8y.mongodb.net/test?appName=Gagan"; // Safely store credentials in secrets/.env in production

async function detectDuplicates() {
  const client = new MongoClient(MONGO_URI);
  await client.connect();

  const db = client.db();
  const FileMeta = db.collection("FileMeta");
  // IMPORTANT: Use only native MongoDB driver handle here. DO NOT use Mongoose model!
  const DuplicateFile = db.collection("DuplicateFile");

  // Optionally clear previous results for simple reruns
  await DuplicateFile.deleteMany({});

  // Step 2/3: Count-only aggregation, streaming cursor
  const cursor = FileMeta.aggregate(
    [
      { $group: { _id: "$fingerprint", count: { $sum: 1 } } },
      { $match: { count: { $gt: 1 } } },
    ],
    { allowDiskUse: true }
  );

  let groups = 0;
  for await (const { _id: fingerprint, count } of cursor) {
    // Step 4: Indexed, fast fetch per fingerprint
    const files = await FileMeta.find({ fingerprint })
      .project({
        path: 1,
        name: 1,
        ext: 1,
        size: 1,
        modifiedAt: 1,
        scanTime: 1,
        _id: 0,
      })
      .toArray();

    await DuplicateFile.insertOne({
      fingerprint,
      count,
      files,
      detectedAt: new Date(),
    });

    groups++;
    if (groups % 500 === 0) {
      console.log(`Processed ${groups} duplicate groups`);
    }
  }

  console.log("✅ Duplicate detection finished");
  await client.close();
}

detectDuplicates().catch(console.error);
