const mongoose = require("mongoose");
const connectDB = require("./db");

async function dropIndex() {
  try {
    await connectDB();
    
    const FileMeta = require("./models/FileMeta");
    
    // Get all indexes
    const indexes = await FileMeta.collection.getIndexes();
    console.log("Current indexes:", indexes);
    
    // Drop unique index on fullPath if it exists
    try {
      await FileMeta.collection.dropIndex("fullPath_1");
      console.log("✅ Dropped unique index on fullPath");
    } catch (err) {
      if (err.code === 27) {
        console.log("ℹ️  Index on fullPath doesn't exist");
      } else {
        console.error("Error dropping index:", err.message);
      }
    }
    
    // Verify indexes after drop
    const indexesAfter = await FileMeta.collection.getIndexes();
    console.log("Indexes after drop:", indexesAfter);
    
    process.exit(0);
  } catch (err) {
    console.error("Error:", err);
    process.exit(1);
  }
}

dropIndex();

