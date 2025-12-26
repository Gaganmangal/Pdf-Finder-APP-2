const mongoose = require("mongoose");

async function connectDB() {
  try {
    await mongoose.connect(
      "mongodb+srv://Gaganfnr:ndLz9yHCsOmv9S3k@gagan.jhuti8y.mongodb.net/test?appName=Gagan"
    );

    console.log("MongoDB Atlas connected");
  } catch (err) {
    console.error("MongoDB connection failed:", err.message);
    process.exit(1);
  }
}

module.exports = connectDB;
