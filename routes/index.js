const express = require("express");
const router = express.Router();
const { indexFiles } = require("../services/indexFiles");

router.post("/index", async (req, res) => {
  try {
    await indexFiles("D:/PDFS");
    res.json({ message: "Indexing completed" });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;
