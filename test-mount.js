const fs = require("fs");
const path = require("path");

// Test script to verify network mount is accessible
const mountPoint = "/mnt/pdfs";

console.log("🔍 Testing network mount...\n");

// Check if mount point exists
if (!fs.existsSync(mountPoint)) {
  console.error(`❌ Mount point ${mountPoint} does not exist!`);
  console.log("💡 Run: sudo mkdir -p /mnt/pdfs");
  process.exit(1);
}

// Check if it's a directory
try {
  const stats = fs.statSync(mountPoint);
  if (!stats.isDirectory()) {
    console.error(`❌ ${mountPoint} is not a directory!`);
    process.exit(1);
  }
} catch (err) {
  console.error(`❌ Cannot access ${mountPoint}:`, err.message);
  process.exit(1);
}

// Try to read directory
try {
  const items = fs.readdirSync(mountPoint);
  console.log(`✅ Mount point is accessible!`);
  console.log(`📁 Found ${items.length} items in ${mountPoint}\n`);
  
  if (items.length > 0) {
    console.log("📋 First 10 items:");
    items.slice(0, 10).forEach((item, index) => {
      const itemPath = path.join(mountPoint, item);
      try {
        const stat = fs.statSync(itemPath);
        const type = stat.isDirectory() ? "📁 Directory" : "📄 File";
        const size = stat.isFile() ? ` (${(stat.size / 1024).toFixed(2)} KB)` : "";
        console.log(`   ${index + 1}. ${type}: ${item}${size}`);
      } catch (err) {
        console.log(`   ${index + 1}. ❌ ${item} (error reading)`);
      }
    });
  }
  
  console.log("\n✅ Network mount is ready for scanning!");
} catch (err) {
  console.error(`❌ Cannot read directory ${mountPoint}:`, err.message);
  console.log("\n💡 Possible issues:");
  console.log("   1. Mount point is not mounted");
  console.log("   2. Permission issues");
  console.log("   3. Network connection problem");
  console.log("\n💡 Try mounting again:");
  console.log("   sudo mount -t cifs //172.31.3.171/SharedData /mnt/pdfs \\");
  console.log("     -o username='Administrator',domain=WORKGROUP,password='7JxSwzcJ%2!sW*fmL73z-hDrcgs4QlIi@123',vers=3.0,sec=ntlmssp");
  process.exit(1);
}

