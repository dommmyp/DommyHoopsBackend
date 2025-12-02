// check-db.js
// Run: node check-db.js
// Checks that your DuckDB file can open and return tables.

import { execFile } from "node:child_process";
import fs from "node:fs";

const DB_PATH = "/Users/dominicparolin/Code/dommyhoops/backend/cbb_data.duckdb";
const DUCKDB_BIN = "duckdb"; // assumes Homebrew's duckdb is in PATH

if (!fs.existsSync(DB_PATH)) {
  console.error("❌ Database file not found:", DB_PATH);
  process.exit(1);
}

// Test SQL — note: no semicolon inside COPY (...)
const sql = `
  SELECT table_schema, table_name
  FROM information_schema.tables
  ORDER BY 1,2
  LIMIT 10
`;

const args = [DB_PATH, "-c", `COPY (${sql}) TO '/dev/stdout' (FORMAT JSON);`];

execFile(DUCKDB_BIN, args, { maxBuffer: 10 * 1024 * 1024 }, (err, stdout, stderr) => {
  if (err) {
    console.error("❌ Error running DuckDB query:");
    console.error(stderr || err.message);
    process.exit(1);
  }

  try {
    const rows = stdout
      .trim()
      .split(/\r?\n/)
      .filter(Boolean)
      .map((line) => JSON.parse(line));

    if (rows.length === 0) {
      console.warn("⚠️ Database opened but returned no tables.");
    } else {
      console.log("✅ Database opened successfully!");
      console.log(`Found ${rows.length} tables. Example:`);
      console.table(rows.slice(0, 5));
    }
  } catch (e) {
    console.error("❌ Failed to parse DuckDB output:", e.message);
    console.error(stdout);
    process.exit(1);
  }
});

