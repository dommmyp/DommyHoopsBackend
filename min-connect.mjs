// backend/min-connect.mjs
import duckdb from "duckdb";
import { createRequire } from "node:module";
const require = createRequire(import.meta.url);
const { version } = require("duckdb/package.json");

console.log("node:", process.version);
console.log("duckdb pkg:", version);

const db = new duckdb.Database(":memory:");
db.connect((err, c) => {
  if (err) return console.error("connect failed:", err);
  c.all("SELECT 42 AS x", (e, rows) => {
    if (e) return console.error("query failed:", e);
    console.log("ok rows:", rows);
    process.exit(0);
  });
});

setTimeout(() => console.error("connect timed out"), 5000);

