// Idempotent migration runner — executes schema.sql against the configured DB.
const fs = require('fs');
const path = require('path');
const mysql = require('mysql2/promise');
const config = require('../config');

async function main() {
  const sql = fs.readFileSync(path.join(__dirname, '..', '..', 'schema.sql'), 'utf8');
  const conn = await mysql.createConnection({
    host: config.db.host,
    port: config.db.port,
    user: config.db.user,
    password: config.db.password,
    multipleStatements: true,
  });
  await conn.query(`CREATE DATABASE IF NOT EXISTS \`${config.db.database}\` DEFAULT CHARACTER SET utf8mb4`);
  await conn.query(`USE \`${config.db.database}\``);
  // Enable Barracuda + large prefix so utf8mb4 VARCHAR(>=192) indexed columns
  // don't trip MariaDB 10.1 / MySQL 5.6 default 767-byte key length limit.
  // These are no-ops on newer servers (defaults are already DYNAMIC + 3072).
  const tune = [
    "SET GLOBAL innodb_file_format = 'Barracuda'",
    'SET GLOBAL innodb_file_per_table = 1',
    'SET GLOBAL innodb_large_prefix = 1',
    'SET SESSION innodb_strict_mode = 0',
    "SET GLOBAL innodb_default_row_format = 'DYNAMIC'",
  ];
  for (const stmt of tune) {
    try { await conn.query(stmt); }
    catch (e) { console.warn(`[migrate] skip "${stmt}":`, e.code || e.message); }
  }

  // Apply schema statement-by-statement so a failing CREATE TABLE names
  // itself instead of just bubbling up "key too long" with no context.
  // Strip line comments before checking emptiness so chunks that begin
  // with a `-- header` block above CREATE TABLE still execute.
  const stmts = sql
    .split(/;\s*(?:\r?\n|$)/)
    .map((s) => s.replace(/^\s*--.*$/gm, '').trim())
    .filter((s) => s.length > 0);
  for (const stmt of stmts) {
    try { await conn.query(stmt); }
    catch (e) {
      const head = stmt.slice(0, 80).replace(/\s+/g, ' ');
      console.error(`[migrate] statement failed: ${head}...`);
      throw e;
    }
  }

  // Idempotent index patches for existing installs (CREATE TABLE IF NOT
  // EXISTS won't add new keys to a pre-existing table).
  const patches = [
    { table: 'orders', name: 'idx_orders_user_updated', cols: '(user_id, updated_at)' },
  ];
  for (const p of patches) {
    const [rows] = await conn.query(
      `SELECT 1 FROM information_schema.STATISTICS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_NAME = ? LIMIT 1`,
      [config.db.database, p.table, p.name],
    );
    if (!rows.length) {
      await conn.query(`ALTER TABLE \`${p.table}\` ADD KEY \`${p.name}\` ${p.cols}`);
      console.log(`[migrate] patched index ${p.table}.${p.name}`);
    }
  }

  // Idempotent column patches for existing installs.
  const colPatches = [
    { table: 'premium_trigger_config', column: 'state', def: 'JSON DEFAULT NULL AFTER config' },
    { table: 'premium_trigger_signals', column: 'mode',
      def: "ENUM('live','paper') NOT NULL DEFAULT 'live' AFTER strike" },
  ];
  for (const p of colPatches) {
    const [exists] = await conn.query(
      `SELECT 1 FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? LIMIT 1`,
      [config.db.database, p.table],
    );
    if (!exists.length) continue;
    const [cols] = await conn.query(
      `SELECT 1 FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ? LIMIT 1`,
      [config.db.database, p.table, p.column],
    );
    if (!cols.length) {
      await conn.query(`ALTER TABLE \`${p.table}\` ADD COLUMN \`${p.column}\` ${p.def}`);
      console.log(`[migrate] patched column ${p.table}.${p.column}`);
    }
  }
  await conn.end();
  console.log(`[migrate] schema applied to ${config.db.database}`);
}

main().catch((err) => {
  console.error('[migrate] failed:', err.message);
  process.exit(1);
});
