// MySQL/MariaDB-backed job queue. Replaces Redis/BullMQ.
//
// Locking strategy: atomic UPDATE ... ORDER BY ... LIMIT with a unique
// per-call claim token, then SELECT the rows back by that token. This
// avoids `FOR UPDATE SKIP LOCKED`, which is unavailable on MariaDB < 10.6.
// Stale-lock recovery sweeps reap jobs whose locked_at is older than
// JOB_LOCK_TIMEOUT_MS in case a worker dies mid-job.

const { pool, query, txn } = require('../db/pool');
const config = require('../config');
const logger = require('../utils/logger');

const VALID_TYPES = ['TRADE', 'STRATEGY', 'MARGIN', 'BASKET', 'SYNC_ORDERS', 'SYNC_TRADES', 'RELOGIN'];

async function enqueue({ userId = null, type, payload = {}, priority = 5, maxRetries = config.worker.maxRetries, runAt = null }) {
  if (!VALID_TYPES.includes(type)) throw new Error(`Invalid job_type: ${type}`);
  const sql = `
    INSERT INTO job_queue (user_id, job_type, priority, payload, status, retry_count, max_retries, next_run_at)
    VALUES (?, ?, ?, ?, 'PENDING', 0, ?, COALESCE(?, NOW()))`;
  const result = await query(sql, [userId, type, priority, JSON.stringify(payload), maxRetries, runAt]);
  return result.insertId;
}

// Atomically claim up to `batch` jobs for this worker.
//
// We mark eligible rows with a unique claim token via UPDATE ... ORDER BY ...
// LIMIT, then read them back by that token. InnoDB serializes the UPDATEs on
// the contended rows, so two workers cannot claim the same job. This works on
// MariaDB versions that lack `FOR UPDATE SKIP LOCKED` (< 10.6).
async function claim(workerId, batch = config.worker.batchSize, allowedTypes = null) {
  const claimToken = `${workerId}#${Date.now()}#${Math.random().toString(36).slice(2, 10)}`;
  const typeFilter = allowedTypes && allowedTypes.length
    ? `AND job_type IN (${allowedTypes.map(() => '?').join(',')})`
    : '';

  return txn(async (conn) => {
    const params = [claimToken, 'PENDING', new Date()];
    if (allowedTypes && allowedTypes.length) params.push(...allowedTypes);
    params.push(batch);

    const [updateResult] = await conn.query(
      `UPDATE job_queue
       SET status = 'PROCESSING', locked_by = ?, locked_at = NOW()
       WHERE status = ? AND next_run_at <= ? ${typeFilter}
       ORDER BY priority ASC, next_run_at ASC, id ASC
       LIMIT ?`,
      params,
    );
    if (!updateResult.affectedRows) return [];

    const [jobs] = await conn.execute(
      `SELECT * FROM job_queue WHERE locked_by = ? AND status = 'PROCESSING'`,
      [claimToken],
    );
    return jobs.map(j => ({
      ...j,
      payload: typeof j.payload === 'string' ? JSON.parse(j.payload) : j.payload,
    }));
  });
}

async function complete(jobId, result) {
  await query(
    `UPDATE job_queue SET status='SUCCESS', result_json=?, locked_by=NULL, locked_at=NULL
     WHERE id = ?`,
    [JSON.stringify(result || {}), jobId],
  );
}

// Reschedule with exponential backoff per spec: 5s * retry_count.
async function fail(jobId, err, opts = {}) {
  const { rescheduleSeconds } = opts;
  const message = (err && err.message) ? err.message.slice(0, 1000) : String(err).slice(0, 1000);

  return txn(async (conn) => {
    const [rows] = await conn.execute(
      'SELECT retry_count, max_retries FROM job_queue WHERE id = ? FOR UPDATE',
      [jobId],
    );
    if (!rows.length) return;
    const { retry_count, max_retries } = rows[0];
    const next = retry_count + 1;

    if (next > max_retries) {
      await conn.execute(
        `UPDATE job_queue
         SET status='FAILED', retry_count=?, last_error=?, locked_by=NULL, locked_at=NULL
         WHERE id = ?`,
        [next, message, jobId],
      );
      return;
    }

    const delay = rescheduleSeconds != null ? rescheduleSeconds : Math.min(5 * next, 120);
    await conn.execute(
      `UPDATE job_queue
       SET status='PENDING', retry_count=?, last_error=?,
           next_run_at = DATE_ADD(NOW(), INTERVAL ? SECOND),
           locked_by=NULL, locked_at=NULL
       WHERE id = ?`,
      [next, message, delay, jobId],
    );
  });
}

// Unlock without retry — used when rate-limited and we just want to push next_run_at out.
async function reschedule(jobId, seconds = 1) {
  await query(
    `UPDATE job_queue
     SET status='PENDING', next_run_at = DATE_ADD(NOW(), INTERVAL ? SECOND),
         locked_by=NULL, locked_at=NULL
     WHERE id = ?`,
    [seconds, jobId],
  );
}

// Reap stale locks (worker crashed mid-job).
async function reapStaleLocks() {
  const sql = `
    UPDATE job_queue
    SET status='PENDING', locked_by=NULL, locked_at=NULL,
        last_error=CONCAT(COALESCE(last_error,''), ' [reaped stale lock]')
    WHERE status='PROCESSING'
      AND locked_at < DATE_SUB(NOW(), INTERVAL ? SECOND)`;
  const r = await query(sql, [Math.ceil(config.worker.lockTimeoutMs / 1000)]);
  if (r.affectedRows) logger.warn('reaped stale locks', { count: r.affectedRows });
  return r.affectedRows;
}

// Move permanently failed jobs to DEAD letter status (idempotent).
async function markDead(jobId, reason) {
  await query(
    `UPDATE job_queue SET status='DEAD', last_error=? WHERE id = ? AND status='FAILED'`,
    [String(reason || 'manual').slice(0, 1000), jobId],
  );
}

async function retryDead(jobId) {
  await query(
    `UPDATE job_queue
     SET status='PENDING', retry_count=0, next_run_at=NOW(), locked_by=NULL, locked_at=NULL, last_error=NULL
     WHERE id = ? AND status IN ('FAILED','DEAD')`,
    [jobId],
  );
}

async function stats() {
  const rows = await query(
    `SELECT status, COUNT(*) AS n FROM job_queue GROUP BY status`,
  );
  return rows.reduce((acc, r) => (acc[r.status] = Number(r.n), acc), {});
}

// Delete old SUCCESS rows to keep the queue table small. FAILED / DEAD are
// retained so operators can inspect failures.
async function purge({ successDays = 7, deadDays = 30 } = {}) {
  const r1 = await query(
    `DELETE FROM job_queue WHERE status = 'SUCCESS' AND updated_at < DATE_SUB(NOW(), INTERVAL ? DAY)`,
    [successDays],
  );
  const r2 = await query(
    `DELETE FROM job_queue WHERE status = 'DEAD' AND updated_at < DATE_SUB(NOW(), INTERVAL ? DAY)`,
    [deadDays],
  );
  return { success: r1.affectedRows, dead: r2.affectedRows };
}

module.exports = {
  enqueue, claim, complete, fail, reschedule,
  reapStaleLocks, markDead, retryDead, stats, purge,
  VALID_TYPES,
};
