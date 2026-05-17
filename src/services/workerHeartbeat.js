// Worker liveness: each worker pings worker_heartbeat every few seconds so
// operators can detect a silently-dead worker before stale-lock reaping
// kicks in. Also exposes a query to list live workers.

const { query } = require('../db/pool');
const logger = require('../utils/logger');

const PING_MS = 5000;
const STALE_AFTER_MS = 30000;

let timer = null;
let workerId = null;

async function ping(meta = null) {
  await query(
    `INSERT INTO worker_heartbeat (worker_id, last_seen, started_at, meta)
     VALUES (?, NOW(), NOW(), ?)
     ON DUPLICATE KEY UPDATE last_seen = NOW(), meta = VALUES(meta)`,
    [workerId, meta ? JSON.stringify(meta) : null],
  );
}

function start(id, meta = null) {
  workerId = id;
  if (timer) return;
  ping(meta).catch((err) => logger.warn('heartbeat: initial ping failed', { err: err.message }));
  timer = setInterval(() => {
    ping(meta).catch((err) => logger.warn('heartbeat: ping failed', { err: err.message }));
  }, PING_MS);
  timer.unref();
}

function stop() {
  if (timer) clearInterval(timer);
  timer = null;
}

async function listLive() {
  const rows = await query(
    `SELECT worker_id, last_seen, started_at FROM worker_heartbeat
      WHERE last_seen > DATE_SUB(NOW(), INTERVAL ? SECOND)`,
    [Math.ceil(STALE_AFTER_MS / 1000)],
  );
  return rows;
}

module.exports = { start, stop, ping, listLive };
