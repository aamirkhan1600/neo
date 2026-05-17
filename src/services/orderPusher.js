// MySQL polling bridge: detects newly updated orders/trades for *online*
// users and pushes them via socket.io. Replaces a Redis pub/sub channel.
//
// The worker process writes authoritative state to MySQL. The app process
// (which holds the socket.io connections) polls every POLL_MS and emits
// 'order_update' / 'trade' events to the relevant user's room.

const { query } = require('../db/pool');
const logger = require('../utils/logger');

const POLL_MS = 1000;

const online = new Map();      // userId -> refcount (sockets per user)
const cursors = new Map();     // userId -> { ordersSince, tradesSince, dirty }

let timer = null;
let saveTimer = null;
let socketServer = null;

function _now() { return new Date(); }

async function _loadCursor(userId) {
  const rows = await query(
    `SELECT orders_at, trades_at FROM user_push_cursor WHERE user_id = ?`,
    [userId],
  );
  if (rows.length) return { ordersSince: rows[0].orders_at, tradesSince: rows[0].trades_at };
  // First time: start "now" so we don't replay historical updates.
  const now = _now();
  await query(
    `INSERT IGNORE INTO user_push_cursor (user_id, orders_at, trades_at) VALUES (?, ?, ?)`,
    [userId, now, now],
  );
  return { ordersSince: now, tradesSince: now };
}

async function _saveCursor(userId) {
  const c = cursors.get(userId);
  if (!c || !c.dirty) return;
  c.dirty = false;
  try {
    await query(
      `INSERT INTO user_push_cursor (user_id, orders_at, trades_at) VALUES (?, ?, ?)
       ON DUPLICATE KEY UPDATE orders_at = VALUES(orders_at), trades_at = VALUES(trades_at)`,
      [userId, c.ordersSince, c.tradesSince],
    );
  } catch (err) {
    logger.warn('orderPusher: cursor save failed', { userId, err: err.message });
  }
}

async function markOnline(userId) {
  const next = (online.get(userId) || 0) + 1;
  online.set(userId, next);
  if (next === 1) {
    try {
      const c = await _loadCursor(userId);
      cursors.set(userId, { ...c, dirty: false });
    } catch (err) {
      // Fall back to NOW() if DB read fails.
      cursors.set(userId, { ordersSince: _now(), tradesSince: _now(), dirty: false });
      logger.warn('orderPusher: cursor load failed', { userId, err: err.message });
    }
  }
}

async function markOffline(userId) {
  const next = (online.get(userId) || 0) - 1;
  if (next <= 0) {
    online.delete(userId);
    await _saveCursor(userId);
    cursors.delete(userId);
  } else {
    online.set(userId, next);
  }
}

async function _tickOnce() {
  if (!online.size || !socketServer) return;

  const userIds = [...online.keys()];
  // Build IN clause; small set (online users) so this is fine.
  const inSql = userIds.map(() => '?').join(',');

  // Find max(updated_at) per user we need — use separate queries because
  // each user has its own cursor.
  for (const userId of userIds) {
    const cur = cursors.get(userId);
    if (!cur) continue;

    // Orders
    try {
      const rows = await query(
        `SELECT id, user_id, broker_order_id, symbol, exchange, side, qty, filled_qty,
                product, order_type, price, status, reject_reason, updated_at
         FROM orders
         WHERE user_id = ? AND updated_at > ?
         ORDER BY updated_at ASC
         LIMIT 50`,
        [userId, cur.ordersSince],
      );
      if (rows.length) {
        cur.ordersSince = rows[rows.length - 1].updated_at;
        cur.dirty = true;
        for (const r of rows) socketServer.emitToUser(userId, 'order_update', r);
      }
    } catch (err) {
      logger.error('orderPusher: orders poll failed', { userId, err: err.message });
    }

    // Trades
    try {
      const rows = await query(
        `SELECT id, user_id, broker_trade_id, broker_order_id, symbol, exchange,
                side, qty, price, created_at
         FROM trades
         WHERE user_id = ? AND created_at > ?
         ORDER BY created_at ASC
         LIMIT 50`,
        [userId, cur.tradesSince],
      );
      if (rows.length) {
        cur.tradesSince = rows[rows.length - 1].created_at;
        cur.dirty = true;
        for (const r of rows) socketServer.emitToUser(userId, 'trade', r);
      }
    } catch (err) {
      logger.error('orderPusher: trades poll failed', { userId, err: err.message });
    }
  }
  // Suppress unused-var lint when there are no online users.
  void inSql;
}

function start(srv) {
  socketServer = srv;
  if (timer) return;
  timer = setInterval(() => { _tickOnce().catch(() => {}); }, POLL_MS);
  timer.unref();
  // Persist dirty cursors every 5 s so a crashed app doesn't replay too much
  // on restart, and online users don't miss updates that happened mid-session.
  saveTimer = setInterval(() => {
    for (const userId of cursors.keys()) _saveCursor(userId).catch(() => {});
  }, 5000);
  saveTimer.unref();
  logger.info('orderPusher started', { pollMs: POLL_MS });
}

function stop() {
  if (timer) clearInterval(timer);
  if (saveTimer) clearInterval(saveTimer);
  timer = saveTimer = null;
}

async function flush() {
  for (const userId of cursors.keys()) await _saveCursor(userId);
}

module.exports = { start, stop, flush, markOnline, markOffline };
