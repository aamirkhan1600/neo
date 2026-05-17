const { query } = require('../db/pool');

async function log(userId, eventType, level, message, meta = null) {
  try {
    await query(
      `INSERT INTO event_log (user_id, event_type, level, message, meta) VALUES (?, ?, ?, ?, ?)`,
      [userId, eventType, level, (message || '').slice(0, 500), meta ? JSON.stringify(meta) : null],
    );
  } catch (_) { /* never let audit logging crash callers */ }
}

module.exports = { log };
