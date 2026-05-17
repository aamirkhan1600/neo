const { query } = require('../db/pool');
const jobQueue = require('./jobQueue');
const orderService = require('./orderService');

async function create(userId, name, legs) {
  if (!Array.isArray(legs) || !legs.length) throw new Error('basket requires legs');
  legs.forEach(orderService.validate);
  const r = await query(
    `INSERT INTO baskets (user_id, name, status, legs_json) VALUES (?, ?, 'DRAFT', ?)`,
    [userId, name, JSON.stringify(legs)],
  );
  return r.insertId;
}

async function execute(userId, basketId) {
  const rows = await query(
    `SELECT * FROM baskets WHERE id = ? AND user_id = ? LIMIT 1`,
    [basketId, userId],
  );
  if (!rows.length) { const e = new Error('basket_not_found'); e.status = 404; throw e; }
  const legs = typeof rows[0].legs_json === 'string' ? JSON.parse(rows[0].legs_json) : rows[0].legs_json;
  await query(`UPDATE baskets SET status = 'EXECUTING' WHERE id = ?`, [basketId]);
  const jobId = await jobQueue.enqueue({
    userId,
    type: 'BASKET',
    priority: 2,
    payload: { basketId, legs },
  });
  return { basketId, jobId };
}

async function list(userId) {
  return query(`SELECT id, name, status, created_at FROM baskets WHERE user_id = ? ORDER BY id DESC`, [userId]);
}

async function setStatus(basketId, status) {
  await query(`UPDATE baskets SET status = ? WHERE id = ?`, [status, basketId]);
}

module.exports = { create, execute, list, setStatus };
