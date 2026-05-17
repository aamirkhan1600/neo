const { query } = require('../db/pool');
const jobQueue = require('./jobQueue');

const ORDER_TYPES = new Set(['MKT', 'L', 'SL', 'SL-M']);
const SIDES = new Set(['BUY', 'SELL']);
const PRODUCTS = new Set(['CNC', 'MIS', 'NRML', 'CO', 'BO', 'MTF']);

// User-friendly exchange -> Kotak Neo `es` segment value.
// Already-segmented values (nse_cm, etc.) pass through unchanged so callers
// can supply either form.
const EXCHANGE_TO_SEGMENT = {
  NSE: 'nse_cm', BSE: 'bse_cm',
  NFO: 'nse_fo', BFO: 'bse_fo',
  CDS: 'cde_fo', MCX: 'mcx_fo',
  nse_cm: 'nse_cm', bse_cm: 'bse_cm',
  nse_fo: 'nse_fo', bse_fo: 'bse_fo',
  cde_fo: 'cde_fo', mcx_fo: 'mcx_fo',
};

function exchangeToSegment(ex) {
  return EXCHANGE_TO_SEGMENT[ex] || EXCHANGE_TO_SEGMENT[String(ex || '').toUpperCase()] || null;
}

function validate(o) {
  if (!o.symbol) throw new Error('symbol (instrument token) required');
  if (!o.tradingSymbol) throw new Error('tradingSymbol required (e.g. RELIANCE-EQ)');
  if (!o.exchange) throw new Error('exchange required');
  if (!exchangeToSegment(o.exchange)) throw new Error(`unsupported exchange ${o.exchange}`);
  if (!SIDES.has(o.side)) throw new Error('invalid side');
  if (!ORDER_TYPES.has(o.orderType)) throw new Error('invalid orderType');
  if (!PRODUCTS.has(o.product)) throw new Error('invalid product');
  if (!Number.isInteger(o.qty) || o.qty <= 0) throw new Error('invalid qty');
  if (o.orderType === 'L' && !(o.price > 0)) throw new Error('price required for limit orders');
  if ((o.orderType === 'SL' || o.orderType === 'SL-M') && !(o.triggerPrice > 0)) {
    throw new Error('triggerPrice required for stop orders');
  }
}

async function createPendingOrder(userId, o, strategyId = null) {
  validate(o);
  const r = await query(
    `INSERT INTO orders
      (user_id, strategy_id, symbol, exchange, side, qty, product, order_type, price, trigger_price, status)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING')`,
    [userId, strategyId, o.symbol, o.exchange, o.side, o.qty, o.product, o.orderType,
     o.price || null, o.triggerPrice || null],
  );
  return r.insertId;
}

async function enqueueTrade(userId, order, opts = {}) {
  const orderId = await createPendingOrder(userId, order, opts.strategyId || null);
  const jobId = await jobQueue.enqueue({
    userId,
    type: 'TRADE',
    priority: 1,
    payload: { orderId, order },
  });
  return { orderId, jobId };
}

async function updateFromBrokerResponse(orderId, response) {
  // Kotak Neo place-order success: { nOrdNo, stat: 'Ok', stCode: 200 }
  // Failure:                       { stat: 'Not_Ok', emsg: '...', stCode: 1004 }
  const brokerOrderId = response?.nOrdNo || null;
  const ok = response?.stat === 'Ok';
  const status = ok ? 'PLACED' : 'REJECTED';
  const reject = !ok ? (response?.emsg || `stCode=${response?.stCode}`) : null;
  await query(
    `UPDATE orders SET broker_order_id = ?, status = ?, reject_reason = ?, raw_response = ?, placed_at = NOW()
     WHERE id = ?`,
    [brokerOrderId, status, reject ? String(reject).slice(0, 255) : null, JSON.stringify(response || {}), orderId],
  );
}

async function markRejected(orderId, reason, raw = null) {
  await query(
    `UPDATE orders SET status = 'REJECTED', reject_reason = ?, raw_response = ? WHERE id = ?`,
    [String(reason).slice(0, 255), raw ? JSON.stringify(raw) : null, orderId],
  );
}

async function listForUser(userId, { limit = 100, offset = 0 } = {}) {
  return query(
    `SELECT * FROM orders WHERE user_id = ? ORDER BY id DESC LIMIT ? OFFSET ?`,
    [userId, limit, offset],
  );
}

module.exports = { validate, createPendingOrder, enqueueTrade, updateFromBrokerResponse, markRejected, listForUser, exchangeToSegment };
