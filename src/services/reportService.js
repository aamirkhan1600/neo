const { query } = require('../db/pool');
const brokerService = require('./brokerService');

// ---------------------------------------------------------------------------
// Order book / Trade book sync — field names from Kotak Neo Trade API docs.
// ---------------------------------------------------------------------------
//
// Order Book row:
//   { nOrdNo, ordSt, trdSym, qty, fldQty?, prc, avgPrc, trnsTp, prcTp,
//     vldt, exSeg, prod, ordGenTp, ordDtTm, rejRsn }
//
// Trade Book row:
//   { nOrdNo, trdSym, qty, fldQty, avgPrc, flDt, exOrdId, exTm,
//     prcTp, prod, ordDur, trnsTp, exSeg, exOrdId }
//
// Both endpoints return { stat:'Ok', stCode:200, data:[...] } on success.

function side(t) { return (t === 'B' || t === 'b') ? 'BUY' : 'SELL'; }

async function syncOrders(userId) {
  const res = await brokerService.fetchOrderBook(userId);
  if (res?.stat && res.stat !== 'Ok') return 0;
  const list = res?.data || [];
  for (const o of list) {
    const brokerOrderId = o.nOrdNo;
    if (!brokerOrderId) continue;
    await query(
      `INSERT INTO orders
        (user_id, broker_order_id, symbol, exchange, side, qty, filled_qty,
         product, order_type, price, status, raw_response, placed_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW())
       ON DUPLICATE KEY UPDATE
         filled_qty = VALUES(filled_qty),
         status = VALUES(status),
         raw_response = VALUES(raw_response)`,
      [
        userId,
        String(brokerOrderId),
        o.trdSym || '',
        o.exSeg || '',
        side(o.trnsTp),
        parseInt(o.qty || 0, 10),
        parseInt(o.fldQty || o.filledQty || 0, 10),
        o.prod || '',
        o.prcTp || '',
        parseFloat(o.prc || 0),
        (o.ordSt || 'PENDING').toUpperCase(),
        JSON.stringify(o),
      ],
    );
  }
  return list.length;
}

async function syncTrades(userId) {
  const res = await brokerService.fetchTradeBook(userId);
  if (res?.stat && res.stat !== 'Ok') return 0;
  const list = res?.data || [];
  for (const t of list) {
    // Trades don't have a unique single id field across docs — use exOrdId
    // (exchange order id) when present, else build a composite from nOrdNo
    // + exTm so the (user_id, broker_trade_id) UNIQUE upsert is stable.
    const tradeId = t.exOrdId || `${t.nOrdNo}-${t.exTm || ''}`;
    if (!tradeId) continue;
    await query(
      `INSERT INTO trades
        (user_id, broker_trade_id, broker_order_id, symbol, exchange, side, qty, price, raw_response)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
       ON DUPLICATE KEY UPDATE
         qty = VALUES(qty),
         price = VALUES(price),
         raw_response = VALUES(raw_response)`,
      [
        userId,
        String(tradeId),
        t.nOrdNo ? String(t.nOrdNo) : null,
        t.trdSym || '',
        t.exSeg || '',
        side(t.trnsTp),
        parseInt(t.fldQty || t.qty || 0, 10),
        parseFloat(t.avgPrc || 0),
        JSON.stringify(t),
      ],
    );
  }
  return list.length;
}

async function listOrders(userId, { limit = 100 } = {}) {
  return query(`SELECT * FROM orders WHERE user_id = ? ORDER BY id DESC LIMIT ?`, [userId, limit]);
}
async function listTrades(userId, { limit = 100 } = {}) {
  return query(`SELECT * FROM trades WHERE user_id = ? ORDER BY id DESC LIMIT ?`, [userId, limit]);
}

module.exports = { syncOrders, syncTrades, listOrders, listTrades };
