// Strategy evaluation engine.
//
// Each strategy is a JSON rule:
//   { symbol, condition: "price > 22000", action: "BUY", qty: 50,
//     product: "MIS", orderType: "MKT", exchange: "NSE", cooldownSec: 60 }
//
// We evaluate `condition` against an in-memory price tick rather than hitting
// the DB, then enqueue a TRADE job when the condition is true. A per-strategy
// cooldown prevents repeated firing on every tick.

const { query } = require('../db/pool');
const orderService = require('./orderService');
const eventLog = require('./eventLog');
const logger = require('../utils/logger');

const lastFired = new Map(); // strategyId -> timestamp ms

function compileCondition(expr) {
  // Whitelist: allow `price`, numbers, comparison + arithmetic + logical ops only.
  if (!/^[\s0-9+\-*/().<>=!&|priceltp]+$/i.test(expr.replace(/\s/g, ''))) {
    throw new Error(`unsafe condition: ${expr}`);
  }
  // Map ltp -> price for convenience.
  const safe = expr.replace(/ltp/gi, 'price');
  // eslint-disable-next-line no-new-func
  return new Function('price', `"use strict"; return (${safe});`);
}

async function loadActive(symbol = null) {
  const sql = symbol
    ? `SELECT * FROM strategies WHERE is_active = 1 AND symbol = ?`
    : `SELECT * FROM strategies WHERE is_active = 1`;
  const rows = await query(sql, symbol ? [symbol] : []);
  return rows.map(r => ({
    ...r,
    rules: typeof r.rules_json === 'string' ? JSON.parse(r.rules_json) : r.rules_json,
  }));
}

async function evaluateTick({ userId, symbol, ltp, strategies }) {
  const now = Date.now();
  const fired = [];
  for (const s of strategies) {
    if (userId != null && s.user_id !== userId) continue;
    if (s.symbol !== symbol) continue;
    const cooldown = (s.rules.cooldownSec || 30) * 1000;
    const last = lastFired.get(s.id) || 0;
    if (now - last < cooldown) continue;

    let evalFn;
    try { evalFn = compileCondition(s.rules.condition); }
    catch (err) {
      logger.error('strategy condition unsafe', { id: s.id, err: err.message });
      continue;
    }

    let truthy;
    try { truthy = !!evalFn(ltp); } catch { continue; }
    if (!truthy) continue;

    lastFired.set(s.id, now);
    try {
      const { orderId, jobId } = await orderService.enqueueTrade(s.user_id, {
        symbol: s.symbol,                                  // instrument token
        tradingSymbol: s.rules.tradingSymbol || s.symbol,  // scrip name (e.g. RELIANCE-EQ)
        exchange: s.exchange,
        side: s.rules.action,
        qty: s.rules.qty,
        product: s.rules.product || 'MIS',
        orderType: s.rules.orderType || 'MKT',
        price: s.rules.price || 0,
        triggerPrice: s.rules.triggerPrice || 0,
      }, { strategyId: s.id });
      await query(`UPDATE strategies SET last_run_at = NOW() WHERE id = ?`, [s.id]);
      await eventLog.log(s.user_id, 'STRATEGY_FIRED', 'INFO',
        `Strategy ${s.id} fired ${s.rules.action} ${s.rules.qty} ${s.symbol} @ ${ltp}`,
        { jobId, orderId });
      fired.push({ strategyId: s.id, jobId, orderId });
    } catch (err) {
      logger.error('strategy enqueue failed', { id: s.id, err: err.message });
    }
  }
  return fired;
}

async function create(userId, body) {
  const rules = {
    condition: body.condition,
    action: body.action,
    qty: parseInt(body.qty, 10),
    product: body.product || 'MIS',
    orderType: body.orderType || 'MKT',
    price: body.price ? parseFloat(body.price) : 0,
    triggerPrice: body.triggerPrice ? parseFloat(body.triggerPrice) : 0,
    tradingSymbol: body.tradingSymbol || body.symbol,
    cooldownSec: body.cooldownSec ? parseInt(body.cooldownSec, 10) : 30,
  };
  // Validate condition syntax up-front
  compileCondition(rules.condition);
  const r = await query(
    `INSERT INTO strategies (user_id, name, symbol, exchange, rules_json, is_active)
     VALUES (?, ?, ?, ?, ?, ?)`,
    [userId, body.name, body.symbol, body.exchange || 'NSE', JSON.stringify(rules), body.is_active ? 1 : 0],
  );
  return r.insertId;
}

async function setActive(userId, id, active) {
  await query(`UPDATE strategies SET is_active = ? WHERE id = ? AND user_id = ?`, [active ? 1 : 0, id, userId]);
}

async function list(userId) {
  return query(`SELECT * FROM strategies WHERE user_id = ? ORDER BY id DESC`, [userId]);
}

async function remove(userId, id) {
  await query(`DELETE FROM strategies WHERE id = ? AND user_id = ?`, [id, userId]);
}

module.exports = { compileCondition, loadActive, evaluateTick, create, setActive, list, remove };
