// Per-leg broker-side bracket lifecycle (SL-M + LIMIT with OCO).
//
// Owned by premiumTrigger when cfg.useBrokerBracketExits is true. After
// the SELL entry fills, openBracket places two resting BUY orders at the
// broker:
//   - SL-M with trigger = entry + stoploss
//   - LIMIT with price  = entry - target
// The status poll loop watches both child orders. When one fills, the
// other is cancelled (one-cancels-other). On restrike, EOD, or manual
// exit, closeBracket cancels both and the caller flattens any residual
// SHORT with a fresh MKT BUY.
//
// Paper mode keeps the same shape but never hits the broker; checkPaper
// is called from the tick handler and synthesises whichever fill the
// LTP just crossed.

const { query } = require('../db/pool');
const brokerService = require('./brokerService');
const eventLog = require('./eventLog');
const logger = require('../utils/logger');

const LIVE_STATUSES = new Set(['open', 'open pending', 'trigger pending']);
const FILLED_STATUSES = new Set(['complete', 'completed', 'filled', 'fully executed']);
const CANCELLED_STATUSES = new Set(['cancelled', 'canceled', 'cancel confirmed']);
const REJECTED_STATUSES = new Set(['rejected', 'reject']);

// state[userId][side] = { ...bracket }
const state = new Map();

function key(userId) {
  if (!state.has(userId)) state.set(userId, {});
  return state.get(userId);
}

function getBracket(userId, side) {
  return key(userId)[side] || null;
}

function clear(userId, side) {
  delete key(userId)[side];
}

async function dbInsert(b) {
  try {
    const r = await query(
      `INSERT INTO premium_trigger_brackets
         (user_id, side, symbol, qty, entry_price, target_price, sl_trigger_price,
          entry_order_id, sl_order_id, target_order_id, status, mode)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [b.userId, b.side, b.symbol, b.qty, b.entryPrice, b.targetPrice,
       b.slTriggerPrice, b.entryOrderId || null, b.slOrderId || null,
       b.targetOrderId || null, b.status, b.mode],
    );
    b.id = r.insertId;
  } catch (err) {
    logger.warn('bracketManager: dbInsert failed', { err: err.message });
  }
}

async function dbUpdate(b, fields) {
  if (!b.id) return;
  const cols = Object.keys(fields);
  if (!cols.length) return;
  const set = cols.map(c => `${c} = ?`).join(', ');
  const vals = cols.map(c => fields[c]);
  vals.push(b.id);
  try {
    await query(`UPDATE premium_trigger_brackets SET ${set} WHERE id = ?`, vals);
  } catch (err) {
    logger.warn('bracketManager: dbUpdate failed', { err: err.message });
  }
}

async function dbClose(b, status, reason, exitPrice) {
  await dbUpdate(b, {
    status,
    closed_reason: reason || null,
    exit_price: exitPrice != null ? exitPrice : null,
    closed_at: new Date(),
  });
}

function paperId(prefix, userId) {
  return `${prefix}-${userId}-${Date.now()}-${Math.floor(Math.random() * 1000)}`;
}

// Place a single child order (SL-M or LIMIT) against the broker. ctx
// supplies the symbol/token/segment/qty; `kind` is 'sl' or 'target'.
async function placeChild(userId, ctx, kind, slTriggerPrice, targetPrice) {
  const order = {
    symbol: ctx.token,
    tradingSymbol: ctx.symbol,
    exchangeSegment: ctx.segment,
    side: 'BUY',
    qty: ctx.qty,
    product: 'MIS',
    price: 0,
    triggerPrice: 0,
  };
  if (kind === 'sl') {
    order.orderType = 'SL-M';
    order.triggerPrice = Math.max(0.05, Number(slTriggerPrice.toFixed(2)));
  } else {
    order.orderType = 'L';
    order.price = Math.max(0.05, Number(targetPrice.toFixed(2)));
  }
  const resp = await brokerService.placeOrder(userId, order);
  const ok = resp && resp.stat === 'Ok' && resp.nOrdNo;
  return ok
    ? { ok: true, brokerOrderId: String(resp.nOrdNo), raw: resp }
    : { ok: false, reason: resp?.emsg || resp?.errMsg || resp?.stat || 'unknown', raw: resp };
}

async function cancelChild(userId, brokerOrderId, tradingSymbol) {
  if (!brokerOrderId) return { ok: true, alreadyMissing: true };
  if (String(brokerOrderId).startsWith('PAPER-')) return { ok: true, paper: true };
  try {
    const resp = await brokerService.cancelOrder(userId, { brokerOrderId, tradingSymbol });
    const ok = resp && (resp.stat === 'Ok' || resp.stCode === 200 || resp.cancelled === true);
    return ok ? { ok: true, raw: resp } : { ok: false, reason: resp?.emsg || resp?.errMsg || 'unknown', raw: resp };
  } catch (err) {
    return { ok: false, reason: err.message };
  }
}

// openBracket: invoked right after the SELL entry fills. Returns
// { ok, bracket, slPlaced, targetPlaced, reason } so the caller can
// decide whether to flatten on partial failure.
async function openBracket(userId, side, ctx) {
  const { mode, qty, symbol, token, segment, entryPrice, target, stoploss } = ctx;
  if (!Number.isFinite(entryPrice) || entryPrice <= 0) {
    return { ok: false, reason: 'invalid entryPrice' };
  }
  const targetPrice = Math.max(0.05, entryPrice - target);
  const slTriggerPrice = entryPrice + stoploss;
  const b = {
    userId,
    side,
    symbol,
    token,
    segment,
    qty,
    entryPrice,
    targetPrice,
    slTriggerPrice,
    entryOrderId: ctx.entryOrderId || null,
    slOrderId: null,
    targetOrderId: null,
    status: 'placing',
    mode,
    openedAt: Date.now(),
  };
  key(userId)[side] = b;
  await dbInsert(b);

  if (mode === 'paper') {
    b.slOrderId = paperId('PAPER-SL', userId);
    b.targetOrderId = paperId('PAPER-TGT', userId);
    b.status = 'open';
    await dbUpdate(b, { sl_order_id: b.slOrderId, target_order_id: b.targetOrderId, status: 'open' });
    eventLog.log(userId, 'PT_BRACKET_OPENED', 'INFO',
      `[PAPER] ${side.toUpperCase()} bracket: SL ${slTriggerPrice.toFixed(2)} / TGT ${targetPrice.toFixed(2)}`,
      { side, slOrderId: b.slOrderId, targetOrderId: b.targetOrderId });
    return { ok: true, bracket: b };
  }

  // Live: place SL-M first (the safety order), then LIMIT target.
  const slRes = await placeChild(userId, ctx, 'sl', slTriggerPrice, targetPrice);
  if (!slRes.ok) {
    await dbClose(b, 'error', `sl_placement_failed:${slRes.reason}`, null);
    clear(userId, side);
    eventLog.log(userId, 'PT_BRACKET_FAILED', 'WARN',
      `${side.toUpperCase()} SL placement failed: ${slRes.reason}`, { side });
    return { ok: false, reason: `sl_placement_failed:${slRes.reason}` };
  }
  b.slOrderId = slRes.brokerOrderId;
  await dbUpdate(b, { sl_order_id: b.slOrderId });

  const tgtRes = await placeChild(userId, ctx, 'target', slTriggerPrice, targetPrice);
  if (!tgtRes.ok) {
    // Roll back the SL we just placed. If the cancel itself fails, the
    // SL-M is now orphaned at the broker — surface it loudly so the
    // operator can clean up manually.
    const slCancel = await cancelChild(userId, b.slOrderId, ctx.symbol);
    if (!slCancel.ok) {
      logger.error('bracketManager: SL rollback cancel failed — order may be orphaned at broker', {
        userId, side, slOrderId: b.slOrderId, reason: slCancel.reason,
      });
      eventLog.log(userId, 'PT_BRACKET_ORPHAN_SL', 'WARN',
        `${side.toUpperCase()} SL order ${b.slOrderId} could not be cancelled after target placement failed; check Kotak terminal`,
        { side, slOrderId: b.slOrderId, reason: slCancel.reason });
    }
    await dbClose(b, 'error', `target_placement_failed:${tgtRes.reason}`, null);
    clear(userId, side);
    eventLog.log(userId, 'PT_BRACKET_FAILED', 'WARN',
      `${side.toUpperCase()} target placement failed: ${tgtRes.reason}`, { side });
    return { ok: false, reason: `target_placement_failed:${tgtRes.reason}` };
  }
  b.targetOrderId = tgtRes.brokerOrderId;
  b.status = 'open';
  await dbUpdate(b, { target_order_id: b.targetOrderId, status: 'open' });
  eventLog.log(userId, 'PT_BRACKET_OPENED', 'INFO',
    `[LIVE] ${side.toUpperCase()} bracket: SL ${slTriggerPrice.toFixed(2)} (#${b.slOrderId}) / TGT ${targetPrice.toFixed(2)} (#${b.targetOrderId})`,
    { side, slOrderId: b.slOrderId, targetOrderId: b.targetOrderId });
  return { ok: true, bracket: b };
}

// Cancel both children. Used by restrike, EOD, manual exit. Caller is
// responsible for flattening any residual SHORT with a MKT BUY after
// this returns. Returns { ok, slCancelled, targetCancelled }.
async function closeBracket(userId, side, reason) {
  const b = getBracket(userId, side);
  if (!b) return { ok: true, missing: true };
  b.status = 'closing';
  const slRes = await cancelChild(userId, b.slOrderId, b.symbol);
  const tgtRes = await cancelChild(userId, b.targetOrderId, b.symbol);
  await dbClose(b, 'cancelled', reason || 'manual', null);
  clear(userId, side);
  eventLog.log(userId, 'PT_BRACKET_CANCELLED', 'INFO',
    `${side.toUpperCase()} bracket cancelled (${reason || 'manual'})`,
    { side, slCancelled: slRes.ok, targetCancelled: tgtRes.ok });
  return { ok: slRes.ok && tgtRes.ok, slCancelled: slRes.ok, targetCancelled: tgtRes.ok };
}

// Tick safety hammer. Called by premiumTrigger.evaluate when LTP has
// overshot SL by more than the configured cushion and the broker hasn't
// reported a fill within the timeout. Cancels both children and signals
// to the caller that they should flatten with MKT.
async function emergencyFlatten(userId, side, reason) {
  const b = getBracket(userId, side);
  if (!b) return { ok: true, missing: true };
  await cancelChild(userId, b.slOrderId, b.symbol);
  await cancelChild(userId, b.targetOrderId, b.symbol);
  await dbClose(b, 'safety_flattened', reason || 'sl_safety_hammer', null);
  clear(userId, side);
  eventLog.log(userId, 'PT_BRACKET_SAFETY_FLATTEN', 'WARN',
    `${side.toUpperCase()} bracket safety-flattened (${reason})`, { side });
  return { ok: true };
}

// Paper-mode tick check. Called from the tick handler with the current
// LTP. If LTP crosses SL trigger or target price, synthesise the fill,
// auto-cancel the sibling, return { filled: true, kind, exitPrice }.
function checkPaper(userId, side, ltp) {
  const b = getBracket(userId, side);
  if (!b || b.mode !== 'paper' || b.status !== 'open') return null;
  if (!Number.isFinite(ltp) || ltp <= 0) return null;
  let kind = null;
  let exitPrice = null;
  if (ltp >= b.slTriggerPrice) {
    kind = 'sl';
    exitPrice = ltp;
  } else if (ltp <= b.targetPrice) {
    kind = 'target';
    exitPrice = b.targetPrice;
  }
  if (!kind) return null;
  b.status = kind === 'sl' ? 'sl_filled' : 'target_filled';
  dbClose(b, b.status, kind, exitPrice).catch(() => {});
  clear(userId, side);
  eventLog.log(userId, 'PT_BRACKET_FILLED', 'INFO',
    `[PAPER] ${side.toUpperCase()} ${kind} fill @ ${exitPrice.toFixed(2)}`,
    { side, kind, exitPrice });
  return { filled: true, kind, exitPrice };
}

// Live-mode status poller. Walks every open bracket for this user and
// queries broker order status. When one child fills, cancels the other.
// Returns a list of { side, kind, exitPrice, qty } for the caller to
// record as exit signals + P&L.
async function pollStatus(userId) {
  const userBrackets = state.get(userId);
  if (!userBrackets) return [];
  const fills = [];
  for (const side of Object.keys(userBrackets)) {
    const b = userBrackets[side];
    if (!b || b.mode !== 'live' || b.status !== 'open') continue;
    let slStatus = null, slAvg = null;
    let tgtStatus = null, tgtAvg = null;
    try {
      const slH = await brokerService.fetchOrderHistory(userId, b.slOrderId);
      const slData = Array.isArray(slH?.data) ? slH.data[0] : slH?.data || slH;
      slStatus = String(slData?.ordSt || '').toLowerCase();
      slAvg = parseFloat(slData?.avgPrc ?? slData?.flPrc ?? 0);
    } catch (_) { /* swallow — retry next tick */ }
    try {
      const tgtH = await brokerService.fetchOrderHistory(userId, b.targetOrderId);
      const tgtData = Array.isArray(tgtH?.data) ? tgtH.data[0] : tgtH?.data || tgtH;
      tgtStatus = String(tgtData?.ordSt || '').toLowerCase();
      tgtAvg = parseFloat(tgtData?.avgPrc ?? tgtData?.flPrc ?? 0);
    } catch (_) { /* swallow */ }

    const slFilled = slStatus && FILLED_STATUSES.has(slStatus);
    const tgtFilled = tgtStatus && FILLED_STATUSES.has(tgtStatus);

    if (slFilled && tgtFilled) {
      // Rare double-fill — net qty might be zero. Log and clear; caller
      // will reconcile via positions on next poll.
      await dbClose(b, 'error', 'double_fill', Number.isFinite(slAvg) ? slAvg : null);
      clear(userId, side);
      eventLog.log(userId, 'PT_BRACKET_DOUBLE_FILL', 'WARN',
        `${side.toUpperCase()} both children filled`, { side });
      fills.push({ side, kind: 'double_fill', exitPrice: slAvg, qty: b.qty, double: true });
    } else if (slFilled) {
      await cancelChild(userId, b.targetOrderId, b.symbol);
      const exit = Number.isFinite(slAvg) && slAvg > 0 ? slAvg : b.slTriggerPrice;
      await dbClose(b, 'sl_filled', 'sl', exit);
      clear(userId, side);
      eventLog.log(userId, 'PT_BRACKET_FILLED', 'INFO',
        `[LIVE] ${side.toUpperCase()} SL fill @ ${exit.toFixed(2)} (#${b.slOrderId})`,
        { side, kind: 'sl', exitPrice: exit });
      fills.push({ side, kind: 'sl', exitPrice: exit, qty: b.qty });
    } else if (tgtFilled) {
      await cancelChild(userId, b.slOrderId, b.symbol);
      const exit = Number.isFinite(tgtAvg) && tgtAvg > 0 ? tgtAvg : b.targetPrice;
      await dbClose(b, 'target_filled', 'target', exit);
      clear(userId, side);
      eventLog.log(userId, 'PT_BRACKET_FILLED', 'INFO',
        `[LIVE] ${side.toUpperCase()} target fill @ ${exit.toFixed(2)} (#${b.targetOrderId})`,
        { side, kind: 'target', exitPrice: exit });
      fills.push({ side, kind: 'target', exitPrice: exit, qty: b.qty });
    } else if (
      (slStatus && (CANCELLED_STATUSES.has(slStatus) || REJECTED_STATUSES.has(slStatus))) ||
      (tgtStatus && (CANCELLED_STATUSES.has(tgtStatus) || REJECTED_STATUSES.has(tgtStatus)))
    ) {
      // One child died unexpectedly without the other filling. Cancel
      // the survivor; caller will see status='error' and flatten.
      await cancelChild(userId, b.slOrderId, b.symbol);
      await cancelChild(userId, b.targetOrderId, b.symbol);
      await dbClose(b, 'error', `child_died:sl=${slStatus};tgt=${tgtStatus}`, null);
      clear(userId, side);
      eventLog.log(userId, 'PT_BRACKET_CHILD_DIED', 'WARN',
        `${side.toUpperCase()} bracket child died (sl=${slStatus} tgt=${tgtStatus})`, { side });
      fills.push({ side, kind: 'child_died', exitPrice: null, qty: b.qty });
    }
  }
  return fills;
}

// On boot or reconcile, scan the order book for SL-M / LIMIT BUY orders
// matching this leg's symbol and rehydrate the bracket row. Used by
// premiumTrigger.reconcilePositions to recover from a process restart.
async function recoverFromOrderBook(userId, side, ctx) {
  if (getBracket(userId, side)) return null;
  let ob;
  try {
    const r = await brokerService.fetchOrderBook(userId);
    ob = Array.isArray(r?.data) ? r.data : [];
  } catch (err) {
    logger.warn('bracketManager: fetchOrderBook failed during recover', { err: err.message });
    return null;
  }
  const candidates = ob.filter(o =>
    o && o.trdSym === ctx.symbol
      && o.trnsTp === 'B'
      && o.prod === 'MIS'
      && o.ordSt && LIVE_STATUSES.has(String(o.ordSt).toLowerCase()));
  // Order type is in `prcTp` per Kotak's order-book schema (see
  // reportService.js:9). Match exactly so 'SL' (with-trigger limit)
  // doesn't accidentally match the SL-M regex.
  const sl = candidates.find(o => /^SL-?M$/i.test(String(o.prcTp || '')));
  const tgt = candidates.find(o => /^L$/i.test(String(o.prcTp || '')));
  if (!sl && !tgt) return null;
  // Order book exposes the LIMIT price in `prc`, but the trigger price
  // field for SL-M isn't documented in this codebase. Derive both
  // levels from the persisted entry+target+stoploss to avoid relying
  // on undocumented fields. If we don't have entryPrice in ctx (e.g.
  // reconcile ran before legState was rehydrated), skip recovery.
  if (ctx.entryPrice == null || ctx.target == null || ctx.stoploss == null) {
    logger.warn('bracketManager: insufficient ctx to rehydrate bracket — skipping recover',
      { userId, side, hasEntry: ctx.entryPrice != null });
    return null;
  }
  const b = {
    userId,
    side,
    symbol: ctx.symbol,
    token: ctx.token,
    segment: ctx.segment,
    qty: ctx.qty,
    entryPrice: ctx.entryPrice,
    targetPrice: ctx.entryPrice - ctx.target,
    slTriggerPrice: ctx.entryPrice + ctx.stoploss,
    entryOrderId: ctx.entryOrderId || null,
    slOrderId: sl ? String(sl.nOrdNo) : null,
    targetOrderId: tgt ? String(tgt.nOrdNo) : null,
    status: 'open',
    mode: 'live',
    openedAt: Date.now(),
  };
  key(userId)[side] = b;
  await dbInsert(b);
  eventLog.log(userId, 'PT_BRACKET_RECOVERED', 'INFO',
    `${side.toUpperCase()} bracket recovered from order book`, { side, slOrderId: b.slOrderId, targetOrderId: b.targetOrderId });
  return b;
}

// For UI / status display.
function snapshot(userId, side) {
  const b = getBracket(userId, side);
  if (!b) return null;
  return {
    status: b.status,
    slOrderId: b.slOrderId,
    targetOrderId: b.targetOrderId,
    slTriggerPrice: b.slTriggerPrice,
    targetPrice: b.targetPrice,
    openedAt: b.openedAt,
    mode: b.mode,
  };
}

module.exports = {
  openBracket,
  closeBracket,
  emergencyFlatten,
  checkPaper,
  pollStatus,
  recoverFromOrderBook,
  getBracket,
  snapshot,
};
