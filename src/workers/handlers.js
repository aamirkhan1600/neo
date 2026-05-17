// Per-job-type handlers. Each handler receives the dehydrated job row and
// returns either a result object (success) or throws (failure -> retry).

const brokerService = require('../services/brokerService');
const orderService = require('../services/orderService');
const reports = require('../services/reportService');
const baskets = require('../services/basketService');
const accounts = require('../services/brokerAccount');
const strategyEngine = require('../services/strategyEngine');
const eventLog = require('../services/eventLog');
const logger = require('../utils/logger');

// Translate the application-level order shape into the Kotak Neo client's
// expected fields. `symbol` is the instrument token; `tradingSymbol` is the
// scrip name; exchange maps to the Neo `es` segment (nse_cm, nse_fo, ...).
function toBrokerOrder(o) {
  return {
    ...o,
    exchangeSegment: orderService.exchangeToSegment(o.exchange) || o.exchange,
    token: o.symbol,
    // Defaults for fields the docs accept
    afterMarket: !!o.afterMarket,
    discQty: o.discQty || 0,
    marketProtection: o.marketProtection || 0,
    validity: o.validity || 'DAY',
  };
}

// Margin response per docs:
//   { avlCash, avlMrgn, insufFund, mrgnUsd, ordMrgn, reqdMrgn, rmsVldtd, stat, stCode }
// Insufficient = rmsVldtd is NOT_OK or insufFund > 0.
function isInsufficient(margin) {
  if (!margin) return false;
  if (margin.rmsVldtd === 'NOT_OK') return true;
  const insuf = parseFloat(margin.insufFund || 0);
  return Number.isFinite(insuf) && insuf > 0;
}

async function TRADE(job) {
  const { orderId, order } = job.payload;
  const brokerOrder = toBrokerOrder(order);

  let margin;
  try {
    margin = await brokerService.checkMargin(job.user_id, brokerOrder);
  } catch (err) {
    if (err instanceof brokerService.RateLimitedError) throw err;
    await orderService.markRejected(orderId, `margin_check_failed: ${err.message}`, err.data);
    throw err;
  }

  if (isInsufficient(margin)) {
    await orderService.markRejected(orderId, 'insufficient_funds', margin);
    await eventLog.log(job.user_id, 'ORDER_REJECTED', 'WARN', 'insufficient_funds', { orderId, margin });
    return { rejected: true, reason: 'insufficient_funds', margin };
  }

  let response;
  try {
    response = await brokerService.placeOrder(job.user_id, brokerOrder);
  } catch (err) {
    if (err instanceof brokerService.RateLimitedError) throw err;
    await orderService.markRejected(orderId, err.message, err.data);
    throw err;
  }

  await orderService.updateFromBrokerResponse(orderId, response);
  if (response?.stat !== 'Ok') {
    await eventLog.log(job.user_id, 'ORDER_REJECTED', 'WARN',
      response?.emsg || `stCode=${response?.stCode}`, { orderId, response });
  }
  return { orderId, response };
}

async function MARGIN(job) {
  return brokerService.checkMargin(job.user_id, toBrokerOrder(job.payload.order));
}

async function STRATEGY(job) {
  // Optional: scheduled strategy evaluation off a tick. Loads active
  // strategies and runs `evaluateTick` against the supplied price.
  const { symbol, ltp } = job.payload;
  const strategies = await strategyEngine.loadActive(symbol);
  return strategyEngine.evaluateTick({ userId: job.user_id, symbol, ltp, strategies });
}

async function BASKET(job) {
  const { basketId, legs } = job.payload;
  const results = [];
  for (const leg of legs) {
    const brokerLeg = toBrokerOrder(leg);
    try {
      const margin = await brokerService.checkMargin(job.user_id, brokerLeg);
      if (isInsufficient(margin)) {
        results.push({ leg, rejected: true, reason: 'insufficient_funds', margin });
        continue;
      }
      const resp = await brokerService.placeOrder(job.user_id, brokerLeg);
      results.push({ leg, response: resp });
    } catch (err) {
      if (err instanceof brokerService.RateLimitedError) throw err;
      results.push({ leg, error: err.message });
    }
  }
  const allFailed = results.every(r => r.error || r.rejected || r.response?.stat !== 'Ok');
  await baskets.setStatus(basketId, allFailed ? 'FAILED' : 'COMPLETED');
  return { basketId, results };
}

async function SYNC_ORDERS(job) {
  const n = await reports.syncOrders(job.user_id);
  return { synced: n };
}
async function SYNC_TRADES(job) {
  const n = await reports.syncTrades(job.user_id);
  return { synced: n };
}

async function RELOGIN(job) {
  // RELOGIN jobs cannot run unattended (we don't store MPIN/TOTP). Mark the
  // account as EXPIRED and notify the user via event_log; the UI prompts a
  // manual re-login. Treating this as a successful sentinel job avoids retry
  // storms.
  await accounts.markStatus(job.user_id, 'EXPIRED');
  await eventLog.log(job.user_id, 'BROKER_RELOGIN_REQUIRED', 'ERROR',
    'Manual re-login required: MPIN/TOTP is not stored for security.');
  return { manualReloginRequired: true };
}

module.exports = { TRADE, MARGIN, STRATEGY, BASKET, SYNC_ORDERS, SYNC_TRADES, RELOGIN };
