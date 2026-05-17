// Bridges per-user marketData ticks into strategyEngine.evaluateTick.
//
// Lives in the app process. For every market-data client we attach, we
// listen for ticks and fire active strategies — throttled per user×symbol so
// a noisy book doesn't pin a CPU.

const { query } = require('../db/pool');
const strategyEngine = require('./strategyEngine');
const logger = require('../utils/logger');

const CACHE_TTL_MS = 30_000;
const THROTTLE_MS = 200;

const cache = new Map();      // userId -> { strategies, expiresAt }
const lastEval = new Map();   // `${userId}:${symbol}` -> ms
const attached = new Set();   // userIds currently attached

async function loadActiveCached(userId) {
  const now = Date.now();
  const hit = cache.get(userId);
  if (hit && hit.expiresAt > now) return hit.strategies;
  const rows = await query(
    `SELECT * FROM strategies WHERE user_id = ? AND is_active = 1`,
    [userId],
  );
  const strategies = rows.map(r => ({
    ...r,
    rules: typeof r.rules_json === 'string' ? JSON.parse(r.rules_json) : r.rules_json,
  }));
  cache.set(userId, { strategies, expiresAt: now + CACHE_TTL_MS });
  return strategies;
}

function invalidate(userId) {
  cache.delete(userId);
}

// Subscribe the WS client to every distinct symbol used by the user's
// active strategies. Without this no ticks arrive for those instruments
// and the strategies never fire even though the runner is attached.
async function syncSubscriptions(userId, mdc) {
  if (!mdc) return;
  try {
    const strategies = await loadActiveCached(userId);
    if (!strategies.length) return;
    const orderService = require('./orderService');
    const items = strategies
      .map(s => ({
        token: String(s.symbol || ''),
        segment: orderService.exchangeToSegment(s.exchange) || 'nse_cm',
      }))
      .filter(s => s.token);
    if (!items.length) return;
    mdc.subscribe(items);
    logger.info('strategyRunner: subscribed strategy symbols', {
      userId, count: items.length,
    });
  } catch (err) {
    logger.warn('strategyRunner: subscribe failed', { userId, err: err.message });
  }
}

function attach(userId, mdc) {
  if (!mdc || mdc.__strategyAttached) return;
  mdc.__strategyAttached = true;
  attached.add(userId);

  mdc.on('tick', async (tick) => {
    const key = `${userId}:${tick.symbol}`;
    const now = Date.now();
    if (now - (lastEval.get(key) || 0) < THROTTLE_MS) return;
    lastEval.set(key, now);

    try {
      const strategies = await loadActiveCached(userId);
      if (!strategies.length) return;
      await strategyEngine.evaluateTick({
        userId,
        symbol: tick.symbol,
        ltp: tick.ltp,
        strategies,
      });
    } catch (err) {
      logger.error('strategyRunner: evaluation failed', {
        userId, symbol: tick.symbol, err: err.message,
      });
    }
  });

  // Subscribe immediately, and re-subscribe after every WS reconnect.
  mdc.on('open', () => { syncSubscriptions(userId, mdc).catch(() => {}); });
  syncSubscriptions(userId, mdc).catch(() => {});

  logger.info('strategyRunner: attached', { userId });
}

function detach(userId) {
  attached.delete(userId);
  cache.delete(userId);
  for (const k of [...lastEval.keys()]) {
    if (k.startsWith(`${userId}:`)) lastEval.delete(k);
  }
}

function isAttached(userId) { return attached.has(userId); }

// Called by route handlers after a strategy is created / toggled / deleted
// so the next tick picks up the new state and any new symbol gets subscribed.
async function refresh(userId, mdc) {
  invalidate(userId);
  if (mdc) await syncSubscriptions(userId, mdc);
}

module.exports = { attach, detach, isAttached, invalidate, loadActiveCached, refresh, syncSubscriptions };
