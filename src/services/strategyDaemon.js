// Ensures every user with active strategies + a valid broker session has a
// market-data WS client attached, so strategies fire even when no browser
// tab is open. Idempotent — safe to run alongside on-connect attachment.

const { query } = require('../db/pool');
const accounts = require('./brokerAccount');
const marketData = require('./marketData');
const strategyRunner = require('./strategyRunner');
const logger = require('../utils/logger');

const SCAN_INTERVAL_MS = 60_000;

let timer = null;

async function _scanOnce() {
  let userIds;
  try {
    const rows = await query(
      `SELECT DISTINCT user_id FROM strategies WHERE is_active = 1`,
    );
    userIds = rows.map(r => r.user_id);
  } catch (err) {
    logger.error('strategyDaemon: scan failed', { err: err.message });
    return;
  }

  for (const userId of userIds) {
    if (marketData.getClient(userId)) {
      // Make sure tick→strategy bridge is attached even if md was set up by
      // a different code path (e.g. socket connection).
      const mdc = marketData.getClient(userId);
      if (!strategyRunner.isAttached(userId)) strategyRunner.attach(userId, mdc);
      continue;
    }
    let acc;
    try { acc = await accounts.getByUserId(userId); }
    catch { continue; }
    if (!acc || acc.status !== 'CONNECTED' || !acc.session_token) continue;

    try {
      const mdc = marketData.attachClient(userId, acc);
      strategyRunner.attach(userId, mdc);
      logger.info('strategyDaemon: attached', { userId });
    } catch (err) {
      logger.warn('strategyDaemon: attach failed', { userId, err: err.message });
    }
  }
}

function start() {
  if (timer) return;
  // PM2 cluster: only the first instance runs the daemon. Otherwise every
  // cluster fork would open its own Kotak Neo WS per user, multiplying
  // connections by N and likely tripping per-user limits.
  const instance = process.env.NODE_APP_INSTANCE;
  if (instance != null && instance !== '0') {
    logger.info('strategyDaemon: skipped (not instance 0)', { instance });
    return;
  }
  // Run once on startup to warm-attach existing users.
  setImmediate(() => { _scanOnce().catch(() => {}); });
  timer = setInterval(() => { _scanOnce().catch(() => {}); }, SCAN_INTERVAL_MS);
  timer.unref();
  logger.info('strategyDaemon started', { intervalMs: SCAN_INTERVAL_MS });
}

function stop() {
  if (timer) clearInterval(timer);
  timer = null;
}

module.exports = { start, stop };
