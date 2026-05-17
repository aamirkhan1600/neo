// Memoises one premiumTrigger instance per user and resumes any
// `running=1` users on app boot. Mirrors the strategyDaemon pattern but
// for the premium-trigger strategy.

const { query } = require('../db/pool');
const { createPremiumTrigger } = require('./premiumTrigger');
const logger = require('../utils/logger');

const instances = new Map();

function forUser(userId) {
  const id = Number(userId);
  if (!instances.has(id)) instances.set(id, createPremiumTrigger(id));
  return instances.get(id);
}

let bootTimer = null;

async function bootResume() {
  // Only the primary cluster instance should auto-resume — otherwise
  // every fork would attach its own marketData WS for the same users.
  const inst = process.env.NODE_APP_INSTANCE;
  if (inst != null && inst !== '0') {
    logger.info('premiumTriggerManager: skipped (not instance 0)', { inst });
    return;
  }
  let rows;
  try {
    rows = await query(
      'SELECT user_id FROM premium_trigger_config WHERE running = 1 OR enabled = 1',
    );
  } catch (err) {
    logger.warn('premiumTriggerManager: bootResume query failed', { err: err.message });
    return;
  }
  for (const r of rows) {
    const userId = Number(r.user_id);
    try {
      const pt = forUser(userId);
      await pt.load();
      if (pt.get().enabled) {
        await pt.start();
      }
    } catch (err) {
      logger.warn('premiumTriggerManager: resume failed', { userId, err: err.message });
    }
  }
}

function start() {
  if (bootTimer) return;
  // Delay 5s after app boot so DB pool + market data are settled before
  // the first WS connect lands.
  bootTimer = setTimeout(() => {
    bootResume().catch(err => logger.error('bootResume threw', { err: err.message }));
  }, 5000);
  bootTimer.unref?.();
  logger.info('premiumTriggerManager: scheduled bootResume in 5s');
}

function stop() {
  if (bootTimer) clearTimeout(bootTimer);
  bootTimer = null;
}

// Snapshot of currently-running users — used by tickRecorder to know
// which marketData clients to listen on.
function runningUserIds() {
  const out = [];
  for (const [userId, pt] of instances) {
    try { if (pt.status().running) out.push(userId); }
    catch (_) {}
  }
  return out;
}

module.exports = { forUser, start, stop, bootResume, runningUserIds };
