// Tick recorder for premium-trigger backtests.
//
// Hooks into the marketData WS for any user with a running premium-trigger
// strategy and persists ticks to `tick_history` (sampled at 1 Hz per
// token, batched at 500 ms). Also snapshots the resolved CE/PE/spot leg
// metadata to `premium_trigger_session` once per (user, trade_date) so a
// backtest can reconstruct historical tokens even after a scrip-master
// rollover. Runs only on the primary cluster instance to avoid duplicate
// writes from each fork.

const { query } = require('../db/pool');
const marketData = require('../services/marketData');
const ptManager = require('./premiumTriggerManager');
const logger = require('../utils/logger');

const SAMPLE_MS = 1000;        // keep at most 1 row per token per second
const FLUSH_MS  = 500;
const BUFFER_CAP = 1000;       // drop oldest if we can't flush in time
const PURGE_DAYS = 30;

// per-user state
const subs = new Map();        // userId -> { mdc, listener, lastSampleAt: Map<token, ts> }
let buffer = [];               // pending rows: [user_id, trade_date, token, segment, ltp, ts_ms]
let flushTimer = null;
let purgeTimer = null;

function isoDate(ts) {
  const d = new Date(ts);
  return d.getUTCFullYear() + '-'
    + String(d.getUTCMonth() + 1).padStart(2, '0') + '-'
    + String(d.getUTCDate()).padStart(2, '0');
}

async function persistSession(userId, cfg, legs) {
  const today = isoDate(Date.now());
  try {
    await query(
      `INSERT IGNORE INTO premium_trigger_session
        (user_id, trade_date, cfg_snapshot, legs)
       VALUES (?, ?, ?, ?)`,
      [userId, today, JSON.stringify(cfg), JSON.stringify(legs)],
    );
  } catch (err) {
    logger.warn('tickRecorder: persistSession failed', { userId, err: err.message });
  }
}

function attachToUser(userId) {
  if (subs.has(userId)) return;
  const mdc = marketData.getClient(userId);
  if (!mdc) return;

  const lastSampleAt = new Map();
  const listener = (tick) => {
    if (!tick || tick.symbol == null || tick.ltp == null) return;
    const tok = String(tick.symbol);
    const now = tick.ts || Date.now();
    const last = lastSampleAt.get(tok) || 0;
    if (now - last < SAMPLE_MS) return;       // 1 Hz throttle
    lastSampleAt.set(tok, now);

    // We don't have segment in the tick payload — derive from the user's
    // current premium-trigger leg state. Spot might be a name like
    // "Nifty 50"; we record whatever segment the strategy resolved.
    const pt = ptManager.forUser(userId);
    const stat = pt.status();
    let segment = null;
    for (const side of ['ce', 'pe']) {
      const st = stat?.state?.[side];
      if (st && String(st.token) === tok) { segment = st.exchangeSegment; break; }
    }
    if (!segment) {
      // Spot symbol path — match against SPOT_REF mapping
      const cfg = stat?.cfg;
      // The strategy already decoded and stored spot under cfg; we
      // don't have direct access to SPOT_REF here, but the marketData
      // tick.symbol IS the same string we subscribed with, so we can
      // skip the segment lookup if it's not a leg token.
      segment = 'nse_cm';
    }

    const tradeDate = isoDate(now);
    buffer.push([userId, tradeDate, tok, segment, parseFloat(tick.ltp), Math.floor(now)]);
    if (buffer.length > BUFFER_CAP) buffer.splice(0, buffer.length - BUFFER_CAP);
  };
  mdc.on('tick', listener);
  subs.set(userId, { mdc, listener, lastSampleAt });

  // Snapshot the session legs (best-effort, idempotent on (user, today))
  const pt = ptManager.forUser(userId);
  const stat = pt.status();
  const legs = {
    ce: stat?.state?.ce ? {
      token: stat.state.ce.token,
      symbol: stat.state.ce.symbol,
      exchangeSegment: stat.state.ce.exchangeSegment,
      lotSize: stat.state.ce.lotSize,
      strike: stat.state.ce.strike,
      expiry: stat.state.ce.expiry,
    } : null,
    pe: stat?.state?.pe ? {
      token: stat.state.pe.token,
      symbol: stat.state.pe.symbol,
      exchangeSegment: stat.state.pe.exchangeSegment,
      lotSize: stat.state.pe.lotSize,
      strike: stat.state.pe.strike,
      expiry: stat.state.pe.expiry,
    } : null,
  };
  persistSession(userId, stat?.cfg || {}, legs).catch(() => {});

  logger.info('tickRecorder: attached', { userId });
}

function detachFromUser(userId) {
  const sub = subs.get(userId);
  if (!sub) return;
  try { sub.mdc.removeListener('tick', sub.listener); } catch (_) {}
  subs.delete(userId);
  logger.info('tickRecorder: detached', { userId });
}

async function flush() {
  if (!buffer.length) return;
  const rows = buffer.splice(0, buffer.length);
  // Multi-row INSERT; mysql2 accepts an array of arrays with the
  // standard `INSERT INTO ... VALUES ?` syntax.
  try {
    const placeholders = rows.map(() => '(?, ?, ?, ?, ?, ?)').join(',');
    const flat = [];
    for (const r of rows) flat.push(...r);
    await query(
      `INSERT INTO tick_history (user_id, trade_date, token, segment, ltp, ts_ms)
       VALUES ${placeholders}`,
      flat,
    );
  } catch (err) {
    logger.warn('tickRecorder: flush failed', { rows: rows.length, err: err.message });
  }
}

async function purgeOld() {
  try {
    const r = await query(
      `DELETE FROM tick_history WHERE trade_date < DATE_SUB(CURDATE(), INTERVAL ? DAY)`,
      [PURGE_DAYS],
    );
    if (r.affectedRows) logger.info('tickRecorder: purged old ticks', { rows: r.affectedRows });
  } catch (err) {
    logger.warn('tickRecorder: purge failed', { err: err.message });
  }
}

// Periodic sweep that attaches/detaches as users start/stop the strategy.
function syncSubscriptions() {
  const running = ptManager.runningUserIds ? ptManager.runningUserIds() : [];
  // Attach for every running user that we don't already track.
  for (const userId of running) {
    if (!subs.has(userId)) attachToUser(userId);
  }
  // Detach any tracked user that's no longer running.
  for (const userId of [...subs.keys()]) {
    if (!running.includes(userId)) detachFromUser(userId);
  }
}

function start() {
  // PM2 cluster: only the primary instance writes — otherwise every
  // fork would persist the same tick N times.
  const inst = process.env.NODE_APP_INSTANCE;
  if (inst != null && inst !== '0') {
    logger.info('tickRecorder: skipped (not instance 0)', { inst });
    return;
  }
  if (flushTimer) return;
  flushTimer = setInterval(() => flush().catch(() => {}), FLUSH_MS);
  flushTimer.unref?.();
  // Hourly sweep; cheap because it's just iterating a Map.
  setInterval(syncSubscriptions, 5_000).unref?.();
  // Daily purge.
  purgeTimer = setInterval(purgeOld, 24 * 60 * 60 * 1000);
  purgeTimer.unref?.();
  // Run an initial purge a minute after boot.
  setTimeout(() => { purgeOld().catch(() => {}); }, 60_000).unref?.();
  logger.info('tickRecorder started', { sampleMs: SAMPLE_MS, flushMs: FLUSH_MS });
}

function stop() {
  if (flushTimer) clearInterval(flushTimer);
  if (purgeTimer) clearInterval(purgeTimer);
  flushTimer = purgeTimer = null;
  for (const userId of [...subs.keys()]) detachFromUser(userId);
}

// Public helper: enqueue ticks from any source (e.g. the option-chain
// REST quote poller) into the same batch buffer the WS recorder uses.
// Each tick is { token, segment, ltp, ts_ms? }. Sampled at the same
// 1 Hz per (user, token) so a 2-second poll loop doesn't double-write.
const externalLastSampleAt = new Map(); // key = `${userId}|${token}` -> ts_ms
function recordTicks(userId, ticks) {
  if (!Array.isArray(ticks) || !ticks.length) return;
  // PM2 cluster: only instance 0 writes, same as the WS path.
  const inst = process.env.NODE_APP_INSTANCE;
  if (inst != null && inst !== '0') return;
  for (const t of ticks) {
    const tok = String(t.token || '');
    const seg = String(t.segment || 'nse_cm').toLowerCase();
    const ltp = parseFloat(t.ltp);
    if (!tok || !seg || !Number.isFinite(ltp) || ltp <= 0) continue;
    const ts = Number(t.ts_ms || t.ts || Date.now());
    const key = `${userId}|${tok}`;
    if (ts - (externalLastSampleAt.get(key) || 0) < SAMPLE_MS) continue;
    externalLastSampleAt.set(key, ts);
    buffer.push([userId, isoDate(ts), tok, seg, ltp, ts]);
    if (buffer.length > BUFFER_CAP) buffer.splice(0, buffer.length - BUFFER_CAP);
  }
}

module.exports = { start, stop, attachToUser, detachFromUser, flush, purgeOld, recordTicks };
