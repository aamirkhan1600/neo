// Date-wise backtest replay for premium-trigger.
//
// Loads recorded ticks for (userId, date), sets up an isolated strategy
// instance with cfg._backtest = true so placeOrder / persistState
// short-circuit, replays ticks chronologically through _testing.feedTick,
// captures the resulting P&L + per-trade signals + equity curve, and
// stores the run in `backtest_runs`.

const { query } = require('../db/pool');
const { createPremiumTrigger } = require('./premiumTrigger');
const logger = require('../utils/logger');

const TICK_LIMIT = 500_000;            // hard cap per replay
const RESULT_SIGNALS_CAP = 5_000;      // signals stored in result JSON

async function loadSession(userId, date) {
  const rows = await query(
    `SELECT cfg_snapshot, legs FROM premium_trigger_session
       WHERE user_id = ? AND trade_date = ? LIMIT 1`,
    [userId, date],
  );
  if (!rows.length) return null;
  const cfgSnap = typeof rows[0].cfg_snapshot === 'string'
    ? JSON.parse(rows[0].cfg_snapshot) : rows[0].cfg_snapshot;
  const legs = typeof rows[0].legs === 'string'
    ? JSON.parse(rows[0].legs) : rows[0].legs;
  return { cfgSnap, legs };
}

async function loadTicks(userId, date, tokens) {
  if (!tokens.length) return [];
  const placeholders = tokens.map(() => '?').join(',');
  const rows = await query(
    `SELECT token, segment, ltp, ts_ms
       FROM tick_history
       WHERE user_id = ? AND trade_date = ? AND token IN (${placeholders})
       ORDER BY ts_ms ASC
       LIMIT ?`,
    [userId, date, ...tokens, TICK_LIMIT],
  );
  return rows;
}

async function listRecordedDates(userId, limit = 60) {
  const rows = await query(
    `SELECT trade_date, COUNT(*) AS ticks
       FROM tick_history
       WHERE user_id = ?
       GROUP BY trade_date
       ORDER BY trade_date DESC
       LIMIT ?`,
    [userId, limit],
  );
  return rows.map(r => ({
    date: r.trade_date instanceof Date
      ? r.trade_date.toISOString().slice(0, 10)
      : String(r.trade_date).slice(0, 10),
    ticks: Number(r.ticks),
  }));
}

function summariseSignals(signals) {
  // Pair entries (SELL) with exits (BUY) per leg in chronological order.
  // Signals come from in-memory signalHistory which is already sorted
  // ascending (push order). Charges have already been applied by
  // recordTradeStats inside the strategy, but the in-memory log doesn't
  // carry per-trade P&L (P&L lives on legState aggregates). For the
  // result UI we recompute per-trade P&L = entry.price - exit.price (as
  // gross points) so the trade table is self-contained.
  const trades = [];
  const open = { ce: null, pe: null };
  for (const s of signals) {
    const side = s.side;
    if (s.action === 'SELL' && (s.status === 'placed' || s.status === 'paper-filled' || s.status === 'backtest-filled')) {
      open[side] = s;
    } else if (s.action === 'BUY' && open[side]) {
      const entry = open[side];
      open[side] = null;
      const points = entry.price - s.price;
      const gross = points * s.qty;
      trades.push({
        side, entryAt: entry.at, exitAt: s.at,
        entryPrice: entry.price, exitPrice: s.price,
        qty: s.qty, points, grossPnl: gross,
        reason: s.reason, symbol: s.symbol, strike: s.strike,
      });
    }
  }
  return trades;
}

function buildEquityCurve(trades) {
  // Simple cumulative gross P&L vs exit timestamp.
  let cum = 0;
  return trades.map(t => ({ t: t.exitAt, pnl: (cum += t.grossPnl) }));
}

async function run({ userId, date, cfgOverride }) {
  const t0 = Date.now();
  if (!userId || !date) throw new Error('userId and date are required');

  const session = await loadSession(userId, date);
  if (!session) {
    throw new Error(
      `no recorded session for ${date} — run premium-trigger live or paper on that date first to record ticks`,
    );
  }

  const cfg = { ...session.cfgSnap, ...(cfgOverride || {}) };
  cfg._backtest = true;
  cfg.mode = 'paper';                  // backtest never touches the broker
  cfg.tradingHoursEnabled = false;     // we replay timestamps; the
                                       // operator's window is implicit
                                       // in the recorded data.

  const legs = session.legs || {};
  if (!legs.ce || !legs.pe) {
    throw new Error('session legs incomplete; cannot replay');
  }

  const tokens = [
    legs.ce && String(legs.ce.token),
    legs.pe && String(legs.pe.token),
  ].filter(Boolean);

  const ticks = await loadTicks(userId, date, tokens);
  if (!ticks.length) {
    throw new Error(`no ticks recorded for ${date}`);
  }

  // Build an isolated, in-memory premiumTrigger instance for this replay.
  // It still uses the real `userId` so logging is identifiable, but the
  // _backtest flag short-circuits all persistence.
  const pt = createPremiumTrigger(userId);
  pt._testing.setCfg(cfg);
  pt._testing.setLeg('ce', legs.ce);
  pt._testing.setLeg('pe', legs.pe);
  pt._testing.attach();

  for (const t of ticks) {
    pt._testing.feedTick({ symbol: String(t.token), ltp: Number(t.ltp), ts: Number(t.ts_ms) });
  }

  const snapshot = pt._testing.snapshot();
  const status = pt.status();
  pt._testing.detach();

  const signals = (status && status.signals) || [];
  const trades = summariseSignals(signals);
  const wins = trades.filter(t => t.grossPnl > 0).length;
  const losses = trades.filter(t => t.grossPnl < 0).length;
  const totalPoints = trades.reduce((a, t) => a + t.points, 0);
  const totalPnl = trades.reduce((a, t) => a + t.grossPnl, 0);

  const result = {
    summary: {
      trades: trades.length,
      wins, losses,
      accuracy: (wins + losses) ? +(100 * wins / (wins + losses)).toFixed(2) : null,
      totalPoints: +totalPoints.toFixed(2),
      grossPnl: +totalPnl.toFixed(2),
      realizedPnl: snapshot.legState.ce.realizedPnl + snapshot.legState.pe.realizedPnl,
      totalCharges: snapshot.legState.ce.totalCharges + snapshot.legState.pe.totalCharges,
      maxDrawdown: snapshot.maxDrawdown,
      peakRealized: snapshot.peakRealized,
    },
    legs,
    cfg,
    trades,
    equity: buildEquityCurve(trades),
    signals: signals.slice(-RESULT_SIGNALS_CAP),
    tickCount: ticks.length,
    legSnapshot: snapshot.legState,
  };

  const durationMs = Date.now() - t0;
  let runId;
  try {
    const r = await query(
      `INSERT INTO backtest_runs (user_id, trade_date, cfg, result, duration_ms)
       VALUES (?, ?, ?, ?, ?)`,
      [userId, date, JSON.stringify(cfg), JSON.stringify(result), durationMs],
    );
    runId = r.insertId;
  } catch (err) {
    logger.warn('backtest: persist run failed', { err: err.message });
  }

  return { runId, durationMs, ...result };
}

async function listRuns(userId, limit = 30) {
  return query(
    `SELECT id, trade_date, duration_ms, created_at,
            JSON_EXTRACT(result, '$.summary') AS summary
       FROM backtest_runs
       WHERE user_id = ?
       ORDER BY id DESC
       LIMIT ?`,
    [userId, limit],
  );
}

async function getRun(userId, id) {
  const rows = await query(
    `SELECT id, trade_date, cfg, result, duration_ms, created_at
       FROM backtest_runs
       WHERE id = ? AND user_id = ?`,
    [id, userId],
  );
  if (!rows.length) return null;
  const row = rows[0];
  return {
    id: row.id,
    trade_date: row.trade_date instanceof Date
      ? row.trade_date.toISOString().slice(0, 10)
      : String(row.trade_date).slice(0, 10),
    cfg: typeof row.cfg === 'string' ? JSON.parse(row.cfg) : row.cfg,
    result: typeof row.result === 'string' ? JSON.parse(row.result) : row.result,
    duration_ms: row.duration_ms,
    created_at: row.created_at,
  };
}

module.exports = { run, listRecordedDates, listRuns, getRun };
