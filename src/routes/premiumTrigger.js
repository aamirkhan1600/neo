const express = require('express');
const { authRequired } = require('../middleware/auth');
const manager = require('../services/premiumTriggerManager');
const instruments = require('../services/instrumentService');
const backtest = require('../services/backtestRunner');
const backfill = require('../services/backfillRunner');
const { query } = require('../db/pool');

const router = express.Router();

function ptFor(req) { return manager.forUser(req.user.id); }

router.get('/status', authRequired, async (req, res, next) => {
  try {
    const pt = ptFor(req);
    await pt.load();
    res.json(pt.status());
  } catch (err) { next(err); }
});

router.get('/underlyings', authRequired, async (_req, res, next) => {
  try {
    res.json([...instruments.MAJOR_UNDERLYINGS]);
  } catch (err) { next(err); }
});

router.get('/expiries', authRequired, async (req, res, next) => {
  try {
    const u = String(req.query.underlying || '').trim();
    if (!u) return res.status(400).json({ error: 'underlying required' });
    res.json(await instruments.listExpiries(u));
  } catch (err) { next(err); }
});

// Refresh the broker's NFO scrip master so a fresh weekly expiry shows
// up in the Expiry dropdown. Same as POST /api/option-chain/sync but
// kept here so the spec's surface map is complete.
router.post('/reload-instruments', authRequired, async (req, res, next) => {
  try {
    const out = await instruments.sync(req.user.id);
    res.json({ ok: true, ...out });
  } catch (err) { next(err); }
});

router.post('/settings', authRequired, async (req, res, next) => {
  try {
    const body = req.body || {};
    const patch = {};
    for (const k of [
      'enabled', 'mode', 'underlyingName', 'expiry', 'strikeMode', 'fixedStrike', 'qty',
      'maxDailyLoss', 'autoRestrikeOnDrift', 'restrikeDwellSec', 'restrikeCooldownSec',
      'useBrokerBracketExits', 'bracketSafetyOvershoot', 'bracketSafetyTimeoutMs', 'bracketStatusPollMs',
      'brokeragePerOrder', 'sttPctSell', 'exchTxnPct', 'sebiPct', 'stampPctBuy', 'gstPct',
      'tradingHoursEnabled', 'tradingHoursStart', 'eodCutoff', 'eodAutoExit', 'tradingHoursEnd',
    ]) {
      if (body[k] !== undefined) patch[k] = body[k];
    }
    // tradingHoursEnabled is a checkbox — undefined ⇒ unchecked ⇒ false.
    // The form-submit JS sets it explicitly; this branch covers other clients.
    if (patch.tradingHoursEnabled !== undefined) {
      patch.tradingHoursEnabled = patch.tradingHoursEnabled === true
        || patch.tradingHoursEnabled === 'true' || patch.tradingHoursEnabled === 'on'
        || patch.tradingHoursEnabled === '1';
    }
    // Coerce booleans coming from form-encoded checkboxes
    if (patch.autoRestrikeOnDrift !== undefined) {
      patch.autoRestrikeOnDrift = patch.autoRestrikeOnDrift === true
        || patch.autoRestrikeOnDrift === 'true' || patch.autoRestrikeOnDrift === 'on'
        || patch.autoRestrikeOnDrift === '1';
    }
    if (patch.enabled !== undefined) {
      patch.enabled = patch.enabled === true
        || patch.enabled === 'true' || patch.enabled === 'on' || patch.enabled === '1';
    }
    if (patch.useBrokerBracketExits !== undefined) {
      patch.useBrokerBracketExits = patch.useBrokerBracketExits === true
        || patch.useBrokerBracketExits === 'true' || patch.useBrokerBracketExits === 'on'
        || patch.useBrokerBracketExits === '1';
    }

    if (body.legs) patch.legs = body.legs;
    // Flat per-leg form fields (ce_entryTrigger, pe_target, …)
    for (const side of ['ce', 'pe']) {
      const leg = {};
      for (const k of [
        'enabled', 'entryTrigger', 'target', 'stoploss', 'reentryOffset',
        'reentryTarget', 'maxEntryDeviation', 'rearmRange', 'rearmLower',
        'rearmUpper', 'captureFadeThreshold',
        'trailingStopLoss', 'trailingStopLossValue',
      ]) {
        const key = `${side}_${k}`;
        if (body[key] !== undefined) leg[k] = body[key];
      }
      if (leg.enabled !== undefined) {
        leg.enabled = leg.enabled === true || leg.enabled === 'true'
          || leg.enabled === 'on' || leg.enabled === '1';
      }
      if (leg.trailingStopLoss !== undefined) {
        leg.trailingStopLoss = leg.trailingStopLoss === true
          || leg.trailingStopLoss === 'true' || leg.trailingStopLoss === 'on'
          || leg.trailingStopLoss === '1';
      }
      if (Object.keys(leg).length) {
        patch.legs = patch.legs || {};
        patch.legs[side] = { ...(patch.legs[side] || {}), ...leg };
      }
    }

    const pt = ptFor(req);
    const next = await pt.save(patch);
    res.json({ ok: true, cfg: next });
  } catch (err) { next(err); }
});

router.post('/toggle', authRequired, async (req, res, next) => {
  try {
    const on = req.body?.on === true || req.body?.on === 'true' || req.query.on === 'true';
    const pt = ptFor(req);
    await pt.toggle(on);
    res.json({ ok: true, status: pt.status() });
  } catch (err) { next(err); }
});

router.post('/start', authRequired, async (req, res, next) => {
  try { const pt = ptFor(req); await pt.start(); res.json(pt.status()); }
  catch (err) { next(err); }
});

router.post('/stop', authRequired, async (req, res, next) => {
  try { const pt = ptFor(req); await pt.stop(); res.json(pt.status()); }
  catch (err) { next(err); }
});

router.post('/exit-all', authRequired, async (req, res, next) => {
  try {
    const pt = ptFor(req);
    res.json(await pt.exitAll(req.body?.reason || 'manual'));
  } catch (err) { next(err); }
});

router.post('/restrike', authRequired, async (req, res, next) => {
  try {
    const side = String(req.query.side || req.body?.side || '').toLowerCase();
    const pt = ptFor(req);
    res.json(await pt.restrikeLeg(side));
  } catch (err) { next(err); }
});

router.get('/signals', authRequired, async (req, res, next) => {
  try {
    const limit = Math.min(200, parseInt(req.query.limit, 10) || 50);
    const rows = await query(
      `SELECT id, side, action, reason, price, qty, symbol, strike,
              mode, status, broker_order_id, reject_reason, created_at
         FROM premium_trigger_signals
         WHERE user_id = ?
         ORDER BY id DESC
         LIMIT ?`,
      [req.user.id, limit],
    );
    res.json(rows);
  } catch (err) { next(err); }
});

// CSV export of the full signal log for offline analysis / audit.
router.get('/signals.csv', authRequired, async (req, res, next) => {
  try {
    const limit = Math.min(10000, parseInt(req.query.limit, 10) || 5000);
    const rows = await query(
      `SELECT created_at, mode, side, action, reason, price, qty, symbol,
              strike, status, broker_order_id, reject_reason
         FROM premium_trigger_signals
         WHERE user_id = ?
         ORDER BY id DESC
         LIMIT ?`,
      [req.user.id, limit],
    );
    const header = 'created_at,mode,side,action,reason,price,qty,symbol,strike,status,broker_order_id,reject_reason\n';
    const escape = (v) => {
      if (v == null) return '';
      const s = String(v);
      return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
    };
    const body = rows.map(r => [
      r.created_at && r.created_at.toISOString ? r.created_at.toISOString() : r.created_at,
      r.mode, r.side, r.action, r.reason, r.price, r.qty,
      r.symbol, r.strike, r.status, r.broker_order_id, r.reject_reason,
    ].map(escape).join(',')).join('\n');
    const stamp = new Date().toISOString().slice(0, 10);
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="premium-trigger-signals-${stamp}.csv"`);
    res.send(header + body + '\n');
  } catch (err) { next(err); }
});

// ---------------------------------------------------------------------------
// Backtest routes — replay recorded ticks for a chosen date.
// ---------------------------------------------------------------------------

router.get('/backtest/dates', authRequired, async (req, res, next) => {
  try { res.json(await backtest.listRecordedDates(req.user.id)); }
  catch (err) { next(err); }
});

router.get('/backtest/runs', authRequired, async (req, res, next) => {
  try { res.json(await backtest.listRuns(req.user.id)); }
  catch (err) { next(err); }
});

router.get('/backtest/runs/:id', authRequired, async (req, res, next) => {
  try {
    const r = await backtest.getRun(req.user.id, req.params.id);
    if (!r) return res.status(404).json({ error: 'not_found' });
    res.json(r);
  } catch (err) { next(err); }
});

// Diagnostic for the (undocumented) Kotak charts/historical endpoint.
// Probes each known URL shape with a tiny range against the user's
// most-recently-resolved leg token and reports per-shape status codes
// + first ~120 chars of any response body. Read-only — never writes
// to tick_history. Useful when /backtest/import returns "endpoint not
// available" so the operator can see exactly which shape (if any) the
// account actually accepts.
router.get('/backtest/diag', authRequired, async (req, res) => {
  try {
    // Pick a probe token: most recent leg snapshot for this user, or
    // the explicit ?token + ?segment query overrides.
    let segment = String(req.query.segment || '').toLowerCase();
    let token = String(req.query.token || '').trim();
    let date = String(req.query.date || '').trim();
    if (!token || !segment) {
      const rows = await query(
        `SELECT trade_date, legs FROM premium_trigger_session
           WHERE user_id = ?
           ORDER BY trade_date DESC
           LIMIT 1`,
        [req.user.id],
      );
      if (rows.length) {
        const legs = typeof rows[0].legs === 'string' ? JSON.parse(rows[0].legs) : rows[0].legs;
        if (legs?.ce) {
          token = token || String(legs.ce.token);
          segment = segment || legs.ce.exchangeSegment;
          date = date || (rows[0].trade_date instanceof Date
            ? rows[0].trade_date.toISOString().slice(0, 10)
            : String(rows[0].trade_date).slice(0, 10));
        }
      }
    }
    if (!token || !segment) {
      return res.status(400).json({
        error: 'no_probe_token',
        message: 'No leg snapshot for this user. Save settings + click Backfill once first to seed a probe target, OR pass ?token=&segment=&date=YYYY-MM-DD.',
      });
    }
    if (!date || !/^\d{4}-\d{2}-\d{2}$/.test(date)) {
      // Default: yesterday in IST.
      const d = new Date(Date.now() - 24 * 3600 * 1000);
      date = d.toISOString().slice(0, 10);
    }
    const fromMs = backfill.rangeForDate(date).fromMs;
    const toMs = fromMs + 60 * 60 * 1000; // probe a 1-hour window

    const accounts = require('../services/brokerAccount');
    const broker = require('../services/brokerClient');
    const account = await accounts.getByUserId(req.user.id);
    if (!account?.session_token) {
      return res.status(412).json({ error: 'broker_not_connected' });
    }
    const result = await broker.getHistoricalBars(account, {
      token, segment, from: fromMs, to: toMs, interval: 'I1', returnAttempts: true,
    });
    res.json({
      probe: { token, segment, date, fromMs, toMs },
      ...result,
      bars: result.data ? backfill.parseBars(result.data).length : 0,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Best-effort historical backfill via Kotak's (undocumented) charts
// endpoint. Synthesises 4 ticks per 1-minute OHLC bar into tick_history.
// Approximate — see views/premium-trigger-backtest.ejs for the operator
// disclaimer. Fails cleanly if the endpoint isn't enabled on the account.
router.post('/backtest/import', authRequired, async (req, res) => {
  try {
    const date = String(req.body?.date || '').trim();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
      return res.status(400).json({ error: 'date YYYY-MM-DD required' });
    }
    const interval = String(req.body?.interval || 'I1').trim();
    const out = await backfill.tryImport({ userId: req.user.id, date, interval });
    res.json({ ok: true, ...out });
  } catch (err) {
    res.status(400).json({ error: 'import_failed', message: err.message });
  }
});

router.post('/backtest/run', authRequired, async (req, res, next) => {
  try {
    const date = String(req.body?.date || '').trim();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
      return res.status(400).json({ error: 'date YYYY-MM-DD required' });
    }
    const cfgOverride = req.body?.cfg && typeof req.body.cfg === 'object'
      ? req.body.cfg : null;
    const out = await backtest.run({ userId: req.user.id, date, cfgOverride });
    res.json(out);
  } catch (err) {
    res.status(400).json({ error: 'backtest_failed', message: err.message });
  }
});

module.exports = router;
