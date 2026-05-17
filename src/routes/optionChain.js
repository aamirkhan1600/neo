const express = require('express');
const { authRequired } = require('../middleware/auth');
const instruments = require('../services/instrumentService');
const brokerService = require('../services/brokerService');
const tickRecorder = require('../services/tickRecorder');

const router = express.Router();

// Polled-LTP path. Browser sends an array of { token, segment } for the
// strikes currently visible; brokerService.fetchQuotesAuto chunks (10
// per HTTP call), spaces them out, and retries on Kotak's
// "too many requests". Returns a token -> ltp map.
const QUOTES_HARD_CAP = 250;

router.post('/quotes', authRequired, async (req, res, next) => {
  try {
    const symbols = Array.isArray(req.body?.symbols) ? req.body.symbols : [];
    if (!symbols.length) return res.json({ ltps: {} });
    if (symbols.length > QUOTES_HARD_CAP) {
      return res.status(400).json({ error: 'too_many_symbols', limit: QUOTES_HARD_CAP });
    }

    const queries = symbols
      .map(s => ({
        exchangeSegment: String(s.segment || s.exchangeSegment || '').toLowerCase(),
        symbol: String(s.token || s.symbol || ''),
      }))
      .filter(q => q.exchangeSegment && q.symbol);
    if (!queries.length) return res.json({ ltps: {} });

    let list = [];
    try {
      list = await brokerService.fetchQuotesAuto(req.user.id, queries, 'ltp');
    } catch (err) {
      return res.status(502).json({ error: 'quote_fetch_failed', message: err.message });
    }
    const ltps = {};
    const recordable = [];
    // Map token -> segment from the request payload so we can persist
    // the polled LTPs into tick_history below.
    const segByToken = new Map();
    for (const q of queries) segByToken.set(q.symbol, q.exchangeSegment);
    for (const row of list) {
      const t = String(row.exchange_token || row.exchangeToken || row.symbol || '');
      const ltp = parseFloat(row.ltp ?? row.LTP ?? row.last_price);
      if (t && Number.isFinite(ltp)) {
        ltps[t] = ltp;
        const seg = segByToken.get(t);
        if (seg && ltp > 0) recordable.push({ token: t, segment: seg, ltp });
      }
    }
    // Build the user's tick history opportunistically — polls fire from
    // the option-chain page every 2s while the user has it open, which
    // is enough to seed the backtest dataset without a separate daemon.
    if (recordable.length) {
      try { tickRecorder.recordTicks(req.user.id, recordable); } catch (_) {}
    }
    res.json({ ltps, returned: Object.keys(ltps).length });
  } catch (err) { next(err); }
});

// Distribution of ingested contracts by (underlying, option_type) — useful
// for confirming that scrip-master parsing actually populated CE/PE rows.
router.get('/diag', authRequired, async (_req, res, next) => {
  try { res.json(await instruments.diag()); } catch (err) { next(err); }
});

router.get('/indices', authRequired, async (_req, res, next) => {
  try {
    const persisted = await instruments.listIndices();
    const map = new Map(persisted.map(r => [r.underlying, r]));
    const out = [...instruments.MAJOR_UNDERLYINGS].map(u => ({
      underlying: u,
      contracts: map.get(u)?.contracts || 0,
      updated_at: map.get(u)?.updated_at || null,
    }));
    res.json(out);
  } catch (err) { next(err); }
});

router.get('/:underlying/expiries', authRequired, async (req, res, next) => {
  try {
    res.json(await instruments.listExpiries(req.params.underlying));
  } catch (err) { next(err); }
});

router.get('/:underlying', authRequired, async (req, res, next) => {
  try {
    let expiry = req.query.expiry;
    if (!expiry) {
      const expiries = await instruments.listExpiries(req.params.underlying);
      expiry = expiries[0];
    }
    if (!expiry) return res.status(404).json({ error: 'no_expiries_loaded',
      hint: 'POST /api/instruments/sync first' });
    res.json(await instruments.getChain(req.params.underlying, expiry));
  } catch (err) { next(err); }
});

// Trigger a scrip-master sync. Run this once per trading day; the table
// upserts so re-runs are safe.
router.post('/sync', authRequired, async (req, res, next) => {
  try {
    const out = await instruments.sync(req.user.id);
    res.json(out);
  } catch (err) { next(err); }
});

module.exports = router;
