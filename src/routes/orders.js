const express = require('express');
const { authRequired } = require('../middleware/auth');
const orderService = require('../services/orderService');
const reports = require('../services/reportService');
const jobQueue = require('../services/jobQueue');

const router = express.Router();

// Manual order placement (enqueues a TRADE job; the worker calls Kotak).
router.post('/', authRequired, async (req, res, next) => {
  try {
    const o = req.body || {};
    o.qty = parseInt(o.qty, 10);
    if (o.price) o.price = parseFloat(o.price);
    if (o.triggerPrice) o.triggerPrice = parseFloat(o.triggerPrice);
    const out = await orderService.enqueueTrade(req.user.id, o);
    res.status(202).json(out);
  } catch (err) { next(err); }
});

router.get('/', authRequired, async (req, res, next) => {
  try { res.json(await reports.listOrders(req.user.id, { limit: 200 })); } catch (err) { next(err); }
});

router.get('/trades', authRequired, async (req, res, next) => {
  try { res.json(await reports.listTrades(req.user.id, { limit: 200 })); } catch (err) { next(err); }
});

router.post('/sync', authRequired, async (req, res, next) => {
  try {
    await jobQueue.enqueue({ userId: req.user.id, type: 'SYNC_ORDERS', priority: 4, payload: {} });
    await jobQueue.enqueue({ userId: req.user.id, type: 'SYNC_TRADES', priority: 4, payload: {} });
    res.json({ ok: true });
  } catch (err) { next(err); }
});

router.post('/jobs/:id/retry', authRequired, async (req, res, next) => {
  try {
    await jobQueue.retryDead(req.params.id);
    res.json({ ok: true });
  } catch (err) { next(err); }
});

module.exports = router;
