const express = require('express');
const { authRequired } = require('../middleware/auth');
const brokerService = require('../services/brokerService');
const reports = require('../services/reportService');
const jobQueue = require('../services/jobQueue');
const heartbeat = require('../services/workerHeartbeat');

const router = express.Router();

router.get('/orders', authRequired, async (req, res, next) => {
  try { res.json(await reports.listOrders(req.user.id, { limit: 200 })); } catch (err) { next(err); }
});

router.get('/trades', authRequired, async (req, res, next) => {
  try { res.json(await reports.listTrades(req.user.id, { limit: 200 })); } catch (err) { next(err); }
});

router.get('/positions', authRequired, async (req, res, next) => {
  try { res.json(await brokerService.fetchPositions(req.user.id)); } catch (err) { next(err); }
});

router.get('/holdings', authRequired, async (req, res, next) => {
  try { res.json(await brokerService.fetchHoldings(req.user.id)); } catch (err) { next(err); }
});

router.get('/queue', authRequired, async (_req, res, next) => {
  try { res.json(await jobQueue.stats()); } catch (err) { next(err); }
});

router.get('/workers', authRequired, async (_req, res, next) => {
  try { res.json(await heartbeat.listLive()); } catch (err) { next(err); }
});

module.exports = router;
