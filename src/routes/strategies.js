const express = require('express');
const { authRequired } = require('../middleware/auth');
const engine = require('../services/strategyEngine');
const strategyRunner = require('../services/strategyRunner');
const marketData = require('../services/marketData');

const router = express.Router();

function refresh(userId) {
  return strategyRunner.refresh(userId, marketData.getClient(userId));
}

router.get('/', authRequired, async (req, res, next) => {
  try { res.json(await engine.list(req.user.id)); } catch (err) { next(err); }
});

router.post('/', authRequired, async (req, res, next) => {
  try {
    const { name, symbol, exchange, condition, action, qty, product, orderType, price, triggerPrice, tradingSymbol, cooldownSec, is_active } = req.body || {};
    if (!name || !symbol || !condition || !action || !qty) {
      return res.status(400).json({ error: 'missing_fields' });
    }
    const id = await engine.create(req.user.id, { name, symbol, exchange, condition, action, qty, product, orderType, price, triggerPrice, tradingSymbol, cooldownSec, is_active });
    await refresh(req.user.id);
    res.status(201).json({ id });
  } catch (err) { next(err); }
});

router.post('/:id/toggle', authRequired, async (req, res, next) => {
  try {
    await engine.setActive(req.user.id, req.params.id, !!req.body?.active);
    await refresh(req.user.id);
    res.json({ ok: true });
  } catch (err) { next(err); }
});

router.delete('/:id', authRequired, async (req, res, next) => {
  try {
    await engine.remove(req.user.id, req.params.id);
    await refresh(req.user.id);
    res.json({ ok: true });
  } catch (err) { next(err); }
});

module.exports = router;
