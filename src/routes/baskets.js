const express = require('express');
const { authRequired } = require('../middleware/auth');
const baskets = require('../services/basketService');

const router = express.Router();

router.get('/', authRequired, async (req, res, next) => {
  try { res.json(await baskets.list(req.user.id)); } catch (err) { next(err); }
});

router.post('/', authRequired, async (req, res, next) => {
  try {
    const { name, legs } = req.body || {};
    if (!name || !Array.isArray(legs) || !legs.length) return res.status(400).json({ error: 'name_and_legs_required' });
    const id = await baskets.create(req.user.id, name, legs);
    res.status(201).json({ id });
  } catch (err) { next(err); }
});

router.post('/:id/execute', authRequired, async (req, res, next) => {
  try {
    const out = await baskets.execute(req.user.id, req.params.id);
    res.status(202).json(out);
  } catch (err) { next(err); }
});

module.exports = router;
