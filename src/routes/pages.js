const express = require('express');
const { pageAuth } = require('../middleware/auth');
const accounts = require('../services/brokerAccount');
const reports = require('../services/reportService');
const engine = require('../services/strategyEngine');
const instruments = require('../services/instrumentService');

const router = express.Router();

router.get('/', (req, res) => {
  if (req.cookies?.access_token) return res.redirect('/dashboard');
  res.redirect('/login');
});

router.get('/login', (_req, res) => res.render('login', { title: 'Login', error: null }));
router.get('/register', (_req, res) => res.render('register', { title: 'Register', error: null }));

router.get('/dashboard', pageAuth, async (req, res, next) => {
  try {
    const broker = await accounts.getByUserId(req.user.id);
    res.render('dashboard', { title: 'Dashboard', broker });
  } catch (err) { next(err); }
});

router.get('/strategies', pageAuth, async (req, res, next) => {
  try {
    const list = await engine.list(req.user.id);
    res.render('strategies', { title: 'Strategies', strategies: list });
  } catch (err) { next(err); }
});

router.get('/orders', pageAuth, async (req, res, next) => {
  try {
    const orders = await reports.listOrders(req.user.id, { limit: 200 });
    res.render('orders', { title: 'Order Book', orders });
  } catch (err) { next(err); }
});

router.get('/portfolio', pageAuth, async (_req, res) => {
  res.render('portfolio', { title: 'Portfolio' });
});

router.get('/option-chain', pageAuth, async (_req, res) => {
  res.render('option-chain', {
    title: 'Option Chain',
    indices: [...instruments.MAJOR_UNDERLYINGS],
  });
});

router.get('/premium-trigger', pageAuth, (_req, res) => {
  res.render('premium-trigger', { title: 'Premium Trigger' });
});

router.get('/premium-trigger/backtest', pageAuth, (_req, res) => {
  res.render('premium-trigger-backtest', { title: 'Premium Trigger — Backtest' });
});

router.get('/premium-trigger/settings', pageAuth, async (req, res, next) => {
  try {
    // Load the user's persisted config server-side so the form renders
    // with the current values on the first paint — no XHR flash.
    const ptm = require('../services/premiumTriggerManager');
    const pt = ptm.forUser(req.user.id);
    let userCfg;
    try { userCfg = await pt.load(); } catch (_) { userCfg = pt.get(); }
    res.render('premium-trigger-settings', {
      title: 'Premium Trigger — Settings',
      cfg: { ...userCfg, indices: [...instruments.MAJOR_UNDERLYINGS] },
    });
  } catch (err) { next(err); }
});

router.get('/broker', pageAuth, async (req, res, next) => {
  try {
    const broker = await accounts.getByUserId(req.user.id);
    res.render('broker', { title: 'Broker Connect', broker });
  } catch (err) { next(err); }
});

module.exports = router;
