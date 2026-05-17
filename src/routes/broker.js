const express = require('express');
const { authRequired } = require('../middleware/auth');
const brokerService = require('../services/brokerService');
const accounts = require('../services/brokerAccount');
const marketData = require('../services/marketData');
const strategyRunner = require('../services/strategyRunner');
const socketServer = require('../ws/socketServer');

const router = express.Router();

// Diagnostic: confirm Kotak Trade API config. Reports shape only, never values.
router.get('/diag', authRequired, async (_req, res) => {
  const cfg = require('../config').kotak;
  const looksJwt = (s) => typeof s === 'string' && s.split('.').length === 3;
  const looksUuid = (s) => /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(s || '');
  res.json({
    loginUrl: cfg.loginUrl,
    apiBase: cfg.apiBase,
    finKey: cfg.finKey,
    apiToken: {
      set: !!cfg.apiToken,
      length: cfg.apiToken.length,
      looksJwt: looksJwt(cfg.apiToken),
      looksUuid: looksUuid(cfg.apiToken),
    },
    activeFlow: cfg.apiToken ? 'trade-api-token' : 'NONE',
    advice: !cfg.apiToken
      ? 'Set KOTAK_API_TOKEN from NEO App → Invest → Trade API → Your Applications.'
      : null,
  });
});

router.get('/status', authRequired, async (req, res, next) => {
  try {
    const acc = await accounts.getByUserId(req.user.id);
    if (!acc) return res.json({ connected: false });
    res.json({
      connected: acc.status === 'CONNECTED',
      status: acc.status,
      ucc: acc.ucc,
      mobile: acc.mobile,
      data_center: acc.data_center,
      last_login_at: acc.last_login_at,
    });
  } catch (err) { next(err); }
});

// Kotak Neo Trade API login. Body: { mobile, ucc, totp, mpin }.
// All four are required. Credentials are forwarded once and discarded;
// only the encrypted Trade token + sid + baseUrl are persisted.
router.post('/login', authRequired, async (req, res, next) => {
  try {
    const body = req.body || {};
    const mobile = String(body.mobile || '').trim();
    const ucc = String(body.ucc || '').trim();
    const totp = String(body.totp || '').trim();
    const mpin = String(body.mpin || '').trim();
    if (!mobile || !ucc || !totp || !mpin) {
      return res.status(400).json({ error: 'mobile_ucc_totp_mpin_required' });
    }
    const acc = await brokerService.loginFlow({ userId: req.user.id, mobile, ucc, totp, mpin });
    // Best-effort attach a market-data WS client and the tick→strategy
    // and tick→browser bridges. Failures here don't fail the login.
    try {
      const mdc = marketData.attachClient(req.user.id, acc);
      strategyRunner.attach(req.user.id, mdc);
      socketServer.bridgeTicksToBrowser(req.user.id, mdc);
    } catch {}
    res.json({ connected: true, ucc: acc.ucc, base_url: acc.base_url });
  } catch (err) { next(err); }
});

router.post('/logout', authRequired, async (req, res, next) => {
  try {
    // Stop the premium-trigger strategy first — its tokens belong to
    // the session we're about to invalidate. Persisted P&L stays.
    try {
      const ptManager = require('../services/premiumTriggerManager');
      await ptManager.forUser(req.user.id).onBrokerSessionEnd();
    } catch (_) { /* best-effort */ }
    await accounts.clearSession(req.user.id);
    strategyRunner.detach(req.user.id);
    marketData.detachClient(req.user.id);
    res.json({ ok: true });
  } catch (err) { next(err); }
});

module.exports = router;
