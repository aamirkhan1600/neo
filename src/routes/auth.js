const express = require('express');
const authService = require('../services/authService');
const { loginLimiter } = require('../middleware/rateLimit');
const { authRequired } = require('../middleware/auth');

const router = express.Router();

router.post('/register', loginLimiter, async (req, res, next) => {
  try {
    const { email, password, fullName } = req.body || {};
    if (!email || !password) return res.status(400).json({ error: 'email_password_required' });
    if (password.length < 8) return res.status(400).json({ error: 'password_too_short' });
    const out = await authService.register({ email, password, fullName });
    res.status(201).json(out);
  } catch (err) { next(err); }
});

router.post('/login', loginLimiter, async (req, res, next) => {
  try {
    const { email, password } = req.body || {};
    if (!email || !password) return res.status(400).json({ error: 'email_password_required' });
    const tokens = await authService.login({ email, password });
    res.cookie('access_token', tokens.accessToken, {
      httpOnly: true, secure: process.env.NODE_ENV === 'production', sameSite: 'lax', maxAge: 15 * 60 * 1000,
    });
    res.cookie('refresh_token', tokens.refreshToken, {
      httpOnly: true, secure: process.env.NODE_ENV === 'production', sameSite: 'lax',
      maxAge: 7 * 24 * 60 * 60 * 1000, path: '/api/auth',
    });
    res.json(tokens);
  } catch (err) { next(err); }
});

router.post('/refresh', async (req, res, next) => {
  try {
    const refreshToken = req.cookies?.refresh_token || req.body?.refreshToken;
    if (!refreshToken) return res.status(401).json({ error: 'no_refresh_token' });
    const tokens = await authService.rotateRefresh(refreshToken);
    res.cookie('access_token', tokens.accessToken, {
      httpOnly: true, secure: process.env.NODE_ENV === 'production', sameSite: 'lax', maxAge: 15 * 60 * 1000,
    });
    res.cookie('refresh_token', tokens.refreshToken, {
      httpOnly: true, secure: process.env.NODE_ENV === 'production', sameSite: 'lax',
      maxAge: 7 * 24 * 60 * 60 * 1000, path: '/api/auth',
    });
    res.json(tokens);
  } catch (err) { next(err); }
});

router.post('/logout', authRequired, async (req, res, next) => {
  try {
    await authService.revokeAll(req.user.id);
    res.clearCookie('access_token');
    res.clearCookie('refresh_token', { path: '/api/auth' });
    res.json({ ok: true });
  } catch (err) { next(err); }
});

router.get('/me', authRequired, (req, res) => {
  res.json({ user: req.user });
});

module.exports = router;
