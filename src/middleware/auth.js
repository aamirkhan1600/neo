const { verifyAccess } = require('../services/authService');

// Reads the access token from Authorization: Bearer or from a httpOnly cookie.
function authRequired(req, res, next) {
  let token = null;
  const header = req.headers.authorization;
  if (header && header.startsWith('Bearer ')) token = header.slice(7);
  if (!token && req.cookies?.access_token) token = req.cookies.access_token;
  if (!token) return res.status(401).json({ error: 'unauthorized' });
  try {
    const payload = verifyAccess(token);
    req.user = { id: payload.sub, email: payload.email };
    next();
  } catch {
    return res.status(401).json({ error: 'invalid_token' });
  }
}

function authOptional(req, _res, next) {
  let token = null;
  if (req.headers.authorization?.startsWith('Bearer ')) token = req.headers.authorization.slice(7);
  if (!token && req.cookies?.access_token) token = req.cookies.access_token;
  if (token) {
    try { req.user = verifyAccess(token); req.user.id = req.user.sub; } catch {}
  }
  next();
}

// EJS pages: redirect to /login when not authed.
function pageAuth(req, res, next) {
  let token = req.cookies?.access_token;
  if (!token) return res.redirect('/login');
  try {
    const p = verifyAccess(token);
    req.user = { id: p.sub, email: p.email };
    res.locals.user = req.user;
    next();
  } catch {
    return res.redirect('/login');
  }
}

module.exports = { authRequired, authOptional, pageAuth };
