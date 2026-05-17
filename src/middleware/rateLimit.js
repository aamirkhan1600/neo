const rateLimit = require('express-rate-limit');

// Per-IP global rate limit. Tune for production.
const apiLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: 600,                  // 10 rps avg
  standardHeaders: true,
  legacyHeaders: false,
});

const loginLimiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 20,
  message: { error: 'too_many_login_attempts' },
  standardHeaders: true,
  legacyHeaders: false,
});

module.exports = { apiLimiter, loginLimiter };
