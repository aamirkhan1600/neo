require('dotenv').config();

const required = (name, fallback) => {
  const v = process.env[name] ?? fallback;
  if (v === undefined || v === '') throw new Error(`Missing env: ${name}`);
  return v;
};

module.exports = {
  env: process.env.NODE_ENV || 'development',
  port: parseInt(process.env.PORT || '3000', 10),
  appUrl: process.env.APP_URL || 'http://localhost:3000',

  db: {
    host: required('DB_HOST', '127.0.0.1'),
    port: parseInt(process.env.DB_PORT || '3306', 10),
    user: required('DB_USER', 'root'),
    password: process.env.DB_PASSWORD || '',
    database: required('DB_NAME', 'kotak_neo_saas'),
    connectionLimit: parseInt(process.env.DB_CONNECTION_LIMIT || '50', 10),
  },

  jwt: {
    secret: required('JWT_SECRET'),
    expiresIn: process.env.JWT_EXPIRES_IN || '15m',
    refreshExpiresIn: process.env.REFRESH_TOKEN_EXPIRES_IN || '7d',
  },

  sessionSecret: required('SESSION_SECRET'),
  tokenEncKey: required('TOKEN_ENC_KEY'),

  kotak: {
    // Whitespace in these values lands inside HTTP headers and Kotak's
    // gateway will reject them — strip aggressively.
    apiBase: (process.env.KOTAK_API_BASE || 'https://gw-napi.kotaksecurities.com').trim(),
    // Login lives at mis.kotaksecurities.com per the Trade API docs.
    loginUrl: (process.env.KOTAK_LOGIN_URL || 'https://mis.kotaksecurities.com').trim(),
    // The plain access token from NEO App → Invest → Trade API → Your
    // Applications. Used directly in Authorization header (no scheme).
    apiToken: (process.env.KOTAK_API_TOKEN || '').replace(/[\s\r\n]+/g, ''),
    finKey: (process.env.KOTAK_NEO_FIN_KEY || 'neotradeapi').trim(),
  },

  worker: {
    id: process.env.WORKER_ID || `worker-${process.pid}`,
    pollMs: parseInt(process.env.WORKER_POLL_MS || '200', 10),
    batchSize: parseInt(process.env.WORKER_BATCH_SIZE || '5', 10),
    maxRetries: parseInt(process.env.JOB_MAX_RETRIES || '3', 10),
    lockTimeoutMs: parseInt(process.env.JOB_LOCK_TIMEOUT_MS || '60000', 10),
  },

  brokerRps: parseInt(process.env.BROKER_RPS || '10', 10),
  ws: {
    maxSymbols: parseInt(process.env.WS_MAX_SYMBOLS || '200', 10),
  },
};
