// Higher-level broker orchestration. Wraps the raw HTTP client with token
// management, rate limiting, and auto-relogin on 401.

const broker = require('./brokerClient');
const accounts = require('./brokerAccount');
const bucket = require('./tokenBucket');
const eventLog = require('./eventLog');
const jobQueue = require('./jobQueue');
const logger = require('../utils/logger');

class RateLimitedError extends Error {
  constructor(waitMs) { super('rate_limited'); this.waitMs = waitMs; }
}

async function execWithRateLimit(userId, fn) {
  if (!bucket.tryConsume(userId, 1)) {
    throw new RateLimitedError(bucket.waitMs(userId, 1));
  }
  return fn();
}

// Run a broker call, transparently triggering re-login on 401.
async function withSession(userId, fn) {
  const account = await accounts.getByUserId(userId);
  if (!account || !account.session_token) {
    const err = new Error('broker_not_connected'); err.status = 412; throw err;
  }
  try {
    return await execWithRateLimit(userId, () => fn(account));
  } catch (err) {
    if (err instanceof broker.BrokerError && err.isAuth) {
      logger.warn('broker session expired, scheduling relogin', { userId });
      await accounts.markStatus(userId, 'EXPIRED');
      await jobQueue.enqueue({ userId, type: 'RELOGIN', priority: 1, payload: {} });
      await eventLog.log(userId, 'BROKER_SESSION_EXPIRED', 'WARN', err.message);
    }
    throw err;
  }
}

// Two-step Kotak Neo Trade API login. Caller supplies credentials; we
// never persist mobile/UCC/TOTP/MPIN — only the resulting Trade token,
// sid, and per-user baseUrl (encrypted at rest).
async function loginFlow({ userId, mobile, ucc, totp, mpin }) {
  const step1 = await broker.tradeApiLogin({ mobileNumber: mobile, ucc, totp });
  const step2 = await broker.tradeApiValidate({
    viewToken: step1.viewToken,
    viewSid: step1.viewSid,
    mpin,
  });

  const account = await accounts.upsert(userId, {
    ucc: step2.ucc || step1.ucc || ucc,
    mobile,
    view_token: step1.viewToken,
    session_token: step2.sessionToken,
    sid: step2.sid,
    base_url: step2.baseUrl,
    data_center: step2.dataCenter,
    hsServerId: step2.hsServerId,
    status: 'CONNECTED',
    last_login_at: new Date(),
  });

  await eventLog.log(userId, 'BROKER_LOGIN', 'INFO',
    `Kotak Neo session established (baseUrl=${step2.baseUrl})`);
  return account;
}

// Public broker actions (used by routes + workers) ---------------------------

const placeOrder        = (userId, order) => withSession(userId, (a) => broker.placeOrder(a, order));
const modifyOrder       = (userId, mod)   => withSession(userId, (a) => broker.modifyOrder(a, mod));
const cancelOrder       = (userId, args)  => withSession(userId, (a) => broker.cancelOrder(a, args));
const cancelBO          = (userId, args)  => withSession(userId, (a) => broker.cancelBO(a, args));
const checkMargin       = (userId, order) => withSession(userId, (a) => broker.getMargin(a, order));
const fetchLimits       = (userId, opts)  => withSession(userId, (a) => broker.getLimits(a, opts));
const fetchOrderBook    = (userId)        => withSession(userId, (a) => broker.orderBook(a));
const fetchTradeBook    = (userId)        => withSession(userId, (a) => broker.tradeBook(a));
const fetchOrderHistory = (userId, oid)   => withSession(userId, (a) => broker.orderHistory(a, oid));
const fetchPositions    = (userId)        => withSession(userId, (a) => broker.positions(a));
const fetchHoldings     = (userId)        => withSession(userId, (a) => broker.holdings(a));
const fetchScripMaster  = (userId)        => withSession(userId, (a) => broker.scripMaster(a));
const fetchQuotes       = (userId, qs, f) => withSession(userId, (a) => broker.getQuotes(a, qs, f));
const fetchHistoricalBars = (userId, args) => withSession(userId, (a) => broker.getHistoricalBars(a, args));

// Auto-chunked quote fetcher: splits the query list into batches of
// QUOTES_BATCH (10) and runs them sequentially with a small gap so the
// REST quotes endpoint doesn't 429. Each batch is retried up to 3 times
// on rate-limit errors with exponential backoff. Returns the merged
// flat list of broker rows.
const QUOTES_BATCH = 10;
const QUOTES_GAP_MS = 200;

async function fetchQuotesAuto(userId, queries, filter = 'ltp') {
  const out = [];
  for (let i = 0; i < queries.length; i += QUOTES_BATCH) {
    const slice = queries.slice(i, i + QUOTES_BATCH);
    let attempt = 0;
    while (true) {
      try {
        const data = await fetchQuotes(userId, slice, filter);
        const list = Array.isArray(data) ? data : (data?.data || []);
        for (const row of list) out.push(row);
        break;
      } catch (err) {
        attempt += 1;
        const msg = String(err.message || '').toLowerCase();
        const isRate = err.status === 429
          || (err instanceof RateLimitedError)
          || msg.includes('too many') || msg.includes('rate');
        if (!isRate || attempt >= 3) throw err;
        const waitMs = (err.waitMs || 0) || (500 * attempt);
        await new Promise(r => setTimeout(r, waitMs));
      }
    }
    if (i + QUOTES_BATCH < queries.length) {
      await new Promise(r => setTimeout(r, QUOTES_GAP_MS));
    }
  }
  return out;
}

module.exports = {
  RateLimitedError,
  loginFlow,
  placeOrder,
  modifyOrder,
  cancelOrder,
  cancelBO,
  checkMargin,
  fetchLimits,
  fetchOrderBook,
  fetchTradeBook,
  fetchOrderHistory,
  fetchPositions,
  fetchHoldings,
  fetchScripMaster,
  fetchQuotes,
  fetchQuotesAuto,
  fetchHistoricalBars,
};
