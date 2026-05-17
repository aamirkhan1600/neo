// Kotak Neo Trade API client — built directly off the official docs.
//
// Auth flow (TWO steps, NO OAuth2 round-trip):
//   1. POST mis.kotaksecurities.com/login/1.0/tradeApiLogin
//        Headers: Authorization: <KOTAK_API_TOKEN>   (plain, no scheme)
//                 neo-fin-key: neotradeapi
//        Body:    { mobileNumber, ucc, totp }
//        Returns: data.data.{token,sid,rid,kType:"View"}
//
//   2. POST mis.kotaksecurities.com/login/1.0/tradeApiValidate
//        Headers: Authorization: <KOTAK_API_TOKEN>
//                 Auth: <viewToken from step 1>
//                 sid:  <viewSid from step 1>
//                 neo-fin-key: neotradeapi
//        Body:    { mpin }
//        Returns: data.data.{token (Trade), sid, baseUrl, kType:"Trade", ...}
//
// Trading endpoints live on the per-user baseUrl from step 2 (e.g.
// https://cis.kotaksecurities.com). Their auth uses Auth + Sid headers
// (NOT Authorization), Content-Type: application/x-www-form-urlencoded,
// body is `jData=<urlencoded JSON>`.

const axios = require('axios');
const config = require('../config');
const logger = require('../utils/logger');

const DEFAULT_TIMEOUT_MS = 10000;

class BrokerError extends Error {
  constructor(message, { status, code, data, step } = {}) {
    super(message);
    this.name = 'BrokerError';
    this.status = status;
    this.code = code;
    this.data = data;
    this.step = step;
    this.isAuth = status === 401;
    this.isRateLimited = status === 429;
  }
}

function clean(s) {
  return (s == null ? '' : String(s)).replace(/[\s\r\n]+/g, '');
}

function wrap(err, step) {
  if (err.response) {
    const data = err.response.data || {};
    // Kotak's error envelope is inconsistent across endpoints — sometimes
    // `emsg`, sometimes `errMsg`, sometimes nested under `data` or `error`.
    // Try every shape we've seen so a real reason actually reaches the log
    // instead of the generic axios "Request failed with status code 422".
    const inner = data.data || {};
    const errArr = Array.isArray(data.errors) ? data.errors[0] : null;
    const msg =
      data.emsg || data.errMsg || data.message || data.errorMessage ||
      data.error_description || data.error ||
      inner.emsg || inner.errMsg || inner.message ||
      (errArr && (errArr.message || errArr.errMsg || errArr.code)) ||
      (typeof data === 'string' ? data : null) ||
      err.message;
    return new BrokerError(`${step}: ${msg}`, {
      status: err.response.status, code: data.errorCode || data.stCode, data, step,
    });
  }
  return new BrokerError(`${step}: ${err.message}`, { step });
}

function requireToken() {
  const token = clean(config.kotak.apiToken);
  if (!token) {
    throw new BrokerError(
      'KOTAK_API_TOKEN is empty. Get it from NEO App → Invest → Trade API → Your Applications.',
      { step: 'config' },
    );
  }
  return token;
}

// ---------------------------------------------------------------------------
// Step 1: tradeApiLogin
// ---------------------------------------------------------------------------
async function tradeApiLogin({ mobileNumber, ucc, totp }) {
  const accessToken = requireToken();
  const m = clean(mobileNumber);
  const u = clean(ucc);
  const t = clean(totp);
  if (!m || !u || !t) {
    throw new BrokerError('tradeApiLogin: mobileNumber, ucc, totp are all required', { step: 'tradeApiLogin' });
  }

  let data;
  try {
    const res = await axios.post(
      `${config.kotak.loginUrl}/login/1.0/tradeApiLogin`,
      { mobileNumber: m, ucc: u, totp: t },
      {
        timeout: DEFAULT_TIMEOUT_MS,
        headers: {
          Authorization: accessToken,             // PLAIN — no "Bearer "
          'neo-fin-key': config.kotak.finKey,
          'Content-Type': 'application/json',
          accept: 'application/json',
        },
      },
    );
    data = res.data;
  } catch (err) {
    throw wrap(err, 'tradeApiLogin');
  }

  const inner = data?.data || data || {};
  const token = clean(inner.token);
  const sid = clean(inner.sid);
  if (!token || !sid) {
    const msg = data?.message || data?.emsg || data?.errMsg ||
      'Kotak did not return a view token (check mobile / UCC / TOTP).';
    logger.warn('tradeApiLogin: malformed response', {
      topLevelKeys: Object.keys(data || {}).slice(0, 20),
      innerKeys: Object.keys(inner || {}).slice(0, 20),
    });
    throw new BrokerError(`tradeApiLogin: ${msg}`, { status: 200, data, step: 'tradeApiLogin' });
  }
  return {
    viewToken: token,
    viewSid: sid,
    rid: inner.rid,
    ucc: inner.ucc,
    greetingName: inner.greetingName,
    kType: inner.kType,                // 'View'
  };
}

// ---------------------------------------------------------------------------
// Step 2: tradeApiValidate
// ---------------------------------------------------------------------------
async function tradeApiValidate({ viewToken, viewSid, mpin }) {
  const accessToken = requireToken();
  const vt = clean(viewToken);
  const vs = clean(viewSid);
  const pin = clean(mpin);
  if (!vt || !vs) throw new BrokerError('tradeApiValidate: missing view token / sid', { step: 'tradeApiValidate' });
  if (!pin) throw new BrokerError('tradeApiValidate: mpin is required', { step: 'tradeApiValidate' });

  let data;
  try {
    const res = await axios.post(
      `${config.kotak.loginUrl}/login/1.0/tradeApiValidate`,
      { mpin: pin },
      {
        timeout: DEFAULT_TIMEOUT_MS,
        headers: {
          Authorization: accessToken,        // PLAIN
          Auth: vt,
          sid: vs,
          'neo-fin-key': config.kotak.finKey,
          'Content-Type': 'application/json',
          accept: 'application/json',
        },
      },
    );
    data = res.data;
  } catch (err) {
    throw wrap(err, 'tradeApiValidate');
  }

  const inner = data?.data || data || {};
  const token = clean(inner.token);
  const sid = clean(inner.sid) || vs;
  const baseUrl = (inner.baseUrl || '').trim();
  if (!token || inner.kType !== 'Trade') {
    const msg = data?.message || data?.emsg || data?.errMsg ||
      'Kotak did not return a Trade token (check MPIN).';
    logger.warn('tradeApiValidate: malformed response', {
      topLevelKeys: Object.keys(data || {}).slice(0, 20),
      kType: inner.kType,
    });
    throw new BrokerError(`tradeApiValidate: ${msg}`, { status: 200, data, step: 'tradeApiValidate' });
  }
  return {
    sessionToken: token,
    sid,
    baseUrl: baseUrl || config.kotak.apiBase,
    dataCenter: inner.dataCenter,
    ucc: inner.ucc,
    rid: inner.rid,
    kType: inner.kType,                       // 'Trade'
    greetingName: inner.greetingName,
    hsServerId: inner.hsServerId || null,
  };
}

// ---------------------------------------------------------------------------
// Authenticated trading-API helper
// ---------------------------------------------------------------------------
async function call(account, { method = 'POST', path, jData = null }) {
  const baseURL = (account.base_url || config.kotak.apiBase).replace(/\/+$/, '');
  const sessionToken = clean(account.session_token);
  const sid = clean(account.sid);
  if (!sessionToken || !sid) {
    throw new BrokerError('call: missing session_token / sid (re-login required)', { status: 401, step: 'call' });
  }

  const headers = {
    Auth: sessionToken,
    Sid: sid,
    'neo-fin-key': config.kotak.finKey,
    accept: 'application/json',
  };

  let body;
  if (method !== 'GET') {
    headers['Content-Type'] = 'application/x-www-form-urlencoded';
    if (jData != null) {
      const params = new URLSearchParams();
      params.set('jData', JSON.stringify(jData));
      body = params.toString();
    }
  }

  try {
    const res = await axios({
      method,
      url: baseURL + path,
      timeout: DEFAULT_TIMEOUT_MS,
      headers,
      data: body,
    });
    return res.data;
  } catch (err) {
    throw wrap(err, 'call');
  }
}

// ---------------------------------------------------------------------------
// Trading endpoints
// ---------------------------------------------------------------------------
async function placeOrder(account, order) {
  const jData = {
    am: order.afterMarket ? 'YES' : 'NO',
    dq: String(order.discQty || 0),
    es: order.exchangeSegment || order.exchange,   // expects nse_cm / bse_cm / nse_fo / etc
    mp: String(order.marketProtection || 0),
    pc: order.product,                              // CNC / MIS / NRML / CO / BO / MTF
    pf: 'N',
    pr: String(order.price || 0),
    pt: order.orderType,                            // L / MKT / SL / SL-M
    qt: String(order.qty),
    rt: order.validity || 'DAY',
    tp: String(order.triggerPrice || 0),
    ts: order.tradingSymbol,                        // e.g. ITBEES-EQ, RELIANCE-EQ
    tt: order.side === 'BUY' ? 'B' : 'S',
  };
  // Optional Bracket Order params
  if (order.product === 'BO') {
    Object.assign(jData, {
      sot: order.squareOffType || 'Absolute',
      slt: order.stopLossType || 'Absolute',
      slv: String(order.stopLossValue || 0),
      sov: String(order.squareOffValue || 0),
      tlt: order.trailingStopLoss ? 'Y' : 'N',
      tsv: String(order.trailingStopLossValue || 0),
    });
  }
  return call(account, { method: 'POST', path: '/quick/order/rule/ms/place', jData });
}

async function modifyOrder(account, m) {
  const jData = {
    tk: String(m.token || ''),
    mp: String(m.marketProtection || 0),
    pc: m.product,
    dd: 'NA',
    dq: String(m.discQty || 0),
    vd: m.validity || 'DAY',
    ts: m.tradingSymbol,
    tt: m.side === 'BUY' ? 'B' : 'S',
    pr: String(m.price || 0),
    tp: String(m.triggerPrice || 0),
    qt: String(m.qty),
    no: String(m.brokerOrderId),
    es: m.exchangeSegment || m.exchange,
    pt: m.orderType,
  };
  return call(account, { method: 'POST', path: '/quick/order/vr/modify', jData });
}

async function cancelOrder(account, { brokerOrderId, afterMarket = false, tradingSymbol = null }) {
  const jData = { on: String(brokerOrderId), am: afterMarket ? 'YES' : 'NO' };
  if (afterMarket && tradingSymbol) jData.ts = tradingSymbol;
  return call(account, { method: 'POST', path: '/quick/order/cancel', jData });
}

async function cancelBO(account, { brokerOrderId, afterMarket = false }) {
  return call(account, { method: 'POST', path: '/quick/order/bo/exit',
    jData: { on: String(brokerOrderId), am: afterMarket ? 'YES' : 'NO' } });
}

async function cancelCO(account, { brokerOrderId, afterMarket = false }) {
  return call(account, { method: 'POST', path: '/quick/order/co/exit',
    jData: { on: String(brokerOrderId), am: afterMarket ? 'YES' : 'NO' } });
}

async function getMargin(account, order) {
  const jData = {
    brkName: 'KOTAK',
    brnchId: 'ONLINE',
    exSeg: order.exchangeSegment || order.exchange,
    prc: String(order.price || 0),
    prcTp: order.orderType,
    prod: order.product,
    qty: String(order.qty),
    tok: String(order.token || ''),
    trnsTp: order.side === 'BUY' ? 'B' : 'S',
  };
  if (order.triggerPrice) jData.trgPrc = String(order.triggerPrice);
  return call(account, { method: 'POST', path: '/quick/user/check-margin', jData });
}

async function getLimits(account, { seg = 'ALL', exch = 'ALL', prod = 'ALL' } = {}) {
  return call(account, {
    method: 'POST',
    path: '/quick/user/limits',
    jData: { seg, exch, prod },
  });
}

async function orderBook(account) {
  return call(account, { method: 'GET', path: '/quick/user/orders' });
}

async function tradeBook(account) {
  return call(account, { method: 'GET', path: '/quick/user/trades' });
}

async function orderHistory(account, brokerOrderId) {
  return call(account, {
    method: 'POST',
    path: '/quick/order/history',
    jData: { nOrdNo: String(brokerOrderId) },
  });
}

async function positions(account) {
  return call(account, { method: 'GET', path: '/quick/user/positions' });
}

async function holdings(account) {
  return call(account, { method: 'GET', path: '/portfolio/v1/holdings' });
}

// Scrip Master uses the access token (NEO portal) directly, not the session.
async function scripMaster(account) {
  const baseURL = (account.base_url || config.kotak.apiBase).replace(/\/+$/, '');
  const accessToken = requireToken();
  try {
    const res = await axios.get(`${baseURL}/script-details/1.0/masterscrip/file-paths`, {
      timeout: DEFAULT_TIMEOUT_MS,
      headers: { Authorization: accessToken, accept: 'application/json' },
    });
    return res.data;
  } catch (err) {
    throw wrap(err, 'scripMaster');
  }
}

// Quotes REST API. Per the official Kotak Neo docs:
//   GET <baseUrl>/script-details/1.0/quotes/neosymbol/<seg>|<sym>[,<seg>|<sym>]/<filter>
//   Headers: Authorization: <access_token>  (NO neo-fin-key, NO Auth, NO Sid)
//
// `queries` is an array of { exchangeSegment, symbol }. `filter` is one of
//   all | 52W | scrip_details | circuit_limits | ohlc | oi | depth | ltp
async function getQuotes(account, queries, filter = 'ltp') {
  if (!Array.isArray(queries) || !queries.length) return [];
  const baseURL = (account.base_url || config.kotak.apiBase).replace(/\/+$/, '');
  const accessToken = requireToken();
  // Path components contain `|` and possibly spaces (for indices like
  // 'Nifty 50'). encodeURIComponent each component separately so commas
  // between queries are NOT encoded — Kotak's gateway parses them as
  // delimiters.
  const path = queries
    .map(q => `${q.exchangeSegment}|${q.symbol}`)
    .map(s => encodeURIComponent(s).replace(/%7C/g, '|').replace(/%20/g, '%20'))
    .join(',');
  const url = `${baseURL}/script-details/1.0/quotes/neosymbol/${path}/${encodeURIComponent(filter)}`;
  try {
    const res = await axios.get(url, {
      timeout: DEFAULT_TIMEOUT_MS,
      headers: { Authorization: accessToken, accept: 'application/json' },
    });
    return res.data;
  } catch (err) {
    throw wrap(err, 'getQuotes');
  }
}

// Historical OHLC bars. NOT in the documented Trade API surface — some
// Kotak Neo accounts have it enabled, others 404. SDK forks across
// the ecosystem use at least six different URL shapes. We probe each
// in sequence and return on the first 200 response. Non-404/405
// errors short-circuit (they indicate auth / server problems, not a
// shape mismatch).
//
// `interval` examples observed: I1, I5, I15, I60, I240, D, 1, 5, …

const HIST_SHAPES = [
  // 1. Title-case path, ms epoch path-args   (most common SDK shape)
  (seg, tok, iv, fMs, tMs) => ({ method: 'GET',
    path: `/Charts/v2/QUOTE/${encodeURIComponent(seg + '|' + tok)}/${encodeURIComponent(iv)}/${fMs}/${tMs}` }),
  // 2. lower-case
  (seg, tok, iv, fMs, tMs) => ({ method: 'GET',
    path: `/charts/v2/QUOTE/${encodeURIComponent(seg + '|' + tok)}/${encodeURIComponent(iv)}/${fMs}/${tMs}` }),
  // 3. seconds epoch path-args
  (seg, tok, iv, fMs, tMs) => ({ method: 'GET',
    path: `/Charts/v2/QUOTE/${encodeURIComponent(seg + '|' + tok)}/${encodeURIComponent(iv)}/${Math.floor(fMs/1000)}/${Math.floor(tMs/1000)}` }),
  // 4. ms epoch as query params
  (seg, tok, iv, fMs, tMs) => ({ method: 'GET',
    path: `/Charts/v2/QUOTE/${encodeURIComponent(seg + '|' + tok)}/${encodeURIComponent(iv)}`,
    params: { from: fMs, to: tMs } }),
  // 5. seconds epoch as query params
  (seg, tok, iv, fMs, tMs) => ({ method: 'GET',
    path: `/Charts/v2/QUOTE/${encodeURIComponent(seg + '|' + tok)}/${encodeURIComponent(iv)}`,
    params: { from: Math.floor(fMs/1000), to: Math.floor(tMs/1000) } }),
  // 6. older /charts/data shape
  (seg, tok, iv, fMs, tMs) => ({ method: 'GET',
    path: `/charts/data/${encodeURIComponent(seg + '|' + tok)}/${encodeURIComponent(iv)}`,
    params: { from: fMs, to: tMs } }),
];

async function getHistoricalBars(account, { token, segment, from, to, interval = 'I1', returnAttempts = false }) {
  const baseURL = (account.base_url || config.kotak.apiBase).replace(/\/+$/, '');
  const sessionToken = clean(account.session_token);
  const sid = clean(account.sid);
  if (!sessionToken || !sid) {
    throw new BrokerError('historical: missing session_token / sid', { status: 401 });
  }
  const fromMs = Number(from);
  const toMs = Number(to);
  if (!Number.isFinite(fromMs) || !Number.isFinite(toMs)) {
    throw new BrokerError('historical: from/to must be epoch ms', { step: 'history' });
  }
  const headers = {
    Auth: sessionToken,
    Sid: sid,
    'neo-fin-key': config.kotak.finKey,
    accept: 'application/json',
  };

  const attempts = [];
  let lastErr = null;
  for (let i = 0; i < HIST_SHAPES.length; i++) {
    const shape = HIST_SHAPES[i](segment, token, interval, fromMs, toMs);
    try {
      const res = await axios({
        method: shape.method,
        baseURL,
        url: shape.path,
        params: shape.params,
        timeout: 15000,
        headers,
      });
      attempts.push({ idx: i + 1, path: shape.path, params: shape.params, status: res.status });
      if (returnAttempts) return { data: res.data, attempts, ok: true };
      return res.data;
    } catch (err) {
      const status = err.response?.status;
      attempts.push({
        idx: i + 1, path: shape.path, params: shape.params,
        status: status || 'network',
        msg: err.response?.data?.message || err.message,
      });
      lastErr = err;
      // Auth / server errors are not shape mismatches — bail out.
      if (status && status !== 404 && status !== 405) break;
    }
  }
  if (returnAttempts) return { data: null, attempts, ok: false, error: lastErr?.message };
  const e = wrap(lastErr || new Error('all charts URL shapes returned 404'), 'getHistoricalBars');
  e.attempts = attempts;
  throw e;
}

module.exports = {
  BrokerError,
  tradeApiLogin,
  tradeApiValidate,
  placeOrder,
  modifyOrder,
  cancelOrder,
  cancelBO,
  cancelCO,
  getMargin,
  getLimits,
  orderBook,
  tradeBook,
  orderHistory,
  positions,
  holdings,
  scripMaster,
  getQuotes,
  getHistoricalBars,
};
