// Scrip-master ingestion + option-chain queries.
//
// We fetch the file-paths from Kotak Neo's /script-details/1.0/masterscrip
// API, download the relevant CSVs, and persist only contracts whose
// `pSymbolName` is one of the major indices supported by this app.
//
// Kotak's transformed F&O CSV columns (per the public docs / SDK) include:
//   pSymbol            instrument token
//   pExchSeg           exchange segment, e.g. nse_fo
//   pInstType          OPTIDX / FUTIDX / OPTSTK / FUTSTK / EQ
//   pSymbolName        underlying, e.g. NIFTY / BANKNIFTY / RELIANCE
//   pTrdSymbol         trading symbol used by Orders API
//   pOptionType        CE / PE / XX
//   dStrikePrice       strike (sometimes in paise — divided when > 100000)
//   lExpiryDate        epoch seconds
//   lLotSize           lot size
//   lTickSize          tick size in paise
// Header names vary slightly by version; we read defensively.

const axios = require('axios');
const { query } = require('../db/pool');
const brokerService = require('./brokerService');
const logger = require('../utils/logger');

const MAJOR_UNDERLYINGS = new Set([
  'NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'SENSEX', 'BANKEX',
]);

// Subset of files we care about. Major-index F&O lives on NSE (NIFTY,
// BANKNIFTY, FINNIFTY, MIDCPNIFTY) and BSE (SENSEX, BANKEX).
const ALLOWED_SEGMENTS = new Set(['nse_fo', 'bse_fo']);

function pickHeader(headers, candidates) {
  const lower = headers.map(h => h.toLowerCase());
  for (const c of candidates) {
    const idx = lower.indexOf(c.toLowerCase());
    if (idx !== -1) return headers[idx];
  }
  return null;
}

// Tiny CSV parser — Kotak transformed CSVs are simple (no embedded commas
// or quoted multi-line fields). Falls back gracefully on stray quotes.
function parseCsv(text) {
  const lines = text.split(/\r?\n/);
  while (lines.length && !lines[0].trim()) lines.shift();
  if (!lines.length) return { headers: [], rows: [] };
  const headers = lines[0].split(',').map(h => h.trim().replace(/^"|"$/g, ''));
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i];
    if (!line.trim()) continue;
    const cols = line.split(',').map(c => c.trim().replace(/^"|"$/g, ''));
    rows.push(cols);
  }
  return { headers, rows };
}

function detectSegmentFromUrl(url) {
  const m = url.match(/\/([a-z]+_[a-z]+)(?:-v\d+)?\.csv/i);
  return m ? m[1].toLowerCase() : null;
}

function normalizeStrike(raw) {
  const v = parseFloat(raw);
  if (!Number.isFinite(v) || v <= 0) return null;
  // Some Neo CSVs encode strike in paise (NIFTY 22000 -> 2200000).
  return v >= 100000 ? +(v / 100).toFixed(4) : +v.toFixed(4);
}

// NSE/BSE F&O trading symbols on Kotak Neo come in two flavors:
//   Monthly:  PREFIX + YY + MMM         + STRIKE + (CE|PE)   e.g. NIFTY26MAY24500CE
//   Weekly:   PREFIX + YY + M[1-9OND]   + DD     + STRIKE + (CE|PE)   e.g. NIFTY2650523350CE
//                                                                      └26┘└5┘└05┘└23350┘
// where M is 1-9 for Jan-Sep, O for Oct, N for Nov, D for Dec.
// Parsing the symbol is the only reliable fallback when dStrikePrice
// is missing from the CSV.
function parseTradingSymbol(ts, underlying) {
  if (!ts) return {};
  const upper = String(ts).toUpperCase();
  const tail = upper.match(/^(.+?)(CE|PE)$/);
  if (!tail) return {};
  const optionType = tail[2];
  let body = tail[1];
  const u = String(underlying || '').toUpperCase();
  if (u && body.startsWith(u)) body = body.slice(u.length);

  // Monthly expiry: 2 digit year + 3-letter month + strike
  const monthly = body.match(/^(\d{2})([A-Z]{3})(\d+(?:\.\d+)?)$/);
  if (monthly) return { optionType, strike: parseFloat(monthly[3]) };

  // Weekly expiry: 2 digit year + 1 char month code + 2 digit day + strike
  const weekly = body.match(/^(\d{2})([1-9OND])(\d{2})(\d+(?:\.\d+)?)$/);
  if (weekly) return { optionType, strike: parseFloat(weekly[4]) };

  // Fallback: take the trailing digit run only — but cap to a plausible
  // strike width (4-7 digits) so we don't accidentally swallow expiry digits.
  const trailing = body.match(/(\d{4,7})$/);
  if (trailing) return { optionType, strike: parseFloat(trailing[1]) };
  return { optionType };
}

// Kotak Neo's lExpiryDate is "seconds since 1980-01-01 UTC" (NSE/Kotak
// convention), NOT Unix seconds. Convert by adding the 10-year offset.
//   1980-01-01 UTC in Unix = 315 532 800 s
const NEO_EPOCH_OFFSET_S = 315532800;

function epochToDate(raw) {
  const e = parseInt(raw, 10);
  if (!Number.isFinite(e) || e <= 0) return null;
  // If the value already lands inside a sane range when treated as 1980-epoch
  // seconds, prefer that. Otherwise fall back to interpreting as Unix seconds
  // (some segments / older masters expose Unix epochs directly).
  const candidates = [
    e + NEO_EPOCH_OFFSET_S,    // Kotak: seconds since 1980
    e,                         // Unix seconds
    Math.floor(e / 1000),      // Unix milliseconds
  ];
  const nowS = Date.now() / 1000;
  for (const s of candidates) {
    if (s > nowS - 365 * 24 * 3600 && s < nowS + 5 * 365 * 24 * 3600) {
      const d = new Date(s * 1000);
      if (!Number.isNaN(d.getTime())) return d.toISOString().slice(0, 10);
    }
  }
  return null;
}

async function downloadAndIngest(url) {
  const segment = detectSegmentFromUrl(url);
  if (!segment || !ALLOWED_SEGMENTS.has(segment)) return 0;

  let text;
  try {
    const res = await axios.get(url, { responseType: 'text', timeout: 60000, transformResponse: x => x });
    text = res.data;
  } catch (err) {
    logger.warn('instrumentService: download failed', { url, err: err.message });
    return 0;
  }

  const { headers, rows } = parseCsv(text);
  if (!rows.length) return 0;

  const H = {
    token: pickHeader(headers, ['pSymbol', 'lInstrumentToken', 'instrumentToken']),
    exSeg: pickHeader(headers, ['pExchSeg', 'exchangeSegment']),
    instType: pickHeader(headers, ['pInstType', 'pInstName', 'instrumentType']),
    underlying: pickHeader(headers, ['pSymbolName', 'pSymName', 'underlyingSymbol', 'symbolName']),
    tradingSymbol: pickHeader(headers, ['pTrdSymbol', 'tradingSymbol']),
    optionType: pickHeader(headers, ['pOptionType', 'pOpType', 'optionType', 'OptionType', 'pOptType']),
    strike: pickHeader(headers, ['dStrikePrice', 'strikePrice', 'strike']),
    expiry: pickHeader(headers, ['lExpiryDate', 'expiryDate', 'expiry']),
    lotSize: pickHeader(headers, ['lLotSize', 'lotSize']),
    tickSize: pickHeader(headers, ['lTickSize', 'tickSize']),
  };

  if (!H.token || !H.tradingSymbol || !H.underlying) {
    logger.warn('instrumentService: csv missing required headers', { url, headers });
    return 0;
  }

  const idx = (name) => headers.indexOf(H[name]);
  const I = {
    token: idx('token'),
    exSeg: idx('exSeg'),
    instType: idx('instType'),
    underlying: idx('underlying'),
    tradingSymbol: idx('tradingSymbol'),
    optionType: idx('optionType'),
    strike: idx('strike'),
    expiry: idx('expiry'),
    lotSize: idx('lotSize'),
    tickSize: idx('tickSize'),
  };

  let inserted = 0;
  for (const cols of rows) {
    const instType = (cols[I.instType] || '').toUpperCase();
    const underlying = (cols[I.underlying] || '').toUpperCase();
    if (!['OPTIDX', 'FUTIDX'].includes(instType)) continue;
    if (!MAJOR_UNDERLYINGS.has(underlying)) continue;

    const token = cols[I.token];
    const tradingSymbol = cols[I.tradingSymbol];
    if (!token || !tradingSymbol) continue;

    let optionType = 'XX';
    let parsedStrike = null;
    const rawOpt = I.optionType >= 0 ? (cols[I.optionType] || '').toUpperCase() : '';
    if (rawOpt === 'CE' || rawOpt === 'PE') optionType = rawOpt;
    else if (instType === 'FUTIDX') optionType = 'FUT';
    else {
      // Derive both option type AND strike from the trading symbol when the
      // CSV column is missing or has a value we don't recognize.
      const parsed = parseTradingSymbol(tradingSymbol, underlying);
      if (parsed.optionType) optionType = parsed.optionType;
      parsedStrike = parsed.strike != null ? parsed.strike : null;
    }

    let strike = null;
    if (optionType === 'CE' || optionType === 'PE') {
      strike = normalizeStrike(cols[I.strike]);
      if (strike == null && parsedStrike != null) strike = parsedStrike;
      if (strike == null) {
        const parsed = parseTradingSymbol(tradingSymbol, underlying);
        if (parsed.strike != null) strike = parsed.strike;
      }
    }
    const expiry = epochToDate(cols[I.expiry]);
    const lotSize = parseInt(cols[I.lotSize] || '1', 10) || 1;
    const tickRaw = parseFloat(cols[I.tickSize] || '0');
    const tickSize = Number.isFinite(tickRaw) && tickRaw > 0
      ? +(tickRaw >= 100 ? tickRaw / 100 : tickRaw).toFixed(4)
      : null;

    try {
      await query(
        `INSERT INTO instruments
          (token, exchange_segment, instrument_type, underlying, trading_symbol,
           option_type, strike, expiry_date, lot_size, tick_size)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON DUPLICATE KEY UPDATE
           instrument_type = VALUES(instrument_type),
           underlying = VALUES(underlying),
           trading_symbol = VALUES(trading_symbol),
           option_type = VALUES(option_type),
           strike = VALUES(strike),
           expiry_date = VALUES(expiry_date),
           lot_size = VALUES(lot_size),
           tick_size = VALUES(tick_size),
           updated_at = NOW()`,
        [String(token), segment, instType, underlying, tradingSymbol,
         optionType, strike, expiry, lotSize, tickSize],
      );
      inserted++;
    } catch (err) {
      logger.warn('instrumentService: insert failed', { token, err: err.message });
    }
  }
  return inserted;
}

async function sync(userId) {
  let res;
  try {
    res = await brokerService.fetchScripMaster(userId);
  } catch (err) {
    throw new Error(`scripMaster fetch failed: ${err.message}`);
  }
  const paths = res?.data?.filesPaths || [];
  const wanted = paths.filter(p => /(nse_fo|bse_fo)\.csv/i.test(p));
  let total = 0;
  const perFile = {};
  for (const url of wanted) {
    const n = await downloadAndIngest(url);
    perFile[url] = n;
    total += n;
  }
  logger.info('instrumentService: sync complete', { total, perFile: Object.keys(perFile).length });
  return { total, perFile };
}

// ---------------------------------------------------------------------------
// Chain queries
// ---------------------------------------------------------------------------

async function listIndices() {
  const rows = await query(
    `SELECT underlying, COUNT(*) AS contracts, MAX(updated_at) AS updated_at
       FROM instruments
       WHERE option_type IN ('CE','PE')
       GROUP BY underlying
       ORDER BY underlying ASC`,
  );
  return rows;
}

async function listExpiries(underlying) {
  const u = String(underlying || '').toUpperCase();
  if (!MAJOR_UNDERLYINGS.has(u)) return [];
  // Hide already-expired contracts (Kotak's scrip master keeps them around
  // for a while). Today's expiry is still tradable until close, so use >=.
  const rows = await query(
    `SELECT DISTINCT expiry_date FROM instruments
       WHERE underlying = ? AND option_type IN ('CE','PE')
         AND expiry_date IS NOT NULL
         AND expiry_date >= CURDATE()
       ORDER BY expiry_date ASC`,
    [u],
  );
  return rows
    .map(r => r.expiry_date instanceof Date
      ? r.expiry_date.toISOString().slice(0, 10)
      : String(r.expiry_date).slice(0, 10))
    .filter(Boolean);
}

async function getChain(underlying, expiry) {
  const u = String(underlying || '').toUpperCase();
  if (!MAJOR_UNDERLYINGS.has(u)) return { underlying: u, expiry, chain: [] };
  const rows = await query(
    `SELECT token, exchange_segment, trading_symbol, option_type, strike, lot_size, tick_size, expiry_date
       FROM instruments
       WHERE underlying = ? AND expiry_date = ? AND option_type IN ('CE','PE')
       ORDER BY strike ASC, option_type ASC`,
    [u, expiry],
  );
  const byStrike = new Map();
  for (const r of rows) {
    const k = parseFloat(r.strike);
    if (!Number.isFinite(k)) continue;
    let row = byStrike.get(k);
    if (!row) { row = { strike: k }; byStrike.set(k, row); }
    const cell = {
      token: String(r.token),
      tradingSymbol: r.trading_symbol,
      exchangeSegment: r.exchange_segment,
      lotSize: r.lot_size,
      tickSize: r.tick_size != null ? Number(r.tick_size) : null,
    };
    if (r.option_type === 'CE') row.ce = cell;
    else row.pe = cell;
  }
  return {
    underlying: u,
    expiry,
    lotSize: rows[0]?.lot_size || null,
    chain: [...byStrike.values()].sort((a, b) => a.strike - b.strike),
  };
}

async function diag() {
  const rows = await query(
    `SELECT underlying, option_type, COUNT(*) AS n,
            MIN(expiry_date) AS first_expiry, MAX(expiry_date) AS last_expiry
       FROM instruments
       GROUP BY underlying, option_type
       ORDER BY underlying, option_type`,
  );
  return rows.map(r => ({
    ...r,
    first_expiry: r.first_expiry instanceof Date ? r.first_expiry.toISOString().slice(0, 10) : r.first_expiry,
    last_expiry:  r.last_expiry  instanceof Date ? r.last_expiry.toISOString().slice(0, 10)  : r.last_expiry,
  }));
}

module.exports = {
  MAJOR_UNDERLYINGS,
  sync,
  listIndices,
  listExpiries,
  getChain,
  diag,
};
