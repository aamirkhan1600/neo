// Best-effort historical backfill for premium-trigger backtests.
//
// Tries Kotak Neo's undocumented charts/historical endpoint
// (brokerClient.getHistoricalBars). If it works on the user's account,
// we receive 1-minute OHLC bars per token and synthesise a four-tick
// walk per bar — open at 0s, low at 15s, high at 45s, close at 60s
// (or open/high/low/close ordering when close >= open) — and write
// them into tick_history.
//
// IMPORTANT: synthetic ticks are an APPROXIMATION. The real tick path
// inside a 1-minute bar is unobservable from OHLC alone. Backtests
// driven by this data will be approximately right but won't match a
// live recording. The UI surfaces this caveat clearly.

const { query } = require('../db/pool');
const brokerService = require('./brokerService');
const tickRecorder = require('./tickRecorder');
const instrumentService = require('./instrumentService');
const logger = require('../utils/logger');

// Mirror of premiumTrigger.SPOT_REF — duplicated here so we don't pull
// in the full strategy module just to look up the index quote symbol.
const SPOT_REF = {
  NIFTY:      { segment: 'nse_cm', symbol: 'Nifty 50' },
  BANKNIFTY:  { segment: 'nse_cm', symbol: 'Nifty Bank' },
  FINNIFTY:   { segment: 'nse_cm', symbol: 'Nifty Fin Service' },
  MIDCPNIFTY: { segment: 'nse_cm', symbol: 'Nifty Midcap Select' },
  SENSEX:     { segment: 'bse_cm', symbol: 'SENSEX' },
  BANKEX:     { segment: 'bse_cm', symbol: 'BANKEX' },
};

const IST_OFFSET_MIN = 330;

function istMidnight(dateStr) {
  // 2026-05-05 → epoch ms at 00:00:00 IST
  const [y, m, d] = String(dateStr).split('-').map(Number);
  // Build a UTC date for that calendar day, subtract the IST offset to
  // land on midnight IST.
  return Date.UTC(y, m - 1, d) - IST_OFFSET_MIN * 60_000;
}

function rangeForDate(dateStr) {
  const startIST = istMidnight(dateStr) + (9 * 60 + 15) * 60_000;   // 09:15
  const endIST   = istMidnight(dateStr) + (15 * 60 + 30) * 60_000;  // 15:30
  return { fromMs: startIST, toMs: endIST };
}

// Normalise the broker's response into [{ts, o, h, l, c}, ...].
// Kotak's charts payload shape varies across SDK forks; we accept a few.
function parseBars(raw) {
  if (!raw) return [];
  // Shape A: { data: [[ts, o, h, l, c, v], ...] }
  if (Array.isArray(raw?.data) && Array.isArray(raw.data[0])) {
    return raw.data.map(r => ({
      ts: Number(r[0]) * (r[0] < 1e12 ? 1000 : 1),
      o: Number(r[1]), h: Number(r[2]), l: Number(r[3]), c: Number(r[4]),
    })).filter(b => Number.isFinite(b.ts) && Number.isFinite(b.c));
  }
  // Shape B: { candles: [[ts, o, h, l, c, v], ...] }
  if (Array.isArray(raw?.candles) && Array.isArray(raw.candles[0])) {
    return raw.candles.map(r => ({
      ts: Number(r[0]) * (r[0] < 1e12 ? 1000 : 1),
      o: Number(r[1]), h: Number(r[2]), l: Number(r[3]), c: Number(r[4]),
    })).filter(b => Number.isFinite(b.ts) && Number.isFinite(b.c));
  }
  // Shape C: { data: [{time, open, high, low, close}, ...] }
  if (Array.isArray(raw?.data) && raw.data[0] && raw.data[0].open != null) {
    return raw.data.map(r => ({
      ts: Number(r.time || r.ts) * ((r.time || r.ts) < 1e12 ? 1000 : 1),
      o: Number(r.open), h: Number(r.high), l: Number(r.low), c: Number(r.close),
    })).filter(b => Number.isFinite(b.ts) && Number.isFinite(b.c));
  }
  return [];
}

// Walk one bar into 4 synthetic ticks at fixed offsets within the
// bar's minute. If close >= open we walk O→L→H→C, otherwise O→H→L→C —
// a crude but defensible reconstruction.
function barToTicks(bar) {
  const downBar = bar.c < bar.o;
  const path = downBar ? [bar.o, bar.h, bar.l, bar.c] : [bar.o, bar.l, bar.h, bar.c];
  return [
    { ts: bar.ts,                ltp: path[0] },
    { ts: bar.ts + 15_000,       ltp: path[1] },
    { ts: bar.ts + 45_000,       ltp: path[2] },
    { ts: bar.ts + 59_500,       ltp: path[3] },
  ];
}

async function findSession(userId, dateStr) {
  const rows = await query(
    `SELECT cfg_snapshot, legs FROM premium_trigger_session
       WHERE user_id = ? AND trade_date = ? LIMIT 1`,
    [userId, dateStr],
  );
  if (!rows.length) return null;
  return {
    cfg: typeof rows[0].cfg_snapshot === 'string' ? JSON.parse(rows[0].cfg_snapshot) : rows[0].cfg_snapshot,
    legs: typeof rows[0].legs === 'string' ? JSON.parse(rows[0].legs) : rows[0].legs,
  };
}

// Best-effort spot estimate for the underlying. Tries the live REST
// quotes API; today's spot is a reasonable proxy when picking ATM for
// a date within the same week. Returns null on failure (caller falls
// back to median strike).
async function fetchSpotEstimate(userId, underlyingName) {
  const ref = SPOT_REF[String(underlyingName || '').toUpperCase()];
  if (!ref) return null;
  try {
    const data = await brokerService.fetchQuotes(
      userId,
      [{ exchangeSegment: ref.segment, symbol: ref.symbol }],
      'ltp',
    );
    const list = Array.isArray(data) ? data : (data?.data || []);
    const ltp = parseFloat(list[0]?.ltp ?? list[0]?.LTP);
    return Number.isFinite(ltp) && ltp > 0 ? ltp : null;
  } catch (_) {
    return null;
  }
}

// Build a session snapshot from the user's current premium-trigger cfg
// when no row exists for the requested date. Reuses instrumentService
// to find a CE/PE pair for an appropriate strike + expiry. Persists
// the synthesised snapshot so subsequent operations (replay + future
// imports) see it. Returns the same shape as findSession().
async function autoResolveSession(userId, dateStr) {
  const cfgRows = await query(
    'SELECT config FROM premium_trigger_config WHERE user_id = ? LIMIT 1',
    [userId],
  );
  if (!cfgRows.length) {
    throw new Error(
      'no premium-trigger settings yet — open /premium-trigger/settings, '
      + 'pick an underlying / expiry / strike mode, and Save first.',
    );
  }
  const cfg = typeof cfgRows[0].config === 'string'
    ? JSON.parse(cfgRows[0].config) : cfgRows[0].config;

  const underlying = String(cfg.underlyingName || 'NIFTY').toUpperCase();

  // 1. Pick the expiry. Operator's saved cfg.expiry wins; otherwise
  // pick the first expiry on or after the requested date.
  let expiry = cfg.expiry || null;
  if (!expiry || !/^\d{4}-\d{2}-\d{2}$/.test(expiry)) {
    const expiries = await instrumentService.listExpiries(underlying);
    expiry = expiries.find(e => e >= dateStr) || expiries[0] || null;
    if (!expiry) {
      throw new Error(
        `no expiries loaded for ${underlying}. Open /option-chain and click `
        + `Sync Master to populate the instruments table first.`,
      );
    }
  }

  // 2. Pull the chain.
  const chainData = await instrumentService.getChain(underlying, expiry);
  const chain = chainData?.chain || [];
  if (!chain.length) {
    throw new Error(
      `no strikes for ${underlying} ${expiry} in the instruments table. `
      + `Run Sync Master on /option-chain.`,
    );
  }
  const strikes = chain.map(r => r.strike).sort((a, b) => a - b);

  // 3. Spot estimate for ATM resolution.
  const spot = (cfg.strikeMode === 'fixed' && cfg.fixedStrike)
    ? null
    : await fetchSpotEstimate(userId, underlying);

  // 4. Pick a strike per the user's configured mode.
  let strike;
  if (cfg.strikeMode === 'fixed' && cfg.fixedStrike) {
    strike = strikes.reduce((b, s) => Math.abs(s - cfg.fixedStrike) < Math.abs(b - cfg.fixedStrike) ? s : b, strikes[0]);
  } else if (spot != null) {
    strike = strikes.reduce((b, s) => Math.abs(s - spot) < Math.abs(b - spot) ? s : b, strikes[0]);
  } else {
    strike = strikes[Math.floor(strikes.length / 2)];
  }
  const row = chain.find(r => r.strike === strike);
  if (!row || !row.ce || !row.pe) {
    throw new Error(`no CE+PE pair found at strike ${strike} for ${underlying} ${expiry}`);
  }

  const legs = {
    ce: {
      token: String(row.ce.token),
      symbol: row.ce.tradingSymbol,
      exchangeSegment: row.ce.exchangeSegment,
      lotSize: row.ce.lotSize || 1,
      strike, expiry,
    },
    pe: {
      token: String(row.pe.token),
      symbol: row.pe.tradingSymbol,
      exchangeSegment: row.pe.exchangeSegment,
      lotSize: row.pe.lotSize || 1,
      strike, expiry,
    },
  };

  await query(
    `INSERT IGNORE INTO premium_trigger_session
       (user_id, trade_date, cfg_snapshot, legs)
     VALUES (?, ?, ?, ?)`,
    [userId, dateStr, JSON.stringify(cfg), JSON.stringify(legs)],
  );

  return {
    cfg, legs,
    autoResolved: true,
    resolvedFromSpot: spot,
    resolvedStrike: strike,
    expiry,
  };
}

async function tryImport({ userId, date, interval = 'I1' }) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) throw new Error('date YYYY-MM-DD required');

  // We need leg metadata to know which tokens to fetch. Prefer the
  // recorded session snapshot (if the user ran the strategy on that
  // date). Otherwise auto-build one from their current cfg + the
  // persisted instruments table — same code-path the live recorder
  // would have produced, just synthesised on-demand for backfill.
  let session = await findSession(userId, date);
  if (!session) {
    session = await autoResolveSession(userId, date);
  }

  const tokens = [];
  for (const side of ['ce', 'pe']) {
    const leg = session.legs?.[side];
    if (leg && leg.token && leg.exchangeSegment) {
      tokens.push({ token: String(leg.token), segment: leg.exchangeSegment });
    }
  }
  if (!tokens.length) throw new Error('no CE/PE tokens in the session snapshot');

  const { fromMs, toMs } = rangeForDate(date);
  let inserted = 0;
  let perToken = {};
  let firstError = null;

  for (const { token, segment } of tokens) {
    let raw;
    try {
      raw = await brokerService.fetchHistoricalBars(userId, {
        token, segment, from: fromMs, to: toMs, interval,
      });
    } catch (err) {
      perToken[token] = { error: err.message };
      if (!firstError) firstError = err;
      continue;
    }
    const bars = parseBars(raw);
    if (!bars.length) {
      perToken[token] = { bars: 0, ticks: 0 };
      continue;
    }
    const ticks = bars.flatMap(barToTicks).map(t => ({
      token, segment, ltp: t.ltp, ts_ms: t.ts,
    }));
    tickRecorder.recordTicks(userId, ticks);
    perToken[token] = { bars: bars.length, ticks: ticks.length };
    inserted += ticks.length;
  }

  if (!inserted && firstError) {
    throw new Error(
      `Kotak charts endpoint not available on this account: ${firstError.message}. `
      + `Use the live recorder (open /option-chain or run premium-trigger paper) instead.`,
    );
  }
  return {
    date,
    tokens: tokens.length,
    ticksWritten: inserted,
    perToken,
    interval,
    autoResolved: !!session.autoResolved,
    resolvedFromSpot: session.resolvedFromSpot ?? null,
    resolvedStrike: session.resolvedStrike ?? null,
    expiry: session.expiry ?? null,
  };
}

module.exports = { tryImport, parseBars, barToTicks, rangeForDate };
