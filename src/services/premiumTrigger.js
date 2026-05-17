// Premium-trigger short-options strategy for Kotak Neo.
//
// Spec: kotak-neo-saas/docs/premium-trigger-strategy.md
//
// One instance per user. Listens to that user's marketData WebSocket
// 'tick' events for the resolved CE / PE / underlying tokens, drives a
// per-leg state machine (NONE -> SHORT -> NONE), and routes orders
// through brokerService.placeOrder. Persists config + counters in
// `premium_trigger_config`. Signal log goes into `premium_trigger_signals`
// for the dashboard.

const { query } = require('../db/pool');
const brokerService = require('./brokerService');
const orderService = require('./orderService');
const marketData = require('./marketData');
const accounts = require('./brokerAccount');
const instrumentService = require('./instrumentService');
const eventLog = require('./eventLog');
const bracketManager = require('./bracketManager');
const logger = require('../utils/logger');

// ---------------------------------------------------------------------------
// Defaults + helpers
// ---------------------------------------------------------------------------

const DEFAULTS = {
  enabled: false,
  // 'paper' = simulate fills locally, no broker call. 'live' = real
  // orders. We default to PAPER so a freshly-saved config can never
  // accidentally fire a real trade — the operator must explicitly
  // flip to live.
  mode: 'paper',
  underlyingName: 'NIFTY',
  expiry: null,                  // YYYY-MM-DD or null/'auto'
  strikeMode: 'atm',             // atm | fixed | otm
  fixedStrike: null,
  qty: 1,                        // lot multiplier
  maxDailyLoss: null,            // ₹ realised-loss circuit breaker
  // Default ON: in OTM mode the whole point is to follow premium across
  // strikes. Without auto-restrike a drifted leg sits idle forever; the
  // operator almost always wants the rotation. Ignored in atm/fixed modes.
  autoRestrikeOnDrift: true,
  restrikeDwellSec: 60,
  restrikeCooldownSec: 300,

  // Trading window guards (Indian market hours by default).
  // When tradingHoursEnabled=true:
  //   * No entries before tradingHoursStart or after eodCutoff
  //   * exitAll('eod') fires at eodAutoExit, then stop()
  //   * Set tradingHoursEnabled=false to disable for backtests / paper
  tradingHoursEnabled: true,
  tradingHoursStart: '09:15',
  eodCutoff: '15:15',            // refuse new entries after this
  eodAutoExit: '15:20',          // square off all + stop strategy
  tradingHoursEnd: '15:30',
  // Broker-side bracket exits (SL-M + LIMIT with OCO). When false (the
  // default) target/SL is monitored on every tick and exits fire as
  // MIS MKT BUY. When true, openBracket places resting child orders at
  // entry fill and the status poll loop handles fills + cancels.
  // The tick-based "safety hammer" is always armed when brackets are
  // open: if LTP overshoots SL by `bracketSafetyOvershoot` and the
  // broker hasn't confirmed a fill within `bracketSafetyTimeoutMs`,
  // the strategy cancels both children and flattens with MKT.
  useBrokerBracketExits: false,
  bracketSafetyOvershoot: 2,
  bracketSafetyTimeoutMs: 5000,
  bracketStatusPollMs: 2000,

  // Indian options NFO defaults — Zerodha/Kotak retail.
  brokeragePerOrder: 20,
  sttPctSell: 0.0625,
  exchTxnPct: 0.03503,
  sebiPct: 0.0001,
  stampPctBuy: 0.003,
  gstPct: 18,
  legs: {
    ce: { enabled: false, entryTrigger: null, target: 1, stoploss: 5, reentryOffset: 1, reentryTarget: 1, maxEntryDeviation: 10, rearmRange: 5, rearmLower: null, rearmUpper: null, captureFadeThreshold: 2, trailingStopLoss: false, trailingStopLossValue: 0 },
    pe: { enabled: false, entryTrigger: null, target: 1, stoploss: 5, reentryOffset: 1, reentryTarget: 1, maxEntryDeviation: 10, rearmRange: 5, rearmLower: null, rearmUpper: null, captureFadeThreshold: 2, trailingStopLoss: false, trailingStopLossValue: 0 },
  },
};

// Underlying name -> Kotak Neo spot quote tuple (segment, symbol).
// Indices use case-sensitive name per the official Quotes API doc.
const SPOT_REF = {
  NIFTY:      { segment: 'nse_cm', symbol: 'Nifty 50' },
  BANKNIFTY:  { segment: 'nse_cm', symbol: 'Nifty Bank' },
  FINNIFTY:   { segment: 'nse_cm', symbol: 'Nifty Fin Service' },
  MIDCPNIFTY: { segment: 'nse_cm', symbol: 'Nifty Midcap Select' },
  SENSEX:     { segment: 'bse_cm', symbol: 'SENSEX' },
  BANKEX:     { segment: 'bse_cm', symbol: 'BANKEX' },
};

function emptyLeg() {
  return {
    token: null, symbol: null, exchangeSegment: null, lotSize: 1,
    strike: null, expiry: null,
    ltp: null, position: 'NONE', entryPrice: null,
    pending: false, exitInFlight: false, armed: false, slLocked: false,
    entryCount: 0, triggerPrice: null, currentTradeTarget: null,
    realizedPnl: 0, tradesPlaced: 0, tradesCompleted: 0,
    tradesWon: 0, tradesLost: 0,
    grossProfit: 0, grossLoss: 0, maxTradeProfit: 0, maxTradeLoss: 0,
    realizedPoints: 0, grossProfitPoints: 0, grossLossPoints: 0,
    maxTradeProfitPoints: 0, maxTradeLossPoints: 0,
    outsideWindowSince: null, lastRestrikeAt: null, restrikeInProgress: false,
    restrikeCount: 0,
    // Counts auto-restrike cycles where the picker returned the same token
    // (chain has no in-window candidate). Once it crosses the threshold,
    // evaluate() drops the in-window gate so trades fire on the available
    // premium. Reset on a real rotation, on a successful entry, or on a
    // structural save.
    stuckRestrikeCount: 0,
    totalCharges: 0, lastTradeCharges: null,
    // Diagnostic: human-readable reason the most recent tick did NOT
    // produce an entry. Surfaced in /api/premium-trigger/status so the
    // dashboard can explain "why isn't it firing?".
    lastDecision: null,
    // Per-leg broker-rejection cooldown (commit 965f3ac). evaluate()
    // refuses to (re-)issue orders while rejectCooldownUntil is in the
    // future; consecutiveRejects drives the exponential backoff.
    consecutiveRejects: 0,
    rejectCooldownUntil: null,
    lastRejectReason: null,
    // Set when a BO entry is filled. Used to route force-exits through
    // /quick/order/bo/exit and to suppress duplicate exit orders when the
    // broker's auto target/SL leg fires.
    entryBrokerOrderId: null,
  };
}

function numOrNull(v) {
  if (v === null || v === undefined || v === '') return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function deepClone(o) { return JSON.parse(JSON.stringify(o)); }

function merge(base, patch) {
  const out = { ...base, ...patch };
  if (patch && patch.legs) {
    out.legs = {
      ce: { ...base.legs.ce, ...(patch.legs.ce || {}) },
      pe: { ...base.legs.pe, ...(patch.legs.pe || {}) },
    };
  }
  return out;
}

function rearmBounds(leg) {
  const base = leg.rearmRange != null ? leg.rearmRange : 5;
  const lower = leg.rearmLower != null ? leg.rearmLower : base;
  const upper = leg.rearmUpper != null ? leg.rearmUpper : base;
  return { lower, upper };
}

// ---------------------------------------------------------------------------
// Trading-window helpers (IST). `Date.prototype.getTime()` is always UTC
// milliseconds, so shifting forward by the fixed IST offset and then reading
// UTC fields yields the IST wall-clock values regardless of the host's
// timezone. (The earlier attempt subtracted getTimezoneOffset() too, which
// double-counted on IST hosts and pushed every check 5h30m into the future.)
// ---------------------------------------------------------------------------
const IST_OFFSET_MIN = 330;

function nowIST() {
  return new Date(Date.now() + IST_OFFSET_MIN * 60_000);
}
function hhmmToMin(s) {
  if (!s || !/^\d{1,2}:\d{2}$/.test(String(s))) return null;
  const [h, m] = String(s).split(':').map(Number);
  return h * 60 + m;
}
function nowMinIST() {
  const d = nowIST();
  return d.getUTCHours() * 60 + d.getUTCMinutes();
}
// Mon–Fri in IST. We don't maintain a holiday calendar — operator
// turns the strategy off on holidays via the Enabled checkbox.
function isWeekday() {
  const d = nowIST();
  const dow = d.getUTCDay();
  return dow >= 1 && dow <= 5;
}
function inMarketHours(cfg) {
  if (!cfg.tradingHoursEnabled) return true;
  if (!isWeekday()) return false;
  const now = nowMinIST();
  const start = hhmmToMin(cfg.tradingHoursStart);
  const end = hhmmToMin(cfg.tradingHoursEnd);
  if (start == null || end == null) return true;
  return now >= start && now <= end;
}
function entriesAllowed(cfg) {
  if (!cfg.tradingHoursEnabled) return true;
  if (!isWeekday()) return false;
  const now = nowMinIST();
  const start = hhmmToMin(cfg.tradingHoursStart);
  const cutoff = hhmmToMin(cfg.eodCutoff);
  if (start == null || cutoff == null) return true;
  return now >= start && now < cutoff;
}

function validateLeg(side, leg) {
  if (!leg.enabled) return;
  const L = side.toUpperCase();
  if (leg.entryTrigger == null || leg.entryTrigger <= 0) {
    throw new Error(`${L} leg: entry trigger must be > 0`);
  }
  for (const [k, label] of [
    ['target', 'target (profit points)'],
    ['stoploss', 'stoploss (loss points)'],
    ['reentryOffset', 're-entry offset (points)'],
    ['reentryTarget', 're-entry target (points)'],
    ['maxEntryDeviation', 'max entry deviation (points)'],
    ['rearmRange', 're-arm range (points)'],
    ['rearmLower', 're-arm lower (points)'],
    ['rearmUpper', 're-arm upper (points)'],
    ['captureFadeThreshold', 'capture fade threshold (points)'],
  ]) {
    if (leg[k] != null && leg[k] <= 0) throw new Error(`${L} leg: ${label} must be > 0`);
  }
}

// ---------------------------------------------------------------------------
// Per-user instance factory
// ---------------------------------------------------------------------------

function createPremiumTrigger(userId) {
  const log = logger.child ? logger.child({ component: `pt:${userId}` }) : logger;
  let cfg = deepClone(DEFAULTS);
  let running = false;
  let lastTickAt = null;
  let lastError = null;
  let spot = null;
  const signalHistory = []; // last 50 signals (most recent first not enforced)
  const SIGNAL_HISTORY_MAX = 50;
  const legState = { ce: emptyLeg(), pe: emptyLeg() };
  let peakRealized = 0, maxDrawdown = 0, dayLossLocked = false;
  let tickHandler = null;
  let mdc = null; // marketDataClient
  let paperOrderSeq = 0; // increments per simulated fill in paper mode
  let eodTimer = null;   // scheduled EOD auto-exit

  // -------------------------------------------------------------------------
  // Load / save
  // -------------------------------------------------------------------------

  async function load() {
    try {
      const rows = await query(
        'SELECT config, state, enabled, running FROM premium_trigger_config WHERE user_id = ?',
        [userId],
      );
      if (rows.length) {
        const stored = typeof rows[0].config === 'string'
          ? JSON.parse(rows[0].config)
          : rows[0].config;
        cfg = merge(cfg, stored || {});
        cfg.enabled = !!rows[0].enabled;

        // Restore the persisted runtime snapshot so an app restart
        // doesn't lose track of an open SHORT position, accumulated
        // P&L, or session-level drawdown / day-loss state.
        const rawState = rows[0].state;
        if (rawState) {
          let snap;
          try {
            snap = typeof rawState === 'string' ? JSON.parse(rawState) : rawState;
          } catch (_) { snap = null; }
          if (snap && snap.legState) {
            for (const side of ['ce', 'pe']) {
              const persisted = snap.legState[side];
              if (!persisted) continue;
              Object.assign(legState[side], persisted);
              // Live flags are never trusted from disk — a crash
              // mid-order or mid-restrike must not leave them stuck.
              legState[side].pending = false;
              legState[side].exitInFlight = false;
              legState[side].restrikeInProgress = false;
            }
            peakRealized = Number(snap.peakRealized) || 0;
            maxDrawdown = Number(snap.maxDrawdown) || 0;
            dayLossLocked = !!snap.dayLossLocked;
          }
        }
      }
    } catch (err) {
      log.warn('premiumTrigger: load failed', { userId, err: err.message });
    }
    return cfg;
  }

  // Snapshot the runtime state to DB. Best-effort — the strategy must
  // never block on a write, and a stale snapshot is preferable to a
  // crashed strategy. Skipped entirely during backtest replay.
  async function persistState() {
    if (cfg._backtest) return;
    try {
      const snap = {
        legState: deepClone(legState),
        peakRealized,
        maxDrawdown,
        dayLossLocked,
        ts: Date.now(),
      };
      // Drop transient fields from the snapshot — we always reload them
      // from a live tick or by detecting them at evaluate() time.
      for (const side of ['ce', 'pe']) {
        const s = snap.legState[side];
        s.pending = false;
        s.restrikeInProgress = false;
      }
      await query(
        `UPDATE premium_trigger_config SET state = ? WHERE user_id = ?`,
        [JSON.stringify(snap), userId],
      );
    } catch (err) {
      log.warn('premiumTrigger: persistState failed', { userId, err: err.message });
    }
  }

  function normalisedConfig(patch) {
    const next = merge(cfg, patch || {});
    for (const side of ['ce', 'pe']) {
      const leg = next.legs[side];
      leg.entryTrigger = numOrNull(leg.entryTrigger);
      leg.target = numOrNull(leg.target);
      leg.stoploss = numOrNull(leg.stoploss);
      leg.reentryOffset = numOrNull(leg.reentryOffset);
      leg.reentryTarget = numOrNull(leg.reentryTarget);
      leg.maxEntryDeviation = numOrNull(leg.maxEntryDeviation);
      leg.rearmRange = numOrNull(leg.rearmRange);
      leg.rearmLower = numOrNull(leg.rearmLower);
      leg.rearmUpper = numOrNull(leg.rearmUpper);
      leg.captureFadeThreshold = numOrNull(leg.captureFadeThreshold);
      leg.trailingStopLossValue = numOrNull(leg.trailingStopLossValue);
      if (leg.trailingStopLossValue == null) leg.trailingStopLossValue = 0;
      leg.trailingStopLoss = !!leg.trailingStopLoss;
      leg.enabled = !!leg.enabled;
      validateLeg(side, leg);
    }
    const modeStr = String(next.mode || 'paper').toLowerCase();
    next.mode = modeStr === 'live' ? 'live' : 'paper';
    next.qty = Math.max(1, Math.floor(Number(next.qty) || 1));
    next.underlyingName = String(next.underlyingName || 'NIFTY').toUpperCase();
    if (next.expiry === '' || next.expiry === 'auto' || next.expiry == null) {
      next.expiry = null;
    } else {
      const s = String(next.expiry).trim();
      next.expiry = /^\d{4}-\d{2}-\d{2}$/.test(s) ? s : null;
    }
    const mode = String(next.strikeMode || 'atm').toLowerCase();
    next.strikeMode = ['atm', 'fixed', 'otm'].includes(mode) ? mode : 'atm';
    next.fixedStrike = numOrNull(next.fixedStrike);
    next.maxDailyLoss = numOrNull(next.maxDailyLoss);
    if (next.maxDailyLoss != null && next.maxDailyLoss <= 0) {
      throw new Error('max daily loss (rupees) must be > 0');
    }
    next.autoRestrikeOnDrift = !!next.autoRestrikeOnDrift;
    next.restrikeDwellSec = numOrNull(next.restrikeDwellSec) || 60;
    next.restrikeCooldownSec = numOrNull(next.restrikeCooldownSec) || 300;

    next.useBrokerBracketExits = !!next.useBrokerBracketExits;
    next.bracketSafetyOvershoot = numOrNull(next.bracketSafetyOvershoot);
    if (next.bracketSafetyOvershoot == null || next.bracketSafetyOvershoot < 0) {
      next.bracketSafetyOvershoot = DEFAULTS.bracketSafetyOvershoot;
    }
    next.bracketSafetyTimeoutMs = Math.max(500,
      numOrNull(next.bracketSafetyTimeoutMs) || DEFAULTS.bracketSafetyTimeoutMs);
    next.bracketStatusPollMs = Math.max(500,
      numOrNull(next.bracketStatusPollMs) || DEFAULTS.bracketStatusPollMs);

    next.tradingHoursEnabled = next.tradingHoursEnabled !== false && next.tradingHoursEnabled !== 'false' && next.tradingHoursEnabled !== '0';
    for (const k of ['tradingHoursStart', 'eodCutoff', 'eodAutoExit', 'tradingHoursEnd']) {
      const v = String(next[k] || '').trim();
      next[k] = /^\d{1,2}:\d{2}$/.test(v) ? v : DEFAULTS[k];
    }
    for (const k of ['brokeragePerOrder', 'sttPctSell', 'exchTxnPct', 'sebiPct', 'stampPctBuy', 'gstPct']) {
      const v = numOrNull(next[k]);
      if (v != null && v < 0) throw new Error(`${k} must be >= 0`);
      next[k] = v != null ? v : 0;
    }
    next.enabled = !!next.enabled;
    return next;
  }

  // A "structural" change is one that alters the contract the running
  // session is based on: which instrument we're trading, the size, or
  // the mode. Tweaks to dwell/cooldown, auto-restrike, charges, or
  // trading-window times are tunings — they should NOT wipe accumulated
  // P&L, armed state, win/loss counters, or trip a stop/restart.
  function isStructuralChange(prev, next) {
    if (!prev) return true;
    const keys = ['mode', 'underlyingName', 'expiry', 'strikeMode', 'fixedStrike', 'qty'];
    for (const k of keys) {
      if ((prev[k] ?? null) !== (next[k] ?? null)) return true;
    }
    // entryTrigger / target / stoploss / rearm bounds also change the
    // strategy contract — re-arm so the new bounds take effect cleanly,
    // but don't wipe realised P&L from earlier in the session.
    for (const side of ['ce', 'pe']) {
      const a = (prev.legs && prev.legs[side]) || {};
      const b = (next.legs && next.legs[side]) || {};
      const legKeys = ['enabled', 'entryTrigger', 'target', 'stoploss',
        'reentryOffset', 'reentryTarget', 'maxEntryDeviation',
        'rearmRange', 'rearmLower', 'rearmUpper', 'captureFadeThreshold'];
      for (const k of legKeys) {
        if ((a[k] ?? null) !== (b[k] ?? null)) return true;
      }
    }
    return false;
  }

  async function save(patch) {
    const prev = cfg;
    const next = normalisedConfig(patch);
    const structural = isStructuralChange(prev, next);
    await query(
      `INSERT INTO premium_trigger_config (user_id, config, enabled)
       VALUES (?, ?, ?)
       ON DUPLICATE KEY UPDATE config = VALUES(config), enabled = VALUES(enabled)`,
      [userId, JSON.stringify(next), next.enabled ? 1 : 0],
    );
    const eodChanged = (prev && prev.eodAutoExit !== next.eodAutoExit);
    cfg = next;
    if (!structural) {
      // Tuning-only save: keep all session state intact. Persist the
      // updated config so a restart picks up the new dwell/cooldown/etc.
      // Special-case the EOD auto-exit time: scheduleEod() only runs at
      // start(), so if the operator just changed eodAutoExit while the
      // strategy is running, rebind the timer to the new wall-clock.
      if (eodChanged && running) { cancelEod(); scheduleEod(); }
      persistState().catch(() => {});
      return cfg;
    }
    // Structural save = new session: clear counters, drop slLocked, etc.
    for (const side of ['ce', 'pe']) {
      const st = legState[side];
      st.slLocked = false; st.armed = false; st.triggerPrice = null;
      st.entryCount = 0;
      st.realizedPnl = 0; st.tradesPlaced = 0; st.tradesCompleted = 0;
      st.tradesWon = 0; st.tradesLost = 0;
      st.grossProfit = 0; st.grossLoss = 0;
      st.maxTradeProfit = 0; st.maxTradeLoss = 0;
      st.realizedPoints = 0; st.grossProfitPoints = 0; st.grossLossPoints = 0;
      st.maxTradeProfitPoints = 0; st.maxTradeLossPoints = 0;
      st.totalCharges = 0; st.lastTradeCharges = null;
    }
    peakRealized = 0; maxDrawdown = 0; dayLossLocked = false;
    // Wipe the persisted runtime snapshot — structural save = "new session".
    try {
      await query(
        `UPDATE premium_trigger_config SET state = NULL WHERE user_id = ?`,
        [userId],
      );
    } catch (_) {}
    if (running) { await stop(); await start(); }
    return cfg;
  }

  function get() { return cfg; }

  function status() {
    function unrealized(side) {
      const st = legState[side];
      if (st.position !== 'SHORT' || !st.entryPrice || !st.ltp) return null;
      const qty = cfg.qty * (st.lotSize || 1);
      return (st.entryPrice - st.ltp) * qty;
    }
    function unrealizedPoints(side) {
      const st = legState[side];
      if (st.position !== 'SHORT' || st.entryPrice == null || st.ltp == null) return null;
      return st.entryPrice - st.ltp;
    }
    function targetPrice(side) {
      const st = legState[side];
      const t = st.currentTradeTarget != null ? st.currentTradeTarget : (cfg.legs[side] && cfg.legs[side].target);
      if (st.position !== 'SHORT' || st.entryPrice == null || t == null) return null;
      return st.entryPrice - t;
    }
    function slPrice(side) {
      const st = legState[side];
      const s = cfg.legs[side] && cfg.legs[side].stoploss;
      if (st.position !== 'SHORT' || st.entryPrice == null || s == null) return null;
      return st.entryPrice + s;
    }
    const stClone = deepClone(legState);
    for (const side of ['ce', 'pe']) {
      stClone[side].qty = cfg.qty * (legState[side].lotSize || 1);
      stClone[side].unrealized = unrealized(side);
      stClone[side].unrealizedPoints = unrealizedPoints(side);
      stClone[side].targetPrice = targetPrice(side);
      stClone[side].slPrice = slPrice(side);
      stClone[side].bracket = bracketManager.snapshot(userId, side);
    }
    const totalUnreal = (stClone.ce.unrealized || 0) + (stClone.pe.unrealized || 0);
    const totalRealized = legState.ce.realizedPnl + legState.pe.realizedPnl;
    const totalCharges = legState.ce.totalCharges + legState.pe.totalCharges;
    const totalTrades = legState.ce.tradesPlaced + legState.pe.tradesPlaced;
    const totalCompleted = legState.ce.tradesCompleted + legState.pe.tradesCompleted;
    const totalWon = legState.ce.tradesWon + legState.pe.tradesWon;
    const totalLost = legState.ce.tradesLost + legState.pe.tradesLost;
    const decided = totalWon + totalLost;
    const accuracy = decided > 0 ? (totalWon / decided) * 100 : null;
    const grossProfit = legState.ce.grossProfit + legState.pe.grossProfit;
    const grossLoss = legState.ce.grossLoss + legState.pe.grossLoss;
    const profitFactor = grossLoss > 0 ? grossProfit / grossLoss : (grossProfit > 0 ? Infinity : null);
    const currentDrawdown = Math.max(0, peakRealized - totalRealized);
    return {
      enabled: cfg.enabled,
      running, lastTickAt, lastError, spot, cfg,
      ws: mdc && typeof mdc.diag === 'function' ? mdc.diag() : null,
      state: stClone,
      totalUnrealized: totalUnreal,
      totalRealized,
      totalCharges,
      totalPnl: totalRealized + totalUnreal,
      totalTrades, totalCompleted, totalWon, totalLost,
      accuracy, profitFactor,
      grossProfit, grossLoss,
      peakRealized, currentDrawdown, maxDrawdown,
      dayLossLocked, maxDailyLoss: cfg.maxDailyLoss,
      restrikeCount: legState.ce.restrikeCount + legState.pe.restrikeCount,
      signals: signalHistory.slice(-20).reverse(),
    };
  }

  // -------------------------------------------------------------------------
  // Spot LTP fetch (REST quotes — no WS dependency)
  // -------------------------------------------------------------------------

  // Per-instance cache. fetchSpot is called from start(), drift restrike,
  // and the manual restrike route — all of which can fire within seconds
  // of each other. Without a cache they each fire a separate Kotak quote
  // request and stampede the rate limiter, especially while marketData's
  // REST poller is also active. 2s TTL is fresh enough for OTM scanning.
  const SPOT_FRESH_MS = 2000;
  const SPOT_STALE_OK_MS = 30000;
  let _spotCache = { ltp: null, ts: 0 };

  async function fetchSpot() {
    const ref = SPOT_REF[cfg.underlyingName];
    if (!ref) return null;
    const now = Date.now();
    if (_spotCache.ltp != null && now - _spotCache.ts < SPOT_FRESH_MS) {
      return _spotCache.ltp;
    }
    try {
      const data = await brokerService.fetchQuotes(
        userId,
        [{ exchangeSegment: ref.segment, symbol: ref.symbol }],
        'ltp',
      );
      const list = Array.isArray(data) ? data : (data?.data || []);
      const ltp = parseFloat(list[0]?.ltp ?? list[0]?.LTP);
      if (Number.isFinite(ltp) && ltp > 0) {
        _spotCache = { ltp, ts: now };
        return ltp;
      }
      return null;
    } catch (err) {
      log.warn('premiumTrigger: spot fetch failed', { err: err.message });
      // Rate-limit or transient broker error — fall back to a recent
      // cached value rather than failing resolveLegs/restrike outright.
      // Otherwise a single rate-limit response cascades into a tight
      // retry loop that itself prolongs the rate limit.
      const isRateLimited = /rate.?limit|too many/i.test(err.message || '');
      if (isRateLimited && _spotCache.ltp != null && now - _spotCache.ts < SPOT_STALE_OK_MS) {
        return _spotCache.ltp;
      }
      return null;
    }
  }

  // -------------------------------------------------------------------------
  // Strike resolution
  // -------------------------------------------------------------------------

  async function resolveLegs() {
    // Walk the chain from instrumentService — it already filters to our
    // major underlyings + persists tokens after a Sync Master run.
    let expiry = cfg.expiry;
    if (!expiry) {
      const expiries = await instrumentService.listExpiries(cfg.underlyingName);
      if (!expiries.length) {
        throw new Error(
          `no expiries loaded for ${cfg.underlyingName}. Open /option-chain and click Sync Master.`,
        );
      }
      expiry = expiries[0];
    }

    const chainData = await instrumentService.getChain(cfg.underlyingName, expiry);
    if (!chainData.chain.length) {
      throw new Error(`no strikes for ${cfg.underlyingName} ${expiry}`);
    }

    spot = await fetchSpot();
    const strikes = chainData.chain.map(r => r.strike);

    if (cfg.strikeMode === 'otm') {
      await resolveOTMLeg('ce', spot, expiry, chainData.chain);
      await resolveOTMLeg('pe', spot, expiry, chainData.chain);
      return { expiry, ceStrike: legState.ce.strike, peStrike: legState.pe.strike };
    }

    let strike;
    if (cfg.strikeMode === 'fixed' && cfg.fixedStrike) {
      strike = strikes.reduce((b, s) => Math.abs(s - cfg.fixedStrike) < Math.abs(b - cfg.fixedStrike) ? s : b, strikes[0]);
    } else if (spot) {
      strike = strikes.reduce((b, s) => Math.abs(s - spot) < Math.abs(b - spot) ? s : b, strikes[0]);
    } else {
      strike = strikes[Math.floor(strikes.length / 2)];
    }

    const row = chainData.chain.find(r => r.strike === strike);
    for (const side of ['ce', 'pe']) {
      const cell = row?.[side];
      const st = legState[side];
      st.strike = strike;
      st.expiry = expiry;
      if (cell) {
        st.token = String(cell.token);
        st.symbol = cell.tradingSymbol;
        st.exchangeSegment = cell.exchangeSegment;
        st.lotSize = cell.lotSize || 1;
      }
      if (st.position === 'NONE') st.armed = true;
    }
    return { expiry, strike };
  }

  // 20 candidates is plenty: with 50-rupee strike spacing and an entry
  // trigger anywhere reasonable, the matching strike is usually within
  // ~10 steps of ATM. Smaller list = shorter URL = no rate-limit issues.
  const OTM_CANDIDATES = 20;

  async function resolveOTMLeg(side, spotPrice, expiry, chain) {
    if (!spotPrice) throw new Error('spot LTP required for OTM mode');
    const legCfg = cfg.legs[side];
    if (!legCfg.entryTrigger) throw new Error(`${side.toUpperCase()} entry trigger required for OTM`);

    // Pull the right-side OTM strikes (CE: > spot, PE: < spot). Sort by
    // distance from spot so the nearest OTMs (highest premium) come first.
    const ordered = chain
      .filter(r => side === 'ce' ? r.strike > spotPrice : r.strike < spotPrice)
      .sort((a, b) => side === 'ce' ? a.strike - b.strike : b.strike - a.strike);
    const candidates = ordered
      .map(r => r[side])
      .filter(Boolean)
      .slice(0, OTM_CANDIDATES);
    if (!candidates.length) throw new Error(`no OTM ${side.toUpperCase()} strikes available`);

    // Batch-quote them via the auto-chunked helper (10 per HTTP call,
    // 200ms gap, retries on Kotak's 'too many requests'). Then pick the
    // strike whose LTP is closest to the leg's entryTrigger.
    const queries = candidates.map(c => ({ exchangeSegment: c.exchangeSegment, symbol: c.token }));
    let quotes;
    try {
      quotes = await brokerService.fetchQuotesAuto(userId, queries, 'ltp');
    } catch (err) {
      throw new Error(`OTM batch quote failed: ${err.message}`);
    }
    const ltpByToken = new Map();
    for (const q of quotes) {
      const t = String(q.exchange_token || q.exchangeToken || q.symbol || '');
      const ltp = parseFloat(q.ltp ?? q.LTP);
      if (t && Number.isFinite(ltp) && ltp > 0) ltpByToken.set(t, ltp);
    }
    if (!ltpByToken.size) {
      throw new Error(
        `${side.toUpperCase()} OTM quote returned no LTPs — `
        + 'market may be closed or the token list is wrong. Try Sync Master on /option-chain.'
      );
    }

    // Strike picker: a strike whose LTP falls inside this leg's rearm
    // window is genuinely tradable on the next tick. A strike with LTP
    // outside the window may be the closest match by absolute distance
    // but it leaves the leg armed-and-stuck — auto-restrike then fires
    // every dwell cycle, picks the same strike, and never progresses.
    // Prefer in-window strikes; fall back to closest-absolute only if
    // nothing fits.
    const { lower, upper } = rearmBounds(legCfg);
    const winLow = legCfg.entryTrigger - lower;
    const winHigh = legCfg.entryTrigger + upper;
    let best = null, bestDist = Infinity;
    for (const c of candidates) {
      const ltp = ltpByToken.get(String(c.token));
      if (!Number.isFinite(ltp)) continue;
      if (ltp < winLow || ltp > winHigh) continue;
      const dist = Math.abs(ltp - legCfg.entryTrigger);
      if (dist < bestDist) { bestDist = dist; best = { c, ltp }; }
    }
    if (!best) {
      bestDist = Infinity;
      for (const c of candidates) {
        const ltp = ltpByToken.get(String(c.token));
        if (!Number.isFinite(ltp)) continue;
        const dist = Math.abs(ltp - legCfg.entryTrigger);
        if (dist < bestDist) { bestDist = dist; best = { c, ltp }; }
      }
    }
    if (!best) throw new Error(`could not match any ${side.toUpperCase()} strike to entryTrigger ${legCfg.entryTrigger}`);

    // Look up the strike for the chosen token.
    const row = chain.find(r => r[side] && String(r[side].token) === String(best.c.token));
    const st = legState[side];
    st.strike = row ? row.strike : null;
    st.expiry = expiry;
    st.token = String(best.c.token);
    st.symbol = best.c.tradingSymbol;
    st.exchangeSegment = best.c.exchangeSegment;
    st.lotSize = best.c.lotSize || 1;
    st.ltp = best.ltp;
    if (st.position === 'NONE') st.armed = true;
    log.info('premiumTrigger: OTM resolved', {
      userId, side, strike: st.strike, sym: st.symbol, ltp: best.ltp, target: legCfg.entryTrigger,
    });
  }

  // -------------------------------------------------------------------------
  // Tick handling + per-leg evaluator
  // -------------------------------------------------------------------------

  // Synthetic clock: during a backtest replay, every state-time
  // comparison must use the tick's recorded timestamp, not wall-clock
  // Date.now(). Otherwise a 6-hour session that replays in <1s will
  // never accumulate enough "outside window" seconds to fire an
  // auto-restrike (or, worse, will trip cooldowns that don't apply to
  // the next live run). In live mode currentTickTs is null and every
  // helper falls back to Date.now() — identical to the previous
  // behaviour.
  let currentTickTs = null;
  function clockNow() {
    return cfg._backtest && currentTickTs != null ? currentTickTs : Date.now();
  }

  function onTick(tick) {
    if (!running || !tick) return;
    currentTickTs = (cfg._backtest && Number.isFinite(Number(tick.ts)))
      ? Number(tick.ts) : null;
    lastTickAt = currentTickTs != null ? currentTickTs : Date.now();
    lastError = null;
    const tok = String(tick.symbol || '');
    const ltp = parseFloat(tick.ltp);
    if (!Number.isFinite(ltp)) return;

    // Spot? (case-insensitive symbol match)
    const ref = SPOT_REF[cfg.underlyingName];
    if (ref && (tok === ref.symbol || tok.toLowerCase() === String(ref.symbol).toLowerCase())) {
      spot = ltp;
      return;
    }
    for (const side of ['ce', 'pe']) {
      if (legState[side].token === tok) {
        legState[side].ltp = ltp;
        evaluate(side).catch((err) => {
          lastError = err.message;
          log.error('premiumTrigger: evaluate failed', { side, err: err.message });
        });
      }
    }
  }

  async function evaluate(side) {
    const legCfg = cfg.legs[side];
    const st = legState[side];
    if (!legCfg.enabled) {
      st.lastDecision = 'leg disabled';
      return;
    }
    const ltp = st.ltp;
    if (ltp == null) { st.lastDecision = 'no LTP yet'; return; }
    if (st.pending) { st.lastDecision = 'pending order'; return; }

    // Adaptive entry: when auto-restrike has tried N times in a row and
    // every chain candidate's LTP was outside the rearm window (no real
    // rotation possible because premiums have decayed below entryTrigger),
    // drop the in-window gate so trades can fire on the available premium.
    // Operator opt-in via `cfg.stuckRestrikeThreshold` (default 3 cycles).
    // The maxEntryDeviation ceiling above entryTrigger still applies — see
    // the cap below.
    const stuckThreshold = (cfg.stuckRestrikeThreshold != null ? cfg.stuckRestrikeThreshold : 3);
    const loosened = (st.stuckRestrikeCount || 0) >= stuckThreshold;

    // 1. Re-arm gate
    if (!st.armed && !st.slLocked && legCfg.entryTrigger != null && st.position === 'NONE') {
      const { lower, upper } = rearmBounds(legCfg);
      const inWindow = ltp >= legCfg.entryTrigger - lower && ltp <= legCfg.entryTrigger + upper;
      if (inWindow || loosened) {
        st.armed = true; st.triggerPrice = null;
      }
    }

    // 2. Drift watcher
    if (st.position === 'NONE' && legCfg.entryTrigger != null) {
      const { lower, upper } = rearmBounds(legCfg);
      const inWindow = ltp >= legCfg.entryTrigger - lower && ltp <= legCfg.entryTrigger + upper;
      if (inWindow) st.outsideWindowSince = null;
      else if (!st.outsideWindowSince) st.outsideWindowSince = clockNow();
      maybeRestrike(side).catch((err) => log.error('maybeRestrike threw', { err: err.message }));
    }

    if (dayLossLocked) { st.lastDecision = 'day-loss locked'; return; }

    // Trading-window guard. Exits still fire (leg is in SHORT branch
    // below); only NEW entries are blocked outside the entry window.
    const canEnter = entriesAllowed(cfg);

    // 3 + 4. Capture / Confirmation -> SELL
    if (st.position === 'NONE' && !st.slLocked && legCfg.entryTrigger != null) {
      // Reject cooldown gate is *entry-only*. A SHORT position must still
      // be allowed to exit while a previous entry-rejection cools down,
      // otherwise a transient broker reject would leave the leg unable
      // to take target / SL on the position it does have.
      if (st.rejectCooldownUntil && Date.now() < st.rejectCooldownUntil) {
        const sec = Math.ceil((st.rejectCooldownUntil - Date.now()) / 1000);
        st.lastDecision = `broker rejected (${st.lastRejectReason || 'unknown'}) — entry cooldown ${sec}s`;
        st.armed = false;
        st.triggerPrice = null;
        return;
      }
      if (!canEnter) { st.lastDecision = `outside trading window (${cfg.tradingHoursStart}-${cfg.eodCutoff} IST)`; return; }
      if (!st.armed) {
        const { lower, upper } = rearmBounds(legCfg);
        st.lastDecision = `not armed — LTP outside rearm window [${(legCfg.entryTrigger - lower).toFixed(2)}..${(legCfg.entryTrigger + upper).toFixed(2)}]`;
        return;
      }
      if (st.triggerPrice != null) {
        const fade = legCfg.captureFadeThreshold != null ? legCfg.captureFadeThreshold : 2;
        if (ltp <= st.triggerPrice - fade) st.triggerPrice = null;
      }
      if (st.triggerPrice != null && !loosened) {
        // Standard mode: drop the captured trigger if LTP exits the rearm
        // window. In loosened mode we accept any LTP, so don't invalidate.
        const { lower, upper } = rearmBounds(legCfg);
        if (ltp < legCfg.entryTrigger - lower || ltp > legCfg.entryTrigger + upper) st.triggerPrice = null;
      }
      if (st.triggerPrice == null) {
        const { lower, upper } = rearmBounds(legCfg);
        const inWindow = ltp >= legCfg.entryTrigger - lower && ltp <= legCfg.entryTrigger + upper;
        if (inWindow || loosened) {
          st.triggerPrice = ltp;
          const need = (ltp + (legCfg.reentryOffset != null ? legCfg.reentryOffset : 1)).toFixed(2);
          const tag = loosened && !inWindow ? ' (loosened — chain decayed)' : '';
          st.lastDecision = `captured ${ltp.toFixed(2)}${tag}, need LTP ≥ ${need} to confirm entry`;
        } else {
          st.lastDecision = `outside rearm window [${(legCfg.entryTrigger - lower).toFixed(2)}..${(legCfg.entryTrigger + upper).toFixed(2)}]`;
        }
        return;
      }
      const offset = legCfg.reentryOffset != null ? legCfg.reentryOffset : 1;
      if (ltp >= st.triggerPrice + offset) {
        // Hard cap: refuse entry beyond maxEntryDeviation above the
        // trigger — but only when NOT loosened. The operator opted into
        // "keep entries open even when LTP is outside window"; that has
        // to extend to both directions, otherwise a leg whose chain
        // premium has risen above (entryTrigger + maxEntryDeviation)
        // is left silently stuck the same way the floor would have
        // stuck it before commit 957bf2c.
        if (!loosened
          && legCfg.maxEntryDeviation != null
          && ltp > legCfg.entryTrigger + legCfg.maxEntryDeviation) {
          st.lastDecision = `LTP > entryTrigger + maxEntryDeviation (cap ${(legCfg.entryTrigger + legCfg.maxEntryDeviation).toFixed(2)}); refusing entry`;
          return;
        }
        const isReentry = st.entryCount > 0;
        const tradeTarget = isReentry
          ? (legCfg.reentryTarget != null ? legCfg.reentryTarget : 1)
          : (legCfg.target != null ? legCfg.target : 1);
        st.lastDecision = `firing ${isReentry ? 'reentry' : 'entry'} @ ${ltp.toFixed(2)}`;
        await doEntry(side, ltp, tradeTarget, isReentry ? 'reentry' : 'entry');
      } else {
        const tag = loosened ? ' (loosened)' : '';
        st.lastDecision = `armed @ ${st.triggerPrice.toFixed(2)}${tag}, need LTP ≥ ${(st.triggerPrice + offset).toFixed(2)}`;
      }
      return;
    }
    if (st.position === 'NONE' && st.slLocked) {
      st.lastDecision = 'stoploss locked for session';
    }

    // 5. Exit
    if (st.position === 'SHORT') {
      // Bracket mode: target/SL is handled by resting broker child orders
      // (paper mode simulates them via bracketManager.checkPaper). We
      // skip the normal tick-driven exit but keep a safety hammer: if
      // LTP overshoots SL by `bracketSafetyOvershoot` and the broker
      // hasn't reported a fill within `bracketSafetyTimeoutMs`, cancel
      // both children and flatten with MKT. Same idempotency lock applies.
      if (cfg.useBrokerBracketExits) {
        const bracket = bracketManager.getBracket(userId, side);
        if (bracket && bracket.status === 'open') {
          if (cfg.mode === 'paper') {
            const paperFill = bracketManager.checkPaper(userId, side, ltp);
            if (paperFill && paperFill.filled) {
              await handleBracketExit(side, paperFill.kind, paperFill.exitPrice);
              return;
            }
          }
          const overshoot = cfg.bracketSafetyOvershoot ?? 2;
          const elapsed = Date.now() - (bracket.openedAt || Date.now());
          if (
            !st.exitInFlight
            && !st.pending
            && Number.isFinite(bracket.slTriggerPrice)
            && ltp >= bracket.slTriggerPrice + overshoot
            && elapsed > (cfg.bracketSafetyTimeoutMs ?? 5000)
          ) {
            // Claim the locks synchronously before going async, so a
            // second tick arriving milliseconds later can't also pass
            // this gate. handleBracketSafety re-uses these locks; the
            // poll-loop path sets them itself.
            st.exitInFlight = true;
            st.pending = true;
            st.lastDecision = `safety hammer @ ${ltp.toFixed(2)} (bracket no-fill)`;
            await handleBracketSafety(side, ltp);
            return;
          }
          const tStr = bracket.targetPrice != null ? bracket.targetPrice.toFixed(2) : '—';
          const sStr = bracket.slTriggerPrice != null ? bracket.slTriggerPrice.toFixed(2) : '—';
          st.lastDecision = `SHORT @ ${st.entryPrice.toFixed(2)} · bracket TGT ${tStr} / SL ${sStr}`;
          return;
        }
      }
      // Strict idempotency lock. `pending` should be enough on its own
      // (set synchronously before any await), but the screenshot showed
      // dual BUYs fired ~1s apart for a single SHORT. Whatever the path
      // — race, rollback, retry — we will not place a second exit for
      // the same trade while one is in flight. Cleared in the finally
      // block below.
      if (st.exitInFlight) {
        st.lastDecision = 'exit already in flight — ignoring duplicate trigger';
        return;
      }
      // Exit-side rejection cooldown. A recurring exit BUY rejection
      // (margin shortfall, contract suspended, etc.) would otherwise
      // hammer Kotak on every tick because evaluate's rollback restores
      // SHORT and the next tick re-fires.
      if (st.rejectCooldownUntil && Date.now() < st.rejectCooldownUntil) {
        const sec = Math.ceil((st.rejectCooldownUntil - Date.now()) / 1000);
        st.lastDecision = `exit blocked: broker rejected (${st.lastRejectReason || 'unknown'}) — cooldown ${sec}s`;
        return;
      }
      const activeTarget = st.currentTradeTarget != null ? st.currentTradeTarget : legCfg.target;
      const targetPrice = activeTarget != null && st.entryPrice != null ? st.entryPrice - activeTarget : null;
      const slPrice = legCfg.stoploss != null && st.entryPrice != null ? st.entryPrice + legCfg.stoploss : null;
      const hitTarget = targetPrice != null && ltp <= targetPrice;
      const hitStop = slPrice != null && ltp >= slPrice;
      if (!hitTarget && !hitStop) {
        // In trade — keep the dashboard's Status line useful. Show the
        // entry / target / SL anchors so the operator can see how close
        // we are to either exit. The live LTP is rendered separately by
        // the browser, so we deliberately omit it from this snapshot.
        const tStr = targetPrice != null ? targetPrice.toFixed(2) : '—';
        const sStr = slPrice != null ? slPrice.toFixed(2) : '—';
        st.lastDecision = `SHORT @ ${st.entryPrice.toFixed(2)} · target ${tStr} · SL ${sStr}`;
        return;
      }
      const reason = hitTarget ? 'target' : 'stoploss';
      const prevEntry = st.entryPrice;
      const prevTradeTarget = st.currentTradeTarget;
      // Claim the exit lock synchronously, BEFORE any state mutation or
      // await, so a second concurrent evaluator on the same leg can't
      // get past the gate above.
      st.exitInFlight = true;
      st.pending = true;
      st.position = 'NONE';
      st.entryPrice = null;
      st.armed = false;
      st.triggerPrice = null;
      st.currentTradeTarget = null;
      st.lastDecision = `${reason} hit @ ${ltp.toFixed(2)} — exiting`;
      try {
        const result = await placeOrder(side, 'BUY', ltp, reason);
        if (!result || !result.ok) {
          // Roll back
          st.position = 'SHORT';
          st.entryPrice = prevEntry;
          st.currentTradeTarget = prevTradeTarget;
        } else {
          if (hitStop) st.slLocked = true;
          const qty = cfg.qty * (st.lotSize || 1);
          const grossPoints = prevEntry - ltp;
          const grossPnl = grossPoints * qty;
          const charges = tradeCharges(prevEntry, ltp, qty);
          const netPnl = grossPnl - charges.total;
          st.totalCharges += charges.total;
          st.lastTradeCharges = charges;
          recordTradeStats(st, netPnl, hitTarget ? 'win' : hitStop ? 'loss' : null, grossPoints);
        }
      } catch (err) {
        st.position = 'SHORT';
        st.entryPrice = prevEntry;
        st.currentTradeTarget = prevTradeTarget;
        throw err;
      } finally {
        st.pending = false;
        st.exitInFlight = false;
        persistState().catch(() => {});
      }
    }
  }

  function tradeCharges(entry, exit, qty) {
    const sellTurnover = entry * qty;
    const buyTurnover = exit * qty;
    const total2 = sellTurnover + buyTurnover;
    const brokerage = 2 * (cfg.brokeragePerOrder || 0);
    const stt = sellTurnover * (cfg.sttPctSell || 0) / 100;
    const exch = total2 * (cfg.exchTxnPct || 0) / 100;
    const sebi = total2 * (cfg.sebiPct || 0) / 100;
    const stamp = buyTurnover * (cfg.stampPctBuy || 0) / 100;
    const gst = (brokerage + exch + sebi) * (cfg.gstPct || 0) / 100;
    const total = brokerage + stt + exch + sebi + stamp + gst;
    return { brokerage, stt, exch, sebi, stamp, gst, total };
  }

  function recordTradeStats(st, netPnl, outcome, points) {
    st.realizedPnl += netPnl;
    st.tradesCompleted += 1;
    const pts = Number.isFinite(points) ? points : 0;
    st.realizedPoints += pts;
    if (outcome === 'win') {
      st.tradesWon += 1;
      st.grossProfit += netPnl;
      if (netPnl > st.maxTradeProfit) st.maxTradeProfit = netPnl;
      if (pts > 0) {
        st.grossProfitPoints += pts;
        if (pts > st.maxTradeProfitPoints) st.maxTradeProfitPoints = pts;
      }
    } else if (outcome === 'loss') {
      st.tradesLost += 1;
      const absLoss = Math.abs(netPnl);
      st.grossLoss += absLoss;
      if (absLoss > st.maxTradeLoss) st.maxTradeLoss = absLoss;
      const absPts = Math.abs(pts);
      st.grossLossPoints += absPts;
      if (absPts > st.maxTradeLossPoints) st.maxTradeLossPoints = absPts;
    }
    const tot = legState.ce.realizedPnl + legState.pe.realizedPnl;
    if (tot > peakRealized) peakRealized = tot;
    const dd = peakRealized - tot;
    if (dd > maxDrawdown) maxDrawdown = dd;
    if (cfg.maxDailyLoss != null && !dayLossLocked && tot <= -cfg.maxDailyLoss) {
      dayLossLocked = true;
      lastError = `Max daily loss ₹${cfg.maxDailyLoss} reached (realised ₹${tot.toFixed(2)}); both legs locked. Re-save settings to resume.`;
      log.warn('premiumTrigger: daily loss cap hit', { userId, tot, cap: cfg.maxDailyLoss });
    }
  }

  async function doEntry(side, ltp, tradeTarget, reason) {
    const st = legState[side];
    st.pending = true;
    st.position = 'SHORT';
    st.entryPrice = ltp;
    st.armed = false;
    st.triggerPrice = null;
    st.currentTradeTarget = tradeTarget;
    try {
      const result = await placeOrder(side, 'SELL', ltp, reason);
      if (!result || !result.ok) {
        st.position = 'NONE';
        st.entryPrice = null;
        st.currentTradeTarget = null;
      } else {
        st.entryCount += 1;
        st.tradesPlaced += 1;
        st.entryBrokerOrderId = result.brokerOrderId || null;
        // A successful entry means loosened mode worked — drop the
        // counter so subsequent restrike cycles start fresh.
        st.stuckRestrikeCount = 0;
        if (cfg.useBrokerBracketExits) {
          await openBracketForLeg(side, ltp, tradeTarget, result.brokerOrderId);
        }
      }
    } catch (err) {
      st.position = 'NONE';
      st.entryPrice = null;
      st.currentTradeTarget = null;
      throw err;
    } finally {
      st.pending = false;
      persistState().catch(() => {});
    }
  }

  // Bracket child filled — record the exit, update P&L, log a synthetic
  // signal row so the audit trail looks the same as the tick-driven path.
  async function handleBracketExit(side, kind, exitPrice) {
    const st = legState[side];
    if (st.position !== 'SHORT') return;
    const prevEntry = st.entryPrice;
    st.exitInFlight = true;
    st.pending = true;
    st.position = 'NONE';
    st.entryPrice = null;
    st.armed = false;
    st.triggerPrice = null;
    st.currentTradeTarget = null;
    st.entryBrokerOrderId = null;
    if (kind === 'sl') st.slLocked = true;
    try {
      const qty = cfg.qty * (st.lotSize || 1);
      const grossPoints = prevEntry - exitPrice;
      const grossPnl = grossPoints * qty;
      const charges = tradeCharges(prevEntry, exitPrice, qty);
      const netPnl = grossPnl - charges.total;
      st.totalCharges += charges.total;
      st.lastTradeCharges = charges;
      const outcome = kind === 'target' ? 'win' : kind === 'sl' ? 'loss' : null;
      recordTradeStats(st, netPnl, outcome, grossPoints);
      const reason = kind === 'sl' ? 'stoploss' : kind === 'target' ? 'target' : 'bracket_exit';
      pushSignal({ side, action: 'BUY', reason, price: exitPrice, qty, status: 'placed' });
    } finally {
      st.pending = false;
      st.exitInFlight = false;
      persistState().catch(() => {});
    }
  }

  // Tick safety hammer fired. Cancel both children and flatten with MKT
  // so we don't sit naked while waiting for the broker to wake up.
  async function handleBracketSafety(side, ltp) {
    await bracketManager.emergencyFlatten(userId, side, 'sl_safety_hammer');
    const st = legState[side];
    if (st.position !== 'SHORT') return;
    const prevEntry = st.entryPrice;
    st.exitInFlight = true;
    st.pending = true;
    st.position = 'NONE';
    st.entryPrice = null;
    st.armed = false;
    st.triggerPrice = null;
    st.currentTradeTarget = null;
    st.slLocked = true;
    try {
      const result = await placeOrder(side, 'BUY', ltp, 'sl_safety_hammer');
      if (result?.ok) {
        const qty = cfg.qty * (st.lotSize || 1);
        const grossPoints = prevEntry - ltp;
        const grossPnl = grossPoints * qty;
        const charges = tradeCharges(prevEntry, ltp, qty);
        const netPnl = grossPnl - charges.total;
        st.totalCharges += charges.total;
        st.lastTradeCharges = charges;
        recordTradeStats(st, netPnl, 'loss', grossPoints);
      } else {
        st.position = 'SHORT';
        st.entryPrice = prevEntry;
      }
    } finally {
      st.pending = false;
      st.exitInFlight = false;
      persistState().catch(() => {});
    }
  }

  // Lightweight in-memory signal log for bracket-driven exits, mirroring
  // what placeOrder writes for the tick-driven path. Persists to the
  // signals table so /api/premium-trigger/signals reflects the trade.
  function pushSignal({ side, action, reason, price, qty, status }) {
    const st = legState[side];
    const entry = {
      at: new Date().toISOString(),
      id: `BRACKET-${userId}-${Date.now()}`,
      mode: cfg.mode,
      side, action, reason,
      price: Number(price.toFixed(2)),
      qty,
      symbol: st.symbol,
      strike: st.strike,
      status,
    };
    signalHistory.push(entry);
    if (signalHistory.length > 200) signalHistory.splice(0, signalHistory.length - 200);
    query(
      `INSERT INTO premium_trigger_signals
         (user_id, side, action, reason, price, qty, symbol, strike, mode, status)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [userId, side, action, reason, entry.price, qty, st.symbol, st.strike, cfg.mode, status],
    ).catch(() => {});
  }

  // Wrapper used by doEntry when broker-side bracket exits are enabled.
  // Returns silently on success; on failure flattens the just-opened
  // SHORT (paper or live) so we never leave an exposed naked position.
  async function openBracketForLeg(side, entryPrice, tradeTarget, entryOrderId) {
    const st = legState[side];
    const legCfg = cfg.legs[side];
    const qty = cfg.qty * (st.lotSize || 1);
    const target = tradeTarget != null ? tradeTarget : legCfg.target;
    const stoploss = legCfg.stoploss;
    if (!Number.isFinite(target) || !Number.isFinite(stoploss)) {
      log.warn('premiumTrigger: bracket skipped — missing target/sl', { side });
      return;
    }
    const res = await bracketManager.openBracket(userId, side, {
      mode: cfg.mode,
      qty,
      symbol: st.symbol,
      token: st.token,
      segment: st.exchangeSegment,
      entryPrice,
      target,
      stoploss,
      entryOrderId,
    });
    if (!res.ok) {
      log.error('premiumTrigger: bracket open failed; flattening exposed entry',
        { side, reason: res.reason });
      // Flatten right away so we don't sit naked — the caller's doEntry
      // already set position=SHORT. Only clear local position state when
      // the flatten BUY actually succeeds; a broker reject must leave us
      // SHORT so reconcile / next tick keeps trying.
      let flatOk = false;
      try {
        const flat = await placeOrder(side, 'BUY', entryPrice, 'bracket_failed');
        flatOk = !!flat?.ok;
        if (flatOk) {
          // Wash trade by construction (entry and immediate exit at the
          // same price). P&L ≈ −charges; record so the audit log and
          // P&L counters stay consistent.
          const qty = cfg.qty * (st.lotSize || 1);
          const charges = tradeCharges(entryPrice, entryPrice, qty);
          const netPnl = -charges.total;
          st.totalCharges += charges.total;
          st.lastTradeCharges = charges;
          recordTradeStats(st, netPnl, 'loss', 0);
        }
      } catch (err) {
        log.error('premiumTrigger: emergency flatten after bracket fail also failed',
          { side, err: err.message });
      }
      if (flatOk) {
        st.position = 'NONE';
        st.entryPrice = null;
        st.currentTradeTarget = null;
        st.entryBrokerOrderId = null;
      }
      // else: keep position SHORT so the next tick / reconcile sees we
      // still owe a flatten. The reject cooldown below will throttle
      // re-attempts.
      st.consecutiveRejects = (st.consecutiveRejects || 0) + 1;
      st.lastRejectReason = res.reason;
      const backoffMs = Math.min(60_000, 5000 * Math.pow(2, st.consecutiveRejects - 1));
      st.rejectCooldownUntil = Date.now() + backoffMs;
    }
  }

  // -------------------------------------------------------------------------
  // Order placement (sync, direct against brokerService)
  // -------------------------------------------------------------------------

  async function placeOrder(side, action, price, reason) {
    const st = legState[side];
    if (!st.symbol) {
      log.warn('premiumTrigger: missing symbol — cannot place order', { side });
      return { ok: false, reason: 'no_symbol' };
    }
    const qty = cfg.qty * (st.lotSize || 1);

    // Backtest short-circuit: synthesise a fill, push to in-memory
    // signalHistory only, NO DB writes, NO broker call. The state
    // machine itself is unchanged — same evaluate / recordTradeStats
    // path runs above and below this call.
    if (cfg._backtest) {
      paperOrderSeq += 1;
      const ts = clockNow();
      const id = `BACKTEST-${userId}-${ts}-${paperOrderSeq}`;
      const entry = {
        at: ts, id,
        mode: 'backtest',
        side, action, reason, price, qty,
        symbol: st.symbol, strike: st.strike,
        status: 'backtest-filled',
        brokerOrderId: id,
        rejectReason: null,
      };
      signalHistory.push(entry);
      if (signalHistory.length > SIGNAL_HISTORY_MAX) {
        signalHistory.splice(0, signalHistory.length - SIGNAL_HISTORY_MAX);
      }
      return { ok: true, brokerOrderId: id, paper: true, backtest: true };
    }

    const isPaper = cfg.mode === 'paper';
    log.info(`premiumTrigger: signal (${isPaper ? 'PAPER' : 'LIVE'})`,
      { userId, side, action, price, reason, qty, symbol: st.symbol });

    const sigRow = await query(
      `INSERT INTO premium_trigger_signals
         (user_id, side, action, reason, price, qty, symbol, strike, mode, status)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')`,
      [userId, side, action, reason, price, qty, st.symbol, st.strike,
       isPaper ? 'paper' : 'live'],
    );

    let result;
    if (isPaper) {
      // Paper trading: synthesise an immediate "fill" at the supplied
      // price (which is the LTP that triggered the signal in evaluate).
      // No broker call, no margin check, no rate-limit consumption.
      // Charges still get applied downstream in recordTradeStats so
      // paper P&L matches what the live account would have produced.
      paperOrderSeq += 1;
      const paperId = `PAPER-${userId}-${Date.now()}-${paperOrderSeq}`;
      result = { ok: true, brokerOrderId: paperId, paper: true };
    } else if (action === 'SELL') {
      // MIS entry. Kotak's TRADEAPI rejects BO/CO ("BO/CO OrderType not
      // allowed for TRADEAPI") — those products are GUI-only on the NEO
      // app. We use plain MIS so the position auto-squares-off at session
      // end if nothing else closed it; target / stoploss exits are issued
      // by evaluate() as explicit BUY MKT orders below.
      const order = {
        symbol: st.token,
        tradingSymbol: st.symbol,
        exchangeSegment: st.exchangeSegment,
        side, qty, product: 'MIS', orderType: 'MKT',
        price: 0, triggerPrice: 0,
      };
      try {
        const resp = await brokerService.placeOrder(userId, order);
        const ok = resp?.stat === 'Ok' && resp?.nOrdNo;
        result = ok
          ? { ok: true, brokerOrderId: resp.nOrdNo, raw: resp }
          : { ok: false, reason: resp?.emsg || resp?.errMsg || resp?.stat || 'unknown', raw: resp };
      } catch (err) {
        result = { ok: false, reason: err.message };
      }
    } else {
      // Exit (BUY) path. The strategy owns target / stoploss — Kotak's
      // TRADEAPI doesn't auto-fire bracket legs, so every exit reason
      // (target, stoploss, manual, eod, capture_fade, restrike) goes
      // through the same plain MIS BUY MKT to flatten the SHORT.
      const order = {
        symbol: st.token, tradingSymbol: st.symbol,
        exchangeSegment: st.exchangeSegment,
        side, qty, product: 'MIS', orderType: 'MKT',
        price: 0, triggerPrice: 0,
      };
      try {
        const resp = await brokerService.placeOrder(userId, order);
        const ok = resp?.stat === 'Ok' && resp?.nOrdNo;
        result = ok
          ? { ok: true, brokerOrderId: resp.nOrdNo, raw: resp }
          : { ok: false, reason: resp?.emsg || resp?.errMsg || resp?.stat || 'unknown', raw: resp };
      } catch (err) {
        result = { ok: false, reason: err.message };
      }
      if (result.ok) st.entryBrokerOrderId = null;
    }

    const entry = {
      at: Date.now(),
      id: sigRow.insertId,
      mode: isPaper ? 'paper' : 'live',
      side, action, reason, price, qty,
      symbol: st.symbol, strike: st.strike,
      status: result.ok ? (isPaper ? 'paper-filled' : 'placed') : 'rejected',
      brokerOrderId: result.brokerOrderId || null,
      rejectReason: result.ok ? null : result.reason,
    };
    signalHistory.push(entry);
    if (signalHistory.length > SIGNAL_HISTORY_MAX) {
      signalHistory.splice(0, signalHistory.length - SIGNAL_HISTORY_MAX);
    }

    await query(
      `UPDATE premium_trigger_signals
         SET status = ?, broker_order_id = ?, reject_reason = ?
       WHERE id = ?`,
      [entry.status, entry.brokerOrderId, entry.rejectReason, sigRow.insertId],
    );

    if (!result.ok) {
      lastError = `signal ${side}:${reason} rejected: ${result.reason}`;
      // Per-leg backoff so a recurring rejection (lot-size mismatch, BO not
      // allowed for F&O, margin shortfall, etc.) doesn't hammer Kotak with
      // a fresh order on every tick. evaluate() honours `rejectCooldownUntil`
      // and refuses to re-arm until then. Cooldown grows up to 5 minutes
      // for repeated failures.
      st.consecutiveRejects = (st.consecutiveRejects || 0) + 1;
      const backoffSec = Math.min(30 * Math.pow(2, st.consecutiveRejects - 1), 300);
      st.rejectCooldownUntil = Date.now() + backoffSec * 1000;
      st.lastRejectReason = result.reason;
      // Surface the raw Kotak response on first rejection so the operator
      // can see exactly what the broker objected to (eg. invalid lot size,
      // BO not enabled for FNO, etc).
      const raw = result.raw && Object.keys(result.raw).length
        ? JSON.stringify(result.raw).slice(0, 500) : null;
      log.warn('premiumTrigger: order rejected', {
        side, action, reason,
        rejectReason: result.reason, raw,
        consecutiveRejects: st.consecutiveRejects,
        cooldownSec: backoffSec,
      });
    } else {
      st.consecutiveRejects = 0;
      st.rejectCooldownUntil = null;
      st.lastRejectReason = null;
      eventLog.log(userId,
        isPaper ? 'PT_ORDER_PAPER' : 'PT_ORDER_PLACED',
        'INFO',
        `[${isPaper ? 'PAPER' : 'LIVE'}] ${action} ${qty} ${st.symbol} @ ${price} (${reason})`,
        { brokerOrderId: result.brokerOrderId }).catch(() => {});
    }
    return result;
  }

  // -------------------------------------------------------------------------
  // Auto-restrike (OTM only)
  // -------------------------------------------------------------------------

  async function maybeRestrike(side) {
    const st = legState[side];
    if (st.restrikeInProgress) return;
    if (!cfg.autoRestrikeOnDrift) return;
    if (cfg.strikeMode !== 'otm') return;
    if (st.position !== 'NONE') return;
    if (dayLossLocked) return;

    const dwellMs = (cfg.restrikeDwellSec != null ? cfg.restrikeDwellSec : 60) * 1000;
    const cooldownMs = (cfg.restrikeCooldownSec != null ? cfg.restrikeCooldownSec : 300) * 1000;
    const now = clockNow();
    if (!st.outsideWindowSince || now - st.outsideWindowSince < dwellMs) return;
    if (st.lastRestrikeAt && now - st.lastRestrikeAt < cooldownMs) return;

    st.restrikeInProgress = true;
    const oldToken = st.token, oldSegment = st.exchangeSegment;
    try {
      const expiry = legState[side].expiry || cfg.expiry;
      if (!expiry) throw new Error('no expiry resolved');
      const chain = (await instrumentService.getChain(cfg.underlyingName, expiry)).chain;
      if (spot == null) spot = await fetchSpot();
      if (spot == null) throw new Error('spot LTP unavailable');
      await resolveOTMLeg(side, spot, expiry, chain);
      const newToken = st.token;
      if (newToken !== oldToken && mdc) {
        try { mdc.unsubscribe([{ token: oldToken, segment: oldSegment }]); } catch (_) {}
        try { mdc.subscribe([{ token: newToken, segment: st.exchangeSegment }]); } catch (_) {}
        st.ltp = null;
        st.triggerPrice = null;
        st.armed = false;
        st.outsideWindowSince = null;
        st.restrikeCount += 1;
        // Real rotation — chain conditions changed, drop the stuck counter
        // so the loosened-entry adaptive mode reverts to standard behavior.
        st.stuckRestrikeCount = 0;
        log.info('premiumTrigger: auto-restrike', { side, oldToken, newToken });
      } else {
        // Same strike picked again — no chain candidate has its LTP
        // in the rearm window. Increment the stuck counter; once it
        // crosses cfg.stuckRestrikeThreshold (default 3), evaluate()
        // drops the in-window gate so trades can fire on the chain's
        // available premium. Leave `outsideWindowSince` intact so the
        // drift watcher and dashboard keep showing the truth.
        st.stuckRestrikeCount = (st.stuckRestrikeCount || 0) + 1;
        const threshold = (cfg.stuckRestrikeThreshold != null ? cfg.stuckRestrikeThreshold : 3);
        const cyclesUntilLoose = Math.max(0, threshold - st.stuckRestrikeCount);
        const remainingMs = Math.max(0, cooldownMs - (now - st.lastRestrikeAt));
        const cooldownSec = Math.ceil(remainingMs / 1000);
        st.lastDecision = cyclesUntilLoose > 0
          ? `auto-restrike: no chain fit (retry in ${cooldownSec}s, loosened entry in ${cyclesUntilLoose} more cycle${cyclesUntilLoose === 1 ? '' : 's'})`
          : `auto-restrike: chain decayed below entryTrigger — loosened entry active`;
        log.warn('premiumTrigger: auto-restrike picked same strike — no in-window candidate',
          { side, token: newToken, stuckCount: st.stuckRestrikeCount, threshold });
      }
      st.lastRestrikeAt = now;
    } catch (err) {
      lastError = `auto-restrike (${side}) failed: ${err.message}`;
      log.error('premiumTrigger: auto-restrike failed', { side, err: err.message });
    } finally {
      st.restrikeInProgress = false;
      persistState().catch(() => {});
    }
  }

  async function restrikeLeg(side) {
    if (side !== 'ce' && side !== 'pe') throw new Error('side must be ce or pe');
    if (!running) throw new Error('strategy not running');
    if (cfg.strikeMode !== 'otm') throw new Error('manual restrike requires OTM strike mode');
    const st = legState[side];
    if (st.restrikeInProgress) throw new Error('restrike already in progress');
    if (st.pending) throw new Error(`${side.toUpperCase()} order in flight; try again in a moment`);

    // If the leg is currently SHORT, square it off first so we don't
    // orphan the open position when the strike (and therefore the
    // symbol/token) rotates. We mirror the per-leg exit logic from
    // exitAll(): place a BUY MKT, record stats on success, roll state
    // back on failure. If the broker rejects the exit, abort the
    // restrike — the operator's open position must never be left in a
    // limbo state.
    if (st.position === 'SHORT') {
      if (st.ltp == null) throw new Error(`${side.toUpperCase()} has no LTP yet; cannot square off automatically`);
      // Bracket mode: cancel both child orders before flattening so we
      // don't leave SL-M / LIMIT alive after the position is gone.
      if (cfg.useBrokerBracketExits && bracketManager.getBracket(userId, side)) {
        const closed = await bracketManager.closeBracket(userId, side, 'restrike');
        if (!closed.ok) throw new Error('failed to cancel bracket children; aborting restrike');
      }
      const prevEntry = st.entryPrice;
      const prevTradeTarget = st.currentTradeTarget;
      const exitLtp = st.ltp;
      st.pending = true;
      st.position = 'NONE';
      st.entryPrice = null;
      st.armed = false;
      st.triggerPrice = null;
      st.currentTradeTarget = null;
      try {
        const result = await placeOrder(side, 'BUY', exitLtp, 'restrike');
        if (!result || !result.ok) {
          st.position = 'SHORT';
          st.entryPrice = prevEntry;
          st.currentTradeTarget = prevTradeTarget;
          throw new Error(`square-off rejected: ${result?.reason || 'unknown'}`);
        }
        const qty = cfg.qty * (st.lotSize || 1);
        const grossPoints = prevEntry - exitLtp;
        const grossPnl = grossPoints * qty;
        const charges = tradeCharges(prevEntry, exitLtp, qty);
        const netPnl = grossPnl - charges.total;
        st.totalCharges += charges.total;
        st.lastTradeCharges = charges;
        // Manual restrike's square-off succeeded — clear any stale reject
        // lock so the new strike can fire normally.
        st.consecutiveRejects = 0;
        st.rejectCooldownUntil = null;
        st.lastRejectReason = null;
        recordTradeStats(st, netPnl, netPnl > 0 ? 'win' : netPnl < 0 ? 'loss' : null, grossPoints);
      } catch (err) {
        if (st.position !== 'SHORT') {
          st.position = 'SHORT';
          st.entryPrice = prevEntry;
          st.currentTradeTarget = prevTradeTarget;
        }
        throw err;
      } finally {
        st.pending = false;
        persistState().catch(() => {});
      }
    }

    st.restrikeInProgress = true;
    const oldToken = st.token, oldSegment = st.exchangeSegment;
    try {
      const expiry = st.expiry || cfg.expiry;
      if (!expiry) throw new Error('no expiry resolved');
      const chain = (await instrumentService.getChain(cfg.underlyingName, expiry)).chain;
      if (spot == null) spot = await fetchSpot();
      if (spot == null) throw new Error('spot LTP unavailable');
      await resolveOTMLeg(side, spot, expiry, chain);
      const newToken = st.token;
      if (newToken !== oldToken && mdc) {
        try { mdc.unsubscribe([{ token: oldToken, segment: oldSegment }]); } catch (_) {}
        try { mdc.subscribe([{ token: newToken, segment: st.exchangeSegment }]); } catch (_) {}
        st.ltp = null;
        st.triggerPrice = null;
        st.armed = false;
        st.outsideWindowSince = null;
        st.restrikeCount += 1;
        st.stuckRestrikeCount = 0;
      }
      st.lastRestrikeAt = Date.now();
    } finally {
      st.restrikeInProgress = false;
      persistState().catch(() => {});
    }
    return status();
  }

  // -------------------------------------------------------------------------
  // Lifecycle: start / stop / toggle / exitAll
  // -------------------------------------------------------------------------

  async function start() {
    if (running) return;
    await load();
    if (!cfg.enabled) { log.info('premiumTrigger: not enabled; skipping start'); return; }
    try {
      for (const side of ['ce', 'pe']) validateLeg(side, cfg.legs[side]);
    } catch (err) {
      lastError = `invalid config: ${err.message}`;
      log.error('premiumTrigger: refusing to start', { err: err.message });
      return;
    }

    // Both paper and live modes need a broker session — paper mode
    // still uses the live tick stream + REST quotes for spot lookups
    // and OTM scanning. Only the order placement is simulated.
    const account = await accounts.getByUserId(userId);
    if (!account || !account.session_token) {
      lastError = 'broker session not connected — log in to /broker first';
      log.warn(lastError);
      return;
    }
    mdc = marketData.getClient(userId);
    if (!mdc) mdc = marketData.attachClient(userId, account);

    try {
      await resolveLegs();
    } catch (err) {
      lastError = err.message;
      log.error('premiumTrigger: resolveLegs failed', { err: err.message });
      return;
    }

    // Reconcile persisted legState.position with what the broker
    // actually shows. Only matters when restoring from a snapshot —
    // the user may have manually closed a leg via Kotak's app while
    // we were down. Live mode only; in paper mode the broker has no
    // position to reconcile against.
    if (cfg.mode === 'live') {
      try { await reconcilePositions(); }
      catch (err) { log.warn('premiumTrigger: reconcile failed', { err: err.message }); }
    }

    // Subscribe CE + PE + spot
    const items = [];
    for (const side of ['ce', 'pe']) {
      if (legState[side].token) {
        items.push({ token: legState[side].token, segment: legState[side].exchangeSegment });
      }
    }
    const ref = SPOT_REF[cfg.underlyingName];
    if (ref) items.push({ token: ref.symbol, segment: ref.segment });
    try { mdc.subscribe(items); } catch (err) { log.warn('subscribe failed', { err: err.message }); }

    tickHandler = (tick) => onTick(tick);
    mdc.on('tick', tickHandler);
    running = true;

    // Hydrate the in-memory signal history from DB so the dashboard
    // shows the last few signals even on a fresh app boot.
    try {
      const rows = await query(
        `SELECT id, side, action, reason, price, qty, symbol, strike,
                status, broker_order_id AS brokerOrderId,
                reject_reason AS rejectReason,
                UNIX_TIMESTAMP(created_at) * 1000 AS at
           FROM premium_trigger_signals
           WHERE user_id = ?
           ORDER BY id DESC
           LIMIT ?`,
        [userId, SIGNAL_HISTORY_MAX],
      );
      signalHistory.length = 0;
      for (const r of rows.reverse()) signalHistory.push(r);
    } catch (err) {
      log.warn('premiumTrigger: signal history hydrate failed', { err: err.message });
    }

    await query('UPDATE premium_trigger_config SET running = 1 WHERE user_id = ?', [userId]);
    scheduleEod();
    startBracketPoll();
    log.info('premiumTrigger: started', {
      userId, ce: legState.ce.symbol, pe: legState.pe.symbol,
      mode: cfg.mode, tradingHours: cfg.tradingHoursEnabled,
      brackets: !!cfg.useBrokerBracketExits,
    });
  }

  function clearEodTimer() {
    if (eodTimer) { clearTimeout(eodTimer); eodTimer = null; }
  }

  let bracketPollTimer = null;
  function startBracketPoll() {
    if (bracketPollTimer) return;
    if (!cfg.useBrokerBracketExits) return;
    if (cfg.mode !== 'live') return;
    const ms = Math.max(500, cfg.bracketStatusPollMs || 2000);
    bracketPollTimer = setInterval(async () => {
      try {
        const fills = await bracketManager.pollStatus(userId);
        for (const f of fills) {
          if (f.kind === 'sl' || f.kind === 'target') {
            await handleBracketExit(f.side, f.kind, f.exitPrice);
          } else if (f.kind === 'child_died' || f.kind === 'double_fill') {
            // One leg died unexpectedly. Flatten any residual SHORT
            // with a fresh MKT BUY so the position doesn't sit naked.
            const st = legState[f.side];
            if (st.position === 'SHORT' && st.ltp != null) {
              await handleBracketSafety(f.side, st.ltp);
            }
          }
        }
      } catch (err) {
        log.warn('bracket poll failed', { err: err.message });
      }
    }, ms);
    bracketPollTimer.unref?.();
  }
  function clearBracketPoll() {
    if (bracketPollTimer) { clearInterval(bracketPollTimer); bracketPollTimer = null; }
  }

  // Schedule a one-shot timer to fire at eodAutoExit IST. The handler
  // squares any open SHORTs, stops the strategy, and re-arms the timer
  // for the next trading day. Best-effort: if the process restarts,
  // bootResume reschedules on the next start().
  function scheduleEod() {
    clearEodTimer();
    if (!cfg.tradingHoursEnabled) return;
    const target = hhmmToMin(cfg.eodAutoExit);
    if (target == null) return;
    const now = nowIST();
    const todayTarget = new Date(now);
    todayTarget.setUTCHours(Math.floor(target / 60), target % 60, 0, 0);
    let delay = todayTarget.getTime() - now.getTime();
    // If already past EOD, schedule for tomorrow.
    if (delay <= 0) delay += 24 * 60 * 60 * 1000;
    eodTimer = setTimeout(async () => {
      log.info('premiumTrigger: EOD auto-exit fired', { userId, eodAutoExit: cfg.eodAutoExit });
      try {
        await exitAll('eod');
      } catch (err) {
        log.error('premiumTrigger: EOD exitAll failed', { err: err.message });
      }
      // Stop the strategy until tomorrow. The operator (or cron / next
      // start()) can resume; for paper-only sessions there's nothing
      // unsafe about leaving it running, but the spec is clear: square
      // off then halt.
      try { await stop(); } catch (_) {}
    }, delay);
    eodTimer.unref?.();
    log.info('premiumTrigger: EOD scheduled', {
      userId, fireInMs: delay, eodAutoExit: cfg.eodAutoExit,
    });
  }

  async function stop() {
    if (!running) return;
    running = false;
    clearEodTimer();
    clearBracketPoll();
    if (mdc && tickHandler) {
      try { mdc.removeListener('tick', tickHandler); } catch (_) {}
    }
    tickHandler = null;
    try {
      await query('UPDATE premium_trigger_config SET running = 0 WHERE user_id = ?', [userId]);
    } catch (_) {}
    log.info('premiumTrigger: stopped', { userId });
  }

  async function toggle(on) {
    await save({ enabled: !!on });
    if (on) await start(); else await stop();
    return cfg;
  }

  async function exitAll(reason = 'manual') {
    for (const side of ['ce', 'pe']) {
      const st = legState[side];
      if (st.position !== 'SHORT' || st.ltp == null || st.pending) continue;
      // Bracket mode: cancel both child orders before flattening so the
      // SL-M / LIMIT can't fire after we've already exited via MKT.
      if (cfg.useBrokerBracketExits && bracketManager.getBracket(userId, side)) {
        try { await bracketManager.closeBracket(userId, side, reason); }
        catch (err) { log.warn('exitAll: closeBracket failed', { side, err: err.message }); }
      }
      const prevEntry = st.entryPrice;
      const prevTradeTarget = st.currentTradeTarget;
      st.pending = true;
      st.position = 'NONE';
      st.entryPrice = null;
      st.armed = false;
      st.triggerPrice = null;
      st.currentTradeTarget = null;
      const exitLtp = st.ltp;
      try {
        const result = await placeOrder(side, 'BUY', exitLtp, reason);
        if (!result || !result.ok) {
          st.position = 'SHORT';
          st.entryPrice = prevEntry;
          st.currentTradeTarget = prevTradeTarget;
        } else {
          const qty = cfg.qty * (st.lotSize || 1);
          const grossPoints = prevEntry - exitLtp;
          const grossPnl = grossPoints * qty;
          const charges = tradeCharges(prevEntry, exitLtp, qty);
          const netPnl = grossPnl - charges.total;
          st.totalCharges += charges.total;
          st.lastTradeCharges = charges;
          // A clean square-off clears the reject cooldown so the next
          // session isn't silently blocked by a stale lock.
          st.consecutiveRejects = 0;
          st.rejectCooldownUntil = null;
          st.lastRejectReason = null;
          recordTradeStats(st, netPnl, netPnl > 0 ? 'win' : netPnl < 0 ? 'loss' : null, grossPoints);
        }
      } catch (err) {
        st.position = 'SHORT';
        st.entryPrice = prevEntry;
        st.currentTradeTarget = prevTradeTarget;
        log.error('exitAll failed', { side, err: err.message });
      } finally {
        st.pending = false;
      }
    }
    persistState().catch(() => {});
    return status();
  }

  // After a restart we restore legState from disk. If the operator
  // manually closed a leg via the Kotak mobile app while we were
  // down, our snapshot says position='SHORT' but the broker shows
  // nothing. Fetching positions and matching by tradingSymbol is
  // the only correct way to know — otherwise we'd fire a phantom
  // BUY-to-close on the next exit signal.
  async function reconcilePositions() {
    let resp;
    try {
      resp = await brokerService.fetchPositions(userId);
    } catch (err) {
      log.warn('premiumTrigger: fetchPositions failed', { err: err.message });
      return;
    }
    const list = Array.isArray(resp?.data) ? resp.data : [];
    let orderBook = null;
    const liveOrdSt = new Set(['open', 'open pending', 'trigger pending',
      'complete', 'partially filled', 'after market order req received']);
    for (const side of ['ce', 'pe']) {
      const st = legState[side];
      if (st.position !== 'SHORT' || !st.symbol) continue;
      const match = list.find(p => p.trdSym === st.symbol);
      const netQty = match ? parseInt(match.qty, 10) || 0 : 0;
      // For a live SHORT, broker's net qty is negative (sold). If
      // netQty >= 0 (no short, or accidentally long) — clear local.
      if (netQty >= 0) {
        log.warn('premiumTrigger: reconciliation cleared phantom SHORT', {
          side, symbol: st.symbol, brokerQty: netQty,
        });
        st.position = 'NONE';
        st.entryPrice = null;
        st.currentTradeTarget = null;
        st.armed = false;
        st.triggerPrice = null;
        st.entryBrokerOrderId = null;
        continue;
      }
      // Position is still SHORT on the broker side. If our snapshot lost
      // the entry SELL order id (e.g. process killed mid-trade), look it
      // up in the order book so it appears in audit / signal logs. Exit
      // BUYs always fire as fresh MIS MKT orders regardless.
      if (!st.entryBrokerOrderId) {
        if (orderBook == null) {
          try {
            const ob = await brokerService.fetchOrderBook(userId);
            orderBook = Array.isArray(ob?.data) ? ob.data : [];
          } catch (err) {
            log.warn('premiumTrigger: fetchOrderBook failed during reconcile',
              { err: err.message });
            orderBook = [];
          }
        }
        const candidates = orderBook.filter(o =>
          o && o.trdSym === st.symbol
            && o.trnsTp === 'S'
            && o.prod === 'MIS'
            && (!o.ordSt || liveOrdSt.has(String(o.ordSt).toLowerCase())));
        if (candidates.length) {
          candidates.sort((a, b) => {
            const at = a.ordDtTm || a.flDtTm || a.nOrdNo || '';
            const bt = b.ordDtTm || b.flDtTm || b.nOrdNo || '';
            return String(bt).localeCompare(String(at));
          });
          st.entryBrokerOrderId = String(candidates[0].nOrdNo);
          log.info('premiumTrigger: recovered entry order id from order book',
            { side, symbol: st.symbol, brokerOrderId: st.entryBrokerOrderId });
        } else {
          log.warn('premiumTrigger: entry order id not recoverable; exits will still fire as MIS BUY',
            { side, symbol: st.symbol });
        }
      }
      // Bracket mode: rehydrate any open SL-M / LIMIT children from the
      // broker's order book so the poll loop picks them up.
      if (cfg.useBrokerBracketExits && cfg.mode === 'live') {
        try {
          await bracketManager.recoverFromOrderBook(userId, side, {
            symbol: st.symbol,
            token: st.token,
            segment: st.exchangeSegment,
            qty: cfg.qty * (st.lotSize || 1),
            entryPrice: st.entryPrice,
            target: cfg.legs[side].target,
            stoploss: cfg.legs[side].stoploss,
            entryOrderId: st.entryBrokerOrderId,
          });
        } catch (err) {
          log.warn('premiumTrigger: bracket recover failed', { side, err: err.message });
        }
      }
    }
    await persistState();
  }

  // Called by routes/broker.js POST /logout when the user clears their
  // broker session. Stops the strategy (idempotent) and invalidates the
  // resolved leg tokens — they belonged to the previous session and
  // are not portable. Persisted P&L counters and any open SHORT
  // positions on the broker side are preserved in the snapshot so the
  // operator can decide what to do after re-login.
  async function onBrokerSessionEnd() {
    const wasRunning = running;
    if (running) {
      try { await stop(); } catch (_) {}
    }
    for (const side of ['ce', 'pe']) {
      const st = legState[side];
      st.token = null;
      st.symbol = null;
      st.exchangeSegment = null;
      st.ltp = null;
      st.armed = false;
      st.triggerPrice = null;
      st.outsideWindowSince = null;
      st.restrikeInProgress = false;
      // Entry order id from the previous broker session is just audit
      // metadata now — exits always issue a fresh MIS BUY MKT regardless.
      st.entryBrokerOrderId = null;
    }
    await persistState();
    return { stopped: wasRunning };
  }

  // Tokens (canonical "segment|symbol" form) the user's marketData WS
  // needs to keep subscribed for this strategy to keep getting LTPs.
  // Used by marketData on reconnect so a ticker drop does not silently
  // leave legs deaf. Also handy for ad-hoc diagnostics from the dashboard.
  function activeLegTokens() {
    if (!running) return [];
    const items = [];
    for (const side of ['ce', 'pe']) {
      const st = legState[side];
      if (st.token && st.exchangeSegment) {
        items.push({ token: st.token, segment: st.exchangeSegment, key: `${st.exchangeSegment}|${st.token}` });
      }
    }
    const ref = SPOT_REF[cfg.underlyingName];
    if (ref) items.push({ token: ref.symbol, segment: ref.segment, key: `${ref.segment}|${ref.symbol}` });
    return items;
  }

  // Backtest harness API: drive the strategy directly off historical
  // ticks without touching the live ticker, broker, or DB writes.
  const _testing = {
    setCfg(patch) {
      cfg = merge(cfg, patch || {});
      for (const side of ['ce', 'pe']) {
        const leg = cfg.legs[side];
        leg.entryTrigger = numOrNull(leg.entryTrigger);
        leg.target = numOrNull(leg.target);
        leg.stoploss = numOrNull(leg.stoploss);
        leg.reentryOffset = numOrNull(leg.reentryOffset);
        leg.reentryTarget = numOrNull(leg.reentryTarget);
        leg.maxEntryDeviation = numOrNull(leg.maxEntryDeviation);
        leg.rearmRange = numOrNull(leg.rearmRange);
        leg.rearmLower = numOrNull(leg.rearmLower);
        leg.rearmUpper = numOrNull(leg.rearmUpper);
        leg.captureFadeThreshold = numOrNull(leg.captureFadeThreshold);
        leg.enabled = !!leg.enabled;
      }
      cfg.maxDailyLoss = numOrNull(cfg.maxDailyLoss);
      return cfg;
    },
    setLeg(side, meta) {
      const st = legState[side];
      if (!st) throw new Error('unknown side: ' + side);
      st.token = meta.token != null ? String(meta.token) : null;
      st.symbol = meta.symbol != null ? String(meta.symbol) : null;
      st.exchangeSegment = String(meta.exchangeSegment || meta.exchange || 'nse_fo');
      st.lotSize = Number(meta.lotSize || 1);
      st.strike = meta.strike != null ? Number(meta.strike) : null;
      st.expiry = meta.expiry || null;
      if (st.position === 'NONE') st.armed = true;
    },
    feedTick(tick) { onTick(tick); },
    attach() { running = true; },
    detach() { running = false; },
    resetState() {
      for (const side of ['ce', 'pe']) {
        const empty = emptyLeg();
        const persist = ['token', 'symbol', 'exchangeSegment', 'lotSize', 'strike', 'expiry'];
        const saved = {};
        for (const k of persist) saved[k] = legState[side][k];
        Object.assign(legState[side], empty, saved);
      }
      peakRealized = 0;
      maxDrawdown = 0;
      dayLossLocked = false;
      lastError = null;
      signalHistory.length = 0;
    },
    snapshot() { return deepClone({ legState, peakRealized, maxDrawdown, dayLossLocked }); },
  };

  return {
    userId,
    DEFAULTS,
    load, save, get, status,
    start, stop, toggle, exitAll, restrikeLeg,
    activeLegTokens, onBrokerSessionEnd,
    persistState,
    _testing,
  };
}

module.exports = { createPremiumTrigger, DEFAULTS, SPOT_REF };
