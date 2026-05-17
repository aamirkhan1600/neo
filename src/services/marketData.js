// Market data WebSocket bridge.
//
// Connects to Kotak Neo WS using the user's session token, sid and data
// center. Maintains an in-memory price store keyed by symbol and broadcasts
// ticks to the strategy engine and to socket.io clients. Auto-reconnects on
// failure with exponential backoff.

const WebSocket = require('ws');
const EventEmitter = require('events');
const config = require('../config');
const logger = require('../utils/logger');

class MarketDataClient extends EventEmitter {
  constructor({ userId, sessionToken, sid, dataCenter, hsServerId }) {
    super();
    this.userId = userId;
    this.sessionToken = sessionToken;
    this.sid = sid;
    this.dataCenter = dataCenter;
    this.hsServerId = hsServerId;
    this.ws = null;
    this.subs = new Set();           // canonical "segment|token" keys subscribed
    this.subMeta = new Map();        // key -> { segment, token } for REST polling
    this.prices = new Map();         // symbol -> { ltp, ts }
    this.reconnectAttempts = 0;
    this.maxReconnectDelay = 30000;
    this.closed = false;
    this.url = this._buildUrl();
    // REST poll fallback. Kotak's binary HSM gateway accepts subscribes
    // but on many accounts never streams a single frame. We poll the
    // documented Quotes REST API in parallel so ticks still flow. Set
    // MARKET_DATA_POLL_MS=0 to disable; default 1000ms.
    const envPoll = Number(process.env.MARKET_DATA_POLL_MS);
    this.pollMs = Number.isFinite(envPoll) ? envPoll : 1000;
    this._pollTimer = null;
    this._pollErrors = 0;
    this._pollLastError = null;
    this._pollLastOk = null;
    this._pollCount = 0;
  }

  _buildUrl() {
    // Per Kotak docs the WS endpoint is region-specific. Default to gw1.
    const dc = (this.dataCenter || 'gw1').toLowerCase();
    return `wss://mlhsm.kotaksecurities.com/realtime?sid=${encodeURIComponent(this.sid)}`;
  }

  connect() {
    if (this.closed) return;
    logger.info('marketData: connecting', { userId: this.userId, url: this.url });
    // Per Kotak's Python SDK, the WS handshake takes NO custom HTTP headers
    // for auth — auth is via the ?sid= query string and the post-open `cn`
    // packet. We DO set Origin because some Kotak edges silently refuse to
    // stream data to upgrades that don't look like a browser.
    this.ws = new WebSocket(this.url, {
      handshakeTimeout: 10000,
      origin: 'https://www.kotaksecurities.com',
    });
    this.ws.binaryType = 'nodebuffer';
    this._openedAt = Date.now();

    this.ws.on('open', () => {
      logger.info('marketData: open', { userId: this.userId });
      this.reconnectAttempts = 0;
      this._loggedFirstFrame = false;
      this._frameCount = 0;
      this._connected = false;
      this.emit('open');

      // Step 1 — connect packet. Step 2 — subscribe (immediately; the
      // gateway accepts subscribes right after cn). Step 3 — start a
      // 20s heartbeat thread which Kotak's HSM gateway uses to mark the
      // session as alive. Without it, ticks may never start streaming.
      this._sendConnect();
      if (this.subs.size) this._sendSubscribe([...this.subs]);
      this._startHeartbeat();
    });

    this.ws.on('message', (raw, isBinary) => {
      this._frameCount = (this._frameCount || 0) + 1;
      this._lastFrameAt = Date.now();
      const within30s = Date.now() - this._openedAt < 30_000;
      if (within30s || this._frameCount <= 5) {
        const buf = Buffer.isBuffer(raw) ? raw : Buffer.from(raw);
        logger.info('marketData: ws-frame', {
          userId: this.userId,
          n: this._frameCount,
          isBinary: !!isBinary,
          length: buf.length,
          hex: isBinary ? buf.slice(0, Math.min(48, buf.length)).toString('hex') : undefined,
          textPreview: !isBinary ? buf.toString('utf8').slice(0, 240) : undefined,
        });
      }
      this._onMessage(raw, isBinary);
    });

    this.ws.on('error', (err) => {
      logger.warn('marketData: error', { userId: this.userId, message: err.message });
    });

    this.ws.on('close', (code, reason) => {
      const r = reason ? reason.toString() : null;
      logger.info('marketData: close', { userId: this.userId, code, reason: r });
      if (code === 1006 || code === 1011) {
        logger.warn('marketData: gateway rejected handshake — check session_token / sid', {
          userId: this.userId, code,
        });
      }
      this._stopHeartbeat();
      this.emit('close');
      if (this.closed) return;
      const delay = Math.min(this.maxReconnectDelay, 1000 * 2 ** this.reconnectAttempts++);
      setTimeout(() => this.connect(), delay);
    });
  }

  // Instrumented send so we can confirm in the log that connect / subscribe /
  // heartbeat actually leave the process. Returns true on success.
  _send(label, payload) {
    if (this.ws?.readyState !== WebSocket.OPEN) {
      logger.warn('marketData: send skipped (not open)', { userId: this.userId, label });
      return false;
    }
    try {
      const data = typeof payload === 'string' ? payload : JSON.stringify(payload);
      this.ws.send(data);
      logger.info('marketData: sent', { userId: this.userId, label, bytes: data.length });
      return true;
    } catch (err) {
      logger.warn('marketData: send failed', { userId: this.userId, label, err: err.message });
      return false;
    }
  }

  _sendConnect() {
    // Match HSWebSocket.on_open in the Kotak Neo SDK. The official docs
    // also list dataCenter (e.g. E41, E43) as a required input — include
    // it whenever we have it.
    const packet = {
      Authorization: this.sessionToken,
      Sid: this.sid,
      type: 'cn',
      source: 'API',
    };
    if (this.dataCenter) packet.dataCenter = this.dataCenter;
    this._send('cn', packet);
  }

  _startHeartbeat() {
    this._stopHeartbeat();
    this._hbTimer = setInterval(() => {
      if (this.ws?.readyState !== WebSocket.OPEN) return;
      // Kotak's HSWebSocket sends the literal string "HBChk" every 20s.
      try { this.ws.send('HBChk'); } catch {}
    }, 20_000);
    this._hbTimer.unref?.();
  }

  _stopHeartbeat() {
    if (this._hbTimer) {
      clearInterval(this._hbTimer);
      this._hbTimer = null;
    }
  }

  _onMessage(raw, isBinary) {
    if (isBinary) return this._onBinary(Buffer.isBuffer(raw) ? raw : Buffer.from(raw));

    // Text frames are JSON: connect ack, heartbeats, error notifications.
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }
    const ticks = Array.isArray(msg) ? msg : [msg];
    for (const t of ticks) {
      // Some gateways wrap connect / status messages as JSON too.
      if (t && t.type === 'cn') { this._connected = true; continue; }
      const sym = t.tk || t.tradingSymbol || t.ts || t.symbol;
      const ltpRaw = t.lp ?? t.ltp ?? t.last_price ?? t.last;
      const ltp = ltpRaw != null ? parseFloat(ltpRaw) : NaN;
      if (!sym || !Number.isFinite(ltp)) continue;
      this._emitTick(String(sym), ltp);
    }
  }

  // Kotak Neo HSM sends quote ticks as packed binary frames. Format
  // (best-effort, derived from public Kotak SDKs):
  //
  //   byte 0          : number of packets in this frame
  //   for each packet:
  //     bytes [0..1]  : packet length (uint16 BE)
  //     bytes [2..]   : packet body — first byte is packet type
  //                     followed by big-endian fields.
  //
  // We only parse the LTP field for now (that's what the option chain
  // shows). Token is at body offset 1 (uint32 BE), LTP at offset 5
  // (int32 BE, in paise — divide by 100 for rupees).
  _onBinary(buf) {
    if (!buf || buf.length < 1) return;
    const numPackets = buf[0];
    let offset = 1;
    let parsed = 0;
    for (let i = 0; i < numPackets && offset < buf.length; i++) {
      if (offset + 2 > buf.length) break;
      const plen = buf.readUInt16BE(offset);
      offset += 2;
      if (offset + plen > buf.length || plen < 9) { offset += plen; continue; }
      const body = buf.slice(offset, offset + plen);
      offset += plen;

      // body[0] = packet type. Quote-style packets carry token+ltp at the
      // expected offsets; depth packets and others may not. We just check
      // if the decoded values are sane and emit when they are.
      try {
        const token = body.readUInt32BE(1);
        const ltpPaise = body.readInt32BE(5);
        const ltp = ltpPaise / 100;
        if (token > 0 && Number.isFinite(ltp) && ltp > 0 && ltp < 10_000_000) {
          this._emitTick(String(token), ltp);
          parsed++;
        }
      } catch { /* malformed packet — skip */ }
    }
    if (!parsed && !this._warnedNoParse) {
      this._warnedNoParse = true;
      logger.warn('marketData: binary frame produced no ticks — protocol may differ', {
        userId: this.userId,
        firstByte: buf[0], length: buf.length,
        hex: buf.slice(0, Math.min(32, buf.length)).toString('hex'),
      });
    }
  }

  _emitTick(symbol, ltp) {
    const ts = Date.now();
    this.prices.set(symbol, { ltp, ts });
    this._tickCount = (this._tickCount || 0) + 1;
    this._lastTickAt = ts;
    this.emit('tick', { symbol, ltp, ts });
  }

  // Kotak Neo's WebSocket expects subscriptions in `segment|token` form,
  // ampersand-separated (e.g. "nse_fo|45876&nse_cm|11536"). Bare tokens
  // are silently ignored, which is why the previous version produced no
  // ticks for option chain rows. We accept three caller formats:
  //   'nse_fo|45876'                                 (canonical key)
  //   '45876'                                        (legacy — assumes nse_cm)
  //   { token: '45876', segment: 'nse_fo' }          (structured)
  _normalize(item) {
    if (item == null) return null;
    if (typeof item === 'object') {
      const t = String(item.token || '').trim();
      const s = String(item.segment || item.exchangeSegment || 'nse_cm').trim().toLowerCase();
      return t ? `${s}|${t}` : null;
    }
    const s = String(item).trim();
    if (!s) return null;
    return s.includes('|') ? s.toLowerCase() : `nse_cm|${s}`;
  }

  _sendSubscribe(keys) {
    this._send('subscribe', { type: 'mws', scrips: keys.join('&'), channelnum: '1', task: 'mws' });
  }

  subscribe(items) {
    const list = Array.isArray(items) ? items : [items];
    const keys = [];
    for (const item of list) {
      const key = this._normalize(item);
      if (!key) continue;
      keys.push(key);
      // Cache segment+token per key so the REST poller knows what to fetch.
      const [segment, token] = key.split('|');
      if (!this.subMeta.has(key)) this.subMeta.set(key, { segment, token });
    }
    const next = new Set(this.subs);
    for (const k of keys) next.add(k);
    if (next.size > config.ws.maxSymbols) {
      throw new Error(`subscribe limit exceeded: ${next.size} > ${config.ws.maxSymbols}`);
    }
    const added = [...next].filter(k => !this.subs.has(k));
    this.subs = next;
    if (added.length) this._sendSubscribe(added);
    if (this.subs.size > 0 && this.pollMs > 0) this._startRestPoll();
  }

  unsubscribe(items) {
    const keys = (Array.isArray(items) ? items : [items])
      .map(i => this._normalize(i))
      .filter(Boolean);
    for (const k of keys) { this.subs.delete(k); this.subMeta.delete(k); }
    if (this.ws?.readyState === WebSocket.OPEN && keys.length) {
      this.ws.send(JSON.stringify({ type: 'mws', scrips: keys.join('&'), channelnum: '1', task: 'mwu' }));
    }
    if (this.subs.size === 0) this._stopRestPoll();
  }

  // REST poll loop — runs in parallel with the WS. Idempotent.
  _startRestPoll() {
    if (this._pollTimer || this.closed || this.pollMs <= 0) return;
    const tick = () => { this._pollOnce().catch(() => {}); };
    this._pollTimer = setInterval(tick, this.pollMs);
    this._pollTimer.unref?.();
    tick(); // fire immediately so the dashboard isn't stuck on "no LTP yet"
  }

  _stopRestPoll() {
    if (this._pollTimer) { clearInterval(this._pollTimer); this._pollTimer = null; }
  }

  async _pollOnce() {
    if (this.closed || this.subMeta.size === 0) return;
    // Lazy require to avoid a startup cycle (brokerService doesn't depend
    // on marketData, but the module-graph is happier loaded on demand).
    let brokerService;
    try { brokerService = require('./brokerService'); }
    catch (err) { this._pollLastError = `brokerService load failed: ${err.message}`; return; }
    // Build queries in deterministic order so the response can be matched
    // back positionally if Kotak ever omits the token field.
    const subs = [...this.subMeta.values()];
    const queries = subs.map(m => ({ exchangeSegment: m.segment, symbol: m.token }));
    if (!queries.length) return;
    // `filter='ltp'` is the only filter the OTM scanner uses successfully
    // against this account (premiumTrigger.js:554). An earlier attempt to
    // use 'quote' here regressed — Kotak's response either returned rows
    // that didn't carry our subscribed token in any echoed field, or
    // dropped the request entirely; downstream the legs froze and the
    // age annotation pegged at red. Stay on 'ltp' until we have account
    // entitlements confirmed for richer filters.
    try {
      const data = await brokerService.fetchQuotesAuto(this.userId, queries, 'ltp');
      const rows = Array.isArray(data) ? data : (data?.data || []);
      // Index by every plausible identifier the row exposes so we can match
      // it back to one of our subscribed tokens.
      let emitted = 0;
      for (let i = 0; i < rows.length; i++) {
        const row = rows[i];
        const ltp = parseFloat(
          row.ltp ?? row.LTP ?? row.lp ?? row.last_price ?? row.last_traded_price
        );
        if (!Number.isFinite(ltp) || ltp <= 0) continue;
        const ids = [
          row.exchange_token, row.exchangeToken, row.token,
          row.symbol, row.tradingSymbol, row.tSym, row.tk,
        ].filter(v => v != null).map(String);
        // Find the subscribed meta whose token appears anywhere in the row's
        // identifier set. Same trick optionChain.js uses.
        let meta = null;
        for (const id of ids) {
          meta = subs.find(m => m.token === id);
          if (meta) break;
        }
        // Last resort: match positionally if Kotak preserved request order.
        if (!meta && rows.length === subs.length) meta = subs[i];
        if (!meta) continue;
        this._emitTick(meta.token, ltp);
        emitted++;
      }
      this._pollLastOk = Date.now();
      this._pollCount = (this._pollCount || 0) + 1;
      this._pollLastEmitted = emitted;
      if (this._pollErrors > 0) {
        logger.info('marketData: REST poll recovered', { userId: this.userId, emitted });
        this._pollErrors = 0;
        this._pollLastError = null;
      }
    } catch (err) {
      this._pollErrors = (this._pollErrors || 0) + 1;
      this._pollLastError = err.message;
      // Log first error and every 20th to avoid spam.
      if (this._pollErrors === 1 || this._pollErrors % 20 === 0) {
        logger.warn('marketData: REST poll failed', {
          userId: this.userId, err: err.message, attempt: this._pollErrors,
        });
      }
    }
  }

  getPrice(symbol) { return this.prices.get(symbol); }

  // Live diagnostic snapshot — exposed via /api/premium-trigger/status so
  // operators can see if the ticker is connected, if frames are arriving,
  // and if any subscription has been registered. Helps distinguish
  // "WS not open" from "open but no ticks for our subs" from "open but
  // binary parser produced 0 ticks".
  diag() {
    const READY = ['CONNECTING', 'OPEN', 'CLOSING', 'CLOSED'];
    return {
      url: this.url,
      readyState: this.ws ? this.ws.readyState : null,
      readyStateLabel: this.ws ? READY[this.ws.readyState] : 'NO_WS',
      connected: this._connected === true && this.ws && this.ws.readyState === WebSocket.OPEN,
      framesReceived: this._frameCount || 0,
      ticksEmitted: this._tickCount || 0,
      lastFrameAt: this._lastFrameAt || null,
      lastTickAt: this._lastTickAt || null,
      subsCount: this.subs.size,
      subs: [...this.subs],
      pricesCount: this.prices.size,
      binaryParserFailed: !!this._warnedNoParse,
      reconnectAttempts: this.reconnectAttempts,
      pollMs: this.pollMs,
      pollActive: !!this._pollTimer,
      pollCount: this._pollCount || 0,
      pollErrors: this._pollErrors || 0,
      pollLastOk: this._pollLastOk || null,
      pollLastError: this._pollLastError || null,
    };
  }

  close() {
    this.closed = true;
    this._stopHeartbeat();
    this._stopRestPoll();
    try { this.ws?.close(); } catch {}
  }
}

// Manager: one client per active user.
const clients = new Map();

function getClient(userId) {
  return clients.get(userId);
}

function attachClient(userId, account) {
  const existing = clients.get(userId);
  if (existing) existing.close();
  const c = new MarketDataClient({
    userId,
    sessionToken: account.session_token,
    sid: account.sid,
    dataCenter: account.data_center,
    hsServerId: account.hsServerId,
  });
  clients.set(userId, c);
  c.connect();
  return c;
}

function detachClient(userId) {
  const c = clients.get(userId);
  if (c) { c.close(); clients.delete(userId); }
}

module.exports = { MarketDataClient, getClient, attachClient, detachClient, clients };
