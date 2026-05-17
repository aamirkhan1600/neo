// Socket.io server: bridges market-data ticks + job/order updates to the
// browser. Authenticates via the access_token cookie / handshake auth.

const { Server } = require('socket.io');
const { verifyAccess } = require('../services/authService');
const marketData = require('../services/marketData');
const accounts = require('../services/brokerAccount');
const strategyRunner = require('../services/strategyRunner');
const orderPusher = require('../services/orderPusher');
const logger = require('../utils/logger');

let io = null;

function parseCookies(header) {
  if (!header) return {};
  return Object.fromEntries(header.split(';').map(p => {
    const [k, ...v] = p.trim().split('=');
    return [k, decodeURIComponent(v.join('='))];
  }));
}

function bridgeTicksToBrowser(userId, mdc) {
  if (mdc.__browserBridged) return;
  mdc.__browserBridged = true;
  mdc.on('tick', (tick) => {
    if (io) io.to(`user:${userId}`).emit('tick', tick);
  });
}

function init(httpServer) {
  io = new Server(httpServer, { cors: { origin: false } });

  io.use((socket, next) => {
    try {
      const tokenFromAuth = socket.handshake.auth?.token;
      const cookies = parseCookies(socket.handshake.headers.cookie);
      const token = tokenFromAuth || cookies.access_token;
      if (!token) return next(new Error('unauthorized'));
      const payload = verifyAccess(token);
      socket.userId = payload.sub;
      next();
    } catch {
      next(new Error('unauthorized'));
    }
  });

  io.on('connection', async (socket) => {
    socket.join(`user:${socket.userId}`);
    await orderPusher.markOnline(socket.userId);
    logger.info('socket connected', { userId: socket.userId });

    let mdc = marketData.getClient(socket.userId);
    if (!mdc) {
      const acc = await accounts.getByUserId(socket.userId).catch(() => null);
      if (acc?.session_token) mdc = marketData.attachClient(socket.userId, acc);
    }
    if (mdc) {
      bridgeTicksToBrowser(socket.userId, mdc);
      strategyRunner.attach(socket.userId, mdc);
    }

    socket.on('subscribe', (tokens) => {
      try {
        const c = marketData.getClient(socket.userId);
        if (c) c.subscribe(Array.isArray(tokens) ? tokens : [tokens]);
      } catch (err) { socket.emit('error_msg', err.message); }
    });

    socket.on('unsubscribe', (tokens) => {
      const c = marketData.getClient(socket.userId);
      if (c) c.unsubscribe(Array.isArray(tokens) ? tokens : [tokens]);
    });

    socket.on('disconnect', () => {
      orderPusher.markOffline(socket.userId).catch(() => {});
      logger.info('socket disconnected', { userId: socket.userId });
      // Note: we intentionally DO NOT detach the marketData client on
      // disconnect. The strategyDaemon is responsible for keeping clients
      // alive for users with active strategies.
    });
  });

  return io;
}

function emitToUser(userId, event, payload) {
  if (!io) return;
  io.to(`user:${userId}`).emit(event, payload);
}

module.exports = { init, emitToUser, bridgeTicksToBrowser };
