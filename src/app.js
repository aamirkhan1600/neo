// HTTP application entrypoint. Run alongside src/worker.js (one or more
// dedicated worker processes) for full operation.

const path = require('path');
const http = require('http');
const express = require('express');
const helmet = require('helmet');
const compression = require('compression');
const cookieParser = require('cookie-parser');

const config = require('./config');
const logger = require('./utils/logger');
const { apiLimiter } = require('./middleware/rateLimit');
const { notFound, errorHandler } = require('./middleware/errorHandler');

const authRoutes = require('./routes/auth');
const brokerRoutes = require('./routes/broker');
const strategyRoutes = require('./routes/strategies');
const orderRoutes = require('./routes/orders');
const basketRoutes = require('./routes/baskets');
const reportRoutes = require('./routes/reports');
const optionChainRoutes = require('./routes/optionChain');
const premiumTriggerRoutes = require('./routes/premiumTrigger');
const pageRoutes = require('./routes/pages');
const premiumTriggerManager = require('./services/premiumTriggerManager');
const tickRecorder = require('./services/tickRecorder');
const socketServer = require('./ws/socketServer');
const orderPusher = require('./services/orderPusher');
const strategyDaemon = require('./services/strategyDaemon');
const jobQueue = require('./services/jobQueue');

const app = express();
app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, '..', 'views'));
app.disable('x-powered-by');

app.use(helmet({
  contentSecurityPolicy: false, // EJS + inline socket.io client; tighten in prod
}));
app.use(compression());
app.use(express.json({ limit: '256kb' }));
app.use(express.urlencoded({ extended: true, limit: '256kb' }));
app.use(cookieParser());
app.use('/static', express.static(path.join(__dirname, '..', 'public')));

app.get('/health', (_req, res) => res.json({ ok: true, env: config.env }));

app.use('/api', apiLimiter);
app.use('/api/auth', authRoutes);
app.use('/api/broker', brokerRoutes);
app.use('/api/strategies', strategyRoutes);
app.use('/api/orders', orderRoutes);
app.use('/api/baskets', basketRoutes);
app.use('/api/reports', reportRoutes);
app.use('/api/option-chain', optionChainRoutes);
app.use('/api/premium-trigger', premiumTriggerRoutes);

app.use('/', pageRoutes);

app.use(notFound);
app.use(errorHandler);

const server = http.createServer(app);
socketServer.init(server);

// Bridge worker DB writes to live browser clients via socket.io.
orderPusher.start(socketServer);
// Keep market-data WS clients attached for users with active strategies,
// even when no browser tab is open.
strategyDaemon.start();
// Resume premium-trigger strategies for users that had it running.
premiumTriggerManager.start();
// Record live ticks into tick_history for any user with a running
// premium-trigger strategy. Required for the backtest replay path.
tickRecorder.start();

// Periodic job-queue purge — only on the first cluster instance to avoid
// every fork hammering DELETE simultaneously.
const isPrimaryInstance = process.env.NODE_APP_INSTANCE == null
  || process.env.NODE_APP_INSTANCE === '0';
if (isPrimaryInstance) {
  setInterval(() => {
    jobQueue.purge().then((r) => {
      if (r.success || r.dead) logger.info('jobQueue.purge', r);
    }).catch((err) => logger.warn('jobQueue.purge failed', { err: err.message }));
  }, 6 * 60 * 60 * 1000).unref(); // every 6h
}

server.listen(config.port, () => {
  logger.info(`kotak-neo-saas app listening`, { port: config.port, env: config.env });
});

async function shutdown(signal) {
  logger.info(`received ${signal}, shutting down`);
  try { await orderPusher.flush(); } catch {}
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(1), 10000).unref();
}
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

module.exports = app;
