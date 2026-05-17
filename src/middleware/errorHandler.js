const logger = require('../utils/logger');

function notFound(_req, res) {
  res.status(404).json({ error: 'not_found' });
}

function errorHandler(err, req, res, _next) {
  const status = err.status || 500;
  if (status >= 500) {
    logger.error('request error', { message: err.message, stack: err.stack, path: req.path });
  }
  res.status(status).json({
    error: err.code || (status >= 500 ? 'internal_error' : 'request_error'),
    message: err.message,
  });
}

module.exports = { notFound, errorHandler };
