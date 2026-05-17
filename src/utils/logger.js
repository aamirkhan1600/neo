const path = require('path');
const winston = require('winston');
require('winston-daily-rotate-file');

const logDir = path.join(__dirname, '..', '..', 'logs');

const fmt = winston.format.combine(
  winston.format.timestamp(),
  winston.format.errors({ stack: true }),
  winston.format.splat(),
  winston.format.json(),
);

const logger = winston.createLogger({
  level: process.env.LOG_LEVEL || 'info',
  format: fmt,
  defaultMeta: { service: 'kotak-neo-saas' },
  transports: [
    new winston.transports.DailyRotateFile({
      filename: path.join(logDir, 'app-%DATE%.log'),
      datePattern: 'YYYY-MM-DD',
      maxSize: '20m',
      maxFiles: '14d',
    }),
    new winston.transports.DailyRotateFile({
      filename: path.join(logDir, 'error-%DATE%.log'),
      level: 'error',
      datePattern: 'YYYY-MM-DD',
      maxSize: '20m',
      maxFiles: '30d',
    }),
  ],
});

if (process.env.NODE_ENV !== 'production') {
  logger.add(new winston.transports.Console({
    format: winston.format.combine(
      winston.format.colorize(),
      winston.format.timestamp({ format: 'HH:mm:ss' }),
      winston.format.printf(({ timestamp, level, message, ...rest }) => {
        const meta = Object.keys(rest).filter(k => k !== 'service').length
          ? ' ' + JSON.stringify(rest, (k, v) => k === 'service' ? undefined : v)
          : '';
        return `${timestamp} ${level} ${message}${meta}`;
      }),
    ),
  }));
}

module.exports = logger;
