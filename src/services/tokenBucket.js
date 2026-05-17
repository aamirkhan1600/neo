// Per-user token bucket for the broker's 10 req/sec limit.
// In-process and per-worker — when scaling workers horizontally, set
// BROKER_RPS to (totalRps / numWorkers) so the global cap is respected.

const config = require('../config');

const buckets = new Map(); // key: userId -> { tokens, lastRefill, capacity }

function getBucket(userId) {
  let b = buckets.get(userId);
  if (!b) {
    b = { tokens: config.brokerRps, lastRefill: Date.now(), capacity: config.brokerRps };
    buckets.set(userId, b);
  }
  return b;
}

function refill(b) {
  const now = Date.now();
  const elapsed = (now - b.lastRefill) / 1000;
  if (elapsed > 0) {
    b.tokens = Math.min(b.capacity, b.tokens + elapsed * b.capacity);
    b.lastRefill = now;
  }
}

// Returns true if a token was consumed; false otherwise.
function tryConsume(userId, n = 1) {
  const b = getBucket(userId);
  refill(b);
  if (b.tokens >= n) {
    b.tokens -= n;
    return true;
  }
  return false;
}

// How many ms until `n` tokens will be available again.
function waitMs(userId, n = 1) {
  const b = getBucket(userId);
  refill(b);
  if (b.tokens >= n) return 0;
  const need = n - b.tokens;
  return Math.ceil((need / b.capacity) * 1000);
}

function reset(userId) {
  buckets.delete(userId);
}

module.exports = { tryConsume, waitMs, reset };
