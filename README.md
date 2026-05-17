# Kotak Neo Trading SaaS   

Production-grade auto-trading SaaS platform built on **Node.js + Express + EJS + MySQL** — integrated with the **Kotak Neo** broker APIs. Designed to handle ~5000 concurrent users **without Redis or BullMQ**: the job queue lives in MySQL.

## Highlights

| Module | What it does |
| --- | --- |
| `src/app.js` | Express HTTP + EJS server. Serves UI + REST API. PM2 cluster-ready. |
| `src/worker.js` | Standalone worker process. Polls `job_queue` and dispatches handlers. |
| `src/services/jobQueue.js` | MySQL/MariaDB-backed queue. Atomic claim via `UPDATE ... ORDER BY ... LIMIT` with a per-call token (works on MariaDB < 10.6, no `SKIP LOCKED` needed). Stale-lock reaper, exponential backoff (`5s × retry_count`), DEAD letter status, manual retry. |
| `src/services/tokenBucket.js` | Per-user 10 rps token bucket for the Kotak rate limit. |
| `src/services/brokerClient.js` | Raw Kotak Neo HTTP client — view-token → TOTP login → MPIN/OTP validate → trade APIs. |
| `src/services/brokerService.js` | Adds rate-limiting, transparent re-login on 401, decryption of session tokens. |
| `src/services/marketData.js` | WebSocket bridge to Kotak with auto-reconnect, in-memory price store, 200-symbol cap. |
| `src/services/strategyEngine.js` | JSON rule evaluator (`price > 22000` → enqueue TRADE). Per-strategy cooldown, sandboxed condition compiler. |
| `src/services/orderService.js` | Validates & enqueues trade jobs, mirrors broker responses into `orders`. |
| `src/services/basketService.js` | Multi-leg basket orders, sequential execution, margin-checked. |
| `src/services/reportService.js` | Sync order book / trade book / positions / holdings into MySQL. |
| `src/ws/socketServer.js` | Socket.io server. Authenticates via JWT cookie, forwards ticks to the user's room. |
| `src/services/strategyRunner.js` | Tick → `strategyEngine.evaluateTick` bridge. 30 s strategy cache, per user×symbol throttle. |
| `src/services/orderPusher.js` | Polls `orders` / `trades` for online users every 1 s and pushes via socket.io. Replaces a Redis pub/sub channel. |
| `src/services/strategyDaemon.js` | Every 60 s, ensures users with active strategies + valid broker session have a market-data WS client attached, even when no browser tab is open. **Single-instance** — gated to `NODE_APP_INSTANCE === '0'` to avoid N×WS connections in PM2 cluster mode. |
| `src/services/workerHeartbeat.js` | Worker liveness pings (5 s) into `worker_heartbeat`. Exposed via `GET /api/reports/workers`. |
| `src/services/crypto.js` | AES-256-GCM token-at-rest encryption. |

## Architecture

```
User → EJS UI → Express API
                 │
                 ▼
            MySQL (InnoDB)  ←─────  Worker process(es)  ──► Kotak Neo APIs
                 ▲                              │
                 └──── Socket.io ◄── Market WS ─┘
```

* **App tier**: PM2 cluster mode, 2× Node servers behind NGINX.
* **Worker tier**: Dedicated server, `pm2 start ecosystem.config.js` — multiple worker instances safely share `job_queue` via an atomic `UPDATE ... ORDER BY ... LIMIT` with a per-call claim token (no `SKIP LOCKED` required, so MariaDB < 10.6 is supported).
* **DB tier**: MySQL 8 / MariaDB 10.4+ (InnoDB), connection pool, `(status, next_run_at, priority)` index for queue pickup.

## Quickstart

```bash
cd kotak-neo-saas
cp .env.example .env       # fill in DB creds + 64-hex TOKEN_ENC_KEY + JWT_SECRET + KOTAK_API_TOKEN
npm install
npm run migrate            # creates DB + applies schema.sql (idempotent)

# In two terminals:
npm start                  # HTTP app (port 3000)
npm run worker             # background job processor

# Or with PM2:
npm run pm2:start
```

### Kotak Neo onboarding (one-time)

1. Register TOTP via the **NEO App → API Dashboard → TOTP Registration**, scan the QR with Google / Microsoft Authenticator.
2. Create a Trade API app under **Invest → Trade API → Your Applications** and copy the access token shown — that goes into `KOTAK_API_TOKEN`.
3. Open `http://localhost:3000`, register a user, navigate to `/broker`, and submit:
   - Mobile (with country code, e.g. `+919876543210`)
   - UCC (Client Code)
   - Current 6-digit TOTP from the authenticator app
   - 6-digit MPIN

The backend calls Kotak Neo's `tradeApiLogin` then `tradeApiValidate` (no OAuth2 round-trip), persists only the encrypted Trade token + sid + per-user `baseUrl`, and never stores mobile / UCC / TOTP / MPIN.

`GET /api/broker/diag` reports the configured access-token shape (length, jwt-vs-uuid heuristics) without leaking the value — useful when debugging credentials.

## Endpoints

```
POST /api/auth/register
POST /api/auth/login
POST /api/auth/refresh
POST /api/auth/logout
GET  /api/auth/me

POST /api/broker/login            # Kotak Neo 3-step login
GET  /api/broker/status
POST /api/broker/logout

GET    /api/strategies
POST   /api/strategies            # { name, symbol, exchange, condition, action, qty, ... }
POST   /api/strategies/:id/toggle
DELETE /api/strategies/:id

POST /api/orders                  # { symbol, exchange, side, qty, orderType, product, price?, triggerPrice? }
GET  /api/orders
GET  /api/orders/trades
POST /api/orders/sync             # enqueues SYNC_ORDERS + SYNC_TRADES jobs
POST /api/orders/jobs/:id/retry   # manually retry FAILED/DEAD job

GET  /api/baskets
POST /api/baskets                 # { name, legs: [...] }
POST /api/baskets/:id/execute

GET  /api/reports/orders
GET  /api/reports/trades
GET  /api/reports/positions
GET  /api/reports/holdings
GET  /api/reports/queue           # job-queue status counts
```

## Job Queue Mechanics

| Field | Purpose |
| --- | --- |
| `status` | `PENDING → PROCESSING → SUCCESS / FAILED / DEAD` |
| `priority` | Lower = higher (TRADE=1, BASKET=2, MARGIN=3, SYNC=4, default=5) |
| `next_run_at` | Earliest time eligible for pickup |
| `locked_by`, `locked_at` | Worker ownership; reaped after `JOB_LOCK_TIMEOUT_MS` |
| `retry_count` / `max_retries` | Exponential backoff (5 × retry seconds, capped 120s) |

Workers claim with an atomic `UPDATE` that stamps a unique per-call token,
then read those rows back by the token. InnoDB serializes the contended
row updates so two workers cannot claim the same job — and unlike
`FOR UPDATE SKIP LOCKED`, this works on MariaDB versions older than 10.6.

```sql
UPDATE job_queue
   SET status='PROCESSING', locked_by=:token, locked_at=NOW()
 WHERE status='PENDING' AND next_run_at <= NOW()
 ORDER BY priority, next_run_at, id
 LIMIT N;

SELECT * FROM job_queue WHERE locked_by=:token AND status='PROCESSING';
```

## Worker → Browser flow (no Redis)

```
worker proc                         app proc
   │                                   │
   ├─ writes orders/trades to MySQL ──►│
                                       │  orderPusher polls orders.updated_at
                                       │  every 1 s for online users only
                                       │
                                       └─► socket.io 'order_update' / 'trade'
```

```
marketData WS  ──tick──►  strategyRunner ──►  strategyEngine.evaluateTick
                                                  │
                                                  └──► jobQueue.enqueue('TRADE')
                                                              │
                                                              ▼
                                                          worker proc → Kotak API
```

`strategyDaemon` runs every 60 s on the **first** PM2 cluster instance only and warm-attaches a market-data client for any user that has at least one active strategy and a valid broker session — so strategies fire 24/7 without the user having a tab open. `strategyRunner.attach` also subscribes the WS to every active strategy's instrument token (and re-subscribes on reconnect), so ticks actually flow.

### Operational tables

* `user_push_cursor` — per-user `(orders_at, trades_at)` watermark used by `orderPusher` so updates aren't missed across app restarts.
* `worker_heartbeat` — `(worker_id, last_seen)` written every 5 s; expose via `/api/reports/workers`.
* `job_queue` is auto-purged on the primary cluster instance every 6 h: `SUCCESS` rows older than 7 d, `DEAD` older than 30 d.

## Security

* **Tokens encrypted** at rest with AES-256-GCM (`TOKEN_ENC_KEY` = 32-byte hex).
* **MPIN / TOTP are NEVER stored** — they're forwarded to Kotak once and discarded.
* JWT access tokens (15 min) + rotating refresh tokens (7 days, hashed with SHA-256 in DB).
* Helmet + per-IP rate limiter + per-user broker rate limiter.
* On HTTP 401 from Kotak, the session is wiped and a `RELOGIN` event is logged so the user re-authenticates manually.

## Scale Plan

* App: 2× Node servers, PM2 cluster (`instances: max`), NGINX reverse proxy + HTTPS.
* Worker: 1 dedicated server, 2-N PM2 forks. `BROKER_RPS` should be set to `(perUserCap / numWorkers)` if scaling out.
* DB: MySQL 8, optimized `innodb_buffer_pool_size` (≥ 60% of RAM), `(status, next_run_at, priority)` index already present.
* WebSocket: 1 client per user, max 200 symbols subscribed.

## Targets

| Metric | Target |
| --- | --- |
| Strategy → trade enqueue | < 300 ms |
| Order placement (worker → broker) | < 700 ms |
| WebSocket tick → browser | < 100 ms |
| Concurrent users | 5,000 |

## Future Work

Multi-broker (Zerodha, mstock, Fyers); backtesting engine; RSI/MACD indicators; mobile app via the same REST API.
#   n e o 
 
 