-- Kotak Neo Trading SaaS - MySQL InnoDB schema
-- Targets: 5000 concurrent users, no Redis, MySQL-backed job queue

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ---------------------------------------------------------------------------
-- Users
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS users (
  id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  email           VARCHAR(191) NOT NULL,
  password_hash   VARCHAR(255) NOT NULL,
  full_name       VARCHAR(150) DEFAULT NULL,
  status          ENUM('ACTIVE','DISABLED') NOT NULL DEFAULT 'ACTIVE',
  created_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uk_users_email (email)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci ROW_FORMAT=DYNAMIC;

-- ---------------------------------------------------------------------------
-- Refresh tokens
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS refresh_tokens (
  id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  user_id      BIGINT UNSIGNED NOT NULL,
  token_hash   CHAR(64) NOT NULL,
  expires_at   DATETIME NOT NULL,
  revoked      TINYINT(1) NOT NULL DEFAULT 0,
  created_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uk_refresh_token_hash (token_hash),
  KEY idx_refresh_user (user_id, revoked),
  CONSTRAINT fk_refresh_user FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC;

-- ---------------------------------------------------------------------------
-- Broker accounts (Kotak Neo) — tokens stored AES-256 encrypted
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS broker_accounts (
  id                 BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  user_id            BIGINT UNSIGNED NOT NULL,
  broker             VARCHAR(32) NOT NULL DEFAULT 'KOTAK_NEO',
  ucc                VARCHAR(64) DEFAULT NULL,
  mobile             VARCHAR(32) DEFAULT NULL,
  view_token_enc     TEXT DEFAULT NULL,
  session_token_enc  TEXT DEFAULT NULL,
  sid_enc            TEXT DEFAULT NULL,
  base_url           VARCHAR(255) DEFAULT NULL,
  data_center        VARCHAR(32) DEFAULT NULL,
  hsServerId         VARCHAR(64) DEFAULT NULL,
  token_expires_at   DATETIME DEFAULT NULL,
  status             ENUM('CONNECTED','DISCONNECTED','EXPIRED') NOT NULL DEFAULT 'DISCONNECTED',
  last_login_at      DATETIME DEFAULT NULL,
  created_at         DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at         DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uk_broker_user (user_id, broker),
  KEY idx_broker_status (status),
  CONSTRAINT fk_broker_user FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC;

-- ---------------------------------------------------------------------------
-- Strategies
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS strategies (
  id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  user_id      BIGINT UNSIGNED NOT NULL,
  name         VARCHAR(150) NOT NULL,
  symbol       VARCHAR(64) NOT NULL,
  exchange     VARCHAR(16) NOT NULL DEFAULT 'NSE',
  rules_json   LONGTEXT NOT NULL,
  is_active    TINYINT(1) NOT NULL DEFAULT 1,
  last_run_at  DATETIME DEFAULT NULL,
  created_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_strategy_user (user_id, is_active),
  KEY idx_strategy_symbol (symbol, is_active),
  CONSTRAINT fk_strategy_user FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC;

-- ---------------------------------------------------------------------------
-- Orders (mirror of broker order book)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS orders (
  id                BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  user_id           BIGINT UNSIGNED NOT NULL,
  strategy_id       BIGINT UNSIGNED DEFAULT NULL,
  broker_order_id   VARCHAR(64) DEFAULT NULL,
  symbol            VARCHAR(64) NOT NULL,
  exchange          VARCHAR(16) NOT NULL,
  side              ENUM('BUY','SELL') NOT NULL,
  qty               INT NOT NULL,
  filled_qty        INT NOT NULL DEFAULT 0,
  product           VARCHAR(16) NOT NULL DEFAULT 'CNC',
  order_type        VARCHAR(16) NOT NULL DEFAULT 'MKT',
  price             DECIMAL(12,2) DEFAULT NULL,
  trigger_price     DECIMAL(12,2) DEFAULT NULL,
  status            VARCHAR(32) NOT NULL DEFAULT 'PENDING',
  reject_reason     VARCHAR(255) DEFAULT NULL,
  raw_response      LONGTEXT DEFAULT NULL,
  placed_at         DATETIME DEFAULT NULL,
  created_at        DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at        DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uk_orders_broker (user_id, broker_order_id),
  KEY idx_orders_user_status (user_id, status, created_at),
  KEY idx_orders_user_updated (user_id, updated_at),
  KEY idx_orders_strategy (strategy_id),
  CONSTRAINT fk_orders_user FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC;

-- ---------------------------------------------------------------------------
-- Trades (executed fills)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trades (
  id                BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  user_id           BIGINT UNSIGNED NOT NULL,
  order_id          BIGINT UNSIGNED DEFAULT NULL,
  broker_trade_id   VARCHAR(64) DEFAULT NULL,
  broker_order_id   VARCHAR(64) DEFAULT NULL,
  symbol            VARCHAR(64) NOT NULL,
  exchange          VARCHAR(16) NOT NULL,
  side              ENUM('BUY','SELL') NOT NULL,
  qty               INT NOT NULL,
  price             DECIMAL(12,2) NOT NULL,
  trade_time        DATETIME DEFAULT NULL,
  raw_response      LONGTEXT DEFAULT NULL,
  created_at        DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uk_trades_broker (user_id, broker_trade_id),
  KEY idx_trades_user (user_id, created_at),
  KEY idx_trades_order (order_id),
  CONSTRAINT fk_trades_user FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC;

-- ---------------------------------------------------------------------------
-- Baskets
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS baskets (
  id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  user_id     BIGINT UNSIGNED NOT NULL,
  name        VARCHAR(150) NOT NULL,
  status      ENUM('DRAFT','EXECUTING','COMPLETED','FAILED') NOT NULL DEFAULT 'DRAFT',
  legs_json   LONGTEXT NOT NULL,
  created_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_basket_user (user_id, status),
  CONSTRAINT fk_basket_user FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC;

-- ---------------------------------------------------------------------------
-- Job queue (the heart of the system — replaces Redis/BullMQ)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS job_queue (
  id            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  user_id       BIGINT UNSIGNED DEFAULT NULL,
  job_type      ENUM('TRADE','STRATEGY','MARGIN','BASKET','SYNC_ORDERS','SYNC_TRADES','RELOGIN') NOT NULL,
  priority      TINYINT NOT NULL DEFAULT 5,
  payload       LONGTEXT NOT NULL,
  status        ENUM('PENDING','PROCESSING','SUCCESS','FAILED','DEAD') NOT NULL DEFAULT 'PENDING',
  retry_count   INT NOT NULL DEFAULT 0,
  max_retries   INT NOT NULL DEFAULT 3,
  next_run_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  locked_by     VARCHAR(64) DEFAULT NULL,
  locked_at     DATETIME DEFAULT NULL,
  last_error    TEXT DEFAULT NULL,
  result_json   LONGTEXT DEFAULT NULL,
  created_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_job_pickup (status, next_run_at, priority),
  KEY idx_job_locked (locked_by, locked_at),
  KEY idx_job_user (user_id, status, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC;

-- ---------------------------------------------------------------------------
-- Audit / event log
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS event_log (
  id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  user_id     BIGINT UNSIGNED DEFAULT NULL,
  event_type  VARCHAR(64) NOT NULL,
  level       ENUM('INFO','WARN','ERROR') NOT NULL DEFAULT 'INFO',
  message     VARCHAR(500) DEFAULT NULL,
  meta        LONGTEXT DEFAULT NULL,
  created_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_event_user_time (user_id, created_at),
  KEY idx_event_type (event_type, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC;

-- ---------------------------------------------------------------------------
-- Per-user push cursor (for orderPusher across app restarts)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS user_push_cursor (
  user_id     BIGINT UNSIGNED NOT NULL,
  orders_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  trades_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (user_id),
  CONSTRAINT fk_pushcur_user FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC;

-- ---------------------------------------------------------------------------
-- Worker heartbeat
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS worker_heartbeat (
  worker_id  VARCHAR(96) NOT NULL,
  last_seen  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  started_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  meta       LONGTEXT DEFAULT NULL,
  PRIMARY KEY (worker_id),
  KEY idx_heartbeat_seen (last_seen)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC;

-- ---------------------------------------------------------------------------
-- Instruments (scrip master) — only major-index F&O contracts persisted.
-- Sourced from Kotak Neo /script-details/1.0/masterscrip/file-paths CSVs.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS instruments (
  token             VARCHAR(32)  NOT NULL,
  exchange_segment  VARCHAR(16)  NOT NULL,         -- nse_fo, bse_fo, nse_cm, ...
  instrument_type   VARCHAR(16)  NOT NULL,         -- OPTIDX, FUTIDX, FUTSTK, ...
  underlying        VARCHAR(32)  NOT NULL,         -- NIFTY, BANKNIFTY, ...
  trading_symbol    VARCHAR(96)  NOT NULL,         -- NIFTY24DEC25C25000
  option_type       ENUM('CE','PE','XX','FUT') NOT NULL DEFAULT 'XX',
  strike            DECIMAL(14,4) DEFAULT NULL,
  expiry_date       DATE         DEFAULT NULL,
  lot_size          INT          NOT NULL DEFAULT 1,
  tick_size         DECIMAL(10,4) DEFAULT NULL,
  updated_at        DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (token, exchange_segment),
  KEY idx_inst_chain (underlying, expiry_date, strike, option_type),
  KEY idx_inst_underlying (underlying, instrument_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC;

-- ---------------------------------------------------------------------------
-- Premium-trigger strategy: per-user config, runtime state, and signal log.
-- One row per user — each user runs at most one premium-trigger instance.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS premium_trigger_config (
  user_id     BIGINT UNSIGNED NOT NULL,
  config      LONGTEXT         NOT NULL,
  state       LONGTEXT         DEFAULT NULL,         -- snapshot of legState + drawdown for crash recovery
  enabled     TINYINT(1)   NOT NULL DEFAULT 0,
  running     TINYINT(1)   NOT NULL DEFAULT 0,
  last_error  VARCHAR(500) DEFAULT NULL,
  updated_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (user_id),
  CONSTRAINT fk_pt_user FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC;

CREATE TABLE IF NOT EXISTS premium_trigger_signals (
  id              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  user_id         BIGINT UNSIGNED NOT NULL,
  side            ENUM('ce','pe') NOT NULL,
  action          ENUM('BUY','SELL') NOT NULL,
  reason          VARCHAR(64)  NOT NULL,         -- entry / reentry / target / stoploss / manual
  price           DECIMAL(12,2) NOT NULL,
  qty             INT          NOT NULL,
  symbol          VARCHAR(96)  NOT NULL,
  strike          DECIMAL(14,4) DEFAULT NULL,
  mode            ENUM('live','paper') NOT NULL DEFAULT 'live',
  status          VARCHAR(32)  NOT NULL DEFAULT 'pending',
  broker_order_id VARCHAR(64)  DEFAULT NULL,
  reject_reason   VARCHAR(255) DEFAULT NULL,
  created_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_pt_signals_user (user_id, created_at),
  CONSTRAINT fk_pt_sig_user FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC;

-- ---------------------------------------------------------------------------
-- Backtesting: tick recordings + per-session leg snapshot + saved runs.
-- ---------------------------------------------------------------------------

-- Per-user, per-token tick recording. Sampled at 1 Hz per token to keep
-- the table bounded (a 6.25h session ≈ 22 500 rows × CE/PE/spot tokens).
CREATE TABLE IF NOT EXISTS tick_history (
  id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  user_id     BIGINT UNSIGNED NOT NULL,
  trade_date  DATE NOT NULL,
  token       VARCHAR(64) NOT NULL,
  segment     VARCHAR(16) NOT NULL,
  ltp         DECIMAL(12,2) NOT NULL,
  ts_ms       BIGINT NOT NULL,
  PRIMARY KEY (id),
  KEY idx_replay (user_id, trade_date, token, ts_ms),
  KEY idx_purge  (trade_date),
  CONSTRAINT fk_th_user FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC;

-- Snapshot of the resolved CE/PE/spot legs at recording time so a
-- backtest can reconstruct the exact tokens that were active that
-- day, independent of today's scrip master rollovers.
CREATE TABLE IF NOT EXISTS premium_trigger_session (
  user_id      BIGINT UNSIGNED NOT NULL,
  trade_date   DATE NOT NULL,
  cfg_snapshot LONGTEXT NOT NULL,
  legs         LONGTEXT NOT NULL,
  started_at   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (user_id, trade_date),
  CONSTRAINT fk_pts_user FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC;

CREATE TABLE IF NOT EXISTS backtest_runs (
  id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  user_id     BIGINT UNSIGNED NOT NULL,
  trade_date  DATE NOT NULL,
  cfg         LONGTEXT NOT NULL,
  result      LONGTEXT NOT NULL,
  duration_ms INT  NOT NULL DEFAULT 0,
  created_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_runs_user (user_id, created_at),
  CONSTRAINT fk_br_user FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC;

-- One row per bracket (entry + SL-M child + LIMIT child). Tracks the
-- broker-side OCO lifecycle when premiumTrigger.useBrokerBracketExits
-- is enabled. premium_trigger_signals still records the raw entry/exit
-- decisions; this table tracks the resting child orders and final fate.
CREATE TABLE IF NOT EXISTS premium_trigger_brackets (
  id                BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  user_id           BIGINT UNSIGNED NOT NULL,
  side              ENUM('ce','pe') NOT NULL,
  symbol            VARCHAR(96) NOT NULL,
  qty               INT NOT NULL,
  entry_price       DECIMAL(12,2) NOT NULL,
  target_price      DECIMAL(12,2) NOT NULL,
  sl_trigger_price  DECIMAL(12,2) NOT NULL,
  entry_order_id    VARCHAR(64),
  sl_order_id       VARCHAR(64),
  target_order_id   VARCHAR(64),
  status            ENUM('placing','open','sl_filled','target_filled',
                         'cancelled','safety_flattened','error') NOT NULL,
  closed_reason     VARCHAR(64),
  exit_price        DECIMAL(12,2),
  mode              ENUM('live','paper') NOT NULL,
  opened_at         DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  closed_at         DATETIME,
  PRIMARY KEY (id),
  KEY idx_brk_user_side (user_id, side, status),
  CONSTRAINT fk_brk_user FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci ROW_FORMAT=DYNAMIC;

SET FOREIGN_KEY_CHECKS = 1;
