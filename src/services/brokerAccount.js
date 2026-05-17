// Persistence + decryption layer for broker_accounts. Tokens are stored
// AES-256-GCM encrypted and decrypted on read.

const { query } = require('../db/pool');
const { encrypt, decrypt } = require('./crypto');

async function getByUserId(userId) {
  const rows = await query(
    `SELECT * FROM broker_accounts WHERE user_id = ? AND broker = 'KOTAK_NEO' LIMIT 1`,
    [userId],
  );
  if (!rows.length) return null;
  return hydrate(rows[0]);
}

function hydrate(row) {
  return {
    id: row.id,
    user_id: row.user_id,
    broker: row.broker,
    ucc: row.ucc,
    mobile: row.mobile,
    view_token: decrypt(row.view_token_enc),
    session_token: decrypt(row.session_token_enc),
    sid: decrypt(row.sid_enc),
    base_url: row.base_url,
    data_center: row.data_center,
    hsServerId: row.hsServerId,
    token_expires_at: row.token_expires_at,
    status: row.status,
    last_login_at: row.last_login_at,
  };
}

async function upsert(userId, fields) {
  const enc = {
    view_token_enc: fields.view_token != null ? encrypt(fields.view_token) : null,
    session_token_enc: fields.session_token != null ? encrypt(fields.session_token) : null,
    sid_enc: fields.sid != null ? encrypt(fields.sid) : null,
  };
  const exists = await getByUserId(userId);
  if (exists) {
    await query(
      `UPDATE broker_accounts SET
        ucc = COALESCE(?, ucc),
        mobile = COALESCE(?, mobile),
        view_token_enc = COALESCE(?, view_token_enc),
        session_token_enc = COALESCE(?, session_token_enc),
        sid_enc = COALESCE(?, sid_enc),
        base_url = COALESCE(?, base_url),
        data_center = COALESCE(?, data_center),
        hsServerId = COALESCE(?, hsServerId),
        token_expires_at = COALESCE(?, token_expires_at),
        status = COALESCE(?, status),
        last_login_at = COALESCE(?, last_login_at)
       WHERE user_id = ? AND broker = 'KOTAK_NEO'`,
      [
        fields.ucc || null,
        fields.mobile || null,
        enc.view_token_enc,
        enc.session_token_enc,
        enc.sid_enc,
        fields.base_url || null,
        fields.data_center || null,
        fields.hsServerId || null,
        fields.token_expires_at || null,
        fields.status || null,
        fields.last_login_at || null,
        userId,
      ],
    );
  } else {
    await query(
      `INSERT INTO broker_accounts
        (user_id, broker, ucc, mobile, view_token_enc, session_token_enc, sid_enc,
         base_url, data_center, hsServerId, token_expires_at, status, last_login_at)
       VALUES (?, 'KOTAK_NEO', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        userId,
        fields.ucc || null,
        fields.mobile || null,
        enc.view_token_enc,
        enc.session_token_enc,
        enc.sid_enc,
        fields.base_url || null,
        fields.data_center || null,
        fields.hsServerId || null,
        fields.token_expires_at || null,
        fields.status || 'CONNECTED',
        fields.last_login_at || new Date(),
      ],
    );
  }
  return getByUserId(userId);
}

async function markStatus(userId, status) {
  await query(
    `UPDATE broker_accounts SET status = ? WHERE user_id = ? AND broker = 'KOTAK_NEO'`,
    [status, userId],
  );
}

async function clearSession(userId) {
  await query(
    `UPDATE broker_accounts
     SET session_token_enc = NULL, sid_enc = NULL, status = 'EXPIRED'
     WHERE user_id = ? AND broker = 'KOTAK_NEO'`,
    [userId],
  );
}

module.exports = { getByUserId, upsert, markStatus, clearSession };
