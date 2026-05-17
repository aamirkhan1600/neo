const bcrypt = require('bcrypt');
const jwt = require('jsonwebtoken');
const crypto = require('crypto');
const { query } = require('../db/pool');
const config = require('../config');
const { sha256 } = require('./crypto');

const BCRYPT_ROUNDS = 10;

async function register({ email, password, fullName }) {
  const existing = await query('SELECT id FROM users WHERE email = ?', [email]);
  if (existing.length) {
    const err = new Error('Email already registered');
    err.status = 409;
    throw err;
  }
  const hash = await bcrypt.hash(password, BCRYPT_ROUNDS);
  const r = await query(
    'INSERT INTO users (email, password_hash, full_name) VALUES (?, ?, ?)',
    [email, hash, fullName || null],
  );
  return { id: r.insertId, email, fullName };
}

async function login({ email, password }) {
  const rows = await query('SELECT id, email, password_hash, status FROM users WHERE email = ?', [email]);
  if (!rows.length) {
    const err = new Error('Invalid credentials'); err.status = 401; throw err;
  }
  const user = rows[0];
  if (user.status !== 'ACTIVE') { const err = new Error('Account disabled'); err.status = 403; throw err; }
  const ok = await bcrypt.compare(password, user.password_hash);
  if (!ok) { const err = new Error('Invalid credentials'); err.status = 401; throw err; }
  return issueTokens(user);
}

async function issueTokens(user) {
  const accessToken = jwt.sign(
    { sub: user.id, email: user.email },
    config.jwt.secret,
    { expiresIn: config.jwt.expiresIn },
  );
  const refresh = crypto.randomBytes(48).toString('hex');
  const days = parseInt(config.jwt.refreshExpiresIn, 10) || 7;
  const expires = new Date(Date.now() + days * 24 * 60 * 60 * 1000);
  await query(
    'INSERT INTO refresh_tokens (user_id, token_hash, expires_at) VALUES (?, ?, ?)',
    [user.id, sha256(refresh), expires],
  );
  return { accessToken, refreshToken: refresh, user: { id: user.id, email: user.email } };
}

async function rotateRefresh(refreshToken) {
  const hash = sha256(refreshToken);
  const rows = await query(
    `SELECT rt.*, u.email, u.status FROM refresh_tokens rt
     JOIN users u ON u.id = rt.user_id
     WHERE rt.token_hash = ? AND rt.revoked = 0 AND rt.expires_at > NOW()`,
    [hash],
  );
  if (!rows.length) { const err = new Error('Invalid refresh token'); err.status = 401; throw err; }
  const row = rows[0];
  await query('UPDATE refresh_tokens SET revoked = 1 WHERE id = ?', [row.id]);
  return issueTokens({ id: row.user_id, email: row.email });
}

async function revokeAll(userId) {
  await query('UPDATE refresh_tokens SET revoked = 1 WHERE user_id = ?', [userId]);
}

function verifyAccess(token) {
  return jwt.verify(token, config.jwt.secret);
}

module.exports = { register, login, rotateRefresh, revokeAll, verifyAccess };
