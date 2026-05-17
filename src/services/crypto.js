// AES-256-GCM token encryption for broker secrets at rest.
const crypto = require('crypto');
const config = require('../config');

const KEY = Buffer.from(config.tokenEncKey, 'hex');
if (KEY.length !== 32) {
  throw new Error('TOKEN_ENC_KEY must be 64 hex chars (32 bytes) for AES-256');
}

function encrypt(plain) {
  if (plain == null) return null;
  const iv = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv('aes-256-gcm', KEY, iv);
  const ct = Buffer.concat([cipher.update(String(plain), 'utf8'), cipher.final()]);
  const tag = cipher.getAuthTag();
  return Buffer.concat([iv, tag, ct]).toString('base64');
}

function decrypt(blob) {
  if (!blob) return null;
  const buf = Buffer.from(blob, 'base64');
  const iv = buf.subarray(0, 12);
  const tag = buf.subarray(12, 28);
  const ct = buf.subarray(28);
  const decipher = crypto.createDecipheriv('aes-256-gcm', KEY, iv);
  decipher.setAuthTag(tag);
  const pt = Buffer.concat([decipher.update(ct), decipher.final()]);
  return pt.toString('utf8');
}

function sha256(s) {
  return crypto.createHash('sha256').update(String(s)).digest('hex');
}

module.exports = { encrypt, decrypt, sha256 };
