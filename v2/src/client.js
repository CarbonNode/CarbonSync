'use strict';

const fs = require('node:fs');
const fsp = require('node:fs/promises');
const http = require('node:http');
const https = require('node:https');

const JSON_TIMEOUT_MS = 15_000;
const TRANSFER_TIMEOUT_MS = 10 * 60 * 1000;

class HttpError extends Error {
  constructor(status, message) {
    super(`hub ${status}: ${message}`);
    this.status = status;
  }
}

/**
 * Spoke -> hub HTTP client. Stateless requests over a keep-alive agent: there
 * is no persistent connection object to leak, resurrect, or storm (v1
 * landmines #2/#3/#4 are structural non-issues here).
 *
 * TLS trust = TOFU fingerprint pinning: first contact stores the hub cert's
 * sha256; afterwards any change is a hard failure until the pin is cleared.
 */
class HubClient {
  constructor({ hubUrl, token, pinGet, pinSet, log }) {
    const u = new URL(hubUrl);
    this.tls = u.protocol === 'https:';
    this.host = u.hostname;
    this.port = Number(u.port) || (this.tls ? 443 : 80);
    this.token = token;
    this.log = log;
    const mod = this.tls ? https : http;
    this.mod = mod;
    this.agent = new mod.Agent({
      keepAlive: true,
      maxSockets: 8,
      ...(this.tls
        ? {
            rejectUnauthorized: false, // we pin instead of using the CA store
            checkServerIdentity: (_host, cert) => {
              const fp = cert.fingerprint256;
              const pinned = pinGet();
              if (!pinned) {
                pinSet(fp);
                return undefined;
              }
              if (pinned === fp) return undefined;
              return new Error('hub TLS fingerprint changed — clear the stored pin to accept the new hub identity');
            },
          }
        : {}),
    });
  }

  _options(method, pathname, extraHeaders = {}) {
    return {
      method,
      host: this.host,
      port: this.port,
      path: pathname,
      agent: this.agent,
      headers: { authorization: `Bearer ${this.token}`, ...extraHeaders },
    };
  }

  json(method, pathname, body) {
    return new Promise((resolve, reject) => {
      const payload = body === undefined ? null : Buffer.from(JSON.stringify(body));
      const req = this.mod.request(this._options(method, pathname, payload
        ? { 'content-type': 'application/json', 'content-length': payload.length }
        : {}), res => {
        const chunks = [];
        res.on('data', c => chunks.push(c));
        res.on('end', () => {
          const text = Buffer.concat(chunks).toString('utf8');
          let data = null;
          try { data = text ? JSON.parse(text) : null; } catch { /* non-JSON error body */ }
          if (res.statusCode >= 400) {
            reject(new HttpError(res.statusCode, (data && data.error) || text.slice(0, 200) || 'request failed'));
          } else {
            resolve(data);
          }
        });
        res.on('error', reject);
      });
      req.setTimeout(JSON_TIMEOUT_MS, () => req.destroy(new Error('hub request timeout')));
      req.on('error', reject);
      if (payload) req.write(payload);
      req.end();
    });
  }

  /** Streams a file to destPath; verifies byte count against content-length. */
  download(pathname, destPath) {
    return new Promise((resolve, reject) => {
      const req = this.mod.request(this._options('GET', pathname), res => {
        if (res.statusCode >= 400) {
          res.resume();
          reject(new HttpError(res.statusCode, 'download failed'));
          return;
        }
        const expected = Number(res.headers['content-length']);
        const out = fs.createWriteStream(destPath);
        let received = 0;
        res.on('data', c => { received += c.length; });
        res.pipe(out);
        out.on('error', err => { res.destroy(); reject(err); });
        out.on('finish', () => {
          if (Number.isFinite(expected) && received !== expected) {
            reject(new Error(`short download: ${received}/${expected} bytes`));
          } else {
            resolve({ size: received, mtimeMs: Number(res.headers['x-mtime-ms']) });
          }
        });
        res.on('error', reject);
      });
      req.setTimeout(TRANSFER_TIMEOUT_MS, () => req.destroy(new Error('download timeout')));
      req.on('error', reject);
      req.end();
    });
  }

  async upload(pathname, absFile, size) {
    await fsp.access(absFile);
    return new Promise((resolve, reject) => {
      const req = this.mod.request(this._options('PUT', pathname, {
        'content-type': 'application/octet-stream',
        'content-length': size,
      }), res => {
        res.resume();
        res.on('end', () => {
          if (res.statusCode >= 400) reject(new HttpError(res.statusCode, 'upload failed'));
          else resolve();
        });
        res.on('error', reject);
      });
      req.setTimeout(TRANSFER_TIMEOUT_MS, () => req.destroy(new Error('upload timeout')));
      req.on('error', reject);
      const src = fs.createReadStream(absFile);
      src.on('error', err => { req.destroy(); reject(err); });
      src.pipe(req);
    });
  }

  // ---- API surface used by the spoke ----

  poll(body) { return this.json('POST', '/v1/spoke/poll', body); }

  folderDigest(id) { return this.json('GET', `/v1/folders/${encodeURIComponent(id)}/digest`); }

  indexPage(id, after, limit) {
    const q = `after=${encodeURIComponent(after)}&limit=${limit}`;
    return this.json('GET', `/v1/folders/${encodeURIComponent(id)}/index?${q}`);
  }

  fileGet(id, rel, destPath) {
    return this.download(`/v1/folders/${encodeURIComponent(id)}/file?path=${encodeURIComponent(rel)}`, destPath);
  }

  filePut(id, rel, absFile, { size, mtimeMs, conflictBackup = false, fromDevice = '' }) {
    const q = `path=${encodeURIComponent(rel)}&mtimeMs=${mtimeMs}&conflictBackup=${conflictBackup ? 1 : 0}&fromDevice=${encodeURIComponent(fromDevice)}`;
    return this.upload(`/v1/folders/${encodeURIComponent(id)}/file?${q}`, absFile, size);
  }

  fileDelete(id, rel, fromDevice = '') {
    const q = `path=${encodeURIComponent(rel)}&fromDevice=${encodeURIComponent(fromDevice)}`;
    return this.json('DELETE', `/v1/folders/${encodeURIComponent(id)}/file?${q}`);
  }

  close() { this.agent.destroy(); }
}

module.exports = { HubClient, HttpError };
