'use strict';

const fs = require('node:fs');
const fsp = require('node:fs/promises');
const http = require('node:http');
const https = require('node:https');
const path = require('node:path');
const crypto = require('node:crypto');

const PROTOCOL_VERSION = 1;
const MAX_JSON_BODY = 1024 * 1024;

function tokenEqual(a, b) {
  const ba = Buffer.from(String(a));
  const bb = Buffer.from(String(b));
  if (ba.length !== bb.length) return false;
  return crypto.timingSafeEqual(ba, bb);
}

/** Reject traversal/absolute/UNC paths before they touch the filesystem. */
function safeResolve(root, rel) {
  if (typeof rel !== 'string' || !rel || rel.length > 4096) return null;
  if (rel.includes('\0') || rel.includes('\\')) return null;
  if (rel.startsWith('/') || /^[a-zA-Z]:/.test(rel)) return null;
  for (const seg of rel.split('/')) {
    if (!seg || seg === '.' || seg === '..') return null;
  }
  const abs = path.resolve(root, rel);
  const normRoot = path.resolve(root);
  if (abs !== normRoot && !abs.startsWith(normRoot + path.sep)) return null;
  return abs;
}

function makeConflictName(rel, who) {
  const dir = path.posix.dirname(rel);
  const base = path.posix.basename(rel);
  const dot = base.lastIndexOf('.');
  const stem = dot > 0 ? base.slice(0, dot) : base;
  const ext = dot > 0 ? base.slice(dot) : '';
  const name = `${stem}.conflict-${who}-${Date.now()}${ext}`;
  return dir === '.' ? name : `${dir}/${name}`;
}

function readJson(req, limit = MAX_JSON_BODY) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let total = 0;
    req.on('data', c => {
      total += c.length;
      if (total > limit) {
        reject(new Error('body too large'));
        req.destroy();
        return;
      }
      chunks.push(c);
    });
    req.on('end', () => {
      try {
        resolve(chunks.length ? JSON.parse(Buffer.concat(chunks).toString('utf8')) : {});
      } catch {
        reject(new Error('invalid JSON body'));
      }
    });
    req.on('error', reject);
  });
}

function send(res, status, data) {
  const body = JSON.stringify(data);
  res.writeHead(status, { 'content-type': 'application/json', 'content-length': Buffer.byteLength(body) });
  res.end(body);
}

function ensureTls(stateDir) {
  const keyFile = path.join(stateDir, 'tls.key');
  const certFile = path.join(stateDir, 'tls.cert');
  if (fs.existsSync(keyFile) && fs.existsSync(certFile)) {
    return { key: fs.readFileSync(keyFile), cert: fs.readFileSync(certFile) };
  }
  const selfsigned = require('selfsigned');
  const pems = selfsigned.generate([{ name: 'commonName', value: 'carbonsync-hub' }], {
    days: 3650,
    keySize: 2048,
    algorithm: 'sha256',
  });
  fs.mkdirSync(stateDir, { recursive: true });
  fs.writeFileSync(keyFile, pems.private);
  fs.writeFileSync(certFile, pems.cert);
  return { key: pems.private, cert: pems.cert };
}

/**
 * One handler serves both listeners:
 *  - hub data plane (LAN, TLS+token): /v1/spoke/poll, /v1/folders/*
 *  - localhost admin API (every node; what the CortexAgent probe wraps)
 *
 * ctx is provided by the daemon:
 *  { config, role, deviceId, deviceName, version, folders: Map<id, rt>,
 *    eventLog, log, spokes: Map, commandQueue: Map<device, cmd[]>,
 *    getStatus(), requestSync(id, {force}), setPause(id, paused), requestRescan(id) }
 */
function createHandler(ctx) {
  const { config } = ctx;

  return async function handle(req, res) {
    try {
      const u = new URL(req.url, 'http://internal');
      const auth = req.headers.authorization || '';
      if (!auth.startsWith('Bearer ') || !tokenEqual(auth.slice(7), config.token)) {
        send(res, 401, { error: 'unauthorized' });
        req.resume();
        return;
      }

      const parts = u.pathname.split('/').filter(Boolean); // e.g. ['v1','folders',id,'file']
      if (parts[0] !== 'v1') return send(res, 404, { error: 'not found' });
      const route = parts[1];

      // ---- common (hub + spoke) ----
      if (route === 'handshake' && req.method === 'GET') {
        return send(res, 200, {
          deviceId: ctx.deviceId, deviceName: ctx.deviceName,
          role: ctx.role, version: ctx.version, pv: PROTOCOL_VERSION,
        });
      }
      if (route === 'status' && req.method === 'GET') {
        return send(res, 200, ctx.getStatus());
      }
      if (route === 'events' && req.method === 'GET') {
        const limit = Number(u.searchParams.get('limit')) || 100;
        const folder = u.searchParams.get('folder') || null;
        return send(res, 200, { events: ctx.eventLog.recent({ limit, folder }) });
      }
      if (route === 'sync_now' && req.method === 'POST') {
        const body = await readJson(req);
        if (ctx.role === 'spoke') {
          ctx.requestSync(body.folder || null, { force: false });
          return send(res, 200, { ok: true });
        }
        // hub: queue a sync_now for every spoke assigned to the folder
        let queued = 0;
        for (const f of config.folders) {
          if (body.folder && f.id !== body.folder) continue;
          for (const dev of Object.keys(f.devices)) {
            if (dev === ctx.deviceName) continue;
            ctx.enqueueCommand(dev, { type: 'sync_now', folder: f.id });
            queued++;
          }
        }
        return send(res, 200, { ok: true, queued });
      }
      if (route === 'force_sync' && req.method === 'POST') {
        const body = await readJson(req);
        if (!body.folder) return send(res, 400, { error: 'folder required' });
        if (ctx.role === 'spoke') {
          ctx.requestSync(body.folder, { force: true });
          return send(res, 200, { ok: true });
        }
        if (!body.device) return send(res, 400, { error: 'device required on hub' });
        ctx.enqueueCommand(body.device, { type: 'force_sync', folder: body.folder });
        return send(res, 200, { ok: true });
      }
      if (route === 'pause' && req.method === 'POST') {
        const body = await readJson(req);
        if (!body.folder) return send(res, 400, { error: 'folder required' });
        if (ctx.role === 'hub' && body.device) {
          ctx.enqueueCommand(body.device, { type: 'pause', folder: body.folder, paused: !!body.paused });
          return send(res, 200, { ok: true });
        }
        ctx.setPause(body.folder, !!body.paused);
        return send(res, 200, { ok: true });
      }
      if (route === 'rescan' && req.method === 'POST') {
        const body = await readJson(req);
        ctx.requestRescan(body.folder || null);
        return send(res, 200, { ok: true });
      }

      // ---- hub only ----
      if (ctx.role !== 'hub') return send(res, 404, { error: 'not found' });

      if (route === 'fleet' && req.method === 'GET') {
        const spokes = {};
        for (const [name, s] of ctx.spokes) spokes[name] = s;
        return send(res, 200, { hub: ctx.getStatus(), spokes });
      }

      if (route === 'spoke' && parts[2] === 'poll' && req.method === 'POST') {
        const body = await readJson(req);
        if (!body.device || typeof body.device !== 'string') return send(res, 400, { error: 'device required' });
        if (body.pv !== PROTOCOL_VERSION) return send(res, 409, { error: `protocol mismatch (hub pv=${PROTOCOL_VERSION})` });
        // Landmine #1: a node must never sync against itself.
        if (body.deviceId === ctx.deviceId) return send(res, 400, { error: 'self-connection refused' });

        ctx.spokes.set(body.device, {
          deviceId: body.deviceId || null,
          version: body.version || null,
          lastSeen: Date.now(),
          status: body.status || null,
        });

        const folders = [];
        for (const f of config.folders) {
          const assignment = f.devices[body.device];
          if (!assignment) continue;
          const rt = ctx.folders.get(f.id);
          folders.push({
            id: f.id,
            name: f.name,
            path: assignment.path,
            mode: assignment.mode,
            excludes: [...(f.excludes || []), ...(assignment.excludes || [])],
            hubDigest: rt && rt.ready && !rt.offline ? rt.db.digest().hex : null,
            hubReady: !!(rt && rt.ready && !rt.offline),
          });
        }
        const commands = ctx.drainCommands(body.device);
        return send(res, 200, { hubDeviceId: ctx.deviceId, hubName: ctx.deviceName, folders, commands });
      }

      if (route === 'spokes' && parts[3] === 'command' && req.method === 'POST') {
        const body = await readJson(req);
        if (!body.type) return send(res, 400, { error: 'type required' });
        ctx.enqueueCommand(decodeURIComponent(parts[2]), body);
        return send(res, 200, { ok: true });
      }

      if (route === 'folders' && parts.length >= 4) {
        const id = decodeURIComponent(parts[2]);
        const rt = ctx.folders.get(id);
        if (!rt) return send(res, 404, { error: `unknown folder ${id}` });
        const sub = parts[3];

        if (sub === 'digest' && req.method === 'GET') {
          if (!rt.ready || rt.offline) return send(res, 503, { error: 'folder not ready' });
          const d = rt.db.digest();
          return send(res, 200, { hex: d.hex, count: d.count });
        }

        if (sub === 'index' && req.method === 'GET') {
          if (!rt.ready || rt.offline) return send(res, 503, { error: 'folder not ready' });
          const after = u.searchParams.get('after') || '';
          const limit = Math.min(Math.max(1, Number(u.searchParams.get('limit')) || config.pageSize), 5000);
          const rows = rt.db.page(after, limit).map(r => ({
            path: r.path, size: r.size, mtimeMs: r.mtimeMs, deleted: r.deleted ? 1 : 0,
          }));
          return send(res, 200, { rows });
        }

        if (sub === 'file') {
          const rel = u.searchParams.get('path');
          const abs = rel != null ? safeResolve(rt.root, rel) : null;
          if (!abs) return send(res, 400, { error: 'bad path' });

          if (req.method === 'GET') {
            let st;
            try {
              st = await fsp.stat(abs);
            } catch {
              return send(res, 404, { error: 'no such file' });
            }
            res.writeHead(200, {
              'content-type': 'application/octet-stream',
              'content-length': st.size,
              'x-mtime-ms': Math.round(st.mtimeMs),
            });
            const stream = fs.createReadStream(abs);
            stream.on('error', () => res.destroy());
            stream.pipe(res);
            return;
          }

          if (req.method === 'PUT') {
            const mtimeMs = Number(u.searchParams.get('mtimeMs'));
            if (!Number.isFinite(mtimeMs)) return send(res, 400, { error: 'mtimeMs required' });
            const conflictBackup = u.searchParams.get('conflictBackup') === '1';
            const expected = Number(req.headers['content-length']);

            await fsp.mkdir(path.dirname(abs), { recursive: true });
            const tmp = path.join(path.dirname(abs), `.carbonsync.tmp-${crypto.randomBytes(6).toString('hex')}`);
            try {
              let received = 0;
              await new Promise((resolve, reject) => {
                const out = fs.createWriteStream(tmp);
                req.on('data', c => { received += c.length; });
                req.pipe(out);
                out.on('finish', resolve);
                out.on('error', reject);
                req.on('error', reject);
              });
              if (Number.isFinite(expected) && received !== expected) throw new Error(`short upload: ${received}/${expected}`);

              if (conflictBackup) {
                try {
                  await fsp.access(abs);
                  const conflictRel = makeConflictName(rel, ctx.deviceName);
                  await fsp.copyFile(abs, path.join(rt.root, conflictRel));
                  ctx.eventLog.add('warn', id, 'conflict_backup', rel, `hub copy preserved as ${conflictRel}`);
                } catch { /* no existing file — nothing to preserve */ }
              }

              const when = new Date(mtimeMs);
              await fsp.utimes(tmp, when, when);
              await fsp.rename(tmp, abs);
              const st = await fsp.stat(abs);
              rt.db.applyLocal([{ path: rel, size: st.size, mtimeMs: Math.round(st.mtimeMs) }]);
              return send(res, 200, { ok: true, size: st.size, mtimeMs: Math.round(st.mtimeMs) });
            } catch (err) {
              await fsp.rm(tmp, { force: true }).catch(() => {});
              throw err;
            }
          }

          if (req.method === 'DELETE') {
            await fsp.rm(abs, { force: true });
            rt.db.markDeletedSynced(rel);
            return send(res, 200, { ok: true });
          }
        }
      }

      send(res, 404, { error: 'not found' });
    } catch (err) {
      ctx.log.warn({ err: err.message, url: req.url }, 'request failed');
      if (!res.headersSent) send(res, 500, { error: err.message });
      else res.destroy();
    }
  };
}

/** Returns { dataPort, apiPort, close() }. Ports may be 0 in config (tests) — actual ports reported. */
async function createServers(ctx) {
  const handler = createHandler(ctx);
  const servers = [];

  const listen = (server, port, host) => new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(port, host, () => resolve(server.address().port));
  });

  let dataPort = null;
  if (ctx.role === 'hub') {
    const dataServer = ctx.config.tls
      ? https.createServer(ensureTls(ctx.config.stateDir), handler)
      : http.createServer(handler);
    dataServer.requestTimeout = 0; // large transfers; per-request timeouts live client-side
    dataPort = await listen(dataServer, ctx.config.dataPort, '0.0.0.0');
    servers.push(dataServer);
  }

  const apiServer = http.createServer(handler);
  const apiPort = await listen(apiServer, ctx.config.apiPort, '127.0.0.1');
  servers.push(apiServer);

  return {
    dataPort,
    apiPort,
    close: () => Promise.all(servers.map(s => new Promise(r => s.close(r)))),
  };
}

module.exports = { createServers, createHandler, safeResolve, makeConflictName, PROTOCOL_VERSION };
