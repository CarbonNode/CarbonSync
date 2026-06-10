'use strict';

const path = require('node:path');
const fsp = require('node:fs/promises');
const { FolderIndex, EventLog } = require('./db');
const { loadDeviceId } = require('./ids');
const { writeMcpTokenFile } = require('./config');
const { buildIgnore, fullScan, rescanPaths } = require('./scanner');
const { watchFolder } = require('./watcher');
const { createServers, PROTOCOL_VERSION } = require('./server');
const { HubClient } = require('./client');
const { syncFolder } = require('./syncer');

const VERSION = require('../package.json').version;
const PURGE_INTERVAL_MS = 6 * 60 * 60 * 1000;
const MAX_POLL_BACKOFF_MS = 60_000;
const COMMAND_QUEUE_CAP = 100;

function jitter(ms) {
  return Math.round(ms * (0.85 + Math.random() * 0.3));
}

class Daemon {
  constructor(cfg, log) {
    this.cfg = cfg;
    this.log = log;
    this.startedAt = Date.now();
    this.folders = new Map(); // id -> rt
    this.spokes = new Map(); // hub only: device -> last report
    this.commandQueue = new Map(); // hub only: device -> cmd[]
    this._io = Promise.resolve(); // serializes scans (one disk walk at a time)
    this._syncQueue = [];
    this._drainPromise = null;
    this._pollTimer = null;
    this._timers = [];
    this._stopped = false;
    this._polling = false;
    this._pollFailing = false;
    this._selfHub = false;
    this._backoff = cfg.pollIntervalMs;
  }

  async start() {
    const { cfg } = this;
    this.deviceId = loadDeviceId(cfg.stateDir);
    this.eventLog = new EventLog(cfg.stateDir);
    writeMcpTokenFile(cfg);

    if (cfg.role === 'hub') {
      for (const f of cfg.folders) {
        const a = f.devices[cfg.deviceName];
        const rt = this._makeRt({
          id: f.id, name: f.name, path: a.path, mode: a.mode,
          excludes: [...(f.excludes || []), ...(a.excludes || [])],
        });
        this.folders.set(f.id, rt);
        this._chainIo(() => this._scanFolder(rt));
      }
    }

    this.servers = await createServers({
      config: cfg,
      role: cfg.role,
      deviceId: this.deviceId,
      deviceName: cfg.deviceName,
      version: VERSION,
      folders: this.folders,
      eventLog: this.eventLog,
      log: this.log,
      spokes: this.spokes,
      getStatus: () => this.getStatus(),
      requestSync: (id, opts) => this.requestSync(id, opts),
      setPause: (id, paused) => this.setPause(id, paused),
      requestRescan: id => this.requestRescan(id),
      enqueueCommand: (dev, cmd) => this.enqueueCommand(dev, cmd),
      drainCommands: dev => this.drainCommands(dev),
    });

    if (cfg.role === 'spoke') {
      this.hubClient = new HubClient({
        hubUrl: cfg.hubUrl,
        token: cfg.token,
        pinGet: () => this.eventLog.kvGet('hub_tls_pin'),
        pinSet: fp => this.eventLog.kvSet('hub_tls_pin', fp),
        log: this.log,
      });
      this._schedulePoll(0);
    }

    const rescanTimer = setInterval(() => {
      for (const rt of this.folders.values()) {
        this._chainIo(async () => {
          const res = await this._scanFolder(rt);
          if (this.cfg.role === 'spoke' && rt.ready && res && (res.changed || res.removed)) this.queueSync(rt.id);
        });
      }
    }, cfg.rescanIntervalMs);
    const purgeTimer = setInterval(() => {
      const before = Date.now() - cfg.tombstoneTtlMs;
      for (const rt of this.folders.values()) rt.db.purgeTombstones(before);
    }, PURGE_INTERVAL_MS);
    // Watcher events are NOT reliable when the watched root itself disappears
    // (unmount, rm -rf) — this watchdog is what actually flips folders
    // offline/online, on both roles.
    const rootTimer = setInterval(() => { this._rootWatchdog(); }, cfg.rootCheckIntervalMs);
    this._timers.push(rescanTimer, purgeTimer, rootTimer);

    this.log.info({
      role: cfg.role, device: cfg.deviceName, deviceId: this.deviceId,
      version: VERSION, dataPort: this.servers.dataPort, apiPort: this.servers.apiPort,
    }, 'carbonsyncd started');
    this.eventLog.add('info', null, 'daemon_start', null, `v${VERSION} role=${cfg.role}`);

    return { dataPort: this.servers.dataPort, apiPort: this.servers.apiPort };
  }

  _makeRt(def) {
    return {
      id: def.id,
      name: def.name,
      root: path.resolve(def.path),
      mode: def.mode,
      ig: buildIgnore(def.excludes),
      db: new FolderIndex(this.cfg.stateDir, def.id),
      ready: false,
      offline: false,
      paused: false,
      syncing: false,
      guardTripped: false,
      guardHubDigest: null,
      lastSyncAt: null,
      lastError: null,
      watcher: null,
    };
  }

  _chainIo(fn) {
    this._io = this._io.then(fn).catch(err => {
      this.log.warn({ err: err.message }, 'background io task failed');
    });
    return this._io;
  }

  async _scanFolder(rt) {
    if (this._stopped) return null;
    const res = await fullScan(rt.root, rt.db, rt.ig);
    if (res.offline) {
      if (!rt.offline) {
        rt.offline = true;
        this.eventLog.add('warn', rt.id, 'folder_offline', null, `root missing: ${rt.root}`);
        this.log.warn({ folder: rt.id, root: rt.root }, 'folder root missing — NOT treating as deletion');
      }
      rt.ready = false;
      if (rt.watcher) {
        await rt.watcher.close().catch(() => {});
        rt.watcher = null;
      }
      return res;
    }

    if (rt.offline) this.eventLog.add('info', rt.id, 'folder_online', null, rt.root);
    rt.offline = false;
    rt.ready = true;
    if (res.changed || res.removed) {
      this.eventLog.add('info', rt.id, 'scan', null, `changed=${res.changed} removed=${res.removed} scanned=${res.scanned}`);
    }
    if (!rt.watcher && !this._stopped) {
      try {
        rt.watcher = await watchFolder(rt.root, paths => this._onWatch(rt, paths), {
          debounceMs: this.cfg.debounceMs, log: this.log,
        });
      } catch (err) {
        this.log.warn({ folder: rt.id, err: err.message }, 'watcher unavailable — relying on periodic rescans');
      }
    }
    return res;
  }

  async _rootWatchdog() {
    if (this._stopped) return;
    for (const rt of this.folders.values()) {
      let present = false;
      try {
        present = (await fsp.stat(rt.root)).isDirectory();
      } catch { /* missing */ }
      if (rt.ready && !present) {
        this._chainIo(() => this._scanFolder(rt)); // flips offline, closes watcher, NO tombstones
      } else if (rt.offline && present) {
        this._chainIo(async () => {
          await this._scanFolder(rt);
          if (this.cfg.role === 'spoke' && rt.ready) this.queueSync(rt.id);
        });
      }
    }
  }

  async _onWatch(rt, paths) {
    if (this._stopped || !rt.ready) return;
    const res = await rescanPaths(rt.root, rt.db, rt.ig, paths);
    if (res.offline) {
      await this._chainIo(() => this._scanFolder(rt)); // flips it offline properly
      return;
    }
    if ((res.changed || res.removed) && this.cfg.role === 'spoke') this.queueSync(rt.id);
  }

  // ---- spoke poll loop: single timer, stateless requests, jittered backoff ----

  _schedulePoll(ms) {
    if (this._stopped) return;
    if (this._pollTimer) clearTimeout(this._pollTimer);
    this._pollTimer = setTimeout(() => { this._pollOnce(); }, ms);
  }

  async _pollOnce() {
    if (this._stopped || this._polling) return;
    this._polling = true;
    try {
      const resp = await this.hubClient.poll({
        device: this.cfg.deviceName,
        deviceId: this.deviceId,
        version: VERSION,
        pv: PROTOCOL_VERSION,
        status: { folders: this._folderStatusList() },
      });

      // Landmine #1, checked from both ends: never sync against yourself.
      if (resp.hubDeviceId === this.deviceId) {
        if (!this._selfHub) {
          this._selfHub = true;
          this.eventLog.add('error', null, 'self_hub', null, 'hubUrl resolves to this device — refusing to sync');
          this.log.error('hubUrl resolves to this device — refusing to sync');
        }
        this._backoff = MAX_POLL_BACKOFF_MS;
        return;
      }
      this._selfHub = false;

      if (this._pollFailing) {
        this._pollFailing = false;
        this.eventLog.add('info', null, 'hub_reconnected', null, this.cfg.hubUrl);
      }
      this._backoff = this.cfg.pollIntervalMs;

      await this._reconcileAssignments(resp.folders || []);
      for (const cmd of resp.commands || []) this._handleCommand(cmd);

      for (const a of resp.folders || []) {
        const rt = this.folders.get(a.id);
        if (!rt || !rt.ready || rt.offline || rt.paused || !a.hubReady || !a.hubDigest) continue;
        if (rt.guardTripped) {
          if (a.hubDigest === rt.guardHubDigest) continue; // nothing changed since the block
          rt.guardHubDigest = a.hubDigest;
        }
        const cur = rt.db.digest().hex;
        if (a.hubDigest !== rt.db.getMeta('lastHubDigest') || cur !== rt.db.getMeta('lastLocalDigest')) {
          this.queueSync(a.id);
        }
      }
    } catch (err) {
      if (!this._pollFailing) {
        this._pollFailing = true;
        this.eventLog.add('warn', null, 'hub_unreachable', null, err.message);
      }
      this.log.warn({ err: err.message }, 'hub poll failed');
      this._backoff = Math.min(Math.max(this._backoff * 2, 5000), MAX_POLL_BACKOFF_MS);
    } finally {
      this._polling = false;
      this._schedulePoll(jitter(this._backoff));
    }
  }

  _folderStatusList() {
    const out = [];
    for (const rt of this.folders.values()) {
      out.push({
        id: rt.id,
        ready: rt.ready,
        offline: rt.offline,
        paused: rt.paused,
        syncing: rt.syncing,
        guardTripped: rt.guardTripped,
        liveCount: rt.ready ? rt.db.liveCount() : null,
        digest: rt.ready ? rt.db.digest().hex : null,
        lastSyncAt: rt.lastSyncAt,
        lastError: rt.lastError,
      });
    }
    return out;
  }

  async _reconcileAssignments(list) {
    const incoming = new Set(list.map(a => a.id));
    for (const a of list) {
      let rt = this.folders.get(a.id);
      const root = path.resolve(a.path);
      if (rt) {
        rt.mode = a.mode;
        rt.ig = buildIgnore(a.excludes);
        if (rt.root !== root) {
          this.eventLog.add('info', a.id, 'folder_moved', null, `${rt.root} -> ${root}`);
          if (rt.watcher) {
            await rt.watcher.close().catch(() => {});
            rt.watcher = null;
          }
          rt.root = root;
          rt.ready = false;
          this._chainIo(async () => {
            await this._scanFolder(rt);
            if (rt.ready) this.queueSync(rt.id);
          });
        }
        continue;
      }
      rt = this._makeRt(a);
      this.folders.set(a.id, rt);
      this.eventLog.add('info', a.id, 'folder_assigned', null, `${root} (${a.mode})`);
      this._chainIo(async () => {
        await this._scanFolder(rt);
        if (rt.ready) this.queueSync(rt.id);
      });
    }
    for (const [id, rt] of this.folders) {
      if (incoming.has(id)) continue;
      this.folders.delete(id);
      this.eventLog.add('info', id, 'folder_unassigned', null, 'index retained, data untouched');
      if (rt.watcher) await rt.watcher.close().catch(() => {});
      rt.db.close();
    }
  }

  _handleCommand(cmd) {
    switch (cmd.type) {
      case 'sync_now': return this.requestSync(cmd.folder || null, { force: false });
      case 'force_sync': return cmd.folder && this.requestSync(cmd.folder, { force: true });
      case 'pause': return cmd.folder && this.setPause(cmd.folder, !!cmd.paused);
      case 'rescan': return this.requestRescan(cmd.folder || null);
      default:
        this.log.warn({ cmd }, 'unknown command from hub');
    }
  }

  // ---- sync queue: serial, deduped ----

  queueSync(id, { force = false } = {}) {
    if (this._stopped || this.cfg.role !== 'spoke') return;
    const existing = this._syncQueue.find(q => q.id === id);
    if (existing) existing.force = existing.force || force;
    else this._syncQueue.push({ id, force });
    if (!this._drainPromise) this._drainPromise = this._drainSyncs().finally(() => { this._drainPromise = null; });
  }

  async _drainSyncs() {
    while (this._syncQueue.length && !this._stopped) {
      const { id, force } = this._syncQueue.shift();
      const rt = this.folders.get(id);
      if (!rt || !rt.ready || rt.offline || rt.paused) continue;
      if (rt.guardTripped && !force) continue;
      rt.syncing = true;
      try {
        const res = await syncFolder(rt, this.hubClient, this.cfg, this.eventLog, this.log, {
          force, deviceName: this.cfg.deviceName,
        });
        if (res.blocked) {
          rt.lastError = 'delete_guard';
          rt.guardHubDigest = await this.hubClient.folderDigest(id).then(d => d.hex).catch(() => null);
        } else {
          rt.lastError = res.errors.length ? res.errors[0].error : null;
        }
      } catch (err) {
        rt.lastError = err.message;
        this.eventLog.add('error', id, 'sync_failed', null, err.message);
        this.log.warn({ folder: id, err: err.message }, 'sync failed');
      } finally {
        rt.syncing = false;
        rt.lastSyncAt = Date.now();
      }
    }
  }

  // ---- admin surface (HTTP/MCP-facing) ----

  requestSync(id, { force = false } = {}) {
    if (id) {
      if (force) {
        const rt = this.folders.get(id);
        if (rt) rt.guardTripped = false;
      }
      this.queueSync(id, { force });
    } else {
      for (const rtId of this.folders.keys()) this.queueSync(rtId, { force: false });
    }
  }

  setPause(id, paused) {
    const rt = this.folders.get(id);
    if (!rt) return;
    rt.paused = paused;
    this.eventLog.add('info', id, paused ? 'paused' : 'resumed', null, null);
  }

  requestRescan(id) {
    const targets = id ? [this.folders.get(id)].filter(Boolean) : [...this.folders.values()];
    for (const rt of targets) {
      this._chainIo(async () => {
        const res = await this._scanFolder(rt);
        if (this.cfg.role === 'spoke' && rt.ready && res && (res.changed || res.removed)) this.queueSync(rt.id);
      });
    }
  }

  enqueueCommand(device, cmd) {
    const q = this.commandQueue.get(device) || [];
    if (q.length >= COMMAND_QUEUE_CAP) q.shift();
    q.push(cmd);
    this.commandQueue.set(device, q);
  }

  drainCommands(device) {
    const q = this.commandQueue.get(device) || [];
    this.commandQueue.delete(device);
    return q;
  }

  getStatus() {
    const usable = !this._stopped; // dbs are closed after stop()
    const folders = [];
    for (const rt of this.folders.values()) {
      folders.push({
        id: rt.id, name: rt.name, root: rt.root, mode: rt.mode,
        ready: rt.ready, offline: rt.offline, paused: rt.paused,
        syncing: rt.syncing, guardTripped: rt.guardTripped,
        liveCount: usable && rt.ready ? rt.db.liveCount() : null,
        digest: usable && rt.ready ? rt.db.digest().hex : null,
        lastSyncAt: rt.lastSyncAt, lastError: rt.lastError,
      });
    }
    const status = {
      device: this.cfg.deviceName,
      deviceId: this.deviceId,
      role: this.cfg.role,
      version: VERSION,
      pv: PROTOCOL_VERSION,
      uptimeSec: Math.round((Date.now() - this.startedAt) / 1000),
      folders,
    };
    if (this.cfg.role === 'spoke') {
      status.hub = { url: this.cfg.hubUrl, reachable: !this._pollFailing, selfHub: this._selfHub };
    }
    return status;
  }

  async stop() {
    if (this._stopped) return;
    this._stopped = true;
    if (this._pollTimer) clearTimeout(this._pollTimer);
    for (const t of this._timers) clearInterval(t);
    await this._io.catch(() => {});
    if (this._drainPromise) await this._drainPromise.catch(() => {});
    for (const rt of this.folders.values()) {
      if (rt.watcher) await rt.watcher.close().catch(() => {});
    }
    if (this.servers) await this.servers.close();
    if (this.hubClient) this.hubClient.close();
    for (const rt of this.folders.values()) rt.db.close();
    if (this.eventLog) {
      this.eventLog.add('info', null, 'daemon_stop', null, null);
      this.eventLog.close();
    }
  }
}

module.exports = { Daemon, VERSION };
