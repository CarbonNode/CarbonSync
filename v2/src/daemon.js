'use strict';

const path = require('node:path');
const fs = require('node:fs');
const fsp = require('node:fs/promises');
const { execFile } = require('node:child_process');
const { promisify } = require('node:util');
const execFileP = promisify(execFile);
const { FolderIndex, EventLog } = require('./db');
const { loadDeviceId } = require('./ids');
const { writeMcpTokenFile } = require('./config');
const { buildIgnore, fullScan, rescanPaths, rootState } = require('./scanner');
const { watchFolder } = require('./watcher');
const { createServers, PROTOCOL_VERSION } = require('./server');
const { HubClient } = require('./client');
const { syncFolder } = require('./syncer');

const VERSION = require('../package.json').version;
const PURGE_INTERVAL_MS = 6 * 60 * 60 * 1000;
const MAX_POLL_BACKOFF_MS = 60_000;
const COMMAND_QUEUE_CAP = 100;

function semverCmp(a, b) {
  const parse = v => {
    const [core, pre = ''] = String(v).split('-');
    return { nums: core.split('.').map(n => parseInt(n, 10) || 0), pre };
  };
  const A = parse(a), B = parse(b);
  for (let i = 0; i < 3; i++) {
    if ((A.nums[i] || 0) !== (B.nums[i] || 0)) return (A.nums[i] || 0) - (B.nums[i] || 0);
  }
  if (A.pre === B.pre) return 0;
  if (!A.pre) return 1; // release > prerelease
  if (!B.pre) return -1;
  // alpha.10 > alpha.2: compare dot-segments numerically when both numeric
  const as = A.pre.split('.'), bs = B.pre.split('.');
  for (let i = 0; i < Math.max(as.length, bs.length); i++) {
    const x = as[i], y = bs[i];
    if (x === y) continue;
    if (x === undefined) return -1;
    if (y === undefined) return 1;
    const xn = /^\d+$/.test(x), yn = /^\d+$/.test(y);
    if (xn && yn) return parseInt(x, 10) - parseInt(y, 10);
    return x < y ? -1 : 1;
  }
  return 0;
}

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
    this._exit = code => process.exit(code); // injectable for tests
  }

  async start() {
    const { cfg } = this;
    this.deviceId = loadDeviceId(cfg.stateDir);
    this.eventLog = new EventLog(cfg.stateDir);
    writeMcpTokenFile(cfg);

    if (cfg.role === 'hub') {
      for (const f of cfg.folders) {
        const a = f.devices[cfg.deviceName];
        const rt = this._makeRtPersisted({
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
      topology: (action, params) => this.topology(action, params),
      requestRestart: reason => this.requestRestart(reason),
      requestSelfUpdate: () => this.requestSelfUpdate(),
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
      paused: false, // restored from db meta below — pause survives restarts (incident 2026-06-10)
      syncing: false,
      guardTripped: false,
      guardHubDigest: null,
      lastSyncAt: null,
      lastError: null,
      watcher: null,
    };
  }

  _makeRtPersisted(def) {
    const rt = this._makeRt(def);
    rt.paused = rt.db.getMeta('paused') === '1';
    return rt;
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
    if (res.transient) return res; // stat hiccup under load — keep current state
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
      const state = await rootState(rt.root);
      if (state === 'unknown') continue; // transient stat error is NOT "missing" (incident 2026-06-10)
      const present = state === 'present';
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
    if (res.transient) return;
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
      rt = this._makeRtPersisted(a);
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
    rt.paused = paused; // syncer checks this live mid-pass — pause aborts in-flight work
    rt.db.setMeta('paused', paused ? '1' : '0');
    this.eventLog.add('info', id, paused ? 'paused' : 'resumed', null, null);
    if (!paused) this.queueSync(id);
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

  // ---- full-MCP control: topology / lifecycle ----

  /** Hub-only. Mutates folder topology, persists config, applies live.
   *  Spokes pick the change up on their next poll. Indexes/data are NEVER
   *  deleted by topology changes — removal only stops managing. */
  async topology(action, p = {}) {
    if (this.cfg.role !== 'hub') throw new Error('topology tools are hub-only');
    const SLUG = /^[a-zA-Z0-9][a-zA-Z0-9_-]*$/;
    const MODES = ['two-way', 'pull', 'push'];
    const folders = this.cfg.folders;
    const find = id => folders.find(f => f.id === id);
    const needFolder = id => {
      const f = find(id);
      if (!f) throw new Error(`unknown folder ${id}`);
      return f;
    };

    switch (action) {
      case 'add_folder': {
        if (typeof p.id !== 'string' || !SLUG.test(p.id)) throw new Error('id must be a slug (letters/digits/dash/underscore)');
        if (find(p.id)) throw new Error(`folder ${p.id} already exists`);
        if (typeof p.hubPath !== 'string' || !p.hubPath) throw new Error('hubPath required (the hub holds a copy of every folder)');
        const mode = p.mode || 'two-way';
        if (!MODES.includes(mode)) throw new Error(`bad mode ${p.mode}`);
        const def = { id: p.id, name: p.name || p.id, devices: { [this.cfg.deviceName]: { path: p.hubPath, mode } } };
        if (Array.isArray(p.excludes)) def.excludes = p.excludes;
        folders.push(def);
        break;
      }
      case 'remove_folder': {
        const f = needFolder(p.folder);
        folders.splice(folders.indexOf(f), 1);
        break;
      }
      case 'assign_device': {
        const f = needFolder(p.folder);
        if (typeof p.device !== 'string' || !SLUG.test(p.device)) throw new Error('device must be a slug');
        if (typeof p.path !== 'string' || !p.path) throw new Error('path required');
        const mode = p.mode || (f.devices[p.device] && f.devices[p.device].mode) || 'two-way';
        if (!MODES.includes(mode)) throw new Error(`bad mode ${p.mode}`);
        const a = { path: p.path, mode };
        if (Array.isArray(p.excludes)) a.excludes = p.excludes;
        f.devices[p.device] = a;
        break;
      }
      case 'unassign_device': {
        const f = needFolder(p.folder);
        if (!f.devices[p.device]) throw new Error(`${p.device} is not assigned to ${p.folder}`);
        if (p.device === this.cfg.deviceName) throw new Error('the hub holds every folder — use remove_folder instead');
        delete f.devices[p.device];
        break;
      }
      case 'set_mode': {
        const f = needFolder(p.folder);
        const a = f.devices[p.device];
        if (!a) throw new Error(`${p.device} is not assigned to ${p.folder}`);
        if (!MODES.includes(p.mode)) throw new Error(`bad mode ${p.mode}`);
        a.mode = p.mode;
        break;
      }
      case 'set_excludes': {
        const f = needFolder(p.folder);
        if (!Array.isArray(p.excludes)) throw new Error('excludes must be an array of gitignore-style patterns');
        if (p.device) {
          const a = f.devices[p.device];
          if (!a) throw new Error(`${p.device} is not assigned to ${p.folder}`);
          a.excludes = p.excludes;
        } else {
          f.excludes = p.excludes;
        }
        break;
      }
      default:
        throw new Error(`unknown topology action ${action}`);
    }

    this._persistConfig();
    await this._reconcileHubFolders();
    this.eventLog.add('info', p.folder || p.id || null, 'topology', null, `${action} ${JSON.stringify(p).slice(0, 300)}`);
    return {
      ok: true,
      folders: folders.map(f => ({ id: f.id, name: f.name, devices: Object.fromEntries(Object.entries(f.devices).map(([d, a]) => [d, { path: a.path, mode: a.mode }])) })),
    };
  }

  _persistConfig() {
    const tmp = this.cfg.configPath + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify(this.cfg._raw, null, 2) + '\n');
    fs.renameSync(tmp, this.cfg.configPath);
  }

  /** Diff cfg.folders against running hub rts and apply (same idea as the spoke's poll reconcile). */
  async _reconcileHubFolders() {
    const wanted = new Map(this.cfg.folders.map(f => [f.id, f]));
    for (const [id, rt] of this.folders) {
      if (wanted.has(id)) continue;
      this.folders.delete(id);
      if (rt.watcher) await rt.watcher.close().catch(() => {});
      rt.db.close();
      this.eventLog.add('info', id, 'folder_removed', null, 'index retained, data untouched');
    }
    for (const [id, f] of wanted) {
      const a = f.devices[this.cfg.deviceName];
      if (!a) continue;
      const root = path.resolve(a.path);
      const excludes = [...(f.excludes || []), ...(a.excludes || [])];
      let rt = this.folders.get(id);
      if (!rt) {
        rt = this._makeRtPersisted({ id, name: f.name, path: a.path, mode: a.mode, excludes });
        this.folders.set(id, rt);
        this._chainIo(() => this._scanFolder(rt));
        continue;
      }
      rt.name = f.name;
      rt.mode = a.mode;
      rt.ig = buildIgnore(excludes);
      if (rt.root !== root) {
        if (rt.watcher) {
          await rt.watcher.close().catch(() => {});
          rt.watcher = null;
        }
        rt.root = root;
        rt.ready = false;
        this._chainIo(() => this._scanFolder(rt));
      }
    }
  }

  /** Clean exit with code 2 — the scheduled task's restart-on-failure policy revives us. */
  requestRestart(reason) {
    this.eventLog.add('warn', null, 'restart', null, reason || 'requested via API');
    this.log.warn({ reason }, 'restart requested — exiting for task-scheduler revive');
    setTimeout(() => this._exit(2), 400).unref();
  }

  /** Code-only self-update from the local git clone (strictly-no-downgrade),
   *  then restart. Dependency changes still need install.ps1 — node_modules
   *  is not touched (native addons are locked while loaded). */
  async requestSelfUpdate() {
    const repo = this.cfg.repoDir;
    if (!repo || !fs.existsSync(path.join(repo, '.git'))) throw new Error(`repoDir is not a git clone: ${repo}`);
    const v2dir = path.join(repo, 'v2');
    await execFileP('git', ['-C', repo, 'pull', '--ff-only'], { timeout: 120_000 });
    const pkg = JSON.parse(fs.readFileSync(path.join(v2dir, 'package.json'), 'utf8'));
    if (semverCmp(pkg.version, VERSION) < 0) throw new Error(`repo has ${pkg.version} < running ${VERSION} — refusing downgrade`);
    const installDir = path.resolve(__dirname, '..');
    fs.cpSync(path.join(v2dir, 'src'), path.join(installDir, 'src'), { recursive: true, force: true });
    fs.copyFileSync(path.join(v2dir, 'package.json'), path.join(installDir, 'package.json'));
    this.eventLog.add('warn', null, 'self_update', null, `${VERSION} -> ${pkg.version}; restarting`);
    this.log.warn({ from: VERSION, to: pkg.version }, 'self-update applied — restarting');
    setTimeout(() => this._exit(2), 600).unref();
    return { ok: true, from: VERSION, to: pkg.version, restarting: true };
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
      rssMb: Math.round(process.memoryUsage.rss() / 1024 / 1024),
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
