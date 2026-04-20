/**
 * CarbonSync Unified Device
 *
 * Every device runs this. It combines:
 * - SyncEngine (scan + watch local folders)
 * - SyncServer (accept connections from other devices)
 * - Hub connection (connect to hub if configured, push/pull per folder direction)
 *
 * Direction model:
 *   push    = send my files to the hub
 *   receive = pull files from the hub
 *   both    = bidirectional (pull first, then push unique files)
 */

const path = require('path');
const os = require('os');
const fs = require('fs');
const fsp = require('fs/promises');
const crypto = require('crypto');
const { EventEmitter } = require('events');
const { Config } = require('./config');
const { SyncEngine } = require('./sync-engine');
const { SyncServer, SyncClient, writeFrame, streamFileToSocket } = require('./transport');
const { Discovery } = require('./discovery');
const { ensureFirewallRule } = require('./firewall');
const { ensureCerts } = require('./tls-certs');
const { MSG, SYNC_STATE } = require('../shared/protocol');
const { GameSaveManager } = require('./game-save-manager');
const { moveToTrash, pruneTrash, pruneConflicts } = require('./trash');
const { evaluateDeletion, getThresholds, BLOCKED_LOG_MESSAGE } = require('./deletion-guard');
const { classifyDeletion, classifyDeletionBatch, evaluateDeletionWithDiscovery } = require('./stale-peer-guard');
const { classifyOverwrite, getShrinkThreshold, SHRINK_BLOCKED_LOG_MESSAGE } = require('./shrink-guard');

const MAX_CONCURRENT_PUSHES = 4;
const PUSH_DEBOUNCE_MS = 500;
const RECENTLY_WRITTEN_TTL = 5000; // Ignore watcher events for files we just wrote

class CarbonSyncDevice extends EventEmitter {
  constructor(configDir) {
    super();
    this.configDir = configDir || path.join(os.homedir(), '.carbonsync');
    this.config = new Config(this.configDir);
    this.engine = null;
    this.transport = null;        // TCP server (always on)
    this.hubConnection = null;    // SyncClient to hub (if not the hub)
    this.peerConnections = new Map(); // ip:port -> { client: SyncClient, deviceName, connected }
    this.discovery = null;
    this.fingerprint = '';
    this._scanInterval = null;
    this.deviceSync = new Map();  // deviceName -> { folder -> status }
    this._pushQueues = new Map(); // folderName -> Set of relPaths pending push
    this._pushTimers = new Map(); // folderName -> debounce timer
    this._recentlyWritten = new Map(); // relPath -> timestamp (avoid watcher feedback loop)
    this.peerFolders = new Map();     // peerKey -> [{ name, fileCount, direction }]
    this.gameSaveManager = null;  // Set by main.js if game save feature enabled
    // Capped ring of recent deletion-guard rejections (Phase 2 P0 fix).
    // Surfaced through GET /blocked-deletions for the UI banner.
    this._blockedDeletions = [];
    // Capped ring of deletions preserved by the stale-peer guard (Phase 3 P0).
    // Surfaced through GET /preserved-deletions.
    this._preservedDeletions = [];
    // Capped ring of overwrites blocked by the shrink-guard (Phase 6 P0).
    // Surfaced through GET /shrink-blocked. Each entry carries the sidecar
    // path so the operator can recover the incoming bytes if needed.
    this._shrinkBlocked = [];
    // Tracks per-peer single-file delete bursts so a flood of one-at-a-time
    // deletes still trips the guard. Map<peerName, { count, windowStart }>.
    this._singleDeleteWindows = new Map();
    // Tracks which (peerId, folderName) pairs have been seeded for upgrades
    // so we don't re-seed on every reconnect. Process-lifetime memory is
    // sufficient because hasPeerKnown() short-circuits on subsequent runs.
    this._seededPeers = new Set();
    // Phase 7 P0: rate-limit per-peer "engine not ready" log lines. Without
    // this, every NOTIFY/PUSH_INDEX/INDEX_REQUEST during the boot window
    // would spam the log. Map<peerName, lastLogTimestamp>; one line per peer
    // per minute is plenty for diagnostics.
    this._engineNotReadyLogged = new Map();
    // Phase 7 P0: poll timer that flips _engineReady true once engine.start()
    // has resolved AND every enabled folder reports isInitialScanComplete().
    this._engineReadyPoll = null;
  }

  /**
   * Stable peer identifier for the per-peer last-known-state table.
   * Names can change; we use a 'peer:' prefix + the peer's reported deviceName
   * (which is the OS hostname returned by WELCOME). For the hub, see
   * _hubPeerId below. Both prefixes are namespaced so a hostname collision
   * with a hub address can't happen.
   */
  _peerIdFor(peerInfo) {
    if (!peerInfo) return null;
    if (peerInfo.deviceId) return `peer:${peerInfo.deviceId}`;
    if (peerInfo.deviceName) return `peer:${peerInfo.deviceName}`;
    if (peerInfo.ip && peerInfo.port) return `peer:${peerInfo.ip}:${peerInfo.port}`;
    return null;
  }

  _hubPeerId() {
    return `hub:${this.config.hubAddress || 'hub'}`;
  }

  /**
   * Stable peer id for an inbound client connection (used in handlers).
   */
  _peerIdForClient(client) {
    if (!client) return null;
    if (client.deviceId) return `peer:${client.deviceId}`;
    if (client.deviceName) return `peer:${client.deviceName}`;
    if (client.ip) return `peer:${client.ip}`;
    return null;
  }

  /**
   * Phase 5: discovery-first sync — no optimistic seeding.
   *
   * Phase 3 (v2.7.0) tried to short-circuit the stale-peer guard on first
   * contact by INSERTing peer_state rows from the local file index. That
   * backfired: a stale peer's "delete X" diff would then match the seeded
   * row and be approved, recreating the original data-loss bug on the very
   * first sync after upgrade.
   *
   * Instead, on first authentication we mark each (folder, peer) pair as
   * "currently discovering". The deletion path checks this flag and
   * preserves every proposed deletion (queueing all for push-back) until a
   * complete clean round-trip in either direction finishes — at which point
   * markPeerDiscovered() is called and the existing classifyDeletion guard
   * takes over.
   *
   * The function name is preserved so existing call sites and tests don't
   * need to change.
   *
   * @param {string} peerId
   * @param {string} peerLabel - For logging (deviceName or hub address).
   */
  _maybeSeedPeerKnown(peerId, peerLabel) {
    if (!peerId || !this.engine) return;
    for (const folder of this.config.folders) {
      if (!folder.enabled || folder.internal) continue;
      const ef = this._findEngineFolder(folder.name);
      if (!ef) continue;
      try {
        if (!ef.scanner.hasAnyDiscoveryRecord(peerId)) {
          ef.scanner.markPeerDiscovering(peerId);
          const line = `[${new Date().toISOString()}] DISCOVERING: peer=${peerLabel || peerId} folder=${folder.name} — preserving deletions until first round-trip completes`;
          console.log(line);
          try { fs.appendFileSync(path.join(this.configDir, 'sync.log'), line + '\n'); } catch {}
        }
      } catch (err) {
        console.warn(`Mark peer discovering failed [${folder.name}]: ${err.message}`);
      }
    }
  }

  /**
   * Phase 9 P0: rate-limited "scanner stale" warning. The scanner's `_stale`
   * flag is set when a scan is aborted mid-walk, when the folder became
   * inaccessible, or when a full scan throws. Pushing a partial index in that
   * state produces the same "peer sees phantom deletions" bug Phase 7 fixed
   * at the initial-scan-complete level — this is the in-flight variant.
   */
  _logScannerStale(folderName) {
    const key = `stale:${folderName || 'unknown'}`;
    const now = Date.now();
    if (!this._engineNotReadyLogged) this._engineNotReadyLogged = new Map();
    const last = this._engineNotReadyLogged.get(key) || 0;
    if (now - last < 60_000) return;
    this._engineNotReadyLogged.set(key, now);
    const line = `[${new Date().toISOString()}] SCANNER-STALE: deferred sync for folder=${folderName || 'unknown'} — scanner reports stale index, waiting for next successful scan`;
    console.warn(line);
    try { fs.appendFileSync(path.join(this.configDir, 'sync.log'), line + '\n'); } catch {}
  }

  /**
   * Phase 7 P0: rate-limited "engine not ready" warning. Called by every
   * gated sync entry point so a spammy peer can't fill the log during boot.
   * One line per peer per minute is enough to know what's deferred.
   */
  _logEngineNotReady(peerName) {
    const key = peerName || 'unknown';
    const now = Date.now();
    const last = this._engineNotReadyLogged.get(key) || 0;
    if (now - last < 60_000) return;
    this._engineNotReadyLogged.set(key, now);
    const line = `[${new Date().toISOString()}] ENGINE-NOT-READY: deferred sync from peer=${key} — initial scan still running, peer will be picked up by 15s quick-sync once ready`;
    console.log(line);
    try { fs.appendFileSync(path.join(this.configDir, 'sync.log'), line + '\n'); } catch {}
    // Bound the map so a long uptime with many transient peers can't grow it.
    if (this._engineNotReadyLogged.size > 256) {
      const cutoff = now - 5 * 60_000;
      for (const [k, ts] of this._engineNotReadyLogged) {
        if (ts < cutoff) this._engineNotReadyLogged.delete(k);
      }
    }
  }

  /**
   * Append a stale-peer-guard preservation record. Capped at 100 entries.
   */
  _recordPreservedDeletion(entry) {
    this._preservedDeletions.push(entry);
    if (this._preservedDeletions.length > 100) this._preservedDeletions.shift();
  }

  /**
   * Stale-peer guard wrapper. Logs PRESERVED lines, updates the in-memory
   * ring, and queues push-backs. Returns the partition so the caller can
   * apply the surviving deletes.
   *
   * Phase 5: routes through evaluateDeletionWithDiscovery, which short-
   * circuits to "preserve everything" for peers that haven't completed a
   * full sync round-trip with us yet.
   */
  _applyStalePeerGuard({ scanner, peerId, peerLabel, folderName, paths }) {
    const empty = { delete: [], preserve: [], pushBack: [] };
    if (!paths || paths.length === 0) return empty;
    if (!scanner || !peerId) {
      // Without a peer identity we have no last-known-state to consult. Be
      // conservative: preserve everything and push back.
      const preserve = paths.map(p => ({ path: p, reason: 'no-peer-identity' }));
      for (const e of preserve) {
        const line = `[${new Date().toISOString()}] PRESERVED: ${folderName}/${e.path} — ${e.reason} (peer=${peerLabel || 'unknown'})`;
        console.warn(line);
        try { fs.appendFileSync(path.join(this.configDir, 'sync.log'), line + '\n'); } catch {}
        this._recordPreservedDeletion({
          folder: folderName, peer: peerLabel || 'unknown',
          path: e.path, reason: e.reason, timestamp: Date.now(),
        });
      }
      return { delete: [], preserve, pushBack: paths.slice() };
    }

    // Phase 5: discovery-first short-circuit. If we haven't completed a
    // round-trip with this peer yet, preserve every deletion and queue all
    // paths for push-back. Logs use a distinct PRESERVED-DISCOVERY tag so
    // operators can tell first-sync caution apart from genuine guard hits.
    const isDiscovering = typeof scanner.isPeerDiscovered === 'function' &&
                          !scanner.isPeerDiscovered(peerId);
    if (isDiscovering) {
      const preserve = paths.map(p => ({ path: p, reason: 'discovery-first-sync' }));
      const line = `[${new Date().toISOString()}] PRESERVED-DISCOVERY: ${preserve.length} files for peer=${peerLabel || peerId} folder=${folderName} — first-sync caution`;
      console.warn(line);
      try { fs.appendFileSync(path.join(this.configDir, 'sync.log'), line + '\n'); } catch {}
      for (const e of preserve) {
        this._recordPreservedDeletion({
          folder: folderName, peer: peerLabel || peerId,
          path: e.path, reason: e.reason, timestamp: Date.now(),
        });
      }
      return { delete: [], preserve, pushBack: paths.slice() };
    }

    const result = classifyDeletionBatch({ scanner, peerId, paths });
    for (const e of result.preserve) {
      const line = `[${new Date().toISOString()}] PRESERVED: ${folderName}/${e.path} — ${e.reason} (peer=${peerLabel || peerId})`;
      console.warn(line);
      try { fs.appendFileSync(path.join(this.configDir, 'sync.log'), line + '\n'); } catch {}
      this._recordPreservedDeletion({
        folder: folderName, peer: peerLabel || peerId,
        path: e.path, reason: e.reason, timestamp: Date.now(),
      });
    }
    return result;
  }

  /**
   * Append a guard-rejection record. Capped at 100 entries (oldest dropped).
   */
  _recordBlockedDeletion(entry) {
    this._blockedDeletions.push(entry);
    if (this._blockedDeletions.length > 100) this._blockedDeletions.shift();
  }

  /**
   * Run the deletion guard for a batch operation. Centralised so all four
   * deletion code paths (pull-from-hub, pull-from-peer, client pull, push-
   * delete-receive) share identical behaviour: threshold check, sync.log
   * line, in-memory ring entry, console.warn.
   *
   * @returns {boolean} true if caller should proceed with the trash loop,
   *                    false if the batch was rejected.
   */
  _checkDeletionAllowed({ folderConfig, folderName, scanner, toDeleteCount, peerName }) {
    if (!toDeleteCount || toDeleteCount <= 0) return true;
    let totalFiles = 0;
    try { totalFiles = scanner?.getIndexMap?.().size || 0; } catch { totalFiles = 0; }
    const guard = evaluateDeletion({
      folderName,
      totalFiles,
      toDeleteCount,
      peerName,
      thresholds: getThresholds(folderConfig),
    });
    if (guard.allowed) return true;
    const line = BLOCKED_LOG_MESSAGE({
      folderName,
      peerName,
      count: toDeleteCount,
      reason: guard.reason,
    });
    console.warn(line);
    try {
      fs.appendFileSync(path.join(this.configDir, 'sync.log'), line + '\n');
    } catch {}
    this._recordBlockedDeletion({
      folder: folderName,
      peer: peerName,
      count: toDeleteCount,
      reason: guard.reason,
      timestamp: new Date().toISOString(),
    });
    return false;
  }

  _recordShrinkBlocked(entry) {
    this._shrinkBlocked.push(entry);
    if (this._shrinkBlocked.length > 100) this._shrinkBlocked.shift();
  }

  /**
   * Drastic-shrink overwrite guard (Phase 6 P0).
   *
   * Called immediately before every sync-driven atomic rename. Stats the
   * existing file, classifies, and on `preserve` writes the incoming bytes
   * to a `.shrink-blocked.<peer>.<ts>` sidecar, cleans up the prepared tmp,
   * logs a SHRINK-BLOCKED line, and returns false so the caller skips the
   * rename and leaves the existing file untouched.
   *
   * @param {object} args
   * @param {object} args.folderConfig - Folder config record (for shrinkGuard override).
   * @param {string} args.folderName   - Folder display name (for logs).
   * @param {string} args.absPath      - Absolute target path.
   * @param {string} args.relPath      - Path relative to folder root (for logs).
   * @param {string} args.tmpPath      - Prepared tmp file we'd otherwise rename.
   * @param {Buffer} args.fileData     - Incoming bytes (used for size + sidecar).
   * @param {string} args.peerName     - Source peer name (for logs / sidecar name).
   * @returns {Promise<boolean>} true → caller should proceed with the rename.
   *                             false → guard tripped, caller must return early.
   */
  async _checkShrinkOverwrite({ folderConfig, folderName, absPath, relPath, tmpPath, fileData, peerName }) {
    const existingStat = await fsp.stat(absPath).catch(() => null);
    const existingSize = existingStat ? existingStat.size : null;
    const incomingSize = fileData ? fileData.length : 0;

    const verdict = classifyOverwrite({
      existingSize,
      incomingSize,
      threshold: getShrinkThreshold(folderConfig),
    });

    if (verdict.action === 'allow') return true;

    // verdict.action === 'preserve'. Save incoming as a sidecar so the user
    // can compare/recover; leave the existing file intact.
    const safePeer = String(peerName || 'unknown').replace(/[^A-Za-z0-9._-]/g, '_');
    const ext = path.extname(absPath);
    const base = absPath.slice(0, absPath.length - ext.length);
    const conflictPath = `${base}.shrink-blocked.${safePeer}.${Date.now()}${ext}`;

    try {
      await fsp.mkdir(path.dirname(conflictPath), { recursive: true });
      if (fileData) await fsp.writeFile(conflictPath, fileData);
    } catch (err) {
      console.warn(`Shrink-guard sidecar write failed [${conflictPath}]: ${err.message}`);
    }

    // The tmp was already written by the caller (or about to be); regardless,
    // it must not survive — clean it up so we don't leave orphans.
    if (tmpPath) {
      try { await fsp.unlink(tmpPath); } catch {}
    }

    const line = SHRINK_BLOCKED_LOG_MESSAGE({
      folderName,
      relPath,
      peerName: peerName || 'unknown',
      existingSize: existingSize == null ? 0 : existingSize,
      incomingSize,
      reason: verdict.reason,
      conflictPath,
    });
    console.warn(line);
    try {
      fs.appendFileSync(path.join(this.configDir, 'sync.log'), line + '\n');
    } catch {}

    this._recordShrinkBlocked({
      folder: folderName,
      path: relPath,
      peer: peerName || 'unknown',
      existingSize: existingSize == null ? 0 : existingSize,
      incomingSize,
      reason: verdict.reason,
      conflictPath,
      timestamp: new Date().toISOString(),
    });

    return false;
  }

  async start() {
    console.log('=== CarbonSync Starting ===');
    console.log(`Device: ${this.config.deviceName} (${this.config.deviceId})`);
    console.log(`Role: ${this.config.isHub ? 'HUB' : 'DEVICE → ' + this.config.hubAddress}`);
    console.log(`Port: ${this.config.port}`);

    await ensureFirewallRule(this.config.port);

    // 1. Start sync engine (scan + watch ALL local folders)
    this.engine = new SyncEngine({
      configDir: this.configDir,
      folders: this.config.folders.filter(f => f.enabled),
    });

    this.engine.on('scan-progress', (p) => this.emit('progress', p));

    this.engine.on('changes', ({ folder, changes }) => {
      // Match by engine name OR by path (engine name may have hash suffix)
      const engineFolder = this.engine?.folders.get(folder);
      const folderConfig = this.config.folders.find(f => f.name === folder) ||
                           (engineFolder && this.config.folders.find(f => f.path === engineFolder.path));
      const direction = folderConfig?.direction || 'both';

      // Game Saves folder is managed by GameSaveManager — don't flood the activity feed
      // Only emit once per batch, not per file
      if (folderConfig?.internal) {
        // Silently handle without spamming — just push to peers quietly
        if ((direction === 'push' || direction === 'both') && this.hubConnection?.authenticated) {
          this._queuePush(folder, changes);
        }
        return;
      }

      // Filter out files we just wrote (from incoming pushes/pulls)
      const filtered = changes.filter(c => {
        const key = `${folder}/${c.path}`;
        const recentWrite = this._recentlyWritten.get(key);
        if (recentWrite && Date.now() - recentWrite < RECENTLY_WRITTEN_TTL) return false;
        return true;
      });

      if (filtered.length === 0) return;

      const peerCount = [...(this.peerConnections || new Map())].filter(([,p]) => p.connected && p.client?.authenticated).length;
      const logMsg = `[${new Date().toISOString()}] Changes: ${folder} → ${filtered.length} file(s) [dir=${direction}] peers=${peerCount} config=${folderConfig ? 'found' : 'MISSING'}`;
      console.log(logMsg);
      // Write to log file for debugging
      try { require('fs').appendFileSync(require('path').join(this.configDir, 'sync.log'), logMsg + '\n'); } catch {}

      // Push to hub if direction is push or both
      if ((direction === 'push' || direction === 'both') && this.hubConnection?.authenticated) {
        this._queuePush(folder, filtered);
      }

      // Push to all connected peers too
      const pushFolderName = folderConfig?.name || folder;
      if (direction === 'push' || direction === 'both') {
        for (const [, peerInfo] of this.peerConnections) {
          if (peerInfo.connected && peerInfo.client?.authenticated) {
            this._queuePushToPeer(peerInfo, pushFolderName, filtered);
          }
        }
      }

      // Broadcast to connected devices (hub behavior — for receivers)
      if (this.transport) {
        this.transport.broadcast({
          type: MSG.NOTIFY, folder,
          changes: filtered.map(c => ({ type: c.type, path: c.path, size: c.size, hash: c.hash })),
        });
      }

      this.emit('changes', { folder, changes: filtered });
    });

    // Start engine but don't await scan — let UI render first
    // Scan runs in background, UI updates via progress events
    //
    // Phase 7 P0: _engineReady is true ONLY when engine.start() has resolved
    // AND every enabled folder reports isInitialScanComplete(). Resolution of
    // engine.start() alone is NOT sufficient — that promise can settle while
    // a folder's scanner is still mid-batch (or while a freshly added folder
    // hasn't started scanning yet). A 2s poll re-checks until all folders
    // are ready, then stops. Inbound NOTIFY/PUSH_INDEX/INDEX_REQUEST and
    // authentication-triggered syncs are gated on this flag — gaps in the
    // scanner index would otherwise be interpreted by the peer as deletions.
    this._engineReady = false;
    this._engineStarted = false;
    const reevaluateReady = () => {
      if (!this._engineStarted || !this.engine) return false;
      try {
        const ready = typeof this.engine.areAllFoldersScanned === 'function'
          ? this.engine.areAllFoldersScanned()
          : true;
        if (ready && !this._engineReady) {
          this._engineReady = true;
          this.emit('scan-complete');
          console.log('Initial scan complete (all folders)');
          // Trigger full sync with all peers once scan is done
          this._syncAllPeers();
          if (this._engineReadyPoll) {
            clearInterval(this._engineReadyPoll);
            this._engineReadyPoll = null;
          }
        }
        return ready;
      } catch (err) {
        console.warn(`Engine-ready check failed: ${err.message}`);
        return false;
      }
    };
    this._reevaluateEngineReady = reevaluateReady;
    this.engine.start().then(() => {
      this._engineStarted = true;
      // Try once immediately; if a folder's scan hasn't persisted the marker
      // yet (very unlikely — start() awaits each fullScan), the poll catches it.
      if (!reevaluateReady()) {
        this._engineReadyPoll = setInterval(reevaluateReady, 2000);
      }
    }).catch(err => {
      console.error('Engine start failed:', err.message);
    });

    // 2. TLS certs
    const certs = ensureCerts(this.configDir);
    this.fingerprint = certs.fingerprint;

    // 3. Start TCP server (always — hub receives pushes, devices serve status)
    this.transport = new SyncServer({
      port: this.config.port,
      apiKey: this.config.apiKey,
      tlsKey: certs.key,
      tlsCert: certs.cert,
      deviceId: this.config.deviceId,
    });

    this.transport.on('client-connected', (c) => {
      console.log(`Client connected: ${c.deviceName}`);

      // Phase 7 P0: defer discovery marking until the initial scan completes.
      // _maybeSeedPeerKnown iterates engine folders and writes peer_discovery
      // rows; doing that against a partial scanner index would either skip
      // folders that haven't loaded yet or write incomplete state. The 15s
      // quick-sync path's outbound calls also defer until engine ready, so
      // the seed will happen on the first real exchange after ready flips.
      if (!this._engineReady) {
        this._logEngineNotReady(c.deviceName || c.ip || 'inbound-client');
      } else {
        // Phase 3 P0: seed per-peer last-known-state for this inbound peer if
        // we've never recorded any state for them before. Without this the
        // guard would preserve EVERY proposed deletion on installs that
        // pre-date this feature, which would silently break deletion sync.
        try {
          const peerId = this._peerIdForClient(c);
          if (peerId) this._maybeSeedPeerKnown(peerId, c.deviceName || c.ip);
        } catch (err) {
          console.warn(`Seed peer-known on inbound auth failed: ${err.message}`);
        }
      }

      // Check if this is a known/approved peer
      const savedPeers = this.config.data.savedPeers || [];
      const approvedPeers = this.config.data.approvedPeers || [];
      const remoteIp = c.socket.remoteAddress?.replace('::ffff:', '') || '';
      const isLan = remoteIp.startsWith('192.168.') || remoteIp.startsWith('10.') || remoteIp === '127.0.0.1';
      const isKnown = isLan ||
                       savedPeers.some(p => p.ip === remoteIp || p.deviceName === c.deviceName) ||
                       approvedPeers.some(p => p.ip === remoteIp || p.deviceId === c.deviceId || p.deviceName === c.deviceName);
      console.log(`Peer: ${c.deviceName} (${remoteIp}) — ${isKnown ? 'auto-approved' : 'needs approval'}`);

      if (isKnown) {
        // Auto-approve LAN and known peers
        this.emit('client-connected', { deviceName: c.deviceName, deviceId: c.deviceId, ip: remoteIp, auto: true });
        this._sendFolderList(c);

        // Auto-connect back after a short delay (avoid blocking the inbound handler)
        const peerKey = `${remoteIp}:${this.config.port}`;
        const alreadyConnected = this.peerConnections.has(peerKey) && this.peerConnections.get(peerKey).connected;
        if (remoteIp && !alreadyConnected) {
          setTimeout(() => {
            console.log(`Auto-connecting back to ${c.deviceName} (${remoteIp})...`);
            this.connectToPeer(remoteIp, this.config.port).then(result => {
              if (result.success) {
                console.log(`Bi-directional connection with ${c.deviceName} established`);
                if (!this.config.data.savedPeers) this.config.data.savedPeers = [];
                if (!this.config.data.savedPeers.some(p => p.ip === remoteIp)) {
                  this.config.data.savedPeers.push({ ip: remoteIp, port: this.config.port, deviceName: c.deviceName });
                  this.config.save();
                }
              } else {
                console.log(`Auto-connect back to ${c.deviceName} failed: ${result.message || 'unknown'}`);
              }
            }).catch(err => {
              console.log(`Auto-connect back error: ${err.message}`);
            });
          }, 3000); // Wait 3s for inbound to fully settle
        }
      } else {
        // New peer — emit sync request for UI to show approval popup
        console.log(`New peer requesting sync: ${c.deviceName} (${remoteIp})`);
        this.emit('sync-request', {
          deviceName: c.deviceName,
          deviceId: c.deviceId,
          ip: remoteIp,
          clientId: c.id,
          folders: this.config.folders.filter(f => f.enabled).map(f => ({
            name: f.name,
            path: f.path,
            direction: f.direction || 'both',
            fileCount: this.engine?.folders.get(f.name)?.scanner.getFileCount() || 0,
          })),
        });
      }
    });

    this.transport.on('client-disconnected', (c) => {
      console.log(`Client disconnected: ${c.deviceName || 'unknown'}`);
      this.emit('client-disconnected', { deviceName: c.deviceName });
    });

    this.transport.on('message', (client, msg) => this._handleMessage(client, msg));
    this.transport.on('binary', (client, data) => this._handleBinary(client, data));
    this.transport.start();

    // 4. Discovery
    this.discovery = new Discovery({
      port: this.config.port,
      role: this.config.isHub ? 'hub' : 'device',
      deviceId: this.config.deviceId,
    });
    this.discovery.publish();
    this.discovery.browse();

    // 5. Connect to hub if configured
    if (!this.config.isHub && this.config.hubAddress) {
      this._connectToHub();
    }

    // Fast sync every 15s: push/pull using current index (no disk rescan).
    // Catches silently dropped pushes so sync stays near-instant.
    this._quickSyncInterval = setInterval(() => {
      if (!this._engineReady) return;
      this._quickSyncAllPeers();
    }, 15000);

    // Full rescan every 5 minutes: safety net for when watchers miss changes.
    this._periodicSyncInterval = setInterval(() => {
      if (!this._engineReady) return;
      this._syncAllPeers();
    }, 5 * 60 * 1000);

    // Prune trash buckets at startup and every 6h. 7-day retention.
    this._pruneAllTrash();
    this._trashPruneInterval = setInterval(() => this._pruneAllTrash(), 6 * 60 * 60 * 1000);

    // 7. Game save detection & backup
    if (this.config.data.settings?.gameSaveEnabled !== false) {
      this.gameSaveManager = new GameSaveManager({
        configDir: this.configDir,
        config: this.config,
      });
      try {
        await this.gameSaveManager.start();

        // Auto-register game-saves folder for sync (direction: both, hidden from Folders tab)
        const gameSavesDir = path.join(this.configDir, 'game-saves');
        fs.mkdirSync(gameSavesDir, { recursive: true }); // Ensure it exists on fresh installs
        const alreadySynced = this.config.folders.some(f => path.resolve(f.path) === path.resolve(gameSavesDir));
        if (!alreadySynced) {
          try {
            this.config.data.folders.push({
              path: path.resolve(gameSavesDir),
              name: 'Game Saves',
              ignorePatterns: [],
              excludes: [],
              direction: 'both',
              enabled: true,
              internal: true, // Hidden from Folders tab UI
            });
            this.config.save();
            this.engine.addFolder(this.config.folders.find(f => f.name === 'Game Saves'));
            await this.engine.rescan('Game Saves');
            console.log('Auto-registered game-saves folder for sync');
          } catch (err) {
            console.error('Failed to auto-register game-saves:', err.message);
          }
        } else {
          // Ensure existing entry is marked internal
          const existing = this.config.folders.find(f => f.name === 'Game Saves');
          if (existing && !existing.internal) {
            existing.internal = true;
            this.config.save();
          }
        }
      } catch (err) {
        console.error('Game save manager failed:', err.message);
      }
    }

    // 8. HTTP Status API (for Carbon Vision and other integrations)
    this._startHttpApi();

    console.log('=== CarbonSync Ready ===');
    this.emit('ready');
  }

  _startHttpApi() {
    const http = require('http');
    const httpPort = this.config.port + 2; // 21549 by default

    this._httpServer = http.createServer((req, res) => {
      res.setHeader('Access-Control-Allow-Origin', '*');
      res.setHeader('Content-Type', 'application/json');

      if (req.url === '/health') {
        res.end(JSON.stringify({ ok: true, version: require('../../package.json').version }));
      } else if (req.url === '/status') {
        try {
          res.end(JSON.stringify(this.getStatus()));
        } catch (err) {
          res.statusCode = 500;
          res.end(JSON.stringify({ error: err.message }));
        }
      } else if (req.url === '/sync-log') {
        try {
          const logPath = require('path').join(this.configDir, 'sync.log');
          const content = require('fs').readFileSync(logPath, 'utf-8');
          const lines = content.trim().split('\n').slice(-50);
          res.end(JSON.stringify({ lines }));
        } catch (err) {
          res.end(JSON.stringify({ lines: [], error: err.message }));
        }
      } else if (req.url === '/blocked-deletions') {
        // P0 deletion-guard rejections so the UI can show a banner.
        // Newest last; capped to 100 entries.
        try {
          res.end(JSON.stringify({ blocked: this._blockedDeletions || [] }));
        } catch (err) {
          res.statusCode = 500;
          res.end(JSON.stringify({ error: err.message }));
        }
      } else if (req.url === '/preserved-deletions') {
        // Phase 3 P0: stale-peer-guard preservations so the UI can show
        // which deletions were refused and why. Newest last; capped to 100.
        try {
          res.end(JSON.stringify({ preserved: this._preservedDeletions || [] }));
        } catch (err) {
          res.statusCode = 500;
          res.end(JSON.stringify({ error: err.message }));
        }
      } else if (req.url === '/shrink-blocked') {
        // Phase 6 P0: drastic-shrink overwrites that the guard refused.
        // Each entry includes the .shrink-blocked sidecar path so the UI
        // (or operator) can show the user where the incoming bytes went.
        // Newest last; capped to 100.
        try {
          res.end(JSON.stringify({ blocked: this._shrinkBlocked || [] }));
        } catch (err) {
          res.statusCode = 500;
          res.end(JSON.stringify({ error: err.message }));
        }
      } else {
        res.statusCode = 404;
        res.end(JSON.stringify({ error: 'Not found. Use /status or /health' }));
      }
    });

    this._httpServer.on('error', (err) => {
      console.warn(`HTTP API failed to start on port ${httpPort}: ${err.message}`);
    });

    this._httpServer.listen(httpPort, '0.0.0.0', () => {
      console.log(`HTTP API listening on port ${httpPort}`);
    });
  }

  // ---- Hub Connection (device → hub) ----

  _connectToHub() {
    const [host, portStr] = this.config.hubAddress.split(':');
    const port = parseInt(portStr) || 21547;

    console.log(`Connecting to hub: ${host}:${port}`);

    this.hubConnection = new SyncClient({
      host, port,
      apiKey: this.config.hubApiKey || this.config.apiKey,
      deviceId: this.config.deviceId,
      deviceName: this.config.deviceName,
    });

    this.hubConnection.on('authenticated', (msg) => {
      console.log(`Authenticated with hub: ${msg.serverName}`);
      this.emit('hub-connected', msg);
      // Phase 7 P0: gate on engine ready. Without the initial scan complete,
      // _maybeSeedPeerKnown would mark folders discovering against an index
      // that doesn't yet reflect what we have on disk, and _startDirectionalSync
      // would push/pull a partial index — peers infer wrong toDelete from gaps.
      // Defer both: 15s quick-sync re-runs once _engineReady flips true.
      if (!this._engineReady) {
        this._logEngineNotReady(msg.serverName || this.config.hubAddress || 'hub');
        return;
      }
      // Phase 3 P0: seed per-hub last-known-state on first contact.
      try {
        this._maybeSeedPeerKnown(this._hubPeerId(), msg.serverName || this.config.hubAddress);
      } catch (err) {
        console.warn(`Seed peer-known on hub auth failed: ${err.message}`);
      }
      this._startDirectionalSync();
    });

    this.hubConnection.on('message', (msg) => {
      if (msg.type === MSG.NOTIFY) {
        // Phase 7 P0: gate on engine ready. A NOTIFY-driven _pullFolder run
        // before initial scan completes would compare hub's index against a
        // partial local one — files we just haven't scanned would land in
        // toDelete and get trashed. 15s quick-sync re-runs after ready flips.
        if (!this._engineReady) {
          this._logEngineNotReady(this.config.hubAddress || 'hub');
          return;
        }
        const folderConfig = this.config.folders.find(f => f.name === msg.folder) ||
                             this.config.folders.find(f => f.name?.toLowerCase() === msg.folder?.toLowerCase());
        if (folderConfig && (folderConfig.direction === 'receive' || folderConfig.direction === 'both')) {
          this._pullFolder(folderConfig);
        }
      } else if (msg.type === MSG.FOLDER_LIST) {
        this.emit('hub-folders', msg.folders);
      } else if (msg.type === 'set_device_name') {
        console.log(`Hub renamed this device to: ${msg.name}`);
        this.config.setDeviceName(msg.name);
        this.emit('device-renamed', msg.name);
      } else if (msg.type === 'folder_renamed') {
        const folder = this.config.folders.find(f => f.path === msg.path);
        if (folder) {
          folder.name = msg.name;
          this.config.save();
          this.emit('folder-renamed', msg);
        }
      } else if (msg.type === 'folder_icon') {
        if (msg.iconBase64 && msg.name) {
          const iconDir = path.join(this.configDir, 'folder-icons');
          fs.mkdirSync(iconDir, { recursive: true });
          const destName = msg.name.replace(/[^a-zA-Z0-9]/g, '_') + (msg.ext || '.png');
          const dest = path.join(iconDir, destName);
          fs.writeFileSync(dest, Buffer.from(msg.iconBase64, 'base64'));
          const folder = this.config.folders.find(f => f.name === msg.name);
          if (folder) { folder.icon = dest; this.config.save(); }
        }
      }
    });

    this.hubConnection.on('disconnected', () => {
      this.emit('hub-disconnected');
    });

    this.hubConnection.on('error', (err) => {
      console.error(`Hub connection error: ${err.message}`);
    });

    this.hubConnection.connect();
  }

  /**
   * Start syncing all folders based on their direction.
   */
  async _startDirectionalSync() {
    for (const folder of this.config.folders) {
      if (!folder.enabled) continue;
      try {
        switch (folder.direction) {
          case 'receive':
            await this._pullFolder(folder);
            this.hubConnection.send({ type: MSG.SUBSCRIBE, folder: folder.name });
            break;
          case 'push':
            await this._pushFullFolder(folder);
            break;
          case 'both':
            // Push-before-pull: see _syncWithPeer comment.
            await this._pushFullFolder(folder);
            await this._pullFolder(folder);
            this.hubConnection.send({ type: MSG.SUBSCRIBE, folder: folder.name });
            break;
        }
      } catch (err) {
        console.error(`Sync failed for ${folder.name}: ${err.message}`);
      }
    }
  }

  // ---- Peer Connections ----

  /**
   * Connect to a peer and start syncing folders.
   * Called when user adds a peer via UI.
   */
  async connectToPeer(ip, port, apiKey) {
    const key = `${ip}:${port}`;
    if (this.peerConnections.has(key)) {
      const existing = this.peerConnections.get(key);
      if (existing.client?.connected) return { success: true, message: 'Already connected' };
      // Disconnect old one
      existing.client?.disconnect();
    }

    const client = new SyncClient({
      host: ip,
      port,
      apiKey: apiKey || this.config.apiKey,
      deviceId: this.config.deviceId,
      deviceName: this.config.deviceName,
    });

    const peerInfo = { client, ip, port, deviceName: null, connected: false };
    this.peerConnections.set(key, peerInfo);

    return new Promise((resolve) => {
      const timeout = setTimeout(() => {
        resolve({ success: false, message: 'Connection timed out' });
      }, 10000);

      client.on('authenticated', async (msg) => {
        clearTimeout(timeout);
        peerInfo.deviceName = msg.serverName || ip;
        if (msg.deviceId) peerInfo.deviceId = msg.deviceId;
        peerInfo.connected = true;
        console.log(`Connected to peer: ${peerInfo.deviceName} (${key})`);
        this.emit('peer-connected', { ip, port, deviceName: peerInfo.deviceName });

        // Folder list is metadata only (no scanner reads beyond getFileCount);
        // safe to send even before engine is ready so the peer's UI populates.
        const myFolders = this.config.folders.filter(f => f.enabled && !f.internal).map(f => ({
          id: f.id, name: f.name, fileCount: this.engine?.folders.get(f.name)?.scanner.getFileCount() || 0,
          direction: f.direction || 'both',
        }));
        writeFrame(client.socket, { type: MSG.FOLDER_LIST, folders: myFolders });

        // Phase 7 P0: gate seeding + sync on engine ready. Marking discovery
        // and running _syncWithPeer against a partial scanner index would let
        // gaps in our file list be interpreted by the peer as deletions. The
        // 15s _quickSyncAllPeers loop will pick this peer up after ready flips.
        if (!this._engineReady) {
          this._logEngineNotReady(peerInfo.deviceName || ip);
          resolve({ success: true, deviceName: peerInfo.deviceName });
          return;
        }

        // Seed per-peer last-known-state on first contact (Phase 3 P0 upgrade
        // path) so the stale-peer guard doesn't preserve every existing file
        // forever on installs that pre-date this feature.
        try {
          const peerId = this._peerIdFor(peerInfo);
          if (peerId) this._maybeSeedPeerKnown(peerId, peerInfo.deviceName);
        } catch (err) {
          console.warn(`Seed peer-known on outbound auth failed: ${err.message}`);
        }

        // Start syncing all folders with this peer
        this._syncWithPeer(peerInfo);
        resolve({ success: true, deviceName: peerInfo.deviceName });
      });

      client.on('message', (msg) => {
        if (msg.type === MSG.NOTIFY) {
          // Phase 7 P0: gate on engine ready. _pullFolderFromPeer with a
          // partial local index would treat unscanned-yet files as missing,
          // and the peer's diff would land them in toDelete. 15s quick-sync
          // re-runs once _engineReady flips true.
          if (!this._engineReady) {
            this._logEngineNotReady(peerInfo.deviceName || ip);
            return;
          }
          // Peer has changes — re-sync that folder
          const folderConfig = this.config.folders.find(f => f.name === msg.folder) ||
                               this.config.folders.find(f => f.name?.toLowerCase() === msg.folder?.toLowerCase());
          if (folderConfig && (folderConfig.direction === 'receive' || folderConfig.direction === 'both')) {
            this._pullFolderFromPeer(peerInfo, folderConfig);
          } else {
            console.log(`NOTIFY ignored: folder=${msg.folder} config=${folderConfig ? 'found,dir=' + folderConfig.direction : 'NOT FOUND'}`);
          }
        } else if (msg.type === MSG.FOLDER_LIST) {
          this.peerFolders.set(peerInfo.deviceName || key, msg.folders);
          this.emit('peer-folders', { peer: peerInfo.deviceName || key, deviceName: peerInfo.deviceName, folders: msg.folders });
        }
      });

      client.on('disconnected', () => {
        peerInfo.connected = false;
        console.log(`Peer disconnected: ${peerInfo.deviceName || key}`);
        this.emit('peer-disconnected', { ip, port, deviceName: peerInfo.deviceName });

        // Auto-reconnect with backoff (only for saved peers)
        const isSaved = (this.config.data.savedPeers || []).some(p => p.ip === ip);
        if (isSaved && !this._stopping) {
          const delay = 10000 + Math.random() * 5000; // 10-15s jitter
          console.log(`Will reconnect to ${peerInfo.deviceName || ip} in ${Math.round(delay / 1000)}s`);
          setTimeout(() => {
            if (this._stopping) return;
            if (peerInfo.connected) return; // Already reconnected (e.g. inbound)
            console.log(`Reconnecting to ${peerInfo.deviceName || ip}...`);
            this.connectToPeer(ip, port).then(r => {
              if (r.success) console.log(`Reconnected to ${peerInfo.deviceName || ip}`);
              else console.log(`Reconnect failed: ${r.message}`);
            }).catch(() => {});
          }, delay);
        }
      });

      client.on('error', (err) => {
        clearTimeout(timeout);
        console.error(`Peer ${key} error: ${err.message}`);
        if (!peerInfo.connected) resolve({ success: false, message: err.message });
      });

      client.connect();
    });
  }

  /**
   * Full bidirectional sync with a connected peer.
   * Pulls what we're missing, pushes what they're missing.
   */
  async _syncWithPeer(peerInfo) {
    if (!peerInfo.connected) return;

    for (const folder of this.config.folders) {
      if (!folder.enabled || folder.internal) continue;
      const dir = folder.direction || 'both';

      try {
        // For 'both', push BEFORE pull: a stale peer index would otherwise
        // make pull infer "delete" for files we just added locally.
        if (dir === 'push' || dir === 'both') {
          console.log(`Pushing ${folder.name} to ${peerInfo.deviceName}...`);
          await this._pushFullFolderToPeer(peerInfo, folder);
        }

        if (dir === 'receive' || dir === 'both') {
          console.log(`Pulling ${folder.name} from ${peerInfo.deviceName}...`);
          await this._pullFolderFromPeer(peerInfo, folder);
          peerInfo.client.send({ type: MSG.SUBSCRIBE, folder: folder.name });
        }
      } catch (err) {
        console.error(`Peer sync failed [${folder.name}]: ${err.message}`);
      }
    }
  }

  /**
   * Push all files the peer is missing for a folder.
   */
  async _pushFullFolderToPeer(peerInfo, folder) {
    if (!peerInfo.client?.authenticated) {
      console.log(`Push to ${peerInfo.deviceName} skipped: not authenticated`);
      return;
    }
    // Phase 7 P0: defense in depth — never push a partial index. If the peer
    // sees gaps in our index, they'll think we deleted those files locally.
    if (!this._engineReady) {
      this._logEngineNotReady(peerInfo.deviceName || peerInfo.ip || 'peer');
      return;
    }
    const engineFolder = this._findEngineFolder(folder.name);
    if (!engineFolder) {
      console.log(`Push to ${peerInfo.deviceName} skipped: engine folder '${folder.name}' not found (engine has: ${this.engine?.getFolderNames().join(', ')})`);
      return;
    }
    if (typeof engineFolder.scanner.isInitialScanComplete === 'function' &&
        !engineFolder.scanner.isInitialScanComplete()) {
      this._logEngineNotReady(peerInfo.deviceName || peerInfo.ip || 'peer');
      return;
    }

    // Phase 9 P0: defense in depth against pushing a partial index. The
    // initial-scan gate above only covers the very first scan; a subsequent
    // scan that was cancelled / aborted / errored flips scanner._stale back
    // to true but leaves initial_scan_complete set. Pushing that index would
    // present gaps to the peer which they'd interpret as deletions.
    if (typeof engineFolder.scanner.isStale === 'function' &&
        engineFolder.scanner.isStale()) {
      this._logScannerStale(folder.name);
      return;
    }

    const localIndex = engineFolder.scanner.getIndex();

    // Send our index, get back what they need
    let response;
    try {
      response = await peerInfo.client.request({
        type: MSG.PUSH_INDEX,
        folder: folder.name,
        index: localIndex.map(f => ({ path: f.path, hash: f.hash, size: f.size, mtime_ms: f.mtime_ms })),
      }, 60000);
    } catch (err) {
      console.error(`Push index to ${peerInfo.deviceName} failed: ${err.message}`);
      return;
    }

    if (response.type === MSG.ERROR) {
      console.error(`Push index error: ${response.message}`);
      return;
    }

    const needed = response.needed || [];

    // Phase 3: every file in our index that the peer DIDN'T ask for is one
    // they already have at our hash (PUSH_INDEX diffs by hash). Record those
    // as peer-known so future deletes can be classified as peer-explicit.
    const peerId = this._peerIdFor(peerInfo);
    if (peerId) {
      try {
        const neededSet = new Set(needed);
        const known = [];
        for (const f of localIndex) {
          if (neededSet.has(f.path)) continue;
          if (!f.hash || String(f.hash).startsWith('fast:')) continue;
          known.push({ path: f.path, hash: f.hash });
        }
        if (known.length > 0) engineFolder.scanner.recordPeerKnownBulk(peerId, known);
      } catch {}
    }

    if (needed.length === 0) {
      console.log(`${folder.name}: peer ${peerInfo.deviceName} is up to date`);
      // Phase 5: index exchange succeeded; that alone proves contact, mark
      // discovery complete so the next pull's diff is trusted.
      if (peerId && typeof engineFolder.scanner.markPeerDiscovered === 'function') {
        try { engineFolder.scanner.markPeerDiscovered(peerId); } catch {}
      }
      return;
    }

    console.log(`${folder.name}: pushing ${needed.length} files to ${peerInfo.deviceName}`);
    const logMsg = `[${new Date().toISOString()}] FULL PUSH: ${needed.length} files for ${folder.name} to ${peerInfo.deviceName}`;
    try { fs.appendFileSync(path.join(this.configDir, 'sync.log'), logMsg + '\n'); } catch {}

    for (const relPath of needed) {
      try {
        const absPath = path.join(engineFolder.path, relPath);
        let stat;
        try { stat = await fsp.stat(absPath); } catch { continue; }
        if (!stat.isFile()) continue;

        const data = await fsp.readFile(absPath);
        const hash = crypto.createHash('sha256').update(data).digest('hex');

        const resp = await peerInfo.client.request({
          type: MSG.FILE_PUSH,
          folder: folder.name,
          path: relPath,
          size: data.length,
          hash,
          mtime_ms: Math.floor(stat.mtimeMs),
        }, 30000);

        if (resp.type === MSG.ERROR || resp.type === 'error') {
          console.error(`FULL PUSH rejected [${relPath}]: ${resp.message}`);
          const errLog = `[${new Date().toISOString()}] PUSH REJECTED: ${relPath} → ${peerInfo.deviceName}: ${resp.message}`;
          try { fs.appendFileSync(path.join(this.configDir, 'sync.log'), errLog + '\n'); } catch {}
          continue;
        }
        if (resp.status === 'skip') {
          // Peer already has it at this hash — record peer-known.
          if (peerId) {
            try { engineFolder.scanner.recordPeerKnown(peerId, relPath, hash); } catch {}
          }
          continue;
        }

        const header = Buffer.alloc(5);
        header.writeUInt32BE(data.length + 1);
        header[4] = 0xFF;
        peerInfo.client.socket.write(Buffer.concat([header, data]));
        writeFrame(peerInfo.client.socket, {
          type: 'transfer_end', folder: folder.name, path: relPath, bytesSent: data.length,
        });

        // Successful push — peer now has this file at this hash.
        if (peerId) {
          try { engineFolder.scanner.recordPeerKnown(peerId, relPath, hash); } catch {}
        }

        // Small delay between files to avoid overwhelming
        await new Promise(r => setTimeout(r, 50));
      } catch (err) {
        console.error(`Push file failed [${relPath}]: ${err.message}`);
      }
    }

    // Phase 5: a successful full push completes discovery for this folder/peer
    // — even if `needed` was empty (peer already had everything), the index
    // exchange itself is enough proof of contact.
    if (peerId && typeof engineFolder.scanner.markPeerDiscovered === 'function') {
      try { engineFolder.scanner.markPeerDiscovered(peerId); } catch {}
    }

    console.log(`${folder.name}: push to ${peerInfo.deviceName} complete`);
  }

  /**
   * Pull a folder from a specific peer.
   */
  async _pullFolderFromPeer(peerInfo, folder, _retryCount = 0) {
    if (!peerInfo.client?.authenticated) return;
    // Phase 7 P0: defense in depth. Inbound NOTIFY/auth handlers already gate
    // on _engineReady, but this function is also reachable from queue drains
    // and explicit syncFolder() calls — never let it run before the index is
    // complete. The 15s quick-sync re-runs after _engineReady flips true.
    if (!this._engineReady) {
      this._logEngineNotReady(peerInfo.deviceName || peerInfo.ip || 'peer');
      return;
    }
    if (!this.engine?.folders.has(folder.name)) return;

    const scanner = this.engine.folders.get(folder.name).scanner;
    // Per-folder defense: a folder added late or with a failed initial scan
    // would otherwise present a partial index to the peer (whose diff would
    // then place real local files into toDelete).
    if (typeof scanner.isInitialScanComplete === 'function' &&
        !scanner.isInitialScanComplete()) {
      this._logEngineNotReady(peerInfo.deviceName || peerInfo.ip || 'peer');
      return;
    }
    const localIndex = scanner.getIndex();

    let response;
    try {
      response = await peerInfo.client.request({
        type: MSG.INDEX_REQUEST,
        folder: folder.name,
        clientIndex: localIndex.map(f => ({ path: f.path, hash: f.hash, size: f.size })),
      }, 60000);
    } catch (err) {
      console.error(`Pull from peer failed [${folder.name}]: ${err.message}`);
      return;
    }

    if (response.type === MSG.ERROR) {
      if (response.message?.includes('stale') && _retryCount < 5) {
        const delay = Math.min(3000 * (_retryCount + 1), 15000);
        console.log(`Peer index stale for ${folder.name}, retrying in ${delay / 1000}s (attempt ${_retryCount + 1}/5)`);
        await new Promise(r => setTimeout(r, delay));
        return this._pullFolderFromPeer(peerInfo, folder, _retryCount + 1);
      }
      console.error(`Pull from peer error [${folder.name}]: ${response.message}`);
      return;
    }
    if (!response.diff) return;

    const { toDownload = [], toDelete: rawToDelete = [], toCopy = [] } = response.diff;
    let toDelete = rawToDelete;
    if (toDownload.length === 0 && toDelete.length === 0 && toCopy.length === 0) return;

    console.log(`Pull from ${peerInfo.deviceName}: ${folder.name} — ${toDownload.length} download, ${toDelete.length} delete`);

    // Deletions — moved to .carbonsync-trash/<date>/ instead of unlinked,
    // so a stale-peer diff can never silently destroy local files.
    // Guard refuses the WHOLE batch if it looks catastrophic. Downloads
    // continue regardless so legit file pushes aren't lost.
    const folderConfig = this.config.folders.find(f => f.name === folder.name) || folder;

    // Stale-peer guard (Phase 3) — classify each proposed deletion against
    // per-peer last-known-state BEFORE the threshold check. Survivors then
    // go through the threshold check and into the trash bucket.
    const peerId = this._peerIdFor(peerInfo);
    const peerLabel = peerInfo.deviceName || 'peer';
    const guarded = this._applyStalePeerGuard({
      scanner, peerId, peerLabel, folderName: folder.name, paths: toDelete,
    });
    toDelete = guarded.delete;

    if (guarded.pushBack.length > 0) {
      // Re-push survivors that we still have but the peer doesn't (or has at
      // a stale hash). Use the existing per-peer queue so it inherits the
      // same retry/backoff path as natural change-driven pushes.
      const changes = guarded.pushBack.map(p => ({ type: 'modify', path: p }));
      try { this._queuePushToPeer(peerInfo, folder.name, changes); } catch {}
    }

    if (this._checkDeletionAllowed({
      folderConfig,
      folderName: folder.name,
      scanner,
      toDeleteCount: toDelete.length,
      peerName: peerInfo.deviceName || 'peer',
    })) {
      for (const relPath of toDelete) {
        const absPath = path.join(folder.path, relPath);
        try {
          this._markRecentlyWritten(folder.name, relPath);
          await moveToTrash(folder.path, relPath, { reason: 'sync-delete' });
          scanner.removeFile(absPath);
          // Mutually accepted — clear the peer-known entry so we don't
          // resurrect this deletion on the next compare.
          if (peerId) {
            try { scanner.clearPeerKnown(peerId, relPath); } catch {}
          }
        } catch (err) {
          if (err.code !== 'ENOENT') console.warn(`Delete failed: ${err.message}`);
        }
      }
    }

    // Downloads
    for (const file of toDownload) {
      try {
        await this._downloadFileFromPeer(peerInfo, folder, file, scanner);
        // Successful download: peer demonstrably has this file at this hash.
        if (peerId && file.hash && !file.hash.startsWith('fast:')) {
          try { scanner.recordPeerKnown(peerId, file.path, file.hash); } catch {}
        }
      } catch (err) {
        console.error(`Download from peer failed [${file.path}]: ${err.message}`);
      }
    }

    // Local copies (move detection) — peer has the same content, different path.
    if (peerId) {
      for (const c of toCopy) {
        if (c.hash && !c.hash.startsWith('fast:')) {
          try { scanner.recordPeerKnown(peerId, c.to, c.hash); } catch {}
        }
      }
      // For files we already had matching the peer (not in any of the diff
      // arrays), record peer-known for our local copy. This catches the
      // "in sync" baseline so future deletions can be classified peer-explicit.
      try {
        const localMap = scanner.getIndexMap();
        const seen = new Set([
          ...toDownload.map(f => f.path),
          ...toCopy.map(c => c.to),
          ...rawToDelete,
        ]);
        const known = [];
        for (const [p, row] of localMap) {
          if (seen.has(p)) continue;
          if (!row.hash || row.hash.startsWith('fast:')) continue;
          known.push({ path: p, hash: row.hash });
        }
        if (known.length > 0) scanner.recordPeerKnownBulk(peerId, known);
      } catch {}
    }

    // Phase 5: a successful pull round-trip means we trust this peer's diffs
    // from now on. Subsequent syncs use the existing classifyDeletion guard
    // against per-peer last-known-state instead of preserving everything.
    if (peerId && typeof scanner.markPeerDiscovered === 'function') {
      try { scanner.markPeerDiscovered(peerId); } catch {}
    }
  }

  async _downloadFileFromPeer(peerInfo, folder, fileInfo, scanner) {
    const absPath = path.join(folder.path, fileInfo.path);
    const tmpPath = absPath + '.carbonsync.tmp';
    await fsp.mkdir(path.dirname(absPath), { recursive: true });

    try {
      const response = await peerInfo.client.request({
        type: MSG.BLOCK_REQUEST,
        folder: folder.name,
        path: fileInfo.path,
      }, 300000);

      if (response.type === MSG.ERROR) throw new Error(response.message);

      let fileData;
      if (response.streaming || response.binaryPromise) {
        const chunks = await response.binaryPromise;
        fileData = Buffer.concat(chunks);
      } else {
        fileData = await new Promise((resolve, reject) => {
          const timeout = setTimeout(() => reject(new Error('Binary timeout')), 120000);
          peerInfo.client.once('binary', (data) => { clearTimeout(timeout); resolve(data); });
        });
      }

      const hash = crypto.createHash('sha256').update(fileData).digest('hex');
      if (fileInfo.hash && !fileInfo.hash.startsWith('fast:') && hash !== fileInfo.hash) {
        throw new Error('Hash mismatch');
      }

      // Phase 6 P0: shrink-guard. Refuse to silently overwrite a substantial
      // local file with an empty/drastically-shrunk peer copy (corrupted,
      // partially-written, ransomware, etc.). On preserve the helper writes
      // the incoming bytes to a .shrink-blocked sidecar and we bail without
      // renaming — existing file untouched.
      const folderConfig = this.config.folders.find(f => f.name === folder.name) || folder;
      const proceed = await this._checkShrinkOverwrite({
        folderConfig,
        folderName: folder.name,
        absPath,
        relPath: fileInfo.path,
        tmpPath: null, // tmp not yet written
        fileData,
        peerName: peerInfo?.deviceName || 'peer',
      });
      if (!proceed) return;

      this._markRecentlyWritten(folder.name, fileInfo.path);
      await fsp.writeFile(tmpPath, fileData);
      if (fileInfo.mtime_ms) {
        const mtime = new Date(fileInfo.mtime_ms);
        await fsp.utimes(tmpPath, mtime, mtime);
      }
      await fsp.rename(tmpPath, absPath);
      await scanner.updateFile(absPath);
    } catch (err) {
      try { await fsp.unlink(tmpPath); } catch {}
      throw err;
    }
  }

  /**
   * Get all connected peers.
   */
  _getRemoteFolders() {
    // Match by folder ID (stable across renames), fall back to name
    const myFolderIds = new Set(this.config.folders.map(f => f.id).filter(Boolean));
    const myFolderNames = new Set(this.config.folders.map(f => f.name));
    const peers = this.config.data.peers || {};
    const seen = new Map(); // folderId or name -> entry
    for (const [peerKey, folders] of this.peerFolders) {
      const peerInfo = this.peerConnections.get(peerKey);
      let deviceName = peerInfo?.deviceName || '';
      if (!deviceName) {
        const ip = peerKey.split(':')[0];
        for (const c of (this.transport?.getConnectedClients() || [])) {
          if (c.ip === ip) { deviceName = c.deviceName; break; }
        }
      }
      deviceName = deviceName || peerKey;
      const friendlyName = peers[deviceName] || deviceName;

      for (const f of folders) {
        if (f.internal) continue;
        // Skip if we already have this folder (match by ID first, then name)
        if (f.id && myFolderIds.has(f.id)) continue;
        if (myFolderNames.has(f.name)) continue;
        const key = f.id || f.name;
        if (seen.has(key)) continue;

        // Auto-update local folder name if ID matches but name changed (rename sync)
        if (f.id) {
          const localFolder = this.config.folders.find(lf => lf.id === f.id);
          if (localFolder && localFolder.name !== f.name) {
            console.log(`Folder renamed by peer: ${localFolder.name} → ${f.name}`);
            localFolder.name = f.name;
            this.config.save();
          }
        }

        seen.set(key, {
          id: f.id,
          name: f.name,
          fileCount: f.fileCount || 0,
          direction: f.direction || 'both',
          deviceName: friendlyName,
        });
      }
    }
    return Array.from(seen.values());
  }

  getConnectedPeers() {
    const peers = [];
    for (const [key, info] of this.peerConnections) {
      const [ip, portStr] = key.split(':');
      peers.push({
        address: key,
        ip,
        port: parseInt(portStr) || this.config.port,
        hostname: info.deviceName || ip,
        friendlyName: info.deviceName || ip,
        deviceName: info.deviceName,
        connected: info.connected,
      });
    }
    return peers;
  }

  // ---- Pull (device ← hub) ----

  async _pullFolder(folder, _retryCount = 0) {
    if (!this.hubConnection?.authenticated) return;
    // Phase 7 P0: defense in depth — see _pullFolderFromPeer.
    if (!this._engineReady) {
      this._logEngineNotReady(this.config.hubAddress || 'hub');
      return;
    }
    if (!this.engine?.folders.has(folder.name)) return;

    const scanner = this.engine.folders.get(folder.name).scanner;
    if (typeof scanner.isInitialScanComplete === 'function' &&
        !scanner.isInitialScanComplete()) {
      this._logEngineNotReady(this.config.hubAddress || 'hub');
      return;
    }
    const localIndex = scanner.getIndex();

    const response = await this.hubConnection.request({
      type: MSG.INDEX_REQUEST,
      folder: folder.name,
      clientIndex: localIndex.map(f => ({ path: f.path, hash: f.hash, size: f.size })),
    }, 60000);

    if (response.type === MSG.ERROR) {
      if (response.message?.includes('stale') && _retryCount < 5) {
        const delay = Math.min(3000 * (_retryCount + 1), 15000);
        console.log(`Hub index stale for ${folder.name}, retrying in ${delay / 1000}s (attempt ${_retryCount + 1}/5)`);
        await new Promise(r => setTimeout(r, delay));
        return this._pullFolder(folder, _retryCount + 1);
      }
      console.error(`Pull index error for ${folder.name}: ${response.message}`);
      return;
    }

    const diff = response.diff;
    if (!diff) return;

    const { toDownload = [], toDelete: rawToDelete = [], toCopy = [] } = diff;
    let toDelete = rawToDelete;

    if (toDownload.length === 0 && toDelete.length === 0 && toCopy.length === 0) {
      console.log(`${folder.name}: in sync (pull)`);
      return;
    }

    console.log(`${folder.name} pull: ${toDownload.length} download, ${toCopy.length} copy, ${toDelete.length} delete`);

    // Deletions — see _pullFolderFromPeer comment; route through trash bucket
    // and apply the same threshold guard. Hub identified just as 'hub' since
    // we don't track its hostname here.
    const hubPeerName = this.config.hubAddress || 'hub';
    const peerId = this._hubPeerId();

    // Stale-peer guard (Phase 3)
    const guarded = this._applyStalePeerGuard({
      scanner, peerId, peerLabel: hubPeerName,
      folderName: folder.name, paths: toDelete,
    });
    toDelete = guarded.delete;

    if (guarded.pushBack.length > 0 && this.hubConnection?.authenticated) {
      // Re-push survivors to the hub via the existing debounced push queue.
      const changes = guarded.pushBack.map(p => ({ type: 'modify', path: p }));
      try { this._queuePush(folder.name, changes); } catch {}
    }

    if (this._checkDeletionAllowed({
      folderConfig: folder,
      folderName: folder.name,
      scanner,
      toDeleteCount: toDelete.length,
      peerName: hubPeerName,
    })) {
      for (const relPath of toDelete) {
        const absPath = path.join(folder.path, relPath);
        try {
          this._markRecentlyWritten(folder.name, relPath);
          await moveToTrash(folder.path, relPath, { reason: 'sync-delete' });
          scanner.removeFile(absPath);
          if (peerId) {
            try { scanner.clearPeerKnown(peerId, relPath); } catch {}
          }
        } catch (err) {
          if (err.code !== 'ENOENT') console.warn(`Delete failed [${relPath}]: ${err.message}`);
        }
      }
    }

    // Local copies (move detection)
    for (const copy of toCopy) {
      try {
        const fromAbs = path.join(folder.path, copy.from);
        const toAbs = path.join(folder.path, copy.to);
        await fsp.mkdir(path.dirname(toAbs), { recursive: true });
        this._markRecentlyWritten(folder.name, copy.to);
        await fsp.copyFile(fromAbs, toAbs);
        await scanner.updateFile(toAbs);
        if (peerId && copy.hash && !copy.hash.startsWith('fast:')) {
          try { scanner.recordPeerKnown(peerId, copy.to, copy.hash); } catch {}
        }
      } catch {
        toDownload.push({ path: copy.to, size: copy.size, hash: copy.hash, mtime_ms: copy.mtime_ms });
      }
    }

    // Downloads
    for (const file of toDownload) {
      try {
        await this._downloadFile(folder, file, scanner);
        if (peerId && file.hash && !file.hash.startsWith('fast:')) {
          try { scanner.recordPeerKnown(peerId, file.path, file.hash); } catch {}
        }
      } catch (err) {
        console.error(`Download failed [${file.path}]: ${err.message}`);
      }
    }

    // "In-sync" baseline: any file we had that wasn't in toDownload/toCopy/
    // toDelete is presumably also on the hub at the same hash — record so
    // future deletes can be classified.
    if (peerId) {
      try {
        const localMap = scanner.getIndexMap();
        const seen = new Set([
          ...toDownload.map(f => f.path),
          ...toCopy.map(c => c.to),
          ...rawToDelete,
        ]);
        const known = [];
        for (const [p, row] of localMap) {
          if (seen.has(p)) continue;
          if (!row.hash || row.hash.startsWith('fast:')) continue;
          known.push({ path: p, hash: row.hash });
        }
        if (known.length > 0) scanner.recordPeerKnownBulk(peerId, known);
      } catch {}
    }

    // Phase 5: a successful hub pull completes discovery for this folder/peer.
    if (peerId && typeof scanner.markPeerDiscovered === 'function') {
      try { scanner.markPeerDiscovered(peerId); } catch {}
    }

    console.log(`${folder.name}: pull complete`);
  }

  async _downloadFile(folder, fileInfo, scanner) {
    const absPath = path.join(folder.path, fileInfo.path);
    const tmpPath = absPath + '.carbonsync.tmp';

    await fsp.mkdir(path.dirname(absPath), { recursive: true });

    try {
      const response = await this.hubConnection.request({
        type: MSG.BLOCK_REQUEST,
        folder: folder.name,
        path: fileInfo.path,
      }, 300000);

      if (response.type === MSG.ERROR) throw new Error(response.message);

      let fileData;
      if (response.streaming || response.binaryPromise) {
        const chunks = await response.binaryPromise;
        fileData = Buffer.concat(chunks);
      } else {
        fileData = await new Promise((resolve, reject) => {
          const timeout = setTimeout(() => reject(new Error('Binary timeout')), 120000);
          this.hubConnection.once('binary', (data) => { clearTimeout(timeout); resolve(data); });
        });
      }

      // Verify hash
      const hash = crypto.createHash('sha256').update(fileData).digest('hex');
      if (fileInfo.hash && !fileInfo.hash.startsWith('fast:') && hash !== fileInfo.hash) {
        throw new Error(`Hash mismatch`);
      }

      // Phase 6 P0: shrink-guard before atomic rename.
      const folderConfig = this.config.folders.find(f => f.name === folder.name) || folder;
      const proceed = await this._checkShrinkOverwrite({
        folderConfig,
        folderName: folder.name,
        absPath,
        relPath: fileInfo.path,
        tmpPath: null, // tmp not yet written
        fileData,
        peerName: this.config.hubAddress || 'hub',
      });
      if (!proceed) return;

      this._markRecentlyWritten(folder.name, fileInfo.path);
      await fsp.writeFile(tmpPath, fileData);
      if (fileInfo.mtime_ms) {
        const mtime = new Date(fileInfo.mtime_ms);
        await fsp.utimes(tmpPath, mtime, mtime);
      }
      await fsp.rename(tmpPath, absPath);
      await scanner.updateFile(absPath);
    } catch (err) {
      try { await fsp.unlink(tmpPath); } catch {}
      throw err;
    }
  }

  // ---- Push (device → hub) ----

  _queuePush(folderName, changes) {
    if (!this._pushQueues.has(folderName)) this._pushQueues.set(folderName, new Set());
    const queue = this._pushQueues.get(folderName);

    for (const change of changes) {
      queue.add(JSON.stringify({ type: change.type, path: change.path }));
    }

    // Debounce: wait for more changes before pushing
    if (this._pushTimers.has(folderName)) clearTimeout(this._pushTimers.get(folderName));
    this._pushTimers.set(folderName, setTimeout(() => {
      this._processPushQueue(folderName);
    }, PUSH_DEBOUNCE_MS));
  }

  async _processPushQueue(folderName) {
    if (!this.hubConnection?.authenticated) return;

    const queue = this._pushQueues.get(folderName);
    if (!queue || queue.size === 0) return;

    const changes = [...queue].map(s => JSON.parse(s));
    queue.clear();

    const folder = this.config.folders.find(f => f.name === folderName);
    if (!folder) return;

    console.log(`Pushing ${changes.length} changes for ${folderName}`);

    for (const change of changes) {
      try {
        if (change.type === 'delete') {
          await this.hubConnection.request({
            type: MSG.FILE_DELETE_PUSH,
            folder: folderName,
            path: change.path,
          }, 10000);
        } else {
          await this._pushFile(folder, change.path);
        }
      } catch (err) {
        console.error(`Push failed [${change.path}]: ${err.message}`);
      }
    }
  }

  async _pushFile(folder, relPath) {
    const absPath = path.join(folder.path, relPath);

    let stat;
    try { stat = await fsp.stat(absPath); } catch { return; }
    if (!stat.isFile()) return;

    const hash = crypto.createHash('sha256');
    const data = await fsp.readFile(absPath);
    const fileHash = hash.update(data).digest('hex');

    // Send metadata
    const response = await this.hubConnection.request({
      type: MSG.FILE_PUSH,
      folder: folder.name,
      path: relPath,
      size: data.length,
      hash: fileHash,
      mtime_ms: Math.floor(stat.mtimeMs),
    }, 30000);

    if (response.status === 'skip') return; // Hub already has this version

    // Send binary data
    const header = Buffer.alloc(5);
    header.writeUInt32BE(data.length + 1);
    header[4] = 0xFF;
    this.hubConnection.socket.write(Buffer.concat([header, data]));

    // Send transfer end
    writeFrame(this.hubConnection.socket, {
      type: 'transfer_end',
      folder: folder.name,
      path: relPath,
      bytesSent: data.length,
    });
  }

  /**
   * Full push: send index, get diff, push missing files.
   */
  async _pushFullFolder(folder) {
    if (!this.hubConnection?.authenticated) return;
    // Phase 7 P0: defense in depth — see _pushFullFolderToPeer.
    if (!this._engineReady) {
      this._logEngineNotReady(this.config.hubAddress || 'hub');
      return;
    }
    if (!this.engine?.folders.has(folder.name)) return;

    const scanner = this.engine.folders.get(folder.name).scanner;
    if (typeof scanner.isInitialScanComplete === 'function' &&
        !scanner.isInitialScanComplete()) {
      this._logEngineNotReady(this.config.hubAddress || 'hub');
      return;
    }
    // Phase 9 P0: scanner-stale guard — see _pushFullFolderToPeer.
    if (typeof scanner.isStale === 'function' && scanner.isStale()) {
      this._logScannerStale(folder.name);
      return;
    }
    const localIndex = scanner.getIndex();

    const response = await this.hubConnection.request({
      type: MSG.PUSH_INDEX,
      folder: folder.name,
      index: localIndex.map(f => ({ path: f.path, hash: f.hash, size: f.size, mtime_ms: f.mtime_ms })),
    }, 60000);

    if (response.type === MSG.ERROR) {
      console.error(`Push index error: ${response.message}`);
      return;
    }

    const needed = response.needed || [];
    const toDelete = response.toDelete || [];

    const hubPeerId = this._hubPeerId();

    if (needed.length === 0 && toDelete.length === 0) {
      console.log(`${folder.name}: in sync (push)`);
      // Phase 5: index exchange itself proves contact; mark discovered.
      if (hubPeerId && typeof scanner.markPeerDiscovered === 'function') {
        try { scanner.markPeerDiscovered(hubPeerId); } catch {}
      }
      return;
    }

    console.log(`${folder.name} push: ${needed.length} files needed by hub, ${toDelete.length} deletions`);

    for (const relPath of needed) {
      try {
        await this._pushFile(folder, relPath);
      } catch (err) {
        console.error(`Push failed [${relPath}]: ${err.message}`);
      }
    }

    // Push deletions
    for (const relPath of toDelete) {
      try {
        await this.hubConnection.request({
          type: MSG.FILE_DELETE_PUSH,
          folder: folder.name,
          path: relPath,
        }, 10000);
      } catch {}
    }

    // Phase 5: a successful full push completes discovery for this folder/hub.
    if (hubPeerId && typeof scanner.markPeerDiscovered === 'function') {
      try { scanner.markPeerDiscovered(hubPeerId); } catch {}
    }

    console.log(`${folder.name}: push complete`);
  }

  /**
   * Push changes to a specific peer (debounced).
   */
  _queuePushToPeer(peerInfo, folderName, changes) {
    const key = `peer:${peerInfo.ip}:${peerInfo.port}:${folderName}`;
    if (!this._pushQueues.has(key)) this._pushQueues.set(key, new Set());
    const queue = this._pushQueues.get(key);

    for (const change of changes) {
      queue.add(JSON.stringify({ type: change.type, path: change.path }));
    }

    if (this._pushTimers.has(key)) clearTimeout(this._pushTimers.get(key));
    this._pushTimers.set(key, setTimeout(async () => {
      if (!peerInfo.connected || !peerInfo.client?.authenticated) {
        console.log(`Push skipped: ${peerInfo.deviceName} not connected/authenticated for ${folderName}`);
        return;
      }
      const items = [...queue].map(s => JSON.parse(s));
      queue.clear();

      const folder = this.config.folders.find(f => f.name === folderName);
      if (!folder) {
        console.log(`Push skipped: folder ${folderName} not found in config`);
        return;
      }

      const pushLog = `[${new Date().toISOString()}] PUSH: ${items.length} files to ${peerInfo.deviceName} for ${folderName}`;
      console.log(pushLog);
      try { require('fs').appendFileSync(require('path').join(require('os').homedir(), '.carbonsync', 'sync.log'), pushLog + '\n'); } catch {}
      for (const change of items) {
        try {
          if (change.type === 'delete') {
            await peerInfo.client.request({
              type: MSG.FILE_DELETE_PUSH, folder: folderName, path: change.path,
            }, 10000);
          } else {
            const absPath = path.join(folder.path, change.path);
            let stat;
            try { stat = await fsp.stat(absPath); } catch { continue; }
            if (!stat.isFile()) continue;

            const data = await fsp.readFile(absPath);
            const hash = crypto.createHash('sha256').update(data).digest('hex');

            const resp = await peerInfo.client.request({
              type: MSG.FILE_PUSH, folder: folderName, path: change.path,
              size: data.length, hash, mtime_ms: Math.floor(stat.mtimeMs),
            }, 30000);

            if (resp.type === 'error') {
              const errLog = `[${new Date().toISOString()}] PUSH REJECTED: ${change.path} → ${peerInfo.deviceName}: ${resp.message}`;
              console.error(errLog);
              try { fs.appendFileSync(path.join(os.homedir(), '.carbonsync', 'sync.log'), errLog + '\n'); } catch {}
              continue;
            }
            if (resp.status === 'skip') continue;

            const header = Buffer.alloc(5);
            header.writeUInt32BE(data.length + 1);
            header[4] = 0xFF;
            peerInfo.client.socket.write(Buffer.concat([header, data]));
            writeFrame(peerInfo.client.socket, {
              type: 'transfer_end', folder: folderName, path: change.path, bytesSent: data.length,
            });
          }
        } catch (err) {
          console.error(`Push to peer failed [${change.path}]: ${err.message}`);
        }
      }
    }, PUSH_DEBOUNCE_MS));
  }

  // ---- Handle messages from connected devices ----

  async _handleMessage(client, msg) {
    try {
      switch (msg.type) {
        case MSG.INDEX_REQUEST:
          await this._handleIndexRequest(client, msg);
          break;
        case MSG.BLOCK_REQUEST:
          await this._handleBlockRequest(client, msg);
          break;
        case MSG.FILE_PUSH:
          this._handleFilePushStart(client, msg);
          break;
        case MSG.FILE_DELETE_PUSH:
          await this._handleFileDeletePush(client, msg);
          break;
        case MSG.PUSH_INDEX:
          this._handlePushIndex(client, msg);
          break;
        case MSG.SUBSCRIBE:
          client.subscriptions.add(msg.folder);
          break;
        case MSG.FILE_DONE:
          this._trackSyncProgress(client, msg);
          break;
        case 'sync_complete':
          this._trackSyncComplete(client, msg);
          break;
        case 'set_excludes':
          this._handleSetExcludes(client, msg);
          break;
        case 'transfer_end':
          await this._handleTransferEnd(client, msg);
          break;
        case MSG.FOLDER_LIST:
          // Inbound client sent us their folder list — key by deviceName to deduplicate
          if (client.deviceName) {
            this.peerFolders.set(client.deviceName, msg.folders || []);
            this.emit('peer-folders', { peer: client.deviceName, deviceName: client.deviceName, folders: msg.folders || [] });
          }
          break;
        case MSG.HASH_CHECK: {
          // Respond with root hashes for all enabled folders
          const hashes = {};
          for (const f of this.config.folders) {
            if (!f.enabled || f.internal) continue;
            const ef = this._findEngineFolder(f.name);
            if (ef) hashes[f.name] = ef.scanner.getRootHash();
          }
          writeFrame(client.socket, { type: MSG.HASH_CHECK_RESPONSE, hashes, _requestId: msg._requestId });
          break;
        }
        case MSG.PING:
          writeFrame(client.socket, { type: MSG.PONG, _requestId: msg._requestId });
          break;
        default:
          this.emit('message', client, msg);
      }
    } catch (err) {
      console.error(`Error from ${client.deviceName}: ${err.message}`);
      writeFrame(client.socket, { type: MSG.ERROR, message: 'Server error', _requestId: msg._requestId });
    }
  }

  // ---- Push reception (hub receiving files from devices) ----

  _findEngineFolder(name) {
    // Direct match
    if (this.engine?.folders.has(name)) return this.engine.folders.get(name);
    // Match by config name → path → engine (case-insensitive for Windows)
    const cfgFolder = this.config.folders.find(f => f.name === name);
    if (cfgFolder) {
      const cfgPath = path.resolve(cfgFolder.path).toLowerCase();
      for (const [, ef] of this.engine?.folders || new Map()) {
        if (path.resolve(ef.path).toLowerCase() === cfgPath) return ef;
      }
    }
    // Last resort: try matching engine folder names case-insensitively
    const nameLower = name.toLowerCase();
    for (const [engineName, ef] of this.engine?.folders || new Map()) {
      if (engineName.toLowerCase() === nameLower) return ef;
    }
    return null;
  }

  _handleFilePushStart(client, msg) {
    const folder = this._findEngineFolder(msg.folder);
    if (!folder) {
      const errMsg = `FILE_PUSH rejected: unknown folder '${msg.folder}' (engine has: ${[...(this.engine?.getFolderNames() || [])].join(', ')})`;
      console.log(errMsg);
      try { fs.appendFileSync(path.join(this.configDir, 'sync.log'), `[${new Date().toISOString()}] ${errMsg}\n`); } catch {}
      writeFrame(client.socket, { type: MSG.ERROR, message: 'Unknown folder', _requestId: msg._requestId });
      return;
    }

    // Check if we already have this exact file
    const existing = folder.scanner.getFile(msg.path);
    if (existing && existing.hash === msg.hash) {
      writeFrame(client.socket, { type: MSG.FILE_PUSH_ACK, status: 'skip', _requestId: msg._requestId });
      return;
    }

    // Set up binary collector for this client
    client._pushPending = {
      folder: msg.folder,
      path: msg.path,
      hash: msg.hash,
      size: msg.size,
      mtime_ms: msg.mtime_ms,
      chunks: [],
      requestId: msg._requestId,
      existingMtime: existing?.mtime_ms || 0,
    };

    writeFrame(client.socket, { type: MSG.FILE_PUSH_ACK, status: 'ready', _requestId: msg._requestId });
  }

  _handleBinary(client, data) {
    if (client._pushPending) {
      client._pushPending.chunks.push(data);
    }
  }

  async _handleTransferEnd(client, msg) {
    const pending = client._pushPending;
    if (!pending) return;
    client._pushPending = null;

    const folder = this._findEngineFolder(pending.folder);
    if (!folder) return;

    const fileData = Buffer.concat(pending.chunks);

    // Verify hash
    const hash = crypto.createHash('sha256').update(fileData).digest('hex');
    if (hash !== pending.hash) {
      console.warn(`Push hash mismatch for ${pending.path} from ${client.deviceName}`);
      return;
    }

    const absPath = path.join(folder.path, pending.path);
    const tmpPath = absPath + '.carbonsync.tmp';

    try {
      // Phase 6 P0: shrink-guard runs FIRST — before the mtime-based
      // conflict logic — because a peer with a corrupted/truncated copy
      // may also be the "newer" side by mtime, and we must never let an
      // empty/drastically-shrunk file overwrite a substantial local one.
      const folderConfig = this.config.folders.find(f => f.name === pending.folder) ||
                           this.config.folders.find(f => f.path === folder.path) ||
                           folder;
      const proceed = await this._checkShrinkOverwrite({
        folderConfig,
        folderName: pending.folder,
        absPath,
        relPath: pending.path,
        tmpPath,
        fileData,
        peerName: client.deviceName || 'peer',
      });
      if (!proceed) return;

      // Conflict check: if hub has a newer version, backup the loser
      const existing = folder.scanner.getFile(pending.path);
      if (existing && existing.hash !== pending.hash) {
        if (existing.mtime_ms > pending.mtime_ms) {
          // Hub's version is newer — save incoming as conflict
          const ext = path.extname(pending.path);
          const base = pending.path.slice(0, pending.path.length - ext.length);
          const conflictPath = path.join(folder.path, `${base}.conflict.${client.deviceName}.${Date.now()}${ext}`);
          await fsp.mkdir(path.dirname(conflictPath), { recursive: true });
          await fsp.writeFile(conflictPath, fileData);
          console.log(`Conflict: ${pending.path} — hub version newer, incoming saved as conflict`);
          return;
        } else {
          // Incoming is newer — backup hub's version
          const ext = path.extname(pending.path);
          const base = pending.path.slice(0, pending.path.length - ext.length);
          const conflictPath = path.join(folder.path, `${base}.conflict.hub.${Date.now()}${ext}`);
          try { await fsp.copyFile(absPath, conflictPath); } catch {}
        }
      }

      // Write file
      await fsp.mkdir(path.dirname(absPath), { recursive: true });
      this._markRecentlyWritten(pending.folder, pending.path);
      await fsp.writeFile(tmpPath, fileData);
      if (pending.mtime_ms) {
        const mtime = new Date(pending.mtime_ms);
        await fsp.utimes(tmpPath, mtime, mtime);
      }
      await fsp.rename(tmpPath, absPath);
      await folder.scanner.updateFile(absPath);

      // Record peer-known: the sending peer demonstrably has this file at
      // this hash (they just gave it to us). Phase 3 P0 fix.
      const peerId = this._peerIdForClient(client);
      if (peerId && pending.hash && !pending.hash.startsWith('fast:')) {
        try { folder.scanner.recordPeerKnown(peerId, pending.path, pending.hash); } catch {}
      }

      console.log(`Received push: ${pending.folder}/${pending.path} from ${client.deviceName}`);

      // Notify other connected clients (receivers)
      this.transport.broadcast({
        type: MSG.NOTIFY,
        folder: pending.folder,
        changes: [{ type: 'modify', path: pending.path }],
      });

    } catch (err) {
      console.error(`Push write failed [${pending.path}]: ${err.message}`);
      try { await fsp.unlink(tmpPath); } catch {}
    }
  }

  async _handleFileDeletePush(client, msg) {
    const folder = this._findEngineFolder(msg.folder);
    if (!folder) {
      writeFrame(client.socket, { type: MSG.FILE_DELETE_ACK, _requestId: msg._requestId });
      return;
    }

    const peerName = client.deviceName || client.ip || 'peer';
    const folderConfig = this.config.folders.find(f => f.name === msg.folder) || folder;

    // Stale-peer guard (Phase 3) — classify against per-peer last-known-state
    // before the threshold check. If the peer never had this file (or has it
    // at a different hash than we do), preserve and log; don't delete.
    const peerId = this._peerIdForClient(client);

    // Phase 5: discovery-first sync. Before we trust this peer's "delete X"
    // for any file, we need at least one complete round-trip with them. On
    // first contact, preserve unconditionally and log PRESERVED-DISCOVERY.
    if (peerId && typeof folder.scanner.isPeerDiscovered === 'function' &&
        !folder.scanner.isPeerDiscovered(peerId)) {
      const line = `[${new Date().toISOString()}] PRESERVED-DISCOVERY: ${msg.folder}/${msg.path} — first-sync caution (peer=${peerName})`;
      console.warn(line);
      try { fs.appendFileSync(path.join(this.configDir, 'sync.log'), line + '\n'); } catch {}
      this._recordPreservedDeletion({
        folder: msg.folder, peer: peerName,
        path: msg.path, reason: 'discovery-first-sync', timestamp: Date.now(),
      });
      writeFrame(client.socket, { type: MSG.FILE_DELETE_ACK, _requestId: msg._requestId });
      return;
    }

    const localRow = folder.scanner.getFile(msg.path);
    const peerKnown = peerId ? folder.scanner.getPeerKnown(peerId, msg.path) : null;
    const cls = classifyDeletion({
      peerKnown,
      currentLocalHash: localRow ? localRow.hash : null,
    });
    if (cls.action === 'preserve') {
      const line = `[${new Date().toISOString()}] PRESERVED: ${msg.folder}/${msg.path} — ${cls.reason} (peer=${peerName})`;
      console.warn(line);
      try { fs.appendFileSync(path.join(this.configDir, 'sync.log'), line + '\n'); } catch {}
      this._recordPreservedDeletion({
        folder: msg.folder, peer: peerName,
        path: msg.path, reason: cls.reason, timestamp: Date.now(),
      });
      writeFrame(client.socket, { type: MSG.FILE_DELETE_ACK, _requestId: msg._requestId });
      return;
    }

    // Single-file path: the per-batch guard would be a no-op here (1 file is
    // always under the 50/25% defaults). Aggregate single-file deletes from
    // the same peer in a 30s window so a flood of one-at-a-time deletes still
    // trips the guard.
    const WINDOW_MS = 30_000;
    const key = `${peerName}::${msg.folder}`;
    const now = Date.now();
    let entry = this._singleDeleteWindows.get(key);
    if (!entry || now - entry.windowStart > WINDOW_MS) {
      entry = { count: 0, windowStart: now };
    }
    entry.count += 1;
    this._singleDeleteWindows.set(key, entry);

    if (this._checkDeletionAllowed({
      folderConfig,
      folderName: msg.folder,
      scanner: folder.scanner,
      toDeleteCount: entry.count,
      peerName,
    }) === false) {
      // Rejected by guard — ack so the peer doesn't hang, but skip the trash
      // move and the broadcast.
      writeFrame(client.socket, { type: MSG.FILE_DELETE_ACK, _requestId: msg._requestId });
      return;
    }

    const absPath = path.join(folder.path, msg.path);
    try {
      this._markRecentlyWritten(msg.folder, msg.path);
      // Route incoming delete pushes through the trash bucket so a peer with
      // a stale view of our state can't silently destroy files here either.
      await moveToTrash(folder.path, msg.path, { reason: 'sync-delete-push' });
      folder.scanner.removeFile(absPath);
      if (peerId) {
        try { folder.scanner.clearPeerKnown(peerId, msg.path); } catch {}
      }
      console.log(`Received delete push: ${msg.folder}/${msg.path}`);

      this.transport.broadcast({
        type: MSG.NOTIFY,
        folder: msg.folder,
        changes: [{ type: 'delete', path: msg.path }],
      });
    } catch {}

    writeFrame(client.socket, { type: MSG.FILE_DELETE_ACK, _requestId: msg._requestId });
  }

  _handlePushIndex(client, msg) {
    // Phase 7 P0: gate on engine ready. Computing the diff against a partial
    // local index sends back a `toDelete` list of files we just haven't
    // scanned yet — the peer would interpret that as authoritative and
    // delete real files. Reply with ERROR so the peer's existing retry/
    // backoff path (already used for "stale" errors) re-issues the request
    // once we're ready; the peer's 15s quick-sync also covers the gap.
    if (!this._engineReady) {
      this._logEngineNotReady(client.deviceName || client.ip || 'inbound-client');
      writeFrame(client.socket, { type: MSG.ERROR, message: 'Index stale', _requestId: msg._requestId });
      return;
    }
    const folder = this._findEngineFolder(msg.folder);
    if (!folder) {
      writeFrame(client.socket, { type: MSG.ERROR, message: 'Unknown folder', _requestId: msg._requestId });
      return;
    }

    // Per-folder defense in depth: even if the engine reports ready, a
    // folder added late or that failed its initial scan must not contribute
    // a partial index to the peer's diff.
    if (typeof folder.scanner.isInitialScanComplete === 'function' &&
        !folder.scanner.isInitialScanComplete()) {
      this._logEngineNotReady(client.deviceName || client.ip || 'inbound-client');
      writeFrame(client.socket, { type: MSG.ERROR, message: 'Index stale', _requestId: msg._requestId });
      return;
    }

    // Phase 9 P0: scanner-stale guard. A scan in-flight, cancelled, or
    // failing midway leaves _stale=true with a possibly-partial index in
    // place. Replying with a toDelete computed against that index tells the
    // peer to delete files we simply haven't finished scanning yet. Reply
    // 'Index stale' so the peer's existing retry path re-issues shortly.
    if (typeof folder.scanner.isStale === 'function' && folder.scanner.isStale()) {
      this._logScannerStale(msg.folder);
      writeFrame(client.socket, { type: MSG.ERROR, message: 'Index stale', _requestId: msg._requestId });
      return;
    }

    const hubIndex = folder.scanner.getIndexMap();
    const clientIndex = msg.index || [];

    // Record peer-known state: every entry in the pushed index is something
    // the peer demonstrably has at that hash right now. Phase 3 P0 fix.
    const peerId = this._peerIdForClient(client);
    if (peerId && clientIndex.length > 0) {
      const known = clientIndex
        .filter(e => e && e.path && e.hash && !String(e.hash).startsWith('fast:'))
        .map(e => ({ path: e.path, hash: e.hash }));
      if (known.length > 0) {
        try { folder.scanner.recordPeerKnownBulk(peerId, known); } catch {}
      }
    }

    // What does the hub need from this device?
    const needed = [];
    const clientMap = new Map();
    for (const entry of clientIndex) clientMap.set(entry.path, entry);

    for (const [relPath, clientEntry] of clientMap) {
      const hubEntry = hubIndex.get(relPath);
      if (!hubEntry) {
        // Hub doesn't have this file at all — always request it
        needed.push(relPath);
      } else if (hubEntry.hash !== clientEntry.hash) {
        // Both sides have the file but hashes differ.
        // Skip if either side has a fast: hash — can't reliably compare
        if (clientEntry.hash.startsWith('fast:') || hubEntry.hash.startsWith('fast:')) {
          // Fall back to size comparison: if same size, assume same file
          if (clientEntry.size === hubEntry.size) continue;
        }
        needed.push(relPath);
      }
    }

    // Files on hub that client doesn't have (client deleted them) — push deletions
    const toDelete = [];
    for (const [relPath] of hubIndex) {
      if (!clientMap.has(relPath)) toDelete.push(relPath);
    }

    writeFrame(client.socket, {
      type: MSG.PUSH_DIFF,
      folder: msg.folder,
      needed,
      toDelete,
      _requestId: msg._requestId,
    });
  }

  // ---- Existing handlers (from server.js) ----

  async _handleIndexRequest(client, msg) {
    // Phase 7 P0: gate on engine ready. Returning our index (or a diff
    // computed against it) before the initial scan completes leaks a
    // partial picture to the peer, who would compute deletions against
    // gaps and act on them. Reply 'Index stale' which the peer's existing
    // retry-with-backoff path already handles.
    if (!this._engineReady) {
      this._logEngineNotReady(client.deviceName || client.ip || 'inbound-client');
      writeFrame(client.socket, { type: MSG.ERROR, message: 'Index stale', _requestId: msg._requestId });
      return;
    }
    const folder = this._findEngineFolder(msg.folder);
    if (!folder) {
      writeFrame(client.socket, { type: MSG.ERROR, message: 'Unknown folder', _requestId: msg._requestId });
      return;
    }

    // Per-folder defense in depth — see _handlePushIndex.
    if (typeof folder.scanner.isInitialScanComplete === 'function' &&
        !folder.scanner.isInitialScanComplete()) {
      this._logEngineNotReady(client.deviceName || client.ip || 'inbound-client');
      writeFrame(client.socket, { type: MSG.ERROR, message: 'Index stale', _requestId: msg._requestId });
      return;
    }

    if (folder.scanner.isStale()) {
      writeFrame(client.socket, { type: MSG.ERROR, message: 'Index stale', _requestId: msg._requestId });
      return;
    }

    if (msg.clientIndex) {
      const diff = await this.engine.computeDiff(msg.folder, msg.clientIndex);
      writeFrame(client.socket, {
        type: MSG.INDEX_RESPONSE, folder: msg.folder,
        rootHash: this.engine.getRootHash(msg.folder),
        diff, _requestId: msg._requestId,
      });
    } else {
      const index = this.engine.getIndex(msg.folder);
      const info = this.engine.getFolderInfo(msg.folder);
      writeFrame(client.socket, {
        type: MSG.INDEX_RESPONSE, folder: msg.folder,
        rootHash: info?.rootHash || '', index: index || [],
        fileCount: index?.length || 0, totalSize: info?.totalSize || 0,
        _requestId: msg._requestId,
      });
    }
  }

  async _handleBlockRequest(client, msg) {
    const folder = this._findEngineFolder(msg.folder);
    if (!folder) {
      writeFrame(client.socket, { type: MSG.ERROR, message: 'Unknown folder', _requestId: msg._requestId });
      return;
    }

    const absPath = path.join(folder.path, msg.path);
    const resolved = path.resolve(absPath);
    if (!resolved.startsWith(folder.path)) {
      writeFrame(client.socket, { type: MSG.ERROR, message: 'Path traversal', _requestId: msg._requestId });
      return;
    }

    try {
      const stat = await fsp.stat(resolved);
      if (stat.size > 4 * 1024 * 1024) {
        await streamFileToSocket(client.socket, resolved, {
          folder: msg.folder, path: msg.path, size: stat.size, _requestId: msg._requestId,
        });
      } else {
        const data = await fsp.readFile(resolved);
        writeFrame(client.socket, {
          type: MSG.BLOCK_RESPONSE, folder: msg.folder, path: msg.path,
          size: data.length, _requestId: msg._requestId,
        });
        const header = Buffer.alloc(5);
        header.writeUInt32BE(data.length + 1);
        header[4] = 0xFF;
        client.socket.write(Buffer.concat([header, data]));
        writeFrame(client.socket, {
          type: 'transfer_end', folder: msg.folder, path: msg.path, bytesSent: data.length,
        });
      }
    } catch (err) {
      writeFrame(client.socket, { type: MSG.ERROR, message: `Read failed`, _requestId: msg._requestId });
    }
  }

  _sendFolderList(client) {
    const folders = this.config.folders.filter(f => f.enabled && !f.internal).map(f => ({
      id: f.id, name: f.name, excludes: f.excludes || [], direction: f.direction || 'both',
      fileCount: this.engine?.folders.get(f.name)?.scanner.getFileCount() || 0,
    }));
    writeFrame(client.socket, { type: MSG.FOLDER_LIST, folders });
  }

  _handleSetExcludes(client, msg) {
    if (msg.folder && Array.isArray(msg.excludes)) {
      const folder = this.config.folders.find(f => f.name === msg.folder);
      if (folder) {
        this.config.setFolderExcludes(folder.path, msg.excludes);
        writeFrame(client.socket, { type: 'excludes_updated', folder: msg.folder, _requestId: msg._requestId });
      }
    }
  }

  _trackSyncProgress(client, msg) {
    if (!client.deviceName) return;
    if (!this.deviceSync.has(client.deviceName)) this.deviceSync.set(client.deviceName, {});
    const ds = this.deviceSync.get(client.deviceName);
    if (!ds[msg.folder]) ds[msg.folder] = { status: 'syncing', filesComplete: 0 };
    ds[msg.folder].filesComplete++;
    ds[msg.folder].lastFile = msg.path;
    this.emit('sync-progress-update');
  }

  _trackSyncComplete(client, msg) {
    if (!client.deviceName) return;
    if (!this.deviceSync.has(client.deviceName)) this.deviceSync.set(client.deviceName, {});
    const ds = this.deviceSync.get(client.deviceName);
    ds[msg.folder] = { status: 'synced', lastSync: Date.now() };
    this.emit('sync-progress-update');
  }

  // ---- Helpers ----

  _markRecentlyWritten(folder, relPath) {
    const key = `${folder}/${relPath}`;
    this._recentlyWritten.set(key, Date.now());
    // Cleanup old entries
    if (this._recentlyWritten.size > 1000) {
      const now = Date.now();
      for (const [k, ts] of this._recentlyWritten) {
        if (now - ts > RECENTLY_WRITTEN_TTL * 2) this._recentlyWritten.delete(k);
      }
    }
  }

  /**
   * Approve a sync request from a new peer.
   * Saves them as approved so future connections are auto-accepted.
   */
  approvePeer(clientId, selectedFolders) {
    const client = this.transport?.clients.get(clientId);
    if (!client) return false;

    const remoteIp = client.socket.remoteAddress?.replace('::ffff:', '') || '';

    // Save as approved peer
    if (!this.config.data.approvedPeers) this.config.data.approvedPeers = [];
    if (!this.config.data.approvedPeers.some(p => p.deviceId === client.deviceId)) {
      this.config.data.approvedPeers.push({
        deviceId: client.deviceId,
        deviceName: client.deviceName,
        ip: remoteIp,
        approvedAt: Date.now(),
        folders: selectedFolders || [],
      });
      this.config.save();
    }

    // Send folder list to the now-approved client
    this._sendFolderList(client);
    this.emit('client-connected', { deviceName: client.deviceName, deviceId: client.deviceId, ip: remoteIp });

    console.log(`Approved peer: ${client.deviceName} (${remoteIp})`);
    return true;
  }

  /**
   * Reject a sync request — disconnect the client.
   */
  rejectPeer(clientId) {
    const client = this.transport?.clients.get(clientId);
    if (client) {
      writeFrame(client.socket, { type: MSG.ERROR, message: 'Connection rejected' });
      client.socket.destroy();
    }
    return true;
  }

  reconnectHub() {
    if (this.hubConnection) {
      this.hubConnection.disconnect();
      this.hubConnection = null;
    }
    if (!this.config.isHub && this.config.hubAddress) {
      this._connectToHub();
    }
  }

  // ---- Status ----

  getStatus() {
    const folders = [];
    if (this.engine) {
      for (const engineName of this.engine.getFolderNames()) {
        const info = this.engine.getFolderInfo(engineName);
        // Match config by path (not name, since name may have been renamed)
        const enginePath = this.engine.folders.get(engineName)?.path;
        const cfgFolder = this.config.folders.find(f => f.path === enginePath) ||
                          this.config.folders.find(f => f.name === engineName);
        // Use config name (may be renamed) over engine name
        info.name = cfgFolder?.name || engineName;
        info.excludes = cfgFolder?.excludes || [];
        info.direction = cfgFolder?.direction || 'both';
        info.folderId = cfgFolder?.id || null;
        info.icon = cfgFolder?.icon || null;
        info.group = cfgFolder?.group || null;
        info.folderPath = cfgFolder?.path || enginePath || '';
        info.internal = cfgFolder?.internal || false;

        info.devices = {};
        info.devices[this.config.deviceName] = {
          status: this.config.isHub ? 'hub' : 'source',
          progress: 100,
        };
        for (const [deviceName, folderSync] of this.deviceSync) {
          if (folderSync[name]) {
            const ds = folderSync[name];
            info.devices[deviceName] = { ...ds };
          }
        }
        folders.push(info);
      }
    }

    const peers = this.config.data.peers || {};

    return {
      deviceName: this.config.deviceName,
      deviceId: this.config.deviceId,
      port: this.config.port,
      apiKey: this.config.apiKey,
      fingerprint: this.fingerprint,
      tlsEnabled: !!this.transport?.tlsKey,
      isHub: this.config.isHub,
      hubAddress: this.config.hubAddress,
      hubConnected: this.hubConnection?.authenticated || false,
      connectedClients: this.transport?.getClientCount() || 0,
      connectedPeers: this.getConnectedPeers(),
      inboundClients: (this.transport?.getConnectedClients() || []).map(c => ({
        ...c,
        hostname: c.deviceName,
        friendlyName: c.deviceName,
        source: 'inbound',
      })),
      savedPeers: this.config.data.savedPeers || [],
      remoteFolders: this._getRemoteFolders(),
      folders,
      gameSaves: this.gameSaveManager?.getLibrary() || [],
      discoveredDevices: (this.discovery?.getServices() || []).map(d => ({
        ...d, friendlyName: peers[d.hostname] || d.hostname,
      })),
      peers,
    };
  }

  /**
   * Quick sync: compare root hashes with each peer, only do full push/pull
   * for folders that actually differ. Runs every 15s — nearly free when in sync.
   */
  async _quickSyncAllPeers() {
    if (this._quickSyncRunning) return;
    this._quickSyncRunning = true;
    try {
      for (const [, peerInfo] of this.peerConnections) {
        if (!peerInfo.connected || !peerInfo.client?.authenticated) continue;

        // Ask peer for root hashes of all its folders
        let peerHashes;
        try {
          const resp = await peerInfo.client.request({ type: MSG.HASH_CHECK }, 10000);
          peerHashes = resp.hashes || {};
        } catch {
          // Peer doesn't support HASH_CHECK (older version) — fall back to full sync
          peerHashes = null;
        }

        for (const folder of this.config.folders) {
          if (!folder.enabled || folder.internal) continue;
          const dir = folder.direction || 'both';
          const ef = this._findEngineFolder(folder.name);
          if (!ef) continue;

          // If peer supports hash check and hashes match, skip this folder
          if (peerHashes !== null) {
            const localHash = ef.scanner.getRootHash();
            const peerHash = peerHashes[folder.name];
            if (peerHash && localHash === peerHash) continue; // In sync — skip
          }

          try {
            // Push-before-pull for 'both': prevents stale peer index from
            // inferring deletions for files we just added locally.
            if (dir === 'push' || dir === 'both') {
              await this._pushFullFolderToPeer(peerInfo, folder);
            }
            if (dir === 'receive' || dir === 'both') {
              await this._pullFolderFromPeer(peerInfo, folder);
            }
          } catch (err) {
            console.error(`Quick sync failed [${folder.name}→${peerInfo.deviceName}]: ${err.message}`);
          }
        }
      }

      // Hub sync with hash check
      if (this.hubConnection?.authenticated) {
        for (const folder of this.config.folders) {
          if (!folder.enabled || folder.internal) continue;
          const dir = folder.direction || 'both';
          try {
            // Push-before-pull for 'both': see _syncWithPeer comment.
            if (dir === 'push' || dir === 'both') await this._pushFullFolder(folder);
            if (dir === 'receive' || dir === 'both') await this._pullFolder(folder);
          } catch (err) {
            console.error(`Quick hub sync failed [${folder.name}]: ${err.message}`);
          }
        }
      }
    } finally {
      this._quickSyncRunning = false;
    }
  }

  /**
   * Rescan all folders and sync with every connected peer.
   * Called on startup after initial scan, periodically, and on peer connect.
   */
  async _syncAllPeers() {
    if (this._fullSyncRunning) return;
    this._fullSyncRunning = true;
    try {
      for (const folder of this.config.folders) {
        if (!folder.enabled || folder.internal) continue;
        try {
          await this.syncFolder(folder.name);
        } catch (err) {
          console.error(`Periodic sync failed [${folder.name}]: ${err.message}`);
        }
      }
    } finally {
      this._fullSyncRunning = false;
    }
  }

  /**
   * Force rescan a folder and sync with all connected peers.
   * Called by UI "Rescan" button.
   *
   * Phase 9 P0: engine-ready gate. The downstream _pushFullFolder* / _pull*
   * calls already gate on _engineReady and return silently; that's fine for
   * timer-driven callers but hides the reason from a user who just clicked
   * "Rescan." Throw here so the UI layer can surface a clear "still scanning"
   * message instead of pretending the rescan succeeded.
   */
  async syncFolder(folderName) {
    if (!this._engineReady) {
      this._logEngineNotReady('user-triggered-syncFolder');
      throw new Error('Engine not ready — initial scan in progress. Please wait.');
    }
    // Force rescan local files first
    await this.engine.rescan(folderName);

    const folder = this.config.folders.find(f => f.name === folderName);
    if (!folder) return;
    const dir = folder.direction || 'both';

    // Sync with all connected peers
    for (const [, peerInfo] of this.peerConnections) {
      if (!peerInfo.connected || !peerInfo.client?.authenticated) continue;
      try {
        // Push-before-pull for 'both': see _syncWithPeer comment.
        if (dir === 'push' || dir === 'both') {
          console.log(`Sync: pushing ${folderName} to ${peerInfo.deviceName}`);
          await this._pushFullFolderToPeer(peerInfo, folder);
        }
        if (dir === 'receive' || dir === 'both') {
          console.log(`Sync: pulling ${folderName} from ${peerInfo.deviceName}`);
          await this._pullFolderFromPeer(peerInfo, folder);
        }
      } catch (err) {
        console.error(`Sync with ${peerInfo.deviceName} failed [${folderName}]: ${err.message}`);
      }
    }

    // Sync with hub if connected
    if (this.hubConnection?.authenticated) {
      try {
        // Push-before-pull for 'both': see _syncWithPeer comment.
        if (dir === 'push' || dir === 'both') {
          await this._pushFullFolder(folder);
        }
        if (dir === 'receive' || dir === 'both') {
          await this._pullFolder(folder);
        }
      } catch (err) {
        console.error(`Hub sync failed [${folderName}]: ${err.message}`);
      }
    }
  }

  async addFolder(folderPath, name, direction, folderId) {
    this.config.addFolder(folderPath, name, direction, folderId);
    const folder = this.config.folders.find(f => f.path === path.resolve(folderPath));
    if (folder) {
      this.engine.addFolder(folder);
      await this.engine.rescan(folder.name);

      // Sync the new folder with all connected peers
      for (const [, peerInfo] of this.peerConnections) {
        if (peerInfo.connected && peerInfo.client?.authenticated) {
          const dir = folder.direction || 'both';
          try {
            // Push-before-pull for 'both': see _syncWithPeer comment.
            if (dir === 'push' || dir === 'both') {
              await this._pushFullFolderToPeer(peerInfo, folder);
            }
            if (dir === 'receive' || dir === 'both') {
              await this._pullFolderFromPeer(peerInfo, folder);
              peerInfo.client.send({ type: MSG.SUBSCRIBE, folder: folder.name });
            }
          } catch (err) {
            console.error(`Sync new folder ${folder.name} with ${peerInfo.deviceName} failed: ${err.message}`);
          }
        }
      }

      // Also sync with hub if connected
      if (this.hubConnection?.authenticated) {
        try {
          const dir = folder.direction || 'both';
          // Push-before-pull for 'both': see _syncWithPeer comment.
          if (dir === 'push' || dir === 'both') {
            await this._pushFullFolder(folder);
          }
          if (dir === 'receive' || dir === 'both') {
            await this._pullFolder(folder);
            this.hubConnection.send({ type: MSG.SUBSCRIBE, folder: folder.name });
          }
        } catch (err) {
          console.error(`Sync new folder ${folder.name} with hub failed: ${err.message}`);
        }
      }
    }
  }

  removeFolder(folderPath) {
    const resolved = path.resolve(folderPath);
    const folder = this.config.folders.find(f => f.path === resolved);
    if (folder) {
      this.engine.removeFolder(folder.name);
      this.config.removeFolder(folderPath);
    }
  }

  async _pruneAllTrash() {
    for (const folder of this.config.folders) {
      if (!folder.enabled) continue;
      try {
        const removed = await pruneTrash(folder.path, { retentionDays: 7 });
        if (removed > 0) console.log(`Pruned ${removed} expired trash dir(s) from ${folder.name}`);
      } catch (err) {
        console.warn(`Trash prune failed [${folder.name}]: ${err.message}`);
      }
      // Phase 9 P0: conflict sidecars don't expire on their own. 30-day
      // retention (longer than trash — rarer, and every conflict sidecar
      // represents genuine user content that a merge decision has to be
      // made about).
      try {
        const conflicts = await pruneConflicts(folder.path, { retentionDays: 30 });
        if (conflicts > 0) {
          console.log(`Pruned ${conflicts} expired conflict sidecar(s) from ${folder.name}`);
        }
      } catch (err) {
        console.warn(`Conflict prune failed [${folder.name}]: ${err.message}`);
      }
    }
  }

  async stop() {
    this._stopping = true;
    if (this._scanInterval) { clearInterval(this._scanInterval); this._scanInterval = null; }
    if (this._quickSyncInterval) { clearInterval(this._quickSyncInterval); this._quickSyncInterval = null; }
    if (this._periodicSyncInterval) { clearInterval(this._periodicSyncInterval); this._periodicSyncInterval = null; }
    if (this._trashPruneInterval) { clearInterval(this._trashPruneInterval); this._trashPruneInterval = null; }
    if (this._engineReadyPoll) { clearInterval(this._engineReadyPoll); this._engineReadyPoll = null; }
    for (const [, timer] of this._pushTimers) clearTimeout(timer);
    this._pushTimers.clear();
    if (this._httpServer) { this._httpServer.close(); this._httpServer = null; }
    if (this.gameSaveManager) await this.gameSaveManager.stop();
    if (this.hubConnection) this.hubConnection.disconnect();
    for (const [, peer] of this.peerConnections) {
      peer.client?.disconnect();
    }
    this.peerConnections.clear();
    if (this.discovery) this.discovery.stop();
    if (this.transport) this.transport.stop();
    if (this.engine) await this.engine.stop();
    console.log('CarbonSync stopped');
  }
}

module.exports = { CarbonSyncDevice };
