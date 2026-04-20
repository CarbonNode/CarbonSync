/**
 * CarbonSync Client — pulls files from the server.
 *
 * Fixed: streaming downloads (no 64MB limit), concurrent transfers,
 * hash verification before write, temp file cleanup, retry logic,
 * resume on reconnect.
 */

const fs = require('fs');
const fsp = require('fs/promises');
const path = require('path');
const crypto = require('crypto');
const { EventEmitter } = require('events');
const { SyncClient } = require('./transport');
const { Scanner } = require('./scanner');
const { Config } = require('./config');
const { Discovery } = require('./discovery');
const { ensureFirewallRule } = require('./firewall');
const { MSG, SYNC_STATE } = require('../shared/protocol');
const { ResumeState } = require('./resume');
const { moveToTrash } = require('./trash');
const { evaluateDeletion, getThresholds, BLOCKED_LOG_MESSAGE } = require('./deletion-guard');
const { classifyOverwrite, getShrinkThreshold, SHRINK_BLOCKED_LOG_MESSAGE } = require('./shrink-guard');
const os = require('os');

const MAX_CONCURRENT_DOWNLOADS = 4;
const MAX_RETRIES = 3;

class CarbonSyncClient extends EventEmitter {
  constructor(configDir) {
    super();
    this.configDir = configDir || path.join(os.homedir(), '.carbonsync');
    this.config = new Config(this.configDir);
    this.connection = null;
    this.discovery = null;
    this.scanners = new Map();
    this.state = SYNC_STATE.IDLE;
    this._syncInProgress = false;
    this._serverInfo = null;
    this._pendingSync = new Set();
    this.resumeState = new ResumeState(this.configDir);
    // Capped ring of recent guard-blocked deletion batches. Surfaced via the
    // device's /blocked-deletions HTTP endpoint (and the equivalent on this
    // client when wired); also written to sync.log for offline debugging.
    this._blockedDeletions = [];
    // Phase 6 P0: shrink-guard refusals. Mirrors device.js — newest last,
    // capped at 100. Surfaced via the equivalent /shrink-blocked endpoint
    // when this client process exposes one.
    this._shrinkBlocked = [];
  }

  _recordBlockedDeletion(entry) {
    this._blockedDeletions.push(entry);
    if (this._blockedDeletions.length > 100) {
      // Drop oldest. push+shift > slice for a 100-item bound.
      this._blockedDeletions.shift();
    }
  }

  _recordShrinkBlocked(entry) {
    this._shrinkBlocked.push(entry);
    if (this._shrinkBlocked.length > 100) this._shrinkBlocked.shift();
  }

  /**
   * Phase 6 P0: drastic-shrink overwrite guard. Same contract as the
   * helper in device.js — see shrink-guard.js for the threshold logic.
   * Returns true if caller should proceed with the rename, false if the
   * guard tripped (incoming bytes saved as a sidecar, existing untouched).
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

    const safePeer = String(peerName || 'server').replace(/[^A-Za-z0-9._-]/g, '_');
    const ext = path.extname(absPath);
    const base = absPath.slice(0, absPath.length - ext.length);
    const conflictPath = `${base}.shrink-blocked.${safePeer}.${Date.now()}${ext}`;

    try {
      await fsp.mkdir(path.dirname(conflictPath), { recursive: true });
      if (fileData) await fsp.writeFile(conflictPath, fileData);
    } catch (err) {
      console.warn(`Shrink-guard sidecar write failed [${conflictPath}]: ${err.message}`);
    }

    if (tmpPath) {
      try { await fsp.unlink(tmpPath); } catch {}
    }

    const line = SHRINK_BLOCKED_LOG_MESSAGE({
      folderName,
      relPath,
      peerName: peerName || 'server',
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
      peer: peerName || 'server',
      existingSize: existingSize == null ? 0 : existingSize,
      incomingSize,
      reason: verdict.reason,
      conflictPath,
      timestamp: new Date().toISOString(),
    });

    return false;
  }

  async start() {
    console.log('=== CarbonSync Client Starting ===');
    console.log(`Device: ${this.config.deviceName} (${this.config.deviceId})`);

    await ensureFirewallRule(this.config.port);

    this.discovery = new Discovery({
      port: this.config.port,
      role: 'client',
      deviceId: this.config.deviceId,
      onFound: (service) => {
        if (service.role === 'server') {
          console.log(`Found server: ${service.hostname} at ${service.ip}:${service.port}`);
          this._serverInfo = service;
          this.emit('server-found', service);
          this._connectToServer(service);
        }
      },
      onLost: (service) => {
        console.log(`Server lost: ${service.hostname}`);
        this.emit('server-lost', service);
      },
    });

    this.discovery.publish();
    this.discovery.browse();

    console.log('=== CarbonSync Client Ready (searching for server...) ===');
  }

  async _connectToServer(serverInfo) {
    if (this.connection?.connected) return;

    this.connection = new SyncClient({
      host: serverInfo.ip,
      port: serverInfo.port,
      apiKey: this.config.apiKey || serverInfo.apiKey,
      deviceId: this.config.deviceId,
      deviceName: this.config.deviceName,
    });

    this.connection.on('authenticated', (msg) => {
      console.log(`Authenticated with server: ${msg.serverName}`);
      this.emit('connected', msg);
      this._startSync();
    });

    this.connection.on('message', (msg) => {
      if (msg.type === MSG.NOTIFY) {
        console.log(`Change notification: ${msg.folder} — ${msg.changes?.length || 0} changes`);
        this._pendingSync.add(msg.folder);
        // Debounce: wait 2s for more changes before syncing
        setTimeout(() => this._processPendingSync(), 2000);
      }
    });

    this.connection.on('disconnected', () => {
      this.state = SYNC_STATE.IDLE;
      this.emit('disconnected');
    });

    this.connection.on('error', (err) => {
      console.error(`Connection error: ${err.message}`);
    });

    this.connection.connect();
  }

  async _processPendingSync() {
    if (this._syncInProgress) return;
    const folders = [...this._pendingSync];
    this._pendingSync.clear();
    for (const folder of folders) {
      await this._syncFolder(folder);
    }
  }

  async _startSync() {
    if (this._syncInProgress) return;
    this._syncInProgress = true;

    try {
      for (const folder of this.config.folders) {
        if (folder.enabled) await this._syncFolder(folder.name);
      }
    } catch (err) {
      console.error(`Sync failed: ${err.message}`);
    } finally {
      this._syncInProgress = false;
      this.state = SYNC_STATE.IDLE;
      this.emit('state', this.state);
    }
  }

  async _syncFolder(folderName, _retryCount = 0) {
    if (!this.connection?.authenticated) return;

    const folder = this.config.folders.find(f => f.name === folderName);
    if (!folder) return;

    this.state = SYNC_STATE.COMPARING;
    this.emit('state', this.state);

    // Ensure local scanner
    if (!this.scanners.has(folderName)) {
      const dbPath = path.join(this.configDir, `index_${folderName.replace(/[^a-zA-Z0-9]/g, '_')}.db`);
      this.scanners.set(folderName, new Scanner(folder.path, dbPath));
    }

    const scanner = this.scanners.get(folderName);
    await scanner.fullScan();
    const localIndex = scanner.getIndex();

    // Request diff from server
    let response;
    try {
      response = await this.connection.request({
        type: MSG.INDEX_REQUEST,
        folder: folderName,
        clientIndex: localIndex.map(f => ({ path: f.path, hash: f.hash, size: f.size })),
      }, 60000);
    } catch (err) {
      console.error(`Index request failed: ${err.message}`);
      return;
    }

    if (response.type === MSG.ERROR) {
      if (response.message?.includes('stale') && _retryCount < 5) {
        const delay = Math.min(3000 * (_retryCount + 1), 15000);
        console.log(`Server index stale for ${folderName}, retrying in ${delay / 1000}s (attempt ${_retryCount + 1}/5)`);
        await new Promise(r => setTimeout(r, delay));
        return this._syncFolder(folderName, _retryCount + 1);
      }
      console.error(`Index error: ${response.message}`);
      return;
    }

    const diff = response.diff;
    if (!diff) return;

    const { toDownload = [], toCopy = [], toDelete = [], dirs = [] } = diff;

    if (toDownload.length === 0 && toCopy.length === 0 && toDelete.length === 0) {
      console.log(`${folderName}: in sync`);
      return;
    }

    // Resume: filter out already-downloaded files from a previous interrupted sync
    const serverRootHash = response.rootHash || '';
    const filteredDownload = this.resumeState.filterCompleted(folderName, serverRootHash, toDownload);

    if (filteredDownload.length !== toDownload.length) {
      console.log(`${folderName}: resuming — ${toDownload.length - filteredDownload.length} already done`);
    }

    console.log(`${folderName}: ${filteredDownload.length} download, ${toCopy.length} copy, ${toDelete.length} delete`);

    // Start resume tracking
    const totalBytes = filteredDownload.reduce((s, f) => s + f.size, 0);
    this.resumeState.start(folderName, serverRootHash, filteredDownload.length, totalBytes);

    // Create empty directories first
    for (const dir of dirs) {
      const absDir = path.join(folder.path, dir);
      try { await fsp.mkdir(absDir, { recursive: true }); } catch {}
    }

    // Apply local copies (rename/move detection — no network transfer needed!)
    for (const copy of toCopy) {
      try {
        const fromAbs = path.join(folder.path, copy.from);
        const toAbs = path.join(folder.path, copy.to);
        await fsp.mkdir(path.dirname(toAbs), { recursive: true });
        await fsp.copyFile(fromAbs, toAbs);
        if (copy.mtime_ms) {
          const mtime = new Date(copy.mtime_ms);
          await fsp.utimes(toAbs, mtime, mtime);
        }
        await scanner.updateFile(toAbs);
        console.log(`  Copied: ${copy.from} → ${copy.to}`);
      } catch (err) {
        console.warn(`  Copy failed [${copy.to}]: ${err.message}, will download instead`);
        toDownload.push({ path: copy.to, size: copy.size, hash: copy.hash, mtime_ms: copy.mtime_ms });
      }
    }

    // Apply deletions — moved to .carbonsync-trash/<date>/ instead of unlinked,
    // so a stale-server diff can never silently destroy local files.
    //
    // Guard: refuse the WHOLE batch if it looks catastrophic (>50 files OR
    // >25% of the folder by default; per-folder configurable). This is
    // belt-and-suspenders with the trash bucket — trash gives us 7 days to
    // recover, the guard prevents the deletion from being attempted at all
    // when the diff smells wrong (e.g. stale peer index).
    const totalFiles = scanner.getIndexMap().size;
    const peerName = this._serverInfo?.hostname || this._serverInfo?.deviceName || 'server';
    const guard = evaluateDeletion({
      folderName,
      totalFiles,
      toDeleteCount: toDelete.length,
      peerName,
      thresholds: getThresholds(folder),
    });
    if (!guard.allowed) {
      const line = BLOCKED_LOG_MESSAGE({
        folderName,
        peerName,
        count: toDelete.length,
        reason: guard.reason,
      });
      console.warn(line);
      try {
        fs.appendFileSync(path.join(this.configDir, 'sync.log'), line + '\n');
      } catch {}
      this._recordBlockedDeletion({
        folder: folderName,
        peer: peerName,
        count: toDelete.length,
        reason: guard.reason,
        timestamp: new Date().toISOString(),
      });
      // Don't abort the rest of the sync — downloads/copies still proceed.
    } else {
      for (const relPath of toDelete) {
        const absPath = path.join(folder.path, relPath);
        try {
          await moveToTrash(folder.path, relPath, { reason: 'sync-delete' });
          scanner.removeFile(absPath);
        } catch (err) {
          if (err.code !== 'ENOENT') console.warn(`Delete failed [${relPath}]: ${err.message}`);
        }
      }
    }

    // Download files with concurrency limit
    this.state = SYNC_STATE.TRANSFERRING;
    this.emit('state', this.state);

    let transferredBytes = 0;
    let filesComplete = 0;
    let filesFailed = 0;

    const queue = [...filteredDownload];
    const active = new Set();

    const processNext = async () => {
      while (queue.length > 0 && active.size < MAX_CONCURRENT_DOWNLOADS) {
        const file = queue.shift();
        const promise = this._downloadFileWithRetry(folder.path, folderName, file, scanner, (bytes) => {
          transferredBytes += bytes;
          this.emit('progress', {
            folder: folderName,
            phase: 'transferring',
            currentFile: file.path,
            filesComplete,
            filesTotal: toDownload.length,
            filesFailed,
            bytesTransferred: transferredBytes,
            bytesTotal: totalBytes,
            totalProgress: totalBytes > 0 ? Math.round((transferredBytes / totalBytes) * 100) : 0,
          });
        }).then(() => {
          filesComplete++;
          this.resumeState.markCompleted(folderName, file.path);
          active.delete(promise);
          return processNext();
        }).catch((err) => {
          filesFailed++;
          console.error(`  FAILED [${file.path}]: ${err.message}`);
          active.delete(promise);
          return processNext();
        });

        active.add(promise);
      }

      if (active.size > 0) {
        await Promise.all(active);
      }
    };

    await processNext();

    // Subscribe to live changes
    this.connection.send({ type: MSG.SUBSCRIBE, folder: folderName });

    // Clear resume state on successful completion
    this.resumeState.clear(folderName);

    console.log(`${folderName}: sync complete — ${filesComplete} downloaded, ${filesFailed} failed, ${toDelete.length} deleted`);
    this.state = SYNC_STATE.DONE;
    this.emit('state', this.state);
    this.emit('sync-complete', { folder: folderName, downloaded: filesComplete, failed: filesFailed, deleted: toDelete.length });
  }

  /**
   * Download with retry logic.
   */
  async _downloadFileWithRetry(folderPath, folderName, fileInfo, scanner, onProgress) {
    let lastErr;
    for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
      try {
        await this._downloadFile(folderPath, folderName, fileInfo, onProgress);
        await scanner.updateFile(path.join(folderPath, fileInfo.path));
        this.connection.send({ type: MSG.FILE_DONE, folder: folderName, path: fileInfo.path });
        return;
      } catch (err) {
        lastErr = err;
        if (attempt < MAX_RETRIES) {
          console.warn(`  Retry ${attempt}/${MAX_RETRIES} [${fileInfo.path}]: ${err.message}`);
          await new Promise(r => setTimeout(r, 1000 * attempt));
        }
      }
    }
    throw lastErr;
  }

  /**
   * Download a file using streaming protocol. Atomic write with hash verification.
   */
  async _downloadFile(folderPath, folderName, fileInfo, onProgress) {
    const absPath = path.join(folderPath, fileInfo.path);
    const tmpPath = absPath + '.carbonsync.tmp';

    await fsp.mkdir(path.dirname(absPath), { recursive: true });

    try {
      // Request file from server
      const response = await this.connection.request({
        type: MSG.BLOCK_REQUEST,
        folder: folderName,
        path: fileInfo.path,
      }, 300000); // 5 min timeout for very large files

      if (response.type === MSG.ERROR) throw new Error(response.message);

      let fileData;

      if (response.streaming || response.binaryPromise) {
        // Large file: collect streamed chunks
        const chunks = await response.binaryPromise;
        fileData = Buffer.concat(chunks);
      } else {
        // Small file: single binary frame follows
        fileData = await new Promise((resolve, reject) => {
          const timeout = setTimeout(() => reject(new Error('Binary data timeout')), 120000);
          this.connection.once('binary', (data) => { clearTimeout(timeout); resolve(data); });
        });
      }

      // Verify hash BEFORE writing to disk
      const hash = crypto.createHash('sha256').update(fileData).digest('hex');
      if (hash !== fileInfo.hash) {
        throw new Error(`Hash mismatch: expected ${fileInfo.hash.substring(0, 12)}..., got ${hash.substring(0, 12)}...`);
      }

      // Phase 6 P0: shrink-guard. Refuse to silently overwrite a substantial
      // local file with an empty/drastically-shrunk server copy.
      const folderConfig = this.config.folders.find(f => f.name === folderName) ||
                           this.config.folders.find(f => path.resolve(f.path) === path.resolve(folderPath));
      const proceed = await this._checkShrinkOverwrite({
        folderConfig,
        folderName,
        absPath,
        relPath: fileInfo.path,
        tmpPath: null, // tmp not yet written
        fileData,
        peerName: this._serverInfo?.hostname || 'server',
      });
      if (!proceed) return;

      // Atomic write: tmp → verify → rename
      await fsp.writeFile(tmpPath, fileData);

      // Set mtime to match server
      if (fileInfo.mtime_ms) {
        const mtime = new Date(fileInfo.mtime_ms);
        await fsp.utimes(tmpPath, mtime, mtime);
      }

      await fsp.rename(tmpPath, absPath);

      if (onProgress) onProgress(fileData.length);

    } catch (err) {
      // Always clean up temp file on failure
      try { await fsp.unlink(tmpPath); } catch { /* doesn't exist */ }
      throw err;
    }
  }

  // ---- Management ----

  addFolder(folderPath, name) {
    this.config.addFolder(folderPath, name);
    if (this.connection?.authenticated) {
      this._syncFolder(name || path.basename(folderPath));
    }
  }

  removeFolder(folderPath) {
    const resolved = path.resolve(folderPath);
    const folder = this.config.folders.find(f => f.path === resolved);
    if (folder && this.scanners.has(folder.name)) {
      this.scanners.get(folder.name).close();
      this.scanners.delete(folder.name);
    }
    this.config.removeFolder(folderPath);
  }

  getStatus() {
    return {
      deviceName: this.config.deviceName,
      state: this.state,
      connected: this.connection?.authenticated || false,
      serverInfo: this._serverInfo,
      folders: this.config.folders,
    };
  }

  async stop() {
    if (this.discovery) this.discovery.stop();
    if (this.connection) this.connection.disconnect();
    for (const [, scanner] of this.scanners) scanner.close();
    this.scanners.clear();
    console.log('CarbonSync client stopped');
  }
}

module.exports = { CarbonSyncClient };
