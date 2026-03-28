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
    this._pendingSync = new Set(); // Folders that need syncing
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

  async _syncFolder(folderName) {
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
      console.error(`Index error: ${response.message}`);
      return;
    }

    const diff = response.diff;
    if (!diff) return;

    const { toDownload, toDelete } = diff;

    if (toDownload.length === 0 && toDelete.length === 0) {
      console.log(`${folderName}: in sync`);
      return;
    }

    console.log(`${folderName}: ${toDownload.length} to download, ${toDelete.length} to delete`);

    // Apply deletions
    for (const relPath of toDelete) {
      const absPath = path.join(folder.path, relPath);
      try {
        await fsp.unlink(absPath);
        scanner.removeFile(absPath);
      } catch (err) {
        if (err.code !== 'ENOENT') console.warn(`Delete failed [${relPath}]: ${err.message}`);
      }
    }

    // Download files with concurrency limit
    this.state = SYNC_STATE.TRANSFERRING;
    this.emit('state', this.state);

    const totalBytes = toDownload.reduce((s, f) => s + f.size, 0);
    let transferredBytes = 0;
    let filesComplete = 0;
    let filesFailed = 0;

    // Process downloads in batches
    const queue = [...toDownload];
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
