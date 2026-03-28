/**
 * CarbonSync Client — pulls files from the server.
 *
 * Connects to the hub, compares indexes, downloads changed files.
 * All writes are atomic (write to .tmp, verify hash, rename).
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

class CarbonSyncClient extends EventEmitter {
  constructor(configDir) {
    super();
    this.configDir = configDir || path.join(os.homedir(), '.carbonsync');
    this.config = new Config(this.configDir);
    this.connection = null;
    this.discovery = null;
    this.scanners = new Map(); // folder name -> Scanner
    this.state = SYNC_STATE.IDLE;
    this._syncInProgress = false;
    this._serverInfo = null;
  }

  async start() {
    console.log('=== CarbonSync Client Starting ===');
    console.log(`Device: ${this.config.deviceName} (${this.config.deviceId})`);

    await ensureFirewallRule(this.config.port);

    // Start mDNS discovery to find the server
    this.discovery = new Discovery({
      port: this.config.port,
      role: 'client',
      apiKey: this.config.apiKey,
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
    if (this.connection && this.connection.connected) {
      return; // Already connected
    }

    this.connection = new SyncClient({
      host: serverInfo.ip,
      port: serverInfo.port,
      apiKey: serverInfo.apiKey,
      deviceId: this.config.deviceId,
      deviceName: this.config.deviceName,
    });

    this.connection.on('authenticated', (msg) => {
      console.log(`Authenticated with server: ${msg.serverName}`);
      this.emit('connected', msg);
      // Start syncing
      this._startSync();
    });

    this.connection.on('message', (msg) => {
      if (msg.type === MSG.NOTIFY) {
        // Server pushed a change notification
        console.log(`Change notification for ${msg.folder}: ${msg.changes.length} changes`);
        this._syncFolder(msg.folder);
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

  /**
   * Start syncing all folders with the server.
   */
  async _startSync() {
    if (this._syncInProgress) return;
    this._syncInProgress = true;

    try {
      // Request folder list from server (by requesting index of each known folder)
      // For now, sync all folders configured on this client
      for (const folder of this.config.folders) {
        if (folder.enabled) {
          await this._syncFolder(folder.name);
        }
      }
    } catch (err) {
      console.error(`Sync failed: ${err.message}`);
    } finally {
      this._syncInProgress = false;
      this.state = SYNC_STATE.IDLE;
      this.emit('state', this.state);
    }
  }

  /**
   * Sync a single folder with the server.
   */
  async _syncFolder(folderName) {
    if (!this.connection || !this.connection.authenticated) return;

    const folder = this.config.folders.find(f => f.name === folderName);
    if (!folder) {
      console.warn(`Unknown folder: ${folderName}`);
      return;
    }

    this.state = SYNC_STATE.COMPARING;
    this.emit('state', this.state);

    // Ensure local scanner exists
    if (!this.scanners.has(folderName)) {
      const dbPath = path.join(this.configDir, `index_${folderName.replace(/[^a-zA-Z0-9]/g, '_')}.db`);
      this.scanners.set(folderName, new Scanner(folder.path, dbPath));
    }

    const scanner = this.scanners.get(folderName);

    // Build local index
    await scanner.fullScan();
    const localIndex = scanner.getIndex();

    // Send local index to server, get diff
    const response = await this.connection.request({
      type: MSG.INDEX_REQUEST,
      folder: folderName,
      clientIndex: localIndex.map(f => ({ path: f.path, hash: f.hash, size: f.size })),
    }, 60000);

    if (response.type === MSG.ERROR) {
      console.error(`Index request failed: ${response.message}`);
      return;
    }

    const diff = response.diff;
    if (!diff) return;

    const { toDownload, toDelete } = diff;

    if (toDownload.length === 0 && toDelete.length === 0) {
      console.log(`${folderName}: already in sync`);
      return;
    }

    console.log(`${folderName}: ${toDownload.length} to download, ${toDelete.length} to delete`);

    // ---- Apply deletions ----
    for (const relPath of toDelete) {
      const absPath = path.join(folder.path, relPath);
      try {
        await fsp.unlink(absPath);
        scanner.removeFile(absPath);
        console.log(`  Deleted: ${relPath}`);
      } catch (err) {
        if (err.code !== 'ENOENT') {
          console.warn(`  Delete failed: ${relPath}: ${err.message}`);
        }
      }
    }

    // ---- Download files ----
    this.state = SYNC_STATE.TRANSFERRING;
    this.emit('state', this.state);

    let downloaded = 0;
    const totalBytes = toDownload.reduce((sum, f) => sum + f.size, 0);
    let transferredBytes = 0;

    for (const file of toDownload) {
      try {
        await this._downloadFile(folder.path, folderName, file, (bytesReceived) => {
          transferredBytes += bytesReceived;
          this.emit('progress', {
            folder: folderName,
            phase: 'transferring',
            currentFile: file.path,
            fileProgress: Math.round((bytesReceived / file.size) * 100),
            totalProgress: Math.round((transferredBytes / totalBytes) * 100),
            filesComplete: downloaded,
            filesTotal: toDownload.length,
            bytesTransferred: transferredBytes,
            bytesTotal: totalBytes,
          });
        });

        downloaded++;
        scanner.updateFile(path.join(folder.path, file.path));

        // Confirm to server
        this.connection.send({
          type: MSG.FILE_DONE,
          folder: folderName,
          path: file.path,
        });

      } catch (err) {
        console.error(`  Download failed: ${file.path}: ${err.message}`);
        // Don't abort — continue with next file (Syncthing's "stuck file" problem solved)
      }
    }

    // Subscribe to live changes
    this.connection.send({
      type: MSG.SUBSCRIBE,
      folder: folderName,
    });

    console.log(`${folderName}: sync complete — ${downloaded}/${toDownload.length} files, ${toDelete.length} deleted`);
    this.state = SYNC_STATE.DONE;
    this.emit('state', this.state);
    this.emit('sync-complete', { folder: folderName, downloaded, deleted: toDelete.length });
  }

  /**
   * Download a single file from the server. Atomic write with hash verification.
   */
  async _downloadFile(folderPath, folderName, fileInfo, onProgress) {
    const absPath = path.join(folderPath, fileInfo.path);
    const tmpPath = absPath + '.carbonsync.tmp';

    // Ensure parent directory exists
    await fsp.mkdir(path.dirname(absPath), { recursive: true });

    // Request file from server
    const response = await this.connection.request({
      type: MSG.BLOCK_REQUEST,
      folder: folderName,
      path: fileInfo.path,
    }, 120000); // 2 min timeout for large files

    if (response.type === MSG.ERROR) {
      throw new Error(response.message);
    }

    // Wait for binary data
    const data = await new Promise((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('Binary data timeout')), 120000);
      this.connection.once('binary', (binaryData) => {
        clearTimeout(timeout);
        resolve(binaryData);
      });
    });

    // Verify hash before writing
    const hash = crypto.createHash('sha256').update(data).digest('hex');
    if (hash !== fileInfo.hash) {
      throw new Error(`Hash mismatch for ${fileInfo.path}: expected ${fileInfo.hash}, got ${hash}`);
    }

    // Atomic write: tmp → rename
    await fsp.writeFile(tmpPath, data);

    // Set mtime to match server
    if (fileInfo.mtime_ms) {
      const mtime = new Date(fileInfo.mtime_ms);
      await fsp.utimes(tmpPath, mtime, mtime);
    }

    // Atomic rename
    await fsp.rename(tmpPath, absPath);

    if (onProgress) onProgress(data.length);
  }

  // ---- Management ----

  addFolder(folderPath, name) {
    this.config.addFolder(folderPath, name);
    // Trigger sync if connected
    if (this.connection && this.connection.authenticated) {
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
    for (const [name, scanner] of this.scanners) {
      scanner.close();
    }
    this.scanners.clear();
    console.log('CarbonSync client stopped');
  }
}

module.exports = { CarbonSyncClient };
