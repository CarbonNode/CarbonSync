/**
 * CarbonSync Server Daemon
 *
 * Fixed: streaming file transfers (no OOM), stale index protection,
 * paginated index for large folders, concurrent download support.
 */

const path = require('path');
const os = require('os');
const { EventEmitter } = require('events');
const { Config } = require('./config');
const { SyncEngine } = require('./sync-engine');
const { SyncServer, writeFrame, streamFileToSocket } = require('./transport');
const { Discovery } = require('./discovery');
const { ensureFirewallRule } = require('./firewall');
const { ensureCerts } = require('./tls-certs');
const { MSG, SYNC_STATE } = require('../shared/protocol');

const INDEX_PAGE_SIZE = 5000;

class CarbonSyncServer extends EventEmitter {
  constructor(configDir) {
    super();
    this.configDir = configDir || path.join(os.homedir(), '.carbonsync');
    this.config = new Config(this.configDir);
    this.engine = null;
    this.transport = null;
    this.discovery = null;
    this._scanInterval = null;
    this.deviceSync = new Map(); // deviceName -> { folder -> { status, progress, lastSync } }
  }

  async start() {
    console.log('=== CarbonSync Server Starting ===');
    console.log(`Device: ${this.config.deviceName} (${this.config.deviceId})`);
    console.log(`Port: ${this.config.port}`);
    console.log(`Config: ${this.configDir}`);
    console.log(`Folders: ${this.config.folders.length}`);

    await ensureFirewallRule(this.config.port);

    // Sync engine
    this.engine = new SyncEngine({
      configDir: this.configDir,
      folders: this.config.folders.filter(f => f.enabled),
    });

    this.engine.on('scan-progress', (p) => this.emit('progress', p));

    this.engine.on('changes', ({ folder, changes }) => {
      console.log(`Changes in ${folder}: ${changes.length} file(s)`);
      if (this.transport) {
        this.transport.broadcast({
          type: MSG.NOTIFY,
          folder,
          changes: changes.map(c => ({ type: c.type, path: c.path, size: c.size, hash: c.hash })),
        });
      }
      this.emit('changes', { folder, changes });
    });

    await this.engine.start();

    // TLS certificates
    const certs = ensureCerts(this.configDir);
    this.fingerprint = certs.fingerprint;

    // Network transport (TLS if certs available)
    this.transport = new SyncServer({
      port: this.config.port,
      apiKey: this.config.apiKey,
      tlsKey: certs.key,
      tlsCert: certs.cert,
    });

    this.transport.on('client-connected', (c) => {
      console.log(`Client connected: ${c.deviceName}`);
      this.emit('client-connected', { deviceName: c.deviceName, deviceId: c.deviceId });

      // Send available folders to the new client
      const folderList = this.config.folders.filter(f => f.enabled).map(f => ({
        name: f.name,
        path: f.path,
        excludes: f.excludes || [],
        fileCount: this.engine?.folders.get(f.name)?.scanner.getFileCount() || 0,
      }));
      writeFrame(c.socket, { type: 'folder_list', folders: folderList });
    });
    this.transport.on('client-disconnected', (c) => {
      console.log(`Client disconnected: ${c.deviceName || 'unknown'}`);
      this.emit('client-disconnected', { deviceName: c.deviceName });
    });
    this.transport.on('message', (client, msg) => this._handleClientMessage(client, msg));

    this.transport.start();

    // Discovery (don't advertise API key — clients get it during pairing)
    this.discovery = new Discovery({
      port: this.config.port,
      role: 'server',
      deviceId: this.config.deviceId,
      // apiKey intentionally NOT advertised for security
    });
    this.discovery.publish();
    this.discovery.browse();

    // Periodic rescan (watcher handles most changes, this catches edge cases)
    const intervalMs = (this.config.settings.scanIntervalMinutes || 5) * 60 * 1000;
    this._scanInterval = setInterval(async () => {
      for (const name of this.engine.getFolderNames()) {
        await this.engine.rescan(name);
      }
    }, intervalMs);

    console.log('=== CarbonSync Server Ready ===');
    this.emit('ready');
  }

  async _handleClientMessage(client, msg) {
    try {
      switch (msg.type) {
        case MSG.INDEX_REQUEST:
          this._handleIndexRequest(client, msg);
          break;
        case MSG.BLOCK_REQUEST:
          await this._handleBlockRequest(client, msg);
          break;
        case MSG.SUBSCRIBE:
          client.subscriptions.add(msg.folder);
          break;
        case MSG.FILE_DONE:
          console.log(`Client ${client.deviceName} synced: ${msg.folder}/${msg.path}`);
          // Track per-device sync progress
          if (client.deviceName) {
            if (!this.deviceSync.has(client.deviceName)) this.deviceSync.set(client.deviceName, {});
            const ds = this.deviceSync.get(client.deviceName);
            if (!ds[msg.folder]) ds[msg.folder] = { status: 'syncing', filesComplete: 0, filesTotal: 0, lastSync: null };
            ds[msg.folder].filesComplete = (ds[msg.folder].filesComplete || 0) + 1;
            ds[msg.folder].lastFile = msg.path;
            this.emit('sync-progress-update');
          }
          break;

        case 'sync_complete':
          if (client.deviceName) {
            if (!this.deviceSync.has(client.deviceName)) this.deviceSync.set(client.deviceName, {});
            const ds = this.deviceSync.get(client.deviceName);
            ds[msg.folder] = { status: 'synced', filesComplete: msg.filesComplete || 0, filesTotal: msg.filesTotal || 0, lastSync: Date.now() };
            this.emit('sync-progress-update');
          }
          break;

        case 'set_excludes':
          if (msg.folder && Array.isArray(msg.excludes)) {
            const folder = this.config.folders.find(f => f.name === msg.folder);
            if (folder) {
              this.config.setFolderExcludes(folder.path, msg.excludes);
              writeFrame(client.socket, { type: 'excludes_updated', folder: msg.folder, excludes: msg.excludes, _requestId: msg._requestId });
            }
          }
          break;
        case MSG.PING:
          writeFrame(client.socket, { type: MSG.PONG, _requestId: msg._requestId });
          break;
        default:
          writeFrame(client.socket, { type: MSG.ERROR, message: 'Unknown message type', _requestId: msg._requestId });
      }
    } catch (err) {
      console.error(`Error handling message from ${client.deviceName}: ${err.message}`);
      writeFrame(client.socket, { type: MSG.ERROR, message: 'Server error', _requestId: msg._requestId });
    }
  }

  _handleIndexRequest(client, msg) {
    const folderName = msg.folder;
    const folder = this.engine.folders.get(folderName);

    if (!folder) {
      writeFrame(client.socket, { type: MSG.ERROR, message: `Unknown folder: ${folderName}`, _requestId: msg._requestId });
      return;
    }

    // Don't serve stale index
    if (folder.scanner.isStale()) {
      writeFrame(client.socket, { type: MSG.ERROR, message: 'Index is stale, rescan in progress', _requestId: msg._requestId });
      return;
    }

    if (msg.clientIndex) {
      // Diff mode
      const diff = this.engine.computeDiff(folderName, msg.clientIndex);
      writeFrame(client.socket, {
        type: MSG.INDEX_RESPONSE,
        folder: folderName,
        rootHash: this.engine.getRootHash(folderName),
        diff,
        _requestId: msg._requestId,
      });
    } else if (msg.page !== undefined) {
      // Paginated mode for large indexes
      const page = folder.scanner.getIndexPage(msg.page * INDEX_PAGE_SIZE, INDEX_PAGE_SIZE);
      const totalFiles = folder.scanner.getFileCount();
      writeFrame(client.socket, {
        type: MSG.INDEX_RESPONSE,
        folder: folderName,
        page: msg.page,
        pageSize: INDEX_PAGE_SIZE,
        totalFiles,
        totalPages: Math.ceil(totalFiles / INDEX_PAGE_SIZE),
        index: page,
        _requestId: msg._requestId,
      });
    } else {
      // Full index — paginate if large
      const totalFiles = folder.scanner.getFileCount();
      if (totalFiles > INDEX_PAGE_SIZE) {
        // Too large for one frame — tell client to use pagination
        writeFrame(client.socket, {
          type: MSG.INDEX_RESPONSE,
          folder: folderName,
          rootHash: this.engine.getRootHash(folderName),
          totalFiles,
          paginated: true,
          totalPages: Math.ceil(totalFiles / INDEX_PAGE_SIZE),
          _requestId: msg._requestId,
        });
      } else {
        const index = this.engine.getIndex(folderName);
        const info = this.engine.getFolderInfo(folderName);
        writeFrame(client.socket, {
          type: MSG.INDEX_RESPONSE,
          folder: folderName,
          rootHash: info?.rootHash || '',
          index: index || [],
          fileCount: index?.length || 0,
          totalSize: info?.totalSize || 0,
          _requestId: msg._requestId,
        });
      }
    }
  }

  async _handleBlockRequest(client, msg) {
    const { folder, path: relPath } = msg;

    try {
      const folderObj = this.engine.folders.get(folder);
      if (!folderObj) throw new Error(`Unknown folder: ${folder}`);

      const absPath = path.join(folderObj.path, relPath);
      const resolved = path.resolve(absPath);
      if (!resolved.startsWith(folderObj.path)) throw new Error('Path traversal');

      const stat = await require('fs/promises').stat(resolved);

      if (stat.size > 4 * 1024 * 1024) {
        // Large file: stream in chunks (no OOM)
        await streamFileToSocket(client.socket, resolved, {
          folder, path: relPath,
          size: stat.size,
          _requestId: msg._requestId,
        });
      } else {
        // Small file: send in one frame
        const data = await require('fs/promises').readFile(resolved);
        writeFrame(client.socket, {
          type: MSG.BLOCK_RESPONSE,
          folder, path: relPath,
          size: data.length,
          _requestId: msg._requestId,
        });
        // Send binary data
        const header = Buffer.alloc(5);
        header.writeUInt32BE(data.length + 1);
        header[4] = 0xFF;
        client.socket.write(Buffer.concat([header, data]));
        // Send transfer end
        writeFrame(client.socket, {
          type: 'transfer_end',
          folder, path: relPath,
          bytesSent: data.length,
        });
      }
    } catch (err) {
      writeFrame(client.socket, {
        type: MSG.ERROR,
        message: `Read failed: ${err.message}`,
        _requestId: msg._requestId,
      });
    }
  }

  // ---- Status ----

  getStatus() {
    const folders = [];
    if (this.engine) {
      for (const name of this.engine.getFolderNames()) {
        const info = this.engine.getFolderInfo(name);
        const cfgFolder = this.config.folders.find(f => f.name === name);
        info.excludes = cfgFolder?.excludes || [];

        // Add per-device sync status for this folder
        info.devices = {};
        info.devices[this.config.deviceName] = { status: 'source', progress: 100 };
        for (const [deviceName, folderSync] of this.deviceSync) {
          if (folderSync[name]) {
            const ds = folderSync[name];
            info.devices[deviceName] = {
              status: ds.status,
              progress: ds.filesTotal > 0 ? Math.round((ds.filesComplete / ds.filesTotal) * 100) : 0,
              filesComplete: ds.filesComplete,
              filesTotal: ds.filesTotal,
              lastSync: ds.lastSync,
              lastFile: ds.lastFile,
            };
          }
        }
        folders.push(info);
      }
    }

    // Get peer names from config
    const peers = this.config.data.peers || {};

    return {
      deviceName: this.config.deviceName,
      deviceId: this.config.deviceId,
      port: this.config.port,
      apiKey: this.config.apiKey,
      fingerprint: this.fingerprint || '',
      tlsEnabled: !!this.transport?.tlsKey,
      connectedClients: this.transport?.getClientCount() || 0,
      folders,
      discoveredDevices: (this.discovery?.getServices() || []).map(d => ({
        ...d,
        friendlyName: peers[d.hostname] || d.hostname,
      })),
      peers,
    };
  }

  async addFolder(folderPath, name) {
    this.config.addFolder(folderPath, name);
    const folder = this.config.folders.find(f => f.path === path.resolve(folderPath));
    if (folder) {
      this.engine.addFolder(folder);
      await this.engine.rescan(folder.name);
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

  async stop() {
    if (this._scanInterval) { clearInterval(this._scanInterval); this._scanInterval = null; }
    if (this.discovery) this.discovery.stop();
    if (this.transport) this.transport.stop();
    if (this.engine) await this.engine.stop();
    console.log('CarbonSync server stopped');
  }
}

module.exports = { CarbonSyncServer };
