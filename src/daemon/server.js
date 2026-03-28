/**
 * CarbonSync Server Daemon
 *
 * The main entry point for the server (hub) side. Ties together:
 * - Config management
 * - File scanning and indexing
 * - File watching for live changes
 * - Network transport (serving files to clients)
 * - mDNS discovery
 * - Firewall rule management
 *
 * This is the authoritative source. Clients pull from here. Server always wins.
 */

const path = require('path');
const os = require('os');
const { EventEmitter } = require('events');
const { Config } = require('./config');
const { SyncEngine } = require('./sync-engine');
const { SyncServer, writeFrame } = require('./transport');
const { Discovery } = require('./discovery');
const { ensureFirewallRule } = require('./firewall');
const { MSG, SYNC_STATE } = require('../shared/protocol');

class CarbonSyncServer extends EventEmitter {
  constructor(configDir) {
    super();
    this.configDir = configDir || path.join(os.homedir(), '.carbonsync');
    this.config = new Config(this.configDir);
    this.engine = null;
    this.transport = null;
    this.discovery = null;
    this._scanInterval = null;
  }

  async start() {
    console.log('=== CarbonSync Server Starting ===');
    console.log(`Device: ${this.config.deviceName} (${this.config.deviceId})`);
    console.log(`Port: ${this.config.port}`);
    console.log(`Config: ${this.configDir}`);
    console.log(`Folders: ${this.config.folders.length}`);

    // 1. Ensure firewall rule
    await ensureFirewallRule(this.config.port);

    // 2. Start sync engine (scanner + watcher)
    this.engine = new SyncEngine({
      configDir: this.configDir,
      folders: this.config.folders.filter(f => f.enabled),
    });

    this.engine.on('scan-progress', (progress) => {
      this.emit('progress', progress);
    });

    this.engine.on('changes', ({ folder, changes }) => {
      console.log(`Changes in ${folder}: ${changes.length} file(s)`);
      // Push notifications to connected clients
      if (this.transport) {
        this.transport.broadcast({
          type: MSG.NOTIFY,
          folder,
          changes: changes.map(c => ({
            type: c.type,
            path: c.path,
            size: c.size,
            hash: c.hash,
          })),
        });
      }
      this.emit('changes', { folder, changes });
    });

    await this.engine.start();

    // 3. Start network transport
    this.transport = new SyncServer({
      port: this.config.port,
      configDir: this.configDir,
      apiKey: this.config.apiKey,
    });

    this.transport.on('client-connected', (client) => {
      console.log(`Client connected: ${client.deviceName}`);
      this.emit('client-connected', { deviceName: client.deviceName, deviceId: client.deviceId });
    });

    this.transport.on('client-disconnected', (client) => {
      console.log(`Client disconnected: ${client.deviceName || 'unknown'}`);
      this.emit('client-disconnected', { deviceName: client.deviceName });
    });

    this.transport.on('message', (client, msg) => {
      this._handleClientMessage(client, msg);
    });

    this.transport.start();

    // 4. Start mDNS discovery
    this.discovery = new Discovery({
      port: this.config.port,
      role: 'server',
      apiKey: this.config.apiKey,
      deviceId: this.config.deviceId,
    });
    this.discovery.publish();

    // 5. Periodic rescan
    const intervalMs = (this.config.settings.scanIntervalMinutes || 5) * 60 * 1000;
    this._scanInterval = setInterval(async () => {
      for (const name of this.engine.getFolderNames()) {
        await this.engine.rescan(name);
      }
    }, intervalMs);

    console.log('=== CarbonSync Server Ready ===');
    this.emit('ready');
  }

  /**
   * Handle messages from authenticated clients.
   */
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
          // Client confirms file received — log it
          console.log(`Client ${client.deviceName} synced: ${msg.folder}/${msg.path}`);
          break;

        case MSG.PING:
          writeFrame(client.socket, { type: MSG.PONG, _requestId: msg._requestId });
          break;

        default:
          writeFrame(client.socket, {
            type: MSG.ERROR,
            message: `Unknown message type: ${msg.type}`,
            _requestId: msg._requestId,
          });
      }
    } catch (err) {
      console.error(`Error handling message from ${client.deviceName}: ${err.message}`);
      writeFrame(client.socket, {
        type: MSG.ERROR,
        message: err.message,
        _requestId: msg._requestId,
      });
    }
  }

  _handleIndexRequest(client, msg) {
    const folderName = msg.folder;

    if (msg.clientIndex) {
      // Client sent their index — compute diff
      const diff = this.engine.computeDiff(folderName, msg.clientIndex);
      writeFrame(client.socket, {
        type: MSG.INDEX_RESPONSE,
        folder: folderName,
        rootHash: this.engine.getRootHash(folderName),
        diff,
        _requestId: msg._requestId,
      });
    } else {
      // Client wants full index
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

  async _handleBlockRequest(client, msg) {
    const { folder, path: relPath, offset, length } = msg;

    try {
      let data;
      if (offset !== undefined && length !== undefined) {
        // Chunk request
        data = await this.engine.readFileChunk(folder, relPath, offset, length);
      } else {
        // Full file request
        data = await this.engine.readFile(folder, relPath);
      }

      // Send metadata frame
      writeFrame(client.socket, {
        type: MSG.BLOCK_RESPONSE,
        folder,
        path: relPath,
        offset: offset || 0,
        length: data.length,
        _requestId: msg._requestId,
      });

      // Send binary data frame
      const { writeBinaryFrame } = require('./transport');
      writeBinaryFrame(client.socket, data);

    } catch (err) {
      writeFrame(client.socket, {
        type: MSG.ERROR,
        message: `Failed to read ${relPath}: ${err.message}`,
        _requestId: msg._requestId,
      });
    }
  }

  // ---- Status ----

  getStatus() {
    const folders = [];
    for (const name of this.engine.getFolderNames()) {
      folders.push(this.engine.getFolderInfo(name));
    }
    return {
      deviceName: this.config.deviceName,
      deviceId: this.config.deviceId,
      port: this.config.port,
      apiKey: this.config.apiKey,
      connectedClients: this.transport?.getClientCount() || 0,
      folders,
      discoveredDevices: this.discovery?.getServices() || [],
    };
  }

  // ---- Folder management (live) ----

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

  // ---- Shutdown ----

  async stop() {
    if (this._scanInterval) {
      clearInterval(this._scanInterval);
      this._scanInterval = null;
    }
    if (this.discovery) this.discovery.stop();
    if (this.transport) this.transport.stop();
    if (this.engine) await this.engine.stop();
    console.log('CarbonSync server stopped');
  }
}

module.exports = { CarbonSyncServer };
