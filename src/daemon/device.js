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

const MAX_CONCURRENT_PUSHES = 4;
const PUSH_DEBOUNCE_MS = 2000;
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
      console.log(`Changes in ${folder}: ${filtered.length} file(s) [direction: ${direction}] peers: ${peerCount}`);

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
    this.engine.start().then(() => {
      this.emit('scan-complete');
      console.log('Initial scan complete');
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
    });

    this.transport.on('client-connected', (c) => {
      console.log(`Client connected: ${c.deviceName}`);

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

    // No periodic rescan — watcher handles real-time changes
    // Startup scan above catches anything missed while app was closed
    // Manual rescan available via UI button

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
      this._startDirectionalSync();
    });

    this.hubConnection.on('message', (msg) => {
      if (msg.type === MSG.NOTIFY) {
        const folderConfig = this.config.folders.find(f => f.name === msg.folder);
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
            await this._pullFolder(folder);
            await this._pushFullFolder(folder);
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
        peerInfo.connected = true;
        console.log(`Connected to peer: ${peerInfo.deviceName} (${key})`);
        this.emit('peer-connected', { ip, port, deviceName: peerInfo.deviceName });

        // Send our folder list to the peer
        const myFolders = this.config.folders.filter(f => f.enabled && !f.internal).map(f => ({
          id: f.id, name: f.name, fileCount: this.engine?.folders.get(f.name)?.scanner.getFileCount() || 0,
          direction: f.direction || 'both',
        }));
        writeFrame(client.socket, { type: MSG.FOLDER_LIST, folders: myFolders });

        // Start syncing all folders with this peer
        this._syncWithPeer(peerInfo);
        resolve({ success: true, deviceName: peerInfo.deviceName });
      });

      client.on('message', (msg) => {
        if (msg.type === MSG.NOTIFY) {
          // Peer has changes — re-sync that folder
          const folderConfig = this.config.folders.find(f => f.name === msg.folder);
          if (folderConfig && (folderConfig.direction === 'receive' || folderConfig.direction === 'both')) {
            this._pullFolderFromPeer(peerInfo, folderConfig);
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
   * Sync all folders with a connected peer based on direction.
   */
  async _syncWithPeer(peerInfo) {
    if (!peerInfo.connected) return;

    for (const folder of this.config.folders) {
      if (!folder.enabled) continue;
      try {
        switch (folder.direction) {
          case 'receive':
          case 'both':
            await this._pullFolderFromPeer(peerInfo, folder);
            peerInfo.client.send({ type: MSG.SUBSCRIBE, folder: folder.name });
            break;
        }
        // Push is handled by the watcher — when files change locally,
        // they get pushed to ALL connected peers
      } catch (err) {
        console.error(`Peer sync failed [${folder.name}]: ${err.message}`);
      }
    }
  }

  /**
   * Pull a folder from a specific peer.
   */
  async _pullFolderFromPeer(peerInfo, folder) {
    if (!peerInfo.client?.authenticated) return;
    if (!this.engine?.folders.has(folder.name)) return;

    const scanner = this.engine.folders.get(folder.name).scanner;
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

    if (response.type === MSG.ERROR || !response.diff) return;

    const { toDownload = [], toDelete = [], toCopy = [] } = response.diff;
    if (toDownload.length === 0 && toDelete.length === 0 && toCopy.length === 0) return;

    console.log(`Pull from ${peerInfo.deviceName}: ${folder.name} — ${toDownload.length} download, ${toDelete.length} delete`);

    // Deletions
    for (const relPath of toDelete) {
      const absPath = path.join(folder.path, relPath);
      try {
        this._markRecentlyWritten(folder.name, relPath);
        await fsp.unlink(absPath);
        scanner.removeFile(absPath);
      } catch (err) {
        if (err.code !== 'ENOENT') console.warn(`Delete failed: ${err.message}`);
      }
    }

    // Downloads
    for (const file of toDownload) {
      try {
        await this._downloadFileFromPeer(peerInfo, folder, file, scanner);
      } catch (err) {
        console.error(`Download from peer failed [${file.path}]: ${err.message}`);
      }
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

  async _pullFolder(folder) {
    if (!this.hubConnection?.authenticated) return;
    if (!this.engine?.folders.has(folder.name)) return;

    const scanner = this.engine.folders.get(folder.name).scanner;
    const localIndex = scanner.getIndex();

    const response = await this.hubConnection.request({
      type: MSG.INDEX_REQUEST,
      folder: folder.name,
      clientIndex: localIndex.map(f => ({ path: f.path, hash: f.hash, size: f.size })),
    }, 60000);

    if (response.type === MSG.ERROR) {
      console.error(`Pull index error for ${folder.name}: ${response.message}`);
      return;
    }

    const diff = response.diff;
    if (!diff) return;

    const { toDownload = [], toDelete = [], toCopy = [] } = diff;

    if (toDownload.length === 0 && toDelete.length === 0 && toCopy.length === 0) {
      console.log(`${folder.name}: in sync (pull)`);
      return;
    }

    console.log(`${folder.name} pull: ${toDownload.length} download, ${toCopy.length} copy, ${toDelete.length} delete`);

    // Deletions
    for (const relPath of toDelete) {
      const absPath = path.join(folder.path, relPath);
      try {
        this._markRecentlyWritten(folder.name, relPath);
        await fsp.unlink(absPath);
        scanner.removeFile(absPath);
      } catch (err) {
        if (err.code !== 'ENOENT') console.warn(`Delete failed [${relPath}]: ${err.message}`);
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
      } catch {
        toDownload.push({ path: copy.to, size: copy.size, hash: copy.hash, mtime_ms: copy.mtime_ms });
      }
    }

    // Downloads
    for (const file of toDownload) {
      try {
        await this._downloadFile(folder, file, scanner);
      } catch (err) {
        console.error(`Download failed [${file.path}]: ${err.message}`);
      }
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
    if (!this.engine?.folders.has(folder.name)) return;

    const scanner = this.engine.folders.get(folder.name).scanner;
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

    if (needed.length === 0 && toDelete.length === 0) {
      console.log(`${folder.name}: in sync (push)`);
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
      if (!peerInfo.connected) return;
      const items = [...queue].map(s => JSON.parse(s));
      queue.clear();

      const folder = this.config.folders.find(f => f.name === folderName);
      if (!folder) return;

      console.log(`Pushing ${items.length} changes to peer ${peerInfo.deviceName} for ${folderName}`);
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
          this._handleIndexRequest(client, msg);
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

  _handleFilePushStart(client, msg) {
    const folder = this.engine?.folders.get(msg.folder);
    if (!folder) {
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

    const folder = this.engine?.folders.get(pending.folder);
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
    const folder = this.engine?.folders.get(msg.folder);
    if (!folder) return;

    const absPath = path.join(folder.path, msg.path);
    try {
      this._markRecentlyWritten(msg.folder, msg.path);
      await fsp.unlink(absPath);
      folder.scanner.removeFile(absPath);
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
    const folder = this.engine?.folders.get(msg.folder);
    if (!folder) {
      writeFrame(client.socket, { type: MSG.ERROR, message: 'Unknown folder', _requestId: msg._requestId });
      return;
    }

    const hubIndex = folder.scanner.getIndexMap();
    const clientIndex = msg.index || [];

    // What does the hub need from this device?
    const needed = [];
    const clientMap = new Map();
    for (const entry of clientIndex) clientMap.set(entry.path, entry);

    for (const [relPath, clientEntry] of clientMap) {
      const hubEntry = hubIndex.get(relPath);
      if (!hubEntry || hubEntry.hash !== clientEntry.hash) {
        // Skip fast hashes — they're not comparable
        if (clientEntry.hash.startsWith('fast:')) continue;
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

  _handleIndexRequest(client, msg) {
    const folder = this.engine?.folders.get(msg.folder);
    if (!folder) {
      writeFrame(client.socket, { type: MSG.ERROR, message: 'Unknown folder', _requestId: msg._requestId });
      return;
    }

    if (folder.scanner.isStale()) {
      writeFrame(client.socket, { type: MSG.ERROR, message: 'Index stale', _requestId: msg._requestId });
      return;
    }

    if (msg.clientIndex) {
      const diff = this.engine.computeDiff(msg.folder, msg.clientIndex);
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
    const folder = this.engine?.folders.get(msg.folder);
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

  async addFolder(folderPath, name, direction, folderId) {
    this.config.addFolder(folderPath, name, direction, folderId);
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
