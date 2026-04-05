/**
 * CarbonSync Daemon — runs in a forked child process.
 *
 * All heavy work (SQLite, file hashing, network I/O, scanning 30k+ files)
 * happens here, keeping the Electron main process free for the UI.
 *
 * Communication with parent:
 *   Parent → Child:  { id, type: 'call', method, args }
 *   Child → Parent:  { id, type: 'result', data } | { id, type: 'error', message }
 *   Child → Parent:  { type: 'event', event, data }
 */

const path = require('path');
const fs = require('fs');
const { CarbonSyncDevice } = require('./device');
const { writeFrame } = require('./transport');

const configDir = process.argv[2] || path.join(require('os').homedir(), '.carbonsync');
let server = null;

// ---- RPC Method Handler ----

async function handleCall(method, args) {
  switch (method) {
    // Core
    case 'getStatus': return server.getStatus();
    case 'stop': await server.stop(); return;

    // Folders
    case 'syncFolder': return server.syncFolder(args[0]);
    case 'addFolder': {
      await server.addFolder(args[0], args[1], args[2], args[3]);
      return server.getStatus();
    }
    case 'removeFolder': {
      server.removeFolder(args[0]);
      return server.getStatus();
    }
    case 'cancelScan': {
      if (server.engine?.folders.has(args[0])) {
        server.engine.folders.get(args[0]).scanner.cancelScan();
        return true;
      }
      return false;
    }

    // Config
    case 'getConfig': return server.config.data;
    case 'updateSettings': {
      server.config.updateSettings(args[0]);
      return true;
    }
    case 'setDeviceName': {
      if (args[0] && server.config) {
        server.config.setDeviceName(args[0].trim());
        return true;
      }
      return false;
    }
    case 'renameFolder': {
      server.config.renameFolder(args[0], args[1]);
      if (server.transport) {
        server.transport.broadcast({ type: 'folder_renamed', path: args[0], name: args[1] });
      }
      return server.getStatus();
    }
    case 'setFolderIcon': {
      // args: [folderPath, destPath, iconBase64, ext]
      const folder = server.config.folders.find(f => f.path === path.resolve(args[0]));
      if (folder) {
        folder.icon = args[1];
        server.config.save();
        if (server.transport && args[2]) {
          server.transport.broadcast({
            type: 'folder_icon', path: args[0], name: folder.name,
            iconBase64: args[2], ext: args[3],
          });
        }
      }
      return server.getStatus();
    }
    case 'setFolderGroup': {
      server.config.setFolderGroup(args[0], args[1]);
      return server.getStatus();
    }
    case 'setFolderDirection': {
      const folder = server.config.folders.find(f => f.name === args[0]);
      if (folder) server.config.setFolderDirection(folder.path, args[1]);
      return server.getStatus();
    }
    case 'setFolderExcludes': {
      const folder = server.config.folders.find(f => f.name === args[0]);
      if (folder) {
        server.config.setFolderExcludes(folder.path, args[1]);
        if (server.engine?.folders.has(args[0])) {
          server.engine.folders.get(args[0]).scanner.reloadIgnore();
        }
      }
      return server.getStatus();
    }
    case 'getFolderExcludes': {
      const folder = server.config.folders.find(f => f.name === args[0]);
      return folder?.excludes || [];
    }

    // Peers
    case 'connectToPeer': return server.connectToPeer(args[0], args[1], args[2]);
    case 'addPeer': {
      const ip = args[0];
      const port = args[1] || 21547;
      const result = await server.connectToPeer(ip, port);
      if (result.success) {
        if (server.discovery) {
          server.discovery.services.set(ip, {
            role: 'peer', ip, port, hostname: result.deviceName || ip,
            deviceId: '', name: result.deviceName || ip,
          });
        }
        if (!server.config.data.savedPeers) server.config.data.savedPeers = [];
        const friendlyName = server.config.data.peers?.[result.deviceName] || result.deviceName || ip;
        const existing = server.config.data.savedPeers.find(p => p.ip === ip && p.port === port);
        if (existing) existing.deviceName = friendlyName;
        else server.config.data.savedPeers.push({ ip, port, deviceName: friendlyName });
        server.config.save();
      }
      return result;
    }
    case 'removePeer': {
      const ip = args[0];
      const port = args[1] || 21547;
      const key = `${ip}:${port}`;
      if (server.peerConnections?.has(key)) {
        server.peerConnections.get(key).client?.disconnect();
        server.peerConnections.delete(key);
      }
      if (server.config.data.savedPeers) {
        server.config.data.savedPeers = server.config.data.savedPeers.filter(
          p => !(p.ip === ip && p.port === port)
        );
        server.config.save();
      }
      if (server.discovery) server.discovery.services.delete(ip);
      return { success: true };
    }
    case 'approvePeer': {
      server.approvePeer(args[0], args[1]);
      return server.getStatus();
    }
    case 'rejectPeer': {
      server.rejectPeer(args[0]);
      return true;
    }
    case 'renamePeer': {
      if (!server.config.data.peers) server.config.data.peers = {};
      server.config.data.peers[args[0]] = args[1];
      server.config.save();
      if (server.transport) {
        for (const [, client] of server.transport.clients) {
          if (client.authenticated && (client.deviceName === args[0] || client.deviceId === args[0])) {
            writeFrame(client.socket, { type: 'set_device_name', name: args[1] });
          }
        }
      }
      return server.getStatus();
    }

    // Hub
    case 'setHubConnection': {
      server.config.setHubConnection(args[0], args[1]);
      server.reconnectHub();
      return server.getStatus();
    }

    // Diagnostics
    case 'syncDiag': {
      const peers = [];
      for (const [key, info] of server.peerConnections || new Map()) {
        peers.push({
          address: key, deviceName: info.deviceName,
          connected: info.connected, authenticated: info.client?.authenticated || false,
        });
      }
      let recentLog = '';
      try {
        const logPath = path.join(server.configDir, 'sync.log');
        const content = fs.readFileSync(logPath, 'utf-8');
        recentLog = content.trim().split('\n').slice(-20).join('\n');
      } catch {}
      return {
        peerConnections: peers,
        inboundClients: server.transport?.getConnectedClients() || [],
        hubConnected: server.hubConnection?.authenticated || false,
        pushQueues: Object.fromEntries(
          [...(server._pushQueues || new Map())].map(([k, v]) => [k, v.size])
        ),
        watchedFolders: server.engine?.getFolderNames() || [],
        recentLog,
      };
    }
    case 'forcePush': {
      const folder = server.config.folders.find(f => f.name === args[0]);
      if (!folder) return { error: 'Folder not in config' };
      let pushed = 0;
      for (const [, peerInfo] of server.peerConnections || new Map()) {
        if (peerInfo.connected && peerInfo.client?.authenticated) {
          try {
            await server._pushFullFolderToPeer(peerInfo, folder);
            pushed++;
          } catch (err) { return { error: err.message }; }
        }
      }
      return { pushed, totalPeers: server.peerConnections?.size || 0 };
    }

    // Game saves
    case 'getGameLibrary': return server.gameSaveManager?.getLibrary() || [];
    case 'getSaveHistory': return server.gameSaveManager?.getHistory(args[0]) || [];
    case 'restoreSave': return server.gameSaveManager?.restore(args[0], args[1]);
    case 'restoreCurrent': return server.gameSaveManager?.restoreCurrent(args[0]);
    case 'scanGames': return server.gameSaveManager?.scanNow();
    case 'addCustomGame': return server.gameSaveManager?.addCustomGame(args[0]);
    case 'removeGame': return server.gameSaveManager?.removeGame(args[0], args[1]);
    case 'renameGame': return server.gameSaveManager?.renameGame(args[0], args[1]);
    case 'setGameExcludes': return server.gameSaveManager?.setGameExcludes(args[0], args[1]);
    case 'getGameExcludes': return server.gameSaveManager?.getGameExcludes(args[0]) || [];
    case 'toggleGameSync': return server.gameSaveManager?.toggleSync(args[0], args[1]);
    case 'confirmGame': return server.gameSaveManager?.confirmGame(args[0]);
    case 'dismissGame': return server.gameSaveManager?.dismissGame(args[0]);
    case 'backupGameNow': return server.gameSaveManager?.backupNow(args[0]);
    case 'massLookup': {
      if (!server.gameSaveManager) return { found: 0, new: 0 };
      const result = await server.gameSaveManager.massLookup();
      return result;
    }
    case 'backupAll': {
      if (!server.gameSaveManager) return { success: 0, skipped: 0 };
      const result = await server.gameSaveManager.backupAll((progress) => {
        process.send({ type: 'event', event: 'backup-all-progress', data: progress });
      });
      return result;
    }
    case 'cleanBackups': return server.gameSaveManager?.backup?.cleanBackups() || { removed: 0 };
    case 'getBackupFiles': return server.gameSaveManager?.getBackupFiles(args[0], args[1]) || [];
    case 'getBackupFolderPath': {
      if (!server.gameSaveManager) return null;
      const entry = server.gameSaveManager._library?.get(args[0]);
      if (!entry) return null;
      const displayName = server.gameSaveManager._getDisplayName(entry);
      return path.join(server.gameSaveManager.backup.gameDir(displayName), 'backups', args[1]);
    }

    default:
      throw new Error(`Unknown daemon method: ${method}`);
  }
}

// ---- Setup ----

server = new CarbonSyncDevice(configDir);

// Forward all daemon events to parent process
const EVENTS = [
  'ready', 'scan-complete', 'client-connected', 'client-disconnected',
  'changes', 'progress', 'sync-progress-update', 'peer-folders', 'sync-request',
  'peer-connected', 'peer-disconnected', 'hub-connected', 'hub-disconnected',
  'device-renamed', 'folder-renamed',
];

for (const name of EVENTS) {
  server.on(name, (data) => {
    try { process.send({ type: 'event', event: name, data }); } catch {}
  });
}

// Handle RPC calls from parent
process.on('message', async (msg) => {
  if (msg.type === 'call') {
    try {
      const result = await handleCall(msg.method, msg.args || []);
      process.send({ id: msg.id, type: 'result', data: result });
    } catch (err) {
      process.send({ id: msg.id, type: 'error', message: err.message });
    }
  }
});

// Start
server.start().then(() => {
  console.log('Daemon process ready');

  // Forward game save manager events (created during start)
  if (server.gameSaveManager) {
    const GSM_EVENTS = ['game-detected', 'save-backed-up', 'game-running', 'library-updated', 'save-restored'];
    for (const name of GSM_EVENTS) {
      server.gameSaveManager.on(name, (data) => {
        try { process.send({ type: 'event', event: name, data }); } catch {}
      });
    }
  }

  // Auto-reconnect to saved peers
  const savedPeers = server.config.data.savedPeers || [];
  if (savedPeers.length > 0) {
    console.log(`Reconnecting to ${savedPeers.length} saved peer(s)...`);
    for (const peer of savedPeers) {
      server.connectToPeer(peer.ip, peer.port).then(result => {
        if (result.success) {
          const name = peer.deviceName || result.deviceName || peer.ip;
          console.log(`Reconnected to ${name}`);
          if (server.discovery) {
            server.discovery.services.set(peer.ip, {
              role: 'peer', ip: peer.ip, port: peer.port,
              hostname: name, friendlyName: name,
            });
          }
        }
      }).catch(() => {});
    }
  }
}).catch(err => {
  console.error('Daemon start failed:', err.message);
  process.exit(1);
});

// Graceful shutdown
process.on('SIGTERM', async () => {
  await server.stop();
  process.exit(0);
});
