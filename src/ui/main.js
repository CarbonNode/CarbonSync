/**
 * CarbonSync Electron UI — System Tray + Settings Window
 * Manages the daemon and provides a configuration interface.
 */

const { app, BrowserWindow, Tray, Menu, ipcMain, nativeImage, dialog } = require('electron');
const path = require('path');
const os = require('os');
const { CarbonSyncDevice } = require('../daemon/device');
const { writeFrame } = require('../daemon/transport');
const { getLatestRelease, downloadInstaller, installAndRestart } = require('../daemon/updater');

app.name = 'CarbonSync';
if (process.platform === 'win32') {
  app.setAppUserModelId('com.carbonsync.app');
}

// Set userData to avoid sharing with other Electron apps
app.setPath('userData', path.join(app.getPath('appData'), 'CarbonSync'));

const configDir = path.join(os.homedir(), '.carbonsync');
const iconPath = path.join(__dirname, '..', '..', 'assets', 'icon.ico');
let tray = null;
let mainWindow = null;
let server = null;

function sendToUI(channel, data) {
  if (mainWindow && !mainWindow.isDestroyed()) {
    mainWindow.webContents.send(channel, data);
  }
}

const gotLock = app.requestSingleInstanceLock();
if (!gotLock) { app.quit(); }

app.on('second-instance', () => {
  if (mainWindow) { mainWindow.show(); mainWindow.focus(); }
});

function createTrayIcon() {
  try {
    if (require('fs').existsSync(iconPath)) {
      return nativeImage.createFromPath(iconPath);
    }
  } catch {}
  // Fallback: generated green square
  const size = 16;
  const pixels = Buffer.alloc(size * size * 4);
  for (let i = 0; i < size * size; i++) {
    pixels[i * 4] = 0x22; pixels[i * 4 + 1] = 0xc5;
    pixels[i * 4 + 2] = 0x5e; pixels[i * 4 + 3] = 0xff;
  }
  const img = nativeImage.createFromBuffer(pixels, { width: size, height: size });
  return nativeImage.createFromBuffer(img.toPNG());
}

function createWindow() {
  // Force window onto primary display (multi-monitor fix)
  const { screen } = require('electron');
  const primary = screen.getPrimaryDisplay();

  mainWindow = new BrowserWindow({
    width: 1000, height: 700,
    x: primary.bounds.x + Math.round((primary.bounds.width - 1000) / 2),
    y: primary.bounds.y + Math.round((primary.bounds.height - 700) / 2),
    minWidth: 700, minHeight: 500,
    frame: false, show: false,
    title: 'CarbonSync',
    icon: iconPath,
    backgroundColor: '#0a0a0f',
    webPreferences: {
      preload: path.join(__dirname, 'preload.js'),
      contextIsolation: true,
      nodeIntegration: false,
      sandbox: false, // Required for drag-drop file.path access
    },
  });

  const htmlPath = path.join(__dirname, 'renderer', 'index.html');
  console.log('Loading HTML from:', htmlPath);
  mainWindow.loadFile(htmlPath);

  // Only show window after content is painted (no white flash)
  mainWindow.once('ready-to-show', () => {
    if (!process.argv.includes('--hidden')) {
      mainWindow.show();
    }
  });

  mainWindow.webContents.on('did-fail-load', (e, code, desc) => {
    console.error('Page load failed:', code, desc);
  });

  mainWindow.webContents.on('console-message', (e, level, msg) => {
    console.log('Renderer:', msg);
  });

  mainWindow.on('close', (e) => {
    if (!app.isQuitting) { e.preventDefault(); mainWindow.hide(); }
  });

  if (process.argv.includes('--dev')) {
    mainWindow.webContents.openDevTools({ mode: 'detach' });
  }
}

function updateTrayMenu() {
  const status = server ? server.getStatus() : {};
  const folderCount = status.folders?.length || 0;
  const clientCount = status.connectedClients || 0;

  const menu = Menu.buildFromTemplate([
    { label: `CarbonSync — ${folderCount} folder(s), ${clientCount} client(s)`, enabled: false },
    { type: 'separator' },
    { label: 'Open Settings', click: () => { mainWindow?.show(); mainWindow?.focus(); } },
    { label: 'Add Folder...', click: () => addFolderDialog() },
    { type: 'separator' },
    { label: 'Quit', click: () => { app.isQuitting = true; app.quit(); } },
  ]);
  tray.setContextMenu(menu);
}

async function addFolderDialog() {
  const result = await dialog.showOpenDialog({
    properties: ['openDirectory'],
    title: 'Select folder to sync',
  });
  if (result.canceled || result.filePaths.length === 0) return;
  try {
    await server.addFolder(result.filePaths[0]);
    updateTrayMenu();
    if (mainWindow && !mainWindow.isDestroyed()) {
      mainWindow.webContents.send('status-update', server.getStatus());
    }
  } catch (err) {
    dialog.showErrorBox('Error', err.message);
  }
}

function setupIPC() {
  ipcMain.handle('get-status', () => server?.getStatus() || {});

  ipcMain.handle('add-folder', async (_, folderPath, name) => {
    await server.addFolder(folderPath, name);
    updateTrayMenu();
    return server.getStatus();
  });

  ipcMain.handle('remove-folder', (_, folderPath) => {
    server.removeFolder(folderPath);
    updateTrayMenu();
    return server.getStatus();
  });

  ipcMain.handle('get-config', () => server?.config?.data || {});

  ipcMain.handle('update-settings', (_, settings) => {
    server.config.updateSettings(settings);
    // Apply start-with-Windows immediately if changed
    if (settings.startWithWindows !== undefined) {
      app.setLoginItemSettings({ openAtLogin: settings.startWithWindows, args: ['--hidden'] });
    }
    return true;
  });

  ipcMain.handle('pick-folder', async () => {
    const result = await dialog.showOpenDialog({
      properties: ['openDirectory'],
      title: 'Select folder to sync',
    });
    if (result.canceled) return null;
    return result.filePaths[0];
  });

  ipcMain.handle('rescan', async (_, folderName) => {
    if (server?.engine) return server.engine.rescan(folderName);
  });

  ipcMain.handle('cancel-scan', (_, folderName) => {
    if (server?.engine?.folders.has(folderName)) {
      server.engine.folders.get(folderName).scanner.cancelScan();
      return true;
    }
    return false;
  });

  ipcMain.handle('window-minimize', () => mainWindow?.minimize());
  ipcMain.handle('window-close', () => mainWindow?.hide());

  ipcMain.handle('approve-peer', (_, clientId, selectedFolders) => {
    server.approvePeer(clientId, selectedFolders);
    sendToUI('status-update', server.getStatus());
    return true;
  });

  ipcMain.handle('reject-peer', (_, clientId) => {
    server.rejectPeer(clientId);
    return true;
  });

  ipcMain.handle('set-hub-connection', (_, address, apiKey) => {
    server.config.setHubConnection(address, apiKey);
    server.reconnectHub();
    return server.getStatus();
  });

  ipcMain.handle('set-folder-direction', (_, folderName, direction) => {
    const folder = server.config.folders.find(f => f.name === folderName);
    if (folder) server.config.setFolderDirection(folder.path, direction);
    return server.getStatus();
  });

  ipcMain.handle('set-device-name', (_, name) => {
    if (name && server?.config) {
      server.config.setDeviceName(name.trim());
      return true;
    }
    return false;
  });

  ipcMain.handle('rename-peer', (_, hostname, friendlyName) => {
    if (!server?.config?.data) return;
    if (!server.config.data.peers) server.config.data.peers = {};
    server.config.data.peers[hostname] = friendlyName;
    server.config.save();

    // Tell the peer to update its own device name
    if (server.transport) {
      for (const [, client] of server.transport.clients) {
        if (client.authenticated && (client.deviceName === hostname || client.deviceId === hostname)) {
          writeFrame(client.socket, { type: 'set_device_name', name: friendlyName });
        }
      }
    }

    return server.getStatus();
  });

  ipcMain.handle('set-folder-excludes', (_, folderName, excludes) => {
    const folder = server?.config?.folders.find(f => f.name === folderName);
    if (folder) {
      server.config.setFolderExcludes(folder.path, excludes);
      // Reload ignore patterns in scanner
      if (server.engine?.folders.has(folderName)) {
        server.engine.folders.get(folderName).scanner.reloadIgnore();
      }
    }
    return server?.getStatus();
  });

  ipcMain.handle('get-folder-excludes', (_, folderName) => {
    const folder = server?.config?.folders.find(f => f.name === folderName);
    return folder?.excludes || [];
  });

  ipcMain.handle('add-peer', async (_, rawIp, port) => {
    if (!rawIp) return { error: 'IP required' };
    // Handle ip:port format in the IP field
    let ip = rawIp.trim();
    if (ip.includes(':')) {
      const parts = ip.split(':');
      ip = parts[0];
      port = parseInt(parts[1]) || port;
    }
    port = port || 21547;

    // Prevent connecting to self
    if (ip === '127.0.0.1' || ip === 'localhost') return { error: 'Cannot connect to self' };
    const localIPs = [];
    for (const ifaces of Object.values(require('os').networkInterfaces())) {
      for (const iface of ifaces) { if (iface.family === 'IPv4') localIPs.push(iface.address); }
    }
    if (localIPs.includes(ip)) return { error: 'Cannot connect to self' };

    // Connect to peer and start syncing
    const result = await server.connectToPeer(ip, port);
    if (result.success) {
      // Also add to discovered devices list for UI
      const peer = { role: 'peer', ip, port, hostname: result.deviceName || ip, deviceId: '', name: result.deviceName || ip };
      if (server?.discovery) {
        server.discovery.services.set(ip, peer);
      }
      // Save peer address for auto-reconnect on restart
      if (!server.config.data.savedPeers) server.config.data.savedPeers = [];
      // Get friendly name if we've renamed this peer
      const friendlyName = server.config.data.peers?.[result.deviceName] || result.deviceName || ip;

      const existing = server.config.data.savedPeers.find(p => p.ip === ip && p.port === port);
      if (existing) {
        existing.deviceName = friendlyName;
      } else {
        server.config.data.savedPeers.push({ ip, port, deviceName: friendlyName });
      }
      server.config.save();
      sendToUI('activity', { type: 'connect', message: `Connected to ${result.deviceName || ip}:${port} — syncing folders`, time: Date.now() });
    }
    sendToUI('status-update', server?.getStatus());
    return result;
  });

  ipcMain.handle('remove-peer', async (_, rawIp, port) => {
    let ip = rawIp?.trim();
    if (!ip) return;
    if (ip.includes(':')) {
      const parts = ip.split(':');
      ip = parts[0];
      port = parseInt(parts[1]) || port;
    }
    port = port || 21547;
    const key = `${ip}:${port}`;

    // Disconnect
    if (server.peerConnections?.has(key)) {
      server.peerConnections.get(key).client?.disconnect();
      server.peerConnections.delete(key);
    }

    // Remove from saved peers
    if (server.config.data.savedPeers) {
      server.config.data.savedPeers = server.config.data.savedPeers.filter(p => !(p.ip === ip && p.port === port));
      server.config.save();
    }

    // Remove from discovered
    if (server.discovery) {
      server.discovery.services.delete(ip);
    }

    sendToUI('activity', { type: 'disconnect', message: `Removed peer ${ip}:${port}`, time: Date.now() });
    sendToUI('status-update', server?.getStatus());
    return { success: true };
  });

  // ---- Game Save IPC ----

  ipcMain.handle('get-game-library', () => server?.gameSaveManager?.getLibrary() || []);

  ipcMain.handle('get-save-history', async (_, gameId) => {
    return server?.gameSaveManager?.getHistory(gameId) || [];
  });

  ipcMain.handle('restore-save', async (_, gameId, ts) => {
    return server?.gameSaveManager?.restore(gameId, ts);
  });

  ipcMain.handle('restore-current', async (_, gameId) => {
    return server?.gameSaveManager?.restoreCurrent(gameId);
  });

  ipcMain.handle('scan-games', async () => {
    return server?.gameSaveManager?.scanNow();
  });

  ipcMain.handle('mass-lookup', () => {
    // Run in background — don't block UI
    if (!server?.gameSaveManager) return { found: 0, new: 0 };
    server.gameSaveManager.massLookup().then(result => {
      sendToUI('mass-lookup-done', result);
      sendToUI('status-update', server.getStatus());
    }).catch(err => sendToUI('mass-lookup-done', { error: err.message }));
    return { started: true };
  });

  ipcMain.handle('backup-all', () => {
    // Run in background — don't block UI
    if (!server?.gameSaveManager) return { success: 0, skipped: 0 };
    server.gameSaveManager.backupAll((progress) => {
      sendToUI('backup-all-progress', progress);
    }).then(result => {
      sendToUI('backup-all-done', result);
    }).catch(err => sendToUI('backup-all-done', { error: err.message }));
    return { started: true };
  });

  ipcMain.handle('open-folder', (_, folderPath) => {
    if (folderPath) require('electron').shell.openPath(folderPath);
  });

  ipcMain.handle('open-backup-folder', async (_, gameId, backupDir) => {
    if (!server?.gameSaveManager) return;
    const entry = server.gameSaveManager._library?.get(gameId);
    if (!entry) return;
    const displayName = server.gameSaveManager._getDisplayName(entry);
    const fullPath = require('path').join(server.gameSaveManager.backup.gameDir(displayName), 'backups', backupDir);
    require('electron').shell.openPath(fullPath);
  });

  ipcMain.handle('add-custom-game', async (_, cfg) => {
    return server?.gameSaveManager?.addCustomGame(cfg);
  });

  ipcMain.handle('remove-game', async (_, gameId, deleteBackups) => {
    return server?.gameSaveManager?.removeGame(gameId, deleteBackups);
  });

  ipcMain.handle('rename-game', async (_, gameId, name) => {
    return server?.gameSaveManager?.renameGame(gameId, name);
  });

  ipcMain.handle('set-game-excludes', async (_, gameId, excludes) => {
    return server?.gameSaveManager?.setGameExcludes(gameId, excludes);
  });

  ipcMain.handle('get-game-excludes', (_, gameId) => {
    return server?.gameSaveManager?.getGameExcludes(gameId) || [];
  });

  ipcMain.handle('toggle-game-sync', async (_, gameId, enabled) => {
    return server?.gameSaveManager?.toggleSync(gameId, enabled);
  });

  ipcMain.handle('confirm-game', async (_, gameId) => {
    return server?.gameSaveManager?.confirmGame(gameId);
  });

  ipcMain.handle('dismiss-game', async (_, gameId) => {
    return server?.gameSaveManager?.dismissGame(gameId);
  });

  ipcMain.handle('backup-game-now', async (_, gameId) => {
    return server?.gameSaveManager?.backupNow(gameId);
  });

  ipcMain.handle('pick-game-folder', async () => {
    const result = await dialog.showOpenDialog({
      properties: ['openDirectory'],
      title: 'Select game save folder',
    });
    if (result.canceled) return null;
    return result.filePaths[0];
  });

  ipcMain.handle('get-backup-files', async (_, gameId, backupDir) => {
    return server?.gameSaveManager?.getBackupFiles(gameId, backupDir) || [];
  });

  ipcMain.handle('check-update', async () => {
    try {
      return await getLatestRelease();
    } catch (err) {
      return { error: err.message };
    }
  });

  ipcMain.handle('download-update', async () => {
    try {
      const release = await getLatestRelease();
      if (!release.hasUpdate) return { success: false, message: 'Already up to date' };
      if (!release.url) return { success: false, message: 'No download URL' };

      sendToUI('activity', { type: 'update', message: `Downloading v${release.version}...`, time: Date.now() });

      const installerPath = await downloadInstaller(release.url, (pct) => {
        sendToUI('sync-progress', { phase: 'updating', file: `CarbonSync v${release.version}`, current: pct, total: 100 });
      });

      sendToUI('activity', { type: 'update', message: `Installing v${release.version}...`, time: Date.now() });

      // Run installer silently and quit
      installAndRestart(installerPath, app);

      return { success: true, version: release.version, installing: true };
    } catch (err) {
      return { success: false, message: err.message };
    }
  });
}

app.on('ready', async () => {
  // Start daemon
  server = new CarbonSyncDevice(configDir);

  server.on('ready', () => updateTrayMenu());
  server.on('client-connected', (info) => {
    updateTrayMenu();
    sendToUI('activity', { type: 'client-connected', ...info, time: Date.now() });
    sendToUI('status-update', server.getStatus());
  });
  server.on('client-disconnected', (info) => {
    updateTrayMenu();
    sendToUI('activity', { type: 'client-disconnected', ...info, time: Date.now() });
    sendToUI('status-update', server.getStatus());
  });
  server.on('changes', ({ folder, changes }) => {
    sendToUI('status-update', server.getStatus());
    sendToUI('activity', { type: 'file-changes', folder, count: changes.length, time: Date.now() });
  });
  server.on('progress', (p) => {
    sendToUI('sync-progress', p);
  });
  server.on('sync-progress-update', () => {
    sendToUI('status-update', server.getStatus());
  });
  server.on('sync-request', (request) => {
    console.log(`Sync request from: ${request.deviceName} (${request.ip})`);
    sendToUI('sync-request', request);
    // Show window if hidden so user can approve
    if (mainWindow && !mainWindow.isVisible()) {
      mainWindow.show();
      mainWindow.focus();
    }
  });

  console.log('UI: creating window...');
  createWindow();
  console.log('UI: window created');
  setupIPC();
  console.log('UI: IPC ready');
  // Window shows via ready-to-show event in createWindow()
  mainWindow.focus();
  console.log('UI: window shown');

  // Tray
  tray = new Tray(createTrayIcon());
  tray.setToolTip('CarbonSync');
  updateTrayMenu();
  tray.on('double-click', () => { mainWindow?.show(); mainWindow?.focus(); });
  console.log('UI: tray created');

  if (process.argv.includes('--hidden')) {
    // Stay hidden — tray only
  }

  // Auto-start with Windows (respect config, default: on)
  const startWithWindows = server.config.data.settings?.startWithWindows !== false;
  app.setLoginItemSettings({ openAtLogin: startWithWindows, args: ['--hidden'] });

  // Start server in background (may take a few seconds for firewall/scan)
  server.start().then(() => {
    updateTrayMenu();
    sendToUI('status-update', server.getStatus());

    // Forward game save events to renderer
    if (server.gameSaveManager) {
      server.gameSaveManager.on('game-detected', (info) => sendToUI('game-detected', info));
      server.gameSaveManager.on('save-backed-up', (info) => {
        sendToUI('save-backed-up', info);
        sendToUI('activity', { type: 'game-backup', message: `Backed up ${info.game?.displayName || info.game?.name}: ${info.fileCount} files`, time: Date.now() });
      });
      server.gameSaveManager.on('game-running', (info) => sendToUI('game-running', info));
      server.gameSaveManager.on('save-restored', (info) => {
        sendToUI('save-restored', info);
        sendToUI('activity', { type: 'game-restore', message: `Auto-restored ${info.game?.displayName || info.game?.name} from sync`, time: Date.now() });
      });
    }

    // Auto-reconnect to saved peers
    const savedPeers = server.config.data.savedPeers || [];
    if (savedPeers.length > 0) {
      console.log(`Reconnecting to ${savedPeers.length} saved peer(s)...`);
      for (const peer of savedPeers) {
        const displayName = peer.deviceName || peer.ip;
        server.connectToPeer(peer.ip, peer.port).then(result => {
          if (result.success) {
            // Use saved friendly name, not raw hostname
            const name = peer.deviceName || result.deviceName || peer.ip;
            sendToUI('activity', { type: 'connect', message: `Reconnected to ${name}`, time: Date.now() });
            const peerEntry = { role: 'peer', ip: peer.ip, port: peer.port, hostname: name, friendlyName: name };
            if (server.discovery) server.discovery.services.set(peer.ip, peerEntry);
            sendToUI('status-update', server.getStatus());
          }
        }).catch(() => {});
      }
    }
  }).catch(err => {
    console.error('Server start failed:', err);
    sendToUI('activity', { type: 'error', message: `Server failed: ${err.message}`, time: Date.now() });
  });
});

app.on('before-quit', async () => {
  app.isQuitting = true;
  if (server) await server.stop();
});

app.on('window-all-closed', () => { /* stay in tray */ });

process.on('uncaughtException', (err) => console.error('Uncaught:', err));
process.on('unhandledRejection', (err) => console.error('Unhandled:', err));
