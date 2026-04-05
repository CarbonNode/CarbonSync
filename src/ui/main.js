/**
 * CarbonSync Electron UI — System Tray + Settings Window
 *
 * The daemon runs in a SEPARATE PROCESS (daemon-process.js) so that
 * heavy work (SQLite, hashing, network) never blocks the UI.
 * All communication goes through DaemonProxy (IPC messages).
 */

const { app, BrowserWindow, Tray, Menu, ipcMain, nativeImage, dialog, shell } = require('electron');
const path = require('path');
const fs = require('fs');
const os = require('os');
const { DaemonProxy } = require('../daemon/daemon-proxy');
const { getLatestRelease, downloadInstaller, installAndRestart } = require('../daemon/updater');

app.name = 'CarbonSync';
if (process.platform === 'win32') {
  app.setAppUserModelId('com.carbonsync.app');
}
app.setPath('userData', path.join(app.getPath('appData'), 'CarbonSync'));

const configDir = path.join(os.homedir(), '.carbonsync');
const iconPath = (() => {
  const extra = path.join(process.resourcesPath || '', 'icon.ico');
  if (fs.existsSync(extra)) return extra;
  return path.join(__dirname, '..', '..', 'assets', 'icon.ico');
})();

let tray = null;
let mainWindow = null;
let server = null;       // DaemonProxy instance
let cachedStatus = {};   // Last known status (for sync tray menu updates)

function sendToUI(channel, data) {
  if (mainWindow && !mainWindow.isDestroyed()) {
    mainWindow.webContents.send(channel, data);
  }
}

const gotLock = app.requestSingleInstanceLock();
if (!gotLock) { app.quit(); }
app.on('second-instance', () => { mainWindow?.show(); mainWindow?.focus(); });

// ---- Window & Tray ----

function createTrayIcon() {
  try {
    if (fs.existsSync(iconPath)) return nativeImage.createFromPath(iconPath);
  } catch {}
  const size = 16;
  const pixels = Buffer.alloc(size * size * 4);
  for (let i = 0; i < size * size; i++) {
    pixels[i * 4] = 0x22; pixels[i * 4 + 1] = 0xc5;
    pixels[i * 4 + 2] = 0x5e; pixels[i * 4 + 3] = 0xff;
  }
  return nativeImage.createFromBuffer(
    nativeImage.createFromBuffer(pixels, { width: size, height: size }).toPNG()
  );
}

function createWindow() {
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
      sandbox: false,
    },
  });

  mainWindow.loadFile(path.join(__dirname, 'renderer', 'index.html'));
  mainWindow.once('ready-to-show', () => {
    if (!process.argv.includes('--hidden')) mainWindow.show();
  });
  mainWindow.webContents.on('did-fail-load', (e, code, desc) => console.error('Page load failed:', code, desc));
  mainWindow.webContents.on('console-message', (e, level, msg) => console.log('Renderer:', msg));
  mainWindow.on('close', (e) => { if (!app.isQuitting) { e.preventDefault(); mainWindow.hide(); } });
  if (process.argv.includes('--dev')) mainWindow.webContents.openDevTools({ mode: 'detach' });
}

function updateTrayMenu() {
  if (!tray) return;
  const folderCount = cachedStatus.folders?.length || 0;
  const clientCount = cachedStatus.connectedClients || 0;
  tray.setContextMenu(Menu.buildFromTemplate([
    { label: `CarbonSync — ${folderCount} folder(s), ${clientCount} client(s)`, enabled: false },
    { type: 'separator' },
    { label: 'Open Settings', click: () => { mainWindow?.show(); mainWindow?.focus(); } },
    { label: 'Add Folder...', click: () => addFolderDialog() },
    { type: 'separator' },
    { label: 'Quit', click: () => { app.isQuitting = true; app.quit(); } },
  ]));
}

async function addFolderDialog() {
  const result = await dialog.showOpenDialog({ properties: ['openDirectory'], title: 'Select folder to sync' });
  if (result.canceled || !result.filePaths[0]) return;
  try {
    const status = await server.call('addFolder', result.filePaths[0]);
    cachedStatus = status || cachedStatus;
    updateTrayMenu();
    sendToUI('status-update', cachedStatus);
  } catch (err) {
    dialog.showErrorBox('Error', err.message);
  }
}

// Helper: call daemon and refresh status
async function callAndRefresh(method, ...args) {
  const result = await server.call(method, ...args);
  // Most mutation calls return the new status
  if (result && result.folders) cachedStatus = result;
  sendToUI('status-update', cachedStatus);
  return result;
}

// ---- IPC Handlers ----

function setupIPC() {
  // Status & config
  ipcMain.handle('get-status', async () => {
    try { cachedStatus = await server.call('getStatus'); } catch {}
    return cachedStatus;
  });
  ipcMain.handle('get-config', () => server.call('getConfig'));

  // Folders
  ipcMain.handle('add-folder', async (_, folderPath, name, direction, folderId) => {
    const status = await server.call('addFolder', folderPath, name, direction, folderId);
    cachedStatus = status || cachedStatus;
    updateTrayMenu();
    sendToUI('status-update', cachedStatus);
    return cachedStatus;
  });
  ipcMain.handle('remove-folder', async (_, folderPath) => {
    const status = await server.call('removeFolder', folderPath);
    cachedStatus = status || cachedStatus;
    updateTrayMenu();
    return cachedStatus;
  });
  ipcMain.handle('rescan', (_, folderName) => server.call('syncFolder', folderName));
  ipcMain.handle('cancel-scan', (_, folderName) => server.call('cancelScan', folderName));
  ipcMain.handle('rename-folder', async (_, folderPath, newName) => {
    const status = await server.call('renameFolder', folderPath, newName);
    cachedStatus = status || cachedStatus;
    sendToUI('status-update', cachedStatus);
    return cachedStatus;
  });
  ipcMain.handle('set-folder-icon', async (_, folderPath) => {
    const result = await dialog.showOpenDialog({
      properties: ['openFile'], title: 'Choose folder icon',
      filters: [{ name: 'Images', extensions: ['png', 'jpg', 'jpeg', 'ico', 'svg', 'webp'] }],
    });
    if (result.canceled || !result.filePaths[0]) return null;

    const src = result.filePaths[0];
    const ext = path.extname(src);
    // Copy icon locally (main process handles file dialogs + copies)
    const iconDir = path.join(configDir, 'folder-icons');
    fs.mkdirSync(iconDir, { recursive: true });
    // Use a temp name, daemon will find folder by path
    const config = await server.call('getConfig');
    const folder = config.folders?.find(f => f.path === path.resolve(folderPath));
    if (!folder) return null;
    const destName = folder.name.replace(/[^a-zA-Z0-9]/g, '_') + ext;
    const dest = path.join(iconDir, destName);
    fs.copyFileSync(src, dest);
    const iconBase64 = fs.readFileSync(dest).toString('base64');

    await server.call('setFolderIcon', folderPath, dest, iconBase64, ext);
    cachedStatus = await server.call('getStatus');
    sendToUI('status-update', cachedStatus);
    return dest;
  });
  ipcMain.handle('set-folder-group', (_, folderPath, group) => callAndRefresh('setFolderGroup', folderPath, group));
  ipcMain.handle('set-folder-direction', (_, folderName, direction) => callAndRefresh('setFolderDirection', folderName, direction));
  ipcMain.handle('set-folder-excludes', (_, folderName, excludes) => callAndRefresh('setFolderExcludes', folderName, excludes));
  ipcMain.handle('get-folder-excludes', (_, folderName) => server.call('getFolderExcludes', folderName));

  // Settings
  ipcMain.handle('update-settings', async (_, settings) => {
    await server.call('updateSettings', settings);
    if (settings.startWithWindows !== undefined) {
      app.setLoginItemSettings({ openAtLogin: settings.startWithWindows, args: ['--hidden'] });
    }
    return true;
  });
  ipcMain.handle('set-device-name', (_, name) => server.call('setDeviceName', name));

  // Peers
  ipcMain.handle('add-peer', async (_, rawIp, port) => {
    if (!rawIp) return { error: 'IP required' };
    let ip = rawIp.trim();
    if (ip.includes(':')) { const p = ip.split(':'); ip = p[0]; port = parseInt(p[1]) || port; }
    port = port || 21547;

    if (ip === '127.0.0.1' || ip === 'localhost') return { error: 'Cannot connect to self' };
    const localIPs = [];
    for (const ifaces of Object.values(os.networkInterfaces())) {
      for (const iface of ifaces) { if (iface.family === 'IPv4') localIPs.push(iface.address); }
    }
    if (localIPs.includes(ip)) return { error: 'Cannot connect to self' };

    const result = await server.call('addPeer', ip, port);
    if (result.success) {
      sendToUI('activity', { type: 'connect', message: `Connected to ${result.deviceName || ip}:${port} — syncing folders`, time: Date.now() });
    }
    cachedStatus = await server.call('getStatus');
    sendToUI('status-update', cachedStatus);
    return result;
  });
  ipcMain.handle('remove-peer', async (_, rawIp, port) => {
    let ip = rawIp?.trim();
    if (!ip) return;
    if (ip.includes(':')) { const p = ip.split(':'); ip = p[0]; port = parseInt(p[1]) || port; }
    port = port || 21547;
    await server.call('removePeer', ip, port);
    sendToUI('activity', { type: 'disconnect', message: `Removed peer ${ip}:${port}`, time: Date.now() });
    cachedStatus = await server.call('getStatus');
    sendToUI('status-update', cachedStatus);
    return { success: true };
  });
  ipcMain.handle('approve-peer', async (_, clientId, selectedFolders) => {
    await server.call('approvePeer', clientId, selectedFolders);
    cachedStatus = await server.call('getStatus');
    sendToUI('status-update', cachedStatus);
    return true;
  });
  ipcMain.handle('reject-peer', (_, clientId) => server.call('rejectPeer', clientId));
  ipcMain.handle('rename-peer', async (_, hostname, friendlyName) => {
    return callAndRefresh('renamePeer', hostname, friendlyName);
  });

  // Hub
  ipcMain.handle('set-hub-connection', (_, address, apiKey) => callAndRefresh('setHubConnection', address, apiKey));

  // Window
  ipcMain.handle('window-minimize', () => mainWindow?.minimize());
  ipcMain.handle('window-close', () => mainWindow?.hide());
  ipcMain.handle('pick-folder', async () => {
    const r = await dialog.showOpenDialog({ properties: ['openDirectory'], title: 'Select folder to sync' });
    return r.canceled ? null : r.filePaths[0];
  });

  // Diagnostics
  ipcMain.handle('sync-diag', () => server.call('syncDiag'));
  ipcMain.handle('force-push', (_, folder) => server.call('forcePush', folder));

  // Game saves — all proxied to daemon
  ipcMain.handle('get-game-library', () => server.call('getGameLibrary'));
  ipcMain.handle('get-save-history', (_, gameId) => server.call('getSaveHistory', gameId));
  ipcMain.handle('restore-save', (_, gameId, ts) => server.call('restoreSave', gameId, ts));
  ipcMain.handle('restore-current', (_, gameId) => server.call('restoreCurrent', gameId));
  ipcMain.handle('scan-games', () => server.call('scanGames'));
  ipcMain.handle('add-custom-game', (_, cfg) => server.call('addCustomGame', cfg));
  ipcMain.handle('remove-game', (_, gameId, del) => server.call('removeGame', gameId, del));
  ipcMain.handle('rename-game', (_, gameId, name) => server.call('renameGame', gameId, name));
  ipcMain.handle('set-game-excludes', (_, gameId, ex) => server.call('setGameExcludes', gameId, ex));
  ipcMain.handle('get-game-excludes', (_, gameId) => server.call('getGameExcludes', gameId));
  ipcMain.handle('toggle-game-sync', (_, gameId, on) => server.call('toggleGameSync', gameId, on));
  ipcMain.handle('confirm-game', (_, gameId) => server.call('confirmGame', gameId));
  ipcMain.handle('dismiss-game', (_, gameId) => server.call('dismissGame', gameId));
  ipcMain.handle('backup-game-now', (_, gameId) => server.call('backupGameNow', gameId));
  ipcMain.handle('get-backup-files', (_, gameId, dir) => server.call('getBackupFiles', gameId, dir));
  ipcMain.handle('clean-backups', () => server.call('cleanBackups'));

  ipcMain.handle('mass-lookup', async () => {
    // Run async — daemon handles the work, we forward the result
    server.call('massLookup').then(result => {
      sendToUI('mass-lookup-done', result);
      server.call('getStatus').then(s => { cachedStatus = s; sendToUI('status-update', s); }).catch(() => {});
    }).catch(err => sendToUI('mass-lookup-done', { error: err.message }));
    return { started: true };
  });

  ipcMain.handle('backup-all', async () => {
    // Progress events come via daemon 'backup-all-progress' event
    server.call('backupAll').then(result => {
      sendToUI('backup-all-done', result);
    }).catch(err => sendToUI('backup-all-done', { error: err.message }));
    return { started: true };
  });

  // Folders that need shell (must stay in main process)
  ipcMain.handle('open-folder', (_, folderPath) => { if (folderPath) shell.openPath(folderPath); });
  ipcMain.handle('open-backup-folder', async (_, gameId, backupDir) => {
    const fullPath = await server.call('getBackupFolderPath', gameId, backupDir);
    if (fullPath) shell.openPath(fullPath);
  });
  ipcMain.handle('pick-game-folder', async () => {
    const r = await dialog.showOpenDialog({ properties: ['openDirectory'], title: 'Select game save folder' });
    return r.canceled ? null : r.filePaths[0];
  });

  // Updates (runs in main process — needs app.quit)
  ipcMain.handle('check-update', async () => {
    try { return await getLatestRelease(); } catch (err) { return { error: err.message }; }
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
      installAndRestart(installerPath, app);
      return { success: true, version: release.version, installing: true };
    } catch (err) { return { success: false, message: err.message }; }
  });
}

// ---- App Lifecycle ----

app.on('ready', async () => {
  // Fork daemon into a separate process
  server = new DaemonProxy(configDir);
  server.start();

  // Forward daemon events to renderer
  server.on('ready', async () => {
    cachedStatus = await server.call('getStatus').catch(() => ({}));
    updateTrayMenu();
    sendToUI('status-update', cachedStatus);
  });
  server.on('client-connected', async (info) => {
    updateTrayMenu();
    sendToUI('activity', { type: 'client-connected', ...info, time: Date.now() });
    cachedStatus = await server.call('getStatus').catch(() => cachedStatus);
    sendToUI('status-update', cachedStatus);
  });
  server.on('client-disconnected', async (info) => {
    updateTrayMenu();
    sendToUI('activity', { type: 'client-disconnected', ...info, time: Date.now() });
    cachedStatus = await server.call('getStatus').catch(() => cachedStatus);
    sendToUI('status-update', cachedStatus);
  });
  server.on('changes', async ({ folder, changes }) => {
    cachedStatus = await server.call('getStatus').catch(() => cachedStatus);
    sendToUI('status-update', cachedStatus);
    sendToUI('activity', { type: 'file-changes', folder, count: changes?.length || 0, time: Date.now() });
  });
  server.on('progress', (p) => sendToUI('sync-progress', p));
  server.on('sync-progress-update', async () => {
    cachedStatus = await server.call('getStatus').catch(() => cachedStatus);
    sendToUI('status-update', cachedStatus);
  });
  server.on('peer-folders', async () => {
    cachedStatus = await server.call('getStatus').catch(() => cachedStatus);
    sendToUI('status-update', cachedStatus);
  });
  server.on('sync-request', (request) => {
    sendToUI('sync-request', request);
    if (mainWindow && !mainWindow.isVisible()) { mainWindow.show(); mainWindow.focus(); }
  });

  // Game save events forwarded from daemon
  server.on('game-detected', (info) => sendToUI('game-detected', info));
  server.on('save-backed-up', (info) => {
    sendToUI('save-backed-up', info);
    sendToUI('activity', { type: 'game-backup', message: `Backed up ${info.game?.displayName || info.game?.name}: ${info.fileCount} files`, time: Date.now() });
  });
  server.on('game-running', (info) => sendToUI('game-running', info));
  server.on('library-updated', () => sendToUI('game-library-updated', {}));
  server.on('save-restored', (info) => {
    sendToUI('save-restored', info);
    sendToUI('activity', { type: 'game-restore', message: `Auto-restored ${info.game?.displayName || info.game?.name} from sync`, time: Date.now() });
  });
  server.on('backup-all-progress', (data) => sendToUI('backup-all-progress', data));

  server.on('daemon-exit', ({ code }) => {
    console.error(`Daemon exited unexpectedly (code ${code}), restarting...`);
    setTimeout(() => { server.start(); }, 2000);
  });

  // Create UI
  createWindow();
  setupIPC();
  mainWindow.focus();

  // Tray
  tray = new Tray(createTrayIcon());
  tray.setToolTip('CarbonSync');
  updateTrayMenu();
  tray.on('double-click', () => { mainWindow?.show(); mainWindow?.focus(); });

  // Start-with-Windows (read config from disk directly — fast, no IPC)
  try {
    const configPath = path.join(configDir, 'config.json');
    if (fs.existsSync(configPath)) {
      const cfg = JSON.parse(fs.readFileSync(configPath, 'utf-8'));
      const startWithWindows = cfg.settings?.startWithWindows !== false;
      app.setLoginItemSettings({ openAtLogin: startWithWindows, args: ['--hidden'] });
    }
  } catch {}
});

app.on('before-quit', async () => {
  app.isQuitting = true;
  if (server) {
    try { await server.stop(); } catch {}
  }
});

app.on('window-all-closed', () => { /* stay in tray */ });

process.on('uncaughtException', (err) => console.error('Uncaught:', err));
process.on('unhandledRejection', (err) => console.error('Unhandled:', err));
