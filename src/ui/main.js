/**
 * CarbonSync Electron UI — System Tray + Settings Window
 * Manages the daemon and provides a configuration interface.
 */

const { app, BrowserWindow, Tray, Menu, ipcMain, nativeImage, dialog } = require('electron');
const path = require('path');
const os = require('os');
const { CarbonSyncServer } = require('../daemon/server');
const { getLatestRelease, downloadUpdate } = require('../daemon/updater');

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
    width: 800, height: 520,
    x: primary.bounds.x + Math.round((primary.bounds.width - 800) / 2),
    y: primary.bounds.y + Math.round((primary.bounds.height - 520) / 2),
    minWidth: 600, minHeight: 400,
    frame: true, show: true,
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
    if (server?.engine) {
      return server.engine.rescan(folderName);
    }
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
    // Test connection first
    const net = require('net');
    const ok = await new Promise((resolve) => {
      const s = new net.Socket();
      s.setTimeout(3000);
      s.connect(port, ip, () => { s.destroy(); resolve(true); });
      s.on('error', () => resolve(false));
      s.on('timeout', () => { s.destroy(); resolve(false); });
    });
    if (!ok) return { error: `Cannot connect to ${ip}:${port}` };

    // Add to discovered devices manually
    const peer = { role: 'peer', ip, port, hostname: ip, deviceId: '', name: `Manual: ${ip}` };
    if (server?.discovery) {
      server.discovery.services.set(ip, peer);
    }
    sendToUI('status-update', server?.getStatus());
    sendToUI('activity', { type: 'connect', message: `Added peer ${ip}:${port}`, time: Date.now() });
    return { success: true };
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

      const downloadsDir = app.getPath('downloads');
      const destPath = path.join(downloadsDir, `CarbonSync-${release.version}.exe`);

      sendToUI('activity', { type: 'update', message: `Downloading v${release.version}...`, time: Date.now() });

      await downloadUpdate(release.url, destPath, (pct) => {
        sendToUI('sync-progress', { phase: 'updating', file: `CarbonSync v${release.version}`, current: pct, total: 100 });
      });

      sendToUI('activity', { type: 'update', message: `Downloaded to ${destPath}`, time: Date.now() });

      // Open the downloaded file location
      const { shell } = require('electron');
      shell.showItemInFolder(destPath);

      return { success: true, path: destPath, version: release.version };
    } catch (err) {
      return { success: false, message: err.message };
    }
  });
}

app.on('ready', async () => {
  // Start daemon
  server = new CarbonSyncServer(configDir);

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

  console.log('UI: creating window...');
  createWindow();
  console.log('UI: window created');
  setupIPC();
  console.log('UI: IPC ready');

  // Show window immediately
  mainWindow.show();
  mainWindow.focus();
  console.log('UI: window shown');

  // Tray
  tray = new Tray(createTrayIcon());
  tray.setToolTip('CarbonSync');
  updateTrayMenu();
  tray.on('double-click', () => { mainWindow?.show(); mainWindow?.focus(); });
  console.log('UI: tray created');

  if (!process.argv.includes('--hidden')) {
    mainWindow.show();
  }

  // Auto-start with Windows
  app.setLoginItemSettings({ openAtLogin: true, args: ['--hidden'] });

  // Start server in background (may take a few seconds for firewall/scan)
  server.start().then(() => {
    updateTrayMenu();
    sendToUI('status-update', server.getStatus());
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
