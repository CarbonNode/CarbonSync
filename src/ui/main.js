/**
 * CarbonSync Electron UI — System Tray + Settings Window
 * Manages the daemon and provides a configuration interface.
 */

const { app, BrowserWindow, Tray, Menu, ipcMain, nativeImage, dialog } = require('electron');
const path = require('path');
const os = require('os');
const { CarbonSyncServer } = require('../daemon/server');

if (process.platform === 'win32') {
  app.setAppUserModelId('com.carbonsync.app');
}

const configDir = path.join(os.homedir(), '.carbonsync');
let tray = null;
let mainWindow = null;
let server = null;

const gotLock = app.requestSingleInstanceLock();
if (!gotLock) { app.quit(); }

app.on('second-instance', () => {
  if (mainWindow) { mainWindow.show(); mainWindow.focus(); }
});

function createTrayIcon() {
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
  mainWindow = new BrowserWindow({
    width: 800, height: 520,
    minWidth: 600, minHeight: 400,
    frame: true, show: false,
    title: 'CarbonSync',
    backgroundColor: '#0a0a0f',
    webPreferences: {
      preload: path.join(__dirname, 'preload.js'),
      contextIsolation: true,
      nodeIntegration: false,
      sandbox: true,
    },
  });

  mainWindow.loadFile(path.join(__dirname, 'renderer', 'index.html'));

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
}

app.on('ready', async () => {
  // Start daemon
  server = new CarbonSyncServer(configDir);

  server.on('ready', () => updateTrayMenu());
  server.on('client-connected', () => updateTrayMenu());
  server.on('client-disconnected', () => updateTrayMenu());
  server.on('changes', () => {
    if (mainWindow && !mainWindow.isDestroyed()) {
      mainWindow.webContents.send('status-update', server.getStatus());
    }
  });

  await server.start();

  // Tray
  tray = new Tray(createTrayIcon());
  tray.setToolTip('CarbonSync');
  updateTrayMenu();
  tray.on('double-click', () => { mainWindow?.show(); mainWindow?.focus(); });

  // Window
  createWindow();
  setupIPC();

  // Auto-start with Windows
  app.setLoginItemSettings({ openAtLogin: true, args: ['--hidden'] });

  if (!process.argv.includes('--hidden')) {
    mainWindow.show();
  }
});

app.on('before-quit', async () => {
  app.isQuitting = true;
  if (server) await server.stop();
});

app.on('window-all-closed', () => { /* stay in tray */ });

process.on('uncaughtException', (err) => console.error('Uncaught:', err));
process.on('unhandledRejection', (err) => console.error('Unhandled:', err));
