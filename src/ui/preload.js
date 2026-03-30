const { contextBridge, ipcRenderer } = require('electron');

contextBridge.exposeInMainWorld('carbonsync', {
  getStatus: () => ipcRenderer.invoke('get-status'),
  getConfig: () => ipcRenderer.invoke('get-config'),
  addFolder: (path, name) => ipcRenderer.invoke('add-folder', path, name),
  removeFolder: (path) => ipcRenderer.invoke('remove-folder', path),
  updateSettings: (s) => ipcRenderer.invoke('update-settings', s),
  pickFolder: () => ipcRenderer.invoke('pick-folder'),
  rescan: (name) => ipcRenderer.invoke('rescan', name),
  cancelScan: (name) => ipcRenderer.invoke('cancel-scan', name),
  renamePeer: (hostname, name) => ipcRenderer.invoke('rename-peer', hostname, name),
  setFolderExcludes: (folder, excludes) => ipcRenderer.invoke('set-folder-excludes', folder, excludes),
  getFolderExcludes: (folder) => ipcRenderer.invoke('get-folder-excludes', folder),
  addPeer: (ip, port) => ipcRenderer.invoke('add-peer', ip, port),
  removePeer: (ip, port) => ipcRenderer.invoke('remove-peer', ip, port),
  setHubConnection: (addr, key) => ipcRenderer.invoke('set-hub-connection', addr, key),
  setFolderDirection: (folder, dir) => ipcRenderer.invoke('set-folder-direction', folder, dir),
  checkUpdate: () => ipcRenderer.invoke('check-update'),
  downloadUpdate: () => ipcRenderer.invoke('download-update'),

  // Game saves
  getGameLibrary: () => ipcRenderer.invoke('get-game-library'),
  getSaveHistory: (gameId) => ipcRenderer.invoke('get-save-history', gameId),
  restoreSave: (gameId, ts) => ipcRenderer.invoke('restore-save', gameId, ts),
  restoreCurrent: (gameId) => ipcRenderer.invoke('restore-current', gameId),
  scanGames: () => ipcRenderer.invoke('scan-games'),
  addCustomGame: (cfg) => ipcRenderer.invoke('add-custom-game', cfg),
  removeGame: (gameId, deleteBackups) => ipcRenderer.invoke('remove-game', gameId, deleteBackups),
  renameGame: (gameId, name) => ipcRenderer.invoke('rename-game', gameId, name),
  setGameExcludes: (gameId, excludes) => ipcRenderer.invoke('set-game-excludes', gameId, excludes),
  getGameExcludes: (gameId) => ipcRenderer.invoke('get-game-excludes', gameId),
  toggleGameSync: (gameId, enabled) => ipcRenderer.invoke('toggle-game-sync', gameId, enabled),
  confirmGame: (gameId) => ipcRenderer.invoke('confirm-game', gameId),
  dismissGame: (gameId) => ipcRenderer.invoke('dismiss-game', gameId),
  backupGameNow: (gameId) => ipcRenderer.invoke('backup-game-now', gameId),
  massLookup: () => ipcRenderer.invoke('mass-lookup'),
  openFolder: (p) => ipcRenderer.invoke('open-folder', p),
  openBackupFolder: (gameId, dir) => ipcRenderer.invoke('open-backup-folder', gameId, dir),
  backupAll: () => ipcRenderer.invoke('backup-all'),
  pickGameFolder: () => ipcRenderer.invoke('pick-game-folder'),
  getBackupFiles: (gameId, dir) => ipcRenderer.invoke('get-backup-files', gameId, dir),

  onBackupAllProgress: (cb) => { ipcRenderer.on('backup-all-progress', (_, d) => { try { cb(d); } catch {} }); },
  onBackupAllDone: (cb) => { ipcRenderer.on('backup-all-done', (_, d) => { try { cb(d); } catch {} }); },
  onMassLookupDone: (cb) => { ipcRenderer.on('mass-lookup-done', (_, d) => { try { cb(d); } catch {} }); },

  onStatusUpdate: (cb) => {
    ipcRenderer.on('status-update', (_, data) => { try { cb(data); } catch {} });
  },
  onProgress: (cb) => {
    ipcRenderer.on('sync-progress', (_, data) => { try { cb(data); } catch {} });
  },
  setDeviceName: (name) => ipcRenderer.invoke('set-device-name', name),
  minimize: () => ipcRenderer.invoke('window-minimize'),
  closeWindow: () => ipcRenderer.invoke('window-close'),

  onActivity: (cb) => {
    ipcRenderer.on('activity', (_, data) => { try { cb(data); } catch {} });
  },
});
