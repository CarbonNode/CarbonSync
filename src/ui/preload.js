const { contextBridge, ipcRenderer } = require('electron');

contextBridge.exposeInMainWorld('carbonsync', {
  getStatus: () => ipcRenderer.invoke('get-status'),
  getConfig: () => ipcRenderer.invoke('get-config'),
  addFolder: (path, name) => ipcRenderer.invoke('add-folder', path, name),
  removeFolder: (path) => ipcRenderer.invoke('remove-folder', path),
  updateSettings: (s) => ipcRenderer.invoke('update-settings', s),
  pickFolder: () => ipcRenderer.invoke('pick-folder'),
  rescan: (name) => ipcRenderer.invoke('rescan', name),

  onStatusUpdate: (cb) => {
    ipcRenderer.on('status-update', (_, status) => { try { cb(status); } catch {} });
  },
});
