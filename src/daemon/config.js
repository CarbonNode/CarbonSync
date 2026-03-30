/**
 * CarbonSync Configuration Manager
 *
 * Stores: device identity, folders to sync, API key, settings.
 * Atomic writes with backup.
 */

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const os = require('os');

class Config {
  constructor(configDir) {
    this.configDir = configDir;
    this.configPath = path.join(configDir, 'config.json');
    this.backupPath = path.join(configDir, 'config.backup.json');

    fs.mkdirSync(configDir, { recursive: true });

    this.data = {
      deviceId: '',
      deviceName: os.hostname(),
      apiKey: '',
      port: 21547,
      folders: [],
      ignorePatterns: [],
      settings: {
        scanIntervalMinutes: 5,
        maxConcurrentTransfers: 4,
        bandwidthLimitMBps: 0, // 0 = unlimited
      },
    };

    this.load();
    this._ensureIdentity();
  }

  load() {
    try {
      if (fs.existsSync(this.configPath)) {
        const raw = fs.readFileSync(this.configPath, 'utf-8');
        const parsed = JSON.parse(raw);
        this.data = { ...this.data, ...parsed };
      }
    } catch (err) {
      console.error(`Config load failed: ${err.message}`);
      // Try backup
      try {
        if (fs.existsSync(this.backupPath)) {
          this.data = JSON.parse(fs.readFileSync(this.backupPath, 'utf-8'));
          console.log('Restored from backup config');
        }
      } catch { /* use defaults */ }
    }
  }

  save() {
    try {
      const json = JSON.stringify(this.data, null, 2);
      const tmpPath = this.configPath + '.tmp';
      fs.writeFileSync(tmpPath, json, 'utf-8');

      // Backup current
      try {
        if (fs.existsSync(this.configPath)) {
          fs.copyFileSync(this.configPath, this.backupPath);
        }
      } catch { /* best effort */ }

      fs.renameSync(tmpPath, this.configPath);
    } catch (err) {
      console.error(`Config save failed: ${err.message}`);
      try { fs.unlinkSync(this.configPath + '.tmp'); } catch {}
    }
  }

  _ensureIdentity() {
    let changed = false;
    if (!this.data.deviceId) {
      this.data.deviceId = crypto.randomUUID();
      changed = true;
    }
    if (!this.data.apiKey) {
      this.data.apiKey = crypto.randomBytes(32).toString('hex');
      changed = true;
    }
    if (changed) this.save();
  }

  // ---- Getters ----
  get deviceId() { return this.data.deviceId; }
  get deviceName() { return this.data.deviceName; }
  get apiKey() { return this.data.apiKey; }
  get port() { return this.data.port; }
  get folders() { return this.data.folders; }
  get settings() { return this.data.settings; }

  // ---- Folder management ----

  addFolder(folderPath, name) {
    const resolved = path.resolve(folderPath);
    if (!fs.existsSync(resolved)) {
      throw new Error(`Folder does not exist: ${resolved}`);
    }
    if (this.data.folders.some(f => f.path === resolved)) {
      throw new Error(`Folder already synced: ${resolved}`);
    }
    this.data.folders.push({
      path: resolved,
      name: name || path.basename(resolved),
      ignorePatterns: [],
      excludes: [],  // Per-folder exclude patterns (applied everywhere)
      enabled: true,
    });
    this.save();
  }

  /**
   * Update exclude patterns for a folder.
   */
  setFolderExcludes(folderPath, excludes) {
    const folder = this.data.folders.find(f => f.path === path.resolve(folderPath));
    if (folder) {
      folder.excludes = excludes;
      this.save();
    }
  }

  getFolderExcludes(folderPath) {
    const folder = this.data.folders.find(f => f.path === path.resolve(folderPath));
    return folder?.excludes || [];
  }

  removeFolder(folderPath) {
    const resolved = path.resolve(folderPath);
    this.data.folders = this.data.folders.filter(f => f.path !== resolved);
    this.save();
  }

  updateFolder(folderPath, updates) {
    const folder = this.data.folders.find(f => f.path === path.resolve(folderPath));
    if (folder) {
      Object.assign(folder, updates);
      this.save();
    }
  }

  // ---- Settings ----

  updateSettings(updates) {
    this.data.settings = { ...this.data.settings, ...updates };
    this.save();
  }

  setPort(port) {
    this.data.port = port;
    this.save();
  }

  setDeviceName(name) {
    this.data.deviceName = name;
    this.save();
  }
}

module.exports = { Config };
