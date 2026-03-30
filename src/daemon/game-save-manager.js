/**
 * Game Save Manager — orchestrates game detection, backup, and library.
 *
 * Coordinates GameDB, GameDetector, and GameBackup.
 * Exposes a clean API for the UI and sync layers.
 */

const path = require('path');
const fs = require('fs');
const fsp = require('fs/promises');
const os = require('os');
const { EventEmitter } = require('events');
const { GameDB } = require('./game-db');
const { GameDetector } = require('./game-detector');
const { GameBackup } = require('./game-backup');

class GameSaveManager extends EventEmitter {
  /**
   * @param {object} opts
   * @param {string} opts.configDir
   * @param {import('./config').Config} opts.config
   */
  constructor({ configDir, config }) {
    super();
    this.configDir = configDir;
    this.config = config;
    this.gameDB = new GameDB(config);
    this.detector = new GameDetector({ gameDB: this.gameDB, config });
    this.backup = new GameBackup({ configDir, config });
    this._library = new Map(); // gameId -> library entry
    this._libraryPath = path.join(configDir, 'game-saves', '_library.json');
    this._running = false;
  }

  async start() {
    if (this._running) return;
    if (!this.config.data.settings?.gameSaveEnabled) {
      console.log('Game save detection disabled');
      return;
    }
    this._running = true;

    // Load persisted library
    await this._loadLibrary();

    // Load synced dismissed list (from game-saves folder, shared across peers)
    await this._loadSyncedDismissals();

    // Wire up detector events
    this.detector.on('save-changed', async (info) => {
      await this._handleSaveChanged(info);
    });

    this.detector.on('game-running', (info) => {
      const entry = this._library.get(info.gameId);
      if (entry) entry.running = true;
      this.emit('game-running', info);
    });

    // Always scan on startup — library file may not survive reinstalls
    console.log('Scanning for existing game saves...');
    const existing = await this.detector.scanExistingGames();
    for (const { game, saveBase, rootKey, isHeuristic } of existing) {
      this._ensureLibraryEntry(game, saveBase, rootKey, isHeuristic);
    }

    // Also load games from backed up data
    const backedUp = await this.backup.listBackedUpGames();
    for (const game of backedUp) {
      if (!this._library.has(game.id)) {
        this._library.set(game.id, {
          id: game.id,
          name: game.name,
          displayName: game.displayName || game.name,
          saveBase: game.saveBase,
          enabled: game.enabled !== false,
          isHeuristic: false,
          running: false,
          lastBackup: game.lastBackup,
          backupCount: game.backupCount,
          excludes: game.excludes || [],
        });
      }
    }

    // Remove any games that were dismissed (locally or by peers)
    const allDismissed = this._getMergedDismissals();
    for (const id of allDismissed) {
      this._library.delete(id);
    }

    await this._saveLibrary();
    console.log(`Found ${this._library.size} game(s)`);

    // Start watching for changes
    await this.detector.start();
    console.log('Game save detector started');

    // Watch the game-saves folder itself for incoming sync changes
    this._startSyncWatcher();

    // Auto-restore any saves that are newer from sync (initial check)
    setTimeout(() => this.autoRestoreAll(), 5000);
  }

  async stop() {
    this._running = false;
    await this.detector.stop();
    await this._saveLibrary();
  }

  // ---- Event Handlers ----

  async _handleSaveChanged({ game, gameId, saveBase, rootKey, changedPaths, isHeuristic }) {
    // Ensure game is in library
    const entry = this._ensureLibraryEntry(game, saveBase, rootKey, isHeuristic);
    entry.running = false;

    // Check if game is enabled
    if (!entry.enabled) return;

    // Check user overrides
    const overrides = this.config.data.gameSaveGameOverrides?.[gameId];
    if (overrides?.enabled === false) return;

    // Heuristic games need confirmation before backing up
    const confirmedGames = this.config.data.gameSaveConfirmedGames || [];
    if (isHeuristic && !confirmedGames.includes(gameId)) {
      this.emit('game-detected', { game: entry, isHeuristic: true, needsConfirmation: true });
      return;
    }

    // Apply exclusion rules
    const excludes = overrides?.excludes || entry.excludes || [];
    if (excludes.length > 0) {
      const { matchGlob } = require('./game-db');
      const filteredPaths = changedPaths.filter(p => {
        const rel = path.relative(saveBase, p).replace(/\\/g, '/');
        return !excludes.some(pattern => matchGlob(rel, pattern));
      });
      if (filteredPaths.length === 0) return;
    }

    // Perform backup
    try {
      const displayName = overrides?.displayName || entry.displayName || entry.name;
      const result = await this.backup.backupGame({
        gameId,
        gameName: displayName,
        saveBase,
        changedPaths,
        sourceDevice: os.hostname(),
      });

      if (result) {
        entry.lastBackup = result.timestamp;
        entry.backupCount = (entry.backupCount || 0) + 1;
        await this._saveLibrary();

        this.emit('save-backed-up', {
          game: entry,
          ...result,
        });
        console.log(`Backed up ${displayName}: ${result.fileCount} files (${(result.totalSize / 1024).toFixed(1)} KB)`);
      }
    } catch (err) {
      console.error(`Backup failed for ${entry.name}: ${err.message}`);
    }
  }

  _ensureLibraryEntry(game, saveBase, rootKey, isHeuristic) {
    if (this._library.has(game.id)) {
      const entry = this._library.get(game.id);
      // Update saveBase if changed
      if (saveBase) entry.saveBase = saveBase;
      return entry;
    }

    const overrides = this.config.data.gameSaveGameOverrides?.[game.id];

    const entry = {
      id: game.id,
      name: game.name,
      displayName: overrides?.displayName || game.name,
      saveBase,
      rootKey,
      enabled: overrides?.enabled !== undefined ? overrides.enabled : true,
      isHeuristic: isHeuristic || false,
      running: false,
      lastBackup: null,
      backupCount: 0,
      excludes: overrides?.excludes || [],
    };

    this._library.set(game.id, entry);
    this.emit('game-detected', { game: entry, isHeuristic });
    return entry;
  }

  // ---- Library Persistence ----

  async _loadLibrary() {
    try {
      const raw = await fsp.readFile(this._libraryPath, 'utf-8');
      const data = JSON.parse(raw);
      if (data.games) {
        for (const game of data.games) {
          this._library.set(game.id, { ...game, running: false });
        }
      }
    } catch {
      // No library yet
    }
  }

  /**
   * Load dismissed games list from the game-saves folder (synced across peers).
   * This file lives alongside _library.json so it gets synced.
   */
  async _loadSyncedDismissals() {
    const dismissPath = path.join(this.configDir, 'game-saves', '_dismissed.json');
    try {
      const raw = await fsp.readFile(dismissPath, 'utf-8');
      const data = JSON.parse(raw);
      // Merge synced dismissals into local config
      if (data.dismissed && Array.isArray(data.dismissed)) {
        if (!this.config.data.gameSaveBlockedGames) {
          this.config.data.gameSaveBlockedGames = [];
        }
        for (const id of data.dismissed) {
          if (!this.config.data.gameSaveBlockedGames.includes(id)) {
            this.config.data.gameSaveBlockedGames.push(id);
          }
        }
      }
    } catch {
      // No synced dismissals yet
    }
  }

  async _saveSyncedDismissals() {
    const dismissPath = path.join(this.configDir, 'game-saves', '_dismissed.json');
    const dismissed = this.config.data.gameSaveBlockedGames || [];
    try {
      const dir = path.dirname(dismissPath);
      await fsp.mkdir(dir, { recursive: true });
      await fsp.writeFile(dismissPath, JSON.stringify({
        version: 1,
        lastUpdated: new Date().toISOString(),
        dismissed,
      }, null, 2));
    } catch (err) {
      console.error('Failed to save synced dismissals:', err.message);
    }
  }

  _getMergedDismissals() {
    return new Set(this.config.data.gameSaveBlockedGames || []);
  }

  async _saveLibrary() {
    const data = {
      version: 1,
      lastUpdated: new Date().toISOString(),
      games: Array.from(this._library.values()).map(g => ({
        id: g.id,
        name: g.name,
        displayName: g.displayName,
        saveBase: g.saveBase,
        rootKey: g.rootKey,
        enabled: g.enabled,
        isHeuristic: g.isHeuristic,
        lastBackup: g.lastBackup,
        backupCount: g.backupCount,
        excludes: g.excludes,
      })),
    };
    try {
      const dir = path.dirname(this._libraryPath);
      await fsp.mkdir(dir, { recursive: true });
      await fsp.writeFile(this._libraryPath, JSON.stringify(data, null, 2));
    } catch (err) {
      console.error('Failed to save game library:', err.message);
    }
  }

  // ---- API for UI ----

  /**
   * Get the full game library.
   */
  getLibrary() {
    return Array.from(this._library.values()).map(g => ({
      ...g,
      displayName: this._getDisplayName(g),
    }));
  }

  _getDisplayName(game) {
    const overrides = this.config.data.gameSaveGameOverrides?.[game.id];
    return overrides?.displayName || game.displayName || game.name;
  }

  /**
   * Get save history for a specific game.
   */
  async getHistory(gameId) {
    const entry = this._library.get(gameId);
    if (!entry) return [];
    const displayName = this._getDisplayName(entry);
    return this.backup.getHistory(displayName);
  }

  /**
   * Restore a save from a specific backup version.
   */
  async restore(gameId, backupTimestamp) {
    const entry = this._library.get(gameId);
    if (!entry) throw new Error(`Unknown game: ${gameId}`);
    const displayName = this._getDisplayName(entry);
    return this.backup.restoreSave(displayName, backupTimestamp);
  }

  /**
   * Restore the latest (current) save.
   */
  async restoreCurrent(gameId) {
    const entry = this._library.get(gameId);
    if (!entry) throw new Error(`Unknown game: ${gameId}`);
    const displayName = this._getDisplayName(entry);
    return this.backup.restoreCurrent(displayName);
  }

  /**
   * Force a rescan to find new games.
   */
  async scanNow() {
    const existing = await this.detector.scanExistingGames();
    let newCount = 0;
    for (const { game, saveBase, rootKey, isHeuristic } of existing) {
      if (!this._library.has(game.id)) {
        this._ensureLibraryEntry(game, saveBase, rootKey, isHeuristic);
        newCount++;
      }
    }
    await this._saveLibrary();
    return { found: existing.length, new: newCount };
  }

  /**
   * Deep scan — walks all watched directories up to 3 levels deep looking
   * for save-like files. Slower than scanNow but finds games that the quick
   * scan misses (games not in the DB and not matching engine patterns).
   */
  async massLookup() {
    const existing = await this.detector.deepScanForGames();
    let newCount = 0;
    const dismissed = this._getMergedDismissals();
    for (const { game, saveBase, rootKey, isHeuristic } of existing) {
      if (dismissed.has(game.id)) continue;
      if (!this._library.has(game.id)) {
        this._ensureLibraryEntry(game, saveBase, rootKey, isHeuristic);
        newCount++;
      }
    }
    await this._saveLibrary();
    return { found: existing.length, new: newCount };
  }

  /**
   * Add a custom game with a manually specified path.
   */
  async addCustomGame({ name, savePath }) {
    const id = 'custom-' + name.toLowerCase().replace(/[^a-z0-9]/g, '-').replace(/-+/g, '-');
    const resolved = path.resolve(savePath);

    if (!fs.existsSync(resolved)) {
      throw new Error(`Path does not exist: ${resolved}`);
    }

    // Determine which root this falls under (or none)
    const roots = this.gameDB.resolveRoots();
    let rootKey = null;
    let pattern = '';
    const normalizedPath = resolved.replace(/\\/g, '/');

    for (const [key, rootDir] of Object.entries(roots)) {
      const normalizedRoot = rootDir.replace(/\\/g, '/');
      if (normalizedPath.startsWith(normalizedRoot + '/')) {
        rootKey = key;
        pattern = normalizedPath.slice(normalizedRoot.length + 1);
        break;
      }
    }

    const game = {
      id,
      name,
      savePaths: rootKey ? [{ root: rootKey, pattern }] : [],
      extensions: ['*'],
      type: 'directory',
      custom: true,
    };

    this.gameDB.addCustomGame(game);
    this._ensureLibraryEntry(game, resolved, rootKey, false);
    await this._saveLibrary();

    return { id, name };
  }

  /**
   * Remove a game from the library and optionally delete its backups.
   */
  async removeGame(gameId, deleteBackups = false) {
    const entry = this._library.get(gameId);
    if (!entry) return;

    if (deleteBackups) {
      const displayName = this._getDisplayName(entry);
      await this.backup.removeGame(displayName);
    }

    // Remove from custom games if applicable
    this.gameDB.removeCustomGame(gameId);

    this._library.delete(gameId);
    await this._saveLibrary();
  }

  /**
   * Rename a game (change display name).
   */
  async renameGame(gameId, newDisplayName) {
    const entry = this._library.get(gameId);
    if (!entry) throw new Error(`Unknown game: ${gameId}`);

    const oldDisplayName = this._getDisplayName(entry);

    // Update in overrides (persists across sessions)
    if (!this.config.data.gameSaveGameOverrides) {
      this.config.data.gameSaveGameOverrides = {};
    }
    if (!this.config.data.gameSaveGameOverrides[gameId]) {
      this.config.data.gameSaveGameOverrides[gameId] = {};
    }
    this.config.data.gameSaveGameOverrides[gameId].displayName = newDisplayName;
    this.config.save();

    // Rename the backup directory
    if (oldDisplayName !== newDisplayName) {
      try {
        await this.backup.renameGame(oldDisplayName, newDisplayName);
      } catch (err) {
        console.error(`Failed to rename backup directory: ${err.message}`);
      }
    }

    entry.displayName = newDisplayName;
    await this._saveLibrary();
  }

  /**
   * Set exclusion patterns for a game.
   */
  async setGameExcludes(gameId, excludes) {
    const entry = this._library.get(gameId);
    if (!entry) throw new Error(`Unknown game: ${gameId}`);

    if (!this.config.data.gameSaveGameOverrides) {
      this.config.data.gameSaveGameOverrides = {};
    }
    if (!this.config.data.gameSaveGameOverrides[gameId]) {
      this.config.data.gameSaveGameOverrides[gameId] = {};
    }
    this.config.data.gameSaveGameOverrides[gameId].excludes = excludes;
    this.config.save();

    entry.excludes = excludes;
    await this._saveLibrary();
  }

  /**
   * Get exclusion patterns for a game.
   */
  getGameExcludes(gameId) {
    const overrides = this.config.data.gameSaveGameOverrides?.[gameId];
    return overrides?.excludes || this._library.get(gameId)?.excludes || [];
  }

  /**
   * Toggle sync/backup for a game.
   */
  async toggleSync(gameId, enabled) {
    const entry = this._library.get(gameId);
    if (!entry) throw new Error(`Unknown game: ${gameId}`);

    if (!this.config.data.gameSaveGameOverrides) {
      this.config.data.gameSaveGameOverrides = {};
    }
    if (!this.config.data.gameSaveGameOverrides[gameId]) {
      this.config.data.gameSaveGameOverrides[gameId] = {};
    }
    this.config.data.gameSaveGameOverrides[gameId].enabled = enabled;
    this.config.save();

    entry.enabled = enabled;
    await this._saveLibrary();
  }

  /**
   * Confirm a heuristic-detected game (start backing it up).
   */
  async confirmGame(gameId) {
    if (!this.config.data.gameSaveConfirmedGames) {
      this.config.data.gameSaveConfirmedGames = [];
    }
    if (!this.config.data.gameSaveConfirmedGames.includes(gameId)) {
      this.config.data.gameSaveConfirmedGames.push(gameId);
      this.config.save();
    }
  }

  /**
   * Dismiss a heuristic-detected game (don't show again).
   */
  async dismissGame(gameId) {
    if (!this.config.data.gameSaveBlockedGames) {
      this.config.data.gameSaveBlockedGames = [];
    }
    if (!this.config.data.gameSaveBlockedGames.includes(gameId)) {
      this.config.data.gameSaveBlockedGames.push(gameId);
      this.config.save();
    }

    this._library.delete(gameId);
    await this._saveLibrary();
    // Save to synced file so other peers pick it up
    await this._saveSyncedDismissals();
  }

  /**
   * Force backup a specific game now.
   */
  /**
   * List files inside a specific backup version.
   */
  async getBackupFiles(gameId, backupDir) {
    const entry = this._library.get(gameId);
    if (!entry) return [];
    const displayName = this._getDisplayName(entry);
    return this.backup.listBackupFiles(displayName, backupDir);
  }

  /**
   * Backup all enabled games at once.
   */
  async backupAll() {
    let success = 0;
    let skipped = 0;
    for (const [gameId, entry] of this._library) {
      if (!entry.enabled || !entry.saveBase) { skipped++; continue; }
      // Skip games whose save path doesn't exist on this PC
      // (e.g., synced from another device's library)
      if (!fs.existsSync(entry.saveBase)) { skipped++; continue; }
      try {
        const result = await this.backupNow(gameId);
        if (result) success++;
        else skipped++;
      } catch {
        skipped++;
      }
    }
    return { success, skipped };
  }

  async backupNow(gameId) {
    const entry = this._library.get(gameId);
    if (!entry) throw new Error(`Unknown game: ${gameId}`);

    const displayName = this._getDisplayName(entry);
    const result = await this.backup.backupGame({
      gameId,
      gameName: displayName,
      saveBase: entry.saveBase,
      sourceDevice: os.hostname(),
      force: true, // Manual backup always creates a version
    });

    if (result) {
      entry.lastBackup = result.timestamp;
      entry.backupCount = (entry.backupCount || 0) + 1;
      await this._saveLibrary();
    }

    return result;
  }

  // ---- Auto-Restore (sync → local game dir) ----

  /**
   * Watch the game-saves folder for incoming sync changes.
   * When current/ is updated by sync, auto-restore if newer.
   */
  _startSyncWatcher() {
    const gameSavesDir = path.join(this.configDir, 'game-saves');
    let debounceTimer = null;

    try {
      const watcher = require('@parcel/watcher');
      watcher.subscribe(gameSavesDir, (err, events) => {
        if (err) return;
        // Only care about changes inside current/ dirs (sync updates)
        const hasCurrent = events.some(e => e.path.includes(`${path.sep}current${path.sep}`) || e.path.includes('/current/'));
        if (!hasCurrent) return;

        // Debounce — sync may update many files at once
        if (debounceTimer) clearTimeout(debounceTimer);
        debounceTimer = setTimeout(() => this.autoRestoreAll(), 3000);
      }, {
        ignore: ['**/backups/**'],
      }).then(sub => {
        this._syncWatcherSub = sub;
      }).catch(() => {});
    } catch {}
  }

  /**
   * For each game, check if the synced current/ has newer saves than the
   * local game directory. If so, auto-restore (with pre-restore backup).
   */
  async autoRestoreAll() {
    let restored = 0;
    for (const [gameId, entry] of this._library) {
      if (!entry.enabled || !entry.saveBase) continue;
      try {
        const did = await this._autoRestoreGame(gameId, entry);
        if (did) restored++;
      } catch (err) {
        console.error(`Auto-restore failed for ${entry.displayName || entry.name}: ${err.message}`);
      }
    }
    if (restored > 0) {
      console.log(`Auto-restored ${restored} game(s) from sync`);
    }
  }

  async _autoRestoreGame(gameId, entry) {
    const displayName = this._getDisplayName(entry);
    const currentDir = path.join(this.backup.gameDir(displayName), 'current');

    // Skip auto-restore if the game's save path is already inside a synced folder.
    // The folder sync handles delivery; game saves only provides backup/versioning.
    if (entry.saveBase && this._isInsideSyncedFolder(entry.saveBase)) {
      return false;
    }

    // Check if current/ exists (from sync)
    try {
      await fsp.access(currentDir);
    } catch {
      return false; // No synced saves
    }

    // Get newest mtime in current/ (synced version)
    const syncedMtime = await this._getNewestMtime(currentDir);
    if (!syncedMtime) return false;

    // Get newest mtime in the actual game save dir
    const localMtime = await this._getNewestMtime(entry.saveBase);

    // If synced is newer (or local doesn't exist), auto-restore
    if (!localMtime || syncedMtime > localMtime) {
      // Check if game is running — skip if so
      const locked = await this._isAnyFileLocked(entry.saveBase);
      if (locked) {
        console.log(`Skipping auto-restore for ${displayName}: game appears to be running`);
        return false;
      }

      console.log(`Auto-restoring ${displayName}: synced save is newer (${new Date(syncedMtime).toISOString()} > ${localMtime ? new Date(localMtime).toISOString() : 'none'})`);

      // Pre-restore safety backup
      await this.backup.restoreCurrent(displayName);

      this.emit('save-restored', {
        game: entry,
        source: 'auto-sync',
      });
      return true;
    }

    return false;
  }

  async _getNewestMtime(dir) {
    try {
      await fsp.access(dir);
    } catch {
      return null;
    }

    let newest = 0;
    const walk = async (d) => {
      let entries;
      try { entries = await fsp.readdir(d, { withFileTypes: true }); } catch { return; }
      for (const entry of entries) {
        if (entry.name.startsWith('_meta') || entry.name.startsWith('.')) continue;
        const fullPath = path.join(d, entry.name);
        if (entry.isDirectory()) {
          await walk(fullPath);
        } else if (entry.isFile()) {
          try {
            const stat = await fsp.stat(fullPath);
            if (stat.mtimeMs > newest) newest = stat.mtimeMs;
          } catch {}
        }
      }
    };
    await walk(dir);
    return newest || null;
  }

  /**
   * Check if a path is inside any folder that's already being synced
   * via the Folders tab. If so, folder sync handles delivery — no need
   * for game-save auto-restore to write there too.
   */
  _isInsideSyncedFolder(absPath) {
    const resolved = path.resolve(absPath);
    for (const folder of this.config.folders) {
      if (!folder.enabled) continue;
      const folderResolved = path.resolve(folder.path);
      // Skip the game-saves folder itself — that's our own sync folder
      if (folder.name === 'Game Saves') continue;
      if (resolved.startsWith(folderResolved + path.sep) || resolved === folderResolved) {
        return true;
      }
    }
    return false;
  }

  async _isAnyFileLocked(dir) {
    try {
      await fsp.access(dir);
    } catch {
      return false;
    }
    const entries = await fsp.readdir(dir, { withFileTypes: true });
    for (const entry of entries) {
      if (!entry.isFile()) continue;
      try {
        const fd = await fsp.open(path.join(dir, entry.name), 'r');
        await fd.close();
      } catch (err) {
        if (err.code === 'EBUSY' || err.code === 'EACCES') return true;
      }
    }
    return false;
  }
}

module.exports = { GameSaveManager };
