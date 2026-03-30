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

  async backupNow(gameId) {
    const entry = this._library.get(gameId);
    if (!entry) throw new Error(`Unknown game: ${gameId}`);

    const displayName = this._getDisplayName(entry);
    const result = await this.backup.backupGame({
      gameId,
      gameName: displayName,
      saveBase: entry.saveBase,
      sourceDevice: os.hostname(),
    });

    if (result) {
      entry.lastBackup = result.timestamp;
      entry.backupCount = (entry.backupCount || 0) + 1;
      await this._saveLibrary();
    }

    return result;
  }
}

module.exports = { GameSaveManager };
