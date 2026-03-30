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
    this._library = new Map();
    // Store library in config dir (NOT in game-saves sync folder — avoids overwrite race)
    this._libraryPath = path.join(configDir, '_game-library.json');
    // Migrate from old location if exists
    const oldPath = path.join(configDir, 'game-saves', '_library.json');
    if (!fs.existsSync(this._libraryPath) && fs.existsSync(oldPath)) {
      try { fs.copyFileSync(oldPath, this._libraryPath); } catch {}
    }
    this._running = false;
    this._recentlyRestored = new Map(); // absPath -> timestamp (skip detector feedback loop)
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

    // One-time migration: move hardcoded blockedGameIds from blocklist.json into config
    this._migrateBlocklistIds();

    // Wire up detector events
    this.detector.on('save-changed', async (info) => {
      await this._handleSaveChanged(info);
    });

    this.detector.on('game-running', (info) => {
      const entry = this._library.get(info.gameId);
      if (entry) entry.running = true;
      this.emit('game-running', info);
    });

    // Auto-scan if library is empty — don't block startup, run in background
    if (this._library.size === 0) {
      console.log('No cached library — auto-scanning for game saves in background...');
      this._autoScan().catch(err => console.error('Auto-scan failed:', err.message));
    } else {
      console.log(`Loaded ${this._library.size} games from cache`);
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

    // Populate knownDevices from backup history for each game
    for (const [gameId, entry] of this._library) {
      if (!entry.knownDevices) entry.knownDevices = {};
      try {
        const displayName = this._getDisplayName(entry);
        const history = await this.backup.getHistory(displayName);
        for (const h of history) {
          if (h.sourceDevice && h.sourceDevice !== 'unknown') {
            const existing = entry.knownDevices[h.sourceDevice];
            if (!existing || !existing.lastBackup || h.timestamp > existing.lastBackup) {
              entry.knownDevices[h.sourceDevice] = { status: 'synced', lastBackup: h.timestamp };
            }
          }
        }
      } catch {}
      // Mark this device if save exists locally
      const resolved = this._resolveLocalSaveBase(entry);
      if (resolved && fs.existsSync(resolved)) {
        entry.knownDevices[os.hostname()] = entry.knownDevices[os.hostname()] || { status: 'local', lastBackup: entry.lastBackup };
      }
    }

    // Always save library state (even if empty — prevents stale data on next load)
    await this._saveLibrary();
    console.log(`Found ${this._library.size} game(s)`);

    // Start watching for changes
    await this.detector.start();
    console.log('Game save detector started');

    // Also pick up games from existing backups in game-saves folder
    await this._syncLibraryFromBackups();

    // Watch the game-saves folder itself for incoming sync changes
    this._startSyncWatcher();

    // Auto-restore any saves that are newer from sync (initial check)
    setTimeout(() => this.autoRestoreAll(), 5000);

    // Periodic check for synced games (catches anything the watcher missed)
    this._syncCheckInterval = setInterval(() => this._syncLibraryFromBackups(), 30000);
  }

  async _autoScan() {
    const dismissed = this._getMergedDismissals();
    const existing = await this.detector.scanExistingGames();
    for (const { game, saveBase, rootKey, isHeuristic } of existing) {
      if (dismissed.has(game.id)) continue;
      if (this._isBlockedByName(game.name)) continue;
      this._ensureLibraryEntry(game, saveBase, rootKey, isHeuristic);
    }
    if (this._library.size > 0) {
      await this._saveLibrary();
      console.log(`Auto-scan found ${this._library.size} games`);
      this.emit('library-updated');
    }
  }

  async stop() {
    this._running = false;
    if (this._syncCheckInterval) { clearInterval(this._syncCheckInterval); this._syncCheckInterval = null; }
    await this.detector.stop();
    await this._saveLibrary();
  }

  // ---- Event Handlers ----

  async _handleSaveChanged({ game, gameId, saveBase, rootKey, changedPaths, isHeuristic }) {
    // Ensure game is in library
    const entry = this._ensureLibraryEntry(game, saveBase, rootKey, isHeuristic);
    entry.running = false;

    // Skip files we just restored (prevents restore → detect → backup → sync loop)
    const now = Date.now();
    const filtered = changedPaths.filter(p => {
      const restored = this._recentlyRestored.get(p);
      return !(restored && now - restored < 10000);
    });
    if (filtered.length === 0) return;
    changedPaths = filtered;

    // Check if game is enabled
    if (!entry.enabled) return;

    // Check user overrides
    const overrides = this.config.data.gameSaveGameOverrides?.[gameId];
    if (overrides?.enabled === false) return;

    // Engine-detected and known DB games auto-backup without confirmation.
    // Only low-confidence heuristics (generic save-extension matches) need confirmation.
    const confidence = game.confidence || (isHeuristic ? 'heuristic' : 'known');
    if (confidence === 'heuristic') {
      const confirmedGames = this.config.data.gameSaveConfirmedGames || [];
      if (!confirmedGames.includes(gameId)) {
        this.emit('game-detected', { game: entry, isHeuristic: true, needsConfirmation: true });
        return;
      }
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
        // Track which devices have saves for this game
        if (!entry.knownDevices) entry.knownDevices = {};
        entry.knownDevices[os.hostname()] = { status: 'synced', lastBackup: result.timestamp };
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

  /**
   * Compute the relative path within a root for cross-PC resolution.
   * e.g., C:\Users\rober\AppData\LocalLow\Taboo Tales\Empire of Lust
   *       root = appdata_locallow → relPath = Taboo Tales/Empire of Lust
   */
  _computeRelPath(saveBase, rootKey) {
    if (!saveBase || !rootKey) return null;
    const roots = this.gameDB.resolveRoots();
    const rootDir = roots[rootKey];
    if (!rootDir) return null;
    const normalized = path.resolve(saveBase);
    const normalizedRoot = path.resolve(rootDir);
    if (normalized.startsWith(normalizedRoot + path.sep)) {
      return normalized.slice(normalizedRoot.length + 1);
    }
    return null;
  }

  /**
   * Resolve saveBase for the LOCAL machine from rootKey + relPath.
   * This is what makes cross-PC restore work — different users, same relative path.
   */
  _resolveLocalSaveBase(entry) {
    // If the stored saveBase exists locally, use it
    if (entry.saveBase && fs.existsSync(entry.saveBase)) return entry.saveBase;
    // Otherwise resolve from rootKey + relPath
    if (entry.rootKey && entry.relPath) {
      const roots = this.gameDB.resolveRoots();
      const rootDir = roots[entry.rootKey];
      if (rootDir) return path.join(rootDir, entry.relPath);
    }
    return entry.saveBase; // fallback
  }

  _ensureLibraryEntry(game, saveBase, rootKey, isHeuristic) {
    if (this._library.has(game.id)) {
      const entry = this._library.get(game.id);
      // Update saveBase if this is a local detection (path exists on this PC)
      if (saveBase && fs.existsSync(saveBase)) {
        entry.saveBase = saveBase;
        entry.relPath = this._computeRelPath(saveBase, rootKey || entry.rootKey) || entry.relPath;
        if (rootKey) entry.rootKey = rootKey;
      }
      return entry;
    }

    const overrides = this.config.data.gameSaveGameOverrides?.[game.id];
    const relPath = this._computeRelPath(saveBase, rootKey);

    const entry = {
      id: game.id,
      name: game.name,
      displayName: overrides?.displayName || game.name,
      saveBase,
      rootKey,
      relPath,
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
      console.log(`Loading game library from: ${this._libraryPath}`);
      const raw = await fsp.readFile(this._libraryPath, 'utf-8');
      const data = JSON.parse(raw);
      if (data.games && data.games.length > 0) {
        for (const game of data.games) {
          this._library.set(game.id, { ...game, running: false });
        }
        console.log(`Loaded ${this._library.size} games from library`);
      } else {
        console.log('Library file exists but has no games');
      }
    } catch (err) {
      console.log(`No game library found: ${err.message}`);
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

  _migrateBlocklistIds() {
    try {
      const blocklist = require('../data/blocklist.json');
      if (!blocklist.blockedGameIds || blocklist.blockedGameIds.length === 0) return;
      if (!this.config.data.gameSaveBlockedGames) this.config.data.gameSaveBlockedGames = [];
      let migrated = 0;
      for (const id of blocklist.blockedGameIds) {
        if (!this.config.data.gameSaveBlockedGames.includes(id)) {
          this.config.data.gameSaveBlockedGames.push(id);
          migrated++;
        }
      }
      if (migrated > 0) {
        this.config.save();
        console.log(`Migrated ${migrated} blocked game IDs from blocklist.json to config`);
      }
    } catch {}
  }

  _getMergedDismissals() {
    // Union of: local config dismissals + synced _dismissed.json
    // Both sources merge — if ANY PC dismisses a game, it stays dismissed everywhere
    const set = new Set(this.config.data.gameSaveBlockedGames || []);
    // Synced dismissals were loaded into config on startup (_loadSyncedDismissals)
    // so they're already in gameSaveBlockedGames. This is the single source of truth.
    return set;
  }

  _isBlockedByName(name) {
    if (!name) return true;
    const nl = name.toLowerCase();
    // Block obvious non-game names
    const junkNames = [
      '_gsdata_', 'backups', 'config', 'saved', 'temporary', 'browser',
      'caches', 'crashreports', 'editor', 'temp', '_saved_', '.sync',
      'persistent', 'tokens', 'packages', 'my games', 'programs',
      'publishers', 'user data', 'my project', '[unnamed project]',
    ];
    if (junkNames.includes(nl)) return true;
    if (nl.includes('.conflict')) return true;
    return false;
  }

  async _saveLibrary() {
    const games = Array.from(this._library.values()).map(g => ({
      id: g.id,
      name: g.name,
      displayName: g.displayName,
      saveBase: g.saveBase,
      rootKey: g.rootKey,
      relPath: g.relPath,
      enabled: g.enabled,
      isHeuristic: g.isHeuristic,
      lastBackup: g.lastBackup,
      backupCount: g.backupCount,
      excludes: g.excludes,
      knownDevices: g.knownDevices || {},
    }));

    // Safety: don't overwrite a populated library with empty data
    // (protects synced library from being wiped by a PC with no local games)
    if (games.length === 0) {
      try {
        const existing = await fsp.readFile(this._libraryPath, 'utf-8');
        const parsed = JSON.parse(existing);
        if (parsed.games && parsed.games.length > 0) {
          console.log('Skipping library save — would overwrite synced data with empty');
          return;
        }
      } catch {} // File doesn't exist — fine to write empty
    }

    const data = { version: 1, lastUpdated: new Date().toISOString(), games };
    console.log(`Saving ${games.length} games to: ${this._libraryPath}`);
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
    const myDevice = os.hostname();
    const localSaveExists = new Map(); // gameId -> boolean

    return Array.from(this._library.values()).map(g => {
      const resolved = this._resolveLocalSaveBase(g);
      const existsLocally = resolved && fs.existsSync(resolved);

      // Build device status from cached backup info
      const devices = {};
      // This device
      if (existsLocally) {
        devices[myDevice] = { status: 'local', lastBackup: g.lastBackup };
      }
      // Other devices from backup metadata (cached in library)
      if (g.knownDevices) {
        for (const [dev, info] of Object.entries(g.knownDevices)) {
          if (dev === myDevice) continue;
          devices[dev] = info;
        }
      }

      return {
        ...g,
        displayName: this._getDisplayName(g),
        saveBase: resolved,
        existsLocally,
        devices,
      };
    });
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
      if (this._isBlockedByName(game.name)) continue;
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
    if (entry) {
      const displayName = this._getDisplayName(entry);
      const files = await this.backup.listBackupFiles(displayName, backupDir);
      if (files.length > 0) return files;
    }
    // Fallback: scan all game dirs for a matching backup
    const fsp = require('fs/promises');
    const gameSavesDir = path.join(this.configDir, 'game-saves');
    try {
      const dirs = await fsp.readdir(gameSavesDir, { withFileTypes: true });
      for (const d of dirs) {
        if (!d.isDirectory() || d.name.startsWith('_') || d.name.startsWith('.')) continue;
        const backupPath = path.join(gameSavesDir, d.name, 'backups', backupDir);
        if (fs.existsSync(backupPath)) {
          return this.backup.listBackupFiles(d.name, backupDir);
        }
      }
    } catch {}
    return [];
  }

  /**
   * Backup all enabled games at once.
   */
  async backupAll(onProgress) {
    let success = 0;
    let skipped = 0;
    let done = 0;

    // Collect eligible games
    const eligible = [];
    for (const [gameId, entry] of this._library) {
      if (!entry.enabled || !entry.saveBase) { skipped++; continue; }
      const resolved = this._resolveLocalSaveBase(entry);
      if (!fs.existsSync(resolved)) { skipped++; continue; }
      eligible.push(gameId);
    }

    const total = eligible.length;
    if (onProgress) onProgress({ done: 0, total, current: '' });

    // Run up to 4 backups concurrently
    const CONCURRENCY = 4;
    let i = 0;
    const runNext = async () => {
      while (i < eligible.length) {
        const gameId = eligible[i++];
        const entry = this._library.get(gameId);
        const name = this._getDisplayName(entry);
        try {
          const result = await this.backupNow(gameId);
          if (result) success++;
          else skipped++;
        } catch {
          skipped++;
        }
        done++;
        if (onProgress) onProgress({ done, total, current: name });
      }
    };
    await Promise.all(Array.from({ length: Math.min(CONCURRENCY, eligible.length) }, () => runNext()));

    return { success, skipped, total };
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
    let restoreTimer = null;
    let libraryTimer = null;

    try {
      const watcher = require('@parcel/watcher');
      watcher.subscribe(gameSavesDir, (err, events) => {
        if (err) return;

        // Check for current/ changes (auto-restore trigger)
        const hasCurrent = events.some(e =>
          e.path.includes(`${path.sep}current${path.sep}`) || e.path.includes('/current/'));
        if (hasCurrent) {
          if (restoreTimer) clearTimeout(restoreTimer);
          restoreTimer = setTimeout(() => this.autoRestoreAll(), 3000);
        }

        // Check for new game directories or _dismissed.json changes (library update)
        const hasNewGame = events.some(e =>
          e.path.includes(`${path.sep}_game.json`) ||
          e.path.includes(`${path.sep}_dismissed.json`));
        if (hasNewGame) {
          if (libraryTimer) clearTimeout(libraryTimer);
          libraryTimer = setTimeout(() => this._syncLibraryFromBackups(), 3000);
        }
      }).then(sub => {
        this._syncWatcherSub = sub;
      }).catch(() => {});
    } catch {}
  }

  /**
   * Scan the game-saves directory for game folders that arrived via sync
   * but aren't in the library yet. Also reload dismissed list.
   */
  async _syncLibraryFromBackups() {
    // Reload dismissed list from synced file
    await this._loadSyncedDismissals();
    const dismissed = this._getMergedDismissals();

    // Scan game-saves for _game.json files
    const backedUp = await this.backup.listBackedUpGames();
    let added = 0;
    for (const game of backedUp) {
      if (dismissed.has(game.id)) continue;
      if (this._isBlockedByName(game.name)) continue;
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
          knownDevices: {},
        });
        added++;
      }
    }

    // Remove dismissed games
    for (const id of dismissed) {
      this._library.delete(id);
    }

    if (added > 0) {
      console.log(`Sync library update: ${added} new game(s) from synced backups`);
      await this._saveLibrary();
    }
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

    // Resolve the LOCAL save path (handles cross-PC username differences)
    const localSaveBase = this._resolveLocalSaveBase(entry);

    // Skip auto-restore if the game's save path is already inside a synced folder.
    if (localSaveBase && this._isInsideSyncedFolder(localSaveBase)) {
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

    // Get newest mtime in the actual game save dir (may not exist yet on this PC)
    const localMtime = await this._getNewestMtime(localSaveBase);

    // If synced is newer (or local doesn't exist), auto-restore
    if (!localMtime || syncedMtime > localMtime) {
      // Check if game is running — skip if so
      if (fs.existsSync(localSaveBase)) {
        const locked = await this._isAnyFileLocked(localSaveBase);
        if (locked) {
          console.log(`Skipping auto-restore for ${displayName}: game appears to be running`);
          return false;
        }
      }

      console.log(`Auto-restoring ${displayName} to ${localSaveBase}: synced save is newer`);

      // Pre-restore safety backup + restore to resolved local path
      await this.backup.restoreCurrent(displayName, localSaveBase);

      // Mark all files in the restored directory as recently-restored
      // so the detector doesn't trigger a backup loop
      this._markRestoredDir(localSaveBase);

      // Update entry's saveBase to the resolved local path
      entry.saveBase = localSaveBase;

      this.emit('save-restored', {
        game: entry,
        source: 'auto-sync',
        targetPath: localSaveBase,
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
  _markRestoredDir(dir) {
    const now = Date.now();
    try {
      const walk = (d) => {
        const entries = fs.readdirSync(d, { withFileTypes: true });
        for (const e of entries) {
          const full = path.join(d, e.name);
          this._recentlyRestored.set(full, now);
          if (e.isDirectory()) walk(full);
        }
      };
      this._recentlyRestored.set(dir, now);
      if (fs.existsSync(dir)) walk(dir);
    } catch {}
    // Cleanup old entries
    if (this._recentlyRestored.size > 5000) {
      for (const [k, ts] of this._recentlyRestored) {
        if (now - ts > 15000) this._recentlyRestored.delete(k);
      }
    }
  }

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
