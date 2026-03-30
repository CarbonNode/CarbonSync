/**
 * Game Save Detector — watches Documents and AppData directories
 * for file changes, matches them against known games or heuristics.
 *
 * Uses @parcel/watcher for native OS file events (efficient, event-driven).
 * Does NOT build a full index — only reacts to changes.
 */

const path = require('path');
const fs = require('fs');
const fsp = require('fs/promises');
const { EventEmitter } = require('events');

const DEBOUNCE_MS = 3000;

// Patterns to always skip at the watcher level
const SKIP_PATTERNS = [
  /[/\\]\.carbonsync[/\\]/,
  /\.carbonsync\.tmp$/,
  /\.partial$/,
  /[/\\]desktop\.ini$/i,
  /[/\\]Thumbs\.db$/i,
  /[/\\]\.DS_Store$/i,
  /~\$/, // Office temp files
];

class GameDetector extends EventEmitter {
  /**
   * @param {object} opts
   * @param {import('./game-db').GameDB} opts.gameDB
   * @param {import('./config').Config} opts.config
   */
  constructor({ gameDB, config }) {
    super();
    this.gameDB = gameDB;
    this.config = config;
    this._watchers = [];
    this._debounceTimers = new Map(); // gameId -> timer
    this._pendingChanges = new Map(); // gameId -> { game, paths: Set, rootKey }
    this._running = false;
    this._lockedRetryQueue = new Map(); // absPath -> { gameId, retries }
    this._retryInterval = null;
  }

  async start() {
    if (this._running) return;
    this._running = true;

    const watcher = require('@parcel/watcher');
    const enabledDirs = this.config.data.settings?.gameSaveScanDirs;
    const watchDirs = this.gameDB.getWatchDirs(enabledDirs);

    for (const { key, path: dirPath } of watchDirs) {
      try {
        const sub = await watcher.subscribe(dirPath, (err, events) => {
          if (err) {
            console.error(`Game detector watcher error for ${key}:`, err.message);
            return;
          }
          this._handleEvents(key, dirPath, events);
        }, {
          ignore: [
            '**/node_modules/**',
            '**/.git/**',
            '**/Cache/**',
            '**/Code Cache/**',
            '**/GPUCache/**',
            '**/GrShaderCache/**',
            '**/ShaderCache/**',
            '**/DawnCache/**',
            '**/Session Storage/**',
            '**/Local Storage/**',
            '**/IndexedDB/**',
            '**/Service Worker/**',
            '**/CacheStorage/**',
            '**/blob_storage/**',
            '**/Logs/**',
            '**/logs/**',
            '**/Crash Reports/**',
            '**/CrashDumps/**',
            '**/Crashpad/**',
          ],
        });
        this._watchers.push(sub);
        console.log(`Game detector watching: ${dirPath} (${key})`);
      } catch (err) {
        console.error(`Failed to watch ${dirPath}: ${err.message}`);
      }
    }

    // Retry locked files every 30 seconds
    this._retryInterval = setInterval(() => this._retryLockedFiles(), 30000);
  }

  async stop() {
    this._running = false;
    for (const sub of this._watchers) {
      try { await sub.unsubscribe(); } catch {}
    }
    this._watchers = [];
    for (const timer of this._debounceTimers.values()) {
      clearTimeout(timer);
    }
    this._debounceTimers.clear();
    this._pendingChanges.clear();
    if (this._retryInterval) {
      clearInterval(this._retryInterval);
      this._retryInterval = null;
    }
  }

  _handleEvents(rootKey, rootDir, events) {
    for (const event of events) {
      const absPath = event.path;

      // Quick skip patterns
      if (SKIP_PATTERNS.some(p => p.test(absPath))) continue;

      // Check blocklist (fast string matching)
      if (this.gameDB.isBlocklisted(absPath, rootKey)) continue;

      // Try known game match
      let match = this.gameDB.matchPath(absPath);
      let isHeuristic = false;

      if (!match) {
        // Try heuristic detection
        const heuristic = this.gameDB.detectHeuristic(absPath, rootKey);
        if (heuristic) {
          match = {
            game: heuristic,
            rootKey: heuristic.rootKey,
            rootDir,
            saveBase: heuristic.saveBase,
            relPath: path.relative(heuristic.saveBase, absPath),
          };
          isHeuristic = true;
        }
      }

      if (!match) continue;

      const gameId = match.game.id;

      // Accumulate changes per game
      if (!this._pendingChanges.has(gameId)) {
        this._pendingChanges.set(gameId, {
          game: match.game,
          saveBase: match.saveBase,
          rootKey: match.rootKey,
          paths: new Set(),
          isHeuristic,
        });
      }
      this._pendingChanges.get(gameId).paths.add(absPath);

      // Debounce: wait for burst to settle
      if (this._debounceTimers.has(gameId)) {
        clearTimeout(this._debounceTimers.get(gameId));
      }
      this._debounceTimers.set(gameId, setTimeout(() => {
        this._flushGame(gameId);
      }, DEBOUNCE_MS));
    }
  }

  async _flushGame(gameId) {
    this._debounceTimers.delete(gameId);
    const pending = this._pendingChanges.get(gameId);
    if (!pending) return;
    this._pendingChanges.delete(gameId);

    const { game, saveBase, rootKey, paths, isHeuristic } = pending;

    // Check which files are locked
    const unlocked = [];
    for (const absPath of paths) {
      const locked = await this._isFileLocked(absPath);
      if (locked) {
        // Queue for retry
        if (!this._lockedRetryQueue.has(absPath)) {
          this._lockedRetryQueue.set(absPath, { gameId, game, saveBase, rootKey, isHeuristic, retries: 0 });
        }
      } else {
        unlocked.push(absPath);
      }
    }

    // If ALL files are locked, game is probably running
    if (unlocked.length === 0 && paths.size > 0) {
      this.emit('game-running', { game, gameId, saveBase });
      return;
    }

    // Emit detection event for new games
    this.emit('save-changed', {
      game,
      gameId,
      saveBase,
      rootKey,
      changedPaths: unlocked,
      isHeuristic,
    });
  }

  async _isFileLocked(filePath) {
    try {
      const stat = await fsp.stat(filePath);
      if (stat.isDirectory()) return false;
      const fd = await fsp.open(filePath, 'r');
      await fd.close();
      return false;
    } catch (err) {
      if (err.code === 'EBUSY' || err.code === 'EACCES' || err.code === 'EPERM') {
        return true;
      }
      // File might not exist anymore (deleted between event and check)
      if (err.code === 'ENOENT') return false;
      return false;
    }
  }

  async _retryLockedFiles() {
    const maxRetries = 10;
    const toRemove = [];

    for (const [absPath, info] of this._lockedRetryQueue) {
      info.retries++;
      if (info.retries > maxRetries) {
        toRemove.push(absPath);
        continue;
      }

      const locked = await this._isFileLocked(absPath);
      if (!locked) {
        toRemove.push(absPath);
        // Emit the save change
        this.emit('save-changed', {
          game: info.game,
          gameId: info.gameId,
          saveBase: info.saveBase,
          rootKey: info.rootKey,
          changedPaths: [absPath],
          isHeuristic: info.isHeuristic,
        });
      }
    }

    for (const p of toRemove) {
      this._lockedRetryQueue.delete(p);
    }
  }

  /**
   * Do a one-time scan to find existing games.
   *
   * Strategy 1: For every known game in the DB, check if its save path exists on disk.
   * Strategy 2: Scan LocalLow for Unity games (Company/Game/ pattern).
   * Strategy 3: Scan for engine-specific directories (RenPy/, Godot/, etc.)
   */
  async scanExistingGames() {
    const roots = this.gameDB.resolveRoots();
    const enabledDirs = this.config.data.settings?.gameSaveScanDirs;
    const enabledSet = new Set(enabledDirs || Object.keys(roots));
    const found = new Map(); // gameId -> { game, saveBase, rootKey, isHeuristic }

    // Strategy 1: Check every known game's save path
    for (const game of this.gameDB.getAllGames()) {
      for (const sp of game.savePaths) {
        if (!enabledSet.has(sp.root)) continue;
        const rootDir = roots[sp.root];
        if (!rootDir) continue;

        const saveDir = path.join(rootDir, sp.pattern);
        try {
          await fsp.access(saveDir);
          found.set(game.id, {
            game,
            saveBase: saveDir,
            rootKey: sp.root,
            isHeuristic: false,
          });
        } catch {} // Doesn't exist — skip
      }
    }

    // Strategy 2: Scan LocalLow for Unity games (Company/Game/)
    // Skip directories that are clearly not game companies (hashes, system dirs, etc.)
    const NOT_GAME_COMPANIES = new Set([
      'microsoft', 'adobe', 'google', 'apple', 'mozilla', 'oracle', 'java',
      'nvidia', 'amd', 'intel', 'realtek',
      'dxcache', 'shader cache', 'shadercache', 'tokens', 'temp',
      'sun', 'hp', 'dell', 'lenovo', 'asus',
    ]);
    if (enabledSet.has('appdata_locallow') && roots.appdata_locallow) {
      try {
        const companies = await fsp.readdir(roots.appdata_locallow, { withFileTypes: true });
        for (const company of companies) {
          if (!company.isDirectory()) continue;
          // Skip hash-named dirs (UWP sandboxes), dot-dirs, and known non-game companies
          if (/^[0-9a-f]{20,}$/i.test(company.name)) continue;
          if (company.name.startsWith('.')) continue;
          if (NOT_GAME_COMPANIES.has(company.name.toLowerCase())) continue;
          if (this.gameDB.isBlocklisted(path.join(roots.appdata_locallow, company.name), 'appdata_locallow')) continue;

          try {
            const games = await fsp.readdir(path.join(roots.appdata_locallow, company.name), { withFileTypes: true });
            for (const gameDir of games) {
              if (!gameDir.isDirectory()) continue;
              if (gameDir.name.startsWith('.')) continue;
              const saveBase = path.join(roots.appdata_locallow, company.name, gameDir.name);
              const id = `unity-${company.name}-${gameDir.name}`.toLowerCase().replace(/[^a-z0-9-]/g, '-');
              if (!found.has(id)) {
                found.set(id, {
                  game: { id, name: gameDir.name, confidence: 'engine', engine: 'Unity' },
                  saveBase,
                  rootKey: 'appdata_locallow',
                  isHeuristic: true,
                });
              }
            }
          } catch {}
        }
      } catch {}
    }

    // Strategy 3: Engine directories in Roaming
    if (enabledSet.has('appdata_roaming') && roots.appdata_roaming) {
      // Ren'Py: Roaming/RenPy/<game>/
      const renpyDir = path.join(roots.appdata_roaming, 'RenPy');
      try {
        const entries = await fsp.readdir(renpyDir, { withFileTypes: true });
        for (const entry of entries) {
          if (!entry.isDirectory()) continue;
          const id = `renpy-${entry.name}`.toLowerCase().replace(/[^a-z0-9-]/g, '-');
          if (!found.has(id)) {
            found.set(id, {
              game: { id, name: entry.name, confidence: 'engine', engine: "Ren'Py" },
              saveBase: path.join(renpyDir, entry.name),
              rootKey: 'appdata_roaming',
              isHeuristic: true,
            });
          }
        }
      } catch {}

      // Godot: Roaming/Godot/app_userdata/<project>/
      const godotDir = path.join(roots.appdata_roaming, 'Godot', 'app_userdata');
      try {
        const entries = await fsp.readdir(godotDir, { withFileTypes: true });
        for (const entry of entries) {
          if (!entry.isDirectory()) continue;
          const id = `godot-${entry.name}`.toLowerCase().replace(/[^a-z0-9-]/g, '-');
          if (!found.has(id)) {
            found.set(id, {
              game: { id, name: entry.name, confidence: 'engine', engine: 'Godot' },
              saveBase: path.join(godotDir, entry.name),
              rootKey: 'appdata_roaming',
              isHeuristic: true,
            });
          }
        }
      } catch {}
    }

    console.log(`Scan found ${found.size} game(s)`);
    return Array.from(found.values());
  }

  /**
   * Deep scan — walks all watched dirs up to 3 levels deep, looks for
   * any directory containing save-like files. Much slower than scanExistingGames
   * but catches games not in the DB and not matching engine patterns.
   */
  async deepScanForGames() {
    const roots = this.gameDB.resolveRoots();
    const enabledDirs = this.config.data.settings?.gameSaveScanDirs;
    const enabledSet = new Set(enabledDirs || Object.keys(roots));
    const found = new Map();

    // First run the normal scan
    const normal = await this.scanExistingGames();
    for (const entry of normal) {
      found.set(entry.game.id, entry);
    }

    const SAVE_EXTS = new Set([
      '.sav', '.save', '.savegame', '.dat', '.sl2', '.profile',
      '.es3', '.sfs', '.rws', '.fos', '.ess', '.lsv', '.owsave',
      '.gwsave', '.celeste', '.jkr', '.autosave', '.rpgsave',
      '.rvdata2', '.rvdata', '.rxdata',
    ]);

    const SAVE_DIRS = new Set([
      'saves', 'savedata', 'savegames', 'savegame', 'save',
      'saved games', 'savedgames', 'save_files', 'savefiles',
    ]);

    // Walk each root up to 3 levels
    for (const [rootKey, rootDir] of Object.entries(roots)) {
      if (!enabledSet.has(rootKey) || !rootDir) continue;

      const walk = async (dir, depth, parentName) => {
        if (depth > 3) return;
        let entries;
        try { entries = await fsp.readdir(dir, { withFileTypes: true }); } catch { return; }

        let hasSaveFile = false;
        let hasSaveDir = false;

        for (const entry of entries) {
          if (entry.name.startsWith('.')) continue;
          if (entry.isFile()) {
            const ext = path.extname(entry.name).toLowerCase();
            if (SAVE_EXTS.has(ext)) hasSaveFile = true;
          }
          if (entry.isDirectory()) {
            if (SAVE_DIRS.has(entry.name.toLowerCase())) hasSaveDir = true;
          }
        }

        if ((hasSaveFile || hasSaveDir) && depth >= 1) {
          // This directory looks like it contains game saves
          const gameName = parentName || path.basename(dir);
          const id = `deep-${rootKey}-${gameName}`.toLowerCase().replace(/[^a-z0-9-]/g, '-');

          if (!found.has(id) && !this.gameDB.isBlocklisted(dir, rootKey)) {
            found.set(id, {
              game: { id, name: gameName, confidence: 'deep-scan' },
              saveBase: dir,
              rootKey,
              isHeuristic: true,
            });
          }
        }

        // Recurse into subdirs
        if (depth < 3) {
          for (const entry of entries) {
            if (!entry.isDirectory()) continue;
            if (entry.name.startsWith('.')) continue;
            if (/^[0-9a-f]{20,}$/i.test(entry.name)) continue; // UWP hash dirs
            if (this.gameDB.isBlocklisted(path.join(dir, entry.name), rootKey)) continue;

            await walk(path.join(dir, entry.name), depth + 1, depth === 0 ? entry.name : parentName || entry.name);
          }
        }
      };

      await walk(rootDir, 0, null);
    }

    console.log(`Deep scan found ${found.size} game(s)`);
    return Array.from(found.values());
  }
}

module.exports = { GameDetector };
