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
   * Do a one-time scan of all watched directories to find existing games.
   * Used on startup to populate the game library.
   */
  async scanExistingGames() {
    const enabledDirs = this.config.data.settings?.gameSaveScanDirs;
    const watchDirs = this.gameDB.getWatchDirs(enabledDirs);
    const found = new Map(); // gameId -> { game, saveBase, rootKey, isHeuristic }

    for (const { key, path: rootDir } of watchDirs) {
      try {
        const entries = await fsp.readdir(rootDir, { withFileTypes: true });
        for (const entry of entries) {
          if (!entry.isDirectory()) continue;

          const dirPath = path.join(rootDir, entry.name);

          // Check blocklist
          if (this.gameDB.isBlocklisted(dirPath, key)) continue;

          // Check known games — try matching a synthetic path inside this dir
          const testPath = path.join(dirPath, '__probe__');
          const match = this.gameDB.matchPath(testPath);
          if (match) {
            found.set(match.game.id, {
              game: match.game,
              saveBase: match.saveBase,
              rootKey: key,
              isHeuristic: false,
            });
            continue;
          }

          // For deeper matches, scan one level down
          try {
            const subEntries = await fsp.readdir(dirPath, { withFileTypes: true });
            for (const sub of subEntries) {
              const subPath = path.join(dirPath, sub.name, sub.isDirectory() ? '__probe__' : '');
              const subMatch = this.gameDB.matchPath(sub.isDirectory() ? subPath : path.join(dirPath, sub.name));
              if (subMatch) {
                found.set(subMatch.game.id, {
                  game: subMatch.game,
                  saveBase: subMatch.saveBase,
                  rootKey: key,
                  isHeuristic: false,
                });
              }
            }
          } catch {}
        }
      } catch (err) {
        console.error(`Failed to scan ${rootDir}: ${err.message}`);
      }
    }

    return Array.from(found.values());
  }
}

module.exports = { GameDetector };
