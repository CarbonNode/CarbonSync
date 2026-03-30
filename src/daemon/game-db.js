/**
 * Game Save Database — loads known game definitions + user custom games,
 * matches file paths to games, and provides heuristic detection.
 */

const fs = require('fs');
const path = require('path');
const os = require('os');
const { minimatch } = require('minimatch');

const BUNDLED_DB_PATH = path.join(__dirname, '..', 'data', 'game-db.json');
const BLOCKLIST_PATH = path.join(__dirname, '..', 'data', 'blocklist.json');

// Save-like file extensions for heuristic detection
const SAVE_EXTENSIONS = new Set([
  '.sav', '.save', '.savegame', '.dat', '.dat_old',
  '.sl2', '.profile', '.bak', '.es3', '.sfs',
  '.rws', '.fos', '.ess', '.lsv', '.owsave',
  '.gwsave', '.celeste', '.jkr', '.ntwtf',
  '.autosave', '.uberstate', '.tunic',
]);

// Directory names that strongly suggest game saves
const SAVE_DIR_NAMES = new Set([
  'saves', 'savedata', 'savegames', 'savegame',
  'saved games', 'savedgames', 'save_files', 'save',
  'savefiles', 'saveslots',
]);

// Unity games use AppData/LocalLow/<Company>/<Game>/
// Unreal games use AppData/Local/<Game>/Saved/SaveGames/
const UNREAL_SAVE_PATTERN = /[/\\]Saved[/\\]SaveGames[/\\]/i;
const UNITY_LOCALLOW_RE = /^[^/\\]+[/\\][^/\\]+[/\\]/; // Company/Game/ at start of LocalLow relative path

class GameDB {
  constructor(config) {
    this.config = config;
    this.games = new Map(); // id -> game definition
    this.blocklist = { publishers: [], appFolders: [], patterns: [] };
    this._roots = null;
    this._blocklistFolderSet = null;
    this._load();
  }

  _load() {
    // Load bundled database
    try {
      const raw = fs.readFileSync(BUNDLED_DB_PATH, 'utf-8');
      const db = JSON.parse(raw);
      for (const game of db.games) {
        this.games.set(game.id, game);
      }
    } catch (err) {
      console.error('Failed to load bundled game DB:', err.message);
    }

    // Load blocklist
    try {
      const raw = fs.readFileSync(BLOCKLIST_PATH, 'utf-8');
      this.blocklist = JSON.parse(raw);
      this._blocklistFolderSet = new Set(this.blocklist.appFolders.map(f => f.toLowerCase()));
    } catch (err) {
      console.error('Failed to load blocklist:', err.message);
      this._blocklistFolderSet = new Set();
    }

    // Load user custom games from config
    const customGames = this.config?.data?.gameSaveCustomGames || [];
    for (const game of customGames) {
      this.games.set(game.id, game);
    }
  }

  /**
   * Resolve root tokens to actual OS paths.
   */
  resolveRoots() {
    if (this._roots) return this._roots;
    const home = os.homedir();
    this._roots = {
      documents: path.join(home, 'Documents'),
      appdata_roaming: process.env.APPDATA || path.join(home, 'AppData', 'Roaming'),
      appdata_local: process.env.LOCALAPPDATA || path.join(home, 'AppData', 'Local'),
      appdata_locallow: path.join(home, 'AppData', 'LocalLow'),
    };
    return this._roots;
  }

  /**
   * Get all unique root directories that need watching.
   */
  getWatchDirs(enabledDirs) {
    const roots = this.resolveRoots();
    const dirs = [];
    for (const key of (enabledDirs || Object.keys(roots))) {
      if (roots[key] && fs.existsSync(roots[key])) {
        dirs.push({ key, path: roots[key] });
      }
    }
    return dirs;
  }

  /**
   * Match an absolute file path against the known game database.
   * Returns { game, rootKey, relPath } or null.
   */
  matchPath(absPath) {
    const roots = this.resolveRoots();
    const normalized = absPath.replace(/\\/g, '/');

    for (const [id, game] of this.games) {
      for (const sp of game.savePaths) {
        const rootDir = roots[sp.root];
        if (!rootDir) continue;
        const normalizedRoot = rootDir.replace(/\\/g, '/');

        if (!normalized.startsWith(normalizedRoot + '/')) continue;

        const relFromRoot = normalized.slice(normalizedRoot.length + 1);
        const patternNorm = sp.pattern.replace(/\\/g, '/');

        // Check if the relative path starts with or is inside the save pattern
        if (relFromRoot.startsWith(patternNorm + '/') || relFromRoot === patternNorm) {
          return {
            game,
            rootKey: sp.root,
            rootDir,
            saveBase: path.join(rootDir, sp.pattern),
            relPath: relFromRoot.slice(patternNorm.length + 1) || '',
          };
        }
      }
    }
    return null;
  }

  /**
   * Check if a path should be ignored (blocklisted app).
   */
  isBlocklisted(absPath, rootKey) {
    const roots = this.resolveRoots();
    const rootDir = roots[rootKey];
    if (!rootDir) return false;

    const normalized = absPath.replace(/\\/g, '/');
    const relFromRoot = normalized.slice(rootDir.replace(/\\/g, '/').length + 1);

    // Check if first directory component is a known non-game app
    const firstDir = relFromRoot.split('/')[0];
    if (firstDir && this._blocklistFolderSet.has(firstDir.toLowerCase())) {
      return true;
    }

    // Check pattern blocklist
    for (const pattern of this.blocklist.patterns) {
      if (minimatch(relFromRoot, pattern, { dot: true, nocase: true })) {
        return true;
      }
    }

    return false;
  }

  /**
   * Heuristic detection for unknown games.
   * Returns { id, name, saveBase, confidence } or null.
   */
  detectHeuristic(absPath, rootKey) {
    const roots = this.resolveRoots();
    const rootDir = roots[rootKey];
    if (!rootDir) return null;

    const normalized = absPath.replace(/\\/g, '/');
    const relFromRoot = normalized.slice(rootDir.replace(/\\/g, '/').length + 1);
    const parts = relFromRoot.split('/');
    const ext = path.extname(absPath).toLowerCase();
    const fileName = path.basename(absPath);

    // Skip if user already blocked this
    const blockedGames = this.config?.data?.gameSaveBlockedGames || [];

    // Check save-like extension
    const hasSaveExt = SAVE_EXTENSIONS.has(ext);

    // Check parent directory names for save-like patterns
    const hasSaveDir = parts.some(p => SAVE_DIR_NAMES.has(p.toLowerCase()));

    // Unity game in LocalLow: LocalLow/<Company>/<Game>/
    if (rootKey === 'appdata_locallow' && parts.length >= 2) {
      const company = parts[0];
      const gameName = parts[1];
      const saveBase = path.join(rootDir, company, gameName);
      const id = `heuristic-${company}-${gameName}`.toLowerCase().replace(/[^a-z0-9-]/g, '-');

      if (blockedGames.includes(id)) return null;

      if (hasSaveExt || hasSaveDir) {
        return { id, name: gameName, saveBase, confidence: 'heuristic', rootKey };
      }
    }

    // Unreal game pattern: <Game>/Saved/SaveGames/
    if (UNREAL_SAVE_PATTERN.test(normalized)) {
      const unrealMatch = normalized.match(/[/\\]([^/\\]+)[/\\]Saved[/\\]SaveGames[/\\]/i);
      if (unrealMatch) {
        const gameName = unrealMatch[1];
        const saveBase = normalized.split(/[/\\]Saved[/\\]SaveGames/i)[0] + '/Saved/SaveGames';
        const id = `heuristic-unreal-${gameName}`.toLowerCase().replace(/[^a-z0-9-]/g, '-');

        if (blockedGames.includes(id)) return null;

        return { id, name: gameName, saveBase: saveBase.replace(/\//g, path.sep), confidence: 'heuristic', rootKey };
      }
    }

    // Generic: file with save extension inside a directory that looks like a game
    if (hasSaveExt && parts.length >= 1) {
      // Use first directory component as game name
      const gameName = parts[0];
      const saveBase = path.join(rootDir, gameName);
      const id = `heuristic-${rootKey}-${gameName}`.toLowerCase().replace(/[^a-z0-9-]/g, '-');

      if (blockedGames.includes(id)) return null;

      return { id, name: gameName, saveBase, confidence: 'heuristic', rootKey };
    }

    // Directory named "Saves" or similar
    if (hasSaveDir && parts.length >= 2) {
      const gameName = parts[0];
      const saveBase = path.join(rootDir, gameName);
      const id = `heuristic-${rootKey}-${gameName}`.toLowerCase().replace(/[^a-z0-9-]/g, '-');

      if (blockedGames.includes(id)) return null;

      return { id, name: gameName, saveBase, confidence: 'heuristic', rootKey };
    }

    return null;
  }

  /**
   * Add a custom game definition.
   */
  addCustomGame(game) {
    this.games.set(game.id, game);
    if (!this.config.data.gameSaveCustomGames) {
      this.config.data.gameSaveCustomGames = [];
    }
    // Replace if exists, or push new
    const idx = this.config.data.gameSaveCustomGames.findIndex(g => g.id === game.id);
    if (idx >= 0) this.config.data.gameSaveCustomGames[idx] = game;
    else this.config.data.gameSaveCustomGames.push(game);
    this.config.save();
  }

  /**
   * Remove a custom game (does not remove bundled ones).
   */
  removeCustomGame(gameId) {
    if (!this.config.data.gameSaveCustomGames) return;
    this.config.data.gameSaveCustomGames = this.config.data.gameSaveCustomGames.filter(g => g.id !== gameId);
    this.games.delete(gameId);
    this.config.save();
  }

  /**
   * Get all game definitions.
   */
  getAllGames() {
    return Array.from(this.games.values());
  }

  /**
   * Get a game by ID.
   */
  getGame(id) {
    return this.games.get(id) || null;
  }
}

module.exports = { GameDB };
