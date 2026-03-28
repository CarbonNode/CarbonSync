/**
 * .carbonsyncignore parser — gitignore-compatible ignore patterns.
 *
 * Place a .carbonsyncignore file in any synced folder root.
 * Supports standard gitignore syntax (using the `ignore` npm package).
 */

const fs = require('fs');
const path = require('path');
const ignore = require('ignore');

const IGNORE_FILENAME = '.carbonsyncignore';

// Default patterns always ignored
const DEFAULT_PATTERNS = [
  '.carbonsync/',
  '.carbonsync/**',
  '*.carbonsync.tmp',
  '*.partial',
  'Thumbs.db',
  'desktop.ini',
  '.DS_Store',
  '~$*',           // Office temp files
  '*.swp',         // Vim swap
  '*.swo',
  '.git/',         // Git metadata
  '.git/**',
  'node_modules/', // Node.js
  'node_modules/**',
  '__pycache__/',
  '__pycache__/**',
  '*.pyc',
  '.env',          // Environment secrets
  '.env.*',
];

class SyncIgnore {
  /**
   * @param {string} folderPath - Root of synced folder
   * @param {string[]} extraPatterns - Additional patterns from config
   */
  constructor(folderPath, extraPatterns = []) {
    this.folderPath = folderPath;
    this.ig = ignore();

    // Load defaults
    this.ig.add(DEFAULT_PATTERNS);

    // Load extra from config
    if (extraPatterns.length > 0) {
      this.ig.add(extraPatterns);
    }

    // Load .carbonsyncignore from folder root
    this._loadIgnoreFile();
  }

  _loadIgnoreFile() {
    const ignoreFile = path.join(this.folderPath, IGNORE_FILENAME);
    try {
      if (fs.existsSync(ignoreFile)) {
        const content = fs.readFileSync(ignoreFile, 'utf-8');
        const lines = content.split('\n')
          .map(l => l.trim())
          .filter(l => l && !l.startsWith('#'));
        this.ig.add(lines);
        console.log(`Loaded ${lines.length} patterns from ${IGNORE_FILENAME}`);
      }
    } catch (err) {
      console.warn(`Failed to read ${ignoreFile}: ${err.message}`);
    }
  }

  /**
   * Check if a relative path should be ignored.
   */
  ignores(relPath) {
    // Normalize to forward slashes
    const normalized = relPath.replace(/\\/g, '/');
    return this.ig.ignores(normalized);
  }

  /**
   * Filter an array of relative paths, returning only non-ignored ones.
   */
  filter(paths) {
    return this.ig.filter(paths.map(p => p.replace(/\\/g, '/')));
  }

  /**
   * Reload ignore patterns from disk (call after .carbonsyncignore changes).
   */
  reload() {
    this.ig = ignore();
    this.ig.add(DEFAULT_PATTERNS);
    this._loadIgnoreFile();
  }
}

module.exports = { SyncIgnore, DEFAULT_PATTERNS, IGNORE_FILENAME };
