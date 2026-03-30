/**
 * File Scanner — builds and maintains the file index for a sync folder.
 *
 * Fixed: hashing outside transactions, symlink handling, incomplete scan protection.
 */

const fs = require('fs');
const fsp = require('fs/promises');
const path = require('path');
const crypto = require('crypto');
const Database = require('better-sqlite3');
const { SyncIgnore } = require('./syncignore');

class Scanner {
  constructor(folderPath, dbPath, opts = {}) {
    this.folderPath = path.resolve(folderPath);
    this.db = new Database(dbPath);
    this.syncIgnore = new SyncIgnore(this.folderPath, opts.ignorePatterns || []);
    this.onProgress = opts.onProgress || (() => {});
    this._scanning = false;
    this._stale = false;

    this._initDB();
  }

  _initDB() {
    this.db.pragma('journal_mode = WAL');
    this.db.pragma('synchronous = NORMAL');

    this.db.exec(`
      CREATE TABLE IF NOT EXISTS files (
        path TEXT PRIMARY KEY,
        size INTEGER NOT NULL,
        mtime_ms INTEGER NOT NULL,
        hash TEXT NOT NULL,
        scanned_at INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_files_hash ON files(hash);
      CREATE TABLE IF NOT EXISTS dirs (
        path TEXT PRIMARY KEY,
        scanned_at INTEGER NOT NULL
      );
      CREATE TABLE IF NOT EXISTS meta (
        key TEXT PRIMARY KEY,
        value TEXT
      );
    `);

    this._stmtUpsert = this.db.prepare(`
      INSERT INTO files (path, size, mtime_ms, hash, scanned_at)
      VALUES (?, ?, ?, ?, ?)
      ON CONFLICT(path) DO UPDATE SET
        size = excluded.size, mtime_ms = excluded.mtime_ms,
        hash = excluded.hash, scanned_at = excluded.scanned_at
    `);
    this._stmtGet = this.db.prepare('SELECT * FROM files WHERE path = ?');
    this._stmtDelete = this.db.prepare('DELETE FROM files WHERE path = ?');
    this._stmtAll = this.db.prepare('SELECT * FROM files ORDER BY path');
    this._stmtPurgeOld = this.db.prepare('DELETE FROM files WHERE scanned_at < ?');
    this._stmtSetMeta = this.db.prepare('INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)');
    this._stmtGetMeta = this.db.prepare('SELECT value FROM meta WHERE key = ?');
    this._stmtUpsertDir = this.db.prepare('INSERT OR REPLACE INTO dirs (path, scanned_at) VALUES (?, ?)');
    this._stmtAllDirs = this.db.prepare('SELECT path FROM dirs ORDER BY path');
    this._stmtPurgeDirs = this.db.prepare('DELETE FROM dirs WHERE scanned_at < ?');
  }

  /**
   * Cancel a running scan.
   */
  cancelScan() {
    this._cancelRequested = true;
  }

  /**
   * Full scan. Fast mode: uses mtime:size fingerprint for new files (no hashing).
   * Only hashes files when content comparison is actually needed (during sync).
   * Writes to DB in batches of 500 for crash recovery.
   */
  async fullScan(opts = {}) {
    if (this._scanning) return { skipped: true };
    this._scanning = true;
    this._cancelRequested = false;
    this._stale = false;

    const scanId = Date.now();
    const stats = { added: 0, modified: 0, deleted: 0, unchanged: 0, errors: 0, total: 0 };
    const BATCH_SIZE = 500;

    try {
      try {
        await fsp.access(this.folderPath);
      } catch {
        console.error(`Scan aborted: folder inaccessible: ${this.folderPath}`);
        this._stale = true;
        return { ...stats, error: 'Folder inaccessible' };
      }

      const files = await this._walkDir(this.folderPath);
      stats.total = files.length;

      const batchWrite = this.db.transaction((items) => {
        for (const item of items) {
          this._stmtUpsert.run(item.relPath, item.size, item.mtime, item.hash, item.scanId);
        }
      });

      let batch = [];

      for (let i = 0; i < files.length; i++) {
        // Check for cancellation
        if (this._cancelRequested) {
          // Write what we have so far
          if (batch.length > 0) batchWrite(batch);
          console.log(`Scan cancelled at ${i}/${files.length}`);
          this._stale = true;
          return { ...stats, cancelled: true };
        }

        const file = files[i];
        const relPath = path.relative(this.folderPath, file.path).replace(/\\/g, '/');

        if (i % 100 === 0 || i === files.length - 1) {
          this.onProgress({ phase: 'scanning', current: i + 1, total: files.length, file: relPath });
        }

        try {
          const existing = this._stmtGet.get(relPath);

          // Fast path: skip if mtime and size unchanged
          if (existing &&
              existing.mtime_ms === Math.floor(file.mtime) &&
              existing.size === file.size) {
            batch.push({ relPath, size: existing.size, mtime: existing.mtime_ms, hash: existing.hash, scanId });
            stats.unchanged++;
          } else if (existing) {
            // File modified — hash it for accurate comparison
            const hash = await this._hashFile(file.path);
            batch.push({ relPath, size: file.size, mtime: Math.floor(file.mtime), hash, scanId });
            stats.modified++;
          } else {
            // New file — use fast fingerprint (mtime:size), defer full hash to sync time
            const fastHash = `fast:${file.size}:${Math.floor(file.mtime)}`;
            batch.push({ relPath, size: file.size, mtime: Math.floor(file.mtime), hash: fastHash, scanId });
            stats.added++;
          }
        } catch (err) {
          stats.errors++;
          if (stats.errors <= 10) console.warn(`Scan error [${relPath}]: ${err.message}`);
        }

        // Flush batch to DB periodically (crash recovery)
        if (batch.length >= BATCH_SIZE) {
          batchWrite(batch);
          batch = [];
        }
      }

      // Flush remaining
      if (batch.length > 0) batchWrite(batch);

      // Phase 3: Persist directories (for empty dir sync)
      if (this._dirs) {
        const dirBatch = this.db.transaction((dirList) => {
          for (const d of dirList) this._stmtUpsertDir.run(d, scanId);
        });
        dirBatch(this._dirs);
        this._stmtPurgeDirs.run(scanId);
      }

      // Phase 4: Purge deleted files
      const purged = this._stmtPurgeOld.run(scanId);
      stats.deleted = purged.changes;

      // Mark scan as complete
      this._stmtSetMeta.run('last_scan', String(scanId));
      this._stale = false;

      return stats;
    } catch (err) {
      console.error(`Scan failed: ${err.message}`);
      this._stale = true;
      return { ...stats, error: err.message };
    } finally {
      this._scanning = false;
    }
  }

  /**
   * Incremental update for a single file (called by watcher).
   */
  async updateFile(absolutePath) {
    try {
      const relPath = path.relative(this.folderPath, absolutePath).replace(/\\/g, '/');
      if (this._shouldIgnore(relPath)) return null;

      const stat = await fsp.stat(absolutePath);
      if (!stat.isFile()) return null;

      const hash = await this._hashFile(absolutePath);
      this._stmtUpsert.run(relPath, stat.size, Math.floor(stat.mtimeMs), hash, Date.now());
      return { path: relPath, size: stat.size, hash, mtime_ms: Math.floor(stat.mtimeMs) };
    } catch (err) {
      console.warn(`Update error [${absolutePath}]: ${err.message}`);
      return null;
    }
  }

  removeFile(absolutePath) {
    const relPath = path.relative(this.folderPath, absolutePath).replace(/\\/g, '/');
    this._stmtDelete.run(relPath);
    return relPath;
  }

  getIndex() { return this._stmtAll.all(); }

  getIndexMap() {
    const map = new Map();
    for (const row of this._stmtAll.iterate()) map.set(row.path, row);
    return map;
  }

  getFile(relPath) { return this._stmtGet.get(relPath); }

  /**
   * Ensure a file has a real SHA-256 hash (not a fast fingerprint).
   * Called on-demand during sync when actual content comparison is needed.
   */
  async ensureRealHash(relPath) {
    const entry = this._stmtGet.get(relPath);
    if (!entry) return null;
    if (!entry.hash.startsWith('fast:')) return entry; // Already has real hash

    const absPath = path.join(this.folderPath, relPath);
    try {
      const hash = await this._hashFile(absPath);
      this._stmtUpsert.run(relPath, entry.size, entry.mtime_ms, hash, entry.scanned_at);
      return { ...entry, hash };
    } catch (err) {
      console.warn(`Hash upgrade failed [${relPath}]: ${err.message}`);
      return entry;
    }
  }

  /**
   * Batch upgrade fast hashes to real hashes.
   */
  async upgradeFastHashes(onProgress) {
    const fastFiles = this.db.prepare("SELECT * FROM files WHERE hash LIKE 'fast:%'").all();
    if (fastFiles.length === 0) return 0;

    console.log(`Upgrading ${fastFiles.length} fast hashes to SHA-256...`);
    let upgraded = 0;

    for (let i = 0; i < fastFiles.length; i++) {
      if (this._cancelRequested) break;
      const f = fastFiles[i];
      const absPath = path.join(this.folderPath, f.path);
      try {
        const hash = await this._hashFile(absPath);
        this._stmtUpsert.run(f.path, f.size, f.mtime_ms, hash, f.scanned_at);
        upgraded++;
      } catch { /* skip */ }

      if (onProgress && i % 50 === 0) {
        onProgress({ phase: 'hashing', current: i + 1, total: fastFiles.length, file: f.path });
      }
    }

    console.log(`Upgraded ${upgraded}/${fastFiles.length} hashes`);
    return upgraded;
  }

  isStale() { return this._stale; }

  getRootHash() {
    if (this._stale) return 'stale';
    const rows = this._stmtAll.all();
    if (rows.length === 0) return 'empty';
    const hash = crypto.createHash('sha256');
    for (const row of rows) {
      hash.update(`${row.path}\0${row.hash}\0${row.size}\0${row.mtime_ms}\n`);
    }
    return hash.digest('hex');
  }

  /**
   * Get index page (for large indexes).
   */
  getIndexPage(offset, limit) {
    const stmt = this.db.prepare('SELECT * FROM files ORDER BY path LIMIT ? OFFSET ?');
    return stmt.all(limit, offset);
  }

  getFileCount() {
    return this.db.prepare('SELECT COUNT(*) as count FROM files').get().count;
  }

  close() { this.db.close(); }

  // ---- Private ----

  _hashFile(filePath) {
    return new Promise((resolve, reject) => {
      const hash = crypto.createHash('sha256');
      const stream = fs.createReadStream(filePath, { highWaterMark: 512 * 1024 });
      stream.on('data', (chunk) => hash.update(chunk));
      stream.on('end', () => resolve(hash.digest('hex')));
      stream.on('error', reject);
    });
  }

  async _walkDir(dir) {
    const files = [];
    const dirs = [];
    const stack = [dir];

    while (stack.length > 0) {
      const currentDir = stack.pop();
      let entries;
      try {
        entries = await fsp.readdir(currentDir, { withFileTypes: true });
      } catch (err) {
        console.warn(`Cannot read directory ${currentDir}: ${err.message}`);
        continue;
      }

      // Track this directory (for empty dir sync)
      if (currentDir !== dir) {
        const relDir = path.relative(this.folderPath, currentDir).replace(/\\/g, '/');
        if (!this._shouldIgnore(relDir + '/')) {
          dirs.push(relDir);
        }
      }

      let hasFiles = false;
      for (const entry of entries) {
        if (entry.isSymbolicLink()) continue;

        const fullPath = path.join(currentDir, entry.name);
        const relPath = path.relative(this.folderPath, fullPath).replace(/\\/g, '/');

        if (this._shouldIgnore(relPath)) continue;

        if (entry.isDirectory()) {
          stack.push(fullPath);
        } else if (entry.isFile()) {
          hasFiles = true;
          try {
            const stat = await fsp.stat(fullPath);
            files.push({ path: fullPath, size: stat.size, mtime: stat.mtimeMs });
          } catch { /* file deleted between readdir and stat */ }
        }
      }
    }

    this._dirs = dirs;
    return files;
  }

  /**
   * Get all tracked directories (for syncing empty dirs).
   */
  getDirs() { return this._stmtAllDirs.all().map(r => r.path); }

  /**
   * Reload ignore patterns (call when .carbonsyncignore changes).
   */
  reloadIgnore() { this.syncIgnore.reload(); }

  /**
   * Find files with a given hash (for move/rename detection).
   */
  findByHash(hash) {
    return this.db.prepare('SELECT * FROM files WHERE hash = ?').all(hash);
  }

  _shouldIgnore(relPath) {
    return this.syncIgnore.ignores(relPath);
  }
}

module.exports = { Scanner };
