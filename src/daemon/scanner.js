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

class Scanner {
  constructor(folderPath, dbPath, opts = {}) {
    this.folderPath = path.resolve(folderPath);
    this.db = new Database(dbPath);
    this.ignorePatterns = opts.ignorePatterns || [];
    this.onProgress = opts.onProgress || (() => {});
    this._scanning = false;
    this._stale = false; // True if scan was incomplete

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
  }

  /**
   * Full scan. Hashes OUTSIDE transaction, writes in batched transactions.
   */
  async fullScan() {
    if (this._scanning) return { skipped: true };
    this._scanning = true;
    this._stale = false;

    const scanId = Date.now();
    const stats = { added: 0, modified: 0, deleted: 0, unchanged: 0, errors: 0, total: 0 };

    try {
      // Verify folder is accessible
      try {
        await fsp.access(this.folderPath);
      } catch {
        console.error(`Scan aborted: folder inaccessible: ${this.folderPath}`);
        this._stale = true;
        return { ...stats, error: 'Folder inaccessible' };
      }

      const files = await this._walkDir(this.folderPath);
      stats.total = files.length;

      // Phase 1: Hash files that changed (OUTSIDE transaction)
      const updates = [];
      for (let i = 0; i < files.length; i++) {
        const file = files[i];
        const relPath = path.relative(this.folderPath, file.path).replace(/\\/g, '/');

        this.onProgress({ phase: 'scanning', current: i + 1, total: files.length, file: relPath });

        try {
          const existing = this._stmtGet.get(relPath);

          // Fast path: skip if mtime and size unchanged
          if (existing &&
              existing.mtime_ms === Math.floor(file.mtime) &&
              existing.size === file.size) {
            updates.push({ relPath, size: existing.size, mtime: existing.mtime_ms, hash: existing.hash, scanId, isNew: false });
            stats.unchanged++;
            continue;
          }

          // Hash file (NOT inside transaction — non-blocking for DB)
          const hash = await this._hashFile(file.path);
          updates.push({ relPath, size: file.size, mtime: Math.floor(file.mtime), hash, scanId, isNew: !existing });

          if (existing) stats.modified++;
          else stats.added++;
        } catch (err) {
          stats.errors++;
          console.warn(`Scan error [${relPath}]: ${err.message}`);
        }
      }

      // Phase 2: Write all updates in a single fast transaction
      const batchWrite = this.db.transaction((items) => {
        for (const item of items) {
          this._stmtUpsert.run(item.relPath, item.size, item.mtime, item.hash, item.scanId);
        }
      });
      batchWrite(updates);

      // Phase 3: Purge deleted files
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
    const results = [];
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

      for (const entry of entries) {
        // Skip symlinks entirely (security + no infinite loops)
        if (entry.isSymbolicLink()) continue;

        const fullPath = path.join(currentDir, entry.name);
        const relPath = path.relative(this.folderPath, fullPath).replace(/\\/g, '/');

        if (this._shouldIgnore(relPath)) continue;

        if (entry.isDirectory()) {
          stack.push(fullPath);
        } else if (entry.isFile()) {
          try {
            const stat = await fsp.stat(fullPath);
            results.push({ path: fullPath, size: stat.size, mtime: stat.mtimeMs });
          } catch { /* file deleted between readdir and stat */ }
        }
      }
    }

    return results;
  }

  _shouldIgnore(relPath) {
    if (relPath.startsWith('.carbonsync') || relPath.includes('/.carbonsync')) return true;
    const name = path.basename(relPath);
    if (name === 'Thumbs.db' || name === 'desktop.ini' || name === '.DS_Store') return true;
    if (name.endsWith('.carbonsync.tmp') || name.endsWith('.partial')) return true;
    for (const pattern of this.ignorePatterns) {
      if (relPath.startsWith(pattern) || name === pattern) return true;
    }
    return false;
  }
}

module.exports = { Scanner };
