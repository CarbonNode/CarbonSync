/**
 * File Scanner — builds and maintains the file index for a sync folder.
 *
 * Uses better-sqlite3 for persistence and @parcel/watcher for live changes.
 * The index is the single source of truth for what files exist on this device.
 */

const fs = require('fs');
const fsp = require('fs/promises');
const path = require('path');
const crypto = require('crypto');
const Database = require('better-sqlite3');
const { chunkFile, hashFile } = require('./chunker');

class Scanner {
  /**
   * @param {string} folderPath - Absolute path to sync folder
   * @param {string} dbPath - Path to SQLite index database
   * @param {object} opts - { ignorePatterns: string[], onProgress: fn }
   */
  constructor(folderPath, dbPath, opts = {}) {
    this.folderPath = path.resolve(folderPath);
    this.db = new Database(dbPath);
    this.ignorePatterns = opts.ignorePatterns || [];
    this.onProgress = opts.onProgress || (() => {});
    this._scanning = false;

    this._initDB();
  }

  _initDB() {
    this.db.pragma('journal_mode = WAL');  // Write-Ahead Logging for concurrency
    this.db.pragma('synchronous = NORMAL');

    this.db.exec(`
      CREATE TABLE IF NOT EXISTS files (
        path TEXT PRIMARY KEY,
        size INTEGER NOT NULL,
        mtime_ms INTEGER NOT NULL,
        hash TEXT NOT NULL,
        chunks TEXT,
        scanned_at INTEGER NOT NULL
      );

      CREATE INDEX IF NOT EXISTS idx_files_hash ON files(hash);
    `);

    // Prepared statements for performance
    this._stmtUpsert = this.db.prepare(`
      INSERT INTO files (path, size, mtime_ms, hash, chunks, scanned_at)
      VALUES (?, ?, ?, ?, ?, ?)
      ON CONFLICT(path) DO UPDATE SET
        size = excluded.size,
        mtime_ms = excluded.mtime_ms,
        hash = excluded.hash,
        chunks = excluded.chunks,
        scanned_at = excluded.scanned_at
    `);

    this._stmtGet = this.db.prepare('SELECT * FROM files WHERE path = ?');
    this._stmtDelete = this.db.prepare('DELETE FROM files WHERE path = ?');
    this._stmtAll = this.db.prepare('SELECT * FROM files ORDER BY path');
    this._stmtPurgeOld = this.db.prepare('DELETE FROM files WHERE scanned_at < ?');
  }

  /**
   * Full scan of the folder. Compares mtime to skip unchanged files.
   * Returns { added, modified, deleted, unchanged, errors }
   */
  async fullScan() {
    if (this._scanning) return { skipped: true };
    this._scanning = true;

    const scanId = Date.now();
    const stats = { added: 0, modified: 0, deleted: 0, unchanged: 0, errors: 0, total: 0 };

    try {
      const files = await this._walkDir(this.folderPath);
      stats.total = files.length;

      // Batch insert for speed
      const batchInsert = this.db.transaction((fileList) => {
        for (let i = 0; i < fileList.length; i++) {
          const file = fileList[i];
          const relPath = path.relative(this.folderPath, file.path).replace(/\\/g, '/');

          this.onProgress({
            phase: 'scanning',
            current: i + 1,
            total: fileList.length,
            file: relPath,
          });

          try {
            const existing = this._stmtGet.get(relPath);

            // Skip if mtime and size unchanged (fast path)
            if (existing &&
                existing.mtime_ms === Math.floor(file.mtime) &&
                existing.size === file.size) {
              // Just update scanned_at to mark as still present
              this._stmtUpsert.run(
                relPath, existing.size, existing.mtime_ms,
                existing.hash, existing.chunks, scanId
              );
              stats.unchanged++;
              continue;
            }

            // File is new or modified — hash it
            // For small files (< 256KB), just hash. For larger, chunk.
            let hash, chunks;
            if (file.size < 256 * 1024) {
              hash = this._hashFileSync(file.path);
              chunks = null;
            } else {
              // We can't await inside a transaction easily, so hash sync for now
              hash = this._hashFileSync(file.path);
              chunks = null; // Chunking happens on-demand during transfer
            }

            this._stmtUpsert.run(relPath, file.size, Math.floor(file.mtime), hash, chunks, scanId);

            if (existing) {
              stats.modified++;
            } else {
              stats.added++;
            }
          } catch (err) {
            stats.errors++;
            // Log but don't abort — skip locked/inaccessible files
            console.warn(`Scan error for ${file.path}: ${err.message}`);
          }
        }
      });

      batchInsert(files);

      // Purge files that no longer exist on disk
      const purged = this._stmtPurgeOld.run(scanId);
      stats.deleted = purged.changes;

      return stats;
    } finally {
      this._scanning = false;
    }
  }

  /**
   * Incremental update for a single file (called by watcher).
   */
  updateFile(absolutePath) {
    try {
      const relPath = path.relative(this.folderPath, absolutePath).replace(/\\/g, '/');
      if (this._shouldIgnore(relPath)) return null;

      const stat = fs.statSync(absolutePath);
      if (!stat.isFile()) return null;

      const hash = this._hashFileSync(absolutePath);
      this._stmtUpsert.run(relPath, stat.size, Math.floor(stat.mtimeMs), hash, null, Date.now());
      return { path: relPath, size: stat.size, hash, mtime_ms: Math.floor(stat.mtimeMs) };
    } catch (err) {
      console.warn(`Update error for ${absolutePath}: ${err.message}`);
      return null;
    }
  }

  /**
   * Remove a file from the index (called by watcher on delete).
   */
  removeFile(absolutePath) {
    const relPath = path.relative(this.folderPath, absolutePath).replace(/\\/g, '/');
    this._stmtDelete.run(relPath);
    return relPath;
  }

  /**
   * Get the full index as an array.
   */
  getIndex() {
    return this._stmtAll.all();
  }

  /**
   * Get index as a map for fast lookup.
   */
  getIndexMap() {
    const map = new Map();
    for (const row of this._stmtAll.iterate()) {
      map.set(row.path, row);
    }
    return map;
  }

  /**
   * Get a single file entry.
   */
  getFile(relPath) {
    return this._stmtGet.get(relPath);
  }

  /**
   * Compute a root hash of the entire index (for quick equality check).
   */
  getRootHash() {
    const rows = this._stmtAll.all();
    if (rows.length === 0) return 'empty';
    const hash = crypto.createHash('sha256');
    for (const row of rows) {
      hash.update(`${row.path}\0${row.hash}\0${row.size}\0${row.mtime_ms}\n`);
    }
    return hash.digest('hex');
  }

  close() {
    this.db.close();
  }

  // ---- Private ----

  _hashFileSync(filePath) {
    const hash = crypto.createHash('sha256');
    const fd = fs.openSync(filePath, 'r');
    const buf = Buffer.alloc(256 * 1024);
    let bytesRead;
    try {
      while ((bytesRead = fs.readSync(fd, buf, 0, buf.length, null)) > 0) {
        hash.update(buf.subarray(0, bytesRead));
      }
    } finally {
      fs.closeSync(fd);
    }
    return hash.digest('hex');
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
        const fullPath = path.join(currentDir, entry.name);
        const relPath = path.relative(this.folderPath, fullPath).replace(/\\/g, '/');

        if (this._shouldIgnore(relPath)) continue;

        if (entry.isDirectory()) {
          stack.push(fullPath);
        } else if (entry.isFile()) {
          try {
            const stat = await fsp.stat(fullPath);
            results.push({
              path: fullPath,
              size: stat.size,
              mtime: stat.mtimeMs,
            });
          } catch (err) {
            // File may have been deleted between readdir and stat
            console.warn(`Cannot stat ${fullPath}: ${err.message}`);
          }
        }
      }
    }

    return results;
  }

  _shouldIgnore(relPath) {
    // Always ignore sync metadata
    if (relPath.startsWith('.carbonsync/') || relPath === '.carbonsync') return true;
    if (relPath.includes('/.carbonsync/')) return true;

    // System files
    const name = path.basename(relPath);
    if (name === 'Thumbs.db' || name === 'desktop.ini' || name === '.DS_Store') return true;
    if (name.endsWith('.tmp') || name.endsWith('.partial')) return true;

    // User patterns
    for (const pattern of this.ignorePatterns) {
      if (relPath.startsWith(pattern) || name === pattern) return true;
    }

    return false;
  }
}

module.exports = { Scanner };
