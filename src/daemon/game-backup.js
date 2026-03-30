/**
 * Game Save Backup & Versioning Engine
 *
 * Copies detected game saves into a managed backup directory with:
 * - current/ — latest saves (used for sync)
 * - backups/<timestamp>/ — versioned snapshots
 * - Configurable retention (default 10 versions)
 * - Restore from any version back to original game location
 */

const fs = require('fs');
const fsp = require('fs/promises');
const path = require('path');
const os = require('os');
const crypto = require('crypto');

class GameBackup {
  /**
   * @param {object} opts
   * @param {string} opts.configDir — ~/.carbonsync
   * @param {import('./config').Config} opts.config
   */
  constructor({ configDir, config }) {
    this.configDir = configDir;
    this.config = config;
    this.gameSavesDir = path.join(configDir, 'game-saves');
    fs.mkdirSync(this.gameSavesDir, { recursive: true });
  }

  /**
   * Get the managed directory for a game.
   */
  gameDir(gameName) {
    // Sanitize game name for filesystem
    const safe = gameName.replace(/[<>:"/\\|?*]/g, '_').trim();
    return path.join(this.gameSavesDir, safe);
  }

  /**
   * Backup a game's saves.
   *
   * Always updates current/ (for real-time sync).
   * Only creates a timestamped version if enough time has passed since the last
   * backup (default 5 min) — prevents spam during long play sessions with
   * frequent auto-saves. Force=true bypasses the cooldown (manual "Backup Now").
   *
   * @param {object} opts
   * @param {string} opts.gameId
   * @param {string} opts.gameName — display name
   * @param {string} opts.saveBase — absolute path to the game's save directory
   * @param {string[]} opts.changedPaths — specific files that changed (optional)
   * @param {string} opts.sourceDevice — device name that created this save
   * @param {boolean} opts.force — bypass cooldown (for manual backups)
   * @returns {{ backupDir: string|null, fileCount: number, totalSize: number, timestamp: string, currentOnly: boolean }}
   */
  async backupGame({ gameId, gameName, saveBase, changedPaths, sourceDevice, force }) {
    const dir = this.gameDir(gameName);
    const currentDir = path.join(dir, 'current');
    const backupsDir = path.join(dir, 'backups');

    await fsp.mkdir(currentDir, { recursive: true });
    await fsp.mkdir(backupsDir, { recursive: true });

    // Always update current/ (keeps sync up to date)
    let fileCount = 0;
    let totalSize = 0;
    try {
      const result = await this._copyDirSingle(saveBase, currentDir);
      fileCount = result.fileCount;
      totalSize = result.totalSize;
    } catch (err) {
      console.error(`Failed to update current/ for ${gameName}: ${err.message}`);
      throw err;
    }

    if (fileCount === 0) return null;

    // Update game metadata
    await this._updateGameMeta(dir, { gameId, gameName, saveBase });

    // Smart cooldown: skip versioned backup only if content hasn't changed.
    // If the save is different from the last backup, ALWAYS version it
    // (covers: quick shutdown, switching PCs, etc.)
    // If content is identical, use time-based cooldown to avoid spam.
    const cooldownMs = (this.config.data.settings?.gameSaveBackupCooldownMin || 5) * 60 * 1000;
    const lastBackupTime = await this._getLastBackupTime(backupsDir);
    const elapsed = Date.now() - (lastBackupTime || 0);
    const contentChanged = await this._hasContentChanged(currentDir, backupsDir);

    if (!force && lastBackupTime && elapsed < cooldownMs && !contentChanged) {
      // Same content, too recent — current/ is updated for sync, skip version
      return { backupDir: null, fileCount, totalSize, timestamp: new Date().toISOString(), currentOnly: true };
    }

    // Create timestamped versioned backup
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const backupDir = path.join(backupsDir, timestamp);
    await fsp.mkdir(backupDir, { recursive: true });

    try {
      await this._copyDirSingle(saveBase, backupDir);
    } catch (err) {
      try { await fsp.rm(backupDir, { recursive: true }); } catch {}
      // current/ was still updated, so sync works — just no version created
      return { backupDir: null, fileCount, totalSize, timestamp: new Date().toISOString(), currentOnly: true };
    }

    // Write backup metadata
    const meta = {
      gameId,
      gameName,
      sourceDevice: sourceDevice || os.hostname(),
      timestamp: new Date().toISOString(),
      fileCount,
      totalSize,
    };
    await fsp.writeFile(path.join(backupDir, '_meta.json'), JSON.stringify(meta, null, 2));

    // Enforce retention
    await this._enforceRetention(backupsDir);

    return { backupDir, fileCount, totalSize, timestamp, currentOnly: false };
  }

  async _getLastBackupTime(backupsDir) {
    try {
      const entries = await fsp.readdir(backupsDir, { withFileTypes: true });
      const dirs = entries.filter(e => e.isDirectory() && !e.name.startsWith('.')).map(e => e.name).sort();
      if (dirs.length === 0) return null;
      const latest = dirs[dirs.length - 1];
      const metaPath = path.join(backupsDir, latest, '_meta.json');
      try {
        const meta = JSON.parse(await fsp.readFile(metaPath, 'utf-8'));
        return new Date(meta.timestamp).getTime();
      } catch {
        return null;
      }
    } catch {
      return null;
    }
  }

  /**
   * Quick check: has the content in current/ changed vs the latest backup?
   * Compares total file count and sizes — fast, no hashing needed.
   */
  async _hasContentChanged(currentDir, backupsDir) {
    try {
      const entries = await fsp.readdir(backupsDir, { withFileTypes: true });
      const dirs = entries.filter(e => e.isDirectory() && !e.name.startsWith('.')).map(e => e.name).sort();
      if (dirs.length === 0) return true; // No previous backup — content is "new"

      const latestDir = path.join(backupsDir, dirs[dirs.length - 1]);

      // Get file sizes from current/ and latest backup
      const currentFiles = await this._getFileSizes(currentDir);
      const backupFiles = await this._getFileSizes(latestDir);

      // Different number of files = changed
      if (currentFiles.size !== backupFiles.size) return true;

      // Different sizes for any file = changed
      for (const [filePath, size] of currentFiles) {
        if (backupFiles.get(filePath) !== size) return true;
      }

      return false; // Same files, same sizes
    } catch {
      return true; // Error = assume changed (safe side)
    }
  }

  async _getFileSizes(dir, prefix = '') {
    const result = new Map();
    let entries;
    try { entries = await fsp.readdir(dir, { withFileTypes: true }); } catch { return result; }
    for (const entry of entries) {
      if (entry.name === '_meta.json' || entry.name.startsWith('.')) continue;
      const rel = prefix ? `${prefix}/${entry.name}` : entry.name;
      if (entry.isDirectory()) {
        const sub = await this._getFileSizes(path.join(dir, entry.name), rel);
        for (const [k, v] of sub) result.set(k, v);
      } else if (entry.isFile()) {
        try {
          const stat = await fsp.stat(path.join(dir, entry.name));
          result.set(rel, stat.size);
        } catch {}
      }
    }
    return result;
  }

  /**
   * Recursively copy a directory to two destinations (current + backup).
   */
  async _copyDir(src, destCurrent, destBackup) {
    let fileCount = 0;
    let totalSize = 0;

    const entries = await fsp.readdir(src, { withFileTypes: true });

    for (const entry of entries) {
      const srcPath = path.join(src, entry.name);

      // Skip meta files and temp files
      if (entry.name.startsWith('_meta') || entry.name.endsWith('.tmp') || entry.name.endsWith('.partial')) {
        continue;
      }

      if (entry.isDirectory()) {
        const subCurrent = path.join(destCurrent, entry.name);
        const subBackup = path.join(destBackup, entry.name);
        await fsp.mkdir(subCurrent, { recursive: true });
        await fsp.mkdir(subBackup, { recursive: true });
        const sub = await this._copyDir(srcPath, subCurrent, subBackup);
        fileCount += sub.fileCount;
        totalSize += sub.totalSize;
      } else if (entry.isFile()) {
        try {
          const stat = await fsp.stat(srcPath);

          // Try to copy — if locked, skip
          const currentDest = path.join(destCurrent, entry.name);
          const backupDest = path.join(destBackup, entry.name);

          await fsp.copyFile(srcPath, currentDest);
          await fsp.copyFile(srcPath, backupDest);

          // Preserve mtime
          try {
            await fsp.utimes(currentDest, stat.atime, stat.mtime);
            await fsp.utimes(backupDest, stat.atime, stat.mtime);
          } catch {}

          fileCount++;
          totalSize += stat.size;
        } catch (err) {
          if (err.code === 'EBUSY' || err.code === 'EACCES' || err.code === 'EPERM') {
            console.log(`Skipping locked file: ${srcPath}`);
          } else if (err.code !== 'ENOENT') {
            console.error(`Copy failed: ${srcPath}: ${err.message}`);
          }
        }
      }
    }

    return { fileCount, totalSize };
  }

  /**
   * Update _game.json metadata for a game.
   */
  async _updateGameMeta(gameDir, { gameId, gameName, saveBase }) {
    const metaPath = path.join(gameDir, '_game.json');
    let meta = {};
    try {
      meta = JSON.parse(await fsp.readFile(metaPath, 'utf-8'));
    } catch {}

    meta.id = gameId;
    meta.name = gameName;
    meta.displayName = meta.displayName || gameName; // Preserve user renames
    meta.saveBase = saveBase;
    meta.lastBackup = new Date().toISOString();
    meta.excludes = meta.excludes || [];
    meta.enabled = meta.enabled !== undefined ? meta.enabled : true;

    await fsp.writeFile(metaPath, JSON.stringify(meta, null, 2));
  }

  /**
   * Enforce retention policy: keep at most N backups,
   * but always keep at least 1 backup older than 24 hours.
   */
  async _enforceRetention(backupsDir) {
    const maxVersions = this.config.data.settings?.gameSaveMaxVersions || 10;

    let entries;
    try {
      entries = await fsp.readdir(backupsDir, { withFileTypes: true });
    } catch { return; }

    // Sort by name (timestamps sort lexicographically)
    const dirs = entries
      .filter(e => e.isDirectory() && !e.name.startsWith('.'))
      .map(e => e.name)
      .sort();

    if (dirs.length <= maxVersions) return;

    const now = Date.now();
    const oneDayAgo = now - 24 * 60 * 60 * 1000;

    // Find the oldest backup that's older than 24h
    let protectedOldBackup = null;
    for (const dir of dirs) {
      try {
        const metaPath = path.join(backupsDir, dir, '_meta.json');
        const meta = JSON.parse(await fsp.readFile(metaPath, 'utf-8'));
        if (new Date(meta.timestamp).getTime() < oneDayAgo) {
          protectedOldBackup = dir;
          break; // Oldest one
        }
      } catch {}
    }

    // Delete oldest backups until we're at max
    const toDelete = dirs.slice(0, dirs.length - maxVersions);
    for (const dir of toDelete) {
      if (dir === protectedOldBackup) continue; // Keep at least one old backup
      try {
        await fsp.rm(path.join(backupsDir, dir), { recursive: true });
      } catch (err) {
        console.error(`Failed to remove old backup ${dir}: ${err.message}`);
      }
    }
  }

  /**
   * Get backup history for a game.
   * Returns sorted array (newest first) of { timestamp, fileCount, totalSize, sourceDevice, dir }.
   */
  async getHistory(gameName) {
    const dir = this.gameDir(gameName);
    const backupsDir = path.join(dir, 'backups');

    let entries;
    try {
      entries = await fsp.readdir(backupsDir, { withFileTypes: true });
    } catch { return []; }

    const history = [];
    for (const entry of entries) {
      if (!entry.isDirectory() || entry.name.startsWith('.')) continue;
      try {
        const metaPath = path.join(backupsDir, entry.name, '_meta.json');
        const meta = JSON.parse(await fsp.readFile(metaPath, 'utf-8'));
        history.push({
          timestamp: meta.timestamp,
          fileCount: meta.fileCount,
          totalSize: meta.totalSize,
          sourceDevice: meta.sourceDevice,
          dir: entry.name,
        });
      } catch {
        // Backup without metadata — use directory name as timestamp
        history.push({
          timestamp: entry.name,
          fileCount: 0,
          totalSize: 0,
          sourceDevice: 'unknown',
          dir: entry.name,
        });
      }
    }

    // Newest first
    history.sort((a, b) => b.timestamp.localeCompare(a.timestamp));
    return history;
  }

  /**
   * Restore a game save from a specific backup to the original game location.
   *
   * @param {string} gameName
   * @param {string} backupTimestamp — directory name in backups/
   * @returns {{ restoredFiles: number }}
   */
  async restoreSave(gameName, backupTimestamp) {
    const dir = this.gameDir(gameName);
    const metaPath = path.join(dir, '_game.json');

    let gameMeta;
    try {
      gameMeta = JSON.parse(await fsp.readFile(metaPath, 'utf-8'));
    } catch {
      throw new Error(`Game metadata not found for ${gameName}`);
    }

    const saveBase = gameMeta.saveBase;
    if (!saveBase) throw new Error(`No save location recorded for ${gameName}`);

    const backupDir = path.join(dir, 'backups', backupTimestamp);
    if (!fs.existsSync(backupDir)) {
      throw new Error(`Backup not found: ${backupTimestamp}`);
    }

    // Safety: snapshot current game state BEFORE overwriting
    await this._snapshotBeforeRestore(gameName, gameMeta, dir);

    // Copy backup contents to the original save location
    await fsp.mkdir(saveBase, { recursive: true });
    const result = await this._restoreDir(backupDir, saveBase);

    return { restoredFiles: result.fileCount };
  }

  /**
   * Restore from current/ instead of a specific backup.
   */
  async restoreCurrent(gameName) {
    const dir = this.gameDir(gameName);
    const metaPath = path.join(dir, '_game.json');

    let gameMeta;
    try {
      gameMeta = JSON.parse(await fsp.readFile(metaPath, 'utf-8'));
    } catch {
      throw new Error(`Game metadata not found for ${gameName}`);
    }

    const saveBase = gameMeta.saveBase;
    if (!saveBase) throw new Error(`No save location recorded for ${gameName}`);

    const currentDir = path.join(dir, 'current');
    if (!fs.existsSync(currentDir)) {
      throw new Error(`No current save found for ${gameName}`);
    }

    // Safety: snapshot current game state BEFORE overwriting
    await this._snapshotBeforeRestore(gameName, gameMeta, dir);

    await fsp.mkdir(saveBase, { recursive: true });
    const result = await this._restoreDir(currentDir, saveBase);
    return { restoredFiles: result.fileCount };
  }

  /**
   * Before restoring, snapshot the current game saves so nothing is ever lost.
   * Tagged as "pre-restore" in metadata so the user can identify it.
   */
  async _snapshotBeforeRestore(gameName, gameMeta, gameDir) {
    const saveBase = gameMeta.saveBase;
    if (!saveBase || !fs.existsSync(saveBase)) return;

    const backupsDir = path.join(gameDir, 'backups');
    await fsp.mkdir(backupsDir, { recursive: true });

    const timestamp = 'pre-restore-' + new Date().toISOString().replace(/[:.]/g, '-');
    const snapshotDir = path.join(backupsDir, timestamp);
    await fsp.mkdir(snapshotDir, { recursive: true });

    try {
      // Copy current game directory into the snapshot
      const result = await this._copyDirSingle(saveBase, snapshotDir);
      if (result.fileCount === 0) {
        await fsp.rm(snapshotDir, { recursive: true });
        return;
      }

      // Write metadata
      await fsp.writeFile(path.join(snapshotDir, '_meta.json'), JSON.stringify({
        gameId: gameMeta.id,
        gameName,
        sourceDevice: require('os').hostname(),
        timestamp: new Date().toISOString(),
        fileCount: result.fileCount,
        totalSize: result.totalSize,
        preRestore: true,
      }, null, 2));

      console.log(`Pre-restore snapshot: ${gameName} (${result.fileCount} files)`);
    } catch (err) {
      console.error(`Pre-restore snapshot failed for ${gameName}: ${err.message}`);
      // Non-fatal — proceed with restore anyway
    }
  }

  /**
   * Copy a directory to a single destination (used for pre-restore snapshots).
   */
  async _copyDirSingle(src, dest) {
    let fileCount = 0;
    let totalSize = 0;

    let entries;
    try {
      entries = await fsp.readdir(src, { withFileTypes: true });
    } catch { return { fileCount, totalSize }; }

    for (const entry of entries) {
      if (entry.name.startsWith('_meta') || entry.name.endsWith('.tmp')) continue;
      const srcPath = path.join(src, entry.name);
      const destPath = path.join(dest, entry.name);

      if (entry.isDirectory()) {
        await fsp.mkdir(destPath, { recursive: true });
        const sub = await this._copyDirSingle(srcPath, destPath);
        fileCount += sub.fileCount;
        totalSize += sub.totalSize;
      } else if (entry.isFile()) {
        try {
          const stat = await fsp.stat(srcPath);
          await fsp.copyFile(srcPath, destPath);
          try { await fsp.utimes(destPath, stat.atime, stat.mtime); } catch {}
          fileCount++;
          totalSize += stat.size;
        } catch (err) {
          if (err.code !== 'EBUSY' && err.code !== 'EACCES' && err.code !== 'ENOENT') {
            console.error(`Snapshot copy failed: ${srcPath}: ${err.message}`);
          }
        }
      }
    }
    return { fileCount, totalSize };
  }

  async _restoreDir(src, dest) {
    let fileCount = 0;
    const entries = await fsp.readdir(src, { withFileTypes: true });

    for (const entry of entries) {
      if (entry.name === '_meta.json') continue; // Skip backup metadata

      const srcPath = path.join(src, entry.name);
      const destPath = path.join(dest, entry.name);

      if (entry.isDirectory()) {
        await fsp.mkdir(destPath, { recursive: true });
        const sub = await this._restoreDir(srcPath, destPath);
        fileCount += sub.fileCount;
      } else if (entry.isFile()) {
        await fsp.copyFile(srcPath, destPath);
        // Preserve mtime
        try {
          const stat = await fsp.stat(srcPath);
          await fsp.utimes(destPath, stat.atime, stat.mtime);
        } catch {}
        fileCount++;
      }
    }

    return { fileCount };
  }

  /**
   * Get game metadata from _game.json.
   */
  async getGameMeta(gameName) {
    const metaPath = path.join(this.gameDir(gameName), '_game.json');
    try {
      return JSON.parse(await fsp.readFile(metaPath, 'utf-8'));
    } catch {
      return null;
    }
  }

  /**
   * Update game metadata (for rename, excludes, etc.).
   */
  async updateGameMeta(gameName, updates) {
    const dir = this.gameDir(gameName);
    const metaPath = path.join(dir, '_game.json');

    let meta = {};
    try {
      meta = JSON.parse(await fsp.readFile(metaPath, 'utf-8'));
    } catch {}

    Object.assign(meta, updates);
    await fsp.writeFile(metaPath, JSON.stringify(meta, null, 2));
    return meta;
  }

  /**
   * List all games that have been backed up.
   */
  async listBackedUpGames() {
    const games = [];
    let entries;
    try {
      entries = await fsp.readdir(this.gameSavesDir, { withFileTypes: true });
    } catch { return games; }

    for (const entry of entries) {
      if (!entry.isDirectory() || entry.name.startsWith('.') || entry.name.startsWith('_')) continue;

      const metaPath = path.join(this.gameSavesDir, entry.name, '_game.json');
      try {
        const meta = JSON.parse(await fsp.readFile(metaPath, 'utf-8'));
        const history = await this.getHistory(entry.name);
        games.push({
          ...meta,
          folderName: entry.name,
          backupCount: history.length,
          latestBackup: history[0] || null,
        });
      } catch {
        // Directory without metadata — skip
      }
    }

    return games;
  }

  /**
   * List files inside a specific backup version.
   * Returns [{ path, size }] with paths relative to the backup root.
   */
  async listBackupFiles(gameName, backupDirName) {
    const dir = this.gameDir(gameName);
    const backupDir = path.join(dir, 'backups', backupDirName);
    if (!fs.existsSync(backupDir)) return [];

    const files = [];
    await this._walkFiles(backupDir, '', files);
    return files;
  }

  async _walkFiles(dir, prefix, results) {
    let entries;
    try { entries = await fsp.readdir(dir, { withFileTypes: true }); } catch { return; }
    for (const entry of entries) {
      if (entry.name === '_meta.json') continue;
      const relPath = prefix ? `${prefix}/${entry.name}` : entry.name;
      if (entry.isDirectory()) {
        await this._walkFiles(path.join(dir, entry.name), relPath, results);
      } else if (entry.isFile()) {
        try {
          const stat = await fsp.stat(path.join(dir, entry.name));
          results.push({ path: relPath, size: stat.size });
        } catch {
          results.push({ path: relPath, size: 0 });
        }
      }
    }
  }

  /**
   * Remove all backups for a game.
   */
  async removeGame(gameName) {
    const dir = this.gameDir(gameName);
    try {
      await fsp.rm(dir, { recursive: true });
    } catch (err) {
      if (err.code !== 'ENOENT') throw err;
    }
  }

  /**
   * Rename a game (moves the directory).
   */
  async renameGame(oldName, newName) {
    const oldDir = this.gameDir(oldName);
    const newDir = this.gameDir(newName);

    if (oldDir === newDir) {
      // Same sanitized name — just update the display name in metadata
      const meta = await this.getGameMeta(oldName);
      if (meta) {
        meta.displayName = newName;
        await this.updateGameMeta(oldName, meta);
      }
      return;
    }

    if (fs.existsSync(oldDir)) {
      await fsp.rename(oldDir, newDir);
    }

    // Update display name in metadata
    const meta = await this.getGameMeta(newName);
    if (meta) {
      meta.displayName = newName;
      const metaPath = path.join(newDir, '_game.json');
      await fsp.writeFile(metaPath, JSON.stringify(meta, null, 2));
    }
  }
}

module.exports = { GameBackup };
