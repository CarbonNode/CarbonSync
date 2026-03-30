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
   * Copies all files from saveBase into current/ and creates a timestamped backup.
   *
   * @param {object} opts
   * @param {string} opts.gameId
   * @param {string} opts.gameName — display name
   * @param {string} opts.saveBase — absolute path to the game's save directory
   * @param {string[]} opts.changedPaths — specific files that changed (optional, backs up entire saveBase)
   * @param {string} opts.sourceDevice — device name that created this save
   * @returns {{ backupDir: string, fileCount: number, totalSize: number, timestamp: string }}
   */
  async backupGame({ gameId, gameName, saveBase, changedPaths, sourceDevice }) {
    const dir = this.gameDir(gameName);
    const currentDir = path.join(dir, 'current');
    const backupsDir = path.join(dir, 'backups');

    await fsp.mkdir(currentDir, { recursive: true });
    await fsp.mkdir(backupsDir, { recursive: true });

    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const backupDir = path.join(backupsDir, timestamp);
    await fsp.mkdir(backupDir, { recursive: true });

    // Copy entire saveBase to current/ and backup/
    let fileCount = 0;
    let totalSize = 0;

    try {
      const result = await this._copyDir(saveBase, currentDir, backupDir);
      fileCount = result.fileCount;
      totalSize = result.totalSize;
    } catch (err) {
      console.error(`Backup failed for ${gameName}: ${err.message}`);
      // Cleanup empty backup dir
      try { await fsp.rm(backupDir, { recursive: true }); } catch {}
      throw err;
    }

    if (fileCount === 0) {
      // No files to back up, remove empty dirs
      try { await fsp.rm(backupDir, { recursive: true }); } catch {}
      return null;
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

    // Update game metadata
    await this._updateGameMeta(dir, { gameId, gameName, saveBase });

    // Enforce retention
    await this._enforceRetention(backupsDir);

    return { backupDir, fileCount, totalSize, timestamp };
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

    await fsp.mkdir(saveBase, { recursive: true });
    const result = await this._restoreDir(currentDir, saveBase);
    return { restoredFiles: result.fileCount };
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
