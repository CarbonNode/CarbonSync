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
const { moveToTrash, pruneTrash } = require('./trash');

// Floor for gameSaveMaxVersions — refuse to go below this, no matter what
// the user configures, to avoid evicting normal backups the moment a
// pre-restore snapshot is created.
const MIN_MAX_VERSIONS = 3;

// Keep at most this many pre-restore snapshots regardless of the configured
// regular retention — they're "oh shit" safety nets, not versioned history.
const PRE_RESTORE_RETENTION = 3;

const PRE_RESTORE_PREFIX = 'pre-restore-';

class GameBackup {
  /**
   * @param {object} opts
   * @param {string} opts.configDir — ~/.carbonsync
   * @param {import('./config').Config} opts.config
   */
  constructor({ configDir, config, gameSavesDir }) {
    this.configDir = configDir;
    this.config = config;
    // Allow overriding for tests; default to <configDir>/game-saves.
    this.gameSavesDir = gameSavesDir || path.join(configDir, 'game-saves');
    fs.mkdirSync(this.gameSavesDir, { recursive: true });

    // Kick off a trash prune at startup and every 6h (matches the main
    // daemon's interval). game-saves is an internal folder that isn't in
    // config.folders, so the daemon-level _pruneAllTrash doesn't touch it.
    this._pruneTrash().catch(() => {});
    this._trashPruneInterval = setInterval(() => {
      this._pruneTrash().catch(() => {});
    }, 6 * 60 * 60 * 1000);
    // Don't keep the process alive just for trash pruning.
    if (typeof this._trashPruneInterval.unref === 'function') {
      this._trashPruneInterval.unref();
    }
  }

  /**
   * Stop background intervals. Called on daemon shutdown.
   */
  stop() {
    if (this._trashPruneInterval) {
      clearInterval(this._trashPruneInterval);
      this._trashPruneInterval = null;
    }
  }

  async _pruneTrash() {
    try {
      const removed = await pruneTrash(this.gameSavesDir, { retentionDays: 7 });
      if (removed > 0) {
        console.log(`Pruned ${removed} expired trash dir(s) from game-saves`);
      }
    } catch (err) {
      if (err && err.code !== 'ENOENT') {
        console.warn(`game-saves trash prune failed: ${err.message}`);
      }
    }
  }

  /**
   * Assert that `dir` is a safe target for rm-rf:
   *   - dir !== root
   *   - dir is strictly underneath root (no symlink/.. escape)
   *   - resolved relative path has no ".." segments
   * Throws Error on any violation.
   */
  _assertSafePath(dir, root) {
    if (typeof dir !== 'string' || !dir) {
      throw new Error('unsafe path: empty');
    }
    if (typeof root !== 'string' || !root) {
      throw new Error('unsafe path: empty root');
    }
    const absDir = path.resolve(dir);
    const absRoot = path.resolve(root);
    if (absDir === absRoot) {
      throw new Error(`unsafe path: would target root (${absRoot})`);
    }
    const prefix = absRoot.endsWith(path.sep) ? absRoot : absRoot + path.sep;
    if (!absDir.startsWith(prefix)) {
      throw new Error(`unsafe path: ${absDir} not under ${absRoot}`);
    }
    const rel = path.relative(absRoot, absDir);
    if (!rel || rel.startsWith('..') || rel.split(/[\\/]+/).includes('..')) {
      throw new Error(`unsafe path: relative ${rel} escapes ${absRoot}`);
    }
  }

  /**
   * Move a path (file OR directory) under gameSavesDir into the trash bucket.
   * Safe replacement for fsp.rm / fsp.unlink in destructive cleanup code.
   * Returns true on success, false if skipped/failed.
   */
  async _trashWithinGameSaves(fullPath) {
    try {
      this._assertSafePath(fullPath, this.gameSavesDir);
    } catch (err) {
      console.error(`Refusing trash: ${err.message}`);
      return false;
    }
    const rel = path.relative(this.gameSavesDir, fullPath);
    try {
      return await moveToTrash(this.gameSavesDir, rel);
    } catch (err) {
      console.warn(`moveToTrash failed for ${rel}: ${err.message}`);
      return false;
    }
  }

  /**
   * Get the managed directory for a game.
   * Uses gameId for stable folder names (rename-safe).
   * Falls back to sanitized display name for backwards compat.
   */
  gameDir(gameName, gameId) {
    // Prefer ID-based folder (stable across renames)
    if (gameId) {
      const idDir = path.join(this.gameSavesDir, gameId);
      if (fs.existsSync(idDir)) return idDir;
    }

    // Check legacy name-based folder
    const safe = gameName.replace(/[<>:"/\\|?*]/g, '_').trim();
    const nameDir = path.join(this.gameSavesDir, safe);

    // Migrate: if name-based exists but ID-based doesn't, rename it
    if (gameId && fs.existsSync(nameDir)) {
      const idDir = path.join(this.gameSavesDir, gameId);
      try {
        fs.renameSync(nameDir, idDir);
        console.log(`Migrated game folder: ${safe} → ${gameId}`);
        return idDir;
      } catch {
        return nameDir;
      }
    }

    // New game: use ID if available, else name
    if (gameId) return path.join(this.gameSavesDir, gameId);
    return nameDir;
  }

  /**
   * Convenience: resolve gameDir from a game entry object.
   */
  gameDirFromEntry(entry) {
    return this.gameDir(entry.displayName || entry.name || entry.gameName || '', entry.id || entry.gameId || '');
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
  async backupGame({ gameId, gameName, saveBase, rootKey, relPath, changedPaths, sourceDevice, force }) {
    const dir = this.gameDir(gameName, gameId);
    const currentDir = path.join(dir, 'current');
    const backupsDir = path.join(dir, 'backups');

    await fsp.mkdir(currentDir, { recursive: true });
    await fsp.mkdir(backupsDir, { recursive: true });

    // Bail if saveBase doesn't exist (game not installed on this PC)
    if (!fs.existsSync(saveBase)) return null;

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
    await this._updateGameMeta(dir, { gameId, gameName, saveBase, rootKey, relPath });

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

      // Skip junk files — not needed for save restore
      const n = entry.name;
      const nl = n.toLowerCase();
      if (n.startsWith('_meta') || nl.endsWith('.tmp') || nl.endsWith('.partial')) continue;
      if (nl === 'player.log' || nl === 'player-prev.log' || nl === 'output_log.txt') continue;
      if (nl === 'steam_autocloud.vdf' || nl === 'eventcache') continue;
      if (nl === 'crash.dmp' || nl === 'error.log' || nl === 'debug.log') continue;
      if (n === 'Unity' || n === 'ShaderCache') {
        // Skip Unity analytics/shader dirs entirely
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
  async _updateGameMeta(gameDir, { gameId, gameName, saveBase, rootKey, relPath }) {
    const metaPath = path.join(gameDir, '_game.json');
    let meta = {};
    try {
      meta = JSON.parse(await fsp.readFile(metaPath, 'utf-8'));
    } catch {}

    meta.id = gameId;
    meta.name = gameName;
    meta.displayName = meta.displayName || gameName;
    meta.saveBase = saveBase;
    if (rootKey) meta.rootKey = rootKey;
    if (relPath) meta.relPath = relPath;
    meta.lastBackup = new Date().toISOString();
    meta.excludes = meta.excludes || [];
    meta.enabled = meta.enabled !== undefined ? meta.enabled : true;

    await fsp.writeFile(metaPath, JSON.stringify(meta, null, 2));
  }

  /**
   * Enforce retention policy.
   *
   * - Regular (timestamp-named) backups: keep the newest `maxVersions`
   *   (by metadata.timestamp, falling back to directory name). Clamp
   *   maxVersions to >= MIN_MAX_VERSIONS (3) so misconfig can't leave
   *   a user with just one backup.
   * - Pre-restore snapshots (`pre-restore-*`): tracked separately and
   *   capped at PRE_RESTORE_RETENTION (3). They do NOT count against
   *   the regular retention and cannot evict regular backups.
   * - Always protect the oldest regular backup that's > 24h old.
   * - Deletes route through moveToTrash for 7-day recovery.
   */
  async _enforceRetention(backupsDir) {
    const configured = this.config.data.settings?.gameSaveMaxVersions;
    let maxVersions = Number.isFinite(configured) && configured > 0 ? configured : 10;
    if (maxVersions < MIN_MAX_VERSIONS) maxVersions = MIN_MAX_VERSIONS;

    let entries;
    try {
      entries = await fsp.readdir(backupsDir, { withFileTypes: true });
    } catch { return; }

    // Gather { name, timestamp, isPreRestore } for each backup dir.
    const all = [];
    for (const e of entries) {
      if (!e.isDirectory() || e.name.startsWith('.')) continue;
      const name = e.name;
      const isPreRestore = name.startsWith(PRE_RESTORE_PREFIX);

      // Prefer metadata.timestamp (real wall-clock, ISO string).
      // Fall back to directory name (which also sorts correctly for
      // pure-timestamp names, just not across pre-restore/regular).
      let timestamp = null;
      try {
        const metaPath = path.join(backupsDir, name, '_meta.json');
        const meta = JSON.parse(await fsp.readFile(metaPath, 'utf-8'));
        if (meta && meta.timestamp) timestamp = String(meta.timestamp);
      } catch {}
      if (!timestamp) {
        console.warn(`Backup ${name} missing metadata.timestamp; falling back to directory name for retention sort`);
        // Strip the pre-restore- prefix if present so both groups sort
        // on comparable ISO-ish strings.
        timestamp = isPreRestore ? name.slice(PRE_RESTORE_PREFIX.length) : name;
      }

      all.push({ name, timestamp, isPreRestore });
    }

    // Partition. Regular backups retention is independent from pre-restore.
    const regular = all.filter(b => !b.isPreRestore);
    const preRestore = all.filter(b => b.isPreRestore);

    // Sort each group oldest → newest by timestamp string.
    const byTs = (a, b) => (a.timestamp < b.timestamp ? -1 : a.timestamp > b.timestamp ? 1 : 0);
    regular.sort(byTs);
    preRestore.sort(byTs);

    const toDelete = [];

    // Regular retention — keep newest `maxVersions`.
    if (regular.length > maxVersions) {
      const surplus = regular.slice(0, regular.length - maxVersions);

      // Protect the oldest regular backup that's > 24h old (only one).
      const oneDayAgo = Date.now() - 24 * 60 * 60 * 1000;
      let protectedDir = null;
      for (const b of regular) {
        const t = Date.parse(b.timestamp);
        if (Number.isFinite(t) && t < oneDayAgo) {
          protectedDir = b.name;
          break; // oldest such one
        }
      }

      for (const b of surplus) {
        if (b.name === protectedDir) continue;
        toDelete.push(b.name);
      }
    }

    // Pre-restore retention — keep newest PRE_RESTORE_RETENTION.
    if (preRestore.length > PRE_RESTORE_RETENTION) {
      const surplus = preRestore.slice(0, preRestore.length - PRE_RESTORE_RETENTION);
      for (const b of surplus) toDelete.push(b.name);
    }

    for (const name of toDelete) {
      const full = path.join(backupsDir, name);
      const moved = await this._trashWithinGameSaves(full);
      if (!moved) {
        console.error(`Failed to retire old backup ${name} to trash`);
      }
    }
  }

  /**
   * Get backup history for a game.
   * Returns sorted array (newest first) of { timestamp, fileCount, totalSize, sourceDevice, dir }.
   */
  async getHistory(gameName, gameId) {
    const dir = this.gameDir(gameName, gameId);
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
        // Backup without metadata — count files directly
        const backupPath = path.join(backupsDir, entry.name);
        const sizes = await this._getFileSizes(backupPath);
        let totalSize = 0;
        for (const s of sizes.values()) totalSize += s;
        history.push({
          timestamp: entry.name,
          fileCount: sizes.size,
          totalSize,
          sourceDevice: os.hostname(),
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
  async restoreSave(gameName, backupTimestamp, gameId) {
    const dir = this.gameDir(gameName, gameId);
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
  async restoreCurrent(gameName, targetPath, gameId) {
    const dir = this.gameDir(gameName, gameId);
    const metaPath = path.join(dir, '_game.json');

    let gameMeta;
    try {
      gameMeta = JSON.parse(await fsp.readFile(metaPath, 'utf-8'));
    } catch {
      throw new Error(`Game metadata not found for ${gameName}`);
    }

    // Use targetPath override (for cross-PC restore) or fall back to stored path
    const saveBase = targetPath || gameMeta.saveBase;
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
  /**
   * Clean junk files from each game's current/ directory.
   *
   * Scope (v2.7.5+): ONLY `<game>/current/`. We never touch
   * `backups/<timestamp>/` because snapshots are immutable: a user who
   * restores a 3-month-old backup must get exactly what was there when
   * it was snapshotted. And names like "Unity" aren't reliably junk —
   * many games (Unity Analytics, Unity Cloud Save) store real save data
   * under directories literally called Unity/. Rewriting historical
   * snapshots silently wiped that data.
   *
   * All removals go to `<gameSavesDir>/.carbonsync-trash/` for 7-day
   * recovery, not straight to unlink/rm.
   */
  async cleanBackups() {
    const junkFiles = new Set(['player.log', 'player-prev.log', 'output_log.txt',
      'steam_autocloud.vdf', 'eventcache', 'crash.dmp', 'error.log', 'debug.log']);
    const junkDirs = new Set(['unity', 'shadercache']);
    let removed = 0;

    const walk = async (dir) => {
      let entries;
      try { entries = await fsp.readdir(dir, { withFileTypes: true }); } catch { return; }
      for (const entry of entries) {
        const full = path.join(dir, entry.name);
        const nl = entry.name.toLowerCase();
        if (entry.isDirectory()) {
          if (junkDirs.has(nl)) {
            if (await this._trashWithinGameSaves(full)) removed++;
          } else {
            await walk(full);
          }
        } else if (junkFiles.has(nl)) {
          if (await this._trashWithinGameSaves(full)) removed++;
        }
      }
    };

    // Iterate only per-game current/ dirs. Skip hidden dirs (incl. the
    // trash bucket itself) and anything that isn't a game directory.
    let games;
    try {
      games = await fsp.readdir(this.gameSavesDir, { withFileTypes: true });
    } catch { return { removed }; }

    for (const g of games) {
      if (!g.isDirectory()) continue;
      if (g.name.startsWith('.') || g.name.startsWith('_')) continue;
      const currentDir = path.join(this.gameSavesDir, g.name, 'current');
      // Only walk if current/ exists — don't create it.
      try {
        const st = await fsp.stat(currentDir);
        if (!st.isDirectory()) continue;
      } catch { continue; }
      await walk(currentDir);
    }

    console.log(`Cleaned ${removed} junk files/dirs from current/ (backups/ untouched)`);
    return { removed };
  }

  async _copyDirSingle(src, dest) {
    let fileCount = 0;
    let totalSize = 0;

    let entries;
    try {
      entries = await fsp.readdir(src, { withFileTypes: true });
    } catch { return { fileCount, totalSize }; }

    for (const entry of entries) {
      const nl = entry.name.toLowerCase();
      if (entry.name.startsWith('_meta') || nl.endsWith('.tmp') || nl.endsWith('.partial')) continue;
      if (nl === 'player.log' || nl === 'player-prev.log' || nl === 'output_log.txt') continue;
      if (nl === 'steam_autocloud.vdf' || nl === 'eventcache') continue;
      if (nl === 'crash.dmp' || nl === 'error.log' || nl === 'debug.log') continue;
      if (entry.name === 'Unity' || entry.name === 'ShaderCache') continue;

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
  async getGameMeta(gameName, gameId) {
    const metaPath = path.join(this.gameDir(gameName, gameId), '_game.json');
    try {
      return JSON.parse(await fsp.readFile(metaPath, 'utf-8'));
    } catch {
      return null;
    }
  }

  /**
   * Update game metadata (for rename, excludes, etc.).
   */
  async updateGameMeta(gameName, updates, gameId) {
    const dir = this.gameDir(gameName, gameId);
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
  async listBackupFiles(gameName, backupDirName, gameId) {
    const dir = this.gameDir(gameName, gameId);
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
   *
   * Hardened (v2.7.5): verifies the resolved directory is strictly
   * under gameSavesDir before rm. Empty name+id, path traversal via
   * "../" in the name, or a resolved path that somehow lands on the
   * game-saves root will be refused — silently rm-rf'ing every backed
   * up game is not a recoverable operation.
   */
  async removeGame(gameName, gameId) {
    // Reject empty inputs outright — gameDir() would fall back to a
    // sanitized empty string, producing gameSavesDir itself.
    const hasId = typeof gameId === 'string' && gameId.trim() !== '';
    const hasName = typeof gameName === 'string' && gameName.trim() !== '';
    if (!hasId && !hasName) {
      const msg = 'removeGame: refusing empty gameName AND empty gameId (would target gameSavesDir root)';
      console.error(msg);
      throw new Error(msg);
    }

    const dir = this.gameDir(gameName, gameId);

    try {
      this._assertSafePath(dir, this.gameSavesDir);
    } catch (err) {
      const msg = `removeGame: ${err.message}`;
      console.error(msg);
      throw new Error(msg);
    }

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
