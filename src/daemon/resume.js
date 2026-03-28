/**
 * Sync Resume State — persists download progress to disk.
 *
 * If sync is interrupted (crash, close laptop, network drop),
 * resumes from where it left off instead of starting from scratch.
 *
 * Stored as a simple JSON file per folder.
 */

const fs = require('fs');
const path = require('path');

class ResumeState {
  constructor(configDir) {
    this.configDir = configDir;
    this.stateDir = path.join(configDir, 'resume');
    fs.mkdirSync(this.stateDir, { recursive: true });
  }

  /**
   * Get resume state for a folder.
   * Returns { serverRootHash, completedFiles: Set, totalFiles, totalBytes, startedAt }
   * or null if no resume state.
   */
  get(folderName) {
    const filePath = this._path(folderName);
    try {
      if (!fs.existsSync(filePath)) return null;
      const raw = JSON.parse(fs.readFileSync(filePath, 'utf-8'));

      // Validate structure
      if (!raw.serverRootHash || !Array.isArray(raw.completedFiles)) return null;

      // Expire resume state after 24 hours (server index may have changed)
      if (raw.startedAt && Date.now() - raw.startedAt > 24 * 60 * 60 * 1000) {
        this.clear(folderName);
        return null;
      }

      return {
        serverRootHash: raw.serverRootHash,
        completedFiles: new Set(raw.completedFiles),
        totalFiles: raw.totalFiles || 0,
        totalBytes: raw.totalBytes || 0,
        startedAt: raw.startedAt || 0,
      };
    } catch {
      this.clear(folderName);
      return null;
    }
  }

  /**
   * Start a new sync session for a folder.
   */
  start(folderName, serverRootHash, totalFiles, totalBytes) {
    this._save(folderName, {
      serverRootHash,
      completedFiles: [],
      totalFiles,
      totalBytes,
      startedAt: Date.now(),
    });
  }

  /**
   * Mark a file as completed.
   */
  markCompleted(folderName, relPath) {
    const state = this.get(folderName);
    if (!state) return;

    state.completedFiles.add(relPath);
    this._save(folderName, {
      serverRootHash: state.serverRootHash,
      completedFiles: [...state.completedFiles],
      totalFiles: state.totalFiles,
      totalBytes: state.totalBytes,
      startedAt: state.startedAt,
    });
  }

  /**
   * Clear resume state (sync completed or expired).
   */
  clear(folderName) {
    try { fs.unlinkSync(this._path(folderName)); } catch {}
  }

  /**
   * Filter a download list to exclude already-completed files.
   */
  filterCompleted(folderName, serverRootHash, toDownload) {
    const state = this.get(folderName);
    if (!state || state.serverRootHash !== serverRootHash) {
      // Root hash changed — server index is different, can't resume
      return toDownload;
    }

    const remaining = toDownload.filter(f => !state.completedFiles.has(f.path));
    const skipped = toDownload.length - remaining.length;
    if (skipped > 0) {
      console.log(`Resuming sync: skipping ${skipped} already-synced files`);
    }
    return remaining;
  }

  _path(folderName) {
    return path.join(this.stateDir, `${folderName.replace(/[^a-zA-Z0-9]/g, '_')}.json`);
  }

  _save(folderName, data) {
    try {
      fs.writeFileSync(this._path(folderName), JSON.stringify(data), 'utf-8');
    } catch (err) {
      console.warn(`Resume state save failed: ${err.message}`);
    }
  }
}

module.exports = { ResumeState };
