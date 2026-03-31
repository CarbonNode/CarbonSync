/**
 * Sync Engine — the brain of CarbonSync.
 *
 * SERVER MODE (this PC):
 * - Maintains authoritative file index
 * - Watches folders for changes
 * - Serves file blocks to clients on request
 * - Pushes change notifications to subscribed clients
 *
 * This is the server-side engine. Clients have their own sync logic
 * that pulls from the server.
 */

const fs = require('fs');
const fsp = require('fs/promises');
const path = require('path');
const { EventEmitter } = require('events');
const watcher = require('@parcel/watcher');
const { Scanner } = require('./scanner');
const { readChunk, chunkFile } = require('./chunker');
const { MSG, SYNC_STATE } = require('../shared/protocol');

class SyncEngine extends EventEmitter {
  /**
   * @param {object} opts - { configDir, folders: [{ path, name, ignorePatterns }] }
   */
  constructor(opts) {
    super();
    this.configDir = opts.configDir;
    this.folders = new Map(); // name -> { path, scanner, watcher, state }
    this.state = SYNC_STATE.IDLE;

    // Initialize folders
    for (const folder of (opts.folders || [])) {
      this.addFolder(folder);
    }
  }

  /**
   * Add a folder to sync.
   */
  addFolder(folder) {
    const name = folder.name || path.basename(folder.path);
    const dbPath = path.join(this.configDir, `index_${name.replace(/[^a-zA-Z0-9]/g, '_')}.db`);

    fs.mkdirSync(this.configDir, { recursive: true });

    const scanner = new Scanner(folder.path, dbPath, {
      ignorePatterns: folder.ignorePatterns || [],
      onProgress: (progress) => {
        this.emit('scan-progress', { folder: name, ...progress });
      },
    });

    // Ensure unique name (append path hash if duplicate)
    let uniqueName = name;
    if (this.folders.has(name)) {
      const hash = require('crypto').createHash('md5').update(folder.path).digest('hex').substring(0, 6);
      uniqueName = `${name}_${hash}`;
    }

    this.folders.set(uniqueName, {
      path: folder.path,
      name: uniqueName,
      scanner,
      watcher: null,
      state: SYNC_STATE.IDLE,
    });
  }

  /**
   * Remove a folder from sync.
   */
  removeFolder(name) {
    const folder = this.folders.get(name);
    if (!folder) return;

    if (folder.watcher) {
      folder.watcher.then(sub => sub.unsubscribe()).catch(() => {});
    }
    folder.scanner.close();
    this.folders.delete(name);
  }

  /**
   * Start watching all folders and do initial scan.
   */
  async start() {
    console.log(`SyncEngine starting with ${this.folders.size} folder(s)`);

    for (const [name, folder] of this.folders) {
      // Initial full scan
      this.state = SYNC_STATE.SCANNING;
      this.emit('state', this.state);

      console.log(`Scanning folder: ${name} (${folder.path})`);
      const stats = await folder.scanner.fullScan();
      console.log(`Scan complete: ${name} — ${stats.added} added, ${stats.modified} modified, ${stats.deleted} deleted, ${stats.unchanged} unchanged, ${stats.errors} errors`);

      // Yield to event loop between folders so UI stays responsive
      await new Promise(r => setImmediate(r));

      // Start live watcher with fallback to polling
      await this._startWatcher(name, folder);

      await new Promise(r => setImmediate(r));
    }

    this.state = SYNC_STATE.IDLE;
    this.emit('state', this.state);
    this.emit('ready');
  }

  /**
   * Handle file system events from @parcel/watcher.
   */
  /**
   * Start native file watcher. Falls back to polling on failure.
   */
  async _startWatcher(name, folder) {
    try {
      folder.watcher = watcher.subscribe(folder.path, (err, events) => {
        if (err) {
          console.error(`Watcher error for ${name}: ${err.message}`);
          // Watcher died — fall back to polling
          this._startPolling(name, folder);
          return;
        }
        this._handleWatchEvents(name, folder, events);
      }, {
        ignore: ['.carbonsync', '*.tmp', '*.partial', 'Thumbs.db', 'desktop.ini'],
      });
      folder.watchMode = 'native';
      console.log(`Watching folder: ${name} (native)`);
    } catch (err) {
      console.error(`Native watcher failed for ${name}: ${err.message}`);
      this._startPolling(name, folder);
    }
  }

  /**
   * Fallback: poll for changes every 10 seconds.
   */
  _startPolling(name, folder) {
    if (folder.pollInterval) return; // Already polling

    console.warn(`Falling back to polling for ${name} (every 10s)`);
    folder.watchMode = 'polling';
    this.emit('watcher-fallback', { folder: name, reason: 'Native watcher failed' });

    const oldHash = folder.scanner.getRootHash();

    folder.pollInterval = setInterval(async () => {
      try {
        await folder.scanner.fullScan();
        const newHash = folder.scanner.getRootHash();
        if (newHash !== oldHash) {
          // Something changed — emit generic change event
          this.emit('changes', { folder: name, changes: [{ type: 'poll-rescan' }] });
        }
      } catch (err) {
        console.error(`Poll scan error for ${name}: ${err.message}`);
      }
    }, 10000);
  }

  _handleWatchEvents(folderName, folder, events) {
    // Debounce: collect events, process after 500ms of quiet
    if (!folder._pendingEvents) folder._pendingEvents = [];
    folder._pendingEvents.push(...events);

    if (folder._watchDebounce) clearTimeout(folder._watchDebounce);
    folder._watchDebounce = setTimeout(() => {
      const batch = folder._pendingEvents;
      folder._pendingEvents = [];
      this._processWatchBatch(folderName, folder, batch);
    }, 200);
  }

  async _processWatchBatch(folderName, folder, events) {
    const changes = [];

    for (const event of events) {
      const relPath = path.relative(folder.path, event.path).replace(/\\/g, '/');

      if (relPath.startsWith('.carbonsync/') || relPath.includes('/.carbonsync/')) continue;
      if (relPath.endsWith('.tmp') || relPath.endsWith('.partial')) continue;
      if (relPath.endsWith('Thumbs.db') || relPath.endsWith('desktop.ini')) continue;

      if (event.type === 'delete') {
        folder.scanner.removeFile(event.path);
        changes.push({ type: 'delete', path: relPath });
      } else {
        const entry = await folder.scanner.updateFile(event.path);
        if (entry) {
          changes.push({ type: event.type === 'create' ? 'add' : 'modify', ...entry });
        }
      }
    }

    if (changes.length > 0) {
      this.emit('changes', { folder: folderName, changes });
    }
  }

  /**
   * Get the full index for a folder.
   */
  getIndex(folderName) {
    const folder = this.folders.get(folderName);
    if (!folder) return null;
    return folder.scanner.getIndex();
  }

  /**
   * Get root hash for quick comparison.
   */
  getRootHash(folderName) {
    const folder = this.folders.get(folderName);
    if (!folder) return null;
    return folder.scanner.getRootHash();
  }

  /**
   * Get list of synced folder names.
   */
  getFolderNames() {
    return [...this.folders.keys()];
  }

  /**
   * Get folder info.
   */
  getFolderInfo(name) {
    const folder = this.folders.get(name);
    if (!folder) return null;
    const index = folder.scanner.getIndex();
    const totalSize = index.reduce((sum, f) => sum + f.size, 0);
    return {
      name: folder.name,
      path: folder.path,
      fileCount: index.length,
      totalSize,
      rootHash: folder.scanner.getRootHash(),
    };
  }

  /**
   * Read a file from a synced folder (for serving to clients).
   */
  async readFile(folderName, relPath) {
    const folder = this.folders.get(folderName);
    if (!folder) throw new Error(`Unknown folder: ${folderName}`);

    const absPath = path.join(folder.path, relPath);

    // Security: ensure path doesn't escape folder
    const resolved = path.resolve(absPath);
    if (!resolved.startsWith(folder.path)) {
      throw new Error('Path traversal detected');
    }

    return fsp.readFile(absPath);
  }

  /**
   * Read a chunk of a file (for delta transfer).
   */
  async readFileChunk(folderName, relPath, offset, length) {
    const folder = this.folders.get(folderName);
    if (!folder) throw new Error(`Unknown folder: ${folderName}`);

    const absPath = path.join(folder.path, relPath);
    const resolved = path.resolve(absPath);
    if (!resolved.startsWith(folder.path)) {
      throw new Error('Path traversal detected');
    }

    return readChunk(absPath, offset, length);
  }

  /**
   * Compare server index with client index and return diff.
   * Returns { toDownload, toDelete, toCopy, dirs } from client's perspective.
   *
   * toCopy = move/rename detection: if client has a file with the same hash
   * at a different path, the client can copy locally instead of re-downloading.
   */
  computeDiff(folderName, clientIndex) {
    const folder = this.folders.get(folderName);
    if (!folder) return null;

    const serverIndex = folder.scanner.getIndexMap();
    const clientMap = new Map();
    const clientHashMap = new Map(); // hash -> [paths] for move detection

    for (const entry of clientIndex) {
      clientMap.set(entry.path, entry);
      if (!clientHashMap.has(entry.hash)) clientHashMap.set(entry.hash, []);
      clientHashMap.get(entry.hash).push(entry.path);
    }

    const toDownload = [];
    const toCopy = [];  // { from, to, hash } — local copy on client (no network)
    const toDelete = [];

    for (const [relPath, serverEntry] of serverIndex) {
      const clientEntry = clientMap.get(relPath);
      if (clientEntry && clientEntry.hash === serverEntry.hash) continue; // In sync

      // File needs updating. Check if client has it at a different path (rename/move)
      const existingPaths = clientHashMap.get(serverEntry.hash);
      if (existingPaths && existingPaths.length > 0) {
        // Client already has this content — just copy locally
        toCopy.push({
          from: existingPaths[0],
          to: relPath,
          hash: serverEntry.hash,
          size: serverEntry.size,
          mtime_ms: serverEntry.mtime_ms,
        });
      } else {
        toDownload.push({
          path: relPath,
          size: serverEntry.size,
          hash: serverEntry.hash,
          mtime_ms: serverEntry.mtime_ms,
        });
      }
    }

    for (const [relPath] of clientMap) {
      if (!serverIndex.has(relPath)) toDelete.push(relPath);
    }

    // Include directory list for empty dir sync
    const dirs = folder.scanner.getDirs();

    return { toDownload, toCopy, toDelete, dirs };
  }

  /**
   * Force rescan a folder.
   */
  async rescan(folderName) {
    const folder = this.folders.get(folderName);
    if (!folder) return null;
    return folder.scanner.fullScan();
  }

  /**
   * Stop all watchers and close databases.
   */
  async stop() {
    for (const [name, folder] of this.folders) {
      if (folder._watchDebounce) clearTimeout(folder._watchDebounce);
      if (folder.pollInterval) {
        clearInterval(folder.pollInterval);
        folder.pollInterval = null;
      }
      if (folder.watcher) {
        try {
          const sub = await folder.watcher;
          await sub.unsubscribe();
        } catch {}
      }
      folder.scanner.close();
    }
    this.folders.clear();
    console.log('SyncEngine stopped');
  }
}

module.exports = { SyncEngine };
