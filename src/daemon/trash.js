/**
 * Trash Bucket — recoverable replacement for sync-driven file deletes.
 *
 * Instead of unlinking files when a peer's index says "I don't have these,"
 * move them to `<folderRoot>/.carbonsync-trash/YYYY-MM-DD/<relPath>`.
 * Pruned after `retentionDays` (default 7).
 *
 * The trash directory MUST be ignored by the sync layer (see syncignore.js)
 * so it never propagates to peers.
 */

const fsp = require('node:fs/promises');
const path = require('node:path');

const TRASH_DIRNAME = '.carbonsync-trash';

function todayUtc() {
  return new Date().toISOString().slice(0, 10); // YYYY-MM-DD
}

/**
 * Move a file under `folderRoot` to the trash bucket.
 * @param {string} folderRoot - Absolute path to the synced folder root.
 * @param {string} relPath    - Path relative to folderRoot.
 * @param {{ reason?: string }} [opts]
 * @returns {Promise<boolean>} true if moved, false if source missing.
 */
async function moveToTrash(folderRoot, relPath, _opts = {}) {
  const src = path.join(folderRoot, relPath);

  // Quick missing-source short-circuit so we don't create empty trash dirs.
  try {
    await fsp.access(src);
  } catch (err) {
    if (err.code === 'ENOENT') return false;
    throw err;
  }

  const trashRoot = path.join(folderRoot, TRASH_DIRNAME, todayUtc());
  const baseDest = path.join(trashRoot, relPath);

  await fsp.mkdir(path.dirname(baseDest), { recursive: true });

  // Pick a non-colliding destination by appending .1, .2, ...
  let dest = baseDest;
  let suffix = 0;
  // eslint-disable-next-line no-constant-condition
  while (true) {
    // If destination exists, bump suffix BEFORE attempting rename so we never
    // clobber a previous day's trash entry that landed at the same name.
    try {
      await fsp.access(dest);
      suffix += 1;
      dest = `${baseDest}.${suffix}`;
      continue;
    } catch (err) {
      if (err.code !== 'ENOENT') throw err;
    }

    try {
      await fsp.rename(src, dest);
      return true;
    } catch (err) {
      if (err.code === 'ENOENT') return false; // raced with another deleter
      if (err.code === 'EXDEV') {
        // Cross-device move: copy + unlink.
        await fsp.copyFile(src, dest);
        await fsp.unlink(src);
        return true;
      }
      if (err.code === 'EEXIST' || err.code === 'EPERM' || err.code === 'ENOTEMPTY') {
        suffix += 1;
        dest = `${baseDest}.${suffix}`;
        continue;
      }
      throw err;
    }
  }
}

/**
 * Remove date-named subdirs of the trash bucket older than `retentionDays`.
 * @param {string} folderRoot
 * @param {{ retentionDays?: number }} [opts]
 * @returns {Promise<number>} number of date-dirs removed.
 */
async function pruneTrash(folderRoot, opts = {}) {
  const retentionDays = opts.retentionDays ?? 7;
  const trashRoot = path.join(folderRoot, TRASH_DIRNAME);

  let entries;
  try {
    entries = await fsp.readdir(trashRoot, { withFileTypes: true });
  } catch (err) {
    if (err.code === 'ENOENT') return 0;
    throw err;
  }

  const cutoffMs = Date.now() - retentionDays * 24 * 60 * 60 * 1000;
  let removed = 0;

  for (const entry of entries) {
    if (!entry.isDirectory()) continue;
    const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(entry.name);
    if (!m) continue;
    const dirMs = Date.UTC(Number(m[1]), Number(m[2]) - 1, Number(m[3]));
    if (Number.isNaN(dirMs)) continue;
    if (dirMs >= cutoffMs) continue;

    try {
      await fsp.rm(path.join(trashRoot, entry.name), { recursive: true, force: true });
      removed += 1;
    } catch {
      // best-effort
    }
  }

  return removed;
}

module.exports = { moveToTrash, pruneTrash, TRASH_DIRNAME };
