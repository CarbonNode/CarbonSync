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

/**
 * Phase 9 P0: prune stale conflict sidecars.
 *
 * Phase 3 mtime-conflict sidecars land at `<path>.conflict.<peer>.<ts>`
 * and Phase 6 shrink-blocked sidecars at `<path>.shrink-blocked.<peer>.<ts>`.
 * They were recoverable-by-design with no expiry, which means a folder that
 * sees regular conflicts (e.g. two clocks out of sync) accumulates them
 * forever. Move anything older than `retentionDays` to the trash bucket —
 * not unlink, because they still represent user content.
 *
 * The timestamp is the final `.<number>` segment of the filename; we only
 * accept ASCII-digit segments so a user-named `foo.conflict.peer.v2` isn't
 * misinterpreted as ts=2.
 *
 * Walking is capped at `maxFiles` entries so a pathological folder (e.g.
 * node_modules under a sync root) can't block the 6h pruner indefinitely.
 * Conflicts buried deeper than the walk gets this pass are picked up on
 * subsequent runs.
 *
 * @param {string} folderRoot
 * @param {{ retentionDays?: number, maxFiles?: number }} [opts]
 * @returns {Promise<number>} number of files trashed.
 */
const CONFLICT_PATTERNS = [
  /\.conflict\.[^/\\]+\.(\d+)$/,
  /\.shrink-blocked\.[^/\\]+\.(\d+)$/,
];

function _extractConflictTimestamp(name) {
  for (const re of CONFLICT_PATTERNS) {
    const m = re.exec(name);
    if (m) {
      const ts = Number(m[1]);
      if (Number.isFinite(ts) && ts > 0) return ts;
    }
  }
  return null;
}

async function pruneConflicts(folderRoot, opts = {}) {
  const retentionDays = opts.retentionDays ?? 30;
  const maxFiles = opts.maxFiles ?? 100_000;

  if (!folderRoot) return 0;
  try {
    await fsp.access(folderRoot);
  } catch (err) {
    if (err && err.code === 'ENOENT') return 0;
    throw err;
  }

  const cutoffMs = Date.now() - retentionDays * 24 * 60 * 60 * 1000;
  let trashed = 0;
  let seen = 0;

  // Iterative walk so we can respect maxFiles and skip the trash bucket
  // cheaply without recursing into it.
  const stack = [folderRoot];
  while (stack.length > 0 && seen < maxFiles) {
    const dir = stack.pop();
    let entries;
    try {
      entries = await fsp.readdir(dir, { withFileTypes: true });
    } catch (err) {
      if (err && err.code === 'ENOENT') continue;
      if (err && (err.code === 'EACCES' || err.code === 'EPERM')) continue;
      throw err;
    }

    for (const entry of entries) {
      if (seen >= maxFiles) break;
      // Never descend into (or touch) the trash bucket itself.
      if (entry.name === TRASH_DIRNAME) continue;
      // Skip symlinks — we don't want to follow a loop out of folderRoot.
      if (entry.isSymbolicLink && entry.isSymbolicLink()) continue;

      const full = path.join(dir, entry.name);
      if (entry.isDirectory()) {
        stack.push(full);
        continue;
      }
      if (!entry.isFile()) continue;

      seen += 1;
      const ts = _extractConflictTimestamp(entry.name);
      if (ts == null) continue;
      if (ts >= cutoffMs) continue;

      const rel = path.relative(folderRoot, full);
      try {
        const moved = await moveToTrash(folderRoot, rel);
        if (moved) trashed += 1;
      } catch (err) {
        // Best-effort prune — surface the error but keep walking.
        console.warn(`pruneConflicts: failed to trash ${rel}: ${err.message}`);
      }
    }
  }

  return trashed;
}

module.exports = { moveToTrash, pruneTrash, pruneConflicts, TRASH_DIRNAME };
