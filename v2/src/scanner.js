'use strict';

const fsp = require('node:fs/promises');
const path = require('node:path');
const ignoreFactory = require('ignore');

// Conflict copies and our own temp files must never enter the index.
const DEFAULT_EXCLUDES = [
  'Thumbs.db',
  'desktop.ini',
  '~$*',
  '*.tmp',
  '.carbonsync.tmp-*',
  '*.conflict-*',
];

function buildIgnore(excludes) {
  return ignoreFactory().add(DEFAULT_EXCLUDES).add(excludes || []);
}

function toRel(rel) {
  return rel.split(path.sep).join('/');
}

async function statRoot(root) {
  try {
    const st = await fsp.stat(root);
    return st.isDirectory();
  } catch {
    return false;
  }
}

/**
 * Stat-only full walk. Returns {offline:true} if the root is missing — an
 * unmounted drive must NEVER be interpreted as "every file was deleted".
 *
 * The transient `seen` set is the only O(n) memory here (paths only, freed on
 * return); the index itself is never materialized.
 */
async function fullScan(root, db, ig) {
  if (!(await statRoot(root))) return { offline: true };

  const seen = new Set();
  let batch = [];
  let changed = 0;
  let scanned = 0;

  const stack = [''];
  while (stack.length) {
    const relDir = stack.pop();
    const absDir = relDir ? path.join(root, relDir) : root;
    let entries;
    try {
      entries = await fsp.readdir(absDir, { withFileTypes: true });
    } catch {
      continue; // permission/transient — backstopped by the next rescan
    }

    const statJobs = [];
    for (const e of entries) {
      const rel = relDir ? `${relDir}/${e.name}` : e.name;
      if (e.isDirectory()) {
        if (ig.ignores(rel + '/')) continue;
        stack.push(rel);
      } else if (e.isFile()) {
        if (ig.ignores(rel)) continue;
        statJobs.push(
          fsp.stat(path.join(absDir, e.name)).then(
            st => ({ path: rel, size: st.size, mtimeMs: Math.round(st.mtimeMs) }),
            () => null,
          ),
        );
      }
      // symlinks/other: skipped deliberately
    }

    for (const entry of await Promise.all(statJobs)) {
      if (!entry) continue;
      seen.add(entry.path);
      batch.push(entry);
      scanned++;
      if (batch.length >= 1000) {
        changed += db.applyLocal(batch);
        batch = [];
      }
    }
  }
  changed += db.applyLocal(batch);

  // Deletion pass: anything indexed-live but not seen on disk.
  const gone = [];
  for (const row of db.iterateLivePaths()) {
    if (!seen.has(row.path)) gone.push(row.path);
  }
  const removed = db.markDeleted(gone);

  return { changed, removed, scanned };
}

/**
 * Targeted rescan for watcher-reported paths (files or dirs; possibly gone).
 * Dir deletions tombstone the whole indexed subtree; dir creations walk it.
 */
async function rescanPaths(root, db, ig, relPaths) {
  if (!(await statRoot(root))) return { offline: true };

  const batch = [];
  const gone = [];

  for (const relRaw of relPaths) {
    const rel = toRel(relRaw);
    if (!rel || rel.startsWith('..')) continue;
    if (ig.ignores(rel)) continue;

    let st = null;
    try {
      st = await fsp.stat(path.join(root, rel));
    } catch { /* gone */ }

    if (!st) {
      if (db.get(rel) && !db.get(rel).deleted) gone.push(rel);
      for (const p2 of db.livePathsWithPrefix(rel + '/')) gone.push(p2);
    } else if (st.isFile()) {
      batch.push({ path: rel, size: st.size, mtimeMs: Math.round(st.mtimeMs) });
    } else if (st.isDirectory()) {
      const sub = await collectSubtree(root, rel, ig);
      batch.push(...sub);
    }
  }

  const changed = db.applyLocal(batch);
  const removed = db.markDeleted(gone);
  return { changed, removed };
}

async function collectSubtree(root, relStart, ig) {
  const out = [];
  const stack = [relStart];
  while (stack.length) {
    const relDir = stack.pop();
    let entries;
    try {
      entries = await fsp.readdir(path.join(root, relDir), { withFileTypes: true });
    } catch {
      continue;
    }
    for (const e of entries) {
      const rel = `${relDir}/${e.name}`;
      if (e.isDirectory()) {
        if (!ig.ignores(rel + '/')) stack.push(rel);
      } else if (e.isFile() && !ig.ignores(rel)) {
        try {
          const st = await fsp.stat(path.join(root, rel));
          out.push({ path: rel, size: st.size, mtimeMs: Math.round(st.mtimeMs) });
        } catch { /* raced away */ }
      }
    }
  }
  return out;
}

module.exports = { buildIgnore, fullScan, rescanPaths, DEFAULT_EXCLUDES };
