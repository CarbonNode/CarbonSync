/**
 * Phase 9 — defense-in-depth regression tests.
 *
 * Covers:
 *   - classifyDeletion: preserve when EITHER hash is a `fast:*` fingerprint
 *   - Scanner.recordPeerKnown / recordPeerKnownBulk: filter fast: entries
 *   - pruneConflicts: trash old .conflict.*.* and .shrink-blocked.*.* files,
 *     keep recent ones, no-op on missing folder
 *   - GameBackup._assertSafeSaveBase: blocks root/shallow, allows homedir
 *     subpaths and known-marker paths; _assertSafePath with array roots
 */

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const fsp = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');

const { classifyDeletion } = require('../src/daemon/stale-peer-guard');
const { Scanner } = require('../src/daemon/scanner');
const { pruneConflicts, moveToTrash, TRASH_DIRNAME } = require('../src/daemon/trash');
const { GameBackup } = require('../src/daemon/game-backup');

async function makeTempFolder(tag = 'carbonsync-phase9-') {
  return fsp.mkdtemp(path.join(os.tmpdir(), tag));
}

async function rimraf(dir) {
  try { await fsp.rm(dir, { recursive: true, force: true }); } catch {}
}

async function makeScannerIn(root) {
  const folder = path.join(root, 'folder');
  await fsp.mkdir(folder, { recursive: true });
  const dbPath = path.join(root, 'scanner.db');
  const s = new Scanner(folder, dbPath);
  return { scanner: s, folder, dbPath };
}

// ---------------------------------------------------------------------
// classifyDeletion — fast: hash handling
// ---------------------------------------------------------------------

test('classifyDeletion preserves on fast: local hash', () => {
  const r = classifyDeletion({
    peerKnown: { hash: 'abc123', synced_at: 1 },
    currentLocalHash: 'fast:1234:5678',
  });
  assert.equal(r.action, 'preserve');
  assert.equal(r.reason, 'fast-hash-unreliable');
});

test('classifyDeletion preserves on fast: peer hash', () => {
  const r = classifyDeletion({
    peerKnown: { hash: 'fast:1234:5678', synced_at: 1 },
    currentLocalHash: 'abc123',
  });
  assert.equal(r.action, 'preserve');
  assert.equal(r.reason, 'fast-hash-unreliable');
});

test('classifyDeletion preserves when both are fast:', () => {
  const r = classifyDeletion({
    peerKnown: { hash: 'fast:1234:5678', synced_at: 1 },
    currentLocalHash: 'fast:1234:5678',
  });
  assert.equal(r.action, 'preserve');
  assert.equal(r.reason, 'fast-hash-unreliable');
});

test('classifyDeletion unaffected when neither is fast:', () => {
  // Baseline: peer-explicit still fires, local-modified still fires.
  const same = classifyDeletion({
    peerKnown: { hash: 'real-sha-xxxx', synced_at: 1 },
    currentLocalHash: 'real-sha-xxxx',
  });
  assert.equal(same.action, 'delete');
  assert.equal(same.reason, 'peer-explicit');

  const diff = classifyDeletion({
    peerKnown: { hash: 'real-sha-yyyy', synced_at: 1 },
    currentLocalHash: 'real-sha-zzzz',
  });
  assert.equal(diff.action, 'preserve');
  assert.equal(diff.reason, 'local-modified-since-sync');
});

test('classifyDeletion: already-gone still wins over fast: check', () => {
  // If the file is gone locally there's nothing to lose — let the caller
  // clear state even if peerKnown is a fast: entry.
  const r = classifyDeletion({
    peerKnown: { hash: 'fast:1:2', synced_at: 1 },
    currentLocalHash: null,
  });
  assert.equal(r.action, 'delete');
  assert.equal(r.reason, 'already-gone');
});

// ---------------------------------------------------------------------
// Scanner.recordPeerKnown / recordPeerKnownBulk — fast: filtering
// ---------------------------------------------------------------------

test('Scanner.recordPeerKnown rejects fast: hash', async () => {
  const root = await makeTempFolder('cs-p9-scanner-');
  try {
    const { scanner } = await makeScannerIn(root);
    scanner.recordPeerKnown('peer:A', 'foo.bin', 'fast:1:2');
    assert.equal(scanner.getPeerKnown('peer:A', 'foo.bin'), null,
      'fast: hash must not be stored');

    // Real hash still stores
    scanner.recordPeerKnown('peer:A', 'foo.bin', 'a'.repeat(64));
    const row = scanner.getPeerKnown('peer:A', 'foo.bin');
    assert.ok(row, 'real hash should store');
    assert.equal(row.hash, 'a'.repeat(64));
    scanner.close();
  } finally {
    await rimraf(root);
  }
});

test('Scanner.recordPeerKnownBulk filters fast: entries', async () => {
  const root = await makeTempFolder('cs-p9-scanner-bulk-');
  try {
    const { scanner } = await makeScannerIn(root);
    scanner.recordPeerKnownBulk('peer:B', [
      { path: 'a.bin', hash: 'fast:10:100' },       // rejected
      { path: 'b.bin', hash: 'b'.repeat(64) },      // kept
      { path: 'c.bin', hash: 'fast:20:200' },       // rejected
      { path: 'd.bin', hash: 'd'.repeat(64) },      // kept
      { path: '',      hash: 'e'.repeat(64) },      // rejected (empty path)
      { path: 'f.bin', hash: '' },                  // rejected (empty hash)
    ]);

    assert.equal(scanner.getPeerKnown('peer:B', 'a.bin'), null);
    assert.equal(scanner.getPeerKnown('peer:B', 'c.bin'), null);
    assert.ok(scanner.getPeerKnown('peer:B', 'b.bin'));
    assert.ok(scanner.getPeerKnown('peer:B', 'd.bin'));

    const map = scanner.getPeerKnownMap('peer:B');
    assert.equal(map.size, 2, 'only two real entries should persist');
    scanner.close();
  } finally {
    await rimraf(root);
  }
});

test('Scanner.recordPeerKnownBulk with only fast: entries is a no-op', async () => {
  const root = await makeTempFolder('cs-p9-scanner-noop-');
  try {
    const { scanner } = await makeScannerIn(root);
    scanner.recordPeerKnownBulk('peer:C', [
      { path: 'a.bin', hash: 'fast:10:100' },
      { path: 'b.bin', hash: 'fast:20:200' },
    ]);
    assert.equal(scanner.getPeerKnownMap('peer:C').size, 0);
    scanner.close();
  } finally {
    await rimraf(root);
  }
});

// ---------------------------------------------------------------------
// pruneConflicts — conflict-sidecar retention
// ---------------------------------------------------------------------

test('pruneConflicts removes old .conflict.*.*', async () => {
  const root = await makeTempFolder('cs-p9-prune-old-');
  try {
    const oldTs = Date.now() - 60 * 24 * 60 * 60 * 1000; // 60 days ago
    const rel = `saves/foo.dat.conflict.peerA.${oldTs}`;
    const abs = path.join(root, rel);
    await fsp.mkdir(path.dirname(abs), { recursive: true });
    await fsp.writeFile(abs, 'stale-conflict');

    const trashed = await pruneConflicts(root, { retentionDays: 30 });
    assert.equal(trashed, 1, 'one conflict should have been trashed');
    assert.equal(fs.existsSync(abs), false, 'original should be gone');

    // Confirm the trashed file landed in the trash bucket (under today's
    // date subdir) — we don't pin the exact date to avoid UTC flakiness,
    // just confirm it's under .carbonsync-trash.
    const trashRoot = path.join(root, TRASH_DIRNAME);
    assert.equal(fs.existsSync(trashRoot), true, 'trash bucket should exist');
  } finally {
    await rimraf(root);
  }
});

test('pruneConflicts keeps recent .conflict.*.*', async () => {
  const root = await makeTempFolder('cs-p9-prune-recent-');
  try {
    const recentTs = Date.now() - 1 * 24 * 60 * 60 * 1000; // 1 day ago
    const rel = `saves/foo.dat.conflict.peerA.${recentTs}`;
    const abs = path.join(root, rel);
    await fsp.mkdir(path.dirname(abs), { recursive: true });
    await fsp.writeFile(abs, 'fresh-conflict');

    const trashed = await pruneConflicts(root, { retentionDays: 30 });
    assert.equal(trashed, 0);
    assert.equal(fs.existsSync(abs), true, 'recent conflict must be kept');
  } finally {
    await rimraf(root);
  }
});

test('pruneConflicts handles .shrink-blocked.*.*', async () => {
  const root = await makeTempFolder('cs-p9-prune-shrink-');
  try {
    const oldTs = Date.now() - 90 * 24 * 60 * 60 * 1000;
    const rel = `saves/foo.dat.shrink-blocked.peerA.${oldTs}`;
    const abs = path.join(root, rel);
    await fsp.mkdir(path.dirname(abs), { recursive: true });
    await fsp.writeFile(abs, 'old-shrink-block');

    const trashed = await pruneConflicts(root, { retentionDays: 30 });
    assert.equal(trashed, 1);
    assert.equal(fs.existsSync(abs), false);
  } finally {
    await rimraf(root);
  }
});

test('pruneConflicts safe on missing folder', async () => {
  const bogus = path.join(os.tmpdir(), 'cs-p9-not-a-real-folder-' + Date.now());
  const trashed = await pruneConflicts(bogus, { retentionDays: 30 });
  assert.equal(trashed, 0);
});

test('pruneConflicts ignores non-conflict files', async () => {
  const root = await makeTempFolder('cs-p9-prune-ignore-');
  try {
    await fsp.mkdir(path.join(root, 'a'), { recursive: true });
    await fsp.writeFile(path.join(root, 'a', 'regular.dat'), 'x');
    await fsp.writeFile(path.join(root, 'a', 'foo.conflict.peer.v2'), 'y'); // no numeric ts
    await fsp.writeFile(path.join(root, 'a', 'foo.conflict.peer.1'), 'z'); // ts=1 (epoch) — ancient

    const trashed = await pruneConflicts(root, { retentionDays: 30 });
    // Only the one with a real numeric ts should be trashed.
    assert.equal(trashed, 1);
    assert.equal(fs.existsSync(path.join(root, 'a', 'regular.dat')), true);
    assert.equal(fs.existsSync(path.join(root, 'a', 'foo.conflict.peer.v2')), true);
    assert.equal(fs.existsSync(path.join(root, 'a', 'foo.conflict.peer.1')), false);
  } finally {
    await rimraf(root);
  }
});

test('pruneConflicts skips the trash bucket itself', async () => {
  const root = await makeTempFolder('cs-p9-prune-self-');
  try {
    // Put a fake "conflict" file inside the trash — it must not be re-trashed.
    const oldTs = Date.now() - 100 * 24 * 60 * 60 * 1000;
    const inTrash = path.join(root, TRASH_DIRNAME, '2026-01-01', `x.conflict.peer.${oldTs}`);
    await fsp.mkdir(path.dirname(inTrash), { recursive: true });
    await fsp.writeFile(inTrash, 'already-trashed');

    const trashed = await pruneConflicts(root, { retentionDays: 30 });
    assert.equal(trashed, 0);
    assert.equal(fs.existsSync(inTrash), true,
      'file already inside trash must not be touched');
  } finally {
    await rimraf(root);
  }
});

// ---------------------------------------------------------------------
// GameBackup._assertSafeSaveBase + renameGame path safety
// ---------------------------------------------------------------------

function makeBackup(configDir, gameSavesDir) {
  return new GameBackup({
    configDir,
    config: { data: { settings: {} } },
    gameSavesDir,
  });
}

test('game-backup _assertSafeSaveBase blocks filesystem root', async () => {
  const root = await makeTempFolder('cs-p9-gb-root-');
  try {
    const gameSavesDir = path.join(root, 'saves');
    await fsp.mkdir(gameSavesDir, { recursive: true });
    const gb = makeBackup(root, gameSavesDir);
    try {
      // Windows drive root.
      assert.throws(() => gb._assertSafeSaveBase('C:\\'), /unsafe saveBase/);
      // POSIX root.
      assert.throws(() => gb._assertSafeSaveBase('/'), /unsafe saveBase/);
      // Shallow (1-2 segments) under a drive.
      assert.throws(() => gb._assertSafeSaveBase('C:\\Windows'), /unsafe saveBase/);
      assert.throws(() => gb._assertSafeSaveBase('/usr'), /unsafe saveBase/);
      // Empty.
      assert.throws(() => gb._assertSafeSaveBase(''), /unsafe saveBase/);
    } finally {
      gb.stop();
    }
  } finally {
    await rimraf(root);
  }
});

test('game-backup _assertSafeSaveBase accepts homedir subpath', async () => {
  const root = await makeTempFolder('cs-p9-gb-home-');
  try {
    const gameSavesDir = path.join(root, 'saves');
    await fsp.mkdir(gameSavesDir, { recursive: true });
    const gb = makeBackup(root, gameSavesDir);
    try {
      const home = os.homedir();
      const deep = path.join(home, 'AppData', 'Local', 'SomeGame', 'Saves');
      assert.doesNotThrow(() => gb._assertSafeSaveBase(deep));

      // Any path containing a save marker should pass even if outside
      // homedir (e.g. Steam library on D:).
      assert.doesNotThrow(() =>
        gb._assertSafeSaveBase(path.join('D:', 'Steam', 'userdata', 'AppData', 'Game', 'saves')));

      // A deep path with no marker and not under homedir is refused.
      assert.throws(() =>
        gb._assertSafeSaveBase(path.join('D:', 'Weird', 'Random', 'Place')),
        /unsafe saveBase/);
    } finally {
      gb.stop();
    }
  } finally {
    await rimraf(root);
  }
});

test('game-backup renameGame refuses empty names', async () => {
  const root = await makeTempFolder('cs-p9-gb-rename-empty-');
  try {
    const gameSavesDir = path.join(root, 'saves');
    await fsp.mkdir(gameSavesDir, { recursive: true });
    const gb = makeBackup(root, gameSavesDir);
    try {
      await assert.rejects(() => gb.renameGame('', 'new'), /empty/);
      await assert.rejects(() => gb.renameGame('old', ''), /empty/);
      await assert.rejects(() => gb.renameGame('', ''), /empty/);
    } finally {
      gb.stop();
    }
  } finally {
    await rimraf(root);
  }
});

test('game-backup renameGame refuses path-traversal names', async () => {
  const root = await makeTempFolder('cs-p9-gb-rename-trav-');
  try {
    const gameSavesDir = path.join(root, 'saves');
    await fsp.mkdir(gameSavesDir, { recursive: true });
    const gb = makeBackup(root, gameSavesDir);
    try {
      // gameDir sanitises "/" and "\" so a literal `../foo` collapses to
      // `.._foo` under gameSavesDir (safe). We still exercise the code
      // path — the expectation is "doesn't rm outside gameSavesDir,"
      // which _assertSafePath enforces on whatever gameDir resolved to.
      // Here we just verify it either succeeds harmlessly or throws —
      // never escapes the root.
      try {
        await gb.renameGame('old-game', '../evil');
      } catch {
        // Acceptable — we refused the rename.
      }
      // Nothing should exist outside gameSavesDir afterwards.
      const outside = path.join(root, 'evil');
      assert.equal(fs.existsSync(outside), false,
        'rename must never land outside gameSavesDir');
    } finally {
      gb.stop();
    }
  } finally {
    await rimraf(root);
  }
});

test('_assertSafePath supports array of roots', async () => {
  const root = await makeTempFolder('cs-p9-gb-assert-');
  try {
    const gameSavesDir = path.join(root, 'saves');
    await fsp.mkdir(gameSavesDir, { recursive: true });
    const gb = makeBackup(root, gameSavesDir);
    try {
      const rootA = path.join(root, 'A');
      const rootB = path.join(root, 'B');
      await fsp.mkdir(rootA, { recursive: true });
      await fsp.mkdir(rootB, { recursive: true });

      // Under A — passes.
      assert.doesNotThrow(() =>
        gb._assertSafePath(path.join(rootA, 'sub', 'x'), [rootA, rootB]));
      // Under B — also passes.
      assert.doesNotThrow(() =>
        gb._assertSafePath(path.join(rootB, 'sub', 'y'), [rootA, rootB]));
      // Under neither — throws.
      assert.throws(() =>
        gb._assertSafePath(path.join(root, 'C', 'z'), [rootA, rootB]),
        /unsafe path/);
    } finally {
      gb.stop();
    }
  } finally {
    await rimraf(root);
  }
});
