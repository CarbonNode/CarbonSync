const test = require('node:test');
const assert = require('node:assert/strict');
const fsp = require('node:fs/promises');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const Database = require('better-sqlite3');

const {
  classifyDeletion,
  classifyDeletionBatch,
} = require('../src/daemon/stale-peer-guard');
const { Scanner } = require('../src/daemon/scanner');

// ---- classifyDeletion: single-file outcomes ----

test('classifyDeletion: peer knew exact hash -> delete (peer-explicit)', () => {
  const r = classifyDeletion({
    peerKnown: { hash: 'h1', synced_at: 1 },
    currentLocalHash: 'h1',
  });
  assert.equal(r.action, 'delete');
  assert.equal(r.reason, 'peer-explicit');
});

test('classifyDeletion: peer never knew -> preserve (never-known-by-peer)', () => {
  const r = classifyDeletion({
    peerKnown: null,
    currentLocalHash: 'h1',
  });
  assert.equal(r.action, 'preserve');
  assert.equal(r.reason, 'never-known-by-peer');
});

test('classifyDeletion: peer knew older hash -> preserve (local-modified-since-sync)', () => {
  const r = classifyDeletion({
    peerKnown: { hash: 'older', synced_at: 1 },
    currentLocalHash: 'newer',
  });
  assert.equal(r.action, 'preserve');
  assert.equal(r.reason, 'local-modified-since-sync');
});

test('classifyDeletion: file already gone locally -> delete (already-gone)', () => {
  const r = classifyDeletion({
    peerKnown: { hash: 'h1', synced_at: 1 },
    currentLocalHash: null,
  });
  assert.equal(r.action, 'delete');
  assert.equal(r.reason, 'already-gone');
});

test('classifyDeletion: file gone locally AND peer never knew -> still delete (already-gone wins)', () => {
  // No file = no work, even if peer-never-knew would otherwise preserve.
  const r = classifyDeletion({
    peerKnown: null,
    currentLocalHash: null,
  });
  assert.equal(r.action, 'delete');
  assert.equal(r.reason, 'already-gone');
});

// ---- classifyDeletionBatch: in-memory SQLite stub ----

/**
 * Minimal scanner-shaped object backed by an in-memory better-sqlite3 DB
 * that supports the methods classifyDeletionBatch consults.
 */
function makeMockScanner() {
  const db = new Database(':memory:');
  db.exec(`
    CREATE TABLE files (path TEXT PRIMARY KEY, hash TEXT NOT NULL);
    CREATE TABLE peer_state (
      peer_id TEXT NOT NULL,
      path TEXT NOT NULL,
      hash TEXT NOT NULL,
      synced_at INTEGER NOT NULL,
      PRIMARY KEY (peer_id, path)
    );
  `);
  const insertFile = db.prepare('INSERT OR REPLACE INTO files (path, hash) VALUES (?, ?)');
  const insertPeer = db.prepare(
    'INSERT OR REPLACE INTO peer_state (peer_id, path, hash, synced_at) VALUES (?, ?, ?, ?)'
  );
  const getFileStmt = db.prepare('SELECT path, hash FROM files WHERE path = ?');
  const getPeerStmt = db.prepare(
    'SELECT hash, synced_at FROM peer_state WHERE peer_id = ? AND path = ?'
  );
  return {
    db,
    setFile: (p, h) => insertFile.run(p, h),
    setPeer: (peerId, p, h) => insertPeer.run(peerId, p, h, Date.now()),
    getFile(p) { return getFileStmt.get(p) || null; },
    getPeerKnown(peerId, p) { return getPeerStmt.get(peerId, p) || null; },
  };
}

test('classifyDeletionBatch: mixed outcomes partitioned correctly', () => {
  const scanner = makeMockScanner();
  const peerId = 'peer:TEST';

  // explicit: peer knew it, we still have it at same hash
  scanner.setFile('explicit.txt', 'hX');
  scanner.setPeer(peerId, 'explicit.txt', 'hX');

  // never-known: we have it, peer has no record
  scanner.setFile('never.txt', 'hN');

  // local-modified: peer knew older hash, we have new hash
  scanner.setFile('modified.txt', 'hNew');
  scanner.setPeer(peerId, 'modified.txt', 'hOld');

  // already-gone: peer knew it, but we don't have it
  scanner.setPeer(peerId, 'gone.txt', 'hG');

  const result = classifyDeletionBatch({
    scanner, peerId,
    paths: ['explicit.txt', 'never.txt', 'modified.txt', 'gone.txt'],
  });

  assert.deepEqual(result.delete.sort(), ['explicit.txt', 'gone.txt']);
  assert.equal(result.preserve.length, 2);
  const reasons = Object.fromEntries(result.preserve.map(p => [p.path, p.reason]));
  assert.equal(reasons['never.txt'], 'never-known-by-peer');
  assert.equal(reasons['modified.txt'], 'local-modified-since-sync');
  // pushBack is the subset of preserve worth re-pushing.
  assert.deepEqual(result.pushBack.sort(), ['modified.txt', 'never.txt']);
});

test('classifyDeletionBatch: pushBack excludes peer-explicit and already-gone', () => {
  const scanner = makeMockScanner();
  const peerId = 'peer:TEST';
  scanner.setFile('explicit.txt', 'hX');
  scanner.setPeer(peerId, 'explicit.txt', 'hX');
  scanner.setPeer(peerId, 'gone.txt', 'hG'); // not in files
  const result = classifyDeletionBatch({
    scanner, peerId, paths: ['explicit.txt', 'gone.txt'],
  });
  assert.deepEqual(result.delete.sort(), ['explicit.txt', 'gone.txt']);
  assert.deepEqual(result.preserve, []);
  assert.deepEqual(result.pushBack, []);
});

test('classifyDeletionBatch: empty paths returns empty partitions', () => {
  const scanner = makeMockScanner();
  const result = classifyDeletionBatch({ scanner, peerId: 'peer:T', paths: [] });
  assert.deepEqual(result.delete, []);
  assert.deepEqual(result.preserve, []);
  assert.deepEqual(result.pushBack, []);
});

test('classifyDeletionBatch: missing scanner / peerId returns empty', () => {
  assert.deepEqual(
    classifyDeletionBatch({ scanner: null, peerId: 'p', paths: ['x'] }),
    { delete: [], preserve: [], pushBack: [] }
  );
  assert.deepEqual(
    classifyDeletionBatch({ scanner: makeMockScanner(), peerId: '', paths: ['x'] }),
    { delete: [], preserve: [], pushBack: [] }
  );
});

// ---- Real Scanner: DB round-trip ----

async function makeTempDir() {
  return fsp.mkdtemp(path.join(os.tmpdir(), 'carbonsync-staleguard-'));
}

async function rimraf(dir) {
  try { await fsp.rm(dir, { recursive: true, force: true }); } catch {}
}

test('Scanner peer_state: record / get / clear round-trip', async () => {
  const tmp = await makeTempDir();
  try {
    const dbPath = path.join(tmp, 'index.db');
    const folderPath = path.join(tmp, 'folder');
    await fsp.mkdir(folderPath, { recursive: true });
    const scanner = new Scanner(folderPath, dbPath);
    try {
      const peerId = 'peer:hostA';

      assert.equal(scanner.getPeerKnown(peerId, 'a/b.txt'), null);

      scanner.recordPeerKnown(peerId, 'a/b.txt', 'h1');
      const got = scanner.getPeerKnown(peerId, 'a/b.txt');
      assert.equal(got.hash, 'h1');
      assert.ok(typeof got.synced_at === 'number' && got.synced_at > 0);

      // Bulk + map readback
      scanner.recordPeerKnownBulk(peerId, [
        { path: 'x.txt', hash: 'hx' },
        { path: 'y.txt', hash: 'hy' },
      ]);
      const map = scanner.getPeerKnownMap(peerId);
      assert.equal(map.size, 3);
      assert.equal(map.get('x.txt').hash, 'hx');
      assert.equal(map.get('y.txt').hash, 'hy');

      // hasPeerKnown
      assert.equal(scanner.hasPeerKnown(peerId), true);
      assert.equal(scanner.hasPeerKnown('peer:nobody'), false);

      // Clear single
      scanner.clearPeerKnown(peerId, 'a/b.txt');
      assert.equal(scanner.getPeerKnown(peerId, 'a/b.txt'), null);

      // Clear all
      scanner.clearPeerAll(peerId);
      assert.equal(scanner.getPeerKnownMap(peerId).size, 0);
      assert.equal(scanner.hasPeerKnown(peerId), false);
    } finally {
      scanner.close();
    }
  } finally {
    await rimraf(tmp);
  }
});

test('Scanner seedPeerKnown is a no-op (Phase 5 supersedes optimistic seeding)', async () => {
  // Phase 5: optimistic seeding defeated the stale-peer guard on the first
  // sync after upgrade. seedPeerKnown is preserved as a no-op so any caller
  // (including older test imports) fails safely instead of poisoning
  // peer_state. Discovery-first sync (markPeerDiscovering / isPeerDiscovered)
  // takes its place.
  const tmp = await makeTempDir();
  try {
    const dbPath = path.join(tmp, 'index.db');
    const folderPath = path.join(tmp, 'folder');
    await fsp.mkdir(folderPath, { recursive: true });
    await fsp.writeFile(path.join(folderPath, 'a.txt'), 'AAA');
    await fsp.writeFile(path.join(folderPath, 'b.txt'), 'BBBB');

    const scanner = new Scanner(folderPath, dbPath);
    try {
      await scanner.fullScan({ force: true });
      assert.equal(scanner.getFileCount(), 2);

      const peerId = 'peer:host';
      assert.equal(scanner.hasPeerKnown(peerId), false);
      const seeded = scanner.seedPeerKnown(peerId);
      assert.equal(seeded, 0, 'seedPeerKnown must not insert any rows');
      assert.equal(scanner.hasPeerKnown(peerId), false);
      assert.equal(scanner.getPeerKnownMap(peerId).size, 0);
    } finally {
      scanner.close();
    }
  } finally {
    await rimraf(tmp);
  }
});

test('classifyDeletionBatch against real Scanner reflects peer_state', async () => {
  const tmp = await makeTempDir();
  try {
    const dbPath = path.join(tmp, 'index.db');
    const folderPath = path.join(tmp, 'folder');
    await fsp.mkdir(folderPath, { recursive: true });
    await fsp.writeFile(path.join(folderPath, 'keep.txt'), 'KEEP');
    await fsp.writeFile(path.join(folderPath, 'modified.txt'), 'NEW-CONTENT');

    const scanner = new Scanner(folderPath, dbPath);
    try {
      await scanner.fullScan({ force: true });
      // Force real hashes (fullScan creates fast: hashes for new files).
      await scanner.upgradeFastHashes();
      const indexMap = scanner.getIndexMap();
      const keepRow = indexMap.get('keep.txt');
      const modifiedRow = indexMap.get('modified.txt');
      assert.ok(keepRow && modifiedRow);

      const peerId = 'peer:test';
      // Peer knew "keep" at its current hash (peer-explicit candidate).
      scanner.recordPeerKnown(peerId, 'keep.txt', keepRow.hash);
      // Peer knew "modified" at a different (older) hash -> conflict.
      scanner.recordPeerKnown(peerId, 'modified.txt', 'old-hash');
      // "absent.txt" doesn't exist locally; peer knew it.
      scanner.recordPeerKnown(peerId, 'absent.txt', 'h-absent');
      // "fresh.txt" exists locally but peer never knew it.
      await fsp.writeFile(path.join(folderPath, 'fresh.txt'), 'FRESH');
      await scanner.updateFile(path.join(folderPath, 'fresh.txt'));

      const r = classifyDeletionBatch({
        scanner, peerId,
        paths: ['keep.txt', 'modified.txt', 'absent.txt', 'fresh.txt'],
      });

      assert.deepEqual(r.delete.sort(), ['absent.txt', 'keep.txt']);
      const preserveByPath = Object.fromEntries(
        r.preserve.map(p => [p.path, p.reason])
      );
      assert.equal(preserveByPath['modified.txt'], 'local-modified-since-sync');
      assert.equal(preserveByPath['fresh.txt'], 'never-known-by-peer');
      assert.deepEqual(r.pushBack.sort(), ['fresh.txt', 'modified.txt']);
    } finally {
      scanner.close();
    }
  } finally {
    await rimraf(tmp);
  }
});
