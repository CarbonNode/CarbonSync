/**
 * Phase 5 — discovery-first sync regression tests.
 *
 * Phase 3's optimistic peer_state seeding (v2.7.0) re-created the original
 * silent-data-loss bug on the first sync after upgrade: seeded entries made
 * a stale peer's "delete X" diff classify as peer-explicit. Phase 5 replaces
 * seeding with a discovery flag — until one full sync round-trip completes
 * with a peer, ALL proposed deletions are preserved.
 */

const test = require('node:test');
const assert = require('node:assert/strict');
const fsp = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');
const Database = require('better-sqlite3');

const { Scanner } = require('../src/daemon/scanner');
const { evaluateDeletionWithDiscovery } = require('../src/daemon/stale-peer-guard');

async function makeTempDir() {
  return fsp.mkdtemp(path.join(os.tmpdir(), 'carbonsync-discovery-'));
}

async function rimraf(dir) {
  try { await fsp.rm(dir, { recursive: true, force: true }); } catch {}
}

async function makeScannerWithFolder() {
  const tmp = await makeTempDir();
  const dbPath = path.join(tmp, 'index.db');
  const folderPath = path.join(tmp, 'folder');
  await fsp.mkdir(folderPath, { recursive: true });
  return { tmp, dbPath, folderPath };
}

// ---- discovery flag transitions ----

test('discovery flag round-trip: discovering -> discovered', async () => {
  const { tmp, dbPath, folderPath } = await makeScannerWithFolder();
  try {
    const scanner = new Scanner(folderPath, dbPath);
    try {
      const peerId = 'peer:HOSTA';
      assert.equal(scanner.hasAnyDiscoveryRecord(peerId), false);
      assert.equal(scanner.isPeerDiscovered(peerId), false);

      scanner.markPeerDiscovering(peerId);
      assert.equal(scanner.hasAnyDiscoveryRecord(peerId), true);
      assert.equal(scanner.isPeerDiscovered(peerId), false,
        'discovering does not yet count as discovered');

      scanner.markPeerDiscovered(peerId);
      assert.equal(scanner.hasAnyDiscoveryRecord(peerId), true);
      assert.equal(scanner.isPeerDiscovered(peerId), true);
    } finally {
      scanner.close();
    }
  } finally {
    await rimraf(tmp);
  }
});

test('markPeerDiscovering is idempotent (no row duplication)', async () => {
  const { tmp, dbPath, folderPath } = await makeScannerWithFolder();
  try {
    const scanner = new Scanner(folderPath, dbPath);
    try {
      const peerId = 'peer:HOSTB';
      scanner.markPeerDiscovering(peerId);
      scanner.markPeerDiscovering(peerId);
      scanner.markPeerDiscovering(peerId);

      const count = scanner.db
        .prepare('SELECT COUNT(*) AS c FROM peer_discovery WHERE peer_id = ?')
        .get(peerId).c;
      assert.equal(count, 1, 'must collapse to a single row');
      assert.equal(scanner.isPeerDiscovered(peerId), false);
    } finally {
      scanner.close();
    }
  } finally {
    await rimraf(tmp);
  }
});

test('discovery transition is one-way: re-marking discovering does not reset', async () => {
  const { tmp, dbPath, folderPath } = await makeScannerWithFolder();
  try {
    const scanner = new Scanner(folderPath, dbPath);
    try {
      const peerId = 'peer:HOSTC';
      scanner.markPeerDiscovering(peerId);
      scanner.markPeerDiscovered(peerId);
      const tsAfterMark = scanner.db
        .prepare('SELECT discovered_at FROM peer_discovery WHERE peer_id = ?')
        .get(peerId).discovered_at;
      assert.ok(tsAfterMark != null && tsAfterMark > 0);

      scanner.markPeerDiscovering(peerId);
      const tsAfter = scanner.db
        .prepare('SELECT discovered_at FROM peer_discovery WHERE peer_id = ?')
        .get(peerId).discovered_at;
      assert.equal(tsAfter, tsAfterMark, 'INSERT OR IGNORE must not overwrite discovered_at');
      assert.equal(scanner.isPeerDiscovered(peerId), true);
    } finally {
      scanner.close();
    }
  } finally {
    await rimraf(tmp);
  }
});

test('markPeerDiscovered is idempotent and preserves the original timestamp', async () => {
  const { tmp, dbPath, folderPath } = await makeScannerWithFolder();
  try {
    const scanner = new Scanner(folderPath, dbPath);
    try {
      const peerId = 'peer:HOSTD';
      scanner.markPeerDiscovering(peerId);
      scanner.markPeerDiscovered(peerId);
      const ts1 = scanner.db
        .prepare('SELECT discovered_at FROM peer_discovery WHERE peer_id = ?')
        .get(peerId).discovered_at;
      await new Promise(r => setTimeout(r, 5));
      scanner.markPeerDiscovered(peerId);
      const ts2 = scanner.db
        .prepare('SELECT discovered_at FROM peer_discovery WHERE peer_id = ?')
        .get(peerId).discovered_at;
      assert.equal(ts2, ts1, 'second markPeerDiscovered must not overwrite the timestamp');
    } finally {
      scanner.close();
    }
  } finally {
    await rimraf(tmp);
  }
});

// ---- evaluateDeletionWithDiscovery — the regression scenario ----

test('regression: discovering peer + stale "delete" diff -> preserve all', async () => {
  const { tmp, dbPath, folderPath } = await makeScannerWithFolder();
  try {
    await fsp.writeFile(path.join(folderPath, 'A.txt'), 'A-content');
    await fsp.writeFile(path.join(folderPath, 'B.txt'), 'B-content');
    await fsp.writeFile(path.join(folderPath, 'C.txt'), 'C-content');

    const scanner = new Scanner(folderPath, dbPath);
    try {
      await scanner.fullScan({ force: true });
      await scanner.upgradeFastHashes();

      const peerId = 'peer:STALE';
      scanner.markPeerDiscovering(peerId);
      assert.equal(scanner.isPeerDiscovered(peerId), false);

      // Simulate the bug: pre-populate peer_state at current hashes the way
      // the old optimistic seed would have. Even with this poisoned state,
      // discovery-first sync MUST refuse to delete.
      for (const row of scanner.getIndex()) {
        scanner.recordPeerKnown(peerId, row.path, row.hash);
      }

      const result = evaluateDeletionWithDiscovery({
        scanner, peerId, paths: ['B.txt', 'C.txt'],
      });

      assert.deepEqual(result.delete, [],
        'no deletions allowed during discovery, even when peer_state matches');
      assert.equal(result.preserve.length, 2);
      const reasonsByPath = Object.fromEntries(
        result.preserve.map(p => [p.path, p.reason])
      );
      assert.equal(reasonsByPath['B.txt'], 'discovery-first-sync');
      assert.equal(reasonsByPath['C.txt'], 'discovery-first-sync');
      assert.deepEqual(result.pushBack.sort(), ['B.txt', 'C.txt'],
        'all preserved paths must be queued for re-push to the peer');
    } finally {
      scanner.close();
    }
  } finally {
    await rimraf(tmp);
  }
});

test('post-discovery: legitimate peer-explicit delete is allowed', async () => {
  const { tmp, dbPath, folderPath } = await makeScannerWithFolder();
  try {
    await fsp.writeFile(path.join(folderPath, 'A.txt'), 'A-content');
    await fsp.writeFile(path.join(folderPath, 'B.txt'), 'B-content');
    await fsp.writeFile(path.join(folderPath, 'C.txt'), 'C-content');

    const scanner = new Scanner(folderPath, dbPath);
    try {
      await scanner.fullScan({ force: true });
      await scanner.upgradeFastHashes();

      const peerId = 'peer:GOOD';
      scanner.markPeerDiscovering(peerId);
      scanner.markPeerDiscovered(peerId);

      const idx = scanner.getIndexMap();
      scanner.recordPeerKnown(peerId, 'B.txt', idx.get('B.txt').hash);
      scanner.recordPeerKnown(peerId, 'C.txt', idx.get('C.txt').hash);

      const result = evaluateDeletionWithDiscovery({
        scanner, peerId, paths: ['B.txt', 'C.txt'],
      });

      assert.deepEqual(result.delete.sort(), ['B.txt', 'C.txt'],
        'after discovery, peer-explicit deletes must go through');
      assert.deepEqual(result.preserve, []);
      assert.deepEqual(result.pushBack, []);
    } finally {
      scanner.close();
    }
  } finally {
    await rimraf(tmp);
  }
});

test('evaluateDeletionWithDiscovery: empty paths returns empty partitions', () => {
  const stub = {
    isPeerDiscovered: () => true,
    getPeerKnown: () => null,
    getFile: () => null,
  };
  const r = evaluateDeletionWithDiscovery({ scanner: stub, peerId: 'p', paths: [] });
  assert.deepEqual(r, { delete: [], preserve: [], pushBack: [] });
});

test('evaluateDeletionWithDiscovery: missing scanner / peerId returns empty', () => {
  assert.deepEqual(
    evaluateDeletionWithDiscovery({ scanner: null, peerId: 'p', paths: ['x'] }),
    { delete: [], preserve: [], pushBack: [] }
  );
  assert.deepEqual(
    evaluateDeletionWithDiscovery({ scanner: { isPeerDiscovered: () => true }, peerId: '', paths: ['x'] }),
    { delete: [], preserve: [], pushBack: [] }
  );
});

// ---- Phase 5 one-time migration ----

test('migration wipes pre-existing peer_state on first construction and stamps meta', async () => {
  const tmp = await makeTempDir();
  try {
    const dbPath = path.join(tmp, 'index.db');
    const folderPath = path.join(tmp, 'folder');
    await fsp.mkdir(folderPath, { recursive: true });

    {
      const db = new Database(dbPath);
      db.exec(`
        CREATE TABLE files (
          path TEXT PRIMARY KEY, size INTEGER NOT NULL,
          mtime_ms INTEGER NOT NULL, hash TEXT NOT NULL,
          scanned_at INTEGER NOT NULL
        );
        CREATE TABLE dirs (path TEXT PRIMARY KEY, scanned_at INTEGER NOT NULL);
        CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE peer_state (
          peer_id TEXT NOT NULL, path TEXT NOT NULL,
          hash TEXT NOT NULL, synced_at INTEGER NOT NULL,
          PRIMARY KEY (peer_id, path)
        );
        CREATE TABLE peer_discovery (peer_id TEXT PRIMARY KEY, discovered_at INTEGER);
      `);
      const ins = db.prepare(
        'INSERT INTO peer_state (peer_id, path, hash, synced_at) VALUES (?, ?, ?, ?)'
      );
      ins.run('peer:OLD', 'a.txt', 'h-old', Date.now());
      ins.run('peer:OLD', 'b.txt', 'h-old', Date.now());
      ins.run('peer:OTHER', 'c.txt', 'h-other', Date.now());
      db.prepare('INSERT INTO peer_discovery (peer_id, discovered_at) VALUES (?, ?)')
        .run('peer:OLD', Date.now());
      assert.equal(db.prepare('SELECT COUNT(*) AS c FROM peer_state').get().c, 3);
      assert.equal(db.prepare('SELECT COUNT(*) AS c FROM peer_discovery').get().c, 1);
      db.close();
    }

    const scanner = new Scanner(folderPath, dbPath);
    try {
      assert.equal(scanner.hasPeerKnown('peer:OLD'), false,
        'pre-existing peer_state must be wiped by the v5 migration');
      assert.equal(scanner.hasPeerKnown('peer:OTHER'), false);
      assert.equal(scanner.hasAnyDiscoveryRecord('peer:OLD'), false,
        'pre-existing peer_discovery rows must be wiped so first sync is treated as discovery');
      const marker = scanner.db
        .prepare("SELECT value FROM meta WHERE key = 'peer_state_migrated_v5'")
        .get();
      assert.ok(marker && marker.value === '1', 'migration marker must be set');
    } finally {
      scanner.close();
    }

    {
      const scanner2 = new Scanner(folderPath, dbPath);
      try {
        scanner2.recordPeerKnown('peer:NEW', 'x.txt', 'h-new');
      } finally {
        scanner2.close();
      }
    }
    {
      const scanner3 = new Scanner(folderPath, dbPath);
      try {
        const got = scanner3.getPeerKnown('peer:NEW', 'x.txt');
        assert.ok(got && got.hash === 'h-new',
          'peer_state must persist across reopen — migration must run only once');
      } finally {
        scanner3.close();
      }
    }
  } finally {
    await rimraf(tmp);
  }
});

test('discovery rows are per-peer (one peer being discovered does not gate another)', async () => {
  const { tmp, dbPath, folderPath } = await makeScannerWithFolder();
  try {
    const scanner = new Scanner(folderPath, dbPath);
    try {
      scanner.markPeerDiscovering('peer:A');
      scanner.markPeerDiscovering('peer:B');
      scanner.markPeerDiscovered('peer:A');
      assert.equal(scanner.isPeerDiscovered('peer:A'), true);
      assert.equal(scanner.isPeerDiscovered('peer:B'), false);
      assert.equal(scanner.hasAnyDiscoveryRecord('peer:B'), true);
    } finally {
      scanner.close();
    }
  } finally {
    await rimraf(tmp);
  }
});
