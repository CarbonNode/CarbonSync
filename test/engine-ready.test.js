/**
 * Phase 7 — engine-ready gate tests.
 *
 * The original audit found that engine.start() resolves before the initial
 * full scan finishes for every folder. Inbound NOTIFY/PUSH_INDEX/INDEX_REQUEST
 * and authentication-triggered syncs ran against a partial scanner index, and
 * the gaps were treated by the peer as "we don't have these," driving wrong
 * toDelete on both sides.
 *
 * Phase 7 strengthens _engineReady: only true after engine.start() completes
 * AND every enabled folder reports isInitialScanComplete(). The flag is
 * persisted in the scanner meta table so a user who closed the app mid-scan
 * stays gated until the next full scan finishes.
 *
 * Tests:
 *   - Scanner.isInitialScanComplete: false before scan
 *   - Scanner.isInitialScanComplete: true after fullScan
 *   - Scanner.isInitialScanComplete: persists across reopen
 *   - Scanner.isInitialScanComplete: false on cancelled scan
 *   - SyncEngine.areAllFoldersScanned aggregates folder readiness
 *   - device.js _logEngineNotReady is rate-limited per peer
 */

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const fsp = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');

const { Scanner } = require('../src/daemon/scanner');
const { SyncEngine } = require('../src/daemon/sync-engine');

async function makeTempDir() {
  return fsp.mkdtemp(path.join(os.tmpdir(), 'carbonsync-engine-ready-'));
}

async function rimraf(dir) {
  try { await fsp.rm(dir, { recursive: true, force: true }); } catch {}
}

async function makeScannerWithFolder(opts = {}) {
  const tmp = await makeTempDir();
  const dbPath = path.join(tmp, 'index.db');
  const folderPath = path.join(tmp, 'folder');
  await fsp.mkdir(folderPath, { recursive: true });
  if (opts.files) {
    for (const [name, body] of Object.entries(opts.files)) {
      const abs = path.join(folderPath, name);
      await fsp.mkdir(path.dirname(abs), { recursive: true });
      await fsp.writeFile(abs, body);
    }
  }
  return { tmp, dbPath, folderPath };
}

// ---- Scanner.isInitialScanComplete ----

test('Scanner.isInitialScanComplete: false before scan', async () => {
  const { tmp, dbPath, folderPath } = await makeScannerWithFolder();
  try {
    const scanner = new Scanner(folderPath, dbPath);
    try {
      assert.equal(scanner.isInitialScanComplete(), false,
        'fresh DB must report false before any scan runs');
    } finally {
      scanner.close();
    }
  } finally {
    await rimraf(tmp);
  }
});

test('Scanner.isInitialScanComplete: true after fullScan', async () => {
  const { tmp, dbPath, folderPath } = await makeScannerWithFolder({
    files: {
      'a.txt': 'hello',
      'sub/b.txt': 'world',
      'sub/c.txt': 'carbon',
    },
  });
  try {
    const scanner = new Scanner(folderPath, dbPath);
    try {
      assert.equal(scanner.isInitialScanComplete(), false);
      const stats = await scanner.fullScan();
      assert.equal(stats.added + stats.modified + stats.unchanged, 3,
        'fullScan must observe all three files');
      assert.ok(!stats.cancelled, 'scan must not be cancelled');
      assert.equal(scanner.isInitialScanComplete(), true,
        'after a successful fullScan the marker must be set');
    } finally {
      scanner.close();
    }
  } finally {
    await rimraf(tmp);
  }
});

test('Scanner.isInitialScanComplete: persists across reopen', async () => {
  const { tmp, dbPath, folderPath } = await makeScannerWithFolder({
    files: { 'one.txt': 'data' },
  });
  try {
    {
      const scanner = new Scanner(folderPath, dbPath);
      try {
        await scanner.fullScan();
        assert.equal(scanner.isInitialScanComplete(), true);
      } finally {
        scanner.close();
      }
    }

    // Re-open against the same DB — marker must survive.
    {
      const scanner = new Scanner(folderPath, dbPath);
      try {
        assert.equal(scanner.isInitialScanComplete(), true,
          'marker must persist across reopen');
      } finally {
        scanner.close();
      }
    }
  } finally {
    await rimraf(tmp);
  }
});

test('Scanner.isInitialScanComplete: false on cancelled scan', async () => {
  // Generate enough files that we can cancel mid-scan.
  const { tmp, dbPath, folderPath } = await makeScannerWithFolder();
  try {
    for (let i = 0; i < 800; i++) {
      await fsp.writeFile(path.join(folderPath, `f${i}.txt`), `payload-${i}`);
    }

    const scanner = new Scanner(folderPath, dbPath);
    try {
      // Kick off the scan, then cancel almost immediately.
      const scanPromise = scanner.fullScan();
      // Yield once so the scan starts iterating.
      await new Promise(r => setImmediate(r));
      scanner.cancelScan();
      const stats = await scanPromise;

      // The cancellation MAY race with completion on extremely fast machines;
      // in that case the test would still be valid (marker correctly set on
      // success). What we strictly forbid is "cancelled === true AND marker
      // === true" because that means a partial scan claimed completion.
      if (stats.cancelled) {
        assert.equal(scanner.isInitialScanComplete(), false,
          'a cancelled scan must NOT set the initial-scan-complete marker');
      } else {
        // Race lost — scan completed before cancel took effect. Still fine.
        assert.equal(scanner.isInitialScanComplete(), true,
          'fall-through: scan completed normally so marker must be set');
      }
    } finally {
      scanner.close();
    }
  } finally {
    await rimraf(tmp);
  }
});

test('Scanner.isInitialScanComplete: backfilled for upgrades with last_scan but no marker', async () => {
  // Simulate an install upgraded from a version that wrote `last_scan` but
  // never wrote `initial_scan_complete`. The Phase 7 migration should
  // backfill the marker so we don't re-gate sync on every existing user.
  const { tmp, dbPath, folderPath } = await makeScannerWithFolder({
    files: { 'x.txt': 'x' },
  });
  try {
    {
      const scanner = new Scanner(folderPath, dbPath);
      try {
        await scanner.fullScan();
      } finally {
        scanner.close();
      }
    }

    // Manually clear ONLY the marker, leaving last_scan in place — exactly
    // what an old DB looks like after upgrade.
    {
      const Database = require('better-sqlite3');
      const db = new Database(dbPath);
      db.prepare('DELETE FROM meta WHERE key = ?').run('initial_scan_complete');
      const row = db.prepare("SELECT value FROM meta WHERE key = ?").get('initial_scan_complete');
      assert.equal(row, undefined);
      db.close();
    }

    // Re-open: the migration should backfill the marker because last_scan exists.
    {
      const scanner = new Scanner(folderPath, dbPath);
      try {
        assert.equal(scanner.isInitialScanComplete(), true,
          'upgrade-backfill must mark folders that already completed a scan');
      } finally {
        scanner.close();
      }
    }
  } finally {
    await rimraf(tmp);
  }
});

// ---- SyncEngine.areAllFoldersScanned ----

test('SyncEngine.areAllFoldersScanned: true with no folders', async () => {
  const tmp = await makeTempDir();
  try {
    const engine = new SyncEngine({ configDir: tmp, folders: [] });
    try {
      assert.equal(engine.areAllFoldersScanned(), true,
        'engine with zero folders has nothing to gate; returns true');
    } finally {
      await engine.stop();
    }
  } finally {
    await rimraf(tmp);
  }
});

test('SyncEngine.areAllFoldersScanned: false until every folder scans', async () => {
  const configDir = await makeTempDir();
  try {
    const aDir = await fsp.mkdtemp(path.join(os.tmpdir(), 'engine-a-'));
    const bDir = await fsp.mkdtemp(path.join(os.tmpdir(), 'engine-b-'));
    await fsp.writeFile(path.join(aDir, '1.txt'), 'a');
    await fsp.writeFile(path.join(bDir, '1.txt'), 'b');
    try {
      const engine = new SyncEngine({
        configDir,
        folders: [
          { path: aDir, name: 'A' },
          { path: bDir, name: 'B' },
        ],
      });
      try {
        // Before any scan, both folders' isInitialScanComplete returns false.
        assert.equal(engine.areAllFoldersScanned(), false,
          'fresh engine: no folder has scanned, must report false');

        // Scan only the first folder.
        const folderA = engine.folders.get('A');
        await folderA.scanner.fullScan();
        assert.equal(engine.areAllFoldersScanned(), false,
          'one folder scanned, one pending: must still report false');

        // Scan the second folder.
        const folderB = engine.folders.get('B');
        await folderB.scanner.fullScan();
        assert.equal(engine.areAllFoldersScanned(), true,
          'every folder scanned: must report true');
      } finally {
        await engine.stop();
      }
    } finally {
      await rimraf(aDir);
      await rimraf(bDir);
    }
  } finally {
    await rimraf(configDir);
  }
});

// ---- device.js: _logEngineNotReady rate-limit ----
//
// Wire a minimal stand-in that exercises only the rate-limit logic. We avoid
// constructing a full CarbonSyncDevice because that pulls in mDNS, transport,
// firewall, etc. The function under test only touches the Map and writes to
// configDir, so a lightweight stub is fine and keeps the test hermetic.

test('_logEngineNotReady: rate-limited to one log per peer per minute', async () => {
  const tmp = await makeTempDir();
  try {
    // Reach into device.js to grab the helper as it would behave on `this`.
    // The function uses `this._engineNotReadyLogged` and `this.configDir`; we
    // build a minimal "this" with both, then bind the prototype method.
    const { CarbonSyncDevice } = require('../src/daemon/device');
    const stub = {
      _engineNotReadyLogged: new Map(),
      configDir: tmp,
    };
    const fn = CarbonSyncDevice.prototype._logEngineNotReady.bind(stub);

    const logPath = path.join(tmp, 'sync.log');

    // First call: must write a line.
    fn('PEER_A');
    assert.ok(fs.existsSync(logPath), 'first call must create sync.log');
    let lines = fs.readFileSync(logPath, 'utf8').trim().split('\n').filter(Boolean);
    assert.equal(lines.length, 1, 'first call writes exactly one line');
    assert.match(lines[0], /ENGINE-NOT-READY.*PEER_A/);

    // Second call within window: must be suppressed.
    fn('PEER_A');
    fn('PEER_A');
    fn('PEER_A');
    lines = fs.readFileSync(logPath, 'utf8').trim().split('\n').filter(Boolean);
    assert.equal(lines.length, 1,
      'subsequent calls within 60s for the same peer must be suppressed');

    // Different peer: must log immediately.
    fn('PEER_B');
    lines = fs.readFileSync(logPath, 'utf8').trim().split('\n').filter(Boolean);
    assert.equal(lines.length, 2, 'different peer is tracked independently');
    assert.match(lines[1], /ENGINE-NOT-READY.*PEER_B/);

    // Force the window to "expire" by rewinding the timestamp.
    stub._engineNotReadyLogged.set('PEER_A', Date.now() - 61_000);
    fn('PEER_A');
    lines = fs.readFileSync(logPath, 'utf8').trim().split('\n').filter(Boolean);
    assert.equal(lines.length, 3, 'after 60s, the same peer logs again');
  } finally {
    await rimraf(tmp);
  }
});

test('_logEngineNotReady: empty/missing peer name does not throw', async () => {
  const tmp = await makeTempDir();
  try {
    const { CarbonSyncDevice } = require('../src/daemon/device');
    const stub = { _engineNotReadyLogged: new Map(), configDir: tmp };
    const fn = CarbonSyncDevice.prototype._logEngineNotReady.bind(stub);
    // Should coalesce to 'unknown' and not throw.
    fn();
    fn(null);
    fn('');
    const lines = fs.readFileSync(path.join(tmp, 'sync.log'), 'utf8').trim().split('\n').filter(Boolean);
    // First call writes; the next two collapse to the same 'unknown' bucket.
    assert.equal(lines.length, 1);
    assert.match(lines[0], /ENGINE-NOT-READY.*unknown/);
  } finally {
    await rimraf(tmp);
  }
});
