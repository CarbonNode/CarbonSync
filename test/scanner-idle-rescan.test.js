const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const { Scanner } = require('../src/daemon/scanner');

// Regression guard for the idle-CPU bug: the periodic safety-net rescan used
// to re-upsert every file each pass (scanId-based deletion purge), rewriting
// the entire index — a 168k-file folder rewrote its ~200MB DB every 5 min even
// with zero changes, pegging a CPU core. The scanner now leaves unchanged rows
// untouched and detects deletions by set-difference. These tests pin both:
//   (1) a no-change rescan performs ZERO row writes, and
//   (2) add / modify / delete are still detected correctly.

function freshScanner() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'cs-scan-'));
  const dataDir = path.join(root, 'data');
  fs.mkdirSync(dataDir);
  for (let i = 0; i < 50; i++) {
    fs.writeFileSync(path.join(dataDir, `f${i}.txt`), `content ${i}`);
  }
  fs.mkdirSync(path.join(dataDir, 'sub'));
  fs.writeFileSync(path.join(dataDir, 'sub', 'deep.txt'), 'deep');
  const sc = new Scanner(dataDir, path.join(root, 'index.db'));
  return { sc, dataDir, root, cleanup: () => { sc.close(); fs.rmSync(root, { recursive: true, force: true }); } };
}

test('initial scan indexes every file', async () => {
  const { sc, cleanup } = freshScanner();
  try {
    const s = await sc.fullScan();
    assert.equal(s.added, 51);
    assert.equal(sc.getFileCount(), 51);
  } finally { cleanup(); }
});

test('no-change rescan writes zero rows (no index rewrite)', async () => {
  const { sc, cleanup } = freshScanner();
  try {
    await sc.fullScan();
    const before = sc.getIndex().map(r => ({ p: r.path, at: r.scanned_at }));
    const rootHash = sc.getRootHash();

    // A later scanId (Date.now) — proves we aren't simply colliding timestamps.
    await new Promise(r => setTimeout(r, 1100));
    const s = await sc.fullScan({ force: true });

    assert.equal(s.unchanged, 51);
    assert.equal(s.added, 0);
    assert.equal(s.modified, 0);
    assert.equal(s.deleted, 0);

    const after = sc.getIndex().map(r => ({ p: r.path, at: r.scanned_at }));
    const rewritten = after.filter((r, i) => r.at !== before[i].at);
    assert.equal(rewritten.length, 0, 'no row scanned_at should change on a no-op rescan');
    assert.equal(sc.getRootHash(), rootHash);
  } finally { cleanup(); }
});

test('deletion detected by set-difference', async () => {
  const { sc, dataDir, cleanup } = freshScanner();
  try {
    await sc.fullScan();
    fs.unlinkSync(path.join(dataDir, 'f7.txt'));
    const s = await sc.fullScan({ force: true });
    assert.equal(s.deleted, 1);
    assert.equal(sc.getFileCount(), 50);
    assert.equal(sc.getFile('f7.txt'), undefined);
  } finally { cleanup(); }
});

test('modification re-hashes the changed file only', async () => {
  const { sc, dataDir, cleanup } = freshScanner();
  try {
    await sc.fullScan();
    fs.writeFileSync(path.join(dataDir, 'f3.txt'), 'totally different content now');
    await new Promise(r => setTimeout(r, 50));
    const s = await sc.fullScan({ force: true });
    assert.equal(s.modified, 1);
    assert.equal(s.unchanged, 50);
  } finally { cleanup(); }
});

test('simultaneous add + modify + delete in one scan', async () => {
  const { sc, dataDir, cleanup } = freshScanner();
  try {
    await sc.fullScan();
    fs.unlinkSync(path.join(dataDir, 'f10.txt'));
    fs.writeFileSync(path.join(dataDir, 'f11.txt'), 'changed again');
    fs.writeFileSync(path.join(dataDir, 'brand-new.txt'), 'x');
    await new Promise(r => setTimeout(r, 50));
    const s = await sc.fullScan({ force: true });
    assert.equal(s.added, 1);
    assert.equal(s.modified, 1);
    assert.equal(s.deleted, 1);
    assert.equal(sc.getFileCount(), 51);
  } finally { cleanup(); }
});
