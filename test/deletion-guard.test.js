const test = require('node:test');
const assert = require('node:assert/strict');

const {
  evaluateDeletion,
  getThresholds,
  BLOCKED_LOG_MESSAGE,
} = require('../src/daemon/deletion-guard');

test('allows zero deletions', () => {
  const r = evaluateDeletion({
    folderName: 'X', totalFiles: 1000, toDeleteCount: 0, peerName: 'P',
  });
  assert.equal(r.allowed, true);
  assert.equal(r.reason, null);
  assert.equal(r.threshold, null);
});

test('allows under both thresholds', () => {
  // 5 of 100 = 5% — well under both 25% and 50 absolute.
  const r = evaluateDeletion({
    folderName: 'X', totalFiles: 100, toDeleteCount: 5, peerName: 'P',
  });
  assert.equal(r.allowed, true);
  assert.equal(r.reason, null);
});

test('blocks over absolute threshold', () => {
  // 51 of 1000 = 5.1% — under 25%, but over 50 absolute.
  const r = evaluateDeletion({
    folderName: 'X', totalFiles: 1000, toDeleteCount: 51, peerName: 'P',
  });
  assert.equal(r.allowed, false);
  assert.match(r.reason, /absolute/);
  assert.equal(r.threshold.absolute, 50);
});

test('blocks over percent threshold', () => {
  // 30 of 100 = 30% — over 25%, but under 50 absolute.
  const r = evaluateDeletion({
    folderName: 'X', totalFiles: 100, toDeleteCount: 30, peerName: 'P',
  });
  assert.equal(r.allowed, false);
  assert.match(r.reason, /percent/);
  assert.equal(r.threshold.percent, 25);
});

test('respects custom thresholds', () => {
  const thresholds = { absolute: 5, percent: 10 };

  // 6 of 100 → over absolute=5 (1st check), even though pct=6% < 10%.
  const r1 = evaluateDeletion({
    folderName: 'X', totalFiles: 100, toDeleteCount: 6, peerName: 'P', thresholds,
  });
  assert.equal(r1.allowed, false);
  assert.match(r1.reason, /absolute/);

  // 4 of 100 → 4% under both.
  const r2 = evaluateDeletion({
    folderName: 'X', totalFiles: 100, toDeleteCount: 4, peerName: 'P', thresholds,
  });
  assert.equal(r2.allowed, true);

  // 11 of 1000 → 1.1% under percent, but 11 > absolute=5 → blocked absolute.
  const r3a = evaluateDeletion({
    folderName: 'X', totalFiles: 1000, toDeleteCount: 11, peerName: 'P', thresholds,
  });
  assert.equal(r3a.allowed, false);
  assert.match(r3a.reason, /absolute/);

  // To exercise the percent branch with these custom thresholds, lift the
  // absolute high enough that the percent check is what trips.
  // 11 of 100 = 11%, absolute lifted to 1000.
  const r3b = evaluateDeletion({
    folderName: 'X', totalFiles: 100, toDeleteCount: 11, peerName: 'P',
    thresholds: { absolute: 1000, percent: 10 },
  });
  assert.equal(r3b.allowed, false);
  assert.match(r3b.reason, /percent/);
});

test('getThresholds returns defaults for missing field', () => {
  assert.deepEqual(getThresholds({ name: 'x' }), { absolute: 50, percent: 25 });
});

test('getThresholds reads partial overrides', () => {
  assert.deepEqual(
    getThresholds({ deletionThreshold: { absolute: 100 } }),
    { absolute: 100, percent: 25 }
  );
});

test('getThresholds reads both overrides', () => {
  assert.deepEqual(
    getThresholds({ deletionThreshold: { absolute: 5, percent: 10 } }),
    { absolute: 5, percent: 10 }
  );
});

test('BLOCKED_LOG_MESSAGE format', () => {
  const line = BLOCKED_LOG_MESSAGE({
    folderName: 'NeoCreate',
    peerName: 'DESKTOP-XYZ',
    count: 60,
    reason: 'would delete 60/847 files (7.1%), exceeds absolute threshold of 50',
    timestamp: '2026-04-20T03:06:39.233Z',
  });
  assert.match(line, /BLOCKED/);
  assert.match(line, /NeoCreate/);
  assert.match(line, /DESKTOP-XYZ/);
  assert.match(line, /60/);
  assert.match(line, /exceeds absolute threshold of 50/);
});
