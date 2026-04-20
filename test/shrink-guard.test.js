const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const fsp = require('fs/promises');
const path = require('path');
const os = require('os');
const crypto = require('crypto');

const {
  classifyOverwrite,
  getShrinkThreshold,
  SHRINK_BLOCKED_LOG_MESSAGE,
  DEFAULT_THRESHOLD,
} = require('../src/daemon/shrink-guard');

// ---- classifyOverwrite ----

test('allows new file (no existing)', () => {
  const r = classifyOverwrite({ existingSize: null, incomingSize: 5000 });
  assert.equal(r.action, 'allow');
  assert.equal(r.reason, 'no-existing');
});

test('allows when existing is empty', () => {
  const r = classifyOverwrite({ existingSize: 0, incomingSize: 5000 });
  assert.equal(r.action, 'allow');
  assert.equal(r.reason, 'existing-empty');
});

test('allows when existing is empty and incoming also empty', () => {
  const r = classifyOverwrite({ existingSize: 0, incomingSize: 0 });
  assert.equal(r.action, 'allow');
  assert.equal(r.reason, 'existing-empty');
});

test('allows when incoming is same size', () => {
  const r = classifyOverwrite({ existingSize: 1000, incomingSize: 1000 });
  assert.equal(r.action, 'allow');
  assert.equal(r.reason, 'no-shrink');
});

test('allows when incoming grows', () => {
  const r = classifyOverwrite({ existingSize: 1000, incomingSize: 2000 });
  assert.equal(r.action, 'allow');
  assert.equal(r.reason, 'no-shrink');
});

test('allows tiny existing below floor', () => {
  // 500 < 1024 floor → allow even though incoming is 0
  const r = classifyOverwrite({ existingSize: 500, incomingSize: 0 });
  assert.equal(r.action, 'allow');
  assert.equal(r.reason, 'below-floor');
});

test('allows existing exactly at floor', () => {
  // <= floor should still be considered below — defaults floor=1024
  const r = classifyOverwrite({ existingSize: 1024, incomingSize: 0 });
  assert.equal(r.action, 'allow');
  assert.equal(r.reason, 'below-floor');
});

test('preserves on incoming empty over substantial existing', () => {
  const r = classifyOverwrite({ existingSize: 100000, incomingSize: 0 });
  assert.equal(r.action, 'preserve');
  assert.equal(r.reason, 'incoming-empty');
});

test('preserves on >90% shrink', () => {
  // 95% drop
  const r = classifyOverwrite({ existingSize: 100000, incomingSize: 5000 });
  assert.equal(r.action, 'preserve');
  assert.equal(r.reason, 'drastic-shrink');
});

test('allows on <90% shrink', () => {
  // 50% drop — not drastic by default
  const r = classifyOverwrite({ existingSize: 100000, incomingSize: 50000 });
  assert.equal(r.action, 'allow');
  assert.equal(r.reason, 'normal-update');
});

test('boundary at exactly 90% drop', () => {
  // 10000 → 1000 = 90% drop exactly. Threshold is `>=` so this trips.
  // (Existing must be > absoluteFloor=1024 or we'd short-circuit on
  // below-floor before even reaching the ratio check.)
  const r = classifyOverwrite({ existingSize: 10000, incomingSize: 1000 });
  assert.equal(r.action, 'preserve');
  assert.equal(r.reason, 'drastic-shrink');
});

test('respects custom thresholds', () => {
  const threshold = { absoluteFloor: 100, percentDrop: 0.5 };

  // 200 → 80 = 60% drop, above 50% threshold
  const r1 = classifyOverwrite({
    existingSize: 200, incomingSize: 80, threshold,
  });
  assert.equal(r1.action, 'preserve');
  assert.equal(r1.reason, 'drastic-shrink');

  // 200 → 150 = 25% drop, below 50% threshold
  const r2 = classifyOverwrite({
    existingSize: 200, incomingSize: 150, threshold,
  });
  assert.equal(r2.action, 'allow');
  assert.equal(r2.reason, 'normal-update');

  // 80 → 0 with floor=100 → below floor, allow
  const r3 = classifyOverwrite({
    existingSize: 80, incomingSize: 0, threshold,
  });
  assert.equal(r3.action, 'allow');
  assert.equal(r3.reason, 'below-floor');
});

test('default threshold export is correct', () => {
  assert.equal(DEFAULT_THRESHOLD.absoluteFloor, 1024);
  assert.equal(DEFAULT_THRESHOLD.percentDrop, 0.9);
});

// ---- getShrinkThreshold ----

test('getShrinkThreshold falls back to defaults when folder has no override', () => {
  assert.deepEqual(getShrinkThreshold(undefined), {
    absoluteFloor: 1024,
    percentDrop: 0.9,
  });
  assert.deepEqual(getShrinkThreshold({}), {
    absoluteFloor: 1024,
    percentDrop: 0.9,
  });
  assert.deepEqual(getShrinkThreshold({ shrinkGuard: {} }), {
    absoluteFloor: 1024,
    percentDrop: 0.9,
  });
});

test('getShrinkThreshold uses per-folder override', () => {
  const t = getShrinkThreshold({
    shrinkGuard: { absoluteFloor: 4096, percentDrop: 0.5 },
  });
  assert.equal(t.absoluteFloor, 4096);
  assert.equal(t.percentDrop, 0.5);
});

test('getShrinkThreshold ignores non-finite override values', () => {
  const t = getShrinkThreshold({
    shrinkGuard: { absoluteFloor: NaN, percentDrop: 'bad' },
  });
  assert.equal(t.absoluteFloor, 1024);
  assert.equal(t.percentDrop, 0.9);
});

// ---- SHRINK_BLOCKED_LOG_MESSAGE ----

test('SHRINK_BLOCKED_LOG_MESSAGE formats correctly', () => {
  const line = SHRINK_BLOCKED_LOG_MESSAGE({
    folderName: 'Mods',
    relPath: 'list.json',
    peerName: 'DESKTOP-1',
    existingSize: 240153,
    incomingSize: 0,
    reason: 'incoming-empty',
    conflictPath: '/x/list.shrink-blocked.DESKTOP-1.123.json',
    timestamp: '2026-04-19T00:00:00.000Z',
  });
  assert.match(line, /SHRINK-BLOCKED: Mods\/list\.json/);
  assert.match(line, /existing=240153/);
  assert.match(line, /incoming=0/);
  assert.match(line, /ratio=100\.0%/);
  assert.match(line, /peer=DESKTOP-1/);
  assert.match(line, /reason=incoming-empty/);
  assert.match(line, /sidecar=/);
  assert.match(line, /kept existing/);
});

test('SHRINK_BLOCKED_LOG_MESSAGE handles existing=0 ratio gracefully', () => {
  // (Not a real case — guard never fires when existing=0 — but the formatter
  // shouldn't divide-by-zero if a caller passes those numbers.)
  const line = SHRINK_BLOCKED_LOG_MESSAGE({
    folderName: 'X', relPath: 'f', peerName: 'P',
    existingSize: 0, incomingSize: 0, reason: 'test',
  });
  assert.match(line, /ratio=n\/a/);
});

// ---- Integration: simulate the receive-write wiring decision ----
//
// This exercises the same call shape the device.js receive sites use:
// stat the existing file, classify, and verify the verdict matches what we
// expect for both the corrupted-peer case and the legitimate-update case.

async function tmpDir() {
  return await fsp.mkdtemp(path.join(os.tmpdir(), 'shrinkguard-'));
}

test('integration: corrupted peer 0-byte over real file → preserve', async () => {
  const dir = await tmpDir();
  try {
    const abs = path.join(dir, 'savefile.dat');
    // Write a real "good" file ~100 KB.
    const good = crypto.randomBytes(100 * 1024);
    await fsp.writeFile(abs, good);

    // Simulate: incoming is 0 bytes (corrupted peer copy).
    const incoming = Buffer.alloc(0);
    const stat = await fsp.stat(abs).catch(() => null);
    const verdict = classifyOverwrite({
      existingSize: stat ? stat.size : null,
      incomingSize: incoming.length,
    });

    assert.equal(verdict.action, 'preserve');
    assert.equal(verdict.reason, 'incoming-empty');

    // The wiring contract: existing file is untouched, incoming saved as a
    // .shrink-blocked sidecar. Simulate just that sidecar bit so we know
    // the byte content is recoverable.
    const sidecar = `${abs}.shrink-blocked.peer1.${Date.now()}`;
    await fsp.writeFile(sidecar, incoming);

    const stillThere = await fsp.readFile(abs);
    assert.equal(stillThere.length, good.length);
    assert.ok(stillThere.equals(good), 'existing file content unchanged');

    const sidecarBytes = await fsp.readFile(sidecar);
    assert.equal(sidecarBytes.length, 0);
  } finally {
    await fsp.rm(dir, { recursive: true, force: true });
  }
});

test('integration: legitimate update at same size → allow + write proceeds', async () => {
  const dir = await tmpDir();
  try {
    const abs = path.join(dir, 'savefile.dat');
    const before = crypto.randomBytes(50 * 1024);
    await fsp.writeFile(abs, before);

    // Same-size update.
    const after = crypto.randomBytes(50 * 1024);
    const stat = await fsp.stat(abs).catch(() => null);
    const verdict = classifyOverwrite({
      existingSize: stat ? stat.size : null,
      incomingSize: after.length,
    });
    assert.equal(verdict.action, 'allow');
    assert.equal(verdict.reason, 'no-shrink');

    // Simulate the proceed-with-rename branch.
    const tmp = abs + '.carbonsync.tmp';
    await fsp.writeFile(tmp, after);
    await fsp.rename(tmp, abs);

    const finalBytes = await fsp.readFile(abs);
    assert.ok(finalBytes.equals(after));
  } finally {
    await fsp.rm(dir, { recursive: true, force: true });
  }
});

test('integration: brand-new file (no existing) → allow', async () => {
  const dir = await tmpDir();
  try {
    const abs = path.join(dir, 'newfile.dat');
    const incoming = Buffer.alloc(0); // even an empty file should be allowed if there's no existing
    const stat = await fsp.stat(abs).catch(() => null);
    const verdict = classifyOverwrite({
      existingSize: stat ? stat.size : null,
      incomingSize: incoming.length,
    });
    assert.equal(verdict.action, 'allow');
    assert.equal(verdict.reason, 'no-existing');
  } finally {
    await fsp.rm(dir, { recursive: true, force: true });
  }
});

test('integration: small existing file gets allowed under floor', async () => {
  const dir = await tmpDir();
  try {
    const abs = path.join(dir, 'tiny.dat');
    await fsp.writeFile(abs, Buffer.alloc(200, 'a')); // 200 bytes < 1024 floor

    const stat = await fsp.stat(abs);
    const verdict = classifyOverwrite({
      existingSize: stat.size,
      incomingSize: 0,
    });
    assert.equal(verdict.action, 'allow');
    assert.equal(verdict.reason, 'below-floor');
  } finally {
    await fsp.rm(dir, { recursive: true, force: true });
  }
});
