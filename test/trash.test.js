const test = require('node:test');
const assert = require('node:assert/strict');
const fsp = require('node:fs/promises');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const { moveToTrash, pruneTrash, TRASH_DIRNAME } = require('../src/daemon/trash');

function todayUtc() {
  return new Date().toISOString().slice(0, 10);
}

async function makeTempFolder() {
  const dir = await fsp.mkdtemp(path.join(os.tmpdir(), 'carbonsync-trash-test-'));
  return dir;
}

async function rimraf(dir) {
  try { await fsp.rm(dir, { recursive: true, force: true }); } catch {}
}

test('moveToTrash moves a file to dated subdir', async () => {
  const root = await makeTempFolder();
  try {
    const rel = path.join('mods', 'foo.jar');
    const abs = path.join(root, rel);
    await fsp.mkdir(path.dirname(abs), { recursive: true });
    await fsp.writeFile(abs, 'JAR-CONTENTS');

    const result = await moveToTrash(root, rel, { reason: 'sync-delete' });
    assert.equal(result, true);
    assert.equal(fs.existsSync(abs), false, 'original file should be gone');

    const trashed = path.join(root, TRASH_DIRNAME, todayUtc(), rel);
    assert.equal(fs.existsSync(trashed), true, 'trashed copy should exist');
    assert.equal(await fsp.readFile(trashed, 'utf8'), 'JAR-CONTENTS');
  } finally {
    await rimraf(root);
  }
});

test('moveToTrash handles missing source gracefully', async () => {
  const root = await makeTempFolder();
  try {
    const result = await moveToTrash(root, 'nope/missing.txt');
    assert.equal(result, false);
    // Trash dir should NOT have been created for a missing source.
    assert.equal(fs.existsSync(path.join(root, TRASH_DIRNAME)), false);
  } finally {
    await rimraf(root);
  }
});

test('moveToTrash de-duplicates colliding names', async () => {
  const root = await makeTempFolder();
  try {
    const rel = path.join('mods', 'dup.jar');
    const abs = path.join(root, rel);
    await fsp.mkdir(path.dirname(abs), { recursive: true });

    // First trashing
    await fsp.writeFile(abs, 'V1');
    assert.equal(await moveToTrash(root, rel), true);

    // Recreate same path, trash again
    await fsp.writeFile(abs, 'V2');
    assert.equal(await moveToTrash(root, rel), true);

    const trashedDir = path.join(root, TRASH_DIRNAME, todayUtc(), 'mods');
    const entries = (await fsp.readdir(trashedDir)).sort();
    assert.deepEqual(entries, ['dup.jar', 'dup.jar.1']);

    assert.equal(await fsp.readFile(path.join(trashedDir, 'dup.jar'), 'utf8'), 'V1');
    assert.equal(await fsp.readFile(path.join(trashedDir, 'dup.jar.1'), 'utf8'), 'V2');
  } finally {
    await rimraf(root);
  }
});

test('pruneTrash removes dirs older than retention', async () => {
  const root = await makeTempFolder();
  try {
    const trashRoot = path.join(root, TRASH_DIRNAME);
    const oldDate = '2026-01-01';
    const today = todayUtc();

    await fsp.mkdir(path.join(trashRoot, oldDate), { recursive: true });
    await fsp.writeFile(path.join(trashRoot, oldDate, 'x'), 'old');

    await fsp.mkdir(path.join(trashRoot, today), { recursive: true });
    await fsp.writeFile(path.join(trashRoot, today, 'y'), 'new');

    const removed = await pruneTrash(root, { retentionDays: 7 });
    assert.equal(removed, 1);
    assert.equal(fs.existsSync(path.join(trashRoot, oldDate)), false);
    assert.equal(fs.existsSync(path.join(trashRoot, today, 'y')), true);
  } finally {
    await rimraf(root);
  }
});

test('pruneTrash no-op when trash dir absent', async () => {
  const root = await makeTempFolder();
  try {
    const removed = await pruneTrash(root);
    assert.equal(removed, 0);
  } finally {
    await rimraf(root);
  }
});
