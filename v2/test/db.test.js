'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { FolderIndex, cmpPath } = require('../src/db');

function tmp(t) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'csyncdb-'));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  return dir;
}

test('idle re-apply writes nothing and keeps the digest seq stable', t => {
  const db = new FolderIndex(tmp(t), 'f');
  t.after(() => db.close());
  const rows = [{ path: 'a.txt', size: 3, mtimeMs: 4000 }, { path: 'b/c.txt', size: 5, mtimeMs: 6000 }];
  assert.equal(db.applyLocal(rows), 2);
  const d1 = db.digest();
  assert.equal(db.applyLocal(rows), 0); // the v2.7.11 lesson, as a regression test
  assert.equal(db.digest(), d1); // memoized object — no recompute happened
});

test('digest survives reopen, ignores tombstones, buckets mtime', t => {
  const dir = tmp(t);
  let db = new FolderIndex(dir, 'f');
  db.applyLocal([{ path: 'a', size: 1, mtimeMs: 4100 }, { path: 'b', size: 2, mtimeMs: 9000 }]);
  const full = db.digest().hex;
  db.close();

  db = new FolderIndex(dir, 'f');
  t.after(() => db.close());
  assert.equal(db.digest().hex, full);

  // same 2s bucket -> same digest; different bucket -> different digest
  db.applyLocal([{ path: 'a', size: 1, mtimeMs: 4900 }]);
  assert.equal(db.digest().hex, full);
  db.applyLocal([{ path: 'a', size: 1, mtimeMs: 6100 }]);
  assert.notEqual(db.digest().hex, full);
  db.applyLocal([{ path: 'a', size: 1, mtimeMs: 4100 }]);
  assert.equal(db.digest().hex, full);

  db.markDeleted(['b']);
  const afterDelete = db.digest();
  assert.equal(afterDelete.count, 1);
  db.purgeTombstones(Date.now() + 1000);
  assert.equal(db.digest().hex, afterDelete.hex); // GC may not affect the digest
  assert.equal(db.page('', 10).length, 1);
});

test('pages are path-ordered consistently with cmpPath, tombstones included', t => {
  const db = new FolderIndex(tmp(t), 'f');
  t.after(() => db.close());
  const paths = ['a/b', 'a.b', 'a', 'z', 'a/b/c', 'b'];
  db.applyLocal(paths.map(p => ({ path: p, size: 1, mtimeMs: 1000 })));
  db.markDeleted(['z']);

  const all = [];
  let after = '';
  for (;;) {
    const page = db.page(after, 2);
    if (!page.length) break;
    all.push(...page);
    after = page[page.length - 1].path;
  }
  assert.deepEqual(all.map(r => r.path), [...paths].sort(cmpPath));
  assert.equal(all.find(r => r.path === 'z').deleted, 1);
});

test('synced markers do not disturb the digest', t => {
  const db = new FolderIndex(tmp(t), 'f');
  t.after(() => db.close());
  db.applyLocal([{ path: 'a', size: 1, mtimeMs: 4000 }]);
  const d = db.digest().hex;
  db.setSyncedMarkers('a', 1, 4000);
  assert.equal(db.digest().hex, d);
  const row = db.get('a');
  assert.equal(row.syncedSize, 1);
  assert.equal(row.syncedMtimeMs, 4000);
});
