const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const fsp = require('fs/promises');
const path = require('path');
const os = require('os');

const { GameBackup } = require('../src/daemon/game-backup');
const { TRASH_DIRNAME } = require('../src/daemon/trash');

// ------------------------------------------------------------------
// Test helpers
// ------------------------------------------------------------------

async function makeTempDirs() {
  const configDir = await fsp.mkdtemp(path.join(os.tmpdir(), 'cs-gb-config-'));
  const gameSavesDir = path.join(configDir, 'game-saves');
  await fsp.mkdir(gameSavesDir, { recursive: true });
  return { configDir, gameSavesDir };
}

async function rimraf(dir) {
  try { await fsp.rm(dir, { recursive: true, force: true }); } catch {}
}

function makeConfig(settings = {}) {
  return { data: { settings } };
}

function makeBackup({ gameSavesDir, configDir, settings }) {
  const gb = new GameBackup({
    configDir,
    config: makeConfig(settings),
    gameSavesDir,
  });
  return gb;
}

function todayUtc() {
  return new Date().toISOString().slice(0, 10);
}

async function writeJson(p, obj) {
  await fsp.mkdir(path.dirname(p), { recursive: true });
  await fsp.writeFile(p, JSON.stringify(obj, null, 2));
}

// ------------------------------------------------------------------
// cleanBackups
// ------------------------------------------------------------------

test('cleanBackups removes Unity/ under current/ only, preserves backups/<timestamp>/Unity/', async () => {
  const { configDir, gameSavesDir } = await makeTempDirs();
  try {
    const gb = makeBackup({ configDir, gameSavesDir });

    // Build: Game1/current/Unity/foo.dat + save.dat
    //        Game1/backups/2026-04-01T00-00-00/Unity/foo.dat + save.dat
    const gameDir = path.join(gameSavesDir, 'Game1');
    const currentDir = path.join(gameDir, 'current');
    const snapshotDir = path.join(gameDir, 'backups', '2026-04-01T00-00-00');

    await fsp.mkdir(path.join(currentDir, 'Unity'), { recursive: true });
    await fsp.writeFile(path.join(currentDir, 'Unity', 'foo.dat'), 'cur-unity');
    await fsp.writeFile(path.join(currentDir, 'save.dat'), 'cur-save');

    await fsp.mkdir(path.join(snapshotDir, 'Unity'), { recursive: true });
    await fsp.writeFile(path.join(snapshotDir, 'Unity', 'foo.dat'), 'snap-unity');
    await fsp.writeFile(path.join(snapshotDir, 'save.dat'), 'snap-save');

    const result = await gb.cleanBackups();
    assert.equal(result.removed, 1, 'should have removed one Unity dir under current/');

    // current/Unity/ should be gone
    assert.equal(fs.existsSync(path.join(currentDir, 'Unity')), false, 'current/Unity should be removed');
    // current/save.dat survives
    assert.equal(fs.existsSync(path.join(currentDir, 'save.dat')), true, 'current/save.dat should survive');

    // backups/.../Unity/ must NOT be touched
    assert.equal(fs.existsSync(path.join(snapshotDir, 'Unity')), true, 'snapshot Unity must survive');
    assert.equal(fs.existsSync(path.join(snapshotDir, 'Unity', 'foo.dat')), true, 'snapshot Unity contents must survive');
    assert.equal(fs.existsSync(path.join(snapshotDir, 'save.dat')), true, 'snapshot save.dat must survive');

    // Deleted Unity dir ended up in trash
    const trashed = path.join(gameSavesDir, TRASH_DIRNAME, todayUtc(), 'Game1', 'current', 'Unity');
    assert.equal(fs.existsSync(trashed), true, 'Unity dir should have been moved to trash');
    assert.equal(fs.existsSync(path.join(trashed, 'foo.dat')), true, 'trashed Unity contents should be recoverable');
    assert.equal(
      await fsp.readFile(path.join(trashed, 'foo.dat'), 'utf8'),
      'cur-unity',
      'trashed file content matches original',
    );

    gb.stop();
  } finally {
    await rimraf(configDir);
  }
});

test('cleanBackups skips game-saves trash bucket itself', async () => {
  const { configDir, gameSavesDir } = await makeTempDirs();
  try {
    const gb = makeBackup({ configDir, gameSavesDir });

    // Pre-seed a fake trash dir that contains a "Unity" folder — must NOT
    // be walked into (starts with dot).
    const trashUnity = path.join(gameSavesDir, TRASH_DIRNAME, todayUtc(), 'SomeGame', 'current', 'Unity');
    await fsp.mkdir(trashUnity, { recursive: true });
    await fsp.writeFile(path.join(trashUnity, 'ghost.dat'), 'ghost');

    await gb.cleanBackups();

    assert.equal(fs.existsSync(trashUnity), true, 'trash bucket must not be re-processed');
    gb.stop();
  } finally {
    await rimraf(configDir);
  }
});

// ------------------------------------------------------------------
// removeGame
// ------------------------------------------------------------------

test('removeGame refuses empty name and empty id (would target gameSavesDir)', async () => {
  const { configDir, gameSavesDir } = await makeTempDirs();
  try {
    const gb = makeBackup({ configDir, gameSavesDir });

    // Put a marker file in gameSavesDir and a real game folder. After the
    // refused call, both must still exist.
    await fsp.writeFile(path.join(gameSavesDir, 'canary.txt'), 'hello');
    await fsp.mkdir(path.join(gameSavesDir, 'OtherGame'), { recursive: true });

    await assert.rejects(async () => gb.removeGame('', ''), /refusing empty/);

    assert.equal(fs.existsSync(path.join(gameSavesDir, 'canary.txt')), true, 'canary must survive');
    assert.equal(fs.existsSync(path.join(gameSavesDir, 'OtherGame')), true, 'other game must survive');
    assert.equal(fs.existsSync(gameSavesDir), true, 'gameSavesDir itself must survive');

    gb.stop();
  } finally {
    await rimraf(configDir);
  }
});

test('removeGame removes the matching game directory when given a valid name', async () => {
  const { configDir, gameSavesDir } = await makeTempDirs();
  try {
    const gb = makeBackup({ configDir, gameSavesDir });

    const gameId = 'someid-123';
    const idDir = path.join(gameSavesDir, gameId);
    await fsp.mkdir(path.join(idDir, 'current'), { recursive: true });
    await fsp.writeFile(path.join(idDir, 'current', 'save.dat'), 'hi');

    const otherDir = path.join(gameSavesDir, 'OtherGame');
    await fsp.mkdir(otherDir, { recursive: true });
    await fsp.writeFile(path.join(otherDir, 'save.dat'), 'hi');

    await gb.removeGame('SomeGame', gameId);

    assert.equal(fs.existsSync(idDir), false, 'target game dir should be gone');
    assert.equal(fs.existsSync(otherDir), true, 'other game dir must survive');
    assert.equal(fs.existsSync(gameSavesDir), true, 'gameSavesDir must survive');

    gb.stop();
  } finally {
    await rimraf(configDir);
  }
});

test('removeGame blocks path traversal via name', async () => {
  const { configDir, gameSavesDir } = await makeTempDirs();
  try {
    const gb = makeBackup({ configDir, gameSavesDir });

    // Put a sibling file OUTSIDE gameSavesDir that must not be touched.
    const outside = path.join(configDir, 'do-not-delete.txt');
    await fsp.writeFile(outside, 'precious');

    // Name '..' -> gameDir -> path.join(gameSavesDir, '..') -> parent (configDir)
    await assert.rejects(async () => gb.removeGame('..', ''), /unsafe path/);

    assert.equal(fs.existsSync(outside), true, 'sibling file must not be wiped');
    assert.equal(fs.existsSync(gameSavesDir), true, 'gameSavesDir must survive');

    gb.stop();
  } finally {
    await rimraf(configDir);
  }
});

test('removeGame blocks path traversal via gameId', async () => {
  const { configDir, gameSavesDir } = await makeTempDirs();
  try {
    const gb = makeBackup({ configDir, gameSavesDir });
    // gameId containing ../ would resolve outside gameSavesDir.
    await assert.rejects(async () => gb.removeGame('real-looking', '../escape'), /unsafe path/);
    assert.equal(fs.existsSync(gameSavesDir), true);
    gb.stop();
  } finally {
    await rimraf(configDir);
  }
});

// ------------------------------------------------------------------
// _assertSafePath direct tests
// ------------------------------------------------------------------

test('_assertSafePath accepts a nested path under root', async () => {
  const { configDir, gameSavesDir } = await makeTempDirs();
  try {
    const gb = makeBackup({ configDir, gameSavesDir });
    // Should not throw
    gb._assertSafePath(path.join(gameSavesDir, 'GameA', 'current'), gameSavesDir);
    gb.stop();
  } finally {
    await rimraf(configDir);
  }
});

test('_assertSafePath rejects root itself', async () => {
  const { configDir, gameSavesDir } = await makeTempDirs();
  try {
    const gb = makeBackup({ configDir, gameSavesDir });
    assert.throws(() => gb._assertSafePath(gameSavesDir, gameSavesDir), /would target root/);
    gb.stop();
  } finally {
    await rimraf(configDir);
  }
});

test('_assertSafePath rejects parent directory', async () => {
  const { configDir, gameSavesDir } = await makeTempDirs();
  try {
    const gb = makeBackup({ configDir, gameSavesDir });
    const parent = path.dirname(gameSavesDir);
    assert.throws(() => gb._assertSafePath(parent, gameSavesDir), /not under/);
    gb.stop();
  } finally {
    await rimraf(configDir);
  }
});

test('_assertSafePath rejects empty/nullish dir', async () => {
  const { configDir, gameSavesDir } = await makeTempDirs();
  try {
    const gb = makeBackup({ configDir, gameSavesDir });
    assert.throws(() => gb._assertSafePath('', gameSavesDir), /empty/);
    assert.throws(() => gb._assertSafePath(null, gameSavesDir), /empty/);
    gb.stop();
  } finally {
    await rimraf(configDir);
  }
});

// ------------------------------------------------------------------
// _enforceRetention
// ------------------------------------------------------------------

test('_enforceRetention sorts by metadata.timestamp, pre-restore does not evict regular', async () => {
  const { configDir, gameSavesDir } = await makeTempDirs();
  try {
    const gb = makeBackup({
      configDir,
      gameSavesDir,
      settings: { gameSaveMaxVersions: 5 },
    });

    const gameDir = path.join(gameSavesDir, 'Game1');
    const backupsDir = path.join(gameDir, 'backups');
    await fsp.mkdir(backupsDir, { recursive: true });

    // 12 regular ISO-timestamp backups (all RECENT — within last hour).
    // They're created in reverse order so directory-name sorting would
    // not match timestamp-based sorting unless explicitly requested.
    const now = Date.now();
    const regularNames = [];
    for (let i = 0; i < 12; i++) {
      // Give each a unique ISO timestamp spaced 1 minute apart.
      const ts = new Date(now - (12 - i) * 60 * 1000).toISOString();
      const dirName = ts.replace(/[:.]/g, '-');
      const dir = path.join(backupsDir, dirName);
      await fsp.mkdir(dir, { recursive: true });
      await writeJson(path.join(dir, '_meta.json'), {
        gameId: 'g1',
        gameName: 'Game1',
        timestamp: ts,
        fileCount: 1,
        totalSize: 100,
      });
      await fsp.writeFile(path.join(dir, 'save.dat'), `regular-${i}`);
      regularNames.push({ dirName, ts });
    }

    // 5 pre-restore snapshots — their directory NAMES sort AFTER "2" prefixed
    // ISO names because 'p' > '2'. So string-sort-by-name would keep all 5
    // pre-restores and evict 7 regular backups. We want the opposite.
    const preRestoreNames = [];
    for (let i = 0; i < 5; i++) {
      // Give them OLDER timestamps than the regulars to prove
      // metadata.timestamp (not name) drives the sort.
      const ts = new Date(now - (100 + i) * 60 * 1000).toISOString();
      const dirName = 'pre-restore-' + ts.replace(/[:.]/g, '-');
      const dir = path.join(backupsDir, dirName);
      await fsp.mkdir(dir, { recursive: true });
      await writeJson(path.join(dir, '_meta.json'), {
        gameId: 'g1',
        gameName: 'Game1',
        timestamp: ts,
        preRestore: true,
        fileCount: 1,
        totalSize: 100,
      });
      preRestoreNames.push({ dirName, ts });
    }

    await gb._enforceRetention(backupsDir);

    // Newest 5 regular backups must survive.
    // Regulars were added in order; index 7..11 are the newest 5 by timestamp.
    const survivingRegulars = regularNames.slice(-5).map(r => r.dirName);
    for (const n of survivingRegulars) {
      assert.equal(
        fs.existsSync(path.join(backupsDir, n)),
        true,
        `newest regular ${n} must survive`,
      );
    }
    // Oldest 7 regulars were surplus; with the 24h-old protection, an
    // oldest->24h backup may be retained too. In this test all are <1h old,
    // so NO old-protection applies — 7 must be gone.
    const evictedRegulars = regularNames.slice(0, 7).map(r => r.dirName);
    for (const n of evictedRegulars) {
      assert.equal(
        fs.existsSync(path.join(backupsDir, n)),
        false,
        `old regular ${n} should have been evicted`,
      );
    }

    // Pre-restore: keep newest PRE_RESTORE_RETENTION (3). Newest 3 by ts.
    const sortedPr = preRestoreNames
      .slice()
      .sort((a, b) => (a.ts < b.ts ? -1 : 1));
    const survivingPr = sortedPr.slice(-3).map(r => r.dirName);
    const evictedPr = sortedPr.slice(0, sortedPr.length - 3).map(r => r.dirName);
    for (const n of survivingPr) {
      assert.equal(fs.existsSync(path.join(backupsDir, n)), true, `newest pre-restore ${n} must survive`);
    }
    for (const n of evictedPr) {
      assert.equal(fs.existsSync(path.join(backupsDir, n)), false, `old pre-restore ${n} must be evicted`);
    }

    // Evicted backups must have landed in the game-saves trash.
    const trashRoot = path.join(gameSavesDir, TRASH_DIRNAME, todayUtc(), 'Game1', 'backups');
    const trashEntries = fs.existsSync(trashRoot) ? await fsp.readdir(trashRoot) : [];
    for (const n of evictedRegulars) {
      assert.ok(trashEntries.includes(n), `evicted regular ${n} should be in trash (got ${JSON.stringify(trashEntries)})`);
    }
    for (const n of evictedPr) {
      assert.ok(trashEntries.includes(n), `evicted pre-restore ${n} should be in trash`);
    }

    gb.stop();
  } finally {
    await rimraf(configDir);
  }
});

test('_enforceRetention protects the oldest backup that is >24h old', async () => {
  const { configDir, gameSavesDir } = await makeTempDirs();
  try {
    const gb = makeBackup({
      configDir,
      gameSavesDir,
      settings: { gameSaveMaxVersions: 3 },
    });

    const gameDir = path.join(gameSavesDir, 'Game1');
    const backupsDir = path.join(gameDir, 'backups');
    await fsp.mkdir(backupsDir, { recursive: true });

    const now = Date.now();

    // 5 backups: one 48h old, 4 recent. maxVersions=3 → would evict 2.
    // The 48h-old one must survive (protected).
    const specs = [
      { ageMs: 48 * 60 * 60 * 1000, tag: 'ancient' },
      { ageMs: 30 * 60 * 1000, tag: 'r1' },
      { ageMs: 20 * 60 * 1000, tag: 'r2' },
      { ageMs: 10 * 60 * 1000, tag: 'r3' },
      { ageMs: 5 * 60 * 1000, tag: 'r4' },
    ];
    for (const s of specs) {
      const ts = new Date(now - s.ageMs).toISOString();
      s.dirName = ts.replace(/[:.]/g, '-');
      s.ts = ts;
      const dir = path.join(backupsDir, s.dirName);
      await fsp.mkdir(dir, { recursive: true });
      await writeJson(path.join(dir, '_meta.json'), { timestamp: ts, gameId: 'g1' });
    }

    await gb._enforceRetention(backupsDir);

    const ancient = specs.find(s => s.tag === 'ancient');
    assert.equal(fs.existsSync(path.join(backupsDir, ancient.dirName)), true,
      'oldest >24h backup must be protected');

    // Newest 3 (r2, r3, r4) survive.
    for (const tag of ['r2', 'r3', 'r4']) {
      const s = specs.find(x => x.tag === tag);
      assert.equal(fs.existsSync(path.join(backupsDir, s.dirName)), true,
        `${tag} should survive`);
    }

    gb.stop();
  } finally {
    await rimraf(configDir);
  }
});

test('_enforceRetention clamps gameSaveMaxVersions below floor (1 → 3)', async () => {
  const { configDir, gameSavesDir } = await makeTempDirs();
  try {
    const gb = makeBackup({
      configDir,
      gameSavesDir,
      settings: { gameSaveMaxVersions: 1 }, // aggressive — should clamp to 3
    });

    const gameDir = path.join(gameSavesDir, 'Game1');
    const backupsDir = path.join(gameDir, 'backups');
    await fsp.mkdir(backupsDir, { recursive: true });

    const now = Date.now();
    const names = [];
    for (let i = 0; i < 5; i++) {
      const ts = new Date(now - (5 - i) * 60 * 1000).toISOString();
      const dirName = ts.replace(/[:.]/g, '-');
      await fsp.mkdir(path.join(backupsDir, dirName), { recursive: true });
      await writeJson(path.join(backupsDir, dirName, '_meta.json'), { timestamp: ts, gameId: 'g1' });
      names.push(dirName);
    }

    await gb._enforceRetention(backupsDir);

    // With floor=3, we expect 3 survivors (newest 3), not 1.
    const survivors = names.slice(-3);
    for (const n of survivors) {
      assert.equal(fs.existsSync(path.join(backupsDir, n)), true, `${n} should survive (floor=3)`);
    }
    // 2 evictions.
    const evicted = names.slice(0, 2);
    for (const n of evicted) {
      assert.equal(fs.existsSync(path.join(backupsDir, n)), false, `${n} should be evicted`);
    }

    gb.stop();
  } finally {
    await rimraf(configDir);
  }
});

test('_enforceRetention falls back to directory name when metadata missing', async () => {
  const { configDir, gameSavesDir } = await makeTempDirs();
  try {
    const gb = makeBackup({
      configDir,
      gameSavesDir,
      settings: { gameSaveMaxVersions: 3 },
    });

    const gameDir = path.join(gameSavesDir, 'Game1');
    const backupsDir = path.join(gameDir, 'backups');
    await fsp.mkdir(backupsDir, { recursive: true });

    // 5 backups named by ISO-ish timestamps, NO metadata.json.
    const names = [
      '2026-01-01T00-00-00',
      '2026-02-01T00-00-00',
      '2026-03-01T00-00-00',
      '2026-04-01T00-00-00',
      '2026-05-01T00-00-00',
    ];
    for (const n of names) {
      await fsp.mkdir(path.join(backupsDir, n), { recursive: true });
      await fsp.writeFile(path.join(backupsDir, n, 'save.dat'), n);
    }

    await gb._enforceRetention(backupsDir);

    // Newest 3 (by name) survive.
    assert.equal(fs.existsSync(path.join(backupsDir, '2026-03-01T00-00-00')), true);
    assert.equal(fs.existsSync(path.join(backupsDir, '2026-04-01T00-00-00')), true);
    assert.equal(fs.existsSync(path.join(backupsDir, '2026-05-01T00-00-00')), true);
    // Oldest 2 evicted.
    assert.equal(fs.existsSync(path.join(backupsDir, '2026-01-01T00-00-00')), false);
    assert.equal(fs.existsSync(path.join(backupsDir, '2026-02-01T00-00-00')), false);

    gb.stop();
  } finally {
    await rimraf(configDir);
  }
});
