'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const fsp = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');
const zlib = require('node:zlib');

const { run, parseModName, isLikelyModJar } = require('../tools/recover-mod-list');

async function makeTempInstance() {
  const dir = await fsp.mkdtemp(path.join(os.tmpdir(), 'recover-mod-test-'));
  await fsp.mkdir(path.join(dir, 'logs'), { recursive: true });
  await fsp.mkdir(path.join(dir, 'mods'), { recursive: true });
  return dir;
}

async function rimraf(dir) {
  try {
    await fsp.rm(dir, { recursive: true, force: true });
  } catch {
    /* ignore */
  }
}

test('parseModName extracts name + version from common shapes', () => {
  assert.deepEqual(parseModName('create-1.21.1-6.0.9.jar'), {
    name: 'create',
    version: '1.21.1-6.0.9',
  });
  assert.deepEqual(parseModName('moonlight-1.21-2.29.20-neoforge.jar'), {
    name: 'moonlight',
    version: '1.21-2.29.20-neoforge',
  });
  assert.deepEqual(parseModName('something-without-version.jar'), {
    name: 'something-without-version',
    version: '',
  });
});

test('isLikelyModJar filters launcher / library jars', () => {
  assert.equal(isLikelyModJar('client.jar', ''), false);
  assert.equal(isLikelyModJar('1.21.1-client.jar', 'versions/1.21.1/1.21.1-client.jar'), false);
  assert.equal(isLikelyModJar('neoforge-21.1.x-client.jar', 'libraries/net/neoforged/'), false);
  assert.equal(isLikelyModJar('create-1.21.1-6.0.9.jar', 'Loading create-1.21.1-6.0.9.jar'), true);
});

test('run() classifies mods from latest.log: present vs missing, filters launcher jars', async () => {
  const dir = await makeTempInstance();
  try {
    const logContent = [
      '[22:48:01] [Worker-Main-3/INFO]: Found mod file create-1.21.1-6.0.9.jar',
      '[22:48:01] [Worker-Main-3/INFO]: Loading moonlight-1.21-2.29.20-neoforge.jar',
      '[22:48:02] [Worker-Main-3/INFO]: Loading something-without-version.jar',
      'versions/1.21.1/1.21.1-client.jar',
      'libraries/net/neoforged/neoforge/21.1.x/neoforge-21.1.x-client.jar',
      '',
    ].join('\n');
    await fsp.writeFile(path.join(dir, 'logs', 'latest.log'), logContent, 'utf8');

    // Only one of the historically-loaded mods is still present.
    await fsp.writeFile(path.join(dir, 'mods', 'create-1.21.1-6.0.9.jar'), 'CREATE');

    const outPath = path.join(dir, 'out.md');
    const result = run({ instanceDir: dir, outPath });

    assert.equal(result.totals.latest, 1);
    assert.equal(result.totals.rotated, 0);
    assert.equal(result.totals.all, 3);
    assert.equal(result.totals.present, 1);
    assert.equal(result.totals.missing, 2);

    const md = await fsp.readFile(outPath, 'utf8');

    // Present mod appears in present section.
    const presentSectionStart = md.indexOf('## Currently present');
    const missingSectionStart = md.indexOf('## Missing mods');
    const notesStart = md.indexOf('## Notes');
    assert.ok(presentSectionStart > 0);
    assert.ok(missingSectionStart > 0);
    assert.ok(notesStart > presentSectionStart);

    const missingSection = md.slice(missingSectionStart, presentSectionStart);
    const presentSection = md.slice(presentSectionStart, notesStart);

    assert.match(presentSection, /create-1\.21\.1-6\.0\.9\.jar/);
    assert.match(missingSection, /moonlight-1\.21-2\.29\.20-neoforge\.jar/);
    assert.match(missingSection, /something-without-version\.jar/);

    // Filtered jars must not appear anywhere.
    assert.equal(md.includes('1.21.1-client.jar'), false);
    assert.equal(md.includes('neoforge-21.1.x-client.jar'), false);
  } finally {
    await rimraf(dir);
  }
});

test('run() picks up mods from rotated *.log.gz files', async () => {
  const dir = await makeTempInstance();
  try {
    // Empty latest.log so we know the gz file is the source.
    await fsp.writeFile(path.join(dir, 'logs', 'latest.log'), '', 'utf8');

    const gzContent = [
      '[10:00:00] [main/INFO]: Found mod file jei-1.21.1-19.21.0.247.jar',
      '[10:00:01] [main/INFO]: Loading sodium-neoforge-0.6.0.jar',
      '',
    ].join('\n');
    const gz = zlib.gzipSync(Buffer.from(gzContent, 'utf8'));
    await fsp.writeFile(path.join(dir, 'logs', '2026-04-19-1.log.gz'), gz);

    const outPath = path.join(dir, 'out.md');
    const result = run({ instanceDir: dir, outPath });

    assert.equal(result.totals.rotated, 1);
    assert.equal(result.totals.all, 2);
    assert.equal(result.totals.present, 0);
    assert.equal(result.totals.missing, 2);

    const md = fs.readFileSync(outPath, 'utf8');
    assert.match(md, /jei-1\.21\.1-19\.21\.0\.247\.jar/);
    assert.match(md, /sodium-neoforge-0\.6\.0\.jar/);
  } finally {
    await rimraf(dir);
  }
});

test('run() honors mods/_disabled as also "present"', async () => {
  const dir = await makeTempInstance();
  try {
    await fsp.writeFile(
      path.join(dir, 'logs', 'latest.log'),
      '[INFO]: Loading createaddition-1.5.1.jar\n',
      'utf8',
    );
    await fsp.mkdir(path.join(dir, 'mods', '_disabled'), { recursive: true });
    await fsp.writeFile(path.join(dir, 'mods', '_disabled', 'createaddition-1.5.1.jar'), 'X');

    const result = run({ instanceDir: dir, outPath: path.join(dir, 'out.md'), write: false });
    assert.equal(result.totals.present, 1);
    assert.equal(result.totals.missing, 0);
  } finally {
    await rimraf(dir);
  }
});

test('run() prefers structured "Found mod file" entries from mods folder locator', async () => {
  const dir = await makeTempInstance();
  try {
    // This log mixes:
    //   - real mod from mods folder locator   (should be counted)
    //   - inner jar from sinytra connector    (should NOT be counted)
    //   - neoforge universal from PathBased   (should NOT be counted)
    const logContent = [
      '[22:40:58] [main/INFO]: Found mod file "create-1.21.1-6.0.9.jar" [locator: {mods folder locator at C:\\foo\\mods}, reader: mod manifest]',
      '[22:40:58] [main/INFO]: Found mod file "alloy-forgery-2.4.1+1.21_mapped_moj_1.21.1.jar" [locator: org.sinytra.connector.locator.ConnectorLocator@7caa550]',
      '[22:40:58] [main/INFO]: Found mod file "neoforge-21.1.219-universal.jar" [locator: PathBasedLocator[name=neoforge]]',
      '',
    ].join('\n');
    await fsp.writeFile(path.join(dir, 'logs', 'latest.log'), logContent, 'utf8');

    const result = run({ instanceDir: dir, outPath: path.join(dir, 'out.md'), write: false });
    assert.equal(result.totals.all, 1);
    assert.match(result.markdown, /create-1\.21\.1-6\.0\.9\.jar/);
    assert.equal(result.markdown.includes('alloy-forgery'), false);
    assert.equal(result.markdown.includes('neoforge-21.1.219-universal'), false);
  } finally {
    await rimraf(dir);
  }
});

test('run() throws when instance dir does not exist', () => {
  assert.throws(
    () => run({ instanceDir: path.join(os.tmpdir(), 'definitely-not-a-real-dir-xyz-12345') }),
    /does not exist/,
  );
});
