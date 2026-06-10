'use strict';

const fs = require('node:fs');
const fsp = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');
const pino = require('pino');
const { loadConfig } = require('../src/config');
const { Daemon } = require('../src/daemon');

const TOKEN = 'test-fleet-token-0123456789abcdef';

const FAST = {
  tls: false,
  dataPort: 0,
  apiPort: 0,
  pollIntervalMs: 150,
  debounceMs: 80,
  pageSize: 5, // tiny pages so >20-file tests exercise the paged merge-join
  rescanIntervalMs: 60 * 60 * 1000,
  rootCheckIntervalMs: 250,
};

function logger() {
  return pino({ level: process.env.TEST_LOG_LEVEL || 'silent' });
}

/**
 * Boots a real hub + spoke (full daemons, real HTTP, real watchers) against
 * temp dirs. folders: [{ id, mode, hubMode }] — mode is the spoke's.
 */
async function startFleet(t, { folders, hubTweaks = {}, spokeTweaks = {} }) {
  const base = await fsp.mkdtemp(path.join(os.tmpdir(), 'csync2-'));
  const hubData = id => path.join(base, 'data', 'hub', id);
  const spokeData = id => path.join(base, 'data', 'spoke', id);

  const folderDefs = folders.map(f => {
    fs.mkdirSync(hubData(f.id), { recursive: true });
    fs.mkdirSync(spokeData(f.id), { recursive: true });
    return {
      id: f.id,
      name: f.id,
      devices: {
        hub: { path: hubData(f.id), mode: f.hubMode || 'two-way' },
        spoke1: { path: spokeData(f.id), mode: f.mode || 'two-way' },
      },
    };
  });

  const hubCfgPath = path.join(base, 'hub', 'config.json');
  fs.mkdirSync(path.dirname(hubCfgPath), { recursive: true });
  fs.writeFileSync(hubCfgPath, JSON.stringify({
    role: 'hub', deviceName: 'hub', token: TOKEN, ...FAST, folders: folderDefs, ...hubTweaks,
  }));
  const hub = new Daemon(loadConfig(hubCfgPath), logger());
  const { dataPort } = await hub.start();

  const spokeCfgPath = path.join(base, 'spoke', 'config.json');
  fs.mkdirSync(path.dirname(spokeCfgPath), { recursive: true });
  fs.writeFileSync(spokeCfgPath, JSON.stringify({
    role: 'spoke', deviceName: 'spoke1', token: TOKEN, hubUrl: `http://127.0.0.1:${dataPort}`,
    ...FAST, ...spokeTweaks,
  }));
  const spoke = new Daemon(loadConfig(spokeCfgPath), logger());
  await spoke.start();

  t.after(async () => {
    await spoke.stop().catch(() => {});
    await hub.stop().catch(() => {});
    await fsp.rm(base, { recursive: true, force: true });
  });

  return { base, hub, spoke, hubData, spokeData, spokeCfgPath, token: TOKEN };
}

async function write(root, rel, content, mtimeMs) {
  const abs = path.join(root, ...rel.split('/'));
  await fsp.mkdir(path.dirname(abs), { recursive: true });
  await fsp.writeFile(abs, content);
  if (mtimeMs) {
    const when = new Date(mtimeMs);
    await fsp.utimes(abs, when, when);
  }
}

function isArtifact(rel) {
  return rel.includes('.conflict-') || rel.includes('.carbonsync.tmp-');
}

/** rel -> content for every regular file, minus sync artifacts. */
async function treeOf(root) {
  const out = new Map();
  const stack = [''];
  while (stack.length) {
    const relDir = stack.pop();
    let entries;
    try {
      entries = await fsp.readdir(path.join(root, relDir), { withFileTypes: true });
    } catch {
      continue;
    }
    for (const e of entries) {
      const rel = relDir ? `${relDir}/${e.name}` : e.name;
      if (e.isDirectory()) stack.push(rel);
      else if (e.isFile() && !isArtifact(rel)) {
        try {
          out.set(rel, await fsp.readFile(path.join(root, rel), 'utf8'));
        } catch { /* deleted mid-walk by an in-flight sync — next poll re-walks */ }
      }
    }
  }
  return out;
}

async function listConflicts(root) {
  const out = [];
  const stack = [''];
  while (stack.length) {
    const relDir = stack.pop();
    let entries;
    try {
      entries = await fsp.readdir(path.join(root, relDir), { withFileTypes: true });
    } catch {
      continue;
    }
    for (const e of entries) {
      const rel = relDir ? `${relDir}/${e.name}` : e.name;
      if (e.isDirectory()) stack.push(rel);
      else if (e.isFile() && rel.includes('.conflict-')) out.push(rel);
    }
  }
  return out;
}

function treesEqual(a, b) {
  if (a.size !== b.size) return false;
  for (const [k, v] of a) {
    if (b.get(k) !== v) return false;
  }
  return true;
}

async function waitFor(fn, { timeout = 15_000, interval = 100, desc = 'condition' } = {}) {
  const deadline = Date.now() + timeout;
  let last;
  while (Date.now() < deadline) {
    last = await fn();
    if (last) return last;
    await new Promise(r => setTimeout(r, interval));
  }
  throw new Error(`timeout waiting for ${desc} (last=${JSON.stringify(last)})`);
}

function waitForConverged(hubRoot, spokeRoot, desc) {
  return waitFor(async () => {
    const [a, b] = await Promise.all([treeOf(hubRoot), treeOf(spokeRoot)]);
    return treesEqual(a, b) ? a : false;
  }, { desc: desc || 'trees to converge' });
}

module.exports = { startFleet, write, treeOf, listConflicts, treesEqual, waitFor, waitForConverged, TOKEN };
