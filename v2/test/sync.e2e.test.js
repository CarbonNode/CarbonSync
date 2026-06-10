'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const fsp = require('node:fs/promises');
const path = require('node:path');
const {
  startFleet, write, treeOf, listConflicts, treesEqual, waitFor, waitForConverged, TOKEN,
} = require('./helpers');

const sleep = ms => new Promise(r => setTimeout(r, ms));

test('initial pull converges a nested tree larger than the page size', async t => {
  const { hub, spoke, hubData, spokeData } = await startFleet(t, { folders: [{ id: 'f1' }] });
  const root = hubData('f1');

  // 23 files > 4 pages of 5 — exercises the paged merge-join, plus unicode
  for (let i = 0; i < 20; i++) await write(root, `dir${i % 3}/sub/file-${i}.txt`, `content-${i}`);
  await write(root, 'top.txt', 'top');
  await write(root, 'ünïcode/ñandú.txt', 'south');
  await write(root, 'a.b', 'dot-vs-slash ordering probe');

  const tree = await waitForConverged(root, spokeData('f1'), 'initial pull');
  assert.equal(tree.size, 23);

  const status = spoke.getStatus().folders.find(f => f.id === 'f1');
  assert.equal(status.lastError, null);
  assert.equal(status.liveCount, 23);
  assert.equal(status.digest, hub.getStatus().folders.find(f => f.id === 'f1').digest);
});

test('two-way: live edits, creations and deletions propagate both directions', async t => {
  const { hubData, spokeData } = await startFleet(t, { folders: [{ id: 'f1' }] });
  const hubRoot = hubData('f1');
  const spokeRoot = spokeData('f1');

  await write(hubRoot, 'seed.txt', 'v1');
  await waitForConverged(hubRoot, spokeRoot, 'seed');

  // create on spoke (nested), edit on hub — watcher-driven, no manual triggers
  await write(spokeRoot, 'deep/sub/new-from-spoke.txt', 'hello hub');
  await write(hubRoot, 'seed.txt', 'v2-from-hub');
  let tree = await waitForConverged(hubRoot, spokeRoot, 'cross edits');
  assert.equal(tree.get('seed.txt'), 'v2-from-hub');
  assert.equal(tree.get('deep/sub/new-from-spoke.txt'), 'hello hub');

  // delete hub-side -> spoke loses it; delete spoke-side -> hub loses it
  await fsp.rm(path.join(hubRoot, 'seed.txt'));
  tree = await waitFor(async () => {
    const s = await treeOf(spokeRoot);
    return s.has('seed.txt') ? false : s;
  }, { desc: 'hub deletion to reach spoke' });
  assert.ok(tree.has('deep/sub/new-from-spoke.txt'));

  await fsp.rm(path.join(spokeRoot, 'deep/sub/new-from-spoke.txt'));
  await waitFor(async () => !(await treeOf(hubRoot)).has('deep/sub/new-from-spoke.txt'),
    { desc: 'spoke deletion to reach hub' });
  assert.ok(treesEqual(await treeOf(hubRoot), await treeOf(spokeRoot)));
});

test('concurrent edits resolve last-writer-wins and preserve the loser as a conflict copy', async t => {
  const { spoke, hubData, spokeData } = await startFleet(t, { folders: [{ id: 'f1' }] });
  const hubRoot = hubData('f1');
  const spokeRoot = spokeData('f1');

  await write(hubRoot, 'hub-wins.txt', 'base');
  await write(hubRoot, 'spoke-wins.txt', 'base');
  await waitForConverged(hubRoot, spokeRoot, 'seed');

  // pause the spoke's syncing, edit both sides of both files, resume
  spoke.setPause('f1', true);
  const T = Date.now();
  await write(hubRoot, 'hub-wins.txt', 'HUB NEWER', T);
  await write(spokeRoot, 'hub-wins.txt', 'spoke older', T - 60_000);
  await write(hubRoot, 'spoke-wins.txt', 'hub older', T - 60_000);
  await write(spokeRoot, 'spoke-wins.txt', 'SPOKE NEWER', T);
  await sleep(300); // let both watchers index the edits
  spoke.setPause('f1', false);

  const tree = await waitForConverged(hubRoot, spokeRoot, 'conflict resolution');
  assert.equal(tree.get('hub-wins.txt'), 'HUB NEWER');
  assert.equal(tree.get('spoke-wins.txt'), 'SPOKE NEWER');

  // losers preserved on the side that was overwritten
  const spokeConflicts = await listConflicts(spokeRoot);
  const hubConflicts = await listConflicts(hubRoot);
  assert.equal(spokeConflicts.filter(p => p.startsWith('hub-wins.conflict-')).length, 1);
  assert.equal(hubConflicts.filter(p => p.startsWith('spoke-wins.conflict-')).length, 1);
  assert.equal(await fsp.readFile(path.join(spokeRoot, spokeConflicts[0]), 'utf8'), 'spoke older');
  assert.equal(await fsp.readFile(path.join(hubRoot, hubConflicts[0]), 'utf8'), 'hub older');
});

test('pull mirror: hub is authoritative, spoke extras and edits are overwritten', async t => {
  const { hubData, spokeData } = await startFleet(t, { folders: [{ id: 'f1', mode: 'pull' }] });
  const hubRoot = hubData('f1');
  const spokeRoot = spokeData('f1');

  await write(hubRoot, 'keep.txt', 'authoritative');
  await waitForConverged(hubRoot, spokeRoot, 'seed');

  await write(spokeRoot, 'local-extra.txt', 'should vanish');
  await write(spokeRoot, 'keep.txt', 'local edit, should be clobbered');

  const tree = await waitFor(async () => {
    const s = await treeOf(spokeRoot);
    return s.get('keep.txt') === 'authoritative' && !s.has('local-extra.txt') ? s : false;
  }, { desc: 'pull mirror enforcement' });
  assert.equal(tree.size, 1);
});

test('push mirror: spoke is authoritative, hub extras are deleted', async t => {
  const { hubData, spokeData } = await startFleet(t, { folders: [{ id: 'f1', mode: 'push' }] });
  const hubRoot = hubData('f1');
  const spokeRoot = spokeData('f1');

  await write(hubRoot, 'hub-extra.txt', 'should vanish from hub');
  await write(spokeRoot, 'mine.txt', 'spoke content');

  const tree = await waitFor(async () => {
    const h = await treeOf(hubRoot);
    return h.get('mine.txt') === 'spoke content' && !h.has('hub-extra.txt') ? h : false;
  }, { desc: 'push mirror enforcement' });
  assert.equal(tree.size, 1);
});

test('delete-fraction guard blocks mass deletion until force_sync', async t => {
  const { spoke, hub, hubData, spokeData } = await startFleet(t, {
    folders: [{ id: 'f1' }],
    spokeTweaks: { guardMinDeletes: 3, guardFraction: 0.25 },
  });
  const hubRoot = hubData('f1');
  const spokeRoot = spokeData('f1');

  for (let i = 0; i < 12; i++) await write(hubRoot, `f-${i}.txt`, `v${i}`);
  await waitForConverged(hubRoot, spokeRoot, 'seed');

  // nuke 8/12 on the hub: 8 > max(3, ceil(0.25*12)=3) -> spoke must refuse
  for (let i = 0; i < 8; i++) await fsp.rm(path.join(hubRoot, `f-${i}.txt`));

  await waitFor(async () => {
    const st = spoke.getStatus().folders.find(f => f.id === 'f1');
    return st.guardTripped ? st : false;
  }, { desc: 'guard to trip' });

  assert.equal((await treeOf(spokeRoot)).size, 12, 'spoke must keep all files while blocked');
  const events = spoke.eventLog.recent({ limit: 50, folder: 'f1' });
  assert.ok(events.some(e => e.type === 'delete_guard'), 'delete_guard event recorded');

  // hold the block across further polls (no flapping), then force
  await sleep(500);
  assert.equal((await treeOf(spokeRoot)).size, 12);

  spoke.requestSync('f1', { force: true });
  const tree = await waitForConverged(hubRoot, spokeRoot, 'forced convergence');
  assert.equal(tree.size, 4);
  assert.equal(hub.getStatus().folders.find(f => f.id === 'f1').liveCount, 4);
});

test('missing folder root goes offline and never propagates as mass deletion', async t => {
  const { spoke, hubData, spokeData } = await startFleet(t, { folders: [{ id: 'f1' }] });
  const hubRoot = hubData('f1');
  const spokeRoot = spokeData('f1');

  for (let i = 0; i < 5; i++) await write(hubRoot, `f-${i}.txt`, `v${i}`);
  await waitForConverged(hubRoot, spokeRoot, 'seed');

  await fsp.rm(spokeRoot, { recursive: true, force: true }); // unmounted-drive simulation

  await waitFor(async () => {
    const st = spoke.getStatus().folders.find(f => f.id === 'f1');
    return st.offline ? st : false;
  }, { desc: 'folder to go offline' });

  await write(hubRoot, 'f-new.txt', 'written while spoke offline');
  await sleep(700); // several poll cycles

  const hubTree = await treeOf(hubRoot);
  assert.equal(hubTree.size, 6, 'hub must keep every file while the spoke root is missing');
});

test('localhost admin API: auth required, status and events served', async t => {
  const { spoke, hubData, spokeData } = await startFleet(t, { folders: [{ id: 'f1' }] });
  await write(hubData('f1'), 'x.txt', 'x');
  await waitForConverged(hubData('f1'), spokeData('f1'), 'seed');

  const base = `http://127.0.0.1:${spoke.servers.apiPort}`;
  const unauth = await fetch(`${base}/v1/status`);
  assert.equal(unauth.status, 401);

  const headers = { authorization: `Bearer ${TOKEN}` };
  const status = await (await fetch(`${base}/v1/status`, { headers })).json();
  assert.equal(status.role, 'spoke');
  assert.equal(status.folders[0].id, 'f1');
  assert.equal(status.hub.reachable, true);

  const events = await (await fetch(`${base}/v1/events?limit=20`, { headers })).json();
  assert.ok(events.events.length > 0);

  const handshake = await (await fetch(`${base}/v1/handshake`, { headers })).json();
  assert.equal(handshake.pv, 1);
});

test('idle fleet does NO sync work after convergence (anti-churn regression)', async t => {
  const { spoke, hubData, spokeData } = await startFleet(t, { folders: [{ id: 'f1' }] });
  await write(hubData('f1'), 'a.txt', 'x');
  await write(hubData('f1'), 'deep/b.txt', 'y');
  await waitForConverged(hubData('f1'), spokeData('f1'), 'seed');
  await sleep(500); // let post-convergence settling finish

  const evBefore = spoke.eventLog.recent({ limit: 1000 }).length;
  const before = spoke.getStatus().folders.find(f => f.id === 'f1');

  await sleep(1500); // ~10 poll cycles at 150ms — v1's bug was churn exactly here

  const evAfter = spoke.eventLog.recent({ limit: 1000 }).length;
  const after = spoke.getStatus().folders.find(f => f.id === 'f1');
  assert.equal(evAfter, evBefore, 'no new events while idle');
  assert.equal(after.lastSyncAt, before.lastSyncAt, 'no sync passes ran while idle');
  assert.equal(after.digest, before.digest, 'index untouched while idle');
});
