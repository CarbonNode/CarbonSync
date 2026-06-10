'use strict';

/**
 * Manual scale probe: N-file initial sync through the real HTTP path, with
 * RSS sampling. Validates the bounded-memory design goal (v1 held full index
 * arrays in RAM; Patreon = 168k files ≈ 50 MB JSON per folder, per peer).
 *
 *   node scripts/scale-check.js          # N=10000
 *   N=50000 node scripts/scale-check.js
 */

const fsp = require('node:fs/promises');
const path = require('node:path');
const { startFleet, write, waitFor } = require('../test/helpers');

const N = Number(process.env.N || 10_000);

async function main() {
  const cleanups = [];
  const t = { after: fn => cleanups.push(fn) };
  const tweaks = { pageSize: 1000, transferConcurrency: 8, pollIntervalMs: 200, debounceMs: 100 };
  const { hub, spoke, hubData } = await startFleet(t, {
    folders: [{ id: 'big' }],
    hubTweaks: tweaks,
    spokeTweaks: tweaks,
  });

  console.log(`seeding ${N} files...`);
  const root = hubData('big');
  for (let d = 0; d < Math.ceil(N / 500); d++) {
    await fsp.mkdir(path.join(root, `dir-${d}`), { recursive: true });
  }
  for (let i = 0; i < N; i++) {
    await write(root, `dir-${Math.floor(i / 500)}/file-${i}.dat`, `payload-${i}\n`);
  }

  let peakRss = 0;
  const sampler = setInterval(() => {
    peakRss = Math.max(peakRss, process.memoryUsage.rss());
  }, 100);

  console.log('waiting for convergence...');
  const t0 = Date.now();
  const progress = setInterval(() => {
    const h = hub.getStatus().folders[0];
    const s = spoke.getStatus().folders[0];
    console.log(`  t=${((Date.now() - t0) / 1000).toFixed(0)}s hub:{ready:${h.ready},live:${h.liveCount}} spoke:{ready:${s.ready},live:${s.liveCount},syncing:${s.syncing},err:${s.lastError}}`);
  }, 2000);
  progress.unref();
  await waitFor(() => {
    const h = hub.getStatus().folders[0];
    const s = spoke.getStatus().folders[0];
    return h.ready && s.ready && s.liveCount === N && s.digest && s.digest === h.digest;
  }, { timeout: 15 * 60 * 1000, interval: 500, desc: `${N}-file convergence` });
  const elapsed = Date.now() - t0;

  clearInterval(progress);
  clearInterval(sampler);
  console.log(`converged ${N} files in ${(elapsed / 1000).toFixed(1)}s`);
  console.log(`peak RSS (hub + spoke + harness in ONE process): ${(peakRss / 1024 / 1024).toFixed(0)} MB`);

  for (const fn of cleanups.reverse()) await fn();
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
