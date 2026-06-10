'use strict';

const fs = require('node:fs');
const fsp = require('node:fs/promises');
const path = require('node:path');
const crypto = require('node:crypto');
const { cmpPath, MTIME_BUCKET_MS } = require('./db');
const { safeResolve, makeConflictName } = require('./server');

function near(a, b) {
  return a != null && b != null && Math.abs(a - b) <= MTIME_BUCKET_MS;
}

function sameFile(l, h) {
  return l.size === h.size && near(l.mtimeMs, h.mtimeMs);
}

function markersMatch(row, size, mtimeMs) {
  return row.syncedSize != null && row.syncedSize === size && near(row.syncedMtimeMs, mtimeMs);
}

/**
 * Streamed merge-join over two path-ordered cursors (local SQLite pages, hub
 * HTTP pages). Yields sync ops incrementally — neither index is ever
 * materialized, so a 168k-file folder costs pages, not gigabytes.
 */
async function* mergeOps(rt, hub, pageSize) {
  let localBuf = [], hubBuf = [];
  let localAfter = '', hubAfter = '';
  let localDone = false, hubDone = false;

  const fillLocal = () => {
    if (localDone || localBuf.length) return;
    localBuf = rt.db.page(localAfter, pageSize);
    if (localBuf.length) localAfter = localBuf[localBuf.length - 1].path;
    else localDone = true;
  };
  const fillHub = async () => {
    if (hubDone || hubBuf.length) return;
    const { rows } = await hub.indexPage(rt.id, hubAfter, pageSize);
    hubBuf = rows;
    if (hubBuf.length) hubAfter = hubBuf[hubBuf.length - 1].path;
    else hubDone = true;
  };

  while (true) {
    fillLocal();
    await fillHub();
    const l = localBuf[0];
    const h = hubBuf[0];
    if (!l && !h) return;

    let op = null;
    if (l && (!h || cmpPath(l.path, h.path) < 0)) {
      localBuf.shift();
      op = localOnly(rt.mode, l);
    } else if (h && (!l || cmpPath(h.path, l.path) < 0)) {
      hubBuf.shift();
      op = hubOnly(rt.mode, h);
    } else {
      localBuf.shift();
      hubBuf.shift();
      op = bothSides(rt.mode, l, h);
    }
    if (op) yield op;
  }
}

function localOnly(mode, l) {
  if (l.deleted) return null; // tombstone with no hub row: nothing to do
  if (mode === 'pull') return { type: 'delLocal', path: l.path };
  return { type: 'push', path: l.path, size: l.size, mtimeMs: l.mtimeMs };
}

function hubOnly(mode, h) {
  if (h.deleted) return null;
  if (mode === 'push') return { type: 'delHub', path: h.path };
  return { type: 'pull', path: h.path, size: h.size };
}

function bothSides(mode, l, h) {
  if (l.deleted && h.deleted) return null;

  if (!l.deleted && !h.deleted) {
    if (sameFile(l, h)) {
      if (!markersMatch(l, l.size, l.mtimeMs)) {
        return { type: 'markSynced', path: l.path, size: l.size, mtimeMs: l.mtimeMs };
      }
      return null;
    }
    if (mode === 'pull') return { type: 'pull', path: l.path, size: h.size };
    if (mode === 'push') return { type: 'push', path: l.path, size: l.size, mtimeMs: l.mtimeMs };

    const localChanged = !markersMatch(l, l.size, l.mtimeMs);
    const hubChanged = !markersMatch(l, h.size, h.mtimeMs);
    if (localChanged && !hubChanged) return { type: 'push', path: l.path, size: l.size, mtimeMs: l.mtimeMs };
    if (!localChanged && hubChanged) return { type: 'pull', path: l.path, size: h.size };

    // Changed on both sides since last sync (or never synced): last writer
    // wins; the losing copy is always preserved as a *.conflict-* file.
    if (l.mtimeMs > h.mtimeMs + MTIME_BUCKET_MS) {
      return { type: 'push', path: l.path, size: l.size, mtimeMs: l.mtimeMs, conflictBackup: true, conflict: true };
    }
    return { type: 'pull', path: l.path, size: h.size, conflictCopy: true, conflict: true };
  }

  if (l.deleted && !h.deleted) {
    // Local tombstone vs hub live file.
    if (mode === 'pull') return { type: 'pull', path: l.path, size: h.size };
    if (mode === 'push') return { type: 'delHub', path: l.path };
    const hubChanged = !markersMatch(l, h.size, h.mtimeMs);
    return hubChanged
      ? { type: 'pull', path: l.path, size: h.size } // hub changed after our delete -> resurrect
      : { type: 'delHub', path: l.path };
  }

  // Local live vs hub tombstone.
  if (mode === 'pull') return { type: 'delLocal', path: l.path };
  if (mode === 'push') return { type: 'push', path: l.path, size: l.size, mtimeMs: l.mtimeMs };
  const localChanged = !markersMatch(l, l.size, l.mtimeMs);
  return localChanged
    ? { type: 'push', path: l.path, size: l.size, mtimeMs: l.mtimeMs } // local edit survives remote delete
    : { type: 'delLocal', path: l.path };
}

async function runPull(rt, hub, op, deviceName, eventLog) {
  const abs = safeResolve(rt.root, op.path);
  if (!abs) throw new Error(`unsafe path from hub: ${op.path}`);
  await fsp.mkdir(path.dirname(abs), { recursive: true });
  const tmp = path.join(path.dirname(abs), `.carbonsync.tmp-${crypto.randomBytes(6).toString('hex')}`);
  try {
    const { mtimeMs } = await hub.fileGet(rt.id, op.path, tmp);
    if (op.conflictCopy) {
      try {
        await fsp.access(abs);
        const conflictRel = makeConflictName(op.path, deviceName);
        await fsp.copyFile(abs, path.join(rt.root, conflictRel));
        eventLog.add('warn', rt.id, 'conflict_backup', op.path, `local copy preserved as ${conflictRel}`);
      } catch { /* nothing local to preserve */ }
    }
    if (Number.isFinite(mtimeMs)) {
      const when = new Date(mtimeMs);
      await fsp.utimes(tmp, when, when);
    }
    await fsp.rename(tmp, abs);
    const st = await fsp.stat(abs);
    rt.db.recordSynced(op.path, st.size, Math.round(st.mtimeMs));
  } catch (err) {
    await fsp.rm(tmp, { force: true }).catch(() => {});
    throw err;
  }
}

async function runPush(rt, hub, op, deviceName) {
  const abs = safeResolve(rt.root, op.path);
  if (!abs) throw new Error(`unsafe local path: ${op.path}`);
  let st;
  try {
    st = await fsp.stat(abs);
  } catch {
    return; // raced away since scan; the watcher/rescan will tombstone it
  }
  const size = st.size;
  const mtimeMs = Math.round(st.mtimeMs);
  await hub.filePut(rt.id, op.path, abs, {
    size, mtimeMs,
    conflictBackup: !!op.conflictBackup,
    fromDevice: deviceName,
  });
  rt.db.setSyncedMarkers(op.path, size, mtimeMs);
  if (size !== op.size || !near(mtimeMs, op.mtimeMs)) {
    // file changed mid-sync; make sure the index reflects what we pushed
    rt.db.applyLocal([{ path: op.path, size, mtimeMs }]);
  }
}

async function runDelLocal(rt, op) {
  const abs = safeResolve(rt.root, op.path);
  if (!abs) throw new Error(`unsafe local path: ${op.path}`);
  await fsp.rm(abs, { force: true });
  rt.db.markDeletedSynced(op.path);
}

async function runDelHub(rt, hub, op, deviceName) {
  await hub.fileDelete(rt.id, op.path, deviceName);
  rt.db.markDeletedSynced(op.path);
}

/**
 * One full sync pass for a folder, spoke-driven. Two passes over the merge:
 * a cheap counting pass (drives the delete-fraction guard) then execution.
 */
async function syncFolder(rt, hub, cfg, eventLog, log, { force = false, deviceName }) {
  const t0 = Date.now();

  // ---- pass 1: plan ----
  const plan = { pull: 0, push: 0, delLocal: 0, delHub: 0, markSynced: 0, conflicts: 0, pullBytes: 0, pushBytes: 0 };
  for await (const op of mergeOps(rt, hub, cfg.pageSize)) {
    plan[op.type]++;
    if (op.conflict) plan.conflicts++;
    if (op.type === 'pull') plan.pullBytes += op.size || 0;
    if (op.type === 'push') plan.pushBytes += op.size || 0;
  }

  if (!force) {
    // ---- delete-fraction guard (the "empty side nukes everyone" classic) ----
    const localLive = rt.db.liveCount();
    const localLimit = Math.max(cfg.guardMinDeletes, Math.ceil(cfg.guardFraction * Math.max(1, localLive)));
    let hubLimit = Infinity;
    if (plan.delHub > 0) {
      const hubDigest = await hub.folderDigest(rt.id);
      hubLimit = Math.max(cfg.guardMinDeletes, Math.ceil(cfg.guardFraction * Math.max(1, hubDigest.count)));
    }
    if (plan.delLocal > localLimit || plan.delHub > hubLimit) {
      rt.guardTripped = true;
      const detail = `would delete ${plan.delLocal} local / ${plan.delHub} hub files (limits ${localLimit}/${hubLimit === Infinity ? '-' : hubLimit}); use force_sync to apply`;
      eventLog.add('error', rt.id, 'delete_guard', null, detail);
      log.error({ folder: rt.id, plan }, 'delete guard tripped');
      return { blocked: true, plan };
    }

    // ---- additions guard (incident 2026-06-10: 148 GB walked onto the hub
    // unchallenged — mass transfers need the same confirm as mass deletes) ----
    const adds = plan.pull + plan.push;
    const addBytes = plan.pullBytes + plan.pushBytes;
    if (adds > cfg.guardMinAdds || addBytes > cfg.guardAddBytes) {
      rt.guardTripped = true;
      const detail = `would transfer ${adds} files / ${(addBytes / 1024 / 1024 / 1024).toFixed(2)} GB (pull=${plan.pull} push=${plan.push}); use force_sync to apply`;
      eventLog.add('error', rt.id, 'add_guard', null, detail);
      log.error({ folder: rt.id, plan }, 'additions guard tripped');
      return { blocked: true, plan };
    }

    // ---- free-disk guard (incident: kingdel pulled to ENOSPC) ----
    if (plan.pullBytes > 0) {
      const st = await fsp.statfs(rt.root).catch(() => null);
      if (st && plan.pullBytes + cfg.diskFloorBytes > st.bavail * st.bsize) {
        rt.guardTripped = true;
        const freeGb = (st.bavail * st.bsize / 1024 / 1024 / 1024).toFixed(1);
        eventLog.add('error', rt.id, 'disk_guard', null,
          `pull needs ${(plan.pullBytes / 1024 / 1024 / 1024).toFixed(2)} GB but only ${freeGb} GB free (floor ${(cfg.diskFloorBytes / 1024 / 1024 / 1024).toFixed(0)} GB); free space or force_sync`);
        log.error({ folder: rt.id }, 'disk guard tripped');
        return { blocked: true, plan };
      }
    }
  }
  rt.guardTripped = false;

  // ---- pass 2: execute, small fixed concurrency ----
  const errors = [];
  const running = new Set();
  const launch = op => {
    const p = (async () => {
      switch (op.type) {
        case 'pull': return runPull(rt, hub, op, deviceName, eventLog);
        case 'push': return runPush(rt, hub, op, deviceName);
        case 'delLocal': return runDelLocal(rt, op);
        case 'delHub': return runDelHub(rt, hub, op, deviceName);
        case 'markSynced': return rt.db.setSyncedMarkers(op.path, op.size, op.mtimeMs);
        default: throw new Error(`unknown op ${op.type}`);
      }
    })().catch(err => {
      errors.push({ op: op.type, path: op.path, error: err.message });
      if (errors.length <= 10) eventLog.add('error', rt.id, 'op_failed', op.path, `${op.type}: ${err.message}`);
    }).finally(() => running.delete(p));
    running.add(p);
  };

  let aborted = false;
  for await (const op of mergeOps(rt, hub, cfg.pageSize)) {
    if (rt.paused) { aborted = true; break; } // pause must stop an IN-FLIGHT sync (incident 2026-06-10)
    if (errors.length > 50) break; // something is systemically wrong; stop digging
    launch(op);
    if (running.size >= cfg.transferConcurrency) await Promise.race(running);
  }
  await Promise.all(running);

  if (aborted) {
    eventLog.add('warn', rt.id, 'sync_aborted', null, 'paused mid-sync — digests left unsettled, resync on resume');
    return { blocked: false, aborted: true, plan, errors };
  }

  // ---- settle digests for the idle gate ----
  rt.db.setMeta('lastLocalDigest', rt.db.digest().hex);
  try {
    const hubDigest = await hub.folderDigest(rt.id);
    rt.db.setMeta('lastHubDigest', hubDigest.hex);
  } catch (err) {
    log.warn({ folder: rt.id, err: err.message }, 'could not record hub digest after sync');
  }

  const total = plan.pull + plan.push + plan.delLocal + plan.delHub;
  if (total > 0 || errors.length > 0) {
    eventLog.add(errors.length ? 'warn' : 'info', rt.id, 'sync_done', null,
      `pull=${plan.pull} push=${plan.push} delLocal=${plan.delLocal} delHub=${plan.delHub} conflicts=${plan.conflicts} errors=${errors.length} in ${Date.now() - t0}ms`);
  }
  return { blocked: false, plan, errors };
}

module.exports = { syncFolder, mergeOps };
