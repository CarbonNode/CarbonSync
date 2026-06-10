'use strict';

const path = require('node:path');
const parcelWatcher = require('@parcel/watcher');

/**
 * Debounced FS watching: events accumulate into a dirty-path set and flush as
 * ONE callback after debounceMs of quiet. One timer per folder, ever — a burst
 * of 10k events (big copy landing) becomes one targeted rescan.
 */
async function watchFolder(root, onPaths, { debounceMs = 2000, log } = {}) {
  const pending = new Set();
  let timer = null;
  let closed = false;

  const flush = () => {
    timer = null;
    if (closed || pending.size === 0) return;
    const paths = [...pending];
    pending.clear();
    Promise.resolve(onPaths(paths)).catch(err => {
      if (log) log.warn({ err: err.message }, 'watcher flush failed');
    });
  };

  const sub = await parcelWatcher.subscribe(root, (err, events) => {
    if (closed) return;
    if (err) {
      if (log) log.warn({ err: err.message }, 'watcher error');
      return;
    }
    for (const e of events) {
      const rel = path.relative(root, e.path);
      if (!rel || rel.startsWith('..')) continue;
      pending.add(rel.split(path.sep).join('/'));
    }
    if (timer) clearTimeout(timer);
    timer = setTimeout(flush, debounceMs);
  });

  return {
    async close() {
      closed = true;
      if (timer) clearTimeout(timer);
      await sub.unsubscribe();
    },
  };
}

module.exports = { watchFolder };
