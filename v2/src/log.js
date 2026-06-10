'use strict';

const fs = require('node:fs');
const path = require('node:path');
const pino = require('pino');

const MAX_LOG_BYTES = 25 * 1024 * 1024;
const ROTATE_CHECK_MS = 10 * 60 * 1000;

/**
 * Process logger with size-capped rotation (daemon.log -> daemon.log.1).
 * v1 had no rotation anywhere and laybackrig accumulated a 302 MB daemon.log;
 * this caps total log disk at ~2x MAX_LOG_BYTES.
 */
function createLogger(stateDir, opts = {}) {
  const level = opts.level || process.env.CARBONSYNC_LOG_LEVEL || 'info';
  if (!stateDir) return { logger: pino({ level }), close: () => {} };

  fs.mkdirSync(stateDir, { recursive: true });
  const file = path.join(stateDir, 'daemon.log');
  const dest = pino.destination({ dest: file, sync: false, mkdir: true });
  const logger = pino({ level }, dest);

  const timer = setInterval(() => {
    try {
      const st = fs.statSync(file);
      if (st.size > MAX_LOG_BYTES) {
        fs.rmSync(file + '.1', { force: true });
        fs.renameSync(file, file + '.1');
        dest.reopen();
      }
    } catch { /* log file missing is fine */ }
  }, ROTATE_CHECK_MS);
  timer.unref();

  return {
    logger,
    close: () => {
      clearInterval(timer);
      try { dest.flushSync(); } catch { /* best effort */ }
    },
  };
}

module.exports = { createLogger };
