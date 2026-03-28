/**
 * Structured Logging with Rotation
 *
 * Uses pino for fast structured JSON logging.
 * Logs to file + stdout. File auto-rotates at 10MB.
 */

const pino = require('pino');
const path = require('path');
const fs = require('fs');

const MAX_LOG_SIZE = 10 * 1024 * 1024; // 10MB
const MAX_LOG_FILES = 3;

function createLogger(configDir, name = 'carbonsync') {
  const logDir = path.join(configDir, 'logs');
  fs.mkdirSync(logDir, { recursive: true });

  const logFile = path.join(logDir, `${name}.log`);

  // Rotate if needed
  rotateLog(logFile);

  // Multi-stream: file + stdout
  const streams = [
    { stream: fs.createWriteStream(logFile, { flags: 'a' }) },
    { stream: process.stdout },
  ];

  const logger = pino({
    level: 'info',
    timestamp: pino.stdTimeFunctions.isoTime,
    formatters: {
      level: (label) => ({ level: label }),
    },
  }, pino.multistream(streams));

  return logger;
}

function rotateLog(logFile) {
  try {
    if (!fs.existsSync(logFile)) return;

    const stat = fs.statSync(logFile);
    if (stat.size < MAX_LOG_SIZE) return;

    // Rotate: .log.2 → delete, .log.1 → .log.2, .log → .log.1
    for (let i = MAX_LOG_FILES - 1; i >= 1; i--) {
      const from = i === 1 ? logFile : `${logFile}.${i - 1}`;
      const to = `${logFile}.${i}`;
      try {
        if (fs.existsSync(to)) fs.unlinkSync(to);
        if (fs.existsSync(from)) fs.renameSync(from, to);
      } catch { /* best effort */ }
    }
  } catch (err) {
    console.warn(`Log rotation failed: ${err.message}`);
  }
}

module.exports = { createLogger };
