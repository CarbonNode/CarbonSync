#!/usr/bin/env node
'use strict';

const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const crypto = require('node:crypto');
const { loadConfig } = require('./config');
const { createLogger } = require('./log');
const { Daemon, VERSION } = require('./daemon');

function defaultConfigPath() {
  return process.platform === 'win32'
    ? 'C:\\ProgramData\\CarbonSync\\config.json'
    : path.join(os.homedir(), '.carbonsync2', 'config.json');
}

function parseArgs(argv) {
  const args = { _: [] };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a.startsWith('--')) {
      const key = a.slice(2);
      const next = argv[i + 1];
      if (next === undefined || next.startsWith('--')) args[key] = true;
      else { args[key] = next; i++; }
    } else {
      args._.push(a);
    }
  }
  return args;
}

function cmdInit(args) {
  const configPath = args.config || defaultConfigPath();
  if (fs.existsSync(configPath)) {
    console.error(`refusing to overwrite existing ${configPath}`);
    process.exit(1);
  }
  const role = args.role;
  if (role !== 'hub' && role !== 'spoke') {
    console.error('init requires --role hub|spoke');
    process.exit(1);
  }
  const deviceName = args.name || os.hostname().replace(/[^a-zA-Z0-9_-]/g, '-');
  const token = args.token || crypto.randomBytes(24).toString('hex');

  const cfg = { role, deviceName, token };
  if (role === 'hub') {
    cfg.folders = [];
  } else {
    if (!args.hub) {
      console.error('spoke init requires --hub https://<hub-ip>:21600 (and --token <fleet token>)');
      process.exit(1);
    }
    cfg.hubUrl = args.hub;
  }

  fs.mkdirSync(path.dirname(configPath), { recursive: true });
  fs.writeFileSync(configPath, JSON.stringify(cfg, null, 2) + '\n');
  console.log(`wrote ${configPath}`);
  console.log(`device: ${deviceName}  role: ${role}`);
  if (!args.token) console.log(`fleet token (use the SAME token on every node): ${token}`);
  if (role === 'hub') console.log('next: add folders[] to the config (see scripts/example-hub-config.json), then start the daemon');
}

/** Single instance per install dir — CortexAgent's PID-file pattern, verbatim. */
function ensureSingleInstance(stateDir, log) {
  fs.mkdirSync(stateDir, { recursive: true });
  const pidFile = path.join(stateDir, 'daemon.pid');
  try {
    const pid = parseInt(fs.readFileSync(pidFile, 'utf8').trim(), 10);
    if (Number.isFinite(pid) && pid !== process.pid) {
      try {
        process.kill(pid, 0);
        log.warn({ pid }, 'killing stale carbonsyncd instance');
        process.kill(pid, 'SIGTERM');
      } catch { /* already gone */ }
    }
  } catch { /* no pid file */ }
  fs.writeFileSync(pidFile, String(process.pid));
  const cleanup = () => { try { fs.unlinkSync(pidFile); } catch { /* gone */ } };
  process.on('exit', cleanup);
  return cleanup;
}

async function cmdRun(args) {
  const configPath = args.config || defaultConfigPath();
  const cfg = loadConfig(configPath);
  const { logger, close: closeLog } = createLogger(cfg.stateDir);

  ensureSingleInstance(cfg.stateDir, logger);

  const daemon = new Daemon(cfg, logger);
  const shutdown = async code => {
    logger.info('shutting down');
    try { await daemon.stop(); } catch (err) { logger.error({ err: err.message }, 'stop failed'); }
    closeLog();
    process.exit(code);
  };
  process.on('SIGINT', () => shutdown(130));
  process.on('SIGTERM', () => shutdown(143));
  process.on('uncaughtException', err => {
    logger.fatal({ err: err.stack || err.message }, 'uncaught exception');
    shutdown(1);
  });
  process.on('unhandledRejection', err => {
    logger.fatal({ err: (err && err.stack) || String(err) }, 'unhandled rejection');
    shutdown(1);
  });

  const { dataPort, apiPort } = await daemon.start();
  logger.info({ version: VERSION, dataPort, apiPort, config: configPath }, 'ready');
  // stdout breadcrumb for interactive runs; the real log is state/daemon.log
  console.log(`carbonsyncd ${VERSION} (${cfg.role}) — api 127.0.0.1:${apiPort}${dataPort ? `, data 0.0.0.0:${dataPort}` : ''}`);
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const cmd = args._[0] || 'run';
  if (cmd === 'init') return cmdInit(args);
  if (cmd === 'run') return cmdRun(args);
  if (cmd === 'version') return console.log(VERSION);
  console.error(`unknown command: ${cmd} (expected: run | init | version)`);
  process.exit(1);
}

main().catch(err => {
  console.error(err.stack || err.message);
  process.exit(1);
});
