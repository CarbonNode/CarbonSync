'use strict';

const fs = require('node:fs');
const path = require('node:path');

const DEFAULTS = {
  apiPort: 21601,          // localhost admin API (CortexAgent probes this)
  apiHost: '127.0.0.1',    // set 0.0.0.0 on boxes where the CortexAgent runs in Docker (token-authed either way)
  dataPort: 21600,         // hub-only LAN data plane
  tls: true,               // hub data plane TLS (self-signed + TOFU pin)
  pollIntervalMs: 10_000,
  debounceMs: 2_000,
  rescanIntervalMs: 60 * 60 * 1000,
  rootCheckIntervalMs: 5000,
  pageSize: 1000,
  transferConcurrency: 2,
  guardMinDeletes: 50,     // delete-fraction guard: max(min, fraction*live)
  guardFraction: 0.25,
  guardMinAdds: 5000,      // additions guard: a sync planning more transfers than this blocks until force_sync
  guardAddBytes: 10 * 1024 * 1024 * 1024,
  diskFloorBytes: 2 * 1024 * 1024 * 1024, // refuse pulls that would leave less than this free
  tombstoneTtlMs: 30 * 24 * 60 * 60 * 1000,
  repoDir: process.platform === 'win32' ? 'C:\\Programming\\CarbonSync' : null, // git clone self_update pulls from
};

const SLUG_RE = /^[a-zA-Z0-9][a-zA-Z0-9_-]*$/;

function fail(msg) {
  throw new Error(`config: ${msg}`);
}

function loadConfig(configPath) {
  let raw;
  try {
    raw = fs.readFileSync(configPath, 'utf8');
  } catch (err) {
    fail(`cannot read ${configPath}: ${err.message}`);
  }
  let data;
  try {
    data = JSON.parse(raw);
  } catch (err) {
    fail(`${configPath} is not valid JSON: ${err.message}`);
  }

  const cfg = { ...DEFAULTS, ...data };
  cfg._raw = data; // original file shape — topology tools persist THIS, not the defaults-merged view
  cfg.configPath = path.resolve(configPath);
  cfg.baseDir = path.dirname(cfg.configPath);
  cfg.stateDir = data.stateDir ? path.resolve(data.stateDir) : path.join(cfg.baseDir, 'state');

  if (cfg.role !== 'hub' && cfg.role !== 'spoke') fail(`role must be "hub" or "spoke", got ${JSON.stringify(cfg.role)}`);
  if (typeof cfg.deviceName !== 'string' || !SLUG_RE.test(cfg.deviceName)) fail('deviceName required (letters/digits/dash/underscore)');
  if (typeof cfg.token !== 'string' || cfg.token.length < 16) fail('token required (min 16 chars, shared across the fleet)');

  if (cfg.role === 'spoke') {
    if (typeof cfg.hubUrl !== 'string' || !/^https?:\/\//.test(cfg.hubUrl)) fail('spoke needs hubUrl (http(s)://host:port)');
    cfg.hubUrl = cfg.hubUrl.replace(/\/+$/, '');
  }

  if (cfg.role === 'hub') {
    if (!Array.isArray(cfg.folders)) fail('hub needs folders[]');
    const seen = new Set();
    for (const f of cfg.folders) {
      if (!f || typeof f.id !== 'string' || !SLUG_RE.test(f.id)) fail('every folder needs a slug id');
      if (seen.has(f.id)) fail(`duplicate folder id ${f.id}`);
      seen.add(f.id);
      if (typeof f.name !== 'string' || !f.name) fail(`folder ${f.id} needs a name`);
      if (!f.devices || typeof f.devices !== 'object') fail(`folder ${f.id} needs devices{}`);
      // Star topology: the hub must hold a copy of every folder it serves.
      if (!f.devices[cfg.deviceName]) fail(`folder ${f.id} has no assignment for the hub device "${cfg.deviceName}"`);
      for (const [dev, a] of Object.entries(f.devices)) {
        if (!a || typeof a.path !== 'string' || !a.path) fail(`folder ${f.id} device ${dev} needs a path`);
        const mode = a.mode || 'two-way';
        if (!['two-way', 'pull', 'push'].includes(mode)) fail(`folder ${f.id} device ${dev}: bad mode ${a.mode}`);
        a.mode = mode;
        if (a.excludes && !Array.isArray(a.excludes)) fail(`folder ${f.id} device ${dev}: excludes must be an array`);
      }
      if (f.excludes && !Array.isArray(f.excludes)) fail(`folder ${f.id}: excludes must be an array`);
    }
  } else if (data.folders) {
    fail('spokes do not declare folders — folder topology lives on the hub');
  }

  return cfg;
}

/** Token file the CortexAgent probe reads (ModRelay pattern). */
function writeMcpTokenFile(cfg) {
  const file = path.join(cfg.baseDir, 'mcp-token.json');
  const body = JSON.stringify({ token: cfg.token, apiPort: cfg.apiPort }, null, 2) + '\n';
  try {
    if (fs.existsSync(file) && fs.readFileSync(file, 'utf8') === body) return;
  } catch { /* rewrite below */ }
  fs.writeFileSync(file, body);
}

module.exports = { loadConfig, writeMcpTokenFile, DEFAULTS };
