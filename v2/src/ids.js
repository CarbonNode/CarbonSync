'use strict';

const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');

/** Persisted device identity. Peer identity is ALWAYS this UUID, never ip:port. */
function loadDeviceId(stateDir) {
  const file = path.join(stateDir, 'device-id');
  try {
    const id = fs.readFileSync(file, 'utf8').trim();
    if (/^[0-9a-f-]{36}$/i.test(id)) return id;
  } catch { /* first boot */ }
  const id = crypto.randomUUID();
  fs.mkdirSync(stateDir, { recursive: true });
  fs.writeFileSync(file, id + '\n');
  return id;
}

module.exports = { loadDeviceId };
