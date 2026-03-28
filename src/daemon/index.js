#!/usr/bin/env node
/**
 * CarbonSync Daemon Entry Point
 *
 * Usage:
 *   node src/daemon/index.js server    — Run as hub (authoritative server)
 *   node src/daemon/index.js client    — Run as client (pulls from server)
 *   node src/daemon/index.js status    — Show current status
 */

const path = require('path');
const os = require('os');
const { CarbonSyncServer } = require('./server');
const { CarbonSyncClient } = require('./client-sync');

const configDir = path.join(os.homedir(), '.carbonsync');
const mode = process.argv[2] || 'server';

async function main() {
  if (mode === 'server') {
    const server = new CarbonSyncServer(configDir);

    server.on('ready', () => {
      const status = server.getStatus();
      console.log(`\nServer ready. API Key: ${status.apiKey}`);
      console.log(`Syncing ${status.folders.length} folder(s)`);
      for (const f of status.folders) {
        console.log(`  ${f.name}: ${f.fileCount} files (${formatBytes(f.totalSize)})`);
      }
    });

    server.on('changes', ({ folder, changes }) => {
      for (const c of changes) {
        console.log(`  [${c.type}] ${folder}/${c.path}`);
      }
    });

    server.on('client-connected', (info) => {
      console.log(`Client connected: ${info.deviceName}`);
    });

    await server.start();

    process.on('SIGINT', async () => {
      console.log('\nShutting down...');
      await server.stop();
      process.exit(0);
    });

  } else if (mode === 'client') {
    const client = new CarbonSyncClient(configDir);

    client.on('server-found', (info) => {
      console.log(`Found server: ${info.hostname} at ${info.ip}:${info.port}`);
    });

    client.on('connected', (msg) => {
      console.log(`Connected to server: ${msg.serverName}`);
    });

    client.on('progress', (p) => {
      const pct = p.totalProgress || 0;
      const bar = '█'.repeat(Math.floor(pct / 5)) + '░'.repeat(20 - Math.floor(pct / 5));
      process.stdout.write(`\r  [${bar}] ${pct}% | ${p.currentFile} (${p.filesComplete}/${p.filesTotal})`);
    });

    client.on('sync-complete', (info) => {
      console.log(`\nSync complete: ${info.folder} — ${info.downloaded} downloaded, ${info.deleted} deleted`);
    });

    await client.start();

    process.on('SIGINT', async () => {
      console.log('\nShutting down...');
      await client.stop();
      process.exit(0);
    });

  } else {
    console.log('Usage: carbonsync [server|client]');
    process.exit(1);
  }
}

function formatBytes(bytes) {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${(bytes / Math.pow(k, i)).toFixed(1)} ${sizes[i]}`;
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});

process.on('uncaughtException', (err) => {
  console.error('Uncaught exception:', err);
});

process.on('unhandledRejection', (err) => {
  console.error('Unhandled rejection:', err);
});
