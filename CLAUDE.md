# CarbonSync

LAN-only file sync daemon. Electron UI forks `src/daemon/daemon-process.js`, which drives `CarbonSyncDevice` (`src/daemon/device.js`) — mDNS + UDP discovery, TCP+TLS transport (`src/daemon/transport.js`), per-folder scanners with SQLite indexes, optional hub/device topology. Installed on each peer via `electron-builder` (no containers, no shared deploy server).

Folder index arrays for large folders (Patreon is 168k files / ~50MB JSON) live fully in memory and cross the wire as single frames. Anything that multiplies in-flight work per peer or per folder will OOM the daemon — raising `MAX_FRAME_SIZE` or `--max-old-space-size` is a band-aid, not a fix.

## Do not repeat these mistakes

These anti-patterns caused a multi-hour P0 outage (daemon OOM on every boot, 8.77 GB zombie on one peer, 40× "connected" toast spam). All are fixed in the code below — if you find yourself undoing any of this, stop.

1. **Never put self (`127.0.0.1`, or own `deviceId`) into `savedPeers`.** `daemon-process.js` boot reconnects to every saved peer — a self entry creates an inbound→auto-connect-back loop that amplifies any other bug.
2. **`SyncClient.disconnect()` is terminal.** The `_disposed` flag in `src/daemon/transport.js` blocks `_scheduleReconnect` after intentional teardown. Removing it resurrects every discarded client 5s later as a zombie that reconnects on its own.
3. **`connectToPeer` must return the in-flight promise for concurrent callers.** `peerInfo._pending` in `src/daemon/device.js` prevents N parallel callers (saved-peer reconnect racing with auto-connect-back) from spawning N `SyncClient` instances.
4. **Auto-connect-back is debounced per peer via `_pendingAutoConnectBack`.** A burst of inbound sockets from one peer must schedule at most one 3s timer, not one per socket.
5. **Dedup peer state by `deviceId` when known, not just `ip:port`.** The same peer across NICs or re-IPs must not be tracked as two peers.

## Running / verifying

- Headless daemon: `node src/daemon/index.js server` (uses `CarbonSyncServer`, not the full Electron path — fine for transport-layer checks, not for full device.js boot flows).
- Full UI: `npm run ui` (Electron forks `daemon-process.js`).
- Logs: `~/.carbonsync/daemon.stdout.log` (current), `.prev.log` (last session). `daemon.log` on peers you've installed to.
- Quick health: `curl http://<peer>:21549/status` and `/sync-log`.

## Peer SSH (for remote debugging)

LAN SSH is enabled on every peer. Users:
- `rober@192.168.0.35` — CarbonServer (hub)
- `rober@192.168.0.177` — LAYBACKRIG / DESKTOP-532Q4KG
- `Admin@192.168.0.168` — Kingdel
- Remote shell is Windows CMD; prefer `powershell -Command` for anything beyond a one-liner.

Never edit a peer's `~/.carbonsync/config.json` while its daemon is running — the next `config.save()` will overwrite your edit. Stop `CarbonSync.exe` first (`taskkill /F /IM CarbonSync.exe`).

## Deployment

Code changes require a rebuild (`npm run build` → `electron-builder`) and reinstall of the installer on each peer. There is no hot-reload and no container push. Pushing source alone does not update peers.
