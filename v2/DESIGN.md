# CarbonSync 2.0 — Design

**Status:** agreed direction, pre-implementation (2026-06-10)
**Decision:** ground-up v2 as an invisible per-PC daemon, managed entirely through Carbon Cortex MCP. No UI. The v1 Electron app is retired once v2 reaches parity on the folders that matter.

## Goals

- Invisible: a background daemon you never see. All visibility and control through Cortex MCP tools.
- Bounded resources by construction: no full folder index ever lives in memory or crosses the wire as one frame (v1's Patreon folder = 168k files ≈ 50 MB JSON did both). Target idle RSS < 150 MB per peer with v1's real folder set.
- Zero manual PC visits: install, update, configure, and operate the fleet from Cortex.
- Keep the lessons v1 paid for (see "Landmines carried forward").

## Non-goals (v2.0)

- No Electron, no tray, no local UI of any kind.
- No mDNS/UDP discovery, no mesh, no auto-connect-back — the fleet is small, known, and static-IP'd. Discovery was the root of v1's worst P0s.
- No game-save manager (v1's ~2.6k-LOC subsystem). Stays in v1 or returns later as its own module.
- No internet/WAN sync. LAN only, like v1.

## How it runs on PCs (the run model)

Every PC in the network already runs **CortexAgent** (`C:\ProgramData\CortexAgent`, scheduled task `CortexAgent`, outbound WSS to `gateway.carbonrouting.dev` with token auth). That solves bootstrap, auth, and reachability. v2 rides it without living inside it:

**Two processes per PC:**

| Process | Role | Why separate |
|---|---|---|
| `CortexAgent` (existing, untouched except one new probe file) | Control-plane proxy: surfaces carbonsyncd's API as per-node MCP connectors | Stays tiny and always-up. It is the recovery path (shell access) — sync workload must never be able to take it down. |
| `carbonsyncd` (new) | The sync daemon: scanner, watcher, SQLite indexes, transfers | Heaviest workload on the box (168k-file scans, GB transfers). Crashes/OOMs/updates must be isolated from shell access; CortexAgent self-update restarts must not kill in-flight syncs. |

`carbonsyncd` specifics:
- Plain Node.js (deps: `better-sqlite3`, `@parcel/watcher`, `ignore`, `pino` — all with win32 prebuilds; Node.js is already on every box because CortexAgent requires it).
- Installs to `C:\ProgramData\CarbonSync`, state in `C:\ProgramData\CarbonSync\state\` (SQLite indexes, events DB, logs).
- Runs under scheduled task `CarbonSyncd` (ONSTART, hidden) — same supervision style as CortexAgent.
- Single-instance via PID file in its own install dir (copy CortexAgent's exact pattern — it was added because two-agents-from-one-dir kept happening on maingamingrig).
- Serves a token-authed HTTP API on `127.0.0.1:21601` (every node) and, on the hub only, the LAN data-plane API on `:21600` (TLS + bearer token). Ports deliberately ≠ v1's 21547/21549 so v1 and v2 can coexist during migration.

**Cortex integration (the ModRelay pattern):**
- New `agent/src/carbonsync.ts` in carbon-cortex, mirroring `modrelay.ts`: TCP-probe `127.0.0.1:21601`, read token from `C:\ProgramData\CarbonSync\mcp-token.json`, re-register on the 30s heartbeat when availability changes.
- Gateway surfaces per-node tools as `carbonsync-<box>__*` automatically once the daemon is running — no gateway restart, no new transport, no new auth system.
- A fleet-level view comes free by querying the hub node (it knows every spoke's last-reported state).

**Install / update — using machinery that already exists:**
- Install: driven from Cortex via the existing `shell-<box>` connectors — download release tarball, `npm ci --omit=dev`, register the scheduled task, start. One MCP-driven script per box; no SSH, no RDP, no walking to the machine.
- Update (v2.0): re-run the installer via `shell-<box>` — explicit, observable, per-box.
- Update (v2.1): self-update RPC copied from CortexAgent's (`handlers.ts` "Self-update" — download tarball, overwrite, restart via task), gated by a **strictly-newer semver check** (v1's fleet-downgrade lesson: a wrong "latest" must be refused, never installed).
- ⚠️ Release artifacts must NOT be published as GitHub Releases on `CarbonNode/CarbonSync` while any v1 install exists: v1's `updater.js` auto-installs whatever `releases/latest` is. v2 ships from a separate repo (or, until that exists, from tarballs served off the hub/gateway — never this repo's Releases).

## Topology & data plane

**Star, hub on carbonserver** (always-on box). Spokes make outbound HTTPS to the hub only — no inbound ports on PCs, no firewall rules, nothing listening on gaming rigs/laptops except localhost.

- Control + data: HTTPS to hub `:21600` (self-signed cert generated at first hub boot, TOFU fingerprint pinning on spokes + shared bearer token — v1's TLS trust model, simplified).
- File bytes move **only on the LAN, directly between daemon and hub** — never through the gateway WebSocket or Cloudflare (CortexAgent's WSS path goes through the tunnel; fine for control, unacceptable for GBs of data).
- Spoke↔spoke consistency is transitive through the hub (this is how the fleet actually used v1 anyway).
- Spokes that sleep (laptops, gaming rigs) just stop polling; hub tracks last-seen; the fleet status tool shows staleness. No reconnect storms possible — polling is stateless HTTP with jittered backoff (5s→60s), one timer per spoke, no persistent client objects to resurrect.

## Sync engine (memory-bounded by construction)

- **Index:** SQLite per folder (`files(path PK, size, mtimeMs, deleted, deletedAtMs, seq)` + spoke-side `synced_size/synced_mtime` markers). WAL mode. All reads are cursor-paged; nothing materializes the full file list.
- **Scanning:** stat-only walk (no content hashing — v1 proved mass hashing isn't needed on LAN; integrity check happens at transfer time). Writes only rows that actually changed (v2.7.11's idle-burn lesson: an idle rescan that finds nothing must write nothing). Watcher (`@parcel/watcher`) marks dirty subtrees → debounced targeted rescans. Hourly stat-only full rescan as backstop, staggered per folder.
- **Cheap idle gate:** per-folder digest = sha256 over the live-row cursor `(path,size,mtimeBucket2s)` — memoized by folder seq, recomputed only after a mutation. Idle sync check = one GET comparing digests. Digest excludes tombstones (deletions change the digest by removing live rows), so tombstone GC can't cause permanent digest divergence/re-compare loops.
- **Diff:** when digests differ, stream both indexes in path order (paged merge-join, 1k rows/page) and emit ops incrementally. Per-file compare uses size + mtime with 2s tolerance; after a transfer the destination mtime is set from the source so comparisons converge.
- **Conflicts:** a file changed on both sides since its synced marker → last-writer-wins by mtime, loser preserved as `<name>.conflict-<device>-<ts>` (two-way folders only).
- **Deletes:** tombstones (30-day GC). **Delete-fraction guard:** a sync that would delete > max(50, 25% of folder) aborts with an error event and requires an explicit `force_sync` — the "empty source nukes everyone" classic, structurally blocked. A folder whose root is missing (unmounted drive) is marked offline and skipped, never treated as mass-deletion.
- **Transfers:** streamed (temp file + atomic rename + length check), small fixed concurrency (2/folder), per-folder direction modes `pull` / `push` / `two-way`.
- **Events/logs:** sync events go to a capped SQLite table (last 20k, queryable via API/MCP) — replaces v1's unbounded `sync.log` (656 MB on laybackrig). Process log via pino with size-capped rotation.

## Config: hub is the source of truth

Spoke-local config is only `{deviceName, hubUrl, token}`. Folder topology lives on the hub: `folder → {per-device path, per-device mode}` (paths differ per box, e.g. `F:\` on kingdel). Spokes receive their assignments in each poll response. Add/remove/redirect a folder fleet-wide = one MCP call to the hub; no per-box config files to keep consistent (and no daemon fighting you for `config.json` writes — v1's gotcha).

## MCP tool surface

Per-node (`carbonsync-<box>__*`, via CortexAgent wrap): `status`, `events`, `sync_now`, `pause` / `resume`, `force_sync` (clears a tripped delete-guard, explicit), `rescan`.
Hub-only additions: `fleet_status` (all devices, versions, last-seen, per-folder digest-match matrix), `folder_status`, `add_folder` / `remove_folder` / `assign_device` / `set_mode`, `update_fleet {version}` (v2.1, strictly-newer enforced).

## Landmines carried forward from v1 (structural, not patched)

1. Self-connection impossible: no savedPeers, no discovery; spoke refuses a hub whose handshake deviceId equals its own.
2. No resurrectable clients: no long-lived connection objects at all — stateless polls can't zombie-reconnect.
3. No concurrent-connect races: one poll loop, one timer, per spoke.
4. No connect-back: hub never dials spokes.
5. Peer identity = persisted deviceId UUID, never ip:port.
6. Plus: bounded memory everywhere, capped logs, delete-fraction guard, strictly-newer updates only.

## Migration

1. v2 daemons install alongside stopped v1 (different ports/dirs) — v1 stays installed as fallback.
2. Bring up hub (carbonserver) + one spoke (kingdel — smallest folder set), validate on Game Saves.
3. Move folders over one at a time (fresh stat-only index build is fast); laybackrig + Patreon (168k files) last, watching RSS/CPU via `fleet_status`.
4. Parity reached → uninstall v1 per box → archive v1 code on a branch; v2 becomes `main`.

## Build phases

- **MVP (phase 1):** daemon (index/scan/watch/digest/merge-sync/transfers/events) + hub mode + spoke poll loop + localhost API + install script + e2e test (two daemons in-proc, tmp dirs: pull/push/two-way, delete propagation, conflict copy, delete-guard trip, paging).
- **Phase 2:** CortexAgent probe + gateway proxy entry (carbon-cortex repo, ~modrelay-sized change) → tools live in Cortex.
- **Phase 3:** fleet rollout per Migration above.
- **Phase 4 (v2.1):** self-update RPC, TLS cert rotation, mac-mini support (it's not a Node yet — SSH-only), optional read-only web status page served by the hub.
