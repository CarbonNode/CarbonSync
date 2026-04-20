/**
 * Stale-Peer Guard — root-cause fix for the silent data-loss bug.
 *
 * Phases 1+2 (trash bucket + threshold guard) made deletions recoverable
 * and refused obvious mass-delete batches. They didn't address the root
 * cause: when a peer's index disagrees with ours, the engine *infers*
 * "the peer deleted these files" — but a stale peer that simply never
 * received them looks identical to a peer that explicitly deleted them.
 *
 * This module classifies each proposed deletion against per-peer
 * last-known-state recorded in scanner.peer_state. A deletion is only
 * "real" when the peer demonstrably had the file at the same hash we
 * still have. Anything else gets preserved, and we plan to re-push.
 *
 * Pure logic, no I/O — the scanner is consulted only via its public
 * methods and never written to from this module.
 */

'use strict';

/**
 * Classify a single proposed deletion.
 *
 * @param {object} args
 * @param {{ hash: string, synced_at: number } | null} args.peerKnown
 *        Result of scanner.getPeerKnown(peerId, path), or null if there's
 *        no record.
 * @param {string | null} args.currentLocalHash
 *        The local file's current hash, or null if the file no longer
 *        exists locally.
 * @returns {{ action: 'delete'|'preserve', reason: string }}
 *
 * Outcomes:
 *  - { action: 'delete',   reason: 'peer-explicit' }
 *      Peer knew this file at the exact hash we have now. Safe to delete:
 *      the peer's "delete this" instruction is genuinely a remote deletion.
 *
 *  - { action: 'preserve', reason: 'never-known-by-peer' }
 *      We have no record that this peer ever had this file. Likely a stale
 *      peer or a brand-new file. Preserve locally and plan to push to peer.
 *
 *  - { action: 'preserve', reason: 'local-modified-since-sync' }
 *      Peer knew an older hash; we have a different hash now. Conflict —
 *      our local edit is newer than what the peer last saw. Preserve and
 *      re-push so the peer picks up our changes.
 *
 *  - { action: 'delete',   reason: 'already-gone' }
 *      File already missing locally. The "delete" is a no-op; allow it so
 *      callers can still run any bookkeeping (e.g. clearing peer_state).
 */
function classifyDeletion({ peerKnown, currentLocalHash } = {}) {
  // File already gone locally — let the caller proceed (it's a no-op anyway,
  // but we still want it to clear scanner rows / peer_state without tripping
  // the preserve path and re-pushing nothing).
  if (currentLocalHash == null) {
    return { action: 'delete', reason: 'already-gone' };
  }

  // Phase 9 P0: `fast:<size>:<mtime>` is the scanner's placeholder before a
  // SHA-256 is computed. Two unrelated files with the same size and mtime
  // would compare equal and be authorized as peer-explicit — exactly the
  // silent-data-loss vector Phase 3 tried to close. If either side of the
  // comparison is a fast fingerprint, refuse to authorize the deletion.
  // recordPeerKnown* now rejects fast: hashes, but this check is kept as
  // defense in depth for any caller passing in older rows or for the local
  // currentLocalHash which comes straight from the files table.
  const peerHashIsFast = peerKnown && peerKnown.hash &&
    String(peerKnown.hash).startsWith('fast:');
  const localHashIsFast = String(currentLocalHash).startsWith('fast:');
  if (peerHashIsFast || localHashIsFast) {
    return { action: 'preserve', reason: 'fast-hash-unreliable' };
  }

  // We've never recorded this peer knowing this file. Could be a stale
  // peer (the bug we're fixing) or a freshly-added local file the peer
  // hasn't seen yet. Either way: don't delete; push to peer instead.
  if (!peerKnown) {
    return { action: 'preserve', reason: 'never-known-by-peer' };
  }

  // Peer knew the exact hash we still have. Genuine remote deletion —
  // safe to delete locally.
  if (peerKnown.hash === currentLocalHash) {
    return { action: 'delete', reason: 'peer-explicit' };
  }

  // Peer knew a different hash. We've edited locally since last sync.
  // Preserve our version; peer should receive our update on the next push.
  return { action: 'preserve', reason: 'local-modified-since-sync' };
}

/**
 * Convenience: classify an entire batch of paths the peer wants deleted.
 *
 * @param {object} args
 * @param {object} args.scanner - A Scanner instance (per-folder).
 * @param {string} args.peerId  - Stable peer identifier.
 * @param {string[]} args.paths - Paths the peer says to delete.
 * @returns {{
 *   delete: string[],
 *   preserve: Array<{ path: string, reason: string }>,
 *   pushBack: string[]
 * }}
 *   delete   = paths to actually delete (peer-explicit OR already-gone).
 *   preserve = paths to keep, with the classification reason.
 *   pushBack = subset of preserve where we should re-push to the peer
 *              (never-known-by-peer + local-modified-since-sync). Excludes
 *              already-gone (nothing to push) and peer-explicit (we're
 *              actually deleting it).
 */
function classifyDeletionBatch({ scanner, peerId, paths } = {}) {
  const result = { delete: [], preserve: [], pushBack: [] };
  if (!scanner || !peerId || !Array.isArray(paths) || paths.length === 0) {
    return result;
  }

  for (const p of paths) {
    if (!p) continue;
    const peerKnown = scanner.getPeerKnown(peerId, p);
    const localRow = scanner.getFile(p);
    const currentLocalHash = localRow ? localRow.hash : null;
    const c = classifyDeletion({ peerKnown, currentLocalHash });

    if (c.action === 'delete') {
      result.delete.push(p);
    } else {
      result.preserve.push({ path: p, reason: c.reason });
      // never-known + local-modified both warrant re-pushing our copy.
      // fast-hash-unreliable (Phase 9) also pushes back so the peer can
      // decide with a real hash on the next round.
      // already-gone never reaches this branch (it's an action: 'delete').
      if (c.reason === 'never-known-by-peer' ||
          c.reason === 'local-modified-since-sync' ||
          c.reason === 'fast-hash-unreliable') {
        result.pushBack.push(p);
      }
    }
  }

  return result;
}

/**
 * Discovery-aware deletion evaluation (Phase 5).
 *
 * Wraps classifyDeletionBatch with a "discovery-first sync" gate: if we have
 * never completed a full clean sync round-trip with this peer (folder-scoped),
 * preserve EVERY proposed deletion regardless of peer_state contents. This is
 * the post-Phase-3 fix for the seeding regression: even if peer_state happens
 * to "agree" with the peer's diff, on the very first contact we treat the
 * peer's index as untrusted and push our state instead.
 *
 * Discovery completes when either side finishes a successful push or pull;
 * the caller is responsible for invoking `scanner.markPeerDiscovered(peerId)`
 * at that point.
 *
 * @param {object} args
 * @param {object} args.scanner - A Scanner instance (per-folder).
 * @param {string} args.peerId  - Stable peer identifier.
 * @param {string[]} args.paths - Paths the peer says to delete.
 * @returns {{
 *   delete: string[],
 *   preserve: Array<{ path: string, reason: string }>,
 *   pushBack: string[]
 * }}
 *   When discovering: delete=[], preserve=all-paths-as-discovery-first-sync,
 *   pushBack=all-paths.
 *   When discovered: delegates to classifyDeletionBatch.
 */
function evaluateDeletionWithDiscovery({ scanner, peerId, paths } = {}) {
  const empty = { delete: [], preserve: [], pushBack: [] };
  if (!scanner || !peerId || !Array.isArray(paths) || paths.length === 0) {
    return empty;
  }

  // First contact: preserve everything. We don't trust the peer's diff yet.
  // The push-back queue will send our full state, and the success of either
  // direction's transfer marks discovery complete (so the next diff is real).
  if (typeof scanner.isPeerDiscovered === 'function' &&
      !scanner.isPeerDiscovered(peerId)) {
    return {
      delete: [],
      preserve: paths.map(p => ({ path: p, reason: 'discovery-first-sync' })),
      pushBack: paths.slice(),
    };
  }

  return classifyDeletionBatch({ scanner, peerId, paths });
}

module.exports = {
  classifyDeletion,
  classifyDeletionBatch,
  evaluateDeletionWithDiscovery,
};
