/**
 * Drastic-Shrink Overwrite Guard — refuses to let a sync receive-write
 * silently replace a substantial local file with a 0-byte or massively
 * shrunken version.
 *
 * Background: receive-write paths verify `sha256(incoming) === fileInfo.hash`
 * and then atomically rename `tmp → abs`. Hash verification only proves the
 * incoming bytes match what the peer claims to have — it does NOT prove the
 * peer's copy is itself sane. If a peer's file was truncated to 0 bytes
 * (disk-full bug, ransomware, partial write, NAS hiccup), the peer computes
 * the empty-file SHA, sends 0 bytes, hash matches, rename succeeds, and our
 * last-good copy is gone. The trash bucket only protects deletion paths;
 * this is an overwrite path.
 *
 * Defaults: refuse any overwrite where the incoming size is 0 over a
 * non-empty existing, OR where the incoming drops the file by ≥90%, BUT
 * only when the existing file is large enough to be worth protecting
 * (over `absoluteFloor` = 1024 bytes by default — below that the file is
 * too small to meaningfully classify as "drastically shrunken").
 *
 * Per-folder override: `folder.shrinkGuard = { absoluteFloor, percentDrop }`.
 * See config.js for the field documentation.
 *
 * This module is intentionally pure (no I/O, no side effects) so it can be
 * tested directly and called from any receive-write site.
 */

'use strict';

const DEFAULT_THRESHOLD = Object.freeze({
  absoluteFloor: 1024,
  percentDrop: 0.9,
});

/**
 * Decide whether a sync-driven overwrite should proceed or be preserved.
 *
 * @param {object} args
 * @param {number|null} args.existingSize - Current local file size in bytes,
 *                                          or `null` if the file doesn't
 *                                          exist locally yet.
 * @param {number} args.incomingSize - Size of the incoming version in bytes.
 * @param {{ absoluteFloor?: number, percentDrop?: number }} [args.threshold]
 *                                  - Per-call override of the defaults.
 * @returns {{ action: 'allow'|'preserve', reason: string }}
 */
function classifyOverwrite({ existingSize, incomingSize, threshold } = {}) {
  const absoluteFloor = (threshold && Number.isFinite(threshold.absoluteFloor))
    ? threshold.absoluteFloor
    : DEFAULT_THRESHOLD.absoluteFloor;
  const percentDrop = (threshold && Number.isFinite(threshold.percentDrop))
    ? threshold.percentDrop
    : DEFAULT_THRESHOLD.percentDrop;

  // No existing file → can't shrink anything that isn't there.
  if (existingSize === null || existingSize === undefined) {
    return { action: 'allow', reason: 'no-existing' };
  }

  // Existing is empty → nothing to preserve.
  if (existingSize === 0) {
    return { action: 'allow', reason: 'existing-empty' };
  }

  // Same-size or growing → not a shrink at all.
  if (incomingSize >= existingSize) {
    return { action: 'allow', reason: 'no-shrink' };
  }

  // Existing is too small to bother protecting (config knob — defaults to
  // 1 KiB, below which a "drastic shrink" classification is just noise).
  if (existingSize <= absoluteFloor) {
    return { action: 'allow', reason: 'below-floor' };
  }

  // Hard rule: incoming claims an empty file over a substantial existing
  // file. Almost always a corruption signal, never a legitimate update.
  if (incomingSize === 0 && existingSize > 0) {
    return { action: 'preserve', reason: 'incoming-empty' };
  }

  // Soft rule: drop ratio crosses the configured threshold.
  const dropRatio = (existingSize - incomingSize) / existingSize;
  if (dropRatio >= percentDrop) {
    return { action: 'preserve', reason: 'drastic-shrink' };
  }

  return { action: 'allow', reason: 'normal-update' };
}

/**
 * Resolve effective threshold for a folder config object. Mirrors the
 * `getThresholds` helper in deletion-guard.js so callers can write the
 * same shape `getShrinkThreshold(folderConfig)`.
 *
 * @param {object} folderConfig - Folder config record (may be undefined).
 * @returns {{ absoluteFloor: number, percentDrop: number }}
 */
function getShrinkThreshold(folderConfig) {
  const t = folderConfig && folderConfig.shrinkGuard;
  const absoluteFloor = (t && Number.isFinite(t.absoluteFloor))
    ? t.absoluteFloor
    : DEFAULT_THRESHOLD.absoluteFloor;
  const percentDrop = (t && Number.isFinite(t.percentDrop))
    ? t.percentDrop
    : DEFAULT_THRESHOLD.percentDrop;
  return { absoluteFloor, percentDrop };
}

/**
 * Format a sync.log line describing a blocked overwrite. Companion to
 * BLOCKED_LOG_MESSAGE in deletion-guard.js.
 *
 * Example:
 *   [2026-04-19T03:06:39.233Z] SHRINK-BLOCKED: NeoCreate/mods.json
 *   existing=240153 incoming=0 ratio=100.0% peer=DESKTOP-532Q4KG
 *   reason=incoming-empty — kept existing, incoming saved as
 *   .shrink-blocked.<peer>.<ts>
 */
const SHRINK_BLOCKED_LOG_MESSAGE = ({
  folderName, relPath, peerName, existingSize, incomingSize, reason,
  conflictPath, timestamp,
} = {}) => {
  const ts = timestamp || new Date().toISOString();
  const ratio = existingSize > 0
    ? `${(((existingSize - incomingSize) / existingSize) * 100).toFixed(1)}%`
    : 'n/a';
  const sidecar = conflictPath ? ` sidecar=${conflictPath}` : '';
  return (
    `[${ts}] SHRINK-BLOCKED: ${folderName}/${relPath} ` +
    `existing=${existingSize} incoming=${incomingSize} ratio=${ratio} ` +
    `peer=${peerName} reason=${reason}${sidecar} — kept existing`
  );
};

module.exports = {
  classifyOverwrite,
  getShrinkThreshold,
  SHRINK_BLOCKED_LOG_MESSAGE,
  DEFAULT_THRESHOLD,
};
