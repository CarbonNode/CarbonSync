/**
 * Deletion Threshold Guard — refuses to apply a sync deletion batch that
 * looks catastrophic (i.e. would erase a large fraction of a folder in a
 * single operation).
 *
 * Belt-and-suspenders for the trash bucket: trash.js makes deletes
 * recoverable for `retentionDays`, this guard prevents the deletion from
 * happening in the first place when the diff looks like it came from a
 * stale or compromised peer.
 *
 * Defaults: refuse anything that would delete > 50 files OR > 25% of the
 * folder in one shot. Configurable per-folder via:
 *
 *   folder.deletionThreshold = { absolute?: number, percent?: number }
 *
 * (See config.js — the field is read lazily and not migrated; missing
 * means defaults.)
 */

'use strict';

const DEFAULT_ABSOLUTE = 50;
const DEFAULT_PERCENT = 25;

/**
 * Resolve effective thresholds for a folder config object.
 * @param {object} folderConfig - Folder config record (may be undefined).
 * @returns {{ absolute: number, percent: number }}
 */
function getThresholds(folderConfig) {
  const t = folderConfig && folderConfig.deletionThreshold;
  const absolute = (t && Number.isFinite(t.absolute)) ? t.absolute : DEFAULT_ABSOLUTE;
  const percent = (t && Number.isFinite(t.percent)) ? t.percent : DEFAULT_PERCENT;
  return { absolute, percent };
}

/**
 * Decide whether a proposed batch deletion should be allowed.
 *
 * @param {object} args
 * @param {string} args.folderName     - Folder display name (for messages).
 * @param {number} args.totalFiles     - Current local index size for that folder.
 * @param {number} args.toDeleteCount  - How many files the batch would remove.
 * @param {string} args.peerName       - Source peer (for messages).
 * @param {{ absolute?: number, percent?: number }} [args.thresholds]
 * @returns {{ allowed: boolean, reason: string|null,
 *             threshold: { absolute: number, percent: number }|null }}
 */
function evaluateDeletion({ folderName, totalFiles, toDeleteCount, peerName, thresholds } = {}) {
  // Nothing to delete — always allowed, nothing else to check.
  if (!toDeleteCount || toDeleteCount <= 0) {
    return { allowed: true, reason: null, threshold: null };
  }

  const { absolute, percent } = getThresholds({ deletionThreshold: thresholds });
  const effective = { absolute, percent };

  // Empty index — no baseline to compare a percentage against. Fall through.
  // (We still enforce the absolute ceiling so a peer can't push 1000 deletes
  // into an empty folder either, even though that should be a no-op.)
  if (toDeleteCount > absolute) {
    return {
      allowed: false,
      reason: `would delete ${toDeleteCount} files, exceeds absolute threshold of ${absolute}`,
      threshold: effective,
    };
  }

  if (!totalFiles || totalFiles <= 0) {
    return { allowed: true, reason: null, threshold: null };
  }

  const pct = (toDeleteCount / totalFiles) * 100;
  if (pct > percent) {
    return {
      allowed: false,
      reason:
        `would delete ${toDeleteCount}/${totalFiles} files ` +
        `(${pct.toFixed(1)}%), exceeds percent threshold of ${percent}%`,
      threshold: effective,
    };
  }

  // (folderName/peerName are accepted for symmetry with BLOCKED_LOG_MESSAGE
  // and to make caller wiring consistent; they aren't used in the decision.)
  void folderName; void peerName;

  return { allowed: true, reason: null, threshold: null };
}

/**
 * Format a sync.log line describing a blocked deletion.
 * Callers append this to ~/.carbonsync/sync.log so a human can see exactly
 * which threshold tripped and on what numbers.
 *
 * Example output:
 *   [2026-04-20T03:06:39.233Z] BLOCKED: refused to delete 60 files in
 *   folder=NeoCreate from peer=DESKTOP-532Q4KG — would delete 60/847
 *   files (7.1%), exceeds absolute threshold of 50
 */
const BLOCKED_LOG_MESSAGE = ({ folderName, peerName, count, reason, timestamp } = {}) => {
  const ts = timestamp || new Date().toISOString();
  return (
    `[${ts}] BLOCKED: refused to delete ${count} files ` +
    `in folder=${folderName} from peer=${peerName} — ${reason}`
  );
};

module.exports = {
  evaluateDeletion,
  getThresholds,
  BLOCKED_LOG_MESSAGE,
  DEFAULT_ABSOLUTE,
  DEFAULT_PERCENT,
};
