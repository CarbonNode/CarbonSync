#!/usr/bin/env node
'use strict';

/**
 * recover-mod-list.js
 *
 * Parses Minecraft logs (latest.log + rotated *.log.gz) under a given instance
 * directory, extracts the set of mod jar filenames that were ever loaded, and
 * writes a Markdown checklist marking which are currently present in the
 * instance's mods/ folder vs missing (likely deleted).
 *
 * Usage:
 *   node tools/recover-mod-list.js <instance-dir> [--out <output-path>]
 */

const fs = require('node:fs');
const path = require('node:path');
const zlib = require('node:zlib');
const os = require('node:os');

// --- Filtering rules for non-mod jars --------------------------------------

// Path-substring blacklist (case-insensitive). If a jar token appears with one
// of these segments anywhere in its captured surroundings, drop it.
const PATH_SEGMENT_BLACKLIST = [
  'versions/',
  'versions\\',
  'libraries/',
  'libraries\\',
  'assets/',
  'assets\\',
  'natives/',
  'natives\\',
];

// Filename prefixes to drop (case-insensitive).
const FILENAME_PREFIX_BLACKLIST = [
  'forge-',
  'neoforge-',
  'minecraft-',
  'fmlcore-',
  'fmlloader-',
  'fmlearlydisplay-',
  'javafmllanguage-',
  'lowcodelanguage-',
  'mclanguage-',
];

// Exact filenames to drop (case-insensitive).
const FILENAME_EXACT_BLACKLIST = new Set([
  'client.jar',
  'server.jar',
]);

// --- Jar extraction --------------------------------------------------------

// Match a jar reference. We deliberately exclude whitespace, path separators,
// and quote characters from the captured token. Brackets / parens are allowed
// because real mod filenames sometimes contain them (e.g.
// `born_in_chaos_[Neoforge]_1.21.1_1.7.3.jar`, `Nexus-1.21-(v.2.0.0-NEO).jar`).
const JAR_TOKEN_RE = /[A-Za-z0-9._+\-()\[\]]+\.jar/g;

// NeoForge / FML emits structured "Found mod file" lines that include the
// originating locator. We strongly prefer mods sourced from the
// `mods folder locator` over any other locator: those are the user-installed
// jars in mods/. Other locators (Sinytra connector inner jars, Forge built-ins,
// path-based libraries) are NOT files the user manages and must be skipped.
//
// Example matched line:
//   Found mod file "create-1.21.1-6.0.9.jar" [locator: {mods folder locator at C:\...\mods}, reader: mod manifest]
const FOUND_MOD_FILE_RE = /Found mod file "([^"]+\.jar)" \[locator: ([^\]]+)\]/g;
const MODS_FOLDER_LOCATOR_RE = /mods folder locator/i;

/**
 * Extract structured "Found mod file" entries that came from the mods folder
 * locator. These are the gold standard — the loader is telling us exactly
 * which jars in the user's mods/ folder it picked up. Returns an array of
 * { filename, context } tuples.
 */
function extractStructuredModFileRefs(text) {
  const out = [];
  let m;
  FOUND_MOD_FILE_RE.lastIndex = 0;
  while ((m = FOUND_MOD_FILE_RE.exec(text)) !== null) {
    const rawName = m[1];
    const locator = m[2];
    if (!MODS_FOLDER_LOCATOR_RE.test(locator)) continue;
    const filename = path.basename(rawName.replace(/\\/g, '/'));
    out.push({ filename, context: locator });
  }
  return out;
}

/**
 * Permissive fallback: any `*.jar` token in the text. Returns an array of
 * { filename, context } tuples — context is a small surrounding window so the
 * filter step can inspect path segments like `versions/`.
 */
function extractJarReferences(text) {
  const out = [];
  let m;
  JAR_TOKEN_RE.lastIndex = 0;
  while ((m = JAR_TOKEN_RE.exec(text)) !== null) {
    const token = m[0];
    // The token may be a full path; reduce to basename.
    const filename = path.basename(token.replace(/\\/g, '/'));
    // Capture a context window (60 chars before) so we can detect path segments.
    const start = Math.max(0, m.index - 60);
    const ctx = text.slice(start, m.index + token.length);
    out.push({ filename, context: ctx });
  }
  return out;
}

function isLikelyModJar(filename, context) {
  const lowerName = filename.toLowerCase();
  const lowerCtx = context.toLowerCase();

  if (FILENAME_EXACT_BLACKLIST.has(lowerName)) return false;

  for (const pref of FILENAME_PREFIX_BLACKLIST) {
    if (lowerName.startsWith(pref)) return false;
  }

  for (const seg of PATH_SEGMENT_BLACKLIST) {
    if (lowerCtx.includes(seg)) return false;
  }

  // Reject suspiciously short/empty names.
  if (lowerName.length <= '.jar'.length) return false;

  return true;
}

// --- Mod name/version parse ------------------------------------------------

function parseModName(filename) {
  let stem = filename;
  if (stem.toLowerCase().endsWith('.jar')) stem = stem.slice(0, -4);

  // Try: greedy left, version starting with digit.
  const m = stem.match(/^(.+?)-(\d[\w.+\-]*)$/);
  if (m) {
    return { name: m[1], version: m[2] };
  }

  // Fallback: split on last '-' if right side starts with a digit.
  const lastDash = stem.lastIndexOf('-');
  if (lastDash > 0) {
    const right = stem.slice(lastDash + 1);
    if (/^\d/.test(right)) {
      return { name: stem.slice(0, lastDash), version: right };
    }
  }

  return { name: stem, version: '' };
}

// --- Log gathering ---------------------------------------------------------

function safeReadFile(p) {
  try {
    return fs.readFileSync(p, 'utf8');
  } catch (e) {
    return null;
  }
}

function safeReadGz(p) {
  try {
    const buf = fs.readFileSync(p);
    const out = zlib.gunzipSync(buf);
    return out.toString('utf8');
  } catch (e) {
    return null;
  }
}

function gatherLogTexts(instanceDir) {
  const logsDir = path.join(instanceDir, 'logs');
  const result = { latest: 0, rotated: 0, texts: [] };
  if (!fs.existsSync(logsDir) || !fs.statSync(logsDir).isDirectory()) {
    return result;
  }

  const latestPath = path.join(logsDir, 'latest.log');
  if (fs.existsSync(latestPath)) {
    const t = safeReadFile(latestPath);
    if (t != null) {
      result.texts.push(t);
      result.latest = 1;
    }
  }

  const entries = fs.readdirSync(logsDir);
  for (const entry of entries) {
    const lower = entry.toLowerCase();
    if (!lower.endsWith('.log.gz')) continue;
    // Skip debug logs: they're crash/stack dumps full of library jars in
    // stack traces (e.g. `[bootstraplauncher-2.0.2.jar:?]`) and never
    // contain the structured "Found mod file" entries we care about.
    if (lower.startsWith('debug')) continue;
    const full = path.join(logsDir, entry);
    const t = safeReadGz(full);
    if (t != null) {
      result.texts.push(t);
      result.rotated += 1;
    }
  }

  return result;
}

// --- Current mods on disk --------------------------------------------------

function listJarsIn(dir) {
  if (!fs.existsSync(dir)) return [];
  let entries;
  try {
    entries = fs.readdirSync(dir, { withFileTypes: true });
  } catch {
    return [];
  }
  const out = [];
  for (const ent of entries) {
    if (ent.isFile() && ent.name.toLowerCase().endsWith('.jar')) {
      out.push(ent.name);
    }
  }
  return out;
}

function gatherPresentJars(instanceDir) {
  const modsDir = path.join(instanceDir, 'mods');
  const disabledDir = path.join(modsDir, '_disabled');
  const set = new Set();
  for (const f of listJarsIn(modsDir)) set.add(f);
  for (const f of listJarsIn(disabledDir)) set.add(f);
  return set;
}

// --- Markdown rendering ----------------------------------------------------

function escapeBacktick(s) {
  return s.replace(/`/g, '\\`');
}

function renderMarkdown({ instanceDir, latestCount, rotatedCount, allMods, presentSet }) {
  const allSorted = [...allMods].sort((a, b) => a.toLowerCase().localeCompare(b.toLowerCase()));
  const missing = allSorted.filter((f) => !presentSet.has(f));
  const present = allSorted.filter((f) => presentSet.has(f));

  const lines = [];
  lines.push('# NeoCreate Lost Mods - Recovery Checklist');
  lines.push('');
  lines.push(`Generated: ${new Date().toISOString()}`);
  lines.push(`Instance: \`${escapeBacktick(instanceDir)}\``);
  lines.push(`Source logs scanned: ${latestCount} latest + ${rotatedCount} rotated`);
  lines.push(`Total unique mods loaded historically: ${allSorted.length}`);
  lines.push(`Currently present: ${present.length}`);
  lines.push(`Missing (need re-download): ${missing.length}`);
  lines.push('');
  lines.push('## Missing mods (alphabetical)');
  lines.push('');
  if (missing.length === 0) {
    lines.push('_None - every historically-loaded mod is still present._');
  } else {
    for (const f of missing) {
      const { name, version } = parseModName(f);
      const versionPart = version ? ` \`${escapeBacktick(version)}\`` : '';
      lines.push(`- [ ] **${name}**${versionPart} - \`${escapeBacktick(f)}\``);
    }
  }
  lines.push('');
  lines.push('## Currently present (sanity check)');
  lines.push('');
  if (present.length === 0) {
    lines.push('_None - mods/ folder appears empty._');
  } else {
    for (const f of present) {
      lines.push(`- [x] \`${escapeBacktick(f)}\``);
    }
  }
  lines.push('');
  lines.push('## Notes');
  lines.push('');
  lines.push('- Filenames are taken from the most recent successful game launch logs.');
  lines.push('- Versions are best-effort parses - verify against Modrinth/CurseForge before downloading.');
  lines.push('- Look in `.carbonsync-trash/<date>/mods/` first if you upgraded to v2.6.1+ - your deleted files may be recoverable there for 7 days.');
  lines.push('');

  return lines.join('\n');
}

// --- Public entry point ----------------------------------------------------

function defaultOutputPath() {
  const home = os.homedir();
  return path.join(home, 'Desktop', 'NeoCreate_LostMods_Checklist.md');
}

/**
 * Run the recovery scan.
 * @param {object} opts
 * @param {string} opts.instanceDir - absolute path to the Minecraft instance
 * @param {string} [opts.outPath]   - where to write the markdown checklist
 * @param {boolean} [opts.write=true] - actually write to disk
 * @returns {{ outPath: string, markdown: string, totals: { all: number, present: number, missing: number, latest: number, rotated: number } }}
 */
function run(opts) {
  if (!opts || !opts.instanceDir) {
    throw new Error('run({ instanceDir }) is required');
  }
  const instanceDir = path.resolve(opts.instanceDir);
  const outPath = opts.outPath ? path.resolve(opts.outPath) : defaultOutputPath();
  const write = opts.write !== false;

  if (!fs.existsSync(instanceDir) || !fs.statSync(instanceDir).isDirectory()) {
    throw new Error(`Instance directory does not exist: ${instanceDir}`);
  }

  const { latest, rotated, texts } = gatherLogTexts(instanceDir);

  const seen = new Set();
  for (const t of texts) {
    // Prefer the structured "Found mod file ... [locator: mods folder locator]"
    // entries when present — they're authoritative. Only fall back to the
    // permissive scan for log files that have no such structured lines (older
    // loader versions, partial logs).
    const structured = extractStructuredModFileRefs(t);
    if (structured.length > 0) {
      for (const ref of structured) {
        if (isLikelyModJar(ref.filename, ref.context)) {
          seen.add(ref.filename);
        }
      }
    } else {
      for (const ref of extractJarReferences(t)) {
        if (isLikelyModJar(ref.filename, ref.context)) {
          seen.add(ref.filename);
        }
      }
    }
  }

  const present = gatherPresentJars(instanceDir);

  const markdown = renderMarkdown({
    instanceDir,
    latestCount: latest,
    rotatedCount: rotated,
    allMods: seen,
    presentSet: present,
  });

  if (write) {
    fs.mkdirSync(path.dirname(outPath), { recursive: true });
    fs.writeFileSync(outPath, markdown, 'utf8');
  }

  const allArr = [...seen];
  const missingCount = allArr.filter((f) => !present.has(f)).length;
  const presentCount = allArr.length - missingCount;

  return {
    outPath,
    markdown,
    totals: {
      all: allArr.length,
      present: presentCount,
      missing: missingCount,
      latest,
      rotated,
    },
  };
}

// --- CLI -------------------------------------------------------------------

function parseArgv(argv) {
  const args = argv.slice(2);
  let instanceDir = null;
  let outPath = null;
  for (let i = 0; i < args.length; i++) {
    const a = args[i];
    if (a === '--out' || a === '-o') {
      outPath = args[++i];
    } else if (a === '--help' || a === '-h') {
      return { help: true };
    } else if (!instanceDir) {
      instanceDir = a;
    }
  }
  return { instanceDir, outPath };
}

function printHelp() {
  process.stdout.write(
    'Usage: node tools/recover-mod-list.js <minecraft-instance-dir> [--out <output-path>]\n' +
      '\n' +
      'Scans <instance-dir>/logs/latest.log and <instance-dir>/logs/*.log.gz for\n' +
      'mod jar filenames, then writes a Markdown checklist of which are missing\n' +
      'from <instance-dir>/mods/. Defaults output to your Desktop.\n',
  );
}

function main() {
  const parsed = parseArgv(process.argv);
  if (parsed.help) {
    printHelp();
    return;
  }
  if (!parsed.instanceDir) {
    process.stderr.write('error: missing <minecraft-instance-dir>\n');
    printHelp();
    process.exit(2);
  }
  const result = run({ instanceDir: parsed.instanceDir, outPath: parsed.outPath });
  process.stdout.write(
    `Scanned ${result.totals.latest} latest + ${result.totals.rotated} rotated logs.\n` +
      `Mods loaded historically: ${result.totals.all}\n` +
      `Present: ${result.totals.present}\n` +
      `Missing: ${result.totals.missing}\n` +
      `Wrote: ${result.outPath}\n`,
  );
}

if (require.main === module) {
  main();
}

module.exports = {
  run,
  // exported for testing
  extractJarReferences,
  isLikelyModJar,
  parseModName,
};
