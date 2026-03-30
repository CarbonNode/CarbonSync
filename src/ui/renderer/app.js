const api = window.carbonsync;
const activityLog = [];
const MAX_ACTIVITY = 100;
let currentStatus = {};

let gameLibrary = [];
const expandedGames = new Set();

async function init() {
  setupTitlebar();
  setupTabs();
  setupFolderActions();
  setupDragDrop();
  setupDevices();
  setupSettings();
  setupGames();
  setupLiveEvents();
  await refresh();
  setInterval(refresh, 8000);
}

// ---- Titlebar ----
function setupTitlebar() {
  document.getElementById('btn-minimize').addEventListener('click', () => api.minimize());
  document.getElementById('btn-close').addEventListener('click', () => api.closeWindow());
}

// ---- Tabs ----
function setupTabs() {
  document.querySelectorAll('.tab').forEach(tab => {
    tab.addEventListener('click', () => {
      document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
      document.querySelectorAll('.tab-pane').forEach(p => p.classList.remove('active'));
      tab.classList.add('active');
      document.getElementById(`tab-${tab.dataset.tab}`).classList.add('active');
    });
  });
}

// ---- Refresh ----
async function refresh() {
  try { currentStatus = await api.getStatus(); } catch { return; }
  const s = currentStatus;

  document.getElementById('subtitle').textContent = s.deviceName || 'Starting...';
  document.getElementById('port').textContent = s.port || '—';
  document.getElementById('clients').textContent = s.connectedClients || 0;
  document.getElementById('tls-status').innerHTML = s.tlsEnabled
    ? '<span class="tls-badge on">TLS</span>' : '<span class="tls-badge off">TCP</span>';

  renderFolders(s.folders || []);
  renderDevices(s.discoveredDevices || []);
  renderSettings(s);
}

// ---- Folders ----
let folderSearchQuery = '';

function renderFolders(folders) {
  // Filter out internal folders (Game Saves — managed in Games tab)
  folders = folders.filter(f => !f.internal);
  // Apply search filter
  if (folderSearchQuery) {
    const q = folderSearchQuery.toLowerCase();
    folders = folders.filter(f => f.name.toLowerCase().includes(q) || f.path.toLowerCase().includes(q));
  }
  const el = document.getElementById('folders-list');
  if (folders.length === 0) {
    el.innerHTML = '<div class="empty">No folders synced yet. Add a folder or drag one in.</div>';
    return;
  }

  el.innerHTML = folders.map(f => {
    const excludeCount = (f.excludes || []).length;
    const dir = f.direction || 'both';

    // Device sync icons
    let deviceHtml = '';
    if (f.devices) {
      for (const [name, ds] of Object.entries(f.devices)) {
        const peerName = currentStatus.peers?.[name] || name;
        const icon = ds.status === 'source' ? '🟢' :
                     ds.status === 'synced' ? '✅' :
                     ds.status === 'syncing' ? '🔄' : '⬜';
        const pctText = ds.status === 'syncing' ? ` ${ds.progress}%` : '';
        const tip = ds.status === 'source' ? 'Source (this PC)' :
                    ds.status === 'synced' ? `Synced${ds.lastSync ? ' ' + new Date(ds.lastSync).toLocaleTimeString() : ''}` :
                    ds.status === 'syncing' ? `Syncing ${ds.filesComplete}/${ds.filesTotal}` : 'Not synced';
        deviceHtml += `<span class="device-icon" title="${esc(tip)}">${icon} ${esc(peerName)}${pctText}</span>`;
      }
    }

    return `<div class="folder-card">
      <div class="folder-header">
        <div class="folder-info">
          <div class="folder-name">${esc(f.name)}</div>
          <div class="folder-path">${esc(f.path)}</div>
          <div class="folder-meta">
            <span><strong>${(f.fileCount || 0).toLocaleString()}</strong> files</span>
            <span><strong>${fmt(f.totalSize)}</strong></span>
            ${excludeCount > 0 ? `<span class="exclude-badge">${excludeCount} exclude${excludeCount > 1 ? 's' : ''}</span>` : ''}
          </div>
          <div class="device-row">${deviceHtml}</div>
        </div>
        <div class="folder-btns">
          <select class="direction-select" onchange="setDirection('${escA(f.name)}', this.value)">
            <option value="both" ${dir==='both'?'selected':''}>⇆ Push + Receive</option>
            <option value="push" ${dir==='push'?'selected':''}>⬆ Push to Hub</option>
            <option value="receive" ${dir==='receive'?'selected':''}>⬇ Receive from Hub</option>
          </select>
          <button class="btn sm ghost" onclick="openFolderSettings('${escA(f.name)}')">⚙</button>
          <button class="btn sm ghost" onclick="rescanFolder('${escA(f.name)}')">Rescan</button>
          <button class="btn sm red" onclick="removeFolder('${escA(f.path)}')">Remove</button>
        </div>
      </div>
    </div>`;
  }).join('');
}

// ---- Folder Settings Popup ----
async function openFolderSettings(folderName) {
  const excludes = await api.getFolderExcludes(folderName);
  const popup = document.getElementById('folder-settings-popup');
  const list = document.getElementById('excludes-list');
  const nameEl = document.getElementById('popup-folder-name');

  nameEl.textContent = folderName;
  popup.dataset.folder = folderName;

  list.innerHTML = excludes.length > 0
    ? excludes.map((e, i) => `<div class="exclude-item">
        <span class="exclude-pattern">${esc(e)}</span>
        <button class="btn sm red" onclick="removeExclude(${i})">×</button>
      </div>`).join('')
    : '<div class="empty">No excludes. All files will sync.</div>';

  popup.classList.remove('hidden');
}

function closePopup() {
  document.getElementById('folder-settings-popup').classList.add('hidden');
}

async function addExclude() {
  const input = document.getElementById('new-exclude');
  const pattern = input.value.trim();
  if (!pattern) return;

  const folder = document.getElementById('folder-settings-popup').dataset.folder;
  const excludes = await api.getFolderExcludes(folder);
  excludes.push(pattern);
  await api.setFolderExcludes(folder, excludes);
  input.value = '';
  openFolderSettings(folder);
  refresh();
  toast(`Exclude added: ${pattern}`, 'success');
}

async function removeExclude(index) {
  const folder = document.getElementById('folder-settings-popup').dataset.folder;
  const excludes = await api.getFolderExcludes(folder);
  excludes.splice(index, 1);
  await api.setFolderExcludes(folder, excludes);
  openFolderSettings(folder);
  refresh();
}

function setupFolderActions() {
  document.getElementById('folder-search').addEventListener('input', (e) => {
    folderSearchQuery = e.target.value;
    renderFolders(currentStatus.folders || []);
  });

  document.getElementById('btn-add').addEventListener('click', async () => {
    const p = await api.pickFolder();
    if (p) {
      await api.addFolder(p);
      addActivity('scan', `Added folder: ${p}`);
      toast(`Added: ${p}`, 'success');
      refresh();
    }
  });

  document.getElementById('btn-add-exclude').addEventListener('click', addExclude);
  document.getElementById('new-exclude').addEventListener('keydown', (e) => {
    if (e.key === 'Enter') addExclude();
  });
  document.getElementById('btn-close-popup').addEventListener('click', closePopup);
  document.getElementById('folder-settings-popup').addEventListener('click', (e) => {
    if (e.target === e.currentTarget) closePopup();
  });
}

async function setDirection(folderName, direction) {
  await api.setFolderDirection(folderName, direction);
  toast(`${folderName}: ${direction}`, 'success');
  refresh();
}

async function rescanFolder(name) {
  addActivity('scan', `Rescanning ${name}...`);
  await api.rescan(name);
  setTimeout(refresh, 2000);
}

async function removeFolder(p) {
  await api.removeFolder(p);
  addActivity('scan', `Removed folder: ${p}`);
  toast('Folder removed', 'info');
  refresh();
}

// ---- Drag & Drop ----
function setupDragDrop() {
  const zone = document.getElementById('drop-zone');
  ['dragenter', 'dragover'].forEach(e => {
    document.body.addEventListener(e, (ev) => { ev.preventDefault(); zone.classList.add('over'); });
  });
  ['dragleave', 'drop'].forEach(e => {
    document.body.addEventListener(e, () => zone.classList.remove('over'));
  });
  document.body.addEventListener('drop', async (ev) => {
    ev.preventDefault();
    const files = ev.dataTransfer?.files;
    if (!files) return;
    for (const file of files) {
      if (file.path) {
        try {
          await api.addFolder(file.path);
          toast(`Added: ${file.name}`, 'success');
        } catch (err) { toast(`Failed: ${err.message}`, 'error'); }
      }
    }
    refresh();
  });
}

// ---- Activity ----
function addActivity(type, message) {
  activityLog.unshift({ type, message, time: Date.now() });
  if (activityLog.length > MAX_ACTIVITY) activityLog.pop();
  renderActivity();
}

function renderActivity() {
  const el = document.getElementById('activity-feed');
  if (activityLog.length === 0) { el.innerHTML = '<div class="empty">No activity yet</div>'; return; }
  el.innerHTML = activityLog.map(a => {
    const iconClass = a.type === 'connect' ? 'green' : a.type === 'disconnect' ? 'red' : a.type === 'change' ? 'blue' : 'dim';
    return `<div class="activity-item">
      <div class="activity-icon ${iconClass}"></div>
      <span class="activity-time">${new Date(a.time).toLocaleTimeString()}</span>
      <span class="activity-msg">${esc(a.message)}</span>
    </div>`;
  }).join('');
}

// ---- Devices ----
function renderDevices(devices) {
  // Filter out self (by deviceId or hostname matching this device's name)
  const myId = currentStatus.deviceId || '';
  const myName = currentStatus.deviceName || '';
  devices = devices.filter(d => {
    if (d.deviceId && d.deviceId === myId) return false;
    if (d.hostname === myName) return false;
    return true;
  });

  const el = document.getElementById('devices-list');
  if (devices.length === 0) {
    el.innerHTML = '<div class="empty">No other CarbonSync devices found</div>';
    return;
  }
  el.innerHTML = devices.map(d => `
    <div class="device-card">
      <div>
        <div class="device-name">
          <span>${esc(d.friendlyName || d.hostname)}</span>
          ${d.friendlyName !== d.hostname ? `<span class="device-hostname">(${esc(d.hostname)})</span>` : ''}
          <button class="btn sm ghost rename-btn" data-hostname="${escA(d.hostname)}" onclick="renamePeer('${escA(d.hostname)}', '${escA(d.friendlyName || d.hostname)}')">Rename</button>
        </div>
        <div class="device-detail">${d.ip}:${d.port}</div>
      </div>
      <div style="display:flex;align-items:center;gap:6px;">
        <span class="device-role ${d.role}">${d.role}</span>
        <button class="btn sm red" onclick="removePeer('${escA(d.ip)}', ${d.port})">Remove</button>
      </div>
    </div>
  `).join('');
}

function renamePeer(hostname, currentName) {
  // Show inline rename input
  const el = document.getElementById('devices-list');
  const cards = el.querySelectorAll('.device-card');
  for (const card of cards) {
    const nameEl = card.querySelector('.device-name span');
    if (!nameEl) continue;
    const btn = card.querySelector('.rename-btn');
    if (btn && btn.dataset.hostname === hostname) {
      // Replace name with input
      const input = document.createElement('input');
      input.type = 'text';
      input.value = currentName;
      input.style.cssText = 'background:var(--bg);border:1px solid var(--accent);border-radius:4px;padding:3px 8px;color:var(--text);font-size:13px;width:180px;outline:none;';
      nameEl.replaceWith(input);
      input.focus();
      input.select();

      let saved = false;
      const save = async () => {
        if (saved) return;
        saved = true;
        const newName = input.value.trim();
        if (newName && newName !== currentName) {
          await api.renamePeer(hostname, newName);
          toast(`Renamed to: ${newName}`, 'success');
        }
        refresh();
      };

      input.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') { e.preventDefault(); save(); }
        if (e.key === 'Escape') { saved = true; refresh(); }
      });
      input.addEventListener('blur', save);
      break;
    }
  }
}

async function removePeer(ip, port) {
  await api.removePeer(ip, port);
  toast(`Removed peer ${ip}:${port}`, 'info');
  refresh();
}

function setupDevices() {
  document.getElementById('btn-add-peer').addEventListener('click', async () => {
    const ip = document.getElementById('peer-ip').value.trim();
    const port = parseInt(document.getElementById('peer-port').value) || 21547;
    if (!ip) { toast('Enter an IP address', 'error'); return; }
    const btn = document.getElementById('btn-add-peer');
    btn.disabled = true; btn.textContent = 'Connecting...';
    const result = await api.addPeer(ip, port);
    btn.disabled = false; btn.textContent = 'Add Peer';
    if (result.error) toast(result.error, 'error');
    else { toast(`Connected to ${ip}:${port}`, 'success'); document.getElementById('peer-ip').value = ''; refresh(); }
  });
}

// ---- Settings ----
function renderSettings(s) {
  document.getElementById('fp-display').textContent = s.fingerprint || '—';
  document.getElementById('key-display').textContent = s.apiKey || '—';
  document.getElementById('device-id').textContent = s.deviceId || '—';
}

function setupSettings() {
  document.getElementById('btn-save-name').addEventListener('click', async () => {
    const name = document.getElementById('set-device-name').value.trim();
    if (name) {
      await api.setDeviceName(name);
      toast(`Device name set to: ${name}`, 'success');
      refresh();
    }
  });

  document.getElementById('btn-save-settings').addEventListener('click', async () => {
    await api.updateSettings({
      scanIntervalMinutes: parseInt(document.getElementById('set-scan-interval').value) || 5,
      bandwidthLimitMBps: parseInt(document.getElementById('set-bandwidth').value) || 0,
      maxConcurrentTransfers: parseInt(document.getElementById('set-concurrent').value) || 4,
    });
    toast('Settings saved', 'success');
  });

  document.getElementById('btn-check-update').addEventListener('click', async () => {
    const btn = document.getElementById('btn-check-update');
    const status = document.getElementById('update-status');
    btn.disabled = true; btn.textContent = 'Checking...';
    try {
      const r = await api.checkUpdate();
      if (r.error) status.innerHTML = `<span style="color:var(--red)">${esc(r.error)}</span>`;
      else if (r.hasUpdate) {
        status.innerHTML = `<span style="color:var(--green)">v${esc(r.version)} available!</span> <button class="btn sm green" id="btn-do-update">Download</button>`;
        document.getElementById('btn-do-update').addEventListener('click', async () => {
          status.innerHTML = '<span style="color:var(--accent)">Downloading...</span>';
          const result = await api.downloadUpdate();
          status.innerHTML = result.success
            ? `<span style="color:var(--green)">Downloaded! Close and run new exe.</span>`
            : `<span style="color:var(--red)">${esc(result.message)}</span>`;
        });
      } else status.innerHTML = '<span style="color:var(--text-dim)">Up to date</span>';
    } catch (e) { status.innerHTML = `<span style="color:var(--red)">${esc(e.message)}</span>`; }
    btn.disabled = false; btn.textContent = 'Check for Updates';
  });

  api.getConfig().then(cfg => {
    const s = cfg.settings || {};
    document.getElementById('set-scan-interval').value = s.scanIntervalMinutes || 5;
    document.getElementById('set-bandwidth').value = s.bandwidthLimitMBps || 0;
    document.getElementById('set-concurrent').value = s.maxConcurrentTransfers || 4;
    document.getElementById('set-port').value = cfg.port || 21547;
    document.getElementById('set-device-name').value = cfg.deviceName || '';
  });
  api.checkUpdate().then(r => {
    document.getElementById('current-version').textContent = r.current || '—';
  }).catch(() => {});
}

// ---- Games ----

let scanDirsExpanded = false;
const expandedBackups = new Set(); // "gameId:backupDir" keys

async function refreshGames() {
  try { gameLibrary = await api.getGameLibrary(); } catch { gameLibrary = []; }
  renderGames();
  renderScanDirs();
  renderScanStatus();
}

function renderScanStatus() {
  const el = document.getElementById('games-scan-status');
  if (!el) return;
  api.getConfig().then(cfg => {
    const enabled = cfg.settings?.gameSaveEnabled !== false;
    if (!enabled) {
      el.innerHTML = '<span style="color:var(--red)">Detection disabled</span>';
    } else {
      const dirs = cfg.settings?.gameSaveScanDirs || ['documents', 'appdata_roaming', 'appdata_local', 'appdata_locallow'];
      el.innerHTML = `<span style="color:var(--green)">Watching ${dirs.length} dir${dirs.length !== 1 ? 's' : ''}</span>`;
    }
  }).catch(() => {});
}

const SCAN_DIR_LABELS = {
  documents: 'Documents',
  appdata_roaming: 'AppData / Roaming',
  appdata_local: 'AppData / Local',
  appdata_locallow: 'AppData / LocalLow',
};

function toggleScanDirsPanel() {
  scanDirsExpanded = !scanDirsExpanded;
  const body = document.getElementById('scan-dirs-body');
  const chevron = document.getElementById('scan-dirs-chevron');
  if (scanDirsExpanded) {
    body.classList.remove('hidden');
    chevron.classList.add('expanded');
  } else {
    body.classList.add('hidden');
    chevron.classList.remove('expanded');
  }
}

async function renderScanDirs() {
  const body = document.getElementById('scan-dirs-body');
  if (!body) return;
  let cfg;
  try { cfg = await api.getConfig(); } catch { return; }
  const enabled = cfg.settings?.gameSaveScanDirs || ['documents', 'appdata_roaming', 'appdata_local', 'appdata_locallow'];

  body.innerHTML = Object.entries(SCAN_DIR_LABELS).map(([key, label]) => {
    const isOn = enabled.includes(key);
    return `<div class="scan-dir-row">
      <label class="game-toggle">
        <input type="checkbox" ${isOn ? 'checked' : ''} onchange="toggleScanDir('${key}', this.checked)">
        <span class="toggle-slider"></span>
      </label>
      <span class="scan-dir-label">${label}</span>
    </div>`;
  }).join('');
}

async function toggleScanDir(dirKey, enabled) {
  let cfg;
  try { cfg = await api.getConfig(); } catch { return; }
  let dirs = cfg.settings?.gameSaveScanDirs || ['documents', 'appdata_roaming', 'appdata_local', 'appdata_locallow'];
  if (enabled && !dirs.includes(dirKey)) dirs.push(dirKey);
  else if (!enabled) dirs = dirs.filter(d => d !== dirKey);
  await api.updateSettings({ gameSaveScanDirs: dirs });
  toast(enabled ? `Now watching ${SCAN_DIR_LABELS[dirKey]}` : `Stopped watching ${SCAN_DIR_LABELS[dirKey]}`, 'success');
  renderScanDirs();
  renderScanStatus();
}

let gameSearchQuery = '';

function renderGames() {
  const el = document.getElementById('games-list');
  const countEl = document.getElementById('games-count');
  const enabledCount = gameLibrary.filter(g => g.enabled).length;
  countEl.textContent = `${gameLibrary.length} game${gameLibrary.length !== 1 ? 's' : ''} detected (${enabledCount} active)`;

  let filtered = gameLibrary;
  if (gameSearchQuery) {
    const q = gameSearchQuery.toLowerCase();
    filtered = gameLibrary.filter(g =>
      (g.displayName || g.name).toLowerCase().includes(q) ||
      (g.saveBase || '').toLowerCase().includes(q) ||
      (g.id || '').toLowerCase().includes(q)
    );
  }

  if (filtered.length === 0) {
    el.innerHTML = gameLibrary.length === 0
      ? '<div class="empty">No game saves detected yet. Games will appear here automatically when saves are found in Documents or AppData.</div>'
      : '<div class="empty">No games match your search.</div>';
    return;
  }

  el.innerHTML = filtered.map(g => {
    const isExpanded = expandedGames.has(g.id);
    const displayName = g.displayName || g.name;
    const timeAgo = g.lastBackup ? fmtTimeAgo(g.lastBackup) : 'never';

    return `<div class="game-card${g.enabled ? '' : ' disabled'}">
      <div class="game-header" onclick="toggleGameExpand('${escA(g.id)}')">
        <svg class="game-chevron${isExpanded ? ' expanded' : ''}" viewBox="0 0 24 24" width="16" height="16">
          <path d="M9 6l6 6-6 6" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"/>
        </svg>
        <div class="game-info">
          <div class="game-name">
            ${esc(displayName)}
            ${g.isHeuristic && !isGameConfirmed(g.id) ? '<span class="game-heuristic" title="Auto-detected — click to confirm or dismiss">?</span>' : ''}
            ${g.running ? '<span class="game-running">RUNNING</span>' : ''}
          </div>
          <div class="game-meta">
            <span>Last backup: <strong>${timeAgo}</strong></span>
            <span>${g.backupCount || 0} version${(g.backupCount || 0) !== 1 ? 's' : ''}</span>
            ${g.excludes?.length > 0 ? `<span class="exclude-badge">${g.excludes.length} exclude${g.excludes.length > 1 ? 's' : ''}</span>` : ''}
          </div>
        </div>
        <div class="game-actions" onclick="event.stopPropagation()">
          <label class="game-toggle" title="${g.enabled ? 'Disable' : 'Enable'} sync">
            <input type="checkbox" ${g.enabled ? 'checked' : ''} onchange="toggleGameSync('${escA(g.id)}', this.checked)">
            <span class="toggle-slider"></span>
          </label>
          <button class="btn sm ghost" onclick="openGameSettings('${escA(g.id)}')" title="Settings">&#9881;</button>
          <button class="btn sm ghost" onclick="backupGameNow('${escA(g.id)}')" title="Backup now">&#8635;</button>
          <button class="btn sm ghost" onclick="dismissGameEntry('${escA(g.id)}')" title="Dismiss this game">&#10005;</button>
        </div>
      </div>
      ${isExpanded ? `<div class="save-history" id="history-${g.id}"><div class="empty" style="padding:8px 0;">Loading...</div></div>` : ''}
    </div>`;
  }).join('');

  // Load history for expanded games
  for (const id of expandedGames) {
    loadGameHistory(id);
  }
}

function isGameConfirmed(gameId) {
  // Check local cache — this is a simplification, real check is server-side
  const game = gameLibrary.find(g => g.id === gameId);
  return game && !game.isHeuristic;
}

async function toggleGameExpand(gameId) {
  if (expandedGames.has(gameId)) {
    expandedGames.delete(gameId);
  } else {
    expandedGames.add(gameId);
  }
  renderGames();
}

async function loadGameHistory(gameId) {
  const el = document.getElementById(`history-${gameId}`);
  if (!el) return;

  try {
    const history = await api.getSaveHistory(gameId);
    if (history.length === 0) {
      const game = gameLibrary.find(g => g.id === gameId);
      const savePath = game?.saveBase ? `<div class="save-path-hint">${esc(game.saveBase)}</div>` : '';
      el.innerHTML = `<div class="empty" style="padding:8px 0;">No backups yet — play the game or click &#8635; to backup now</div>${savePath}`;
      return;
    }

    el.innerHTML = history.map(h => {
      const isPreRestore = h.dir.startsWith('pre-restore-');
      const backupKey = `${gameId}:${h.dir}`;
      const filesExpanded = expandedBackups.has(backupKey);
      const date = new Date(h.timestamp);
      const dateStr = date.toLocaleDateString() + ' ' + date.toLocaleTimeString();
      return `<div class="save-entry${isPreRestore ? ' pre-restore' : ''}">
        <div class="save-row">
          <svg class="save-chevron${filesExpanded ? ' expanded' : ''}" viewBox="0 0 24 24" width="12" height="12"
               onclick="toggleBackupFiles('${escA(gameId)}', '${escA(h.dir)}')" style="cursor:pointer;flex-shrink:0;">
            <path d="M9 6l6 6-6 6" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"/>
          </svg>
          <span class="save-time">${dateStr}</span>
          ${isPreRestore ? '<span class="save-safety-tag">PRE-RESTORE</span>' : ''}
          <span class="save-detail">${h.fileCount} file${h.fileCount !== 1 ? 's' : ''}, ${fmt(h.totalSize)}</span>
          <span class="save-device">${esc(h.sourceDevice)}</span>
          <button class="btn sm" onclick="restoreSave('${escA(gameId)}', '${escA(h.dir)}')">Restore</button>
        </div>
        ${filesExpanded ? `<div class="save-files" id="files-${backupKey.replace(/[^a-z0-9-]/gi, '_')}"><span class="save-files-loading">Loading files...</span></div>` : ''}
      </div>`;
    }).join('');

    // Load file lists for expanded backups
    for (const key of expandedBackups) {
      if (key.startsWith(gameId + ':')) {
        const dir = key.slice(gameId.length + 1);
        loadBackupFiles(gameId, dir);
      }
    }
  } catch (err) {
    el.innerHTML = `<div class="empty" style="padding:8px 0;color:var(--red);">Error: ${esc(err.message)}</div>`;
  }
}

async function toggleGameSync(gameId, enabled) {
  await api.toggleGameSync(gameId, enabled);
  toast(enabled ? 'Sync enabled' : 'Sync disabled', 'success');
  refreshGames();
}

async function dismissGameEntry(gameId) {
  const game = gameLibrary.find(g => g.id === gameId);
  const name = game?.displayName || game?.name || gameId;
  await api.dismissGame(gameId);
  toast(`Dismissed: ${name}`, 'info');
  refreshGames();
}

async function backupGameNow(gameId) {
  try {
    const result = await api.backupGameNow(gameId);
    if (result) toast(`Backed up: ${result.fileCount} files`, 'success');
    else toast('No files to back up', 'info');
    refreshGames();
  } catch (err) {
    toast(`Backup failed: ${err.message}`, 'error');
  }
}

async function restoreSave(gameId, backupDir) {
  const game = gameLibrary.find(g => g.id === gameId);
  if (game?.running) {
    toast('Game appears to be running — close it before restoring', 'error');
    return;
  }
  if (!confirm('Restore this save? A safety backup of your current save will be created first.')) return;
  try {
    const result = await api.restoreSave(gameId, backupDir);
    toast(`Restored ${result.restoredFiles} files (pre-restore backup saved)`, 'success');
    refreshGames();
  } catch (err) {
    toast(`Restore failed: ${err.message}`, 'error');
  }
}

function toggleBackupFiles(gameId, dir) {
  const key = `${gameId}:${dir}`;
  if (expandedBackups.has(key)) {
    expandedBackups.delete(key);
  } else {
    expandedBackups.add(key);
  }
  // Re-render just the history for this game
  loadGameHistory(gameId);
}

async function loadBackupFiles(gameId, dir) {
  const key = `${gameId}:${dir}`;
  const elId = `files-${key.replace(/[^a-z0-9-]/gi, '_')}`;
  const el = document.getElementById(elId);
  if (!el) return;

  try {
    const files = await api.getBackupFiles(gameId, dir);
    if (!files || files.length === 0) {
      el.innerHTML = '<span class="save-files-empty">No files</span>';
      return;
    }
    el.innerHTML = files.map(f =>
      `<div class="save-file-row">
        <span class="save-file-name">${esc(f.path)}</span>
        <span class="save-file-size">${fmt(f.size)}</span>
      </div>`
    ).join('');
  } catch {
    el.innerHTML = '<span class="save-files-empty">Could not list files</span>';
  }
}

// ---- Game Settings Popup ----

let currentGameId = null;

async function openGameSettings(gameId) {
  currentGameId = gameId;
  const game = gameLibrary.find(g => g.id === gameId);
  if (!game) return;

  const popup = document.getElementById('game-settings-popup');
  document.getElementById('game-popup-name').textContent = game.displayName || game.name;
  document.getElementById('game-rename-input').value = game.displayName || game.name;

  // Load excludes
  const excludes = await api.getGameExcludes(gameId);
  renderGameExcludes(excludes);

  popup.classList.remove('hidden');
}

function renderGameExcludes(excludes) {
  const list = document.getElementById('game-excludes-list');
  list.innerHTML = excludes.length > 0
    ? excludes.map((e, i) => `<div class="exclude-item">
        <span class="exclude-pattern">${esc(e)}</span>
        <button class="btn sm red" onclick="removeGameExclude(${i})">&#10005;</button>
      </div>`).join('')
    : '<div class="empty">No excludes. All save files will be backed up.</div>';
}

function closeGamePopup() {
  document.getElementById('game-settings-popup').classList.add('hidden');
  currentGameId = null;
}

async function addGameExclude() {
  const input = document.getElementById('game-new-exclude');
  const pattern = input.value.trim();
  if (!pattern || !currentGameId) return;

  const excludes = await api.getGameExcludes(currentGameId);
  excludes.push(pattern);
  await api.setGameExcludes(currentGameId, excludes);
  input.value = '';
  renderGameExcludes(excludes);
  refreshGames();
  toast(`Exclude added: ${pattern}`, 'success');
}

async function removeGameExclude(index) {
  if (!currentGameId) return;
  const excludes = await api.getGameExcludes(currentGameId);
  excludes.splice(index, 1);
  await api.setGameExcludes(currentGameId, excludes);
  renderGameExcludes(excludes);
  refreshGames();
}

async function renameCurrentGame() {
  if (!currentGameId) return;
  const name = document.getElementById('game-rename-input').value.trim();
  if (!name) return;
  await api.renameGame(currentGameId, name);
  document.getElementById('game-popup-name').textContent = name;
  toast(`Renamed to: ${name}`, 'success');
  refreshGames();
}

async function removeCurrentGame() {
  if (!currentGameId) return;
  if (!confirm('Remove this game? Backups can optionally be deleted.')) return;
  const deleteBackups = confirm('Also delete all backups for this game?');
  await api.removeGame(currentGameId, deleteBackups);
  closeGamePopup();
  toast('Game removed', 'info');
  refreshGames();
}

// ---- Add Custom Game Popup ----

function openAddGamePopup() {
  document.getElementById('add-game-popup').classList.remove('hidden');
  document.getElementById('custom-game-name').value = '';
  document.getElementById('custom-game-path').value = '';
}

function closeAddGamePopup() {
  document.getElementById('add-game-popup').classList.add('hidden');
}

async function pickGameFolder() {
  const p = await api.pickGameFolder();
  if (p) document.getElementById('custom-game-path').value = p;
}

async function confirmAddGame() {
  const name = document.getElementById('custom-game-name').value.trim();
  const savePath = document.getElementById('custom-game-path').value.trim();
  if (!name) { toast('Enter a game name', 'error'); return; }
  if (!savePath) { toast('Select a save folder', 'error'); return; }

  try {
    await api.addCustomGame({ name, savePath });
    toast(`Added: ${name}`, 'success');
    closeAddGamePopup();
    refreshGames();
  } catch (err) {
    toast(`Failed: ${err.message}`, 'error');
  }
}

function setupGames() {
  // Search
  document.getElementById('game-search').addEventListener('input', (e) => {
    gameSearchQuery = e.target.value;
    renderGames();
  });

  // Rescan (quick — checks known game DB paths)
  document.getElementById('btn-scan-games').addEventListener('click', async () => {
    const btn = document.getElementById('btn-scan-games');
    btn.disabled = true; btn.textContent = 'Scanning...';
    try {
      const result = await api.scanGames();
      toast(`Found ${result.found} game(s), ${result.new} new`, 'success');
      refreshGames();
    } catch (err) { toast(`Scan failed: ${err.message}`, 'error'); }
    btn.disabled = false; btn.textContent = 'Rescan';
  });

  // Mass Lookup (deep — walks all watched directories for anything game-like)
  document.getElementById('btn-mass-lookup').addEventListener('click', async () => {
    const btn = document.getElementById('btn-mass-lookup');
    btn.disabled = true; btn.textContent = 'Scanning...';
    try {
      const result = await api.massLookup();
      toast(`Deep scan: ${result.found} game(s) found, ${result.new} new`, 'success');
      refreshGames();
    } catch (err) { toast(`Mass lookup failed: ${err.message}`, 'error'); }
    btn.disabled = false; btn.textContent = 'Mass Lookup';
  });

  // Backup All
  document.getElementById('btn-backup-all').addEventListener('click', async () => {
    const btn = document.getElementById('btn-backup-all');
    btn.disabled = true; btn.textContent = 'Backing up...';
    try {
      const result = await api.backupAll();
      toast(`Backed up ${result.success} game(s)${result.skipped ? `, ${result.skipped} skipped` : ''}`, 'success');
      refreshGames();
    } catch (err) { toast(`Backup failed: ${err.message}`, 'error'); }
    btn.disabled = false; btn.textContent = 'Backup All';
  });

  document.getElementById('btn-add-game').addEventListener('click', openAddGamePopup);

  // Game settings popup
  document.getElementById('btn-close-game-popup').addEventListener('click', closeGamePopup);
  document.getElementById('game-settings-popup').addEventListener('click', (e) => {
    if (e.target === e.currentTarget) closeGamePopup();
  });
  document.getElementById('btn-game-rename').addEventListener('click', renameCurrentGame);
  document.getElementById('game-rename-input').addEventListener('keydown', (e) => {
    if (e.key === 'Enter') renameCurrentGame();
  });
  document.getElementById('btn-game-add-exclude').addEventListener('click', addGameExclude);
  document.getElementById('game-new-exclude').addEventListener('keydown', (e) => {
    if (e.key === 'Enter') addGameExclude();
  });
  document.getElementById('btn-game-remove').addEventListener('click', removeCurrentGame);

  // Add game popup
  document.getElementById('btn-close-add-game').addEventListener('click', closeAddGamePopup);
  document.getElementById('add-game-popup').addEventListener('click', (e) => {
    if (e.target === e.currentTarget) closeAddGamePopup();
  });
  document.getElementById('btn-pick-game-folder').addEventListener('click', pickGameFolder);
  document.getElementById('btn-confirm-add-game').addEventListener('click', confirmAddGame);

  // Game save settings in Settings tab
  const saveSettingsBtn = document.getElementById('btn-save-gamesave-settings');
  if (saveSettingsBtn) {
    saveSettingsBtn.addEventListener('click', async () => {
      await api.updateSettings({
        gameSaveEnabled: document.getElementById('set-gamesave-enabled').checked,
        gameSaveMaxVersions: parseInt(document.getElementById('set-gamesave-versions').value) || 10,
        gameSaveAutoRestore: document.getElementById('set-gamesave-autorestore').checked,
      });
      toast('Game save settings saved', 'success');
      renderScanStatus();
    });
  }

  // Load game save settings
  api.getConfig().then(cfg => {
    const s = cfg.settings || {};
    const el1 = document.getElementById('set-gamesave-enabled');
    const el2 = document.getElementById('set-gamesave-versions');
    const el3 = document.getElementById('set-gamesave-autorestore');
    if (el1) el1.checked = s.gameSaveEnabled !== false;
    if (el2) el2.value = s.gameSaveMaxVersions || 10;
    if (el3) el3.checked = !!s.gameSaveAutoRestore;
  }).catch(() => {});

  // Initial load
  refreshGames();
}

function fmtTimeAgo(isoString) {
  if (!isoString) return 'never';
  const diff = Date.now() - new Date(isoString).getTime();
  if (diff < 0) return 'just now';
  const mins = Math.floor(diff / 60000);
  if (mins < 1) return 'just now';
  if (mins < 60) return `${mins}m ago`;
  const hours = Math.floor(mins / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}

// ---- Live Events ----
function setupLiveEvents() {
  api.onStatusUpdate(refresh);
  api.onProgress((p) => {
    const el = document.getElementById('scan-progress');
    if (p.phase === 'scanning' && p.total > 0) {
      const pct = Math.round((p.current / p.total) * 100);
      el.textContent = `Scanning: ${p.current}/${p.total} (${pct}%) — ${p.file || ''}`;
      // Clear when done
      if (p.current >= p.total) {
        setTimeout(() => { el.textContent = ''; }, 2000);
      }
    } else if (p.phase === 'hashing') {
      el.textContent = `Hashing: ${p.current}/${p.total} — ${p.file || ''}`;
    } else {
      el.textContent = '';
    }
  });
  api.onActivity((a) => {
    if (a.type === 'client-connected') { addActivity('connect', `${a.deviceName || 'Client'} connected`); toast(`${a.deviceName} connected`, 'success'); }
    else if (a.type === 'client-disconnected') addActivity('disconnect', `${a.deviceName || 'Client'} disconnected`);
    else if (a.type === 'file-changes') addActivity('change', `${a.count} file(s) changed in ${a.folder}`);
    else if (a.type === 'game-backup') addActivity('change', a.message);
    else if (a.type === 'game-restore') { addActivity('change', a.message); refreshGames(); }
    const badge = document.getElementById('status-badge');
    if (a.type === 'client-disconnected') {
      badge.className = 'badge yellow';
      setTimeout(() => { badge.className = 'badge green'; }, 5000);
    } else {
      badge.className = 'badge blue';
      setTimeout(() => { badge.className = 'badge green'; }, 2000);
    }
  });
}

// ---- Helpers ----
function esc(s) { const d = document.createElement('div'); d.textContent = s; return d.innerHTML; }
function escA(s) { return String(s).replace(/'/g, "\\'").replace(/\\/g, '\\\\'); }
function fmt(b) {
  if (!b) return '0 B';
  const k = 1024; const s = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(b) / Math.log(k));
  return (b / Math.pow(k, i)).toFixed(1) + ' ' + s[i];
}
function toast(msg, type = 'info') {
  const el = document.createElement('div'); el.className = `toast ${type}`; el.textContent = msg;
  document.getElementById('toast-container').appendChild(el);
  setTimeout(() => el.remove(), 3500);
}
function copyText(id) {
  navigator.clipboard.writeText(document.getElementById(id).textContent).then(() => toast('Copied!', 'success')).catch(() => {});
}

document.addEventListener('DOMContentLoaded', init);
