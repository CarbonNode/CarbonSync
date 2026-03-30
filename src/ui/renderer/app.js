const api = window.carbonsync;
const activityLog = [];
const MAX_ACTIVITY = 100;
let currentStatus = {};

async function init() {
  setupTitlebar();
  setupTabs();
  setupFolderActions();
  setupDragDrop();
  setupDevices();
  setupSettings();
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
function renderFolders(folders) {
  const el = document.getElementById('folders-list');
  if (folders.length === 0) {
    el.innerHTML = '<div class="empty">No folders synced yet. Add a folder or drag one in.</div>';
    return;
  }

  el.innerHTML = folders.map(f => {
    const sizeClass = f.totalSize > 10e9 ? 'huge' : f.totalSize > 1e9 ? 'large' : f.totalSize > 100e6 ? 'medium' : 'small';
    const maxSize = Math.max(...folders.map(x => x.totalSize || 0), 1);
    const pct = Math.max(2, Math.round((f.totalSize / maxSize) * 100));
    const excludeCount = (f.excludes || []).length;

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
          <button class="btn sm ghost" onclick="openFolderSettings('${escA(f.name)}')">⚙</button>
          <button class="btn sm ghost" onclick="rescanFolder('${escA(f.name)}')">Rescan</button>
          <button class="btn sm red" onclick="removeFolder('${escA(f.path)}')">Remove</button>
        </div>
      </div>
      <div class="size-bar"><div class="size-fill ${sizeClass}" style="width:${pct}%"></div></div>
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
          <button class="btn sm ghost" onclick="renamePeer('${escA(d.hostname)}', '${escA(d.friendlyName || d.hostname)}')">✏</button>
        </div>
        <div class="device-detail">${d.ip}:${d.port}</div>
      </div>
      <span class="device-role ${d.role}">${d.role}</span>
    </div>
  `).join('');
}

async function renamePeer(hostname, currentName) {
  const newName = prompt('Rename device:', currentName);
  if (newName && newName !== currentName) {
    await api.renamePeer(hostname, newName);
    toast(`Renamed to: ${newName}`, 'success');
    refresh();
  }
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
