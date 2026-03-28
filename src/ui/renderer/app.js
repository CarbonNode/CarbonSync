const api = window.carbonsync;
const activityLog = [];
const MAX_ACTIVITY = 100;
let currentStatus = {};

// ---- Init ----

async function init() {
  setupTabs();
  setupFolderActions();
  setupDragDrop();
  setupSettings();
  setupLiveEvents();
  await refresh();
  setInterval(refresh, 8000);
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
  try {
    currentStatus = await api.getStatus();
  } catch { return; }
  const s = currentStatus;

  document.getElementById('subtitle').textContent = s.deviceName || 'Starting...';
  document.getElementById('port').textContent = s.port || '—';
  document.getElementById('clients').textContent = s.connectedClients || 0;

  const tlsEl = document.getElementById('tls-status');
  tlsEl.innerHTML = s.tlsEnabled
    ? '<span class="tls-badge on">TLS</span>'
    : '<span class="tls-badge off">TCP</span>';

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

  const maxSize = Math.max(...folders.map(f => f.totalSize || 0), 1);

  el.innerHTML = folders.map(f => {
    const sizeClass = f.totalSize > 10e9 ? 'huge' : f.totalSize > 1e9 ? 'large' : f.totalSize > 100e6 ? 'medium' : 'small';
    const pct = Math.max(2, Math.round((f.totalSize / maxSize) * 100));

    return `<div class="folder-card">
      <div class="folder-header">
        <div class="folder-info">
          <div class="folder-name">${esc(f.name)}</div>
          <div class="folder-path">${esc(f.path)}</div>
          <div class="folder-meta">
            <span><strong>${f.fileCount.toLocaleString()}</strong> files</span>
            <span><strong>${fmt(f.totalSize)}</strong></span>
            <span>Hash: <strong style="font-family:Consolas;font-size:10px;">${(f.rootHash || '').substring(0, 12)}...</strong></span>
          </div>
        </div>
        <div class="folder-btns">
          <button class="btn sm ghost" onclick="rescanFolder('${escA(f.name)}')">Rescan</button>
          <button class="btn sm red" onclick="removeFolder('${escA(f.path)}')">Remove</button>
        </div>
      </div>
      <div class="size-bar"><div class="size-fill ${sizeClass}" style="width:${pct}%"></div></div>
    </div>`;
  }).join('');
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
  const body = document.body;

  ['dragenter', 'dragover'].forEach(e => {
    body.addEventListener(e, (ev) => { ev.preventDefault(); zone.classList.add('over'); });
  });
  ['dragleave', 'drop'].forEach(e => {
    body.addEventListener(e, () => zone.classList.remove('over'));
  });

  body.addEventListener('drop', async (ev) => {
    ev.preventDefault();
    const files = ev.dataTransfer?.files;
    if (!files) return;
    for (const file of files) {
      if (file.path) {
        try {
          await api.addFolder(file.path);
          addActivity('scan', `Dropped folder: ${file.path}`);
          toast(`Added: ${file.name}`, 'success');
        } catch (err) {
          toast(`Failed: ${err.message}`, 'error');
        }
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
  if (activityLog.length === 0) {
    el.innerHTML = '<div class="empty">No activity yet</div>';
    return;
  }
  el.innerHTML = activityLog.map(a => {
    const iconClass = a.type === 'connect' ? 'green' :
                      a.type === 'disconnect' ? 'red' :
                      a.type === 'change' ? 'blue' : 'dim';
    const time = new Date(a.time).toLocaleTimeString();
    return `<div class="activity-item">
      <div class="activity-icon ${iconClass}"></div>
      <span class="activity-time">${time}</span>
      <span class="activity-msg">${esc(a.message)}</span>
    </div>`;
  }).join('');
}

// ---- Devices ----

function renderDevices(devices) {
  const el = document.getElementById('devices-list');
  if (devices.length === 0) {
    el.innerHTML = '<div class="empty">No other CarbonSync devices found on network</div>';
    return;
  }
  el.innerHTML = devices.map(d => `
    <div class="device-card">
      <div>
        <div class="device-name">${esc(d.hostname)}</div>
        <div class="device-detail">${d.ip}:${d.port}</div>
      </div>
      <span class="device-role ${d.role}">${d.role}</span>
    </div>
  `).join('');
}

// ---- Settings ----

function renderSettings(s) {
  const cfg = s || {};
  document.getElementById('fp-display').textContent = cfg.fingerprint || '—';
  document.getElementById('key-display').textContent = cfg.apiKey || '—';
  document.getElementById('device-id').textContent = cfg.deviceId || '—';
}

function setupSettings() {
  document.getElementById('btn-save-settings').addEventListener('click', async () => {
    const settings = {
      scanIntervalMinutes: parseInt(document.getElementById('set-scan-interval').value) || 5,
      bandwidthLimitMBps: parseInt(document.getElementById('set-bandwidth').value) || 0,
      maxConcurrentTransfers: parseInt(document.getElementById('set-concurrent').value) || 4,
    };
    const port = parseInt(document.getElementById('set-port').value);
    if (port) settings.port = port;

    await api.updateSettings(settings);
    toast('Settings saved', 'success');
  });

  // Load current settings
  api.getConfig().then(cfg => {
    const s = cfg.settings || {};
    document.getElementById('set-scan-interval').value = s.scanIntervalMinutes || 5;
    document.getElementById('set-bandwidth').value = s.bandwidthLimitMBps || 0;
    document.getElementById('set-concurrent').value = s.maxConcurrentTransfers || 4;
    document.getElementById('set-port').value = cfg.port || 21547;
  });
}

// ---- Live Events ----

function setupLiveEvents() {
  api.onStatusUpdate(refresh);

  api.onProgress((p) => {
    const el = document.getElementById('scan-progress');
    if (p.phase === 'scanning' && p.total > 0) {
      const pct = Math.round((p.current / p.total) * 100);
      el.textContent = `Scanning: ${p.current}/${p.total} (${pct}%) — ${p.file || ''}`;
    } else {
      el.textContent = '';
    }
  });

  api.onActivity((a) => {
    if (a.type === 'client-connected') {
      addActivity('connect', `${a.deviceName || 'Client'} connected`);
      toast(`${a.deviceName} connected`, 'success');
    } else if (a.type === 'client-disconnected') {
      addActivity('disconnect', `${a.deviceName || 'Client'} disconnected`);
    } else if (a.type === 'file-changes') {
      addActivity('change', `${a.count} file(s) changed in ${a.folder}`);
    }

    // Pulse badge
    const badge = document.getElementById('status-badge');
    badge.className = 'badge blue';
    setTimeout(() => { badge.className = 'badge green'; }, 2000);
  });
}

// ---- Helpers ----

function esc(s) { const d = document.createElement('div'); d.textContent = s; return d.innerHTML; }
function escA(s) { return String(s).replace(/'/g, "\\'").replace(/\\/g, '\\\\'); }

function fmt(b) {
  if (!b) return '0 B';
  const k = 1024;
  const s = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(b) / Math.log(k));
  return (b / Math.pow(k, i)).toFixed(1) + ' ' + s[i];
}

function toast(msg, type = 'info') {
  const el = document.createElement('div');
  el.className = `toast ${type}`;
  el.textContent = msg;
  document.getElementById('toast-container').appendChild(el);
  setTimeout(() => el.remove(), 3500);
}

function copyText(id) {
  const text = document.getElementById(id).textContent;
  navigator.clipboard.writeText(text).then(() => toast('Copied!', 'success')).catch(() => {});
}

document.addEventListener('DOMContentLoaded', init);
