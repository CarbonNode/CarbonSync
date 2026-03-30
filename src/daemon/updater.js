/**
 * GitHub Release Auto-Updater
 *
 * Downloads NSIS installer from latest GitHub release,
 * runs it silently, and quits current app.
 * The installer overwrites the old version automatically.
 */

const https = require('https');
const fs = require('fs');
const path = require('path');
const { execFile } = require('child_process');
const os = require('os');

const REPO = 'CarbonNode/CarbonSync';
const CURRENT_VERSION = require('../../package.json').version;

function getLatestRelease() {
  return new Promise((resolve, reject) => {
    const opts = {
      hostname: 'api.github.com',
      path: `/repos/${REPO}/releases/latest`,
      headers: { 'User-Agent': 'CarbonSync' },
    };
    https.get(opts, (res) => {
      let data = '';
      res.on('data', (c) => { data += c; });
      res.on('end', () => {
        try {
          const release = JSON.parse(data);
          const asset = release.assets?.find(a => a.name.endsWith('.exe'));
          const version = release.tag_name?.replace(/^v/, '') || '';
          resolve({
            version,
            name: release.name,
            url: asset?.browser_download_url || '',
            size: asset?.size || 0,
            notes: release.body || '',
            current: CURRENT_VERSION,
            hasUpdate: version && version !== CURRENT_VERSION,
          });
        } catch (err) { reject(err); }
      });
      res.on('error', reject);
    }).on('error', reject);
  });
}

/**
 * Download the installer to a temp location.
 */
function downloadInstaller(url, onProgress) {
  const destPath = path.join(os.tmpdir(), 'CarbonSync-Update.exe');

  return new Promise((resolve, reject) => {
    const doDownload = (downloadUrl) => {
      https.get(downloadUrl, { headers: { 'User-Agent': 'CarbonSync' } }, (res) => {
        if (res.statusCode === 301 || res.statusCode === 302) {
          doDownload(res.headers.location);
          return;
        }
        if (res.statusCode !== 200) {
          reject(new Error(`HTTP ${res.statusCode}`));
          return;
        }

        const totalSize = parseInt(res.headers['content-length'], 10) || 0;
        let downloaded = 0;
        const file = fs.createWriteStream(destPath);

        res.on('data', (chunk) => {
          downloaded += chunk.length;
          if (onProgress && totalSize > 0) {
            onProgress(Math.round((downloaded / totalSize) * 100));
          }
        });

        res.pipe(file);
        file.on('finish', () => file.close(() => resolve(destPath)));
        file.on('error', (err) => {
          try { fs.unlinkSync(destPath); } catch {}
          reject(err);
        });
      }).on('error', reject);
    };

    doDownload(url);
  });
}

/**
 * Run the downloaded installer silently and quit the current app.
 * NSIS /S flag = silent install, overwrites existing installation.
 */
function installAndRestart(installerPath, app) {
  // Run installer with silent flag
  execFile(installerPath, ['/S'], { detached: true, stdio: 'ignore' }).unref();

  // Quit current app so installer can overwrite files
  setTimeout(() => {
    app.isQuitting = true;
    app.quit();
  }, 1000);
}

module.exports = { getLatestRelease, downloadInstaller, installAndRestart, CURRENT_VERSION };
