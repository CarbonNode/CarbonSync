/**
 * GitHub Release Updater
 * Checks for latest release, downloads portable exe, replaces current.
 */

const https = require('https');
const fs = require('fs');
const path = require('path');
const { execFile } = require('child_process');

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
          resolve({
            version: release.tag_name?.replace(/^v/, '') || '',
            name: release.name,
            url: asset?.browser_download_url || '',
            size: asset?.size || 0,
            notes: release.body || '',
            current: CURRENT_VERSION,
            hasUpdate: release.tag_name?.replace(/^v/, '') !== CURRENT_VERSION,
          });
        } catch (err) { reject(err); }
      });
      res.on('error', reject);
    }).on('error', reject);
  });
}

function downloadUpdate(url, destPath, onProgress) {
  return new Promise((resolve, reject) => {
    const tmpPath = destPath + '.download';

    const doDownload = (downloadUrl) => {
      https.get(downloadUrl, { headers: { 'User-Agent': 'CarbonSync' } }, (res) => {
        // Follow redirects
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
        const file = fs.createWriteStream(tmpPath);

        res.on('data', (chunk) => {
          downloaded += chunk.length;
          if (onProgress && totalSize > 0) {
            onProgress(Math.round((downloaded / totalSize) * 100));
          }
        });

        res.pipe(file);
        file.on('finish', () => {
          file.close(() => {
            fs.renameSync(tmpPath, destPath);
            resolve(destPath);
          });
        });
        file.on('error', (err) => {
          try { fs.unlinkSync(tmpPath); } catch {}
          reject(err);
        });
      }).on('error', reject);
    };

    doDownload(url);
  });
}

module.exports = { getLatestRelease, downloadUpdate, CURRENT_VERSION };
