/**
 * TLS Certificate Management
 *
 * Generates self-signed certs on first run, stores in config dir.
 * Uses them for all sync connections. Fingerprint shown in UI for verification.
 */

const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const { execFileSync } = require('child_process');

/**
 * Ensure TLS certificates exist. Generate if missing.
 * Returns { key, cert, fingerprint }
 */
function ensureCerts(configDir) {
  const keyPath = path.join(configDir, 'tls.key');
  const certPath = path.join(configDir, 'tls.cert');
  const fpPath = path.join(configDir, 'tls.fingerprint');

  fs.mkdirSync(configDir, { recursive: true });

  if (fs.existsSync(keyPath) && fs.existsSync(certPath)) {
    const key = fs.readFileSync(keyPath);
    const cert = fs.readFileSync(certPath);
    const fingerprint = fs.existsSync(fpPath) ? fs.readFileSync(fpPath, 'utf-8').trim() : computeFingerprint(cert);
    return { key, cert, fingerprint };
  }

  console.log('Generating TLS certificate...');

  // Generate using Node.js crypto (self-signed)
  const { privateKey, publicKey } = crypto.generateKeyPairSync('rsa', {
    modulusLength: 2048,
    publicKeyEncoding: { type: 'spki', format: 'pem' },
    privateKeyEncoding: { type: 'pkcs8', format: 'pem' },
  });

  // Create self-signed X.509 certificate using Node's built-in crypto
  // Since Node doesn't have a built-in cert builder, we'll create a minimal one
  // For production, use openssl. For our use case, we use the key pair directly
  // with TLS's rejectUnauthorized: false + API key auth for actual security.
  //
  // Try openssl first (usually available on Windows via Git)
  let cert;
  try {
    // Find openssl (bundled with Git for Windows)
    const opensslPaths = [
      'C:\\Program Files\\Git\\usr\\bin\\openssl.exe',
      'C:\\Program Files (x86)\\Git\\usr\\bin\\openssl.exe',
      'openssl',
    ];

    let opensslBin = null;
    for (const p of opensslPaths) {
      try {
        execFileSync(p, ['version'], { timeout: 5000, stdio: 'pipe', windowsHide: true });
        opensslBin = p;
        break;
      } catch {}
    }

    if (opensslBin) {
      fs.writeFileSync(keyPath, privateKey);
      execFileSync(opensslBin, [
        'req', '-new', '-x509', '-key', keyPath,
        '-out', certPath, '-days', '3650',
        '-subj', '/CN=CarbonSync/O=CarbonSync',
      ], { timeout: 10000, stdio: 'pipe', windowsHide: true });

      cert = fs.readFileSync(certPath);
    }
  } catch {}

  if (!cert) {
    // Fallback: just save the key, use plain TCP with API key auth
    // (TLS without a proper cert is no better than API key over TCP)
    console.warn('OpenSSL not found — using TCP with API key auth (no TLS)');
    fs.writeFileSync(keyPath, privateKey);
    fs.writeFileSync(certPath, publicKey); // Not a real cert, but marks as "attempted"
    const fingerprint = computeFingerprint(Buffer.from(publicKey));
    fs.writeFileSync(fpPath, fingerprint);
    return { key: null, cert: null, fingerprint };
  }

  const fingerprint = computeFingerprint(cert);
  fs.writeFileSync(fpPath, fingerprint);

  console.log(`TLS certificate generated. Fingerprint: ${fingerprint}`);
  return { key: privateKey, cert, fingerprint };
}

function computeFingerprint(certData) {
  const hash = crypto.createHash('sha256').update(certData).digest('hex');
  // Format as XXXX-XXXX-XXXX for easy visual comparison
  return hash.substring(0, 4).toUpperCase() + '-' +
         hash.substring(4, 8).toUpperCase() + '-' +
         hash.substring(8, 12).toUpperCase();
}

module.exports = { ensureCerts, computeFingerprint };
