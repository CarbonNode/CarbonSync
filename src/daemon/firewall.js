/**
 * Windows Firewall Rule Manager
 * Auto-adds inbound TCP rule for CarbonSync on install/first run.
 */

const { execFile } = require('child_process');
const { DEFAULT_PORT } = require('../shared/protocol');

const RULE_NAME = 'CarbonSync';

/**
 * Check if CarbonSync firewall rule exists.
 */
function hasFirewallRule() {
  return new Promise((resolve) => {
    execFile('netsh', [
      'advfirewall', 'firewall', 'show', 'rule', `name=${RULE_NAME}`,
    ], (err, stdout) => {
      resolve(!err && stdout.includes(RULE_NAME));
    });
  });
}

/**
 * Add CarbonSync firewall rule (requires admin, but will prompt UAC).
 */
function addFirewallRule(port) {
  port = port || DEFAULT_PORT;
  return new Promise((resolve, reject) => {
    // Use powershell Start-Process to trigger UAC elevation
    const psCmd = `Start-Process netsh -ArgumentList 'advfirewall firewall add rule name=${RULE_NAME} dir=in action=allow protocol=TCP localport=${port} profile=private,domain' -Verb RunAs -Wait`;

    execFile('powershell', ['-Command', psCmd], (err, stdout, stderr) => {
      if (err) {
        console.error(`Firewall rule failed: ${err.message}`);
        reject(err);
      } else {
        console.log(`Firewall rule added: ${RULE_NAME} on port ${port}`);
        resolve(true);
      }
    });
  });
}

/**
 * Remove CarbonSync firewall rule.
 */
function removeFirewallRule() {
  return new Promise((resolve) => {
    const psCmd = `Start-Process netsh -ArgumentList 'advfirewall firewall delete rule name=${RULE_NAME}' -Verb RunAs -Wait`;
    execFile('powershell', ['-Command', psCmd], () => resolve());
  });
}

/**
 * Ensure firewall rule exists, add if missing.
 */
/**
 * Ensure both CarbonSync and mDNS firewall rules exist.
 */
async function ensureFirewallRule(port) {
  const exists = await hasFirewallRule();
  if (!exists) {
    console.log('Firewall rules not found, adding...');
    try {
      // Add both CarbonSync TCP and mDNS UDP rules in one UAC prompt
      const psCmd = [
        `netsh advfirewall firewall add rule name=${RULE_NAME} dir=in action=allow protocol=TCP localport=${port} profile=private,domain`,
        `netsh advfirewall firewall add rule name="${RULE_NAME} mDNS" dir=in action=allow protocol=UDP localport=5353 profile=private,domain`,
      ].join(' & ');

      await new Promise((resolve, reject) => {
        execFile('powershell', ['-Command', `Start-Process cmd -ArgumentList '/c ${psCmd}' -Verb RunAs -Wait`], (err) => {
          if (err) reject(err);
          else resolve();
        });
      });

      console.log(`Firewall rules added: ${RULE_NAME} (TCP ${port}) + mDNS (UDP 5353)`);
      return true;
    } catch (err) {
      console.warn('Could not add firewall rules automatically. You may need to allow CarbonSync and mDNS (UDP 5353) through Windows Firewall manually.');
      return false;
    }
  }
  console.log('Firewall rule already exists');

  // Check mDNS rule separately
  const hasMdns = await new Promise((resolve) => {
    execFile('netsh', ['advfirewall', 'firewall', 'show', 'rule', `name=${RULE_NAME} mDNS`], (err, stdout) => {
      resolve(!err && stdout.includes(RULE_NAME));
    });
  });

  if (!hasMdns) {
    console.log('Adding mDNS firewall rule...');
    try {
      await new Promise((resolve, reject) => {
        execFile('powershell', ['-Command',
          `Start-Process netsh -ArgumentList 'advfirewall firewall add rule name="${RULE_NAME} mDNS" dir=in action=allow protocol=UDP localport=5353 profile=private,domain' -Verb RunAs -Wait`
        ], (err) => err ? reject(err) : resolve());
      });
      console.log('mDNS firewall rule added');
    } catch {
      console.warn('Could not add mDNS firewall rule');
    }
  }

  return true;
}

module.exports = { ensureFirewallRule, addFirewallRule, removeFirewallRule, hasFirewallRule };
