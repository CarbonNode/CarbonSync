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
async function ensureFirewallRule(port) {
  const exists = await hasFirewallRule();
  if (!exists) {
    console.log('Firewall rule not found, adding...');
    try {
      await addFirewallRule(port);
      return true;
    } catch (err) {
      console.warn('Could not add firewall rule automatically. You may need to allow CarbonSync through Windows Firewall manually.');
      return false;
    }
  }
  console.log('Firewall rule already exists');
  return true;
}

module.exports = { ensureFirewallRule, addFirewallRule, removeFirewallRule, hasFirewallRule };
