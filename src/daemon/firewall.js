/**
 * Windows Firewall Rule Manager
 * All commands use windowsHide: true to prevent cmd window flash.
 */

const { execFile } = require('child_process');
const { DEFAULT_PORT } = require('../shared/protocol');

const RULE_NAME = 'CarbonSync';
const HIDDEN = { windowsHide: true };

function hasFirewallRule() {
  return new Promise((resolve) => {
    execFile('netsh', [
      'advfirewall', 'firewall', 'show', 'rule', `name=${RULE_NAME}`,
    ], HIDDEN, (err, stdout) => {
      resolve(!err && stdout.includes(RULE_NAME));
    });
  });
}

async function ensureFirewallRule(port) {
  const exists = await hasFirewallRule();
  if (exists) {
    console.log('Firewall rule already exists');
    // Check mDNS
    const hasMdns = await new Promise((resolve) => {
      execFile('netsh', ['advfirewall', 'firewall', 'show', 'rule', `name=${RULE_NAME} mDNS`], HIDDEN, (err, stdout) => {
        resolve(!err && stdout.includes(RULE_NAME));
      });
    });
    if (!hasMdns) {
      try {
        await runElevated(`netsh advfirewall firewall add rule name="${RULE_NAME} mDNS" dir=in action=allow protocol=UDP localport=5353 profile=private,domain`);
      } catch { console.warn('Could not add mDNS firewall rule'); }
    }
    return true;
  }

  console.log('Adding firewall rules...');
  try {
    const cmds = [
      `netsh advfirewall firewall add rule name=${RULE_NAME} dir=in action=allow protocol=TCP localport=${port || DEFAULT_PORT} profile=private,domain`,
      `netsh advfirewall firewall add rule name="${RULE_NAME} mDNS" dir=in action=allow protocol=UDP localport=5353 profile=private,domain`,
    ].join(' & ');
    await runElevated(cmds);
    console.log('Firewall rules added');
    return true;
  } catch (err) {
    console.warn('Could not add firewall rules automatically:', err.message);
    return false;
  }
}

function runElevated(cmd) {
  return new Promise((resolve, reject) => {
    execFile('powershell', ['-Command',
      `Start-Process cmd -ArgumentList '/c ${cmd}' -Verb RunAs -Wait -WindowStyle Hidden`
    ], HIDDEN, (err) => err ? reject(err) : resolve());
  });
}

function removeFirewallRule() {
  return new Promise((resolve) => {
    execFile('powershell', ['-Command',
      `Start-Process netsh -ArgumentList 'advfirewall firewall delete rule name=${RULE_NAME}' -Verb RunAs -Wait -WindowStyle Hidden`
    ], HIDDEN, () => resolve());
  });
}

module.exports = { ensureFirewallRule, removeFirewallRule, hasFirewallRule };
