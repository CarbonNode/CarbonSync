/**
 * mDNS/Bonjour discovery for CarbonSync.
 * Server advertises _carbonsync._tcp, clients find it automatically.
 */

const { Bonjour } = require('bonjour-service');
const os = require('os');
const { SERVICE_TYPE, DEFAULT_PORT } = require('../shared/protocol');

class Discovery {
  constructor(opts = {}) {
    this.bonjour = new Bonjour();
    this.port = opts.port || DEFAULT_PORT;
    this.role = opts.role || 'server';
    this.apiKey = opts.apiKey || '';
    this.deviceId = opts.deviceId || '';
    this.hostname = opts.hostname || os.hostname();
    this.service = null;
    this.browser = null;
    this.onFound = opts.onFound || (() => {});
    this.onLost = opts.onLost || (() => {});
    this.services = new Map();
  }

  /**
   * Advertise this device on the network.
   */
  publish() {
    this.service = this.bonjour.publish({
      name: `CarbonSync-${this.hostname}`,
      type: SERVICE_TYPE,
      port: this.port,
      txt: {
        role: this.role,
        apiKey: this.apiKey,
        deviceId: this.deviceId,
        hostname: this.hostname,
        version: '1',
      },
    });
    console.log(`Discovery: advertising as ${this.role} on port ${this.port}`);
  }

  /**
   * Browse for CarbonSync services on the network.
   */
  browse() {
    this.browser = this.bonjour.find({ type: SERVICE_TYPE });

    this.browser.on('up', (service) => {
      const info = {
        role: service.txt?.role || 'unknown',
        ip: this._getIPv4(service),
        port: service.port,
        apiKey: service.txt?.apiKey || '',
        deviceId: service.txt?.deviceId || '',
        hostname: service.txt?.hostname || service.name,
        name: service.name,
      };

      if (!info.ip) return;

      console.log(`Discovery: found ${info.role} at ${info.ip}:${info.port} (${info.hostname})`);
      this.services.set(info.hostname, info);
      this.onFound(info);
    });

    this.browser.on('down', (service) => {
      const hostname = service.txt?.hostname || service.name;
      console.log(`Discovery: lost ${hostname}`);
      this.services.delete(hostname);
      this.onLost({ hostname });
    });

    console.log('Discovery: browsing for CarbonSync services...');
  }

  getServices() {
    return [...this.services.values()];
  }

  getByRole(role) {
    return this.getServices().filter(s => s.role === role);
  }

  stop() {
    if (this.service) { this.service.stop(); this.service = null; }
    if (this.browser) { this.browser.stop(); this.browser = null; }
    this.bonjour.destroy();
  }

  _getIPv4(service) {
    if (!service.addresses) return service.referer?.address || null;

    const ipv4s = service.addresses.filter(a => /^\d+\.\d+\.\d+\.\d+$/.test(a));
    if (ipv4s.length === 0) return service.addresses[0] || null;

    // Prefer real LAN IPs over virtual adapters
    const lan = ipv4s.find(a => a.startsWith('192.168.') || a.startsWith('10.'));
    if (lan) return lan;

    // Avoid Hyper-V/WSL range (172.16-31.x.x)
    const nonVirtual = ipv4s.find(a => {
      const parts = a.split('.');
      const second = parseInt(parts[1], 10);
      return !(parts[0] === '172' && second >= 16 && second <= 31);
    });
    return (nonVirtual || ipv4s[0]);
  }
}

module.exports = { Discovery };
