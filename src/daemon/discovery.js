/**
 * CarbonSync Device Discovery
 *
 * Uses UDP broadcast (reliable on Windows) as primary discovery.
 * Falls back to mDNS/Bonjour as secondary.
 *
 * Every instance broadcasts a beacon every 10s on UDP port (tcpPort + 1).
 * Other instances on the same subnet hear it and add the device.
 * Simple, works through firewalls that block mDNS multicast.
 */

const dgram = require('dgram');
const os = require('os');
const { Bonjour } = require('bonjour-service');
const { SERVICE_TYPE, DEFAULT_PORT } = require('../shared/protocol');

const BEACON_INTERVAL = 10000;  // Broadcast every 10 seconds
const BEACON_MAGIC = 'CARBONSYNC';
const DEVICE_TTL = 35000;       // Remove device if no beacon for 35s

class Discovery {
  constructor(opts = {}) {
    this.port = opts.port || DEFAULT_PORT;
    this.udpPort = this.port + 1;
    this.role = opts.role || 'device';
    this.deviceId = opts.deviceId || '';
    this.hostname = opts.hostname || os.hostname();
    this.onFound = opts.onFound || (() => {});
    this.onLost = opts.onLost || (() => {});
    this.services = new Map();  // hostname -> device info
    this._lastSeen = new Map(); // hostname -> timestamp
    this._beaconTimer = null;
    this._cleanupTimer = null;
    this._udpSocket = null;
    this._bonjour = null;
    this._mdnsService = null;
    this._mdnsBrowser = null;
  }

  /**
   * Start broadcasting presence and listening for other devices.
   */
  publish() {
    this._startUdpBeacon();
    this._startMdns();
  }

  /**
   * Start listening for other devices.
   */
  browse() {
    // UDP listener is already started by publish()
    // mDNS browse is started by _startMdns()
  }

  // ---- UDP Broadcast (primary, reliable) ----

  _startUdpBeacon() {
    try {
      this._udpSocket = dgram.createSocket({ type: 'udp4', reuseAddr: true });

      this._udpSocket.on('message', (msg, rinfo) => {
        try {
          const str = msg.toString('utf-8');
          if (!str.startsWith(BEACON_MAGIC + '|')) return;

          const parts = str.split('|');
          if (parts.length < 5) return;

          const info = {
            hostname: parts[1],
            deviceId: parts[2],
            port: parseInt(parts[3]) || this.port,
            role: parts[4] || 'device',
            ip: rinfo.address,
          };

          // Skip self
          if (info.deviceId === this.deviceId) return;

          const isNew = !this._lastSeen.has(info.hostname);
          this._lastSeen.set(info.hostname, Date.now());

          if (isNew || !this.services.has(info.hostname)) {
            console.log(`Discovery: found ${info.hostname} at ${info.ip}:${info.port} (UDP broadcast)`);
            this.services.set(info.hostname, info);
            this.onFound(info);
          } else {
            // Update IP in case it changed
            const existing = this.services.get(info.hostname);
            if (existing) existing.ip = info.ip;
          }
        } catch {}
      });

      this._udpSocket.on('error', (err) => {
        console.warn(`Discovery UDP error: ${err.message}`);
      });

      this._udpSocket.bind(this.udpPort, () => {
        this._udpSocket.setBroadcast(true);
        console.log(`Discovery: listening on UDP ${this.udpPort}`);
      });

      // Send beacon immediately, then every 10s
      this._sendBeacon();
      this._beaconTimer = setInterval(() => this._sendBeacon(), BEACON_INTERVAL);

      // Cleanup stale devices every 15s
      this._cleanupTimer = setInterval(() => this._cleanupStale(), 15000);

    } catch (err) {
      console.warn(`Discovery: UDP beacon failed: ${err.message}`);
    }
  }

  _sendBeacon() {
    if (!this._udpSocket) return;
    const msg = `${BEACON_MAGIC}|${this.hostname}|${this.deviceId}|${this.port}|${this.role}`;
    const buf = Buffer.from(msg, 'utf-8');

    // Broadcast to all local network interfaces
    const interfaces = os.networkInterfaces();
    for (const ifaces of Object.values(interfaces)) {
      for (const iface of ifaces) {
        if (iface.family !== 'IPv4' || iface.internal) continue;
        // Skip virtual adapters (Hyper-V, WSL, Docker)
        const parts = iface.address.split('.');
        if (parts[0] === '172' && parseInt(parts[1]) >= 16 && parseInt(parts[1]) <= 31) continue;

        // Calculate broadcast address from IP + netmask
        const ip = iface.address.split('.').map(Number);
        const mask = iface.netmask.split('.').map(Number);
        const broadcast = ip.map((octet, i) => (octet | (~mask[i] & 255))).join('.');

        try {
          this._udpSocket.send(buf, 0, buf.length, this.udpPort, broadcast);
        } catch {}
      }
    }
  }

  _cleanupStale() {
    const now = Date.now();
    for (const [hostname, lastSeen] of this._lastSeen) {
      if (now - lastSeen > DEVICE_TTL) {
        console.log(`Discovery: lost ${hostname} (no beacon for ${Math.round((now - lastSeen) / 1000)}s)`);
        this._lastSeen.delete(hostname);
        this.services.delete(hostname);
        this.onLost({ hostname });
      }
    }
  }

  // ---- mDNS/Bonjour (secondary, fallback) ----

  _startMdns() {
    try {
      this._bonjour = new Bonjour();

      // Publish
      const suffix = this.deviceId ? this.deviceId.slice(0, 8) : Date.now().toString(36);
      try {
        this._mdnsService = this._bonjour.publish({
          name: `CarbonSync-${this.hostname}-${suffix}`,
          type: SERVICE_TYPE,
          port: this.port,
          txt: { role: this.role, deviceId: this.deviceId, hostname: this.hostname },
        });
        this._mdnsService.on('error', () => {});
      } catch {}

      // Browse
      this._mdnsBrowser = this._bonjour.find({ type: SERVICE_TYPE });
      this._mdnsBrowser.on('up', (service) => {
        const info = {
          role: service.txt?.role || 'unknown',
          ip: this._getIPv4(service),
          port: service.port,
          deviceId: service.txt?.deviceId || '',
          hostname: service.txt?.hostname || service.name,
        };
        if (!info.ip) return;
        if (info.deviceId && info.deviceId === this.deviceId) return;
        // Only add if not already found via UDP
        if (!this.services.has(info.hostname)) {
          console.log(`Discovery: found ${info.hostname} at ${info.ip}:${info.port} (mDNS)`);
          this.services.set(info.hostname, info);
          this._lastSeen.set(info.hostname, Date.now());
          this.onFound(info);
        }
      });
    } catch (err) {
      console.warn(`Discovery: mDNS failed: ${err.message}`);
    }
  }

  // ---- Public API ----

  getServices() {
    return [...this.services.values()];
  }

  getByRole(role) {
    return this.getServices().filter(s => s.role === role);
  }

  stop() {
    if (this._beaconTimer) { clearInterval(this._beaconTimer); this._beaconTimer = null; }
    if (this._cleanupTimer) { clearInterval(this._cleanupTimer); this._cleanupTimer = null; }
    if (this._udpSocket) { try { this._udpSocket.close(); } catch {} this._udpSocket = null; }
    if (this._mdnsService) { try { this._mdnsService.stop(); } catch {} }
    if (this._mdnsBrowser) { try { this._mdnsBrowser.stop(); } catch {} }
    if (this._bonjour) { try { this._bonjour.destroy(); } catch {} }
  }

  _getIPv4(service) {
    if (!service.addresses) return service.referer?.address || null;
    const ipv4s = service.addresses.filter(a => /^\d+\.\d+\.\d+\.\d+$/.test(a));
    if (ipv4s.length === 0) return service.addresses[0] || null;
    const lan = ipv4s.find(a => a.startsWith('192.168.') || a.startsWith('10.'));
    if (lan) return lan;
    const nonVirtual = ipv4s.find(a => {
      const parts = a.split('.');
      const second = parseInt(parts[1], 10);
      return !(parts[0] === '172' && second >= 16 && second <= 31);
    });
    return (nonVirtual || ipv4s[0]);
  }
}

module.exports = { Discovery };
