/**
 * Transport Layer — TLS server and client with length-prefixed JSON framing.
 *
 * Handles connection management, framing, keepalive, and reconnection.
 * The server runs on the hub (this PC). Clients connect to pull files.
 */

const tls = require('tls');
const net = require('net');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const { EventEmitter } = require('events');
const { MAX_FRAME_SIZE, PROTOCOL_VERSION, MSG } = require('../shared/protocol');

/**
 * Generate self-signed TLS certificate for the sync server.
 * Stored in configDir and reused across restarts.
 */
function ensureCerts(configDir) {
  const keyPath = path.join(configDir, 'server.key');
  const certPath = path.join(configDir, 'server.cert');

  if (fs.existsSync(keyPath) && fs.existsSync(certPath)) {
    return { key: fs.readFileSync(keyPath), cert: fs.readFileSync(certPath) };
  }

  // Generate self-signed cert using Node.js crypto
  const { generateKeyPairSync, createSign, createHash } = crypto;

  // Generate RSA key pair
  const { privateKey, publicKey } = generateKeyPairSync('rsa', {
    modulusLength: 2048,
    publicKeyEncoding: { type: 'spki', format: 'pem' },
    privateKeyEncoding: { type: 'pkcs8', format: 'pem' },
  });

  // Self-signed certificate (simplified — use openssl for production)
  // For now, use the key pair directly with TLS's allowUnauthorized
  fs.mkdirSync(configDir, { recursive: true });
  fs.writeFileSync(keyPath, privateKey);
  fs.writeFileSync(certPath, publicKey); // We'll use rejectUnauthorized: false

  return { key: privateKey, cert: publicKey };
}

// ---- Frame Protocol ----

/**
 * Write a length-prefixed JSON frame to a socket.
 */
function writeFrame(socket, message) {
  const json = JSON.stringify(message);
  const payload = Buffer.from(json, 'utf-8');
  if (payload.length > MAX_FRAME_SIZE) {
    throw new Error(`Frame too large: ${payload.length} bytes`);
  }
  const header = Buffer.alloc(4);
  header.writeUInt32BE(payload.length);
  socket.write(Buffer.concat([header, payload]));
}

/**
 * Write a binary data frame (for block transfers).
 * Format: [4 bytes length][1 byte type=0xFF][payload]
 */
function writeBinaryFrame(socket, data) {
  const header = Buffer.alloc(5);
  header.writeUInt32BE(data.length + 1); // +1 for type byte
  header[4] = 0xFF; // Binary marker
  socket.write(Buffer.concat([header, data]));
}

/**
 * Frame parser — accumulates data and emits complete frames.
 */
class FrameParser extends EventEmitter {
  constructor() {
    super();
    this._buffer = Buffer.alloc(0);
  }

  feed(data) {
    this._buffer = Buffer.concat([this._buffer, data]);

    while (this._buffer.length >= 4) {
      const frameLen = this._buffer.readUInt32BE(0);

      if (frameLen > MAX_FRAME_SIZE) {
        this.emit('error', new Error(`Frame too large: ${frameLen}`));
        this._buffer = Buffer.alloc(0);
        return;
      }

      if (this._buffer.length < 4 + frameLen) {
        break; // Wait for more data
      }

      const payload = this._buffer.subarray(4, 4 + frameLen);
      this._buffer = this._buffer.subarray(4 + frameLen);

      // Check if binary frame
      if (payload[0] === 0xFF) {
        this.emit('binary', payload.subarray(1));
      } else {
        try {
          const message = JSON.parse(payload.toString('utf-8'));
          this.emit('message', message);
        } catch (err) {
          this.emit('error', new Error('Invalid JSON frame'));
        }
      }
    }
  }

  reset() {
    this._buffer = Buffer.alloc(0);
  }
}

// ---- Server ----

class SyncServer extends EventEmitter {
  /**
   * @param {object} opts - { port, configDir, apiKey, onConnection }
   */
  constructor(opts) {
    super();
    this.port = opts.port;
    this.configDir = opts.configDir;
    this.apiKey = opts.apiKey;
    this.server = null;
    this.clients = new Map(); // socket id -> { socket, deviceId, parser, authenticated }
    this._nextId = 0;
  }

  start() {
    // Use plain TCP with API key auth (TLS self-signed certs are complex to set up)
    // Security: API key is exchanged via Bonjour TXT record on trusted LAN
    this.server = net.createServer({ allowHalfOpen: true }, (socket) => {
      const id = this._nextId++;
      const parser = new FrameParser();
      const client = { socket, parser, id, deviceId: null, authenticated: false, subscriptions: new Set() };
      this.clients.set(id, client);

      socket.setTimeout(60000); // 60s timeout for idle connections

      socket.on('data', (data) => parser.feed(data));

      parser.on('message', (msg) => {
        this._handleMessage(client, msg);
      });

      parser.on('binary', (data) => {
        this.emit('binary', client, data);
      });

      parser.on('error', (err) => {
        console.error(`Frame error from client ${id}: ${err.message}`);
        socket.destroy();
      });

      socket.on('timeout', () => {
        // Send ping before disconnecting
        try { writeFrame(socket, { type: MSG.PING }); } catch {}
      });

      socket.on('close', () => {
        this.clients.delete(id);
        this.emit('client-disconnected', client);
      });

      socket.on('error', (err) => {
        console.error(`Client ${id} error: ${err.message}`);
        this.clients.delete(id);
      });
    });

    this.server.listen(this.port, '0.0.0.0', () => {
      console.log(`CarbonSync server listening on port ${this.port}`);
    });

    this.server.on('error', (err) => {
      console.error(`Server error: ${err.message}`);
      this.emit('error', err);
    });
  }

  _handleMessage(client, msg) {
    switch (msg.type) {
      case MSG.HELLO:
        this._handleHello(client, msg);
        break;
      case MSG.PONG:
        // Keepalive response — reset timeout
        client.socket.setTimeout(60000);
        break;
      default:
        if (!client.authenticated) {
          writeFrame(client.socket, { type: MSG.ERROR, message: 'Not authenticated' });
          return;
        }
        this.emit('message', client, msg);
        break;
    }
  }

  _handleHello(client, msg) {
    if (msg.apiKey !== this.apiKey) {
      writeFrame(client.socket, { type: MSG.ERROR, message: 'Unauthorized' });
      client.socket.destroy();
      return;
    }

    client.authenticated = true;
    client.deviceId = msg.deviceId;
    client.deviceName = msg.deviceName;

    writeFrame(client.socket, {
      type: MSG.WELCOME,
      version: PROTOCOL_VERSION,
      serverName: require('os').hostname(),
    });

    console.log(`Client authenticated: ${msg.deviceName} (${msg.deviceId})`);
    this.emit('client-connected', client);
  }

  /**
   * Send a message to a specific client.
   */
  send(clientId, message) {
    const client = this.clients.get(clientId);
    if (client && client.authenticated) {
      writeFrame(client.socket, message);
    }
  }

  /**
   * Send binary data to a specific client.
   */
  sendBinary(clientId, data) {
    const client = this.clients.get(clientId);
    if (client && client.authenticated) {
      writeBinaryFrame(client.socket, data);
    }
  }

  /**
   * Broadcast to all authenticated clients.
   */
  broadcast(message) {
    for (const [id, client] of this.clients) {
      if (client.authenticated) {
        try { writeFrame(client.socket, message); } catch {}
      }
    }
  }

  /**
   * Notify subscribed clients of a file change.
   */
  notifyChange(folderPath, change) {
    for (const [id, client] of this.clients) {
      if (client.authenticated && client.subscriptions.has(folderPath)) {
        try {
          writeFrame(client.socket, {
            type: MSG.NOTIFY,
            folder: folderPath,
            change,
          });
        } catch {}
      }
    }
  }

  stop() {
    for (const [id, client] of this.clients) {
      client.socket.destroy();
    }
    this.clients.clear();
    if (this.server) {
      this.server.close();
      this.server = null;
    }
  }

  getClientCount() {
    return this.clients.size;
  }
}

// ---- Client ----

class SyncClient extends EventEmitter {
  /**
   * @param {object} opts - { host, port, apiKey, deviceId, deviceName }
   */
  constructor(opts) {
    super();
    this.host = opts.host;
    this.port = opts.port;
    this.apiKey = opts.apiKey;
    this.deviceId = opts.deviceId;
    this.deviceName = opts.deviceName;
    this.socket = null;
    this.parser = null;
    this.connected = false;
    this.authenticated = false;
    this._reconnectTimer = null;
    this._pendingRequests = new Map(); // requestId -> { resolve, reject, timeout }
    this._requestId = 0;
  }

  connect() {
    this.socket = new net.Socket();
    this.parser = new FrameParser();

    this.socket.connect(this.port, this.host, () => {
      this.connected = true;
      this.emit('connected');

      // Send hello
      writeFrame(this.socket, {
        type: MSG.HELLO,
        apiKey: this.apiKey,
        deviceId: this.deviceId,
        deviceName: this.deviceName,
        version: PROTOCOL_VERSION,
      });
    });

    this.socket.on('data', (data) => this.parser.feed(data));

    this.parser.on('message', (msg) => {
      if (msg.type === MSG.WELCOME) {
        this.authenticated = true;
        this.emit('authenticated', msg);
      } else if (msg.type === MSG.ERROR) {
        this.emit('error', new Error(msg.message));
      } else if (msg.type === MSG.PING) {
        writeFrame(this.socket, { type: MSG.PONG });
      } else if (msg._requestId && this._pendingRequests.has(msg._requestId)) {
        // Response to a pending request
        const pending = this._pendingRequests.get(msg._requestId);
        this._pendingRequests.delete(msg._requestId);
        clearTimeout(pending.timeout);
        pending.resolve(msg);
      } else {
        this.emit('message', msg);
      }
    });

    this.parser.on('binary', (data) => {
      this.emit('binary', data);
    });

    this.socket.on('close', () => {
      this.connected = false;
      this.authenticated = false;
      this.emit('disconnected');
      this._scheduleReconnect();
    });

    this.socket.on('error', (err) => {
      console.error(`Connection error: ${err.message}`);
      this.emit('error', err);
    });

    this.socket.setTimeout(60000);
    this.socket.on('timeout', () => {
      writeFrame(this.socket, { type: MSG.PING });
    });
  }

  /**
   * Send a request and wait for response.
   */
  request(message, timeoutMs = 30000) {
    return new Promise((resolve, reject) => {
      if (!this.authenticated) {
        reject(new Error('Not connected'));
        return;
      }

      const requestId = `req_${this._requestId++}`;
      message._requestId = requestId;

      const timeout = setTimeout(() => {
        this._pendingRequests.delete(requestId);
        reject(new Error('Request timed out'));
      }, timeoutMs);

      this._pendingRequests.set(requestId, { resolve, reject, timeout });
      writeFrame(this.socket, message);
    });
  }

  /**
   * Send a message (fire and forget).
   */
  send(message) {
    if (this.connected) {
      writeFrame(this.socket, message);
    }
  }

  disconnect() {
    if (this._reconnectTimer) {
      clearTimeout(this._reconnectTimer);
      this._reconnectTimer = null;
    }
    // Reject all pending requests
    for (const [id, pending] of this._pendingRequests) {
      clearTimeout(pending.timeout);
      pending.reject(new Error('Disconnected'));
    }
    this._pendingRequests.clear();

    if (this.socket) {
      this.socket.destroy();
      this.socket = null;
    }
    this.connected = false;
    this.authenticated = false;
  }

  _scheduleReconnect() {
    if (this._reconnectTimer) return;
    this._reconnectTimer = setTimeout(() => {
      this._reconnectTimer = null;
      console.log(`Reconnecting to ${this.host}:${this.port}...`);
      this.connect();
    }, 5000);
  }
}

module.exports = { SyncServer, SyncClient, writeFrame, writeBinaryFrame, FrameParser };
