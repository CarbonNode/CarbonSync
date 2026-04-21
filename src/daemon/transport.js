/**
 * Transport Layer — TCP server/client with streaming file support.
 *
 * Fixed: streaming for large files, backpressure, write error handling,
 * request ID isolation per connection, proper cleanup.
 */

const net = require('net');
const tls = require('tls');
const fs = require('fs');
const crypto = require('crypto');
const { EventEmitter } = require('events');
const { MAX_FRAME_SIZE, PROTOCOL_VERSION, MSG } = require('../shared/protocol');

// ---- Frame Protocol ----

function writeFrame(socket, message) {
  if (!socket || socket.destroyed) return false;
  try {
    const json = JSON.stringify(message);
    const payload = Buffer.from(json, 'utf-8');
    if (payload.length > MAX_FRAME_SIZE) {
      console.error(`Frame too large: ${payload.length} bytes (max ${MAX_FRAME_SIZE})`);
      return false;
    }
    const header = Buffer.alloc(4);
    header.writeUInt32BE(payload.length);
    const ok = socket.write(Buffer.concat([header, payload]));
    if (!ok) {
      // Backpressure — socket buffer full. Node will buffer internally.
      // For critical messages this is acceptable; for bulk data, use streaming.
    }
    return true;
  } catch (err) {
    console.error(`writeFrame error: ${err.message}`);
    return false;
  }
}

/**
 * Stream a file to a socket in chunks with frame headers.
 * Handles backpressure properly. Used for large file transfers.
 *
 * Sends: [BLOCK_RESPONSE metadata frame] then [N x binary data frames] then [FILE_TRANSFER_END frame]
 */
function streamFileToSocket(socket, filePath, meta, chunkSize = 512 * 1024) {
  return new Promise((resolve, reject) => {
    if (!socket || socket.destroyed) return reject(new Error('Socket closed'));

    // Send metadata frame first
    writeFrame(socket, {
      type: MSG.BLOCK_RESPONSE,
      ...meta,
      streaming: true,
    });

    const stream = fs.createReadStream(filePath, { highWaterMark: chunkSize });
    let bytesSent = 0;

    stream.on('data', (chunk) => {
      // Write binary frame: [4 bytes length][1 byte 0xFF marker][data]
      const header = Buffer.alloc(5);
      header.writeUInt32BE(chunk.length + 1);
      header[4] = 0xFF;

      const ok = socket.write(Buffer.concat([header, chunk]));
      bytesSent += chunk.length;

      if (!ok) {
        // Backpressure: pause reading until socket drains
        stream.pause();
        socket.once('drain', () => stream.resume());
      }
    });

    stream.on('end', () => {
      // Send end-of-transfer marker
      writeFrame(socket, {
        type: 'transfer_end',
        ...meta,
        bytesSent,
      });
      resolve(bytesSent);
    });

    stream.on('error', (err) => {
      reject(err);
    });

    socket.on('error', (err) => {
      stream.destroy();
      reject(err);
    });
  });
}

// ---- Frame Parser ----

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

      if (this._buffer.length < 4 + frameLen) break;

      const payload = this._buffer.subarray(4, 4 + frameLen);
      this._buffer = this._buffer.subarray(4 + frameLen);

      if (payload.length > 0 && payload[0] === 0xFF) {
        this.emit('binary', payload.subarray(1));
      } else {
        try {
          const message = JSON.parse(payload.toString('utf-8'));
          this.emit('message', message);
        } catch {
          this.emit('error', new Error('Invalid JSON frame'));
        }
      }
    }
  }

  reset() { this._buffer = Buffer.alloc(0); }
}

// ---- Server ----

class SyncServer extends EventEmitter {
  constructor(opts) {
    super();
    this.port = opts.port;
    this.apiKey = opts.apiKey;
    this.tlsKey = opts.tlsKey || null;   // PEM key buffer
    this.tlsCert = opts.tlsCert || null;  // PEM cert buffer
    this.deviceId = opts.deviceId || ''; // Sent to peers in WELCOME for stable identity
    this.server = null;
    this.clients = new Map();
    this._nextId = 0;
    this._rateLimits = new Map();
  }

  start() {
    const connectionHandler = (socket) => {
      const id = this._nextId++;
      const parser = new FrameParser();
      const client = {
        socket, parser, id,
        deviceId: null, deviceName: null,
        authenticated: false,
        subscriptions: new Set(),
      };
      this.clients.set(id, client);

      socket.setTimeout(60000);

      socket.on('data', (data) => parser.feed(data));

      parser.on('message', (msg) => this._handleMessage(client, msg));
      parser.on('binary', (data) => this.emit('binary', client, data));
      parser.on('error', (err) => {
        console.error(`Frame error from client ${id}: ${err.message}`);
        socket.destroy();
      });

      socket.on('timeout', () => {
        writeFrame(socket, { type: MSG.PING });
        // Give 15s to respond, then kill
        socket.setTimeout(15000);
      });

      socket.on('close', () => {
        this.clients.delete(id);
        this._rateLimits.delete(id);
        this.emit('client-disconnected', client);
      });

      socket.on('error', () => {
        this.clients.delete(id);
        this._rateLimits.delete(id);
      });
    };

    // Use TLS if certs available, otherwise plain TCP
    if (this.tlsKey && this.tlsCert) {
      this.server = tls.createServer({
        key: this.tlsKey,
        cert: this.tlsCert,
        allowHalfOpen: true,
        rejectUnauthorized: false, // Self-signed; API key provides auth
      }, connectionHandler);
      console.log('TLS enabled');
    } else {
      this.server = net.createServer({ allowHalfOpen: true }, connectionHandler);
      console.log('TLS not available — using TCP with API key auth');
    }

    this.server.listen(this.port, '0.0.0.0', () => {
      console.log(`CarbonSync server listening on port ${this.port}${this.tlsKey ? ' (TLS)' : ''}`);
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
        client.socket.setTimeout(60000);
        break;
      default:
        if (!client.authenticated) {
          writeFrame(client.socket, { type: MSG.ERROR, message: 'Not authenticated' });
          return;
        }
        // Rate limit index requests (max 1 per 3 seconds)
        if (msg.type === MSG.INDEX_REQUEST) {
          const limit = this._rateLimits.get(client.id) || {};
          if (limit.lastIndexRequest && Date.now() - limit.lastIndexRequest < 3000) {
            writeFrame(client.socket, { type: MSG.ERROR, message: 'Rate limited', _requestId: msg._requestId });
            return;
          }
          limit.lastIndexRequest = Date.now();
          this._rateLimits.set(client.id, limit);
        }
        this.emit('message', client, msg);
        break;
    }
  }

  _handleHello(client, msg) {
    // Accept if: API key matches, OR connecting from private LAN IP
    const remoteIp = client.socket.remoteAddress?.replace('::ffff:', '') || '';
    const isLan = remoteIp.startsWith('192.168.') || remoteIp.startsWith('10.') || remoteIp === '127.0.0.1';

    if (!isLan && msg.apiKey !== this.apiKey) {
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
      deviceId: this.deviceId || '',
    });
    console.log(`Client authenticated: ${msg.deviceName} (${msg.deviceId})`);
    this.emit('client-connected', client);
  }

  send(clientId, message) {
    const client = this.clients.get(clientId);
    if (client?.authenticated) writeFrame(client.socket, message);
  }

  broadcast(message) {
    for (const [, client] of this.clients) {
      if (client.authenticated) writeFrame(client.socket, message);
    }
  }

  stop() {
    for (const [, client] of this.clients) client.socket.destroy();
    this.clients.clear();
    this._rateLimits.clear();
    if (this.server) { this.server.close(); this.server = null; }
  }

  getClientCount() { return this.clients.size; }

  getConnectedClients() {
    const result = [];
    for (const [id, client] of this.clients) {
      if (!client.authenticated) continue;
      const ip = client.socket?.remoteAddress?.replace('::ffff:', '') || '';
      result.push({
        id,
        ip,
        port: client.socket?.remotePort,
        deviceName: client.deviceName || ip,
        deviceId: client.deviceId || '',
      });
    }
    return result;
  }
}

// ---- Client ----

class SyncClient extends EventEmitter {
  constructor(opts) {
    super();
    this.host = opts.host;
    this.port = opts.port;
    this.apiKey = opts.apiKey;
    this.deviceId = opts.deviceId;
    this.deviceName = opts.deviceName;
    this.useTls = opts.useTls !== false; // Default: try TLS
    this.socket = null;
    this.parser = null;
    this.connected = false;
    this.authenticated = false;
    this._reconnectTimer = null;
    this._pendingRequests = new Map();
    this._requestId = 0;
    this._binaryCollector = null;
    // Set by disconnect() so a stray socket-close firing after intentional
    // teardown can't resurrect the client via _scheduleReconnect.
    this._disposed = false;
  }

  connect() {
    if (this.socket) this.disconnect();
    this._disposed = false;
    this._requestId = 0;

    this.parser = new FrameParser();

    const onConnect = () => {
      this.connected = true;
      this.emit('connected');
      writeFrame(this.socket, {
        type: MSG.HELLO,
        apiKey: this.apiKey,
        deviceId: this.deviceId,
        deviceName: this.deviceName,
        version: PROTOCOL_VERSION,
      });
    };

    // Try TLS first, fall back to plain TCP
    if (this.useTls) {
      this.socket = tls.connect({
        host: this.host,
        port: this.port,
        rejectUnauthorized: false, // Self-signed; API key provides auth
      }, onConnect);
    } else {
      this.socket = new net.Socket();
      this.socket.connect(this.port, this.host, onConnect);
    }

    this.socket.on('data', (data) => this.parser.feed(data));

    this.parser.on('message', (msg) => {
      if (msg.type === MSG.WELCOME) {
        this.authenticated = true;
        this.emit('authenticated', msg);
      } else if (msg.type === MSG.ERROR && msg._requestId && this._pendingRequests.has(msg._requestId)) {
        const pending = this._pendingRequests.get(msg._requestId);
        this._pendingRequests.delete(msg._requestId);
        clearTimeout(pending.timeout);
        pending.reject(new Error(msg.message || 'Server error'));
      } else if (msg.type === MSG.PING) {
        writeFrame(this.socket, { type: MSG.PONG });
      } else if (msg.type === 'transfer_end') {
        // File stream complete
        if (this._binaryCollector) {
          this._binaryCollector.resolve(this._binaryCollector.chunks);
          this._binaryCollector = null;
        }
      } else if (msg._requestId && this._pendingRequests.has(msg._requestId)) {
        const pending = this._pendingRequests.get(msg._requestId);
        this._pendingRequests.delete(msg._requestId);
        clearTimeout(pending.timeout);

        // If response indicates streaming, set up binary collector
        if (msg.streaming) {
          this._binaryCollector = {
            meta: msg,
            chunks: [],
            totalBytes: 0,
            resolve: null,
            reject: null,
          };
          // Return a promise that resolves when all binary data received
          const binaryPromise = new Promise((resolve, reject) => {
            this._binaryCollector.resolve = resolve;
            this._binaryCollector.reject = reject;
          });
          pending.resolve({ ...msg, binaryPromise });
        } else {
          pending.resolve(msg);
        }
      } else {
        this.emit('message', msg);
      }
    });

    this.parser.on('binary', (data) => {
      if (this._binaryCollector) {
        this._binaryCollector.chunks.push(data);
        this._binaryCollector.totalBytes += data.length;
        this.emit('transfer-progress', {
          bytes: this._binaryCollector.totalBytes,
          meta: this._binaryCollector.meta,
        });
      } else {
        this.emit('binary', data);
      }
    });

    this.parser.on('error', (err) => {
      console.error(`Frame error: ${err.message}`);
    });

    this.socket.on('close', () => {
      this.connected = false;
      this.authenticated = false;
      // Reject all pending requests
      for (const [, pending] of this._pendingRequests) {
        clearTimeout(pending.timeout);
        pending.reject(new Error('Disconnected'));
      }
      this._pendingRequests.clear();
      if (this._binaryCollector) {
        this._binaryCollector.reject?.(new Error('Disconnected'));
        this._binaryCollector = null;
      }
      this.emit('disconnected');
      this._scheduleReconnect();
    });

    this.socket.on('error', (err) => {
      this.emit('error', err);
    });

    this.socket.setTimeout(60000);
    this.socket.on('timeout', () => {
      writeFrame(this.socket, { type: MSG.PING });
    });
  }

  request(message, timeoutMs = 30000) {
    return new Promise((resolve, reject) => {
      if (!this.authenticated) return reject(new Error('Not connected'));
      const requestId = `r_${Date.now()}_${this._requestId++}`;
      message._requestId = requestId;
      const timeout = setTimeout(() => {
        this._pendingRequests.delete(requestId);
        reject(new Error('Request timed out'));
      }, timeoutMs);
      this._pendingRequests.set(requestId, { resolve, reject, timeout });
      writeFrame(this.socket, message);
    });
  }

  send(message) { if (this.connected) writeFrame(this.socket, message); }

  disconnect() {
    this._disposed = true;
    if (this._reconnectTimer) { clearTimeout(this._reconnectTimer); this._reconnectTimer = null; }
    for (const [, pending] of this._pendingRequests) {
      clearTimeout(pending.timeout);
      pending.reject(new Error('Disconnected'));
    }
    this._pendingRequests.clear();
    if (this._binaryCollector) {
      this._binaryCollector.reject?.(new Error('Disconnected'));
      this._binaryCollector = null;
    }
    if (this.socket) {
      // Drop listeners before destroy so the async close event can't fire handlers
      // that write to a new parser/socket from a subsequent connect().
      this.socket.removeAllListeners();
      this.socket.destroy();
      this.socket = null;
    }
    this.connected = false;
    this.authenticated = false;
  }

  _scheduleReconnect() {
    if (this._disposed) return;
    if (this._reconnectTimer) return;
    this._reconnectTimer = setTimeout(() => {
      this._reconnectTimer = null;
      console.log(`Reconnecting to ${this.host}:${this.port}...`);
      this.connect();
    }, 5000);
  }
}

module.exports = { SyncServer, SyncClient, writeFrame, streamFileToSocket, FrameParser };
