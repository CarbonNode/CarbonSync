/**
 * DaemonProxy — runs in the Electron main process.
 *
 * Forks the daemon into a separate Node.js process and proxies all calls
 * via IPC. The main process event loop stays free for UI rendering.
 *
 * Usage:
 *   const proxy = new DaemonProxy(configDir);
 *   proxy.start();
 *   const status = await proxy.call('getStatus');
 *   proxy.on('changes', (data) => { ... });
 */

const { fork } = require('child_process');
const path = require('path');
const { EventEmitter } = require('events');

class DaemonProxy extends EventEmitter {
  constructor(configDir) {
    super();
    this.configDir = configDir;
    this._process = null;
    this._nextId = 0;
    this._pending = new Map(); // id -> { resolve, reject, timer }
    this._alive = false;
  }

  /**
   * Fork the daemon process and start listening for events.
   */
  start() {
    const scriptPath = path.join(__dirname, 'daemon-process.js');
    this._process = fork(scriptPath, [this.configDir], {
      stdio: ['pipe', 'pipe', 'pipe', 'ipc'],
      execArgv: ['--max-old-space-size=8192'],
    });
    this._alive = true;

    // Forward child stdout/stderr to parent console
    if (this._process.stdout) {
      this._process.stdout.on('data', (d) => process.stdout.write(d));
    }
    if (this._process.stderr) {
      this._process.stderr.on('data', (d) => process.stderr.write(d));
    }

    this._process.on('message', (msg) => {
      if (msg.type === 'event') {
        this.emit(msg.event, msg.data);
      } else if (msg.id !== undefined) {
        const pending = this._pending.get(msg.id);
        if (!pending) return;
        this._pending.delete(msg.id);
        if (pending.timer) clearTimeout(pending.timer);
        if (msg.type === 'error') {
          pending.reject(new Error(msg.message));
        } else {
          pending.resolve(msg.data);
        }
      }
    });

    this._process.on('exit', (code, signal) => {
      this._alive = false;
      console.error(`Daemon process exited: code=${code} signal=${signal}`);
      // Reject all pending calls
      for (const [id, { reject, timer }] of this._pending) {
        if (timer) clearTimeout(timer);
        reject(new Error('Daemon process exited'));
      }
      this._pending.clear();
      this.emit('daemon-exit', { code, signal });
    });

    this._process.on('error', (err) => {
      console.error(`Daemon process error: ${err.message}`);
    });
  }

  /**
   * Send an RPC call to the daemon process.
   * Returns a Promise that resolves with the result.
   */
  call(method, ...args) {
    if (!this._alive) return Promise.reject(new Error('Daemon not running'));

    return new Promise((resolve, reject) => {
      const id = ++this._nextId;
      // 60s timeout for any call (scans/syncs can take time)
      const timer = setTimeout(() => {
        this._pending.delete(id);
        reject(new Error(`Daemon call '${method}' timed out`));
      }, 60000);

      this._pending.set(id, { resolve, reject, timer });
      this._process.send({ id, type: 'call', method, args });
    });
  }

  /**
   * Gracefully stop the daemon.
   */
  async stop() {
    if (!this._alive) return;
    try {
      await this.call('stop');
    } catch {
      // If call fails, force kill
    }
    if (this._process && !this._process.killed) {
      this._process.kill('SIGTERM');
    }
    this._alive = false;
  }

  get isAlive() {
    return this._alive;
  }
}

module.exports = { DaemonProxy };
