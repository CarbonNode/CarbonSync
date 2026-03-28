/**
 * Bandwidth Throttle — limits transfer speed.
 *
 * Uses a token bucket algorithm. Configurable via settings.bandwidthLimitMBps.
 * 0 = unlimited.
 */

const { Transform } = require('stream');

class ThrottleStream extends Transform {
  /**
   * @param {number} bytesPerSecond - Max bytes/sec (0 = unlimited)
   */
  constructor(bytesPerSecond) {
    super();
    this.bytesPerSecond = bytesPerSecond;
    this.tokens = bytesPerSecond;
    this.lastRefill = Date.now();
    this.totalBytes = 0;
  }

  _transform(chunk, encoding, callback) {
    if (this.bytesPerSecond <= 0) {
      // Unlimited
      this.totalBytes += chunk.length;
      this.push(chunk);
      return callback();
    }

    this._sendChunk(chunk, 0, callback);
  }

  _sendChunk(chunk, offset, callback) {
    // Refill tokens based on elapsed time
    const now = Date.now();
    const elapsed = (now - this.lastRefill) / 1000;
    this.tokens = Math.min(this.bytesPerSecond, this.tokens + elapsed * this.bytesPerSecond);
    this.lastRefill = now;

    const remaining = chunk.length - offset;
    if (remaining <= 0) return callback();

    const canSend = Math.min(remaining, Math.floor(this.tokens));

    if (canSend > 0) {
      this.push(chunk.subarray(offset, offset + canSend));
      this.tokens -= canSend;
      this.totalBytes += canSend;
      offset += canSend;
    }

    if (offset < chunk.length) {
      // Wait for more tokens
      const waitMs = Math.ceil((chunk.length - offset) / this.bytesPerSecond * 1000);
      setTimeout(() => this._sendChunk(chunk, offset, callback), Math.min(waitMs, 100));
    } else {
      callback();
    }
  }
}

/**
 * Create a throttle stream.
 * @param {number} mbps - Megabytes per second (0 = unlimited)
 */
function createThrottle(mbps) {
  const bytesPerSecond = mbps > 0 ? mbps * 1024 * 1024 : 0;
  return new ThrottleStream(bytesPerSecond);
}

module.exports = { ThrottleStream, createThrottle };
