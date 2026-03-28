/**
 * Content-Defined Chunking (CDC) Engine
 *
 * Uses Gear hash (rolling hash) to find chunk boundaries that are
 * determined by FILE CONTENT, not position. This means:
 * - Insert 1 byte at position 0 of a 4GB file → only 1-2 chunks change
 * - Rename/copy a file → zero re-transfer (same content = same chunks)
 * - Modify middle of file → only affected chunks transfer
 *
 * Each chunk is hashed with SHA-256 (fast enough, built-in, no native deps).
 */

const crypto = require('crypto');
const fs = require('fs');
const { CHUNK_SIZE_TARGET, CHUNK_SIZE_MIN, CHUNK_SIZE_MAX } = require('../shared/protocol');

// Gear hash lookup table — 256 random 32-bit values
// Pre-generated for deterministic chunking across all devices
const GEAR_TABLE = new Uint32Array([
  0x5c95c078, 0x22408989, 0x2d48a214, 0x12842087, 0x530f8afb, 0x474536b9, 0x2963b4f1, 0x44cb738b,
  0x4ea7403d, 0x4b2e0868, 0x75b50d71, 0x5c29cdde, 0x0e424ba8, 0x30ed37e4, 0x520b8425, 0x5765e5be,
  0x5765a616, 0x0e0f14ed, 0x339f4c22, 0x24cc1be3, 0x4b24de49, 0x7a3a0c8e, 0x6a430cd0, 0x4f55de9a,
  0x725e2e63, 0x4b1e0bbc, 0x20b85a9a, 0x0a4f5a0c, 0x1c0f7ae0, 0x59c56c93, 0x51cd62fc, 0x2a831edf,
  0x4e5cad86, 0x7c4d98c0, 0x3c25f527, 0x3e9c0990, 0x40c05c87, 0x0b5b5c53, 0x4d00e6e7, 0x52c58b65,
  0x0f9e8c61, 0x3a2be724, 0x78d5f3a0, 0x51a5e793, 0x2bfec357, 0x1b28d0a9, 0x44976bbb, 0x26b8e471,
  0x315e6ca3, 0x47937b9e, 0x30e4481c, 0x5bde1e8b, 0x3a3b7e22, 0x28fb9ed6, 0x10a7e52a, 0x5116444b,
  0x7ea5fa0c, 0x0e0f2b31, 0x17e2c6b0, 0x4a5e6ae7, 0x6bfa1e21, 0x1a3f4d6a, 0x395a087c, 0x50e9b1a4,
  0x44d2ef1c, 0x62b9442f, 0x7bb22f28, 0x75a6cb7c, 0x0ac2d2ea, 0x5fab2e77, 0x3ee35333, 0x5f3d4db0,
  0x527f2d69, 0x4cee1e64, 0x0d6f15b4, 0x3f1b6690, 0x26e2fb94, 0x2e513797, 0x4b1d3cd1, 0x5b1e7f0c,
  0x2c3e5f48, 0x5a4d08cf, 0x19f2ad10, 0x4c3e9f3c, 0x5aa5d078, 0x3b2e9a40, 0x4e5f2b7e, 0x58b28f15,
  0x145c9dad, 0x7b33dcab, 0x5f2b3f38, 0x4f03c35c, 0x2ddf2f7c, 0x2b8a5e52, 0x3c6e75b8, 0x42f5e5f3,
  0x6c9e34c1, 0x37a4c3dc, 0x10e7bf54, 0x3e88ab1c, 0x5a9a5819, 0x4bab3e91, 0x24dde3da, 0x6f0a3148,
  0x4cdd8ead, 0x3ba0ec7a, 0x7b735eb9, 0x3d5e2c25, 0x4e9e31b0, 0x1f98e24e, 0x4b5f2fcc, 0x57a89c31,
  0x5f17d0d6, 0x3e9cbe2a, 0x6f0a3a47, 0x28fd6a2e, 0x78d7ee50, 0x2d7ebcae, 0x0a56791e, 0x5b3ab918,
  0x5e4e4891, 0x7af02a12, 0x3c88b2cc, 0x1b6d8a5a, 0x6d3f2e5c, 0x2b8b4e7a, 0x47e37c53, 0x5cc40b4e,
  0x36e6f399, 0x4ac3c8c9, 0x09a60e4c, 0x51c66b4a, 0x1c7ae2a3, 0x3db6d2ef, 0x2ad6b1e0, 0x38c4cd16,
  0x1bfe7f84, 0x5f4f2ce7, 0x18f8e8c9, 0x556a7e27, 0x4e6b1e5a, 0x6c3a62b4, 0x2c5b38ab, 0x4d0fba71,
  0x6eb83a99, 0x33a7b750, 0x0e5a3d93, 0x4ec4e5b2, 0x276e3bc4, 0x3d1b9e45, 0x51d5e81a, 0x62e2f975,
  0x5b2ec1b3, 0x7a2f9d3a, 0x45c3e5c6, 0x096be4fe, 0x3e1f5b4e, 0x4a2e7c3d, 0x58d58e2b, 0x214fb719,
  0x1a7f13c0, 0x4f6a5bd8, 0x2d35e5ac, 0x78e26c52, 0x16b73e1a, 0x5e2c4f8d, 0x3c4f7e56, 0x7fb82e3a,
  0x0e1c3a56, 0x509e72e6, 0x3c2b5e4f, 0x4a7d6eb1, 0x7e1f3a5b, 0x28e65c3d, 0x5d3b7a4c, 0x163e5b2f,
  0x5a4f7e83, 0x3e2c5f7a, 0x74b6e2c5, 0x0d5a3e71, 0x44e5c3b9, 0x286f5d2c, 0x5e3a4f71, 0x68b9e2a4,
  0x1c3f5b6e, 0x4b7e2d3a, 0x5a1c3e7b, 0x32e4c6a5, 0x7f3b5e2a, 0x0a4c7e3d, 0x5b2e3f6a, 0x41c5e8b7,
  0x6e3a5b2c, 0x15e7c3f4, 0x4a2d5e3b, 0x583f7c2e, 0x2c5b3e7a, 0x7a4e5c3d, 0x3f2b5e4a, 0x5e7c3a2b,
  0x4b3f5e2d, 0x6a2c3e5b, 0x1e5b7a3c, 0x53e2c4b7, 0x7c3a5e2b, 0x2e4f3b5a, 0x5a7c2e3b, 0x45e3b7c2,
  0x3e5a7b2c, 0x6b2e3a5c, 0x1a5c3e7b, 0x5e2b4a3c, 0x7b3c5e2a, 0x4a5e3b2c, 0x2c3b5e7a, 0x5e7a3c2b,
  0x3b2c5e7a, 0x7a5e2c3b, 0x5c3b7a2e, 0x2e7a3c5b, 0x4c5e3a7b, 0x6a3b5c2e, 0x1c7b5e3a, 0x5a2c3b7e,
  0x3c5b2e7a, 0x7e3a5c2b, 0x2b5c7a3e, 0x5b3e2c7a, 0x4e7a3b5c, 0x6c2e5a3b, 0x3a7b5c2e, 0x5e2b7a3c,
  0x7a3c2e5b, 0x2c5e3b7a, 0x5c7a2e3b, 0x4a3b7c5e, 0x1e5c3a7b, 0x53b7e2c4, 0x3b5e7a2c, 0x7c2b5e3a,
  0x5e3a7c2b, 0x2b7a5e3c, 0x4c3e5b7a, 0x6e5a3c2b, 0x3c2b7a5e, 0x5a7e3c2b, 0x7b5c2e3a, 0x2e3a5c7b,
  0x5b7a3c2e, 0x4e2c7b5a, 0x1a3c5e7b, 0x57e2c3b4, 0x3e7b5a2c, 0x7a2c3e5b, 0x2c3e5b7a, 0x5e3b7a2c,
]);

// Mask determines average chunk size: ~64KB
// Lower bits = larger average size. For 64KB: mask with ~16 bits set
const CHUNK_MASK = 0x0000FFFF;

/**
 * Chunk a file using content-defined chunking.
 * Returns array of { offset, length, hash } objects.
 */
async function chunkFile(filePath) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let gearHash = 0;
    let chunkStart = 0;
    let offset = 0;
    let currentHash = crypto.createHash('sha256');

    const stream = fs.createReadStream(filePath, { highWaterMark: 256 * 1024 });

    stream.on('data', (buffer) => {
      for (let i = 0; i < buffer.length; i++) {
        const byte = buffer[i];
        gearHash = ((gearHash << 1) + GEAR_TABLE[byte]) >>> 0;
        currentHash.update(Buffer.from([byte]));
        offset++;

        const chunkLen = offset - chunkStart;

        // Check for chunk boundary
        if (chunkLen >= CHUNK_SIZE_MIN) {
          if ((gearHash & CHUNK_MASK) === 0 || chunkLen >= CHUNK_SIZE_MAX) {
            chunks.push({
              offset: chunkStart,
              length: chunkLen,
              hash: currentHash.digest('hex'),
            });
            chunkStart = offset;
            gearHash = 0;
            currentHash = crypto.createHash('sha256');
          }
        }
      }
    });

    stream.on('end', () => {
      // Flush remaining data as final chunk
      const remaining = offset - chunkStart;
      if (remaining > 0) {
        chunks.push({
          offset: chunkStart,
          length: remaining,
          hash: currentHash.digest('hex'),
        });
      }
      resolve(chunks);
    });

    stream.on('error', reject);
  });
}

/**
 * Hash an entire file (for small files or quick comparison).
 */
async function hashFile(filePath) {
  return new Promise((resolve, reject) => {
    const hash = crypto.createHash('sha256');
    const stream = fs.createReadStream(filePath, { highWaterMark: 1024 * 1024 });
    stream.on('data', (chunk) => hash.update(chunk));
    stream.on('end', () => resolve(hash.digest('hex')));
    stream.on('error', reject);
  });
}

/**
 * Read a specific chunk from a file.
 */
async function readChunk(filePath, offset, length) {
  return new Promise((resolve, reject) => {
    const buf = Buffer.alloc(length);
    fs.open(filePath, 'r', (err, fd) => {
      if (err) return reject(err);
      fs.read(fd, buf, 0, length, offset, (err, bytesRead) => {
        fs.close(fd, () => {});
        if (err) return reject(err);
        resolve(buf.subarray(0, bytesRead));
      });
    });
  });
}

module.exports = { chunkFile, hashFile, readChunk };
