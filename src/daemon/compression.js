/**
 * Wire Compression — zlib compress/decompress for network transfers.
 *
 * Compresses JSON frames and file data on the wire.
 * Typical 2-5x compression on text files, 1.1-1.5x on binary.
 * Huge win for WAN sync and large indexes.
 */

const zlib = require('zlib');

const COMPRESSION_THRESHOLD = 1024; // Don't compress frames < 1KB

/**
 * Compress a buffer if it's worth it.
 * Returns { compressed: bool, data: Buffer }
 */
function compress(data) {
  if (data.length < COMPRESSION_THRESHOLD) {
    return { compressed: false, data };
  }

  try {
    const deflated = zlib.deflateSync(data, {
      level: zlib.constants.Z_DEFAULT_COMPRESSION,
    });

    // Only use compressed version if it's actually smaller
    if (deflated.length < data.length * 0.95) {
      return { compressed: true, data: deflated };
    }
    return { compressed: false, data };
  } catch {
    return { compressed: false, data };
  }
}

/**
 * Decompress a buffer.
 */
function decompress(data) {
  try {
    return zlib.inflateSync(data);
  } catch (err) {
    throw new Error(`Decompression failed: ${err.message}`);
  }
}

/**
 * Create a streaming compressor for file transfers.
 */
function createCompressStream() {
  return zlib.createDeflate({
    level: zlib.constants.Z_DEFAULT_COMPRESSION,
    chunkSize: 64 * 1024,
  });
}

/**
 * Create a streaming decompressor.
 */
function createDecompressStream() {
  return zlib.createInflate({ chunkSize: 64 * 1024 });
}

module.exports = { compress, decompress, createCompressStream, createDecompressStream };
