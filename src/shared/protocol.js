/**
 * CarbonSync Wire Protocol
 *
 * All communication is length-prefixed JSON over TLS.
 * Frame: [4 bytes big-endian length][JSON payload]
 */

const PROTOCOL_VERSION = 1;
const MAX_FRAME_SIZE = 64 * 1024 * 1024; // 64MB max frame (for block transfers)
const DEFAULT_PORT = 21547; // CarbonSync default port
const SERVICE_TYPE = 'carbonsync';
const CHUNK_SIZE_TARGET = 65536;  // 64KB target chunk size
const CHUNK_SIZE_MIN = 16384;     // 16KB minimum
const CHUNK_SIZE_MAX = 262144;    // 256KB maximum

// Message types
const MSG = {
  // Handshake
  HELLO: 'hello',           // Initial handshake with device info
  WELCOME: 'welcome',       // Server response to hello

  // Index
  INDEX_REQUEST: 'index_request',   // Request folder index from server
  INDEX_RESPONSE: 'index_response', // Full file index for a folder
  INDEX_DELTA: 'index_delta',       // Incremental index update

  // File transfer
  BLOCK_REQUEST: 'block_request',   // Request file blocks by hash
  BLOCK_RESPONSE: 'block_response', // Block data
  FILE_DONE: 'file_done',          // Client confirms file fully received + verified

  // Control
  PING: 'ping',
  PONG: 'pong',
  SUBSCRIBE: 'subscribe',   // Client subscribes to folder changes
  NOTIFY: 'notify',         // Server pushes change notification to subscribed clients

  // Status
  PROGRESS: 'progress',     // Sync progress update
  ERROR: 'error',
};

// Sync states
const SYNC_STATE = {
  IDLE: 'idle',
  SCANNING: 'scanning',
  COMPARING: 'comparing',
  TRANSFERRING: 'transferring',
  APPLYING: 'applying',
  DONE: 'done',
  ERROR: 'error',
};

module.exports = {
  PROTOCOL_VERSION,
  MAX_FRAME_SIZE,
  DEFAULT_PORT,
  SERVICE_TYPE,
  CHUNK_SIZE_TARGET,
  CHUNK_SIZE_MIN,
  CHUNK_SIZE_MAX,
  MSG,
  SYNC_STATE,
};
