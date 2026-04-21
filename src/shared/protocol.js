/**
 * CarbonSync Wire Protocol
 *
 * All communication is length-prefixed JSON over TLS.
 * Frame: [4 bytes big-endian length][JSON payload]
 */

const PROTOCOL_VERSION = 1;
const MAX_FRAME_SIZE = 256 * 1024 * 1024; // 256MB max JSON frame (large folder indexes)
// Binary data frames are 512KB each (streamed, not limited by MAX_FRAME_SIZE)
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

  // File push (device → hub)
  FILE_PUSH: 'file_push',             // Device pushes a file to hub
  FILE_PUSH_ACK: 'file_push_ack',     // Hub acknowledges file push
  FILE_DELETE_PUSH: 'file_delete_push', // Device pushes a deletion
  FILE_DELETE_ACK: 'file_delete_ack',
  PUSH_INDEX: 'push_index',           // Device sends its index for a push folder
  PUSH_DIFF: 'push_diff',             // Hub responds with what it needs
  FOLDER_LIST: 'folder_list',         // Hub broadcasts available folders

  // Quick sync
  HASH_CHECK: 'hash_check',           // Ask peer for root hashes of all folders
  HASH_CHECK_RESPONSE: 'hash_check_response',

  // Game saves
  SAVE_PUSH: 'save_push',
  SAVE_PUSH_ACK: 'save_push_ack',

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
