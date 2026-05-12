# LatZero Node.js Client

A Node.js client for connecting to LatZero server mode.

## Installation

```bash
npm install latzero-node-client
```

## Usage

```javascript
import LatZeroClient from 'latzero-node-client';

// Create client
const client = new LatZeroClient('latzero://my-node-client', 'my-pool', {
    host: '127.0.0.1',
    port: 14130,
    authToken: null, // optional
    timeout: 5000 // request timeout in ms
});

// Wait for connection
client.on('connect', () => {
    console.log('Connected to LatZero server');
});

// Handle connection events
client.on('disconnect', () => {
    console.log('Disconnected from server');
});

client.on('error', (err) => {
    console.error('Connection error:', err);
});

// Basic key-value operations
await client.set('user', { name: 'Alice', age: 30 });
const user = await client.get('user');
console.log(user); // { name: 'Alice', age: 30 }

const keys = await client.keys();
console.log(keys); // ['user']

await client.delete('user');
console.log(await client.exists('user')); // false

// Batch operations
await client.mset({
    'key1': 'value1',
    'key2': 'value2'
});

const values = await client.mget(['key1', 'key2']);
console.log(values); // { key1: 'value1', key2: 'value2' }

// Event handling
client.on('presence', (data) => {
    console.log('Presence update:', data);
});

client.on('bufferUpdate', (data) => {
    console.log('Buffer update:', data);
});

// Register event handlers
client.on('compute:multiply', ({ x, y }) => {
    return x * y;
});

// Emit events
await client.emitEvent('user:login', {
    data: { username: 'alice' }
});

// Call events (RPC)
const result = await client.callEvent('compute:multiply', {
    targetClientId: 'other-client',
    data: { x: 7, y: 6 }
});
console.log(result); // 42

// Cleanup
client.disconnect();
```

## API Reference

### Constructor
- `new LatZeroClient(dsn, pool, options)`

**Parameters:**
- `dsn` (string): Client DSN in format `latzero://client-id`
- `pool` (string): Pool name to join
- `options` (object): Optional configuration
  - `host` (string): Server host (default: '127.0.0.1')
  - `port` (number): Server port (default: 14130)
  - `authToken` (string): Optional authentication token
  - `timeout` (number): Request timeout in ms (default: 5000)

### Key-Value Operations
- `set(key, value, options)` - Set a key with optional TTL and persistence
- `get(key, defaultValue)` - Get a value, return default if not found
- `delete(key)` - Delete a key
- `exists(key)` - Check if key exists
- `keys(pattern)` - List keys, optional pattern filtering
- `values(pattern)` - Get all values, optional pattern filtering
- `items(pattern)` - Get key-value pairs, optional pattern filtering
- `mset(data, options)` - Set multiple keys
- `mget(keys)` - Get multiple keys
- `deleteMany(keys)` - Delete multiple keys
- `size()` - Get number of keys
- `stats()` - Get client and pool statistics
- `scan(cursor, count)` - Paginated key scanning

### Event Operations
- `on(event, handler)` - Register event handler
- `off(event, handler)` - Remove event handler
- `emitEvent(event, options)` - Emit fire-and-forget event
- `callEvent(event, options)` - Emit RPC-style event with response

### Connection Management
- `connect()` - Connect to server (called automatically)
- `disconnect()` - Disconnect from server
- `switchPool(pool, authToken)` - Switch to different pool

### Built-in Events
- `connect` - Client connected to server
- `disconnect` - Client disconnected from server
- `error` - Connection or protocol error
- `presence` - Client presence updates
- `bufferUpdate` - Buffer change notifications
