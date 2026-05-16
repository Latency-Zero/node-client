# LatZero Node.js Client

A Node.js client for [LatZero](https://latzero.dev) — the real-time communication fabric for distributed systems.

Connect multiple Node.js processes, Python services, and browser clients into shared **pools**, exchange **events**, call remote **processes**, and share live **buffer** state — all over a single TCP connection.

---

## Table of Contents

- [Core Concepts](#core-concepts)
- [Two Clients Explained](#two-clients-explained)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [API Reference](#api-reference)
  - [Constructor & Connection](#constructor--connection)
  - [Buffer Operations](#buffer-operations)
  - [Events](#events)
  - [Process Pool](#process-pool)
- [Advanced Patterns](#advanced-patterns)
- [Error Handling](#error-handling)

---

## Core Concepts

LatZero is built around five primitives. Understanding them will make every API decision obvious.

### Concept Map

| Concept | What it is | Analogy | Scope |
|---|---|---|---|
| **Server** | The LatZero daemon that all clients connect to | A router / message broker | Infrastructure |
| **Pool** | A named namespace that groups clients together | A room / channel | Multi-client |
| **Client** | A single connected process with a unique ID | A participant in the room | Per-process |
| **Buffer** | A shared key-value store inside a pool | Redis keys inside a DB | Pool-wide |
| **Event** | A fire-and-forget message sent to clients | WebSocket broadcast | One-to-many |
| **Process** | A registered callable function on a client | An RPC endpoint | One-to-one or one-to-many |

### Detailed Breakdown

#### 🏊 Pool
A pool is a named group. Every client joins exactly one pool at a time. Clients in the same pool share the same buffer namespace and can discover each other's processes.

```
Pool: "production"
  ├── Client: "api-server"
  ├── Client: "worker-1"
  └── Client: "dashboard"
```

#### 💾 Buffer
A buffer is a shared, in-memory key-value store scoped to a pool. Any client in the pool can read or write any key. Think of it as a lightweight, real-time Redis — values must be JSON-serializable.

| Feature | Buffer |
|---|---|
| Scope | Pool-wide (all clients share it) |
| Persistence | Optional (`persistent: true`) |
| Auto-expiry | Optional (`autoClean: <ms>`) |
| Data format | Any JSON value |
| Access | Any client in the pool |

#### 📡 Event
Events are real-time messages. A client emits an event; other clients that have registered a handler receive it. Events are fire-and-forget — the sender does not wait for a response.

| Feature | Event |
|---|---|
| Direction | One → Many (or One → One with `targetClientId`) |
| Response | None (fire-and-forget) |
| Handler | `client.on('event-name', fn)` |
| Delivery | Real-time push from server |

#### ⚙️ Process
A process is a **callable function** that a client registers with the server. Other clients can call it by its full ID (`clientId:processName`) and receive the return value. This is LatZero's RPC layer.

| Feature | Process |
|---|---|
| Direction | Caller → One specific process |
| Response | Returns the function's return value |
| Registration | `client.process.register(fn, 'name')` |
| Call | `client.process.call('clientId:name', data)` |
| Discovery | `client.process.list()` |
| Cross-language | Yes — Python and browser clients too |

### Event vs Process — Key Differences

| | **Event** (`emitEvent`) | **Process** (`process.call`) |
|---|---|---|
| Returns a value? | ❌ No | ✅ Yes |
| Waits for completion? | ❌ No | ✅ Yes |
| Multiple receivers? | ✅ Yes (broadcast) | ❌ No (one specific target) |
| Use case | Notifications, pub/sub | RPC, computation, data fetching |
| Handler registration | `client.on('name', fn)` | `client.process.register(fn, 'name')` |

---

## Two Clients Explained

This package exports **two clients** with different ergonomics for different use-cases.

### `LatZeroClient` *(default export)* — Sync-style

The default client. Operations are queued internally before the connection is established — you never need to `await` the connection or worry about race conditions on startup.

**Writes** are fire-and-forget (return Promises you can ignore).  
**Reads** return Promises you `.then()` or `await`.

```js
import LatZeroClient from 'latzero';

const client = new LatZeroClient('latzero://my-service', 'my-pool');

// These fire immediately — queued internally until connected:
client.set('status', 'booting');
client.process.register(myFn, 'handler');

// Reads return Promises:
const val = await client.get('status');

// connect() is optional — but you can await it if needed:
await client.connect();
```

### `LatZeroAsyncClient` *(named export)* — Explicit async

The async client. Every operation must be `await`-ed. You must ensure the connection is ready before using the client (or wait for the `connect` event).

```js
import { LatZeroAsyncClient } from 'latzero';

const client = new LatZeroAsyncClient('latzero://my-service', 'my-pool');

await client.connect();
await client.set('status', 'booting');
await client.process.register(myFn, 'handler');
const val = await client.get('status');
```

### Side-by-side Comparison

| | `LatZeroClient` (default) | `LatZeroAsyncClient` |
|---|---|---|
| Style | Queue-first, sync-feel | Explicit async/await |
| Pre-connect ops | ✅ Queued automatically | ❌ Must await connect first |
| Write return value | Promise (ignorable) | Promise (must await) |
| `connect()` | Optional; accepts callback | Must await |
| Best for | Services, scripts, quick setup | Precise control, testing |

---

## Installation

```bash
npm install latzero
```

Requires Node.js 18+. Uses ES Modules (`"type": "module"` in package.json).

Make sure a LatZero server is running:
```bash
latzero-server --port 14130
```

---

## Quick Start

### Sync-style (Recommended)

```js
import LatZeroClient from 'latzero';

const client = new LatZeroClient('latzero://my-app', 'production', {
    host: '127.0.0.1',
    port: 14130
});

// Attach lifecycle listeners
client.on('connect',    () => console.log('Connected'));
client.on('disconnect', () => console.log('Disconnected'));
client.on('error',      (e) => console.error('Error:', e.message));

// Write immediately — queued until connected, then sent:
client.set('app:status', 'starting');

// Read with await:
const status = await client.get('app:status');
console.log(status); // "starting"

// Register a callable process:
client.process.register((data) => data.a + data.b, 'add');

// Call a process on another client:
const result = await client.process.call('calculator:add', { a: 10, b: 5 });
console.log(result.payload.value); // 15
```

### Async-style

```js
import { LatZeroAsyncClient } from 'latzero';

const client = new LatZeroAsyncClient('latzero://my-app', 'production');

client.on('connect', async () => {
    await client.set('app:status', 'ready');
    await client.process.register(async (data) => data.a + data.b, 'add');
    console.log('Ready');
});
```

---

## API Reference

### Constructor & Connection

```js
new LatZeroClient(dsn, pool, options?)
new LatZeroAsyncClient(dsn, pool, options?)
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `dsn` | string | — | Client DSN: `latzero://your-client-id` |
| `pool` | string | — | Pool name to join |
| `options.host` | string | `'127.0.0.1'` | Server hostname |
| `options.port` | number | `14130` | Server TCP port |
| `options.timeout` | number | `5000` | Request timeout (ms) |
| `options.authToken` | string | `null` | Optional pool auth token |
| `options.autoConnect` | boolean | `true` | Connect on construction |

#### `connect(callback?)` — LatZeroClient only

```js
// Fire and forget:
client.connect();

// Callback style:
client.connect((err) => { if (!err) console.log('Ready'); });

// Promise style:
await client.connect();
```

#### `connect()` — LatZeroAsyncClient

```js
await client.connect();
```

#### `disconnect()`

```js
client.disconnect();
```

#### `switchPool(pool, authToken?)`

```js
// Sync:
client.switchPool('new-pool');

// Async:
await client.switchPool('new-pool', 'auth-token');
```

---

### Buffer Operations

Buffers are scoped to the pool. Keys are strings; values are any JSON-serializable type.

#### `set(key, value, options?)`

```js
client.set('user:42', { name: 'Alice', role: 'admin' });
client.set('session:abc', token, { autoClean: 30000 });  // expires in 30s
client.set('config',  { debug: true }, { persistent: true });
```

| Option | Type | Description |
|---|---|---|
| `autoClean` | number (ms) | Auto-delete after this many milliseconds |
| `persistent` | boolean | Survive server restarts |

#### `get(key, defaultValue?)`

```js
const user    = await client.get('user:42');
const missing = await client.get('no-such-key', 'fallback'); // → "fallback"
```

#### `delete(key)`

```js
const wasDeleted = await client.delete('user:42'); // → true / false
```

#### `exists(key)`

```js
const found = await client.exists('user:42'); // → true / false
```

#### `keys(pattern?)`

```js
const all   = await client.keys();           // all keys
const users = await client.keys('user:*');   // filtered
```

#### `values(pattern?)` / `items(pattern?)`

```js
const vals  = await client.values('user:*');  // [val, val, ...]
const pairs = await client.items('user:*');   // [[key, val], ...]
```

#### `mset(data, options?)` / `mget(keys)`

```js
await client.mset({ 'k1': 1, 'k2': 2, 'k3': 3 });
const map = await client.mget(['k1', 'k2', 'k3']); // { k1: 1, k2: 2, k3: 3 }
```

#### `deleteMany(keys)`

```js
const count = await client.deleteMany(['k1', 'k2']); // number deleted
```

#### `size()` / `stats()` / `scan(cursor, count)`

```js
const n     = await client.size();
const info  = await client.stats();
// { name, client_id, server_mode, key_count }

const [nextCursor, page] = await client.scan(0, 50);
```

#### `subscribe(key)` / `unsubscribe(key)`

Subscribe to real-time updates on a key. Updates arrive via the `bufferUpdate` event.

```js
await client.subscribe('user:42');

client.on('bufferUpdate', (data) => {
    console.log('Key changed:', data.key, '→', data.value);
});
```

---

### Events

Events are real-time push messages. They are **fire-and-forget** — no return value.

#### `on(event, handler)` / `off(event, handler)`

Register or remove handlers for any event — including server lifecycle events.

```js
// Server lifecycle
client.on('connect',      ()    => console.log('Connected'));
client.on('disconnect',   ()    => console.log('Disconnected'));
client.on('error',        (err) => console.error(err));
client.on('presence',     (p)   => console.log('Presence:', p));
client.on('bufferUpdate', (d)   => console.log('Buffer changed:', d));

// Custom application events
client.on('task:complete', (data) => saveResult(data));
```

#### `emitEvent(event, options?)`

Broadcast an event to other clients. Optionally target a single client.

```js
// Broadcast to all clients in pool:
client.emitEvent('alert', { data: { message: 'Deploy started' } });

// Target a specific client:
client.emitEvent('user:refresh', {
    data:           { userId: 42 },
    targetClientId: 'dashboard'
});
```

| Option | Type | Description |
|---|---|---|
| `data` | object | Payload sent to handlers |
| `targetClientId` | string | Restrict to one recipient |
| `responseTo` | string | Correlation ID for tracking |

#### `callEvent(event, options)` — RPC via event name

Call a named handler on a specific client and receive its return value. Unlike `emitEvent`, this waits for a response.

```js
const response = await client.callEvent('get-user-info', {
    targetClientId: 'user-service',
    data:           { userId: 42 },
    timeout:        8000
});

console.log(response.payload.value); // { name: 'Alice', ... }
```

> Handlers registered with `client.on('get-user-info', fn)` on the target client will be invoked, and their return value is sent back.

---

### Process Pool

Processes are named, callable functions registered with the server. They form LatZero's RPC layer.

#### `client.process.register(fn, name?)`

Register a function as a callable process. The function's `name` property is used if no explicit name is given.

```js
// Named function — name inferred:
client.process.register(function add(data) {
    return data.a + data.b;
});

// Arrow function — must pass explicit name:
client.process.register((data) => data.x * data.y, 'multiply');

// Async process:
client.process.register(async (data) => {
    const row = await db.query(data.id);
    return row;
}, 'fetch-user');
```

The process is reachable from any client in the pool as `clientId:processName`.

#### `client.process.call(processId, data?, options?)`

Call a specific process and wait for its return value.

```js
// Call a process on another client:
const r = await client.process.call('worker-1:add', { a: 3, b: 7 });
console.log(r.payload.value); // 10

// With timeout:
const r = await client.process.call('slow-service:fetch', { id: 42 }, { timeout: 15000 });

// Fire-and-forget (no return value needed):
client.process.call('logger:log', { msg: 'Hello' }, { responseTo: null });
```

#### `client.process.broadcast(processName, data?, options?)`

Call **all** processes registered under a given short name across the pool.

```js
const invoked = await client.process.broadcast('worker', { task: 'flush-cache' });
console.log(`Called ${invoked.length} workers: ${invoked.join(', ')}`);
```

#### `client.process.list(pattern?)`

Discover all registered processes. Optionally filter by pattern.

```js
const all     = await client.process.list();
const workers = await client.process.list('worker-*');
const adders  = await client.process.list('*:add');

// Returns: { 'worker-1:add': {...}, 'worker-2:add': {...}, ... }
```

#### `client.process.unregister(name)`

Unregister a process by its short name.

```js
client.process.unregister('add');
```

---

## Advanced Patterns

### Graceful Shutdown

```js
process.on('SIGINT', async () => {
    await client.process.unregister('my-worker');
    client.disconnect();
    process.exit(0);
});
```

### Pre-connection Setup (LatZeroClient)

Because `LatZeroClient` queues all ops, you can set up your entire service before the connection is ready. This is the canonical pattern:

```js
const client = new LatZeroClient('latzero://api-server', 'prod');

// All of this queues internally and fires once connected:
client.set('api:status', 'starting');
client.process.register(handleRequest, 'handle-request');
client.process.register(healthCheck, 'health');

client.on('connect', () => {
    client.set('api:status', 'ready');
});
```

### Cross-Language RPC

```js
// Call a Python process from Node.js:
const result = await client.process.call('python-ml:predict', {
    features: [1.2, 3.4, 5.6]
});
console.log(result.payload.value); // { label: 'cat', confidence: 0.97 }

// Call a browser client from Node.js:
const ui = await client.process.call('browser-dashboard:render', {
    chartData: myData
});
```

### Distributed Worker Pool

```js
// Register this instance as a worker:
client.process.register(async (job) => {
    console.log('Processing job:', job.id);
    const result = await doWork(job);
    return result;
}, 'worker');

// Coordinator broadcasts to all workers:
const workers = await client.process.broadcast('worker', { id: 'job-42', payload: data });
console.log(`Distributed to ${workers.length} workers`);
```

### Correlation with `responseTo`

```js
// Tag events/calls with a correlation ID for tracing:
const correlationId = crypto.randomUUID();

await client.emitEvent('job:start', {
    data: { jobId: 42 },
    responseTo: correlationId
});
```

---

## Error Handling

All methods return Promises. Use `try/catch` for `await` usage, or `.catch()` on fire-and-forget.

```js
// Await style:
try {
    await client.set('key', circularRef);
} catch (err) {
    console.error(err.message); // "Only JSON-serializable values are supported"
}

// Fire-and-forget with error tracking:
client.set('key', value).catch(err => {
    console.error('Write failed:', err.message);
});

// Timeout errors:
try {
    const r = await client.process.call('slow-service:compute', data, { timeout: 3000 });
} catch (err) {
    if (err.message === 'Process call timeout') {
        console.warn('Service is slow, retrying...');
    }
}

// Connection errors:
client.on('error', (err) => {
    console.error('Connection error:', err.message);
    // Optionally reconnect:
    setTimeout(() => client.connect(), 5000);
});
```

### Error Reference

| Error message | Cause |
|---|---|
| `DSN must look like latzero://client-id` | Malformed DSN in constructor |
| `Not connected to server` | Called `sendMessage` before connection established |
| `Request timeout` | Server did not respond within `options.timeout` |
| `Process call timeout` | Remote process did not return within timeout |
| `Call event timeout` | `callEvent` target did not respond in time |
| `Only JSON-serializable values are supported` | Passed a circular ref, function, or Symbol |
| `Pass an explicit name for anonymous functions` | Registered `(data) => ...` without a name argument |
| `callEvent requires targetClientId` | Forgot `targetClientId` option on `callEvent` |

---

## License

MIT © LatZero
