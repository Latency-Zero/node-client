# LatZero Node.js Client Usage Guide

## Overview

The LatZero Node.js Client is a comprehensive client library that provides both buffer operations and process pool functionality for LatZero server mode. It enables real-time communication between Node.js processes, web clients, and Python processes in the same pool.

## Features

- **Buffer Operations**: Set, get, delete, keys, values, items, mset, mget, etc.
- **Process Pool**: Register, call, broadcast, and manage distributed processes
- **Event System**: Real-time event handling and cross-process communication
- **TCP Connection**: Reliable TCP connection with automatic reconnection
- **Cross-Language Compatibility**: Works seamlessly with web and Python clients
- **ES Modules**: Modern JavaScript with full async/await support

## Installation

### Setup

1. Copy the fixed client file to your project:
```bash
cp index-fixed.js latzero-client.js
```

2. Install dependencies (if using package.json):
```bash
npm install
```

### Basic Setup

```javascript
import LatZeroClient from './latzero-client.js';
```

## Quick Start

```javascript
import LatZeroClient from './index-fixed.js';

// Create client instance
const client = new LatZeroClient('latzero://my-node-client', 'my-pool', {
    host: '127.0.0.1',
    port: 14130,
    autoConnect: true
});

// Set up event listeners
client.on('connect', () => {
    console.log('Connected to LatZero server');
});

client.on('disconnect', () => {
    console.log('Disconnected from server');
});

client.on('error', (error) => {
    console.error('Connection error:', error.message);
});
```

## API Reference

### Constructor

```javascript
new LatZeroClient(dsn, pool, options)
```

**Parameters:**
- `dsn` (string): Client DSN in format `latzero://client-id`
- `pool` (string): Pool name to join
- `options` (object): Optional configuration
  - `host` (string): Server host (default: '127.0.0.1')
  - `port` (number): Server port (default: 14130)
  - `timeout` (number): Request timeout in ms (default: 5000)
  - `autoConnect` (boolean): Auto-connect on creation (default: true)

### Buffer Operations

#### set(key, value, options)
Store a value in the buffer.

```javascript
await client.set('user:123', { name: 'John', age: 30 });
await client.set('temp:data', 'expires soon', { autoClean: 30000 });
await client.set('config', { debug: true }, { persistent: true });
```

#### get(key, defaultValue)
Retrieve a value from the buffer.

```javascript
const user = await client.get('user:123');
const config = await client.get('config', { debug: false });
```

#### delete(key)
Delete a key from the buffer.

```javascript
const deleted = await client.delete('user:123');
```

#### exists(key)
Check if a key exists.

```javascript
const exists = await client.exists('user:123');
```

#### keys(pattern)
List all keys matching a pattern.

```javascript
const allKeys = await client.keys();
const userKeys = await client.keys('user:*');
```

#### values(pattern)
Get all values for keys matching a pattern.

```javascript
const allValues = await client.values();
const userValues = await client.values('user:*');
```

#### items(pattern)
Get key-value pairs for keys matching a pattern.

```javascript
const allItems = await client.items();
const userItems = await client.items('user:*');
```

#### mset(data, options)
Set multiple key-value pairs.

```javascript
await client.mset({
    'user:123': { name: 'John' },
    'user:456': { name: 'Jane' },
    'config': { debug: true }
});
```

#### mget(keys)
Get multiple values.

```javascript
const values = await client.mget(['user:123', 'user:456', 'config']);
```

#### deleteMany(keys)
Delete multiple keys.

```javascript
const deletedCount = await client.deleteMany(['user:123', 'user:456']);
```

#### size()
Get the number of keys in the buffer.

```javascript
const count = await client.size();
```

#### stats()
Get pool statistics.

```javascript
const stats = await client.stats();
console.log(stats);
// {
//   name: 'my-pool',
//   client_id: 'my-node-client',
//   server_mode: true,
//   key_count: 42
// }
```

#### scan(cursor, count)
Paginate through keys.

```javascript
const [nextCursor, keys] = await client.scan(0, 100);
```

### Process Pool Operations

#### client.process.register(fn, nameOverride)
Register a function as a named process.

```javascript
// Register an add function
await client.process.register(function(data) {
    const { a, b } = data;
    return a + b;
}, 'add');

// Register with explicit name (for anonymous functions)
await client.process.register((data) => {
    return data.x * data.y;
}, 'multiply');

// Async function
await client.process.register(async (data) => {
    const result = await expensiveCalculation(data);
    return result;
}, 'calculate');
```

#### client.process.call(processId, data, options)
Call a specific process by ID.

```javascript
const result = await client.process.call('other-client:add', { a: 5, b: 3 });
console.log(result.payload.value); // 8

// With timeout
const result = await client.process.call('client:process', { x: 10 }, { timeout: 10000 });

// Non-blocking (fire and forget)
await client.process.call('client:logger', { message: 'Task completed' }, { 
    responseTo: null 
});
```

#### client.process.broadcast(processName, data, options)
Broadcast to all processes with a given name.

```javascript
const invoked = await client.process.broadcast('add', { a: 5, b: 3 });
console.log(`Invoked ${invoked.length} processes: ${invoked.join(', ')}`);

// Broadcast to specific pattern
const invoked = await client.process.broadcast('worker:*', { task: 'process-data' });
```

#### client.process.list(pattern)
List all registered processes.

```javascript
const allProcesses = await client.process.list();
const myProcesses = await client.process.list('my-client:*');
const workerProcesses = await client.process.list('*:worker');
```

#### client.process.unregister(name)
Unregister a process.

```javascript
await client.process.unregister('add');
```

### Event System

#### on(event, handler)
Register an event handler.

```javascript
client.on('user-updated', (data) => {
    console.log('User updated:', data);
    updateUserInDatabase(data);
});

// Chainable
client.on('task-completed', handleTaskCompleted)
     .on('error', handleError);
```

#### off(event, handler)
Remove an event handler.

```javascript
const handler = (data) => console.log(data);
client.on('test', handler);
client.off('test', handler);
```

#### emitEvent(event, options)
Emit a fire-and-forget event.

```javascript
await client.emitEvent('user-updated', {
    data: { userId: 123, name: 'John' },
    targetClientId: 'admin-client'
});

// Broadcast to all clients
await client.emitEvent('system-alert', {
    data: { message: 'Server maintenance in 5 minutes' }
});
```

#### callEvent(event, options)
Emit an RPC-style event with response.

```javascript
const response = await client.callEvent('get-user-info', {
    targetClientId: 'user-service',
    data: { userId: 123 }
});

console.log('User info:', response.payload.value);
```

### Connection Management

#### connect()
Connect to the server.

```javascript
await client.connect();
```

#### disconnect()
Disconnect from the server.

```javascript
client.disconnect();
```

#### switchPool(pool, authToken)
Switch to a different pool.

```javascript
await client.switchPool('new-pool', 'auth-token');
```

## Cross-Process Communication

### Node.js to Web Client

```javascript
// Web client registers 'add' function
// Node.js calls it
const result = await client.process.call('web-client:add', { x: 10, y: 20 });
console.log(result.payload.value); // 30
```

### Node.js to Python Process

```javascript
// Python process registers 'calculate' function
// Node.js calls it
const result = await client.process.call('python-client:calculate', {
    input: [1, 2, 3, 4, 5],
    operation: 'sum'
});
```

### Receiving Calls from Other Clients

```javascript
// Register function that can be called by other clients
await client.process.register(async (data) => {
    console.log('Processing request:', data);
    
    // Handle different data formats for compatibility
    const a = data.a !== undefined ? data.a : data.x;
    const b = data.b !== undefined ? data.b : data.y;
    
    const result = a + b;
    console.log(`Returning result: ${result}`);
    
    return result;
}, 'add');
```

## Event Handling

### Server Events

```javascript
client.on('presence', (data) => {
    console.log('Client joined/left:', data);
    updateClientList();
});

client.on('bufferUpdate', (data) => {
    console.log('Buffer changed:', data);
    invalidateCache(data.key);
});
```

### Custom Events

```javascript
// Register handler
client.on('notification', (data) => {
    sendEmail(data.recipient, data.subject, data.message);
});

// Emit from another client
await client.emitEvent('notification', {
    data: { 
        recipient: 'admin@example.com',
        subject: 'Critical Alert',
        message: 'Server CPU usage above 90%'
    },
    targetClientId: 'notification-service'
});
```

### Process Events

```javascript
// Handle process-specific events
client.on('process-completed', (data) => {
    console.log(`Process ${data.processId} completed with result:`, data.result);
    updateJobStatus(data.jobId, 'completed');
});
```

## Error Handling

### Try-Catch Pattern

```javascript
try {
    await client.set('key', 'value');
    console.log('Value stored successfully');
} catch (error) {
    if (error.code === 'timeout') {
        console.log('Request timed out, retrying...');
        await client.set('key', 'value');
    } else {
        console.error('Operation failed:', error.message);
        throw error;
    }
}
```

### Event-Based Error Handling

```javascript
client.on('error', (error) => {
    console.error('Client error:', error.message);
    
    if (error.code === 'connection_lost') {
        // Implement reconnection logic
        setTimeout(() => {
            console.log('Attempting to reconnect...');
            client.connect();
        }, 5000);
    }
});
```

### Process Error Handling

```javascript
await client.process.register(async (data) => {
    try {
        const result = await riskyOperation(data);
        return result;
    } catch (error) {
        console.error('Process error:', error);
        // Re-throw to send error back to caller
        throw error;
    }
}, 'risky-operation');
```

## Best Practices

### Process Registration

```javascript
// ✅ Good: Use explicit names for anonymous functions
await client.process.register((data) => {
    return data.x * data.y;
}, 'multiply');

// ✅ Good: Handle both data formats for compatibility
await client.process.register(function(data) {
    const a = data.a !== undefined ? data.a : data.x;
    const b = data.b !== undefined ? data.b : data.y;
    return a + b;
}, 'add');

// ✅ Good: Async functions with proper error handling
await client.process.register(async (data) => {
    try {
        const result = await databaseOperation(data);
        return result;
    } catch (error) {
        console.error('Database operation failed:', error);
        throw error; // Propagate error to caller
    }
}, 'db-operation');

// ❌ Avoid: Anonymous functions without explicit names
await client.process.register((data) => data.a + data.b); // Will fail
```

### Connection Management

```javascript
// ✅ Good: Graceful shutdown
process.on('SIGINT', async () => {
    console.log('Shutting down gracefully...');
    
    // Unregister processes
    await client.process.unregister('add');
    await client.process.unregister('multiply');
    
    // Disconnect
    client.disconnect();
    
    process.exit(0);
});

// ✅ Good: Handle connection states
client.on('connect', async () => {
    console.log('Connected, registering processes...');
    await registerAllProcesses();
});

client.on('disconnect', () => {
    console.log('Disconnected, pausing operations...');
    pausePeriodicTasks();
});
```

### Error Recovery

```javascript
// ✅ Good: Implement retry logic with exponential backoff
async function robustOperation(operation, maxRetries = 3) {
    for (let i = 0; i < maxRetries; i++) {
        try {
            return await operation();
        } catch (error) {
            if (i === maxRetries - 1) throw error;
            
            const delay = Math.pow(2, i) * 1000; // Exponential backoff
            console.log(`Operation failed, retrying in ${delay}ms...`);
            await new Promise(resolve => setTimeout(resolve, delay));
        }
    }
}

// Usage
await robustOperation(() => client.set('critical-key', 'value'));
```

### Resource Management

```javascript
// ✅ Good: Clean up resources
class MyService {
    constructor(client) {
        this.client = client;
        this.processes = ['add', 'multiply', 'divide'];
        this.eventHandlers = [];
    }
    
    async start() {
        // Register processes
        for (const processName of this.processes) {
            await this.client.process.register(this[processName].bind(this), processName);
        }
        
        // Register event handlers
        const handler = (data) => this.handleData(data);
        this.client.on('data-update', handler);
        this.eventHandlers.push(['data-update', handler]);
    }
    
    async stop() {
        // Unregister processes
        for (const processName of this.processes) {
            await this.client.process.unregister(processName);
        }
        
        // Remove event handlers
        for (const [event, handler] of this.eventHandlers) {
            this.client.off(event, handler);
        }
        
        this.client.disconnect();
    }
}
```

## Advanced Usage

### Process Chaining

```javascript
// Register multiple processes that work together
await client.process.register(async (data) => {
    const numbers = await client.process.call('data-service:get-numbers', { 
        source: data.source 
    });
    
    const sum = await client.process.call('calculator:add', { 
        a: numbers.payload.value[0], 
        b: numbers.payload.value[1] 
    });
    
    return sum.payload.value;
}, 'calculate-sum-from-source');
```

### Distributed Task Processing

```javascript
class TaskWorker {
    constructor(client) {
        this.client = client;
        this.processing = false;
    }
    
    async start() {
        // Register as a worker
        await this.client.process.register(this.processTask.bind(this), 'worker');
        
        // Listen for task events
        this.client.on('task-available', (data) => {
            if (!this.processing) {
                this.requestTask();
            }
        });
        
        console.log('Worker started');
    }
    
    async processTask(data) {
        this.processing = true;
        
        try {
            console.log('Processing task:', data);
            
            // Simulate work
            await new Promise(resolve => setTimeout(resolve, 1000));
            
            const result = {
                taskId: data.taskId,
                result: data.input * 2,
                workerId: this.client.clientId,
                timestamp: Date.now()
            };
            
            console.log('Task completed:', result);
            return result;
            
        } finally {
            this.processing = false;
            
            // Request next task
            this.requestTask();
        }
    }
    
    async requestTask() {
        await this.client.emitEvent('worker-ready', {
            data: { workerId: this.client.clientId }
        });
    }
}
```

### Real-time Data Synchronization

```javascript
class DataSync {
    constructor(client) {
        this.client = client;
        this.localCache = new Map();
        this.syncInProgress = false;
    }
    
    async start() {
        // Listen for buffer updates
        this.client.on('bufferUpdate', (data) => {
            this.handleBufferUpdate(data);
        });
        
        // Initial sync
        await this.syncAll();
        
        // Periodic sync
        setInterval(() => this.syncAll(), 30000); // Every 30 seconds
    }
    
    handleBufferUpdate(data) {
        if (data.key.startsWith('sync:')) {
            const localKey = data.key.replace('sync:', '');
            
            if (data.deleted) {
                this.localCache.delete(localKey);
                console.log(`Local key deleted: ${localKey}`);
            } else {
                this.localCache.set(localKey, data.value);
                console.log(`Local key updated: ${localKey}`);
            }
            
            this.notifySubscribers(localKey, data.value);
        }
    }
    
    async syncAll() {
        if (this.syncInProgress) return;
        
        this.syncInProgress = true;
        
        try {
            const remoteKeys = await this.client.keys('sync:*');
            
            for (const remoteKey of remoteKeys) {
                const localKey = remoteKey.replace('sync:', '');
                const remoteValue = await this.client.get(remoteKey);
                const localValue = this.localCache.get(localKey);
                
                if (JSON.stringify(remoteValue) !== JSON.stringify(localValue)) {
                    this.localCache.set(localKey, remoteValue);
                    this.notifySubscribers(localKey, remoteValue);
                }
            }
            
            console.log('Sync completed');
        } finally {
            this.syncInProgress = false;
        }
    }
    
    notifySubscribers(key, value) {
        // Emit local event for application code
        this.client.emit(`local:${key}`, value);
    }
    
    get(key) {
        return this.localCache.get(key);
    }
    
    async set(key, value) {
        this.localCache.set(key, value);
        await this.client.set(`sync:${key}`, value);
    }
}
```

## Testing

### Unit Testing

```javascript
import { LatZeroClient } from './index-fixed.js';

// Mock server for testing
class MockServer {
    constructor() {
        this.clients = new Map();
        this.processes = new Map();
        this.buffers = new Map();
    }
    
    // Implement mock server methods...
}

// Test process registration
async function testProcessRegistration() {
    const mockServer = new MockServer();
    const client = new LatZeroClient('latzero://test-client', 'test-pool');
    
    // Mock connection
    client.connected = true;
    
    // Register process
    await client.process.register((data) => data.a + data.b, 'add');
    
    // Verify registration
    const processes = await client.process.list();
    console.assert(processes['test-client:add'] !== undefined, 'Process not registered');
    
    console.log('✅ Process registration test passed');
}

// Test cross-process communication
async function testCrossProcessCommunication() {
    const client1 = new LatZeroClient('latzero://client1', 'test-pool');
    const client2 = new LatZeroClient('latzero://client2', 'test-pool');
    
    // Mock connections
    client1.connected = true;
    client2.connected = true;
    
    // Register process on client2
    await client2.process.register((data) => data.x * data.y, 'multiply');
    
    // Call from client1
    const result = await client1.process.call('client2:multiply', { x: 5, y: 3 });
    
    console.assert(result.payload.value === 15, 'Cross-process call failed');
    
    console.log('✅ Cross-process communication test passed');
}
```

### Integration Testing

```javascript
// test-fixed.js provides comprehensive integration testing
// Run with: node test-fixed.js
```

## Troubleshooting

### Common Issues

**"Process not found in server TUI"**
- Ensure you're using `index-fixed.js` (fixed version)
- Check that the client successfully connected
- Verify process registration completed without errors
- Check server logs for registration messages

**"Cross-process calls failing"**
- Ensure both clients are in the same pool
- Check process ID format: `client-id:process-name`
- Verify data format compatibility
- Check network connectivity

**"Connection issues"**
- Check server is running on correct host/port
- Verify TCP server is accessible
- Check for firewall issues
- Ensure no other process is using the port

**"Memory leaks"**
- Properly unregister processes when shutting down
- Remove event handlers
- Disconnect client properly
- Clean up intervals and timeouts

### Debug Mode

```javascript
// Enable detailed logging
client.on('connect', () => {
    console.log('Connected successfully');
});

client.on('disconnect', () => {
    console.log('Disconnected');
});

client.on('error', (error) => {
    console.error('Connection error:', error);
});

// Log all server messages
client.on('presence', (data) => {
    console.log('Presence:', data);
});

client.on('bufferUpdate', (data) => {
    console.log('Buffer update:', data);
});
```

### Performance Monitoring

```javascript
class PerformanceMonitor {
    constructor(client) {
        this.client = client;
        this.metrics = {
            requests: 0,
            errors: 0,
            responseTime: []
        };
        
        this.startMonitoring();
    }
    
    startMonitoring() {
        // Monitor request patterns
        const originalSendRequest = client.sendRequest.bind(client);
        client.sendRequest = async (...args) => {
            const start = Date.now();
            this.metrics.requests++;
            
            try {
                const result = await originalSendRequest(...args);
                const responseTime = Date.now() - start;
                this.metrics.responseTime.push(responseTime);
                
                if (responseTime > 1000) {
                    console.warn(`Slow request: ${responseTime}ms`);
                }
                
                return result;
            } catch (error) {
                this.metrics.errors++;
                throw error;
            }
        };
        
        // Report metrics every 30 seconds
        setInterval(() => this.reportMetrics(), 30000);
    }
    
    reportMetrics() {
        const avgResponseTime = this.metrics.responseTime.length > 0
            ? this.metrics.responseTime.reduce((a, b) => a + b, 0) / this.metrics.responseTime.length
            : 0;
            
        console.log('Performance Metrics:', {
            requests: this.metrics.requests,
            errors: this.metrics.errors,
            avgResponseTime: Math.round(avgResponseTime),
            errorRate: (this.metrics.errors / this.metrics.requests * 100).toFixed(2) + '%'
        });
        
        // Reset metrics
        this.metrics.responseTime = [];
    }
}
```

## Server Integration

The Node.js client integrates seamlessly with the LatZero server TUI:

- **Processes Tab**: Shows all registered processes with their owners
- **Clients Tab**: Displays connected Node.js clients
- **Buffers Tab**: Shows stored key-value pairs
- **Events Tab**: Real-time event log

Ensure processes appear in the server TUI to verify proper registration and visibility.

## Package.json

```json
{
  "name": "latzero-node-client",
  "version": "1.0.0",
  "type": "module",
  "description": "LatZero Node.js client with process pool and buffer operations",
  "main": "index-fixed.js",
  "scripts": {
    "test": "node test-fixed.js",
    "start": "node example.js"
  },
  "dependencies": {},
  "devDependencies": {
    "node": ">=18.0.0"
  },
  "engines": {
    "node": ">=18.0.0"
  }
}
```

## Examples

### Basic Example

```javascript
import LatZeroClient from './index-fixed.js';

const client = new LatZeroClient('latzero://example-client', 'demo-pool');

client.on('connect', async () => {
    console.log('Connected!');
    
    // Register a simple add function
    await client.process.register((data) => {
        return data.a + data.b;
    }, 'add');
    
    // Test it
    const result = await client.process.call('example-client:add', { a: 5, b: 3 });
    console.log('5 + 3 =', result.payload.value);
});
```

### Advanced Example

See `test-fixed.js` for a comprehensive example demonstrating:
- Process registration and calling
- Buffer operations
- Cross-process communication
- Event handling
- Error recovery
- Graceful shutdown
