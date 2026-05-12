/**
 * LatZero Node.js Client (Fixed)
 * 
 * A Node.js client for connecting to LatZero server mode.
 * Supports all core operations: set, get, delete, keys, events, etc.
 * 
 * FIXED: Correct join_pool message structure for server compatibility
 */

import net from 'net';
import { EventEmitter } from 'events';

class LatZeroClient extends EventEmitter {
    constructor(dsn, pool, options = {}) {
        super();
        
        // Parse DSN: latzero://client-id
        const parsed = new URL(dsn);
        if (parsed.protocol !== 'latzero:' || !parsed.hostname) {
            throw new Error('DSN must look like latzero://client-id');
        }
        
        this.clientId = parsed.hostname;
        this.poolName = pool;
        this.authToken = options.authToken || null;
        this.host = options.host || '127.0.0.1';
        this.port = options.port || 14130;
        this.timeout = options.timeout || 5000;
        
        // Connection state
        this.socket = null;
        this.connected = false;
        this.pending = new Map(); // request_id -> resolve/reject
        this.eventHandlers = new Map(); // event -> [handlers]
        this.messageBuffer = '';
        this._processes = new Map(); // short process name -> fn

        // Process pool proxy — all process operations live under client.process.*
        const _self = this;
        this.process = {
            /**
             * Register a function as a named process.
             * Name is inferred from fn.name, or pass an explicit name as second arg.
             *
             *   await client.process.register(myFn);
             *   await client.process.register(myFn, 'override-name');
             *   await client.process.register(x => x, 'square');  // explicit required for anon
             */
            register: async (fn, nameOverride = null) => {
                const name = nameOverride || fn.name;
                if (!name) {
                    throw new Error(
                        'Cannot infer process name from an anonymous function. ' +
                        'Pass an explicit name as the second argument.'
                    );
                }
                // Register under compound key so existing handleAppCall dispatch picks it up
                const compoundKey = `${_self.clientId}:${name}`;
                if (!_self.eventHandlers.has(compoundKey)) {
                    _self.eventHandlers.set(compoundKey, []);
                }
                _self.eventHandlers.get(compoundKey).push(fn);
                _self._processes.set(name, fn);
                await _self.sendRequest('register_process', { process_name: name });
            },

            /**
             * Unregister a process by its short name.
             */
            unregister: async (name) => {
                const compoundKey = `${_self.clientId}:${name}`;
                _self.eventHandlers.delete(compoundKey);
                _self._processes.delete(name);
                await _self.sendRequest('unregister_process', { process_name: name });
            },

            /**
             * Call a process by its full ID (client_id:process_name).
             */
            call: async (processId, data = {}, options = {}) => {
                _self.ensureJsonable(data);
                const timeoutMs = options.timeout || _self.timeout;

                if (options.responseTo) {
                    // Non-blocking: fire, wait for ack only
                    return _self.sendRequest('call_process', {
                        process_id: processId,
                        data,
                        response_to: options.responseTo,
                        timeout: timeoutMs / 1000
                    });
                }

                // Blocking: wait for actual result
                const requestId = _self.generateRequestId();
                return new Promise((resolve, reject) => {
                    _self.pending.set(requestId, { resolve, reject });

                    try {
                        _self.sendMessage({
                            type: 'call_process',
                            request_id: requestId,
                            client_id: _self.clientId,
                            pool: _self.poolName,
                            payload: {
                                process_id: processId,
                                data,
                                timeout: timeoutMs / 1000
                            }
                        });
                    } catch (err) {
                        _self.pending.delete(requestId);
                        reject(err);
                        return;
                    }

                    setTimeout(() => {
                        if (_self.pending.has(requestId)) {
                            _self.pending.delete(requestId);
                            reject(new Error('Process call timeout'));
                        }
                    }, timeoutMs);
                });
            },

            /**
             * Broadcast to all processes registered under the given short name.
             * Returns the list of process_ids that were invoked.
             */
            broadcast: async (processName, data = {}, options = {}) => {
                _self.ensureJsonable(data);
                const response = await _self.sendRequest('broadcast_process', {
                    process_name: processName,
                    data,
                    response_to: options.responseTo || null,
                    timeout: (options.timeout || _self.timeout) / 1000
                });
                return response.payload?.invoked_processes || [];
            },

            /**
             * List all registered processes in the pool.
             * pattern optionally filters by client_id prefix.
             */
            list: async (pattern = null) => {
                const response = await _self.sendRequest('list_processes', { pattern });
                return response.payload?.processes || {};
            },
        };
        
        // Auto-connect if not disabled
        if (options.autoConnect !== false) {
            this.connect();
        }
    }
    
    connect() {
        return new Promise((resolve, reject) => {
            if (this.connected) {
                resolve();
                return;
            }
            
            this.socket = new net.Socket();
            this.socket.connect(this.port, this.host);
            
            this.socket.on('connect', () => {
                console.log(`[LatZero] Connected to TCP server at ${this.host}:${this.port}`);
                this.connected = true;
                
                // Start handshake
                this.sendRequest('hello', null, null)
                    .then(() => this.sendRequest('join_pool', {
                        client_id: this.clientId,
                        pool: this.poolName,
                        auth_token: this.authToken
                    }))
                    .then(() => {
                        this.emit('connect');
                        resolve();
                    })
                    .catch(reject);
            });
            
            this.socket.on('data', (data) => {
                this.messageBuffer += data.toString();
                
                // Process complete messages separated by newlines
                let newlineIndex;
                while ((newlineIndex = this.messageBuffer.indexOf('\n')) !== -1) {
                    const message = this.messageBuffer.substring(0, newlineIndex);
                    this.messageBuffer = this.messageBuffer.substring(newlineIndex + 1);
                    
                    if (message.trim()) {
                        try {
                            const messageObj = JSON.parse(message);
                            this.handleMessage(messageObj);
                        } catch (err) {
                            console.error('[LatZero] Failed to parse message:', message, err);
                        }
                    }
                }
            });
            
            this.socket.on('error', (error) => {
                console.error('[LatZero] Socket error:', error);
                this.connected = false;
                this.emit('error', error);
                reject(error);
            });
            
            this.socket.on('close', () => {
                console.log('[LatZero] Connection closed');
                this.connected = false;
                this.emit('disconnect');
            });
        });
    }
    
    handleMessage(message) {
        const { type, request_id, client_id, pool, payload } = message;
        
        // Handle responses to pending requests
        if (request_id && this.pending.has(request_id)) {
            const { resolve, reject } = this.pending.get(request_id);
            this.pending.delete(request_id);
            
            if (type === 'ack') {
                resolve({ type, payload });
            } else if (type === 'error') {
                const error = new Error(payload?.message || 'Server error');
                error.code = payload?.code || 'server_error';
                reject(error);
            } else if (type === 'app_result') {
                resolve({ type, payload });
            }
        } else {
            // Handle server-sent events
            this.handleServerMessage(message);
        }
    }
    
    handleServerMessage(message) {
        const { type, payload } = message;
        
        switch (type) {
            case 'presence_update':
                this.emit('presence', payload);
                break;
            case 'buffer_update':
                this.emit('bufferUpdate', payload);
                break;
            case 'emit_event':
                this.handleEventMessage(payload);
                break;
            case 'call_app':
                this.handleAppCall(message);
                break;
        }
    }
    
    handleEventMessage(payload) {
        const { event, data } = payload;
        const handlers = this.eventHandlers.get(event) || [];
        handlers.forEach(handler => {
            try {
                handler(data);
            } catch (err) {
                console.error('[LatZero] Event handler error:', err);
            }
        });
    }
    
    async handleAppCall(message) {
        const { request_id, payload } = message;
        const { event, data } = payload;
        
        // Check for handlers - the event should match the compound key format used for process registration
        const handlers = this.eventHandlers.get(event) || [];
        if (handlers.length === 0) {
            this.sendMessage({
                type: 'app_result',
                request_id,
                client_id: this.clientId,
                pool: this.poolName,
                payload: {
                    value: null,
                    error: { type: 'NoHandler', message: `No handler registered for '${event}'` }
                }
            });
            return;
        }
        
        const handler = handlers[0];
        try {
            const result = await Promise.resolve(handler(data));
            this.sendMessage({
                type: 'app_result',
                request_id,
                client_id: this.clientId,
                pool: this.poolName,
                payload: {
                    value: result,
                    error: null
                }
            });
        } catch (err) {
            this.sendMessage({
                type: 'app_result',
                request_id,
                client_id: this.clientId,
                pool: this.poolName,
                payload: {
                    value: null,
                    error: { type: err.constructor.name, message: err.message }
                }
            });
        }
    }
    
    sendMessage(message) {
        if (!this.connected || !this.socket) {
            throw new Error('Not connected to server');
        }
        
        const data = JSON.stringify(message) + '\n';
        this.socket.write(data);
    }
    
    sendRequest(type, payload, pool = null) {
        return new Promise((resolve, reject) => {
            const requestId = this.generateRequestId();
            this.pending.set(requestId, { resolve, reject });
            
            this.sendMessage({
                type,
                request_id: requestId,
                client_id: this.clientId,
                pool: pool || this.poolName,
                payload: payload || {}
            });
            
            // Set timeout
            setTimeout(() => {
                if (this.pending.has(requestId)) {
                    this.pending.delete(requestId);
                    reject(new Error('Request timeout'));
                }
            }, this.timeout);
        });
    }
    
    generateRequestId() {
        return 'req_' + Date.now() + '_' + Math.random().toString(36).substr(2, 9);
    }
    
    // Core API methods
    async set(key, value, options = {}) {
        this.ensureJsonable(value);
        await this.sendRequest('set_buffer', {
            key,
            value,
            ttl: options.autoClean,
            persistent: options.persistent || false
        });
    }
    
    async get(key, defaultValue = null) {
        const response = await this.sendRequest('get_buffer', { key });
        const payload = response.payload || {};
        if (!payload.exists) {
            return defaultValue;
        }
        return payload.entry?.value || defaultValue;
    }
    
    async delete(key) {
        const response = await this.sendRequest('delete_buffer', { key });
        return !!(response.payload?.deleted);
    }
    
    async exists(key) {
        const response = await this.sendRequest('get_buffer', { key });
        return !!(response.payload?.exists);
    }
    
    async keys(pattern = null) {
        const response = await this.sendRequest('list_buffers', { pattern });
        return response.payload?.keys || [];
    }
    
    async values(pattern = null) {
        const keys = await this.keys(pattern);
        const promises = keys.map(key => this.get(key));
        return await Promise.all(promises);
    }
    
    async items(pattern = null) {
        const keys = await this.keys(pattern);
        const promises = keys.map(async key => [key, await this.get(key)]);
        return await Promise.all(promises);
    }
    
    async mset(data, options = {}) {
        const promises = Object.entries(data).map(([key, value]) => 
            this.set(key, value, options)
        );
        await Promise.all(promises);
    }
    
    async mget(keys) {
        const promises = keys.map(key => this.get(key));
        const values = await Promise.all(promises);
        const result = {};
        keys.forEach((key, i) => result[key] = values[i]);
        return result;
    }
    
    async deleteMany(keys) {
        const promises = keys.map(key => this.delete(key));
        const results = await Promise.all(promises);
        return results.filter(Boolean).length;
    }
    
    async size() {
        const keys = await this.keys();
        return keys.length;
    }
    
    async stats() {
        return {
            name: this.poolName,
            client_id: this.clientId,
            server_mode: true,
            key_count: await this.size()
        };
    }
    
    async scan(cursor = 0, count = 100) {
        const keys = await this.keys();
        const end = Math.min(cursor + count, keys.length);
        const nextCursor = end < keys.length ? end : 0;
        return [nextCursor, keys.slice(cursor, end)];
    }
    
    // Subscription methods
    async subscribe(key) {
        await this.sendRequest('subscribe_buffer', { key });
    }
    
    async unsubscribe(key) {
        await this.sendRequest('unsubscribe_buffer', { key });
    }
    
    // Event methods
    on(event, handler) {
        if (!this.eventHandlers.has(event)) {
            this.eventHandlers.set(event, []);
        }
        this.eventHandlers.get(event).push(handler);
        return this;
    }
    
    off(event, handler) {
        if (!this.eventHandlers.has(event)) return this;
        const handlers = this.eventHandlers.get(event);
        const index = handlers.indexOf(handler);
        if (index > -1) {
            handlers.splice(index, 1);
        }
        if (handlers.length === 0) {
            this.eventHandlers.delete(event);
        }
        return this;
    }
    
    async emitEvent(event, options = {}) {
        this.ensureJsonable(options.data || {});
        await this.sendRequest('emit_event', {
            event,
            data: options.data || {},
            target_client_id: options.targetClientId || null,
            response_to: options.responseTo || null
        });
    }
    
    async callEvent(event, options = {}) {
        if (!options.targetClientId) {
            throw new Error('callEvent requires targetClientId');
        }
        
        const requestId = this.generateRequestId();
        await this.sendRequest('call_app', {
            target_client_id: options.targetClientId,
            event,
            data: options.data || {},
            response_to: options.responseTo || null,
            timeout: options.timeout || this.timeout / 1000
        });
        
        // Wait for app_result response
        return new Promise((resolve, reject) => {
            const checkResult = () => {
                const result = Array.from(this.pending.entries()).find(([id, { resolve }]) => 
                    id.startsWith('req_') && resolve.toString().includes(event)
                );
                if (result) {
                    const [id, { resolve: res }] = result;
                    this.pending.delete(id);
                    res(resolve);
                } else {
                    setTimeout(checkResult, 10);
                }
            };
            checkResult();
        });
    }
    
    // Utility methods
    ensureJsonable(value) {
        try {
            JSON.stringify(value);
        } catch (err) {
            throw new TypeError('Server mode only supports JSON-serializable values');
        }
    }
    
    async switchPool(pool, authToken = null) {
        await this.sendRequest('switch_pool', {
            client_id: this.clientId,
            pool,
            auth_token: authToken || this.authToken
        });
        this.poolName = pool;
        this.authToken = authToken;
    }
    
    disconnect() {
        if (this.connected) {
            this.sendRequest('leave_pool', {});
        }
        if (this.socket) {
            this.socket.end();
        }
        this.connected = false;
    }
}

export default LatZeroClient;
