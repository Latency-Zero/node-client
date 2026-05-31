/**
 * LatZero Node.js Client — CommonJS entry
 */
'use strict';

const net = require('net');
const { EventEmitter } = require('events');

class LatZeroBaseClient extends EventEmitter {
    constructor(dsn, pool, options = {}) {
        super();

        const parsed = new URL(dsn);
        if (parsed.protocol !== 'latzero:' || !parsed.hostname)
            throw new Error('DSN must look like latzero://client-id');

        this.clientId   = parsed.hostname;
        this.poolName   = pool;
        this.authToken  = options.authToken || null;
        this.host       = options.host || '127.0.0.1';
        this.port       = options.port || 14130;
        this.timeout    = options.timeout || 5000;

        this.socket        = null;
        this.connected     = false;
        this.pending       = new Map();
        this.eventHandlers = new Map();
        this.messageBuffer = '';
        this._processes    = new Map();
    }

    _connectSocket() {
        return new Promise((resolve, reject) => {
            if (this.connected) { resolve(); return; }

            this.socket = new net.Socket();
            this.socket.connect(this.port, this.host);

            this.socket.on('connect', () => {
                this.connected = true;
                this.sendRequest('hello', null, null)
                    .then(() => this.sendRequest('join_pool', {
                        client_id: this.clientId,
                        pool:      this.poolName,
                        auth_token: this.authToken
                    }))
                    .then(() => { this.emit('connect'); resolve(); })
                    .catch(reject);
            });

            this.socket.on('data', (chunk) => {
                this.messageBuffer += chunk.toString();
                let idx;
                while ((idx = this.messageBuffer.indexOf('\n')) !== -1) {
                    const raw = this.messageBuffer.substring(0, idx);
                    this.messageBuffer = this.messageBuffer.substring(idx + 1);
                    if (raw.trim()) {
                        try { this.handleMessage(JSON.parse(raw)); }
                        catch (e) { console.error('[LatZero] Parse error:', e); }
                    }
                }
            });

            this.socket.on('error', (err) => {
                this.connected = false;
                this.emit('error', err);
                reject(err);
            });

            this.socket.on('close', () => {
                this.connected = false;
                this.emit('disconnect');
            });
        });
    }

    handleMessage(msg) {
        const { type, request_id, payload } = msg;
        if (request_id && this.pending.has(request_id)) {
            const entry = this.pending.get(request_id);
            if (type === 'ack' && (entry.requestType === 'call_process' || entry.requestType === 'call_app')) return;
            if (type === 'ack' || type === 'error' || type === 'app_result') {
                this.pending.delete(request_id);
                if (type === 'error') {
                    const err = new Error(payload?.message || 'Server error');
                    err.code = payload?.code || 'server_error';
                    entry.reject(err);
                } else {
                    entry.resolve({ type, payload });
                }
                return;
            }
        }
        this.handleServerMessage(msg);
    }

    handleServerMessage(msg) {
        switch (msg.type) {
            case 'presence_update': this.emit('presence',     msg.payload); break;
            case 'buffer_update':   this.emit('bufferUpdate', msg.payload); break;
            case 'emit_event':      this._dispatchEvent(msg.payload);       break;
            case 'call_app':        this._handleAppCall(msg);               break;
            case 'call_process':    this._handleProcessCall(msg);           break;
        }
    }

    _dispatchEvent({ event, data }) {
        (this.eventHandlers.get(event) || []).forEach(h => {
            try { h(data); } catch (e) { console.error('[LatZero] Event handler error:', e); }
        });
    }

    async _handleAppCall({ request_id, payload: { event, data } }) {
        const handlers = this.eventHandlers.get(event) || [];
        const reply = (value, error = null) => this.sendMessage({
            type: 'app_result', request_id,
            client_id: this.clientId, pool: this.poolName,
            payload: { value, error }
        });
        if (!handlers.length) {
            reply(null, { type: 'NoHandler', message: `No handler for '${event}'` });
            return;
        }
        try { reply(await Promise.resolve(handlers[0](data))); }
        catch (e) { reply(null, { type: e.constructor.name, message: e.message }); }
    }

    async _handleProcessCall({ request_id, payload: { process_id, data } }) {
        const [clientId, processName] = process_id.split(':');
        const key = `${clientId}:${processName}`;
        const handlers = this.eventHandlers.get(key) || [];
        const reply = (value, error = null) => this.sendMessage({
            type: 'app_result', request_id,
            client_id: this.clientId, pool: this.poolName,
            payload: { value, error }
        });
        if (!handlers.length) {
            reply(null, { type: 'NoHandler', message: `No handler for '${processName}'` });
            return;
        }
        try { reply(await Promise.resolve(handlers[0](data))); }
        catch (e) { reply(null, { type: e.constructor.name, message: e.message }); }
    }

    sendMessage(msg) {
        if (!this.connected || !this.socket) throw new Error('Not connected to server');
        this.socket.write(JSON.stringify(msg) + '\n');
    }

    sendRequest(type, payload, pool = null) {
        return new Promise((resolve, reject) => {
            const requestId = this.generateRequestId();
            this.pending.set(requestId, { resolve, reject, requestType: type });
            try {
                this.sendMessage({
                    type, request_id: requestId,
                    client_id: this.clientId,
                    pool: pool !== undefined ? pool : this.poolName,
                    payload: payload || {}
                });
            } catch (err) {
                this.pending.delete(requestId);
                reject(err);
                return;
            }
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

    ensureJsonable(value) {
        try { JSON.stringify(value); }
        catch { throw new TypeError('Only JSON-serializable values are supported'); }
    }

    on(event, handler) {
        if (!this.eventHandlers.has(event)) this.eventHandlers.set(event, []);
        this.eventHandlers.get(event).push(handler);
        return this;
    }

    off(event, handler) {
        if (!this.eventHandlers.has(event)) return this;
        const list = this.eventHandlers.get(event);
        const i = list.indexOf(handler);
        if (i > -1) list.splice(i, 1);
        if (!list.length) this.eventHandlers.delete(event);
        return this;
    }

    disconnect() {
        if (this.connected) this.sendRequest('leave_pool', {}).catch(() => {});
        if (this.socket) this.socket.end();
        this.connected = false;
    }
}

class LatZeroClient extends LatZeroBaseClient {
    constructor(dsn, pool, options = {}) {
        super(dsn, pool, options);

        this._opQueue   = [];
        this._ready     = false;
        this._connecting = false;

        this.process = this._makeProcessProxy();

        if (options.autoConnect !== false) this._startConnect();
    }

    _startConnect() {
        if (this._connecting || this._ready) return;
        this._connecting = true;
        this._connectSocket()
            .then(() => {
                this._ready = true;
                this._connecting = false;
                const q = this._opQueue.splice(0);
                for (const fn of q) { try { fn(); } catch {} }
            })
            .catch(err => {
                this._connecting = false;
                this.emit('error', err);
            });
    }

    _enqueue(fn) {
        if (this._ready) fn();
        else this._opQueue.push(fn);
    }

    _q(type, payload, pool = null) {
        return new Promise((resolve, reject) => {
            this._enqueue(() => this.sendRequest(type, payload, pool).then(resolve).catch(reject));
        });
    }

    connect(callback) {
        if (callback) {
            if (this._ready) { callback(null); return this; }
            this.once('connect', () => callback(null));
            this.once('error',   (e) => callback(e));
            this._startConnect();
            return this;
        }
        if (this._ready) return Promise.resolve();
        this._startConnect();
        return new Promise((resolve, reject) => {
            this.once('connect', resolve);
            this.once('error', reject);
        });
    }

    _makeProcessProxy() {
        const s = this;
        return {
            register(fn, nameOverride = null, options = {}) {
                const name = nameOverride || fn.name;
                if (!name) throw new Error('Pass an explicit name for anonymous functions.');
                const key = `${s.clientId}:${name}`;
                if (!s.eventHandlers.has(key)) s.eventHandlers.set(key, []);
                s.eventHandlers.get(key).push(fn);
                s._processes.set(name, fn);
                return s._q('register_process', {
                    process_name: name,
                    worker_kind: options.workerKind || "thread",
                    min_workers: options.minWorkers || 1,
                    max_workers: options.maxWorkers || 10,
                });
            },
            unregister(name) {
                s.eventHandlers.delete(`${s.clientId}:${name}`);
                s._processes.delete(name);
                return s._q('unregister_process', { process_name: name });
            },
            call(processId, data = {}, options = {}) {
                s.ensureJsonable(data);
                const ms = options.timeout || s.timeout;
                if (options.responseTo) {
                    return s._q('call_process', {
                        process_id: processId, data,
                        response_to: options.responseTo, timeout: ms / 1000
                    });
                }
                return new Promise((resolve, reject) => {
                    s._enqueue(() => {
                        const reqId = s.generateRequestId();
                        s.pending.set(reqId, { resolve, reject, requestType: 'call_process' });
                        try {
                            s.sendMessage({
                                type: 'call_process', request_id: reqId,
                                client_id: s.clientId, pool: s.poolName,
                                payload: { process_id: processId, data, timeout: ms / 1000 }
                            });
                        } catch (e) { s.pending.delete(reqId); reject(e); return; }
                        setTimeout(() => {
                            if (s.pending.has(reqId)) { s.pending.delete(reqId); reject(new Error('Process call timeout')); }
                        }, ms);
                    });
                });
            },
            broadcast(processName, data = {}, options = {}) {
                s.ensureJsonable(data);
                return s._q('broadcast_process', {
                    process_name: processName, data,
                    response_to: options.responseTo || null,
                    timeout: (options.timeout || s.timeout) / 1000
                }).then(r => r.payload?.invoked_processes || []);
            },
            list(pattern = null) {
                return s._q('list_processes', { pattern }).then(r => r.payload?.processes || {});
            }
        };
    }

    set(key, value, options = {}) {
        this.ensureJsonable(value);
        return this._q('set_buffer', { key, value, ttl: options.autoClean, persistent: options.persistent || false });
    }

    get(key, defaultValue = null) {
        return this._q('get_buffer', { key }).then(r => {
            const p = r.payload || {};
            return p.exists ? (p.entry?.value ?? defaultValue) : defaultValue;
        });
    }

    delete(key) {
        return this._q('delete_buffer', { key }).then(r => !!(r.payload?.deleted));
    }

    exists(key) {
        return this._q('get_buffer', { key }).then(r => !!(r.payload?.exists));
    }

    keys(pattern = null) {
        return this._q('list_buffers', { pattern }).then(r => r.payload?.keys || []);
    }

    values(pattern = null) {
        return this.keys(pattern).then(ks => Promise.all(ks.map(k => this.get(k))));
    }

    items(pattern = null) {
        return this.keys(pattern).then(ks => Promise.all(ks.map(async k => [k, await this.get(k)])));
    }

    mset(data, options = {}) {
        return Promise.all(Object.entries(data).map(([k, v]) => this.set(k, v, options))).then(() => {});
    }

    mget(keys) {
        return Promise.all(keys.map(k => this.get(k))).then(vals => {
            const out = {};
            keys.forEach((k, i) => out[k] = vals[i]);
            return out;
        });
    }

    deleteMany(keys) {
        return Promise.all(keys.map(k => this.delete(k))).then(rs => rs.filter(Boolean).length);
    }

    size() { return this.keys().then(k => k.length); }

    stats() {
        return this.size().then(key_count => ({
            name: this.poolName, client_id: this.clientId,
            server_mode: true, key_count
        }));
    }

    scan(cursor = 0, count = 100) {
        return this.keys().then(ks => {
            const end = Math.min(cursor + count, ks.length);
            return [end < ks.length ? end : 0, ks.slice(cursor, end)];
        });
    }

    subscribe(key)   { return this._q('subscribe_buffer',   { key }); }
    unsubscribe(key) { return this._q('unsubscribe_buffer', { key }); }

    emitEvent(event, options = {}) {
        this.ensureJsonable(options.data || {});
        return this._q('emit_event', {
            event, data: options.data || {},
            target_client_id: options.targetClientId || null,
            response_to: options.responseTo || null
        });
    }

    callEvent(event, options = {}) {
        if (!options.targetClientId) throw new Error('callEvent requires targetClientId');
        const ms = options.timeout || this.timeout;
        return new Promise((resolve, reject) => {
            this._enqueue(() => {
                const reqId = this.generateRequestId();
                this.pending.set(reqId, { resolve, reject, requestType: 'call_app' });
                try {
                    this.sendMessage({
                        type: 'call_app', request_id: reqId,
                        client_id: this.clientId, pool: this.poolName,
                        payload: {
                            target_client_id: options.targetClientId,
                            event, data: options.data || {}, timeout: ms / 1000
                        }
                    });
                } catch (e) { this.pending.delete(reqId); reject(e); return; }
                setTimeout(() => {
                    if (this.pending.has(reqId)) { this.pending.delete(reqId); reject(new Error('Call event timeout')); }
                }, ms);
            });
        });
    }

    switchPool(pool, authToken = null) {
        return this._q('switch_pool', {
            client_id: this.clientId, pool, auth_token: authToken || this.authToken
        }).then(() => { this.poolName = pool; this.authToken = authToken; });
    }
}

class LatZeroAsyncClient extends LatZeroBaseClient {
    constructor(dsn, pool, options = {}) {
        super(dsn, pool, options);

        const s = this;
        this.process = {
            async register(fn, nameOverride = null, options = {}) {
                const name = nameOverride || fn.name;
                if (!name) throw new Error('Pass an explicit name for anonymous functions.');
                const key = `${s.clientId}:${name}`;
                if (!s.eventHandlers.has(key)) s.eventHandlers.set(key, []);
                s.eventHandlers.get(key).push(fn);
                s._processes.set(name, fn);
                await s.sendRequest('register_process', {
                    process_name: name,
                    worker_kind: options.workerKind || "thread",
                    min_workers: options.minWorkers || 1,
                    max_workers: options.maxWorkers || 10,
                });
            },
            async unregister(name) {
                s.eventHandlers.delete(`${s.clientId}:${name}`);
                s._processes.delete(name);
                await s.sendRequest('unregister_process', { process_name: name });
            },
            async call(processId, data = {}, options = {}) {
                s.ensureJsonable(data);
                const ms = options.timeout || s.timeout;
                if (options.responseTo) {
                    return s.sendRequest('call_process', {
                        process_id: processId, data,
                        response_to: options.responseTo, timeout: ms / 1000
                    });
                }
                const reqId = s.generateRequestId();
                return new Promise((resolve, reject) => {
                    s.pending.set(reqId, { resolve, reject, requestType: 'call_process' });
                    try {
                        s.sendMessage({
                            type: 'call_process', request_id: reqId,
                            client_id: s.clientId, pool: s.poolName,
                            payload: { process_id: processId, data, timeout: ms / 1000 }
                        });
                    } catch (e) { s.pending.delete(reqId); reject(e); return; }
                    setTimeout(() => {
                        if (s.pending.has(reqId)) { s.pending.delete(reqId); reject(new Error('Process call timeout')); }
                    }, ms);
                });
            },
            async broadcast(processName, data = {}, options = {}) {
                s.ensureJsonable(data);
                const r = await s.sendRequest('broadcast_process', {
                    process_name: processName, data,
                    response_to: options.responseTo || null,
                    timeout: (options.timeout || s.timeout) / 1000
                });
                return r.payload?.invoked_processes || [];
            },
            async list(pattern = null) {
                const r = await s.sendRequest('list_processes', { pattern });
                return r.payload?.processes || {};
            }
        };

        if (options.autoConnect !== false) this.connect();
    }

    connect() { return this._connectSocket(); }

    async set(key, value, options = {}) {
        this.ensureJsonable(value);
        await this.sendRequest('set_buffer', { key, value, ttl: options.autoClean, persistent: options.persistent || false });
    }

    async get(key, defaultValue = null) {
        const r = await this.sendRequest('get_buffer', { key });
        const p = r.payload || {};
        return p.exists ? (p.entry?.value ?? defaultValue) : defaultValue;
    }

    async delete(key) {
        const r = await this.sendRequest('delete_buffer', { key });
        return !!(r.payload?.deleted);
    }

    async exists(key) {
        const r = await this.sendRequest('get_buffer', { key });
        return !!(r.payload?.exists);
    }

    async keys(pattern = null) {
        const r = await this.sendRequest('list_buffers', { pattern });
        return r.payload?.keys || [];
    }

    async values(pattern = null) {
        return Promise.all((await this.keys(pattern)).map(k => this.get(k)));
    }

    async items(pattern = null) {
        return Promise.all((await this.keys(pattern)).map(async k => [k, await this.get(k)]));
    }

    async mset(data, options = {}) {
        await Promise.all(Object.entries(data).map(([k, v]) => this.set(k, v, options)));
    }

    async mget(keys) {
        const vals = await Promise.all(keys.map(k => this.get(k)));
        const out = {};
        keys.forEach((k, i) => out[k] = vals[i]);
        return out;
    }

    async deleteMany(keys) {
        return (await Promise.all(keys.map(k => this.delete(k)))).filter(Boolean).length;
    }

    async size() { return (await this.keys()).length; }

    async stats() {
        return { name: this.poolName, client_id: this.clientId, server_mode: true, key_count: await this.size() };
    }

    async scan(cursor = 0, count = 100) {
        const ks = await this.keys();
        const end = Math.min(cursor + count, ks.length);
        return [end < ks.length ? end : 0, ks.slice(cursor, end)];
    }

    async subscribe(key)   { await this.sendRequest('subscribe_buffer',   { key }); }
    async unsubscribe(key) { await this.sendRequest('unsubscribe_buffer', { key }); }

    async emitEvent(event, options = {}) {
        this.ensureJsonable(options.data || {});
        await this.sendRequest('emit_event', {
            event, data: options.data || {},
            target_client_id: options.targetClientId || null,
            response_to: options.responseTo || null
        });
    }

    async callEvent(event, options = {}) {
        if (!options.targetClientId) throw new Error('callEvent requires targetClientId');
        const ms = options.timeout || this.timeout;
        const reqId = this.generateRequestId();
        return new Promise((resolve, reject) => {
            this.pending.set(reqId, { resolve, reject, requestType: 'call_app' });
            try {
                this.sendMessage({
                    type: 'call_app', request_id: reqId,
                    client_id: this.clientId, pool: this.poolName,
                    payload: {
                        target_client_id: options.targetClientId,
                        event, data: options.data || {}, timeout: ms / 1000
                    }
                });
            } catch (e) { this.pending.delete(reqId); reject(e); return; }
            setTimeout(() => {
                if (this.pending.has(reqId)) { this.pending.delete(reqId); reject(new Error('Call event timeout')); }
            }, ms);
        });
    }

    async switchPool(pool, authToken = null) {
        await this.sendRequest('switch_pool', {
            client_id: this.clientId, pool, auth_token: authToken || this.authToken
        });
        this.poolName = pool;
        this.authToken = authToken;
    }
}

module.exports = LatZeroClient;
module.exports.LatZeroAsyncClient = LatZeroAsyncClient;
