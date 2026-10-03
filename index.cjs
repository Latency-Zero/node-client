'use strict';

/**
 * LatZero Node.js Client
 *
 * Exports:
 *   - LatZeroClient       (default) — sync-style, auto-queuing, no await needed for writes
 *   - LatZeroAsyncClient  (named)   — explicit async/await API
 */

const net = require('net');
const { EventEmitter } = require('events');
const { performance } = require('perf_hooks');
const { StringDecoder } = require('string_decoder');

// ─────────────────────────────────────────────────────────────────────────────
// Internal base — shared TCP logic, message routing, event handling
// ─────────────────────────────────────────────────────────────────────────────

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
        this.timeout    = options.timeout ?? 5000;
        this.maxPendingRequests = options.maxPendingRequests ?? 1024;
        this.maxQueuedBytes = options.maxQueuedBytes ?? 1024 * 1024;
        this.maxFrameBytes = options.maxFrameBytes ?? 1024 * 1024;
        this.maxBatchSize = options.maxBatchSize ?? 256;
        this.maxConcurrentHandlers = options.maxConcurrentHandlers ?? 64;
        this.maxRedirects = options.maxRedirects ?? 4;
        this.allowRedirects = options.allowRedirects ?? true;
        this._timeoutMs(this.timeout);
        for (const name of ['maxPendingRequests', 'maxQueuedBytes', 'maxFrameBytes', 'maxBatchSize', 'maxConcurrentHandlers']) {
            if (!Number.isSafeInteger(this[name]) || this[name] <= 0) throw new TypeError(`${name} must be a positive integer`);
        }
        if (!Number.isSafeInteger(this.maxRedirects) || this.maxRedirects < 0 || this.maxRedirects > 16) throw new TypeError('maxRedirects must be an integer from 0 to 16');
        if (typeof this.allowRedirects !== 'boolean') throw new TypeError('allowRedirects must be a boolean');

        this.socket        = null;
        this.connected     = false;
        this._socketConnected = false;
        this._endpoint = null;
        this.pending       = new Map();
        this.messageBuffer = '';
        this._processes    = new Map();
        this._registrations = new Map();
        this._opQueue = [];
        this._outbox = [];
        this._queuedBytes = 0;
        this._pendingUsers = 0;
        this._ready = false;
        this._connecting = false;
        this._connectionPromise = null;
        this._connectionReject = null;
        this._connectionFlow = null;
        this._connectTimer = null;
        this._drainTimer = null;
        this._writeBlocked = false;
        this._generation = 0;
        this._requestSequence = 0;
        this._activeHandlers = 0;
        this._switching = false;
        this._queueBeforeReady = false;
    }

    // ── Socket ────────────────────────────────────────────────────────────────

    _createSocket() { return new net.Socket(); }

    get endpoint() { return this._endpoint; }

    _connectSocket() {
        if (this._connectionPromise) return this._connectionPromise;
        const flow = this._redirectFlow(this.poolName, this.authToken, performance.now() + this.timeout, 'connect');
        const promise = this._beginConnection(flow);
        this._openSocket({ host: this.host, port: this.port }, flow);
        return promise;
    }

    _beginConnection(flow) {
        this._connectionPromise = new Promise((resolve, reject) => {
            flow.resolve = resolve;
            this._connectionReject = reject;
        });
        const promise = this._connectionPromise;
        // Auto-connect may have no caller yet; explicit connect still receives rejection.
        promise.catch(() => {});
        this._connectionFlow = flow;
        this._connecting = true;
        clearTimeout(this._connectTimer);
        this._connectTimer = setTimeout(() => {
            if (this._connectionFlow === flow) this._closeConnection(this._error(flow.kind === 'connect' ? 'Connection timeout' : 'Request timeout', 'timeout'), true);
        }, Math.max(1, flow.deadline - performance.now()));
        return promise;
    }

    _openSocket(endpoint, flow) {
        if (this._connectionFlow !== flow) return;
        if (flow.deadline <= performance.now()) {
            this._closeConnection(this._error(flow.kind === 'connect' ? 'Connection timeout' : 'Request timeout', 'timeout'), true);
            return;
        }
        let socket;
        try { socket = this._createSocket(); }
        catch (err) { this._closeConnection(err, true); return; }
        const decoder = new StringDecoder('utf8');
        this.socket = socket;
        this._endpoint = Object.freeze({ host: endpoint.host, port: endpoint.port });
        this._socketConnected = false;
        this.messageBuffer = '';

        socket.on('connect', () => {
            if (this.socket !== socket) return;
            const onFailure = err => { if (this.socket === socket) this._closeConnection(err, true); };
            this._socketConnected = true;
            this._request('hello', { capabilities: ['pool_redirect_v1'] }, null, {
                handshake: true, deadline: flow.deadline, clientId: flow.clientId, onFailure
            }, false)
                .then(() => {
                    if (this.socket !== socket) throw this._error('Connection closed', 'connection_lost');
                    return this._request('join_pool', {
                        client_id: flow.clientId, pool: flow.pool, auth_token: flow.token
                    }, null, {
                        handshake: true, deadline: flow.deadline, clientId: flow.clientId, redirectFlow: flow, onFailure,
                        onSuccess: result => this._joined(socket, flow, result)
                    }, false);
                })
                .catch(err => {
                    if (this.socket === socket) this._closeConnection(err, true);
                });
        });
        socket.on('data', chunk => {
            if (this.socket !== socket) return;
            this.messageBuffer += decoder.write(chunk);
            let idx;
            while (this.socket === socket && (idx = this.messageBuffer.indexOf('\n')) !== -1) {
                const raw = this.messageBuffer.slice(0, idx);
                this.messageBuffer = this.messageBuffer.slice(idx + 1);
                if (Buffer.byteLength(raw) + 1 > this.maxFrameBytes) {
                    this._closeConnection(this._error('Incoming frame exceeds maxFrameBytes', 'frame_too_large'), true);
                    return;
                }
                if (!raw.trim()) continue;
                let msg;
                try {
                    msg = JSON.parse(raw);
                    if (!msg || Array.isArray(msg) || typeof msg !== 'object' || typeof msg.type !== 'string') throw new Error('Invalid message envelope');
                } catch (err) {
                    this._closeConnection(this._error(`Invalid server frame: ${err.message}`, 'protocol_error'), true);
                    return;
                }
                try { this.handleMessage(msg); }
                catch (err) { this._reportError(err); }
            }
            if (this.socket === socket && Buffer.byteLength(this.messageBuffer) > this.maxFrameBytes) {
                this._closeConnection(this._error('Incoming frame exceeds maxFrameBytes', 'frame_too_large'), true);
            }
        });
        socket.on('drain', () => {
            if (this.socket !== socket) return;
            this._writeBlocked = false;
            clearTimeout(this._drainTimer);
            this._drainTimer = null;
            this._flushWrites();
        });
        socket.on('error', err => {
            if (this.socket === socket) this._closeConnection(err, true);
        });
        socket.on('close', () => {
            if (this.socket === socket) this._closeConnection(this._error('Connection closed', 'connection_lost'));
        });
        try { socket.connect(endpoint.port, endpoint.host); }
        catch (err) { this._closeConnection(err, true); }
    }

    _joined(socket, flow, result) {
        if (this.socket !== socket || this._connectionFlow !== flow) return;
        this._ready = this.connected = true;
        this._connecting = false;
        if (flow.kind === 'switch') {
            this.poolName = flow.pool;
            this.authToken = flow.token;
            this._switching = false;
        }
        clearTimeout(this._connectTimer);
        this._connectTimer = null;
        this._outbox = this._outbox.concat(this._opQueue);
        this._opQueue = [];
        this._flushWrites();
        if (this.socket !== socket || !this._ready || this._connectionFlow !== flow) return;
        this._connectionReject = this._connectionFlow = null;
        const transition = flow.transitionEntry;
        flow.transitionEntry = null;
        try { transition?.onSuccess?.(result); }
        finally {
            flow.resolve(flow.kind === 'connect' ? undefined : result);
            this.emit('connect');
        }
    }

    _redirectFlow(pool, token, deadline, kind) {
        const endpoint = this._endpoint || { host: this.host, port: this.port };
        const visited = new Set([this._endpointKey(endpoint.host, endpoint.port)]);
        if (typeof endpoint.host === 'string' && endpoint.host.toLowerCase() === 'localhost') {
            visited.add(this._endpointKey('::1', endpoint.port));
        }
        return { clientId: this.clientId, pool, token, deadline, kind, entryHost: this.host, visited, pods: new Set(), hops: 0, metadata: null };
    }

    _loopbackHost(host) {
        if (typeof host !== 'string' || host.includes('%')) return null;
        if (net.isIP(host) === 4) return host.startsWith('127.') ? host : null;
        if (net.isIP(host) === 6) {
            const normalized = new URL(`http://[${host}]/`).hostname.slice(1, -1);
            return normalized === '::1' ? normalized : null;
        }
        return null;
    }

    _endpointKey(host, port) {
        return `${this._loopbackHost(host) || (typeof host === 'string' && host.toLowerCase() === 'localhost' ? '127.0.0.1' : host)}|${port}`;
    }

    _redirectTarget(msg, entry) {
        const flow = entry.redirectFlow;
        const fail = (message, code = 'invalid_redirect') => { throw this._error(message, code); };
        if (!this.allowRedirects || this.maxRedirects === 0) fail('Pool owner redirect required but redirects are disabled', 'redirect_required');
        if (!flow || msg.client_id !== flow.clientId || typeof flow.pool !== 'string' || !flow.pool || msg.pool !== flow.pool) fail('Redirect identity or pool does not match the request');
        if (!this._loopbackHost(flow.entryHost) && !(typeof flow.entryHost === 'string' && flow.entryHost.toLowerCase() === 'localhost')) fail('Redirects require a loopback or localhost entry host', 'unsafe_redirect');
        const payload = msg.payload;
        const port = value => Number.isSafeInteger(value) && value > 0 && value <= 65535;
        const wsPort = value => value === null || port(value);
        if (!payload || Array.isArray(payload) || typeof payload !== 'object' || payload.protocol !== 'pool_redirect_v1' || payload.pool !== flow.pool) fail('Invalid redirect protocol or target pool');
        const host = this._loopbackHost(payload.host);
        const routerHost = this._loopbackHost(payload.router_host);
        if (!host || !routerHost) fail('Redirect hosts must be numeric loopback addresses', 'unsafe_redirect');
        if (!port(payload.port) || !wsPort(payload.ws_port) || !port(payload.router_port) || !wsPort(payload.router_ws_port)) fail('Invalid redirect port');
        if (!Number.isSafeInteger(payload.pod_count) || payload.pod_count < 1 || payload.pod_count > 64 || !Number.isSafeInteger(payload.pod_index) || payload.pod_index < 0 || payload.pod_index >= payload.pod_count) fail('Invalid redirect pod index or count');
        if (typeof payload.cluster_id !== 'string' || !payload.cluster_id.trim()) fail('Invalid redirect cluster ID');
        const metadata = JSON.stringify([payload.cluster_id, payload.pod_count, routerHost, payload.router_port, payload.router_ws_port]);
        if (flow.metadata !== null && flow.metadata !== metadata) fail('Redirect cluster or router changed during the operation');
        const key = this._endpointKey(host, payload.port);
        if (flow.visited.has(key) || flow.pods.has(payload.pod_index)) fail('Pool redirect loop', 'redirect_loop');
        if (flow.hops >= this.maxRedirects) fail('Pool redirect hop limit exceeded', 'redirect_limit');
        return { host, port: payload.port, key, pod: payload.pod_index, metadata };
    }

    _followRedirect(target, entry) {
        const flow = entry.redirectFlow;
        flow.hops++;
        flow.visited.add(target.key);
        flow.pods.add(target.pod);
        flow.metadata = target.metadata;
        if (flow.kind === 'switch' && this._connectionFlow !== flow) {
            flow.transitionEntry = entry;
            this._beginConnection(flow);
        }
        const promise = this._connectionPromise;
        const socket = this.socket;
        const wasReady = this._ready;
        // Initial hops share one work generation; a registered owner replacement fences it.
        if (flow.kind === 'switch') this._generation++;
        this.socket = this._endpoint = null;
        this._socketConnected = this.connected = this._ready = false;
        this._writeBlocked = false;
        clearTimeout(this._drainTimer);
        this._drainTimer = null;
        this.messageBuffer = '';
        const err = this._error('Transport replaced by pool owner redirect; work was not replayed', 'connection_replaced');
        for (const id of [...this.pending.keys()]) this._settle(id, err);
        for (const frame of [...this._outbox, ...this._opQueue]) clearTimeout(frame.timer);
        this._outbox = [];
        this._opQueue = [];
        this._queuedBytes = 0;
        this._processes.clear();
        this._registrations.clear();
        this._switching = flow.kind === 'switch';
        socket?.destroy();
        if (wasReady) this.emit('disconnect');
        this._openSocket(target, flow);
        return promise;
    }

    _closeConnection(err, report = false) {
        const socket = this.socket;
        this.socket = null;
        this._endpoint = null;
        this._socketConnected = false;
        this.connected = false;
        this._ready = false;
        this._connecting = false;
        this._switching = false;
        this._generation++;
        clearTimeout(this._connectTimer);
        clearTimeout(this._drainTimer);
        this._connectTimer = this._drainTimer = null;
        this._writeBlocked = false;
        const rejectConnection = this._connectionReject;
        this._connectionReject = this._connectionPromise = null;
        this._connectionFlow = null;
        this._processes.clear();
        this._registrations.clear();
        for (const id of [...this.pending.keys()]) this._settle(id, err);
        for (const frame of [...this._outbox, ...this._opQueue]) clearTimeout(frame.timer);
        this._outbox = [];
        this._opQueue = [];
        this._queuedBytes = 0;
        this.messageBuffer = '';
        rejectConnection?.(err);
        socket?.destroy();
        if (socket) this.emit('disconnect');
        if (report) this._reportError(err);
    }

    _reportError(err) {
        if (this.listenerCount('error')) this.emit('error', err);
    }

    // ── Message routing ───────────────────────────────────────────────────────

    handleMessage(msg) {
        const { type, request_id, payload } = msg;
        const entry = this.pending.get(request_id);
        if (entry?.sent && entry.generation === this._generation) {
            if (['ack', 'error', 'app_result', 'redirect'].includes(type) && entry.deadline <= performance.now()) {
                this._settle(request_id, this._error(entry.timeoutMessage, 'timeout'));
                this.handleServerMessage(msg);
                return;
            }
            if (type === 'error') {
                const err = this._error(payload?.message || 'Server error', payload?.code || 'server_error');
                err.protocol = true;
                this._settle(request_id, err);
                return;
            }
            if (type === 'redirect') {
                if (!['join_pool', 'switch_pool'].includes(entry.requestType)) return;
                let target;
                try { target = this._redirectTarget(msg, entry); }
                catch (err) {
                    const socket = this.socket;
                    this._settle(request_id, err);
                    if (socket && this.socket === socket) this._closeConnection(err, true);
                    return;
                }
                this._settle(request_id, null, { ...msg }, target);
                return;
            }
            if (type === 'ack') {
                if (entry.kind === 'result') return;
                // Legacy self-invocations can share an ID with the callee's delivery ACK.
                if (entry.kind === 'acceptance' && payload?.delivered && !payload?.queued && !payload?.accepted) return;
                this._settle(request_id, null, { ...msg });
                return;
            }
            if (type === 'app_result' && entry.kind === 'result') {
                this._settle(request_id, null, { ...msg });
                return;
            }
        }
        this.handleServerMessage(msg);
    }

    handleServerMessage(msg) {
        if (msg.pool != null && msg.pool !== this.poolName) return;
        switch (msg.type) {
            case 'presence_update': this.emit('presence',     msg.payload); break;
            case 'buffer_update':   this.emit('bufferUpdate', msg.payload); break;
            case 'emit_event':      this._dispatchEvent(msg.payload);       break;
            case 'call_app':        this._handleAppCall(msg);               break;
            case 'call_process':    this._handleProcessCall(msg);           break;
            case 'app_result':      this.emit('app_result', msg);            break;
        }
    }

    _dispatchEvent({ event, data }) {
        this.emit(event, data);
    }

    _handleAppCall(msg) { return this._handleCall(msg, msg.payload?.event); }

    _handleProcessCall(msg) { return this._handleCall(msg, msg.payload?.process_id); }

    async _handleCall(msg, event) {
        const context = { socket: this.socket, generation: this._generation, pool: msg.pool ?? this.poolName };
        if (!context.socket || !this._contextIsCurrent(context)) return;
        const prefix = `${this.clientId}:`;
        const processHandler = typeof event === 'string' && event.startsWith(prefix) ? this._processes.get(event.slice(prefix.length)) : null;
        // rawListeners preserves EventEmitter's once wrappers and existing on(event, fn) RPC usage.
        const handler = processHandler || (typeof event === 'string' ? this.rawListeners(event)[0] : null);
        let payload;
        if (this._activeHandlers >= this.maxConcurrentHandlers) {
            payload = { value: null, error: 'Client handler limit reached' };
        } else if (!handler) {
            payload = { value: null, error: { type: 'NoHandler', message: `No handler for '${event}'` } };
        } else {
            this._activeHandlers++;
            try {
                const value = await handler.call(this, msg.payload?.data);
                try {
                    // Capture JSON once, so custom toJSON cannot change between validation and reply.
                    const encoded = JSON.stringify(value === undefined ? null : value);
                    if (encoded === undefined) throw new TypeError('Only JSON-serializable values are supported');
                    if (Buffer.byteLength(encoded) > this.maxFrameBytes) throw this._error('Handler result exceeds maxFrameBytes', 'frame_too_large');
                    payload = { value: JSON.parse(encoded), error: null };
                } catch (err) {
                    payload = { value: null, error: this._handlerError(err).message };
                }
            } catch (err) {
                payload = { value: null, error: this._handlerError(err) };
            } finally {
                this._activeHandlers--;
            }
        }
        if (!this._contextIsCurrent(context)) return;
        const reply = { type: 'app_result', request_id: msg.request_id, client_id: this.clientId, pool: context.pool, payload };
        try {
            let frame;
            try { frame = this._encode(reply); }
            catch (err) {
                reply.payload = { value: null, error: this._handlerError(err).message };
                frame = this._encode(reply);
            }
            this._queueFrame(frame, null, context);
        } catch (err) {
            if (this._contextIsCurrent(context)) this._closeConnection(err, true);
        }
    }

    _contextIsCurrent(context) {
        return this._ready && !this._switching && this.socket === context.socket && this._generation === context.generation && this.poolName === context.pool;
    }

    // ── Transport ─────────────────────────────────────────────────────────────

    sendMessage(msg) {
        if (!this._ready || !this.connected || !this.socket) throw new Error('Not connected to server');
        if (this._switching) throw this._error('Pool switch in progress', 'pool_switching');
        this._queueFrame(this._encode(msg), null, { socket: this.socket, generation: this._generation, pool: this.poolName });
    }

    sendRequest(type, payload, pool = null, options = {}) {
        return this._request(type, payload, pool, options, false);
    }

    _q(type, payload, pool = null, options = {}) {
        return this._request(type, payload, pool, options, this._queueBeforeReady);
    }

    _request(type, payload, pool, options, queue) {
        return new Promise((resolve, reject) => {
            let entry;
            try {
                const rpc = type === 'call_app' || type === 'call_process';
                const wireTimed = rpc || type === 'broadcast_process';
                if (wireTimed && payload?.timeout != null && (typeof payload.timeout !== 'number' || !Number.isFinite(payload.timeout) || payload.timeout <= 0)) throw new TypeError('Wire timeout must be positive finite seconds');
                const ms = this._timeoutMs(options.timeout ?? (wireTimed && payload?.timeout != null ? payload.timeout * 1000 : this.timeout));
                const deadline = options.deadline ?? performance.now() + ms;
                if (this._switching && !options.allowSwitch && !options.handshake) throw this._error('Pool switch in progress', 'pool_switching');
                if (!options.handshake && !this._ready && !queue) throw this._error('Not connected to server', 'not_connected');
                if (options.handshake && !this._socketConnected) throw this._error('Not connected to server', 'not_connected');
                if (!options.handshake && this._pendingUsers >= this.maxPendingRequests) throw this._error('Outstanding request limit reached', 'client_overloaded');
                const requestId = this.generateRequestId();
                const kind = rpc ? (payload?.response_to && payload.response_to !== this.clientId ? 'acceptance' : 'result') : 'ack';
                entry = {
                    requestId, requestType: type, kind, resolve, reject, deadline,
                    generation: this._generation, handshake: !!options.handshake,
                    onSuccess: options.onSuccess, onFailure: options.onFailure,
                    redirectFlow: options.redirectFlow,
                    timeoutMessage: options.timeoutMessage || (type === 'call_process' ? 'Process call timeout' : type === 'call_app' ? 'Call event timeout' : 'Request timeout'),
                    sent: false, frame: null, timer: null
                };
                const frame = this._encode({
                    type, request_id: requestId, client_id: options.clientId ?? this.clientId,
                    pool: pool !== undefined ? pool : this.poolName, payload: payload || {}
                });
                if (['join_pool', 'switch_pool'].includes(type) && !entry.redirectFlow) {
                    const encodedPayload = JSON.parse(frame).payload;
                    entry.redirectFlow = this._redirectFlow(encodedPayload?.pool, encodedPayload?.auth_token ?? null, deadline, 'switch');
                }
                this.pending.set(requestId, entry);
                if (!entry.handshake) this._pendingUsers++;
                const remaining = deadline - performance.now();
                if (remaining <= 0) throw this._error(entry.timeoutMessage, 'timeout');
                entry.timer = setTimeout(() => this._settle(requestId, this._error(entry.timeoutMessage, 'timeout')), remaining);
                this._queueFrame(frame, entry);
            } catch (err) {
                if (entry && this.pending.has(entry.requestId)) this._settle(entry.requestId, err);
                else { options.onFailure?.(err); reject(err); }
            }
        });
    }

    _encode(msg) {
        const encoded = JSON.stringify(msg);
        if (encoded === undefined) throw new TypeError('Only JSON-serializable values are supported');
        const frame = encoded + '\n';
        if (Buffer.byteLength(frame) > this.maxFrameBytes) throw this._error('Outgoing frame exceeds maxFrameBytes', 'frame_too_large');
        return frame;
    }

    _queueFrame(data, entry, context = null) {
        const bytes = Buffer.byteLength(data);
        // Two handshake requests have a fixed, bounded reserve so pre-connect work cannot starve joining.
        const limit = this.maxQueuedBytes + (entry?.handshake ? 16384 : 0);
        if (this._queuedBytes + (this.socket?.writableLength || 0) + bytes > limit) throw this._error('Transport byte limit reached', 'client_overloaded');
        const frame = {
            data, bytes, entry, context, deadline: entry?.deadline ?? performance.now() + this.timeout, timer: null
        };
        this._queuedBytes += bytes;
        if (entry) entry.frame = frame;
        else frame.timer = setTimeout(() => {
            if (this._outbox.includes(frame)) this._closeConnection(this._error('Send timeout', 'timeout'), true);
        }, this.timeout);
        if (entry && !entry.handshake && !this._ready) this._opQueue.push(frame);
        else this._outbox.push(frame);
        this._flushWrites();
    }

    _flushWrites() {
        while (this._socketConnected && this.socket && !this._writeBlocked && this._outbox.length) {
            const frame = this._outbox[0];
            const entry = frame.entry;
            if (frame.deadline <= performance.now()) {
                if (entry) { this._settle(entry.requestId, this._error(entry.timeoutMessage, 'timeout')); continue; }
                this._closeConnection(this._error('Send timeout', 'timeout'), true);
                return;
            }
            if ((entry && entry.generation !== this._generation) || (frame.context && !this._contextIsCurrent(frame.context))) {
                if (entry) this._settle(entry.requestId, this._error('Stale request generation', 'connection_lost'));
                else this._removeFrame(frame);
                continue;
            }
            if (entry && ['call_app', 'call_process', 'broadcast_process'].includes(entry.requestType)) {
                let data;
                try {
                    const msg = JSON.parse(frame.data);
                    if (!msg.payload || typeof msg.payload !== 'object' || Array.isArray(msg.payload)) throw new TypeError('RPC payload must be a JSON object');
                    msg.payload.timeout = (entry.deadline - performance.now()) / 1000;
                    data = this._encode(msg);
                }
                catch (err) { this._settle(entry.requestId, err); continue; }
                const bytes = Buffer.byteLength(data);
                if (this._queuedBytes - frame.bytes + bytes + (this.socket.writableLength || 0) > this.maxQueuedBytes) {
                    this._settle(entry.requestId, this._error('Transport byte limit reached', 'client_overloaded'));
                    continue;
                }
                this._queuedBytes += bytes - frame.bytes;
                frame.data = data;
                frame.bytes = bytes;
            }
            if (frame.deadline <= performance.now()) {
                if (entry) { this._settle(entry.requestId, this._error(entry.timeoutMessage, 'timeout')); continue; }
                this._closeConnection(this._error('Send timeout', 'timeout'), true);
                return;
            }
            this._removeFrame(frame);
            if (entry) entry.sent = true;
            const socket = this.socket;
            try {
                // false means accepted by Node, not rejected: never re-write this frame.
                if (!socket.write(frame.data) && this.socket === socket) {
                    this._writeBlocked = true;
                    this._drainTimer = setTimeout(() => {
                        if (this.socket === socket && this._writeBlocked) this._closeConnection(this._error('Send timeout', 'timeout'), true);
                    }, Math.max(1, frame.deadline - performance.now()));
                }
            } catch (err) {
                this._closeConnection(err, true);
                return;
            }
        }
    }

    _removeFrame(frame) {
        for (const queue of [this._outbox, this._opQueue]) {
            const idx = queue.indexOf(frame);
            if (idx !== -1) {
                queue.splice(idx, 1);
                this._queuedBytes -= frame.bytes;
                break;
            }
        }
        clearTimeout(frame.timer);
        if (frame.entry) frame.entry.frame = null;
    }

    _settle(requestId, err, result, redirect = null) {
        const entry = this.pending.get(requestId);
        if (!entry) return;
        this.pending.delete(requestId);
        if (!entry.handshake) this._pendingUsers--;
        clearTimeout(entry.timer);
        entry.timer = null;
        if (entry.frame) this._removeFrame(entry.frame);
        if (err) {
            try { entry.onFailure?.(err, entry); }
            finally { entry.reject(err); }
        } else if (redirect) {
            const flow = entry.redirectFlow;
            try {
                const promise = this._followRedirect(redirect, entry);
                entry.resolve(promise.catch(failure => { entry.onFailure?.(failure, entry); throw failure; }));
            } catch (failure) {
                try { entry.onFailure?.(failure, entry); }
                finally {
                    entry.reject(failure);
                    if (this._connectionFlow === flow) this._closeConnection(failure, true);
                }
            }
        } else {
            try { entry.onSuccess?.(result); }
            finally { entry.resolve(result); }
        }
    }

    _error(message, code) { return Object.assign(new Error(message), { code }); }

    _handlerError(err) {
        let type = 'Error';
        let message = 'Handler failed';
        try { if (typeof err?.constructor?.name === 'string') type = err.constructor.name; } catch {}
        try { message = String(err?.message || err); } catch {}
        return { type, message };
    }

    _timeoutMs(ms) {
        if (typeof ms !== 'number' || !Number.isFinite(ms) || ms <= 0 || ms > 2147483647) throw new TypeError('timeout must be positive finite milliseconds (at most 2147483647)');
        return ms;
    }

    _ttlSeconds(ms) {
        if (ms == null) return ms;
        if (typeof ms !== 'number' || !Number.isFinite(ms) || ms < 0) throw new TypeError('autoClean must be nonnegative finite milliseconds');
        return ms / 1000;
    }

    _checkBatch(length) {
        if (length > this.maxBatchSize || length + this._pendingUsers > this.maxPendingRequests) throw this._error('Batch request limit reached', 'client_overloaded');
    }

    _registerProcess(fn, nameOverride = null, options = {}) {
        if (typeof fn !== 'function') throw new TypeError('Process handler must be a function');
        const name = nameOverride || fn.name;
        if (!name) throw new Error('Pass an explicit name for anonymous functions.');
        const min = options.minWorkers ?? 1;
        const max = options.maxWorkers ?? 10;
        if (!Number.isSafeInteger(min) || !Number.isSafeInteger(max) || min <= 0 || max < min) throw new TypeError('Worker bounds must be positive integers with minWorkers <= maxWorkers');
        return this._changeRegistration(name, fn, 'register_process', {
            process_name: name, worker_kind: options.workerKind || 'thread', min_workers: min, max_workers: max
        });
    }

    _changeRegistration(name, fn, type, payload) {
        const state = { fn, status: 'pending', prior: this._registrations.get(name) };
        this._registrations.set(name, state);
        this._applyRegistration(name);
        return this._q(type, payload, null, {
            onSuccess: () => {
                state.status = 'accepted';
                state.prior = null;
                this._applyRegistration(name);
            },
            onFailure: () => {
                state.status = 'failed';
                this._applyRegistration(name);
            }
        });
    }

    _applyRegistration(name) {
        let state = this._registrations.get(name);
        while (state?.status === 'failed') state = state.prior;
        let current = state;
        while (current?.prior) {
            if (current.prior.status === 'failed') current.prior = current.prior.prior;
            else current = current.prior;
        }
        if (state && (state.fn || state.status === 'pending')) this._registrations.set(name, state);
        else this._registrations.delete(name);
        if (state?.fn) this._processes.set(name, state.fn);
        else this._processes.delete(name);
    }

    _makeProcessProxy() {
        const s = this;
        return {
            register(fn, nameOverride = null, options = {}) { return s._registerProcess(fn, nameOverride, options); },
            unregister(name) { return s._changeRegistration(name, null, 'unregister_process', { process_name: name }); },
            call(processId, data = {}, options = {}) {
                const ms = s._timeoutMs(options.timeout ?? s.timeout);
                const deadline = performance.now() + ms;
                s.ensureJsonable(data);
                return s._q('call_process', {
                    process_id: processId, data, response_to: options.responseTo || null, timeout: ms / 1000
                }, null, { timeout: ms, deadline });
            },
            broadcast(processName, data = {}, options = {}) {
                const ms = s._timeoutMs(options.timeout ?? s.timeout);
                const deadline = performance.now() + ms;
                s.ensureJsonable(data);
                return s._q('broadcast_process', {
                    process_name: processName, data, response_to: options.responseTo || null, timeout: ms / 1000
                }, null, { timeout: ms, deadline }).then(r => r.payload?.invoked_processes || r.payload?.targets || []);
            },
            list(pattern = null) { return s._q('list_processes', { pattern }).then(r => r.payload?.processes || {}); }
        };
    }

    _callEvent(event, options) {
        if (!options.targetClientId) throw new Error('callEvent requires targetClientId');
        const ms = this._timeoutMs(options.timeout ?? this.timeout);
        const deadline = performance.now() + ms;
        const data = options.data ?? {};
        this.ensureJsonable(data);
        return this._q('call_app', {
            target_client_id: options.targetClientId, event, data,
            response_to: options.responseTo || null, timeout: ms / 1000
        }, null, { timeout: ms, deadline });
    }

    _switchPool(pool, authToken) {
        if (this._switching) return Promise.reject(this._error('Pool switch in progress', 'pool_switching'));
        const token = authToken || this.authToken;
        const deadline = performance.now() + this.timeout;
        const redirectFlow = this._redirectFlow(pool, token, deadline, 'switch');
        if (pool === this.poolName) {
            return this._q('switch_pool', { client_id: this.clientId, pool, auth_token: token }, null, {
                deadline, redirectFlow,
                onSuccess: () => { this.authToken = token; }
            });
        }
        this._switching = true;
        const err = this._error('Pool changed; old work was cancelled', 'pool_changed');
        for (const id of [...this.pending.keys()]) {
            if (!this.pending.get(id).handshake) this._settle(id, err);
        }
        const oldProcesses = this._processes;
        const oldRegistrations = this._registrations;
        this._processes = new Map();
        this._registrations = new Map();
        this._generation++;
        for (const entry of this.pending.values()) {
            if (entry.handshake) entry.generation = this._generation;
        }
        for (const frame of [...this._outbox]) {
            if (!frame.entry) this._removeFrame(frame);
        }
        const generation = this._generation;
        return this._q('switch_pool', { client_id: this.clientId, pool, auth_token: token }, null, {
            allowSwitch: true, deadline, redirectFlow,
            onSuccess: () => {
                this.poolName = pool;
                this.authToken = token;
                this._switching = false;
            },
            onFailure: (failure, entry) => {
                if (this._generation !== generation) return;
                this._switching = false;
                this._processes = oldProcesses;
                this._registrations = oldRegistrations;
                // A transmitted switch with no definitive ACK leaves membership uncertain.
                if (!failure.protocol && failure.code === 'timeout' && entry?.sent && this.connected) this._closeConnection(failure);
            }
        });
    }

    generateRequestId() {
        return `req_${Date.now()}_${++this._requestSequence}_${Math.random().toString(36).slice(2, 11)}`;
    }

    ensureJsonable(value) {
        try { if (JSON.stringify(value) === undefined) throw new TypeError(); }
        catch { throw new TypeError('Only JSON-serializable values are supported'); }
    }

    disconnect() {
        this._closeConnection(this._error('Client disconnected', 'disconnected'));
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// LatZeroClient — sync-style default export
//
// All ops queue automatically before the connection is established.
// Writes are fire-and-forget (return Promises you can ignore).
// Reads return Promises you can .then() or await.
// ─────────────────────────────────────────────────────────────────────────────

class LatZeroClient extends LatZeroBaseClient {
    constructor(dsn, pool, options = {}) {
        super(dsn, pool, options);

        this._queueBeforeReady = true;
        this.process = this._makeProcessProxy();

        if (options.autoConnect !== false) this._startConnect();
    }

    // ── Queue engine ──────────────────────────────────────────────────────────

    _startConnect() {
        return this._connectSocket();
    }

    // ── connect() ─ optional callback or Promise ──────────────────────────────

    connect(callback) {
        const promise = this._startConnect();
        if (!callback) return promise;
        const callbackPromise = promise.then(() => { callback(null); }, err => { callback(err); throw err; });
        callbackPromise.catch(() => {});
        return callbackPromise;
    }

    // ── Buffer ops ────────────────────────────────────────────────────────────

    set(key, value, options = {}) {
        this.ensureJsonable(value);
        return this._q('set_buffer', { key, value, ttl: this._ttlSeconds(options.autoClean), persistent: options.persistent || false });
    }

    get(key, defaultValue = null) {
        return this._q('get_buffer', { key }).then(r => {
            const p = r.payload || {};
            return p.exists && p.entry != null ? p.entry.value : defaultValue;
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

    clients() {
        return this._q('list_clients', {}).then(r => r.payload?.clients || []);
    }

    values(pattern = null) {
        return this.keys(pattern).then(ks => { this._checkBatch(ks.length); return Promise.all(ks.map(k => this.get(k))); });
    }

    items(pattern = null) {
        return this.keys(pattern).then(ks => { this._checkBatch(ks.length); return Promise.all(ks.map(async k => [k, await this.get(k)])); });
    }

    mset(data, options = {}) {
        const entries = Object.entries(data);
        this._checkBatch(entries.length);
        for (const [, value] of entries) this.ensureJsonable(value);
        this._ttlSeconds(options.autoClean);
        return Promise.all(entries.map(([k, v]) => this.set(k, v, options))).then(() => {});
    }

    mget(keys) {
        this._checkBatch(keys.length);
        return Promise.all(keys.map(k => this.get(k))).then(vals => {
            const out = {};
            keys.forEach((k, i) => out[k] = vals[i]);
            return out;
        });
    }

    deleteMany(keys) {
        this._checkBatch(keys.length);
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
        return this._callEvent(event, options);
    }

    switchPool(pool, authToken = null) {
        return this._switchPool(pool, authToken).then(() => {});
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// LatZeroAsyncClient — explicit async/await API (named export)
// ─────────────────────────────────────────────────────────────────────────────

class LatZeroAsyncClient extends LatZeroBaseClient {
    constructor(dsn, pool, options = {}) {
        super(dsn, pool, options);

        const process = this._makeProcessProxy();
        this.process = {
            async register(...args) { await process.register(...args); },
            async unregister(...args) { await process.unregister(...args); },
            async call(...args) { return process.call(...args); },
            async broadcast(...args) { return process.broadcast(...args); },
            async list(...args) { return process.list(...args); }
        };

        if (options.autoConnect !== false) this.connect();
    }

    connect() { return this._connectSocket(); }

    async set(key, value, options = {}) {
        this.ensureJsonable(value);
        await this.sendRequest('set_buffer', { key, value, ttl: this._ttlSeconds(options.autoClean), persistent: options.persistent || false });
    }

    async get(key, defaultValue = null) {
        const r = await this.sendRequest('get_buffer', { key });
        const p = r.payload || {};
        return p.exists && p.entry != null ? p.entry.value : defaultValue;
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

    async clients() {
        const r = await this.sendRequest('list_clients', {});
        return r.payload?.clients || [];
    }

    async values(pattern = null) {
        const keys = await this.keys(pattern);
        this._checkBatch(keys.length);
        return Promise.all(keys.map(k => this.get(k)));
    }

    async items(pattern = null) {
        const keys = await this.keys(pattern);
        this._checkBatch(keys.length);
        return Promise.all(keys.map(async k => [k, await this.get(k)]));
    }

    async mset(data, options = {}) {
        const entries = Object.entries(data);
        this._checkBatch(entries.length);
        for (const [, value] of entries) this.ensureJsonable(value);
        this._ttlSeconds(options.autoClean);
        await Promise.all(entries.map(([k, v]) => this.set(k, v, options)));
    }

    async mget(keys) {
        this._checkBatch(keys.length);
        const vals = await Promise.all(keys.map(k => this.get(k)));
        const out = {};
        keys.forEach((k, i) => out[k] = vals[i]);
        return out;
    }

    async deleteMany(keys) {
        this._checkBatch(keys.length);
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
        return this._callEvent(event, options);
    }

    async switchPool(pool, authToken = null) {
        await this._switchPool(pool, authToken);
    }
}

module.exports = LatZeroClient;
module.exports.LatZeroAsyncClient = LatZeroAsyncClient;
