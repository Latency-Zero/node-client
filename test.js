import assert from 'node:assert/strict';
import test from 'node:test';
import net from 'node:net';
import { EventEmitter, once } from 'node:events';
import { createRequire } from 'node:module';
import { performance } from 'node:perf_hooks';
import { setImmediate as nextTurn } from 'node:timers/promises';
import { readFile } from 'node:fs/promises';
import LatZeroClient, { LatZeroAsyncClient } from './index.js';

const require = createRequire(import.meta.url);
const CommonJSClient = require('./index.cjs');
const variants = [
    ['ESM queued', LatZeroClient, true],
    ['ESM async', LatZeroAsyncClient, false],
    ['CJS queued', CommonJSClient, true],
    ['CJS async', CommonJSClient.LatZeroAsyncClient, false]
];

class MockSocket extends EventEmitter {
    constructor() {
        super();
        this.writes = [];
        this.writableLength = 0;
        this.destroyed = false;
        this.blockNext = false;
        this.autoHandshake = true;
    }

    connect() { queueMicrotask(() => { if (!this.destroyed) this.emit('connect'); }); }

    write(data) {
        if (this.destroyed) throw new Error('Socket is destroyed');
        const msg = JSON.parse(data);
        this.writes.push(msg);
        const blocked = this.blockNext;
        this.blockNext = false;
        if (blocked) this.writableLength += Buffer.byteLength(data);
        if (this.autoHandshake && ['hello', 'join_pool'].includes(msg.type)) {
            queueMicrotask(() => this.receive({ type: 'ack', request_id: msg.request_id, pool: null, payload: {} }));
        }
        return !blocked;
    }

    receive(msg) { if (!this.destroyed) this.emit('data', Buffer.from(JSON.stringify(msg) + '\n')); }

    drain() { this.writableLength = 0; this.emit('drain'); }

    destroy() {
        if (this.destroyed) return;
        this.destroyed = true;
        this.writableLength = 0;
        queueMicrotask(() => this.emit('close'));
    }
}

function makeClient(t, Client, options = {}) {
    const client = new Client('latzero://node-test', 'pool-a', { autoConnect: false, timeout: 10000, ...options });
    const sockets = [];
    client._createSocket = () => { const socket = new MockSocket(); sockets.push(socket); return socket; };
    t.after(() => client.disconnect());
    return { client, sockets };
}

function controlledTime(t) {
    const set = globalThis.setTimeout;
    const clear = globalThis.clearTimeout;
    const nowDescriptor = Object.getOwnPropertyDescriptor(performance, 'now');
    let now = 1000;
    const timers = new Map();
    Object.defineProperty(performance, 'now', { configurable: true, value: () => now });
    globalThis.setTimeout = (callback, ms, ...args) => {
        const timer = { at: now + ms, callback, args };
        timers.set(timer, timer);
        return timer;
    };
    globalThis.clearTimeout = timer => timers.delete(timer);
    t.after(() => {
        globalThis.setTimeout = set;
        globalThis.clearTimeout = clear;
        if (nowDescriptor) Object.defineProperty(performance, 'now', nowDescriptor);
        else delete performance.now;
        timers.clear();
    });
    return {
        timers,
        advance(ms) {
            now += ms;
            for (;;) {
                const due = [...timers.values()].filter(timer => timer.at <= now).sort((a, b) => a.at - b.at)[0];
                if (!due) break;
                timers.delete(due);
                due.callback(...due.args);
            }
        }
    };
}

async function connected(t, Client, options = {}) {
    const context = makeClient(t, Client, options);
    await context.client.connect();
    context.socket = context.sockets[0];
    return context;
}

function lastRequest(socket, type) {
    return [...socket.writes].reverse().find(msg => msg.type === type);
}

function ack(socket, msg, payload = {}) {
    socket.receive({ type: 'ack', request_id: msg.request_id, pool: null, payload });
}

function result(socket, msg, value = 42, error = null) {
    socket.receive({
        type: 'app_result', request_id: msg.request_id, client_id: 'worker', pool: 'pool-a',
        payload: { request_id: msg.request_id, value, error }
    });
}

for (const [label, Client, queued] of variants) {
    test(`${label}: constructor auto-connect exposes its real in-flight Promise`, async t => {
        class AutoClient extends Client {
            _createSocket() { return new MockSocket(); }
        }
        const client = new AutoClient('latzero://auto-node', 'pool-a', { timeout: 10000 });
        t.after(() => client.disconnect());
        const socket = client.socket;
        const promise = client.connect();
        assert.equal(promise, client._connectionPromise);
        const startup = queued ? client.get('startup') : null;
        await promise;
        assert.equal(client.socket, socket);
        assert.equal(client._ready, true);
        if (startup) {
            ack(socket, lastRequest(socket, 'get_buffer'), { exists: false });
            await startup;
        }
    });

    test(`${label}: connect shares one promise, callback fires once, readiness resets`, { timeout: 3000 }, async t => {
        const { client, sockets } = makeClient(t, Client);
        let connects = 0;
        let disconnects = 0;
        let callbacks = 0;
        client.on('connect', () => connects++);
        client.on('disconnect', () => disconnects++);
        const first = client.connect();
        assert.equal(typeof first.then, 'function');
        assert.equal(client.connect(), first);
        const callback = queued ? client.connect(err => { assert.equal(err, null); callbacks++; }) : first;
        await Promise.all([first, callback]);
        assert.equal(client._ready, true);
        assert.equal(client._connecting, false);
        assert.equal(connects, 1);
        assert.equal(callbacks, queued ? 1 : 0);
        assert.equal(sockets.length, 1);
        assert.deepEqual(sockets[0].writes.map(msg => msg.type), ['hello', 'join_pool']);
        assert.equal(sockets[0].writes[0].pool, null);
        assert.equal(sockets[0].writes[1].pool, null);
        client.disconnect();
        assert.equal(client._ready, false);
        assert.equal(client.connected, false);
        assert.equal(client._connectionPromise, null);
        await nextTurn();
        assert.equal(sockets.length, 1, 'intentional disconnect never reconnects');
        assert.equal(disconnects, 1);
        await client.connect();
        assert.equal(connects, 2);
        assert.equal(sockets.length, 2);
        assert.equal(client._ready, true);
        client.disconnect();
        assert.equal(disconnects, 2);
    });

    test(`${label}: get preserves stored null and falsy values, defaults only for missing entries`, async t => {
        const { client, socket } = await connected(t, Client);
        for (const value of [null, false, 0, '']) {
            const read = client.get('scalar', 'absent');
            ack(socket, lastRequest(socket, 'get_buffer'), { exists: true, entry: { value } });
            assert.equal(await read, value);
        }
        for (const payload of [
            { exists: false },
            { exists: false, entry: { value: 'ignored' } },
            { exists: true },
            { exists: true, entry: null }
        ]) {
            const read = client.get('missing', 'absent');
            ack(socket, lastRequest(socket, 'get_buffer'), payload);
            assert.equal(await read, 'absent');
        }
        const missing = client.get('missing');
        ack(socket, lastRequest(socket, 'get_buffer'), { exists: false });
        assert.equal(await missing, null);
        assert.equal(client.pending.size, 0);
    });

    test(`${label}: on/off/once/removeListener/emit use one EventEmitter registry`, async t => {
        const { client } = makeClient(t, Client);
        const received = [];
        function handler(value) { assert.equal(this, client); received.push(value); }
        assert.equal(client.on('local', handler), client);
        assert.equal(client.once('local', handler), client);
        assert.equal(client.emit('local', 1), true);
        assert.deepEqual(received, [1, 1]);
        assert.equal(client.listenerCount('local'), 1);
        assert.equal(client.removeListener('local', handler), client);
        assert.equal(client.emit('local', 2), false);
        client.on('local', handler).off('local', handler);
        assert.equal(client.listenerCount('local'), 0);
        client.once('presence', handler);
        client.handleMessage({ type: 'presence_update', payload: 3, pool: 'pool-a' });
        client.handleMessage({ type: 'presence_update', payload: 4, pool: 'pool-a' });
        client.on('bufferUpdate', handler);
        client.handleMessage({ type: 'buffer_update', payload: 5 });
        client.on('custom', handler);
        client.handleMessage({ type: 'emit_event', payload: { event: 'custom', data: 6 } });
        assert.deepEqual(received, [1, 1, 3, 5, 6]);
        assert.throws(() => client.emit('error', new Error('manual error')), /manual error/);
    });

    test(`${label}: receive decoder preserves a UTF-8 code point split inside a frame`, async t => {
        const { client, socket } = await connected(t, Client);
        const received = [];
        client.on('split', value => received.push(value));
        const unicode = String.fromCodePoint(0x1f680);
        const frame = Buffer.from(JSON.stringify({ type: 'emit_event', payload: { event: 'split', data: unicode } }) + '\n');
        const split = frame.indexOf(Buffer.from(unicode)) + 1;
        socket.emit('data', frame.subarray(0, split));
        assert.deepEqual(received, []);
        socket.emit('data', frame.subarray(split));
        assert.deepEqual(received, [unicode]);
        assert.equal(client.messageBuffer, '');
    });

    test(`${label}: failed hello and socket error reject queued work and permit explicit reconnect`, async t => {
        const { client, sockets } = makeClient(t, Client);
        const errors = [];
        client.on('error', err => errors.push(err));
        const connection = client.connect();
        sockets[0].autoHandshake = false;
        const rejectedConnection = assert.rejects(connection, /denied/);
        const queuedRequest = queued ? client.get('queued') : null;
        const rejectedRequest = queuedRequest ? assert.rejects(queuedRequest, /denied/) : null;
        await nextTurn();
        const hello = lastRequest(sockets[0], 'hello');
        sockets[0].receive({ type: 'error', request_id: hello.request_id, payload: { code: 'denied', message: 'denied' } });
        await rejectedConnection;
        if (rejectedRequest) await rejectedRequest;
        assert.equal(client.pending.size, 0);
        assert.equal(client._queuedBytes, 0);
        assert.equal(client._connectTimer, null);
        assert.equal(client._ready, false);
        assert.equal(errors.length, 1);
        await client.connect();
        const request = client.get('held');
        const rejected = assert.rejects(request, /connection reset/);
        sockets[1].emit('error', new Error('connection reset'));
        await rejected;
        assert.equal(errors.length, 2);
        assert.equal(client.pending.size, 0);
        await nextTurn();
        assert.equal(sockets.length, 2);
    });

    test(`${label}: remote EOF resets readiness and late socket frames cannot affect reconnect`, async t => {
        const { client, socket, sockets } = await connected(t, Client);
        const pending = client.get('remote-close');
        const rejection = assert.rejects(pending, err => err.code === 'connection_lost');
        socket.emit('close');
        await rejection;
        assert.equal(client._ready, false);
        assert.equal(client._connectionPromise, null);
        await client.connect();
        const events = [];
        client.on('presence', msg => events.push(msg));
        socket.emit('data', Buffer.from(JSON.stringify({ type: 'presence_update', payload: 'stale' }) + '\n'));
        socket.emit('error', new Error('late old-socket error'));
        assert.equal(client.socket, sockets[1]);
        assert.equal(client._ready, true);
        assert.deepEqual(events, []);
    });

    test(`${label}: one hello/join deadline and no-listener connection rejection`, async t => {
        const clock = controlledTime(t);
        const { client, sockets } = makeClient(t, Client, { timeout: 30 });
        const connection = client.connect();
        sockets[0].autoHandshake = false;
        const rejected = assert.rejects(connection, err => err.code === 'timeout');
        await nextTurn();
        const hello = lastRequest(sockets[0], 'hello');
        ack(sockets[0], hello);
        await nextTurn();
        clock.advance(30);
        await rejected;
        assert.equal(sockets[0].destroyed, true);
        assert.equal(client.pending.size, 0);
        assert.equal(client._connectionPromise, null);
    });

    test(`${label}: app/process ACK is not completion, both result orders preserve raw errors`, async t => {
        const { client, socket } = await connected(t, Client);
        for (const type of ['call_app', 'call_process']) {
            for (const resultFirst of [false, true]) {
                const call = type === 'call_app'
                    ? client.callEvent('compute', { targetClientId: 'worker', data: { n: 1 } })
                    : client.process.call('worker:compute', { n: 1 });
                const msg = lastRequest(socket, type);
                const entry = client.pending.get(msg.request_id);
                let settled = false;
                call.then(() => { settled = true; });
                if (!resultFirst) {
                    ack(socket, msg, { queued: true });
                    await nextTurn();
                    assert.equal(settled, false);
                    assert.equal(client.pending.has(msg.request_id), true);
                }
                result(socket, msg, null, { type: 'ApplicationError', message: 'not a transport rejection' });
                const response = await call;
                assert.equal(response.type, 'app_result');
                assert.equal(response.request_id, msg.request_id);
                assert.deepEqual(response.payload, {
                    request_id: msg.request_id, value: null, error: { type: 'ApplicationError', message: 'not a transport rejection' }
                });
                assert.equal(entry.timer, null);
                if (resultFirst) ack(socket, msg, { queued: true });
                assert.equal(client.pending.size, 0);
            }
        }
    });

    test(`${label}: matching self call and push IDs do not consume terminal waiters`, async t => {
        const { client, socket } = await connected(t, Client);
        let calls = 0;
        client.once('self', function(data) { assert.equal(this, client); calls++; return data.n + 1; });
        const call = client.callEvent('self', { targetClientId: client.clientId, data: { n: 4 }, responseTo: client.clientId });
        const msg = lastRequest(socket, 'call_app');
        socket.receive({ type: 'call_app', request_id: msg.request_id, pool: 'pool-a', payload: { event: 'self', data: { n: 4 } } });
        await nextTurn();
        const reply = lastRequest(socket, 'app_result');
        assert.equal(reply.request_id, msg.request_id);
        assert.deepEqual(reply.payload, { value: 5, error: null });
        assert.equal(calls, 1);
        assert.equal(client.listenerCount('self'), 0);
        assert.equal(client.pending.has(msg.request_id), true);
        ack(socket, msg, { delivered: true });
        assert.equal(client.pending.has(msg.request_id), true);
        result(socket, msg, 5);
        assert.equal((await call).payload.value, 5);
        const events = [];
        client.on('push', data => events.push(data));
        const pushed = client.emitEvent('push', { data: 7, targetClientId: client.clientId });
        const emitted = lastRequest(socket, 'emit_event');
        socket.receive({ type: 'emit_event', request_id: emitted.request_id, payload: { event: 'push', data: 7 } });
        assert.deepEqual(events, [7]);
        assert.equal(client.pending.has(emitted.request_id), true);
        ack(socket, emitted, { delivered: true });
        await pushed;
    });

    test(`${label}: third-party acceptance ignores callee delivery ACK and emits full unsolicited results`, async t => {
        const { client, socket } = await connected(t, Client);
        const results = [];
        client.on('app_result', envelope => results.push(envelope));
        for (const type of ['call_app', 'call_process']) {
            const call = type === 'call_app'
                ? client.callEvent('third-party', { targetClientId: client.clientId, responseTo: 'recipient' })
                : client.process.call('worker:job', {}, { responseTo: 'recipient' });
            const msg = lastRequest(socket, type);
            ack(socket, msg, { delivered: true });
            assert.equal(client.pending.has(msg.request_id), true);
            const envelope = {
                type: 'app_result', request_id: msg.request_id, client_id: 'worker', pool: 'pool-a',
                payload: { request_id: msg.request_id, response_to: 'recipient', value: 99, error: null }
            };
            socket.receive(envelope);
            assert.deepEqual(results.at(-1), envelope);
            assert.equal(client.pending.has(msg.request_id), true, 'result does not settle acceptance request');
            ack(socket, msg, { queued: true, request_id: msg.request_id, response_to: 'recipient' });
            const response = await call;
            assert.equal(response.type, 'ack');
            assert.equal(response.payload.queued, true);
            assert.equal(response.request_id, msg.request_id);
        }
        const entryCall = client.get('key');
        const msg = lastRequest(socket, 'get_buffer');
        result(socket, msg, 11);
        assert.equal(client.pending.has(msg.request_id), true, 'app_result is not a buffer ACK');
        ack(socket, msg, { exists: false });
        await entryCall;
    });

    test(`${label}: wire TTL/timeouts use fractional seconds and timeout clears state without replay`, async t => {
        const clock = controlledTime(t);
        const { client, socket } = await connected(t, Client);
        for (const [ms, seconds] of [[30000, 30], [125, 0.125], [0, 0]]) {
            const set = client.set('ttl', 'value', { autoClean: ms });
            const msg = lastRequest(socket, 'set_buffer');
            assert.equal(msg.payload.ttl, seconds);
            ack(socket, msg);
            await set;
        }
        const call = client.process.call('worker:slow', {}, { timeout: 35 });
        const rejected = assert.rejects(call, err => err.code === 'timeout' && err.message === 'Process call timeout');
        const msg = lastRequest(socket, 'call_process');
        const entry = client.pending.get(msg.request_id);
        assert.ok(msg.payload.timeout > 0 && msg.payload.timeout <= 0.035);
        ack(socket, msg, { queued: true });
        clock.advance(35);
        await rejected;
        assert.equal(entry.timer, null);
        assert.equal(client.pending.size, 0);
        assert.equal(client._queuedBytes, 0);
        const late = [];
        client.once('app_result', envelope => late.push(envelope));
        result(socket, msg, 'late-result');
        assert.equal(late[0].payload.value, 'late-result');
        await nextTurn();
        assert.equal(socket.writes.filter(frame => frame.request_id === msg.request_id).length, 1);
        const rejectedRequest = client.get('bad');
        const rejection = assert.rejects(rejectedRequest, err => err.code === 'bad_request');
        const request = lastRequest(socket, 'get_buffer');
        const requestEntry = client.pending.get(request.request_id);
        socket.receive({ type: 'error', request_id: request.request_id, payload: { code: 'bad_request', message: 'bad request' } });
        await rejection;
        assert.equal(requestEntry.timer, null);
    });

    test(`${label}: request count, batches, frame and transport bytes are finite`, async t => {
        const { client, socket } = await connected(t, Client, { maxPendingRequests: 2, maxBatchSize: 2, maxQueuedBytes: 512, maxFrameBytes: 512 });
        const held = client.get('one');
        const two = client.get('two');
        const heldRejection = assert.rejects(held, /disconnected/);
        const twoRejection = assert.rejects(two, /disconnected/);
        await assert.rejects(client.get('three'), err => err.code === 'client_overloaded');
        const beforeBatch = socket.writes.length;
        if (queued) assert.throws(() => client.mget(['a', 'b', 'c']), err => err.code === 'client_overloaded');
        else await assert.rejects(client.mget(['a', 'b', 'c']), err => err.code === 'client_overloaded');
        assert.equal(socket.writes.length, beforeBatch);
        client.disconnect();
        await Promise.all([heldRejection, twoRejection]);
        assert.equal(client._pendingUsers, 0);
        await client.connect();
        await assert.rejects(client.set('too-big', 'x'.repeat(1000)), err => err.code === 'frame_too_large');
        assert.equal(client.pending.size, 0);
        const current = lastRequest(client.socket, 'join_pool');
        assert.ok(current);
        client.socket.writableLength = 500;
        await assert.rejects(client.get('over-bytes'), err => err.code === 'client_overloaded');
        assert.equal(client._queuedBytes, 0);
        client.socket.writableLength = 0;
    });

    test(`${label}: write(false) accepts once, stops writes until drain, cancels unsent timeout`, async t => {
        const clock = controlledTime(t);
        const { client, socket } = await connected(t, Client);
        socket.blockNext = true;
        const first = client.get('first');
        const firstMsg = lastRequest(socket, 'get_buffer');
        const expired = client.process.call('worker:expired', {}, { timeout: 30 });
        const expiredRejection = assert.rejects(expired, err => err.code === 'timeout');
        const expiredEntry = [...client.pending.values()].find(entry => entry.requestType === 'call_process');
        const third = client.get('third');
        const before = socket.writes.length;
        ack(socket, firstMsg, { exists: false });
        await first;
        clock.advance(30);
        await expiredRejection;
        assert.equal(socket.writes.length, before);
        assert.equal(client.pending.has(expiredEntry.requestId), false);
        assert.equal(expiredEntry.timer, null);
        socket.drain();
        const thirdMsg = lastRequest(socket, 'get_buffer');
        assert.equal(thirdMsg.payload.key, 'third');
        ack(socket, thirdMsg, { exists: false });
        await third;
        assert.equal(socket.writes.filter(msg => msg.request_id === firstMsg.request_id).length, 1);
        assert.equal(socket.writes.some(msg => msg.request_id === expiredEntry.requestId), false);
        assert.equal(client._outbox.length, 0);
        assert.equal(client._queuedBytes, 0);
        assert.equal(client._drainTimer, null);
        assert.equal(clock.timers.size, 0);
    });

    test(`${label}: missed drain explicitly closes slow transport and fails remaining work`, async t => {
        const clock = controlledTime(t);
        const { client, socket } = await connected(t, Client, { timeout: 35 });
        socket.blockNext = true;
        const request = client.get('blocked');
        ack(socket, lastRequest(socket, 'get_buffer'), { exists: false });
        await request;
        const disconnected = once(client, 'disconnect');
        clock.advance(35);
        await disconnected;
        assert.equal(socket.destroyed, true);
        assert.equal(client._ready, false);
        assert.equal(client._drainTimer, null);
        assert.equal(client.pending.size, 0);
    });

    test(`${label}: replacements install before ACK, rollback failures without stale listeners`, async t => {
        const { client, socket } = await connected(t, Client);
        const old = () => 'old';
        const replacement = () => 'new';
        const registration = client.process.register(old, 'job');
        ack(socket, lastRequest(socket, 'register_process'));
        await registration;
        const change = client.process.register(replacement, 'job');
        socket.receive({ type: 'call_app', request_id: 'opaque-hop-before-ack', pool: 'pool-a', payload: { event: `${client.clientId}:job`, data: {} } });
        await nextTurn();
        assert.equal(lastRequest(socket, 'app_result').payload.value, 'new');
        ack(socket, lastRequest(socket, 'register_process'));
        await change;
        assert.equal(client._processes.get('job'), replacement);
        assert.equal(client.listenerCount(`${client.clientId}:job`), 0);
        const failed = client.process.register(() => 'failed', 'job');
        const rejected = assert.rejects(failed, /registration rejected/);
        const msg = lastRequest(socket, 'register_process');
        socket.receive({ type: 'error', request_id: msg.request_id, payload: { message: 'registration rejected' } });
        await rejected;
        assert.equal(client._processes.get('job'), replacement);
        const unregister = client.process.unregister('job');
        const rejectedUnregister = assert.rejects(unregister, /unregister rejected/);
        socket.receive({ type: 'error', request_id: lastRequest(socket, 'unregister_process').request_id, payload: { message: 'unregister rejected' } });
        await rejectedUnregister;
        assert.equal(client._processes.get('job'), replacement);
        const removed = client.process.unregister('job');
        ack(socket, lastRequest(socket, 'unregister_process'));
        await removed;
        assert.equal(client._processes.has('job'), false);
        const earlier = client.process.register(() => 'earlier', 'race');
        const earlierMsg = lastRequest(socket, 'register_process');
        const later = client.process.register(() => 'later', 'race');
        const laterMsg = lastRequest(socket, 'register_process');
        const earlierRejected = assert.rejects(earlier, /earlier failed/);
        const laterRejected = assert.rejects(later, /later failed/);
        socket.receive({ type: 'error', request_id: earlierMsg.request_id, payload: { message: 'earlier failed' } });
        socket.receive({ type: 'error', request_id: laterMsg.request_id, payload: { message: 'later failed' } });
        await Promise.all([earlierRejected, laterRejected]);
        assert.equal(client._processes.has('race'), false);
        assert.equal(client._registrations.has('race'), false);
    });

    test(`${label}: failed replacement chains stay bounded behind one pending registration`, async t => {
        const { client, socket } = await connected(t, Client);
        const accepted = client.process.register(() => 'stable', 'job');
        ack(socket, lastRequest(socket, 'register_process'));
        await accepted;
        const stable = client._processes.get('job');
        const first = client.process.register(() => 'pending', 'job');
        const firstMsg = lastRequest(socket, 'register_process');
        const firstRejected = assert.rejects(first, /first failed/);
        for (let i = 0; i < 20; i++) {
            const registration = client.process.register(() => i, 'job');
            const msg = lastRequest(socket, 'register_process');
            const rejected = assert.rejects(registration, /replacement failed/);
            socket.receive({ type: 'error', request_id: msg.request_id, payload: { message: 'replacement failed' } });
            await rejected;
            let depth = 0;
            for (let state = client._registrations.get('job'); state; state = state.prior) depth++;
            assert.equal(depth, 2, 'only pending and stable accepted functions retained');
        }
        socket.receive({ type: 'error', request_id: firstMsg.request_id, payload: { message: 'first failed' } });
        await firstRejected;
        assert.equal(client._processes.get('job'), stable);
        assert.equal(client._registrations.get('job').prior, null);
    });

    test(`${label}: async unserializable handlers reply JSON-safe errors and retain thrown error shape`, async t => {
        const { client, socket } = await connected(t, Client);
        const circular = {}; circular.self = circular;
        const values = [circular, 1n, () => {}, Symbol('invalid'), { toJSON() { throw new Error('bad toJSON'); } }];
        for (let i = 0; i < values.length; i++) {
            client.once(`invalid-${i}`, async () => values[i]);
            await client._handleAppCall({ type: 'call_app', request_id: `opaque-${i}`, pool: 'pool-a', payload: { event: `invalid-${i}`, data: {} } });
            const reply = lastRequest(socket, 'app_result');
            assert.equal(reply.request_id, `opaque-${i}`);
            assert.equal(reply.payload.value, null);
            assert.equal(typeof reply.payload.error, 'string');
            assert.ok(reply.payload.error.length);
        }
        client.once('throw', () => { throw new TypeError('application failed'); });
        await client._handleAppCall({ type: 'call_app', request_id: 'opaque-error', payload: { event: 'throw', data: {} } });
        assert.deepEqual(lastRequest(socket, 'app_result').payload, { value: null, error: { type: 'TypeError', message: 'application failed' } });
        client.once('undefined', () => undefined);
        await client._handleAppCall({ type: 'call_app', request_id: 'opaque-empty', payload: { event: 'undefined', data: {} } });
        assert.deepEqual(lastRequest(socket, 'app_result').payload, { value: null, error: null });
        let serializations = 0;
        client.once('toJSON-once', () => ({ toJSON() { serializations++; return { serializations }; } }));
        await client._handleAppCall({ type: 'call_app', request_id: 'opaque-json', payload: { event: 'toJSON-once', data: {} } });
        assert.equal(serializations, 1);
        assert.deepEqual(lastRequest(socket, 'app_result').payload, { value: { serializations: 1 }, error: null });
        const hostileError = {};
        Object.defineProperty(hostileError, 'message', { get() { throw new Error('getter failed'); } });
        hostileError.toString = () => { throw new Error('string failed'); };
        client.once('hostile-error', () => { throw hostileError; });
        await client._handleAppCall({ type: 'call_app', request_id: 'opaque-hostile', payload: { event: 'hostile-error', data: {} } });
        assert.deepEqual(lastRequest(socket, 'app_result').payload, { value: null, error: { type: 'Object', message: 'Handler failed' } });
    });

    test(`${label}: oversized handler result becomes a small safe reply`, async t => {
        const { client, socket } = await connected(t, Client, { maxFrameBytes: 512 });
        client.on('oversized', async () => 'x'.repeat(1000));
        await client._handleAppCall({ type: 'call_app', request_id: 'opaque-oversized', payload: { event: 'oversized', data: {} } });
        const reply = lastRequest(socket, 'app_result');
        assert.equal(reply.payload.value, null);
        assert.equal(typeof reply.payload.error, 'string');
        assert.ok(Buffer.byteLength(JSON.stringify(reply) + '\n') <= 512);
        assert.equal(client._ready, true);
    });

    test(`${label}: handler reply overload closes explicitly rather than dropping silently`, async t => {
        const { client, socket } = await connected(t, Client, { maxQueuedBytes: 512 });
        const errors = [];
        client.on('error', err => errors.push(err));
        socket.writableLength = 500;
        client.once('reply', () => 1);
        await client._handleAppCall({ type: 'call_app', request_id: 'opaque-reply', payload: { event: 'reply', data: {} } });
        assert.equal(socket.destroyed, true);
        assert.equal(client._ready, false);
        assert.equal(errors[0].code, 'client_overloaded');
        assert.equal(socket.writes.some(msg => msg.request_id === 'opaque-reply'), false);
    });

    test(`${label}: bounded handler admission, socket generation fences async replies`, async t => {
        const { client, socket, sockets } = await connected(t, Client, { maxConcurrentHandlers: 1 });
        let release;
        let calls = 0;
        client.on('slow', () => { calls++; return new Promise(resolve => { release = resolve; }); });
        const old = client._handleAppCall({ type: 'call_app', request_id: 'old-hop', pool: 'pool-a', payload: { event: 'slow', data: {} } });
        await client._handleAppCall({ type: 'call_app', request_id: 'overload-hop', pool: 'pool-a', payload: { event: 'slow', data: {} } });
        assert.equal(calls, 1);
        assert.deepEqual(lastRequest(socket, 'app_result').payload, { value: null, error: 'Client handler limit reached' });
        const before = socket.writes.length;
        client.disconnect();
        await client.connect();
        release('must not reach new socket');
        await old;
        assert.equal(socket.writes.length, before);
        assert.equal(sockets[1].writes.some(msg => msg.type === 'app_result'), false);
        assert.equal(client._activeHandlers, 0);
    });

    test(`${label}: pool switch cancels old requests/replies and commits before following frames`, async t => {
        const { client, socket } = await connected(t, Client);
        const registration = client.process.register(() => 'old process', 'job');
        ack(socket, lastRequest(socket, 'register_process'));
        await registration;
        let release;
        client.on('slow', () => new Promise(resolve => { release = resolve; }));
        const oldHandler = client._handleAppCall({ type: 'call_app', request_id: 'old-hop', pool: 'pool-a', payload: { event: 'slow', data: {} } });
        const oldCall = client.process.call('worker:old');
        const oldRejected = assert.rejects(oldCall, err => err.code === 'pool_changed');
        const switchCall = client.switchPool('pool-b', 'new-token');
        const switchMsg = lastRequest(socket, 'switch_pool');
        assert.equal(client._processes.size, 0);
        await assert.rejects(client.get('during-switch'), err => err.code === 'pool_switching');
        const presence = [];
        client.on('presence', payload => presence.push(payload));
        socket.emit('data', Buffer.from(JSON.stringify({ type: 'ack', request_id: switchMsg.request_id, pool: 'pool-b', payload: {} }) + '\n' + JSON.stringify({ type: 'presence_update', pool: 'pool-b', payload: { clients: ['new'] } }) + '\n'));
        await switchCall;
        await oldRejected;
        assert.equal(client.poolName, 'pool-b');
        assert.equal(client.authToken, 'new-token');
        assert.deepEqual(presence, [{ clients: ['new'] }]);
        release('stale');
        await oldHandler;
        assert.equal(socket.writes.some(msg => msg.request_id === 'old-hop'), false);
        const unsolicited = [];
        client.on('app_result', msg => unsolicited.push(msg));
        socket.receive({ type: 'app_result', request_id: 'old-result', pool: 'pool-a', payload: { value: 1, error: null } });
        assert.equal(unsolicited.length, 0);
    });

    test(`${label}: switch removes completed handler replies still queued behind drain`, async t => {
        const { client, socket } = await connected(t, Client);
        socket.blockNext = true;
        const oldRequest = client.get('blocked');
        const oldRejected = assert.rejects(oldRequest, err => err.code === 'pool_changed');
        client.once('queued-reply', () => 1);
        await client._handleAppCall({ type: 'call_app', request_id: 'old-queued-hop', payload: { event: 'queued-reply', data: {} } });
        assert.equal(client._outbox.length, 1);
        const switched = client.switchPool('pool-b');
        await oldRejected;
        socket.drain();
        const switchMsg = lastRequest(socket, 'switch_pool');
        ack(socket, switchMsg);
        await switched;
        assert.equal(socket.writes.some(msg => msg.request_id === 'old-queued-hop'), false);
        assert.equal(client._outbox.length, 0);
        assert.equal(client._queuedBytes, 0);
    });

    test(`${label}: failed pool switch restores registrations but fences prior handler work`, async t => {
        const { client, socket } = await connected(t, Client);
        const handler = () => 1;
        const registered = client.process.register(handler, 'job');
        ack(socket, lastRequest(socket, 'register_process'));
        await registered;
        const switched = client.switchPool('denied-pool');
        const rejected = assert.rejects(switched, /wrong auth/);
        socket.receive({ type: 'error', request_id: lastRequest(socket, 'switch_pool').request_id, payload: { message: 'wrong auth', code: 'auth_failed' } });
        await rejected;
        assert.equal(client.poolName, 'pool-a');
        assert.equal(client._processes.get('job'), handler);
        assert.equal(client._switching, false);
        assert.equal(client._ready, true);
    });

    test(`${label}: same-pool rejoin retains registrations, pending calls, and handler generation`, async t => {
        const { client, socket } = await connected(t, Client);
        const registered = client.process.register(() => 1, 'job');
        ack(socket, lastRequest(socket, 'register_process'));
        await registered;
        const handler = client._processes.get('job');
        const pending = client.process.call('worker:pending');
        const pendingMsg = lastRequest(socket, 'call_process');
        const generation = client._generation;
        const rejoined = client.switchPool('pool-a');
        assert.equal(client.pending.has(pendingMsg.request_id), true);
        assert.equal(client._processes.get('job'), handler);
        assert.equal(client._generation, generation);
        ack(socket, lastRequest(socket, 'switch_pool'));
        await rejoined;
        result(socket, pendingMsg, 2);
        assert.equal((await pending).payload.value, 2);
        assert.equal(client._processes.get('job'), handler);
    });

    test(`${label}: timed-out transmitted pool switch closes uncertain membership`, async t => {
        const clock = controlledTime(t);
        const { client, socket } = await connected(t, Client, { timeout: 35 });
        const rejected = assert.rejects(client.switchPool('pool-b'), err => err.code === 'timeout');
        clock.advance(35);
        await rejected;
        assert.equal(lastRequest(socket, 'switch_pool').payload.pool, 'pool-b');
        assert.equal(socket.destroyed, true);
        assert.equal(client._ready, false);
        assert.equal(client.pending.size, 0);
    });

    test(`${label}: direct wire call timeout seconds sets the same local deadline`, async t => {
        const clock = controlledTime(t);
        const { client, socket } = await connected(t, Client);
        const start = performance.now();
        const call = client.sendRequest('call_process', { process_id: 'worker:slow', timeout: 0.04 });
        const rejected = assert.rejects(call, err => err.code === 'timeout');
        const msg = lastRequest(socket, 'call_process');
        const entry = client.pending.get(msg.request_id);
        assert.equal(entry.deadline - start, 40);
        assert.ok(msg.payload.timeout > 0 && msg.payload.timeout <= 0.04);
        clock.advance(40);
        await rejected;
        await assert.rejects(client.sendRequest('call_process', { toJSON() { return null; } }), /RPC payload must be a JSON object/);
        assert.equal(client.pending.size, 0);
    });

    test(`${label}: malformed and oversized receive frames fail connection without retained bytes`, async t => {
        const { client, socket } = await connected(t, Client, { maxFrameBytes: 512 });
        socket.emit('data', Buffer.from('[]\n'));
        assert.equal(socket.destroyed, true);
        assert.equal(client._ready, false);
        assert.equal(client.messageBuffer, '');
        await client.connect();
        client.socket.emit('data', Buffer.from('x'.repeat(513)));
        assert.equal(client._ready, false);
        assert.equal(client.messageBuffer, '');
    });

    if (queued) {
        test(`${label}: connect error callback fires once and retains Promise rejection`, async t => {
            const { client, sockets } = makeClient(t, Client);
            let callbacks = 0;
            const connection = client.connect(err => { assert.equal(err.message, 'callback failure'); callbacks++; });
            const rejected = assert.rejects(connection, /callback failure/);
            sockets[0].emit('error', new Error('callback failure'));
            await rejected;
            await nextTurn();
            assert.equal(callbacks, 1);
            assert.equal(client.listenerCount('connect'), 0);
            assert.equal(client.listenerCount('error'), 0);
        });

        test(`${label}: failed first queued write rejects connection instead of emitting ready`, async t => {
            const { client, sockets } = makeClient(t, Client);
            const request = client.get('startup');
            const rejectedRequest = assert.rejects(request, /startup write failed/);
            client._createSocket = () => {
                const socket = new MockSocket();
                const write = socket.write.bind(socket);
                socket.write = data => {
                    if (JSON.parse(data).type === 'get_buffer') throw new Error('startup write failed');
                    return write(data);
                };
                sockets.push(socket);
                return socket;
            };
            let connects = 0;
            client.on('connect', () => connects++);
            await assert.rejects(client.connect(), /startup write failed/);
            await rejectedRequest;
            assert.equal(connects, 0);
            assert.equal(client._ready, false);
            assert.equal(client.pending.size, 0);
        });

        test(`${label}: pre-connect calls are bounded and deadline includes queue/handshake`, async t => {
            const clock = controlledTime(t);
            const { client, sockets } = makeClient(t, Client, { maxPendingRequests: 1, maxQueuedBytes: 512 });
            const expires = client.process.call('worker:job', {}, { timeout: 30 });
            const rejected = assert.rejects(expires, err => err.code === 'timeout');
            const id = [...client.pending.keys()][0];
            await assert.rejects(client.get('overload'), err => err.code === 'client_overloaded');
            clock.advance(30);
            await rejected;
            assert.equal(client._opQueue.length, 0);
            assert.equal(client._queuedBytes, 0);
            await client.connect();
            assert.equal(sockets[0].writes.some(msg => msg.request_id === id), false);
            client.disconnect();
            const queuedGet = client.get('new-key');
            await client.connect();
            const msg = lastRequest(sockets[1], 'get_buffer');
            ack(sockets[1], msg, { exists: true, entry: { value: 'new-value' } });
            assert.equal(await queuedGet, 'new-value');
        });

        test(`${label}: queued pool switch during hello preserves handshake correlation`, async t => {
            const { client, sockets } = makeClient(t, Client);
            const connection = client.connect();
            sockets[0].autoHandshake = false;
            await nextTurn();
            const hello = lastRequest(sockets[0], 'hello');
            assert.equal(client.pending.get(hello.request_id).sent, true);
            const switched = client.switchPool('pool-b');
            ack(sockets[0], hello);
            await nextTurn();
            const join = lastRequest(sockets[0], 'join_pool');
            assert.equal(join.payload.pool, 'pool-a');
            ack(sockets[0], join);
            await connection;
            const switchMsg = lastRequest(sockets[0], 'switch_pool');
            assert.equal(switchMsg.payload.pool, 'pool-b');
            ack(sockets[0], switchMsg);
            await switched;
            assert.equal(client.poolName, 'pool-b');
            assert.equal(client.pending.size, 0);
            assert.equal(client._switching, false);
        });

        test(`${label}: RPC wire budget is remaining deadline after admission wait`, async t => {
            const clock = controlledTime(t);
            const { client, sockets } = makeClient(t, Client);
            const call = client.process.call('worker:budget', {}, { timeout: 10000 });
            call.catch(() => {});
            clock.advance(5000);
            await client.connect();
            const msg = lastRequest(sockets[0], 'call_process');
            assert.equal(msg.payload.timeout, 5);
            result(sockets[0], msg, 1);
            await call;
        });
    } else {
        test(`${label}: explicit async operations reject before readiness`, async t => {
            const { client } = makeClient(t, Client);
            await assert.rejects(client.get('before-connect'), err => err.code === 'not_connected');
            assert.equal(client.pending.size, 0);
        });
    }
}

test('ESM/CJS implementation parity excludes only entry syntax', async () => {
    let esm = (await readFile(new URL('./index.js', import.meta.url), 'utf8')).replace(/\r\n/g, '\n');
    const cjs = (await readFile(new URL('./index.cjs', import.meta.url), 'utf8')).replace(/\r\n/g, '\n');
    esm = esm.replace("import net from 'net';", "const net = require('net');")
        .replace("import { EventEmitter } from 'events';", "const { EventEmitter } = require('events');")
        .replace("import { performance } from 'perf_hooks';", "const { performance } = require('perf_hooks');")
        .replace("import { StringDecoder } from 'string_decoder';", "const { StringDecoder } = require('string_decoder');")
        .replace('export default LatZeroClient;\nexport { LatZeroAsyncClient };', 'module.exports = LatZeroClient;\nmodule.exports.LatZeroAsyncClient = LatZeroAsyncClient;');
    assert.equal(cjs, "'use strict';\n\n" + esm);
});

for (const [label, Client] of variants) {
    test(`${label}: temporary raw TCP server, opaque self routes and split UTF-8`, { timeout: 5000 }, async t => {
        const peers = new Set();
        const routes = new Map();
        const frames = [];
        const buffers = new Map();
        let hop = 0;
        const server = net.createServer(socket => {
            peers.add(socket);
            socket.on('error', () => {});
            socket.on('close', () => peers.delete(socket));
            let input = '';
            const send = msg => socket.write(JSON.stringify(msg) + '\n');
            socket.on('data', chunk => {
                input += chunk.toString();
                let idx;
                while ((idx = input.indexOf('\n')) !== -1) {
                    const msg = JSON.parse(input.slice(0, idx));
                    input = input.slice(idx + 1);
                    frames.push(msg);
                    const reply = payload => send({ type: 'ack', request_id: msg.request_id, pool: null, payload });
                    if (msg.type === 'hello') reply({});
                    else if (msg.type === 'join_pool') {
                        reply({ joined: true });
                        const unicode = String.fromCodePoint(0x1f680);
                        const frame = Buffer.from(JSON.stringify({ type: 'emit_event', pool: 'pool-a', payload: { event: 'unicode', data: unicode } }) + '\n');
                        const split = frame.indexOf(Buffer.from(unicode)) + 1;
                        socket.write(frame.subarray(0, split));
                        setImmediate(() => { if (!socket.destroyed) socket.write(frame.subarray(split)); });
                    } else if (msg.type === 'set_buffer') {
                        buffers.set(msg.payload.key, msg.payload.value);
                        reply({});
                    } else if (msg.type === 'get_buffer') {
                        reply({ exists: buffers.has(msg.payload.key), entry: { value: buffers.get(msg.payload.key) } });
                    } else if (msg.type === 'register_process') reply({ process_id: `${msg.client_id}:${msg.payload.process_name}` });
                    else if (msg.type === 'call_process' || msg.type === 'call_app') {
                        const id = `opaque-hop/${++hop}`;
                        routes.set(id, msg);
                        send({
                            type: 'call_app', request_id: id, pool: 'pool-a',
                            payload: { event: msg.payload.event || msg.payload.process_id, data: msg.payload.data }
                        });
                        if (hop % 2) reply({ queued: true });
                    } else if (msg.type === 'app_result') {
                        const origin = routes.get(msg.request_id);
                        routes.delete(msg.request_id);
                        reply({ delivered: true });
                        send({ type: 'app_result', request_id: origin.request_id, pool: 'pool-a', payload: { ...msg.payload, request_id: origin.request_id } });
                        if (routes.size === 0) send({ type: 'ack', request_id: origin.request_id, pool: null, payload: { queued: true } });
                    }
                }
            });
        });
        t.after(async () => {
            for (const peer of peers) peer.destroy();
            if (server.listening) await new Promise(resolve => server.close(resolve));
        });
        server.listen(0, '127.0.0.1');
        await once(server, 'listening');
        const client = new Client('latzero://raw-node', 'pool-a', { autoConnect: false, timeout: 2000, port: server.address().port });
        t.after(() => client.disconnect());
        const unicodeEvent = once(client, 'unicode');
        await client.connect();
        assert.deepEqual(await unicodeEvent, [String.fromCodePoint(0x1f680)]);
        await client.set('raw', { value: 7 }, { autoClean: 125 });
        assert.deepEqual(await client.get('raw'), { value: 7 });
        assert.equal(frames.find(msg => msg.type === 'set_buffer').payload.ttl, 0.125);
        await client.process.register(async data => data.a + data.b, 'add');
        const processResult = await client.process.call('raw-node:add', { a: 3, b: 7 });
        assert.equal(processResult.payload.value, 10);
        client.once('echo', async data => data);
        const appResult = await client.callEvent('echo', { targetClientId: client.clientId, data: { echo: true } });
        assert.deepEqual(appResult.payload.value, { echo: true });
        const incomingReplies = frames.filter(msg => msg.type === 'app_result');
        assert.deepEqual(incomingReplies.map(msg => msg.request_id), ['opaque-hop/1', 'opaque-hop/2']);
        assert.notEqual(processResult.request_id, incomingReplies[0].request_id);
        assert.equal(processResult.request_id, processResult.payload.request_id);
        assert.equal(client.pending.size, 0);
    });
}
