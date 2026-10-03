import assert from 'node:assert/strict';
import test from 'node:test';
import net from 'node:net';
import { EventEmitter, once } from 'node:events';
import { createRequire } from 'node:module';
import { performance } from 'node:perf_hooks';
import { StringDecoder } from 'node:string_decoder';
import { setImmediate as nextTurn } from 'node:timers/promises';
import LatZeroClient, { LatZeroAsyncClient } from './index.js';

const CommonJSClient = createRequire(import.meta.url)('./index.cjs');
const variants = [
    ['ESM queued', LatZeroClient, true],
    ['ESM async', LatZeroAsyncClient, false],
    ['CJS queued', CommonJSClient, true],
    ['CJS async', CommonJSClient.LatZeroAsyncClient, false]
];

function deferred() {
    let resolve;
    const promise = new Promise(done => { resolve = done; });
    return { promise, resolve };
}

async function tcpFixture(t, onMessage = () => {}, autoHello = true) {
    const records = [];
    const peers = new Set();
    const events = new EventEmitter();
    const server = net.createServer(socket => {
        const peer = {
            socket,
            send(msg) { return socket.write(JSON.stringify(msg) + '\n'); },
            ack(msg, payload = {}) { this.send({ type: 'ack', request_id: msg.request_id, pool: null, payload }); }
        };
        peers.add(peer);
        socket.on('error', () => {});
        socket.on('close', () => peers.delete(peer));
        const decoder = new StringDecoder('utf8');
        let input = '';
        socket.on('data', chunk => {
            input += decoder.write(chunk);
            let index;
            while ((index = input.indexOf('\n')) !== -1) {
                const msg = JSON.parse(input.slice(0, index));
                input = input.slice(index + 1);
                const record = { msg, peer };
                records.push(record);
                events.emit('frame', record);
                if (msg.type === 'hello' && autoHello) peer.ack(msg);
                else onMessage(msg, peer);
            }
        });
    });
    t.after(async () => {
        events.removeAllListeners();
        for (const peer of peers) peer.socket.destroy();
        if (server.listening) await new Promise(resolve => server.close(resolve));
    });
    server.listen(0, '127.0.0.1');
    await once(server, 'listening');
    return {
        port: server.address().port, records, peers,
        waitFor(predicate) {
            const found = records.find(predicate);
            if (found) return Promise.resolve(found);
            return new Promise(resolve => {
                const listener = record => {
                    if (!predicate(record)) return;
                    events.off('frame', listener);
                    resolve(record);
                };
                events.on('frame', listener);
            });
        }
    };
}

function redirect(msg, target, router, payload = {}) {
    return {
        type: 'redirect', request_id: msg.request_id, client_id: msg.client_id, pool: msg.payload.pool,
        payload: {
            protocol: 'pool_redirect_v1', host: '127.0.0.1', port: target.port, ws_port: null,
            pool: msg.payload.pool, pod_index: 0, pod_count: 8,
            router_host: '127.0.0.1', router_port: router.port, router_ws_port: null,
            cluster_id: 'fixture-cluster', ...payload
        }
    };
}

function clientFor(t, Client, router, options = {}) {
    const client = new Client('latzero://pod-node', 'pool-a', {
        autoConnect: false, timeout: 2000, host: '127.0.0.1', port: router.port, ...options
    });
    t.after(() => client.disconnect());
    return client;
}

function controlledTime(t) {
    const set = globalThis.setTimeout;
    const clear = globalThis.clearTimeout;
    const descriptor = Object.getOwnPropertyDescriptor(performance, 'now');
    const timers = new Map();
    let now = 1000;
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
        if (descriptor) Object.defineProperty(performance, 'now', descriptor);
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

const malformed = [
    ['protocol', msg => { msg.payload.protocol = 'pool_redirect_v2'; }],
    ['missing protocol', msg => { delete msg.payload.protocol; }],
    ['envelope pool', msg => { msg.pool = 'other-pool'; }],
    ['payload pool', msg => { msg.payload.pool = 'other-pool'; }],
    ['numeric pool', msg => { msg.payload.pool = 1; }],
    ['identity', msg => { msg.client_id = 'other-client'; }],
    ['missing identity', msg => { delete msg.client_id; }],
    ['payload array', msg => { msg.payload = []; }],
    ['zero port', msg => { msg.payload.port = 0; }],
    ['negative port', msg => { msg.payload.port = -1; }],
    ['high port', msg => { msg.payload.port = 65536; }],
    ['fractional port', msg => { msg.payload.port = 80.5; }],
    ['string port', msg => { msg.payload.port = '80'; }],
    ['boolean port', msg => { msg.payload.port = true; }],
    ['null port', msg => { msg.payload.port = null; }],
    ['missing ws port', msg => { delete msg.payload.ws_port; }],
    ['bad ws port', msg => { msg.payload.ws_port = 65536; }],
    ['bad router port', msg => { msg.payload.router_port = 0; }],
    ['bad router ws port', msg => { msg.payload.router_ws_port = false; }],
    ['zero count', msg => { msg.payload.pod_count = 0; }],
    ['high count', msg => { msg.payload.pod_count = 65; }],
    ['fractional count', msg => { msg.payload.pod_count = 2.5; }],
    ['boolean count', msg => { msg.payload.pod_count = true; }],
    ['negative index', msg => { msg.payload.pod_index = -1; }],
    ['out-of-range index', msg => { msg.payload.pod_index = 8; }],
    ['fractional index', msg => { msg.payload.pod_index = 0.5; }],
    ['string index', msg => { msg.payload.pod_index = '0'; }],
    ['missing index', msg => { delete msg.payload.pod_index; }],
    ['empty cluster', msg => { msg.payload.cluster_id = ''; }],
    ['blank cluster', msg => { msg.payload.cluster_id = ' '; }],
    ['missing cluster', msg => { delete msg.payload.cluster_id; }],
    ['numeric cluster', msg => { msg.payload.cluster_id = 1; }],
    ...['localhost', 'localhost.evil.test', '192.0.2.1', '0.0.0.0', '169.254.169.254',
        '127.1', '2130706433', '0x7f000001', 'http://127.0.0.1/', '127.0.0.1:80',
        'user@127.0.0.1', '::', '::ffff:127.0.0.1', '::1%lo', null, ''].map(host =>
        [`unsafe host ${host}`, msg => { msg.payload.host = host; }, 'unsafe_redirect']),
    ['unsafe router host', msg => { msg.payload.router_host = 'localhost'; }, 'unsafe_redirect']
];

for (const [label, Client, queued] of variants) {
    test(`${label}: redirect connect has one Promise and generation, final-owner readiness, explicit entry reconnect`, { timeout: 10000 }, async t => {
        let holdOwner = true;
        const owner = await tcpFixture(t, (msg, peer) => {
            if (msg.type === 'join_pool' && !holdOwner) peer.ack(msg);
            if (msg.type === 'list_clients') peer.ack(msg, { clients: ['owner'] });
        });
        const router = await tcpFixture(t, (msg, peer) => {
            if (msg.type !== 'join_pool') return;
            const packet = redirect(msg, owner, router);
            peer.socket.end(JSON.stringify(packet) + '\n' + JSON.stringify(packet) + '\n' +
                JSON.stringify({ type: 'presence_update', pool: 'pool-a', payload: 'stale-router' }) + '\n');
        });
        const client = clientFor(t, Client, router, { authToken: 'secret' });
        const lifecycle = [];
        const presence = [];
        client.on('connect', () => lifecycle.push('connect'));
        client.on('disconnect', () => lifecycle.push('disconnect'));
        client.on('presence', value => presence.push(value));
        const connection = client.connect();
        const oldSocket = client.socket;
        const generation = client._generation;
        let callbacks = 0;
        const callback = queued ? client.connect(err => { assert.equal(err, null); callbacks++; }) : connection;
        assert.equal(client.connect(), connection);
        const ownerJoin = await owner.waitFor(record => record.msg.type === 'join_pool');
        assert.equal(client.connect(), connection);
        assert.equal(client._connectionPromise, connection);
        assert.equal(client._generation, generation);
        assert.equal(client.connected, false);
        assert.equal(client._ready, false);
        assert.deepEqual(lifecycle, []);
        assert.deepEqual(ownerJoin.msg.payload, { client_id: 'pod-node', pool: 'pool-a', auth_token: 'secret' });
        for (const fixture of [router, owner]) {
            const hello = fixture.records.find(record => record.msg.type === 'hello').msg;
            assert.deepEqual(hello.payload, { capabilities: ['pool_redirect_v1'] });
            assert.equal(hello.pool, null);
            assert.equal(hello.client_id, 'pod-node');
        }
        assert.equal(ownerJoin.msg.pool, null);
        ownerJoin.peer.ack(ownerJoin.msg);
        await Promise.all([connection, callback]);
        assert.equal(client.connected, true);
        assert.equal(client._ready, true);
        assert.equal(callbacks, queued ? 1 : 0);
        assert.deepEqual(lifecycle, ['connect']);
        assert.deepEqual(presence, []);
        assert.equal(client.host, '127.0.0.1');
        assert.equal(client.port, router.port);
        assert.deepEqual(client.endpoint, { host: '127.0.0.1', port: owner.port });
        assert.throws(() => { client.endpoint.port = router.port; }, TypeError);
        const currentSocket = client.socket;
        oldSocket.emit('close');
        oldSocket.emit('error', new Error('stale router error'));
        oldSocket.emit('data', Buffer.from('{"type":"presence_update","payload":"late"}\n'));
        assert.equal(client.socket, currentSocket);
        assert.equal(client.connected, true);
        assert.deepEqual(await client.clients(), ['owner']);
        const disconnected = once(client, 'disconnect');
        ownerJoin.peer.socket.end();
        await disconnected;
        assert.equal(client._connectionPromise, null);
        assert.equal(client.endpoint, null);
        await nextTurn();
        assert.equal(router.records.filter(record => record.msg.type === 'join_pool').length, 1);
        holdOwner = false;
        const reconnection = client.connect();
        assert.notEqual(reconnection, connection);
        await reconnection;
        assert.equal(router.records.filter(record => record.msg.type === 'join_pool').length, 2);
        assert.deepEqual(lifecycle, ['connect', 'disconnect', 'connect']);
        client.disconnect();
        await nextTurn();
        assert.deepEqual(lifecycle, ['connect', 'disconnect', 'connect', 'disconnect']);
        assert.equal(router.records.filter(record => record.msg.type === 'join_pool').length, 2);
    });

    test(`${label}: owner-changing switch fences handlers/routes, captures auth, and commits before following frames`, { timeout: 10000 }, async t => {
        const ownerB = await tcpFixture(t, (msg, peer) => {
            if (['register_process', 'switch_pool'].includes(msg.type)) peer.ack(msg);
        });
        const ownerA = await tcpFixture(t, (msg, peer) => {
            if (['join_pool', 'register_process'].includes(msg.type)) peer.ack(msg);
            if (msg.type === 'switch_pool') peer.socket.end(JSON.stringify(redirect(msg, ownerB, router, { pod_index: 1 })) + '\n' +
                JSON.stringify({ type: 'call_app', request_id: 'stale-new-call', pool: 'pool-a', payload: { event: 'pod-node:job', data: {} } }) + '\n');
        });
        const router = await tcpFixture(t, (msg, peer) => {
            if (msg.type === 'join_pool') peer.socket.end(JSON.stringify(redirect(msg, ownerA, router)) + '\n');
        });
        const client = clientFor(t, Client, router, { authToken: 'old-token' });
        const lifecycle = [];
        client.on('connect', () => lifecycle.push('connect'));
        client.on('disconnect', () => lifecycle.push('disconnect'));
        await client.connect();
        const originalConnection = client.connect();
        const oldSocket = client.socket;
        const started = deferred();
        const released = deferred();
        let runs = 0;
        await client.process.register(async () => { runs++; started.resolve(); return released.promise; }, 'job');
        const oldHandler = client._handleAppCall({ type: 'call_app', request_id: 'old-handler-hop', pool: 'pool-a', payload: { event: 'pod-node:job', data: {} } });
        await started.promise;
        const pending = client.process.call('worker:pending');
        const cancelled = assert.rejects(pending, err => err.code === 'pool_changed');
        const pendingWire = await ownerA.waitFor(record => record.msg.type === 'call_process');
        const switching = client.switchPool('pool-b', 'new-token');
        const switchedWire = await ownerA.waitFor(record => record.msg.type === 'switch_pool');
        assert.equal(switchedWire.msg.pool, null);
        assert.deepEqual(switchedWire.msg.payload, { client_id: 'pod-node', pool: 'pool-b', auth_token: 'new-token' });
        const joined = await ownerB.waitFor(record => record.msg.type === 'join_pool');
        assert.deepEqual(joined.msg.payload, switchedWire.msg.payload);
        assert.equal(client._ready, false);
        assert.equal(client.connected, false);
        assert.equal(client.poolName, 'pool-a');
        assert.equal(client._processes.size, 0);
        assert.equal(client._registrations.size, 0);
        await assert.rejects(client.get('during-switch'), err => err.code === 'pool_switching');
        const ownerConnection = client.connect();
        assert.notEqual(ownerConnection, originalConnection);
        assert.equal(client.connect(), ownerConnection);
        const presence = [];
        client.on('presence', value => presence.push(value));
        joined.peer.socket.write(JSON.stringify({ type: 'ack', request_id: joined.msg.request_id, pool: null, payload: { joined: true } }) + '\n' +
            JSON.stringify({ type: 'presence_update', pool: 'pool-b', payload: 'new-owner' }) + '\n');
        await Promise.all([switching, ownerConnection, cancelled]);
        assert.equal(client.poolName, 'pool-b');
        assert.equal(client.authToken, 'new-token');
        assert.equal(client.connected, true);
        assert.deepEqual(presence, ['new-owner']);
        assert.deepEqual(lifecycle, ['connect', 'disconnect', 'connect']);
        released.resolve('must not escape');
        await oldHandler;
        oldSocket.emit('close');
        oldSocket.emit('error', new Error('old owner EOF followup'));
        oldSocket.emit('data', Buffer.from(JSON.stringify({ type: 'app_result', request_id: pendingWire.msg.request_id, pool: 'pool-a', payload: { value: 'late', error: null } }) + '\n'));
        assert.equal(client.connected, true);
        assert.equal(client.endpoint.port, ownerB.port);
        assert.equal(runs, 1);
        assert.equal(ownerA.records.some(record => record.msg.type === 'app_result'), false);
        assert.equal(ownerB.records.some(record => ['call_process', 'register_process', 'app_result'].includes(record.msg.type)), false);
        joined.peer.send({ type: 'call_app', request_id: 'not-reregistered', pool: 'pool-b', payload: { event: 'pod-node:job', data: {} } });
        const noHandler = await ownerB.waitFor(record => record.msg.request_id === 'not-reregistered');
        assert.equal(noHandler.msg.payload.error.type, 'NoHandler');
        assert.equal(runs, 1);
        await client.process.register(() => 'explicitly registered', 'job');
        const functionBefore = client._processes.get('job');
        const stillPending = client.process.call('worker:retained');
        const retainedWire = await ownerB.waitFor(record => record.msg.type === 'call_process');
        const generation = client._generation;
        await client.switchPool('pool-b');
        assert.equal(client._generation, generation);
        assert.equal(client._processes.get('job'), functionBefore);
        assert.equal(client.pending.has(retainedWire.msg.request_id), true);
        const envelope = { type: 'app_result', request_id: retainedWire.msg.request_id, client_id: 'worker', pool: 'pool-b', payload: { request_id: retainedWire.msg.request_id, value: 7, error: null } };
        retainedWire.peer.send(envelope);
        assert.deepEqual(await stillPending, envelope);
        const results = [];
        client.on('app_result', value => results.push(value));
        const unsolicited = { ...envelope, request_id: 'independent-result', payload: { value: 9, error: null, request_id: 'independent-result' } };
        const resultBarrier = once(client, 'app_result');
        joined.peer.send(unsolicited);
        await resultBarrier;
        assert.deepEqual(results, [unsolicited]);
        assert.deepEqual(lifecycle, ['connect', 'disconnect', 'connect']);
    });

    test(`${label}: redirects are consumed only by sent/correlated membership requests`, { timeout: 10000 }, async t => {
        const unused = await tcpFixture(t);
        const server = await tcpFixture(t, (msg, peer) => {
            if (msg.type === 'join_pool') peer.ack(msg);
        }, false);
        const client = clientFor(t, Client, server);
        const connection = client.connect();
        const hello = await server.waitFor(record => record.msg.type === 'hello');
        hello.peer.send(redirect({ ...hello.msg, payload: { pool: 'pool-a' } }, unused, server));
        hello.peer.send({ type: 'redirect', request_id: 'unmatched', client_id: client.clientId, pool: 'pool-a', payload: {} });
        hello.peer.ack(hello.msg);
        await connection;
        const socket = client.socket;
        for (const type of ['get_buffer', 'call_app', 'call_process', 'register_process']) {
            const call = client.sendRequest(type, type === 'get_buffer' ? { key: 'key' } : type === 'register_process' ? { process_name: 'job' } : { event: 'job', process_id: 'worker:job' });
            const wire = await server.waitFor(record => record.msg.type === type);
            wire.peer.send(redirect({ ...wire.msg, payload: { pool: 'pool-a' } }, unused, server));
            // A same-stream control barrier makes the ignored redirect precede the observed response.
            const barrier = client.sendRequest('list_clients', {});
            const barrierWire = await server.waitFor(record => record.msg.type === 'list_clients' && !record.used);
            barrierWire.used = true;
            barrierWire.peer.ack(barrierWire.msg);
            await barrier;
            assert.equal(client.pending.has(wire.msg.request_id), true);
            assert.equal(client.socket, socket);
            if (type === 'call_app' || type === 'call_process') wire.peer.send({ type: 'app_result', request_id: wire.msg.request_id, pool: null, payload: { value: 5, error: null } });
            else wire.peer.ack(wire.msg);
            await call;
        }
        assert.equal(unused.records.length, 0);
        assert.equal(server.records.filter(record => record.msg.type === 'hello').length, 1);
        assert.equal(client.pending.size, 0);
    });

    test(`${label}: malformed redirects reject finitely without dialing targets`, { timeout: 30000 }, async t => {
        for (const [name, mutate, code = 'invalid_redirect'] of malformed) {
            await t.test(name, async subtest => {
                const owner = await tcpFixture(subtest);
                const router = await tcpFixture(subtest, (msg, peer) => {
                    if (msg.type !== 'join_pool') return;
                    const packet = redirect(msg, owner, router);
                    mutate(packet);
                    peer.send(packet);
                });
                const client = clientFor(subtest, Client, router);
                const errors = [];
                client.on('error', err => errors.push(err));
                await assert.rejects(client.connect(), err => err.code === code);
                assert.equal(client.connected, false);
                assert.equal(client.socket, null);
                assert.equal(client._connectionPromise, null);
                assert.equal(client.pending.size, 0);
                assert.equal(client._connectTimer, null);
                assert.equal(client._queuedBytes, 0);
                assert.equal(errors.length, 1);
                assert.equal(owner.records.length, 0);
            });
        }
    });

    test(`${label}: remote entry never gains loopback redirect trust through DNS or its actual peer`, { timeout: 10000 }, async t => {
        for (const host of ['remote.example.test', '192.0.2.10', 'localhost.evil.test', '127.1']) {
            const owner = await tcpFixture(t);
            const router = await tcpFixture(t, (msg, peer) => {
                if (msg.type === 'join_pool') peer.send(redirect(msg, owner, router));
            });
            const client = clientFor(t, Client, router, { host });
            client._createSocket = () => {
                const socket = new net.Socket();
                const connect = socket.connect.bind(socket);
                // Only the test substitutes the actual peer; configured trust remains remote.
                socket.connect = port => connect(port, '127.0.0.1');
                return socket;
            };
            await assert.rejects(client.connect(), err => err.code === 'unsafe_redirect');
            assert.equal(owner.records.length, 0);
            assert.equal(client.host, host);
            assert.equal(client.port, router.port);
        }
    });

    test(`${label}: duplicate endpoints/pods, cycles, cluster changes and hop limits fail finitely`, { timeout: 10000 }, async t => {
        for (const scenario of ['entry-loop', 'cycle', 'duplicate-pod', 'cluster-change', 'count-change', 'router-change', 'hop-limit']) {
            await t.test(scenario, async subtest => {
                const last = await tcpFixture(subtest);
                const owner = await tcpFixture(subtest, (msg, peer) => {
                    if (msg.type !== 'join_pool') return;
                    const target = scenario === 'cycle' ? router : last;
                    const packet = redirect(msg, target, router, {
                        pod_index: scenario === 'duplicate-pod' ? 0 : 1,
                        ...(scenario === 'cluster-change' ? { cluster_id: 'other-cluster' } : {}),
                        ...(scenario === 'count-change' ? { pod_count: 7 } : {}),
                        ...(scenario === 'router-change' ? { router_port: last.port } : {})
                    });
                    peer.send(packet);
                });
                const router = await tcpFixture(subtest, (msg, peer) => {
                    if (msg.type === 'join_pool') peer.send(redirect(msg, scenario === 'entry-loop' ? router : owner, router));
                });
                const client = clientFor(subtest, Client, router, scenario === 'hop-limit' ? { maxRedirects: 1 } : {});
                const code = ['cluster-change', 'count-change', 'router-change'].includes(scenario) ? 'invalid_redirect' : scenario === 'hop-limit' ? 'redirect_limit' : 'redirect_loop';
                await assert.rejects(client.connect(), err => err.code === code);
                assert.equal(last.records.length, 0);
                assert.equal(client.pending.size, 0);
                assert.equal(client._connectionPromise, null);
            });
        }
    });

    test(`${label}: default four hops succeed and a fifth is rejected before connection`, { timeout: 10000 }, async t => {
        for (const hops of [4, 5]) {
            const owners = [];
            for (let index = 0; index < hops; index++) {
                owners.push(await tcpFixture(t, (msg, peer) => {
                    if (msg.type !== 'join_pool') return;
                    if (index === hops - 1) peer.ack(msg);
                    else peer.send(redirect(msg, owners[index + 1], router, { pod_index: index + 1 }));
                }));
            }
            const router = await tcpFixture(t, (msg, peer) => {
                if (msg.type === 'join_pool') peer.send(redirect(msg, owners[0], router));
            });
            const client = clientFor(t, Client, router);
            const generation = client._generation;
            if (hops === 4) {
                await client.connect();
                assert.equal(client._generation, generation);
                assert.equal(client.endpoint.port, owners[3].port);
                assert.equal(client.connected, true);
                assert.equal(owners.flatMap(owner => owner.records).filter(record => record.msg.type === 'join_pool').length, 4);
            } else {
                await assert.rejects(client.connect(), err => err.code === 'redirect_limit');
                assert.equal(owners[4].records.length, 0);
                assert.equal(client._connectionPromise, null);
            }
        }
    });

    test(`${label}: hello/join and all connect hops share the original timeout budget`, { timeout: 10000 }, async t => {
        const clock = controlledTime(t);
        const second = await tcpFixture(t, () => {}, false);
        const first = await tcpFixture(t, () => {}, false);
        const router = await tcpFixture(t, () => {}, false);
        const client = clientFor(t, Client, router, { timeout: 100 });
        const connection = client.connect();
        const rejected = assert.rejects(connection, err => err.code === 'timeout');
        const timer = client._connectTimer;
        const hello = await router.waitFor(record => record.msg.type === 'hello');
        clock.advance(25);
        hello.peer.ack(hello.msg);
        const join = await router.waitFor(record => record.msg.type === 'join_pool');
        assert.equal(client.pending.get(join.msg.request_id).deadline, 1100);
        clock.advance(25);
        join.peer.send(redirect(join.msg, first, router));
        const firstHello = await first.waitFor(record => record.msg.type === 'hello');
        assert.equal(client._connectTimer, timer);
        assert.equal(client.pending.get(firstHello.msg.request_id).deadline, 1100);
        clock.advance(25);
        firstHello.peer.ack(firstHello.msg);
        const firstJoin = await first.waitFor(record => record.msg.type === 'join_pool');
        firstJoin.peer.send(redirect(firstJoin.msg, second, router, { pod_index: 1 }));
        const secondHello = await second.waitFor(record => record.msg.type === 'hello');
        clock.advance(20);
        secondHello.peer.ack(secondHello.msg);
        const finalJoin = await second.waitFor(record => record.msg.type === 'join_pool');
        assert.equal(client.pending.get(finalJoin.msg.request_id).deadline, 1100);
        assert.equal(client._connectTimer, timer);
        clock.advance(5);
        await rejected;
        assert.equal(client.connected, false);
        assert.equal(client.pending.size, 0);
        assert.equal(client._connectionPromise, null);
        assert.equal(clock.timers.size, 0);
    });

    test(`${label}: switch redirects reuse the switch deadline, not a fresh connect timeout`, { timeout: 10000 }, async t => {
        const clock = controlledTime(t);
        const target = await tcpFixture(t, () => {}, false);
        const owner = await tcpFixture(t, (msg, peer) => {
            if (msg.type === 'join_pool') peer.ack(msg);
        });
        const client = clientFor(t, Client, owner, { timeout: 100 });
        await client.connect();
        const switching = client.switchPool('pool-b', 'secret');
        const rejected = assert.rejects(switching, err => err.code === 'timeout');
        const wire = await owner.waitFor(record => record.msg.type === 'switch_pool');
        const deadline = client.pending.get(wire.msg.request_id).deadline;
        clock.advance(60);
        wire.peer.send(redirect(wire.msg, target, owner));
        const hello = await target.waitFor(record => record.msg.type === 'hello');
        const connection = client.connect();
        const connectionRejected = assert.rejects(connection, err => err.code === 'timeout');
        assert.equal(client.pending.get(hello.msg.request_id).deadline, deadline);
        assert.equal(client._connectTimer.at, deadline);
        clock.advance(20);
        hello.peer.ack(hello.msg);
        const join = await target.waitFor(record => record.msg.type === 'join_pool');
        assert.equal(client.pending.get(join.msg.request_id).deadline, deadline);
        clock.advance(20);
        await Promise.all([rejected, connectionRejected]);
        assert.equal(client.socket, null);
        assert.equal(client.pending.size, 0);
        assert.equal(client._switching, false);
        assert.equal(clock.timers.size, 0);
    });

    test(`${label}: owner auth denial propagates through connect/switch without retry or stale rollback`, { timeout: 10000 }, async t => {
        for (const changePool of [false, true]) {
            const target = await tcpFixture(t, (msg, peer) => {
                if (msg.type === 'join_pool') peer.socket.end(JSON.stringify({ type: 'error', request_id: msg.request_id, pool: null, payload: { code: 'auth_failed', message: 'Owner denied token' } }) + '\n');
            });
            const router = await tcpFixture(t, (msg, peer) => {
                if (msg.type === 'join_pool') {
                    if (changePool) peer.ack(msg);
                    else peer.socket.end(JSON.stringify(redirect(msg, target, router)) + '\n');
                }
                if (msg.type === 'register_process') peer.ack(msg);
                if (msg.type === 'switch_pool') peer.socket.end(JSON.stringify(redirect(msg, target, router)) + '\n');
            });
            const client = clientFor(t, Client, router, { authToken: 'old-token' });
            const errors = [];
            client.on('error', err => errors.push(err));
            let operation;
            if (changePool) {
                await client.connect();
                await client.process.register(() => 'old', 'job');
                operation = client.switchPool('pool-b', 'denied-token');
            } else operation = client.connect();
            await assert.rejects(operation, err => err.code === 'auth_failed' && err.protocol === true);
            assert.equal(errors.length, 1);
            assert.equal(client.socket, null);
            assert.equal(client._connectionPromise, null);
            assert.equal(client._processes.size, 0);
            assert.equal(client._registrations.size, 0);
            assert.equal(client.poolName, 'pool-a');
            const join = target.records.find(record => record.msg.type === 'join_pool').msg;
            assert.equal(join.payload.auth_token, changePool ? 'denied-token' : 'old-token');
            await nextTurn();
            assert.equal(target.records.filter(record => record.msg.type === 'hello').length, 1);
            assert.equal(router.records.filter(record => record.msg.type === 'hello').length, 1);
        }
    });

    test(`${label}: old-server ACK compatibility and explicit redirect_required remain intact`, { timeout: 10000 }, async t => {
        const server = await tcpFixture(t, (msg, peer) => {
            if (['join_pool', 'switch_pool'].includes(msg.type)) peer.ack(msg);
        });
        const client = clientFor(t, Client, server, { allowRedirects: false });
        await client.connect();
        const socket = client.socket;
        await client.switchPool('pool-b');
        assert.equal(client.poolName, 'pool-b');
        assert.equal(client.socket, socket);
        assert.deepEqual(server.records[0].msg.payload.capabilities, ['pool_redirect_v1']);
        for (const options of [{ allowRedirects: false }, { maxRedirects: 0 }, {}]) {
            const owner = await tcpFixture(t);
            const router = await tcpFixture(t, (msg, peer) => {
                if (msg.type !== 'join_pool') return;
                if (Object.keys(options).length) peer.send(redirect(msg, owner, router));
                else peer.send({ type: 'error', request_id: msg.request_id, pool: null, payload: { code: 'redirect_required', message: 'Upgrade SDK for pods' } });
            });
            const denied = clientFor(t, Client, router, options);
            await assert.rejects(denied.connect(), err => err.code === 'redirect_required');
            assert.equal(owner.records.length, 0);
            assert.equal(denied._connectionPromise, null);
        }
    });

    test(`${label}: redirect settings reject unbounded or malformed configuration`, t => {
        for (const maxRedirects of [-1, 17, 1.5, Infinity, '4', true]) {
            assert.throws(() => clientFor(t, Client, { port: 1 }, { maxRedirects }), /maxRedirects/);
        }
        for (const allowRedirects of [0, 1, 'true']) {
            assert.throws(() => clientFor(t, Client, { port: 1 }, { allowRedirects }), /allowRedirects/);
        }
        for (const maxRedirects of [0, 4, 16]) {
            assert.equal(clientFor(t, Client, { port: 1 }, { maxRedirects }).maxRedirects, maxRedirects);
        }
    });

    test(`${label}: error listener explicit reconnect survives invalid-router cleanup`, { timeout: 10000 }, async t => {
        let first = true;
        const owner = await tcpFixture(t, (msg, peer) => {
            if (msg.type === 'join_pool') peer.ack(msg);
        });
        const router = await tcpFixture(t, (msg, peer) => {
            if (msg.type !== 'join_pool') return;
            const packet = redirect(msg, owner, router);
            if (first) { first = false; packet.payload.port = 0; }
            peer.send(packet);
        });
        const client = clientFor(t, Client, router);
        let reconnected;
        let errors = 0;
        client.on('error', () => { errors++; reconnected = client.connect(); });
        const connection = client.connect();
        await assert.rejects(connection, err => err.code === 'invalid_redirect');
        await reconnected;
        assert.equal(errors, 1);
        assert.equal(client._ready, true);
        assert.equal(client.connected, true);
        assert.equal(client.endpoint.port, owner.port);
        assert.equal(router.records.filter(record => record.msg.type === 'hello').length, 2);
    });

    test(`${label}: disconnect listener can cancel an intentional owner handoff without reconnect`, { timeout: 10000 }, async t => {
        const unused = await tcpFixture(t);
        const server = await tcpFixture(t, (msg, peer) => {
            if (msg.type === 'join_pool') peer.ack(msg);
            if (msg.type === 'switch_pool') peer.send(redirect(msg, unused, server));
        });
        const client = clientFor(t, Client, server);
        await client.connect();
        let disconnected = 0;
        client.on('disconnect', () => { disconnected++; client.disconnect(); });
        await assert.rejects(client.switchPool('pool-b'), err => err.code === 'disconnected');
        await nextTurn();
        assert.equal(client.socket, null);
        assert.equal(client._connectionPromise, null);
        assert.equal(client._connecting, false);
        assert.equal(disconnected, 1);
        assert.equal(unused.records.length, 0);
        assert.equal(server.records.filter(record => record.msg.type === 'hello').length, 1);
    });

    test(`${label}: low-level membership redirects retain the final ACK envelope contract`, { timeout: 10000 }, async t => {
        for (const type of ['join_pool', 'switch_pool']) {
            const owner = await tcpFixture(t, (msg, peer) => {
                if (msg.type === 'join_pool') peer.ack(msg, { joined: true, owner: 'final-owner' });
            });
            const server = await tcpFixture(t, (msg, peer) => {
                if (msg.type === 'join_pool' && msg.payload.pool === 'pool-a') peer.ack(msg);
                else if (msg.type === type) peer.send(redirect(msg, owner, server));
            });
            const client = clientFor(t, Client, server);
            await client.connect();
            const response = await client.sendRequest(type, { client_id: client.clientId, pool: 'pool-b', auth_token: 'token' });
            const joined = owner.records.find(record => record.msg.type === 'join_pool').msg;
            assert.deepEqual(response, { type: 'ack', request_id: joined.request_id, pool: null, payload: { joined: true, owner: 'final-owner' } });
            assert.equal(client.poolName, 'pool-b');
            assert.equal(client.authToken, 'token');
            assert.equal(client.endpoint.port, owner.port);
            assert.equal(client.pending.size, 0);
        }
    });

    test(`${label}: same-pool redirected replacement rejects remaining routes and registrations`, { timeout: 10000 }, async t => {
        const owner = await tcpFixture(t, (msg, peer) => {
            if (msg.type === 'join_pool') peer.ack(msg);
        });
        const server = await tcpFixture(t, (msg, peer) => {
            if (['join_pool', 'register_process'].includes(msg.type)) peer.ack(msg);
            if (msg.type === 'switch_pool') peer.send(redirect(msg, owner, server));
        });
        const client = clientFor(t, Client, server);
        await client.connect();
        const started = deferred();
        const release = deferred();
        let calls = 0;
        await client.process.register(async () => { calls++; started.resolve(); return release.promise; }, 'job');
        const oldHandler = client._handleAppCall({ type: 'call_app', request_id: 'same-pool-old-hop', pool: null, payload: { event: 'pod-node:job', data: {} } });
        await started.promise;
        const pending = client.process.call('worker:pending');
        const rejected = assert.rejects(pending, err => err.code === 'connection_replaced');
        const generation = client._generation;
        await client.switchPool('pool-a');
        await rejected;
        assert.ok(client._generation > generation);
        assert.equal(client._processes.size, 0);
        assert.equal(client._registrations.size, 0);
        assert.equal(client.poolName, 'pool-a');
        release.resolve('stale');
        await oldHandler;
        assert.equal(calls, 1);
        assert.equal(server.records.some(record => record.msg.type === 'app_result'), false);
        assert.equal(owner.records.some(record => ['app_result', 'call_process', 'register_process'].includes(record.msg.type)), false);
        assert.equal(client.pending.size, 0);
    });

    test(`${label}: redirects for unsent membership and expired waiters cannot create a socket hop`, { timeout: 10000 }, async t => {
        const clock = controlledTime(t);
        const unused = await tcpFixture(t);
        const server = await tcpFixture(t, (msg, peer) => {
            if (msg.type === 'join_pool') peer.ack(msg);
            if (msg.type === 'list_clients') peer.ack(msg);
        });
        const client = clientFor(t, Client, server, { timeout: 100 });
        await client.connect();
        client._writeBlocked = true;
        const switching = client.switchPool('pool-b');
        const rejection = assert.rejects(switching, err => err.code === 'timeout');
        const entry = [...client.pending.values()].find(item => item.requestType === 'switch_pool');
        const packet = redirect({ request_id: entry.requestId, client_id: client.clientId, payload: { pool: 'pool-b' } }, unused, server);
        const peer = [...server.peers][0];
        peer.send(packet);
        // Process the uncorrelated frame before releasing the writer's explicit barrier.
        const observed = deferred();
        const handle = client.handleMessage.bind(client);
        client.handleMessage = msg => { handle(msg); if (msg.type === 'redirect') observed.resolve(); };
        await observed.promise;
        assert.equal(client.pending.has(entry.requestId), true);
        assert.equal(entry.sent, false);
        clock.advance(100);
        await rejection;
        assert.equal(client._ready, true, 'unsent switch failure leaves known old membership');
        peer.send(packet);
        client._writeBlocked = false;
        const barrier = client.clients();
        await barrier;
        assert.equal(unused.records.length, 0);
        assert.equal(server.records.some(record => record.msg.type === 'switch_pool'), false);
        assert.equal(client._switching, false);
        assert.equal(client.pending.size, 0);
    });

    test(`${label}: numeric IPv4 loopback and expanded IPv6 loopback normalize without DNS targets`, async t => {
        const client = clientFor(t, Client, { port: 10001 }, { host: 'LOCALHOST' });
        const flow = client._redirectFlow('pool-a', null, performance.now() + 1000, 'connect');
        const packet = redirect({ request_id: 'membership', client_id: client.clientId, payload: { pool: 'pool-a' } }, { port: 10002 }, { port: 10001 }, {
            host: '0:0:0:0:0:0:0:1', router_host: '127.0.0.2', ws_port: 10003, router_ws_port: 10004
        });
        assert.equal(client._redirectTarget(packet, { redirectFlow: flow }).host, '::1');
        packet.payload.host = '127.255.0.1';
        assert.equal(client._redirectTarget(packet, { redirectFlow: flow }).host, '127.255.0.1');
        packet.payload.host = '::1';
        packet.payload.port = 10001;
        assert.throws(() => client._redirectTarget(packet, { redirectFlow: flow }), err => err.code === 'redirect_loop');
    });

    if (queued) {
        test(`${label}: initial transport replacement rejects queued effects, never replays registrations or workers`, { timeout: 10000 }, async t => {
            const owner = await tcpFixture(t);
            const router = await tcpFixture(t);
            const client = clientFor(t, Client, router);
            const connection = client.connect();
            const set = client.set('queued-write', 'must not replay');
            const call = client.process.call('worker:queued');
            let runs = 0;
            const registration = client.process.register(() => { runs++; return 'never rerun'; }, 'job');
            const rejected = [set, call, registration].map(promise => assert.rejects(promise, err => err.code === 'connection_replaced'));
            const routerJoin = await router.waitFor(record => record.msg.type === 'join_pool');
            routerJoin.peer.send(redirect(routerJoin.msg, owner, router));
            const ownerJoin = await owner.waitFor(record => record.msg.type === 'join_pool');
            await Promise.all(rejected);
            assert.equal(client._opQueue.length, 0);
            assert.equal(client._processes.size, 0);
            const afterReplacement = client.set('newly-requested', 'explicit');
            ownerJoin.peer.ack(ownerJoin.msg);
            await connection;
            const newWrite = await owner.waitFor(record => record.msg.type === 'set_buffer');
            newWrite.peer.ack(newWrite.msg);
            await afterReplacement;
            assert.equal(runs, 0);
            for (const fixture of [router, owner]) assert.equal(fixture.records.some(record => ['call_process', 'register_process'].includes(record.msg.type)), false);
            assert.deepEqual(owner.records.filter(record => record.msg.type === 'set_buffer').map(record => record.msg.payload.key), ['newly-requested']);
            assert.equal(client.pending.size, 0);
            assert.equal(client._queuedBytes, 0);
        });
    }
}
