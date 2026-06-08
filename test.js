import LatZeroClient, { LatZeroAsyncClient } from './index.js';

const HOST = '127.0.0.1';
const PORT = 14130;

async function run() {
    // ─── 1. LatZeroClient (sync-style, auto-queueing) ────────────────────────
    console.log('=== LatZeroClient (sync-style) ===\n');

    const sync = new LatZeroClient('latzero://test-sync', 'test', {
        host: HOST, port: PORT
    });

    // These queue internally and fire once connected — no await needed:
    sync.set('hello', 'world');
    sync.process.register((data) => data.a + data.b, 'add');

    await sync.connect();
    console.log('Connected');

    const val = await sync.get('hello');
    console.log('get("hello") →', val);

    const result = await sync.process.call('test-sync:add', { a: 3, b: 7 });
    console.log('process.call("test-sync:add", {a:3, b:7}) →', result.payload.value);

    sync.disconnect();
    console.log('Disconnected\n');

    // ─── 2. LatZeroAsyncClient (explicit async/await) ────────────────────────
    console.log('=== LatZeroAsyncClient (async-style) ===\n');

    const async = new LatZeroAsyncClient('latzero://test-async', 'test', {
        host: HOST, port: PORT, autoConnect: true
    });

    await new Promise((resolve, reject) => {
        async.once('connect', resolve);
        async.once('error', reject);
        setTimeout(() => reject(new Error('Connection timeout')), 5000);
    });
    console.log('Connected');

    await async.set('foo', { bar: [1, 2, 3] });
    const v = await async.get('foo');
    console.log('get("foo") →', JSON.stringify(v));

    const exists = await async.exists('foo');
    console.log('exists("foo") →', exists);

    const keys = await async.keys();
    console.log('keys() →', keys);

    await async.process.register((data) => data.x * data.y, 'multiply');
    const r2 = await async.process.call('test-async:multiply', { x: 4, y: 5 });
    console.log('process.call("multiply", {x:4, y:5}) →', r2.payload.value);

    const processes = await async.process.list();
    console.log('Registered processes:', Object.keys(processes));

    async.disconnect();
    console.log('Disconnected\n');

    console.log('=== All tests passed ===');
}

run().catch((err) => {
    console.error('FAILED:', err.message);
    console.error('Make sure latzero-server is running on', HOST + ':' + PORT);
    process.exit(1);
});
