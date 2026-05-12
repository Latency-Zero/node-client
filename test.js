import LatZeroClient from './index.js';

async function test() {
    console.log('=== LatZero Node.js Client Test ===\n');
    
    // Create client
    const client = new LatZeroClient('latzero://node-test-client', 'alpha');
    
    // Wait for connection
    await new Promise(resolve => {
        client.on('connect', resolve);
        client.on('error', (err) => {
            console.error('Connection failed:', err);
            process.exit(1);
        });
    });
    
    console.log('✓ Connected to server');
    
    // Test basic operations
    console.log('\n--- Testing Key-Value Operations ---');
    await client.set('user', { name: 'Alice', age: 30 }, { persistent: true });
    console.log('✓ Set user data');
    
    const user = await client.get('user');
    console.log('✓ Get user:', user);
    
    await client.set('counter', 42);
    console.log('✓ Set counter');
    
    const keys = await client.keys();
    console.log('✓ Keys:', keys);
    
    // Test batch operations
    console.log('\n--- Testing Batch Operations ---');
    await client.mset({
        'batch1': 'value1',
        'batch2': 'value2',
        'batch3': 'value3'
    });
    console.log('✓ MSet completed');
    
    const values = await client.mget(['batch1', 'batch2', 'batch3']);
    console.log('✓ MGet:', values);
    
    // Test event handling
    console.log('\n--- Testing Events ---');
    client.on('test:event', (data) => {
        console.log('✓ Received test:event:', data);
    });
    
    client.on('presence', (data) => {
        console.log('✓ Presence update:', data);
    });
    
    // Test event emission
    await client.emitEvent('test:event', {
        data: { message: 'Hello from Node.js!' }
    });
    
    // Test statistics
    console.log('\n--- Testing Statistics ---');
    const stats = await client.stats();
    console.log('✓ Stats:', stats);
    
    // Cleanup
    console.log('\n--- Cleaning Up ---');
    await client.delete('counter');
    console.log('✓ Deleted counter');
    
    client.disconnect();
    console.log('✓ Disconnected');
    
    console.log('\n=== Test Complete ===');
}

test().catch(console.error);
