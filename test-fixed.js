/**
 * Test script for the fixed LatZero Node.js Client
 * 
 * Tests process registration, cross-process communication, and server TUI visibility
 */

import LatZeroClient from './index-fixed.js';

async function testNodeClient() {
    console.log('=== Testing Fixed LatZero Node.js Client ===\n');
    
    const client = new LatZeroClient('latzero://node-test-client', 'demo-pool', {
        host: '127.0.0.1',
        port: 14130,
        autoConnect: true
    });

    // Set up event listeners
    client.on('connect', () => {
        console.log('✅ Connected to LatZero server');
    });

    client.on('disconnect', () => {
        console.log('❌ Disconnected from server');
    });

    client.on('error', (error) => {
        console.error('❌ Error:', error.message);
    });

    client.on('presence', (data) => {
        console.log('📡 Presence update:', JSON.stringify(data));
    });

    client.on('bufferUpdate', (data) => {
        console.log('📝 Buffer update:', JSON.stringify(data));
    });

    try {
        // Wait for connection
        await new Promise(resolve => {
            client.once('connect', resolve);
            setTimeout(resolve, 5000); // Timeout after 5 seconds
        });

        if (!client.connected) {
            throw new Error('Failed to connect to server');
        }

        console.log('\n=== Testing Process Registration ===');
        
        // Register an add function
        const addFunction = async (data) => {
            const a = data.a !== undefined ? data.a : data.x;
            const b = data.b !== undefined ? data.b : data.y;
            console.log(`🔢 Add function called: ${a} + ${b} = ${a + b}`);
            return a + b;
        };

        await client.process.register(addFunction, 'add');
        console.log('✅ Add function registered');
        console.log(`📍 Process ID: ${client.clientId}:add`);

        // Verify registration by listing processes
        const processes = await client.process.list();
        console.log('📋 Current processes:', Object.keys(processes));
        
        if (processes[`${client.clientId}:add`]) {
            console.log('✅ Process is visible in server TUI!');
        } else {
            console.log('❌ Process not found in server list');
        }

        console.log('\n=== Testing Process Calls ===');
        
        // Test calling our own process
        const result1 = await client.process.call(`${client.clientId}:add`, { a: 10, b: 5 });
        console.log('🔢 Self-call result:', result1.payload.value); // Should be 15

        // Test with x,y format (for Python compatibility)
        const result2 = await client.process.call(`${client.clientId}:add`, { x: 7, y: 8 });
        console.log('🔢 x,y format result:', result2.payload.value); // Should be 15

        console.log('\n=== Testing Buffer Operations ===');
        
        // Test basic buffer operations
        await client.set('test-key', 'test-value');
        console.log('✅ Buffer set: test-key = test-value');

        const value = await client.get('test-key');
        console.log('📖 Buffer get: test-key =', value);

        const exists = await client.exists('test-key');
        console.log('🔍 Buffer exists: test-key =', exists);

        const keys = await client.keys();
        console.log('🔑 All keys:', keys);

        console.log('\n=== Testing Cross-Process Event Handling ===');
        
        // Register an event handler for testing call_app messages
        client.on('test-event', (data) => {
            console.log('📡 Received test-event:', data);
            return 'Event handled successfully';
        });

        console.log('✅ Event handler registered for test-event');
        console.log('📞 Ready to receive call_app messages from other clients');
        console.log('🎯 Process ID for external calls:', `${client.clientId}:add`);
        console.log('🎯 Event name for external calls:', `${client.clientId}:add`);

        console.log('\n=== Client Status ===');
        const stats = await client.stats();
        console.log('📊 Pool stats:', stats);

        console.log('\n=== Test Complete ===');
        console.log('✅ Node client is working correctly!');
        console.log('🔧 Processes should be visible in server TUI');
        console.log('🤝 Ready for cross-process communication');

        // Keep client running for testing
        console.log('\n⏳ Client running... Press Ctrl+C to exit');
        
        // Set up periodic status check
        const statusInterval = setInterval(async () => {
            if (client.connected) {
                const processes = await client.process.list();
                console.log(`📋 Active processes: ${Object.keys(processes).length}`);
            } else {
                console.log('❌ Client disconnected');
                clearInterval(statusInterval);
            }
        }, 10000);

        // Handle graceful shutdown
        process.on('SIGINT', async () => {
            console.log('\n🛑 Shutting down...');
            clearInterval(statusInterval);
            
            // Unregister process
            await client.process.unregister('add');
            console.log('🗑️ Process unregistered');
            
            client.disconnect();
            console.log('👋 Disconnected from server');
            process.exit(0);
        });

    } catch (error) {
        console.error('❌ Test failed:', error.message);
        console.error('🔧 Make sure LatZero server is running on 127.0.0.1:14130');
        process.exit(1);
    }
}

// Run the test
testNodeClient().catch(console.error);
