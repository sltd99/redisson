package org.redisson.test;

import org.junit.Test;
import org.redisson.Redisson;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * Demonstration of Redisson lock mechanism and master-slave failover handling
 */
public class RedissonLockMechanismDemo {

    @Test
    public void demonstrateLockWithWaitSynchronization() {
        // Create configuration that uses WAIT command for slave synchronization
        Config config = new Config();
        config.useSingleServer()
              .setAddress("redis://127.0.0.1:6379");
        
        // Enable slave synchronization checks
        config.setCheckLockSyncedSlaves(true);
        config.setSlavesSyncTimeout(1000); // 1 second timeout for slave sync
        
        RedissonClient redisson = Redisson.create(config);
        
        try {
            RLock lock = redisson.getLock("demo-lock");
            
            // This will use syncedEval internally which employs WAIT command
            // to ensure replication to slaves before returning success
            boolean acquired = lock.tryLock(5, 10, TimeUnit.SECONDS);
            
            assertTrue("Lock should be acquired", acquired);
            
            // Verify lock is held
            assertTrue("Lock should be held by current thread", lock.isHeldByCurrentThread());
            assertTrue("Lock should be locked", lock.isLocked());
            
            // Release the lock
            lock.unlock();
            
            assertFalse("Lock should not be held after unlock", lock.isHeldByCurrentThread());
            
        } finally {
            redisson.shutdown();
        }
    }
    
    @Test
    public void demonstrateWatchdogMechanism() throws InterruptedException {
        Config config = new Config();
        config.useSingleServer()
              .setAddress("redis://127.0.0.1:6379");
        
        // Set a shorter watchdog timeout for demonstration
        config.setLockWatchdogTimeout(5000); // 5 seconds
        
        RedissonClient redisson = Redisson.create(config);
        
        try {
            RLock lock = redisson.getLock("watchdog-demo");
            
            // Acquire lock without specifying lease time - enables watchdog
            lock.lock();
            
            assertTrue("Lock should be acquired", lock.isHeldByCurrentThread());
            
            // Wait longer than initial lease time to test watchdog renewal
            Thread.sleep(6000); // 6 seconds
            
            // Lock should still be held due to watchdog renewal
            assertTrue("Lock should still be held due to watchdog", lock.isHeldByCurrentThread());
            
            lock.unlock();
            
        } finally {
            redisson.shutdown();
        }
    }
    
    @Test
    public void demonstrateReentrantLock() {
        Config config = new Config();
        config.useSingleServer()
              .setAddress("redis://127.0.0.1:6379");
        
        RedissonClient redisson = Redisson.create(config);
        
        try {
            RLock lock = redisson.getLock("reentrant-demo");
            
            // First acquisition
            lock.lock();
            assertTrue("Lock should be acquired", lock.isHeldByCurrentThread());
            assertEquals("Hold count should be 1", 1, lock.getHoldCount());
            
            // Reentrant acquisition (same thread)
            lock.lock();
            assertTrue("Lock should still be held", lock.isHeldByCurrentThread());
            assertEquals("Hold count should be 2", 2, lock.getHoldCount());
            
            // First unlock
            lock.unlock();
            assertTrue("Lock should still be held", lock.isHeldByCurrentThread());
            assertEquals("Hold count should be 1", 1, lock.getHoldCount());
            
            // Second unlock
            lock.unlock();
            assertFalse("Lock should not be held", lock.isHeldByCurrentThread());
            assertEquals("Hold count should be 0", 0, lock.getHoldCount());
            
        } finally {
            redisson.shutdown();
        }
    }
    
    @Test
    public void demonstrateElastiCacheServerlessCompatibility() {
        // Configuration optimized for AWS ElastiCache Serverless
        Config config = new Config();
        
        // Disable slave synchronization check for serverless compatibility
        config.setCheckLockSyncedSlaves(false);
        config.setLockWatchdogTimeout(30000);
        
        // Example cluster configuration for ElastiCache Serverless with cluster mode
        config.useClusterServers()
              .addNodeAddress("redis://127.0.0.1:6379") // Replace with actual ElastiCache endpoint
              .setConnectTimeout(3000)
              .setTimeout(3000);
        
        // Alternative: Proxy mode for single endpoint ElastiCache Serverless
        // config.useProxyServers()
        //       .addAddress("redis://your-elasticache-serverless-endpoint:6379");
        
        RedissonClient redisson = Redisson.create(config);
        
        try {
            RLock lock = redisson.getLock("elasticache-serverless-lock");
            
            // Lock operations work normally even without WAIT command support
            boolean acquired = lock.tryLock(5, 10, TimeUnit.SECONDS);
            assertTrue("Lock should be acquired in serverless mode", acquired);
            
            // Verify basic lock functionality
            assertTrue("Lock should be held", lock.isHeldByCurrentThread());
            assertTrue("Lock should be locked", lock.isLocked());
            
            // Test reentrancy
            lock.lock();
            assertEquals("Hold count should be 2", 2, lock.getHoldCount());
            
            lock.unlock();
            assertEquals("Hold count should be 1", 1, lock.getHoldCount());
            
            lock.unlock();
            assertFalse("Lock should not be held after final unlock", lock.isHeldByCurrentThread());
            
        } finally {
            redisson.shutdown();
        }
    }
}