# How Redisson Locks Work and Handle Master-Slave Failover

## Overview

Redisson implements distributed locks using Redis/Valkey with several sophisticated mechanisms to handle master-slave failover scenarios and ensure lock consistency across the cluster.

## Lock Acquisition Process

### 1. Basic Lock Implementation

The main lock implementation is in `RedissonLock.java`. When acquiring a lock:

```lua
if ((redis.call('exists', KEYS[1]) == 0) 
    or (redis.call('hexists', KEYS[1], ARGV[2]) == 1)) then 
    redis.call('hincrby', KEYS[1], ARGV[2], 1); 
    redis.call('pexpire', KEYS[1], ARGV[1]); 
    return nil; 
end; 
return redis.call('pttl', KEYS[1]);
```

- **KEYS[1]**: Lock name
- **ARGV[1]**: Lease time in milliseconds
- **ARGV[2]**: Lock identifier (client_id:thread_id)

The lock uses a Redis hash structure where:
- The key is the lock name
- The hash field is the lock identifier (client ID + thread ID)
- The hash value is the reentrant count
- The key has an expiration time

### 2. Watchdog Mechanism

Redisson implements a "watchdog" mechanism for lock renewal:

- **Default timeout**: 30 seconds (`lockWatchdogTimeout`)
- **Renewal process**: Automatically renews locks every 10 seconds (1/3 of timeout)
- **Batch processing**: Renews up to 100 locks per batch (`lockWatchdogBatchSize`)

The watchdog prevents locks from hanging indefinitely if the client crashes.

## Master-Slave Failover Handling

### 1. Synchronization with WAIT Command

Redisson uses Redis's `WAIT` and `WAITAOF` commands to ensure replication synchronization:

```java
// From CommandAsyncService.java
private <T> RFuture<T> syncedEval(long timeout, SyncMode syncMode, boolean retry, 
                                  String key, Codec codec, RedisCommand<T> evalCommandType,
                                  String script, List<Object> keys, Object... params)
```

**Key Configuration Parameters:**
- `slavesSyncTimeout`: Default 1000ms - timeout for slave synchronization
- `checkLockSyncedSlaves`: Default true - verifies synchronized slaves after lock acquisition

### 2. Synchronization Modes

Redisson supports three synchronization modes:

1. **AUTO**: Automatically detects and uses available synchronization
2. **WAIT**: Uses Redis `WAIT` command for replication synchronization
3. **WAIT_AOF**: Uses Redis `WAITAOF` command for AOF persistence synchronization

### 3. The Window Problem and Solution

**The Problem:**
When a master node fails after a lock is acquired but before the lock information is replicated to slaves, there's a risk that:
1. Client A acquires lock on master
2. Master fails before replication
3. Slave becomes new master (without lock info)
4. Client B can acquire the same lock

**Redisson's Solution:**

#### A. WAIT Command Usage
```java
// Redisson uses WAIT command to ensure replication before returning success
CommandBatchService ee = createCommandBatchService(BatchOptions.defaults());
ee.writeAsync(key, RedisCommands.WAIT, 0, 0);
ee.writeAsync(key, RedisCommands.WAITAOF, 0, 0, 0);
```

The `WAIT` command blocks until:
- A specified number of slaves have received the write
- Or a timeout occurs

#### B. Slave Synchronization Check
```java
if (getServiceManager().getCfg().isCheckLockSyncedSlaves()
        && res.getSyncedSlaves() == 0 && availableSlaves > 0) {
    throw new CompletionException(
        new NoSyncedSlavesException("None of slaves were synced. Try to increase slavesSyncTimeout setting or set checkLockSyncedSlaves = false."));
}
```

If no slaves are synchronized, Redisson throws an exception rather than returning success.

#### C. Conditional Execution
The synchronization only happens when:
- Not using single server configuration
- Not in batch mode
- WAIT commands are supported by the Redis version
- Available slaves exist

```java
if (getServiceManager().getCfg().isSingleConfig()
        || this instanceof CommandBatchService
       || (waitSupportedCommands != null && waitSupportedCommands.isEmpty() && syncMode == SyncMode.AUTO)
        || (waitSupportedCommands != null && !waitSupportedCommands.contains(RedisCommands.WAIT.getName()) && syncMode == SyncMode.WAIT)
            || (waitSupportedCommands != null && !waitSupportedCommands.contains(RedisCommands.WAITAOF.getName()) && syncMode == SyncMode.WAIT_AOF)
        ) {
    // Skip synchronization
    return evalWriteAsync(key, codec, evalCommandType, script, keys, params);
}
```

## Lock Release Process

During unlock, Redisson:

1. **Decrements the reentrant counter**
2. **Checks if counter reaches zero**
3. **Deletes the lock and publishes unlock message**
4. **Uses syncedEval for consistency**

```lua
local counter = redis.call('hincrby', KEYS[1], ARGV[3], -1); 
if (counter > 0) then 
    redis.call('pexpire', KEYS[1], ARGV[2]); 
    return 0; 
else 
    redis.call('del', KEYS[1]); 
    redis.call(ARGV[4], KEYS[2], ARGV[1]); 
    return 1; 
end;
```

## Advanced Lock Types

### 1. RedLock (Deprecated)
- Originally implemented for multiple independent Redis instances
- Deprecated due to theoretical issues identified by Martin Kleppmann
- Superseded by single-instance locks with proper replication

### 2. Fair Lock
- Implements FIFO ordering for lock acquisition
- Uses queuing mechanism with timeouts
- Handles dead thread detection (5-second timeout per dead thread)

### 3. Fenced Lock
- Provides fencing tokens to detect delayed clients
- Prevents issues from long GC pauses or network delays
- Each successful lock operation returns an incrementing token

## Configuration Best Practices

### 1. For High Availability
```java
Config config = new Config();
config.setCheckLockSyncedSlaves(true);  // Verify slave synchronization
config.setSlavesSyncTimeout(3000);      // Increase timeout for reliability
config.setLockWatchdogTimeout(30000);   // Default watchdog timeout
```

### 2. For Performance vs Consistency Tradeoff
```java
// High consistency (slower)
config.setCheckLockSyncedSlaves(true);
config.setSlavesSyncTimeout(5000);

// High performance (potential consistency issues)
config.setCheckLockSyncedSlaves(false);
config.setSlavesSyncTimeout(100);
```

## AWS ElastiCache Serverless Compatibility

**Yes, Redisson is fully compatible with AWS ElastiCache Serverless**, with some important considerations for the lock mechanism:

### Supported Configuration Modes

1. **Cluster Mode** (Recommended for ElastiCache Serverless with cluster mode enabled):
```java
Config config = new Config();
config.useClusterServers()
      .addNodeAddress("rediss://your-elasticache-serverless-endpoint:6379");
```

2. **Proxy Mode** (For ElastiCache Serverless single endpoint):
```java
Config config = new Config();
config.useProxyServers()
      .addAddress("rediss://your-elasticache-serverless-endpoint:6379");
```

### WAIT Command Considerations

ElastiCache Serverless may not support Redis `WAIT` and `WAITAOF` commands used for enhanced lock synchronization. Redisson handles this gracefully:

1. **Automatic Fallback**: If WAIT commands are unsupported, Redisson automatically falls back to standard lock operations
2. **Error Handling**: The system detects "ERR unknown command" responses and adjusts behavior accordingly
3. **Configuration Options**: You can disable synchronization checks if needed:

```java
Config config = new Config();
config.setCheckLockSyncedSlaves(false);  // Disable sync verification for serverless
config.useClusterServers()
      .addNodeAddress("rediss://your-elasticache-serverless-endpoint:6379");
```

### Lock Behavior with ElastiCache Serverless

- **Basic Lock Functionality**: All core lock features work normally (acquisition, release, reentrancy, watchdog)
- **Reduced Failover Protection**: Without WAIT command support, the enhanced master-slave failover protection is not available
- **Automatic Scaling**: Locks work seamlessly as ElastiCache Serverless scales up/down based on demand
- **SSL/TLS Support**: Use `rediss://` protocol for secure connections

### Best Practices for ElastiCache Serverless

```java
Config config = new Config();
config.setCheckLockSyncedSlaves(false);     // Disable for serverless compatibility
config.setLockWatchdogTimeout(30000);       // Standard watchdog timeout
config.useClusterServers()
      .addNodeAddress("rediss://your-endpoint:6379")
      .setConnectTimeout(3000)
      .setTimeout(3000);

RedissonClient redisson = Redisson.create(config);
```

## Summary

Redisson handles the master-slave failover window through:

1. **WAIT Command Usage**: Ensures replication to slaves before confirming lock acquisition (when supported)
2. **Graceful Fallback**: Automatically adapts when WAIT commands are unavailable (e.g., ElastiCache Serverless)
3. **Slave Synchronization Verification**: Checks that at least one slave received the lock (configurable)
4. **Configurable Timeouts**: Allows tuning between consistency and performance
5. **Watchdog Mechanism**: Prevents indefinite lock hanging
6. **Atomic Operations**: Uses Lua scripts for atomic lock operations
7. **Cloud Service Compatibility**: Works with AWS ElastiCache Serverless, Azure Redis Cache, and other managed services

The key insight is that Redisson **does use the WAIT command when available** to handle the critical window where master fails but lock info isn't yet replicated to slaves. For services like AWS ElastiCache Serverless that may not support WAIT commands, Redisson provides graceful fallback mechanisms while maintaining core lock functionality.